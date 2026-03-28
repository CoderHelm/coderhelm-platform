use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{info, warn};

use crate::models::InfraAnalyzeMessage;
use crate::WorkerState;

/// System prompt for infra analysis.
const SYSTEM: &str = r#"You are an expert AWS infrastructure reviewer.
Given infrastructure-as-code (CDK, Terraform, Serverless Framework, SAM/CloudFormation, or Pulumi) extracted from a repository, you will:
1. Generate a mermaid architecture-beta diagram of the infrastructure.
2. List findings with severity (error/warning/info), category (security/performance/cost/reliability), and a concise title + detail.

Output format — respond with EXACTLY two blocks and nothing else:

```mermaid
<mermaid architecture-beta diagram here>
```

```json
[
  {"severity": "warning", "category": "security", "title": "...", "detail": "...", "file": "optional/path.ts"}
]
```

Rules for the mermaid diagram:
- Use architecture-beta syntax only.
- Use logos:aws-* icons where available (logos:aws-lambda, logos:aws-dynamodb, logos:aws-sqs, logos:aws-s3, logos:aws-cloudfront, logos:aws-api-gateway, logos:aws-ses, logos:aws-secrets-manager, logos:aws-cloudwatch).
- Group services logically (edge, compute, data, async).
- Keep it under 15 services — collapse where needed.
- Flow left to right: internet → CDN/WAF → API → compute → data.
"#;

pub async fn run(
    state: &WorkerState,
    msg: InfraAnalyzeMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Get tenant to find installation_id + repos
    let tenant = get_tenant(state, &msg.tenant_id).await?;
    let Some((install_id, repos)) = tenant else {
        warn!(tenant_id = %msg.tenant_id, "Tenant not found for infra analyze");
        return Ok(());
    };

    // 2. Scan repos for CDK/Terraform files via GitHub API
    let github = crate::clients::github::GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        install_id,
        &state.http,
    )?;

    let infra_code = collect_infra_code(&github, &repos).await;

    if infra_code.is_empty() {
        // Store "no_infra" result
        store_no_infra(state, &msg.tenant_id).await?;
        info!(tenant_id = %msg.tenant_id, "No infrastructure code found");
        return Ok(());
    }

    // 3. Call Bedrock to analyze and generate diagram + findings
    let code_context = format_code_context(&infra_code);
    let response = call_bedrock(state, &code_context).await?;

    // 4. Parse response
    let (diagram, findings_json) = parse_response(&response);

    let diagram_str = diagram.unwrap_or_default();
    let findings_str = findings_json.unwrap_or_else(|| "[]".to_string());

    // 5. Store in DynamoDB
    let now = chrono::Utc::now().to_rfc3339();
    let scanned: Vec<String> = infra_code.iter().map(|(f, _)| f.clone()).collect();

    state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", AttributeValue::S(msg.tenant_id.clone()))
        .item("sk", AttributeValue::S("INFRA#analysis".to_string()))
        .item("status", AttributeValue::S("ready".to_string()))
        .item("has_infra", AttributeValue::Bool(true))
        .item("diagram", AttributeValue::S(diagram_str))
        .item(
            "diagram_title",
            AttributeValue::S("Architecture".to_string()),
        )
        .item("findings", AttributeValue::S(findings_str))
        .item(
            "scanned_repos",
            AttributeValue::S(serde_json::to_string(&scanned)?),
        )
        .item("cached_at", AttributeValue::S(now))
        .send()
        .await?;

    info!(tenant_id = %msg.tenant_id, files = scanned.len(), "Infra analysis complete");
    Ok(())
}

// ── Helpers ────────────────────────────────────────────────────────────────────

async fn get_tenant(
    state: &WorkerState,
    tenant_id: &str,
) -> Result<Option<(u64, Vec<String>)>, Box<dyn std::error::Error + Send + Sync>> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
        .key("sk", AttributeValue::S("META".to_string()))
        .send()
        .await?;

    let item = match result.item() {
        None => return Ok(None),
        Some(i) => i,
    };

    let install_id: u64 = item
        .get("github_install_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);

    // Get repos from a separate query
    let repos_result = state
        .dynamo
        .query()
        .table_name(&state.config.table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", AttributeValue::S(tenant_id.to_string()))
        .expression_attribute_values(":prefix", AttributeValue::S("REPO#".to_string()))
        .send()
        .await?;

    let repos: Vec<String> = repos_result
        .items()
        .iter()
        .filter_map(|item| item.get("repo_name").and_then(|v| v.as_s().ok()).cloned())
        .collect();

    Ok(Some((install_id, repos)))
}

async fn collect_infra_code(
    github: &crate::clients::github::GitHubClient,
    repos: &[String],
) -> Vec<(String, String)> {
    let mut found: Vec<(String, String)> = Vec::new();

    for repo_full in repos.iter().take(5) {
        let parts: Vec<&str> = repo_full.splitn(2, '/').collect();
        if parts.len() != 2 {
            continue;
        }
        let (owner, repo) = (parts[0], parts[1]);

        // AWS CDK — detect cdk.json then discover stack files via directory listing
        let cdk_locations = ["", "infra", "infrastructure"];
        for base in &cdk_locations {
            let cdk_json = if base.is_empty() {
                "cdk.json".to_string()
            } else {
                format!("{base}/cdk.json")
            };
            if github.get_file_content(owner, repo, &cdk_json).await.is_ok() {
                // Determine the root directory for CDK files
                let cdk_root = if base.is_empty() { "." } else { base };

                // Scan common CDK subdirectories for .ts, .py, .js files
                let scan_dirs: Vec<String> = if cdk_root == "." {
                    vec![
                        "bin".to_string(),
                        "lib".to_string(),
                    ]
                } else {
                    vec![
                        format!("{cdk_root}/bin"),
                        format!("{cdk_root}/lib"),
                        cdk_root.to_string(),
                    ]
                };

                // Also grab the CDK app entry if it's a Python CDK project (app.py)
                let app_path = if cdk_root == "." {
                    "app.py".to_string()
                } else {
                    format!("{cdk_root}/app.py")
                };
                if let Ok(c) = github.get_file_content(owner, repo, &app_path).await {
                    found.push((format!("{repo_full}/{app_path}"), c));
                }

                for dir in &scan_dirs {
                    if let Ok(entries) = github.list_directory(owner, repo, dir, "HEAD").await {
                        for entry in entries {
                            if entry.entry_type != "file" {
                                continue;
                            }
                            let is_cdk_file = entry.name.ends_with(".ts")
                                || entry.name.ends_with(".py")
                                || entry.name.ends_with(".js");
                            let skip = entry.name.ends_with(".test.ts")
                                || entry.name.ends_with(".d.ts")
                                || entry.name == "jest.config.js";
                            if is_cdk_file && !skip {
                                if let Ok(c) =
                                    github.get_file_content(owner, repo, &entry.path).await
                                {
                                    found.push((format!("{repo_full}/{}", entry.path), c));
                                }
                            }
                        }
                    }
                }
                break;
            }
        }

        // Terraform
        for tf_path in &["main.tf", "infra/main.tf", "terraform/main.tf"] {
            if let Ok(content) = github.get_file_content(owner, repo, tf_path).await {
                found.push((format!("{repo_full}/{tf_path}"), content));
                break;
            }
        }

        // Serverless Framework
        for sls_path in &[
            "serverless.yml",
            "serverless.yaml",
            "serverless.ts",
            "infra/serverless.yml",
        ] {
            if let Ok(content) = github.get_file_content(owner, repo, sls_path).await {
                found.push((format!("{repo_full}/{sls_path}"), content));
                break;
            }
        }

        // SAM / CloudFormation
        for cfn_path in &[
            "template.yaml",
            "template.yml",
            "template.json",
            "sam/template.yaml",
            "cloudformation/template.yaml",
            "infra/template.yaml",
        ] {
            if let Ok(content) = github.get_file_content(owner, repo, cfn_path).await {
                found.push((format!("{repo_full}/{cfn_path}"), content));
                break;
            }
        }

        // Pulumi
        for pulumi_path in &["Pulumi.yaml", "infra/Pulumi.yaml"] {
            if let Ok(content) = github.get_file_content(owner, repo, pulumi_path).await {
                found.push((format!("{repo_full}/{pulumi_path}"), content));
                for prog_path in &["index.ts", "__main__.py", "main.go", "Pulumi.ts"] {
                    if let Ok(c) = github.get_file_content(owner, repo, prog_path).await {
                        found.push((format!("{repo_full}/{prog_path}"), c));
                    }
                }
                break;
            }
        }

        if found.len() >= 15 {
            break;
        }
    }

    found
}

fn format_code_context(infra_code: &[(String, String)]) -> String {
    infra_code
        .iter()
        .map(|(path, content)| {
            let trimmed = if content.len() > 4000 {
                &content[..4000]
            } else {
                content
            };
            let lang = if path.ends_with(".ts") {
                "typescript"
            } else if path.ends_with(".py") {
                "python"
            } else if path.ends_with(".go") {
                "go"
            } else if path.ends_with(".tf") {
                "hcl"
            } else if path.ends_with(".yaml") || path.ends_with(".yml") {
                "yaml"
            } else if path.ends_with(".json") {
                "json"
            } else {
                ""
            };
            format!("### {path}\n```{lang}\n{trimmed}\n```")
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

async fn call_bedrock(
    state: &WorkerState,
    code_context: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let prompt = format!(
        "Analyze this infrastructure code and generate the diagram and findings:\n\n{code_context}"
    );

    let messages = vec![aws_sdk_bedrockruntime::types::Message::builder()
        .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
        .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(prompt))
        .build()
        .map_err(|e| format!("Failed to build message: {e}"))?];

    let response = state
        .bedrock
        .converse()
        .model_id(&state.config.model_id)
        .system(aws_sdk_bedrockruntime::types::SystemContentBlock::Text(
            SYSTEM.to_string(),
        ))
        .set_messages(Some(messages))
        .send()
        .await?;

    let text = match response.output() {
        Some(aws_sdk_bedrockruntime::types::ConverseOutput::Message(msg)) => msg
            .content()
            .iter()
            .find_map(|block| {
                if let aws_sdk_bedrockruntime::types::ContentBlock::Text(t) = block {
                    Some(t.clone())
                } else {
                    None
                }
            })
            .unwrap_or_default(),
        _ => String::new(),
    };

    Ok(text)
}

fn parse_response(response: &str) -> (Option<String>, Option<String>) {
    let mermaid = extract_block(response, "mermaid");
    let json_block = extract_block(response, "json");
    (mermaid, json_block)
}

fn extract_block(text: &str, lang: &str) -> Option<String> {
    let open = format!("```{lang}");
    let start = text.find(&open)?;
    let after_open = start + open.len();
    let end = text[after_open..].find("```")?;
    let content = text[after_open..after_open + end].trim().to_string();
    if content.is_empty() {
        None
    } else {
        Some(content)
    }
}

async fn store_no_infra(
    state: &WorkerState,
    tenant_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", AttributeValue::S(tenant_id.to_string()))
        .item("sk", AttributeValue::S("INFRA#analysis".to_string()))
        .item("status", AttributeValue::S("no_infra".to_string()))
        .item("has_infra", AttributeValue::Bool(false))
        .item("cached_at", AttributeValue::S(now))
        .send()
        .await?;
    Ok(())
}
