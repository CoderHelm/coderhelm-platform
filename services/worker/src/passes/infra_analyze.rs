use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{info, warn};

use crate::models::InfraAnalyzeMessage;
use crate::WorkerState;

/// System prompt for infra analysis.
const SYSTEM: &str = r#"You are an expert AWS infrastructure reviewer.
Given infrastructure-as-code extracted from a repository, you will:
1. Generate a mermaid architecture-beta diagram of the infrastructure.
2. List findings — only severity error or warning.

Output format — respond with EXACTLY two blocks and nothing else:

```mermaid
<diagram>
```

```json
[{"severity":"warning","category":"security","title":"...","detail":"...","file":"optional/path.ts"}]
```

─── DIAGRAM RULES (architecture-beta syntax) ───

CRITICAL SYNTAX:
- First line must be: architecture-beta
- Groups: group {id}({icon})[{Label}]  — icon is REQUIRED in parens before label
- Services: service {id}({icon})[{Label}] in {group_id}  — "in {group}" is REQUIRED
- Edges: {svcId}:{T|B|L|R} --> {T|B|L|R}:{svcId}  — arrows connect ports
- Junctions: junction {id} in {group_id}
- IDs: lowercase alphanumeric + underscores ONLY. No hyphens, no dots, no special chars.
- Labels in []: alphanumeric, spaces, hyphens, periods ONLY. No slashes / brackets / braces / colons.
- DECLARE all groups and services FIRST, then ALL edges LAST. Never interleave.
- FORBIDDEN: -->|text|, <--, subgraph/end, ---, -.-, flowchart syntax of any kind.

ICONS — use logos:aws-* icons:
  logos:aws-lambda, logos:aws-dynamodb, logos:aws-sqs, logos:aws-s3,
  logos:aws-cloudfront, logos:aws-api-gateway, logos:aws-ses,
  logos:aws-secrets-manager, logos:aws-cloudwatch, logos:aws-waf,
  logos:aws-route53, logos:aws-iam, logos:aws-sns, logos:aws-eventbridge,
  logos:aws-ec2, logos:aws-ecs, logos:aws-step-functions, logos:aws-cognito.

─── UNDERSTANDING THE LAYOUT ENGINE (read carefully) ───

Mermaid architecture-beta uses cytoscape.js with fcose (force-directed) layout.
The renderer builds an IMPLICIT GRID from your edge port directions via BFS.
Each edge places connected nodes on a grid: R→L means "target is 1 column right",
B→T means "target is 1 row below". The layout engine ONLY works cleanly when:

  ★ Every connected pair of nodes is EXACTLY 1 grid unit apart.

When nodes end up >1 unit apart, the grid constraints collapse and you get:
diagonal lines, non-deterministic rendering, overlapping nodes, extreme spacing.

Additionally, each port (T, B, L, R) on a service supports EXACTLY ONE edge.
If you connect two edges to the same port, nodes will overlap.

─── LAYOUT STRATEGY ───

1. MAX 8 services total. Ruthlessly merge: "3 DynamoDB tables" → one node,
   "5 Lambdas" → one per role (API + Worker). Show only the MAIN request path.

2. Use exactly 3 groups arranged left-to-right:
   - group edge(cloud)[Edge] — CDN, WAF, API Gateway, Route53
   - group compute(cloud)[Compute] — Lambdas, ECS, Step Functions
   - group data(cloud)[Data] — DynamoDB, S3, SQS, SNS, SES, EventBridge

3. Keep 2-3 services per group max. If a group would have 4+, merge nodes.

4. EDGE RULES — these are the most important rules for clean rendering:

   a. BETWEEN groups: ONLY use R --> L (source right port to target left port).
      This places groups exactly 1 column apart in the grid.

   b. WITHIN a group: ONLY use B --> T (upper service bottom to lower service top).
      This stacks services vertically, each exactly 1 row apart.

   c. NEVER use T or B ports for inter-group edges — this creates diagonal
      lines because it places nodes on a different row AND column simultaneously.

   d. NEVER use R or L ports for intra-group edges — this forces horizontal
      layout inside a group, conflicting with the vertical stacking.

   e. NEVER connect backwards (data → compute, compute → edge).

5. JUNCTION RULES for fan-out:
   When a service needs to reach 2+ targets, you MUST use junctions because
   each port supports only one edge. Use this exact pattern:

   junction j1 in {group}
   source:R --> L:j1
   j1:R --> L:target_a
   j1:B --> T:target_b

   Junctions in the TARGET group (for inter-group fan-out):
   junction j_data in data
   api_fn:R --> L:j_data
   j_data:R --> L:db
   j_data:B --> T:queue

   Junctions in the SOURCE group (for multiple services reaching same target):
   junction j_compute in compute
   api_fn:B --> T:j_compute
   worker_fn:B --> T:j_compute   ← WRONG! same port used twice
   ✗ Don't connect two services to the same junction port — use a chain instead.

6. GRID ALIGNMENT — before writing edges, sketch the grid mentally:

        col0(edge)  col1(compute)  col2(data)
   row0:  cdn         api_fn         db
   row1:  apigw       worker_fn      queue

   Then write edges that match this grid:
   - cdn → apigw: same column, adjacent rows → B --> T ✓
   - apigw → api_fn: adjacent columns, same row → R --> L ✓
   - api_fn → worker_fn: same column, adjacent rows → B --> T ✓
   - api_fn → db: adjacent columns, same row → R --> L ✓
   - worker_fn → queue: adjacent columns, same row → R --> L ✓

   If you want db → queue (same column, adjacent rows): B --> T ✓
   If you want cdn → api_fn: they are col0→col1, row0→row0 → R --> L ✓
   If you want cdn → worker_fn: col0→col1, row0→row1 — THIS IS A DIAGONAL!
      Route through a junction or connect through apigw instead.

EXAMPLE of a clean diagram:
architecture-beta
    group edge(cloud)[Edge]
    service cdn(logos:aws-cloudfront)[CloudFront] in edge
    service apigw(logos:aws-api-gateway)[API Gateway] in edge

    group compute(cloud)[Compute]
    service api_fn(logos:aws-lambda)[API Lambda] in compute
    service worker_fn(logos:aws-lambda)[Worker Lambda] in compute

    group data(cloud)[Data]
    service db(logos:aws-dynamodb)[DynamoDB] in data
    service queue(logos:aws-sqs)[SQS] in data

    cdn:B --> T:apigw
    apigw:R --> L:api_fn
    api_fn:B --> T:worker_fn
    api_fn:R --> L:db
    worker_fn:R --> L:queue

Grid: cdn[0,0] apigw[0,1] api_fn[1,0] worker_fn[1,1] db[2,0] queue[2,1]
Every edge connects nodes exactly 1 unit apart. Result: clean orthogonal lines.

EXAMPLE with junction for fan-out:
architecture-beta
    group edge(cloud)[Edge]
    service cdn(logos:aws-cloudfront)[CloudFront] in edge

    group compute(cloud)[Compute]
    service api_fn(logos:aws-lambda)[API Lambda] in compute

    group data(cloud)[Data]
    junction j_data in data
    service db(logos:aws-dynamodb)[DynamoDB] in data
    service queue(logos:aws-sqs)[SQS] in data
    service store(logos:aws-s3)[S3] in data

    cdn:R --> L:api_fn
    api_fn:R --> L:j_data
    j_data:R --> L:db
    j_data:B --> T:queue
    queue:B --> T:store

IMPORTANT: A diagram with 6 well-placed nodes and clean lines beats one with
12 tangled nodes. Simplicity is the goal.

─── FINDINGS RULES ───
- Only error and warning severity. No info-level notes.
- Focus on actionable security risks, reliability gaps, or cost issues.
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

    // If a specific repo is requested, only scan that one
    let scan_repos: Vec<String> = if let Some(ref repo) = msg.repo {
        repos.into_iter().filter(|r| r == repo).collect()
    } else {
        repos
    };

    let infra_code = collect_infra_code(&github, &scan_repos).await;

    // Determine the DynamoDB sort key based on per-repo vs global
    let sk = match &msg.repo {
        Some(repo) => format!("INFRA#REPO#{repo}"),
        None => "INFRA#analysis".to_string(),
    };

    if infra_code.is_empty() {
        store_no_infra(state, &msg.tenant_id, &sk).await?;
        info!(tenant_id = %msg.tenant_id, sk = %sk, "No infrastructure code found");
        return Ok(());
    }

    // 3. Call Bedrock to analyze and generate diagram + findings
    let code_context = format_code_context(&infra_code);
    let response = call_bedrock(state, &code_context).await?;

    // 4. Parse response
    let (diagram, findings_json) = parse_response(&response);

    // 5. Validate diagram; if invalid, retry once with error feedback
    let (diagram_str, findings_str) = match &diagram {
        Some(d) if validate_diagram(d).is_err() => {
            let errs = validate_diagram(d).unwrap_err();
            warn!(tenant_id = %msg.tenant_id, errors = %errs, "Diagram validation failed, retrying");
            let retry = call_bedrock_retry(state, &code_context, d, &errs).await?;
            let (retry_diagram, retry_findings) = parse_response(&retry);
            let rd = retry_diagram.unwrap_or_default();
            let rf =
                retry_findings.unwrap_or_else(|| findings_json.unwrap_or_else(|| "[]".to_string()));
            if let Err(e2) = validate_diagram(&rd) {
                warn!(tenant_id = %msg.tenant_id, errors = %e2, "Diagram still invalid after retry, using as-is");
            }
            (rd, rf)
        }
        _ => (
            diagram.unwrap_or_default(),
            findings_json.unwrap_or_else(|| "[]".to_string()),
        ),
    };

    // 6. Store in DynamoDB
    let now = chrono::Utc::now().to_rfc3339();
    let scanned: Vec<String> = infra_code.iter().map(|(f, _)| f.clone()).collect();

    state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", AttributeValue::S(msg.tenant_id.clone()))
        .item("sk", AttributeValue::S(sk.clone()))
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
        .filter(|item| {
            item.get("enabled")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false)
        })
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
            if github
                .get_file_content(owner, repo, &cdk_json)
                .await
                .is_ok()
            {
                // Determine the root directory for CDK files
                let cdk_root = if base.is_empty() { "." } else { base };

                // Scan common CDK subdirectories for .ts, .py, .js files
                let scan_dirs: Vec<String> = if cdk_root == "." {
                    vec!["bin".to_string(), "lib".to_string()]
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

/// Retry Bedrock with the original diagram and validation errors as feedback.
async fn call_bedrock_retry(
    state: &WorkerState,
    code_context: &str,
    bad_diagram: &str,
    errors: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let prompt = format!(
        "Analyze this infrastructure code and generate the diagram and findings:\n\n{code_context}"
    );
    let fix_prompt = format!(
        "Your previous diagram had syntax errors:\n{errors}\n\nHere was the broken diagram:\n```mermaid\n{bad_diagram}\n```\n\nFix ALL errors and regenerate. Rules: groups need (icon) before [label], no slashes in labels, NO -->|text| or <-- or subgraph. MAX 8-10 services in 3 groups (edge, compute, data) flowing left to right. Use junctions for 3+ edges from one port."
    );

    let messages = vec![
        aws_sdk_bedrockruntime::types::Message::builder()
            .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
            .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(prompt))
            .build()
            .map_err(|e| format!("Failed to build message: {e}"))?,
        aws_sdk_bedrockruntime::types::Message::builder()
            .role(aws_sdk_bedrockruntime::types::ConversationRole::Assistant)
            .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(format!(
                "```mermaid\n{bad_diagram}\n```"
            )))
            .build()
            .map_err(|e| format!("Failed to build message: {e}"))?,
        aws_sdk_bedrockruntime::types::Message::builder()
            .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
            .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(
                fix_prompt,
            ))
            .build()
            .map_err(|e| format!("Failed to build message: {e}"))?,
    ];

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

/// Validate architecture-beta diagram syntax before storing.
/// Returns Ok(()) if valid, Err with description of problems if not.
fn validate_diagram(diagram: &str) -> Result<(), String> {
    let mut errors: Vec<String> = Vec::new();
    // Track which group each service belongs to
    let mut service_groups: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    // Track port usage: (service_id, port) -> count
    let mut port_usage: std::collections::HashMap<(String, String), usize> =
        std::collections::HashMap::new();

    for (i, line) in diagram.lines().enumerate() {
        let trimmed = line.trim();
        let line_num = i + 1;

        // Skip blank lines and the architecture-beta declaration
        if trimmed.is_empty() || trimmed == "architecture-beta" {
            continue;
        }

        // Check group declarations have (icon) before [label]
        if let Some(after_group) = trimmed.strip_prefix("group ") {
            if let Some(bracket_pos) = after_group.find('[') {
                let before_bracket = &after_group[..bracket_pos];
                if !before_bracket.contains('(') {
                    errors.push(format!(
                        "line {line_num}: group missing (icon) before [label]: {trimmed}"
                    ));
                }
                // Check group ID doesn't contain hyphens
                let id_part = before_bracket.split('(').next().unwrap_or("").trim();
                if id_part.contains('-') {
                    errors.push(format!(
                        "line {line_num}: group ID contains hyphen (use underscores): {id_part}"
                    ));
                }
            }
        }

        // Check service declarations have "in group_id" and valid IDs
        if let Some(after_service) = trimmed.strip_prefix("service ") {
            // Check ID doesn't contain hyphens
            let id_part = after_service.split('(').next().unwrap_or("").trim();
            if id_part.contains('-') {
                errors.push(format!(
                    "line {line_num}: service ID contains hyphen (use underscores): {id_part}"
                ));
            }
            // Check that service has "in <group>" clause and track the group
            if let Some(in_pos) = after_service.rfind(" in ") {
                let group_id = after_service[in_pos + 4..].trim().to_string();
                service_groups.insert(id_part.to_string(), group_id);
            } else {
                errors.push(format!(
                    "line {line_num}: service missing 'in <group_id>' clause: {trimmed}"
                ));
            }
        }

        // Track junction groups
        if let Some(after_junction) = trimmed.strip_prefix("junction ") {
            if let Some(in_pos) = after_junction.rfind(" in ") {
                let id_part = after_junction[..in_pos].trim();
                let group_id = after_junction[in_pos + 4..].trim();
                service_groups.insert(id_part.to_string(), group_id.to_string());
            }
        }

        // Check labels don't contain forbidden characters
        if let Some(start) = trimmed.find('[') {
            if let Some(end) = trimmed[start..].find(']') {
                let label = &trimmed[start + 1..start + end];
                if label.contains('/')
                    || label.contains('{')
                    || label.contains('}')
                    || label.contains(':')
                {
                    errors.push(format!(
                        "line {line_num}: label contains forbidden char: [{label}]"
                    ));
                }
            }
        }

        // Reject flowchart-style pipe labels: -->|label|  or --|label|
        if trimmed.contains("-->|") || trimmed.contains("--|") || trimmed.contains("|-->") {
            errors.push(format!(
                "line {line_num}: pipe labels not allowed in architecture-beta: {trimmed}"
            ));
        }

        // Reject reverse arrows <-- (architecture-beta only supports -->)
        if trimmed.contains("<--") {
            errors.push(format!(
                "line {line_num}: reverse arrows <-- not supported, use --> with swapped sides: {trimmed}"
            ));
        }

        // Reject subgraph (flowchart syntax, not architecture-beta)
        if trimmed.starts_with("subgraph ") || trimmed == "end" {
            errors.push(format!(
                "line {line_num}: subgraph/end is flowchart syntax, use group in architecture-beta: {trimmed}"
            ));
        }

        // Reject dashed edges (not architecture-beta syntax)
        if trimmed.contains("-.-") || trimmed.contains("---") {
            errors.push(format!(
                "line {line_num}: dashed/triple-dash edges not supported in architecture-beta: {trimmed}"
            ));
        }

        // Parse edge lines to track port usage and check inter-group directions
        // Format: svcId:PORT --> PORT:svcId  or  svcId:PORT -- PORT:svcId
        if trimmed.contains("--") && !trimmed.starts_with("group ") && !trimmed.starts_with("service ") && !trimmed.starts_with("junction ") && !trimmed.starts_with("subgraph ") {
            // Try to parse: left:PORT ... PORT:right
            let parts: Vec<&str> = if trimmed.contains("-->") {
                trimmed.splitn(2, "-->").collect()
            } else if trimmed.contains("--") {
                trimmed.splitn(2, "--").collect()
            } else {
                vec![]
            };
            if parts.len() == 2 {
                let lhs = parts[0].trim().trim_end_matches('>').trim();
                let rhs = parts[1].trim().trim_start_matches('>').trim();
                // Parse left side: svcId{group}?:PORT
                if let Some(colon) = lhs.rfind(':') {
                    let svc_raw = &lhs[..colon];
                    let svc_id = svc_raw.replace("{group}", "");
                    let port = lhs[colon + 1..].trim().to_uppercase();
                    if ["T", "B", "L", "R"].contains(&port.as_str()) {
                        *port_usage.entry((svc_id.clone(), port.clone())).or_insert(0) += 1;
                    }
                    // Check inter-group T/B
                    if let Some(rhs_colon) = rhs.find(':') {
                        let rhs_port = rhs[..rhs_colon].trim().to_uppercase();
                        let rhs_svc_raw = &rhs[rhs_colon + 1..];
                        let rhs_svc_id = rhs_svc_raw.trim().replace("{group}", "");
                        if ["T", "B", "L", "R"].contains(&rhs_port.as_str()) {
                            *port_usage.entry((rhs_svc_id.clone(), rhs_port.clone())).or_insert(0) += 1;
                        }
                        let lhs_group = service_groups.get(&svc_id);
                        let rhs_group = service_groups.get(&rhs_svc_id);
                        if let (Some(lg), Some(rg)) = (lhs_group, rhs_group) {
                            if lg != rg {
                                // Inter-group edge — warn if using T or B
                                if port == "T" || port == "B" || rhs_port == "T" || rhs_port == "B" {
                                    errors.push(format!(
                                        "line {line_num}: inter-group edge uses T/B port (causes diagonal lines), use R-->L instead: {trimmed}"
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Check for duplicate port usage (same port on same service used >1 time)
    for ((svc_id, port), count) in &port_usage {
        if *count > 1 {
            errors.push(format!(
                "service '{svc_id}' port {port} used {count} times (max 1 per port, use junctions)"
            ));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

async fn store_no_infra(
    state: &WorkerState,
    tenant_id: &str,
    sk: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .put_item()
        .table_name(&state.config.table_name)
        .item("pk", AttributeValue::S(tenant_id.to_string()))
        .item("sk", AttributeValue::S(sk.to_string()))
        .item("status", AttributeValue::S("no_infra".to_string()))
        .item("has_infra", AttributeValue::Bool(false))
        .item("cached_at", AttributeValue::S(now))
        .send()
        .await?;
    Ok(())
}
