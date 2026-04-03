use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{info, warn};

use crate::models::InfraAnalyzeMessage;
use crate::WorkerState;

/// System prompt for infra analysis.
const SYSTEM: &str = r#"You are an expert AWS infrastructure architect that produces
publication-quality architecture diagrams using Mermaid architecture-beta syntax.

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

━━━ SYNTAX REFERENCE ━━━

- First line: architecture-beta
- Groups:     group {id}({icon})[{Label}]
- Services:   service {id}({icon})[{Label}] in {group_id}
- Edges:      {svcId}:{T|B|L|R} --> {T|B|L|R}:{svcId}
- Junctions:  junction {id} in {group_id}
- IDs: lowercase alphanumeric + underscores ONLY. No hyphens, dots, or special chars.
- Labels []: alphanumeric, spaces, hyphens, periods ONLY. No slashes / brackets / braces / colons.
- DECLARE all groups and services FIRST, then ALL edges LAST. Never interleave.
- FORBIDDEN: -->|text|, <--, subgraph/end, ---, -.-, flowchart syntax of any kind.

━━━ ICONS ━━━

Use logos:aws-* icons. Available:
  logos:aws-lambda, logos:aws-dynamodb, logos:aws-sqs, logos:aws-s3,
  logos:aws-cloudfront, logos:aws-api-gateway, logos:aws-ses,
  logos:aws-secrets-manager, logos:aws-cloudwatch, logos:aws-waf,
  logos:aws-route53, logos:aws-iam, logos:aws-sns, logos:aws-eventbridge,
  logos:aws-ec2, logos:aws-ecs, logos:aws-step-functions, logos:aws-cognito,
  logos:aws-elastic-load-balancing, logos:aws-aurora, logos:aws-elasticache,
  logos:aws-open-search, logos:aws-msk, logos:aws-ecr, logos:aws-kms,
  logos:aws-vpc, logos:aws-cloudformation.
Non-AWS: logos:shopify, logos:nextjs, logos:redis, logos:postgresql,
  logos:graphql, logos:kafka, logos:vercel, logos:fastly, logos:contentful.
Generic fallbacks: cloud, database, server, disk, internet.

━━━ LAYOUT ENGINE — CRITICAL KNOWLEDGE ━━━

The renderer uses cytoscape.js + fcose. It builds an IMPLICIT GRID by BFS-walking
edges. Each edge places nodes on the grid:
  R→L = target 1 column right     B→T = target 1 row below

★ RULE: Every connected pair MUST be exactly 1 grid unit apart.
When nodes end up >1 unit apart: diagonal lines, overlapping, non-deterministic.

★ RULE: Each port (T, B, L, R) supports EXACTLY ONE edge.
Two edges on one port = overlapping nodes. Use junctions to split.

━━━ ARCHITECTURAL THINKING ━━━

Before writing ANY syntax, analyze the infrastructure through these lenses:

STEP 1 — IDENTIFY THE ARCHITECTURE PATTERN:
  a) Serverless API: API Gateway → Lambda → DynamoDB/S3
  b) Container Service: ALB → ECS/Fargate → RDS/ElastiCache
  c) Event-Driven: EventBridge/SQS/SNS → Lambda → DynamoDB/S3
  d) Static Site + API: CloudFront → S3 (static) + API Gateway → Lambda
  e) Data Pipeline: Kinesis/MSK → Lambda/ECS → S3/Redshift
  f) Hybrid: Combination — pick the PRIMARY request path.

STEP 2 — CHOOSE GROUP STRATEGY:
  Groups represent tier boundaries, NOT AWS service categories.
  Use the pattern name to pick the right 3 groups:

  Serverless API:
    group edge(cloud)[Edge]               — CloudFront, API Gateway, WAF
    group compute(logos:aws-lambda)[Compute]  — Lambda functions
    group data(database)[Data]            — DynamoDB, S3, SQS

  Container Service:
    group edge(cloud)[Edge]               — CloudFront, ALB, Route 53
    group compute(logos:aws-ecs)[Compute] — Fargate/ECS tasks
    group data(database)[Data]            — RDS, ElastiCache, S3

  Event-Driven:
    group ingest(cloud)[Ingest]           — API Gateway, EventBridge, SQS
    group process(logos:aws-lambda)[Process] — Lambda processors, Step Functions
    group store(database)[Store]          — DynamoDB, S3, SES

  Static Site + API:
    group cdn(logos:aws-cloudfront)[CDN]  — CloudFront, S3 (static assets)
    group api(logos:aws-api-gateway)[API] — API Gateway, Lambda
    group data(database)[Data]            — DynamoDB, S3 (data bucket)

STEP 3 — SELECT MAX 8 SERVICES:
  Merging rules (critical for clean diagrams):
  • Multiple DynamoDB tables → one "DynamoDB" node
  • Multiple S3 buckets → one "S3" node (unless they serve DIFFERENT tiers, e.g. static vs data)
  • Multiple Lambda functions → merge by ROLE: "API Lambda" + "Worker Lambda" max
  • API Gateway + WAF → just "API Gateway" (WAF is implied)
  • Route 53 + CloudFront → just "CloudFront" (Route 53 is DNS plumbing)
  • SecretsManager, KMS, IAM, CloudWatch → omit (cross-cutting, not architectural)
  • SNS + SQS in the same path → show the one the code WRITES to; the other is plumbing
  Place at most 3 services per group. If a group needs 4+, merge harder.

STEP 4 — PLAN THE GRID:
  Mentally place services on a 3-column × N-row grid:

       col0 (edge)    col1 (compute)    col2 (data)
  row0:  svc_a          svc_d             svc_f
  row1:  svc_b          svc_e             svc_g
  row2:               junction_j          svc_h

  Then ONLY write edges matching this grid.

━━━ EDGE RULES ━━━

These are the MOST IMPORTANT rules for clean rendering:

1. BETWEEN groups (different columns): ONLY R --> L
   This places groups exactly 1 column apart. Never use T/B between groups.

2. WITHIN a group (same column): ONLY B --> T
   This stacks services vertically, 1 row apart. Never use L/R within a group.

3. NEVER connect backwards (data → compute, compute → edge).
   Data flow goes LEFT to RIGHT only.

4. EVERY service must be connected. No orphan nodes.

━━━ JUNCTION STRATEGY ━━━

When a service has 2+ outbound edges from the same direction, you MUST use a
junction (since each port supports only one edge).

PATTERN — Fan-out into the next group:
  Place the junction IN THE TARGET GROUP, then branch:

  junction j_data in data
  api_fn:R --> L:j_data
  j_data:R --> L:db
  j_data:B --> T:queue

PATTERN — Fan-out within the same group:
  Place the junction IN THE SAME GROUP:

  junction j_edge in edge
  cdn:B --> T:j_edge
  j_edge:B --> T:apigw
  j_edge:R --> L:static_site      ← only if static_site is in the NEXT group

ANTI-PATTERN — Two edges into the same port:
  ✗ api_fn:R --> L:db
  ✗ api_fn:R --> L:queue          ← port R used twice!
  ✓ Use a junction as shown above.

━━━ LABEL CONVENTIONS ━━━

Labels should be short, recognizable AWS service names:
  ✓ "CloudFront"   ✓ "API Gateway"   ✓ "DynamoDB"   ✓ "API Lambda"
  ✓ "Worker Lambda"  ✓ "SQS Queue"   ✓ "S3 Bucket"   ✓ "Aurora DB"
  ✗ "AWS CloudFront Distribution"  ✗ "Amazon DynamoDB Table"  ✗ "MyStack-Lambda-ABC123"
  ✗ "api/v1/users"  ✗ "lambda-handler.ts"
Keep labels to 2-3 words max. Use the service name, optionally prefixed by role.

━━━ EXAMPLES ━━━

EXAMPLE 1 — Serverless API (most common):
architecture-beta
    group edge(cloud)[Edge]
    service cdn(logos:aws-cloudfront)[CloudFront] in edge
    service apigw(logos:aws-api-gateway)[API Gateway] in edge

    group compute(logos:aws-lambda)[Compute]
    service api_fn(logos:aws-lambda)[API Lambda] in compute
    service worker_fn(logos:aws-lambda)[Worker Lambda] in compute

    group data(database)[Data]
    service db(logos:aws-dynamodb)[DynamoDB] in data
    service queue(logos:aws-sqs)[SQS Queue] in data

    cdn:B --> T:apigw
    apigw:R --> L:api_fn
    api_fn:B --> T:worker_fn
    api_fn:R --> L:db
    worker_fn:R --> L:queue

Grid: cdn[0,0] apigw[0,1] api_fn[1,0] worker_fn[1,1] db[2,0] queue[2,1]
All edges are 1 unit. Result: clean orthogonal lines.

EXAMPLE 2 — Serverless with fan-out via junction:
architecture-beta
    group edge(cloud)[Edge]
    service cdn(logos:aws-cloudfront)[CloudFront] in edge

    group compute(logos:aws-lambda)[Compute]
    service api_fn(logos:aws-lambda)[API Lambda] in compute

    group data(database)[Data]
    junction j_data in data
    service db(logos:aws-dynamodb)[DynamoDB] in data
    service queue(logos:aws-sqs)[SQS Queue] in data
    service store(logos:aws-s3)[S3 Bucket] in data

    cdn:R --> L:api_fn
    api_fn:R --> L:j_data
    j_data:R --> L:db
    j_data:B --> T:queue
    queue:B --> T:store

EXAMPLE 3 — Container service:
architecture-beta
    group edge(cloud)[Edge]
    service cdn(logos:aws-cloudfront)[CloudFront] in edge
    service alb(logos:aws-elastic-load-balancing)[ALB] in edge

    group compute(logos:aws-ecs)[Compute]
    service web(logos:aws-ecs)[Web Service] in compute
    service worker(logos:aws-ecs)[Worker] in compute

    group data(database)[Data]
    service db(logos:aws-aurora)[Aurora] in data
    service cache(logos:aws-elasticache)[ElastiCache] in data

    cdn:B --> T:alb
    alb:R --> L:web
    web:B --> T:worker
    web:R --> L:db
    worker:R --> L:cache

EXAMPLE 4 — Event-driven:
architecture-beta
    group ingest(cloud)[Ingest]
    service apigw(logos:aws-api-gateway)[API Gateway] in ingest
    service bus(logos:aws-eventbridge)[EventBridge] in ingest

    group process(logos:aws-lambda)[Process]
    service handler(logos:aws-lambda)[Handler] in process
    service notify(logos:aws-ses)[SES] in process

    group store(database)[Store]
    service db(logos:aws-dynamodb)[DynamoDB] in store
    service bucket(logos:aws-s3)[S3 Bucket] in store

    apigw:B --> T:bus
    bus:R --> L:handler
    handler:B --> T:notify
    handler:R --> L:db
    db:B --> T:bucket

━━━ QUALITY CHECKLIST (verify before outputting) ━━━

□ First line is architecture-beta
□ Every group has (icon) before [label]
□ Every service has "in {group}"
□ No service ID or group ID contains hyphens
□ No label contains / { } :
□ All groups and services declared BEFORE any edges
□ Max 8 services, max 3 per group, exactly 3 groups
□ Inter-group edges use ONLY R --> L
□ Intra-group edges use ONLY B --> T
□ No port used more than once (check R port on every service)
□ Every service is connected (no orphans)
□ No backward edges (right-to-left data flow)
□ 6 well-placed nodes with clean lines > 12 tangled nodes

━━━ FINDINGS RULES ━━━
- Only error and warning severity. No info-level notes.
- Focus on actionable security risks, reliability gaps, or cost issues.
- Categories: security, reliability, cost, performance.
"#;

pub async fn run(
    state: &WorkerState,
    msg: InfraAnalyzeMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Get team to find installation_id + repos
    let team = get_team(state, &msg.team_id).await?;
    let Some((install_id, repos)) = team else {
        warn!(team_id = %msg.team_id, "Team not found for infra analyze");
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
        store_no_infra(state, &msg.team_id, &sk).await?;
        info!(team_id = %msg.team_id, sk = %sk, "No infrastructure code found");
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
            warn!(team_id = %msg.team_id, errors = %errs, "Diagram validation failed, retrying");
            let retry = call_bedrock_retry(state, &code_context, d, &errs).await?;
            let (retry_diagram, retry_findings) = parse_response(&retry);
            let rd = retry_diagram.unwrap_or_default();
            let rf =
                retry_findings.unwrap_or_else(|| findings_json.unwrap_or_else(|| "[]".to_string()));
            if let Err(e2) = validate_diagram(&rd) {
                warn!(team_id = %msg.team_id, errors = %e2, "Diagram still invalid after retry, using as-is");
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
        .table_name(&state.config.infra_table_name)
        .item("pk", AttributeValue::S(msg.team_id.clone()))
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

    info!(team_id = %msg.team_id, files = scanned.len(), "Infra analysis complete");
    Ok(())
}

// ── Helpers ────────────────────────────────────────────────────────────────────

async fn get_team(
    state: &WorkerState,
    team_id: &str,
) -> Result<Option<(u64, Vec<String>)>, Box<dyn std::error::Error + Send + Sync>> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S("META".to_string()))
        .send()
        .await?;

    let item = match result.item() {
        None => return Ok(None),
        Some(i) => i,
    };

    let install_id: u64 = match item
        .get("github_install_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
    {
        Some(id) if id > 0 => id,
        _ => {
            warn!(
                team_id,
                "No github_install_id found for team, skipping infra analyze"
            );
            return Ok(None);
        }
    };

    // Get repos from the repos table
    let repos_result = state
        .dynamo
        .query()
        .table_name(&state.config.repos_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()))
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

    let response = crate::agent::llm::converse_with_retry(
        &state.bedrock,
        &state.config.model_id,
        vec![aws_sdk_bedrockruntime::types::SystemContentBlock::Text(
            SYSTEM.to_string(),
        )],
        messages,
    )
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

    let response = crate::agent::llm::converse_with_retry(
        &state.bedrock,
        &state.config.model_id,
        vec![aws_sdk_bedrockruntime::types::SystemContentBlock::Text(
            SYSTEM.to_string(),
        )],
        messages,
    )
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
    // Track all declared node IDs (services + junctions)
    let mut declared_nodes: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Track edge endpoints for dangling-node check: (line_num, svc_id)
    let mut edge_endpoints: Vec<(usize, String)> = Vec::new();

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
            declared_nodes.insert(id_part.to_string());
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
                declared_nodes.insert(id_part.to_string());
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
        if trimmed.contains("--")
            && !trimmed.starts_with("group ")
            && !trimmed.starts_with("service ")
            && !trimmed.starts_with("junction ")
            && !trimmed.starts_with("subgraph ")
        {
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
                        *port_usage
                            .entry((svc_id.clone(), port.clone()))
                            .or_insert(0) += 1;
                    }
                    edge_endpoints.push((line_num, svc_id.clone()));
                    // Check inter-group T/B
                    if let Some(rhs_colon) = rhs.find(':') {
                        let rhs_port = rhs[..rhs_colon].trim().to_uppercase();
                        let rhs_svc_raw = &rhs[rhs_colon + 1..];
                        let rhs_svc_id = rhs_svc_raw.trim().replace("{group}", "");
                        if ["T", "B", "L", "R"].contains(&rhs_port.as_str()) {
                            *port_usage
                                .entry((rhs_svc_id.clone(), rhs_port.clone()))
                                .or_insert(0) += 1;
                        }
                        edge_endpoints.push((line_num, rhs_svc_id.clone()));
                        let lhs_group = service_groups.get(&svc_id);
                        let rhs_group = service_groups.get(&rhs_svc_id);
                        if let (Some(lg), Some(rg)) = (lhs_group, rhs_group) {
                            if lg != rg {
                                // Inter-group edge — warn if using T or B
                                if port == "T" || port == "B" || rhs_port == "T" || rhs_port == "B"
                                {
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

    // Check for dangling edges (endpoints that reference undeclared services/junctions)
    for (line_num, node_id) in &edge_endpoints {
        if !declared_nodes.contains(node_id) {
            errors.push(format!(
                "line {line_num}: edge references undeclared node '{node_id}' (missing service/junction declaration)"
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
    team_id: &str,
    sk: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .put_item()
        .table_name(&state.config.infra_table_name)
        .item("pk", AttributeValue::S(team_id.to_string()))
        .item("sk", AttributeValue::S(sk.to_string()))
        .item("status", AttributeValue::S("no_infra".to_string()))
        .item("has_infra", AttributeValue::Bool(false))
        .item("cached_at", AttributeValue::S(now))
        .send()
        .await?;
    Ok(())
}
