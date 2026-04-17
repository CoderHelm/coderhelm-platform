use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{error, info, warn};

use crate::clients::github::GitHubClient;
use crate::models::{OnboardMessage, TokenUsage};
use crate::WorkerState;

/// Run onboard pass: analyze each repo and commit .coderhelm/AGENTS.md.
pub async fn run(
    state: &WorkerState,
    msg: OnboardMessage,
    usage: &mut TokenUsage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(
        team_id = %msg.team_id,
        repos = msg.repos.len(),
        "Starting onboard"
    );

    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    // Load custom global instructions from DynamoDB (if any)
    let global_instructions = load_instructions(state, &msg.team_id, "INSTRUCTIONS#GLOBAL").await;

    for repo in &msg.repos {
        let repo_instructions = load_instructions(
            state,
            &msg.team_id,
            &format!("INSTRUCTIONS#REPO#{}/{}", repo.owner, repo.name),
        )
        .await;

        let full_name = format!("{}/{}", repo.owner, repo.name);
        match onboard_repo(
            state,
            &github,
            repo,
            &msg.team_id,
            &global_instructions,
            &repo_instructions,
            usage,
        )
        .await
        {
            Ok(()) => {
                set_onboard_status(state, &msg.team_id, &full_name, "ready", None).await;
            }
            Err(e) => {
                error!(repo = %full_name, error = %e, "Failed to onboard repo");
                set_onboard_status(
                    state,
                    &msg.team_id,
                    &full_name,
                    "failed",
                    Some(&e.to_string()),
                )
                .await;
            }
        }
    }

    // Update global agents overview from all enabled repos in DynamoDB
    let enabled_repos = state
        .dynamo
        .query()
        .table_name(&state.config.repos_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", AttributeValue::S(msg.team_id.clone()))
        .expression_attribute_values(":prefix", AttributeValue::S("REPO#".to_string()))
        .send()
        .await
        .ok()
        .map(|r| {
            r.items()
                .iter()
                .filter(|item| {
                    item.get("enabled")
                        .and_then(|v| v.as_bool().ok())
                        .copied()
                        .unwrap_or(false)
                })
                .filter_map(|item| item.get("repo_name").and_then(|v| v.as_s().ok()).cloned())
                .collect::<Vec<String>>()
        })
        .unwrap_or_default();

    if !enabled_repos.is_empty() {
        // Pull each repo's AGENTS.md for context
        let mut repo_summaries: Vec<String> = Vec::new();
        for repo_name in &enabled_repos {
            let agents_md =
                load_instructions(state, &msg.team_id, &format!("AGENTS#REPO#{repo_name}")).await;
            if !agents_md.is_empty() {
                // Take first ~500 chars (overview + tech stack, not full file)
                let summary = if agents_md.len() > 500 {
                    format!("{}...", &agents_md[..500])
                } else {
                    agents_md
                };
                repo_summaries.push(format!("### {repo_name}\n{summary}"));
            }
        }

        // Use Bedrock to generate a proper org-level summary
        let global_content = if !repo_summaries.is_empty() {
            match generate_global_context(state, &enabled_repos, &repo_summaries, usage).await {
                Ok(content) => content,
                Err(e) => {
                    warn!(error = %e, "Failed to generate global context via Bedrock, using fallback");
                    fallback_global_context(&enabled_repos)
                }
            }
        } else {
            fallback_global_context(&enabled_repos)
        };

        if let Err(e) = state
            .dynamo
            .put_item()
            .table_name(&state.config.settings_table_name)
            .item("pk", AttributeValue::S(msg.team_id.clone()))
            .item("sk", AttributeValue::S("AGENTS#GLOBAL".to_string()))
            .item("content", AttributeValue::S(global_content))
            .item(
                "updated_at",
                AttributeValue::S(chrono::Utc::now().to_rfc3339()),
            )
            .send()
            .await
        {
            warn!(error = %e, "Failed to store global AGENTS.md");
        }
    }

    info!(team_id = %msg.team_id, "Onboard complete");

    Ok(())
}

async fn onboard_repo(
    state: &WorkerState,
    github: &GitHubClient,
    repo: &crate::models::OnboardRepo,
    team_id: &str,
    global_instructions: &str,
    repo_instructions: &str,
    usage: &mut TokenUsage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let full_name = format!("{}/{}", repo.owner, repo.name);
    info!(repo = %full_name, "Onboarding repo");

    // Get repository tree (top 2 levels)
    let tree = github
        .get_tree(&repo.owner, &repo.name, &repo.default_branch)
        .await?;

    // Read signal files if they exist
    let signal_files = [
        "README.md",
        "package.json",
        "Cargo.toml",
        "pyproject.toml",
        "go.mod",
        ".github/CODEOWNERS",
        "Makefile",
        "Dockerfile",
    ];

    let tree_str: String = tree
        .iter()
        .map(|e| format!("{} {}", e.entry_type, e.path))
        .collect::<Vec<_>>()
        .join("\n");
    let mut context_parts: Vec<String> =
        vec![format!("Repository: {full_name}\n\nTree:\n{tree_str}")];

    for path in &signal_files {
        if let Ok(content) = github
            .read_file(&repo.owner, &repo.name, path, &repo.default_branch)
            .await
        {
            // Limit each file to ~4KB to stay within context window
            let truncated = if content.len() > 4096 {
                format!("{}...(truncated)", &content[..4096])
            } else {
                content
            };
            context_parts.push(format!("--- {path} ---\n{truncated}"));
        }
    }

    let repo_context = context_parts.join("\n\n");

    // Build prompt
    let mut system = String::from(
        "You are an expert at analyzing code repositories. \
         Given a repo's file tree and key config files, generate a concise AGENTS.md file that describes:\n\
         1. Project overview (1-2 sentences). CRITICAL: Describe what THIS SPECIFIC REPO does — its unique role, what it deploys, what it serves. \
            Do NOT describe the overall product/organization. \
            BAD: 'Coderhelm is an AI coding platform...' \
            GOOD: 'Next.js dashboard that provides billing, repo management, and infrastructure visualization for the Coderhelm platform.' \
            GOOD: 'Rust Lambda backend (gateway + worker) that processes GitHub webhooks, runs AI agents, and manages billing via Stripe.'\n\
         2. Tech stack and key dependencies\n\
         3. Directory structure with brief descriptions\n\
         4. Build and test commands\n\
         5. Code style and conventions observed\n\
         6. Key files an AI agent should know about\n\n\
         Output ONLY the markdown content for AGENTS.md, no code fences.",
    );

    if !global_instructions.is_empty() {
        system.push_str(&format!(
            "\n\nGlobal custom instructions from the user:\n{global_instructions}"
        ));
    }
    if !repo_instructions.is_empty() {
        system.push_str(&format!(
            "\n\nRepo-specific custom instructions from the user:\n{repo_instructions}"
        ));
    }

    let messages = vec![aws_sdk_bedrockruntime::types::Message::builder()
        .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
        .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(
            repo_context,
        ))
        .build()
        .map_err(|e| format!("Failed to build message: {e}"))?];

    let response = crate::agent::llm::converse_with_retry(
        &state.bedrock,
        &state.config.light_model_id,
        vec![
            aws_sdk_bedrockruntime::types::SystemContentBlock::Text(system),
            aws_sdk_bedrockruntime::types::SystemContentBlock::CachePoint(
                aws_sdk_bedrockruntime::types::CachePointBlock::builder()
                    .r#type(aws_sdk_bedrockruntime::types::CachePointType::Default)
                    .build()
                    .unwrap(),
            ),
        ],
        messages,
    )
    .await
    .map_err(|e| format!("Bedrock converse failed: {e:#}"))?;

    if let Some(u) = response.usage() {
        usage.add(
            u.input_tokens() as u64,
            u.output_tokens() as u64,
            u.cache_read_input_tokens().unwrap_or(0) as u64,
            u.cache_write_input_tokens().unwrap_or(0) as u64,
        );
    }

    let agents_md = extract_text_from_response(&response)?;

    // Store AGENTS.md in DynamoDB (not committed to repo)
    let agents_sk = format!("AGENTS#REPO#{full_name}");
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", AttributeValue::S(team_id.to_string()))
        .item("sk", AttributeValue::S(agents_sk))
        .item("content", AttributeValue::S(agents_md))
        .item(
            "updated_at",
            AttributeValue::S(chrono::Utc::now().to_rfc3339()),
        )
        .send()
        .await
        .map_err(|e| format!("DynamoDB put AGENTS.md failed: {e:#}"))?;

    info!(repo = %full_name, "Stored AGENTS.md in DynamoDB");

    // Generate and store VOICE.md in DynamoDB
    if let Err(e) = generate_voice_md(state, github, repo, team_id, usage).await {
        warn!(repo = %full_name, error = %e, "Failed to generate VOICE.md");
    }

    Ok(())
}

fn extract_text_from_response(
    response: &aws_sdk_bedrockruntime::operation::converse::ConverseOutput,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let output = response.output().ok_or("No output from Bedrock")?;
    match output {
        aws_sdk_bedrockruntime::types::ConverseOutput::Message(msg) => {
            for block in msg.content() {
                if let aws_sdk_bedrockruntime::types::ContentBlock::Text(text) = block {
                    return Ok(text.clone());
                }
            }
            Err("No text in response".into())
        }
        _ => Err("Unexpected output type".into()),
    }
}

async fn generate_voice_md(
    state: &WorkerState,
    github: &GitHubClient,
    repo: &crate::models::OnboardRepo,
    team_id: &str,
    usage: &mut TokenUsage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let full_name = format!("{}/{}", repo.owner, repo.name);

    // Collect recent PR descriptions and review comments for voice analysis
    let prs = github
        .list_pull_requests(&repo.owner, &repo.name, "closed", 10)
        .await
        .unwrap_or_default();

    let mut samples: Vec<String> = Vec::new();

    for pr in prs.as_array().unwrap_or(&vec![]).iter().take(10) {
        if let Some(body) = pr.get("body").and_then(|b| b.as_str()) {
            if !body.is_empty() {
                let truncated = if body.len() > 2048 {
                    &body[..2048]
                } else {
                    body
                };
                samples.push(format!("--- PR description ---\n{truncated}"));
            }
        }
    }

    // Get recent commit messages
    let commits = github
        .list_commits(&repo.owner, &repo.name, &repo.default_branch, 20)
        .await
        .unwrap_or_default();

    for commit in commits.as_array().unwrap_or(&vec![]).iter().take(20) {
        if let Some(message) = commit.pointer("/commit/message").and_then(|m| m.as_str()) {
            samples.push(format!("--- Commit message ---\n{message}"));
        }
    }

    if samples.is_empty() {
        info!(repo = %full_name, "No PR/commit samples found, skipping VOICE.md");
        return Ok(());
    }

    let system = "You analyze code repository communication patterns (PR descriptions, commit messages, review comments) \
                  and generate a concise style guide. Output ONLY the markdown content, no code fences.";

    let prompt = format!(
        "Analyze these PR descriptions and commit messages from {full_name} and create a VOICE.md style guide.\n\n\
         {samples}\n\n\
         Generate a VOICE.md with these sections:\n\
         1. **Tone** — Formal/casual/terse? Emoji usage?\n\
         2. **Commit Messages** — Convention (conventional commits, imperative, etc.), typical length\n\
         3. **PR Descriptions** — Structure, level of detail, typical sections\n\
         4. **Language** — Technical jargon level, abbreviation patterns\n\
         5. **Examples** — 2-3 short examples of commit messages and PR titles matching this voice\n\n\
         Keep it under 80 lines. Be specific about observed patterns, not generic advice.\n\
         IMPORTANT: Do not use dashes (-) as list markers. Use numbered lists, asterisks (*), or plain prose instead.",
        samples = samples.join("\n\n")
    );

    let messages = vec![aws_sdk_bedrockruntime::types::Message::builder()
        .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
        .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(prompt))
        .build()
        .map_err(|e| format!("Failed to build message: {e}"))?];

    let response = crate::agent::llm::converse_with_retry(
        &state.bedrock,
        &state.config.light_model_id,
        vec![
            aws_sdk_bedrockruntime::types::SystemContentBlock::Text(system.to_string()),
            aws_sdk_bedrockruntime::types::SystemContentBlock::CachePoint(
                aws_sdk_bedrockruntime::types::CachePointBlock::builder()
                    .r#type(aws_sdk_bedrockruntime::types::CachePointType::Default)
                    .build()
                    .unwrap(),
            ),
        ],
        messages,
    )
    .await?;

    if let Some(u) = response.usage() {
        usage.add(
            u.input_tokens() as u64,
            u.output_tokens() as u64,
            u.cache_read_input_tokens().unwrap_or(0) as u64,
            u.cache_write_input_tokens().unwrap_or(0) as u64,
        );
    }

    let voice_md = extract_text_from_response(&response)?;

    // Store VOICE.md in DynamoDB (not committed to repo)
    let voice_sk = format!("VOICE#REPO#{full_name}");
    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", AttributeValue::S(team_id.to_string()))
        .item("sk", AttributeValue::S(voice_sk))
        .item("content", AttributeValue::S(voice_md))
        .item(
            "updated_at",
            AttributeValue::S(chrono::Utc::now().to_rfc3339()),
        )
        .send()
        .await?;

    info!(repo = %full_name, "Stored VOICE.md in DynamoDB");
    Ok(())
}

/// Use Bedrock to generate a meaningful org-level context that explains how repos relate.
async fn generate_global_context(
    state: &WorkerState,
    repo_names: &[String],
    repo_summaries: &[String],
    usage: &mut TokenUsage,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let system = "You generate a concise Organization Agent Context document. \
                  Given summaries of each repository, produce a markdown document that:\n\
                  1. Starts with '# Organization Agent Context'\n\
                  2. Has a 2-3 sentence overview of what this organization builds\n\
                  3. Lists each repo with:\n\
                     - Its UNIQUE role (e.g. 'Next.js dashboard', 'Rust API + worker backend', 'CDK infrastructure')\n\
                     - ONE sentence describing what it does that the OTHER repos don't\n\
                     - Key tech (compact, e.g. 'Rust, CDK, DynamoDB')\n\
                  4. Ends with a 'Cross-repo conventions' section noting shared patterns (monorepo? shared infra? same CI?)\n\n\
                  CRITICAL: Each repo description must be UNIQUE and DIFFERENTIATED. Never describe two repos the same way. \
                  Focus on what makes each repo distinct from the others.\n\
                  Keep the entire document under 40 lines. Output markdown only, no code fences.";

    let prompt = format!(
        "Generate an Organization Agent Context for {} repositories:\n\n{}",
        repo_names.len(),
        repo_summaries.join("\n\n")
    );

    let messages = vec![aws_sdk_bedrockruntime::types::Message::builder()
        .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
        .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(prompt))
        .build()
        .map_err(|e| format!("Failed to build message: {e}"))?];

    let response = crate::agent::llm::converse_with_retry(
        &state.bedrock,
        &state.config.light_model_id,
        vec![
            aws_sdk_bedrockruntime::types::SystemContentBlock::Text(system.to_string()),
            aws_sdk_bedrockruntime::types::SystemContentBlock::CachePoint(
                aws_sdk_bedrockruntime::types::CachePointBlock::builder()
                    .r#type(aws_sdk_bedrockruntime::types::CachePointType::Default)
                    .build()
                    .unwrap(),
            ),
        ],
        messages,
    )
    .await?;

    if let Some(u) = response.usage() {
        usage.add(
            u.input_tokens() as u64,
            u.output_tokens() as u64,
            u.cache_read_input_tokens().unwrap_or(0) as u64,
            u.cache_write_input_tokens().unwrap_or(0) as u64,
        );
    }

    extract_text_from_response(&response)
}

/// Fallback when Bedrock call fails — simple list without AI enrichment.
fn fallback_global_context(repo_names: &[String]) -> String {
    let entries: Vec<String> = repo_names
        .iter()
        .map(|name| format!("- **{name}**"))
        .collect();
    format!(
        "# Organization Agent Context\n\n\
         This organization has {} repositories configured with Coderhelm:\n\n\
         {}\n",
        repo_names.len(),
        entries.join("\n")
    )
}

async fn load_instructions(state: &WorkerState, team_id: &str, sk: &str) -> String {
    match state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(sk.to_string()))
        .send()
        .await
    {
        Ok(output) => output
            .item()
            .and_then(|item| item.get("content"))
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default(),
        Err(e) => {
            warn!(error = %e, "Failed to load instructions");
            String::new()
        }
    }
}

async fn set_onboard_status(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    status: &str,
    error_msg: Option<&str>,
) {
    let now = chrono::Utc::now().to_rfc3339();
    let mut update = state
        .dynamo
        .update_item()
        .table_name(&state.config.repos_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(format!("REPO#{repo}")))
        .update_expression(if error_msg.is_some() {
            "SET onboard_status = :s, onboard_error = :e, updated_at = :t"
        } else {
            "SET onboard_status = :s, updated_at = :t REMOVE onboard_error"
        })
        .expression_attribute_values(":s", AttributeValue::S(status.to_string()))
        .expression_attribute_values(":t", AttributeValue::S(now));

    if let Some(err) = error_msg {
        update = update.expression_attribute_values(":e", AttributeValue::S(err.to_string()));
    }

    if let Err(e) = update.send().await {
        warn!(error = %e, repo = %repo, "Failed to update onboard status");
    }
}
