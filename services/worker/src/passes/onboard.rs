use aws_sdk_dynamodb::types::AttributeValue;
use tracing::{error, info, warn};

use crate::clients::email::{self, EmailEvent};
use crate::clients::github::GitHubClient;
use crate::models::OnboardMessage;
use crate::WorkerState;

/// Run onboard pass: analyze each repo and commit .d3ftly/AGENTS.md.
pub async fn run(
    state: &WorkerState,
    msg: OnboardMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(
        tenant_id = %msg.tenant_id,
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
    let global_instructions = load_instructions(state, &msg.tenant_id, "INSTRUCTIONS#GLOBAL").await;

    for repo in &msg.repos {
        let repo_instructions = load_instructions(
            state,
            &msg.tenant_id,
            &format!("INSTRUCTIONS#REPO#{}/{}", repo.owner, repo.name),
        )
        .await;

        if let Err(e) = onboard_repo(
            state,
            &github,
            repo,
            &global_instructions,
            &repo_instructions,
        )
        .await
        {
            error!(
                repo = %format!("{}/{}", repo.owner, repo.name),
                error = %e,
                "Failed to onboard repo"
            );
        }
    }

    // If multi-repo install (org), generate a global AGENTS.md in .github repo
    if msg.repos.len() > 1 {
        if let Some(first) = msg.repos.first() {
            if let Err(e) =
                generate_global_agents_md(state, &github, &first.owner, &msg.repos).await
            {
                warn!(error = %e, "Failed to create global AGENTS.md");
            }
        }
    }

    info!(tenant_id = %msg.tenant_id, "Onboard complete");

    // Send welcome email
    let org = msg
        .repos
        .first()
        .map(|r| r.owner.clone())
        .unwrap_or_default();
    if let Err(e) = email::send_notification(
        state,
        &msg.tenant_id,
        EmailEvent::Welcome {
            org,
            repo_count: msg.repos.len(),
        },
    )
    .await
    {
        error!("Failed to send welcome email: {e}");
    }

    Ok(())
}

async fn onboard_repo(
    state: &WorkerState,
    github: &GitHubClient,
    repo: &crate::models::OnboardRepo,
    global_instructions: &str,
    repo_instructions: &str,
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

    let tree_str: String = tree.iter().map(|e| format!("{} {}", e.entry_type, e.path)).collect::<Vec<_>>().join("\n");
    let mut context_parts: Vec<String> = vec![format!("Repository: {full_name}\n\nTree:\n{tree_str}")];

    for path in &signal_files {
        match github
            .read_file(&repo.owner, &repo.name, path, &repo.default_branch)
            .await
        {
            Ok(content) => {
                // Limit each file to ~4KB to stay within context window
                let truncated = if content.len() > 4096 {
                    format!("{}...(truncated)", &content[..4096])
                } else {
                    content
                };
                context_parts.push(format!("--- {path} ---\n{truncated}"));
            }
            Err(_) => {} // File doesn't exist, skip
        }
    }

    let repo_context = context_parts.join("\n\n");

    // Build prompt
    let mut system = String::from(
        "You are an expert at analyzing code repositories. \
         Given a repo's file tree and key config files, generate a concise AGENTS.md file that describes:\n\
         1. Project overview (1-2 sentences)\n\
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

    let response = state
        .bedrock
        .converse()
        .model_id(&state.config.model_id)
        .system(aws_sdk_bedrockruntime::types::SystemContentBlock::Text(
            system,
        ))
        .system(
            aws_sdk_bedrockruntime::types::SystemContentBlock::CachePoint(
                aws_sdk_bedrockruntime::types::CachePointBlock::builder()
                    .build()
                    .unwrap(),
            ),
        )
        .set_messages(Some(messages))
        .send()
        .await?;

    let agents_md = extract_text_from_response(&response)?;

    // Commit .d3ftly/AGENTS.md to default branch
    github
        .write_file(
            &repo.owner,
            &repo.name,
            ".d3ftly/AGENTS.md",
            &agents_md,
            &repo.default_branch,
            "chore: add .d3ftly/AGENTS.md for AI agent context",
            None,
        )
        .await?;

    info!(repo = %full_name, "Committed .d3ftly/AGENTS.md");
    Ok(())
}

async fn generate_global_agents_md(
    _state: &WorkerState,
    github: &GitHubClient,
    owner: &str,
    repos: &[crate::models::OnboardRepo],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let repo_list: Vec<String> = repos
        .iter()
        .map(|r| format!("- **{}**: default branch `{}`", r.name, r.default_branch))
        .collect();

    let content = format!(
        "# Organization Agent Context\n\n\
         This organization has {} repositories configured with d3ftly:\n\n\
         {}\n\n\
         Each repository has its own `.d3ftly/AGENTS.md` with detailed context.\n",
        repos.len(),
        repo_list.join("\n")
    );

    // Try committing to .github repo first, fall back to first repo
    let target_repo = ".github";
    let default_branch = "main";

    match github
        .write_file(
            owner,
            target_repo,
            "AGENTS.md",
            &content,
            default_branch,
            "chore: add global AGENTS.md for d3ftly",
            None,
        )
        .await
    {
        Ok(_) => {
            info!(owner, "Committed global AGENTS.md to .github repo");
        }
        Err(e) => {
            warn!(error = %e, "Could not write to .github repo, skipping global AGENTS.md");
        }
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

async fn load_instructions(state: &WorkerState, tenant_id: &str, sk: &str) -> String {
    match state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", AttributeValue::S(tenant_id.to_string()))
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
