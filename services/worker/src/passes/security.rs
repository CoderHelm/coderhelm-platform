use serde_json::json;
use tracing::info;

use crate::agent::llm::{self, ToolDefinition, ToolExecutor};
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::WorkerState;

pub struct SecurityResult {
    pub passed: bool,
    pub summary: String,
}

pub async fn run(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    branch: &str,
    usage: &mut TokenUsage,
) -> Result<SecurityResult, Box<dyn std::error::Error + Send + Sync>> {
    let system = format!(
        "You are a security audit agent for the {owner}/{repo} repository. \
         You are READ-ONLY — you cannot modify files. Your job is to review the diff \
         for security vulnerabilities using the OWASP Top 10 and supply chain best practices.\n\n\
         ## Security Checklist\n\
         1. **Injection (A03):** SQL/NoSQL/OS command/log injection via unsanitized input\n\
         2. **Broken Access Control (A01):** Missing auth checks, IDOR, privilege escalation, CORS\n\
         3. **Cryptographic Failures (A02):** Hardcoded secrets, weak hashing, insecure random\n\
         4. **Security Misconfiguration (A05):** Debug mode, default creds, verbose errors\n\
         5. **Vulnerable Components (A06):** New dependencies — flag for review, typosquatting, unpinned versions\n\
         6. **SSRF (A10):** User-controlled URLs in fetch/http requests, missing allowlisting\n\
         7. **XSS (A07):** dangerouslySetInnerHTML, innerHTML, missing output encoding\n\
         8. **Insecure Deserialization:** JSON.parse on untrusted input, prototype pollution\n\
         9. **Sensitive Data Exposure:** Logging PII/tokens, credentials in errors\n\
         10. **DoS:** ReDoS, unbound resource consumption, missing rate limiting\n\
         11. **Rust-specific:** unsafe blocks, .unwrap() on user input, raw pointers\n\
         12. **JS/TS-specific:** eval(), child_process without sanitization, prototype pollution",
        owner = msg.repo_owner,
        repo = msg.repo_name,
    );

    let prompt = "Review the diff for security vulnerabilities. Use get_diff to see all changes, \
         then read any files you need for context.\n\n\
         Output format:\n\
         - If no security issues: start your final message with `SECURITY_PASS`\n\
         - If issues found: start your final message with `SECURITY_FAIL` followed by:\n\n\
         ## Security Issues Found\n\n\
         ### [CRITICAL/HIGH/MEDIUM/LOW] — Issue Title\n\
         - **File:** path/to/file:line\n\
         - **Category:** OWASP category\n\
         - **Description:** What the issue is\n\
         - **Remediation:** How to fix it"
        .to_string();

    let tools = security_tools();
    let executor = SecurityToolExecutor {
        github,
        owner: &msg.repo_owner,
        repo: &msg.repo_name,
        branch,
    };

    let mut messages = vec![aws_sdk_bedrockruntime::types::Message::builder()
        .role(aws_sdk_bedrockruntime::types::ConversationRole::User)
        .content(aws_sdk_bedrockruntime::types::ContentBlock::Text(prompt))
        .build()?];

    let final_text = llm::converse(
        state,
        &state.config.model_id,
        &system,
        &mut messages,
        &tools,
        &executor,
        usage,
    )
    .await?;

    let passed = final_text.contains("SECURITY_PASS");

    info!(passed, "Security audit complete");

    Ok(SecurityResult {
        passed,
        summary: final_text,
    })
}

fn security_tools() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "get_diff".to_string(),
            description: "Compare the current branch to main.".to_string(),
            input_schema: json!({"type": "object", "properties": {}}),
        },
        ToolDefinition {
            name: "read_file".to_string(),
            description: "Read a file from the repository.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "File path relative to repo root"}
                },
                "required": ["path"]
            }),
        },
        ToolDefinition {
            name: "read_file_lines".to_string(),
            description: "Read specific lines from a file (1-indexed, inclusive).".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "File path relative to repo root"},
                    "start_line": {"type": "integer", "description": "First line (1-indexed)"},
                    "end_line": {"type": "integer", "description": "Last line (inclusive)"}
                },
                "required": ["path", "start_line", "end_line"]
            }),
        },
        ToolDefinition {
            name: "search_code".to_string(),
            description: "Search for code in the repository by keyword or symbol.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"}
                },
                "required": ["query"]
            }),
        },
    ]
}

struct SecurityToolExecutor<'a> {
    github: &'a GitHubClient,
    owner: &'a str,
    repo: &'a str,
    branch: &'a str,
}

#[async_trait::async_trait]
impl ToolExecutor for SecurityToolExecutor<'_> {
    async fn execute(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        match name {
            "get_diff" => {
                let diff = self
                    .github
                    .get_diff(self.owner, self.repo, "main", self.branch)
                    .await?;
                Ok(json!(diff))
            }
            "read_file" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                let content = self
                    .github
                    .read_file(self.owner, self.repo, path, self.branch)
                    .await?;
                Ok(json!(super::truncate_content(&content, path)))
            }
            "read_file_lines" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                let start = input
                    .get("start_line")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(1) as usize;
                let end = input
                    .get("end_line")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(start as u64 + 100) as usize;
                let content = self
                    .github
                    .read_file_lines(self.owner, self.repo, path, self.branch, start, end)
                    .await?;
                Ok(json!(content))
            }
            "search_code" => {
                let query = input
                    .get("query")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing query")?;
                let results = self
                    .github
                    .search_code(self.owner, self.repo, query)
                    .await?;
                let lines: Vec<String> = results
                    .iter()
                    .map(|r| {
                        if r.matches.is_empty() {
                            r.path.clone()
                        } else {
                            format!("{}\n{}", r.path, r.matches.join("\n"))
                        }
                    })
                    .collect();
                Ok(json!(lines.join("\n---\n")))
            }
            _ => Err(format!("Unknown tool: {name}").into()),
        }
    }
}
