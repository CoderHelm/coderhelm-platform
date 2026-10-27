use serde_json::json;
use tracing::info;

use crate::agent::llm::{self, ToolDefinition, ToolExecutor};
use crate::agent::provider;
use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::passes::plan::PlanResult;
use crate::passes::FileCache;
use crate::WorkerState;

pub struct SecurityResult {
    pub passed: bool,
    pub summary: String,
}

/// What the security check concluded about the change.
#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    Passed,
    /// Issues, as the audit wrote them.
    Findings(String),
    /// The check didn't reach a verdict (an API or model error). Never a
    /// pass: an error used to count as one, and the PR opened unchecked.
    NotChecked(String),
}

impl Verdict {
    pub fn of(r: Result<SecurityResult, Box<dyn std::error::Error + Send + Sync>>) -> Verdict {
        match r {
            Ok(r) if r.passed => Verdict::Passed,
            Ok(r) => Verdict::Findings(r.summary),
            Err(e) => Verdict::NotChecked(common::run_fail::error_text(&*e)),
        }
    }

    pub fn passed(&self) -> bool {
        matches!(self, Verdict::Passed)
    }
}

/// What the coder is told in a security round whose check didn't run before
/// the pull request opened: look, fix what's found, change nothing otherwise.
/// The check runs again when the round ends.
pub fn recheck_feedback(why: &str) -> String {
    format!(
        "The security check couldn't run on this change before the pull request opened ({}). \
         Review the change on this branch for security issues: injection, unsafe input handling, \
         secrets in code or logs, missing authorization, unpinned or misspelled dependencies. Fix \
         any you find. If you find none, change nothing.",
        common::truncate_str(why.trim(), 300)
    )
}

/// What the coder is told in a security fix.
pub fn fix_feedback(summary: &str) -> String {
    format!("Security audit found vulnerabilities. Fix ALL of the following:\n\n{summary}")
}

/// One issue of the audit's report: its heading, file and remediation.
fn issues(summary: &str) -> Vec<(String, String, String)> {
    let mut out: Vec<(String, String, String)> = Vec::new();
    for line in summary.lines() {
        let t = line.trim();
        if let Some(h) = t.strip_prefix("### ") {
            out.push((h.trim().to_string(), String::new(), String::new()));
        } else if let Some(last) = out.last_mut() {
            let field = |name: &str| {
                t.trim_start_matches(['-', '*', ' '])
                    .strip_prefix(name)
                    .map(|v| v.trim_start_matches(['*', ':', ' ']).trim().to_string())
            };
            if let Some(f) = field("**File:**").or_else(|| field("File:")) {
                last.1 = f;
            } else if let Some(r) = field("**Remediation:**").or_else(|| field("Remediation:")) {
                last.2 = r;
            }
        }
    }
    out
}

/// The security work the pull request still owes, as Open items: each issue
/// not fixed yet, or the check that didn't run. Each is marked, and the pull
/// request stays a draft while one is listed. `fixing` when a security fix
/// round is about to work on the issues in a fresh invocation.
pub fn open_items(v: &Verdict, fixing: bool) -> Vec<super::proof::Finding> {
    let settle = if fixing {
        "CoderHelm works on it in a fresh step on this pull request and runs the check again; the pull request stays a draft until then."
    } else {
        "Fix it on this branch; the pull request stays a draft while this line is here (delete it once the fix is checked)."
    };
    match v {
        Verdict::Passed => Vec::new(),
        Verdict::NotChecked(why) => vec![super::proof::Finding {
            point: "security_unchecked".to_string(),
            detail: format!(
                "The security check couldn't run on this change ({}).",
                common::truncate_str(why.trim(), 160)
            ),
            settle: if fixing {
                "CoderHelm runs it again in a fresh step on this pull request; the pull request stays a draft until it passes."
            } else {
                "Check the change for security issues before merging; the pull request stays a draft while this line is here (delete it once checked)."
            }
            .to_string(),
            check: "security".to_string(),
            ..Default::default()
        }],
        Verdict::Findings(summary) => {
            let found = issues(summary);
            if found.is_empty() {
                let flat = summary
                    .replace("SECURITY_FAIL", "")
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ");
                return vec![super::proof::Finding {
                    point: "security".to_string(),
                    detail: if flat.is_empty() {
                        "The security check found issues in this change.".to_string()
                    } else {
                        flat
                    },
                    settle: settle.to_string(),
                    check: "security".to_string(),
                    ..Default::default()
                }];
            }
            found
                .into_iter()
                .map(|(title, file, fix)| super::proof::Finding {
                    point: "security".to_string(),
                    detail: if file.is_empty() {
                        format!("{title}.")
                    } else {
                        format!("{title} in `{file}`.")
                    },
                    settle: if fix.is_empty() {
                        settle.to_string()
                    } else {
                        format!("{fix} {settle}")
                    },
                    check: "security".to_string(),
                    ..Default::default()
                })
                .collect()
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    plan: &PlanResult,
    branch: &str,
    repo_instructions: &str,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    file_cache: &FileCache,
    _run_id: Option<&str>,
) -> Result<SecurityResult, Box<dyn std::error::Error + Send + Sync>> {
    // Trim repo instructions for the security pass (it only needs high-level context)
    let trimmed = if repo_instructions.len() > 2000 {
        {
            let head = common::truncate_str(repo_instructions, 2000);
            &head[..head.rfind('\n').unwrap_or(head.len())]
        }
    } else {
        repo_instructions
    };
    let instructions_block = super::format_instructions_block(trimmed);
    let system = format!(
        "You are a security audit agent for the {owner}/{repo} repository. \
         You are READ-ONLY. Review the diff for OWASP Top 10 vulnerabilities, \
         supply chain risks (typosquatting, unpinned deps), and language-specific issues \
         (unsafe blocks, eval, prototype pollution, hardcoded secrets).{instructions_block}",
        owner = msg.repo_owner,
        repo = msg.repo_name,
    );

    let openspec_block = super::format_openspec_summary(plan);
    let prompt = format!(
        r#"Review the diff for security vulnerabilities.
{openspec}
Use `get_diff` to see all changes, then read specific functions if you need surrounding context.

Focus your analysis on the attack surfaces introduced by the changes described in the OpenSpec above.
Check that the implementation doesn't introduce vulnerabilities that conflict with the intended scope.

Output format:
- If no security issues: start with `SECURITY_PASS`
- If issues found: start with `SECURITY_FAIL` followed by:

### [CRITICAL/HIGH/MEDIUM/LOW] — Issue Title
- **File:** path/to/file:line
- **Category:** OWASP category
- **Remediation:** How to fix it"#,
        openspec = openspec_block,
    );

    let tools = security_tools();
    let executor = SecurityToolExecutor {
        github,
        owner: &msg.repo_owner,
        repo: &msg.repo_name,
        branch,
        base_branch: &msg.base_branch,
        file_cache,
    };

    let mut messages = vec![(
        "user".to_string(),
        vec![serde_json::json!({"type": "text", "text": prompt})],
    )];

    let model_id = provider.primary_model_id();
    let final_text = provider::converse(
        state,
        provider,
        common::agent_policy::Agent::Reviewer,
        model_id,
        &system,
        &mut messages,
        &tools,
        &executor,
        usage,
        llm::ConverseOptions {
            max_turns: 35,
            max_tokens: 8192,
            deadline: None,
            effort: None,
        },
        None,
        None,
    )
    .await?;

    // Fail-closed verdict: `contains("SECURITY_PASS")` passed any report that
    // merely mentioned the token ("falls short of a SECURITY_PASS").
    let passed = common::parse_verdict(&final_text, "SECURITY_PASS", "SECURITY_FAIL")
        == common::Verdict::Pass;

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
    base_branch: &'a str,
    file_cache: &'a FileCache,
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
                    .get_diff(self.owner, self.repo, self.base_branch, self.branch)
                    .await?;
                let files = diff.get("files").and_then(|v| v.as_array());
                if let Some(files) = files {
                    let mut lines = Vec::new();
                    for f in files {
                        let filename = f.get("filename").and_then(|v| v.as_str()).unwrap_or("");
                        let status = f.get("status").and_then(|v| v.as_str()).unwrap_or("");
                        let adds = f.get("additions").and_then(|v| v.as_u64()).unwrap_or(0);
                        let dels = f.get("deletions").and_then(|v| v.as_u64()).unwrap_or(0);
                        lines.push(format!("{filename} ({status}, +{adds}/-{dels})"));
                        if let Some(patch) = f.get("patch").and_then(|v| v.as_str()) {
                            let truncated = if patch.len() > 4000 {
                                format!(
                                    "{}... (truncated, use read_file for full content)",
                                    common::truncate_str(patch, 4000)
                                )
                            } else {
                                patch.to_string()
                            };
                            lines.push(truncated);
                        }
                    }
                    Ok(json!(lines.join("\n")))
                } else {
                    Ok(json!("No changes compared to main."))
                }
            }
            "read_file" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                let cache_key = format!("{}:{}", self.branch, path);
                let content = if let Some(cached) = self.file_cache.get(&cache_key).await {
                    cached
                } else {
                    let fetched = self
                        .github
                        .read_file(self.owner, self.repo, path, self.branch)
                        .await?;
                    self.file_cache.insert(cache_key, fetched.clone()).await;
                    fetched
                };
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
                // Try slicing from cached full file first
                let cache_key = format!("{}:{}", self.branch, path);
                let content = if let Some(cached) = self.file_cache.get(&cache_key).await {
                    cached
                        .lines()
                        .skip(start.saturating_sub(1))
                        .take(end.saturating_sub(start.saturating_sub(1)))
                        .collect::<Vec<_>>()
                        .join("\n")
                } else {
                    self.github
                        .read_file_lines(self.owner, self.repo, path, self.branch, start, end)
                        .await?
                };
                Ok(json!(content))
            }
            "search_code" => {
                let query = input
                    .get("query")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing query")?;
                let results = self
                    .github
                    .search_code(self.owner, self.repo, self.branch, query)
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

#[cfg(test)]
mod tests {
    use super::*;

    const REPORT: &str = "SECURITY_FAIL\n\n### [HIGH] — SQL built from user input\n- **File:** src/db.rs:42\n- **Category:** A03 Injection\n- **Remediation:** Use a bound parameter.\n\n### [LOW] — Token logged\n- **File:** src/auth.rs:7\n";

    /// A check that errored is never a pass.
    #[test]
    fn an_error_is_not_a_pass() {
        let v = Verdict::of(Err("service error: throttled".into()));
        assert!(!v.passed());
        assert!(matches!(v, Verdict::NotChecked(ref e) if e.contains("throttled")));
        let items = open_items(&v, false);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].point, "security_unchecked");
        assert!(Verdict::of(Ok(SecurityResult {
            passed: true,
            summary: String::new()
        }))
        .passed());
    }

    /// Each issue the audit names is its own Open item, with where and how.
    #[test]
    fn findings_become_open_items() {
        let items = open_items(&Verdict::Findings(REPORT.to_string()), true);
        assert_eq!(items.len(), 2);
        assert!(items[0].detail.contains("SQL built from user input"));
        assert!(items[0].detail.contains("`src/db.rs:42`"));
        assert!(items[0].settle.starts_with("Use a bound parameter."));
        assert!(items[0].settle.contains("fresh step"));
        assert!(items[1].detail.contains("`src/auth.rs:7`"));
        // A report without headings is still one item, never none.
        let one = open_items(
            &Verdict::Findings("SECURITY_FAIL eval of input".into()),
            false,
        );
        assert_eq!(one.len(), 1);
        assert!(one[0].detail.contains("eval of input"));
    }

    /// The items hold the draft and come out of the description when fixed,
    /// leaving every other Open item alone.
    #[test]
    fn the_description_holds_security_items_until_they_are_fixed() {
        use crate::passes::proof;
        let other = proof::Finding {
            point: "assumption".into(),
            detail: "The API returns at most 100 rows.".into(),
            ..Default::default()
        };
        let mut all = vec![other];
        all.extend(open_items(&Verdict::Findings(REPORT.into()), true));
        let body = format!("## Summary\nx\n\n{}", proof::gaps_section(&all));
        assert!(proof::holds_security_items(&body));

        let fixed = proof::with_security_items(&body, &[]).unwrap();
        assert!(!proof::holds_security_items(&fixed));
        assert!(fixed.contains("The API returns at most 100 rows."));
        assert!(fixed.contains("## Open items"));

        // Still failing after the fix: the new findings replace the old.
        let again = open_items(&Verdict::NotChecked("timeout".into()), false);
        let body2 = proof::with_security_items(&body, &again).unwrap();
        assert!(proof::holds_security_items(&body2));
        assert!(!body2.contains("SQL built from user input"));
        assert!(body2.contains("Security check did not run"));

        // Only security items: the section goes when they do.
        let only = format!(
            "## Summary\nx\n\n{}",
            proof::gaps_section(&open_items(&Verdict::Findings(REPORT.into()), false))
        );
        let gone = proof::with_security_items(&only, &[]).unwrap();
        assert!(!gone.contains("Open items"));
        assert_eq!(proof::with_security_items(&gone, &[]), None);
    }
}
