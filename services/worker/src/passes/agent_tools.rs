//! The tools a team agent answers with.
//!
//! A team agent (an `agents/*.md` file in the team's skills repository) is
//! asked one question by the planner, coder or reviewer. It gets exactly the
//! read-only Coderhelm tools its own file lists (`common::skills::
//! agent_toolset`): reading and searching the repository the asking agent is
//! on, and the asking agent's own history, code graph, package and data
//! source reads when its file lists those. Never a write, a command or an MCP
//! server. A file that lists no tools gets none.
//!
//! Every call is bounded ([`MAX_CALLS`], and the ask's own time limit) and
//! kept on the team's access log with the agent's name and the run it was
//! asked during. What a tool returns is redacted and clipped before the
//! agent reads it.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{json, Value};

use crate::agent::llm::{ToolDefinition, ToolExecutor};
use crate::clients::github::GitHubClient;

/// Tool calls one ask may make.
pub const MAX_CALLS: usize = 12;
/// What one tool call may return to the agent.
const RESULT_CHARS: usize = 12_000;

/// Repository reads Coderhelm runs itself for an agent, against the
/// repository and ref the asking agent is on.
pub const OWN: &[&str] = &[
    "read_file",
    "read_file_lines",
    "search_code",
    "read_tree",
    "list_directory",
    "check_package",
];

/// The repository the asking agent is on, for the agent's reads.
#[derive(Clone, Copy)]
pub struct RepoReads<'a> {
    pub github: &'a GitHubClient,
    pub owner: &'a str,
    pub repo: &'a str,
    /// The branch or commit the asking agent reads.
    pub git_ref: &'a str,
    pub http: &'a reqwest::Client,
}

/// What an ask can lend the agent: the repository reads, and the asking
/// agent's own executor for the other read-only tools it has (history,
/// graph, packages, data sources), with their definitions.
#[derive(Clone, Copy, Default)]
pub struct Host<'a> {
    pub reads: Option<RepoReads<'a>>,
    pub parent: Option<&'a dyn ToolExecutor>,
    pub offered: &'a [ToolDefinition],
}

impl<'a> Host<'a> {
    pub fn new(
        reads: Option<RepoReads<'a>>,
        parent: &'a dyn ToolExecutor,
        offered: &'a [ToolDefinition],
    ) -> Self {
        Host {
            reads,
            parent: Some(parent),
            offered,
        }
    }
}

fn clone_def(t: &ToolDefinition) -> ToolDefinition {
    ToolDefinition {
        name: t.name.clone(),
        description: t.description.clone(),
        input_schema: t.input_schema.clone(),
    }
}

/// The definitions Coderhelm has for its own repository reads.
fn own_defs() -> Vec<ToolDefinition> {
    let mut out: Vec<ToolDefinition> = super::implement::coder_tools()
        .into_iter()
        .filter(|t| OWN.contains(&t.name.as_str()))
        .collect();
    out.extend(
        super::review_agent::review_tools()
            .into_iter()
            .filter(|t| t.name == "check_package"),
    );
    out
}

/// The tools the agent gets for this ask: each one its file grants that this
/// ask can run (an own read needs the repository; anything else must be
/// offered by the asking agent). Write tools are never in a grant.
pub fn tools_for(granted: &[String], host: &Host<'_>) -> Vec<ToolDefinition> {
    let own = own_defs();
    let mut out: Vec<ToolDefinition> = Vec::new();
    for g in granted {
        if common::skills::AGENT_WRITE_TOOLS.contains(&g.as_str())
            || !common::skills::AGENT_READ_TOOLS.contains(&g.as_str())
        {
            continue;
        }
        let def = if OWN.contains(&g.as_str()) && host.reads.is_some() {
            own.iter().find(|t| &t.name == g).map(clone_def)
        } else {
            host.offered
                .iter()
                .find(|t| &t.name == g && host.parent.is_some())
                .map(clone_def)
        };
        if let Some(d) = def {
            if !out.iter().any(|t| t.name == d.name) {
                out.push(d);
            }
        }
    }
    out
}

/// One tool call, for the access log.
pub struct Call {
    pub tool: String,
    pub input: String,
    pub outcome: &'static str,
    pub ms: i64,
}

/// Runs the agent's tool calls: only the tools it was given, at most
/// [`MAX_CALLS`], each kept through `log`.
pub struct Bounded<'a> {
    pub allowed: HashSet<String>,
    pub host: Host<'a>,
    pub calls: AtomicUsize,
    pub log: &'a (dyn Fn(Call) -> futures::future::BoxFuture<'a, ()> + Send + Sync),
}

/// What a call's input says, shortened, for the log.
pub fn input_summary(input: &Value) -> String {
    let text = match input {
        Value::Object(m) => m
            .iter()
            .map(|(k, v)| match v {
                Value::String(s) => format!("{k}: {s}"),
                other => format!("{k}: {other}"),
            })
            .collect::<Vec<_>>()
            .join(", "),
        other => other.to_string(),
    };
    common::truncate_str(&text, 240).to_string()
}

impl Bounded<'_> {
    async fn run(&self, name: &str, input: &Value) -> Result<String, String> {
        if OWN.contains(&name) {
            if name == "check_package" {
                let http = self
                    .host
                    .reads
                    .map(|r| r.http)
                    .ok_or("check_package isn't available here")?;
                let registry = input["registry"].as_str().unwrap_or("");
                let pkg = input["name"].as_str().unwrap_or("");
                if pkg.is_empty() || pkg.len() > 200 {
                    return Ok("(check_package: missing/invalid package name)".into());
                }
                return Ok(super::review_agent::check_package_registry(
                    http,
                    registry,
                    pkg,
                    input["version"].as_str(),
                )
                .await);
            }
            if let Some(r) = self.host.reads {
                return own_read(&r, name, input).await.map_err(|e| e.to_string());
            }
        }
        let parent = self.host.parent.ok_or("This tool isn't available here")?;
        parent
            .execute(name, input)
            .await
            .map(|v| match v {
                Value::String(s) => s,
                other => other.to_string(),
            })
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl ToolExecutor for Bounded<'_> {
    async fn execute(
        &self,
        name: &str,
        input: &Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let started = std::time::Instant::now();
        let summary = input_summary(input);
        if !self.allowed.contains(name) {
            (self.log)(Call {
                tool: name.to_string(),
                input: summary,
                outcome: "refused",
                ms: 0,
            })
            .await;
            return Ok(json!(format!(
                "{name} isn't one of your tools. You have: {}. Answer from what you have.",
                {
                    let mut v: Vec<&String> = self.allowed.iter().collect();
                    v.sort();
                    v.into_iter().cloned().collect::<Vec<_>>().join(", ")
                }
            )));
        }
        if self.calls.fetch_add(1, Ordering::SeqCst) >= MAX_CALLS {
            (self.log)(Call {
                tool: name.to_string(),
                input: summary,
                outcome: "limit",
                ms: 0,
            })
            .await;
            return Ok(json!(format!(
                "All {MAX_CALLS} tool calls for this question are used. Answer now from what you have."
            )));
        }
        let r = self.run(name, input).await;
        let ms = started.elapsed().as_millis() as i64;
        let (outcome, text) = match r {
            Ok(t) => ("ok", t),
            Err(e) => ("failed", format!("({name} failed: {e})")),
        };
        (self.log)(Call {
            tool: name.to_string(),
            input: summary,
            outcome,
            ms,
        })
        .await;
        let (clean, _) = common::live::redact::redact_text(&text);
        Ok(json!(common::head_tail_str(&clean, RESULT_CHARS)))
    }
}

async fn own_read(
    r: &RepoReads<'_>,
    name: &str,
    input: &Value,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let path = input["path"].as_str().unwrap_or("");
    match name {
        "read_tree" => {
            let tree = r.github.get_tree(r.owner, r.repo, r.git_ref).await?;
            let paths: Vec<&str> = tree
                .iter()
                .filter(|e| e.entry_type == "blob")
                .map(|e| e.path.as_str())
                .collect();
            Ok(super::truncate_tree(&paths))
        }
        "read_file" => {
            if path.is_empty() {
                return Err("Missing path".into());
            }
            let content = r.github.read_file(r.owner, r.repo, path, r.git_ref).await?;
            Ok(super::truncate_content(&content, path))
        }
        "read_file_lines" => {
            if path.is_empty() {
                return Err("Missing path".into());
            }
            let start = input["start_line"].as_u64().unwrap_or(1) as usize;
            let end = input["end_line"].as_u64().unwrap_or(start as u64 + 100) as usize;
            let end = end.min(start + 200);
            Ok(r.github
                .read_file_lines(r.owner, r.repo, path, r.git_ref, start, end)
                .await?)
        }
        "search_code" => {
            let query = input["query"].as_str().ok_or("Missing query")?;
            let results = r
                .github
                .search_code(r.owner, r.repo, r.git_ref, query)
                .await?;
            Ok(results
                .iter()
                .map(|x| {
                    if x.matches.is_empty() {
                        x.path.clone()
                    } else {
                        format!("{}\n{}", x.path, x.matches.join("\n"))
                    }
                })
                .collect::<Vec<_>>()
                .join("\n---\n"))
        }
        "list_directory" => {
            let entries = r
                .github
                .list_directory(r.owner, r.repo, path, r.git_ref)
                .await?;
            Ok(entries
                .iter()
                .map(|e| format!("{}: {}", e.entry_type, e.name))
                .collect::<Vec<_>>()
                .join("\n"))
        }
        other => Err(format!("Unknown tool: {other}").into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A parent that answers every tool and remembers what it ran.
    struct Parent(Mutex<Vec<String>>);

    #[async_trait::async_trait]
    impl ToolExecutor for Parent {
        async fn execute(
            &self,
            name: &str,
            _input: &Value,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            self.0.lock().unwrap().push(name.to_string());
            Ok(json!(format!(
                "{name} ran; token=ghp_abcdefghijklmnopqrstuvwxyz0123456789"
            )))
        }
    }

    fn def(name: &str) -> ToolDefinition {
        ToolDefinition {
            name: name.into(),
            description: name.into(),
            input_schema: json!({"type": "object"}),
        }
    }

    fn names(v: &[ToolDefinition]) -> Vec<&str> {
        v.iter().map(|t| t.name.as_str()).collect()
    }

    #[test]
    fn an_agent_gets_only_its_granted_reads_the_ask_can_run() {
        let parent = Parent(Mutex::new(vec![]));
        let offered = [
            def("github_history"),
            def("write_file"),
            def("run_checks"),
            def("logs_search"),
            def("confluence_append"),
        ];
        let host = Host::new(None, &parent, &offered);
        let granted: Vec<String> = [
            "github_history",
            "logs_search",
            "write_file",
            "run_checks",
            "confluence_append",
            "read_file",
            "check_package",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        // Without the repository, the repository reads can't run; writes
        // and commands are never given even if a grant somehow named them.
        assert_eq!(
            names(&tools_for(&granted, &host)),
            ["github_history", "logs_search"]
        );
        // Nothing granted, nothing given.
        assert!(tools_for(&[], &host).is_empty());
        // Every own read has a definition.
        assert_eq!(own_defs().len(), OWN.len());
    }

    #[tokio::test]
    async fn calls_are_bounded_refused_off_list_logged_and_redacted() {
        let parent = Parent(Mutex::new(vec![]));
        let offered = [def("github_history"), def("write_file")];
        let logged: Mutex<Vec<(String, &'static str)>> = Mutex::new(vec![]);
        let log = |c: Call| -> futures::future::BoxFuture<'_, ()> {
            logged.lock().unwrap().push((c.tool, c.outcome));
            Box::pin(async {})
        };
        let b = Bounded {
            allowed: ["github_history".to_string()].into_iter().collect(),
            host: Host::new(None, &parent, &offered),
            calls: AtomicUsize::new(0),
            log: &log,
        };
        // A write it wasn't given is refused and never reaches the parent.
        let out = b
            .execute("write_file", &json!({"path": "x"}))
            .await
            .unwrap();
        assert!(out.as_str().unwrap().contains("isn't one of your tools"));
        let out = b
            .execute("github_history", &json!({"action": "commits"}))
            .await
            .unwrap();
        let text = out.as_str().unwrap();
        assert!(text.starts_with("github_history ran"));
        assert!(
            !text.contains("ghp_abcdefghijklmnopqrstuvwxyz0123456789"),
            "{text}"
        );
        for _ in 1..MAX_CALLS {
            b.execute("github_history", &json!({})).await.unwrap();
        }
        let out = b.execute("github_history", &json!({})).await.unwrap();
        assert!(out
            .as_str()
            .unwrap()
            .contains("tool calls for this question are used"));
        assert_eq!(parent.0.lock().unwrap().len(), MAX_CALLS);
        assert!(!parent.0.lock().unwrap().iter().any(|n| n == "write_file"));
        let logged = logged.lock().unwrap();
        assert_eq!(logged.len(), MAX_CALLS + 2, "every call is logged");
        assert_eq!(logged[0], ("write_file".to_string(), "refused"));
        assert_eq!(logged.last().unwrap().1, "limit");
    }

    #[test]
    fn the_logged_input_is_short() {
        let s = input_summary(&json!({"path": "src/a.rs", "start_line": 3}));
        assert_eq!(s, "path: src/a.rs, start_line: 3");
        assert!(input_summary(&json!({"q": "x".repeat(1000)})).len() <= 240);
    }
}
