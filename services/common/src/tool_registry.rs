//! One registry of every tool an agent can be given, with its risk.
//!
//! Every tool is one of three risks:
//!
//! - **read**: looks something up and changes nothing (a file, a search, a
//!   data source, a connection's `get_*`).
//! - **write**: changes something that can be undone or reviewed before it
//!   reaches anyone (a file in the working branch, a draft, a page note).
//! - **consequential**: reaches people or can't be taken back (publish, send,
//!   post, delete, merge, deploy, pay). These never run on a model's say so:
//!   a plugin's consequential tool runs only after a person approves the
//!   exact version of what it sends (see `plugins::approvals`).
//!
//! Before this registry each pass built its own list and the planner kept
//! MCP tools by a name prefix. Now every pass asks [`risk_of`], and a plugin
//! run only gets what its manifest grants ([`Grant`]): a tool that isn't
//! granted doesn't exist for that run.

use serde::{Deserialize, Serialize};

/// How much a tool can change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Risk {
    Read,
    Write,
    Consequential,
}

impl Risk {
    pub fn as_str(self) -> &'static str {
        match self {
            Risk::Read => "read",
            Risk::Write => "write",
            Risk::Consequential => "consequential",
        }
    }

    pub fn parse(s: &str) -> Option<Risk> {
        match s.trim() {
            "read" => Some(Risk::Read),
            "write" => Some(Risk::Write),
            "consequential" => Some(Risk::Consequential),
            _ => None,
        }
    }
}

/// Every built-in tool CoderHelm's agents (code agents, the reviewer, plan
/// chat, team agents and plugin runs) can be given, with its risk. A test
/// reads every tool name the gateway and worker define and fails when one is
/// missing here.
pub const BUILTIN: &[(&str, Risk)] = &[
    // The repository being worked on.
    ("read_file", Risk::Read),
    ("read_file_lines", Risk::Read),
    ("search_code", Risk::Read),
    ("read_tree", Risk::Read),
    ("list_directory", Risk::Read),
    ("get_diff", Risk::Read),
    ("github_history", Risk::Read),
    ("search_org", Risk::Read),
    ("github__read_file", Risk::Read),
    ("github__search_code", Risk::Read),
    ("github__list_directory", Risk::Read),
    ("graph_definition", Risk::Read),
    ("graph_callers", Risk::Read),
    ("graph_impact", Risk::Read),
    ("check_package", Risk::Read),
    ("check_url", Risk::Read),
    ("count_lines", Risk::Read),
    // Data sources and connections, read only.
    ("live_describe", Risk::Read),
    ("live_lookup", Risk::Read),
    ("db_query", Risk::Read),
    ("logs_search", Risk::Read),
    ("api_get", Risk::Read),
    ("search_read", Risk::Read),
    ("aws_identity", Risk::Read),
    ("newrelic_accounts", Risk::Read),
    ("newrelic_nrql", Risk::Read),
    ("newrelic_entities", Risk::Read),
    ("newrelic_changes", Risk::Read),
    ("newrelic_deployment_impact", Risk::Read),
    ("newrelic_golden_metrics", Risk::Read),
    ("confluence_search", Risk::Read),
    ("confluence_page", Risk::Read),
    ("confluence_children", Risk::Read),
    ("metrics_list", Risk::Read),
    ("metrics_query", Risk::Read),
    ("airflow_dags", Risk::Read),
    ("airflow_dag_runs", Risk::Read),
    ("airflow_task_instances", Risk::Read),
    ("airflow_task_log", Risk::Read),
    ("airflow_import_errors", Risk::Read),
    ("airflow_health", Risk::Read),
    // A repository's test reports, where the team keeps them.
    ("test_report", Risk::Read),
    ("test_pass_counts", Risk::Read),
    // Team skills and agents.
    ("read_skill", Risk::Read),
    ("read_skill_file", Risk::Read),
    ("ask_team_agent", Risk::Read),
    // Knowledge bases (the brain's search tool, and a plugin's).
    ("search_knowledge", Risk::Read),
    // Plugin runs: the plugin's own skills, and drafts kept in CoderHelm
    // until a person approves them.
    ("read_plugin_skill", Risk::Read),
    ("save_draft", Risk::Write),
    // Writes to the working branch (reviewed in the PR).
    ("write_file", Risk::Write),
    ("edit_file", Risk::Write),
    ("batch_write", Risk::Write),
    ("restore_file", Risk::Write),
    ("run_codegen", Risk::Write),
    ("run_checks", Risk::Write),
    ("update_pr_description", Risk::Write),
    ("confluence_append", Risk::Write),
];

/// Verbs that make a connection's tool read only, as the first word of its
/// name (`get_issue`, `list_projects`).
const READ_VERBS: &[&str] = &[
    "get", "list", "search", "read", "query", "fetch", "find", "describe", "count", "show", "view",
    "lookup", "inspect", "check", "retrieve", "browse",
];

/// Verbs that make a connection's tool consequential as the first word of
/// its name (`send_email`, `post_message`, `schedule_post`).
const CONSEQUENTIAL_VERBS: &[&str] = &[
    "publish",
    "send",
    "post",
    "delete",
    "remove",
    "destroy",
    "drop",
    "merge",
    "deploy",
    "release",
    "pay",
    "charge",
    "refund",
    "transfer",
    "invite",
    "revoke",
    "archive",
    "email",
    "tweet",
    "share",
    "schedule",
    "submit",
    "purge",
    "unpublish",
    "notify",
    "broadcast",
];

/// Words that make a tool consequential wherever they appear in its name
/// (`bulk_delete_pages`, `get_and_publish`). Only words that are never a
/// plain noun: `get_post` reads a post.
const CONSEQUENTIAL_ANYWHERE: &[&str] = &[
    "publish",
    "unpublish",
    "delete",
    "destroy",
    "purge",
    "deploy",
    "refund",
    "transfer",
    "revoke",
    "broadcast",
];

fn words(name: &str) -> Vec<String> {
    name.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(|w| w.to_ascii_lowercase())
        .collect()
}

/// The risk of a connection's (MCP or HTTP) tool, from its own name: read
/// when it starts with a read verb and names nothing consequential,
/// consequential when any word is, write otherwise. Unknown is never read.
pub fn classify_external(tool: &str) -> Risk {
    let w = words(tool);
    if w.iter()
        .any(|x| CONSEQUENTIAL_ANYWHERE.contains(&x.as_str()))
    {
        return Risk::Consequential;
    }
    match w.first().map(String::as_str) {
        Some(first) if CONSEQUENTIAL_VERBS.contains(&first) => Risk::Consequential,
        Some(first) if READ_VERBS.contains(&first) => Risk::Read,
        _ => Risk::Write,
    }
}

/// A built-in tool's risk, if it is one.
pub fn builtin(name: &str) -> Option<Risk> {
    BUILTIN.iter().find(|(n, _)| *n == name).map(|(_, r)| *r)
}

/// The risk of any tool an agent is offered: a built-in from [`BUILTIN`],
/// a connection's tool (`<server>__<tool>`) from its name. Anything else is
/// treated as write: an unknown tool is never read only.
pub fn risk_of(name: &str) -> Risk {
    if let Some(r) = builtin(name) {
        return r;
    }
    match name.split_once("__") {
        Some((_, tool)) => classify_external(tool),
        None => Risk::Write,
    }
}

/// True for a tool that only reads.
pub fn is_read_only(name: &str) -> bool {
    risk_of(name) == Risk::Read
}

/// The read-only built-in tools, for passes that may only read.
pub fn read_only_builtins() -> impl Iterator<Item = &'static str> {
    BUILTIN
        .iter()
        .filter(|(_, r)| *r == Risk::Read)
        .map(|(n, _)| *n)
}

/// `pattern` with `*` matching any run of characters.
pub fn glob_match(pattern: &str, name: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == name;
    }
    let mut rest = name;
    for (i, p) in parts.iter().enumerate() {
        if i == 0 {
            match rest.strip_prefix(p) {
                Some(r) => rest = r,
                None => return false,
            }
        } else if i == parts.len() - 1 {
            return rest.ends_with(p);
        } else if let Some(at) = rest.find(p) {
            rest = &rest[at + p.len()..];
        } else {
            return false;
        }
    }
    true
}

/// One granted tool: a name or pattern, and the risk it runs at.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Granted {
    /// The name the agent calls (`sanity__create_draft`, `publish_post`), or
    /// a pattern (`sanity__get_*`).
    pub pattern: String,
    /// The risk the manifest declares. It can raise a tool's risk, never
    /// lower it below what the registry says.
    #[serde(default)]
    pub declared: Option<Risk>,
}

/// What a run may call: exactly the granted tools, nothing else.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub tools: Vec<Granted>,
}

impl Grant {
    /// The grant entry that covers `name`, if any.
    pub fn entry(&self, name: &str) -> Option<&Granted> {
        self.tools.iter().find(|g| glob_match(&g.pattern, name))
    }

    /// True when the manifest grants `name`. A tool that isn't granted
    /// doesn't exist for the run.
    pub fn allows(&self, name: &str) -> bool {
        self.entry(name).is_some()
    }

    /// The risk `name` runs at: the registry's, raised to the manifest's
    /// declared risk when that is higher. `None` when it isn't granted.
    pub fn risk(&self, name: &str) -> Option<Risk> {
        let g = self.entry(name)?;
        let base = risk_of(name);
        Some(match g.declared {
            Some(d) if d > base => d,
            _ => base,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn connection_tools_are_classified_from_their_names() {
        assert_eq!(classify_external("get_issue"), Risk::Read);
        assert_eq!(classify_external("list_projects"), Risk::Read);
        assert_eq!(classify_external("search_documents"), Risk::Read);
        assert_eq!(classify_external("create_draft"), Risk::Write);
        assert_eq!(classify_external("update_issue"), Risk::Write);
        assert_eq!(classify_external("publish_post"), Risk::Consequential);
        assert_eq!(classify_external("send_email"), Risk::Consequential);
        assert_eq!(classify_external("delete_page"), Risk::Consequential);
        // A read verb never hides a consequential word.
        assert_eq!(classify_external("get_and_delete"), Risk::Consequential);
        assert_eq!(classify_external("get_post"), Risk::Read);
        assert_eq!(classify_external("post_message"), Risk::Consequential);
        assert_eq!(classify_external("schedule_post"), Risk::Consequential);
        assert_eq!(classify_external("unknown"), Risk::Write);
        assert_eq!(classify_external(""), Risk::Write);
    }

    #[test]
    fn risk_of_knows_builtins_and_mcp_tools() {
        assert_eq!(risk_of("read_file"), Risk::Read);
        assert_eq!(risk_of("write_file"), Risk::Write);
        assert_eq!(risk_of("save_draft"), Risk::Write);
        assert_eq!(risk_of("sentry__list_issues"), Risk::Read);
        assert_eq!(risk_of("linear__create_issue"), Risk::Write);
        assert_eq!(risk_of("sanity__publish_document"), Risk::Consequential);
        assert_eq!(risk_of("something_new"), Risk::Write);
        assert!(is_read_only("api_get"));
        assert!(!is_read_only("confluence_append"));
    }

    #[test]
    fn a_tool_missing_from_the_grant_cannot_be_called() {
        let g = Grant {
            tools: vec![
                Granted {
                    pattern: "sanity__get_*".into(),
                    declared: None,
                },
                Granted {
                    pattern: "sanity__create_draft".into(),
                    declared: None,
                },
                Granted {
                    pattern: "publish_post".into(),
                    declared: Some(Risk::Consequential),
                },
                Granted {
                    pattern: "save_draft".into(),
                    declared: None,
                },
            ],
        };
        assert!(g.allows("sanity__get_document"));
        assert!(g.allows("sanity__create_draft"));
        assert!(!g.allows("sanity__delete_document"));
        assert!(!g.allows("sanity__create_draftx"));
        assert!(!g.allows("write_file"));
        assert!(!g.allows("linear__get_issue"));
        assert_eq!(g.risk("sanity__delete_document"), None);
        assert_eq!(g.risk("publish_post"), Some(Risk::Consequential));
        assert_eq!(g.risk("sanity__get_document"), Some(Risk::Read));
    }

    #[test]
    fn a_declared_risk_raises_but_never_lowers() {
        let g = Grant {
            tools: vec![
                Granted {
                    pattern: "cms__delete_*".into(),
                    declared: Some(Risk::Read),
                },
                Granted {
                    pattern: "cms__get_*".into(),
                    declared: Some(Risk::Write),
                },
            ],
        };
        assert_eq!(g.risk("cms__delete_page"), Some(Risk::Consequential));
        assert_eq!(g.risk("cms__get_page"), Some(Risk::Write));
    }

    #[test]
    fn glob_patterns() {
        assert!(glob_match("get_*", "get_x"));
        assert!(glob_match("*_draft", "create_draft"));
        assert!(glob_match("a*b*c", "aXbYc"));
        assert!(!glob_match("a*b*c", "aXbY"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "exactly"));
        assert!(glob_match("*", "anything"));
    }

    #[test]
    fn builtins_are_listed_once() {
        let mut seen = BTreeSet::new();
        for (n, _) in BUILTIN {
            assert!(seen.insert(*n), "{n} listed twice");
        }
    }

    /// Every tool name the gateway and worker define is in the registry, so
    /// no pass can offer a tool whose risk nobody decided.
    #[test]
    fn every_defined_tool_is_registered() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let mut files = Vec::new();
        for dir in ["worker/src", "gateway/src", "common/src"] {
            collect(&root.join(dir), &mut files);
        }
        let defs = [
            regex::Regex::new(
                r#"ToolDefinition\s*\{\s*name:\s*"([a-z_]+)"\.(?:to_string|into)\(\)"#,
            )
            .unwrap(),
            regex::Regex::new(r#"\bdef\(\s*"([a-z_]+)""#).unwrap(),
            regex::Regex::new(r#"pub const TOOL: &str = "([a-z_]+)";"#).unwrap(),
            regex::Regex::new(r#""name":\s*"([a-z_]+)",\s*"description""#).unwrap(),
        ];
        let ops_tool = regex::Regex::new(r#"\btool\(\s*"([a-z_]+)""#).unwrap();
        let mut found = BTreeSet::new();
        for f in &files {
            let src = std::fs::read_to_string(f).unwrap_or_default();
            let path = f.to_string_lossy();
            for re in &defs {
                for c in re.captures_iter(&src) {
                    found.insert(c[1].to_string());
                }
            }
            // checks.rs's `tool(...)` rows are check programs, not agent tools.
            if !path.ends_with("checks.rs") {
                for c in ops_tool.captures_iter(&src) {
                    found.insert(c[1].to_string());
                }
            }
        }
        assert!(found.len() > 40, "found only {} tools", found.len());
        let missing: Vec<_> = found.iter().filter(|n| builtin(n).is_none()).collect();
        assert!(
            missing.is_empty(),
            "tools with no risk in tool_registry::BUILTIN: {missing:?}"
        );
    }

    fn collect(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                collect(&p, out);
            } else if p.extension().map(|x| x == "rs").unwrap_or(false)
                && !p.ends_with("tool_registry.rs")
                // A remote server's own tools (and its tests' samples) are
                // classified by name at run time, not listed here.
                && !p.to_string_lossy().contains("mcp_remote")
            {
                out.push(p);
            }
        }
    }
}
