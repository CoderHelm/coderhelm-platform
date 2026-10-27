//! What the engine needs from the tool registry: a tool's risk, or that
//! nothing knows it.
//!
//! The plugins work (`common::tool_registry`) adds the one registry of every
//! tool with its risk. Until it is merged, [`Builtin`] stands in for it with
//! the same three risks, the same built-in list and the same naming rules for
//! connection tools. When it lands, `impl ToolRisk for` the real registry and
//! [`default_registry`] returns it; nothing else changes.

use super::model::{CallKind, Risk};

/// A tool's risk. `None` means nothing knows what the tool does, and the
/// engine denies it.
pub trait ToolRisk: Send + Sync {
    fn risk(&self, tool: &str) -> Option<Risk>;
}

/// The registry every loop uses.
pub fn default_registry() -> &'static dyn ToolRisk {
    &Builtin
}

/// Stand-in for `common::tool_registry` until the plugins work is merged.
pub struct Builtin;

impl ToolRisk for Builtin {
    fn risk(&self, tool: &str) -> Option<Risk> {
        if let Some(r) = builtin(tool) {
            return Some(r);
        }
        // A connection's tool: `<server>__<tool>`, classified by its name.
        match tool.split_once("__") {
            Some((server, name)) if !server.is_empty() && !name.is_empty() => {
                Some(classify_external(name))
            }
            _ => None,
        }
    }
}

/// Every built-in tool CoderHelm's agents can be given, with its risk. A test
/// reads every tool name the services define and fails when one is missing.
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
    // Chat's code tools (gateway/src/routes/chat_code.rs).
    ("github__list_repos", Risk::Read),
    ("github__repo_overview", Risk::Read),
    ("github__list_files", Risk::Read),
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
    ("rest_get", Risk::Read),
    ("opensearch_indexes", Risk::Read),
    ("opensearch_mapping", Risk::Read),
    ("opensearch_search", Risk::Read),
    ("opensearch_count", Risk::Read),
    ("aws_identity", Risk::Read),
    ("aws_bucket_settings", Risk::Read),
    ("aws_kms_key", Risk::Read),
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
    // A chat asking the person to connect an account (shows a sign-in card).
    ("request_sign_in", Risk::Read),
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
    // A pull request in a repository other than the ticket's (a fix that
    // also needs the infrastructure code changed).
    ("open_pull_request", Risk::Write),
    ("confluence_append", Risk::Write),
];

/// Verbs that make a connection's tool read only, as the first word of its
/// name (`get_issue`, `list_projects`).
const READ_VERBS: &[&str] = &[
    "get", "list", "search", "read", "query", "fetch", "find", "describe", "count", "show", "view",
    "lookup", "inspect", "check", "retrieve", "browse",
];

/// Verbs that make a connection's tool consequential as the first word.
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

/// Words that make a tool consequential wherever they appear in its name.
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

/// A connection tool's risk from its own name: read when it starts with a
/// read verb and names nothing consequential, consequential when any word
/// is, write otherwise. Never read by accident.
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

pub fn builtin(name: &str) -> Option<Risk> {
    BUILTIN.iter().find(|(n, _)| *n == name).map(|(_, r)| *r)
}

/// Data source tools: their prefix names the source kind they query.
const DATA_SOURCE_TOOLS: &[(&str, &str)] = &[
    ("live_", "data sources"),
    ("db_query", "databases"),
    ("logs_search", "logs"),
    ("api_get", "team tools"),
    ("search_read", "search"),
    ("rest_get", "rest"),
    ("opensearch_", "opensearch"),
    ("aws_identity", "aws"),
    ("aws_bucket_settings", "aws"),
    ("aws_kms_key", "aws"),
    ("newrelic_", "newrelic"),
    ("confluence_", "confluence"),
    ("metrics_", "cloudwatch"),
    ("airflow_", "airflow"),
];

/// What kind of call a tool is, and the server or source it goes to.
pub fn kind_of(tool: &str) -> (CallKind, Option<String>) {
    if tool == "confluence_append" || tool == "update_pr_description" {
        let server = (tool == "confluence_append").then(|| "confluence".to_string());
        return (CallKind::Outbound, server);
    }
    if let Some((_, src)) = DATA_SOURCE_TOOLS.iter().find(|(p, _)| tool.starts_with(p)) {
        return (CallKind::DataSource, Some(src.to_string()));
    }
    match tool.split_once("__") {
        Some(("github", _)) => (CallKind::Tool, Some("github".into())),
        Some((server, _)) if !server.is_empty() => (CallKind::Mcp, Some(server.to_string())),
        _ => (CallKind::Tool, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn builtins_mcp_and_unknown() {
        let r = Builtin;
        assert_eq!(r.risk("read_file"), Some(Risk::Read));
        assert_eq!(r.risk("write_file"), Some(Risk::Write));
        assert_eq!(r.risk("linear__list_issues"), Some(Risk::Read));
        assert_eq!(r.risk("linear__create_issue"), Some(Risk::Write));
        assert_eq!(
            r.risk("sanity__publish_document"),
            Some(Risk::Consequential)
        );
        assert_eq!(r.risk("get_and_delete"), None);
        assert_eq!(r.risk("something_new"), None);
        assert_eq!(r.risk("__x"), None);
        assert_eq!(classify_external("get_post"), Risk::Read);
        assert_eq!(classify_external("post_message"), Risk::Consequential);
        assert_eq!(classify_external("get_and_delete"), Risk::Consequential);
    }

    #[test]
    fn kinds() {
        assert_eq!(kind_of("read_file"), (CallKind::Tool, None));
        assert_eq!(kind_of("db_query").0, CallKind::DataSource);
        assert_eq!(
            kind_of("linear__get_issue"),
            (CallKind::Mcp, Some("linear".into()))
        );
        assert_eq!(kind_of("github__read_file").0, CallKind::Tool);
        assert_eq!(kind_of("confluence_append").0, CallKind::Outbound);
    }

    #[test]
    fn builtins_are_listed_once() {
        let mut seen = BTreeSet::new();
        for (n, _) in BUILTIN {
            assert!(seen.insert(*n), "{n} listed twice");
        }
    }

    /// Every tool name the gateway and worker define has a risk here, so no
    /// agent loop offers a tool the engine would deny as unknown.
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
            if !path.ends_with("checks.rs") {
                for c in ops_tool.captures_iter(&src) {
                    found.insert(c[1].to_string());
                }
            }
        }
        assert!(found.len() > 40, "found only {} tools", found.len());
        let missing: Vec<_> = found.iter().filter(|n| Builtin.risk(n).is_none()).collect();
        assert!(
            missing.is_empty(),
            "tools with no risk in agent_policy::registry::BUILTIN: {missing:?}"
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
                && !p.to_string_lossy().contains("agent_policy")
                && !p.to_string_lossy().contains("mcp_remote")
            {
                out.push(p);
            }
        }
    }
}
