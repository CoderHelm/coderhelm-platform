//! What the engine needs from the tool registry: a tool's risk, or that
//! nothing knows it.
//!
//! The plugins work (`common::tool_registry`) adds the one registry of every
//! tool with its risk. Until it is merged, [`Builtin`] stands in for it with
//! the same three risks, the same built-in list and the same naming rules for
//! connection tools. When it lands, `impl ToolRisk for` the real registry and
//! [`default_registry`] returns it; nothing else changes.
//!
//! Tools a team sets up (its own approved tools, its connections' tools)
//! can't be listed here. Each such [`Family`] registers its tools where it
//! offers them, with the risk its definition gives ([`register`]), and
//! [`known`] reads built-ins, then registrations, then a connection tool's
//! name. A test fails when a runtime tool name belongs to no family.

use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

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

/// A family of tools that exist only once a team sets them up: their names
/// and what they do come from the team's own records, so no list here can
/// name them. Each tool is registered by the code that offers it, with the
/// risk its registration says it has (see [`Registration`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Family {
    /// A team's own tools (`tool_<name>`, `common::live::tools`): a reviewed
    /// read a person approved, by exact version.
    TeamTool,
    /// A connection's tools (`<server>__<tool>`, `common::mcp_remote`): the
    /// server's own hints, else the tool's name.
    Connection,
}

impl Family {
    /// Every family. A test fails when one has no way to give its tools a
    /// risk.
    pub const ALL: &'static [Family] = &[Family::TeamTool, Family::Connection];

    /// The family a tool name belongs to, by its naming rule.
    pub fn of(tool: &str) -> Option<Family> {
        if builtin(tool).is_some() {
            return None;
        }
        if tool
            .strip_prefix(TEAM_TOOL_PREFIX)
            .is_some_and(|n| !n.is_empty())
        {
            return Some(Family::TeamTool);
        }
        match tool.split_once("__") {
            Some((server, name)) if !server.is_empty() && !name.is_empty() => {
                Some(Family::Connection)
            }
            _ => None,
        }
    }

    /// What people call a tool of this family.
    pub fn noun(self) -> &'static str {
        match self {
            Family::TeamTool => "team tool",
            Family::Connection => "connection tool",
        }
    }
}

/// What a team tool's agent-facing name starts with.
pub const TEAM_TOOL_PREFIX: &str = "tool_";

/// One tool a family offers, as the code that offers it describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    /// The agent-facing name (`tool_find_order`, `linear__get_issue`).
    pub tool: String,
    pub family: Family,
    /// The name people know it by, from its definition ("Find order").
    pub label: String,
    /// Its risk, from what its definition allows it to do; or, when no
    /// version of it may run, why (the agent and the decision log say so).
    pub standing: Result<Risk, String>,
}

/// Registered tools, by team, then agent-facing name. A tool's standing is
/// a fact about the team's records (approved, switched on, what it does),
/// never about one run, so every loop for the team reads the same entry.
static REGISTERED: LazyLock<RwLock<HashMap<String, HashMap<String, Registration>>>> =
    LazyLock::new(Default::default);

/// Registers tools a family offers for `team`, replacing what was registered
/// under the same names.
pub fn register<I: IntoIterator<Item = Registration>>(team: &str, regs: I) {
    let mut all = REGISTERED.write().unwrap_or_else(|e| e.into_inner());
    let entry = all.entry(team.to_string()).or_default();
    for r in regs {
        entry.insert(r.tool.clone(), r);
    }
}

/// What was registered for `tool` in `team`.
pub fn registration(team: &str, tool: &str) -> Option<Registration> {
    REGISTERED
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(team)
        .and_then(|t| t.get(tool))
        .cloned()
}

/// What the engine knows about `tool` for `team`: a built-in's risk, a
/// registered tool's standing, a connection tool's risk from its name, or
/// nothing.
pub fn known(team: &str, tool: &str, reg: &dyn ToolRisk) -> super::engine::Known {
    use super::engine::Known;
    if let Some(r) = builtin(tool) {
        return Known::Risk(r);
    }
    if let Some(r) = registration(team, tool) {
        return match r.standing {
            Ok(risk) => Known::Risk(risk),
            Err(why) => Known::Refused(why),
        };
    }
    reg.risk(tool).into()
}

/// The name people know a tool by, when it was registered with one.
pub fn label(team: &str, tool: &str) -> Option<String> {
    registration(team, tool)
        .map(|r| r.label)
        .filter(|l| !l.is_empty())
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
    ("query_content", Risk::Read),
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
    // A team skill's own script in the sandbox (read-only GitHub token at
    // most), and the run's outputs kept in CoderHelm.
    ("run_skill_script", Risk::Write),
    ("save_output", Risk::Write),
    ("read_output", Risk::Read),
    // Outside steps CoderHelm runs after the PR opens (worker outside_exec).
    ("start_workflow", Risk::Write),
    ("write_content", Risk::Write),
    ("update_pr_description", Risk::Write),
    // A pull request in a repository other than the ticket's (a fix that
    // also needs the infrastructure code changed).
    ("open_pull_request", Risk::Write),
    ("confluence_append", Risk::Write),
    // Release steps CoderHelm runs after a pull request (see the worker's
    // release_run): staging writes and deploys, then production's.
    ("release_dry_run", Risk::Write),
    ("release_staging_migration", Risk::Write),
    ("release_staging_deploy", Risk::Write),
    ("release_production_migration", Risk::Consequential),
    ("release_production_deploy", Risk::Consequential),
    ("release_production_tag", Risk::Consequential),
    ("release_cache_purge", Risk::Consequential),
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

    /// A tool of each family, made the way the code that offers it makes
    /// one. A new family fails to compile here until it says how its tools
    /// get a risk.
    fn sample(f: Family) -> Registration {
        match f {
            Family::TeamTool => {
                let def = crate::live::tools::parse_def(
                    "name: find_order\nuse_when: an order number is named\nsource: db\nkind: sql\nread: SELECT id FROM orders WHERE id = :n\ninputs:\n  n: { type: integer, example: 1 }\n",
                )
                .unwrap();
                crate::live::tools::registration(&def.name, &Ok(def.clone()))
            }
            Family::Connection => {
                let server = crate::mcp_remote::store::ServerRecord::new(
                    "acme",
                    "Acme Docs",
                    "https://mcp.acme.example",
                );
                let t: crate::mcp_remote::client::RemoteTool = serde_json::from_value(
                    serde_json::json!({ "name": "get_page", "read_only": true }),
                )
                .unwrap();
                crate::mcp_remote::tools::registration(&server, &t).unwrap()
            }
        }
    }

    /// Every family gives its tools a risk from their registration, and the
    /// engine reads it: no family's tools are denied as unknown.
    #[test]
    fn every_tool_family_has_a_risk_mapping() {
        for (i, f) in Family::ALL.iter().enumerate() {
            let r = sample(*f);
            assert_eq!(r.family, *f);
            assert_eq!(Family::of(&r.tool), Some(*f), "{} names no family", r.tool);
            assert!(r.standing.is_ok(), "{f:?} gave {} no risk", r.tool);
            assert!(!r.label.is_empty(), "{f:?} gave {} no name", r.tool);
            let team = format!("TEAM#family-{i}");
            register(&team, [r.clone()]);
            assert_eq!(
                known(&team, &r.tool, &Builtin).risk(),
                r.standing.clone().ok(),
                "{f:?}: the registry doesn't read the registered risk"
            );
            assert_eq!(label(&team, &r.tool), Some(r.label.clone()));
        }
    }

    /// Every tool name the services build at run time (`tool_{name}`,
    /// `{server}__{tool}`) belongs to a family, and every family is built
    /// somewhere. A new kind of runtime tool needs a family, and so a risk,
    /// before it can be offered.
    #[test]
    fn every_runtime_tool_name_belongs_to_a_family() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let mut files = Vec::new();
        for dir in ["worker/src", "gateway/src", "common/src"] {
            collect(&root.join(dir), &mut files);
        }
        let built = regex::Regex::new(
            r#"(?:name:\s*|let name = |let n = |name = |fn \w*name\([^)]*\) -> String \{\s*)format!\(\s*"([^"]*\{[^"]*)""#,
        )
        .unwrap();
        let slot = regex::Regex::new(r"\{[^}]*\}").unwrap();
        let mut seen = BTreeSet::new();
        let mut orphans = Vec::new();
        for f in &files {
            let src = std::fs::read_to_string(f).unwrap_or_default();
            for c in built.captures_iter(&src) {
                let template = &c[1];
                let literal = slot.replace_all(template, "");
                // Only names a model could be given: letters and `_` around
                // the slots (not paths, labels or email templates).
                if literal.is_empty()
                    || !literal
                        .chars()
                        .all(|ch| ch.is_ascii_lowercase() || ch == '_')
                {
                    continue;
                }
                let example = slot.replace_all(template, "x");
                match Family::of(&example) {
                    Some(fam) => {
                        seen.insert(format!("{fam:?}"));
                    }
                    None => orphans.push(format!("{}: {template}", f.display())),
                }
            }
        }
        assert!(
            orphans.is_empty(),
            "tool names built at run time with no family (and so no risk): {orphans:?}"
        );
        for f in Family::ALL {
            assert!(
                seen.contains(&format!("{f:?}")),
                "no code builds {f:?} tool names: is the family still used?"
            );
        }
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
