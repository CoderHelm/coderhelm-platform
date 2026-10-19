//! Checks a team's repository items against what the team really has in
//! Coderhelm, and keeps a translated version of each (see
//! `common::skill_compat` for the format and the checks on the answer).
//!
//! Runs after every sync and every sweep, and when an admin asks for a
//! re-check. An item is sent to the model only when its text or the team's
//! inventory changed since its last check, or when an admin asked: one call
//! per item, on the light model, counted as `skill_sync`.

use std::time::{Duration, Instant};

use common::skill_compat::{self, Analysis, Inventory, Server, Source, Tool};
use common::skills::{SkillMeta, SkillStore};
use futures::StreamExt;
use serde_json::Value;
use tracing::{info, warn};

use crate::models::TokenUsage;
use crate::WorkerState;

/// Items checked at once.
const PARALLEL: usize = 4;
/// No new check starts after this long, so a run stays well inside the
/// worker's 15 minutes; what's left is checked on the next sweep.
const BUDGET: Duration = Duration::from_secs(11 * 60);
const EFFORT: &str = "medium";
const MAX_TOKENS: i32 = 32_000;

/// Coderhelm's own tools, as an item can rely on them. A test checks each
/// one is a tool an agent is really given.
pub const BUILTIN: &[(&str, &str, bool)] = &[
    ("read_tree", "List every file in the repository", false),
    ("list_directory", "List one folder of the repository", false),
    ("read_file", "Read a file of the repository", false),
    ("read_file_lines", "Read some lines of a file", false),
    (
        "search_code",
        "Search the repository's code for text",
        false,
    ),
    (
        "write_file",
        "Write a file in the repository (lands in the pull request)",
        true,
    ),
    ("edit_file", "Edit part of a file in the repository", true),
    (
        "batch_write",
        "Write several files in the repository at once",
        true,
    ),
    (
        "restore_file",
        "Put a file back as it is on the base branch",
        true,
    ),
    (
        "get_diff",
        "Compare the working branch with the pull request's base branch",
        false,
    ),
    (
        "run_checks",
        "Run the repository's own build, type check, lint and tests in a sandbox (only the \
         commands from its build config; no other commands)",
        false,
    ),
    (
        "run_codegen",
        "Run the repository's own code generation in a sandbox",
        true,
    ),
    (
        "graph_definition",
        "Find where a symbol is defined (reviews)",
        false,
    ),
    (
        "graph_callers",
        "Find which files use a symbol (reviews)",
        false,
    ),
    (
        "graph_impact",
        "Find every file affected by a file's symbols (reviews)",
        false,
    ),
    (
        "search_org",
        "Search the team's other repositories for a symbol (reviews)",
        false,
    ),
    (
        "check_package",
        "Look a package version up on npm, crates.io or PyPI (reviews)",
        false,
    ),
    (
        "read_skill",
        "Read one of the team's skills, commands or agents",
        false,
    ),
    (
        "read_skill_file",
        "Read a file kept next to a team skill",
        false,
    ),
    (
        "ask_team_agent",
        "Ask one of the team's agents a question",
        false,
    ),
];

const GITHUB: &[&str] = &[
    "Read any file of the repository on the branch being worked on",
    "Compare the working branch with the pull request's base branch (get_diff)",
    "Open and update a pull request with its changes",
    "Answer review comments on its own pull requests",
    "It has no tool for commit history, blame, releases, deploy history or CI logs",
];

const NEVER: &[&str] = &[
    "Run shell commands or command line tools: there is no terminal (run_checks and run_codegen \
     run only the repository's own configured commands)",
    "Run git commands: it works through GitHub",
    "Read a person's computer or home folder (paths like ~/...)",
    "Write to outside systems (wikis, tickets, incidents, chat, dashboards) unless a ready MCP \
     server's tool can",
    "Browse the web",
];

/// Reads one item and answers in the check's JSON. The worker's is the
/// team's model; tests use a fake.
#[async_trait::async_trait]
pub trait Checker: Send + Sync {
    fn model(&self) -> &str;
    async fn check(
        &self,
        system: &str,
        user: &str,
        schema: &Value,
    ) -> (Result<Value, String>, TokenUsage);
}

struct TeamModel<'a> {
    provider: crate::agent::provider::ModelProvider,
    model: String,
    _state: &'a WorkerState,
}

#[async_trait::async_trait]
impl Checker for TeamModel<'_> {
    fn model(&self) -> &str {
        &self.model
    }

    async fn check(
        &self,
        system: &str,
        user: &str,
        schema: &Value,
    ) -> (Result<Value, String>, TokenUsage) {
        let mut usage = TokenUsage::default();
        let client = self.provider.client();
        let r = crate::agent::anthropic::converse_json(
            &client,
            &self.model,
            system,
            user,
            schema,
            EFFORT,
            MAX_TOKENS,
            &mut usage,
        )
        .await
        .map_err(|e| e.to_string());
        (r, usage)
    }
}

/// Checks one item: one model call, its answer checked against the
/// inventory. Returns the check and the translated text.
pub async fn check_item(
    checker: &dyn Checker,
    inv: &Inventory,
    m: &SkillMeta,
    body: &str,
) -> Result<(Analysis, String), String> {
    let started = Instant::now();
    let user = skill_compat::user_message(inv, m.kind.as_str(), &m.name, body, &m.files);
    let source_chars = common::skills::parse_front_matter(body)
        .2
        .trim()
        .chars()
        .count();
    let schema = skill_compat::output_schema();
    // A stub or malformed answer is asked again once; both calls count.
    let mut usage = TokenUsage::default();
    let mut result = Err(String::new());
    for _ in 0..2 {
        let (raw, spent) = checker.check(skill_compat::SYSTEM, &user, &schema).await;
        usage.merge(&spent);
        result = raw.and_then(|v| skill_compat::finish(&v, inv, source_chars));
        if result.is_ok() {
            break;
        }
    }
    let (mut a, translated) = result?;
    a.model = checker.model().to_string();
    a.input_tokens = usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens;
    a.output_tokens = usage.output_tokens;
    a.cost_usd = usage.cost_usd;
    a.ms = started.elapsed().as_millis() as u64;
    Ok((a, translated))
}

/// The items to check: those whose text or inventory changed since their
/// last check, or every one asked for when `force`. `only` limits it to one
/// path.
pub fn to_check<'a>(
    items: &'a [(SkillMeta, String)],
    fingerprint: &str,
    only: &str,
    force: bool,
) -> Vec<&'a (SkillMeta, String)> {
    items
        .iter()
        .filter(|(m, _)| only.is_empty() || m.path == only)
        .filter(|(m, _)| {
            skill_compat::needs_check(
                &m.hash,
                fingerprint,
                &m.compat.hash,
                &m.compat.fingerprint,
                force,
            )
        })
        .collect()
}

/// One item's path and checked text hash, and its check (`None` when the
/// run's time was up before it started).
type Outcome = (String, String, Option<Result<(Analysis, String), String>>);

/// Checks `repo`'s items that need it and keeps each result on its item.
/// `only` is one path, or empty for every item; `force` checks even
/// unchanged ones (an admin's re-check).
pub async fn run(state: &WorkerState, team_id: &str, repo: &str, only: &str, force: bool) {
    let call = run_inner(state, team_id, repo, only, force);
    common::usage::with_feature(common::usage::feature::SKILL_SYNC, call).await
}

async fn run_inner(state: &WorkerState, team_id: &str, repo: &str, only: &str, force: bool) {
    let st = SkillStore {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
        team: team_id,
    };
    let items = match st.compat_queue(repo).await {
        Ok(i) => i,
        Err(e) => {
            warn!(error = %e, "Skill check: couldn't read the items");
            return;
        }
    };
    let inv = inventory(state, team_id).await;
    let fp = inv.fingerprint();
    let todo = to_check(&items, &fp, only, force);
    if todo.is_empty() {
        return;
    }
    let provider = match crate::agent::provider::ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "Skill check: no model for this team; skipped");
            return;
        }
    };
    let checker = TeamModel {
        provider,
        model: state.config.light_model_id.clone(),
        _state: state,
    };
    let started = Instant::now();
    let total = todo.len();
    let results: Vec<Outcome> = futures::stream::iter(todo)
        .map(|(m, body)| {
            let (checker, inv) = (&checker, &inv);
            async move {
                if started.elapsed() > BUDGET {
                    return (m.path.clone(), m.hash.clone(), None);
                }
                let r = check_item(checker, inv, m, body).await;
                (m.path.clone(), m.hash.clone(), Some(r))
            }
        })
        .buffer_unordered(PARALLEL)
        .collect()
        .await;
    let mut done = 0usize;
    let mut cost = 0.0f64;
    for (path, hash, r) in results {
        let Some(r) = r else { continue };
        let kept = match &r {
            Ok((a, translated)) => {
                cost += a.cost_usd;
                info!(
                    team_id, repo, path = %path, status = %a.status, steps = a.counts.steps,
                    input_tokens = a.input_tokens, output_tokens = a.output_tokens,
                    cost_usd = a.cost_usd, ms = a.ms, "Skill checked"
                );
                st.put_compat(repo, &path, &hash, &fp, Ok((a, translated.as_str())))
                    .await
            }
            Err(e) => {
                warn!(team_id, repo, path = %path, error = %e, "Skill check failed");
                st.put_compat(repo, &path, &hash, &fp, Err(e.as_str()))
                    .await
            }
        };
        match kept {
            Ok(true) => done += 1,
            Ok(false) => {
                info!(path = %path, "Skill changed while it was checked; left for the next check")
            }
            Err(e) => warn!(error = %e, "Skill check couldn't be kept"),
        }
    }
    info!(
        team_id,
        repo,
        checked = done,
        asked = total,
        cost_usd = cost,
        "Skill checks done"
    );
}

/// Everything the team's items can rely on in Coderhelm right now.
pub async fn inventory(state: &WorkerState, team_id: &str) -> Inventory {
    let table = &state.config.settings_table_name;
    let builtin = BUILTIN
        .iter()
        .map(|(name, what, writes)| Tool {
            name: name.to_string(),
            what: what.to_string(),
            writes_repo: *writes,
        })
        .collect();

    let mcp_table = if state.config.mcp_configs_table_name.is_empty() {
        table.as_str()
    } else {
        state.config.mcp_configs_table_name.as_str()
    };
    let mut servers = Vec::new();
    for (id, ready) in plugin_states(state, mcp_table, team_id).await {
        let tools = if ready {
            crate::agent::mcp::load_tool_cache(&state.s3, &state.config.bucket_name, &id)
                .await
                .map(|c| {
                    c.tools
                        .iter()
                        .map(|t| format!("{id}__{}", t.name))
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        servers.push(Server {
            name: display_name(&id),
            what: if ready {
                "Switched on with its credentials saved; the planner and coder get its tools \
                 when the work links to an outside system"
                    .into()
            } else {
                "Switched on, but its credentials aren't saved, so agents don't get it".into()
            },
            id,
            ready,
            tools,
        });
    }
    let connectable = super::MCP_CATALOG
        .iter()
        .filter(|(id, _, _)| !servers.iter().any(|s| s.id == *id))
        .map(|(id, _, _)| format!("{id}: {}", display_name(id)))
        .collect();

    let mut sources = Vec::new();
    match common::live::store::list_sources(&state.dynamo, table, team_id).await {
        Ok(list) => {
            for s in list
                .into_iter()
                .filter(|s| s.enabled && s.status == "ready")
            {
                sources.push(source(&s));
            }
        }
        Err(e) => warn!(error = %e, "Skill check: couldn't list data sources"),
    }

    let mut team_tools = Vec::new();
    let tool_store = common::live::tools_store::ToolStore {
        dynamo: &state.dynamo,
        table,
        team: team_id,
    };
    match tool_store.list().await {
        Ok(list) => {
            for t in list
                .into_iter()
                .filter(|t| t.enabled && !t.approved.is_empty())
            {
                team_tools.push(Tool {
                    name: t.name,
                    what: "The team's own approved lookup on a data source".into(),
                    writes_repo: false,
                });
            }
        }
        Err(e) => warn!(error = %e, "Skill check: couldn't list the team's tools"),
    }

    Inventory {
        builtin,
        github: GITHUB.iter().map(|s| s.to_string()).collect(),
        servers,
        connectable,
        sources,
        team_tools,
        never: NEVER.iter().map(|s| s.to_string()).collect(),
    }
    .sorted()
}

/// How a data source shows in the inventory, with the tools that read it.
pub fn source(s: &common::live::store::Source) -> Source {
    use common::live::store::Kind;
    let only = if s.repos.is_empty() {
        String::new()
    } else {
        format!(" Only for: {}.", s.repos.join(", "))
    };
    let (kind, detail, read) = match s.kind {
        Kind::Api => (
            "api",
            "A GraphQL API, one read only query per lookup".to_string(),
            "live_lookup",
        ),
        Kind::Database => {
            let aws = s.aws.as_ref();
            let detail = match aws {
                Some(a) if a.engine == "redshift" => format!(
                    "A Redshift data warehouse, one SELECT per lookup{}",
                    if a.databases.is_empty() {
                        String::new()
                    } else {
                        format!(" (databases: {})", a.databases.join(", "))
                    }
                ),
                _ => "A PostgreSQL database, one SELECT per lookup".to_string(),
            };
            ("database", detail, "db_query")
        }
        Kind::Logs => {
            let groups = s
                .aws
                .as_ref()
                .map(|a| a.log_groups.iter().take(40).cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            (
                "logs",
                format!(
                    "CloudWatch log groups, searched with Logs Insights: {}",
                    groups.join(", ")
                ),
                "logs_search",
            )
        }
    };
    Source {
        kind: kind.into(),
        name: s.name.clone(),
        detail: format!("{detail}.{only}"),
        tools: vec!["live_describe".into(), read.into()],
    }
}

/// Each MCP server the team switched on, and whether its credentials are
/// saved (only then do agents get it).
async fn plugin_states(state: &WorkerState, table: &str, team_id: &str) -> Vec<(String, bool)> {
    use aws_sdk_dynamodb::types::AttributeValue;
    let r = state
        .dynamo
        .query()
        .table_name(table)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()))
        .expression_attribute_values(":p", AttributeValue::S("PLUGIN#".into()))
        .projection_expression("sk, enabled, has_credentials")
        .send()
        .await;
    let items = match r {
        Ok(o) => o.items().to_vec(),
        Err(e) => {
            warn!(error = %e, "Skill check: couldn't list MCP servers");
            return Vec::new();
        }
    };
    let flag = |i: &std::collections::HashMap<String, AttributeValue>, k: &str| {
        i.get(k)
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false)
    };
    items
        .iter()
        .filter(|i| flag(i, "enabled"))
        .filter_map(|i| {
            let id = i
                .get("sk")?
                .as_s()
                .ok()?
                .strip_prefix("PLUGIN#")?
                .to_string();
            Some((id, flag(i, "has_credentials")))
        })
        .collect()
}

/// A server's name from its id: `launchdarkly` gives `Launchdarkly`.
fn display_name(id: &str) -> String {
    let mut c = id.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::skills::Kind;
    use serde_json::json;

    struct Fake(Value);

    #[async_trait::async_trait]
    impl Checker for Fake {
        fn model(&self) -> &str {
            "fake-light"
        }
        async fn check(
            &self,
            system: &str,
            user: &str,
            schema: &Value,
        ) -> (Result<Value, String>, TokenUsage) {
            assert!(system.contains("Read the WHOLE text"));
            assert!(user.contains("## Coderhelm inventory"));
            assert!(user.contains("skill `triage`"));
            assert_eq!(schema["required"], json!(["steps", "translated"]));
            let usage = TokenUsage {
                input_tokens: 1000,
                output_tokens: 500,
                cost_usd: 0.01,
                ..Default::default()
            };
            (Ok(self.0.clone()), usage)
        }
    }

    fn meta(path: &str, hash: &str, kept_hash: &str, kept_fp: &str) -> SkillMeta {
        SkillMeta {
            repo: "acme/playbook".into(),
            path: path.into(),
            kind: Kind::Skill,
            name: "triage".into(),
            description: String::new(),
            sha: "s".into(),
            hash: hash.into(),
            enabled: true,
            files: vec![],
            missing_tools: vec![],
            uses: 0,
            last_used_at: String::new(),
            last_used_by: String::new(),
            updated_at: String::new(),
            asks: 0,
            last_asked_at: String::new(),
            switched_by: String::new(),
            switched_at: String::new(),
            compat: common::skills::CompatMeta {
                hash: kept_hash.into(),
                fingerprint: kept_fp.into(),
                ..Default::default()
            },
        }
    }

    fn inv() -> Inventory {
        Inventory {
            builtin: BUILTIN
                .iter()
                .map(|(n, w, r)| Tool {
                    name: n.to_string(),
                    what: w.to_string(),
                    writes_repo: *r,
                })
                .collect(),
            sources: vec![Source {
                kind: "logs".into(),
                name: "Service logs".into(),
                detail: String::new(),
                tools: vec!["live_describe".into(), "logs_search".into()],
            }],
            ..Default::default()
        }
        .sorted()
    }

    #[tokio::test]
    async fn a_check_is_one_model_call_checked_and_counted() {
        let answer = json!({
            "steps": [
                { "title": "Logs", "does": "Look at the service logs", "dropped": false,
                  "requirements": [{ "asks": "CloudWatch", "kind": "data", "status": "translated",
                    "uses": ["logs_search"], "write": "none", "needs": "", "fix": "", "connect": "none" }] },
                { "title": "Shell", "does": "Run a local doctor command", "dropped": true,
                  "requirements": [{ "asks": "a local CLI", "kind": "command", "status": "missing",
                    "uses": [], "write": "none", "needs": "Shell access", "fix": "Coderhelm has no terminal.", "connect": "none" }] }
            ],
            "translated": "1. Search the logs with `logs_search` on Service logs.\nGAP: Shell — run the doctor — not connected in Coderhelm"
        });
        let m = meta("skills/triage/SKILL.md", "h1", "", "");
        let (a, translated) = check_item(&Fake(answer), &inv(), &m, "body").await.unwrap();
        assert_eq!(a.status, "needs_tools");
        assert_eq!(
            a.summary,
            "0 of 2 steps run as written · 1 translated · 1 needs Shell access"
        );
        assert_eq!(a.model, "fake-light");
        assert_eq!((a.input_tokens, a.output_tokens), (1000, 500));
        assert!((a.cost_usd - 0.01).abs() < 1e-9);
        assert!(a.steps[1].dropped);
        assert!(translated.contains("GAP: Shell"));
    }

    #[tokio::test]
    async fn a_bad_answer_is_an_error_not_a_check() {
        let m = meta("skills/triage/SKILL.md", "h1", "", "");
        let err = check_item(&Fake(json!({"steps": "no"})), &inv(), &m, "body")
            .await
            .unwrap_err();
        assert!(err.contains("expected shape"), "{err}");
    }

    /// Answers a stub first, then a real check.
    struct StubThenReal(std::sync::atomic::AtomicUsize, Value);

    #[async_trait::async_trait]
    impl Checker for StubThenReal {
        fn model(&self) -> &str {
            "fake-light"
        }
        async fn check(&self, _: &str, _: &str, _: &Value) -> (Result<Value, String>, TokenUsage) {
            let n = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let usage = TokenUsage {
                input_tokens: 100,
                output_tokens: 10,
                ..Default::default()
            };
            let stub = json!({ "steps": [{ "title": "x", "does": "", "dropped": false, "requirements": [] }], "translated": "x" });
            (Ok(if n == 0 { stub } else { self.1.clone() }), usage)
        }
    }

    #[tokio::test]
    async fn a_stub_answer_is_asked_again_once_and_both_calls_count() {
        let real = json!({
            "steps": [{ "title": "Read the rules", "does": "Read them", "dropped": false, "requirements": [] }],
            "translated": "Read the rules before changing the code, every time."
        });
        let fake = StubThenReal(Default::default(), real);
        let m = meta("skills/triage/SKILL.md", "h1", "", "");
        let (a, _) = check_item(&fake, &inv(), &m, "Read the rules first.")
            .await
            .unwrap();
        assert_eq!(a.status, "ready");
        assert_eq!((a.input_tokens, a.output_tokens), (200, 20));
        assert_eq!(fake.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    fn only_changed_items_are_checked_unless_forced() {
        let items = vec![
            (meta("a", "h1", "h1", "fp"), String::new()),
            (meta("b", "h2", "old", "fp"), String::new()),
            (meta("c", "h3", "h3", "old-fp"), String::new()),
            (meta("d", "h4", "", ""), String::new()),
        ];
        let paths = |v: Vec<&(SkillMeta, String)>| {
            v.iter().map(|(m, _)| m.path.clone()).collect::<Vec<_>>()
        };
        assert_eq!(paths(to_check(&items, "fp", "", false)), ["b", "c", "d"]);
        assert_eq!(
            paths(to_check(&items, "fp", "", true)),
            ["a", "b", "c", "d"]
        );
        assert_eq!(paths(to_check(&items, "fp", "a", true)), ["a"]);
        assert!(to_check(&items, "fp", "a", false).is_empty());
    }

    /// Every tool the inventory names is one an agent is really given.
    #[test]
    fn every_builtin_is_a_real_tool() {
        let mut real: Vec<String> = super::super::implement::tool_names();
        real.extend(
            [
                "run_checks",
                "run_codegen",
                "graph_definition",
                "graph_callers",
                "graph_impact",
                "search_org",
                "check_package",
                "read_skill",
                "read_skill_file",
                "ask_team_agent",
            ]
            .map(String::from),
        );
        for (name, _, _) in BUILTIN {
            assert!(
                real.iter().any(|r| r == name),
                "{name} isn't a tool any agent has"
            );
        }
    }

    #[test]
    fn server_names_read_well() {
        assert_eq!(display_name("sentry"), "Sentry");
        assert_eq!(display_name(""), "");
    }
}
