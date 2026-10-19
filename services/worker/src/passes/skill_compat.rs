//! Checks a team's repository items against what the team really has in
//! Coderhelm, and keeps a translated version of each (see
//! `common::skill_compat` for the format and the checks on the answer).
//!
//! Runs after every sync and every sweep, and when an admin asks for a
//! re-check. An item is sent to the model only when its text or the team's
//! inventory changed since its last check, or when an admin asked: one call
//! per item, on the light model, counted as `skill_sync`. A check of the same
//! text against the same tools is held to the last one
//! (`skill_compat::stabilize`), so a re-check never flips a step on the
//! model's whim.

use std::time::{Duration, Instant};

use common::skill_compat::{self, Analysis, Inventory, Server, Source, Tool, Translation};
use common::skills::{Queued, SkillMeta, SkillStore};
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
/// After an unusable answer, the one more try thinks harder.
const RETRY_EFFORT: &str = "high";

/// How each try asks: its effort, and whether the API holds the answer to
/// the schema. Neither is: on real items about a quarter of the answers
/// held to the schema came back as placeholders (one step titled "x" or
/// "placeholder", no translation), while unconstrained answers were full.
/// The schema is in the instructions, and `finish` checks the answer.
pub const TRIES: [(&str, bool); 2] = [(EFFORT, false), (RETRY_EFFORT, false)];
const MAX_TOKENS: i32 = 32_000;

/// Tools Coderhelm's agents have beside the coding agent's own list (which
/// is read live from the coder, so a tool added there shows up here).
const EXTRA_TOOLS: &[(&str, &str)] = &[
    (
        "graph_definition",
        "Find where a symbol is defined (reviews)",
    ),
    ("graph_callers", "Find which files use a symbol (reviews)"),
    (
        "graph_impact",
        "Find every file affected by a file's symbols (reviews)",
    ),
    (
        "search_org",
        "Search the team's other repositories for a symbol (reviews)",
    ),
    (
        "check_package",
        "Look a package version up on npm, crates.io or PyPI (reviews)",
    ),
    (
        "read_skill",
        "Read one of the team's skills, commands or agents",
    ),
    ("read_skill_file", "Read a file kept next to a team skill"),
    ("ask_team_agent", "Ask one of the team's agents a question"),
    (
        super::github_history::TOOL,
        "Read the repository's history on GitHub (git log, git blame, gh pr list, gh release): \
         commits, blame, pull requests, releases, tags, deployments and Actions runs",
    ),
];

/// Tools that change the repository (in the pull request).
const WRITES_REPO: &[&str] = &[
    "write_file",
    "edit_file",
    "batch_write",
    "restore_file",
    "run_codegen",
];

/// Commands a skill may name that Coderhelm does another way, and the tools
/// that do it. Each is offered to the check only when the team's inventory
/// has every tool it names, so a tool that ships later turns its commands
/// into translated steps on the next check.
const TRANSLATIONS: &[(&str, &[&str], &str)] = &[
    (
        "git clone, git checkout, cat, head, tail, ls, find, tree",
        &["read_tree", "list_directory", "read_file"],
        "Coderhelm already has the repository: it reads its files through the GitHub API, no clone needed",
    ),
    (
        "grep, grep -r, rg, git grep",
        &["search_code"],
        "Searches the repository's code with Coderhelm's code search",
    ),
    (
        "git diff, gh pr diff, the files a pull request changes",
        &["get_diff"],
        "Compares the working branch with the pull request's base through GitHub",
    ),
    (
        "git log, git blame, git show, git tag, gh pr list, gh pr view, gh release list, gh run list, gh api repos/... (reads)",
        &["github_history"],
        "Reads commits, blame, pull requests, releases, tags, deployments and workflow runs through the GitHub API",
    ),
    (
        "npm view, npm info, pip index versions, cargo search",
        &["check_package"],
        "Looks the package up on its registry",
    ),
];

/// The first sentence of a tool's description, for the inventory.
fn first_sentence(text: &str) -> String {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let end = flat
        .find(". ")
        .map(|i| i + 1)
        .unwrap_or(flat.len())
        .min(200);
    common::truncate_str(&flat, end).to_string()
}

/// Coderhelm's own tools as an item can rely on them: the coding agent's
/// real tool list, then the other agents' tools.
pub fn builtin_tools() -> Vec<Tool> {
    let mut out: Vec<Tool> = super::implement::coder_tools()
        .into_iter()
        .map(|t| Tool {
            writes_repo: WRITES_REPO.contains(&t.name.as_str()),
            what: first_sentence(&t.description),
            name: t.name,
        })
        .collect();
    for (name, what) in EXTRA_TOOLS {
        if !out.iter().any(|t| t.name == *name) {
            out.push(Tool {
                name: name.to_string(),
                what: what.to_string(),
                writes_repo: false,
            });
        }
    }
    out
}

/// The translations whose tools are all in `tools`.
pub fn translations(tools: &[Tool]) -> Vec<Translation> {
    TRANSLATIONS
        .iter()
        .filter(|(_, uses, _)| uses.iter().all(|u| tools.iter().any(|t| t.name == *u)))
        .map(|(commands, uses, note)| Translation {
            commands: commands.to_string(),
            uses: uses.iter().map(|u| u.to_string()).collect(),
            note: note.to_string(),
        })
        .collect()
}

const GITHUB: &[&str] = &[
    "Read any file of the repository on the branch being worked on",
    "Compare the working branch with the pull request's base branch (get_diff)",
    "Open and update a pull request with its changes",
    "Answer review comments on its own pull requests",
    "Read the repository's history (github_history): commits, blame, pull requests, releases, \
     tags, deployments and Actions runs",
];

const NEVER: &[&str] = &[
    "Run arbitrary shell commands: there is no terminal (run_checks and run_codegen run only the \
     repository's own configured commands; the reads listed under translations are done another way)",
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
    /// `attempt` is which of [`TRIES`] this is.
    async fn check(
        &self,
        system: &str,
        user: &str,
        schema: &Value,
        attempt: usize,
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
        attempt: usize,
    ) -> (Result<Value, String>, TokenUsage) {
        let mut usage = TokenUsage::default();
        let client = self.provider.client();
        let (effort, constrained) = TRIES[attempt.min(TRIES.len() - 1)];
        let r = crate::agent::anthropic::converse_json(
            &client,
            &self.model,
            system,
            user,
            schema,
            constrained,
            effort,
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
    // An unusable answer is asked again once, told what was wrong; if that
    // one is unusable too, a partial check is kept. Both calls count.
    let mut usage = TokenUsage::default();
    let mut result: Result<(Analysis, String), String> = Err(String::new());
    let mut unusable = String::new();
    for (attempt, (effort, _)) in TRIES.iter().enumerate() {
        let ask = if unusable.is_empty() {
            user.clone()
        } else {
            format!("{user}{}", skill_compat::retry_note(&unusable))
        };
        let (raw, spent) = checker
            .check(skill_compat::SYSTEM, &ask, &schema, attempt)
            .await;
        usage.merge(&spent);
        result = match raw {
            Ok(v) => skill_compat::finish(&v, inv, source_chars).inspect_err(|e| {
                unusable = e.clone();
            }),
            Err(e) => Err(e),
        };
        match &result {
            Ok((a, _)) => {
                if !a.notes.is_empty() {
                    info!(path = %m.path, notes = ?a.notes, "Skill check answer fixed up");
                }
                break;
            }
            Err(e) => warn!(path = %m.path, effort, error = %e, "Skill check answer unusable"),
        }
    }
    if result.is_err() && !unusable.is_empty() {
        result = Ok((skill_compat::partial(&unusable), String::new()));
    }
    let (mut a, translated) = result?;
    a.model = checker.model().to_string();
    a.input_tokens = usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens;
    a.output_tokens = usage.output_tokens;
    a.cost_usd = usage.cost_usd;
    a.ms = started.elapsed().as_millis() as u64;
    Ok((a, translated))
}

/// A new check of `item`, held to its last check when that was of the same
/// text (see `skill_compat::stabilize`).
pub fn hold_to_last(
    item: &Queued,
    inv: &Inventory,
    (a, translated): (Analysis, String),
) -> (Analysis, String) {
    let (m, _, kept) = item;
    match kept {
        Some((last, last_translated)) if m.compat.hash == m.hash => {
            let tools_changed = !skill_compat::same_tools(inv, &m.compat.fingerprint, last.format);
            let last = skill_compat::Last {
                analysis: last,
                translated: last_translated,
                tools_changed,
            };
            skill_compat::stabilize(a, translated, &last, inv)
        }
        _ => (a, translated),
    }
}

/// The items to check: those whose text or inventory changed since their
/// last check, or every one asked for when `force`. `only` limits it to one
/// path.
pub fn to_check<'a>(
    items: &'a [Queued],
    fingerprint: &str,
    only: &str,
    force: bool,
) -> Vec<&'a Queued> {
    items
        .iter()
        .filter(|(m, _, _)| only.is_empty() || m.path == only)
        .filter(|(m, _, _)| {
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

/// One item's path, checked text hash, whether its last check was of other
/// text or another inventory, and its check (`None` when the run's time
/// was up before it started).
type Outcome = (
    String,
    String,
    bool,
    Option<Result<(Analysis, String), String>>,
);

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
        .map(|item| {
            let (m, body, _) = item;
            let (checker, inv) = (&checker, &inv);
            let outdated = m.compat.hash != m.hash || m.compat.fingerprint != fp;
            async move {
                if started.elapsed() > BUDGET {
                    return (m.path.clone(), m.hash.clone(), outdated, None);
                }
                let r = check_item(checker, inv, m, body)
                    .await
                    .map(|done| hold_to_last(item, inv, done));
                (m.path.clone(), m.hash.clone(), outdated, Some(r))
            }
        })
        .buffer_unordered(PARALLEL)
        .collect()
        .await;
    let mut done = 0usize;
    let mut cost = 0.0f64;
    for (path, hash, outdated, r) in results {
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
                st.put_compat(repo, &path, &hash, &fp, Err((e.as_str(), outdated)))
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
    let builtin = builtin_tools();
    let translations = translations(&builtin);

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
    sources.extend(super::ops_tools::inventory_sources(state, team_id).await);

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
        translations,
        connections: super::ops_tools::connectable(state, team_id).await,
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
        writes: Vec::new(),
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
            attempt: usize,
        ) -> (Result<Value, String>, TokenUsage) {
            assert!(attempt < TRIES.len());
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
            always: Default::default(),
            suggestion: Default::default(),
        }
    }

    fn inv() -> Inventory {
        Inventory {
            translations: translations(&builtin_tools()),
            builtin: builtin_tools(),
            sources: vec![Source {
                kind: "logs".into(),
                name: "Service logs".into(),
                detail: String::new(),
                tools: vec!["live_describe".into(), "logs_search".into()],
                writes: Vec::new(),
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
    async fn an_answer_unusable_twice_is_kept_as_a_partial_check() {
        let m = meta("skills/triage/SKILL.md", "h1", "", "");
        let (a, translated) = check_item(&Fake(json!({"steps": "no"})), &inv(), &m, "body")
            .await
            .unwrap();
        assert_eq!(a.status, "partial");
        assert!(a.notes[0].contains("expected shape"), "{:?}", a.notes);
        assert!(translated.is_empty());
        assert_eq!(
            (a.input_tokens, a.output_tokens),
            (2000, 1000),
            "both tries count"
        );
    }

    struct Down;

    #[async_trait::async_trait]
    impl Checker for Down {
        fn model(&self) -> &str {
            "fake-light"
        }
        async fn check(
            &self,
            _: &str,
            _: &str,
            _: &Value,
            _: usize,
        ) -> (Result<Value, String>, TokenUsage) {
            (
                Err("Anthropic API error (500)".into()),
                TokenUsage::default(),
            )
        }
    }

    #[tokio::test]
    async fn a_model_that_cant_be_reached_is_an_error_not_a_check() {
        let m = meta("skills/triage/SKILL.md", "h1", "", "");
        let err = check_item(&Down, &inv(), &m, "body").await.unwrap_err();
        assert!(err.contains("500"));
    }

    /// Answers a stub first, then a real check, keeping what it was asked.
    struct StubThenReal(
        std::sync::atomic::AtomicUsize,
        Value,
        std::sync::Mutex<Vec<String>>,
    );

    #[async_trait::async_trait]
    impl Checker for StubThenReal {
        fn model(&self) -> &str {
            "fake-light"
        }
        async fn check(
            &self,
            _: &str,
            user: &str,
            _: &Value,
            attempt: usize,
        ) -> (Result<Value, String>, TokenUsage) {
            self.2.lock().unwrap().push(user.to_string());
            let n = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(attempt, n);
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
        let fake = StubThenReal(Default::default(), real, Default::default());
        let m = meta("skills/triage/SKILL.md", "h1", "", "");
        let (a, _) = check_item(&fake, &inv(), &m, "Read the rules first.")
            .await
            .unwrap();
        assert_eq!(a.status, "ready");
        assert_eq!((a.input_tokens, a.output_tokens), (200, 20));
        assert_eq!(fake.0.load(std::sync::atomic::Ordering::SeqCst), 2);
        let asked = fake.2.lock().unwrap();
        assert!(!asked[0].contains("couldn't be used"));
        assert!(
            asked[1]
                .contains("Your last answer couldn't be used: The answer listed no usable step."),
            "the second try is told what was wrong"
        );
    }

    #[tokio::test]
    async fn read_commands_are_translated_to_coderhelms_github_backed_tools() {
        // A skill written for a laptop, with the read commands it relies on.
        let body = "## Steps\n1. `git clone` the repo and `cat src/app.ts`.\n2. `grep -r TODO src`.\n\
                    3. `git diff main`.\n4. `npm view react version`.\n5. `rm -rf build && make`.\n";
        let i = inv();
        let commands: Vec<&str> = i.translations.iter().map(|t| t.commands.as_str()).collect();
        assert!(commands.iter().any(|c| c.contains("git clone")));
        assert!(commands.iter().any(|c| c.starts_with("grep")));
        assert!(commands.iter().any(|c| c.starts_with("git diff")));
        assert!(commands.iter().any(|c| c.starts_with("npm view")));
        // The history tool is a built-in, so git log is offered as translated.
        assert!(commands.iter().any(|c| c.starts_with("git log")));
        let mut with_history = builtin_tools();
        with_history.push(Tool {
            name: "github_history".into(),
            what: "Read the repository's history".into(),
            writes_repo: false,
        });
        assert!(translations(&with_history)
            .iter()
            .any(|t| t.commands.starts_with("git log") && t.uses == ["github_history"]));

        let req = |asks: &str, status: &str, uses: &[&str]| {
            json!({ "asks": asks, "kind": "command", "status": status, "uses": uses, "write": "none",
                    "needs": if status == "missing" { "Shell access" } else { "" },
                    "fix": if status == "missing" { "Coderhelm has no terminal." } else { "" },
                    "connect": "none", "server": "" })
        };
        let answer = json!({
            "steps": [
                { "title": "Read the code", "does": "Read the app file", "dropped": false,
                  "requirements": [req("git clone, cat", "translated", &["read_file"])] },
                { "title": "Find TODOs", "does": "Search for TODO", "dropped": false,
                  "requirements": [req("grep -r", "translated", &["search_code"])] },
                { "title": "See the change", "does": "Diff against main", "dropped": false,
                  "requirements": [req("git diff", "translated", &["get_diff"])] },
                { "title": "Check the version", "does": "Look up react", "dropped": false,
                  "requirements": [req("npm view", "translated", &["check_package"])] },
                { "title": "Rebuild", "does": "Clean and build locally", "dropped": true,
                  "requirements": [req("rm -rf build && make", "missing", &[])] }
            ],
            "translated": "1. Read src/app.ts with `read_file` (uses the GitHub API instead of git clone).\n\
                           2. Search for TODO with `search_code`.\n3. Compare with `get_diff`.\n\
                           4. Look react up with `check_package`.\nGAP: Rebuild — clean and build — not possible in Coderhelm"
        });
        let m = meta("skills/triage/SKILL.md", "h1", "", "");
        let (a, translated) = check_item(&Fake(answer), &i, &m, body).await.unwrap();
        let st: Vec<&str> = a.steps.iter().map(|s| s.status.as_str()).collect();
        assert_eq!(
            st,
            [
                "translated",
                "translated",
                "translated",
                "translated",
                "missing"
            ]
        );
        assert_eq!(
            a.summary,
            "0 of 5 steps run as written · 4 translated · 1 needs Shell access"
        );
        assert_eq!(
            a.steps[4].requirements[0].action, "none",
            "nothing to connect for a shell"
        );
        assert!(translated.contains("instead of git clone"));
    }

    #[test]
    fn only_changed_items_are_checked_unless_forced() {
        let items: Vec<Queued> = vec![
            (meta("a", "h1", "h1", "fp"), String::new(), None),
            (meta("b", "h2", "old", "fp"), String::new(), None),
            (meta("c", "h3", "h3", "old-fp"), String::new(), None),
            (meta("d", "h4", "", ""), String::new(), None),
        ];
        let paths = |v: Vec<&Queued>| v.iter().map(|(m, _, _)| m.path.clone()).collect::<Vec<_>>();
        assert_eq!(paths(to_check(&items, "fp", "", false)), ["b", "c", "d"]);
        assert_eq!(
            paths(to_check(&items, "fp", "", true)),
            ["a", "b", "c", "d"]
        );
        assert_eq!(paths(to_check(&items, "fp", "a", true)), ["a"]);
        assert!(to_check(&items, "fp", "a", false).is_empty());
    }

    /// The inventory reads the coder's real tools, and every translation
    /// names tools the inventory knows about.
    #[test]
    fn the_inventory_reads_the_real_tool_list() {
        let tools = builtin_tools();
        for name in super::super::implement::tool_names() {
            assert!(tools.iter().any(|t| t.name == name), "{name} is missing");
        }
        assert!(tools.iter().any(|t| t.name == "run_checks"));
        assert!(
            tools
                .iter()
                .find(|t| t.name == "write_file")
                .unwrap()
                .writes_repo
        );
        assert!(
            !tools
                .iter()
                .find(|t| t.name == "read_file")
                .unwrap()
                .writes_repo
        );
        assert!(tools
            .iter()
            .all(|t| !t.what.is_empty() && t.what.len() <= 200));
        let known: Vec<&str> = tools
            .iter()
            .map(|t| t.name.as_str())
            .chain(["github_history"])
            .collect();
        for (_, uses, _) in TRANSLATIONS {
            for u in *uses {
                assert!(known.contains(u), "{u} isn't a tool");
            }
        }
        assert_eq!(
            first_sentence("Read a file. Large files are cut."),
            "Read a file."
        );
    }

    #[test]
    fn server_names_read_well() {
        assert_eq!(display_name("sentry"), "Sentry");
        assert_eq!(display_name(""), "");
    }

    /// A model that answers the same item differently on every call, in
    /// the borderline ways a real one does: it leaves talking to the user
    /// out, files it as a person, or calls it a missing tool; it names the
    /// history read by its command but calls it missing or picks the wrong
    /// tool; it calls a vague "shell" missing one time and translated the
    /// next; it marks a laptop path readable.
    struct Wavering(std::sync::atomic::AtomicUsize);

    fn wavering_answer(n: usize) -> Value {
        let q = |asks: &str, kind: &str, status: &str, uses: &[&str], needs: &str| {
            json!({ "asks": asks, "kind": kind, "status": status, "uses": uses, "write": "none",
                    "needs": needs, "fix": if needs.is_empty() { "" } else { "Not here." },
                    "connect": "none", "server": "", "connection": "" })
        };
        let step = |title: &str, reqs: Vec<Value>| json!({ "title": title, "does": format!("{title} (answer {n})"), "dropped": false, "requirements": reqs });
        let v = n % 3;
        let ask_user = match v {
            0 => vec![],
            1 => vec![q(
                "ask the user which version",
                "person",
                "available",
                &[],
                "",
            )],
            _ => vec![q("ask the user", "other", "missing", &[], "User input")],
        };
        let commit = match v {
            0 => vec![q(
                "suggest a git commit message",
                "person",
                "available",
                &[],
                "",
            )],
            1 => vec![q("git commit", "command", "missing", &[], "Shell access")],
            _ => vec![q("suggest a commit", "other", "missing", &[], "Git")],
        };
        let history = match v {
            0 => vec![q(
                "git log --oneline",
                "command",
                "translated",
                &["github_history"],
                "",
            )],
            1 => vec![q(
                "git log --oneline",
                "command",
                "missing",
                &[],
                "Shell access",
            )],
            _ => vec![q(
                "`git log --oneline`",
                "command",
                "translated",
                &["read_file"],
                "",
            )],
        };
        let shell = match v {
            0 => vec![q("shell", "command", "missing", &[], "Shell access")],
            1 => vec![q("shell", "command", "translated", &["search_code"], "")],
            _ => vec![q("shell", "command", "missing", &[], "A terminal")],
        };
        let notes = match v {
            0 => vec![q(
                "~/notes/release.md",
                "path",
                "missing",
                &[],
                "Local files",
            )],
            1 => vec![q(
                "~/notes/release.md",
                "path",
                "available",
                &["read_file"],
                "",
            )],
            _ => vec![q(
                "~/notes/release.md",
                "path",
                "missing",
                &[],
                "The laptop",
            )],
        };
        json!({
            "steps": [
                step("Ask the user", ask_user),
                step("Suggest a commit", commit),
                step("Read the history", history),
                step("Count merges", shell),
                step("Read the release notes", notes),
            ],
            "translated": format!("1. Ask the user which version.\n2. Read the history with github_history.\n(answer {n})")
        })
    }

    #[async_trait::async_trait]
    impl Checker for Wavering {
        fn model(&self) -> &str {
            "fake-light"
        }
        async fn check(
            &self,
            _: &str,
            _: &str,
            _: &Value,
            _: usize,
        ) -> (Result<Value, String>, TokenUsage) {
            let n = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let usage = TokenUsage {
                input_tokens: 10 + n as u64,
                output_tokens: 5,
                ..Default::default()
            };
            (Ok(wavering_answer(n)), usage)
        }
    }

    /// What's kept on an item, without what differs on every call anyway
    /// (tokens, cost, time).
    fn stored(a: &Analysis, translated: &str) -> String {
        let mut a = a.clone();
        (a.input_tokens, a.output_tokens, a.cost_usd, a.ms) = (0, 0, 0.0, 0);
        format!("{}\n{translated}", serde_json::to_string(&a).unwrap())
    }

    /// Checks `item` and keeps the result on it, as a run does.
    async fn check_and_keep(checker: &dyn Checker, inv: &Inventory, item: &mut Queued) -> String {
        let (m, body, _) = item.clone();
        let done = check_item(checker, inv, &m, &body).await.unwrap();
        let (a, translated) = hold_to_last(item, inv, done);
        item.0.compat.hash = m.hash.clone();
        item.0.compat.fingerprint = inv.fingerprint();
        let out = stored(&a, &translated);
        item.2 = Some((a, translated));
        out
    }

    const BODY: &str = "Bump the version: ask the user which version, read the history, count merges with a shell, read ~/notes/release.md, suggest a commit.";

    #[tokio::test]
    async fn five_checks_of_the_same_item_keep_the_same_result() {
        let i = inv();
        let fake = Wavering(Default::default());
        // Checked under the last format, then re-checked five times after
        // the format bump with the text and tools unchanged.
        let mut item: Queued = (
            meta("skills/bump/SKILL.md", "h1", "", ""),
            BODY.into(),
            None,
        );
        let first = check_and_keep(&fake, &i, &mut item).await;
        if let Some((a, _)) = item.2.as_mut() {
            a.format = skill_compat::FORMAT - 1;
        }
        item.0.compat.fingerprint = i.fingerprint_at(skill_compat::FORMAT - 1);
        let mut runs = Vec::new();
        for _ in 0..5 {
            runs.push(check_and_keep(&fake, &i, &mut item).await);
        }
        for (n, r) in runs.iter().enumerate() {
            assert_eq!(r, &first, "re-check {} differs from the first check", n + 1);
        }
        let (a, _) = item.2.as_ref().unwrap();
        let st: Vec<(&str, &str)> = a
            .steps
            .iter()
            .map(|s| (s.title.as_str(), s.status.as_str()))
            .collect();
        assert_eq!(
            st,
            [
                ("Ask the user", "available"),
                ("Suggest a commit", "available"),
                ("Read the history", "translated"),
                ("Count merges", "missing"),
                ("Read the release notes", "missing"),
            ]
        );
        assert!(a.notes.is_empty(), "nothing flipped: {:?}", a.notes);
        assert_eq!(
            fake.0.load(std::sync::atomic::Ordering::SeqCst),
            6,
            "every check asked the model"
        );
    }

    #[tokio::test]
    async fn what_the_inventory_settles_is_the_same_on_every_answer() {
        let i = inv();
        let fake = Wavering(Default::default());
        let m = meta("skills/bump/SKILL.md", "h1", "", "");
        let mut seen = Vec::new();
        for _ in 0..5 {
            let (a, _) = check_item(&fake, &i, &m, BODY).await.unwrap();
            let settled: Vec<(String, String, Vec<String>)> = a.steps[2..]
                .iter()
                .filter(|s| s.title != "Count merges")
                .map(|s| {
                    (
                        s.title.clone(),
                        s.status.clone(),
                        s.requirements[0].uses.clone(),
                    )
                })
                .collect();
            seen.push(settled);
        }
        assert!(seen.windows(2).all(|w| w[0] == w[1]), "{seen:?}");
        assert_eq!(seen[0][0].2, ["github_history"]);
    }

    #[tokio::test]
    async fn a_bad_first_check_is_corrected_once_per_step_with_a_note_then_stays() {
        let i = inv();
        // Starts on the answer that calls asking the user a missing tool.
        let fake = Wavering(std::sync::atomic::AtomicUsize::new(2));
        let mut item: Queued = (
            meta("skills/bump/SKILL.md", "h1", "", ""),
            BODY.into(),
            None,
        );
        check_and_keep(&fake, &i, &mut item).await;
        assert_eq!(item.2.as_ref().unwrap().0.steps[0].status, "missing");
        // The next answer files the commit suggestion as talking to the
        // person, which the inventory settles; asking the user it leaves
        // out (the model's judgment), so that step is held.
        check_and_keep(&fake, &i, &mut item).await;
        let (a, _) = item.2.as_ref().unwrap();
        assert_eq!(
            (a.steps[0].status.as_str(), a.steps[1].status.as_str()),
            ("missing", "available")
        );
        // The one after files asking the user as talking to the person too.
        let fixed = check_and_keep(&fake, &i, &mut item).await;
        let (a, _) = item.2.as_ref().unwrap();
        assert_eq!(a.steps[0].status, "available");
        assert_eq!(a.notes.len(), 2, "{:?}", a.notes);
        assert!(
            a.notes
                .iter()
                .any(|n| n.starts_with("Step \"Ask the user\" changed from missing to available")),
            "{:?}",
            a.notes
        );
        for _ in 0..4 {
            assert_eq!(check_and_keep(&fake, &i, &mut item).await, fixed);
        }
    }
}
