//! bm. An audit of every way an event reaches CoderHelm or leaves it found
//! four places where one team's data could reach another team, each through
//! a key that named a shared thing (a Slack workspace, a repository name, a
//! Jira ticket key, a run id) instead of the team:
//!
//! - Any team admin could point a Slack workspace id at their own team
//!   (the chat index was overwritten), taking another team's slash commands
//!   and cutting that team off.
//! - The run page's GitHub check reads were cached by repository name only:
//!   a team asking about a repository with the same name was served another
//!   installation's cached pull request head, checks and review asks.
//! - Replies the model wrote were cached by `JIRA#<ticket>` and comment id:
//!   two Jira sites with ticket `ABC-1` and the same comment id could get one
//!   another's reply.
//! - A run's events (CI results, live tool calls) were keyed by run id only,
//!   with no team on the row.
//!
//! Guarantee: every inbound event path resolves its team from something
//! only that team holds (a signed installation, a per-team token or secret,
//! a claim another team cannot overwrite) and drops an event no team owns;
//! a shared identifier is claimed by one team at a time; every process-wide
//! cache is keyed by the credential or team that read it; every run event
//! row names its team and every read keeps only the asking team's rows. The
//! scans below fail when a new inbound route, a new cache, or a direct
//! events table access is added without saying how it stays in its team.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use common::plugins::flag::{self, TeamSettings};
use common::plugins::kv::Memory;

use crate::team_installation::{claims, decide, single_team, Verdict};

const TEAM_A: &str = "TEAM#a";
const TEAM_B: &str = "TEAM#b";

fn services() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("services dir")
        .to_path_buf()
}

fn read(rel: &str) -> String {
    std::fs::read_to_string(services().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

/// Every `.rs` file under `dir` (relative to services/), tests and the
/// incident cases left out.
fn sources(dir: &str) -> Vec<(String, String)> {
    fn walk(root: &Path, p: &Path, out: &mut Vec<(String, String)>) {
        let Ok(entries) = std::fs::read_dir(p) else {
            return;
        };
        for e in entries.flatten() {
            let path = e.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n == "incidents") {
                    continue;
                }
                walk(root, &path, out);
            } else if path.extension().is_some_and(|x| x == "rs") {
                let rel = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                out.push((rel, std::fs::read_to_string(&path).unwrap_or_default()));
            }
        }
    }
    let root = services();
    let mut out = Vec::new();
    walk(&root, &root.join(dir), &mut out);
    out.sort();
    out
}

/// The source of `name`'s body, from its signature to the next top-level
/// item.
fn body<'a>(src: &'a str, signature: &str) -> &'a str {
    let at = src
        .find(signature)
        .unwrap_or_else(|| panic!("{signature} not found"));
    let rest = &src[at..];
    let end = rest[1..].find("\n}\n").map(|e| e + 3).unwrap_or(rest.len());
    &rest[..end]
}

// ── Inbound event paths ─────────────────────────────────────────────────────

/// Every public inbound route, how it finds its team, and a line of its
/// handler that does it. A new route under `/webhooks` fails until it is
/// added here with its team check.
const INBOUND: &[(&str, &str, &str, &str)] = &[
    (
        "routes::github_webhook::handle",
        "gateway/src/routes/github_webhook.rs",
        "verify_github_signature(",
        "the app's signature; the installation's recorded owner team (resolve_team_by_installation); no team, dropped",
    ),
    (
        "routes::jira_webhook::handle",
        "gateway/src/routes/jira_webhook.rs",
        ".key(\"token\", attr_s(&token))",
        "a random per-team token claimed once, and the team's HMAC secret",
    ),
    (
        "routes::jira_webhook::handle_forge",
        "gateway/src/routes/jira_webhook.rs",
        "forge_secret_matches(stored_secret, request_secret)",
        "the team named, proven by that team's own Forge secret; the work runs under the team's stored installation",
    ),
    (
        "routes::alerts::handle_sns",
        "gateway/src/routes/alerts.rs",
        "sns::verify(&state.http, &msg)",
        "SNS signature; the topic's route, which one team claims at a time",
    ),
    (
        "routes::knowledge_sources::webhook",
        "gateway/src/routes/knowledge_sources.rs",
        "hook_target(&state, &token)",
        "a per-source token and its signature; each team syncs only its own sources",
    ),
    (
        "routes::plugin_pkgs::slack_command",
        "gateway/src/routes/plugin_pkgs.rs",
        "flag::chat_team(",
        "the workspace's one claiming team (flag::link_slack) and that team's signing secret",
    ),
    (
        "routes::plugin_pkgs::teams_message",
        "gateway/src/routes/plugin_pkgs.rs",
        "s.teams_token.as_bytes()",
        "a random per-team token, matched to the team's current one, and its HMAC secret",
    ),
    (
        "routes::tests::ci_upload",
        "gateway/src/routes/tests.rs",
        "teams_for(&state, &claims)",
        "a GitHub Actions OIDC token; only teams whose repo has the token's repository id",
    ),
    (
        "routes::tests::ci_quarantine",
        "gateway/src/routes/tests.rs",
        "teams_for(&state, &claims)",
        "the same OIDC repository id match",
    ),
    (
        "routes::hybrid_install::intake",
        "gateway/src/routes/hybrid_install.rs",
        "only_team(&state)",
        "the relay's signature; a self-hosted install has one team",
    ),
    (
        "routes::hybrid_install::proof",
        "gateway/src/routes/hybrid_install.rs",
        "pub async fn proof",
        "returns the install's own ownership proof; no team data",
    ),
];

#[test]
fn every_inbound_route_names_how_it_finds_its_team() {
    let main = read("gateway/src/main.rs");
    let start = main
        .find("let webhook_routes = Router::new()")
        .expect("the webhook routes");
    let block: String = main[start..start + main[start..].find(";\n").unwrap()]
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    let handlers: Vec<&str> = block
        .split(|c: char| c == '(' || c == ')' || c.is_whitespace() || c == ',')
        .filter(|w| w.starts_with("routes::"))
        .collect();
    assert!(handlers.len() >= 8, "{handlers:?}");
    for h in &handlers {
        let known = INBOUND.iter().find(|(name, ..)| name == h);
        assert!(
            known.is_some(),
            "{h} takes events from outside without a team check listed here (bm_teams_never_cross::INBOUND)"
        );
    }
    for (name, file, check, how) in INBOUND {
        assert!(!how.is_empty());
        assert!(
            read(file).contains(check),
            "{name}: its team check `{check}` is gone from {file}"
        );
    }
}

#[test]
fn an_event_for_an_installation_no_team_owns_is_dropped() {
    let src = read("gateway/src/routes/github_webhook.rs");
    let route = body(&src, "async fn route(");
    let resolve = route
        .find("resolve_team_by_installation(state, installation_id)")
        .expect("routing resolves the installation's team");
    let dropped = route
        .find("\"no_team\"")
        .expect("no team is a failed, unrouted delivery");
    let dispatched = route.find("dispatch(").expect("dispatch");
    assert!(resolve < dropped && dropped < dispatched);
    assert!(
        route.contains("return (Ok(StatusCode::OK), String::new());"),
        "an unowned event is recorded under no team"
    );
}

// ── Queued work ─────────────────────────────────────────────────────────────

#[test]
fn a_message_pairing_one_team_with_another_teams_installation_is_refused() {
    // Team A owns installation 7, team B owns 8 (same repository name on both).
    let forged = serde_json::json!({
        "type": "ticket",
        "team_id": TEAM_A,
        "installation_id": 8,
        "repo_name": "web",
    });
    let (ids, teams) = claims(&forged);
    let team = single_team(&teams).unwrap();
    assert!(matches!(decide(&ids, team, Some(7)), Verdict::Refuse(_)));
    // A message that names both teams never runs.
    let mixed = serde_json::json!({"team_id": TEAM_A, "action": {"team_id": TEAM_B}});
    assert!(single_team(&claims(&mixed).1).is_err());
    // The check runs at dispatch for every message.
    let main = include_str!("../../main.rs");
    assert!(main.contains("installation_is_teams_own(&state, &raw)"));
}

// ── Shared identifiers are claimed by one team ──────────────────────────────

#[tokio::test]
async fn one_slack_workspace_routes_to_one_team() {
    let kv = Memory::new();
    let mut a = TeamSettings::default();
    flag::link_slack(&kv, TEAM_A, &mut a, "T0SHARED")
        .await
        .unwrap();
    flag::save_settings(&kv, TEAM_A, &a).await.unwrap();

    let mut b = TeamSettings::default();
    assert!(flag::link_slack(&kv, TEAM_B, &mut b, "T0SHARED")
        .await
        .is_err());
    assert_eq!(
        flag::chat_team(&kv, &flag::slack_key("T0SHARED"))
            .await
            .as_deref(),
        Some(TEAM_A),
        "team B never receives team A's slash commands"
    );

    // Only flag.rs writes the chat index.
    for (file, src) in sources("gateway/src")
        .into_iter()
        .chain(sources("worker/src"))
        .chain(sources("common/src"))
    {
        if file.ends_with("plugins/flag.rs") || file.ends_with("plugins/kv.rs") {
            continue;
        }
        assert!(
            !src.contains("CHAT_INDEX"),
            "{file} touches the chat index outside flag::link_slack / link_teams"
        );
    }
}

// ── Run events ──────────────────────────────────────────────────────────────

#[test]
fn a_reused_run_id_never_shows_another_teams_events() {
    use aws_sdk_dynamodb::types::AttributeValue;
    use std::collections::HashMap;
    let row = |team: &str| {
        HashMap::from([
            (
                "pk".to_string(),
                AttributeValue::S(common::run_events::pk("01SAME")),
            ),
            (
                common::run_events::TEAM_ATTR.to_string(),
                AttributeValue::S(team.to_string()),
            ),
        ])
    };
    let today = "2026-11-15";
    assert!(common::run_events::owned_by(&row(TEAM_A), TEAM_A, today));
    assert!(!common::run_events::owned_by(&row(TEAM_B), TEAM_A, today));
}

#[test]
fn the_events_table_is_only_reached_through_run_events() {
    for (file, src) in sources("gateway/src")
        .into_iter()
        .chain(sources("worker/src"))
        .chain(sources("common/src"))
    {
        if file.ends_with("common/src/run_events.rs") {
            continue;
        }
        for needle in [
            ".table_name(&state.config.events_table_name)",
            ".table_name(&events_table)",
        ] {
            assert!(
                !src.contains(needle),
                "{file} reads or writes the events table directly; use common::run_events"
            );
        }
        if src.contains("run_events::query(") {
            assert!(
                src.contains("run_events::owned_by("),
                "{file} reads run events without keeping only its team's rows"
            );
        }
    }
}

// ── Process-wide caches ─────────────────────────────────────────────────────

/// Every process-wide map or cache, and what keeps one team out of
/// another's entries. A new one fails until it is listed with its key.
const CACHES: &[(&str, &str)] = &[
    ("common/src/agent_policy/registry.rs::REGISTERED", "tool registrations by provider; no team data"),
    ("common/src/github_grants.rs::KNOWN", "installation id"),
    ("common/src/hybrid/client.rs::TOKENS", "installation id"),
    ("common/src/hybrid/client.rs::FAILING", "installation id"),
    ("gateway/src/routes/jira_heal.rs::LAST", "team id; holds only when that team last asked the Jira app to register again"),
    ("common/src/knowledge/search.rs::C", "S3 key under `{team}/knowledge/`, checked by owns_key before the lookup"),
    ("common/src/knowledge/sources/pace.rs::GATES", "provider rate gate; no data"),
    ("common/src/ticket_decisions.rs::KNOWN", "installation id plus `owner/repo@ticket`: read back only by description writes made with that installation"),
    ("common/src/reply_compose.rs::CACHE", "thread key plus a hash of the whole prompt it was written from"),
    ("common/src/review_check.rs::REFUSED", "installation id"),
    ("common/src/usage.rs::P", "model prices; install-wide config"),
    ("common/src/usage.rs::WARNED", "model id; log throttle"),
    ("gateway/src/mcp_server.rs::SEEN", "grant or key id; throttle"),
    ("gateway/src/middleware/auth.rs::SEEN", "(team, user); throttle"),
    ("gateway/src/middleware/rate_limit.rs::RATE_STORE", "client or grant id; counts only"),
    ("gateway/src/permissions.rs::ROUTES", "route table; no data"),
    ("gateway/src/routes/chat_work_prs.rs::CACHE", "URL, reused only on a 304 to the team's own token; repo checked against the team's repos first"),
    ("gateway/src/routes/github_webhook.rs::SEEN", "installation id; log throttle"),
    ("gateway/src/routes/roles.rs::CACHE", "(team, role)"),
    ("gateway/src/routes/run_checks.rs::TOKENS", "installation id"),
    ("gateway/src/routes/run_checks.rs::PR_HEADS", "installation id (read_key)"),
    ("gateway/src/routes/run_checks.rs::CHECKS", "installation id (read_key)"),
    ("gateway/src/routes/run_checks.rs::REQUIRED", "installation id (read_key)"),
    ("gateway/src/routes/run_checks.rs::ASKS", "installation id (read_key)"),
    ("gateway/src/routes/sns.rs::CACHE", "SNS certificate URL; public keys"),
    ("gateway/src/routes/support_access.rs::C", "team id"),
    ("worker/src/agent/run_log.rs::CURRENT", "run id (a ULID)"),
    ("worker/src/clients/github.rs::BUDGETS", "installation id"),
    ("worker/src/clients/github.rs::LIMITED", "installation id"),
    ("worker/src/clients/github_budget.rs::LEDGER", "installation id"),
    ("worker/src/passes/conversation.rs::CACHE", "GitHub `owner/repo#n`: the thread itself, read with the run's own installation"),
    ("worker/src/passes/pr.rs::CACHE", "installation id plus `owner/repo@branch`: the PR template read with that installation"),
    ("worker/src/passes/conversation.rs::REPLIED", "GitHub `owner/repo#n`: the thread itself"),
    ("worker/src/passes/conversation.rs::REVIEW_COMMENTS", "GitHub `owner/repo#n`: the thread itself"),
    ("worker/src/passes/run_guard.rs::REENTRY", "run id (a ULID)"),
    ("worker/src/passes/stop_gate.rs::DIAGNOSES", "run id (a ULID)"),
];

#[test]
fn every_process_wide_cache_is_keyed_by_its_team_or_credential() {
    let mut found: BTreeMap<String, String> = BTreeMap::new();
    for (file, src) in sources("gateway/src")
        .into_iter()
        .chain(sources("worker/src"))
        .chain(sources("common/src"))
    {
        let lines: Vec<&str> = src.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            let t = line.trim_start();
            let t = t
                .strip_prefix("pub(crate) ")
                .or_else(|| t.strip_prefix("pub "))
                .unwrap_or(t);
            let Some(rest) = t.strip_prefix("static ") else {
                continue;
            };
            let name: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            let decl = lines[i..lines.len().min(i + 3)].join(" ");
            let decl = &decl[..decl.find(" = ").unwrap_or(decl.len())];
            let keyed = [
                "HashMap<",
                "HashSet<",
                "BTreeMap<",
                "TtlCache<",
                "IndexCache",
                "PackCache",
                "Cache>",
            ]
            .iter()
            .any(|k| decl.contains(k));
            if keyed {
                found.insert(format!("{file}::{name}"), decl.to_string());
            }
        }
    }
    for (key, decl) in &found {
        assert!(
            CACHES.iter().any(|(k, why)| k == key && !why.is_empty()),
            "{key} is a process-wide cache with no listed key scope (bm_teams_never_cross::CACHES): {decl}"
        );
    }
    for (k, _) in CACHES {
        assert!(found.contains_key(*k), "{k} is listed but no longer exists");
    }
}

#[test]
fn run_check_reads_are_keyed_by_the_installation() {
    let src = read("gateway/src/routes/run_checks.rs");
    for cache in ["PR_HEADS", "CHECKS", "REQUIRED", "ASKS"] {
        for (i, _) in src.match_indices(&format!("{cache}.get(")) {
            let before = &src[..i];
            let fn_start = before.rfind("\nasync fn ").unwrap_or(0);
            let fn_src: String = src[fn_start..i]
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            assert!(
                fn_src.contains("read_key(installation_id"),
                "{cache} is read with a key that doesn't name the installation"
            );
        }
    }
}
