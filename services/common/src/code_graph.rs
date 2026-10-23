//! The code graph's per-repo switch and build status, shared by the gateway
//! (dashboard, webhooks) and the worker (indexing, agent tools).
//!
//! Every repo CoderHelm works in gets a graph unless someone turned it off
//! for that repo. The graph is its own feature: it is not tied to the
//! reviewer.
//!
//! Storage, all in the settings table:
//!   switch → pk=team_id, sk=GRAPH_CONFIG#REPO#{owner}/{name}  (`enabled`)
//!   legacy → pk=team_id, sk=REVIEW_CONFIG#REPO#{owner}/{name} (`graph_enabled`)
//!   status → pk=CG#{team_id}#{owner}/{name}, sk=STATUS
//!   graph  → pk=CG#{team_id}#{owner}/{name}, sk=META | SUMMARY | NM#/R#/I#/RI#/F#
//!
//! The legacy flag is read only while a repo has no switch of its own: an
//! explicit `false` there keeps the graph off, anything else leaves it on.
//! The first write to the switch replaces it.

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;
use std::collections::HashMap;

/// A build that has been queued or running this long without finishing is
/// treated as failed (the worker runs for at most 15 minutes).
pub const STALE_SECS: i64 = 60 * 60;

/// Partition key of a repo's graph.
pub fn graph_pk(team_id: &str, repo: &str) -> String {
    format!("CG#{team_id}#{repo}")
}

/// Sort key of a repo's graph switch.
pub fn config_sk(repo: &str) -> String {
    format!("GRAPH_CONFIG#REPO#{repo}")
}

/// Sort key of the reviewer config that used to carry the switch.
pub fn legacy_sk(repo: &str) -> String {
    format!("REVIEW_CONFIG#REPO#{repo}")
}

pub const STATUS_SK: &str = "STATUS";
pub const META_SK: &str = "META";

/// Whether a repo's graph is on: its own switch when set, else an explicit
/// `false` on the legacy reviewer flag, else on.
pub fn effective_enabled(own: Option<bool>, legacy: Option<bool>) -> bool {
    match own {
        Some(v) => v,
        None => legacy != Some(false),
    }
}

/// Where a repo's graph build is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildState {
    Queued,
    Indexing,
    Ready,
    Failed,
}

impl BuildState {
    pub fn as_str(self) -> &'static str {
        match self {
            BuildState::Queued => "queued",
            BuildState::Indexing => "indexing",
            BuildState::Ready => "ready",
            BuildState::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Option<BuildState> {
        match s {
            "queued" => Some(BuildState::Queued),
            "indexing" => Some(BuildState::Indexing),
            "ready" => Some(BuildState::Ready),
            "failed" => Some(BuildState::Failed),
            _ => None,
        }
    }
}

/// The STATUS item: the last build's state, when it changed, and why it
/// failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub state: BuildState,
    /// Epoch seconds of the last state change.
    pub at: i64,
    pub error: String,
}

impl Status {
    /// Queued or indexing for longer than any build takes.
    pub fn is_stale(&self, now: i64) -> bool {
        matches!(self.state, BuildState::Queued | BuildState::Indexing)
            && now - self.at > STALE_SECS
    }
}

/// What a repo's graph is right now, for the dashboard and the agents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Shown {
    /// Turned off for this repo.
    Off,
    /// On, but nothing has been built or queued yet.
    NotBuilt,
    Queued,
    Indexing,
    /// Built; a later failed or running pass may be under way.
    Ready,
    Failed(String),
}

impl Shown {
    pub fn as_str(&self) -> &'static str {
        match self {
            Shown::Off => "off",
            Shown::NotBuilt => "not_built",
            Shown::Queued => "queued",
            Shown::Indexing => "indexing",
            Shown::Ready => "ready",
            Shown::Failed(_) => "failed",
        }
    }
}

/// The one reading of (switch, build status, graph presence). A graph that
/// exists stays usable while a later pass runs; a pass stuck past
/// [`STALE_SECS`] reads as failed so it can be queued again.
pub fn shown(enabled: bool, status: Option<&Status>, has_graph: bool, now: i64) -> Shown {
    if !enabled {
        return Shown::Off;
    }
    match status {
        Some(s) if s.is_stale(now) => {
            Shown::Failed("The last build didn't finish. Rebuild to try again.".to_string())
        }
        Some(s) => match s.state {
            BuildState::Queued if !has_graph => Shown::Queued,
            BuildState::Indexing if !has_graph => Shown::Indexing,
            BuildState::Failed => Shown::Failed(if s.error.is_empty() {
                "The build failed.".to_string()
            } else {
                s.error.clone()
            }),
            _ if has_graph => Shown::Ready,
            // Marked ready but the graph is gone: build it again.
            _ => Shown::NotBuilt,
        },
        None if has_graph => Shown::Ready,
        None => Shown::NotBuilt,
    }
}

/// Whether a first build may be queued now: never queued, or the last one
/// got stuck. A failed build is not retried by itself (an admin rebuilds),
/// so a repo that can't be indexed never costs a build on every run.
pub fn may_queue(status: Option<&Status>, has_graph: bool, now: i64) -> bool {
    match status {
        None => !has_graph,
        Some(s) => s.is_stale(now),
    }
}

/// The condition the queue write is made under: the same rule as
/// [`may_queue`], checked by DynamoDB so two runs starting together queue
/// one build.
const QUEUE_CONDITION: &str =
    "attribute_not_exists(sk) OR ((#st = :queued OR #st = :indexing) AND #at < :cutoff)";

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn item_bool(item: &HashMap<String, AttributeValue>, k: &str) -> Option<bool> {
    item.get(k).and_then(|v| v.as_bool().ok()).copied()
}

fn parse_status(item: &HashMap<String, AttributeValue>) -> Option<Status> {
    let state = BuildState::parse(item.get("state")?.as_s().ok()?)?;
    let at = item
        .get("at")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    let error = item
        .get("error")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    Some(Status { state, at, error })
}

async fn get(
    dynamo: &DynamoClient,
    table: &str,
    pk: &str,
    sk: &str,
) -> Option<HashMap<String, AttributeValue>> {
    dynamo
        .get_item()
        .table_name(table)
        .key("pk", s(pk))
        .key("sk", s(sk))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
}

/// Whether the graph is on for `repo` (`owner/name`).
pub async fn is_enabled(dynamo: &DynamoClient, table: &str, team_id: &str, repo: &str) -> bool {
    let own = get(dynamo, table, team_id, &config_sk(repo))
        .await
        .and_then(|it| item_bool(&it, "enabled"));
    if own.is_some() {
        return effective_enabled(own, None);
    }
    let legacy = get(dynamo, table, team_id, &legacy_sk(repo))
        .await
        .and_then(|it| item_bool(&it, "graph_enabled"));
    effective_enabled(None, legacy)
}

/// Every repo with its own switch set, and its value.
pub async fn switches(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
) -> Result<HashMap<String, bool>, String> {
    let mut items = Vec::new();
    let mut start = None;
    loop {
        let out = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", s(team_id))
            .expression_attribute_values(":p", s("GRAPH_CONFIG#REPO#"))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(e)))?;
        items.extend(out.items.unwrap_or_default());
        match out.last_evaluated_key {
            Some(k) if !k.is_empty() => start = Some(k),
            _ => break,
        }
    }
    Ok(items
        .iter()
        .filter_map(|it| {
            let repo = it
                .get("sk")?
                .as_s()
                .ok()?
                .strip_prefix("GRAPH_CONFIG#REPO#")?
                .to_string();
            Some((repo, item_bool(it, "enabled")?))
        })
        .collect())
}

/// Turn the graph on or off for `repo`.
pub async fn set_enabled(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    repo: &str,
    enabled: bool,
    by: &str,
) -> Result<(), String> {
    dynamo
        .put_item()
        .table_name(table)
        .item("pk", s(team_id))
        .item("sk", s(&config_sk(repo)))
        .item("enabled", AttributeValue::Bool(enabled))
        .item("updated_by", s(by))
        .item("updated_at", s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(e)))
}

/// The last build's status, if any.
pub async fn status(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    repo: &str,
) -> Option<Status> {
    get(dynamo, table, &graph_pk(team_id, repo), STATUS_SK)
        .await
        .as_ref()
        .and_then(parse_status)
}

/// Whether a graph has been built for `repo` (its META item exists).
pub async fn has_graph(dynamo: &DynamoClient, table: &str, team_id: &str, repo: &str) -> bool {
    get(dynamo, table, &graph_pk(team_id, repo), META_SK)
        .await
        .is_some()
}

/// Mark a full build as queued. Without `force` it succeeds only under
/// [`may_queue`]'s rule, so concurrent callers queue it once; with `force`
/// (an admin's Rebuild) it always does. `Ok(true)` means the caller must
/// now send the index message.
pub async fn mark_queued(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    repo: &str,
    force: bool,
) -> Result<bool, String> {
    let now = chrono::Utc::now().timestamp();
    let mut put = dynamo
        .put_item()
        .table_name(table)
        .item("pk", s(&graph_pk(team_id, repo)))
        .item("sk", s(STATUS_SK))
        .item("state", s(BuildState::Queued.as_str()))
        .item("at", AttributeValue::N(now.to_string()));
    if !force {
        put = put
            .condition_expression(QUEUE_CONDITION)
            .expression_attribute_names("#st", "state")
            .expression_attribute_names("#at", "at")
            .expression_attribute_values(":queued", s(BuildState::Queued.as_str()))
            .expression_attribute_values(":indexing", s(BuildState::Indexing.as_str()))
            .expression_attribute_values(
                ":cutoff",
                AttributeValue::N((now - STALE_SECS).to_string()),
            );
    }
    match put.send().await {
        Ok(_) => Ok(true),
        Err(e) => {
            let held = e
                .as_service_error()
                .map(|se| se.is_conditional_check_failed_exception())
                .unwrap_or(false);
            if held {
                Ok(false)
            } else {
                Err(format!(
                    "{}",
                    aws_sdk_dynamodb::error::DisplayErrorContext(e)
                ))
            }
        }
    }
}

/// Undo [`mark_queued`] when the index message could not be sent, so the
/// next run can queue it again. Only a still-queued status is removed.
pub async fn unmark_queued(dynamo: &DynamoClient, table: &str, team_id: &str, repo: &str) {
    let _ = dynamo
        .delete_item()
        .table_name(table)
        .key("pk", s(&graph_pk(team_id, repo)))
        .key("sk", s(STATUS_SK))
        .condition_expression("#st = :queued")
        .expression_attribute_names("#st", "state")
        .expression_attribute_values(":queued", s(BuildState::Queued.as_str()))
        .send()
        .await;
}

/// Record a build's progress. `error` is kept only for a failure.
pub async fn set_status(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    repo: &str,
    state: BuildState,
    error: &str,
) {
    let mut put = dynamo
        .put_item()
        .table_name(table)
        .item("pk", s(&graph_pk(team_id, repo)))
        .item("sk", s(STATUS_SK))
        .item("state", s(state.as_str()))
        .item(
            "at",
            AttributeValue::N(chrono::Utc::now().timestamp().to_string()),
        );
    if state == BuildState::Failed && !error.is_empty() {
        put = put.item("error", s(crate::truncate_str(error, 300)));
    }
    if let Err(e) = put.send().await {
        tracing::warn!(error = %aws_sdk_dynamodb::error::DisplayErrorContext(e), repo, "Code graph: could not record build status");
    }
}

/// A failure as people read it on the dashboard: one plain line, no stack.
pub fn failure_reason(err: &str) -> String {
    let first = err.lines().next().unwrap_or("").trim();
    let lower = first.to_lowercase();
    let reason = if lower.contains("404") || lower.contains("not found") {
        "CoderHelm can't read this repository. Check that the GitHub App can access it."
    } else if lower.contains("403") || lower.contains("forbidden") {
        "GitHub refused access to this repository."
    } else if lower.contains("too large") || lower.contains("size") {
        "The repository is too large to index."
    } else {
        ""
    };
    if reason.is_empty() {
        format!("The build failed: {}", crate::truncate_str(first, 200))
    } else {
        reason.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(state: BuildState, at: i64) -> Status {
        Status {
            state,
            at,
            error: String::new(),
        }
    }

    #[test]
    fn a_repo_without_reviewer_config_is_on() {
        assert!(effective_enabled(None, None));
        // A reviewer config that never mentioned the graph.
        assert!(effective_enabled(None, Some(true)));
    }

    #[test]
    fn an_explicit_off_is_respected() {
        assert!(!effective_enabled(None, Some(false)));
        assert!(!effective_enabled(Some(false), None));
        assert!(!effective_enabled(Some(false), Some(true)));
        // The repo's own switch replaces the legacy flag.
        assert!(effective_enabled(Some(true), Some(false)));
    }

    #[test]
    fn a_first_build_is_queued_once() {
        let now = 10_000;
        assert!(may_queue(None, false, now));
        // Queued a moment ago by another run: not again.
        assert!(!may_queue(
            Some(&st(BuildState::Queued, now - 5)),
            false,
            now
        ));
        assert!(!may_queue(
            Some(&st(BuildState::Indexing, now - 5)),
            false,
            now
        ));
        // Built, or failed: not by itself.
        assert!(!may_queue(Some(&st(BuildState::Ready, now - 5)), true, now));
        assert!(!may_queue(
            Some(&st(BuildState::Failed, now - 5)),
            false,
            now
        ));
        // A graph from before the status existed is not rebuilt.
        assert!(!may_queue(None, true, now));
        // A build stuck past the limit may be queued again.
        assert!(may_queue(
            Some(&st(BuildState::Queued, now - STALE_SECS - 1)),
            false,
            now
        ));
    }

    #[test]
    fn the_status_shown_reads_switch_status_and_graph() {
        let now = 10_000;
        assert_eq!(shown(false, None, true, now), Shown::Off);
        assert_eq!(shown(true, None, false, now), Shown::NotBuilt);
        assert_eq!(shown(true, None, true, now), Shown::Ready);
        assert_eq!(
            shown(true, Some(&st(BuildState::Indexing, now)), false, now),
            Shown::Indexing
        );
        // A built graph stays usable while an update runs.
        assert_eq!(
            shown(true, Some(&st(BuildState::Indexing, now)), true, now),
            Shown::Ready
        );
        let failed = Status {
            state: BuildState::Failed,
            at: now,
            error: "GitHub refused access to this repository.".into(),
        };
        assert_eq!(
            shown(true, Some(&failed), false, now),
            Shown::Failed("GitHub refused access to this repository.".into())
        );
        assert!(matches!(
            shown(
                true,
                Some(&st(BuildState::Queued, now - STALE_SECS - 1)),
                false,
                now
            ),
            Shown::Failed(_)
        ));
    }

    #[test]
    fn failures_read_plainly() {
        assert!(failure_reason("HTTP 404 Not Found: repos/x").contains("can't read"));
        assert!(failure_reason("boom\nstack").starts_with("The build failed: boom"));
    }

    #[test]
    fn states_round_trip() {
        for s in [
            BuildState::Queued,
            BuildState::Indexing,
            BuildState::Ready,
            BuildState::Failed,
        ] {
            assert_eq!(BuildState::parse(s.as_str()), Some(s));
        }
        assert_eq!(BuildState::parse("x"), None);
    }
}
