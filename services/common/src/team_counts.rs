//! A team's run and review counts on its analytics items (`<month>` and
//! `ALL_TIME`), each added exactly once.
//!
//! Every count is one transaction: a once-only marker and the `ADD` on each
//! period item. A marker that is already there cancels the whole
//! transaction, so a replayed message, a duplicate delivery or a second run
//! of the backfill (`scripts/team_counts_backfill.py`) never adds twice.
//! Markers live in their own partition (`COUNTED#<team>`), so no reader of
//! a team's period items ever sees them.
//!
//! What is counted:
//! * `total_runs`: every run that has a record in the runs table, agent runs
//!   and plugin runs alike, in the month it started. Plugin runs also add
//!   `completed` or `failed` when they end, like agent runs.
//! * `prs_reviewed`: the pull requests the reviewer posted a review on,
//!   each once per month and once in `ALL_TIME`. Answers to questions and
//!   re-reviews that posted nothing are not reviews; this is the same rule
//!   the weekly email and the analytics page count by ([`counts_as_review`]).

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem, Update};

pub use crate::usage::Added;

/// Runs started (agent and plugin runs).
pub const RUNS: &str = "total_runs";
/// Distinct pull requests the reviewer posted a review on.
pub const PRS_REVIEWED: &str = "prs_reviewed";
/// Runs that finished, and runs that failed (plugin runs add these here;
/// agent runs add them where they finish).
pub const COMPLETED: &str = "completed";
pub const FAILED: &str = "failed";
/// The period item that holds every month's counts together.
pub const ALL_TIME: &str = "ALL_TIME";

/// Whether a stored review is a review: posted on GitHub, and not an answer
/// to a question. The weekly email, the analytics page and the admin page
/// all count by this.
pub fn counts_as_review(verdict: &str, posted: bool) -> bool {
    posted && verdict != "QUESTION"
}

/// The month (`YYYY-MM`) of an RFC 3339 time, or of now when it can't be read.
pub fn month_of(at: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(at)
        .map(|d| d.with_timezone(&chrono::Utc))
        .unwrap_or_else(|_| chrono::Utc::now())
        .format("%Y-%m")
        .to_string()
}

/// The partition holding a team's once-only markers.
pub fn marker_pk(team: &str) -> String {
    format!("COUNTED#{team}")
}

/// A run's started marker.
pub fn run_marker(run_id: &str) -> String {
    format!("RUN#{run_id}")
}

/// A run's ended marker.
pub fn run_end_marker(run_id: &str) -> String {
    format!("RUN_END#{run_id}")
}

/// A pull request's reviewed marker for one period.
pub fn pr_marker(period: &str, repo: &str, pr: u64) -> String {
    format!("PR#{period}#{}#{pr:0>6}", repo.to_ascii_lowercase())
}

/// Adds 1 to a field on a team's period items, exactly once per marker.
#[async_trait::async_trait]
pub trait CountStore: Send + Sync {
    async fn add_once(
        &self,
        team: &str,
        marker: &str,
        field: &str,
        periods: &[String],
    ) -> Result<Added, String>;
}

/// The analytics table.
pub struct DynamoCounts {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub table: String,
}

#[async_trait::async_trait]
impl CountStore for DynamoCounts {
    async fn add_once(
        &self,
        team: &str,
        marker: &str,
        field: &str,
        periods: &[String],
    ) -> Result<Added, String> {
        use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
        let put = Put::builder()
            .table_name(&self.table)
            .item("team_id", AttributeValue::S(marker_pk(team)))
            .item("period", AttributeValue::S(marker.to_string()))
            .item(
                "counted_at",
                AttributeValue::S(chrono::Utc::now().to_rfc3339()),
            )
            .condition_expression("attribute_not_exists(team_id)")
            .build()
            .map_err(|e| e.to_string())?;
        // The marker goes first: its cancellation reason is at index 0.
        let mut tx = self
            .dynamo
            .transact_write_items()
            .transact_items(TransactWriteItem::builder().put(put).build());
        for period in periods {
            let update = Update::builder()
                .table_name(&self.table)
                .key("team_id", AttributeValue::S(team.to_string()))
                .key("period", AttributeValue::S(period.clone()))
                .update_expression("ADD #f :one")
                .expression_attribute_names("#f", field)
                .expression_attribute_values(":one", AttributeValue::N("1".into()))
                .build()
                .map_err(|e| e.to_string())?;
            tx = tx.transact_items(TransactWriteItem::builder().update(update).build());
        }
        match tx.send().await {
            Ok(_) => Ok(Added::Counted),
            Err(e) => {
                let already = matches!(
                    e.as_service_error(),
                    Some(TransactWriteItemsError::TransactionCanceledException(c))
                        if c.cancellation_reasons().first().and_then(|r| r.code())
                            == Some("ConditionalCheckFailed")
                );
                if already {
                    Ok(Added::AlreadyCounted)
                } else {
                    Err(aws_sdk_dynamodb::error::DisplayErrorContext(&e).to_string())
                }
            }
        }
    }
}

/// Counts in memory (tests): the same once-only rule, and the period items
/// as the analytics table would hold them.
#[derive(Default)]
pub struct MemoryCounts {
    markers: Mutex<HashSet<(String, String)>>,
    items: Mutex<HashMap<(String, String), HashMap<String, u64>>>,
}

impl MemoryCounts {
    /// One period item, as DynamoDB returns it (None when nothing was added).
    pub fn item(&self, team: &str, period: &str) -> Option<HashMap<String, AttributeValue>> {
        let items = self.items.lock().unwrap_or_else(|e| e.into_inner());
        items
            .get(&(team.to_string(), period.to_string()))
            .map(|fields| {
                fields
                    .iter()
                    .map(|(k, v)| (k.clone(), AttributeValue::N(v.to_string())))
                    .collect()
            })
    }

    /// One field of one period item (0 when absent).
    pub fn get(&self, team: &str, period: &str, field: &str) -> u64 {
        let items = self.items.lock().unwrap_or_else(|e| e.into_inner());
        items
            .get(&(team.to_string(), period.to_string()))
            .and_then(|f| f.get(field).copied())
            .unwrap_or(0)
    }
}

#[async_trait::async_trait]
impl CountStore for MemoryCounts {
    async fn add_once(
        &self,
        team: &str,
        marker: &str,
        field: &str,
        periods: &[String],
    ) -> Result<Added, String> {
        let mut markers = self.markers.lock().unwrap_or_else(|e| e.into_inner());
        if !markers.insert((team.to_string(), marker.to_string())) {
            return Ok(Added::AlreadyCounted);
        }
        let mut items = self.items.lock().unwrap_or_else(|e| e.into_inner());
        for p in periods {
            *items
                .entry((team.to_string(), p.clone()))
                .or_default()
                .entry(field.to_string())
                .or_default() += 1;
        }
        Ok(Added::Counted)
    }
}

/// How a run ended, for its `completed` / `failed` count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunEnd {
    Completed,
    Failed,
}

/// Counts a run as started in the month of `created_at`. Call it whenever
/// the run's record is written: only the first call counts.
pub async fn count_run_started(
    store: &dyn CountStore,
    team: &str,
    run_id: &str,
    created_at: &str,
) -> Result<Added, String> {
    store
        .add_once(
            team,
            &run_marker(run_id),
            RUNS,
            &[month_of(created_at), ALL_TIME.to_string()],
        )
        .await
}

/// Counts how a run ended, in the month it ended. Only the first call counts.
pub async fn count_run_ended(
    store: &dyn CountStore,
    team: &str,
    run_id: &str,
    end: RunEnd,
    ended_at: &str,
) -> Result<Added, String> {
    let field = match end {
        RunEnd::Completed => COMPLETED,
        RunEnd::Failed => FAILED,
    };
    store
        .add_once(
            team,
            &run_end_marker(run_id),
            field,
            &[month_of(ended_at), ALL_TIME.to_string()],
        )
        .await
}

/// Counts a pull request as reviewed in the month of `created_at` and in
/// `ALL_TIME`, each once. A review that isn't one ([`counts_as_review`]) is
/// not counted.
pub async fn count_review(
    store: &dyn CountStore,
    team: &str,
    repo: &str,
    pr: u64,
    verdict: &str,
    posted: bool,
    created_at: &str,
) -> Result<(), String> {
    if !counts_as_review(verdict, posted) || repo.is_empty() || pr == 0 {
        return Ok(());
    }
    // One transaction per period: a PR first reviewed last month is new
    // this month but not in ALL_TIME.
    for period in [month_of(created_at), ALL_TIME.to_string()] {
        store
            .add_once(
                team,
                &pr_marker(&period, repo, pr),
                PRS_REVIEWED,
                std::slice::from_ref(&period),
            )
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: &str = "TEAM#acme";

    #[test]
    fn only_posted_reviews_that_are_not_answers_count() {
        assert!(counts_as_review("APPROVE", true));
        assert!(counts_as_review("REQUEST_CHANGES", true));
        assert!(counts_as_review("COMMENT", true));
        assert!(!counts_as_review("QUESTION", true));
        assert!(!counts_as_review("APPROVE", false));
    }

    #[test]
    fn months_come_from_the_time_given() {
        assert_eq!(month_of("2026-10-31T23:59:59Z"), "2026-10");
        assert_eq!(month_of("2026-11-01T00:00:00.123456789+00:00"), "2026-11");
        assert_eq!(month_of("not a time").len(), 7);
    }

    #[test]
    fn markers_never_collide_across_kinds_and_repos_case() {
        assert_eq!(
            pr_marker("2026-11", "Acme/Web", 42),
            "PR#2026-11#acme/web#000042"
        );
        assert_ne!(run_marker("01J"), run_end_marker("01J"));
        assert_eq!(marker_pk(T), "COUNTED#TEAM#acme");
    }

    #[tokio::test]
    async fn a_run_counts_once_however_often_its_record_is_written() {
        let s = MemoryCounts::default();
        let at = "2026-11-03T10:00:00Z";
        assert_eq!(count_run_started(&s, T, "r1", at).await, Ok(Added::Counted));
        assert_eq!(
            count_run_started(&s, T, "r1", at).await,
            Ok(Added::AlreadyCounted)
        );
        count_run_started(&s, T, "r2", "2026-10-30T10:00:00Z")
            .await
            .unwrap();
        assert_eq!(s.get(T, "2026-11", RUNS), 1);
        assert_eq!(s.get(T, "2026-10", RUNS), 1);
        assert_eq!(s.get(T, ALL_TIME, RUNS), 2);

        count_run_ended(&s, T, "r1", RunEnd::Completed, at)
            .await
            .unwrap();
        count_run_ended(&s, T, "r1", RunEnd::Failed, at)
            .await
            .unwrap();
        assert_eq!(s.get(T, ALL_TIME, COMPLETED), 1);
        assert_eq!(s.get(T, ALL_TIME, FAILED), 0);
    }

    #[tokio::test]
    async fn a_pr_counts_once_per_month_and_once_in_all_time() {
        let s = MemoryCounts::default();
        let r = |at: &'static str, pr: u64, verdict: &'static str, posted: bool| {
            (at, pr, verdict, posted)
        };
        for (at, pr, verdict, posted) in [
            r("2026-10-20T00:00:00Z", 7, "REQUEST_CHANGES", true),
            r("2026-10-21T00:00:00Z", 7, "APPROVE", true),
            r("2026-11-02T00:00:00Z", 7, "APPROVE", true),
            r("2026-11-02T00:00:00Z", 8, "QUESTION", true),
            r("2026-11-03T00:00:00Z", 9, "COMMENT", false),
            r("2026-11-03T00:00:00Z", 10, "COMMENT", true),
        ] {
            count_review(&s, T, "acme/web", pr, verdict, posted, at)
                .await
                .unwrap();
        }
        assert_eq!(s.get(T, "2026-10", PRS_REVIEWED), 1);
        assert_eq!(s.get(T, "2026-11", PRS_REVIEWED), 2);
        assert_eq!(s.get(T, ALL_TIME, PRS_REVIEWED), 2);
    }

    /// The service sources, read as text.
    fn sources() -> Vec<(String, String)> {
        fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for e in std::fs::read_dir(dir).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p.extension().is_some_and(|x| x == "rs") {
                    out.push(p);
                }
            }
        }
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let mut files = vec![];
        for svc in ["worker/src", "gateway/src", "common/src"] {
            walk(&root.join(svc), &mut files);
        }
        assert!(files.len() > 20, "found the service sources");
        files
            .into_iter()
            .map(|f| {
                let src = std::fs::read_to_string(&f).unwrap();
                (f.display().to_string(), src)
            })
            .collect()
    }

    /// The function around byte `at`: from its `fn` line to the next line
    /// that closes a top-level item.
    fn enclosing_fn(src: &str, at: usize) -> &str {
        let start = src[..at].rfind("fn ").unwrap_or(0);
        let end = src[at..].find("\n}\n").map_or(src.len(), |e| at + e);
        &src[start..end]
    }

    /// Every way a run reaches the runs table counts it, and every stored
    /// review counts its PR: a new run kind or review path that writes its
    /// record without counting fails here. And nothing adds to these
    /// counters except through this module (once per marker).
    #[test]
    fn every_run_kind_and_every_stored_review_is_counted() {
        let files = sources();
        let mut run_writers = 0;
        let mut review_writers = 0;
        let mut offenders = vec![];
        for (name, src) in &files {
            // Agent runs: a put on the runs table.
            for (i, _) in src.match_indices(".put_item()") {
                let next = &src[i..src.len().min(i + 200)];
                if next.contains("runs_table") {
                    run_writers += 1;
                    if !enclosing_fn(src, i).contains("count_run_started(") {
                        offenders.push(format!("{name}: a run record put that isn't counted"));
                    }
                }
            }
            // Plugin runs: the runs-table log.
            if let Some(i) = src.find("impl RunLog for RunsTable") {
                run_writers += 1;
                let body = &src[i..src.len().min(i + 4000)];
                if !body.contains("count_run(") {
                    offenders.push(format!("{name}: plugin runs aren't counted"));
                }
            }
            // Reviews: the stored review record.
            for (i, _) in src.match_indices(r#".item("record_type", attr_s("review"))"#) {
                review_writers += 1;
                if !enclosing_fn(src, i).contains("count_review(") {
                    offenders.push(format!("{name}: a stored review that isn't counted"));
                }
            }
            if name.ends_with("team_counts.rs") {
                continue;
            }
            for (i, _) in src.match_indices("\"ADD ") {
                let rest = &src[i + 1..];
                let lit = &rest[..rest.find('"').unwrap_or(rest.len())];
                let words: Vec<&str> = lit
                    .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .collect();
                if [RUNS, PRS_REVIEWED, "reviews"]
                    .iter()
                    .any(|c| words.contains(c))
                {
                    offenders.push(format!("{name}: counted outside team_counts: {lit}"));
                }
            }
        }
        assert!(run_writers >= 2, "found the agent and plugin run writers");
        assert!(review_writers >= 1, "found the review writer");
        assert!(offenders.is_empty(), "{offenders:#?}");
    }
}
