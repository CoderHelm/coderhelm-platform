//! Stuck-review sweep, and the review records it reads.
//!
//! A review that didn't finish (the model API failed, or its answer couldn't
//! be read) is retried a few times and then left with a note on the PR. Older
//! code posted such a failure as a formal "request changes" review, which
//! then held the PR until someone happened to ask for a re-review. This
//! sweep (EventBridge → SQS, every 30 minutes) finds open PRs whose standing
//! CoderHelm review is one that didn't finish, more than 30 minutes old and
//! with nothing newer, and runs the review again for the PR's current head,
//! once per head.

use crate::clients::github::GitHubClient;
use crate::models::{ReviewMessage, WorkerMessage};
use crate::passes::review_agent::StandingReview;
use crate::passes::{attr_n, attr_s};
use crate::WorkerState;
use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;
use tracing::{info, warn};

/// How the old error path began a review that didn't finish. Records written
/// before failures stopped being posted as verdicts are recognized by it.
pub const LEGACY_FAILED_PREFIX: &str = "Automated review could not complete";

/// How long a review that didn't finish stands before the sweep re-runs it.
pub const STUCK_AFTER_SECS: i64 = 30 * 60;

/// How long the sweep won't re-run the same PR head again.
const SWEEP_CLAIM_TTL_SECS: u64 = 24 * 3600;

const CONFIG_PREFIX: &str = "REVIEW_CONFIG#REPO#";

/// One stored CoderHelm review of a PR (a `REVIEW#` record).
#[derive(Debug, Clone, Default)]
pub struct ReviewRecord {
    pub verdict: String,
    pub posted_as: String,
    pub head_sha: String,
    pub created_at: String,
    pub body: String,
}

impl ReviewRecord {
    /// A review that didn't finish, posted as a verdict by the old error path.
    pub fn failed(&self) -> bool {
        self.body.trim_start().starts_with(LEGACY_FAILED_PREFIX)
    }

    /// A review (not an answer to a question).
    pub fn is_review(&self) -> bool {
        self.verdict != "QUESTION"
            && matches!(
                self.posted_as.as_str(),
                "APPROVE" | "REQUEST_CHANGES" | "COMMENT"
            )
    }

    /// A review that set CoderHelm's review state on GitHub (a COMMENT
    /// review doesn't change it).
    pub fn sets_state(&self) -> bool {
        self.verdict != "QUESTION"
            && matches!(self.posted_as.as_str(), "APPROVE" | "REQUEST_CHANGES")
    }
}

/// CoderHelm's standing formal review: its newest review that set a GitHub
/// review state. `records` is newest first.
pub fn standing_review(records: &[ReviewRecord]) -> Option<StandingReview> {
    records
        .iter()
        .find(|r| r.sets_state())
        .map(|r| StandingReview {
            verdict: r.posted_as.clone(),
            failed: r.failed(),
            head_sha: r.head_sha.clone(),
        })
}

/// The newest stored reviews of a PR, newest first. Empty on any error.
pub async fn review_records(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    pr: u64,
    limit: i32,
) -> Vec<ReviewRecord> {
    let prefix = format!("REVIEW#{repo}#{pr:0>6}#");
    let resp = match state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :sk)")
        .expression_attribute_values(":pk", attr_s(team_id))
        .expression_attribute_values(":sk", attr_s(&prefix))
        .scan_index_forward(false)
        .consistent_read(true)
        .limit(limit)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!(repo, pr, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Could not read review records");
            return Vec::new();
        }
    };
    resp.items().iter().map(record_from_item).collect()
}

fn s_of(i: &HashMap<String, AttributeValue>, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn record_from_item(i: &HashMap<String, AttributeValue>) -> ReviewRecord {
    ReviewRecord {
        verdict: s_of(i, "verdict"),
        posted_as: s_of(i, "posted_as"),
        head_sha: s_of(i, "head_sha"),
        created_at: s_of(i, "created_at"),
        body: s_of(i, "body"),
    }
}

/// The note a review that didn't finish leaves (`REVIEWFAIL#{repo}#{pr}`).
#[derive(Debug, Clone, Default)]
pub struct FailureNote {
    pub head_sha: String,
    /// Every retry failed and the PR was told.
    pub gave_up: bool,
    pub updated_at: String,
}

fn failure_sk(repo: &str, pr: u64) -> String {
    format!("REVIEWFAIL#{repo}#{pr:0>6}")
}

/// Records that a review of `head_sha` didn't finish (`gave_up` once every
/// retry failed). Best-effort: a write failure is logged.
pub async fn note_failure(
    state: &WorkerState,
    msg: &ReviewMessage,
    head_sha: &str,
    reason: &str,
    gave_up: bool,
) {
    let (team_id, pr, attempt) = (&msg.team_id, msg.pr_number, msg.attempt);
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let repo = repo.as_str();
    let now = chrono::Utc::now();
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&failure_sk(repo, pr)))
        .item("repo", attr_s(repo))
        .item("pr_number", attr_n(pr))
        .item("head_sha", attr_s(head_sha))
        .item("attempt", attr_n(attempt))
        .item("reason", attr_s(common::truncate_str(reason, 2000)))
        .item("gave_up", AttributeValue::Bool(gave_up))
        .item("updated_at", attr_s(&now.to_rfc3339()))
        .item("ttl", attr_n(now.timestamp() + 14 * 86_400))
        .send()
        .await
    {
        warn!(repo, pr, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Could not note the failed review");
    }
}

async fn failure_note(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    pr: u64,
) -> Option<FailureNote> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&failure_sk(repo, pr)))
        .consistent_read(true)
        .send()
        .await
        .ok()?
        .item()?
        .clone();
    Some(FailureNote {
        head_sha: s_of(&item, "head_sha"),
        gave_up: item
            .get("gave_up")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        updated_at: s_of(&item, "updated_at"),
    })
}

fn age_secs(rfc3339: &str, now: chrono::DateTime<chrono::Utc>) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(rfc3339)
        .ok()
        .map(|t| (now - t.with_timezone(&chrono::Utc)).num_seconds())
}

/// Why a PR is stuck on a review that didn't finish, or None. `records` is
/// newest first.
///
/// Stuck when CoderHelm's standing review on GitHub is an old error review
/// more than [`STUCK_AFTER_SECS`] old (it holds the merge until replaced), or
/// when every retry of a review of the current head failed more than
/// [`STUCK_AFTER_SECS`] ago and no review has been stored since.
pub fn stuck(
    records: &[ReviewRecord],
    note: Option<&FailureNote>,
    current_head: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<&'static str> {
    if let Some(r) = records.iter().find(|r| r.sets_state()) {
        if r.failed() && age_secs(&r.created_at, now).is_some_and(|a| a >= STUCK_AFTER_SECS) {
            return Some("the standing review is one that didn't finish");
        }
    }
    let n = note?;
    let newest_review = records.iter().find(|r| r.is_review());
    let reviewed_since = newest_review.is_some_and(|r| r.created_at > n.updated_at);
    (n.gave_up
        && (n.head_sha.is_empty() || n.head_sha == current_head)
        && age_secs(&n.updated_at, now).is_some_and(|a| a >= STUCK_AFTER_SECS)
        && !reviewed_since)
        .then_some("every retry of the review of this head failed")
}

/// What one sweep did.
#[derive(Debug, Default)]
pub struct SweepReport {
    pub repos: usize,
    pub open_prs: usize,
    /// `owner/repo#pr@head7: why` for each review sent again.
    pub requeued: Vec<String>,
}

struct ReviewedRepo {
    team_id: String,
    repo: String,
    label: String,
}

/// Every repo with the reviewer on (and not killed). Paginated and bounded.
async fn reviewed_repos(state: &WorkerState) -> Vec<ReviewedRepo> {
    let mut out = vec![];
    let mut last_key: Option<HashMap<String, AttributeValue>> = None;
    for page in 0..50 {
        let mut req = state
            .dynamo
            .scan()
            .table_name(&state.config.settings_table_name)
            .filter_expression("begins_with(sk, :pfx) AND enabled = :t")
            .expression_attribute_values(":pfx", attr_s(CONFIG_PREFIX))
            .expression_attribute_values(":t", AttributeValue::Bool(true));
        if let Some(k) = last_key.take() {
            req = req.set_exclusive_start_key(Some(k));
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Reviewer config scan failed");
                break;
            }
        };
        for item in resp.items() {
            let killed = item
                .get("killed")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false);
            let team_id = s_of(item, "pk");
            let repo = s_of(item, "sk")
                .strip_prefix(CONFIG_PREFIX)
                .unwrap_or("")
                .to_string();
            if killed || team_id.is_empty() || !repo.contains('/') {
                continue;
            }
            let label = Some(s_of(item, "label"))
                .filter(|l| !l.is_empty())
                .unwrap_or_else(|| "ch-review".to_string());
            out.push(ReviewedRepo {
                team_id,
                repo,
                label,
            });
        }
        match resp.last_evaluated_key() {
            Some(k) => last_key = Some(k.clone()),
            None => return out,
        }
        if page == 49 {
            warn!("Reviewer config scan hit its page cap; some repos wait for the next sweep");
        }
    }
    out
}

/// Runs the sweep over every reviewed repo of every team.
pub async fn run(
    state: &WorkerState,
) -> Result<SweepReport, Box<dyn std::error::Error + Send + Sync>> {
    let repos = reviewed_repos(state).await;
    let mut report = SweepReport {
        repos: repos.len(),
        ..Default::default()
    };
    let mut installs: HashMap<String, Option<u64>> = HashMap::new();
    let now = chrono::Utc::now();
    for r in repos {
        let iid = match installs.get(&r.team_id) {
            Some(v) => *v,
            None => {
                let v = super::review_reminder::team_installation_id(state, &r.team_id).await;
                installs.insert(r.team_id.clone(), v);
                v
            }
        };
        let Some(iid) = iid else {
            continue;
        };
        if let Err(e) = sweep_repo(state, &r, iid, now, &mut report).await {
            warn!(repo = %r.repo, error = %e, "Stuck-review sweep failed for repo");
        }
    }
    Ok(report)
}

async fn sweep_repo(
    state: &WorkerState,
    r: &ReviewedRepo,
    installation_id: u64,
    now: chrono::DateTime<chrono::Utc>,
    report: &mut SweepReport,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (owner, name) = r.repo.split_once('/').ok_or("bad repo slug")?;
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        installation_id,
        &state.http,
    )?;
    let pulls = github.list_open_pulls(owner, name).await?;
    for pr in pulls.as_array().map(|a| a.as_slice()).unwrap_or(&[]) {
        if pr["draft"].as_bool().unwrap_or(false) {
            continue;
        }
        let number = pr["number"].as_u64().unwrap_or(0);
        let head = pr["head"]["sha"].as_str().unwrap_or("");
        if number == 0 || head.is_empty() {
            continue;
        }
        report.open_prs += 1;
        let records = review_records(state, &r.team_id, &r.repo, number, 10).await;
        let note = failure_note(state, &r.team_id, &r.repo, number).await;
        let Some(why) = stuck(&records, note.as_ref(), head, now) else {
            continue;
        };
        // Once per head: a review that keeps failing isn't re-run every tick.
        let claim = format!("REVIEWSWEEP#{}#{number:0>6}#{head}", r.repo);
        if !super::review_pr::claim_once_ttl(state, &r.team_id, &claim, SWEEP_CLAIM_TTL_SECS).await
        {
            continue;
        }
        let msg = sweep_message(r, installation_id, owner, name, number, head);
        if super::review_pr::enqueue_review(state, &msg, 0).await {
            let line = format!("{}#{number}@{}: {why}", r.repo, &head[..head.len().min(7)]);
            info!(team_id = %r.team_id, repo = %r.repo, pr = number, head, why, "Stuck review sent again");
            report.requeued.push(line);
        }
    }
    Ok(())
}

/// The review job the sweep sends for a stuck PR's current head.
fn sweep_message(
    r: &ReviewedRepo,
    installation_id: u64,
    owner: &str,
    name: &str,
    pr: u64,
    head: &str,
) -> ReviewMessage {
    ReviewMessage {
        team_id: r.team_id.clone(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number: pr,
        head_sha: head.to_string(),
        label: r.label.clone(),
        question: None,
        trigger: "sweep".to_string(),
        dedup_key: format!("sweep#{head}"),
        reply_to_comment_id: None,
        attempt: 0,
        wait_secs: 0,
        reconcile_note: None,
    }
}

/// The SQS body for a review job.
pub fn review_job_body(msg: &ReviewMessage) -> String {
    serde_json::to_string(&WorkerMessage::Review(msg.clone())).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(verdict: &str, posted_as: &str, at: &str, body: &str) -> ReviewRecord {
        ReviewRecord {
            verdict: verdict.into(),
            posted_as: posted_as.into(),
            head_sha: "a1b2c3d".into(),
            created_at: at.into(),
            body: body.into(),
        }
    }

    fn now() -> chrono::DateTime<chrono::Utc> {
        "2026-10-19T18:00:00Z".parse().unwrap()
    }

    /// The incident: a real request for changes, then an error review on the
    /// fix commit, then five answered questions. The sweep picks it up.
    #[test]
    fn a_pr_held_by_an_error_review_is_picked_up() {
        let records = vec![
            rec("QUESTION", "COMMENT", "2026-10-19T17:35:53Z", "fine to merge"),
            rec("QUESTION", "COMMENT", "2026-10-15T17:31:02Z", "both resolved"),
            rec(
                "REQUEST_CHANGES",
                "REQUEST_CHANGES",
                "2026-10-14T21:51:20Z",
                "Automated review could not complete (Anthropic API error (520 <unknown status code>): error code: 520 ). Requesting a human look.",
            ),
            rec("REQUEST_CHANGES", "REQUEST_CHANGES", "2026-10-14T14:16:57Z", "The PR adopts …"),
        ];
        let st = standing_review(&records).unwrap();
        assert!(st.failed);
        assert_eq!(st.verdict, "REQUEST_CHANGES");
        assert!(stuck(&records, None, "a1b2c3d", now()).is_some());
        // Not before it has stood for 30 minutes.
        let soon: chrono::DateTime<chrono::Utc> = "2026-10-14T22:00:00Z".parse().unwrap();
        assert!(stuck(&records, None, "a1b2c3d", soon).is_none());
    }

    #[test]
    fn a_pr_with_a_real_verdict_is_left_alone() {
        let records = vec![
            rec("APPROVE", "APPROVE", "2026-10-19T10:00:00Z", "Looks good."),
            rec(
                "REQUEST_CHANGES",
                "REQUEST_CHANGES",
                "2026-10-14T21:51:20Z",
                "Automated review could not complete (x).",
            ),
        ];
        assert!(stuck(&records, None, "a1b2c3d", now()).is_none());
        let rc = vec![rec(
            "REQUEST_CHANGES",
            "REQUEST_CHANGES",
            "2026-10-14T14:16:57Z",
            "A defect.",
        )];
        assert!(stuck(&rc, None, "a1b2c3d", now()).is_none());
        assert!(stuck(&[], None, "a1b2c3d", now()).is_none());
    }

    #[test]
    fn a_head_whose_retries_all_failed_is_picked_up_once_it_has_stood() {
        let note = FailureNote {
            head_sha: "a1b2c3d".into(),
            gave_up: true,
            updated_at: "2026-10-19T17:00:00Z".into(),
        };
        let older = vec![rec(
            "REQUEST_CHANGES",
            "REQUEST_CHANGES",
            "2026-10-14T14:16:57Z",
            "A defect.",
        )];
        assert!(stuck(&older, Some(&note), "a1b2c3d", now()).is_some());
        // A newer commit gets its own review; still retrying isn't stuck.
        assert!(stuck(&older, Some(&note), "c0ffee1", now()).is_none());
        let retrying = FailureNote {
            gave_up: false,
            ..note.clone()
        };
        assert!(stuck(&older, Some(&retrying), "a1b2c3d", now()).is_none());
        // A review stored since then answers it.
        let since = vec![rec(
            "COMMENT",
            "COMMENT",
            "2026-10-19T17:10:00Z",
            "Questions",
        )];
        assert!(stuck(&since, Some(&note), "a1b2c3d", now()).is_none());
    }

    #[test]
    fn the_sweep_job_targets_the_current_head_once() {
        let r = ReviewedRepo {
            team_id: "T".into(),
            repo: "o/r".into(),
            label: "ch-review".into(),
        };
        let m = sweep_message(&r, 7, "o", "r", 42, "a1b2c3d");
        assert_eq!(m.head_sha, "a1b2c3d");
        assert_eq!(m.dedup_key, "sweep#a1b2c3d");
        assert!(m.question.is_none());
        let body: serde_json::Value = serde_json::from_str(&review_job_body(&m)).unwrap();
        assert_eq!(body["type"], "review");
        assert_eq!(body["trigger"], "sweep");
    }
}
