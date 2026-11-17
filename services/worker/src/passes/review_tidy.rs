//! CoderHelm's own reviews and threads on a pull request, kept tidy between
//! reviews (`common::review_supersede::tidy` decides, this carries it out).
//!
//! Before this, CoderHelm's words only got out of the way when it posted a
//! new review. An approval four days old stood in full under every later
//! commit, and a "the checks are red on this head" thread stayed open after
//! the checks went green. Now the same tidy runs:
//!
//! - on a push (the gateway's `pull_request` / `synchronize`, after GitHub
//!   has marked the moved lines outdated);
//! - when checks finish on the head (`check_suite` / `completed`);
//! - on a merge or a close (`pull_request` / `closed`);
//! - on the review sweep, once per head per [`SWEEP_TIDY_TTL_SECS`], for
//!   every open pull request CoderHelm reviewed (a delivery that never came).
//!
//! It posts nothing and touches only CoderHelm's own reviews, comments and
//! threads. Cheap: a pull request CoderHelm never reviewed costs one read;
//! the checks and the changed files are read only when an open thread could
//! turn on them. Idempotent: a second run finds nothing to do.

use crate::clients::github::GitHubClient;
use crate::WorkerState;
use common::messages::ReviewTidyMessage;
use common::review_supersede::{self as rs, PrNow, PrState, Tidy};
use serde_json::Value;
use tracing::info;

/// How often the sweep tidies the same pull request head.
pub const SWEEP_TIDY_TTL_SECS: u64 = 6 * 3600;

/// GitHub lists at most this many changed files in one page; a pull request
/// at the limit may change more, so "the file left the diff" is not read.
const FILES_PAGE: usize = 100;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// One event's tidy (a push, checks finishing, a merge or a close).
pub async fn run(state: &WorkerState, msg: &ReviewTidyMessage) -> Result<Tidy, Error> {
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;
    let done = pull_request(
        &github,
        &msg.repo_owner,
        &msg.repo_name,
        msg.pr_number,
        None,
    )
    .await?;
    if !done.is_empty() {
        info!(
            repo = %format!("{}/{}", msg.repo_owner, msg.repo_name),
            pr = msg.pr_number,
            reason = %msg.reason,
            collapsed = done.collapse.len(),
            resolved = done.resolve.len(),
            hidden = done.minimize.len(),
            "Tidied CoderHelm's reviews on the pull request"
        );
    }
    Ok(done)
}

/// Tidies one pull request: `pr` is GitHub's pull request when the caller
/// already read it (the sweep's list of open ones), else it is read here.
pub async fn pull_request(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    pr: Option<&Value>,
) -> Result<Tidy, Error> {
    let reviews =
        super::conversation::reviews_of(&github.list_pr_reviews(owner, repo, number).await?);
    // Never reviewed by CoderHelm: nothing of its own to tidy.
    if !reviews.iter().any(|r| r.own) {
        return Ok(Tidy::default());
    }
    let read;
    let pr = match pr {
        Some(p) => p,
        None => {
            read = github.get_pull_request(owner, repo, number).await?;
            &read
        }
    };
    let threads = super::conversation::threads_of(
        &github
            .list_review_threads_detail(owner, repo, number)
            .await?,
    );
    let mut now = pr_now(owner, repo, number, pr);
    if now.state == PrState::Open {
        let open: Vec<&rs::Thread> = threads
            .iter()
            .filter(|t| t.own() && !t.resolved && !t.person_waiting())
            .collect();
        if open
            .iter()
            .any(|t| rs::failing_check_finding(&t.first_body))
        {
            now.checks_pass = github
                .list_check_runs_for_ref(owner, repo, &now.head)
                .await
                .ok()
                .and_then(|v| rs::checks_pass(&v));
        }
        if !open.is_empty() {
            now.changed_paths = github
                .list_pull_files(owner, repo, number)
                .await
                .ok()
                .filter(|f| f.len() < FILES_PAGE)
                .map(|f| f.into_iter().map(|(path, _)| path).collect());
        }
    }
    let plan = rs::tidy(&reviews, &threads, &now);
    apply(github, owner, repo, number, &plan).await;
    Ok(plan)
}

/// The pull request as the rules read it.
pub fn pr_now(owner: &str, repo: &str, number: u64, pr: &Value) -> PrNow {
    let state = if pr["merged"].as_bool() == Some(true) || pr["merged_at"].is_string() {
        PrState::Merged
    } else if pr["state"].as_str() == Some("closed") {
        PrState::Closed
    } else {
        PrState::Open
    };
    PrNow {
        owner: owner.to_string(),
        repo: repo.to_string(),
        number,
        head: pr["head"]["sha"].as_str().unwrap_or("").to_string(),
        state,
        checks_pass: None,
        changed_paths: None,
    }
}

/// Carries the plan out. Best effort: a step GitHub refuses is skipped with
/// a log line, and the next event or sweep tries it again.
async fn apply(github: &GitHubClient, owner: &str, repo: &str, number: u64, plan: &Tidy) {
    super::conversation::collapse_own_reviews(github, owner, repo, number, &plan.collapse).await;
    for (t, why) in &plan.resolve {
        if let Err(e) = github.resolve_review_thread(t).await {
            info!(pr = number, thread = %t, why = ?why, error = %e, "Skipped resolving a settled thread of CoderHelm's");
        }
    }
    for node in &plan.minimize {
        if let Err(e) = github.hide_own_comment(node, true).await {
            info!(pr = number, comment = %node, error = %e, "Skipped hiding a settled comment of CoderHelm's");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The marks the common rules read are the ones the reviewer writes.
    #[test]
    fn the_rules_read_the_reviewers_own_marks() {
        assert_eq!(
            rs::UPDATED_MARK,
            crate::passes::review_verdict::UPDATED_MARK
        );
        assert_eq!(
            rs::REQUIRED_MARK,
            crate::passes::review_rules::REQUIRED_MARK
        );
        let src = include_str!("review_rules.rs");
        assert!(src.contains(&format!("\"{}\"", rs::FAILING_CHECK_LABEL)));
        // A body brought up to date is read back as the head it names.
        let body = crate::passes::review_verdict::updated_body(
            "✅ **Approved**",
            "aaaaaaa111",
            "bbbbbbb222",
        );
        let r = rs::Review {
            body,
            commit_id: "aaaaaaa111".into(),
            ..Default::default()
        };
        assert_eq!(rs::reviewed_sha(&r), "bbbbbbb");
    }

    #[test]
    fn the_pull_request_state_is_read_from_github() {
        let open = json!({"state": "open", "head": {"sha": "abc1234"}});
        let merged = json!({"state": "closed", "merged": true, "head": {"sha": "abc1234"}});
        let closed = json!({"state": "closed", "merged_at": null, "head": {"sha": "abc1234"}});
        assert_eq!(pr_now("acme", "web", 1, &open).state, PrState::Open);
        assert_eq!(pr_now("acme", "web", 1, &merged).state, PrState::Merged);
        assert_eq!(pr_now("acme", "web", 1, &closed).state, PrState::Closed);
        assert_eq!(pr_now("acme", "web", 1, &open).head, "abc1234");
    }

    /// The thread query asks GitHub for what the code-gone rule reads.
    #[test]
    fn the_thread_query_reads_outdated_lines() {
        let src = include_str!("../clients/github.rs");
        assert!(src.contains("nodes{ id isResolved isOutdated line path comments(first:50)"));
        let t = super::super::conversation::threads_of(&[json!({
            "id": "T1", "isResolved": false, "isOutdated": true, "line": null, "path": "a.ts",
            "comments": {"nodes": [{"id": "C1", "isMinimized": false, "body": "x", "author": {"login": "coderhelm"}, "pullRequestReview": {"databaseId": 1}}]}
        })]);
        assert!(t[0].outdated && t[0].line_gone);
    }
}
