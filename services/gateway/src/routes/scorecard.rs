//! The webhook side of the scorecard (`common::scorecard`) and of what a
//! revert teaches.
//!
//! The GitHub webhook handler calls these for the events that say what
//! became of a pull request CoderHelm opened: opened, reopened, closed or
//! merged ([`on_pull_request`]), a person's review ([`on_review`]), a
//! person's push to its branch and a revert commit on the default branch
//! ([`on_push`]). Each turns the payload into one scorecard event; the
//! counters move by what the event changed, so a redelivered webhook counts
//! nothing twice. Everything here is best effort: a failure is logged and
//! the webhook goes on.

use crate::models::WorkerMessage;
use crate::AppState;
use common::scorecard::{self, Event, Store as _};
use serde_json::Value;
use tracing::{info, warn};

fn store(state: &AppState) -> scorecard::Dynamo {
    scorecard::Dynamo::new(&state.dynamo, &state.config.settings_table_name)
}

fn repo_of(payload: &Value) -> Option<(String, String)> {
    let repo = &payload["repository"];
    let owner = repo["owner"]["login"]
        .as_str()
        .or_else(|| repo["owner"]["name"].as_str())?;
    let name = repo["name"].as_str()?;
    (!owner.is_empty() && !name.is_empty()).then(|| (owner.to_string(), name.to_string()))
}

/// Whether the pull request is one of CoderHelm's own code pull requests:
/// opened by this app's bot (its login is known once GitHub has been asked),
/// or by a CoderHelm bot as the rest of the webhook handler recognises one.
async fn counts(state: &AppState, pr: &Value) -> bool {
    let bot = crate::auth::github_app::app_slug(state)
        .await
        .map(|slug| format!("{slug}[bot]"));
    scorecard::counts(pr, bot.as_deref()) || scorecard::counts(pr, None)
}

async fn record(state: &AppState, team_id: &str, repo: &str, pr: u64, e: &Event) {
    if let Err(err) = scorecard::record(&store(state), team_id, repo, pr, e).await {
        warn!(team_id, repo, pr, error = %err, "Could not count the pull request on the scorecard");
    }
}

/// A `pull_request` webhook for one of CoderHelm's own code pull requests:
/// opened, reopened or closed. `human_commits`: people's commits counted on
/// GitHub at a merge.
pub async fn on_pull_request(
    state: &AppState,
    team_id: &str,
    payload: &Value,
    action: &str,
    human_commits: Option<u64>,
) {
    let pr = &payload["pull_request"];
    if !counts(state, pr).await {
        return;
    }
    let (Some((owner, name)), Some(number)) = (repo_of(payload), pr["number"].as_u64()) else {
        return;
    };
    if let Some(e) = scorecard::pr_event(action, pr, human_commits) {
        record(state, team_id, &format!("{owner}/{name}"), number, &e).await;
    }
}

/// A person's submitted review of one of CoderHelm's pull requests.
pub async fn on_review(state: &AppState, team_id: &str, payload: &Value) {
    let pr = &payload["pull_request"];
    if !counts(state, pr).await {
        return;
    }
    let (Some((owner, name)), Some(number)) = (repo_of(payload), pr["number"].as_u64()) else {
        return;
    };
    if let Some(e) = scorecard::review_event(payload) {
        record(state, team_id, &format!("{owner}/{name}"), number, &e).await;
    }
}

/// A push. To a branch one of CoderHelm's open pull requests is on: the
/// commits a person wrote count as their work on top of CoderHelm's. To any
/// branch: a commit that reverts the merge commit of one of CoderHelm's
/// pull requests marks it reverted. Returns the reverted pull requests, with
/// the reason people gave, for the caller to learn from.
pub async fn on_push(state: &AppState, team_id: &str, payload: &Value) -> Vec<Reverted> {
    let Some((owner, name)) = repo_of(payload) else {
        return vec![];
    };
    let repo = format!("{owner}/{name}");
    let st = store(state);
    let people = scorecard::human_push_shas(payload);
    if let (Some(branch), false) = (scorecard::pushed_branch(payload), people.is_empty()) {
        match st
            .pointer(team_id, &scorecard::branch_sk(&repo, branch))
            .await
        {
            Ok(Some(p)) => {
                record(
                    state,
                    team_id,
                    &repo,
                    p.pr,
                    &Event::HumanPush { shas: people },
                )
                .await;
            }
            Ok(None) => {}
            Err(e) => {
                warn!(team_id, repo = %repo, error = %e, "Could not look up the pushed branch")
            }
        }
    }
    let mut out = vec![];
    for rc in scorecard::revert_commits(payload) {
        let found = st
            .pointer(team_id, &scorecard::sha_sk(&repo, &rc.reverted_sha))
            .await;
        let Ok(Some(p)) = found else {
            continue; // not the merge commit of one of CoderHelm's
        };
        let at = if rc.at.is_empty() {
            chrono::Utc::now().to_rfc3339()
        } else {
            rc.at.clone()
        };
        record(
            state,
            team_id,
            &repo,
            p.pr,
            &Event::Reverted {
                at: at.clone(),
                by: rc.sha.chars().take(12).collect(),
            },
        )
        .await;
        info!(team_id, repo = %repo, pr = p.pr, "A CoderHelm change was reverted by a commit");
        out.push(Reverted {
            owner: owner.clone(),
            name: name.clone(),
            pr: p.pr,
            at,
            by: format!("commit {}", rc.sha.chars().take(12).collect::<String>()),
            reason: rc.reason,
        });
    }
    out
}

/// One of CoderHelm's merged pull requests that was reverted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reverted {
    pub owner: String,
    pub name: String,
    pub pr: u64,
    pub at: String,
    /// What reverted it, in words (`#52`, `commit abc123`).
    pub by: String,
    /// Why, in the person's words; empty when they gave none.
    pub reason: String,
}

/// Counts a revert pull request's target as reverted.
pub async fn on_revert_pr(state: &AppState, team_id: &str, r: &Reverted) {
    record(
        state,
        team_id,
        &format!("{}/{}", r.owner, r.name),
        r.pr,
        &Event::Reverted {
            at: r.at.clone(),
            by: r.by.clone(),
        },
    )
    .await;
}

/// Hands a revert to the worker to learn from: the area's lesson is written
/// again with it and the reason is distilled into a lesson of its own.
pub async fn queue_revert_lesson(
    state: &AppState,
    team_id: &str,
    installation_id: u64,
    run_id: &str,
    r: &Reverted,
) {
    let msg = WorkerMessage::RewardOutcome(common::rewards::RewardOutcomeMessage {
        team_id: team_id.to_string(),
        installation_id,
        run_id: run_id.to_string(),
        repo_owner: r.owner.clone(),
        repo_name: r.name.clone(),
        pr_number: r.pr,
        trigger: common::rewards::TRIGGER_REVERTED.to_string(),
        note: r.reason.clone(),
        ..Default::default()
    });
    if let Err(e) =
        super::github_webhook::send_to_queue(state, &state.config.ticket_queue_url, &msg).await
    {
        warn!(team_id, pr = r.pr, status = ?e, "Could not queue the revert's lesson");
    }
}

/// Hands a closed guide pull request to the worker to settle its lessons.
pub async fn queue_guide_closed(
    state: &AppState,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    name: &str,
    pr_number: u64,
) {
    let msg = WorkerMessage::RewardOutcome(common::rewards::RewardOutcomeMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: owner.to_string(),
        repo_name: name.to_string(),
        pr_number,
        trigger: common::rewards::TRIGGER_GUIDE_CLOSED.to_string(),
        ..Default::default()
    });
    if let Err(e) =
        super::github_webhook::send_to_queue(state, &state.config.ticket_queue_url, &msg).await
    {
        warn!(team_id, pr_number, status = ?e, "Could not queue the closed guide pull request");
    }
}

#[cfg(test)]
mod tests {
    /// One function of the webhook handler's source, from its `fn` line to
    /// the next top-level item.
    fn fn_src<'a>(src: &'a str, head: &str) -> &'a str {
        let start = src.find(head).unwrap_or_else(|| panic!("{head} is gone"));
        let rest = &src[start + head.len()..];
        let end = [
            "\nasync fn ",
            "\nfn ",
            "\npub(crate) ",
            "\npub fn ",
            "\n#[",
            "\n///",
        ]
        .iter()
        .filter_map(|m| rest.find(m))
        .min()
        .unwrap_or(rest.len());
        &src[start..start + head.len() + end]
    }

    /// The scorecard counts only what the webhook handler feeds it, and a
    /// revert teaches only when it is handed on: each event's handler must
    /// still make its call.
    #[test]
    fn the_webhook_handler_feeds_every_event() {
        let hook = include_str!("github_webhook.rs");
        let pr = fn_src(hook, "async fn handle_pull_request(");
        assert!(pr.contains("scorecard::on_pull_request(state, team_id, payload, action, None)"));
        assert!(pr.contains(
            "scorecard::on_pull_request(state, team_id, payload, \"closed\", merged_human_commits)"
        ));
        // The guide pull request is settled, not scored as code work: its
        // check comes before the run lookup and the scorecard.
        let guide = pr
            .find("queue_guide_closed(")
            .expect("guide pull requests are settled");
        let closed = pr.find("\"closed\", merged_human_commits").unwrap();
        assert!(guide < closed);
        assert!(fn_src(hook, "async fn handle_pr_review(").contains("scorecard::on_review("));
        let push = fn_src(hook, "async fn handle_push(");
        assert!(push.contains("scorecard::on_push(") && push.contains("record_revert_commits("));
        // Before the early return for branches other than the default one:
        // a person's push to a pull request's branch is never skipped.
        assert!(
            push.find("scorecard::on_push(").unwrap()
                < push.find("Only the branch the graph tracks").unwrap()
        );
        assert!(fn_src(hook, "async fn record_revert(").contains("revert_reason("));
        // No review job is queued for a guide pull request: its check comes
        // before the label is read and long before the job is sent.
        let trigger = fn_src(hook, "async fn handle_review_trigger(");
        let guide = trigger
            .find("is_guide_branch(")
            .expect("guide pull requests are skipped");
        assert!(guide < trigger.find("let carries_label").unwrap());
        assert!(guide < trigger.find("send_review_job(").unwrap());
        let noted = fn_src(hook, "async fn note_reverted(");
        assert!(
            noted.contains("scorecard::on_revert_pr(") && noted.contains("queue_revert_lesson(")
        );
    }
}
