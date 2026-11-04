//! A re-review when a pull request's description is edited to answer the
//! review.
//!
//! A review that requests changes, or asks the author to confirm facts from
//! the real system ("Confirm before merge"), is often answered in the
//! description. Before, nothing happened on that edit: the author had to push
//! an empty commit or comment `@coderhelm re-review`. Now a `pull_request`
//! `edited` event that changed what the description says, made by a person
//! with standing on a PR CoderHelm reviews, queues a review of the same
//! commit, once per burst of edits and once per (commit, description).

use axum::http::StatusCode;
use common::claim::Claim;
use common::review_description as desc;
use serde_json::Value;
use tracing::{info, warn};

use super::github_webhook::{
    check_run_budget, load_review_config, may_start_work, post_limit_comment, send_to_queue_after,
};
use crate::models::{ReviewMessage, WorkerMessage};
use crate::AppState;

/// A description edit that may answer the review.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DescriptionEdit {
    /// Hash of the description the edit left (`review_description::body_hash`).
    pub hash: String,
}

/// The edit in a `pull_request` `edited` payload, when it counts: the body
/// changed (not only the title or the base branch), a person made it (not
/// CoderHelm or another app), and what the description says changed (not
/// only whitespace or a ticked box).
pub(crate) fn description_edit(payload: &Value) -> Option<DescriptionEdit> {
    if payload["action"].as_str() != Some("edited") {
        return None;
    }
    let from = &payload["changes"]["body"]["from"];
    if payload["changes"]["body"].is_null() || !(from.is_string() || from.is_null()) {
        return None;
    }
    if is_app(&payload["sender"]) {
        return None;
    }
    let old = from.as_str().unwrap_or("");
    let new = payload["pull_request"]["body"].as_str().unwrap_or("");
    desc::changed(old, new).then(|| DescriptionEdit {
        hash: desc::body_hash(new),
    })
}

/// A bot account: CoderHelm itself (its own description updates) or any
/// other app.
fn is_app(sender: &Value) -> bool {
    let login = sender["login"].as_str().unwrap_or("").to_ascii_lowercase();
    sender["type"].as_str() == Some("Bot") || login.ends_with("[bot]") || login.is_empty()
}

/// The editor, shaped like a comment for `may_start_work`. GitHub gives the
/// PR author's association with the repository; the editor carries it only
/// when they are the author (anyone else is checked by team link or repo
/// permission).
pub(crate) fn editor_item(payload: &Value) -> Value {
    let sender = &payload["sender"];
    let pr = &payload["pull_request"];
    let is_author = sender["id"].as_u64().is_some() && sender["id"] == pr["user"]["id"];
    serde_json::json!({
        "user": sender,
        "author_association": if is_author { pr["author_association"].clone() } else { Value::Null },
    })
}

/// One stored CoderHelm review of the PR (a `REVIEW#` record).
#[derive(Debug, Clone, Default)]
pub(crate) struct StoredReview {
    pub verdict: String,
    pub posted_as: String,
    pub body: String,
}

/// Whether CoderHelm's standing review is one a description can answer: its
/// newest review (`records` newest first) is not an approval, and it asked
/// the author to confirm facts ("Confirm before merge") or requested
/// changes. No review yet, or an approval, and an edit changes nothing.
pub(crate) fn standing_review_awaits_answer(records: &[StoredReview]) -> bool {
    let Some(standing) = records.iter().find(|r| {
        r.verdict != "QUESTION"
            && matches!(
                r.posted_as.as_str(),
                "APPROVE" | "REQUEST_CHANGES" | "COMMENT"
            )
    }) else {
        return false;
    };
    if standing.verdict == "APPROVE" || standing.posted_as == "APPROVE" {
        return false;
    }
    standing.verdict == "REQUEST_CHANGES"
        || standing.posted_as == "REQUEST_CHANGES"
        || standing.body.contains("Confirm before merge")
}

/// What the two claims say about queueing a review for this edit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaimStep {
    /// Queue the review.
    Queue,
    /// A review of this commit's description is already waiting; it reads
    /// the description when it runs, so it covers this edit.
    Settling,
    /// This commit was already reviewed with this description.
    AlreadyReviewed,
}

/// The settle claim first (one review per burst), then the (commit,
/// description) claim. A failed claim write lets the review through (a
/// duplicate review is better than a lost one).
pub(crate) fn claim_step(settle: &Claim, seen: Option<&Claim>) -> ClaimStep {
    if *settle == Claim::Held {
        return ClaimStep::Settling;
    }
    match seen {
        Some(Claim::Held) => ClaimStep::AlreadyReviewed,
        _ => ClaimStep::Queue,
    }
}

/// The review job for a counted edit of the PR in `payload`: the commit
/// the PR is on, the description trigger, keyed by (commit, description).
pub(crate) fn review_message(
    payload: &Value,
    team_id: &str,
    installation_id: u64,
    label: &str,
    edit: &DescriptionEdit,
) -> ReviewMessage {
    let pr = &payload["pull_request"];
    let head_sha = pr["head"]["sha"].as_str().unwrap_or("");
    ReviewMessage {
        team_id: team_id.to_string(),
        installation_id,
        repo_owner: payload["repository"]["owner"]["login"]
            .as_str()
            .unwrap_or("")
            .to_string(),
        repo_name: payload["repository"]["name"]
            .as_str()
            .unwrap_or("")
            .to_string(),
        pr_number: pr["number"].as_u64().unwrap_or(0),
        head_sha: head_sha.to_string(),
        label: label.to_string(),
        question: None,
        trigger: desc::TRIGGER.to_string(),
        dedup_key: desc::dedup_key(head_sha, &edit.hash),
        description_hash: Some(edit.hash.clone()),
        ..Default::default()
    }
}

/// `pull_request` `edited`: queue a re-review when the description edit
/// answers CoderHelm's review. Every skip is logged and returns 200.
pub(crate) async fn handle(
    state: &AppState,
    payload: &Value,
    installation_id: u64,
    team_id: &str,
) -> Result<StatusCode, StatusCode> {
    let Some(edit) = description_edit(payload) else {
        return Ok(StatusCode::OK);
    };
    let pr = &payload["pull_request"];
    let pr_number = pr["number"].as_u64().unwrap_or(0);
    let head_sha = pr["head"]["sha"].as_str().unwrap_or("");
    if pr_number == 0 || head_sha.is_empty() || pr["state"].as_str() != Some("open") {
        return Ok(StatusCode::OK);
    }
    let repo = &payload["repository"];
    let owner = repo["owner"]["login"].as_str().unwrap_or("");
    let name = repo["name"].as_str().unwrap_or("");

    // The same PRs a push re-reviews: the reviewer on, the label on the PR,
    // and not a draft.
    let cfg = load_review_config(state, team_id, owner, name).await?;
    if !cfg.enabled || cfg.killed {
        return Ok(StatusCode::OK);
    }
    let carries_label = pr["labels"].as_array().is_some_and(|ls| {
        ls.iter()
            .any(|l| l["name"].as_str() == Some(cfg.label.as_str()))
    });
    if !carries_label || pr["draft"].as_bool().unwrap_or(false) {
        return Ok(StatusCode::OK);
    }

    let records = stored_reviews(state, team_id, owner, name, pr_number).await;
    if !standing_review_awaits_answer(&records) {
        info!(
            owner,
            name,
            pr_number,
            "Description edited — CoderHelm's review doesn't wait on it, no re-review"
        );
        return Ok(StatusCode::OK);
    }

    let editor = editor_item(payload);
    if !may_start_work(
        state,
        team_id,
        installation_id,
        payload,
        &editor,
        "description edit",
    )
    .await
    {
        return Ok(StatusCode::OK);
    }

    let table = &state.config.settings_table_name;
    let settle_sk = desc::settle_sk(owner, name, pr_number, head_sha);
    let settle = common::claim::claim(
        &state.dynamo,
        table,
        team_id,
        &settle_sk,
        desc::SETTLE_CLAIM_SECS,
    )
    .await;
    let seen_sk = desc::seen_sk(owner, name, pr_number, head_sha, &edit.hash);
    let seen = if settle == Claim::Held {
        None
    } else {
        Some(
            common::claim::claim(&state.dynamo, table, team_id, &seen_sk, desc::SEEN_TTL_SECS)
                .await,
        )
    };
    for c in [Some(&settle), seen.as_ref()].into_iter().flatten() {
        if let Claim::Failed(e) = c {
            warn!(owner, name, pr_number, error = %e, "Description review claim errored — allowing review (fail-open)");
        }
    }
    match claim_step(&settle, seen.as_ref()) {
        ClaimStep::Settling => {
            info!(
                owner,
                name,
                pr_number,
                "Description edited — a re-review is already waiting and reads the latest text"
            );
            return Ok(StatusCode::OK);
        }
        ClaimStep::AlreadyReviewed => {
            // Free the burst slot: nothing is waiting on it.
            common::claim::release(&state.dynamo, table, team_id, &settle_sk).await;
            info!(
                owner,
                name, pr_number, "Description edited back to text already reviewed at this commit"
            );
            return Ok(StatusCode::OK);
        }
        ClaimStep::Queue => {}
    }

    if let Some(reason) = check_run_budget(state, team_id).await {
        info!(
            team_id,
            pr_number, "Description re-review skipped — token limit reached"
        );
        common::claim::release(&state.dynamo, table, team_id, &settle_sk).await;
        common::claim::release(&state.dynamo, table, team_id, &seen_sk).await;
        post_limit_comment(state, installation_id, owner, name, pr_number, &reason).await;
        return Ok(StatusCode::OK);
    }

    info!(
        owner,
        name,
        pr_number,
        head = head_sha,
        "Description edited to answer the review — re-review queued"
    );
    let message = review_message(payload, team_id, installation_id, &cfg.label, &edit);
    let sent = send_to_queue_after(
        state,
        &state.config.ticket_queue_url,
        &WorkerMessage::Review(message),
        desc::SETTLE_SECS,
    )
    .await;
    if sent.is_err() {
        // Not queued: the next edit (or GitHub's redelivery) may try again.
        common::claim::release(&state.dynamo, table, team_id, &settle_sk).await;
        common::claim::release(&state.dynamo, table, team_id, &seen_sk).await;
    }
    sent
}

/// CoderHelm's newest stored reviews of the PR, newest first. Empty on any
/// error (no re-review is queued then).
async fn stored_reviews(
    state: &AppState,
    team_id: &str,
    owner: &str,
    name: &str,
    pr_number: u64,
) -> Vec<StoredReview> {
    use aws_sdk_dynamodb::types::AttributeValue;
    let prefix = format!("REVIEW#{owner}/{name}#{pr_number:06}#");
    let resp = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()))
        .expression_attribute_values(":p", AttributeValue::S(prefix))
        .scan_index_forward(false)
        .consistent_read(true)
        .limit(10)
        .send()
        .await;
    let s = |i: &std::collections::HashMap<String, AttributeValue>, k: &str| {
        i.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    match resp {
        Ok(r) => r
            .items()
            .iter()
            .map(|i| StoredReview {
                verdict: s(i, "verdict"),
                posted_as: s(i, "posted_as"),
                body: s(i, "body"),
            })
            .collect(),
        Err(e) => {
            warn!(owner, name, pr_number, error = %e, "Could not read the PR's reviews for a description edit");
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    const OLD: &str = "## Verification\n- [ ] unit tests";
    const NEW: &str =
        "## Verification\n- [ ] unit tests\n- restore of real data: 4,210 rows, 0 errors";

    fn edited(from: Option<&str>, body: &str, sender: Value) -> Value {
        let mut changes = json!({});
        if let Some(f) = from {
            changes["body"] = json!({ "from": f });
        }
        json!({
            "action": "edited",
            "changes": changes,
            "sender": sender,
            "repository": {"name": "web", "owner": {"login": "acme"}},
            "pull_request": {
                "number": 42, "state": "open", "draft": false, "body": body,
                "head": {"sha": "abc123"},
                "user": {"login": "dev", "id": 7, "type": "User"},
                "author_association": "MEMBER",
                "labels": [{"name": "coderhelm-review"}]
            }
        })
    }

    fn person() -> Value {
        json!({"login": "dev", "id": 7, "type": "User"})
    }

    fn review(verdict: &str, posted_as: &str, body: &str) -> StoredReview {
        StoredReview {
            verdict: verdict.into(),
            posted_as: posted_as.into(),
            body: body.into(),
        }
    }

    #[test]
    fn a_body_edit_by_a_person_counts() {
        let e = description_edit(&edited(Some(OLD), NEW, person())).expect("counts");
        assert_eq!(e.hash, desc::body_hash(NEW));
        // A PR that had no description: GitHub sends `from: null`.
        let mut p = edited(Some(OLD), NEW, person());
        p["changes"]["body"]["from"] = Value::Null;
        assert!(description_edit(&p).is_some());
    }

    #[test]
    fn title_only_edits_do_not_count() {
        let mut p = edited(None, NEW, person());
        p["changes"] = json!({"title": {"from": "old title"}});
        assert!(description_edit(&p).is_none());
        p["changes"] = json!({"base": {"ref": {"from": "main"}}});
        assert!(description_edit(&p).is_none());
    }

    #[test]
    fn bot_edits_do_not_count() {
        for sender in [
            json!({"login": "coderhelm[bot]", "id": 1, "type": "Bot"}),
            json!({"login": "coderhelm-dev[bot]", "id": 2, "type": "Bot"}),
            json!({"login": "release-drafter[bot]", "id": 3, "type": "Bot"}),
            json!({"login": "renovate", "id": 4, "type": "Bot"}),
            json!({}),
        ] {
            assert!(
                description_edit(&edited(Some(OLD), NEW, sender.clone())).is_none(),
                "{sender}"
            );
        }
    }

    #[test]
    fn trivial_edits_do_not_count() {
        let ticked = "## Verification\n- [x] unit tests";
        assert!(description_edit(&edited(Some(OLD), ticked, person())).is_none());
        let rewrapped = "## Verification\r\n-  [ ] unit tests\n\n";
        assert!(description_edit(&edited(Some(OLD), rewrapped, person())).is_none());
        let mut p = edited(Some(OLD), OLD, person());
        p["action"] = json!("synchronize");
        assert!(description_edit(&p).is_none());
    }

    #[test]
    fn only_a_review_that_waits_on_an_answer_is_rerun() {
        let confirm = "Changes requested\n\n#### Confirm before merge\n- Not run against a restore of real data.";
        assert!(standing_review_awaits_answer(&[review(
            "REQUEST_CHANGES",
            "REQUEST_CHANGES",
            confirm
        )]));
        assert!(standing_review_awaits_answer(&[review(
            "REQUEST_CHANGES",
            "REQUEST_CHANGES",
            "a defect"
        )]));
        // CoderHelm's own PR: posted as a comment review, same verdict.
        assert!(standing_review_awaits_answer(&[review(
            "REQUEST_CHANGES",
            "COMMENT",
            confirm
        )]));
        // The newest review counts: an approval after a request stands.
        assert!(!standing_review_awaits_answer(&[
            review("APPROVE", "APPROVE", "Looks good"),
            review("REQUEST_CHANGES", "REQUEST_CHANGES", confirm),
        ]));
        // An approval with notes doesn't wait on anything.
        assert!(!standing_review_awaits_answer(&[review(
            "APPROVE",
            "APPROVE",
            "#### Notes\n- x"
        )]));
        // Answers to questions aren't reviews; the review before them stands.
        assert!(standing_review_awaits_answer(&[
            review("QUESTION", "COMMENT", "An answer"),
            review("REQUEST_CHANGES", "REQUEST_CHANGES", confirm),
        ]));
        assert!(!standing_review_awaits_answer(&[]));
    }

    #[test]
    fn standing_is_the_editors_own() {
        // The author: GitHub's association for the PR author applies.
        let item = editor_item(&edited(Some(OLD), NEW, person()));
        assert!(common::standing::shown_by_github(&item));
        // Someone else edited: the author's association isn't theirs.
        let item = editor_item(&edited(
            Some(OLD),
            NEW,
            json!({"login": "drive-by", "id": 99, "type": "User"}),
        ));
        assert_eq!(item["user"]["login"], "drive-by");
        assert!(!common::standing::shown_by_github(&item));
        // An outside author has no standing from GitHub either.
        let mut p = edited(Some(OLD), NEW, person());
        p["pull_request"]["author_association"] = json!("CONTRIBUTOR");
        assert!(!common::standing::shown_by_github(&editor_item(&p)));
    }

    #[test]
    fn the_job_reviews_the_same_commit_with_the_description_trigger() {
        let p = edited(Some(OLD), NEW, person());
        let e = description_edit(&p).unwrap();
        let m = review_message(&p, "T", 9, "coderhelm-review", &e);
        assert_eq!(
            (m.repo_owner.as_str(), m.repo_name.as_str(), m.pr_number),
            ("acme", "web", 42)
        );
        assert_eq!(m.trigger, "description");
        assert_eq!(m.head_sha, "abc123");
        assert_eq!(m.dedup_key, format!("desc#abc123#{}", e.hash));
        assert_eq!(m.description_hash.as_deref(), Some(e.hash.as_str()));
        assert!(m.question.is_none());
    }

    /// The claims as the settings table keeps them: key to expiry.
    struct Claims {
        now: u64,
        held: HashMap<String, u64>,
    }

    impl Claims {
        fn claim(&mut self, sk: &str, ttl: u64) -> Claim {
            match self.held.get(sk) {
                Some(&until) if until >= self.now => Claim::Held,
                _ => {
                    self.held.insert(sk.to_string(), self.now + ttl);
                    Claim::Won
                }
            }
        }

        /// The gateway's steps for one counted edit; true when it queued.
        fn edit(&mut self, head: &str, body: &str) -> bool {
            let hash = desc::body_hash(body);
            let settle = self.claim(
                &desc::settle_sk("acme", "web", 42, head),
                desc::SETTLE_CLAIM_SECS,
            );
            let seen = (settle != Claim::Held).then(|| {
                self.claim(
                    &desc::seen_sk("acme", "web", 42, head, &hash),
                    desc::SEEN_TTL_SECS,
                )
            });
            let step = claim_step(&settle, seen.as_ref());
            if step == ClaimStep::AlreadyReviewed {
                self.held.remove(&desc::settle_sk("acme", "web", 42, head));
            }
            step == ClaimStep::Queue
        }
    }

    #[test]
    fn a_burst_of_edits_queues_one_review() {
        let mut c = Claims {
            now: 1_000,
            held: HashMap::new(),
        };
        assert!(c.edit("abc123", "first answer"));
        c.now += 30;
        assert!(!c.edit("abc123", "first answer, fixed a typo"));
        c.now += 60;
        assert!(!c.edit("abc123", "first answer, fixed a typo, and the row count"));
        // After the burst settled, a new answer is reviewed again.
        c.now += 300;
        assert!(c.edit("abc123", "a second answer"));
    }

    #[test]
    fn the_same_commit_and_description_are_reviewed_once() {
        let mut c = Claims {
            now: 1_000,
            held: HashMap::new(),
        };
        assert!(c.edit("abc123", "the answer"));
        c.now += 600;
        assert!(!c.edit("abc123", "the  answer"), "same text, reformatted");
        // Nothing waits after a skipped edit: a real change right after goes through.
        assert!(c.edit("abc123", "the answer, with the payload"));
        // A new commit is a new review of the same text.
        c.now += 600;
        assert!(c.edit("def456", "the answer"));
    }

    #[test]
    fn a_failed_claim_never_drops_the_review() {
        let failed = Claim::Failed("throttled".into());
        assert_eq!(claim_step(&failed, Some(&failed)), ClaimStep::Queue);
        assert_eq!(claim_step(&Claim::Won, Some(&Claim::Won)), ClaimStep::Queue);
        assert_eq!(claim_step(&Claim::Held, None), ClaimStep::Settling);
        assert_eq!(
            claim_step(&Claim::Won, Some(&Claim::Held)),
            ClaimStep::AlreadyReviewed
        );
    }
}
