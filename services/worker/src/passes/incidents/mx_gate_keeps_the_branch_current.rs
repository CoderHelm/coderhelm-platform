//! mx. A person approved someone's pull request (21:29:27) and the
//! auto-merge gate armed. At 21:29:30 it found the pull request conflicting
//! with its base and stopped (`updating:false`): on a person's pull request
//! it never brought the base in. At 21:41 a person pushed the conflict
//! resolution; CoderHelm reviewed only the pull request's own change since
//! its approval and approved 0516eab. The gate armed at 21:45:51 and
//! stopped a second later "waiting on approvals": the person's approval
//! was of 78e785a, and the gate counted only approvals of the head, though
//! GitHub still counted it. Nothing woke it; the person dismissed their
//! approval and approved again to move it.
//!
//! Guarantee: a conflict or a branch behind its base is CoderHelm's
//! maintenance on anyone's pull request, done as soon as the gate is armed;
//! the person key follows GitHub's own required-review rule; a wait on a
//! person names exactly what is missing, and the gate reads again on a timer
//! while it waits.

use super::fixture::fn_src;
use crate::passes::await_merge::{
    base_merge_line, decide_gate, missing_person_line, needs_base, person_key, review_rules,
    wait_message, Action, BaseMerge, BaseNeed, GateView, Mark, ReviewRules,
};
use crate::passes::review_actions::approval_state;
use common::merge_gate::STATE_WAITING_APPROVAL;
use serde_json::{json, Value};

const APPROVED: &str = "78e785a1c0ffee00000000000000000000000000";
const HEAD: &str = "0516eab2c0ffee00000000000000000000000000";

fn src() -> &'static str {
    include_str!("../await_merge.rs")
}

/// The reviews at 21:45:52: the person's approval of the earlier commit and
/// CoderHelm's approval of the head.
fn reviews(person_state: &str) -> Value {
    json!([
        {"user": {"login": "a-person"}, "state": person_state, "commit_id": APPROVED},
        {"user": {"login": "coderhelm[bot]"}, "state": "APPROVED", "commit_id": APPROVED},
        {"user": {"login": "coderhelm[bot]"}, "state": "APPROVED", "commit_id": HEAD},
    ])
}

fn view(person_state: &str, decision: Option<&str>) -> GateView {
    GateView {
        draft: false,
        require_human: true,
        bot_verdict: Some("APPROVE".into()),
        bot_holding: vec![],
        approvals: approval_state(&reviews(person_state), HEAD),
        want_labels: vec![],
        present_labels: vec![],
        labels_added_at: None,
        taken_off: vec![],
        preview_ran: vec![],
        ci: None,
        now: 1_000,
        head: HEAD.into(),
        base: "main".into(),
        review_decision: decision.map(str::to_string),
        earlier_approval_holds: false,
        rules: ReviewRules::default(),
        requested: vec![],
        base_merge_at_head: false,
    }
}

#[test]
fn a_conflict_on_a_persons_pr_is_brought_in_by_coderhelm_not_a_stop() {
    // 21:29:30: GitHub reports the pull request conflicting.
    let pr = json!({"mergeable": false, "mergeable_state": "dirty"});
    assert_eq!(needs_base(&pr), Some(BaseNeed::Conflict));
    assert_eq!(
        needs_base(&json!({"mergeable_state": "behind"})),
        Some(BaseNeed::Behind)
    );
    assert_eq!(needs_base(&json!({"mergeable_state": "clean"})), None);
    // The gate brings the base in as soon as it is armed, before reading
    // the approvals, and again when GitHub declines the merge for it.
    let eval = fn_src(src(), "async fn evaluate(");
    let at = |needle: &str| eval.find(needle).unwrap_or_else(|| panic!("{needle}"));
    assert!(at("needs_base(&pr)") < at("fetch_approvals("));
    assert!(fn_src(src(), "async fn merge(").contains("keep_current("));
    // On anyone's PR: only the run's conflict round is CoderHelm's own; the
    // base merge itself never asks who wrote the PR.
    let keep = fn_src(src(), "async fn keep_current(");
    assert!(keep.contains("bring_base_in("));
    assert!(!fn_src(src(), "async fn bring_base_in(").contains("self_authored"));
    // The same conflict resolution runs use, and the reviewer's comparison
    // of the PR's own change.
    let bring = fn_src(src(), "async fn bring_base_in(");
    assert!(bring.contains("pr::resolve_conflicts("));
    assert!(bring.contains("review_scope::same_own_change("));
    // After the merge the gate goes on on the new head.
    assert!(keep.contains("arm("));
    let (mark, text) = base_merge_line(
        "main",
        &BaseMerge::Merged {
            sha: HEAD.into(),
            own_unchanged: true,
        },
    );
    assert_eq!(mark, Mark::Waiting);
    assert!(text.contains("CoderHelm merged `main` into this branch (0516eab)"));
    assert!(text.contains("the approval carries to it"));
    // Only a conflict it can't resolve is reported, with the files, once
    // per head (`claim_base_update`).
    let (mark, text) = base_merge_line(
        "main",
        &BaseMerge::Unresolved {
            files: vec!["src/schedule.py".into()],
        },
    );
    assert_eq!(mark, Mark::Blocked);
    assert!(text.contains("`src/schedule.py`"));
    assert!(keep.contains("claim_base_update("));
}

#[test]
fn an_approval_github_still_counts_is_the_person_key() {
    // 21:45:52: the person's approval of 78e785a stands on GitHub.
    let mut v = view("APPROVED", Some("APPROVED"));
    assert!(v.approvals.approved_at_head.is_empty());
    assert!(person_key(&v));
    assert_eq!(decide_gate(&v).action, Action::ReadCi);
    v.ci = Some(Default::default());
    assert_eq!(decide_gate(&v).action, Action::Merge);
    // No required-review rule: the approval counts when every commit since
    // only brought the base in.
    let mut v = view("APPROVED", None);
    assert!(!person_key(&v));
    v.earlier_approval_holds = true;
    assert!(person_key(&v));
    // GitHub saying more reviews are needed is never outvoted.
    let v = view("APPROVED", Some("REVIEW_REQUIRED"));
    assert!(!person_key(&v));
}

#[test]
fn a_wait_on_a_person_names_exactly_what_is_missing() {
    // Where the rules dismiss approvals on push.
    let v = view("DISMISSED", Some("REVIEW_REQUIRED"));
    let d = decide_gate(&v);
    assert_eq!(d.action, Action::WaitForPeople);
    assert_eq!(d.state, STATE_WAITING_APPROVAL);
    let (_, line) = missing_person_line(&v, true);
    assert_eq!(
        line,
        "@a-person's approval of 78e785a was dismissed when new commits were pushed (the head \
         is now 0516eab); a re-approval of 0516eab is all that's left"
    );
    assert!(d.lines.iter().any(|(_, l)| l == &line));
    // When the push was CoderHelm's own base merge, it says so.
    let mut v = view("DISMISSED", Some("REVIEW_REQUIRED"));
    v.base_merge_at_head = true;
    assert!(missing_person_line(&v, true)
        .1
        .contains("dismissed by CoderHelm's merge of `main` at 0516eab"));
    // The branch's required count, and that CoderHelm's approval doesn't
    // count toward it.
    let mut v = view("COMMENTED", Some("REVIEW_REQUIRED"));
    v.rules.required_approvals = Some(2);
    v.requested = vec!["a-reviewer".into()];
    let (_, line) = missing_person_line(&v, true);
    assert!(
        line.contains("need 2 approving reviews from people"),
        "{line}"
    );
    assert!(line.contains("CoderHelm's approval doesn't count"));
    assert!(line.contains("@a-reviewer"));
    // A pending review request is named, never counted as a missing
    // approval once GitHub's rule is met.
    let mut v = view("APPROVED", Some("APPROVED"));
    v.requested = vec!["a-reviewer".into()];
    assert!(person_key(&v));
    // The line goes on the run page (the record's detail) and the one
    // status comment; waiting on people never goes to a team channel
    // (`nt_alerts_channel_alerts_only`).
    let m = wait_message("acme", "web", 7, "Fix the schedule", "https://x/7", &line);
    assert!(m.summary.contains("acme/web#7"));
    assert_eq!(m.buttons.len(), 1);
    let eval = fn_src(src(), "async fn evaluate(");
    assert!(!eval.contains("tell_team("));
    assert!(eval.contains("find(|(m, _)| *m != Mark::Done)"));
}

#[test]
fn the_rules_are_read_from_protection_and_rulesets() {
    let protection = json!({"required_pull_request_reviews": {
        "required_approving_review_count": 1, "dismiss_stale_reviews": true}});
    let rulesets = json!([{"type": "pull_request", "parameters": {
        "required_approving_review_count": 2, "dismiss_stale_reviews_on_push": false}}]);
    assert_eq!(
        review_rules(Some(&protection), Some(&rulesets)),
        ReviewRules {
            required_approvals: Some(2),
            dismiss_stale: Some(true),
        }
    );
    assert_eq!(review_rules(None, None), ReviewRules::default());
}

#[test]
fn waiting_on_people_still_reads_again_on_a_timer() {
    // "stopping until the next review event" is not the only wake-up.
    let eval = fn_src(src(), "async fn evaluate(");
    let wait = &eval[eval.find("Action::WaitForPeople =>").unwrap()..];
    let wait = &wait[..wait.find("Action::ReadCi =>").unwrap()];
    assert!(wait.contains("Ok(Next::Recheck)"), "{wait}");
    assert!(!wait.contains("Ok(Next::Stop)"));
    let run = fn_src(src(), "pub async fn run(");
    assert!(run.contains("recheck_people("));
    let recheck = fn_src(src(), "async fn recheck_people(");
    assert!(recheck.contains("PEOPLE_RECHECK_SECS"));
    assert!(recheck.contains("PEOPLE_WATCH_SECS"));
    // Bound to the chain: a newer arming or a stop ends the timer.
    assert!(recheck.contains("chain_id = :c"));
}

#[test]
fn the_gate_queues_the_review_of_its_own_base_merge_and_the_approval_carries() {
    use crate::passes::review_pr::base_merge_review;
    use crate::passes::review_scope::{approved_base, same_own_change, TRIGGER_PUSH};
    use crate::passes::review_sweep::ReviewRecord;
    // The gate merged `main` into an unlabelled PR's branch, making HEAD.
    // It queues CoderHelm's review of HEAD itself: no push webhook review
    // runs on a PR without the label, and the gate never waits on a review
    // it caused.
    let keep = fn_src(src(), "async fn keep_current(");
    let at = |needle: &str| keep.find(needle).unwrap_or_else(|| panic!("{needle}"));
    assert!(at("review_after_base_merge(") < at("arm("));
    let review_pr = include_str!("../review_pr.rs");
    let queue = fn_src(review_pr, "pub(crate) async fn review_after_base_merge(");
    assert!(queue.contains("enqueue_review("));
    // Review on for the repo is all it asks; the PR's labels are never read.
    assert!(!queue.contains("labels"));
    let m = base_merge_review("team", 1, ("acme", "web", 7), HEAD, "coderhelm-review");
    assert_eq!(m.trigger, TRIGGER_PUSH);
    assert_eq!(m.head_sha, HEAD);
    // Keyed like the gateway's push review: a labelled PR is reviewed once.
    assert_eq!(m.dedup_key, format!("head#{HEAD}"));
    // The review takes the carry-forward path: CoderHelm's latest review
    // approved 78e785a in full.
    let records = vec![ReviewRecord {
        verdict: "APPROVE".into(),
        posted_as: "APPROVE".into(),
        head_sha: APPROVED.into(),
        created_at: "2026-11-18T21:29:00Z".into(),
        body: "Looks right.".into(),
        posted_body: "**Approved.** What was checked: the schedule's window.".into(),
        ..Default::default()
    }];
    assert_eq!(
        approved_base(&m.trigger, m.question.is_some(), &records, &m.head_sha),
        Some(APPROVED.to_string())
    );
    // The PR's own change is the one approved (line numbers moved with the
    // base), so the approval is carried with that review, and the carry
    // arms the gate on HEAD.
    let own = |patch: &str| json!({"files": [{"filename": "src/schedule.py", "status": "modified", "patch": patch}]});
    assert!(same_own_change(
        &own("@@ -10,2 +10,2 @@\n-window = 1\n+window = 2"),
        &own("@@ -14,2 +14,2 @@\n-window = 1\n+window = 2"),
    ));
    let carried = &review_pr[review_pr
        .find("review skipped, approval carried")
        .expect("the carry path")..];
    let carried = &carried[..carried.find("let reason =").unwrap()];
    assert!(carried.contains("carry_approval("));
    assert!(carried.contains("run_on_approve("));
    // Armed on HEAD: CoderHelm's carried approval, the person's approval
    // GitHub keeps, green CI: it merges.
    let mut v = view("APPROVED", Some("APPROVED"));
    v.base_merge_at_head = true;
    v.ci = Some(Default::default());
    assert_eq!(decide_gate(&v).action, Action::Merge);
}
