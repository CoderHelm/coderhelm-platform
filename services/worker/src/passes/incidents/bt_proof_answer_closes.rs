//! bt. A security-updates run opened a draft pull request and CoderHelm's
//! own review asked for changes on its head with one finding: "Build and
//! unit-test proof is from an earlier commit, not the head". The finding was
//! about proof, not code, so the fix rounds changed nothing: the second
//! round ran the build, lint, type-check and unit tests on the head, all
//! passing, and wrote the results in its account. One more check it started
//! (loading a native module at runtime) was cut short by the step's time.
//! The ledger recorded the finding as blocked for lack of time, the round
//! pushed nothing so no review read the head again, the run completed, and
//! the draft was held "for a person": "1 finding from CoderHelm's review is
//! still open. Fix them on the branch, or remove a finding". The merge
//! standing beside it said no person was needed. It stayed that way for
//! thirteen hours.
//!
//! Guarantee: a round that pushed nothing is read by a review of the same
//! head, with the round's account in the reviewer's context, and a finding
//! answered with proof closes there. A check cut short by a step's time is
//! owed, never a block: the round continues in a fresh step that runs it
//! first. Nothing on CoderHelm's side (time, round caps, budgets, its own
//! limits) holds a draft for a person: what is truly left after every
//! approach is listed on the pull request as CoderHelm's note and the pull
//! request goes ready. An already-stuck run recovers on the next look of
//! the ready sweep or the pull request's watch.

use super::fixture::*;
use crate::passes::own_review_ledger::{
    self as ledger, Approach, Blocker, Completion, Ledger, Plan, Settled,
};

const OLD: &str = "Fix them on the branch, or remove a finding";
const HEAD: &str = "4b1d0c9e2f7a";

/// The finding, as the review listed it.
const PROOF: &str =
    "Build and unit-test proof is from an earlier commit, not the head (`package.json:200`)";

/// What the second round wrote: every check on the head passing, one
/// runtime check cut short by the step's time.
const ACCOUNT: &str = "I re-ran the build, lint, type-check and unit tests on the head commit \
4b1d0c9, and all four pass. There were no code changes this round; the finding was about the \
proof, not the code.\n- Build: passed (`pnpm run build`, exit 0).\n- Unit tests: passed \
(`jest --ci`, 188 suites passed).\nEvidence:\n- Confirmed: the sandbox checked out 4b1d0c9.\n- \
Unconfirmed [external:native module at runtime]: whether the module loads and runs a query. It \
didn't run because the sandbox ran out of time this step.";

/// The log line the cut-short check left.
fn cut_short_log() -> Vec<serde_json::Value> {
    vec![serde_json::json!({"content": "not run: not enough time left"})]
}

/// The run's ledger as it was stored: one finding, one round observed and
/// recorded as blocked for time, and the second round ended with no review
/// reading it.
fn the_stuck_ledger() -> Ledger {
    let json = serde_json::json!({
        "findings": [{
            "item": PROOF, "attempts": 1, "stalled": 0, "blocked": 1,
            "last_blocked": ["no_time"], "person_claims": 0, "person_reason": "",
            "person_what": "", "settled": null
        }],
        "last": {"taken": [PROOF], "blocked": ["no_time"], "account": ACCOUNT, "ended": true},
        "continues": 1
    });
    Ledger::of_json(&json.to_string())
}

#[test]
fn a_check_cut_short_by_time_is_resumed_never_blocked() {
    let mut l = Ledger::default();
    let open = vec![PROOF.to_string()];
    l.observe(&open, "");
    // Round after round cut short by the step's time: past the block cap,
    // the finding still gets a fresh round, never a hand-back.
    for n in 1..=ledger::MAX_BLOCKED + 1 {
        let Plan::Round { focus, .. } = l.decide(&open, 0.0, 200.0) else {
            panic!("round {n}: another round, not an ending");
        };
        l.start_round(&focus);
        l.end_round(ledger::blockers_of(&[], &cut_short_log()), ACCOUNT);
        l.observe(&open, "");
        let t = &l.findings[0];
        assert_eq!(t.blocked, 0, "time is not a block: {t:?}");
        assert_eq!(t.cut_short, n);
        match l.decide(&open, 0.0, 200.0) {
            Plan::Round {
                approach: Approach::Unblock(b),
                fresh,
                unscoped,
                focus,
            } => {
                assert_eq!(b, vec![Blocker::NoTime]);
                assert!(fresh, "it starts with its full time");
                assert!(!unscoped, "nothing was refused");
                let told = ledger::instruction(&Approach::Unblock(b), &focus);
                assert!(told.contains("fresh step with its full time"), "{told}");
                assert!(told.contains("first"), "{told}");
            }
            other => panic!("round {n}: {other:?}"),
        }
    }
    // The stored record of the stuck run: its time block is not counted
    // again, and the next round starts fresh.
    let mut stuck = the_stuck_ledger();
    stuck.observe(&open, "");
    assert_eq!(
        (stuck.findings[0].blocked, stuck.findings[0].cut_short),
        (1, 1)
    );
    assert!(matches!(
        stuck.decide(&open, 0.0, 200.0),
        Plan::Round { fresh: true, .. }
    ));
}

#[test]
fn a_step_too_short_for_a_check_owes_it_to_the_next_step() {
    use crate::passes::implement::{owed_first, OWED_CHECKS};
    let base = "CoderHelm's own review found the issues below.";
    let once = owed_first(
        base,
        &["node -e \"require('mod')\"".to_string(), OWED_CHECKS.into()],
    );
    assert!(once.starts_with(base));
    assert!(once.contains("Run each first"), "{once}");
    assert!(once.contains("`node -e \"require('mod')\"`"));
    // Continued twice: the newest list, once.
    let twice = owed_first(&once, &["pnpm test".to_string()]);
    assert_eq!(twice.matches("Owed from the step before").count(), 1);
    assert!(twice.contains("`pnpm test`") && !twice.contains("require('mod')"));
    assert_eq!(owed_first(&twice, &[]), base);
    // The coder's command, `run_checks` and codegen owe what they can't
    // start, and a pass that owes anything continues in a fresh step.
    let implement = include_str!("../implement.rs");
    assert_eq!(
        implement.matches("self.owe(").count(),
        3,
        "every refusal for time owes"
    );
    assert!(implement.contains("|| !owed.is_empty()"));
    assert!(!implement.contains("finalize your changes now"));
    let resume = include_str!("../resume.rs");
    let round = fn_src(resume, "\nasync fn run_fix_round(");
    assert!(round.contains("owed_first(&round.feedback, &r.owed)"));
}

#[test]
fn a_round_that_pushed_nothing_is_read_by_a_review_of_the_same_head() {
    let mut l = the_stuck_ledger();
    assert!(
        l.round_unread(),
        "the second round's account was never read"
    );
    // The reviewer is given the round's account and the rule for proof.
    let account = l.last.as_ref().map(|r| r.account.clone()).unwrap();
    let section = crate::passes::review_pr::round_answer_section(&account);
    assert!(section.contains("all four pass"));
    assert!(section.contains("fixed when the proof is of this head"));
    assert!(section.contains("only when the change rests on it"));
    assert!(crate::passes::review_pr::round_answer_section("  ").is_empty());
    // The review accepts the proof: the finding is fixed and the head is
    // approved, so the run completes and the pull request goes ready.
    l.observe(&[], "");
    assert!(!l.round_unread());
    assert_eq!(l.findings[0].settled, Some(Settled::Fixed));
    assert_eq!(l.decide(&[], 0.0, 200.0), Plan::Ready);
    assert_eq!(
        ledger::completion(Some(("APPROVE", HEAD)), HEAD, &l),
        Completion::Complete
    );
    // Wired: a finished run's held draft sends the same head to review
    // before anything else, the parked round's end goes back to review
    // instead of completing, and the completion guard reviews again rather
    // than rerunning checks that never review.
    let resume = include_str!("../resume.rs");
    let queue = fn_src(resume, "\npub(crate) async fn queue_own_review_round(");
    let unread = queue
        .find("ledger.round_unread()")
        .expect("reads the round first");
    assert!(unread < queue.find("round_left(").unwrap());
    assert!(queue.contains("review_head_again("));
    assert!(!queue.contains("#fh <> :h"), "no once-per-head rule");
    let pending = fn_src(resume, "\nasync fn run_pending_round(");
    let review_fix = pending
        .find("kind == run_round::Kind::ReviewFix")
        .expect("a review-fix round has its own ending");
    assert!(review_fix < pending.find("mark_pr_ready(").unwrap());
    let guard = fn_src(resume, "\nasync fn own_review_completion(");
    assert!(guard.contains("review_head_again("));
    assert!(!guard.contains("recheck_at_checks("));
}

#[test]
fn what_is_left_after_every_approach_is_a_note_on_a_ready_pull_request() {
    use crate::passes::ready::{self, Ci, Decision, Facts};
    let open = vec![PROOF.to_string()];
    let mut l = Ledger::default();
    l.observe(&open, "");
    for _ in 0..ledger::MAX_STALLED {
        let Plan::Round { focus, .. } = l.decide(&open, 0.0, 200.0) else {
            panic!("a round");
        };
        l.start_round(&focus);
        l.end_round(vec![], "Tried.");
        l.observe(&open, "");
    }
    let Plan::Unfixed { why } = l.decide(&open, 0.0, 200.0) else {
        panic!("every approach taken");
    };
    // CoderHelm's limit: its note, never a hold on the run.
    assert!(matches!(
        ledger::completion(Some(("REQUEST_CHANGES", HEAD)), HEAD, &l),
        Completion::Defer(_)
    ));
    l.note(&open, &why);
    let Plan::Settle {
        deferred,
        by_ticket,
        noted,
    } = l.decide(&open, 0.0, 200.0)
    else {
        panic!("settled as a note");
    };
    assert!(deferred.is_empty() && by_ticket.is_empty());
    let lines = ledger::settled_lines(&deferred, &by_ticket, &noted);
    assert!(lines[0].contains("CoderHelm's note"), "{lines:?}");
    assert!(!lines[0].contains("needs a person"));
    // Its review no longer lists it as holding the draft, and never
    // approves over it.
    let mut findings: Vec<crate::passes::review_agent::Finding> =
        serde_json::from_value(serde_json::json!([{
            "file": "package.json", "line": 200, "severity": "medium",
            "category": "correctness", "evidence": "verified-defect",
            "title": "Build and unit-test proof is from an earlier commit, not the head",
            "body": "The results quoted are of an older commit."
        }]))
        .unwrap();
    let mut own_open = open.clone();
    let settled = crate::passes::review_pr::settle_own_findings(&l, &mut findings, &mut own_open);
    assert!(own_open.is_empty());
    assert!(settled.is_empty(), "a note never clears the merge hold");
    assert!(findings[0].holds_merge());
    // A completion on that verdict completes: nothing more to do.
    assert_eq!(
        ledger::completion(Some(("REQUEST_CHANGES", HEAD)), HEAD, &l),
        Completion::Complete
    );
    // The pull request: the open-findings section goes, the note is listed,
    // and the ready decision marks it ready.
    let body = format!(
        "{}\n\n## Summary\nSecurity updates.\n<!-- coderhelm:security -->",
        crate::passes::own_review::section(&open)
    );
    let now = crate::passes::own_review::defer_findings(&body, &lines, "CoderHelm's notes");
    assert!(crate::passes::own_review::hold(&now).is_none());
    assert!(now.contains("## Deferred"));
    let f = Facts {
        draft: true,
        body: now,
        head: HEAD.into(),
        security: true,
        ci: Ci {
            green: true,
            why: String::new(),
        },
        ..Default::default()
    };
    assert!(matches!(ready::decide(&f), Decision::Ready { .. }));
    let mut end = Ending {
        status: "completed".into(),
        ..Default::default()
    };
    end.said(lines.join("\n"));
    end.never_says(OLD);
}

#[test]
fn no_coderhelm_limit_holds_the_draft_for_a_person() {
    use crate::passes::ready::{self, Ci, Decision, Facts};
    use common::draft_hold::{DraftHold, Kind};
    // Findings listed while nothing is queued this moment: CoderHelm takes
    // them on its next look, a wait that needs no person.
    let body = crate::passes::own_review::section(&[PROOF.to_string()]);
    let f = Facts {
        draft: true,
        body,
        head: HEAD.into(),
        security: true,
        own_fixing: true,
        ci: Ci {
            green: true,
            why: String::new(),
        },
        ..Default::default()
    };
    let Decision::Wait { why, .. } = ready::decide(&f) else {
        panic!("a wait, not a hold");
    };
    let hold = DraftHold::new(
        Kind::Waiting,
        vec![common::draft_hold::Reason::new(&why, "")],
        42,
        "",
    )
    .unwrap();
    assert!(!hold.needs_person());
    assert!(!hold.summary().contains(OLD));
    // Every way out of the queue that isn't a round is a wait or a note:
    // no "person" ending is left.
    let resume = include_str!("../resume.rs");
    assert!(!resume.contains("OwnRound::Person"));
    let ready_src = include_str!("../ready.rs");
    let fix = fn_src(ready_src, "\nasync fn fix_own_findings(");
    assert!(fix.contains("OwnRound::Later(why)") && fix.contains("f.own_fixing = true"));
    // The in-run ending for findings left after every approach lists them
    // and finishes, never waiting on a person.
    assert!(resume.contains("note_own_findings(state, &msg.team_id, &msg.run_id, &own_open, &why)"));
}
