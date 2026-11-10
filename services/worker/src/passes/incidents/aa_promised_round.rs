//! aa. A ticket run's own review requested changes on its pull request.
//! The run took review-fix rounds until its automatic fix budget was spent,
//! handed the pull request over with two of its own review's findings still
//! listed, and finished. The ready decision then held the draft every hour
//! for three days on "CoderHelm fixes them in its review rounds; fix them on
//! the branch, or remove a finding from the list once a person has judged
//! it." No round ever ran after the run was done: the hold promised work
//! nothing queued, and the run page told the person CoderHelm was on it.
//!
//! Guarantees:
//! - A hold never promises CoderHelm work that isn't queued.
//! - A finished run's draft held on its own review's findings queues one
//!   review-fix round for them, once per head, while the run has rounds
//!   left (its own-review rounds, its automatic fix pushes and its fix
//!   cycles), and waits for it: "CoderHelm is fixing N findings from its
//!   review", a wait bounded like every other.
//! - With no round left, after the run stopped its rounds itself, or once a
//!   round ran on the head and the findings are still there, the hold is a
//!   person's: what is open and what they do, no CoderHelm work promised.
//! - The other holds say what CoderHelm does only where something is queued
//!   to do it: the security audit and the planned-work rounds don't run
//!   again once the run is over, so those holds name what a person does.
//! - A review of a finished run's pull request leaves its findings to the
//!   ready decision instead of also feeding them back to the run, so they
//!   are never fixed twice or outside the run's round budget.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use common::draft_hold::{DraftHold, Kind, Reason};

use super::fixture::fn_src;
use crate::passes::own_review::{self, RoundBudget};
use crate::passes::ready::{self, Ci, Decision, Facts};
use crate::passes::resume;

/// What the hold said for three days, with nothing queued behind it.
const OLD_PROMISE: &str = "CoderHelm fixes them in its review rounds";

const HEAD: &str = "b0413fa";

fn body() -> String {
    format!(
        "{}\n\n## Summary\nRe-index rows written behind the sync cursor.",
        own_review::section(&[
            "The backfill skips rows with no parent (`sync/index.py:88`)".to_string(),
            "The cursor reads the source's own timestamp (`sync/cursor.py:41`)".to_string(),
        ])
    )
}

fn facts(own_fixing: bool) -> Facts {
    Facts {
        draft: true,
        body: body(),
        title: "fix(sync): re-index rows written behind the cursor".into(),
        ci: Ci {
            green: true,
            why: String::new(),
        },
        head: HEAD.into(),
        own_fixing,
        ..Default::default()
    }
}

/// The run record as it was: finished, waiting on a person, its automatic
/// fix pushes spent, no own-review round recorded.
fn record(status: &str, pushes: u32) -> HashMap<String, AttributeValue> {
    let s = |v: &str| AttributeValue::S(v.into());
    let n = |v: u32| AttributeValue::N(v.to_string());
    HashMap::from([
        ("status".to_string(), s(status)),
        ("auto_fix_pushes".to_string(), n(pushes)),
        ("own_review_open".to_string(), n(2)),
        ("own_review_stalled".to_string(), n(0)),
        ("ci_end".to_string(), s("budget")),
    ])
}

fn says_coderhelm_works(text: &str) -> bool {
    [
        OLD_PROMISE,
        "CoderHelm keeps working",
        "CoderHelm checks again on the next push",
    ]
    .iter()
    .any(|p| text.contains(p))
}

#[test]
fn with_no_round_left_the_hold_is_a_persons_and_promises_nothing() {
    // The run's budget as its record shows it: no round may be queued.
    let budget = resume::own_round_budget(&record("needs_input", 8), 0);
    let why = own_review::round_left(&budget, HEAD, 8, 10).unwrap_err();
    assert!(why.contains("fix budget is spent"), "{why}");

    let d = ready::decide(&facts(false));
    let Decision::Held { reasons } = &d else {
        panic!("{d:?}")
    };
    assert_eq!(reasons.len(), 1);
    assert_eq!(
        reasons[0].line(),
        "2 findings from CoderHelm's review are still open. Fix them on the branch, or remove a \
         finding from the \"Open review findings\" list once you've judged it."
    );
    let status = ready::status_section(&d);
    assert!(!says_coderhelm_works(&status), "{status}");
    // The description's own section promises nothing either.
    assert!(!body().contains("It fixes them"));
}

#[test]
fn with_rounds_left_one_round_is_queued_per_head_and_the_draft_waits_for_it() {
    // The same run with fix pushes left: a round may be queued.
    let budget = resume::own_round_budget(&record("needs_input", 3), 2);
    assert_eq!(own_review::round_left(&budget, HEAD, 8, 10), Ok(()));
    // A finished run only: one still working has its own loop.
    let working = resume::own_round_budget(&record("awaiting_ci", 3), 2);
    assert!(own_review::round_left(&working, HEAD, 8, 10).is_err());

    let d = ready::decide(&facts(true));
    assert_eq!(
        d,
        Decision::Wait {
            why: "CoderHelm is fixing 2 findings from its review".into(),
            until: None
        }
    );
    // A round queued for the head is never queued again on it: once it ran
    // and the findings are still there, a person has them.
    let mut again = record("completed", 4);
    again.insert(
        resume::OWN_FIX_HEAD.to_string(),
        AttributeValue::S(HEAD.into()),
    );
    let ran = resume::own_round_budget(&again, 3);
    assert_eq!(
        own_review::round_left(&ran, HEAD, 8, 10).unwrap_err(),
        "a fix round already ran on this head"
    );
    // A new head (the round pushed) may have its own.
    assert_eq!(own_review::round_left(&ran, "c71e2d0", 8, 10), Ok(()));
}

#[test]
fn every_round_budget_bounds_it() {
    let base = RoundBudget {
        status: "completed".into(),
        ..Default::default()
    };
    assert_eq!(own_review::round_left(&base, HEAD, 8, 10), Ok(()));
    for (b, why) in [
        (
            RoundBudget {
                rounds: own_review::MAX_FIX_ROUNDS,
                ..base.clone()
            },
            "fix rounds",
        ),
        (
            RoundBudget {
                cycles: 10,
                ..base.clone()
            },
            "fix budget",
        ),
        (
            RoundBudget {
                handed: true,
                ..base.clone()
            },
            "handed the findings to a person",
        ),
        (
            RoundBudget {
                guard_open: true,
                ..base.clone()
            },
            "run guard",
        ),
    ] {
        let e = own_review::round_left(&b, HEAD, 8, 10).unwrap_err();
        assert!(e.contains(why), "{e}");
    }
    assert!(own_review::round_left(&base, "", 8, 10).is_err());
}

#[test]
fn the_wait_for_a_round_is_bounded_like_every_wait() {
    let why = own_review::fixing_why(2);
    let t = |s: &str| {
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap()
            .with_timezone(&chrono::Utc)
    };
    let mut prior = DraftHold::new(Kind::Waiting, vec![Reason::new(&why, "")], 564, "").unwrap();
    prior.since = "2026-11-10T03:31:22Z".into();
    prior.head = HEAD.into();
    let mut f = facts(true);
    f.prior = Some(prior);
    f.now = Some(t("2026-11-10T04:31:00Z"));
    assert!(matches!(ready::decide(&f), Decision::Wait { .. }));
    f.now = Some(t("2026-11-10T05:32:00Z"));
    let d = ready::decide(&f);
    let Decision::Held { reasons } = &d else {
        panic!("{d:?}")
    };
    assert!(reasons[0]
        .what
        .contains("hasn't changed in 2 hours on this head"));
}

#[test]
fn no_other_hold_promises_work_nothing_queues() {
    let security = format!(
        "{}\n## Open items\n- The token check. {}\n{}",
        crate::passes::proof::UNVERIFIED_START,
        crate::passes::proof::SECURITY_MARK,
        crate::passes::proof::UNVERIFIED_END
    );
    let unfinished = crate::passes::proof::unfinished_section(&[crate::passes::proof::Finding {
        point: "planned_work".into(),
        detail: "`apps/web/src/card.tsx` is not written.".into(),
        ..Default::default()
    }]);
    for b in [security, unfinished.clone(), body()] {
        let holds = ready::body_holds_on(&b, false);
        assert!(!holds.is_empty(), "{b}");
        for r in holds {
            assert!(!says_coderhelm_works(&r.line()), "{}", r.line());
        }
    }
    assert!(!says_coderhelm_works(&unfinished), "{unfinished}");
}

#[test]
fn the_pipeline_queues_the_round_and_routes_finished_runs_to_it() {
    let ready_src = include_str!("../ready.rs");
    let settle = fn_src(ready_src, "pub async fn settle(");
    let ask = settle
        .find("fix_own_findings(ctx, &run_id, &mut facts)")
        .unwrap();
    let decide = settle.find("let decision = decide(&facts);").unwrap();
    assert!(ask < decide, "the round is queued before the decision");
    assert!(fn_src(ready_src, "async fn fix_own_findings(").contains("queue_own_review_round("));

    let resume_src = include_str!("../resume.rs");
    // The run's own stop marks its rounds as over, so none is queued after.
    assert!(fn_src(resume_src, "async fn stop_on_own_findings(").contains("OWN_REVIEW_HANDED"));
    let queue = fn_src(resume_src, "pub(crate) async fn queue_own_review_round(");
    assert!(queue.contains("claim_auto_fix_slot("));
    assert!(queue.contains("Kind::ReviewFix"));

    let review_src = include_str!("../review_pr.rs");
    assert!(review_src.contains("super::resume::run_finished("));
    assert!(!ready_src.contains(&format!("\"{OLD_PROMISE}")));
}
