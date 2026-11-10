//! w. A run finished and its pull request stayed a draft. Under Open items a
//! line marked as a check the change didn't pass said "the change makes the
//! staging content its end-to-end tests read out of date". The rule that
//! wrote it was wrong and was fixed, but the line was text in the
//! description: nothing ran the check again when the code that wrote it
//! changed, so the pull request stayed held on every sweep until a person
//! edited the description. A mention of CoderHelm on the pull request only
//! resumes a fix round's unfinished work, and this one had none.
//!
//! The same run spent three fresh steps doing nothing. Each step started by
//! rebuilding its stacked branch, the plan check at the start of the step
//! measured the branch's own diff (23 of 38 tasks) and the check after the
//! coder measured with the stacked base (37 of 38). The run guard counted
//! each return to 37 as work done again, so it never stopped the steps,
//! though the coder wrote nothing in any of them.
//!
//! Guarantees:
//! - Every blocked line names the version of the code that wrote it. A pull
//!   request held on a line other code wrote is checked again on its head by
//!   a fresh step of the run, once per run and code version; the checks whose
//!   failure is a hold run again even on an unchanged head, and the Open
//!   items are written from what they find.
//! - A line the running code wrote never starts another check: no loop.
//! - A fall in the plan check's count and its return count as progress only
//!   when the run's coder wrote code in between. Every plan check measures
//!   with the stacked base.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use super::fixture::fn_src;
use crate::passes::completeness::{self, Section};
use crate::passes::proof;
use crate::passes::ready::{self, Ci, Decision, Facts};
use crate::passes::recheck;
use crate::passes::run_guard::{GuardState, Ledger, Progress, Trip};

/// The reason the old rule wrote.
const STALE: &str = "the change makes the staging content its end-to-end tests read out of date.";

/// A version of the checks other than the one running now.
const OLD_VERSION: &str = "00000000feedface";

/// The Open items as the run left them: written by older checks, with no
/// version (lines written before lines carried one) or an older one.
fn held_description(version: Option<&str>) -> String {
    let section = proof::gaps_section(&[proof::check_item("The new-feature rules", STALE)]);
    let now = proof::checked_by(proof::checks_version());
    let section = match version {
        Some(v) => section.replace(&now, &proof::checked_by(v)),
        None => section.replace(&format!(" {now}"), ""),
    };
    format!("## Summary\nOne Title Size setting for every venue block.\n\n{section}")
}

fn facts(body: &str) -> Facts {
    Facts {
        draft: true,
        title: "Add a Title Size setting to the venue page blocks".into(),
        body: body.to_string(),
        ci: Ci {
            green: true,
            why: String::new(),
        },
        approved: true,
        ..Default::default()
    }
}

/// What the checks write when they run again: the Open items replaced by
/// what they find now (`pr::apply_checks`).
fn rewritten(body: &str, found: &[proof::Finding]) -> String {
    let (mut owned, rest) = completeness::take_owned(body);
    owned.set(Section::OpenItems, proof::gaps_section(found));
    completeness::assemble(&rest, &owned)
}

#[test]
fn a_hold_older_checks_wrote_is_checked_again_and_clears() {
    let now = proof::checks_version();
    assert!(!now.is_empty());
    for body in [held_description(None), held_description(Some(OLD_VERSION))] {
        assert!(body.contains(proof::BLOCKED_MARK), "{body}");
        let f = facts(&body);
        let d = ready::decide(&f);
        assert!(matches!(d, Decision::Held { .. }), "{d:?}");
        assert!(ready::recheck_due(&f, &d, now), "{body}");
    }
    // The checks run again with the fixed rule and find nothing: the line
    // goes and the pull request is ready.
    let after = rewritten(&held_description(None), &[]);
    assert!(!after.contains(STALE), "{after}");
    assert!(after.contains("One Title Size setting"), "{after}");
    let f = facts(&after);
    assert_eq!(ready::decide(&f), Decision::Ready { open: vec![] });
}

#[test]
fn a_line_the_running_checks_wrote_never_starts_another_check() {
    let now = proof::checks_version();
    // The check still fails on the new code: it holds, as a check the change
    // didn't pass, and is not checked again.
    let body = held_description(Some(now));
    assert!(body.contains(&proof::checked_by(now)), "{body}");
    assert_eq!(proof::checked_by_of(&body), Some(now));
    let f = facts(&body);
    let d = ready::decide(&f);
    assert!(matches!(d, Decision::Held { .. }));
    assert!(!ready::recheck_due(&f, &d, now));

    // A person's draft and a closed pull request are never checked again.
    let old = held_description(None);
    let mine = Facts {
        put_back_by: Some("someone".into()),
        ..facts(&old)
    };
    assert!(!ready::recheck_due(&mine, &ready::decide(&mine), now));
    let closed = Facts {
        closed: true,
        ..facts(&old)
    };
    assert!(!ready::recheck_due(&closed, &ready::decide(&closed), now));
}

#[test]
fn once_per_run_and_version_recorded_in_the_claim() {
    // The claim and its record are one conditional write: a second decider,
    // or the next sweep on the same code, finds the version taken.
    let src = include_str!("../recheck.rs");
    let claim = fn_src(src, "\nasync fn claim(");
    assert!(claim.contains("SET recheck_version = :v"));
    assert!(claim.contains("recheck_version <> :v"));
    assert!(claim.contains("attribute_not_exists(guard_open)"));
    // `settle` checks again only when it is due and the claim was won.
    let ready_src = include_str!("../ready.rs");
    let settle = fn_src(ready_src, "\npub async fn settle(");
    let at = settle.find("recheck_due(").expect("settle asks");
    assert!(settle[at..].contains("recheck::start("));
    // The step resumes at the checks, and their results from other code
    // don't stand on an unchanged head.
    let m = include_str!("../mod.rs");
    let re = fn_src(m, "\npub(crate) async fn recheck_at_checks(");
    assert!(re.contains("steps::Gate::Checks"));
    let rp = fn_src(m, "\nasync fn run_passes(");
    assert!(rp.contains("for_checks_version(proof::checks_version())"));
}

#[test]
fn results_other_code_wrote_run_again_once_and_keep_their_rounds() {
    let mut l = Ledger::default();
    for r in [&mut l.rules, &mut l.links, &mut l.test_fix, &mut l.proof] {
        r.head = "eeeb43e".into();
        r.rounds = 2;
    }
    assert!(l.for_checks_version("v2"));
    assert!(!l.rules.unchanged("eeeb43e"));
    assert!(!l.links.unchanged("eeeb43e"));
    assert!(!l.test_fix.unchanged("eeeb43e"));
    assert_eq!(l.rules.rounds, 2, "the rounds a run had still count");
    // The proof check's gaps are never a check the change didn't pass.
    assert!(l.proof.unchanged("eeeb43e"));
    // The same code again: its results stand.
    l.rules.head = "eeeb43e".into();
    assert!(!l.for_checks_version("v2"));
    assert!(l.rules.unchanged("eeeb43e"));
}

#[test]
fn the_step_is_rebuilt_from_the_run_record() {
    let s = |v: &str| AttributeValue::S(v.into());
    let n = |v: u64| AttributeValue::N(v.to_string());
    let item: HashMap<String, AttributeValue> = [
        ("team_id", s("TEAM#t")),
        ("run_id", s("R1")),
        ("repo", s("o/web")),
        ("branch", s("coderhelm/gh-41")),
        ("base_branch", s("coderhelm/gh-40")),
        ("installation_id", n(7)),
        ("ticket_source", s("jira")),
        ("ticket_id", s("GH-41")),
        ("ticket_body", s("Add a Title Size setting.")),
        ("title", s("Title Size")),
        ("run_step", n(5)),
        ("requester_key", s("email:a@example.com")),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    let (msg, branch) = recheck::message_of(&item).expect("a step");
    assert_eq!(branch, "coderhelm/gh-41");
    assert_eq!(
        (msg.repo_owner.as_str(), msg.repo_name.as_str()),
        ("o", "web")
    );
    assert_eq!(
        msg.base_branch, "coderhelm/gh-40",
        "the stacked base is kept"
    );
    assert_eq!(msg.continuation_run_id.as_deref(), Some("R1"));
    // The run's last step was 5: the next message is step 6, the same run.
    let next = crate::passes::steps::next_message(&msg, "R1", 1);
    assert_eq!(crate::passes::steps::step_number(next.continuation), 6);
    assert!(next.same_run());

    // No branch: nothing to check again.
    let mut none = item.clone();
    none.remove("branch");
    assert!(recheck::message_of(&none).is_none());

    // Security: an audit that passed needs no second run; one with items does.
    assert!(recheck::carry_of("## Summary\nx").is_some());
    assert!(recheck::carry_of(&format!("- x {}", proof::SECURITY_MARK)).is_none());
}

#[test]
fn a_dip_coderhelm_caused_and_its_return_are_not_progress() {
    // The incident's counts: 37 at the end of a step, 23 after the rebuild
    // at the start of the next, 37 again with nothing written.
    let mut g = GuardState::default();
    assert!(g.apply(&Progress::TasksDone(37)));
    for _ in 0..8 {
        g.on_continuation(1.0);
        assert!(!g.apply(&Progress::TasksDone(23)));
        assert!(
            !g.apply(&Progress::TasksDone(37)),
            "a return with nothing written is not progress"
        );
    }
    assert_eq!(g.trip(), Some(Trip::Continuations));

    // Someone else moved the base and the run's coder redoes the work: that
    // is progress, still a bounded number of times.
    let mut g = GuardState::default();
    assert!(g.apply(&Progress::TasksDone(37)));
    g.on_continuation(1.0);
    assert!(!g.apply(&Progress::TasksDone(23)));
    g.on_own_code();
    assert!(g.apply(&Progress::TasksDone(30)));
    assert_eq!(g.continuations, 0);

    // The coder writing is reported from the one place every round writes,
    // and the plan check at a step's start measures with the stacked base.
    let imp = include_str!("../implement.rs");
    assert!(imp.contains("run_guard::wrote_code("));
    let m = include_str!("../mod.rs");
    let cf = fn_src(m, "\nasync fn continuation_feedback(");
    assert!(cf.contains("plan_coverage::plan_changes("));
    assert!(!cf.contains("plan_coverage::coverage("));
}
