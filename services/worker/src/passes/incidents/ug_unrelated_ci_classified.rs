//! ug. A small web fix's preview check failed on an end-to-end test of
//! another page. CoderHelm's CI fix round showed it wasn't the change's:
//! the staging data the test reads changed three minutes before it ran,
//! staging (the base branch's build) showed the same mismatch, and the
//! change touched nothing on that page. It changed no code, which was
//! right. The run still ended "failed" with "CoderHelm made 1 fix
//! attempt(s) without getting it green" (its record said two rounds), and
//! the pull request stayed a draft. The check runs only on pull requests,
//! so its comparison with the base branch's head never ran and the round's
//! proof counted for nothing. Earlier, a review fix round on the same run
//! ran out of time three steps in a row: each step read the same files
//! again and edited a file the step before had put back on purpose.
//!
//! Guarantee: a round that changes no code and shows, with evidence, that
//! the failure isn't the change's is classified (`common::ci_unrelated`):
//! the failed jobs run again once; green carries on as any green run; red
//! again ends the run completed with `ci_end` = `unrelated_test`, the
//! failure named with its proof on the pull request and the run, the
//! failing test handed to its own flaky-test fix, and the pull request
//! ready. A check that never runs on the base branch finds its base on the
//! base branch's recent commits, then in the failing test's own runs there.
//! The red ending counts rounds and pushed fixes as the record has them. A
//! continuing step is told what the steps before it settled.

use super::fixture::fn_src;
use crate::passes::resume;
use common::ci_repro::{self, Verdict};
use common::ci_unrelated::{self, Rerun};

const HEAD: &str = "c6da43eae6ee1bb8";

/// The round's own conclusion, as it wrote it (names made generic).
const ROUND: &str = "I made no code change. The failing e2e test (`Classes › sport filter shows only that sport's sessions and clears`) failed because staging data changed while it ran, not because of this PR.\n\n**What I found:**\n- Base branch fails the same way.\n- Why it isn't this PR: the PR only changes the camps block and one date helper.\n\nEvidence:\n- Confirmed: on the base branch's build the full-week page still shows session 113154.\n- Confirmed: the session went inactive at 17:01:59; the e2e run went from 17:05 to 17:08.\n- Unconfirmed [check:tests]: I couldn't re-run the failing Playwright test.";

#[test]
fn a_round_that_shows_the_failure_is_outside_the_change_classifies_it() {
    let s = ci_unrelated::classify(
        ROUND,
        &[],
        HEAD,
        &["Deploy Preview".into()],
        &[],
        &["apps/web/src/components/camps-block.tsx".into()],
    )
    .expect("the round's proof is read");
    assert_eq!(
        s.tests,
        vec!["Classes › sport filter shows only that sport's sessions and clears".to_string()]
    );
    assert_eq!(s.proof.len(), 2);
    // The standing reads "not this change's" everywhere: the run's ending,
    // the ready decision, analytics.
    assert!(ci_unrelated::not_ours(ci_unrelated::CI_END));
    assert_eq!(
        resume::ci_state_of_record("failed", ci_unrelated::CI_END),
        crate::passes::outcome::CiState::RedOnBase
    );
    assert!(resume::ci_state_of_record("failed", ci_unrelated::CI_END).allows_done());
    let ready = include_str!("../ready.rs");
    let reader = fn_src(ready, "\nasync fn red_on_base(");
    assert!(reader.contains("ci_unrelated::not_ours"), "{reader}");
    // What the pull request says: the test, the proof, the re-run; ready,
    // never "without getting it green".
    let mut s = s;
    s.rerun = Rerun::Started;
    let sec = ci_unrelated::section(&s);
    assert!(sec.contains("`Deploy Preview` fails on `Classes › sport filter"));
    assert!(sec.contains("ran again once on c6da43e and failed the same way"));
    assert!(sec.contains("ready for review"));
    assert!(!sec.contains("without getting it green") && !sec.contains("stays a draft"));
}

#[test]
fn the_no_change_round_classifies_before_another_attempt_or_a_hand_off() {
    let src = include_str!("../resume.rs");
    let i = src
        .find("if result.files_modified.is_empty() {")
        .expect("the no-change branch");
    let branch = &src[i..];
    let classify = branch.find("unrelated_standing(").expect("classified");
    let rerun = branch.find("rerun_unrelated(").expect("re-run once");
    let attempt = branch.find("changed nothing (").expect("the next attempt");
    let hand_off = branch.find("hand_off_red_ci(").expect("the hand-off");
    assert!(classify < rerun && rerun < attempt && attempt < hand_off);
    // Red again after the one re-run on the same head: classified, no
    // fix round; the run is completed, never failed.
    let fix = src.find("\"CI failed — attempting fix\"").unwrap();
    let finish = src[..fix].rfind("finish_unrelated(").unwrap();
    assert!(
        src[finish..fix].len() < 1200,
        "checked right before a fix starts"
    );
    let finish_fn = fn_src(src, "\nasync fn finish_unrelated(");
    assert!(finish_fn.contains("GaveUp::UnrelatedTest"));
    assert!(finish_fn.contains("fix_unrelated_test("));
    assert!(finish_fn.contains("mark_pr_ready("));
    assert!(finish_fn.contains("common::run_status::COMPLETED"));
    assert!(!finish_fn.contains("fail_run("));
}

#[test]
fn a_check_that_never_runs_on_the_base_finds_its_base() {
    // The test's own newest run on the base branch is the comparison.
    let r = ci_repro::from_history(
        HEAD,
        "Deploy Preview",
        "Classes › sport filter",
        "main",
        ("9a7cb49aaaa", true, "2026-11-23T17:30:00Z"),
    );
    assert!(matches!(r.verdict(), Verdict::NotThisChange(_)), "{r:?}");
    let s = ci_unrelated::classify(ROUND, &[r], HEAD, &[], &[], &[]).unwrap();
    assert!(s.proof[0].starts_with("Not this change's"), "{:?}", s.proof);
    let src = include_str!("../ci_repro.rs");
    let from_ci = fn_src(src, "\npub(crate) async fn from_ci(");
    assert!(from_ci.contains("nearest_base_run("));
}

#[test]
fn the_red_ending_counts_rounds_and_fixes_as_the_record_has_them() {
    let reason = resume::ci_failing_reason(
        &["Deploy Preview".into()],
        "Error: expect(received).toEqual(expected)",
        resume::Tally {
            rounds: 2,
            pushed: 1,
        },
    );
    assert!(
        reason.contains("CoderHelm ran 2 CI fix rounds and pushed 1 fix"),
        "{reason}"
    );
    assert!(!reason.contains("fix attempt(s)"));
    let none = resume::ci_failing_reason(
        &[],
        "",
        resume::Tally {
            rounds: 2,
            pushed: 0,
        },
    );
    assert!(none.contains("2 CI fix rounds changed no code"), "{none}");
}

#[test]
fn a_continuing_step_goes_on_from_what_the_steps_before_settled() {
    use serde_json::json;
    let call = |name: &str, path: &str| json!({"role": "assistant", "content": [{"type": "tool_use", "name": name, "input": {"path": path}}]});
    let step = vec![
        call("write_file", "e2e/tests/browse/callouts.spec.ts"),
        call("edit_file", "e2e/data/venues.ts"),
        call("restore_file", "e2e/data/venues.ts"),
    ];
    let fb = common::run_round::settled_first("Fix the review finding.", &step);
    assert!(fb.contains("`e2e/tests/browse/callouts.spec.ts`"), "{fb}");
    assert!(
        fb.contains("on purpose (leave them as they are): `e2e/data/venues.ts`"),
        "{fb}"
    );
    let src = include_str!("../resume.rs");
    let round = fn_src(src, "\nasync fn run_fix_round(");
    assert!(round.contains("run_round::settled_first("));
}
