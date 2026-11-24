//! by. A web pull request ended with "restore ... to base" commits for
//! files it never meant to touch: an account form hook, a charter
//! service, date validation, logging and their tests.
//! After red checks, the revert went back to the last green head and put
//! back every file that differed from it, including files a merge
//! of the base had brought in, so old base code landed in the pull
//! request. The scope check then saw those changed files no task named,
//! and a round put each one back to the base, one commit apiece. The
//! build itself had no file boundary either: it could edit any file.
//!
//! Guarantee: red checks never roll the branch back to a green head at
//! all (`resume::take_out_failing_bumps` takes out only a dependency run's
//! bumps), so no revert writes base code into the pull request. The
//! build of the plan is bounded by the plan's files: any other existing
//! file is refused until the call says why, and that reason is added to
//! what the plan changes. A run of CoderHelm's own commits that changes a
//! file and puts it back is folded into one commit that carries only what
//! stayed changed (or dropped), at the end of every coder round, after a
//! review-fix round, and before the pull request is marked ready; a
//! person's commits and everything before them are never rewritten.

use crate::clients::github::{fold_plan, own_tail_start};
use crate::passes::write_guard::{RoundFor, ScopeGuard};
use serde_json::json;

const PLANNED: &[&str] = &[
    "apps/web/src/components/blocks/booking-embed.tsx",
    "apps/web/src/lib/routing/routing-service.ts",
    "e2e/tests/browse/slip-booking.spec.ts",
];

/// Files a merge of the base changed: never the pull request's.
const BASE_FILES: &[&str] = &[
    "apps/web/src/components/account/use-account-form.ts",
    "apps/web/src/features/charters/services/charterService.ts",
    "apps/web/src/lib/validation-utils.ts",
    "apps/web/src/test/lib/validation-utils-dates.test.ts",
];

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|p| p.to_string()).collect()
}

fn plan() -> crate::passes::plan::PlanResult {
    crate::passes::plan::PlanResult {
        tasks: PLANNED
            .iter()
            .map(|p| format!("- [ ] {p}: [modify] The slip booking change\n"))
            .collect(),
        ..Default::default()
    }
}

#[test]
fn red_checks_never_roll_back_to_a_green_head_so_no_base_file_comes_back() {
    let resume = include_str!("../resume.rs");
    let out = super::fixture::fn_src(resume, "async fn take_out_failing_bumps(");
    assert!(!out.contains("compare_changed_files(") && !out.contains("list_pr_commits("));
    // The files a merge of the base brought stay the base's: nothing reads
    // them back from an older head.
    assert!(BASE_FILES.iter().all(|f| !out.contains(f)));
}

#[test]
fn the_build_of_the_plan_refuses_an_unplanned_file_until_it_says_why() {
    // A fresh branch: the pull request changes nothing yet.
    let g = ScopeGuard::for_round(RoundFor::PlanTasks, &[], Some(&plan()), "acme/site", "");
    assert!(g.bounded(), "the plan's files anchor the build");
    for p in PLANNED {
        assert!(g.check(p, None).is_ok(), "{p}");
    }
    let stray = BASE_FILES[1];
    let refused = g.check(stray, None).unwrap_err();
    assert!(refused.contains("the plan's tasks"), "{refused}");
    // With a stated reason the plan gains the file and the pull request
    // says why.
    let why = "the booking route reads its charter flag from this service";
    assert!(g.check(stray, Some(why)).is_ok());
    assert!(g.outside().iter().any(|(p, w)| p == stray && w == why));
    // A plan that names no file leaves the build unbounded, and the
    // dependency and generator runs stay unbounded.
    let empty = crate::passes::plan::PlanResult::default();
    assert!(
        !ScopeGuard::for_round(RoundFor::PlanTasks, &[], Some(&empty), "acme/site", "").bounded()
    );
    assert!(
        !ScopeGuard::for_round(RoundFor::Build, &s(PLANNED), Some(&plan()), "acme/site", "")
            .bounded()
    );
}

fn commit(sha: &str, login: &str, parents: usize) -> serde_json::Value {
    let ty = if login.ends_with("[bot]") {
        "Bot"
    } else {
        "User"
    };
    json!({
        "sha": sha,
        "author": {"login": login, "type": ty},
        "parents": (0..parents).map(|i| json!({"sha": format!("p{i}")})).collect::<Vec<_>>(),
    })
}

#[test]
fn an_edit_and_its_restore_fold_away_and_a_persons_commit_is_never_rewritten() {
    let commits = vec![
        commit("a1", "coderhelm[bot]", 1),
        commit("b2", "a-person", 1),
        // CoderHelm's newest run: the revert that touched the base files,
        // the restores that put each one back.
        commit("c3", "coderhelm[bot]", 1),
        commit("d4", "coderhelm[bot]", 1),
        commit("e5", "coderhelm[bot]", 1),
    ];
    // Only what follows the person's commit is CoderHelm's to fold.
    assert_eq!(own_tail_start(&commits), 2);
    // A merge ends the run too.
    let mut merged = commits.clone();
    merged.push(commit("f6", "coderhelm[bot]", 2));
    assert_eq!(own_tail_start(&merged), merged.len());

    let mut revert_files = s(PLANNED);
    revert_files.extend(s(BASE_FILES));
    let touched = vec![
        (
            "Revert the change that broke CI; it is deferred on the pull request".to_string(),
            revert_files,
        ),
        (
            format!("Restore {} from main", BASE_FILES[0]),
            s(&BASE_FILES[..2]),
        ),
        (
            format!("Restore {} from main", BASE_FILES[2]),
            s(&BASE_FILES[2..]),
        ),
    ];
    // Together they change only the planned files.
    let fold = fold_plan(&touched, &s(PLANNED)).expect("churn to fold");
    assert_eq!(fold.churn, s(BASE_FILES));
    assert!(fold
        .message
        .as_deref()
        .is_some_and(|m| m.starts_with("Revert the change")));
    assert!(fold.note(3).contains("changed and put back"));
    // Edits that all came back out: the commits are dropped.
    let drop = fold_plan(&touched[1..], &[]).unwrap();
    assert_eq!(drop.message, None);
    // Nothing put back: nothing to fold.
    assert!(fold_plan(&touched[..1], &touched[0].1).is_none());
}

#[test]
fn churn_is_folded_after_every_round_and_before_ready() {
    let implement = include_str!("../implement.rs");
    assert!(implement
        .contains("super::round::fold_churn(github, &msg.repo_owner, &msg.repo_name, branch)"));
    let feedback = include_str!("../feedback.rs");
    assert!(feedback.contains("super::round::fold_churn(&github,"));
    let ready = include_str!("../ready.rs");
    let fold = ready
        .find("super::round::fold_churn(gh,")
        .expect("ready folds");
    let squash = ready.find(".squash_branch(").unwrap();
    assert!(fold < squash, "the fold runs before the final squash");
    // The build of the plan is the bounded round.
    let passes = include_str!("../mod.rs");
    assert!(!passes.contains("write_guard::RoundFor::Build,"));
    assert!(passes.matches("write_guard::RoundFor::PlanTasks,").count() >= 4);
}

/// Audited runs: a planned-work round restored the shared file the plan
/// changes and rebuilt it again and again; a web run restored files over
/// and over and kept editing its page object. In planned-work, proof and fix rounds a
/// planned file is never restored; any other file only to the base.
#[test]
fn a_round_on_the_pull_request_never_restores_a_planned_file() {
    let pr = s(PLANNED);
    for round in [
        RoundFor::PlannedWork,
        RoundFor::ProofGap,
        RoundFor::OwnReview,
        RoundFor::CiFix,
        RoundFor::TestFix,
        RoundFor::Habits,
    ] {
        let g = ScopeGuard::for_round(round, &pr, Some(&plan()), "acme/site", "");
        let page = PLANNED[2];
        let no = g.restore(page, "main", "main").unwrap_err();
        assert!(no.contains("a file the plan changes"), "{round:?}: {no}");
        // Not from an older commit either.
        assert!(g.restore(page, "aaa1111", "main").is_err(), "{round:?}");
        // A file outside the plan goes back to the base, and only there.
        assert!(
            g.restore(BASE_FILES[0], "main", "main").is_ok(),
            "{round:?}"
        );
        let old = g.restore(BASE_FILES[0], "bbb2222", "main").unwrap_err();
        assert!(old.contains("only to the pull request's base"), "{old}");
    }
    // The build of the plan and a person's ask are not held by it.
    let build = ScopeGuard::for_round(RoundFor::PlanTasks, &pr, Some(&plan()), "acme/site", "");
    assert!(build.restore(PLANNED[2], "main", "main").is_ok());
    let person = ScopeGuard::for_round(RoundFor::PersonAsk, &pr, Some(&plan()), "acme/site", "");
    assert!(person.restore(PLANNED[2], "main", "main").is_ok());
    // Both restore tools ask it before anything else is done.
    assert!(
        include_str!("../implement.rs").contains(".restore(path, asked_ref, &self.base_branch)")
    );
    assert!(include_str!("../feedback.rs").contains(".restore(path, asked_ref, self.base_branch)"));
}
