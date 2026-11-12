//! af. A security update's pull request changed three `package.json` files
//! (a pnpm workspace root and two npm packages with lockfiles of their own)
//! and none of their lockfiles. Every CI job failed at its frozen install.
//! A person pressed Retry. The retry planned its bumps by rule, then found
//! the open pull request red and handed it to a model instead of building
//! its plan: the model read registry pages, ran a codegen command whose
//! first step was the same frozen install, and changed nothing. Landing
//! "nothing" cleaned up the round's work branch, so the second attempt wrote
//! to a branch that no longer existed: every write a 404, a hand edit of a
//! `package-lock.json` among them, then "its changes couldn't be read
//! (404)". The round was called unfinished, the run was sent to its ending
//! with "the ladder is spent" although one attempt of three had run, and it
//! ended failed with "CI is failing: the pull request's checks; CI is
//! failing: the pull request's checks".
//!
//! Guarantee: a lockfile out of sync with its manifest is repaired by code,
//! before any model round and for any ticket: each package manager writes
//! its own lockfile in the sandbox and the result is one commit. A retry of
//! a dependency fix rebuilds its change from its plan. A model can't write
//! a lockfile. A fault inside a fix round (no work branch, a compare or a
//! push GitHub refuses, a sandbox failure, anything else) has a way around
//! it, and the catch-all runs the round again from a clean state; no fault
//! ends the round or the run. A run with red checks goes back to fixing
//! them, and when every way is spent its ending names the cause, once.

use super::fixture::*;
use crate::clients::sandbox;
use crate::passes::generators::{self, LockRepair};
use crate::passes::round::{self, AfterFault, Fault, WayAround};
use crate::passes::{feedback, implement, resume};
use common::lockfile_sync as ls;
use common::run_round::Kind;

const OLD_ENDING: &str =
    "CI is failing: the pull request's checks; CI is failing: the pull request's checks";
const OLD_ROUND: &str = "the last fix round didn't finish";
const OLD_NOTE: &str = "has CI failures — fixing";

const SEC_BRANCH: &str = "coderhelm/security-updates-2026-01-05-a1b2c3";
const THUMBS_LOCK: &str = "lambdas/thumbs/package-lock.json";
const CLIPS_LOCK: &str = "lambdas/clips/package-lock.json";
const ROOT_LOCK: &str = "pnpm-lock.yaml";

/// The fixture monorepo with two npm packages that keep lockfiles of their
/// own beside the pnpm workspace.
fn tree() -> Vec<String> {
    let mut tree = monorepo().tree;
    tree.extend(
        [
            "pnpm-workspace.yaml",
            "lambdas/thumbs/package.json",
            THUMBS_LOCK,
            "lambdas/thumbs/index.mjs",
            "lambdas/clips/package.json",
            CLIPS_LOCK,
            "lambdas/clips/index.mjs",
        ]
        .iter()
        .map(|p| p.to_string()),
    );
    tree
}

/// What the earlier attempt left on the pull request: manifests, no
/// lockfile.
fn pr_changes() -> Vec<String> {
    [
        "package.json",
        "apps/web/package.json",
        "lambdas/thumbs/package.json",
        "lambdas/clips/package.json",
        "lambdas/thumbs/index.test.mjs",
    ]
    .iter()
    .map(|p| p.to_string())
    .collect()
}

/// The three failing workflows' logs, as GitHub Actions printed them.
fn ci_logs() -> String {
    [
        "Workflow 'Deploy Thumbs Lambda' failed",
        "2026-01-05T10:00:01.0000000Z npm error code EUSAGE",
        "2026-01-05T10:00:01.0000000Z npm error `npm ci` can only install packages when your package.json and package-lock.json or npm-shrinkwrap.json are in sync. Please update your lock file with `npm install` before continuing.",
        "2026-01-05T10:00:01.0000000Z npm error Invalid: lock file's undici@6.28.1 does not satisfy undici@8.12.4",
        "2026-01-05T10:00:01.0000000Z npm error Missing: acme-streams@2.0.1 from lock file",
        "--- Next workflow ---",
        "Workflow 'Lint' failed",
        "2026-01-05T10:00:03.0000000Z  ERR_PNPM_LOCKFILE_CONFIG_MISMATCH  Cannot proceed with the frozen installation. The current \"overrides\" configuration doesn't match the value found in the lockfile",
        "2026-01-05T10:00:03.0000000Z Update your lockfile using \"pnpm install --no-frozen-lockfile\"",
    ]
    .join("\n")
}

#[tokio::test]
async fn out_of_sync_lockfiles_are_regenerated_by_code_before_any_model_round() {
    let mut end = Ending::default();
    // CI's own lines name the cause.
    let found = ls::mismatches(&ci_logs());
    assert_eq!(
        found.iter().map(|m| m.manager).collect::<Vec<_>>(),
        ["npm", "pnpm"]
    );
    let cause = ls::cause(&found).expect("a lockfile mismatch");
    assert!(cause.contains("npm ci") && cause.contains("ERR_PNPM_LOCKFILE_CONFIG_MISMATCH"));

    // Every lockfile whose manifest the pull request changes is written
    // again: each npm package's own, and the workspace root's (which
    // doesn't answer for the nested packages).
    let tools = generators::lockfiles_to_repair(&tree(), &pr_changes());
    let locks: Vec<&str> = tools.iter().map(|t| t.lockfile.as_str()).collect();
    assert_eq!(locks, vec![CLIPS_LOCK, THUMBS_LOCK, ROOT_LOCK]);
    assert_eq!(
        tools.iter().map(|t| t.manager).collect::<Vec<_>>(),
        ["npm", "npm", "pnpm"]
    );
    // A pull request that changes one nested manifest regenerates that
    // package's lockfile alone, and one that changes no manifest, none.
    let one = generators::lockfiles_to_repair(&tree(), &["lambdas/clips/package.json".to_string()]);
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].lockfile, CLIPS_LOCK);
    assert!(generators::lockfiles_to_repair(
        &tree(),
        &["apps/web/src/blocks/hero.tsx".to_string()]
    )
    .is_empty());
    // A lockfile edited by hand is written again by its manager too.
    let hand = generators::lockfiles_to_repair(&tree(), &[THUMBS_LOCK.to_string()]);
    assert_eq!(hand.len(), 1);

    // One sandbox build, each manager in its own folder, never a frozen
    // install.
    let cmd = ls::repair_command(&tools);
    assert!(cmd.contains("cd 'lambdas/thumbs' && ((npm install --package-lock-only"));
    assert!(cmd.contains("pnpm install --lockfile-only --ignore-scripts"));
    assert!(!cmd.contains("--frozen-lockfile)") && !cmd.contains("npm ci"));

    // The build, read by the real sandbox reader: three lockfiles written,
    // and a manifest one manager reformatted on the way.
    let archive = tarball(&[
        (
            CLIPS_LOCK,
            b"{\"lockfileVersion\":3,\"clips\":true}".to_vec(),
        ),
        (
            THUMBS_LOCK,
            b"{\"lockfileVersion\":3,\"thumbs\":true}".to_vec(),
        ),
        (
            ROOT_LOCK,
            b"lockfileVersion: '9.0'\noverrides:\n  undici: 8.12.4\n".to_vec(),
        ),
        (
            "lambdas/clips/package.json",
            b"{ \"reformatted\": true }".to_vec(),
        ),
    ]);
    let log = codebuild_log(&[
        "CODERHELM_LOCKFILE 0 0",
        "CODERHELM_LOCKFILE 1 0",
        "CODERHELM_LOCKFILE 2 0",
        "CODERHELM_CODEGEN_END exit=0",
        "===CODERHELM_CODEGEN_UPLOADED===",
    ]);
    let o = sandbox::codegen_outcome(
        &FakeSandbox::new(vec![archive], log),
        "/aws/codebuild/sandbox",
        "s1",
        "out.tgz",
        true,
    )
    .await;
    let (files, repair) = generators::read_lock_repair(&tools, &o);
    assert_eq!(repair.written, vec![CLIPS_LOCK, THUMBS_LOCK, ROOT_LOCK]);
    assert!(
        repair.failed.is_empty() && repair.not_run.is_none(),
        "{repair:?}"
    );
    // Only lockfiles are committed.
    let committed: Vec<&str> = files.iter().map(|(p, _)| p.as_str()).collect();
    assert_eq!(committed, vec![CLIPS_LOCK, THUMBS_LOCK, ROOT_LOCK]);
    let mut gh = FakeGitHub::new();
    gh.commit(
        "lockfile repair",
        SEC_BRANCH,
        &files
            .iter()
            .map(|(p, c)| (p.as_str(), c.as_str()))
            .collect::<Vec<_>>(),
    );
    assert_eq!(gh.changed(SEC_BRANCH).len(), 3);

    // The run page says the cause and what was done; no model was asked.
    let note = resume::lock_repair_note(&cause, &repair);
    end.said(note.clone());
    assert!(
        note.starts_with("CI is failing because the lockfiles are out of sync"),
        "{note}"
    );
    assert!(
        note.contains("regenerated") && note.contains("No model round was needed"),
        "{note}"
    );
    end.status = common::run_status::AWAITING_CI.into();
    for old in [OLD_ENDING, OLD_ROUND, OLD_NOTE] {
        end.never_says(old);
    }

    // The pipeline does it there: in the CI fix path the repair comes
    // before the fix budget, the stall check and the model.
    let src = include_str!("../resume.rs");
    let at = |needle: &str| {
        src.find(needle)
            .unwrap_or_else(|| panic!("{needle} is gone"))
    };
    let repair_at = at("let repair = lock_repair_on_pr(");
    assert!(at("common::lockfile_sync::mismatches(&logs)") < repair_at);
    assert!(repair_at < at("let over_cycle = cycle >= MAX_CI_FIX_CYCLES;"));
    assert!(
        repair_at < at("if check_and_record_stall(state, &msg.team_id, &msg.run_id, &logs).await")
    );
    assert!(repair_at < at("ci_fix_effort(attempt, max_attempts),"));
    // And in a re-run that finds its pull request red, before its model.
    let run = run_passes_src();
    let fix_mode = &run[run
        .find("let mut fixed_by_code = false;")
        .expect("the re-run repair")..];
    assert!(
        fix_mode.find("generators::repair_lockfiles(").unwrap()
            < fix_mode.find("implement::run(").unwrap()
    );
}

#[tokio::test]
async fn a_lockfile_its_manager_cannot_write_is_named_and_the_others_still_land() {
    let tools = generators::lockfiles_to_repair(&tree(), &pr_changes());
    let archive = tarball(&[
        (CLIPS_LOCK, b"{\"lockfileVersion\":3}".to_vec()),
        (ROOT_LOCK, b"lockfileVersion: '9.0'\n".to_vec()),
    ]);
    let log = codebuild_log(&[
        "CODERHELM_LOCKFILE 0 0",
        "CODERHELM_LOCKFILE 1 1",
        "npm error code ETARGET",
        "npm error notarget No matching version found for acme-streams@9.9.9.",
        "CODERHELM_LOCKFILE 2 0",
        "CODERHELM_CODEGEN_END exit=0",
        "===CODERHELM_CODEGEN_UPLOADED===",
    ]);
    let o = sandbox::codegen_outcome(
        &FakeSandbox::new(vec![archive], log),
        "/aws/codebuild/sandbox",
        "s1",
        "out.tgz",
        true,
    )
    .await;
    let (files, repair) = generators::read_lock_repair(&tools, &o);
    assert_eq!(files.len(), 2);
    assert_eq!(repair.written, vec![CLIPS_LOCK, ROOT_LOCK]);
    assert_eq!(repair.failed.len(), 1);
    assert_eq!(repair.failed[0].0, THUMBS_LOCK);
    assert!(
        repair.failed[0].1.contains("acme-streams@9.9.9"),
        "{:?}",
        repair.failed
    );
    let note = repair.note();
    assert!(
        note.contains("regenerated") && note.contains("couldn't be written"),
        "{note}"
    );

    // A build that never ran writes nothing and says why; the fix round
    // goes on with that in hand.
    let none = sandbox::codegen_outcome(
        &FakeSandbox::new(vec![], String::new()),
        "/aws/codebuild/sandbox",
        "s1",
        "out.tgz",
        false,
    )
    .await;
    let (files, repair) = generators::read_lock_repair(&tools, &none);
    assert!(files.is_empty() && repair.written.is_empty());
    assert!(repair.not_run.is_some(), "{repair:?}");
    assert!(repair.note().contains("couldn't be regenerated here"));
    assert!(resume::lock_repair_note("x", &repair).contains("The fix round goes on"));
}

#[test]
fn a_retry_of_a_dependency_fix_rebuilds_its_change_from_its_plan() {
    // The open pull request is red and nobody commented: the dependency
    // run builds its plan (bumps and lockfiles by code); no model patches
    // the earlier attempt.
    assert!(crate::passes::rebuilds_by_plan(true, false));
    // A person's review comments still get their fix round.
    assert!(!crate::passes::rebuilds_by_plan(true, true));
    // Any other ticket takes the fix round, lockfiles first.
    assert!(!crate::passes::rebuilds_by_plan(false, false));

    let run = run_passes_src();
    let decided =
        run.find("let dep_rebuilds = rebuilds_by_plan(dep_run.is_some(), has_review_feedback);");
    let fix_mode = run
        .find("if (has_ci_failure || has_review_feedback) && !context_changed && !dep_rebuilds {");
    let drive = run.find("dep_fix::drive(&work, run, usage)");
    match (decided, fix_mode, drive) {
        (Some(d), Some(f), Some(v)) => assert!(d < f && f < v),
        other => panic!("the re-run no longer decides by the plan: {other:?}"),
    }
    // The rebuild starts from the base branch's files.
    let dep = include_str!("../dep_fix.rs");
    let drive_src = fn_src(dep, "pub async fn drive(");
    assert!(drive_src.contains("clean_slate(w).await"));
    // A plan the rules wrote is no planner failure.
    assert_eq!(crate::passes::no_decision_reason(true), "");
    assert!(!crate::passes::no_decision_reason(false).is_empty());
    assert!(run.contains("no_decision_reason(dep_run.is_some())"));
}

#[test]
fn a_model_cannot_write_a_lockfile() {
    for path in [
        THUMBS_LOCK,
        ROOT_LOCK,
        "yarn.lock",
        "services/Cargo.lock",
        "poetry.lock",
    ] {
        let coder = implement::lockfile_write_refusal(path).unwrap_or_else(|| panic!("{path}"));
        assert!(
            coder.contains("REFUSED") && coder.contains("run_codegen"),
            "{coder}"
        );
        let round = feedback::lockfile_write_refusal(path).unwrap_or_else(|| panic!("{path}"));
        assert!(
            round.contains("REFUSED") && !round.contains("run_codegen"),
            "{round}"
        );
    }
    for path in [
        "package.json",
        "lambdas/thumbs/index.mjs",
        "docs/lockfiles.md",
    ] {
        assert!(implement::lockfile_write_refusal(path).is_none(), "{path}");
        assert!(feedback::lockfile_write_refusal(path).is_none(), "{path}");
    }
    // Every write tool of both coders asks first (write, edit, batch).
    for (name, src) in [
        ("implement.rs", include_str!("../implement.rs")),
        ("feedback.rs", include_str!("../feedback.rs")),
    ] {
        let calls = src.matches("lockfile_write_refusal(path)").count();
        assert!(calls >= 3, "{name}: {calls} write tools ask");
        for tool in [
            "\"write_file\" => {",
            "\"edit_file\" => {",
            "\"batch_write\" => {",
        ] {
            let at = src.find(tool).unwrap_or_else(|| panic!("{name}: {tool}"));
            let body = &src[at..(at + 6000).min(src.len())];
            assert!(
                body.contains("lockfile_write_refusal(path)"),
                "{name}: {tool} writes a lockfile unasked"
            );
        }
    }
    // The tool the refusal names is there whenever a sandbox is, and it
    // writes the lockfiles before any generator installs.
    let src = include_str!("../implement.rs");
    assert!(src.contains("if sandbox.is_some() {\n        tools.push(run_codegen_tool());"));
    let tool = fn_src(src, "    async fn run_codegen_impl(");
    assert!(
        tool.find("self.regenerate_lockfiles().await").unwrap()
            < tool.find("self.run_generators().await").unwrap()
    );
}

/// One attempt of a fix round, as the loop sees it end.
enum Attempt {
    /// It met this error.
    Fault(&'static str),
    /// It landed a change.
    Landed,
}

/// Drives a CI fix round's attempts through the round's own decisions:
/// what it said on the way and how it ended.
fn drive(attempts: &[Attempt], max: usize, redrives: u32) -> (Vec<String>, &'static str) {
    let mut said = Vec::new();
    let mut attempt = 1usize;
    for a in attempts {
        match a {
            Attempt::Landed => return (said, "awaiting_ci"),
            Attempt::Fault(error) => {
                let class = round::fault_of(error);
                let next = round::after_fault(attempt, max, true, redrives);
                said.push(round::fault_note(Kind::CiFix, class, error, &next));
                match next {
                    AfterFault::RetryClean => attempt += 1,
                    AfterFault::Redrive => return (said, "awaiting_ci"),
                    AfterFault::Supervisor => return (said, "supervisor"),
                }
            }
        }
    }
    (said, "running")
}

#[test]
fn every_fault_inside_a_fix_round_has_a_way_around_and_none_ends_the_run() {
    const NO_WIP_COMPARE: &str = "HTTP status client error (404 Not Found) for url (https://api.github.com/repos/acme/site/compare/abc123...coderhelm-wip/coderhelm/security-updates-2026-01-05-a1b2c3): {\"message\":\"Not Found\"}";
    const NO_WIP_REF: &str = "HTTP status client error (404 Not Found) for url (https://api.github.com/repos/acme/site/git/ref/heads/coderhelm-wip/coderhelm/security-updates-2026-01-05-a1b2c3)";
    const COMPARE: &str = "its changes couldn't be read (HTTP status client error (404 Not Found) for url (https://api.github.com/repos/acme/site/compare/abc123...def456))";
    const PUSH: &str = "the one commit couldn't be pushed (HTTP status client error (422 Unprocessable Entity): Update is not a fast forward)";
    const CONFLICT: &str = "HTTP status client error (409 Conflict) for url (https://api.github.com/repos/acme/site/git/refs/heads/x)";
    const CLASH: &str = "the pull request changed the same files meanwhile (package.json)";
    const BUSY: &str = "HTTP status server error (503 Service Unavailable) for url (https://api.github.com/repos/acme/site/git/trees)";
    const SANDBOX: &str = "the sandbox build didn't start: CodeBuild AccountLimitExceededException";
    const OTHER: &str = "the model's reply could not be read";

    // Each class, injected alone: classified, with its own way around,
    // and the round goes on.
    for (error, class, way) in [
        (NO_WIP_COMPARE, Fault::WipMissing, WayAround::ReopenWip),
        (NO_WIP_REF, Fault::WipMissing, WayAround::ReopenWip),
        (COMPARE, Fault::Unreadable, WayAround::ReadFromTrees),
        (PUSH, Fault::PushRejected, WayAround::PushAgain),
        (CONFLICT, Fault::PushRejected, WayAround::PushAgain),
        (CLASH, Fault::Clash, WayAround::RestartFromHead),
        (BUSY, Fault::GitHubBusy, WayAround::WaitForGitHub),
        (SANDBOX, Fault::Sandbox, WayAround::WithoutSandbox),
        (OTHER, Fault::Other, WayAround::RetryClean),
    ] {
        assert_eq!(round::fault_of(error), class, "{error}");
        assert_eq!(round::way_around(class), way, "{error}");
        let (said, ended) = drive(&[Attempt::Fault(error), Attempt::Landed], 3, 0);
        assert_eq!(ended, "awaiting_ci", "{error}");
        assert_eq!(said.len(), 1);
        assert!(said[0].contains("runs it again now"), "{}", said[0]);
        for old in [OLD_ROUND, "It saved no work before it stopped", "failed"] {
            assert!(!said[0].contains(old), "{}", said[0]);
        }
    }

    // The run this case is from: the second attempt had no work branch.
    // The third attempt runs, from a clean branch.
    let (said, ended) = drive(&[Attempt::Fault(NO_WIP_COMPARE), Attempt::Landed], 3, 0);
    assert_eq!(ended, "awaiting_ci");
    assert!(said[0].contains("saved nothing"), "{}", said[0]);

    // Every attempt meets a fault: the round runs again in a fresh step,
    // once, before the supervisor's ending is asked. Never a failed run
    // from inside the round.
    let all = [
        Attempt::Fault(SANDBOX),
        Attempt::Fault(PUSH),
        Attempt::Fault(OTHER),
    ];
    let (said, ended) = drive(&all, 3, 0);
    assert_eq!(ended, "awaiting_ci");
    assert!(said[2].contains("fresh step"), "{}", said[2]);
    let (said, ended) = drive(&all, 3, round::MAX_REDRIVES);
    assert_eq!(ended, "supervisor");
    assert!(said[2].contains("its attempts are used"), "{}", said[2]);

    // The pipeline: every attempt opens its work branch first, a fault
    // leaves the attempt as a fault, and nothing in the CI fix path calls
    // a round unfinished or completes the run from inside it.
    let src = include_str!("../resume.rs");
    let from = src
        .find("let mut ci_round = run_round::Round::start(")
        .expect("the CI round");
    let to = src[from..]
        .find("// A fix that left the tree unchanged")
        .expect("the end of the fix loop")
        + from;
    let ci = &src[from..to];
    let looped = &ci[ci.find("'fix: loop {").expect("the attempt loop")..];
    assert!(
        looped.find("super::round::open_wip(").unwrap() < looped.find("implement::run(").unwrap(),
        "an attempt writes before its work branch is there"
    );
    assert!(ci.contains("Landed::Refused(why) => break 'attempt Some(why)"));
    assert!(ci.contains("super::round::after_fault("));
    assert!(ci.contains("record_synthetic_ci_failed("));
    assert!(
        !ci.contains("finish_incomplete("),
        "a fault calls the round unfinished"
    );
    assert!(
        !ci.contains("complete_run_with_status("),
        "a fault completes the run"
    );
    assert!(!ci.contains("fail_run("), "a fault fails the run");
    // Landing: no work branch is "nothing saved", a refused compare is
    // read from the trees, and a refused push is sent once more.
    let round_src = include_str!("../round.rs");
    let changes = fn_src(round_src, "async fn round_changes(");
    assert!(changes.contains("is_not_found(") && changes.contains("return Ok(None)"));
    assert!(changes.contains("changed_from_trees("));
    let land = fn_src(round_src, "pub(crate) async fn land(");
    assert!(land.contains("Ok(None) => return Landed::Nothing"));
    assert_eq!(
        land.matches(".batch_write(owner, repo, pr.branch, message, &ops)")
            .count(),
        2
    );
}

#[test]
fn an_attempt_that_changed_nothing_changes_approach_by_code() {
    // The deterministic repair first, when it hasn't run yet; then the
    // model with fresh evidence; the last attempt on the strongest model.
    assert_eq!(resume::ci_fix_effort(1, 3), "medium");
    assert_eq!(resume::ci_fix_effort(2, 3), "medium");
    assert_eq!(resume::ci_fix_effort(3, 3), "complex");
    assert_eq!(resume::ci_fix_effort(1, 1), "medium");
    let src = include_str!("../resume.rs");
    let nothing = &src[src
        .find("CI fix changed nothing — the next attempt takes a different approach")
        .expect("the changed-nothing branch")..];
    let repair = nothing
        .find("if !lock_tried {")
        .expect("the repair by code");
    let evidence = nothing.find("feedback = next_attempt_feedback(").unwrap();
    assert!(repair < evidence);
    // Three attempts, as before, and each differs from the one before it.
    let tried = vec!["attempt 1 changed nothing (no explanation)".to_string()];
    let second = resume::next_attempt_feedback("base", 2, 3, &tried, "fresh evidence");
    assert!(second.contains("Take a different approach") && second.contains("fresh evidence"));
}

#[test]
fn red_checks_send_the_run_back_to_fixing_and_its_ending_names_the_cause_once() {
    use crate::passes::outcome::{self, CiState};
    let mut end = Ending::default();
    let cause = ls::cause(&ls::mismatches(&ci_logs())).unwrap();

    // The record names what fails; "the pull request's checks" is only
    // what is said when no round read the failure.
    let state = resume::ci_state_with_cause("failed", "", &cause);
    assert_eq!(state, CiState::Failing(cause.clone()));
    assert_eq!(
        resume::ci_state_with_cause("failed", "", ""),
        CiState::Failing("the pull request's checks".to_string())
    );
    assert_eq!(
        resume::ci_state_with_cause("passed", "", &cause),
        CiState::Green
    );
    assert_eq!(
        resume::ci_state_with_cause("failed", "red_on_base", &cause),
        CiState::RedOnBase
    );
    let facts = outcome::Facts {
        tasks_remaining: vec![],
        ci: state,
        review: outcome::Review::Missing,
        stop: None,
    };
    let red = outcome::decide(&facts).reasons.join("; ");
    assert!(red.contains("out of sync"), "{red}");

    // A run about to be completed with red checks goes back to its CI fix
    // round, once, while fix cycles are left.
    assert!(resume::may_redrive_red_ci(0, 1));
    assert!(!resume::may_redrive_red_ci(round::MAX_REDRIVES, 1));
    assert!(!resume::may_redrive_red_ci(0, 10));
    let src = include_str!("../resume.rs");
    let complete = fn_src(src, "async fn complete_run_with_status(");
    assert!(
        complete.find("redrive_red_ci(").unwrap() < complete.find("wait_for_review(").unwrap(),
        "red checks end the run before the fix round is asked"
    );

    // When every way is spent, the ending says the cause once. The old
    // text joined the same sentence to itself.
    let old_red = "CI is failing: the pull request's checks";
    let old = format!(
        "{}; {old_red}. Every fix and revert CoderHelm tried is on pull request #7.",
        old_red.trim_end_matches('.')
    );
    assert!(old.starts_with(OLD_ENDING));
    let said = resume::red_ending_text(old_red, old_red, 7);
    assert_eq!(said.matches(old_red).count(), 1, "{said}");
    let said = resume::red_ending_text(&red, &red, 7);
    assert_eq!(said.matches("out of sync").count(), 1, "{said}");
    assert!(said.ends_with("is on pull request #7."), "{said}");
    // What the fix round found and the record's cause, both, when they
    // differ; one of them when one holds the other.
    let both = resume::red_ending_text("CI is failing: `Lint`: 3 attempts", &red, 7);
    assert!(
        both.contains("`Lint`") && both.contains("out of sync"),
        "{both}"
    );
    let held = resume::red_ending_text(&format!("{red} (after 3 attempts)"), &red, 7);
    assert_eq!(held.matches("out of sync").count(), 1, "{held}");
    end.said(said);
    end.status = "failed".into();
    end.never_says(OLD_ENDING);

    // The scan the supervisor keeps: the red ending still takes out what
    // broke the checks before it gives anything up.
    let hand_off = fn_src(src, "async fn hand_off_red_ci(");
    assert!(
        hand_off.find("revert_to_green(").unwrap() < hand_off.find("record_ci_outcome(").unwrap()
    );
    let _: LockRepair = LockRepair::default();
}
