//! ac. Two runs on one repository, one afternoon.
//!
//! The first, from a Jira ticket, did seven of its eight planned tasks. The
//! eighth asked to adapt a scroll hook to a taller bar; the coder read the
//! hook, found it measures the bar at runtime, and said so: "The last task
//! needed no code change ... `useStickyScroll.ts` stays as it is". The plan
//! check only knew a task as done when its file changed, and its list of
//! no-change phrases didn't include those words, so two more steps asked the
//! same task, the coder gave the same answer, and the progress guard stopped
//! the run. A separate salvage path then opened a pull request titled
//! "⚠️ Partial: …" whose body was "Closes #0" (the ticket wasn't a GitHub
//! issue) and "the agent ran out of time" (it hadn't), marked the run
//! completed, and the pull request was later marked ready.
//!
//! The second run's pull request failed its end-to-end check. The fix pass
//! read a job log that only showed the report upload, changed nothing, and
//! the run was marked completed and the pull request marked ready with every
//! check red: the ready decision read CI only when CoderHelm's own checks had
//! findings.
//!
//! Guarantee: one path opens a stopped run's pull request (`stop_pr`, the
//! standard ticket line and sections, a "Not finished" section with what is
//! done, what is left and the recorded reason); no body ever names issue
//! `#0`; a planned file the coder says needs no change settles its task and
//! stays settled across steps; a step after one that changed nothing takes a
//! different approach; a CI fix that changes nothing gets new attempts with
//! fresh evidence and then holds the draft with what fails, the run needing
//! attention; and the ready decision never marks a pull request ready with a
//! failing or running check.

use super::fixture::fn_src;
use crate::passes::outcome::{self, CiState};
use crate::passes::plan_coverage::{self, Coverage, Next};
use crate::passes::ready::{self, Ci, Decision, Facts};

const OLD_PARTIAL: &str = "⚠️ Partial";
const OLD_CLOSES: &str = "Closes #0";
const OLD_TIME: &str = "ran out of time";

/// Every `.rs` file of the worker, with its path.
fn worker_sources() -> Vec<(String, String)> {
    fn walk(dir: &std::path::Path, out: &mut Vec<(String, String)>) {
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs") {
                let rel = p
                    .strip_prefix(concat!(env!("CARGO_MANIFEST_DIR"), "/src/"))
                    .unwrap()
                    .to_string_lossy()
                    .to_string();
                out.push((rel, std::fs::read_to_string(&p).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(
        std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src")),
        &mut out,
    );
    out
}

/// The files that call `needle`, outside tests and this file.
fn callers(needle: &str) -> Vec<String> {
    worker_sources()
        .into_iter()
        .filter(|(p, _)| !p.ends_with("ac_partial_and_red_ci.rs"))
        .filter(|(_, src)| {
            // The code before its test module (a `#[cfg(test)]` helper
            // above it is code, too).
            let code = src.split("#[cfg(test)]\nmod ").next().unwrap_or("");
            code.contains(needle)
        })
        .map(|(p, _)| p)
        .collect()
}

/// New code can't open, describe or mark a pull request ready outside the
/// one path for each: a source scan, so a second salvage path fails here.
#[test]
fn pull_requests_are_opened_and_marked_ready_in_one_place_each() {
    let mut opens = callers(".create_pull_request(");
    opens.sort();
    assert_eq!(
        opens,
        vec![
            // The revert of a merged change whose deploy failed, not a
            // run's: opened ready with its evidence, never a draft or a
            // partial (`deploy_revert`).
            "passes/deploy_revert.rs".to_string(),
            // The setup pull request (CI and end-to-end setup), not a run's.
            "passes/e2e_setup.rs".to_string(),
            // A run's pull request.
            "passes/pr.rs".to_string(),
            // A run stopped with work on its branch: the held draft.
            "passes/stop_pr.rs".to_string(),
        ],
        "a new place opens pull requests: route it through `pr` or `stop_pr`"
    );
    assert_eq!(
        callers(".mark_pr_ready("),
        vec!["passes/ready.rs".to_string()],
        "only `ready::settle` marks a pull request ready"
    );
    for (path, src) in worker_sources() {
        if path.ends_with("ac_partial_and_red_ci.rs") || path.ends_with("outcome.rs") {
            continue;
        }
        assert!(!src.contains("salvage_timeout_pr"), "{path}");
        assert!(
            !src.contains("format!(\n            \"⚠️ Partial:"),
            "{path}"
        );
        assert!(!src.contains("This implementation is partial"), "{path}");
    }
    // Every body the client sends goes through the `#0` guard.
    let gh = include_str!("../../clients/github.rs");
    // Each method, up to the next one: the guard runs before the body is sent.
    let method = |head: &str| -> &str {
        let at = gh.find(head).unwrap_or_else(|| panic!("{head} is gone"));
        let rest = &gh[at + head.len()..];
        &rest[..rest.find("pub async fn ").unwrap_or(rest.len())]
    };
    for m in [
        "pub async fn create_pull_request(",
        "pub async fn update_pr_body(",
    ] {
        let body = method(m);
        let guard = body
            .find("outcome::guard_body(body)")
            .unwrap_or_else(|| panic!("{m} has no #0 guard"));
        let send = body
            .find("self.post(")
            .or_else(|| body.find("self.patch("))
            .unwrap();
        assert!(guard < send, "{m}");
    }
}

/// The stopped Jira run's pull request: the standard draft, through
/// `stop_pr`, from both ways a step can stop mid-implement.
#[test]
fn a_run_stopped_mid_implement_opens_the_standard_held_draft() {
    let src = include_str!("../mod.rs");
    let run = fn_src(src, "pub async fn orchestrate_ticket(");
    assert!(
        run.matches("stop_pr::open(").count() >= 2,
        "error and timeout arms"
    );
    assert!(!run.contains("salvage_timeout_pr"));
    let stop = include_str!("../stop_pr.rs");
    let open_with = fn_src(stop, "pub async fn open_with(");
    assert!(open_with.contains("outcome::ticket_line_for(msg)"));
    assert!(open_with.contains("not_finished("));
    // The stop sentence names the task the steps were stuck on.
    let why = crate::passes::steps::stall_reason(
        3,
        "a8c88140",
        "implement_started",
        "coderhelm-wip/coderhelm/web-12",
        Some("task 8 of 8 (`web/src/hooks/useStickyScroll.ts`: adapt the scroll offset)"),
    );
    let stop = outcome::Stop::from_recorded(&why);
    let words = stop.words();
    assert!(
        words.contains("made no change to the branch while working on task 8 of 8"),
        "{words}"
    );
    for old in [OLD_PARTIAL, OLD_CLOSES, OLD_TIME] {
        assert!(!words.contains(old));
    }
}

#[test]
fn issue_zero_never_renders_in_any_body() {
    for (github, n) in [(true, 0u64), (false, 0), (false, 7)] {
        let line = outcome::ticket_line(github, n, "WEB-12");
        assert_eq!(line, "Source ticket: WEB-12");
    }
    assert_eq!(outcome::ticket_line(true, 7, "GH-7"), "Closes #7");
    let pr = include_str!("../pr.rs");
    assert!(pr.contains("outcome::ticket_line_for(msg)"));
    assert!(!pr.contains("\"Closes #{number}"));
    // Whatever a builder writes, the client's guard takes `#0` out.
    let old = "Closes #0\n\n⚠️ **This implementation is partial** — the agent ran out of time.";
    assert!(!outcome::guard_body(old).contains("#0"));
}

/// The coder's own words from the stopped run (file renamed). The old
/// phrase list matched neither line, so the task stayed open for two steps.
const CODER_NOTES: &str = "The last task needed no code change, so the branch now covers all \
eight tasks. `useStickyScroll.ts` stays as it is because it measures the sticky bar's height when \
the page runs, so a 12px taller bar is picked up on its own.";

fn the_plan() -> String {
    (1..=7)
        .map(|i| format!("- [ ] web/src/blocks/part{i}.tsx: adjust the padding"))
        .chain(std::iter::once(
            "- [ ] web/src/hooks/useStickyScroll.ts: adapt the scroll offset to the taller bar"
                .to_string(),
        ))
        .collect::<Vec<_>>()
        .join("\n")
}

fn coverage_of(tasks: &[plan_coverage::PlanTask]) -> Coverage {
    let changed: Vec<String> = (1..=7)
        .map(|i| format!("web/src/blocks/part{i}.tsx"))
        .collect();
    plan_coverage::coverage(tasks, &changed)
}

#[test]
fn a_planned_file_the_coder_says_needs_no_change_settles_and_stays_settled() {
    let md = the_plan();
    let mut tasks = plan_coverage::plan_tasks(&md);
    let cov = coverage_of(&tasks);
    assert_eq!(cov.required_missing().len(), 1);
    // The coder's words settle the task.
    assert!(
        plan_coverage::declined_by_coder(CODER_NOTES, "web/src/hooks/useStickyScroll.ts").is_some()
    );
    let settled = plan_coverage::settle_declined(&mut tasks, &cov, CODER_NOTES);
    assert_eq!(
        settled,
        vec!["web/src/hooks/useStickyScroll.ts".to_string()]
    );
    let cov = coverage_of(&tasks);
    assert_eq!(
        plan_coverage::next(&cov, plan_coverage::MAX_ROUNDS, false),
        Next::OpenWithOptional
    );
    // Persisted on the checklist, the next step reads it settled too: no
    // step asks the same task again.
    let saved =
        plan_coverage::with_declined(&plan_coverage::checked_tasks_md(&md, &cov), &cov.missing);
    let next_step = plan_coverage::plan_tasks(&saved);
    let again = coverage_of(&next_step);
    assert!(again.required_missing().is_empty(), "{saved}");
    // And the pipeline does both: settles from the implement pass's own
    // notes before any round, and saves the settled lines.
    let pc = include_str!("../plan_coverage.rs");
    let close = fn_src(pc, "pub async fn close_gaps(");
    assert!(close.contains("let mut notes = evidence.notes.clone();"));
    assert!(close.contains("settle_declined(&mut tasks, &cov, &notes)"));
    let src = include_str!("../mod.rs");
    assert!(src.contains("plan_coverage::with_declined("));
}

#[test]
fn the_step_after_one_that_changed_nothing_takes_a_different_approach() {
    let md = the_plan();
    let tasks = plan_coverage::plan_tasks(&md);
    let cov = coverage_of(&tasks);
    let files: Vec<String> = (1..=7)
        .map(|i| format!("web/src/blocks/part{i}.tsx"))
        .collect();
    let fb = plan_coverage::narrowed_feedback(&cov, &files).unwrap();
    assert!(fb.contains("takes a different approach: one task only"));
    assert!(fb.contains("task 8 of 8"));
    assert!(fb.contains("web/src/hooks/useStickyScroll.ts"));
    assert!(fb.contains(plan_coverage::NO_CHANGE_LINE));
    assert!(fb.contains("- `web/src/blocks/part1.tsx`"));
    // The usual ask is not what a stalled step gets.
    assert_ne!(fb, plan_coverage::round_feedback(&cov));
    // A coder that answers with the line settles the task.
    let line = format!(
        "{} web/src/hooks/useStickyScroll.ts: it measures the bar at runtime",
        plan_coverage::NO_CHANGE_LINE
    );
    assert!(plan_coverage::declined_by_coder(&line, "web/src/hooks/useStickyScroll.ts").is_some());
    // The pipeline passes the supervisor's rung and uses the narrowed ask,
    // with the rung's change of approach beside it.
    let src = include_str!("../mod.rs");
    let cf = fn_src(src, "async fn continuation_feedback(");
    assert!(cf.contains("rung: supervisor::Rung"));
    assert!(cf.contains("if rung != supervisor::Rung::Continue {"));
    assert!(cf.contains("plan_coverage::narrowed_feedback(&cov, &files)"));
    assert!(cf.contains("supervisor::rung_instruction(rung"));
    assert!(src.contains("stall_rung,"));
}

fn red() -> Ci {
    ready::ci_standing(
        &[
            serde_json::json!({"name": "Deploy Preview", "status": "completed",
            "conclusion": "failure", "app": {"slug": "github-actions"}}),
        ],
        &[],
    )
}

fn running() -> Ci {
    ready::ci_standing(
        &[
            serde_json::json!({"name": "Deploy Preview", "status": "in_progress",
            "conclusion": null, "app": {"slug": "github-actions"}}),
        ],
        &[],
    )
}

fn green() -> Ci {
    ready::ci_standing(
        &[
            serde_json::json!({"name": "Deploy Preview", "status": "completed",
            "conclusion": "success", "app": {"slug": "github-actions"}}),
        ],
        &[],
    )
}

/// The red-CI run's pull request at its hand-off: no CoderHelm findings, CI
/// failing. Before, `decide` said Ready.
#[test]
fn a_draft_with_failing_ci_is_held_with_the_check_named() {
    let f = Facts {
        draft: true,
        title: "WEB-14: Line up the side menu".into(),
        body: "Source ticket: WEB-14\n\n## Summary\nx\n\n## Changes\n- y\n\n## Verification\nz"
            .into(),
        ci: red(),
        ..Default::default()
    };
    assert!(ready::reads_ci(&f));
    match ready::decide(&f) {
        Decision::Held { reasons } => {
            assert!(
                reasons[0]
                    .what
                    .contains("CI is failing on this head: `Deploy Preview`"),
                "{reasons:?}"
            )
        }
        d => panic!("{d:?}"),
    }
    let running = Facts {
        ci: running(),
        ..f.clone()
    };
    assert!(matches!(ready::decide(&running), Decision::Wait { .. }));
    // A failure the base branch has too isn't this change's.
    let on_base = Facts {
        red_on_base: true,
        ..f.clone()
    };
    assert!(matches!(ready::decide(&on_base), Decision::Ready { .. }));
}

/// The ready decision across the combinations: never ready with a failing
/// or running check (not on the base branch too), or with planned work left.
#[test]
fn the_ready_decision_keeps_the_rules_across_combinations() {
    let unfinished = crate::passes::stop_pr::not_finished(
        &outcome::Stop::from_recorded("Stopped: steps 2 and 3 made no progress."),
        &crate::passes::stop_pr::Progress {
            done: vec!["`a.ts`: x".into()],
            remaining: vec!["`b.ts`: y".into()],
        },
        "",
    );
    let feature = Some(crate::passes::feature::FeatureHold {
        sandbox_failed: vec!["`pnpm test`: 1 failed".into()],
        checklist_fails: vec![],
    });
    let mut n = 0;
    for ci in [red(), running(), green(), Ci::default()] {
        for red_on_base in [false, true] {
            for approved in [false, true] {
                for feat in [None, feature.clone()] {
                    for security in [false, true] {
                        for body_unfinished in [false, true] {
                            let body = if body_unfinished {
                                format!("Source ticket: WEB-1\n\n{unfinished}\n\n## Summary\nx")
                            } else {
                                "Source ticket: WEB-1\n\n## Summary\nx".to_string()
                            };
                            let f = Facts {
                                draft: true,
                                body,
                                ci: ci.clone(),
                                red_on_base: red_on_base && ci.failing(),
                                approved,
                                feature: feat.clone(),
                                security,
                                ..Default::default()
                            };
                            let d = ready::decide(&f);
                            let ci_state = CiState::of(&f.ci, f.red_on_base);
                            if matches!(d, Decision::Ready { .. } | Decision::Proving { .. }) {
                                assert!(ci_state.allows_done(), "ready with {ci_state:?}: {f:?}");
                                assert!(!body_unfinished, "ready with planned work left");
                            }
                            n += 1;
                        }
                    }
                }
            }
        }
    }
    assert_eq!(n, 4 * 2 * 2 * 2 * 2 * 2);
}

/// The CI fix's ends: a fix that changed nothing is followed by another
/// attempt, and every give-up goes through the one hand-off (the draft says
/// what fails, the run needs a person); no CI path says "completed" past the
/// outcome rule.
#[test]
fn a_ci_fix_that_changes_nothing_tries_again_then_holds_and_needs_attention() {
    let src = include_str!("../resume.rs");
    let handle = fn_src(src, "pub async fn run(");
    let no_change = &handle[handle
        .find("if result.files_modified.is_empty() {")
        .unwrap()..];
    let no_change = &no_change[..no_change.find("super::GaveUp::NoChange").unwrap()];
    assert!(no_change.contains("if attempt < max_attempts\n"));
    assert!(no_change.contains("next_attempt_feedback("));
    assert!(no_change.contains("continue 'fix;"));
    assert!(no_change.contains("return hand_off_red_ci("));
    assert!(!no_change.contains("complete_run_with_status("));
    assert!(!no_change.contains("mark_pr_ready("));
    for why in ["Budget", "Stalled", "NoChange", "FixError", "NoProgress"] {
        let at = handle
            .find(&format!("super::GaveUp::{why},"))
            .unwrap_or_else(|| panic!("{why} goes through hand_off_red_ci"));
        let before = &handle[..at];
        let call = before.rfind("hand_off_red_ci(").unwrap();
        assert!(at - call < 900, "{why}");
    }
    let hand = fn_src(src, "async fn hand_off_red_ci(");
    assert!(hand.contains("with_ci_failing("));
    assert!(hand.contains("mark_pr_ready("));
    assert!(hand.contains("wait_for_review("));
    assert!(!hand.contains("\"completed\""));
    // Whatever path asks for "completed", red CI on the record turns it into
    // a wait on a person.
    let complete = fn_src(src, "async fn complete_run_with_status(");
    assert!(complete.contains("red_ci_blocks_completion("));
    assert_eq!(
        crate::passes::resume::ci_state_of_record("failed", "no_change"),
        CiState::Failing("the pull request's checks".into())
    );
    assert_eq!(
        crate::passes::resume::ci_state_of_record("failed", "red_on_base"),
        CiState::RedOnBase
    );
    assert_eq!(
        crate::passes::resume::ci_state_of_record("passed", ""),
        CiState::Green
    );
    // Each attempt reads fresh evidence, and the CI fix's conversation is
    // saved like every pass's.
    assert!(handle.contains("ci_evidence("));
    assert!(handle.contains("write_conversation_log("));
    assert!(handle.contains("looks_infra(&logs)"));
}
