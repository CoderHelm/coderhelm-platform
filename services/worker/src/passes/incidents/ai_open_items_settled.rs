//! ah. A dependency update finished "completed" with its checks passed, and
//! its pull request still listed five Open items for a person:
//!
//! 1. the remaining critical and high advisory counts were unknown, because
//!    the package manager's audit never ran and no CI command runs it;
//! 2. nothing showed that the packages an override moved to a new major
//!    still worked ("confirm with the build results and the tests");
//! 3. a README line said build, type check, lint and tests pass while no
//!    check had run (by the end of the run the line had been rewritten and
//!    the checks had run, and the item was still there);
//! 4. the audit's counts were unknown, again, in other words;
//! 5. the plan had no end-to-end checklist, on a change with nothing to
//!    take to production.
//!
//! The coder had asked for the full checks twice and was refused both times
//! ("not run: not enough time left") after two lockfile regenerations of
//! about three minutes each in the same step. Nothing carried the check
//! anywhere.
//!
//! Guarantee: a run settles its own open items before it finishes. What
//! CoderHelm can run (a check of the repository's own, the package
//! manager's audit after a change to the dependencies) runs in its sandbox
//! in a step with a full window, a fresh one when needed, and the result is
//! quoted on the pull request; a failing result is acted on. An item that
//! does not apply to the run is never raised, the same missing fact is one
//! item, and items are derived again when the head moves. What stays on
//! the pull request names why it stays.

use super::fixture::fn_src;
use crate::passes::checks::{CheckKind, Step};
use crate::passes::proof::{self, Finding};
use crate::passes::settle::{self, Action, Class, Facts, Next, Plan, Record, Room};
use crate::passes::triage::TicketIntent;
use common::dep_audit::{self, Audited, Counts, Outcome};

fn item(point: &str, check: &str, detail: &str, settle: &str) -> Finding {
    Finding {
        point: point.into(),
        check: check.into(),
        detail: detail.into(),
        settle: settle.into(),
        ..Default::default()
    }
}

/// The five items, as the run wrote them (package names removed).
fn five() -> Vec<Finding> {
    vec![
        item(
            "unrun_check",
            "security",
            "The remaining critical and high advisory counts are unknown because the audit never ran and no CI command runs it.",
            "Run it and quote the counts in the PR.",
        ),
        item(
            "assumption",
            "",
            "The root overrides force parents onto a new major they never declared, and nothing shows those parents work.",
            "Confirm with the web and Studio build results and the tests, and pin back within the current major any override whose parent breaks.",
        ),
        item(
            "assumption",
            "",
            "The README says build, type check, lint and tests pass, but no check has run on this branch.",
            "Rewrite that line from the actual results.",
        ),
        item(
            "external_fact",
            "",
            "The critical and high counts from the audit are unknown.",
            "",
        ),
        item(
            "end_to_end",
            "",
            "The plan has no end-to-end checklist.",
            "List every step the ticket needs in production and who does it.",
        ),
    ]
}

/// The run: a chore that changes a workspace's manifests and its lockfile.
fn run_facts() -> Facts {
    let tree: Vec<String> = [
        "package.json",
        "pnpm-lock.yaml",
        "pnpm-workspace.yaml",
        "apps/web/package.json",
        "apps/studio/package.json",
        "README.md",
    ]
    .iter()
    .map(|p| p.to_string())
    .collect();
    let changed: Vec<String> = ["package.json", "pnpm-lock.yaml", "README.md"]
        .iter()
        .map(|p| p.to_string())
        .collect();
    let audits = dep_audit::audits_for(&tree, &changed);
    assert_eq!(audits.len(), 1);
    assert_eq!(audits[0].shown, "pnpm audit --json");
    Facts {
        intent: TicketIntent::Chore,
        planned_by_rules: false,
        dep_change: !audits.is_empty(),
        // The coder's two refused checks.
        checks_owed: true,
    }
}

fn steps() -> Vec<Step> {
    let step = |kinds: &[CheckKind], cmd: &str| Step {
        kinds: kinds.to_vec(),
        cmd: cmd.into(),
        scope: None,
    };
    vec![
        step(
            &[CheckKind::Typecheck, CheckKind::Build],
            "pnpm install --frozen-lockfile && pnpm -r typecheck && pnpm -r build",
        ),
        step(&[CheckKind::Lint], "pnpm lint"),
        step(&[CheckKind::Tests], "pnpm test"),
    ]
}

fn audit(outcome: Outcome) -> Audited {
    Audited {
        manager: "pnpm".into(),
        lockfile: "pnpm-lock.yaml".into(),
        shown: "pnpm audit --json".into(),
        outcome,
    }
}

fn room() -> Room {
    Room {
        build: true,
        round: true,
        sandbox: true,
        dep_sets: 0,
    }
}

#[test]
fn each_of_the_five_items_is_run_or_dropped_never_listed_for_a_person() {
    let facts = run_facts();
    let items = five();
    let classes: Vec<Class> = items.iter().map(|f| settle::classify(f, &facts)).collect();
    assert_eq!(classes[0], Class::Run(vec![Action::Audit]));
    assert_eq!(
        classes[1],
        Class::Run(vec![
            Action::Check(CheckKind::Build),
            Action::Check(CheckKind::Tests)
        ])
    );
    assert_eq!(
        classes[2],
        Class::Run(vec![
            Action::Check(CheckKind::Typecheck),
            Action::Check(CheckKind::Build),
            Action::Check(CheckKind::Lint),
            Action::Check(CheckKind::Tests),
        ])
    );
    assert_eq!(classes[3], Class::Run(vec![Action::Audit]));
    assert!(matches!(classes[4], Class::NotApplicable(_)));
    assert!(
        !classes.iter().any(|c| matches!(c, Class::Human(_))),
        "none of the five needs anything only a person has"
    );

    // One build of the repository's checks and one audit answer all of them.
    let plan = settle::plan(&items, &facts);
    assert!(plan.checks && plan.audit);

    // The step the coder left had no window: a fresh step, not a guess.
    let mut rec = Record::default();
    rec.for_head("h1");
    let short = Room {
        build: false,
        ..room()
    };
    assert!(matches!(
        settle::next(&plan, &rec, &short),
        Next::FreshStep(_)
    ));
    assert_eq!(settle::next(&plan, &rec, &room()), Next::Build);

    // The fresh step ran both. Everything passes; the audit leaves one high.
    rec.record_checks(&steps(), &[Some(0), Some(0), Some(0)], "ok");
    rec.record_audits(&[audit(Outcome::Counts(Counts {
        critical: 0,
        high: 1,
        other: 3,
        unrated: 0,
    }))]);
    assert_eq!(settle::next(&plan, &rec, &room()), Next::Done);

    let settled = settle::apply(items, &facts, &rec);
    assert!(
        settled.open.is_empty(),
        "still listed: {:?}",
        settled.open.iter().map(|f| &f.detail).collect::<Vec<_>>()
    );
    // Items 1 and 4 were one item; 2 and 3 closed on the checks; 5 never
    // applied.
    assert_eq!(settled.closed.len(), 3);
    assert_eq!(settled.dropped.len(), 1);
    assert!(settled.closed[0]
        .1
        .contains("0 critical, 1 high and 3 lower advisories left"));
    assert!(settled.closed[1].1.contains("PASS"));

    // The pull request: no Open items, and the real results quoted.
    assert!(proof::gaps_section(&settled.open).is_empty());
    let section = settle::settled_section(&settled, &rec);
    assert!(section.contains(
        "`pnpm audit --json` on `pnpm-lock.yaml`: 0 critical, 1 high and 3 lower advisories left"
    ));
    assert!(section.contains("tests: PASS, `pnpm test`"));
    assert!(section.contains("on commit `h1`"));
    for old in [
        "counts are unknown",
        "no check has run",
        "end-to-end checklist",
    ] {
        assert!(!section.contains(old), "{old}");
    }
}

#[test]
fn a_failing_build_takes_out_what_breaks_it_and_ships_what_passes() {
    let facts = run_facts();
    let plan = settle::plan(&five(), &facts);
    let mut rec = Record::default();
    rec.for_head("h3");
    rec.record_checks(
        &steps(),
        &[Some(2), Some(0), Some(0)],
        "apps/studio build: error TS2305: Module '\"schema-kit\"' has no exported member 'defineKind'",
    );
    // The same build ran the audit.
    rec.record_audits(&[audit(Outcome::Counts(Counts {
        critical: 0,
        high: 0,
        other: 2,
        unrated: 0,
    }))]);
    // A dependency fix with verified sets on the branch: the newest set
    // comes out and the checks run again on what is left.
    let dep = Room {
        dep_sets: 3,
        ..room()
    };
    assert_eq!(settle::next(&plan, &rec, &dep), Next::TakeOutSet);
    let note = crate::passes::resume::settle_revert_note(
        &["`schema-kit` 3.4.0 to 4.0.1 deferred: the checks CoderHelm ran in its sandbox failed with it applied".to_string()],
        2,
    );
    assert!(note.contains("took out that set of 1 bump and kept the 2 sets before it"));
    assert!(note.contains("`schema-kit` 3.4.0 to 4.0.1 deferred"));
    // Any other run: the coder gets the real output, in a round of its own.
    assert_eq!(settle::next(&plan, &rec, &room()), Next::FixRound);
    let feedback = settle::fix_feedback(&rec, &five()[1..2]);
    assert!(feedback.contains("error TS2305"));
    assert!(feedback.contains("typecheck + build: FAIL (exit 2)"));
    assert!(feedback.contains("nothing shows those parents work"));
    // The head moved: the old results say nothing about the new one.
    rec.for_head("h4");
    assert_eq!(settle::next(&plan, &rec, &room()), Next::Build);
    assert_eq!(rec.rounds, 0, "counted where the round starts");

    // The pipeline: the same revert the red-CI path uses, written before
    // the run's saved state moves.
    let src = include_str!("../resume.rs");
    let take = fn_src(src, "pub(crate) async fn take_out_dep_set(");
    let write = take.find("apply_revert(").unwrap();
    assert!(take.find("take_out_last_commit(").unwrap() < write);
    assert!(write < take.find("dep_fix::save(").unwrap());
    assert!(write < take.find("dep_fix::settle(").unwrap());
}

#[test]
fn an_item_a_later_round_fixed_is_gone_when_the_head_moves() {
    let facts = run_facts();
    let readme = five().remove(2);
    // Written on the first head, where no check had run.
    let mut rec = Record::default();
    rec.for_head("h1");
    let open = settle::apply(vec![readme.clone()], &facts, &rec).open;
    assert_eq!(open.len(), 1);
    let body = format!(
        "## Summary\nx\n\n{}",
        settle::stamp(&proof::gaps_section(&open), "h1")
    );
    // A later round rewrote the line: the head is another one, the items
    // are of the old one, and the run goes back to its checks before it
    // finishes. Once per head.
    assert!(settle::redrive_due(&body, "h2", 0, ""));
    assert!(!settle::redrive_due(&body, "h2", 1, "h2"));
    assert!(!settle::redrive_due(&body, "h1", 0, ""));
    // There the checks run on the head, and the item is gone.
    rec.for_head("h2");
    rec.record_checks(&steps(), &[Some(0), Some(0), Some(0)], "ok");
    assert!(settle::apply(vec![readme], &facts, &rec).open.is_empty());

    // The pipeline: a run about to complete asks first, and the checks'
    // step writes every item with its head.
    let resume = include_str!("../resume.rs");
    let complete = fn_src(resume, "async fn complete_run_with_status(");
    assert!(
        complete.find("redrive_stale_items(").unwrap() < complete.find("end_run(").unwrap(),
        "the run ends before its open items are derived again"
    );
    let redrive = fn_src(resume, "async fn redrive_stale_items(");
    assert!(redrive.contains("settle::redrive_due("));
    assert!(redrive.contains("recheck_at_checks("));
    let passes = include_str!("../mod.rs");
    assert_eq!(
        passes
            .matches("settle::stamp(&proof::gaps_section(&gaps), &items_head)")
            .count(),
        2,
        "single and multi repository"
    );
}

#[test]
fn a_check_refused_for_lack_of_time_is_owed_and_run_in_a_fresh_step() {
    // The coder's tool records the refusal and never tells the coder to
    // finalise unverified.
    let implement = include_str!("../implement.rs");
    let checks = fn_src(implement, "async fn run_checks_impl(");
    let refused = &checks[checks
        .find("if !sandbox.has_time(self.deadline) {")
        .unwrap()..];
    let refused = &refused[..refused.find("let changed: Vec<String>").unwrap()];
    assert!(refused.contains("refused_for_time = true"));
    assert!(refused.contains("in a fresh step"));
    assert!(!refused.contains("finalize your changes now"));

    // Owed with no item at all, and carried until a step has the window.
    let facts = Facts {
        checks_owed: true,
        ..Default::default()
    };
    let plan = settle::plan(&[], &facts);
    assert_eq!(
        plan,
        Plan {
            checks: true,
            kinds: vec![],
            audit: false
        }
    );
    let rec = Record::default();
    let short = Room {
        build: false,
        ..room()
    };
    assert!(matches!(
        settle::next(&plan, &rec, &short),
        Next::FreshStep(_)
    ));

    // The pipeline: the stage runs after every check and before the
    // description is written, and a fresh step resumes at the checks.
    let passes = include_str!("../mod.rs");
    let stage = passes
        .find("let (gaps, settled) = match settle::run(")
        .unwrap();
    assert!(passes.find("let mut gaps = proof::prove(&work").unwrap() < stage);
    let after = &passes[stage..];
    assert!(
        after.find("settle_in_fresh_step(").unwrap() < after.find("pr::apply_checks(").unwrap()
    );
    let fresh = fn_src(passes, "async fn settle_in_fresh_step(");
    assert!(fresh.contains("steps::Gate::Checks"));
    assert!(fresh.contains("try_auto_continue("));

    // One lockfile regeneration per state of the branch.
    assert!(implement::lockfiles_current(Some("h1"), "h1"));
    assert!(!implement::lockfiles_current(Some("h1"), "h2"));
    assert!(!implement::lockfiles_current(None, "h1"));
    let locks = fn_src(implement, "async fn regenerate_lockfiles(");
    assert!(
        locks.find("lockfiles_current(").unwrap() < locks.find("run_lock_repair(").unwrap(),
        "a second regeneration of the same head starts a build"
    );
}

#[test]
fn what_stays_on_the_pull_request_names_why() {
    let facts = Facts {
        intent: TicketIntent::Feature,
        ..Default::default()
    };
    let items = vec![
        item(
            "external_fact",
            "",
            "The partner's webhook secret format is unconfirmed: no connection reaches their sandbox.",
            "",
        ),
        item("end_to_end", "", "No owner is named for: rotate the signing key", ""),
        item("unrun_check", "plan", "The plan never ran before the PR.", ""),
    ];
    let settled = settle::apply(items, &facts, &Record::default());
    assert_eq!(settled.open.len(), 3);
    let section = proof::gaps_section(&settled.open);
    for why in [
        "Only a person can settle this: access CoderHelm doesn't have.",
        "Only a person can settle this: a product decision.",
        "Only a person can settle this: a credential only a person can provide.",
    ] {
        assert!(section.contains(why), "{why}\n{section}");
    }
}

use crate::passes::implement;
