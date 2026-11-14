//! av. An outside reviewer read a dependency security pull request CoderHelm
//! had opened, found its code sound and its description wrong in five
//! places: a package row said "not moved on the head" when the lockfile
//! resolves the patched version, an open item said the lockfile was out of
//! sync when a frozen install passes, a fixed advisory was listed as not
//! fixed, the count of high advisories left was wrong, and the package was
//! still listed as open. They also found a title in the form
//! `fix(deps): ...` in a repository whose merged pull requests all read
//! `<ticket key>: <summary>`; commits prefixed `KEY-1:`, which is no ticket
//! (CoderHelm had learned a sample key); a claim "contract unchanged" with
//! nothing behind it; no word on why each major version jump was safe; and
//! CoderHelm's own review thread about the description left unresolved.
//!
//! What let it through: the description was text that many code paths
//! appended to, and nothing held a sentence against what was recorded. The
//! line a review fix round writes to say how it chose its commit message
//! gave the convention as the example `KEY-1: `, in the description, which
//! the learning steps read back; no lesson was ever checked for a
//! placeholder. A run without a ticket wrote `fix(deps):` whatever the
//! repository did.
//!
//! Guarantee: a description is a view of recorded facts
//! (`common::pr_facts`), keyed to the head. Every sentence a person or a
//! model wrote is checked against them on every rebuild, for every kind of
//! run (`common::pr_claims`), and one they do not bear out is rewritten
//! from them or removed. A major version jump states its evidence or that
//! none is recorded. Titles and commits follow the form detected in the
//! repository with a real key or none (`common::pr_conventions`), a lesson
//! that holds a placeholder is refused when learned and removed when found
//! (`common::placeholders`), and a rebuild resolves CoderHelm's own review
//! thread that said the description was behind.

use super::fixture::*;
use crate::passes::description_eval::{dependency_case, dependency_facts, DEPENDENCY_BEFORE};
use crate::passes::review_fix::commit_style;
use crate::passes::{conventions, lesson_cleanup, own_review};
use common::pr_claims::{self, Kind};
use common::pr_conventions::{self, Convention, Form, Inputs, Key, Sample, Sep, Source};
use common::rewards::lessons::{self, Distilled, NotCheckable};

const OLD_TITLE: &str = "fix(deps): security updates for web (3 packages, 4 advisories)";
const OLD_COMMIT: &str = "KEY-1: fix(deps): glob-walk 2.1.0 to 2.1.4";

/// 1. The description check catches every wrong statement, and the
///    description that is saved holds none of them.
#[test]
fn the_description_check_catches_every_statement_the_head_refutes() {
    let facts = dependency_facts();
    let found = pr_claims::check(DEPENDENCY_BEFORE, &facts);
    let kinds: Vec<Kind> = found.iter().map(|c| c.kind).collect();
    let said = |part: &str, kind: Kind| {
        assert!(
            found
                .iter()
                .any(|c| c.kind == kind && c.said.contains(part)),
            "not caught: {part} as {kind:?}\n{found:#?}"
        );
    };
    // The five statements the head refutes.
    said("`glob-walk` | 2.1.0, not moved on the head", Kind::NotMoved);
    said("The lockfile is out of sync", Kind::LockfileSync);
    said("GHSA-1111-2222-3333 is not fixed yet", Kind::AdvisoryState);
    said("1 high advisory remains", Kind::WrongCount);
    said("`glob-walk` is still open", Kind::NotMoved);
    // And the blanket claim, and the total.
    said("contract is unchanged", Kind::Unsupported);
    said("Fixes 31 advisories", Kind::WrongCount);
    assert_eq!(kinds.len(), 7, "{found:#?}");
    // Each correction names the fact it rests on.
    let lock = found.iter().find(|c| c.kind == Kind::LockfileSync).unwrap();
    assert!(
        lock.why
            .contains("a frozen install passes on `5e4d3c2`: `frozen install` passed"),
        "{}",
        lock.why
    );

    // What is saved: none of it, and the lead says why the major jump is
    // safe or that nothing recorded says so.
    let case = dependency_case();
    let out = case.shaped();
    let end = Ending {
        status: "awaiting_ci".into(),
        pr: Some(1),
        draft: false,
        words: vec![out.body.clone()],
    };
    for old in &case.flagged {
        end.never_says(old);
    }
    assert!(end.says(
        "`tiny-yaml` 1.2.0 to 2.0.0 crosses a major version. No safety evidence is recorded"
    ));
    assert!(pr_claims::check(&out.body, &out.facts).is_empty());
}

/// 2. The convention check catches the title and the commit prefix.
#[test]
fn the_convention_check_catches_the_title_and_the_sample_key() {
    // What people merged in that repository.
    let merged: Vec<Sample> = (400..424)
        .map(|n| Sample {
            text: format!("OPS-{n}: Change number {n}"),
            bot: false,
        })
        .chain((0..6).map(|n| Sample {
            text: format!("fix(deps): bump package-{n} from 1.0.0 to 1.0.1"),
            bot: true,
        }))
        .collect();
    let c = pr_conventions::detect(
        pr_conventions::Kind::Title,
        &Inputs {
            merged: &merged,
            ..Default::default()
        },
    );
    assert_eq!(c.form, Form::TicketKey(Sep::Colon));
    assert_eq!(c.source, Source::MergedPullRequests);
    assert_eq!(
        c.evidence,
        "24 of the last 24 merged pull requests written by people"
    );
    // The title as it was, and the commit as it was.
    let title = pr_conventions::violation(&c, OLD_TITLE, &[]).expect("the title is caught");
    assert!(title.contains("a ticket key and a summary"), "{title}");
    assert_eq!(
        pr_conventions::violation(&c, OLD_COMMIT, &[]).as_deref(),
        Some("`KEY-1` is a placeholder, not a ticket key")
    );
    // A key is real only as the run's ticket or in the tracker.
    assert!(!pr_conventions::real_key("KEY-1", None, Some(true)));
    assert!(!pr_conventions::real_key("OPS-431", None, None));
    // Written now: the repository's form with a real key, or the closest
    // valid form with one sentence saying why, and never an invented key.
    let run = conventions::ForRun {
        title: c.clone(),
        commit: c.clone(),
        key: Key::Real("OPS-431".into()),
    };
    let now = run.title(OLD_TITLE);
    assert_eq!(
        now.text,
        "OPS-431: Security updates for web (3 packages, 4 advisories)"
    );
    assert_eq!(
        pr_conventions::violation(&c, &now.text, &["OPS-431".to_string()]),
        None
    );
    assert_eq!(run.commit(OLD_COMMIT), "OPS-431: Glob-walk 2.1.0 to 2.1.4");
    let keyless = conventions::ForRun {
        key: Key::None(
            "creating a ticket for a run that has none is off in the team's settings".into(),
        ),
        ..run
    };
    let now = keyless.title(OLD_TITLE);
    assert_eq!(
        now.text,
        "Security updates for web (3 packages, 4 advisories)"
    );
    assert!(now.note.is_some(), "said once on the pull request");
    assert!(common::placeholders::found(&keyless.commit(OLD_COMMIT), &[]).is_empty());
    // The pipeline applies it where the title and commits are written.
    let pr = include_str!("../pr.rs");
    let run_src = fn_src(pr, "\npub async fn run(");
    assert!(run_src.contains("super::conventions::for_run("));
    assert!(run_src.contains("form.title(&title)") && run_src.contains("form.commit(&squash_msg)"));
    let dep = include_str!("../dep_fix.rs");
    assert!(dep.matches("super::conventions::commit_for(").count() >= 4);
    assert!(!dep.contains("let message = format!(\"fix(deps)"));
    let _ = Convention {
        form: Form::Plain,
        source: Source::Default,
        evidence: String::new(),
    };
}

/// 3. The lesson check catches the placeholder, where it was written,
///    where it was learned and where it is kept.
#[test]
fn the_lesson_check_catches_the_sample_key() {
    // Where it came from: the line a fix round writes into the description
    // no longer holds a sample key.
    let style = commit_style::learn(
        &["OPS-1: a", "OPS-2: b", "OPS-3: c", "OPS-4: d", "OPS-5: e"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
    );
    let how = commit_style::describe(&style);
    assert!(common::placeholders::found(&how, &[]).is_empty(), "{how}");
    assert!(!how.contains("KEY-1"));
    // Learned: refused.
    let lesson = Distilled {
        rule: "Prefix every commit subject with `KEY-1: ` in this repository.".into(),
        check: "The first commit subject starts with `KEY-1: `.".into(),
        ..Default::default()
    };
    assert_eq!(lessons::checkable(&lesson), Err(NotCheckable::Placeholder));
    // Kept from before: removed, once, from the maintenance tick.
    assert!(lesson_cleanup::purgeable(
        &format!("{}acme/site#l1", common::rewards::store::LESSON_PREFIX),
        &lesson.rule,
        &lesson.check
    ));
    let main = include_str!("../../main.rs");
    assert!(main.contains("passes::lesson_cleanup::run_once(&state)"));
    // A real lesson about commits is still kept.
    let real = Distilled {
        rule: "Keep the lockfile and `package.json` in the same commit.".into(),
        check: "A commit that changes `package.json` also changes `package-lock.json`.".into(),
        ..Default::default()
    };
    assert_eq!(lessons::checkable(&real), Ok(()));
}

/// 4. Its own review thread about the description is resolved by the
///    rebuild, and only its own.
#[test]
fn a_rebuild_resolves_coderhelms_own_thread_about_the_description() {
    assert!(own_review::says_description_is_behind(
        "The PR description still says `glob-walk` is not moved on the head."
    ));
    assert!(!own_review::says_description_is_behind(
        "Rename `walk` to `walk_all`."
    ));
    let src = include_str!("../description.rs");
    let resolve = fn_src(src, "\nasync fn resolve_own_description_threads(");
    // Only threads its own reviewer opened are listed.
    assert!(resolve.contains("list_bot_review_threads("));
    assert!(resolve.contains("says_description_is_behind("));
    assert!(resolve.contains("resolve_review_thread("));
    assert!(
        resolve.contains("github_grants::refused("),
        "said once where the app may not"
    );
    let build = fn_src(src, "\nasync fn build(");
    assert!(build.contains("resolve_own_description_threads("));
    // Every description takes the one path: facts, check, lead, layout.
    assert!(build.contains("description_facts::shape("));
    let save = fn_src(src, "\npub async fn save(");
    assert!(save.contains("pr_layout::layout("));
    let pr = include_str!("../pr.rs");
    let rewrite = fn_src(pr, "\npub async fn refresh_description(");
    assert!(
        rewrite.contains("description::save_checked("),
        "what a model writes is checked before it is saved"
    );
}
