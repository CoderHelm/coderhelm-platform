//! am. A dependency security run's pull request had the right code after a
//! review round and the wrong description:
//!
//! - its table still showed the plan: five packages as major version jumps
//!   ("2.3.1 to 4.0.4 (major)") when the final manifest keeps each inside
//!   its current major through a scoped override, and a headline that
//!   counted twenty major versions;
//! - its count of fixed advisories had never been compared with what the
//!   final lockfile installs;
//! - the round that fixed the code wrote "I can't edit the PR description
//!   from here" and left the corrections to a person;
//! - the repository's second app, a CMS studio no CI job builds, took a
//!   package an override moved to a new major, and nothing built it.
//!
//! Guarantee: the description is a function of the head. A dependency pull
//! request's table and headline are what the lockfile on the head resolves
//! compared with the base branch, an advisory counts as fixed only at or
//! past its patched version, and what can't be read is said as not known.
//! Every write of a description goes through one place, every path that
//! moves the head rebuilds it there, and a round's sentence that hands the
//! description to a person becomes the rebuild. A sentence a run writes
//! into a file that says a check passes needs that check's recorded result.
//! Every workspace project that takes a changed package is built when its
//! manifest declares a build, CI job or not; one that can't be proven is
//! named with why.

use super::fixture::*;
use crate::passes::{checks::CheckKind, description, file_claims, proof};
use common::dep_head::{self, Fixed, LockAt};
use common::runtime_proof as rp;
use common::security_fixes::{self as sec, Advisory, Bump, Finding, FindingState, FixUnit};

/// The fixture monorepo's lockfile: the site, the studio no CI job builds,
/// and the studio's tooling bringing in `glob-walk` on the 2 line.
fn lockfile(glob_walk: &str, path_match: &str) -> String {
    format!(
        "lockfileVersion: '9.0'\nimporters:\n  .:\n    devDependencies:\n      turbo:\n        specifier: ^2.0.0\n        version: 2.0.1\n  apps/web:\n    dependencies:\n      path-match:\n        specifier: ^3.1.0\n        version: {path_match}\n  apps/studio:\n    dependencies:\n      studio-kit:\n        specifier: ^3.0.0\n        version: 3.2.0\npackages:\n  glob-walk@{glob_walk}: {{}}\n  path-match@{path_match}: {{}}\n  studio-kit@3.2.0: {{}}\n  turbo@2.0.1: {{}}\nsnapshots:\n  glob-walk@{glob_walk}: {{}}\n  path-match@{path_match}: {{}}\n  studio-kit@3.2.0:\n    dependencies:\n      glob-walk: {glob_walk}\n  turbo@2.0.1: {{}}\n"
    )
}

fn unit() -> FixUnit {
    let bump = |package: &str, from: &str, to: &str| Bump {
        package: package.into(),
        from: from.into(),
        to: to.into(),
        major: sec::is_major_bump(from, to),
        manifest: "pnpm-lock.yaml".into(),
    };
    FixUnit {
        repo: format!("{OWNER}/{REPO}"),
        ecosystem: "npm".into(),
        bundle: true,
        // The plan: both as major version jumps.
        bumps: vec![
            bump("glob-walk", "2.3.1", "4.0.4"),
            bump("path-match", "3.1.2", "11.0.1"),
        ],
        ..Default::default()
    }
}

fn findings() -> Vec<(Finding, FindingState)> {
    let f = |id: &str, package: &str, version: &str, patched: &str| Finding {
        id: format!("f-{id}"),
        package: package.into(),
        version: version.into(),
        fixed_version: Some(patched.into()),
        manifest: "pnpm-lock.yaml".into(),
        advisory: Advisory {
            primary: id.into(),
            ids: vec![id.into()],
            ..Default::default()
        },
        ..Default::default()
    };
    vec![
        (
            f("GHSA-aaaa-0001", "glob-walk", "2.3.1", "2.3.2"),
            FindingState::default(),
        ),
        (
            f("GHSA-bbbb-0002", "path-match", "3.1.2", "3.1.5"),
            FindingState::default(),
        ),
    ]
}

fn lock_at(base: &str, head: &str) -> LockAt {
    LockAt {
        path: "pnpm-lock.yaml".into(),
        base: common::reachability::lock_copies("pnpm-lock.yaml", base),
        head: common::reachability::lock_copies("pnpm-lock.yaml", head),
        why: String::new(),
    }
}

/// The description after the review round kept both packages on their
/// lines: the table and the headline are the head's, not the plan's.
#[test]
fn the_table_and_headline_are_what_the_head_resolves() {
    let (unit, fs) = (unit(), findings());
    let pairs: Vec<(&Finding, &FindingState)> = fs.iter().map(|(f, s)| (f, s)).collect();
    let check = common::upgrade_notes::WhatToCheck::default();
    let links = sec::BodyLinks::default();
    // What the pull request said: the plan.
    let planned = sec::pr_body_linked(&unit, &pairs, "Bumped.", &check, &links);
    assert!(planned.contains("2.3.1 to 4.0.4 (major)"));
    // The head: scoped overrides keep each on its line.
    let head = dep_head::resolve(
        &unit,
        "5e1f00d7a9",
        &[lock_at(
            &lockfile("2.3.1", "3.1.2"),
            &lockfile("2.3.2", "3.1.5"),
        )],
    );
    let body = sec::pr_body_at_head(&unit, &pairs, "Bumped.", &check, &links, &head);
    let mut end = Ending::default();
    end.said(body.clone());
    assert!(end.says("| `glob-walk` | 2.3.1 to 2.3.2 |"), "{body}");
    assert!(end.says("| `path-match` | 3.1.2 to 3.1.5 |"), "{body}");
    assert!(
        end.says("Fixes 2 of 2 advisories in 2 package version lines (0 cross a major version), read from the lockfile on the head `5e1f00d`."),
        "{body}"
    );
    end.never_says("(major)");
    end.never_says("4.0.4");
    end.never_says("11.0.1");
    // A head that stops short of the patch is not counted as a fix.
    let short = dep_head::resolve(
        &unit,
        "0ddba11",
        &[lock_at(
            &lockfile("2.3.1", "3.1.2"),
            &lockfile("2.3.2", "3.1.4"),
        )],
    );
    assert_eq!(short.fixed(&unit, &fs[1].0), Fixed::No);
    let body = sec::pr_body_at_head(&unit, &pairs, "Bumped.", &check, &links, &short);
    assert!(body.contains("Fixes 1 of 2 advisories"), "{body}");
    assert!(
        body.contains("Not fixed on the head: GHSA-bbbb-0002"),
        "{body}"
    );
    // A head that can't be read: said, never the plan as a result.
    let blind = dep_head::unknown(&unit, "`pnpm-lock.yaml` could not be read on the head");
    let body = sec::pr_body_at_head(&unit, &pairs, "Bumped.", &check, &links, &blind);
    assert!(
        body.contains("What the head installs is not known"),
        "{body}"
    );
    assert!(
        body.contains("2.3.1 to 4.0.4 planned; not read from the head"),
        "{body}"
    );
    assert!(!body.contains("Fixes "), "{body}");
}

/// The rebuild replaces the stale sections of the open pull request and
/// keeps the rest; a second rebuild on the same head changes nothing.
#[test]
fn a_rebuild_replaces_the_stale_sections_in_place() {
    let (unit, fs) = (unit(), findings());
    let pairs: Vec<(&Finding, &FindingState)> = fs.iter().map(|(f, s)| (f, s)).collect();
    let check = common::upgrade_notes::WhatToCheck::default();
    let links = sec::BodyLinks::default();
    let open = format!(
        "<!-- coderhelm:draft -->\nDraft: waiting on CI.\n<!-- /coderhelm:draft -->\n\n{}",
        sec::pr_body_linked(&unit, &pairs, "Bumped.", &check, &links)
    );
    let head = dep_head::resolve(
        &unit,
        "5e1f00d7a9",
        &[lock_at(
            &lockfile("2.3.1", "3.1.2"),
            &lockfile("2.3.2", "3.1.5"),
        )],
    );
    let fresh = sec::pr_body_at_head(&unit, &pairs, "Bumped.", &check, &links, &head);
    let sections = ["## Summary", "## What to check", "## Proof"];
    let rebuilt = common::upgrade_notes::replace_sections(&open, &fresh, &sections);
    assert!(rebuilt.starts_with("<!-- coderhelm:draft -->\nDraft: waiting on CI."));
    assert!(!rebuilt.contains("(major)"), "{rebuilt}");
    assert!(rebuilt.contains("| `glob-walk` | 2.3.1 to 2.3.2 |"));
    assert!(rebuilt.contains(sec::MARKER));
    assert_eq!(
        common::upgrade_notes::replace_sections(&rebuilt, &fresh, &sections),
        rebuilt
    );
    // The pipeline rebuilds those sections from the head, in one place.
    let src = include_str!("../security_fixes.rs");
    let rebuild = fn_src(src, "pub(crate) async fn at_head(");
    assert!(rebuild.contains("head_facts("));
    assert!(rebuild.contains("sec::pr_body_at_head("));
    assert!(rebuild.contains("rebuilt_body(body, &fresh)"));
    let pure = fn_src(src, "pub(crate) fn rebuilt_body(");
    for heading in ["\"## Summary\"", "\"## What to check\"", "\"## Proof\""] {
        assert!(pure.contains(heading), "{heading}");
    }
    let reconcile = include_str!("../description.rs");
    assert!(reconcile.contains("security_fixes::at_head("));
}

/// Every section CoderHelm owns elsewhere survives a rebuild word for
/// word, wherever it sits: the draft status, review fixes, the settled
/// items, CI notes, the reviewer guide, open items, and what the run did
/// before and after the code.
#[test]
fn a_rebuild_keeps_every_other_owned_section() {
    use crate::passes::completeness::{self, Section};
    use crate::passes::{habits_check, security_fixes};
    let (unit, fs) = (unit(), findings());
    let pairs: Vec<(&Finding, &FindingState)> = fs.iter().map(|(f, s)| (f, s)).collect();
    let check = common::upgrade_notes::WhatToCheck::default();
    let links = sec::BodyLinks::default();
    let mut open = sec::pr_body_linked(&unit, &pairs, "Bumped.", &check, &links);
    let sections = [
        (Section::DraftStatus, "Draft: waiting on CI for this head."),
        (
            Section::ReviewFixes,
            "Round 1: scoped the overrides to their lines.",
        ),
        (
            Section::Settled,
            "Settled here: the audit ran on this head.",
        ),
        (
            Section::CiNotes,
            "The end-to-end job failed once and passed on a rerun: flaky.",
        ),
        (
            Section::ReviewerGuide,
            "Start with package.json, then the lockfile.",
        ),
        (
            Section::OpenFindings,
            "One finding of CoderHelm's own review is open.",
        ),
        (
            Section::Objections,
            "A reviewer's request for changes stands.",
        ),
    ];
    let mut blocks: Vec<String> = vec![];
    for (section, words) in sections {
        let (start, end) = section.markers();
        let block = format!("{start}\n{words}\n{end}");
        open = completeness::with_section(&open, section, block.clone());
        blocks.push(block);
    }
    let habits = format!(
        "{}\n### Before and after the code\n- Reproduced the advisory's path first.\n{}",
        habits_check::START,
        habits_check::END
    );
    open = habits_check::shape_body(open, &habits);
    blocks.push(habits);
    for b in &blocks {
        assert!(open.contains(b.as_str()), "the fixture lost {b}");
    }
    let head = dep_head::resolve(
        &unit,
        "5e1f00d7a9",
        &[lock_at(
            &lockfile("2.3.1", "3.1.2"),
            &lockfile("2.3.2", "3.1.5"),
        )],
    );
    let fresh = sec::pr_body_at_head(&unit, &pairs, "Bumped.", &check, &links, &head);
    let rebuilt = security_fixes::rebuilt_body(&open, &fresh);
    assert!(
        rebuilt.contains("| `glob-walk` | 2.3.1 to 2.3.2 |"),
        "{rebuilt}"
    );
    assert!(!rebuilt.contains("(major)"), "{rebuilt}");
    for b in &blocks {
        assert_eq!(
            rebuilt.matches(b.as_str()).count(),
            1,
            "lost or doubled: {b}\n{rebuilt}"
        );
    }
    assert!(rebuilt.contains(sec::MARKER));
    // The same head again writes nothing new.
    assert_eq!(security_fixes::rebuilt_body(&rebuilt, &fresh), rebuilt);
    // The model's rewrite keeps them too: the owned sections never reach
    // it, and what the run did before and after the code is put back.
    let pr = include_str!("../pr.rs");
    let refresh = &pr[pr.find("pub async fn refresh_description(").unwrap()..];
    let refresh = &refresh[..refresh.find("\nasync fn fill_placeholders_once(").unwrap()];
    assert!(refresh.contains("completeness::take_owned(&current)"));
    assert!(refresh.contains("habits_check::current_section(&rest)"));
    assert!(refresh.contains("habits_check::shape_body(written, &habits_section)"));
}

/// "I can't edit the PR description from here" never reaches a person: the
/// round is told the description is rebuilt, its account loses the
/// sentence, and the rebuild runs.
#[test]
fn a_round_never_hands_the_description_to_a_person() {
    let account = "Scoped the overrides so glob-walk stays on 2 and path-match on 3.\n\
                   I can't edit the PR description from here. Please update the table in the description: it still lists both as major.";
    let (clean, rebuild) = description::round_account(account);
    assert!(rebuild);
    let mut end = Ending::default();
    end.said(clean);
    assert!(end.says("Scoped the overrides so glob-walk stays on 2"));
    assert!(end.says(common::description_handoff::REBUILT));
    end.never_says("can't edit the PR description");
    end.never_says("Please update the table");
    // Every fix round is told, and has no reason to say it.
    let implement = include_str!("../implement.rs");
    assert!(implement.contains("common::description_handoff::ROUND_NOTE"));
    assert!(common::description_handoff::ROUND_NOTE.contains("rebuilt from the branch"));
    // The description itself never carries such a sentence either.
    let body = "## Open items\n- I can't edit the PR description from here.\n";
    assert!(!description::clean(body).contains("can't edit"));
}

/// The studio no CI job builds takes the changed `glob-walk` line through
/// its tooling: it is found from the lockfile and built from its manifest's
/// build script; when its build can't run in the sandbox, the pull request
/// names it as not proven.
#[test]
fn the_app_no_ci_job_builds_is_built_or_named_as_not_proven() {
    let asked = vec![
        ("glob-walk".to_string(), "2.3.1".to_string()),
        ("path-match".to_string(), "3.1.2".to_string()),
    ];
    let projects = common::reachability::lock_projects(
        "pnpm-lock.yaml",
        &lockfile("2.3.1", "3.1.2"),
        &[],
        &asked,
    )
    .unwrap();
    let affected: Vec<rp::AffectedProject> = projects
        .into_iter()
        .filter(|p| !p.uses.is_empty())
        .map(|p| rp::AffectedProject {
            manifest: Some(match p.dir.as_str() {
                "apps/studio" => r#"{"scripts":{"build":"studio build"}}"#.to_string(),
                _ => r#"{"scripts":{"build":"next build"}}"#.to_string(),
            }),
            dir: p.dir,
            uses: p.uses,
        })
        .collect();
    let dirs: Vec<&str> = affected.iter().map(|a| a.dir.as_str()).collect();
    assert_eq!(dirs, vec!["apps/studio", "apps/web"]);
    // CI builds the site only.
    let ci = [rp::CiBuild {
        cmd: "pnpm --filter web build".into(),
        dir: Some("apps/web".into()),
        workflow: "ci.yml".into(),
        job: "build".into(),
        on_pull_request: true,
    }];
    let mut plan = rp::discover("pnpm-lock.yaml", &ci, &[]);
    rp::add_projects(&mut plan, "pnpm-lock.yaml", &affected);
    let studio = plan
        .builds
        .iter()
        .position(|b| b.dir == "apps/studio")
        .unwrap();
    assert_eq!(plan.builds[studio].cmd, "pnpm run build");
    // The studio's build breaks only with the bumps: blamed, which is what
    // the take-out acts on.
    let base = rp::ProofResult {
        ran: true,
        builds: vec![Some(0), Some(0)],
        start: "skipped".into(),
        ..Default::default()
    };
    let mut head = base.clone();
    head.builds[studio] = Some(1);
    let blamed = rp::blamed(&plan, &base, &head);
    assert_eq!(
        blamed,
        vec!["the build `pnpm run build` fails (exit 1)".to_string()]
    );
    let drive = include_str!("../dep_fix.rs");
    assert!(fn_src(drive, "async fn proof_plan(").contains("affected_projects("));
    assert!(fn_src(drive, "async fn proof_plan(").contains("rp::add_lockfile("));
    // The studio's build needs a token the sandbox lacks: it fails on the
    // base branch too, so it is not blamed and is named as not proven.
    let mut base_red = base.clone();
    base_red.builds[studio] = Some(1);
    assert!(rp::blamed(&plan, &base_red, &head).is_empty());
    let mut end = Ending::default();
    end.said(rp::report(&plan, Some(&base_red), Some(&head)));
    assert!(end.says("Project `apps/studio` takes `glob-walk@2.3.1`: **not proven here**"));
    assert!(end.says("fails on the base branch too"));
    end.never_says("Covers the project of");
}

/// A sentence the run wrote into the README says the build passes; only
/// the tests have a recorded passing result.
#[test]
fn a_file_the_run_wrote_claims_only_what_a_check_recorded() {
    let diff = serde_json::json!({"files": [{
        "filename": "README.md", "status": "modified",
        "patch": "@@ -8,2 +8,4 @@\n ## Security updates\n+The build passes with the new versions.\n+All tests pass.\n Run `pnpm install` after pulling.\n"
    }]});
    let evidence = proof::RunEvidence {
        sandbox: proof::SandboxRecord {
            ran: vec![CheckKind::Tests],
            passed: vec![CheckKind::Tests],
            ..Default::default()
        },
        ..Default::default()
    };
    let open = file_claims::unproven_in_diff(&diff, &evidence);
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].sentence, "The build passes with the new versions.");
    let file = "## Security updates\nThe build passes with the new versions.\nAll tests pass.\nRun `pnpm install` after pulling.\n";
    let mut end = Ending::default();
    end.said(common::file_claims::without("README.md", file, &open).unwrap());
    end.never_says("The build passes");
    assert!(end.says("All tests pass."));
    assert!(end.says("Run `pnpm install` after pulling."));
}
