//! ah. A dependency security run of 64 packages finished with its checks
//! passed and its pull request open, and four things were wrong with it:
//!
//! - two packages were forced onto a new major version through an override
//!   (a 7.x copy pointed at `^8`, a 2.x copy at `^4`) although a patched
//!   release existed on the installed line. The unit listed one bump per
//!   package, to the highest target of all of the package's copies, so a
//!   copy on the 7 line read "to 8.y, a major version" because another copy
//!   sat on the 8 line. CoderHelm's own reviewer caught it and a fix round
//!   pinned them back;
//! - four overrides did have to cross a major version, and nothing showed
//!   their parents work on it: the sandbox ran the install, the type check,
//!   the linter and the unit tests. The application's build never ran and
//!   the application was never started;
//! - one pull request carried all 64 bumps, so one risky bump held every
//!   safe one;
//! - the pull request did not say how many advisories stayed open, or why.
//!
//! Guarantee: a bump stays on its installed version line with the lowest
//! patched release of that line, one bump per package and line. It crosses
//! a major version only when the advisory data lists no patched release on
//! the line, and says which advisory that is. A fix that crosses a major
//! version is a pull request of its own from the start, with its release
//! notes read first and, for a package only pulled in by others, the parent
//! named with the range it declares. Once the bumps are verified the
//! repository's real build (the command its CI runs) and its declared start
//! command run in a sandbox build of their own, on the base branch and on
//! the branch: what passes on the base and fails with the bumps takes out
//! the newest commit of bumps and defers them with the cause; what did not
//! run is said, never claimed. The pull request ends with the advisories
//! that stay open, counted by reason.

use super::fixture::fn_src;
use crate::passes::{dep_fix as worker_dep_fix, upgrade_notes as worker_notes};
use common::dep_fix::{self as df, Action, Cause, DepRun, Open};
use common::runtime_proof as rp;
use common::security_fixes::{
    self as sec, Combine, Finding, FindingState, Relation, Scope, SecurityConfig, Status, Tier,
    Verdict,
};
use common::upgrade_notes as un;

const LOCK: &str = "pnpm-lock.yaml";

fn finding(pkg: &str, version: &str, fixed: &[&str], parent: &str) -> (Finding, FindingState) {
    let st = FindingState {
        fixed: fixed.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    };
    let f = Finding {
        id: format!("f-{pkg}-{version}"),
        repo: "acme/shop".into(),
        manifest: LOCK.into(),
        ecosystem: "npm".into(),
        package: pkg.into(),
        version: version.into(),
        fixed_version: sec::pick_fixed(version, std::slice::from_ref(&st.fixed)),
        tier: Some(Tier::P2),
        verdict: Verdict::Unknown,
        relation: Relation::Transitive,
        scope: Scope::Runtime,
        status: Status::Open,
        path: vec![parent.to_string(), pkg.to_string()],
        due_at: "2026-12-10T00:00:00Z".into(),
        advisory: sec::Advisory {
            primary: format!("GHSA-{pkg}"),
            ids: vec![format!("GHSA-{pkg}")],
            ..Default::default()
        },
        ..Default::default()
    };
    (f, st)
}

/// The run's 64 packages: 58 with a patch on their line, two with a patched
/// release on their line and a newer major beside it (the two the run
/// forced onto the major), and four with no patched release on their line.
fn sixty_four() -> Vec<(Finding, FindingState)> {
    let mut fs: Vec<(Finding, FindingState)> = (0..58)
        .map(|i| finding(&format!("lib-{i:02}"), "2.1.0", &["2.1.5"], "app-shell"))
        .collect();
    fs.push(finding(
        "glob-walk",
        "7.2.0",
        &["7.5.4", "8.1.1"],
        "build-kit",
    ));
    fs.push(finding(
        "yaml-lite",
        "2.3.0",
        &["2.8.3", "4.0.1"],
        "conf-load",
    ));
    fs.push(finding("render-core", "3.4.0", &["4.0.2"], "page-kit"));
    fs.push(finding("route-kit", "1.9.0", &["2.0.0"], "page-kit"));
    fs.push(finding(
        "tmpl-engine",
        "2.3.0",
        &["3.9.0", "4.0.1"],
        "mail-kit",
    ));
    fs.push(finding("css-min", "5.1.0", &["6.0.3"], "build-kit"));
    fs
}

fn pairs(fs: &[(Finding, FindingState)]) -> Vec<(&Finding, &FindingState)> {
    fs.iter().map(|(f, s)| (f, s)).collect()
}

const MAJORS: [&str; 4] = ["css-min", "render-core", "route-kit", "tmpl-engine"];

#[test]
fn a_bump_stays_inside_its_major_and_a_major_jump_is_its_own_pull_request() {
    let mut fs = sixty_four();
    // The copy that misled the run: the same package, one line up, with its
    // own patched release.
    fs.push(finding(
        "glob-walk",
        "8.0.3",
        &["7.5.4", "8.1.1"],
        "page-kit",
    ));
    let cfg = SecurityConfig::default();
    let now = chrono::DateTime::parse_from_rfc3339("2026-11-11T10:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let units = sec::plan_units(&fs, &cfg, now);

    // The low-risk pull request: every bump that stays on its line.
    let bundles: Vec<&sec::FixUnit> = units.iter().filter(|u| u.bundle).collect();
    assert_eq!(bundles.len(), 1);
    let bundle = bundles[0];
    assert_eq!(bundle.finding_ids.len(), 61);
    assert!(bundle.bumps.iter().all(|b| !b.major), "{:?}", bundle.bumps);
    let lines: Vec<(&str, &str)> = bundle
        .bumps
        .iter()
        .filter(|b| b.package == "glob-walk" || b.package == "yaml-lite")
        .map(|b| (b.from.as_str(), b.to.as_str()))
        .collect();
    assert_eq!(
        lines,
        vec![("7.2.0", "7.5.4"), ("8.0.3", "8.1.1"), ("2.3.0", "2.8.3")],
        "one bump per version line, each to its own line's patched release"
    );
    // The ticket the coder reads never names the old target.
    let members: Vec<&Finding> = fs
        .iter()
        .map(|(f, _)| f)
        .filter(|f| bundle.finding_ids.contains(&f.id))
        .collect();
    let body = sec::ticket_body(bundle, &members, &cfg);
    assert!(
        body.contains("- `glob-walk` 7.2.0 to 7.5.4 (pnpm-lock.yaml) — stays on the 7 line"),
        "{body}"
    );
    assert!(!body.contains("`glob-walk` 7.2.0 to 8"), "{body}");
    assert!(!body.contains("`yaml-lite` 2.3.0 to 4"), "{body}");

    // What the sandbox writes for it: every override scoped to its line and
    // pointing inside it.
    let in_bundle: Vec<(&Finding, &FindingState)> = pairs(&fs)
        .into_iter()
        .filter(|(f, _)| bundle.finding_ids.contains(&f.id))
        .collect();
    let plan = df::plan_in(&in_bundle, &cfg, &[LOCK.to_string()]);
    assert_eq!(plan.bumps.len(), 61);
    assert!(plan.deferred.is_empty());
    let all: Vec<&df::PlanBump> = plan.bumps.iter().collect();
    for spec in df::specs(&all) {
        for b in &spec.bumps {
            assert_eq!(
                sec::version_line(&b.to),
                b.line,
                "the override for `{}@{}` leaves its line",
                b.name,
                b.line
            );
        }
    }

    // Each major jump: a pull request of its own, with why it is one.
    let own: Vec<&sec::FixUnit> = units.iter().filter(|u| !u.bundle).collect();
    let mut names: Vec<&str> = own.iter().map(|u| u.bumps[0].package.as_str()).collect();
    names.sort();
    assert_eq!(names, MAJORS);
    for u in &own {
        assert_eq!((u.bumps.len(), u.bumps[0].major), (1, true));
        let mine: Vec<(&Finding, &FindingState)> = pairs(&fs)
            .into_iter()
            .filter(|(f, _)| u.finding_ids.contains(&f.id))
            .collect();
        let plan = df::plan_in(&mine, &cfg, &[LOCK.to_string()]);
        let b = &plan.bumps[0];
        assert!(b.major);
        assert!(
            b.major_reason.starts_with(&format!(
                "GHSA-{} lists no patched release on the",
                b.package
            )),
            "{}",
            b.major_reason
        );
        let refs: Vec<&Finding> = mine.iter().map(|(f, _)| *f).collect();
        let body = sec::ticket_body(u, &refs, &cfg);
        assert!(
            body.contains(&format!(
                "a major version, because GHSA-{} lists no patched release on the",
                b.package
            )),
            "{body}"
        );
    }
    // The lowest patched release above the line, never the newest major.
    let tmpl = own
        .iter()
        .find(|u| u.bumps[0].package == "tmpl-engine")
        .unwrap();
    assert_eq!(tmpl.bumps[0].to, "3.9.0");

    // A person who asks for one pull request still gets one.
    let ids: Vec<String> = fs.iter().map(|(f, _)| f.id.clone()).collect();
    let (asked, _) = sec::manual_units_as(&fs, &ids, &cfg, now, Combine::OnePr);
    assert_eq!(asked.len(), 1);

    // The pipeline: the bundle keeps apart what crosses a major.
    let src = include_str!("../../../../common/src/security_fixes.rs");
    let alone = fn_src(src, "pub fn stands_alone(");
    assert!(alone.contains("crate::dep_fix::crosses_major(f, st, cfg)"));
    let add = fn_src(src, "fn add_bump(");
    assert!(
        add.contains("bumps.iter_mut().find(|b| in_bump(b, f))"),
        "a unit's bumps are merged by package alone again"
    );
}

/// A sandbox where every check passes and the application's build fails
/// with `breaks` applied.
fn drive(run: &mut DepRun, plan: &rp::ProofPlan, breaks: &[&str]) -> u32 {
    let green = df::ProbeResult {
        ran: true,
        applied: true,
        checks: ["install", "typecheck", "lint", "tests"]
            .iter()
            .map(|c| (c.to_string(), true))
            .collect(),
        tail: String::new(),
    };
    let proof = |fails: bool| {
        rp::parse(
            if fails {
                "Module not found: Can't resolve 'route-kit/legacy'\nCODERHELM_PROOF_STEP build 1 1\nCODERHELM_PROOF install=0 start=skipped port=0\n"
            } else {
                "CODERHELM_PROOF_STEP build 1 0\nCODERHELM_PROOF_PAGE / 200\nCODERHELM_PROOF_PAGE /healthz 404\nCODERHELM_PROOF_PAGE /health 404\nCODERHELM_PROOF_PAGE /api/health 200\nCODERHELM_PROOF install=0 start=up port=38411\n"
            },
            plan,
        )
    };
    let (mut commits, mut builds) = (0, 0);
    for _ in 0..200 {
        match run.next_action() {
            Action::Base => run.on_base(&green),
            Action::Probe(groups) => {
                let results: Vec<(usize, df::ProbeResult)> =
                    groups.iter().map(|g| (*g, green.clone())).collect();
                run.on_probes(&results);
            }
            Action::Commit(g) => {
                commits += 1;
                run.on_commit(g, &format!("sha{commits}"));
            }
            Action::CodeFix { .. } => panic!("nothing breaks a check here"),
            Action::ProofPlan => run.on_proof_plan(plan.clone()),
            Action::Proof { base } => {
                builds += 1;
                let fails = !base
                    && run
                        .applied_bumps()
                        .iter()
                        .any(|b| breaks.contains(&b.package.as_str()));
                run.on_proof(base, proof(fails));
            }
            Action::ProofTakeOut { why } => {
                let out = run.take_out_last_commit(&why).expect("a set to take out");
                commits += 1;
                run.on_proof_taken_out(&out, &format!("sha{commits}"));
            }
            // This case replays the runtime proof: nothing to verify with.
            Action::VerifyPlan => run.on_verify_plan(Default::default()),
            Action::Verify { .. } | Action::VerifyTakeOut { .. } => {
                panic!("no verification set in this case")
            }
            Action::Done => return builds,
        }
    }
    panic!("the run never ended");
}

/// The fixture's application: CI builds it with `pnpm --filter web build`,
/// and it declares a start script.
fn shop_proof() -> rp::ProofPlan {
    let workflows = [crate::passes::checks::Workflow {
        path: ".github/workflows/ci.yml".into(),
        text: "on:\n  pull_request:\njobs:\n  build:\n    steps:\n      - run: pnpm install --frozen-lockfile\n      - run: pnpm check-types\n      - run: pnpm lint\n      - run: pnpm --filter web build\n".into(),
    }];
    let ci = crate::passes::checks::build_commands(&workflows, "main");
    rp::discover(
        LOCK,
        &ci,
        &[(
            "package.json".to_string(),
            r#"{"name":"shop","scripts":{"build":"turbo build","start":"next start"}}"#.to_string(),
        )],
    )
}

#[test]
fn the_build_and_the_start_prove_the_bumps_and_what_breaks_them_is_taken_out() {
    // The command comes from the repository's CI, never from a list here.
    let plan = shop_proof();
    assert_eq!(plan.builds.len(), 1);
    assert_eq!(plan.builds[0].cmd, "pnpm --filter web build");
    assert_eq!(
        plan.builds[0].source,
        "the CI workflow `ci.yml`, job `build`"
    );
    assert_eq!(plan.start.as_ref().unwrap().cmd, "pnpm run start");
    let script = rp::script(&plan);
    assert!(script.contains("pnpm install --frozen-lockfile"));
    assert!(script.contains("( pnpm --filter web build ) > /tmp/coderhelm-proof-build-1.log"));
    assert!(script.contains("export PORT=38411 && pnpm run start"));

    // The run as the incident had it: one pull request, majors included.
    let fs = sixty_four();
    let cfg = SecurityConfig::default();
    let mut run = DepRun::new(df::plan(&pairs(&fs), &cfg), "base", 3.0);
    assert_eq!(run.plan.bumps.len(), 64);
    assert_eq!(run.plan.bumps.iter().filter(|b| b.major).count(), 4);
    // The checks pass with every bump; only the build shows `route-kit`.
    let builds = drive(&mut run, &plan, &["route-kit"]);
    assert!(run.proof.done && run.proof.unresolved.is_empty());
    // The base, the branch, then the branch without its newest set (still
    // red: that set was not the cause) and without the one before (green).
    assert_eq!(builds, 4);
    assert!(builds <= df::MAX_PROOF_RUNS);
    assert_eq!(run.applied.len(), 62);
    let left: Vec<&str> = run.deferred.iter().map(|d| d.package.as_str()).collect();
    assert_eq!(left, vec!["tmpl-engine", "route-kit"]);
    // The set whose removal made the build pass is the cause, and is held
    // out of later bundles with what failed.
    let culprit: Vec<&df::Deferral> = run.deferred.iter().filter(|d| !d.later).collect();
    assert_eq!(culprit.len(), 1);
    assert_eq!(
        (culprit[0].package.as_str(), culprit[0].cause),
        ("route-kit", Cause::Broke)
    );
    assert_eq!(
        culprit[0].reason,
        "with it applied the build `pnpm --filter web build` fails (exit 1), which passes on the base branch; taken out so the bumps before it ship"
    );
    // A newer set taken out before it changed nothing: it is not held as
    // breaking, and the next run takes it. No safe bump left the run.
    for d in run.deferred.iter().filter(|d| d.later) {
        assert_eq!(d.cause, Cause::Later);
        assert!(
            d.reason.contains("removing it did not make it pass"),
            "{}",
            d.reason
        );
    }
    assert!(run
        .deferred
        .iter()
        .all(|d| MAJORS.contains(&d.package.as_str())));
    assert!(run
        .applied_bumps()
        .iter()
        .any(|b| b.package == "glob-walk" && b.to == "7.5.4"));

    let report = run.report_md();
    assert!(
        report.contains("- Build `pnpm --filter web build` (from the CI workflow `ci.yml`, job `build`): passes with these bumps."),
        "{report}"
    );
    assert!(
        report.contains("the application started in the sandbox and answered on port 38411. `GET /` 200, `GET /api/health` 200."),
        "{report}"
    );
    assert!(report.contains("- Taken out by this proof: `route-kit` 1.9.0 to 2.0.0 deferred: with it applied the build"), "{report}");
    // Every applied major says why it is one.
    for b in run.applied_bumps().iter().filter(|b| b.major) {
        assert!(
            report.contains(&format!(
                "major version: GHSA-{} lists no patched release on the",
                b.package
            )),
            "{report}"
        );
    }

    // A repository that names no build and no start: said, never claimed.
    let mut bare = DepRun::new(df::plan(&pairs(&fs[..3]), &cfg), "base", 3.0);
    let nothing = rp::discover(LOCK, &[], &[]);
    assert!(nothing.is_empty());
    assert_eq!(drive(&mut bare, &nothing, &[]), 0);
    let report = bare.report_md();
    assert!(
        report.contains("- Not run: no workflow runs a build command"),
        "{report}"
    );
    assert!(
        report.contains("Nothing here claims the application builds or starts"),
        "{report}"
    );

    // The pipeline: the proof is the run's own step with a full window,
    // its commands are discovered, and a take-out is written before the
    // run's state moves.
    let common_src = include_str!("../../../../common/src/dep_fix.rs");
    let next = fn_src(common_src, "pub fn next_action(");
    let proof = next.find("self.proof_action()").expect("the proof step");
    assert!(
        proof < next[proof..].find("self.done = true;").unwrap() + proof,
        "a run ends without its runtime proof"
    );
    let dep = include_str!("../dep_fix.rs");
    let drive_src = fn_src(dep, "pub async fn drive(");
    let waits = drive_src
        .find("proof_waits(left, run.proof.waited)")
        .unwrap();
    let proves = drive_src.find("probe.prove(").unwrap();
    assert!(waits < proves, "a proof build starts without its window");
    let out = drive_src.find("next.take_out_last_commit(&why)").unwrap();
    let written = drive_src.find("restore_to(w, &out)").unwrap();
    let moved = drive_src
        .find("next.on_proof_taken_out(&out, &head)")
        .unwrap();
    assert!(out < written && written < moved);
    let plan_src = fn_src(dep, "async fn proof_plan(");
    assert!(plan_src.contains("checks::build_commands(&workflows, base)"));
    assert!(plan_src.contains("rp::discover(lockfile, &ci, &proof_files(w, lockfile).await)"));
    assert!(worker_dep_fix::proof_waits(
        Some(std::time::Duration::from_secs(240)),
        false
    ));
    assert!(!worker_dep_fix::proof_waits(
        Some(std::time::Duration::from_secs(240)),
        true
    ));
}

#[test]
fn a_major_jump_names_its_parent_and_the_pull_request_counts_what_stays_open() {
    // The parent of a transitive major is read from its dependency path,
    // and a major is read before any patch bump.
    let fs = sixty_four();
    let cfg = SecurityConfig::default();
    let all = pairs(&fs);
    let bump = sec::Bump {
        package: "css-min".into(),
        from: "5.1.0".into(),
        to: "6.0.3".into(),
        major: true,
        manifest: LOCK.into(),
    };
    let deps = [sec::Dep {
        ecosystem: "npm".into(),
        name: "build-kit".into(),
        version: "3.2.1".into(),
        manifest: LOCK.into(),
        ..Default::default()
    }];
    assert_eq!(
        worker_notes::parents_of(&bump, &all, &deps),
        (false, vec![("build-kit".to_string(), "3.2.1".to_string())])
    );
    // What the parent's registry record says, as the pull request reads.
    let record = serde_json::json!({"version": "3.2.1", "dependencies": {"css-min": "^5.0.0"}});
    let range = un::declared_range("npm", &record, "css-min").unwrap();
    let notes = un::PackageNotes {
        package: "css-min".into(),
        from: "5.1.0".into(),
        to: "6.0.3".into(),
        major: true,
        major_reason: df::major_reason(&fs[63].0, &fs[63].1),
        parents: vec![un::ParentSupport {
            parent: "build-kit".into(),
            version: "3.2.1".into(),
            admits: un::range_admits("npm", &range, "6.0.3"),
            declares: range,
            ..Default::default()
        }],
        ..Default::default()
    };
    let check = un::WhatToCheck {
        notes: vec![notes],
        ..Default::default()
    };
    let text = un::what_to_check(std::slice::from_ref(&bump), &check);
    assert!(
        text.contains("- Why a major version: GHSA-css-min lists no patched release on the 5 line (patched releases it lists: 6.0.3)."),
        "{text}"
    );
    assert!(
        text.contains("- Pulled in by `build-kit` 3.2.1, which declares `css-min: ^5.0.0`: 6.0.3 is outside the range it declares, so the override puts it on a version it does not declare support for."),
        "{text}"
    );
    assert!(un::needs_eyes(&bump, &check));
    let read = include_str!("../upgrade_notes.rs");
    let read_src = fn_src(read, "pub async fn read(");
    assert!(read_src.contains("let order = read_order(&unit.bumps);"));
    assert!(read_src.contains("parents_of(b, members, deps)"));

    // What stays open after the pull request, by reason.
    let mut run = DepRun::new(df::plan(&all, &cfg), "base", 3.0);
    drive(&mut run, &shop_proof(), &["route-kit"]);
    let mut repo: Vec<Finding> = fs.iter().map(|(f, _)| f.clone()).collect();
    let mut no_patch = finding("legacy-crypto", "1.2.0", &[], "auth-kit").0;
    no_patch.status = Status::NoFix;
    repo.push(no_patch);
    let refs: Vec<&Finding> = repo.iter().collect();
    let groups = df::remaining(&run, &refs);
    let by: Vec<(Open, usize)> = groups.iter().map(|g| (g.why, g.advisories)).collect();
    assert_eq!(
        by,
        vec![
            (Open::NoPatch, 1),
            (Open::Broke, 1),
            (Open::Later, run.deferred.len() - 1)
        ]
    );
    let text = df::remaining_md(&groups);
    assert!(
        text.starts_with(&format!(
            "**Advisories that stay open after this pull request: {}**",
            1 + run.deferred.len()
        )),
        "{text}"
    );
    assert!(
        text.contains(": no patched release exists yet (`legacy-crypto`)"),
        "{text}"
    );
    assert!(
        text.contains(": deferred because the build or the checks failed with the bump ("),
        "{text}"
    );
    // The pipeline: the count is written wherever the run's report is kept.
    let dep = include_str!("../dep_fix.rs");
    let settle = fn_src(dep, "pub async fn settle(");
    assert!(settle.contains("df::remaining_md(&df::remaining(run, &of_repo))"));
    assert!(settle.find("df::remaining_md(").unwrap() < settle.find("settle_dep_run(").unwrap());
}
