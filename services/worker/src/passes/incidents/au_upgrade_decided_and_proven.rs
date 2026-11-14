//! au. An outside reviewer checked a dependency security pull request of
//! about fifty packages, four of them across a major version. The code was
//! sound. The account of it was not:
//!
//! 1. "three high advisories left" was counted from CoderHelm's own
//!    findings; the package manager's production audit of the same head
//!    showed one, in a package the pull request does not touch;
//! 2. one package of a framework was raised to 10.4.22 while its siblings
//!    from the same source repository stayed at 10.4.15;
//! 3. no type check was recorded, because the repository's CI has none and
//!    CoderHelm's checks mirrored CI; the reviewer ran it themselves, and
//!    a frozen-lockfile install with the package manager version the
//!    deployment image installs;
//! 4. it said "contract unchanged" although a validation library's major
//!    changed the 400 error bodies of an endpoint;
//! 5. each override across a major lacked why it is safe: who depends on
//!    it, where that code runs, whether it is used, and a probe for the
//!    one behind a request (a real upload, with its size-limit error).
//!
//! Guarantee: a dependency update is decided and proven by the pipeline
//! itself. Packages released together are found from the registry and the
//! lockfile and move together, and a lockfile that splits them fails the
//! set. The set a dependency change is verified with is found in the
//! project, whatever CI runs, and each result is recorded against the head.
//! Advisories are counted once, the base branch and the head read the same
//! way and laid beside the package manager's audit, every disagreement
//! with its cause. Each major carries where it runs and how it is fixed.
//! "Unchanged" stands only on a probe: a changed answer is a known
//! behaviour change, and a sentence that claims otherwise is taken out.

use super::fixture::*;
use crate::passes::{checks, proof, settle};
use common::dep_audit::{Listed, Listing, Listings};
use common::dep_fix::{self as df, Action, DepRun};
use common::dep_head::{self, LockAt};
use common::runtime_proof as rp;
use common::security_fixes::{self as sec, Advisory, Bump, Finding, FindingState, FixUnit, Scope};
use common::upgrade_notes::{CallSite, PackageNotes, ParentSupport, WhatToCheck};
use common::upgrade_quality::{
    advisories as adv, behaviour as bh, family as fam, safety, verify as vq, UpgradeFacts,
};

const LOCK: &str = "pnpm-lock.yaml";
const HEAD: &str = "5e1f00d7a9c4";

/// The service's lockfile with each package at the version given.
fn lockfile(versions: &[(&str, &str)]) -> String {
    let entry = |n: &str, v: &str| {
        if n.starts_with('@') {
            format!("  '{n}@{v}': {{}}\n")
        } else {
            format!("  {n}@{v}: {{}}\n")
        }
    };
    let list: String = versions.iter().map(|(n, v)| entry(n, v)).collect();
    format!(
        "lockfileVersion: '9.0'\nimporters:\n  .:\n    dependencies:\n      shape-check:\n        specifier: ^{v}\n        version: {v}\npackages:\n{list}snapshots:\n{list}",
        v = versions
            .iter()
            .find(|(n, _)| *n == "shape-check")
            .map(|(_, v)| *v)
            .unwrap_or("3.2.0"),
    )
}

/// The base branch: a framework at one version, a validation library and
/// an upload middleware about to cross a major, a development tool, and
/// two packages the change never touches.
const BASE: &[(&str, &str)] = &[
    ("@lattice/common", "10.4.15"),
    ("@lattice/core", "10.4.15"),
    ("@lattice/platform-http", "10.4.15"),
    ("shape-check", "3.2.0"),
    ("upload-kit", "1.4.5"),
    ("lint-kit", "8.0.0"),
    ("xml-read", "1.1.0"),
    ("glob-walk", "2.4.0"),
];

/// The head the reviewed pull request had: one member of the family raised.
const SPLIT_HEAD: &[(&str, &str)] = &[
    ("@lattice/common", "10.4.22"),
    ("@lattice/core", "10.4.15"),
    ("@lattice/platform-http", "10.4.15"),
    ("shape-check", "4.0.1"),
    ("upload-kit", "2.0.2"),
    ("lint-kit", "8.0.0"),
    ("xml-read", "1.1.0"),
    ("glob-walk", "2.4.0"),
];

/// The head the pipeline makes: the family moved as one.
const WHOLE_HEAD: &[(&str, &str)] = &[
    ("@lattice/common", "10.4.22"),
    ("@lattice/core", "10.4.22"),
    ("@lattice/platform-http", "10.4.22"),
    ("shape-check", "4.0.1"),
    ("upload-kit", "2.0.2"),
    ("lint-kit", "8.0.0"),
    ("xml-read", "1.1.0"),
    ("glob-walk", "2.4.0"),
];

fn copies(of: &[(&str, &str)]) -> Vec<(String, String)> {
    common::reachability::lock_copies(LOCK, &lockfile(of)).expect("a lockfile CoderHelm reads")
}

fn bump(package: &str, from: &str, to: &str, id: &str) -> df::PlanBump {
    df::PlanBump {
        package: package.into(),
        from: from.into(),
        to: to.into(),
        line: sec::version_line(from),
        manifest: LOCK.into(),
        major: sec::is_major_bump(from, to),
        finding_ids: vec![id.into()],
        ..Default::default()
    }
}

/// What the registry says of the framework's packages and of one package
/// that only shares their version.
fn registry() -> Vec<fam::Known> {
    let known = |name: &str, repo: &str| fam::Known {
        name: name.into(),
        source: Some(repo.into()),
    };
    vec![
        known("@lattice/common", "lattice-js/lattice"),
        known("@lattice/core", "lattice-js/lattice"),
        known("@lattice/platform-http", "lattice-js/lattice"),
    ]
}

/// The family, found the way the run finds it at its start.
fn family() -> fam::Family {
    let base = copies(BASE);
    let candidates = fam::candidates("@lattice/common", "10.4.15", &base);
    assert_eq!(candidates, vec!["@lattice/core", "@lattice/platform-http"]);
    let mut f =
        fam::detect("@lattice/common", "10.4.15", LOCK, &base, &registry(), &[]).expect("a family");
    let published = f
        .members
        .iter()
        .map(|m| (m.clone(), vec!["10.4.22".to_string()]))
        .collect();
    f.target = fam::target(&f, &["10.4.22".to_string()], &published).unwrap();
    f
}

/// Point 2. The family is found from the registry and the lockfile; the
/// one bump carries its siblings; and the lockfile the reviewed pull
/// request had fails the set.
#[test]
fn a_family_moves_together_and_a_split_one_fails_the_set() {
    let f = family();
    assert_eq!(f.key, "lattice-js/lattice");
    assert_eq!(f.target, "10.4.22");
    let mut plan = df::DepPlan {
        bumps: vec![bump("@lattice/common", "10.4.15", "10.4.22", "f-common")],
        deferred: vec![],
    };
    let mut families = vec![f];
    fam::attach(&mut plan, &mut families);
    let told: Vec<String> = df::specs(&[&plan.bumps[0]])[0]
        .bumps
        .iter()
        .map(|b| format!("{}@{}", b.name, b.to))
        .collect();
    assert_eq!(
        told,
        vec![
            "@lattice/common@10.4.22",
            "@lattice/core@10.4.22",
            "@lattice/platform-http@10.4.22"
        ]
    );
    // The reviewed head: found by the probe's own check of the lockfile it
    // wrote, and red like any failing check.
    let bumps = [&plan.bumps[0]];
    let (split, _) = df::lockfile_violations(
        &families,
        &bumps,
        &[(LOCK.to_string(), lockfile(SPLIT_HEAD))],
    );
    assert_eq!(split.len(), 1, "{split:?}");
    assert!(split[0].contains("`@lattice/core` at 10.4.15"));
    let (whole, _) = df::lockfile_violations(
        &families,
        &bumps,
        &[(LOCK.to_string(), lockfile(WHOLE_HEAD))],
    );
    assert!(whole.is_empty());
    // The run: a probe whose lockfile splits the family never commits.
    let mut run = DepRun::new(plan, "base", 3.0);
    run.families = families.clone();
    let green = |extra: Option<&str>| df::ProbeResult {
        ran: true,
        applied: true,
        checks: ["install", "tests"]
            .iter()
            .map(|c| (c.to_string(), true))
            .chain(extra.map(|c| (c.to_string(), false)))
            .collect(),
        tail: split[0].clone(),
    };
    run.on_base(&green(None));
    let Action::Probe(g) = run.next_action() else {
        panic!("a probe")
    };
    run.on_probes(&[(g[0], green(Some(df::FAMILY_CHECK)))]);
    assert!(!matches!(run.next_action(), Action::Commit(_)));
    assert!(run.applied.is_empty());
    assert!(run.deferred[0]
        .reason
        .contains("released together at different versions"));
    // The driver makes the check at the probe, and finds families at the
    // start of the run.
    let drive = include_str!("../dep_fix.rs");
    assert!(fn_src(drive, "    async fn run(&self, sha: &str")
        .contains("df::lockfile_violations(&self.families, bumps, &files)"));
    assert!(fn_src(drive, "pub async fn start(")
        .contains("super::upgrade_quality::families(state, github, msg, &mut plan)"));
    // The pull request states it.
    let text = fam::section(&families, &[], &[]);
    assert!(
        text.contains("move together from 10.4.15 to 10.4.22"),
        "{text}"
    );
    assert!(
        text.contains("the registry names `lattice-js/lattice` as the source repository of each")
    );
}

/// The project: typed, with a build and tests, and a deployment image
/// that installs another pnpm than the manifest declares.
fn project() -> (Vec<String>, Vec<(String, String)>) {
    let tree = [
        "package.json",
        LOCK,
        "tsconfig.json",
        "Dockerfile",
        "src/main.ts",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let files = vec![
        (
            "package.json".to_string(),
            r#"{"packageManager":"pnpm@9.12.3","scripts":{"build":"lattice build","test":"vitest run","lint":"eslint ."}}"#.to_string(),
        ),
        (
            "Dockerfile".to_string(),
            "FROM node:22-slim\nRUN npm install -g pnpm@9.1.0\nCOPY . .\nRUN pnpm install --frozen-lockfile && pnpm build\n".to_string(),
        ),
    ];
    (tree, files)
}

fn verify_plan() -> vq::Plan {
    let (tree, files) = project();
    vq::discover(
        LOCK,
        &tree,
        &files,
        Some(("pnpm audit --prod --json", "pnpm audit --prod --json")),
    )
}

/// Point 3. The repository's CI runs lint and tests and no type check. The
/// set finds the type check in the project, installs with the manifest's
/// pnpm and with the image's, and records every result against the head;
/// an open item "the type check did not run" is closed by the record.
#[test]
fn the_type_check_runs_though_ci_has_none_and_is_recorded_on_the_head() {
    // What CI mirrors: nothing that type-checks.
    let ci = vec![checks::CiCommand {
        id: "C1".into(),
        kind: checks::CheckKind::Lint,
        tool: "eslint".into(),
        cmd: "pnpm exec eslint .".into(),
        workflow: "ci.yml".into(),
        job: "lint".into(),
        dir: None,
        sandbox: true,
    }];
    assert!(!checks::mirrored_steps(&ci)
        .iter()
        .any(|s| s.kinds.contains(&checks::CheckKind::Typecheck)));
    // What the project needs.
    let plan = verify_plan();
    let shown: Vec<&str> = plan.checks.iter().map(|c| c.shown.as_str()).collect();
    assert_eq!(
        shown,
        vec![
            "pnpm install --frozen-lockfile",
            "npx -y pnpm@9.1.0 install --frozen-lockfile --ignore-scripts",
            "npx --no-install tsc --noEmit",
            "pnpm run build",
            "pnpm run lint",
            "pnpm run test",
            "pnpm audit --prod --json",
        ]
    );
    // The run reaches the set after its proof, and its result is of the
    // commit the branch ends on.
    let mut run = DepRun::new(
        df::DepPlan {
            bumps: vec![bump("shape-check", "3.2.0", "3.2.9", "f-shape")],
            deferred: vec![],
        },
        "base",
        3.0,
    );
    let green = df::ProbeResult {
        ran: true,
        applied: true,
        checks: vec![("install".into(), true), ("tests".into(), true)],
        tail: String::new(),
    };
    let all: Vec<usize> = (0..plan.checks.len()).collect();
    let mut verified = 0;
    for _ in 0..50 {
        match run.next_action() {
            Action::Base => run.on_base(&green),
            Action::Probe(g) => run.on_probes(&[(g[0], green.clone())]),
            Action::Commit(g) => run.on_commit(g, HEAD),
            Action::ProofPlan => run.on_proof_plan(rp::ProofPlan::default()),
            Action::VerifyPlan => run.on_verify_plan(plan.clone()),
            Action::Verify { base } => {
                assert!(!base, "a green head needs no base run");
                verified += 1;
                let out: String = all
                    .iter()
                    .map(|i| format!("CODERHELM_VERIFY {i} 0\n"))
                    .collect();
                let sha = run.head.clone();
                run.on_verify(false, vq::read(&plan, &sha, &all, &[], &out));
            }
            Action::Done => break,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(verified, 1);
    let v = run.verify.head.clone().expect("a record");
    assert_eq!(v.sha, HEAD);
    assert!(v.not_run().is_empty());
    let text = vq::section(&v, None);
    assert!(
        text.contains("- type check: `npx --no-install tsc --noEmit`: passes."),
        "{text}"
    );
    assert!(text.contains("`Dockerfile` installs pnpm 9.1.0"), "{text}");
    assert!(
        text.contains("with pnpm 9.12.3 as `package.json` declares"),
        "{text}"
    );
    // The settle stage cites the record: the item never survives, and no
    // second build is asked for.
    let mut rec = settle::Record {
        head: HEAD.into(),
        ..Default::default()
    };
    rec.adopt_verified(&v, None);
    let facts = settle::Facts {
        dep_change: true,
        planned_by_rules: true,
        ..Default::default()
    };
    let item = proof::Finding {
        point: "unrun_check".into(),
        check: "typecheck".into(),
        detail: "The type check never ran before the pull request.".into(),
        ..Default::default()
    };
    let plan_of = settle::plan(std::slice::from_ref(&item), &facts);
    assert!(plan_of.checks);
    let room = settle::Room {
        build: true,
        round: true,
        sandbox: true,
        dep_sets: 1,
    };
    rec.audited = true;
    assert_eq!(settle::next(&plan_of, &rec, &room), settle::Next::Done);
    let settled = settle::apply(vec![item], &facts, &rec);
    assert!(settled.open.is_empty(), "{:?}", settled.open);
    assert!(settled.closed[0]
        .1
        .contains("typecheck: PASS, `npx --no-install tsc --noEmit`"));
    // A record of another commit is not taken: the set runs again.
    let mut other = settle::Record {
        head: "0ddba11".into(),
        ..Default::default()
    };
    other.adopt_verified(&v, None);
    assert!(!other.checked);
    assert_eq!(settle::next(&plan_of, &other, &room), settle::Next::Build);
    // A change to the dependencies outside a dependency run gets the set's
    // checks added to the repository's own.
    let steps = settle::with_min_set(checks::mirrored_steps(&ci), std::slice::from_ref(&plan));
    assert!(steps
        .iter()
        .any(|s| s.kinds == vec![checks::CheckKind::Typecheck]
            && s.cmd == "npx --no-install tsc --noEmit"));
    // The driver runs the set and the settle stage reads it.
    let drive = include_str!("../dep_fix.rs");
    assert!(drive.contains("Action::Verify { base } =>"));
    assert!(drive.contains("probe.verify(&sha, &plan, run.verify.runs as usize)"));
    let settle_src = include_str!("../settle.rs");
    assert!(fn_src(settle_src, "pub async fn run(").contains("rec.adopt_verified("));
    assert!(fn_src(settle_src, "async fn build(").contains("with_min_set(steps"));
}

fn finding(id: &str, package: &str, version: &str, patched: &str, scope: Scope) -> Finding {
    Finding {
        id: format!("f-{package}"),
        repo: format!("{OWNER}/{REPO}"),
        package: package.into(),
        version: version.into(),
        fixed_version: (!patched.is_empty()).then(|| patched.to_string()),
        manifest: LOCK.into(),
        scope,
        advisory: Advisory {
            primary: id.into(),
            ids: vec![id.into()],
            source: "dependabot".into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// CoderHelm's findings for the repository after the pull request: three
/// high advisories it still holds as open.
fn left_open() -> Vec<(Finding, FindingState)> {
    let high = FindingState {
        severity: Some("high".into()),
        ..Default::default()
    };
    vec![
        // Recorded for a version an override never lets the lockfile have.
        (
            finding(
                "GHSA-aaaa-aaaa-aaaa",
                "glob-walk",
                "2.3.1",
                "2.3.9",
                Scope::Runtime,
            ),
            high.clone(),
        ),
        // In a development tool.
        (
            finding(
                "GHSA-bbbb-bbbb-bbbb",
                "lint-kit",
                "8.0.0",
                "9.0.0",
                Scope::Dev,
            ),
            high.clone(),
        ),
        // In production, in a package the change does not touch.
        (
            finding(
                "GHSA-cccc-cccc-cccc",
                "xml-read",
                "1.1.0",
                "",
                Scope::Runtime,
            ),
            high,
        ),
    ]
}

fn audit_of_the_head() -> Listings {
    let listed = |id: &str, pkg: &str| Listed {
        id: id.into(),
        severity: "high".into(),
        package: pkg.into(),
    };
    Listings {
        production: Some(Listing {
            shown: "pnpm audit --prod --json".into(),
            total: 1,
            listed: vec![listed("GHSA-CCCC-CCCC-CCCC", "xml-read")],
        }),
        all: Some(Listing {
            shown: "pnpm audit --json".into(),
            total: 2,
            listed: vec![
                listed("GHSA-CCCC-CCCC-CCCC", "xml-read"),
                listed("GHSA-BBBB-BBBB-BBBB", "lint-kit"),
            ],
        }),
        why: String::new(),
    }
}

fn reconciled() -> adv::Reconciled {
    let held: Vec<adv::Held> = left_open()
        .iter()
        .map(|(f, st)| adv::Held::of(f, st))
        .collect();
    let overrides = adv::overrides_of(r#"{"pnpm":{"overrides":{"glob-walk@2":"^2.4.0"}}}"#);
    adv::reconcile(&adv::Input {
        sha: HEAD,
        lockfile: LOCK,
        held: &held,
        base: &copies(BASE),
        head: &copies(WHOLE_HEAD),
        audit: Some(&audit_of_the_head()),
        overrides: &overrides,
        ..Default::default()
    })
}

/// Point 1. The number the pull request gave, and the one count the
/// pipeline now gives for the same head.
#[test]
fn the_advisories_left_are_counted_once_against_the_audit_of_the_head() {
    // What was written: three, from CoderHelm's findings alone.
    let open = left_open();
    let refs: Vec<&Finding> = open.iter().map(|(f, _)| f).collect();
    let run = DepRun::default();
    let old: usize = df::remaining(&run, &refs)
        .iter()
        .map(|g| g.advisories)
        .sum();
    assert_eq!(old, 3);
    // What the head has: two, one of them in production, none in a
    // package the change touches; the third was never installed.
    let r = reconciled();
    assert_eq!(r.remaining.len(), 2);
    assert_eq!(r.remaining_by_scope(), (1, 1, 0));
    assert!(r.remaining.iter().all(|e| !e.touched));
    let causes: Vec<adv::Cause> = r.differences.iter().map(|d| d.cause).collect();
    assert_eq!(
        causes,
        vec![adv::Cause::Override, adv::Cause::DevelopmentOnly]
    );
    let mut end = Ending::default();
    end.said(r.section());
    assert!(end.says("2 remaining (1 in production, 1 development only), 0 introduced."));
    assert!(end.says("compared with `pnpm audit --prod --json` and `pnpm audit --json`"));
    assert!(end.says("in `xml-read` 1.1.0 (production; this change does not touch the package): no patched release exists"));
    assert!(end.says("the manifest overrides the package, which the source of the finding does not read. Not counted."));
    end.never_says("3 remaining");
    // The run's report gives one count: the reconciled one when it was
    // recorded for this head, CoderHelm's own (said to be its own) only
    // without it.
    let settle_src = include_str!("../dep_fix.rs");
    let settle_fn = fn_src(settle_src, "pub async fn settle(");
    assert!(settle_fn.contains("f.of_head(&run.head) && f.advisories.is_some()"));
    assert!(settle_fn.contains("if reconciled {"));
}

/// The call sites the upgrade notes found for the two production majors.
fn notes() -> Vec<PackageNotes> {
    let note =
        |package: &str, from: &str, to: &str, api: &str, file: &str, breaking: &str| PackageNotes {
            package: package.into(),
            ecosystem: "npm".into(),
            from: from.into(),
            to: to.into(),
            major: true,
            versions: vec![to.into()],
            breaking: vec![breaking.into()],
            apis: vec![api.into()],
            call_sites: vec![CallSite {
                api: api.into(),
                file: file.into(),
                line: 12,
                text: format!("{api}("),
            }],
            ..Default::default()
        };
    vec![
        note(
            "shape-check",
            "3.2.0",
            "4.0.1",
            "safeParse",
            "src/orders.controller.ts",
            "`safeParse` reports issues in a new format; `message` is replaced by `error`",
        ),
        PackageNotes {
            parents: vec![ParentSupport {
                parent: "@lattice/platform-http".into(),
                version: "10.4.22".into(),
                declares: "^1.4.0".into(),
                admits: Some(false),
                ..Default::default()
            }],
            ..note(
                "upload-kit",
                "1.4.5",
                "2.0.2",
                "FileInterceptor",
                "src/files.controller.ts",
                "`FileInterceptor` rejects an oversized file with a new error code",
            )
        },
    ]
}

fn call_site_files() -> Vec<(String, String)> {
    vec![
        (
            "src/orders.controller.ts".to_string(),
            "@Controller('orders')\nexport class OrdersController {\n  @Post()\n  create(@Body() body: unknown) { return schema.safeParse(body); }\n}\n".to_string(),
        ),
        (
            "src/files.controller.ts".to_string(),
            "@Controller('files')\nexport class FilesController {\n  @Post('upload')\n  @UseInterceptors(FileInterceptor('document', { limits: { fileSize: 2 * 1024 * 1024 } }))\n  upload() {}\n}\n".to_string(),
        ),
    ]
}

/// The answers of the two builds: the validation error's body changed, the
/// uploads answer as before.
fn answers(validation_body: &str, sha: &str) -> Vec<Option<rp::ProbeAnswer>> {
    let a = |status: u16, sha: &str, excerpt: &str| {
        Some(rp::ProbeAnswer {
            status,
            content_type: "application/json".into(),
            sha: sha.into(),
            excerpt: excerpt.into(),
            stable: true,
        })
    };
    vec![
        a(400, sha, validation_body),
        a(201, "f11e", "{\"ok\":true}"),
        a(
            400,
            "bad0",
            "{\"message\":\"Unexpected end of JSON input\"}",
        ),
        a(413, "b166", "{\"message\":\"File too large\"}"),
    ]
}

/// Point 4. The requests go through the call sites; the changed 400 body
/// is a known behaviour change; and the sentence "contract unchanged"
/// does not survive into the description.
#[test]
fn a_changed_error_body_is_found_and_unchanged_is_never_written_without_a_probe() {
    let planned = bh::plan(&notes(), &call_site_files());
    let asked: Vec<String> = planned
        .requests
        .iter()
        .map(|q| format!("{} {} ({})", q.method, q.path, q.what))
        .collect();
    assert_eq!(
        asked,
        vec![
            "POST /orders (an empty JSON object (the validation error))",
            "POST /files/upload (a small upload in the field `document`)",
            "POST /orders (a malformed JSON body (the parser error))",
            "POST /files/upload (an upload over the size limit the file states (2048 KiB))",
        ]
    );
    // The proof sends them to both builds, and reads the answers back.
    let plan = rp::ProofPlan {
        install: "pnpm install --frozen-lockfile".into(),
        start: Some(rp::ProofCmd {
            cmd: "pnpm run start".into(),
            ..Default::default()
        }),
        probes: planned.requests.clone(),
        ..Default::default()
    };
    let script = rp::script(&plan);
    assert!(script.contains("CODERHELM_PROOF_PROBE 4 $ch_t"));
    assert!(script.contains("head -c 2162688 /dev/zero >> /tmp/coderhelm-probe-4.in"));
    assert!(script.contains("-X POST -H 'Content-Type: multipart/form-data; boundary=coderhelmprobe' --data-binary @/tmp/coderhelm-probe-4.in"));
    let read = rp::parse(
        "CODERHELM_PROOF_PROBE 1 1 400 application/json aaaa eyJtZXNzYWdlIjpbIm5hbWUiXX0=\nCODERHELM_PROOF_PROBE 1 2 400 application/json aaaa \nCODERHELM_PROOF install=0 start=up port=38411\n",
        &plan,
    );
    assert_eq!(
        read.probes[0].as_ref().unwrap().excerpt,
        "{\"message\":[\"name\"]}"
    );
    assert!(read.probes[0].as_ref().unwrap().stable);
    assert!(read.probes[1].is_none());
    // Base against head.
    let base = answers(
        "{\"message\":[\"name must be a string\"],\"error\":\"Bad Request\"}",
        "aaaa",
    );
    let head = answers(
        "{\"issues\":[{\"path\":[\"name\"],\"code\":\"invalid_type\"}]}",
        "bbbb",
    );
    let compared = bh::compare(&planned.requests, &base, &head);
    let of = |package: &str| -> Vec<bh::Compared> {
        planned
            .of
            .iter()
            .find(|(p, _)| p == package)
            .map(|(_, idx)| idx.iter().map(|i| compared[*i].clone()).collect())
            .unwrap()
    };
    let n = notes();
    let shape = bh::Behaviour {
        package: "shape-check".into(),
        from: "3.2.0".into(),
        to: "4.0.1".into(),
        breaking_used: bh::used_breaking(&n[0]),
        compared: of("shape-check"),
        consumers_searched: "2 other repositories of the team, for `/orders`".into(),
        ..Default::default()
    };
    let upload = bh::Behaviour {
        package: "upload-kit".into(),
        from: "1.4.5".into(),
        to: "2.0.2".into(),
        compared: of("upload-kit"),
        ..Default::default()
    };
    assert_eq!(shape.changes().len(), 1);
    assert!(!shape.unchanged_shown());
    // The upload, small and over the limit, answers as before: shown.
    assert!(upload.unchanged_shown());
    let facts = UpgradeFacts {
        sha: HEAD.into(),
        behaviour: vec![shape, upload],
        ..Default::default()
    };
    assert!(!facts.unchanged_shown());
    // The description: the run's own words claimed it.
    let unit = FixUnit {
        repo: format!("{OWNER}/{REPO}"),
        ecosystem: "npm".into(),
        bumps: vec![Bump {
            package: "shape-check".into(),
            from: "3.2.0".into(),
            to: "4.0.1".into(),
            major: true,
            manifest: LOCK.into(),
        }],
        ..Default::default()
    };
    let fs = [(
        finding(
            "GHSA-dddd-dddd-dddd",
            "shape-check",
            "3.2.0",
            "4.0.1",
            Scope::Runtime,
        ),
        FindingState::default(),
    )];
    let pairs: Vec<(&Finding, &FindingState)> = fs.iter().map(|(f, s)| (f, s)).collect();
    let at_head = dep_head::resolve(
        &unit,
        HEAD,
        &[LockAt {
            path: LOCK.into(),
            base: Some(copies(BASE)),
            head: Some(copies(WHOLE_HEAD)),
            why: String::new(),
        }],
    );
    let check = WhatToCheck {
        upgrade_facts: Some(facts),
        ..Default::default()
    };
    let body = sec::pr_body_at_head(
        &unit,
        &pairs,
        "Raised the validation library. Contract unchanged. The build passes.",
        &check,
        &sec::BodyLinks::default(),
        &at_head,
    );
    let mut end = Ending::default();
    end.said(body.clone());
    end.never_says("Contract unchanged");
    assert!(end.says(bh::TAKEN_OUT), "{body}");
    assert!(end.says("Known behaviour change: `POST /orders` (an empty JSON object (the validation error)) answered 400 application/json on the base branch's build and 400 application/json on this head's: body before `{\"message\":[\"name must be a string\"]"), "{body}");
    assert!(end.says("Its notes list a breaking change in an API this repository calls: `safeParse` reports issues in a new format"), "{body}");
    assert!(end.says("Who could care: no other repository names these endpoints (searched 2 other repositories of the team, for `/orders`)."), "{body}");
    assert!(
        end.says("Unchanged on these 2 requests; nothing is claimed beyond them."),
        "{body}"
    );
    assert!(
        end.says("Recorded by CoderHelm on commit `5e1f00d`."),
        "{body}"
    );
    // The proof plan carries the requests, at its place in the run.
    let drive = include_str!("../dep_fix.rs");
    assert!(drive.contains("super::upgrade_quality::behaviour_plan(state, w.github, msg, &run)"));
    assert!(drive.contains("plan.probes = planned.requests;"));
}

/// Point 5. Each major carries where it runs, who brings it in, how it is
/// fixed and whether it needs a probe.
#[test]
fn each_major_says_where_it_runs_how_it_is_fixed_and_whether_it_is_probed() {
    let n = notes();
    // Behind a request, through a framework package the application
    // imports, fixed by an override that leaves the parent's range.
    let paths = [safety::PathIn {
        root: "@lattice/platform-http".into(),
        declared: safety::Declared::Production,
        importers: vec!["src/files.controller.ts".into()],
        ..Default::default()
    }];
    let used = vec!["src/files.controller.ts:12".to_string()];
    let upload = safety::evidence(&safety::MajorIn {
        package: "upload-kit",
        from: "1.4.5",
        to: "2.0.2",
        ecosystem: "npm",
        why_major: "GHSA-eeee lists no patched release on the 1 line",
        paths: &paths,
        used_in: &used,
        searched:
            "`upload-kit` and the APIs its notes name (`FileInterceptor`) in the repository's code",
        verdict: sec::Verdict::NotReachable,
        parents: &n[1].parents,
        selector: "upload-kit@1",
        ..Default::default()
    });
    // A development tool's dependency.
    let dev = [safety::PathIn {
        root: "lint-kit".into(),
        declared: safety::Declared::Development,
        importers: vec![".eslintrc.js".into()],
        ..Default::default()
    }];
    let tool = safety::evidence(&safety::MajorIn {
        package: "glob-walk",
        from: "2.4.0",
        to: "4.0.4",
        paths: &dev,
        searched: "`glob-walk` in the repository's code",
        verdict: sec::Verdict::NotLoaded,
        selector: "glob-walk@2",
        ..Default::default()
    });
    assert_eq!(upload.class, safety::PathClass::Production);
    assert!(matches!(upload.probe, safety::ProbeNeed::Needed { .. }));
    assert_eq!(tool.class, safety::PathClass::Development);
    assert_eq!(tool.probe, safety::ProbeNeed::NotNeeded);
    let facts = UpgradeFacts {
        sha: HEAD.into(),
        majors: vec![upload, tool],
        ..Default::default()
    };
    let mut end = Ending::default();
    end.said(facts.section(HEAD));
    assert!(end.says("- `upload-kit` 1.4.5 to 2.0.2 (major): runs in production."));
    assert!(end.says("Path through `@lattice/platform-http` (production): `@lattice/platform-http` is imported by `src/files.controller.ts`."));
    assert!(end.says("Use in this repository: `src/files.controller.ts:12`."));
    assert!(end.says("That is not proof of safety"));
    assert!(end.says("Fixed by an override scoped to its version line (`upload-kit@1`)"));
    assert!(end.says("Outside a declared range: `@lattice/platform-http` 10.4.22 declares `^1.4.0` for it, and the override installs 2.0.2, outside that range."));
    assert!(end.says("Needs a functional probe: it runs in production and the repository uses it"));
    assert!(end.says("- `glob-walk` 2.4.0 to 4.0.4 (major): runs in development only."));
    // A production major with no probe behind it never lets the word
    // "unchanged" stand.
    assert!(!facts.unchanged_shown());
    // Recorded on another commit: said, never shown as the head's.
    assert!(facts.section("0ddba11c0de").contains(
        "The head is now `0ddba11`: everything in this section is of the earlier commit"
    ));
    // The run records the evidence when it ends, and the description
    // reads it from the ticket.
    let drive = include_str!("../dep_fix.rs");
    assert!(drive.contains("super::upgrade_quality::record(&at, &run).await"));
    let sec_src = include_str!("../security_fixes.rs");
    assert!(sec_src.matches("check.upgrade_facts = item").count() >= 2);
    let recorder = include_str!("../upgrade_quality.rs");
    let record = fn_src(recorder, "pub async fn record(");
    for part in [
        "fam::splits(",
        "advisories(at, run,",
        "majors(at, run,",
        "behaviour(at, run,",
    ] {
        assert!(record.contains(part), "{part} is gone from the record");
    }
}

/// All five, in the one description of the reviewed head.
#[test]
fn the_description_carries_all_five_for_the_head_it_is_of() {
    let family = family();
    let plan = verify_plan();
    let all: Vec<usize> = (0..plan.checks.len()).collect();
    let out: String = all
        .iter()
        .map(|i| format!("CODERHELM_VERIFY {i} 0\n"))
        .collect();
    let facts = UpgradeFacts {
        sha: HEAD.into(),
        splits: fam::splits(std::slice::from_ref(&family), LOCK, &copies(WHOLE_HEAD)),
        families: vec![family],
        verified: Some(vq::read(&plan, HEAD, &all, &[], &out)),
        advisories: Some(reconciled()),
        ..Default::default()
    };
    let text = facts.section(HEAD);
    for part in [
        "**Advisories on commit `5e1f00d`",
        "**Families released together**",
        "The head installs every member at one version.",
        "**Verified in the sandbox on commit `5e1f00d`**",
        "- frozen install, deployment version: `npx -y pnpm@9.1.0 install --frozen-lockfile --ignore-scripts`: passes.",
    ] {
        assert!(text.contains(part), "{part} is missing from:\n{text}");
    }
    // The record round-trips through the ticket.
    let json = serde_json::to_string(&facts).unwrap();
    assert_eq!(UpgradeFacts::from_json(&json), Some(facts));
}
