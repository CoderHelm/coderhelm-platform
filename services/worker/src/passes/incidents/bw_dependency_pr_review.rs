//! bw. A product owner's review of a dependency pull request asked three
//! things of every one: the type check is run on the changed package even
//! when CI doesn't run one, and a failing type check is a failure the
//! bisect acts on; packages of one family (`@scope/common`, `@scope/core`,
//! `@scope/platform-x`) move together, and the scope's packages on one
//! minor line count as a family even when no registry record names their
//! source repository; and each major override says which package pulls it
//! in and why the bump is safe, written from the lockfile's dependency
//! paths, never free text.
//!
//! Guarantee: the verification set adds the type check to steps that lack
//! one and a failing type check takes the set out; a family found from the
//! lockfile alone is held together and a head that installs it at two
//! versions fails the set; each major's section is built from its paths.

use crate::passes::checks::{CheckKind, Step};
use common::upgrade_quality::{family, safety, verify as vq};

#[test]
fn the_type_check_runs_whatever_ci_runs_and_a_failure_counts() {
    // CI's steps run the build and the tests, no type check.
    let ci = vec![Step {
        kinds: vec![CheckKind::Build, CheckKind::Tests],
        cmd: "pnpm build && pnpm test".into(),
        scope: None,
    }];
    let typed = vq::Plan {
        lockfile: "packages/api/pnpm-lock.yaml".into(),
        manager: "pnpm".into(),
        dir: "packages/api".into(),
        checks: vec![vq::Check {
            kind: vq::Kind::Typecheck,
            shown: "tsc --noEmit".into(),
            cmd: "pnpm exec tsc --noEmit".into(),
            found: "tsconfig.json".into(),
        }],
        ..Default::default()
    };
    let steps = crate::passes::settle::with_min_set(ci, &[typed]);
    let tc = steps
        .iter()
        .find(|s| s.kinds.contains(&CheckKind::Typecheck))
        .expect("the type check is added");
    assert!(
        tc.cmd.contains("cd 'packages/api'") && tc.cmd.contains("tsc --noEmit"),
        "{}",
        tc.cmd
    );
    // A failing type check is the change's: the bisect takes the set out.
    assert!(vq::Kind::Typecheck.blames());
}

#[test]
fn scope_siblings_on_one_minor_line_are_a_family_found_from_the_lockfile() {
    let base: Vec<(String, String)> = [
        ("@scope/common", "10.4.15"),
        ("@scope/core", "10.4.15"),
        ("@scope/platform-x", "10.4.15"),
        ("@scope/testing", "9.1.0"),
        ("@types/node", "10.4.15"),
        ("other", "10.4.15"),
    ]
    .iter()
    .map(|(n, v)| (n.to_string(), v.to_string()))
    .collect();
    assert_eq!(
        family::scope_siblings("@scope/core", "10.4.15", &base),
        vec!["@scope/common".to_string(), "@scope/platform-x".to_string()]
    );
    // Type declarations are versioned one by one; a plain name has no scope.
    assert!(family::scope_siblings("@types/express", "10.4.1", &base).is_empty());
    assert!(family::scope_siblings("other", "10.4.15", &base).is_empty());
    // No registry record: the family comes from the lockfile.
    let fam = family::detect("@scope/core", "10.4.15", "pnpm-lock.yaml", &base, &[], &[])
        .expect("a family");
    assert_eq!(
        fam.members,
        vec!["@scope/common", "@scope/core", "@scope/platform-x"]
    );
    assert!(
        fam.evidence_line().contains("`@scope/` it has on 10.4"),
        "{}",
        fam.evidence_line()
    );
    // A head that raises one of them alone is a gate failure.
    let head: Vec<(String, String)> = [
        ("@scope/common", "10.4.15"),
        ("@scope/core", "10.4.22"),
        ("@scope/platform-x", "10.4.15"),
    ]
    .iter()
    .map(|(n, v)| (n.to_string(), v.to_string()))
    .collect();
    assert_eq!(
        family::splits(std::slice::from_ref(&fam), "pnpm-lock.yaml", &head).len(),
        1
    );
    let together: Vec<(String, String)> = head
        .iter()
        .map(|(n, _)| (n.clone(), "10.4.22".to_string()))
        .collect();
    assert!(family::splits(&[fam], "pnpm-lock.yaml", &together).is_empty());
    let dep_fix = include_str!("../dep_fix.rs");
    assert!(dep_fix.contains("result.checks.push((df::FAMILY_CHECK.to_string(), false));"));
}

#[test]
fn a_major_override_says_who_pulls_it_in_and_why_it_is_safe_from_the_lockfile() {
    let paths = vec![safety::PathIn {
        root: "build-tool".into(),
        declared: safety::Declared::Development,
        importers: vec![],
        install_script: false,
        scripts: vec!["build".into()],
    }];
    let parents = vec![common::upgrade_notes::ParentSupport {
        parent: "build-tool".into(),
        version: "3.2.0".into(),
        declares: "^6.0.0".into(),
        admits: Some(false),
        ..Default::default()
    }];
    let s = safety::evidence(&safety::MajorIn {
        package: "archive-lib",
        from: "6.2.1",
        to: "7.5.21",
        ecosystem: "npm",
        why_major: "no 6.x release fixes the advisory",
        paths: &paths,
        used_in: &[],
        searched: "imports of `archive-lib`",
        verdict: common::security_fixes::Verdict::NotReachable,
        direct: false,
        parents: &parents,
        via_parent: None,
        selector: "archive-lib@6",
    })
    .section();
    assert!(s.contains("Path through `build-tool`"), "{s}");
    assert!(
        s.contains("override scoped to its version line (`archive-lib@6`)"),
        "{s}"
    );
    assert!(s.contains("`build-tool` 3.2.0 declares `^6.0.0`"), "{s}");
    assert!(s.contains("Reachability:"), "{s}");
}
