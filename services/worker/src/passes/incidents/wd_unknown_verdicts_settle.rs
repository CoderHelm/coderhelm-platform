//! wd. A team's Security fixes page showed most of its open advisories with
//! the verdict "Unknown", and the high ones tiered P1 and shown overdue as
//! if they were reachable. The daily model budget wasn't the limit: triage
//! had read the whole backlog, most of it was answered "unknown", and those
//! answers were kept for a week, so each later pass read only the few new
//! packages. The code check had sent most of those packages to the model
//! with lines that settle nothing: a type-only import (`import type { X }
//! from "cms"`), and lint or CLI scripts in `package.json` naming
//! development dependencies. The tier then treated "unknown" like
//! reachable for any runtime dependency with CVSS 7+, which set a seven day
//! due date on advisories with almost no chance of exploitation.
//!
//! Guarantees: a type-only import is neither an import nor a mention; a
//! line that names only development dependencies is a development load, so
//! a package that only development dependencies bring in is settled as not
//! loaded, and one whose runtime ways in nothing imports is settled as not
//! reachable, without the model; a verdict that isn't settled is never
//! tiered as reachable (its score alone puts it one tier lower, and only
//! the exploited list or EPSS raises it), its tier and due date are marked
//! provisional, and the overview counts it as not settled; the code is read
//! again once by the new check while a model answer is reused for the same
//! lines; and the model reads the groups with the most exploitation risk
//! first.

use common::reachability::{decide, dev_only_names, Decision, Query, Scanner};
use common::security_fixes::{
    overview_counts, retier, tier_of, Advisory, Finding, FindingState, Scope, SecurityConfig,
    Status, Tier, Verdict,
};

const TRIAGE: &str = include_str!("../security_fixes.rs");

fn scan(q: Query, files: &[(&str, &str)]) -> Decision {
    let mut s = Scanner::new(vec![q]);
    for (p, t) in files {
        s.add(p, Some(t));
    }
    let out = s.finish();
    decide(&out[0].0, &out[0].1)
}

fn finding(verdict: Verdict, cvss: f64, epss: f64) -> (Finding, FindingState) {
    (
        Finding {
            id: "f".into(),
            repo: "acme/api".into(),
            manifest: "package-lock.json".into(),
            ecosystem: "npm".into(),
            package: "left-pad".into(),
            version: "1.0.0".into(),
            advisory: Advisory {
                ids: vec!["GHSA-aaaa-bbbb-cccc".into()],
                primary: "GHSA-aaaa-bbbb-cccc".into(),
                title: "Denial of service through a long input".into(),
                cvss: Some(cvss),
                epss: Some(epss),
                ..Default::default()
            },
            scope: Scope::Runtime,
            verdict,
            status: Status::Open,
            first_seen_at: "2026-01-05T10:00:00Z".into(),
            ..Default::default()
        },
        FindingState::default(),
    )
}

#[test]
fn lines_that_load_nothing_in_production_settle_without_the_model() {
    let manifest = "{\n  \"scripts\": { \"lint\": \"acme-lint .\", \"migrate\": \"acme-cli migrate\" },\n  \"dependencies\": { \"acme-cms\": \"3.0.0\" },\n  \"devDependencies\": { \"acme-lint\": \"9.0.0\", \"acme-cli\": \"1.0.0\" }\n}\n";
    let dev = dev_only_names([manifest]);
    assert_eq!(dev, ["acme-cli", "acme-lint"]);
    // Brought in only by the linter and a CLI; the scripts name them.
    let q = Query {
        ecosystem: "npm".into(),
        package: "tiny-yaml".into(),
        parents: vec!["acme-cli".into(), "acme-lint".into()],
        parents_from_lockfile: true,
        dev_only: dev.clone(),
        ..Default::default()
    };
    match scan(q, &[("package.json", manifest)]) {
        Decision::Settled(Verdict::NotLoaded, e) => assert!(!e.lines.is_empty()),
        other => panic!("not loaded where the code runs, got {other:?}"),
    }
    // Brought in by a runtime package the code imports only for its types.
    let q = Query {
        ecosystem: "npm".into(),
        package: "left-pad".into(),
        parents: vec!["acme-cms".into()],
        parents_from_lockfile: true,
        dev_only: dev,
        ..Default::default()
    };
    let d = scan(
        q,
        &[
            ("package.json", manifest),
            (
                "src/field.tsx",
                "import type { FieldProps } from \"acme-cms\";\nexport const f = (p: FieldProps) => p;\n",
            ),
        ],
    );
    assert!(
        matches!(d, Decision::Settled(Verdict::NotReachable, _)),
        "{d:?}"
    );
}

#[test]
fn a_verdict_not_settled_is_not_tiered_or_dated_as_reachable() {
    let cfg = SecurityConfig::default();
    // High, almost never exploited, not settled: P2 with P2's due date.
    let (mut f, st) = finding(Verdict::Unknown, 7.5, 0.004);
    retier(&mut f, &st, &cfg);
    assert_eq!(f.tier, Some(Tier::P2));
    assert!(f.tier_provisional, "not settled: the tier is provisional");
    assert!(f.due_at.starts_with("2026-02-04"), "{}", f.due_at);
    // The same advisory, reachable: P1, due in a week.
    let (mut r, st) = finding(Verdict::Reachable, 7.5, 0.004);
    retier(&mut r, &st, &cfg);
    assert_eq!(r.tier, Some(Tier::P1));
    assert!(!r.tier_provisional);
    assert!(r.due_at.starts_with("2026-01-12"), "{}", r.due_at);
    // Evidence that doesn't depend on reachability still raises it.
    let (mut e, st) = finding(Verdict::Unknown, 7.5, 0.3);
    assert_eq!(tier_of(&e, &st), Tier::P1, "likely exploited");
    e.advisory.kev = true;
    assert_eq!(tier_of(&e, &st), Tier::P0, "exploited, runtime");
    // The overview says how many aren't settled.
    let v = overview_counts(
        &[(f, FindingState::default()), (r, FindingState::default())],
        chrono::Utc::now(),
    );
    assert_eq!(v["verdicts"]["settled"], 1);
    assert_eq!(v["verdicts"]["not_settled"], 1);
    assert_eq!(v["verdicts"]["checking"], 0);
}

#[test]
fn the_backlog_is_read_again_once_and_the_riskiest_first() {
    // The new check reads every finding again; a model answer for the same
    // lines is reused.
    assert!(TRIAGE.contains("const TRIAGE_VERSION: &str = \"code-3\";"));
    assert!(TRIAGE.contains("const TRIAGE_ASK_VERSION: &str = \"code-2\";"));
    assert!(TRIAGE.contains("content_hash(&format!(\"{TRIAGE_ASK_VERSION}|{key}\"))"));
    // The exploited list and EPSS rank the reads, before the score.
    let order = &TRIAGE[TRIAGE.find("fn triage_order(").unwrap()..];
    let order = &order[..order.find("\n}\n").unwrap()];
    assert!(order.contains("(tier, !kev, -epss, -score, scope, k.clone())"));
    // The code check knows which ways in are development ones.
    assert!(TRIAGE.contains("dev_only = reach::dev_only_names(texts);"));
    assert!(TRIAGE.contains("parents_from_lockfile: ancestry.found,"));
}
