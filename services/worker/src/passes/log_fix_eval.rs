//! The log-fix eval: one made-up case (a web app whose fetch cache fills the
//! container disk) run through the evidence step against a fake AWS
//! account, the infrastructure repository search, the two linked pull
//! requests' plan, and the description checks on good and bad descriptions.
//! Every name in `log_fix_eval.json` is made up.

use std::collections::BTreeMap;

use chrono::{NaiveDateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::Value;

use super::fix_location::{FixDecision, FixLocation, OutsideChange};
use super::infra_repo::{self, CodeHit, Found, RepoInfo};
use super::log_evidence::fake::{FakeAws, FakeFiles};
use super::log_evidence::{self, Target};
use super::log_fix_checks;
use super::plan::PlanResult;

#[derive(Deserialize)]
struct Case {
    now: String,
    target: TargetJson,
    aws: FakeAws,
    expect_evidence: ExpectEvidence,
    repo_files: BTreeMap<String, String>,
    expect_start: ExpectStart,
    repos: Vec<RepoInfo>,
    primary: String,
    code_hits: Vec<CodeHit>,
    expect_infra_repo: String,
    outside_change: OutsideChange,
    diffs: BTreeMap<String, Value>,
    descriptions: Vec<Description>,
}

#[derive(Deserialize)]
struct TargetJson {
    log_group: String,
    needle: String,
}

#[derive(Deserialize)]
struct ExpectEvidence {
    service: String,
    disk_gib: i32,
    fill_mb_per_hour: f64,
    hours_to_first_error: f64,
    failing_now: Vec<String>,
    total_errors: u64,
}

#[derive(Deserialize)]
struct ExpectStart {
    command: String,
    script: String,
    working_directory: String,
}

#[derive(Deserialize)]
struct Description {
    name: String,
    diff: String,
    #[serde(default)]
    refuse: String,
    body: String,
    expect: BTreeMap<String, bool>,
}

fn case() -> Case {
    serde_json::from_str(include_str!("log_fix_eval.json")).expect("eval case reads")
}

fn now(c: &Case) -> chrono::DateTime<Utc> {
    let t = NaiveDateTime::parse_from_str(&c.now, "%Y-%m-%d %H:%M").unwrap();
    Utc.from_utc_datetime(&t)
}

async fn evidence(c: &Case, aws: &FakeAws) -> log_evidence::LogEvidence {
    let target = Target {
        log_group: c.target.log_group.clone(),
        needle: c.target.needle.clone(),
    };
    let mut ev = log_evidence::gather(aws, &target, now(c)).await;
    ev.start = Some(log_evidence::resolve_start(&FakeFiles(c.repo_files.clone()), &ev).await);
    ev
}

#[tokio::test]
async fn log_fix_eval() {
    let c = case();
    let mut results: Vec<(String, bool, String)> = Vec::new();
    let mut check = |name: &str, ok: bool, detail: String| {
        results.push((name.to_string(), ok, detail));
    };

    // 1. The evidence step.
    let ev = evidence(&c, &c.aws).await;
    let e = &c.expect_evidence;
    let first = ev.tasks.iter().find(|t| t.failing_now);
    check(
        "evidence: service, disk, errors",
        ev.service.as_ref().map(|s| s.name.as_str()) == Some(e.service.as_str())
            && ev.disk_gib == Some(e.disk_gib)
            && ev.total_errors == e.total_errors,
        format!(
            "service {:?}, disk {:?} GiB, {} errors",
            ev.service.as_ref().map(|s| &s.name),
            ev.disk_gib,
            ev.total_errors
        ),
    );
    check(
        "evidence: fill rate from start vs first error",
        first.and_then(|t| t.hours_to_first_error) == Some(e.hours_to_first_error)
            && ev.fill_mb_per_hour == Some(e.fill_mb_per_hour),
        format!(
            "{:?} h to first error, {:?} MB/h",
            first.and_then(|t| t.hours_to_first_error),
            ev.fill_mb_per_hour
        ),
    );
    let failing: Vec<String> = ev.failing_now().iter().map(|t| t.task_id.clone()).collect();
    check(
        "evidence: failing now",
        failing == e.failing_now,
        format!("{failing:?}"),
    );
    let sp = ev.start.clone().unwrap_or_default();
    check(
        "evidence: start path (ECS command over Docker CMD)",
        sp.verified
            && sp.command == c.expect_start.command
            && sp.script == c.expect_start.script
            && sp.working_directory == c.expect_start.working_directory,
        sp.line(),
    );

    // 2. The infrastructure repository and the two linked pull requests.
    let terms = infra_repo::search_terms(&ev);
    let found = infra_repo::pick(&c.primary, &c.repos, &c.code_hits, &terms);
    let (infra, files, hit_terms) = match &found {
        Found::Repo { repo, files, terms } => (repo.clone(), files.clone(), terms.clone()),
        Found::None { .. } => (String::new(), vec![], vec![]),
    };
    check(
        "infra repo found from code search",
        infra == c.expect_infra_repo,
        format!("{infra} via {files:?}"),
    );
    let mut plan = PlanResult {
        proposal: String::new(),
        design: String::new(),
        tasks: "- [ ] Bound the fetch cache by bytes".into(),
        spec: String::new(),
        repo_tasks: vec![],
        fix: Some(FixDecision {
            fix_location: FixLocation::Both,
            outside_change: Some(c.outside_change.clone()),
            ..Default::default()
        }),
    };
    let (po, pn) = c.primary.split_once('/').unwrap();
    let linked = infra_repo::link_into_plan(&mut plan, (po, pn), &infra, &files, &hit_terms);
    let d = plan.fix.clone().unwrap_or_default();
    check(
        "two PRs planned, outside note gone",
        linked
            && plan.repo_tasks.len() == 2
            && d.outside_change.is_none()
            && super::fix_location::route(Some(&d)) == super::fix_location::Route::Code,
        plan.repo_tasks
            .iter()
            .map(|r| format!("{}/{}", r.owner, r.name))
            .collect::<Vec<_>>()
            .join(" + "),
    );
    let none = infra_repo::pick(&c.primary, &c.repos, &[], &terms);
    let note = match &none {
        Found::None { searched, terms } => infra_repo::not_found_note(searched, terms),
        _ => String::new(),
    };
    check(
        "no defining repo: the note names what was searched",
        note.contains("`shop/platform-infra`") && note.contains("`storefront-web`"),
        note,
    );

    // 3. The description checks.
    for desc in &c.descriptions {
        let mut aws = c.aws.clone();
        if !desc.refuse.is_empty() {
            aws.refuse
                .insert(desc.refuse.clone(), "AccessDeniedException".into());
        }
        let ev = evidence(&c, &aws).await;
        let diff = &c.diffs[&desc.diff];
        let got = log_fix_checks::run_all(diff, &desc.body, &ev);
        let ok = got
            .iter()
            .all(|r| desc.expect.get(&r.check).copied() == Some(r.passed));
        check(
            &format!("checks: {}", desc.name),
            ok,
            got.iter()
                .map(|r| format!("{}={}", r.check, if r.passed { "pass" } else { "fail" }))
                .collect::<Vec<_>>()
                .join(" "),
        );
        // What fails is rewritten in code: no unsettled claim and the start
        // path stated survive the rewrite.
        if got.iter().any(|r| !r.passed) {
            let out = log_fix_checks::rewrite(&desc.body, diff, &ev);
            let after = log_fix_checks::run_all(diff, &out, &ev);
            check(
                &format!("rewrite: {}", desc.name),
                after
                    .iter()
                    .filter(|r| r.check != "limits")
                    .all(|r| r.passed)
                    && (after.iter().all(|r| r.passed)
                        || out.contains("without a measured number")),
                after
                    .iter()
                    .map(|r| format!("{}={}", r.check, if r.passed { "pass" } else { "fail" }))
                    .collect::<Vec<_>>()
                    .join(" "),
            );
        }
    }

    let passed = results.iter().filter(|r| r.1).count();
    for (name, ok, detail) in &results {
        println!("[{}] {name}: {detail}", if *ok { "pass" } else { "FAIL" });
    }
    println!("log-fix eval: {passed}/{} pass", results.len());
    assert_eq!(passed, results.len(), "log-fix eval failures above");
}
