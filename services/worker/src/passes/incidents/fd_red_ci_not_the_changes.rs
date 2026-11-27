//! fd. Several pull requests stayed red for days while fix rounds worked on
//! code that wasn't the cause.
//!
//! - A check no workflow webhook reported ("E2E on preview", a job of the
//!   preview workflow) was recorded from GitHub's aggregate as an event that
//!   named nothing. Every fix round read "Workflow 'unknown' failed (run_id:
//!   0)": no check to compare against the base branch, no job log to read,
//!   nothing to re-run. The rounds guessed at code for days.
//! - A preview deploy failed with the cloud account's limit
//!   (`TooManyUniqueTargetGroupsPerLoadBalancer`). The fix rounds changed
//!   nothing, a re-run met the same limit, and the run took its own work
//!   out and failed.
//!
//! Guarantee: an event written without a webhook carries the failing checks
//! read from GitHub the way a webhook writes them (the workflow's name, its
//! run id, its conclusion, the failing jobs), and no failure is ever
//! described as a workflow named "unknown" with run id 0. A failure whose
//! log shows a limit of the cloud account is not the change's: no fix round
//! runs for it, nothing is taken out, and the pull request says so and names
//! the repository's cleanup workflow when it has one.

use serde_json::json;

use super::fixture::fn_src;
use crate::passes::quarantine;
use crate::passes::resume::{
    account_limit_note, cleanup_workflows, collect_ci_failure_info, looks_account_limit,
    looks_infra, synthetic_payloads, RunEvent,
};

/// The head's check runs as GitHub lists them: the preview workflow's two
/// jobs (the deploy passed, the end-to-end job failed), an older failed try
/// of the end-to-end job a re-run replaced, and a lint workflow that passed.
fn checks() -> Vec<serde_json::Value> {
    vec![
        json!({"name": "Deploy Preview Web App", "status": "completed", "conclusion": "success",
               "started_at": "2030-03-18T11:00:00Z",
               "details_url": "https://github.com/acme/site/actions/runs/9100/job/1"}),
        json!({"name": "E2E on preview", "status": "completed", "conclusion": "failure",
               "started_at": "2030-03-18T11:05:00Z",
               "details_url": "https://github.com/acme/site/actions/runs/9100/job/2"}),
        json!({"name": "E2E on preview", "status": "completed", "conclusion": "failure",
               "started_at": "2030-03-18T09:05:00Z",
               "details_url": "https://github.com/acme/site/actions/runs/9001/job/7"}),
        json!({"name": "Web App Lint", "status": "completed", "conclusion": "success",
               "started_at": "2030-03-18T11:00:00Z",
               "details_url": "https://github.com/acme/site/actions/runs/9101/job/3"}),
    ]
}

fn event(payload: serde_json::Value) -> RunEvent {
    RunEvent {
        sk: "EVENT#20300318T111038.439Z#ci_failed".into(),
        event_type: "ci_failed".into(),
        payload,
    }
}

#[test]
fn an_event_written_without_a_webhook_names_the_failing_check() {
    let failed = quarantine::failed_checks(&checks());
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(failed[0].name, "E2E on preview");
    assert_eq!(failed[0].workflow_run_id, Some(9100));

    let payloads = synthetic_payloads(
        "1111aaaa2222bbbb3333cccc4444dddd5555eeee",
        &[(failed[0].clone(), Some("Deploy Preview".to_string()))],
    );
    assert_eq!(payloads.len(), 1);
    let p = &payloads[0];
    // The shape the webhook writes, so the fix round reads its job log,
    // can re-run it, and compares it against the base branch.
    assert_eq!(p["workflow_name"], "Deploy Preview");
    assert_eq!(p["workflow_run_id"], 9100);
    assert_eq!(p["conclusion"], "failure");
    assert_eq!(p["head_sha"], "1111aaaa2222bbbb3333cccc4444dddd5555eeee");

    let line = collect_ci_failure_info(&[event(p.clone())]);
    assert!(
        line.contains("Deploy Preview") && line.contains("9100"),
        "{line}"
    );
    assert!(line.contains("E2E on preview"), "{line}");
}

#[test]
fn a_failure_is_never_a_workflow_named_unknown() {
    // What the run's events held: the head alone.
    for payload in [
        json!({"head_sha": "6666ffff7777aaaa8888bbbb9999cccc0000dddd"}),
        json!({"head_sha": ""}),
        json!({}),
    ] {
        let line = collect_ci_failure_info(&[event(payload)]);
        assert!(!line.contains("'unknown'"), "{line}");
        assert!(!line.contains("run_id: 0"), "{line}");
        assert!(line.contains("A check failed"), "{line}");
    }
    // A check from outside GitHub Actions is named as a check.
    let other = quarantine::FailedCheck {
        name: "Vendor deploy".into(),
        workflow_run_id: None,
        conclusion: "failure".into(),
    };
    let p = synthetic_payloads("abc", &[(other, None)]);
    let line = collect_ci_failure_info(&[event(p[0].clone())]);
    assert!(line.contains("Check 'Vendor deploy' failed"), "{line}");
    // No check could be read: one event that names the head.
    let none = synthetic_payloads("abcdef1234", &[]);
    assert_eq!(none.len(), 1);
    assert_eq!(none[0]["head_sha"], "abcdef1234");
}

#[test]
fn every_synthetic_event_reads_the_failing_checks_from_github() {
    let src = include_str!("../resume.rs");
    let rec = fn_src(src, "async fn record_synthetic_ci_failed(");
    assert!(rec.contains("failing_now("), "{rec}");
    assert!(rec.contains("synthetic_payloads("), "{rec}");
    // Every caller hands it the pull request's repository and branch.
    let calls: Vec<usize> = src
        .match_indices("record_synthetic_ci_failed(\n")
        .map(|(i, _)| i)
        .filter(|i| !src[..*i].ends_with("async fn "))
        .collect();
    assert!(calls.len() >= 4, "{calls:?}");
    let with_github = calls
        .iter()
        .filter(|i| {
            let i = **i;
            let call = &src[i..(i + 600).min(src.len())];
            let end = call.find(".await").unwrap_or(call.len());
            call[..end].contains("Some((&github") || call[..end].contains("github\n")
        })
        .count();
    assert_eq!(
        calls.len(),
        with_github,
        "a synthetic event written without its checks"
    );
}

/// The deploy step's log, as the run's CI showed it.
const DEPLOY_LOG: &str = "2030-03-16T22:40:11.0Z Creating listener rule for pr-41\n\
2030-03-16T22:40:12.0Z aws: [ERROR]: An error occurred (TooManyUniqueTargetGroupsPerLoadBalancer) when calling the CreateRule operation: You have reached the maximum number of unique target groups that you can associate with a load balancer of type 'application':\n\
2030-03-16T22:40:12.1Z ##[error]Process completed with exit code 254.";

#[test]
fn the_cloud_accounts_limit_is_not_the_changes_failure() {
    let line = looks_account_limit(DEPLOY_LOG).expect("an account limit");
    assert!(
        line.contains("TooManyUniqueTargetGroupsPerLoadBalancer"),
        "{line}"
    );
    // The infrastructure check that re-runs once never claimed it: a re-run
    // meets the same limit.
    assert_eq!(looks_infra(DEPLOY_LOG), None);
    // Other account limits read the same way; a rate limit and a test's
    // own failure don't.
    for l in [
        "An error occurred (LimitExceededException) when calling the CreateStack operation",
        "ServiceQuotaExceededException: quota for Elastic IPs reached",
        "An error occurred (TooManyTargets) when calling the RegisterTargets operation",
    ] {
        assert!(looks_account_limit(l).is_some(), "{l}");
    }
    for l in [
        "An error occurred (TooManyRequestsException): Rate exceeded",
        "Error: expect(received).toEqual(expected) // deep equality",
        "  2 failed",
        // The code under test throwing the same words is the change's own.
        "DOMException: QuotaExceededError: the quota has been exceeded (localStorage)",
        "FetchError: TooManyRedirects: maximum redirect reached at /account",
        "TooManyBookingsError: the slot already has a booking",
    ] {
        assert_eq!(looks_account_limit(l), None, "{l}");
    }

    let files: Vec<String> = [
        ".github/workflows/deploy-preview.yml",
        ".github/workflows/cleanup-previews.yml",
        ".github/workflows/lint.yml",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let cleanup = cleanup_workflows(&files);
    assert_eq!(
        cleanup,
        vec![".github/workflows/cleanup-previews.yml".to_string()]
    );
    let note = account_limit_note(&["Deploy Preview".into()], &line, &cleanup);
    assert!(note.contains("not on this change"), "{note}");
    assert!(note.contains("cleanup-previews.yml"), "{note}");
    assert!(note.contains("changed no code"), "{note}");
    assert!(
        note.contains("took none of this change's work out"),
        "{note}"
    );
}

#[test]
fn no_fix_round_runs_for_the_accounts_limit() {
    let src = include_str!("../resume.rs");
    let limit = src
        .find("if let Some(line) = looks_account_limit(&logs)")
        .expect("the CI fix path reads the account's limit");
    let round = src
        .find("let mut ci_round = run_round::Round::start(")
        .expect("the CI fix round");
    let rerun = src.find("let rerun = try_rerun_once(").expect("the re-run");
    assert!(
        limit < rerun && limit < round,
        "the limit is read after a fix or a re-run started"
    );
    let block = &src[limit..limit + 3000];
    assert!(block.contains("mark_pr_ready("), "{block}");
    assert!(block.contains("\"completed\""), "{block}");
    assert!(block.contains("return Ok(());"), "{block}");
    assert!(!block.contains("implement::run("), "{block}");
}
