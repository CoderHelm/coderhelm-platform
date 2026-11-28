//! wb. Promotion gained a job that reads release reports, and it runs only
//! when promoting. The planning job ran after it either way, but the job
//! that attests and tags each release had a bare `if:`. GitHub adds
//! `success()` to such a condition, and `success()` is false when anything
//! upstream was skipped, so every beta was planned, never tagged, and its
//! publish failed waiting for a tag that was never created.
//!
//! Guarantees: the tag job runs whenever planning succeeded with something
//! to release, whether or not the reports were read; and the workflow check
//! in CI refuses any job whose `if:` has no status function while a job it
//! needs, directly or further up, has a condition of its own.

const WORKFLOW: &str = include_str!("../../../../../.github/workflows/auto-release.yml");
const CHECK: &str = include_str!("../../../../../.github/scripts/check_workflows.py");
const CI: &str = include_str!("../../../../../.github/workflows/ci.yml");

fn job(name: &str) -> &'static str {
    // A job runs from its `  name:` line to the next line indented by
    // exactly two spaces.
    let head = format!("\n  {name}:\n");
    let start = WORKFLOW.find(&head).unwrap_or_else(|| panic!("job {name}")) + 1;
    let rest = &WORKFLOW[start..];
    let end = rest
        .match_indices('\n')
        .map(|(i, _)| i + 1)
        .find(|&i| {
            let line = &rest[i..];
            line.starts_with("  ") && !line.starts_with("   ")
        })
        .unwrap_or(rest.len());
    &rest[..end]
}

#[test]
fn the_tag_job_runs_after_a_skipped_evidence_job() {
    let evidence = job("evidence");
    assert!(
        evidence.contains("    if: "),
        "evidence is conditional: {evidence}"
    );
    let tag = job("tag");
    let cond = tag
        .lines()
        .find(|l| l.starts_with("    if: "))
        .expect("tag has a condition");
    assert!(cond.contains("!cancelled()"), "{cond}");
    assert!(cond.contains("needs.plan.result == 'success'"), "{cond}");
}

#[test]
fn ci_refuses_a_job_lost_to_a_skipped_ancestor() {
    assert!(CHECK.contains("def _skipped_upstream("), "the rule exists");
    assert!(
        CHECK.contains("problems += _skipped_upstream(name, jobs)"),
        "and is applied"
    );
    assert!(
        CI.contains("python3 .github/scripts/check_workflows.py"),
        "CI runs it"
    );
}
