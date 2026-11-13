//! The repository's record of flaky checks, and what a red check on a pull
//! request is judged against before a fix round (`common::flaky_checks`).
//!
//! The record is one item per repository in the settings table. It is
//! written when a commit's check failed, was re-run with nothing changed and
//! passed, and read when a later pull request's check fails: a check with
//! such a record on unrelated pull requests, whose failure names no file the
//! pull request changed, is re-run and not "fixed" in code. What was judged,
//! on what evidence, and what was done goes on the pull request.

use aws_sdk_dynamodb::types::AttributeValue;
use chrono::Utc;
use common::flaky_checks::{self as fc, Judgement, Kind, Observation, Record, Rerun, Touch};
use tracing::{info, warn};

use super::{attr_n, attr_s};
use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// The run record's field naming the workflows whose failed jobs were
/// re-run on a head, so a later green on that head is written to the
/// repository's record under their names.
pub(crate) const RERUN_WORKFLOWS_FIELD: &str = "ci_rerun_workflows";

/// The repository's record and its version (0 when there is none yet).
pub(crate) async fn load(state: &WorkerState, team_id: &str, repo: &str) -> (Record, u64) {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&fc::sk(repo)))
        .send()
        .await
        .ok()
        .and_then(|r| r.item);
    let Some(item) = item else {
        return (Record::default(), 0);
    };
    let record = item
        .get("checks")
        .and_then(|v| v.as_s().ok())
        .map(|s| Record::from_json(s))
        .unwrap_or_default();
    let version = item
        .get("version")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    (record, version)
}

/// Adds observations to the repository's record. Two writers at once (a
/// webhook and a sweep, two pull requests) never lose each other's: the
/// write is conditional on the version read, and read again when it moved.
pub(crate) async fn observe(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    checks: &[String],
    o: &Observation,
) {
    if checks.is_empty() || state.config.settings_table_name.is_empty() {
        return;
    }
    for _ in 0..4 {
        let (mut record, version) = load(state, team_id, repo).await;
        let mut changed = false;
        for c in checks {
            changed |= record.observe(c, o.clone());
        }
        if !changed {
            return;
        }
        let mut req = state
            .dynamo
            .put_item()
            .table_name(&state.config.settings_table_name)
            .item("pk", attr_s(team_id))
            .item("sk", attr_s(&fc::sk(repo)))
            .item("record_type", attr_s("flaky_checks"))
            .item("repo", attr_s(repo))
            .item("checks", attr_s(&record.to_json()))
            .item("version", attr_n(version + 1))
            .item("updated_at", attr_s(&Utc::now().to_rfc3339()));
        req = if version == 0 {
            req.condition_expression("attribute_not_exists(sk)")
        } else {
            req.condition_expression("version = :v")
                .expression_attribute_values(":v", attr_n(version))
        };
        match req.send().await {
            Ok(_) => {
                info!(repo, checks = ?checks, kind = ?o.kind, pr = o.pr, "Flaky checks: recorded");
                return;
            }
            Err(e) if common::run_fail::is_condition_failed(&e) => continue,
            Err(e) => {
                warn!(repo, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Flaky checks: could not record");
                return;
            }
        }
    }
    warn!(
        repo,
        "Flaky checks: the record kept moving; observation not written"
    );
}

/// Remembers on the run which workflows were re-run on this head.
pub(crate) async fn remember_rerun(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    workflows: &[String],
) {
    let names: Vec<String> = workflows
        .iter()
        .filter(|w| !w.trim().is_empty())
        .cloned()
        .collect();
    if names.is_empty() {
        return;
    }
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("ADD #f :w")
        .expression_attribute_names("#f", RERUN_WORKFLOWS_FIELD)
        .expression_attribute_values(":w", AttributeValue::Ss(names))
        .send()
        .await
    {
        warn!(run_id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Flaky checks: could not remember the re-run");
    }
}

/// The workflows re-run on this run's heads.
pub(crate) fn rerun_workflows(
    run_record: &std::collections::HashMap<String, AttributeValue>,
) -> Vec<String> {
    run_record
        .get(RERUN_WORKFLOWS_FIELD)
        .and_then(|v| v.as_ss().ok())
        .cloned()
        .unwrap_or_default()
}

/// A head that failed, was re-run with nothing changed and is green now:
/// the workflows re-run on it are written to the repository's record.
pub(crate) async fn passed_after_rerun(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    pr: u64,
    head: &str,
    run_record: &std::collections::HashMap<String, AttributeValue>,
) {
    let workflows = rerun_workflows(run_record);
    if workflows.is_empty() || head.is_empty() {
        return;
    }
    observe(
        state,
        team_id,
        repo,
        &workflows,
        &Observation {
            kind: Kind::PassedOnRerun,
            at: Utc::now().to_rfc3339(),
            pr,
            sha: head.to_string(),
            line: String::new(),
        },
    )
    .await;
}

/// The failure of a pull request's head, as the fix round is about to see it.
pub(crate) struct Failure<'a> {
    pub team_id: &'a str,
    pub owner: &'a str,
    pub repo: &'a str,
    pub pr: u64,
    /// The failing workflows (the record's check names).
    pub failing: &'a [String],
    /// Checks failing on the base branch's head too.
    pub on_base: &'a [String],
    /// Everything the failure printed.
    pub logs: &'a str,
    /// The infrastructure line the output shows, when it shows one.
    pub infra: Option<&'a str>,
}

/// Judges the failure: the pull request's changed files against what the
/// output names, and the failing checks against the repository's record.
/// With the changed files unreadable the failure is the pull request's.
pub(crate) async fn judge(
    state: &WorkerState,
    github: &GitHubClient,
    f: &Failure<'_>,
) -> (Judgement, Touch) {
    let changed: Vec<String> = match github.list_pull_files(f.owner, f.repo, f.pr).await {
        Ok(files) => files.into_iter().map(|(path, _)| path).collect(),
        Err(e) => {
            warn!(pr = f.pr, error = %e, "Flaky checks: could not read the pull request's files");
            return (
                Judgement {
                    ours: f.failing.to_vec(),
                    not_ours: Vec::new(),
                },
                Touch::Unknown,
            );
        }
    };
    let touch = fc::touch(f.logs, &changed);
    let (record, _) = load(state, f.team_id, &format!("{}/{}", f.owner, f.repo)).await;
    let j = fc::judge(
        f.failing,
        f.on_base,
        &touch,
        f.infra,
        &record,
        f.pr,
        Utc::now(),
    );
    (j, touch)
}

/// Writes the judgement on the pull request (its own section of the
/// description, replaced on a later judgement).
pub(crate) async fn note_on_pr(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    pr: u64,
    section: String,
) {
    let body = match github.get_pull_request(owner, repo, pr).await {
        Ok(p) => p["body"].as_str().unwrap_or("").to_string(),
        Err(e) => {
            warn!(pr, error = %e, "Flaky checks: could not read the pull request to note the judgement");
            return;
        }
    };
    let out =
        super::completeness::with_section(&body, super::completeness::Section::CiNotes, section);
    if out != body {
        if let Err(e) = crate::passes::description::save(github, owner, repo, pr, &out).await {
            warn!(pr, error = %e, "Flaky checks: could not note the judgement on the pull request");
        }
    }
}

/// The progress note for a judgement that changed no code.
pub(crate) fn progress_note(j: &Judgement, rerun: &Rerun) -> String {
    let checks: Vec<&str> = j.not_ours.iter().map(|(c, _)| c.as_str()).collect();
    let did = match rerun {
        Rerun::Started => "re-ran the failed jobs and changed no code",
        Rerun::NoPermission => {
            "changed no code; the re-run needs the GitHub App's Actions write permission, which \
             this installation doesn't grant"
        }
        Rerun::NotActions => "changed no code; the checks run outside GitHub Actions",
    };
    format!(
        "CI is red on {} for a reason outside this change (the evidence is on the pull request): \
         CoderHelm {did}.",
        checks.join(", ")
    )
}
