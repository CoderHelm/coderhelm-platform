//! A sentence the run writes into a repository file that says a check
//! passes stands only on a recorded result (`common::file_claims`).
//!
//! The records are the ones the pull request's own sections are written
//! from: the run's last sandbox check (`proof::SandboxRecord`), which says
//! which kinds of check ran and passed. A claim with a passing record
//! stays. One without is taken out of the file before the run finishes, as
//! one commit, and the run page says which sentence and why; the sentence
//! never ships on a guess.

use common::file_claims::{self as fc, Claim, ClaimKind};
use tracing::{info, warn};

use super::checks::CheckKind;
use super::proof::RunEvidence;
use crate::clients::github::{FileOp, GitHubClient};
use crate::models::TicketMessage;
use crate::WorkerState;

/// The kinds of check with a passing result recorded for the branch as the
/// run last checked it.
pub fn proven(evidence: &RunEvidence) -> Vec<ClaimKind> {
    evidence
        .sandbox
        .passed
        .iter()
        .filter_map(|k| match k {
            CheckKind::Build => Some(ClaimKind::Build),
            CheckKind::Tests => Some(ClaimKind::Tests),
            CheckKind::Lint => Some(ClaimKind::Lint),
            CheckKind::Typecheck => Some(ClaimKind::Typecheck),
            _ => None,
        })
        .collect()
}

/// The claims the branch's diff adds to prose files that no recorded
/// passing result covers.
pub fn unproven_in_diff(diff: &serde_json::Value, evidence: &RunEvidence) -> Vec<Claim> {
    let mut claims = vec![];
    for f in diff["files"].as_array().into_iter().flatten() {
        if f["status"].as_str() == Some("removed") {
            continue;
        }
        let (Some(path), Some(patch)) = (f["filename"].as_str(), f["patch"].as_str()) else {
            continue;
        };
        claims.extend(fc::claims_in_patch(path, patch));
    }
    fc::unproven(&claims, &proven(evidence))
}

/// The run page's line for the claims taken out.
pub fn note(removed: &[Claim]) -> String {
    let list: Vec<String> = removed
        .iter()
        .map(|c| {
            format!(
                "`{}` line {}: \"{}\" (no passing result is recorded for {})",
                c.path,
                c.line,
                common::truncate_str(&c.sentence, 160),
                c.what()
            )
        })
        .collect();
    format!(
        "Took {} out of the files this run wrote: a sentence that says a check passes stays only \
         when that check has a recorded passing result on this branch. {}",
        if removed.len() == 1 {
            "one sentence".to_string()
        } else {
            format!("{} sentences", removed.len())
        },
        list.join("; ")
    )
}

/// Reads what the branch adds to its prose files and takes out every claim
/// that a check passes with no recorded passing result. Returns the claims
/// taken out. Best effort: a diff that can't be read changes nothing.
pub async fn settle(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    branch: &str,
    evidence: &RunEvidence,
) -> Vec<Claim> {
    let (owner, repo) = (&msg.repo_owner, &msg.repo_name);
    let Ok(diff) = github.get_diff(owner, repo, &msg.base_branch, branch).await else {
        return vec![];
    };
    let open = unproven_in_diff(&diff, evidence);
    if open.is_empty() {
        return vec![];
    }
    let mut paths: Vec<&str> = open.iter().map(|c| c.path.as_str()).collect();
    paths.sort();
    paths.dedup();
    let mut ops = vec![];
    let mut removed: Vec<Claim> = vec![];
    for path in paths {
        let Ok(Some(text)) = github.read_file_if_exists(owner, repo, path, branch).await else {
            continue;
        };
        if let Some(next) = fc::without(path, &text, &open) {
            removed.extend(
                open.iter()
                    .filter(|c| c.path == path && text.contains(c.sentence.as_str()))
                    .cloned(),
            );
            ops.push(FileOp::Write {
                path: path.to_string(),
                content: next,
            });
        }
    }
    if ops.is_empty() {
        return vec![];
    }
    let conventions = super::load_git_conventions(state, &msg.team_id, &msg.ticket_id).await;
    let message = conventions.commit_message(
        &super::ticket_tokens(msg),
        "Remove claims that a check passes where no result is recorded",
    );
    if let Err(e) = github
        .batch_write(owner, repo, branch, &message, &ops)
        .await
    {
        warn!(branch, error = %e, "Could not take unproven claims out of the files the run wrote");
        return vec![];
    }
    info!(
        branch,
        removed = removed.len(),
        "Took unproven claims out of the run's files"
    );
    removed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::proof::SandboxRecord;
    use serde_json::json;

    fn diff() -> serde_json::Value {
        json!({"files": [
            {"filename": "README.md", "status": "modified",
             "patch": "@@ -4,2 +4,4 @@\n ## Upgrading\n+The build passes with these versions.\n+All tests pass.\n Node 20 or later.\n"},
            {"filename": "src/build.ts", "status": "modified",
             "patch": "@@ -1 +1,2 @@\n+// The build passes with these versions.\n"},
            {"filename": "docs/old.md", "status": "removed",
             "patch": "@@ -1 +0,0 @@\n-All tests pass.\n"}
        ]})
    }

    fn evidence(passed: &[CheckKind]) -> RunEvidence {
        RunEvidence {
            sandbox: SandboxRecord {
                ran: passed.to_vec(),
                passed: passed.to_vec(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn a_claim_needs_the_recorded_result_the_pull_request_is_written_from() {
        // Nothing ran: both sentences are unproven.
        let open = unproven_in_diff(&diff(), &evidence(&[]));
        let got: Vec<(&str, u64, &str)> = open
            .iter()
            .map(|c| (c.path.as_str(), c.line, c.sentence.as_str()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("README.md", 5, "The build passes with these versions."),
                ("README.md", 6, "All tests pass."),
            ]
        );
        // The tests passed in the sandbox; the build never ran.
        let open = unproven_in_diff(&diff(), &evidence(&[CheckKind::Tests, CheckKind::Lint]));
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].sentence, "The build passes with these versions.");
        // Both recorded as passing: the sentences stay.
        assert!(
            unproven_in_diff(&diff(), &evidence(&[CheckKind::Tests, CheckKind::Build])).is_empty()
        );
        let n = note(&open);
        assert!(
            n.starts_with("Took one sentence out of the files this run wrote"),
            "{n}"
        );
        assert!(
            n.contains("`README.md` line 5") && n.contains("recorded for the build"),
            "{n}"
        );
    }

    /// Both paths that write files check them before the description is
    /// built from the branch.
    #[test]
    fn the_run_and_its_fix_rounds_settle_their_claims() {
        let run = include_str!("mod.rs");
        assert!(
            run.matches("file_claims::settle(").count() >= 2,
            "both ways a run opens its pull request"
        );
        let resume = include_str!("resume.rs");
        let round = &resume[resume.find("\nasync fn run_fix_round(").unwrap()..];
        let claims = round.find("file_claims::settle(").unwrap();
        let rebuild = round.find("description::head_moved(").unwrap();
        assert!(
            claims < rebuild,
            "claims are settled before the description is rebuilt"
        );
    }
}
