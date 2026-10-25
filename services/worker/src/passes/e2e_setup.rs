//! Opens the pull request that sets a repo up to send E2E results.
//!
//! The change itself is planned by `common::e2e::ci_setup` (the workflow is
//! edited through a YAML parser, so comments and order stay). This pass reads
//! the files on the default branch, writes the edited workflow on
//! [`SETUP_BRANCH`] and opens (or updates) the pull request, recording each
//! step on the repo's setup record so the dashboard can show progress.

use common::e2e::ci_setup::{
    inspect, package_scripts, plan, pr_request, SetupPr, SetupRequest, SETUP_BRANCH,
};
use common::e2e::messages::TestAction;
use common::e2e::store::Store;
use tracing::{info, warn};

use crate::clients::github::GitHubClient;
use crate::WorkerState;

type Err = Box<dyn std::error::Error + Send + Sync>;

async fn mark(st: &Store<'_>, a: &TestAction, pr: &mut SetupPr, status: &str) {
    pr.status = status.to_string();
    pr.updated_at = chrono::Utc::now().to_rfc3339();
    if let Err(e) = st.put_setup_pr(&a.team_id, &a.repo, pr).await {
        warn!(error = %e, "Could not record setup progress");
    }
}

pub async fn run(state: &WorkerState, a: TestAction) -> Result<(), Err> {
    let st = Store::new(&state.dynamo, &state.config.tests_table_name);
    let req = a.setup.clone().ok_or("setup without a request")?;
    let mut pr: SetupPr =
        serde_json::from_value(st.setup(&a.team_id, &a.repo).await?["pr"].clone())
            .unwrap_or_default();
    pr.error = None;
    match open(state, &st, &a, &req, &mut pr).await {
        Ok(()) => Ok(()),
        Err(e) => {
            warn!(repo = %a.repo, error = %e, "Setup pull request failed");
            pr.error = Some(error_words(&e.to_string()));
            mark(&st, &a, &mut pr, "failed").await;
            Ok(())
        }
    }
}

/// What a person sees when the pull request couldn't be opened.
fn error_words(e: &str) -> String {
    let lower = e.to_lowercase();
    if lower.contains("workflow")
        && (lower.contains("refusing") || lower.contains("403") || lower.contains("permission"))
    {
        return "GitHub refused the change: Coderhelm's GitHub app needs the Workflows permission to edit files in .github/workflows. Accept the permission update in your GitHub app settings, or use Do it myself.".into();
    }
    common::truncate_str(e, 300).to_string()
}

async fn open(
    state: &WorkerState,
    st: &Store<'_>,
    a: &TestAction,
    req: &SetupRequest,
    pr: &mut SetupPr,
) -> Result<(), Err> {
    let (owner, name) = a.repo.split_once('/').ok_or("bad repo")?;
    let gh = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        req.installation_id,
        &state.http,
    )?;
    mark(st, a, pr, "reading").await;
    let base = &req.default_branch;
    let yaml = gh
        .read_file_live(owner, name, &req.workflow_path, base)
        .await?;
    let pkg = gh
        .read_file_if_exists(owner, name, "package.json", base)
        .await
        .ok()
        .flatten();
    let scripts = package_scripts(pkg.as_deref());
    let facts = inspect(&yaml, &scripts).ok_or("The workflow no longer runs tests.")?;
    let planned = plan(&yaml, &scripts, req.options).map_err(|e| -> Err { e.into() })?;
    if !planned.changed() {
        mark(st, a, pr, "nothing_to_do").await;
        return Ok(());
    }
    let request = pr_request(
        &req.workflow_path,
        base,
        &facts,
        &planned,
        req.options.repair,
    );

    mark(st, a, pr, "opening").await;
    // A branch left from an earlier attempt starts again from the base, so
    // the pull request only ever holds this one change.
    let existing = gh
        .find_open_pr_for_branch(owner, name, SETUP_BRANCH)
        .await
        .ok()
        .flatten();
    if existing.is_none() {
        let _ = gh.delete_branch(owner, name, SETUP_BRANCH).await;
    }
    if existing.is_none() || gh.get_ref(owner, name, SETUP_BRANCH).await.is_err() {
        gh.create_branch(owner, name, SETUP_BRANCH, base).await?;
    }
    let sha = gh
        .get_file_sha(owner, name, &request.path, SETUP_BRANCH)
        .await
        .ok();
    let written = gh
        .write_file(
            owner,
            name,
            &request.path,
            &planned.yaml,
            SETUP_BRANCH,
            &request.commit_message,
            sha.as_deref(),
        )
        .await?;
    let opened = match existing {
        Some(p) => p,
        None => {
            gh.create_pull_request(
                owner,
                name,
                &request.title,
                &request.body,
                SETUP_BRANCH,
                base,
                false,
            )
            .await?
        }
    };
    pr.number = opened["number"].as_u64();
    pr.url = opened["html_url"].as_str().map(str::to_string);
    pr.head_sha = written["commit"]["sha"].as_str().map(str::to_string);
    info!(repo = %a.repo, pr = ?pr.number, added = ?planned.added, "Setup pull request open");
    mark(st, a, pr, "open").await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::error_words;

    #[test]
    fn a_missing_workflows_permission_is_explained() {
        let e = "422: refusing to allow a GitHub App to create or update workflow `.github/workflows/e2e.yml` without `workflows` permission";
        assert!(error_words(e).contains("Workflows permission"));
        assert_eq!(error_words("network down"), "network down");
    }
}
