//! The sandbox work behind telling a bug from a flaky test (see
//! `common::e2e::bug_rules`): repeating the affected spec with retries off
//! on any commit, proving a unit test fails before a fix and passes after,
//! and trying each wait or timeout tweak without it.
//!
//! Every command goes through the team's agent permissions and runs in the
//! sandbox on a commit's tarball; temporary branches made for a check are
//! deleted after it.

use std::collections::HashSet;

use common::e2e::bug_rules::{self, Nature, ViewsRead};
use common::e2e::test_package::{self, Repeat, Step};
use tracing::{info, warn};

use crate::agent::provider::{self, ModelProvider};
use crate::clients::github::GitHubClient;
use crate::models::TokenUsage;
use crate::WorkerState;

/// Incidental hunks tried without, per run.
const MAX_NECESSITY_TRIES: usize = 3;

/// Runs commands in the sandbox for one run.
pub struct Runner<'a> {
    pub state: &'a WorkerState,
    pub github: &'a GitHubClient,
    pub owner: &'a str,
    pub repo: &'a str,
    pub team_id: &'a str,
    pub run_id: &'a str,
    pub provider: &'a ModelProvider,
}

impl Runner<'_> {
    fn sandbox_on(&self) -> bool {
        !self.state.config.sandbox_bucket_name.is_empty()
            && !self.state.config.sandbox_project_name.is_empty()
    }

    /// Runs `steps` (and a repeat command) on commit `sha`: each step's exit
    /// code and the output. `Err` says why nothing ran.
    pub async fn run_at(
        &self,
        sha: &str,
        steps: &[Step],
        repeat: Option<&str>,
    ) -> Result<(Vec<Option<i32>>, String), String> {
        if !self.sandbox_on() {
            return Err("no sandbox is set up here".into());
        }
        if sha.is_empty() {
            return Err("the commit couldn't be read".into());
        }
        let gate = crate::agent::policy::gate(
            self.state,
            self.provider.scope.ctx(common::agent_policy::Agent::Coder),
            None,
        );
        let cmds = steps
            .iter()
            .map(|s| s.cmd.as_str())
            .chain(repeat.iter().copied());
        for cmd in cmds {
            match gate.check_command(cmd, "Check a failing test").await {
                common::agent_policy::Outcome::Run { .. } => {}
                common::agent_policy::Outcome::Refused { message, .. }
                | common::agent_policy::Outcome::Pending { message, .. } => {
                    return Err(format!(
                        "the team's agent permissions don't allow `{cmd}` ({message})"
                    ))
                }
                common::agent_policy::Outcome::Stop(_) => return Err("agents are paused".into()),
            }
        }
        let sandbox = crate::clients::sandbox::SandboxClient::new(
            &self.state.codebuild,
            &self.state.s3,
            &self.state.logs,
            &self.state.config.sandbox_bucket_name,
            &self.state.config.sandbox_project_name,
        )
        .with_egress(crate::agent::policy::sandbox_egress(self.state, self.team_id).await);
        let source = sandbox
            .ensure_source(self.run_id, sha, || {
                self.github.download_tarball(self.owner, self.repo, sha)
            })
            .await
            .map_err(|e| format!("the commit couldn't be prepared ({e})"))?;
        let node = super::detect_node_version(self.github, self.owner, self.repo, sha).await;
        let script = test_package::sandbox_script(steps, repeat);
        match sandbox
            .run_checks(&source, &script, node.as_deref(), None, None)
            .await
        {
            Ok(o) if o.ran => {
                let n = steps.len() + usize::from(repeat.is_some());
                Ok((test_package::step_codes(&o.output, n), o.output))
            }
            Ok(o) => Err(format!("the sandbox gave no result ({})", o.reason)),
            Err(e) => Err(format!("the sandbox couldn't run ({e})")),
        }
    }
}

/// How to repeat the affected spec: the package's install and the repeat
/// command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepeatPlan {
    pub install: Vec<Step>,
    pub cmd: String,
    pub specs: Vec<String>,
    pub target: Option<String>,
}

/// The repeat plan for the spec files named in `hints` (paths or their
/// tails, as reports name them), read at `git_ref`.
pub async fn repeat_plan(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
    hints: &[String],
    target: Option<String>,
) -> Option<RepeatPlan> {
    let tree: Vec<String> = github
        .get_tree(owner, repo, git_ref)
        .await
        .ok()?
        .into_iter()
        .map(|e| e.path)
        .collect();
    let spec = hints.iter().find_map(|h| {
        let h = h.trim_start_matches("./");
        tree.iter()
            .find(|p| {
                (*p == h || p.ends_with(&format!("/{h}"))) && common::e2e::fix_guard::is_spec(p)
            })
            .cloned()
    })?;
    let mut texts = std::collections::HashMap::new();
    let dir = test_package::package_dir(&tree, &spec)?;
    for p in [format!("{dir}/package.json"), "package.json".to_string()] {
        let p = p.trim_start_matches('/').to_string();
        if let Ok(t) = github.read_file(owner, repo, &p, git_ref).await {
            texts.insert(p, t);
        }
    }
    let pkg = test_package::package_of(&tree, &spec, &|p| texts.get(p).cloned())?;
    let cmd = test_package::repeat_cmd(
        &pkg,
        &pkg.files,
        test_package::REPEAT_RUNS,
        target.as_deref(),
    )?;
    let install = test_package::check_steps(&pkg)?
        .into_iter()
        .filter(|s| s.kind == "install")
        .collect();
    Some(RepeatPlan {
        install,
        cmd,
        specs: vec![spec],
        target,
    })
}

/// The repeat run's counts on commit `sha`.
pub async fn measure(r: &Runner<'_>, plan: &RepeatPlan, sha: &str) -> Result<Repeat, String> {
    let (_, out) = r.run_at(sha, &plan.install, Some(&plan.cmd)).await?;
    test_package::parse_repeat(&out).ok_or_else(|| "the repeat run printed no counts".into())
}

/// Records the verdict on the run: a progress note and `test_nature`.
pub async fn record_nature(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    nature: Nature,
    line: &str,
) {
    super::add_progress_note(state, team_id, run_id, line).await;
    let value = serde_json::to_value(nature)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default();
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(team_id))
        .key("run_id", super::attr_s(run_id))
        .update_expression("SET test_nature = :n, test_nature_note = :l")
        .expression_attribute_values(":n", super::attr_s(&value))
        .expression_attribute_values(":l", super::attr_s(line))
        .send()
        .await
    {
        warn!(error = %e, "Could not record whether the test is flaky");
    }
}

/// Asks the model whether the failing assertion compares two views.
pub async fn read_views(
    state: &WorkerState,
    provider: &ModelProvider,
    usage: &mut TokenUsage,
    error: &str,
    snippet: &str,
    spec: &str,
) -> Option<ViewsRead> {
    match provider::converse_simple(
        state,
        provider,
        provider.primary_model_id(),
        bug_rules::VIEWS_SYSTEM,
        &bug_rules::views_prompt(error, snippet, spec),
        usage,
    )
    .await
    {
        Ok(t) => bug_rules::parse_views(&t),
        Err(e) => {
            warn!(error = %e, "Could not read what the failing assertion compares");
            None
        }
    }
}

/// Runs the new unit tests on the code before the fix (a temporary branch:
/// the base plus only those test files) and on the branch: `(before,
/// after)` exit codes, or why they couldn't run.
pub async fn unit_before_after(
    r: &Runner<'_>,
    base_branch: &str,
    branch: &str,
    unit_files: &[String],
) -> Result<(Option<i32>, Option<i32>), String> {
    let base_cmd = super::detect_ci_test(r.github, r.owner, r.repo, base_branch)
        .await
        .ok_or("the repository's CI runs no unit test command Coderhelm can read")?;
    let cmd = super::build_scoped_test_cmd(&base_cmd, unit_files)
        .ok_or("the unit test command can't be scoped to the new tests")?;
    let tree: Vec<String> = r
        .github
        .get_tree(r.owner, r.repo, branch)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(|e| e.path)
        .collect();
    let install: Vec<Step> = unit_files
        .first()
        .and_then(|f| test_package::package_of(&tree, f, &|_| None))
        .and_then(|p| test_package::check_steps(&p))
        .map(|s| s.into_iter().filter(|s| s.kind == "install").collect())
        .unwrap_or_default();
    let mut steps = install.clone();
    steps.push(Step {
        kind: "unit",
        cmd: cmd.clone(),
    });
    let head = r
        .github
        .get_ref(r.owner, r.repo, branch)
        .await
        .map_err(|e| e.to_string())?;
    let (after, _) = r.run_at(&head, &steps, None).await?;
    let base_sha = r
        .github
        .get_ref(r.owner, r.repo, base_branch)
        .await
        .map_err(|e| e.to_string())?;
    let temp = format!("{branch}-unit-before");
    r.github
        .create_branch(r.owner, r.repo, &temp, &base_sha)
        .await
        .map_err(|e| format!("a branch for the check couldn't be made ({e})"))?;
    let result = async {
        for f in unit_files {
            let content = r
                .github
                .read_file(r.owner, r.repo, f, branch)
                .await
                .map_err(|e| e.to_string())?;
            let sha = r.github.get_file_sha(r.owner, r.repo, f, &temp).await.ok();
            r.github
                .write_file(
                    r.owner,
                    r.repo,
                    f,
                    &content,
                    &temp,
                    "test: the new unit test on the code before the fix",
                    sha.as_deref(),
                )
                .await
                .map_err(|e| e.to_string())?;
        }
        let before_sha = r
            .github
            .get_ref(r.owner, r.repo, &temp)
            .await
            .map_err(|e| e.to_string())?;
        r.run_at(&before_sha, &steps, None).await
    }
    .await;
    if let Err(e) = r.github.delete_branch(r.owner, r.repo, &temp).await {
        warn!(error = %e, branch = %temp, "Could not delete the unit-test check branch");
    }
    let (before, _) = result?;
    Ok((
        before.last().copied().flatten(),
        after.last().copied().flatten(),
    ))
}

/// After the fix passes: each wait or timeout tweak in test code is taken
/// out on a temporary branch and the repeat run tried again. A tweak the
/// test still passes without is dropped from the branch. Returns one line
/// per tweak tried.
pub async fn necessity(
    r: &Runner<'_>,
    base_branch: &str,
    branch: &str,
    plan: &RepeatPlan,
) -> Vec<String> {
    let mut lines = Vec::new();
    let mut tried: HashSet<String> = HashSet::new();
    for n in 0..MAX_NECESSITY_TRIES {
        let Ok(files) = r
            .github
            .compare_files_with_patches(r.owner, r.repo, base_branch, branch)
            .await
        else {
            break;
        };
        let Some(h) = bug_rules::incidental_hunks(&files)
            .into_iter()
            .find(|h| !tried.contains(&h.key()))
        else {
            break;
        };
        tried.insert(h.key());
        let Ok(content) = r.github.read_file(r.owner, r.repo, &h.path, branch).await else {
            continue;
        };
        let Some(without) = bug_rules::revert(&content, &h) else {
            lines.push(format!(
                "Kept {}: it couldn't be taken out on its own.",
                h.summary()
            ));
            continue;
        };
        let Ok(head) = r.github.get_ref(r.owner, r.repo, branch).await else {
            break;
        };
        let temp = format!("{branch}-necessity-{n}");
        if r.github
            .create_branch(r.owner, r.repo, &temp, &head)
            .await
            .is_err()
        {
            lines.push(format!(
                "Kept {}: no branch could be made to check it.",
                h.summary()
            ));
            continue;
        }
        let checked = async {
            let sha = r
                .github
                .get_file_sha(r.owner, r.repo, &h.path, &temp)
                .await
                .ok();
            r.github
                .write_file(
                    r.owner,
                    r.repo,
                    &h.path,
                    &without,
                    &temp,
                    "test: check the fix without one wait",
                    sha.as_deref(),
                )
                .await
                .map_err(|e| e.to_string())?;
            let sha = r
                .github
                .get_ref(r.owner, r.repo, &temp)
                .await
                .map_err(|e| e.to_string())?;
            measure(r, plan, &sha).await
        }
        .await;
        if let Err(e) = r.github.delete_branch(r.owner, r.repo, &temp).await {
            warn!(error = %e, branch = %temp, "Could not delete the necessity check branch");
        }
        match checked {
            Ok(rep) if rep.green() => {
                let sha = r
                    .github
                    .get_file_sha(r.owner, r.repo, &h.path, branch)
                    .await
                    .ok();
                match r
                    .github
                    .write_file(
                        r.owner,
                        r.repo,
                        &h.path,
                        &without,
                        branch,
                        "test: drop a wait the test passes without",
                        sha.as_deref(),
                    )
                    .await
                {
                    Ok(_) => lines.push(format!(
                        "Dropped {}: the test still passed {} without it.",
                        h.summary(),
                        rep.line()
                    )),
                    Err(e) => lines.push(format!(
                        "Kept {}: it couldn't be dropped ({e}).",
                        h.summary()
                    )),
                }
            }
            Ok(rep) => lines.push(format!(
                "Kept {}: without it the test passed only {}.",
                h.summary(),
                rep.line()
            )),
            Err(e) => lines.push(format!(
                "Kept {}: no repeat run could check it ({e}).",
                h.summary()
            )),
        }
    }
    info!(
        run_id = r.run_id,
        tried = lines.len(),
        "Necessity check of incidental changes done"
    );
    lines
}
