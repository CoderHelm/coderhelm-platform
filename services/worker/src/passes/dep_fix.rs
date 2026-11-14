//! Runs a dependency fix (`common::dep_fix`) for a security ticket: the
//! rules plan the bumps, the sandbox applies them, writes the lockfile with
//! the repository's own package manager and runs its checks; a red set is
//! halved until the breaking bump is found; only green sets are committed.
//! The coder is used only for the call sites of a major the run must fix.
//! Once the batches are settled the repository's real build and its start
//! command run in a sandbox build of their own, on the base branch and on
//! the branch (`common::runtime_proof`): what the bumps break there takes
//! out the newest commit of bumps, deferred with the cause. Then the set a
//! dependency change is verified with whatever CI runs
//! (`common::upgrade_quality::verify`: the project's own frozen install,
//! type check, build, lint, tests and production audit) runs on the branch,
//! and on the base branch where the branch fails something. What the run
//! found is recorded for its head commit (`passes::upgrade_quality`).
//!
//! The run's state is kept on S3 after every probe, so a step that runs out
//! of time hands off and the next one resumes where it stopped (and its
//! progress token tells the step guard the run is moving).

use std::collections::HashMap;
use std::time::Instant;

use common::dep_fix::{self as df, Action, CodeFixed, DepRun, ProbeResult};
use common::runtime_proof as rp;
use common::upgrade_quality::{family as fam, verify as vq};
use tracing::{info, warn};

use crate::clients::github::{FileOp, GitHubClient};
use crate::models::{TicketMessage, TokenUsage};
use crate::WorkerState;

use super::checks::{self, Step};
use super::RunUsageValues;
use common::run_status::EndedValues as _;

/// What a dependency run came to in this step.
pub enum Driven {
    /// Every batch is settled: the run goes on to its pull request with
    /// what is committed (nothing, when every bump was deferred).
    Done(Box<DepRun>),
    /// This step's time is up; the state is saved for the next step.
    Handoff(Box<DepRun>),
}

fn key(team_id: &str, ticket_id: &str, run_id: &str) -> String {
    format!(
        "teams/{team_id}/runs/{}/run_state/{run_id}/dep_fix.json",
        ticket_id.to_lowercase()
    )
}

/// The run's saved state, when an earlier step kept one.
pub async fn load(state: &WorkerState, msg: &TicketMessage, run_id: &str) -> Option<DepRun> {
    let text = super::read_s3_text(state, &key(&msg.team_id, &msg.ticket_id, run_id)).await?;
    serde_json::from_str(&text).ok()
}

pub async fn save(state: &WorkerState, msg: &TicketMessage, run_id: &str, run: &DepRun) {
    let Ok(json) = serde_json::to_string(run) else {
        return;
    };
    if let Err(e) = state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(key(&msg.team_id, &msg.ticket_id, run_id))
        .body(json.into_bytes().into())
        .content_type("application/json")
        .send()
        .await
    {
        warn!(run_id, error = %e, "dep fix: the run's state could not be saved");
    }
}

/// The dependency run for this ticket: its saved state, or a new plan from
/// the unit's findings. None when the ticket isn't a security fix, or its
/// bumps aren't in a lockfile the sandbox can edit (the coder's flow then).
pub async fn start(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    run_id: &str,
) -> Option<DepRun> {
    if !common::security_fixes::is_ticket(&msg.ticket_id) || !sandbox_ready(state) {
        return None;
    }
    if let Some(run) = load(state, msg, run_id).await {
        return Some(run);
    }
    let inputs = super::security_fixes::dep_inputs(state, &msg.team_id, &msg.ticket_id).await?;
    let tree: Vec<String> = github
        .get_tree(&msg.repo_owner, &msg.repo_name, &msg.base_branch)
        .await
        .ok()?
        .into_iter()
        .filter(|e| e.entry_type == "blob")
        .map(|e| e.path)
        .collect();
    let locks: Vec<String> = common::upgrade_notes::lock_tools(&tree)
        .into_iter()
        .map(|t| t.lockfile)
        .collect();
    let pairs: Vec<(
        &common::security_fixes::Finding,
        &common::security_fixes::FindingState,
    )> = inputs.findings.iter().map(|(f, s)| (f, s)).collect();
    let mut plan = df::plan_in(&pairs, &inputs.cfg, &locks);
    if !plan
        .bumps
        .iter()
        .any(|b| df::Manager::of_lockfile(&b.manifest).is_some())
    {
        info!(
            run_id,
            "dep fix: no bump in a lockfile the sandbox writes; the coder's flow"
        );
        return None;
    }
    let head = github
        .get_ref(&msg.repo_owner, &msg.repo_name, &msg.base_branch)
        .await
        .ok()?;
    let budget = inputs.cfg.fix_run_budget_cents as f64 / 100.0;
    // A major a parent's release brings in is fixed by raising the parent,
    // never by an override; packages released together move together.
    let notes = super::security_fixes::dep_notes(state, &msg.team_id, &msg.ticket_id).await;
    common::upgrade_quality::safety::route_through_parents(&mut plan, &notes, &inputs.findings);
    let (families, peers) = super::upgrade_quality::families(state, github, msg, &mut plan).await;
    let mut run = DepRun::new(plan, &head, budget);
    run.families = families;
    run.peers = peers;
    save(state, msg, run_id, &run).await;
    Some(run)
}

fn sandbox_ready(state: &WorkerState) -> bool {
    !state.config.sandbox_bucket_name.is_empty() && !state.config.sandbox_project_name.is_empty()
}

/// The plan a dependency run is held to: one task per lockfile its bumps
/// go through, never a documentation file (see `DepPlan::tasks_md`).
pub fn plan_result(run: &DepRun, only_applied: bool) -> super::plan::PlanResult {
    let plan = if only_applied {
        df::DepPlan {
            bumps: run.applied_bumps().into_iter().cloned().collect(),
            deferred: vec![],
        }
    } else {
        run.plan.clone()
    };
    super::plan::PlanResult {
        proposal: "Apply the security bumps the rules chose, verified in the sandbox with the repository's own checks.".into(),
        design: run.report_md(),
        tasks: plan.tasks_md(),
        spec: String::new(),
        repo_tasks: vec![],
        fix: None,
    }
}

/// Keeps the plan as this run's own plan files, so a continuation reads
/// the same plan (`load_run_plan`) instead of planning again.
pub async fn save_plan_files(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    plan: &super::plan::PlanResult,
) {
    let prefix = super::openspec_prefix(
        &msg.team_id,
        &msg.ticket_id,
        &msg.repo_owner,
        &msg.repo_name,
    );
    let spec = "A dependency fix run: the bumps are applied by rule and verified in the sandbox.";
    for (name, content) in [
        ("proposal.md", plan.proposal.as_str()),
        ("design.md", plan.design.as_str()),
        ("tasks.md", plan.tasks.as_str()),
        ("spec.md", spec),
    ] {
        if let Err(e) = state
            .s3
            .put_object()
            .bucket(&state.config.bucket_name)
            .key(format!("{prefix}/{name}"))
            .body(content.as_bytes().to_vec().into())
            .content_type("text/markdown")
            .send()
            .await
        {
            warn!(run_id, error = %e, "dep fix: the plan could not be kept");
            return;
        }
    }
    super::plan::mark_plan_run(state, msg, run_id).await;
}

/// Whether the run is a finished dependency run that changed only
/// manifests and lockfiles (no coder change): no code for the model's test
/// writer or audit to read.
pub async fn mechanical(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    run_id: &str,
    branch: &str,
) -> bool {
    let Some(run) = load(state, msg, run_id).await else {
        return false;
    };
    if !run.done || run.code_fixes > 0 || run.applied.is_empty() {
        return false;
    }
    let files = github
        .compare_changed_files(&msg.repo_owner, &msg.repo_name, &msg.base_branch, branch)
        .await
        .unwrap_or_default();
    !files.is_empty() && files.iter().all(|f| df::commit_path(f))
}

/// A dependency run where nothing could ship safely ends completed, with
/// every deferral and its reason on the run page: never a question, never a
/// failure to retry.
pub async fn complete_without_pr(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    usage: &mut TokenUsage,
    start: &Instant,
    run: &DepRun,
) {
    let lines: Vec<String> = run
        .all_deferred()
        .iter()
        .take(20)
        .map(|d| format!("- {}", d.line()))
        .collect();
    let note = format!(
        "Dependency fix: no bump could ship safely, so no pull request was opened. Deferred:\n{}",
        lines.join("\n")
    );
    super::add_progress_note(state, &msg.team_id, run_id, &note).await;
    let delta = usage.unrecorded();
    let wrote = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(&msg.team_id))
        .key("run_id", super::attr_s(run_id))
        .update_expression(
            "SET #status = :s, status_run_id = :sri, \
             tokens_in = if_not_exists(tokens_in, :uzero) + :ti, tokens_out = if_not_exists(tokens_out, :uzero) + :to, \
             cache_read_tokens = if_not_exists(cache_read_tokens, :uzero) + :crt, cache_write_tokens = if_not_exists(cache_write_tokens, :uzero) + :cwt, \
             cost_usd = if_not_exists(cost_usd, :uzero) + :c, duration_s = :d, updated_at = :t, current_pass = :cp",
        )
        .expression_attribute_names("#status", "status")
        .condition_expression(common::run_status::not_ended("#status"))
        .ended_values()
        .expression_attribute_values(":s", super::attr_s("completed"))
        .expression_attribute_values(":sri", super::attr_s(&format!("completed#{run_id}")))
        .usage_values(&delta)
        .expression_attribute_values(":d", super::attr_n(start.elapsed().as_secs()))
        .expression_attribute_values(":t", super::attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":cp", super::attr_s("done"))
        .send()
        .await
        .is_ok();
    if wrote {
        usage.mark_recorded(&delta);
        super::webhooks::emit_run(state, &msg.team_id, run_id, "completed").await;
    }
}

/// What every probe of this run needs, read once per step.
struct Probe<'a> {
    msg: &'a TicketMessage,
    github: &'a GitHubClient,
    run_id: &'a str,
    sandbox: crate::clients::sandbox::SandboxClient<'a>,
    steps: Vec<Step>,
    locks: Vec<(String, String)>,
    node: Option<String>,
    deadline: Option<Instant>,
    attempt: std::sync::atomic::AtomicUsize,
    /// The run's families: every lockfile a probe writes is checked
    /// against them.
    families: Vec<fam::Family>,
}

/// One probe's result and the manifests and lockfiles it wrote.
#[derive(Default)]
struct Probed {
    result: ProbeResult,
    files: Vec<(String, String)>,
    /// Packages whose parent was raised instead of an override, and that
    /// the lockfile the probe wrote still installs below the fixed version.
    parent_unmoved: Vec<String>,
}

impl Probe<'_> {
    async fn run(&self, sha: &str, bumps: &[&df::PlanBump], checks_on: bool) -> Probed {
        let (owner, name) = (&self.msg.repo_owner, &self.msg.repo_name);
        let source = match self
            .sandbox
            .ensure_source(self.run_id, sha, || {
                self.github.download_tarball(owner, name, sha)
            })
            .await
        {
            Ok(s) => s,
            Err(e) => {
                return Probed {
                    result: ProbeResult {
                        tail: format!("the branch could not be snapshotted: {e}"),
                        ..Default::default()
                    },
                    ..Default::default()
                }
            }
        };
        let specs = df::specs(bumps);
        let checks = if checks_on {
            checks::script(&self.steps)
        } else {
            "true".to_string()
        };
        let cmd = df::probe_command(&specs, &self.locks, &checks);
        let n = self
            .attempt
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let out = self
            .sandbox
            .run_codegen(
                self.run_id,
                500 + n,
                &source,
                &cmd,
                self.node.as_deref(),
                &[],
                self.deadline,
            )
            .await;
        let o = match out {
            Ok(o) => o,
            Err(e) => {
                return Probed {
                    result: ProbeResult {
                        tail: e.to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                }
            }
        };
        let marks = df::probe_marks(&o.output);
        let ran = o.ran && marks.is_some();
        // A bump the script could not place (no manifest for it) is not
        // applied, whatever the checks say.
        let applied =
            marks.is_some_and(|(a, l)| a == 0 && l == 0) && df::apply_skipped(&o.output).is_empty();
        let codes = checks::step_results(&o.output, self.steps.len());
        let checks: Vec<(String, bool)> = if applied && checks_on {
            if codes.iter().all(Option::is_none) {
                // The checks never reported (cut short): no verdict.
                return Probed {
                    result: ProbeResult {
                        tail: common::tail_str(&o.output, 2000).to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                };
            }
            self.steps
                .iter()
                .zip(&codes)
                .map(|(s, c)| (s.label(), matches!(c, Some(0))))
                .collect()
        } else {
            vec![]
        };
        let files: Vec<(String, String)> = o
            .changed
            .into_iter()
            .filter(|(p, _)| df::commit_path(p))
            .collect();
        let mut result = ProbeResult {
            ran,
            applied,
            checks,
            tail: common::tail_str(&o.output, 3000).to_string(),
        };
        // The lockfile the bumps wrote, against the run's families and the
        // parents it raised: a family at two versions fails the set like
        // any check.
        let (split, parent_unmoved) = if applied {
            df::lockfile_violations(&self.families, bumps, &files)
        } else {
            (vec![], vec![])
        };
        if !split.is_empty() {
            result.checks.push((df::FAMILY_CHECK.to_string(), false));
            result.tail = format!("{}\n{}", split.join("; "), result.tail);
        }
        Probed {
            result,
            files,
            parent_unmoved,
        }
    }
}

/// The longest one verification build is given, and what a step keeps
/// back from it for the build's start and the step's own end.
const VERIFY_WINDOW: std::time::Duration = std::time::Duration::from_secs(11 * 60);
const VERIFY_MARGIN: std::time::Duration = std::time::Duration::from_secs(90);

/// The seconds a verification build started now has for its checks.
pub fn verify_window(left: Option<std::time::Duration>) -> u32 {
    let w = match left {
        Some(l) => l.saturating_sub(VERIFY_MARGIN).min(VERIFY_WINDOW),
        None => VERIFY_WINDOW,
    };
    w.as_secs() as u32
}

/// Whether a verification build should wait for a fresh step: started now
/// it would leave out a check a full window runs, and the run has not
/// already moved to a fresh step for this build.
pub fn verify_waits(plan: &vq::Plan, left: Option<std::time::Duration>, waited: bool) -> bool {
    let now = vq::fit(plan, verify_window(left)).0.len();
    let full = vq::fit(plan, verify_window(None)).0.len();
    !waited && now < full
}

impl Probe<'_> {
    /// One build of the verification set on `sha`: the checks that fit the
    /// time this step has, each recorded against the commit; the ones that
    /// do not fit are recorded as not run, with why. On the branch the
    /// audit's listing is read from the same build.
    async fn verify(
        &self,
        sha: &str,
        plan: &vq::Plan,
        nonce: usize,
    ) -> (vq::Verified, Option<common::dep_audit::Listings>) {
        let (owner, name) = (&self.msg.repo_owner, &self.msg.repo_name);
        let left = self
            .deadline
            .map(|d| d.saturating_duration_since(Instant::now()));
        let (run, not) = vq::fit(plan, verify_window(left));
        if run.is_empty() {
            return (
                vq::read(plan, sha, &[], &not, "").with_reason(
                    "this step had no time left for a sandbox build of the verification set",
                ),
                None,
            );
        }
        let source = match self
            .sandbox
            .ensure_source(self.run_id, sha, || {
                self.github.download_tarball(owner, name, sha)
            })
            .await
        {
            Ok(s) => s,
            Err(e) => {
                return (
                    vq::no_result(
                        plan,
                        sha,
                        &format!("the branch could not be snapshotted: {e}"),
                    ),
                    None,
                )
            }
        };
        let reducer = common::dep_audit::listing(&plan.manager, &plan.dir).unwrap_or_default();
        let out = self
            .sandbox
            .run_codegen(
                self.run_id,
                1300 + nonce,
                &source,
                &vq::script(plan, &run, &reducer),
                self.node.as_deref(),
                &[],
                self.deadline,
            )
            .await;
        match out {
            Ok(o) if o.ran => {
                let audited = run.iter().any(|&i| plan.checks[i].kind == vq::Kind::Audit);
                (
                    vq::read(plan, sha, &run, &not, &o.output),
                    audited.then(|| common::dep_audit::listings(&plan.manager, &o.output)),
                )
            }
            Ok(o) => (vq::no_result(plan, sha, &o.output), None),
            Err(e) => (vq::no_result(plan, sha, &e.to_string()), None),
        }
    }
}

/// The least time left in a step for a runtime proof build to start in it:
/// an install, the application's build and its start. With less, the proof
/// starts in a fresh step, where it has the sandbox's whole wait
/// (`clients::sandbox::MAX_WAIT`, bounded by the step's own time).
const PROOF_WINDOW: std::time::Duration = std::time::Duration::from_secs(9 * 60);

/// Whether a runtime proof build should wait for a fresh step: this step
/// has less than [`PROOF_WINDOW`] left, and the run has not already moved
/// to a fresh step for this build.
pub fn proof_waits(left: Option<std::time::Duration>, waited: bool) -> bool {
    !waited && left.is_some_and(|l| l < PROOF_WINDOW)
}

impl Probe<'_> {
    /// One runtime proof build on `sha`: the install, each build command,
    /// then the start command and its pages.
    async fn prove(&self, sha: &str, plan: &rp::ProofPlan, nonce: usize) -> rp::ProofResult {
        let (owner, name) = (&self.msg.repo_owner, &self.msg.repo_name);
        let source = match self
            .sandbox
            .ensure_source(self.run_id, sha, || {
                self.github.download_tarball(owner, name, sha)
            })
            .await
        {
            Ok(s) => s,
            Err(e) => {
                return rp::ProofResult::no_result(&format!(
                    "the branch could not be snapshotted: {e}"
                ))
            }
        };
        let out = self
            .sandbox
            .run_codegen(
                self.run_id,
                900 + nonce,
                &source,
                &rp::script(plan),
                self.node.as_deref(),
                &[],
                self.deadline,
            )
            .await;
        match out {
            Ok(o) if o.ran => rp::parse(&o.output, plan),
            Ok(o) => rp::ProofResult::no_result(&o.output),
            Err(e) => rp::ProofResult::no_result(&e.to_string()),
        }
    }
}

/// What the runtime proof of a run runs: the build and start commands of
/// the project most of its applied bumps are in, read from the repository's
/// workflows and that project's manifest, then the build of every other
/// project the bumps can break: each workspace project whose dependencies
/// bring in a changed package line (read from the lockfile), and the
/// project of every other lockfile the run bumped. A project with no build
/// to run is named as not built, with why.
async fn proof_plan(w: &Work<'_>, run: &DepRun) -> rp::ProofPlan {
    let (o, n, base) = (&w.msg.repo_owner, &w.msg.repo_name, &w.msg.base_branch);
    let mut by: HashMap<&str, usize> = HashMap::new();
    for b in run.applied_bumps() {
        *by.entry(b.manifest.as_str()).or_default() += 1;
    }
    let mut locks: Vec<(&str, usize)> = by.into_iter().collect();
    locks.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    let Some((lockfile, _)) = locks.first().copied() else {
        return rp::ProofPlan::default();
    };
    let workflows = checks::read_workflows(w.github, o, n, base).await;
    let ci = checks::build_commands(&workflows, base);
    let mut plan = rp::discover(lockfile, &ci, &proof_files(w, lockfile).await);
    if plan.install.is_empty() {
        // Not a lockfile CoderHelm builds from: nothing more to add.
        plan.not_covered = locks.iter().skip(1).map(|(l, _)| l.to_string()).collect();
        return plan;
    }
    // The workspace projects that take a changed package line.
    let asked: Vec<(String, String)> = run
        .applied_bumps()
        .iter()
        .filter(|b| b.manifest == lockfile)
        .map(|b| (b.package.clone(), b.from.clone()))
        .collect();
    match affected_projects(w, lockfile, &asked).await {
        Ok(projects) => rp::add_projects(&mut plan, lockfile, &projects),
        Err(why) => plan.projects_unknown = why,
    }
    // The projects of the other lockfiles with bumps.
    for (other, bumps) in locks.iter().skip(1) {
        let found = rp::discover(other, &ci, &proof_files(w, other).await);
        rp::add_lockfile(&mut plan, other, *bumps, &found);
    }
    plan
}

/// The files [`rp::discover`] reads for a lockfile that exist on the base
/// branch.
async fn proof_files(w: &Work<'_>, lockfile: &str) -> Vec<(String, String)> {
    let (o, n, base) = (&w.msg.repo_owner, &w.msg.repo_name, &w.msg.base_branch);
    let mut files = vec![];
    for path in rp::files_to_read(lockfile) {
        if let Ok(Some(text)) = w.github.read_file_if_exists(o, n, &path, base).await {
            files.push((path, text));
        }
    }
    files
}

/// Workspace manifests one proof reads at most.
const MAX_WORKSPACE_MANIFESTS: usize = 60;

/// The workspace projects of `lockfile` whose dependencies bring in one of
/// the changed package lines (`asked`: package and the version it had),
/// each with its scripts manifest. `Err` says why they could not be worked
/// out; a lockfile with one project gives none.
async fn affected_projects(
    w: &Work<'_>,
    lockfile: &str,
    asked: &[(String, String)],
) -> Result<Vec<rp::AffectedProject>, String> {
    let (o, n, base) = (&w.msg.repo_owner, &w.msg.repo_name, &w.msg.base_branch);
    if !common::reachability::is_lockfile(lockfile) {
        // Not a JavaScript workspace lockfile: one project per lockfile.
        return Ok(vec![]);
    }
    let root = lockfile.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
    let at = |dir: &str| -> String {
        match (root.is_empty(), dir.is_empty()) {
            (_, true) => root.to_string(),
            (true, false) => dir.to_string(),
            (false, false) => format!("{root}/{dir}"),
        }
    };
    let manifest_path = |dir: &str| -> String {
        let d = at(dir);
        if d.is_empty() {
            "package.json".to_string()
        } else {
            format!("{d}/package.json")
        }
    };
    let text = match w.github.read_file_if_exists(o, n, lockfile, base).await {
        Ok(Some(t)) => t,
        _ => return Err(format!("`{lockfile}` could not be read on the base branch")),
    };
    // `yarn.lock` does not list its projects: the root manifest's
    // workspaces and each project's manifest do.
    let mut manifests: Vec<(String, String)> = vec![];
    if lockfile.ends_with("yarn.lock") {
        let Ok(Some(root_manifest)) = w
            .github
            .read_file_if_exists(o, n, &manifest_path(""), base)
            .await
        else {
            return Err(format!(
                "`{}` could not be read to list the workspace",
                manifest_path("")
            ));
        };
        let tree: Vec<String> = match w.github.get_tree(o, n, base).await {
            Ok(t) => t.into_iter().map(|e| e.path).collect(),
            Err(_) => return Err("the repository's files could not be listed".to_string()),
        };
        let dirs = rp::workspace_dirs(&root_manifest, &tree, root);
        if dirs.len() > MAX_WORKSPACE_MANIFESTS {
            return Err(format!(
                "the workspace has {} projects, more than one proof reads",
                dirs.len()
            ));
        }
        manifests.push((String::new(), root_manifest));
        for d in dirs {
            if let Ok(Some(t)) = w
                .github
                .read_file_if_exists(o, n, &manifest_path(&d), base)
                .await
            {
                manifests.push((d, t));
            }
        }
    }
    let Some(projects) = common::reachability::lock_projects(lockfile, &text, &manifests, asked)
    else {
        return Err(format!(
            "`{lockfile}` could not be read as a workspace's lockfile"
        ));
    };
    let mut out = vec![];
    for p in projects.into_iter().filter(|p| !p.uses.is_empty()) {
        let manifest = match manifests.iter().find(|(d, _)| *d == p.dir) {
            Some((_, t)) => Some(t.clone()),
            None => w
                .github
                .read_file_if_exists(o, n, &manifest_path(&p.dir), base)
                .await
                .ok()
                .flatten(),
        };
        out.push(rp::AffectedProject {
            dir: at(&p.dir),
            uses: p.uses,
            manifest,
        });
    }
    Ok(out)
}

/// Everything the driver borrows from the run.
pub struct Work<'a> {
    pub state: &'a WorkerState,
    pub msg: &'a TicketMessage,
    pub github: &'a GitHubClient,
    pub run_id: &'a str,
    pub branch: &'a str,
    pub deadline: Option<Instant>,
    pub rules: &'a [String],
    pub repo_instructions: &'a str,
    pub provider: &'a crate::agent::provider::ModelProvider,
    pub file_cache: &'a crate::passes::FileCache,
}

/// The steps a probe runs: the repository's install and type-check/build,
/// its CI's lint and format commands, and its unit tests (all of them: a
/// dependency touches every caller).
pub(crate) async fn probe_steps(github: &GitHubClient, msg: &TicketMessage) -> Vec<Step> {
    let (o, n, base) = (&msg.repo_owner, &msg.repo_name, &msg.base_branch);
    let ci = checks::detect_ci(github, o, n, base).await;
    let mut steps = vec![];
    if let Some(cmd) = super::detect_check_commands(github, o, n, base).await {
        steps.push(Step {
            kinds: checks::kinds_of(&cmd),
            cmd,
            scope: None,
        });
    }
    steps.extend(checks::mirrored_steps(&ci));
    if let Some(test) = super::detect_ci_test(github, o, n, base).await {
        steps.push(Step {
            kinds: vec![checks::CheckKind::Tests],
            cmd: test,
            scope: Some("the repository's unit tests".into()),
        });
    }
    steps
}

/// Drives the run until it is done or this step's time is up.
pub async fn drive(
    w: &Work<'_>,
    mut run: DepRun,
    usage: &mut TokenUsage,
) -> Result<Driven, Box<dyn std::error::Error + Send + Sync>> {
    let state = w.state;
    let msg = w.msg;
    let (owner, name) = (&msg.repo_owner, &msg.repo_name);
    let tree: Vec<String> = w
        .github
        .get_tree(owner, name, &msg.base_branch)
        .await
        .map(|t| {
            t.into_iter()
                .filter(|e| e.entry_type == "blob")
                .map(|e| e.path)
                .collect()
        })
        .unwrap_or_default();
    let tools = common::upgrade_notes::lock_tools(&tree);
    let gens = super::generators::lockfile_generators(&tree);
    let repo = super::generators::RepoFiles {
        tree: tree.clone(),
        files: HashMap::new(),
    };
    let mut hosts: Vec<String> = gens
        .iter()
        .flat_map(|g| super::generators::generator_hosts(g, &repo))
        .collect();
    hosts.sort();
    hosts.dedup();
    let sandbox = crate::clients::sandbox::SandboxClient::new(
        &state.codebuild,
        &state.s3,
        &state.logs,
        &state.config.sandbox_bucket_name,
        &state.config.sandbox_project_name,
    )
    .with_egress(crate::agent::policy::sandbox_egress(state, &msg.team_id).await)
    .with_generator_hosts(hosts, gens.iter().map(|g| g.command.clone()).collect());
    let probe = Probe {
        msg,
        github: w.github,
        run_id: w.run_id,
        sandbox,
        steps: probe_steps(w.github, msg).await,
        locks: tools
            .iter()
            .map(|t| (t.lockfile.clone(), t.command.clone()))
            .collect(),
        node: super::detect_node_version(w.github, owner, name, &msg.base_branch).await,
        deadline: w.deadline,
        attempt: std::sync::atomic::AtomicUsize::new(run.probes as usize),
        families: run.families.clone(),
    };
    // The team's agent permissions hold the sandbox commands like any other.
    let gate = crate::agent::policy::gate(
        state,
        w.provider.scope.ctx(common::agent_policy::Agent::Coder),
        w.deadline,
    );
    let sample = df::probe_command(&[], &probe.locks, &checks::script(&probe.steps));
    if !matches!(
        gate.check_command(&sample, "Verify security bumps in the sandbox")
            .await,
        common::agent_policy::Outcome::Run { .. }
    ) {
        run.unverified = true;
    }
    if run.base_failing.is_none() && run.applied.is_empty() {
        clean_slate(w).await;
    }
    let mut files: HashMap<usize, Vec<(String, String)>> = HashMap::new();
    loop {
        crate::passes::check_cancelled(state, &msg.team_id, w.run_id).await?;
        let action = run.next_action();
        if !matches!(action, Action::Done | Action::Commit(_))
            && !probe.sandbox.has_time(w.deadline)
        {
            save(state, msg, w.run_id, &run).await;
            return Ok(Driven::Handoff(Box::new(run)));
        }
        match action {
            Action::Base => {
                let base_sha = run.head.clone();
                let p = if run.unverified {
                    // Not permitted to run checks: only the apply and the
                    // lockfile run, and CI is the check.
                    ProbeResult::default()
                } else {
                    probe.run(&base_sha, &[], true).await.result
                };
                run.on_base(&p);
                let note = match &run.base_failing {
                    Some(_) if run.unverified => {
                        "Dependency fix: the sandbox couldn't run the repository's checks on the base branch; same-line fixes go with CI as the check, majors wait".to_string()
                    }
                    Some(f) if !f.is_empty() => format!(
                        "Dependency fix: on the base branch {} already fail{}; noted, never blamed on a bump",
                        f.join(", "),
                        if f.len() == 1 { "s" } else { "" }
                    ),
                    _ => "Dependency fix: the repository's checks pass on the base branch".into(),
                };
                super::add_progress_note(state, &msg.team_id, w.run_id, &note).await;
            }
            Action::Probe(groups) => {
                let head = run.head.clone();
                // Each group's bump set as probed: what it wrote is found
                // again by it once the probes have moved the groups.
                let probed: Vec<Vec<usize>> = groups
                    .iter()
                    .map(|&g| {
                        let mut ids = run.groups[g].bumps.clone();
                        ids.sort();
                        ids
                    })
                    .collect();
                let sets: Vec<Vec<&df::PlanBump>> = groups
                    .iter()
                    .map(|&g| {
                        run.groups[g]
                            .bumps
                            .iter()
                            .map(|&i| &run.plan.bumps[i])
                            .collect()
                    })
                    .collect();
                let checks_on = !run.unverified;
                let outs = futures::future::join_all(
                    sets.iter().map(|set| probe.run(&head, set, checks_on)),
                )
                .await;
                let mut results = vec![];
                let mut keyed: HashMap<Vec<usize>, Vec<(String, String)>> = HashMap::new();
                // A parent raised instead of an override that did not move
                // its package: the override is written after all, and the
                // set is probed again with it.
                let mut unmoved: Vec<String> = vec![];
                for ((g, ids), o) in groups.iter().zip(probed).zip(outs) {
                    if !o.parent_unmoved.is_empty() {
                        unmoved.extend(o.parent_unmoved);
                        continue;
                    }
                    keyed.insert(ids, o.files);
                    results.push((*g, o.result));
                }
                for b in run.plan.bumps.iter_mut() {
                    if b.via_parent.is_some() && unmoved.contains(&b.package) {
                        let parent = b.via_parent.take().map(|p| p.package).unwrap_or_default();
                        super::add_progress_note(
                            state,
                            &msg.team_id,
                            w.run_id,
                            &format!(
                                "Dependency fix: raising `{parent}` did not move `{}` off its vulnerable version, so it is fixed by an override scoped to its version line",
                                b.package
                            ),
                        )
                        .await;
                    }
                }
                run.on_probes(&results);
                info!(
                    run_id = w.run_id,
                    groups = results.len(),
                    probes = run.probes,
                    "dep fix: probes read"
                );
                files.clear();
                for (i, g) in run.groups.iter().enumerate() {
                    let mut ids = g.bumps.clone();
                    ids.sort();
                    if let Some(f) = keyed.remove(&ids) {
                        files.insert(i, f);
                    }
                }
            }
            Action::Commit(g) => {
                let set: Vec<String> = run.groups[g]
                    .bumps
                    .iter()
                    .map(|&i| run.plan.bumps[i].package.clone())
                    .collect();
                let captured = files.remove(&g).unwrap_or_default();
                if captured.is_empty() {
                    // Green and nothing to write: the manifests and the
                    // lockfile already say these versions.
                    let head = run.head.clone();
                    run.on_commit(g, &head);
                    continue;
                }
                let ops: Vec<FileOp> = captured
                    .into_iter()
                    .map(|(path, content)| FileOp::Write { path, content })
                    .collect();
                let message = super::conventions::commit_for(
                    state,
                    w.github,
                    msg,
                    &format!(
                        "fix(deps): {} security update{} verified in the sandbox",
                        set.len(),
                        if set.len() == 1 { "" } else { "s" }
                    ),
                )
                .await;
                match w
                    .github
                    .batch_write(owner, name, w.branch, &message, &ops)
                    .await
                {
                    Ok(_) => {
                        let head = w
                            .github
                            .get_ref(owner, name, w.branch)
                            .await
                            .unwrap_or_default();
                        run.on_commit(g, &head);
                        files.clear();
                        super::add_progress_note(
                            state,
                            &msg.team_id,
                            w.run_id,
                            &format!(
                                "Dependency fix: committed {} verified bump(s): {}",
                                set.len(),
                                common::truncate_str(&set.join(", "), 300)
                            ),
                        )
                        .await;
                    }
                    Err(e) => run.on_commit_failed(g, &e.to_string()),
                }
            }
            Action::CodeFix {
                bump,
                failing,
                error,
            } => {
                let before = usage.clone();
                let r = code_fix(w, &probe, &run, bump, &failing, &error, usage).await;
                let tokens = (usage.input_tokens + usage.output_tokens)
                    .saturating_sub(before.input_tokens + before.output_tokens);
                let usd = (usage.cost_usd - before.cost_usd).max(0.0);
                run.on_code_fix(bump, r, tokens, usd);
            }
            Action::ProofPlan => {
                let mut plan = proof_plan(w, &run).await;
                // The requests that go through the call sites of the
                // applied majors, sent to the base branch's build and to
                // the branch's alike.
                if plan.start.is_some() {
                    let planned =
                        super::upgrade_quality::behaviour_plan(state, w.github, msg, &run).await;
                    plan.probes = planned.requests;
                    run.probed = planned.of;
                    run.not_probed = planned.not_probed;
                }
                // The proof's commands are the repository's own: the team's
                // agent permissions hold them like any other.
                let permitted = plan.is_empty()
                    || matches!(
                        gate.check_command(
                            &rp::script(&plan),
                            "Build and start the application in the sandbox"
                        )
                        .await,
                        common::agent_policy::Outcome::Run { .. }
                    );
                run.on_proof_plan(plan);
                if !permitted {
                    run.skip_proof(
                        "the team's agent permissions do not let the sandbox run the repository's build and start commands",
                    );
                }
            }
            Action::Proof { base } => {
                let left = w
                    .deadline
                    .map(|d| d.saturating_duration_since(Instant::now()));
                if proof_waits(left, run.proof.waited) {
                    // Its own step, with a full time window.
                    run.on_proof_wait();
                    save(state, msg, w.run_id, &run).await;
                    return Ok(Driven::Handoff(Box::new(run)));
                }
                let sha = if base {
                    match run.base_sha() {
                        Some(s) => s.to_string(),
                        None => w
                            .github
                            .get_ref(owner, name, &msg.base_branch)
                            .await
                            .unwrap_or_default(),
                    }
                } else {
                    run.head.clone()
                };
                let plan = run.proof.plan.clone().unwrap_or_default();
                let r = probe.prove(&sha, &plan, run.proof.runs as usize).await;
                let note = proof_note(base, &plan, &r);
                run.on_proof(base, r);
                super::add_progress_note(state, &msg.team_id, w.run_id, &note).await;
            }
            Action::ProofTakeOut { why } => {
                // The state moves only once the branch is written.
                let mut next = run.clone();
                let written = match next.take_out_last_commit(&why) {
                    Some(out) => restore_to(w, &out).await.map(|head| (out, head)),
                    None => None,
                };
                match written {
                    Some((out, head)) => {
                        next.on_proof_taken_out(&out, &head);
                        run = next;
                        super::add_progress_note(
                            state,
                            &msg.team_id,
                            w.run_id,
                            &format!(
                                "Dependency fix: the application's build or start fails with the newest verified set and passes on the base branch; took out {} bump(s) and kept the {} set(s) before it. {}",
                                out.lines.len(),
                                run.commits.len(),
                                common::truncate_str(&out.lines.join("; "), 600)
                            ),
                        )
                        .await;
                    }
                    None => run.on_proof_take_out_failed(),
                }
            }
            Action::VerifyPlan => {
                let plan = super::upgrade_quality::verify_plan(w.github, msg, &run, &tree).await;
                // The set's commands are the project's own: the team's
                // agent permissions hold them like any other.
                let all: Vec<usize> = (0..plan.checks.len()).collect();
                let permitted = plan.is_empty()
                    || matches!(
                        gate.check_command(
                            &vq::script(&plan, &all, ""),
                            "Verify the dependency change with the project's own checks"
                        )
                        .await,
                        common::agent_policy::Outcome::Run { .. }
                    );
                run.on_verify_plan(plan);
                if !permitted {
                    run.skip_verify(
                        "the team's agent permissions do not let the sandbox run the project's install and checks",
                    );
                }
            }
            Action::Verify { base } => {
                let plan = run.verify.plan.clone().unwrap_or_default();
                let left = w
                    .deadline
                    .map(|d| d.saturating_duration_since(Instant::now()));
                if verify_waits(&plan, left, run.verify.waited) {
                    // Its own step, with a full time window.
                    run.on_verify_wait();
                    save(state, msg, w.run_id, &run).await;
                    return Ok(Driven::Handoff(Box::new(run)));
                }
                let sha = if base {
                    match run.base_sha() {
                        Some(s) => s.to_string(),
                        None => w
                            .github
                            .get_ref(owner, name, &msg.base_branch)
                            .await
                            .unwrap_or_default(),
                    }
                } else {
                    run.head.clone()
                };
                let (v, listed) = probe.verify(&sha, &plan, run.verify.runs as usize).await;
                let note = verify_note(base, &v);
                if !base && v.ran {
                    run.verify.audit = listed;
                }
                run.on_verify(base, v);
                super::add_progress_note(state, &msg.team_id, w.run_id, &note).await;
            }
            Action::VerifyTakeOut { why } => {
                // The state moves only once the branch is written.
                let mut next = run.clone();
                let written = match next.take_out_last_commit(&why) {
                    Some(out) => restore_to(w, &out).await.map(|head| (out, head)),
                    None => None,
                };
                match written {
                    Some((out, head)) => {
                        next.on_verify_taken_out(&out, &head);
                        run = next;
                        super::add_progress_note(
                            state,
                            &msg.team_id,
                            w.run_id,
                            &format!(
                                "Dependency fix: a check of the verification set fails with the newest verified set and passes on the base branch; took out {} bump(s) and kept the {} set(s) before it. {}",
                                out.lines.len(),
                                run.commits.len(),
                                common::truncate_str(&out.lines.join("; "), 600)
                            ),
                        )
                        .await;
                    }
                    None => run.on_verify_take_out_failed(),
                }
            }
            Action::Done => {
                // What the run found, recorded for the commit it ends on.
                if !run.applied.is_empty() && run.facts_sha != run.head {
                    let at = super::upgrade_quality::At {
                        state,
                        gh: w.github,
                        msg,
                        branch: w.branch,
                    };
                    let facts = super::upgrade_quality::record(&at, &run).await;
                    run.facts_sha = facts.sha;
                }
                save(state, msg, w.run_id, &run).await;
                return Ok(Driven::Done(Box::new(run)));
            }
        }
        save(state, msg, w.run_id, &run).await;
    }
}

/// The run page's line for one build of the verification set.
fn verify_note(base: bool, v: &vq::Verified) -> String {
    let on = if base {
        "on the base branch"
    } else {
        "with the bumps"
    };
    if !v.ran {
        return format!(
            "Dependency fix: the verification set {on} gave no result ({})",
            common::truncate_str(v.tail.lines().next().unwrap_or(""), 200)
        );
    }
    let words: Vec<String> = v
        .outcomes
        .iter()
        .map(|o| {
            format!(
                "{} {}",
                o.kind.name(),
                match &o.status {
                    vq::Status::Failed { .. } if o.kind == vq::Kind::Audit => {
                        "lists advisories".to_string()
                    }
                    vq::Status::Passed => "passes".to_string(),
                    vq::Status::Failed { exit } => format!("fails (exit {exit})"),
                    vq::Status::TimedOut { .. } => "hit its time limit".to_string(),
                    vq::Status::NotRun => "did not run".to_string(),
                }
            )
        })
        .collect();
    format!(
        "Dependency fix: verification set {on}: {}",
        words.join("; ")
    )
}

/// The run page's line for one runtime proof build.
fn proof_note(base: bool, plan: &rp::ProofPlan, r: &rp::ProofResult) -> String {
    let on = if base {
        "on the base branch"
    } else {
        "with the bumps"
    };
    if !r.ran {
        return format!(
            "Dependency fix: the runtime proof {on} gave no result ({})",
            common::truncate_str(r.tail.lines().next().unwrap_or(""), 200)
        );
    }
    let build = match (plan.builds.is_empty(), r.built()) {
        (true, _) => "no build command found".to_string(),
        (false, true) => "the build passes".to_string(),
        (false, false) => "the build fails".to_string(),
    };
    let start = match (&plan.start, r.start.as_str()) {
        (None, _) => "no start command declared".to_string(),
        (Some(_), "up") => format!(
            "the application started and answered {} of {} requested paths with success",
            r.pages.iter().filter(|(_, c)| rp::page_ok(*c)).count(),
            r.pages.len()
        ),
        (Some(_), "skipped") => "the start did not run".to_string(),
        (Some(_), _) => "the application did not start".to_string(),
    };
    format!("Dependency fix: runtime proof {on}: {build}; {start}")
}

/// Puts the branch back to the files of `out.restore_to` as one commit (the
/// commit a runtime proof takes out) and returns the branch's new head.
/// `None` when nothing was written.
async fn restore_to(w: &Work<'_>, out: &df::TakenOut) -> Option<String> {
    let (owner, name) = (&w.msg.repo_owner, &w.msg.repo_name);
    let changed = w
        .github
        .compare_changed_files(owner, name, &out.restore_to, w.branch)
        .await
        .ok()?;
    let mut ops = vec![];
    for path in changed {
        match w
            .github
            .read_file_if_exists(owner, name, &path, &out.restore_to)
            .await
            .ok()?
        {
            Some(content) => ops.push(FileOp::Write { path, content }),
            None => ops.push(FileOp::Delete { path }),
        }
    }
    if ops.is_empty() {
        return None;
    }
    let message = super::conventions::commit_for(
        w.state,
        w.github,
        w.msg,
        &format!(
            "fix(deps): take out {} security update{} the build or start fails with",
            out.lines.len(),
            if out.lines.len() == 1 { "" } else { "s" }
        ),
    )
    .await;
    w.github
        .batch_write(owner, name, w.branch, &message, &ops)
        .await
        .ok()?;
    w.github.get_ref(owner, name, w.branch).await.ok()
}

/// A dependency run starts from the base branch: an earlier attempt's
/// changes on the branch (a migration for majors the rules now defer, a
/// README edit) are put back to the base's files, so only verified sets
/// land on it. The earlier work stays on its own branch.
async fn clean_slate(w: &Work<'_>) {
    let (owner, name) = (&w.msg.repo_owner, &w.msg.repo_name);
    let Ok(changed) = w
        .github
        .compare_changed_files(owner, name, &w.msg.base_branch, w.branch)
        .await
    else {
        return;
    };
    if changed.is_empty() {
        return;
    }
    let mut ops = vec![];
    for path in &changed {
        match w
            .github
            .read_file(owner, name, path, &w.msg.base_branch)
            .await
        {
            Ok(content) => ops.push(FileOp::Write {
                path: path.clone(),
                content,
            }),
            Err(_) => ops.push(FileOp::Delete { path: path.clone() }),
        }
    }
    let message = "Start the dependency fix from the base branch's files";
    match w.github.batch_write(owner, name, w.branch, message, &ops).await {
        Ok(_) => {
            super::add_progress_note(
                w.state,
                &w.msg.team_id,
                w.run_id,
                &format!(
                    "Dependency fix: the branch held {} file(s) changed by an earlier attempt; put back to the base branch's, so only verified bumps land",
                    changed.len()
                ),
            )
            .await
        }
        Err(e) => warn!(run_id = w.run_id, error = %e, "dep fix: the earlier attempt's changes could not be put back"),
    }
}

/// One coder pass at a major the run must fix: the bump is applied on the
/// branch, the coder changes the call sites the failing checks name, and
/// the head is probed. Red, the branch goes back to where it was.
async fn code_fix(
    w: &Work<'_>,
    probe: &Probe<'_>,
    run: &DepRun,
    bump: usize,
    failing: &[String],
    error: &str,
    usage: &mut TokenUsage,
) -> CodeFixed {
    let (owner, name) = (&w.msg.repo_owner, &w.msg.repo_name);
    let b = &run.plan.bumps[bump];
    let before = run.head.clone();
    let applied = probe.run(&before, &[b], false).await;
    if applied.files.is_empty() || !applied.result.applied {
        return CodeFixed::Failed {
            checks: vec![df::LOCKFILE_CHECK.into()],
            error: applied.result.tail,
        };
    }
    let ops: Vec<FileOp> = applied
        .files
        .iter()
        .map(|(path, content)| FileOp::Write {
            path: path.clone(),
            content: content.clone(),
        })
        .collect();
    let message = super::conventions::commit_for(
        w.state,
        w.github,
        w.msg,
        &format!("fix(deps): {} {} to {}", b.package, b.from, b.to),
    )
    .await;
    if let Err(e) = w
        .github
        .batch_write(owner, name, w.branch, &message, &ops)
        .await
    {
        return CodeFixed::Failed {
            checks: vec![],
            error: format!("the bump could not be committed ({e})"),
        };
    }
    let notes = super::security_fixes::dep_notes(w.state, &w.msg.team_id, &w.msg.ticket_id)
        .await
        .into_iter()
        .find(|n| n.package.eq_ignore_ascii_case(&b.package));
    let plan = code_fix_plan(b, failing, error, notes.as_ref());
    super::add_progress_note(
        w.state,
        &w.msg.team_id,
        w.run_id,
        &format!(
            "Dependency fix: `{}` {} to {} breaks {}; the coder changes its call sites (one pass)",
            b.package,
            b.from,
            b.to,
            failing.join(", ")
        ),
    )
    .await;
    let coded = super::implement::run(
        w.state,
        w.msg,
        w.github,
        &plan,
        w.branch,
        w.rules,
        w.repo_instructions,
        None,
        "simple",
        w.provider,
        usage,
        w.file_cache,
        Some(w.run_id),
        w.deadline,
        super::write_guard::RoundFor::Build,
    )
    .await;
    let head = w
        .github
        .get_ref(owner, name, w.branch)
        .await
        .unwrap_or_default();
    let verdict = if coded.is_ok() {
        let p = probe.run(&head, &[], true).await.result;
        let base = run.base_failing.clone().unwrap_or_default();
        let new: Vec<String> = p
            .failing()
            .into_iter()
            .filter(|c| !base.contains(c))
            .collect();
        if p.ran && new.is_empty() {
            return CodeFixed::Green { head };
        }
        CodeFixed::Failed {
            checks: new,
            error: p.tail,
        }
    } else {
        CodeFixed::Failed {
            checks: failing.to_vec(),
            error: coded.err().map(|e| e.to_string()).unwrap_or_default(),
        }
    };
    // Back to where the branch was: every file the attempt changed.
    if let Ok(changed) = w
        .github
        .compare_changed_files(owner, name, &before, w.branch)
        .await
    {
        let mut ops = vec![];
        for path in changed {
            match w.github.read_file(owner, name, &path, &before).await {
                Ok(content) => ops.push(FileOp::Write { path, content }),
                Err(_) => ops.push(FileOp::Delete { path }),
            }
        }
        if !ops.is_empty() {
            let message = super::conventions::commit_for(
                w.state,
                w.github,
                w.msg,
                &format!(
                    "Revert {} {} to {}: its call sites could not be made to pass",
                    b.package, b.from, b.to
                ),
            )
            .await;
            if let Err(e) = w
                .github
                .batch_write(owner, name, w.branch, &message, &ops)
                .await
            {
                warn!(run_id = w.run_id, error = %e, "dep fix: the failed attempt could not be reverted");
            }
        }
    }
    verdict
}

/// The coder's plan for one major's call sites: only the code its failing
/// checks name, never a manifest, a lockfile or a document.
pub fn code_fix_plan(
    b: &df::PlanBump,
    failing: &[String],
    error: &str,
    notes: Option<&common::upgrade_notes::PackageNotes>,
) -> super::plan::PlanResult {
    let mut files: Vec<String> = error_files(error);
    if let Some(n) = notes {
        for c in &n.call_sites {
            if !files.contains(&c.file) {
                files.push(c.file.clone());
            }
        }
    }
    files.retain(|f| !df::commit_path(f) && !df::is_doc_path(f));
    files.truncate(8);
    let tasks: String = if files.is_empty() {
        format!(
            "- [ ] `src`: change the call sites `{}` {} breaks so {} pass\n",
            b.package,
            b.to,
            failing.join(", ")
        )
    } else {
        files
            .iter()
            .map(|f| {
                format!(
                    "- [ ] `{f}`: update its use of `{}` for {} so {} pass\n",
                    b.package,
                    b.to,
                    failing.join(", ")
                )
            })
            .collect()
    };
    let mut design = format!(
        "`{}` is now {} (was {}) on this branch, with its lockfile written. These checks fail with it: {}.\n\nThe failing output:\n```\n{}\n```\n\nChange only the code that calls `{}`. Never edit a manifest, a lockfile or documentation. Call run_checks before you finish.\n",
        b.package,
        b.to,
        b.from,
        failing.join(", "),
        common::tail_str(error, 3000),
        b.package
    );
    if let Some(n) = notes {
        if !n.breaking.is_empty() {
            design.push_str("\nBreaking changes from its release notes:\n");
            for l in n.breaking.iter().take(20) {
                design.push_str(&format!("- {l}\n"));
            }
        }
    }
    super::plan::PlanResult {
        proposal: format!(
            "Make the repository pass its checks with `{}` {}.",
            b.package, b.to
        ),
        design,
        tasks,
        spec: String::new(),
        repo_tasks: vec![],
        fix: None,
    }
}

/// Source files a compiler or test runner's output names (`src/a.ts(3,1)`,
/// `src/a.ts:3:1`).
pub fn error_files(output: &str) -> Vec<String> {
    let mut out: Vec<String> = vec![];
    for word in output.split(|c: char| c.is_whitespace() || c == '\'' || c == '"') {
        let w = word.trim_start_matches("./");
        let path = w
            .split(['(', ':'])
            .next()
            .unwrap_or("")
            .trim_end_matches(['.', ',']);
        let code = [".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".mts", ".vue"]
            .iter()
            .any(|e| path.ends_with(e));
        if code
            && path.contains('/')
            && !path.contains("node_modules")
            && !out.iter().any(|p| p == path)
        {
            out.push(path.to_string());
        }
    }
    out
}

/// After the run: the findings its pull request fixes stay with the ticket;
/// the deferred ones go back to open with their reason (a run's own
/// deferral holds them out of later bundles), and the unit and its report
/// are saved for the pull request's description.
pub async fn settle(state: &WorkerState, msg: &TicketMessage, run: &DepRun) {
    let deferred: Vec<(String, String, bool)> = run
        .all_deferred()
        .iter()
        .flat_map(|d| {
            d.finding_ids
                .iter()
                .map(move |id| (id.clone(), d.line(), d.later))
        })
        .collect();
    // Only the run's own drops hold a finding out of later bundles; the
    // rules' deferrals are read again on every tick.
    let held: Vec<String> = run
        .deferred
        .iter()
        .filter(|d| !d.later)
        .flat_map(|d| d.finding_ids.clone())
        .collect();
    // What stays open in the repository after this pull request, by reason.
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let all = super::security_fixes::load_findings(state, &msg.team_id).await;
    let of_repo: Vec<&common::security_fixes::Finding> = all
        .iter()
        .map(|(f, _)| f)
        .filter(|f| f.repo.eq_ignore_ascii_case(&repo))
        .collect();
    // One count of what stays open: when the run recorded the advisories
    // of this head against the package manager's audit
    // (`upgrade_quality::advisories`), that count is the one the pull
    // request shows. Without it, CoderHelm's own findings are counted, and
    // said to be its own.
    let reconciled = super::security_fixes::dep_facts(state, &msg.team_id, &msg.ticket_id)
        .await
        .is_some_and(|f| f.of_head(&run.head) && f.advisories.is_some());
    let report = if reconciled {
        run.report_md().trim_end().to_string()
    } else {
        format!(
            "{}\n{}",
            run.report_md().trim_end(),
            df::remaining_md(&df::remaining(run, &of_repo))
        )
    };
    super::security_fixes::settle_dep_run(
        state,
        &msg.team_id,
        &msg.ticket_id,
        &run.fixed_ids(),
        &deferred,
        &held,
        &report,
    )
    .await;
}

/// What the implement step reports for a dependency run: the files on the
/// branch, the run's report as its notes, the last probe's checks.
pub async fn implement_result(
    github: &GitHubClient,
    msg: &TicketMessage,
    branch: &str,
    run: &DepRun,
) -> super::implement::ImplementResult {
    let files = github
        .compare_changed_files(&msg.repo_owner, &msg.repo_name, &msg.base_branch, branch)
        .await
        .unwrap_or_default();
    let ci = checks::detect_ci(github, &msg.repo_owner, &msg.repo_name, &msg.base_branch).await;
    super::implement::ImplementResult {
        files_modified: files,
        timed_out: false,
        conversation_log: vec![],
        writes: Default::default(),
        outside_plan: Vec::new(),
        refused: Vec::new(),
        evidence: super::proof::RunEvidence {
            notes: run.report_md(),
            checks: run
                .last_checks
                .iter()
                .map(|(n, ok)| format!("- {n}: {}", if *ok { "PASS" } else { "FAIL" }))
                .collect(),
            ci_gates: checks::kinds_from(&ci),
            ci_commands: ci,
            ..Default::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_proof_build_starts_only_with_a_full_window_or_in_a_fresh_step() {
        let mins = |m: u64| Some(std::time::Duration::from_secs(m * 60));
        // Little time left in this step: the build waits for a fresh one.
        assert!(proof_waits(mins(4), false));
        assert!(proof_waits(mins(8), false));
        // A fresh step has the window.
        assert!(!proof_waits(mins(10), false));
        // It never waits twice for the same build: a step that starts with
        // less than the window runs it with what it has.
        assert!(!proof_waits(mins(8), true));
        assert!(!proof_waits(None, false));
        // The window fits inside a step and inside the sandbox's own wait.
        assert!(PROOF_WINDOW.as_secs() < super::super::LAMBDA_TIMEOUT_SECS - 150);
    }

    #[test]
    fn a_verification_build_runs_what_fits_or_waits_once_for_a_fresh_step() {
        let mins = |m: u64| Some(std::time::Duration::from_secs(m * 60));
        let tree: Vec<String> = ["package.json", "pnpm-lock.yaml", "tsconfig.json"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let plan = vq::discover(
            "pnpm-lock.yaml",
            &tree,
            &[(
                "package.json".to_string(),
                r#"{"scripts":{"build":"tsc -p .","test":"vitest run","lint":"eslint ."}}"#
                    .to_string(),
            )],
            Some(("pnpm audit --prod --json", "pnpm audit --prod --json")),
        );
        // A step never gives a build more than its own window.
        assert_eq!(verify_window(None), 11 * 60);
        assert_eq!(verify_window(mins(30)), 11 * 60);
        assert_eq!(verify_window(mins(5)), 5 * 60 - 90);
        // A full window runs the whole set.
        assert_eq!(
            vq::fit(&plan, verify_window(None)).0.len(),
            plan.checks.len()
        );
        assert!(!verify_waits(&plan, mins(30), false));
        // Little time left: the build waits for a fresh step, once.
        assert!(verify_waits(&plan, mins(5), false));
        assert!(!verify_waits(&plan, mins(5), true));
        // Then it runs what fits, in priority order, and names the rest.
        let (run, left) = vq::fit(&plan, verify_window(mins(5)));
        let kept: Vec<vq::Kind> = run.iter().map(|&i| plan.checks[i].kind).collect();
        assert_eq!(kept, vec![vq::Kind::Install]);
        assert_eq!(left.len(), plan.checks.len() - 1);
        let v = vq::read(&plan, "abc1234", &run, &left, "CODERHELM_VERIFY 0 0\n");
        assert_eq!(
            verify_note(false, &v),
            "Dependency fix: verification set with the bumps: frozen install passes; type check did not run; build did not run; lint did not run; unit tests did not run; production audit did not run"
        );
        // The window fits inside a step.
        assert!(
            VERIFY_WINDOW.as_secs() + VERIFY_MARGIN.as_secs() < super::super::LAMBDA_TIMEOUT_SECS
        );
    }

    #[test]
    fn the_run_page_says_what_a_proof_build_came_to() {
        let plan = rp::discover(
            "pnpm-lock.yaml",
            &[rp::CiBuild {
                cmd: "pnpm build".into(),
                workflow: "ci.yml".into(),
                job: "build".into(),
                on_pull_request: true,
                ..Default::default()
            }],
            &[(
                "package.json".to_string(),
                r#"{"scripts":{"start":"next start"}}"#.to_string(),
            )],
        );
        let green = rp::parse(
            "CODERHELM_PROOF_STEP build 1 0\nCODERHELM_PROOF_PAGE / 200\nCODERHELM_PROOF_PAGE /healthz 404\nCODERHELM_PROOF install=0 start=up port=38411\n",
            &plan,
        );
        assert_eq!(
            proof_note(false, &plan, &green),
            "Dependency fix: runtime proof with the bumps: the build passes; the application started and answered 1 of 2 requested paths with success"
        );
        let red = rp::parse(
            "CODERHELM_PROOF_STEP build 1 1\nCODERHELM_PROOF install=0 start=skipped port=0\n",
            &plan,
        );
        assert_eq!(
            proof_note(true, &plan, &red),
            "Dependency fix: runtime proof on the base branch: the build fails; the start did not run"
        );
        let none = rp::ProofResult::no_result("the sandbox build timed out (CodeBuild TIMED_OUT)");
        assert_eq!(
            proof_note(false, &plan, &none),
            "Dependency fix: the runtime proof with the bumps gave no result (the sandbox build timed out (CodeBuild TIMED_OUT))"
        );
    }

    fn task(path: &str, what: &str) -> super::super::plan_coverage::PlanTask {
        super::super::plan_coverage::PlanTask {
            path: path.into(),
            task: what.into(),
            is_new: false,
            optional: false,
            generated: false,
        }
    }

    #[test]
    fn a_documentation_task_the_ticket_does_not_name_never_holds_a_run() {
        use super::super::plan_coverage::{drop_non_core, next, Coverage, Next, MAX_ROUNDS};
        // The stuck run: eight manifests done, the README table never written.
        let ticket =
            "Fix these security advisories in the dependencies. Change only what the fix needs.";
        let mut cov = Coverage {
            done: vec![
                task("package.json", "raise ranges"),
                task("apps/web/package.json", "raise"),
            ],
            missing: vec![task(
                "README.md",
                "[modify] In \"Dependency Overrides\": add the rows",
            )],
            on_base: vec![],
        };
        // Before: a required task left, so the run continued in step after
        // step until two made no progress and it stopped asking a person.
        assert_eq!(next(&cov, MAX_ROUNDS, false), Next::Unfinished);
        let dropped = drop_non_core(&mut cov, ticket);
        assert_eq!(dropped.len(), 1);
        assert_eq!(next(&cov, MAX_ROUNDS, false), Next::OpenWithOptional);
        // A core task (code, a manifest) still holds the run, with its reason.
        let mut core = Coverage {
            done: vec![],
            missing: vec![
                task("src/server.ts", "update the call"),
                task("package.json", "bump"),
            ],
            on_base: vec![],
        };
        assert!(drop_non_core(&mut core, ticket).is_empty());
        assert_eq!(next(&core, MAX_ROUNDS, false), Next::Unfinished);
        // A README the ticket asks for is core.
        let mut asked = Coverage {
            done: vec![],
            missing: vec![task("README.md", "document the flag")],
            on_base: vec![],
        };
        assert!(drop_non_core(&mut asked, "Add the flag and document it in README.md").is_empty());
    }

    #[test]
    fn the_coder_is_told_only_the_call_sites_never_a_manifest_or_a_document() {
        let b = df::PlanBump {
            package: "gql-kit".into(),
            from: "4.1.0".into(),
            to: "5.0.2".into(),
            line: "4".into(),
            manifest: "pnpm-lock.yaml".into(),
            major: true,
            ..Default::default()
        };
        let err = "src/graph/server.ts(14,3): error TS2345: Argument of type 'Opts'\n\
                   ./src/graph/plugins.ts:9:1 - error TS2339\n\
                   node_modules/gql-kit/dist/index.d.ts:3:1\n\
                   README.md:4 and package.json:2";
        assert_eq!(
            error_files(err),
            vec![
                "src/graph/server.ts".to_string(),
                "src/graph/plugins.ts".to_string()
            ]
        );
        let notes = common::upgrade_notes::PackageNotes {
            package: "gql-kit".into(),
            breaking: vec!["`startServer` takes an options object".into()],
            call_sites: vec![common::upgrade_notes::CallSite {
                api: "startServer".into(),
                file: "src/main.ts".into(),
                line: 3,
                text: "startServer(app, 4000)".into(),
            }],
            ..Default::default()
        };
        let p = code_fix_plan(&b, &["typecheck".into()], err, Some(&notes));
        let paths: Vec<&str> = p
            .tasks
            .lines()
            .filter_map(|l| l.split('`').nth(1))
            .collect();
        assert_eq!(
            paths,
            vec!["src/graph/server.ts", "src/graph/plugins.ts", "src/main.ts"]
        );
        assert!(p
            .design
            .contains("Never edit a manifest, a lockfile or documentation"));
        assert!(p.design.contains("`startServer` takes an options object"));
        assert!(!p.tasks.to_lowercase().contains("readme"));
    }
}
