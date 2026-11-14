//! The worker's side of fix rounds (see `common::run_round`): each round on
//! an open pull request runs inside its own budget, is written on the run
//! record while it works, continues in a fresh invocation when it runs out
//! of time, and when it can't finish leaves the run completed with the pull
//! request open and a plain account of what the round pushed.

use std::time::{Duration, Instant};

use common::run_round::{self, Incomplete, Kind, Round};
use common::run_status::EndedValues;
use tracing::{info, warn};

use super::attr_s;
use crate::clients::github::{FileOp, GitHubClient};
use crate::WorkerState;

/// When a round that starts in an invocation begun at `invocation_start`
/// must wrap up: its own budget, never the run's age.
pub(crate) fn deadline(invocation_start: Instant) -> Instant {
    invocation_start + Duration::from_secs(run_round::BUDGET_SECS)
}

/// Seconds left before that deadline.
pub(crate) fn secs_left(invocation_start: Instant, now: Instant) -> u64 {
    deadline(invocation_start)
        .saturating_duration_since(now)
        .as_secs()
}

/// Whether a round may start now, or should be parked for a fresh
/// invocation with its whole budget.
pub(crate) fn may_start(invocation_start: Instant, now: Instant) -> bool {
    secs_left(invocation_start, now) >= run_round::MIN_START_SECS
}

/// The branch head, for what a round pushes after it ("" when unknown).
pub(crate) async fn branch_head(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    branch: &str,
) -> String {
    github
        .get_ref(owner, repo, branch)
        .await
        .unwrap_or_default()
}

/// Writes the round on the run record. A new round also clears an earlier
/// round's "didn't finish": it is being worked again.
pub(crate) async fn record(state: &WorkerState, team_id: &str, run_id: &str, round: &Round) {
    let first = round.attempt <= 1;
    let expr = if first {
        "SET #round = :r, updated_at = :t REMOVE #inc"
    } else {
        "SET #round = :r, updated_at = :t"
    };
    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(expr)
        .condition_expression(common::run_status::not_ended("#s"))
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#round", run_round::FIELD)
        .expression_attribute_values(":r", round.to_attr())
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .ended_values();
    if first {
        req = req.expression_attribute_names("#inc", run_round::INCOMPLETE_FIELD);
    }
    if let Err(e) = req.send().await {
        if !common::run_fail::is_condition_failed(&e) {
            warn!(run_id, error = %e, "Could not record the fix round");
        }
    }
}

/// Removes the round from the run record: it finished.
pub(crate) async fn clear(state: &WorkerState, team_id: &str, run_id: &str) {
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("REMOVE #round")
        .expression_attribute_names("#round", run_round::FIELD)
        .send()
        .await;
}

/// What the round pushed since it started, read from the branch.
async fn pushed(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base_sha: &str,
    branch: &str,
) -> (Vec<run_round::Pushed>, Vec<String>) {
    if base_sha.is_empty() {
        return (Vec::new(), Vec::new());
    }
    match github.get_diff(owner, repo, base_sha, branch).await {
        Ok(compare) => run_round::pushed_from_compare(&compare),
        // No WIP branch: the round saved nothing.
        Err(e) if crate::clients::github::is_not_found(&*e) => (Vec::new(), Vec::new()),
        Err(e) => {
            warn!(error = %e, "Could not read what the round pushed");
            (Vec::new(), Vec::new())
        }
    }
}

/// Where a round's pull request is.
pub(crate) struct Pr<'a> {
    pub github: &'a GitHubClient,
    pub owner: &'a str,
    pub repo: &'a str,
    pub branch: &'a str,
}

/// Leaves the run completed with its pull request open, saying the round
/// didn't finish, why, and the commits it pushed. Never a failed run: the
/// pull request is there and Resume runs the round again.
pub(crate) async fn finish_incomplete(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    pr: &Pr<'_>,
    round: &Round,
    reason: &str,
) -> Incomplete {
    let now = chrono::Utc::now().to_rfc3339();
    let mut inc = Incomplete::of(round, reason, &now);
    // What it saved is on its WIP branch; the pull request is untouched.
    let wip = if round.wip_branch.is_empty() {
        pr.branch
    } else {
        round.wip_branch.as_str()
    };
    let (commits, files) = pushed(pr.github, pr.owner, pr.repo, &round.base_sha, wip).await;
    inc.pushed = commits;
    inc.files = files;
    // The caller completes the run (with its usage) and says so.
    write_incomplete(state, team_id, run_id, &inc).await;
    super::add_progress_note(
        state,
        team_id,
        run_id,
        &format!(
            "{} {}",
            inc.status_line(),
            run_round::pushed_note(&inc.pushed, &inc.files, wip)
        ),
    )
    .await;
    warn!(
        run_id,
        reason, "Fix round didn't finish; the pull request stays open"
    );
    inc
}

/// A round whose work is done but held from landing by CoderHelm's checks:
/// on the run record (`round_incomplete`, held), with its WIP branch, where
/// it started and what it was asked, so a mention on the pull request or
/// Resume picks it up first (common::held_work). The caller completes the
/// run as usual; the record stays until the work lands.
pub(crate) async fn hold(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    round: &Round,
    reason: &str,
) {
    let now = chrono::Utc::now().to_rfc3339();
    let inc = Incomplete::held(round, reason, &now);
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET #inc = :inc, updated_at = :t")
        .condition_expression(common::run_status::not_ended("#s"))
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#inc", run_round::INCOMPLETE_FIELD)
        .expression_attribute_values(":inc", inc.to_attr())
        .expression_attribute_values(":t", attr_s(&now))
        .ended_values()
        .send()
        .await;
    if let Err(e) = res {
        if !common::run_fail::is_condition_failed(&e) {
            warn!(run_id, error = %e, "Could not record the held round");
        }
    }
}

/// The write: completed, with the round that didn't finish, never over a
/// run a person ended.
async fn write_incomplete(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    inc: &Incomplete,
) -> bool {
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, #inc = :inc, current_pass = :cp, updated_at = :t \
             REMOVE #round, error_message, #err",
        )
        .condition_expression(common::run_status::not_ended("#s"))
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#inc", run_round::INCOMPLETE_FIELD)
        .expression_attribute_names("#round", run_round::FIELD)
        .expression_attribute_names("#err", "error")
        .expression_attribute_values(":s", attr_s(common::run_status::COMPLETED))
        .expression_attribute_values(
            ":sri",
            attr_s(&format!("{}#{run_id}", common::run_status::COMPLETED)),
        )
        .expression_attribute_values(":inc", inc.to_attr())
        .expression_attribute_values(":cp", attr_s("done"))
        .expression_attribute_values(":t", attr_s(&inc.at))
        .ended_values()
        .send()
        .await;
    match res {
        Ok(_) => true,
        Err(e) if common::run_fail::is_condition_failed(&e) => {
            info!(run_id, "Run was ended by a person — leaving it");
            false
        }
        Err(e) => {
            warn!(run_id, error = %e, "Could not record the unfinished round");
            false
        }
    }
}

/// Planned-work rounds stopped without progress: the run keeps its status
/// (`needs_input`, set by the caller) and says which round stopped and why;
/// the round leaves the record.
pub(crate) async fn stopped(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    round: &Round,
    reason: &str,
) {
    let now = chrono::Utc::now().to_rfc3339();
    let inc = Incomplete::of(round, reason, &now);
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET #inc = :inc, updated_at = :t REMOVE #round")
        .condition_expression(common::run_status::not_ended("#s"))
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#inc", run_round::INCOMPLETE_FIELD)
        .expression_attribute_names("#round", run_round::FIELD)
        .expression_attribute_values(":inc", inc.to_attr())
        .expression_attribute_values(":t", attr_s(&now))
        .ended_values()
        .send()
        .await;
    if let Err(e) = res {
        if !common::run_fail::is_condition_failed(&e) {
            warn!(run_id, error = %e, "Could not record why the planned work stopped");
        }
    }
}

/// After a round's worker failed outright (an error the pass returned):
/// when the run has an open pull request, it is left completed with the
/// round named as not finished instead of failed. Returns false when the
/// run has no pull request (a real failure, for the caller to record) or
/// was ended already.
pub(crate) async fn end_unfinished(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    kind: Kind,
    error: &str,
    only_running: bool,
) -> bool {
    let item = match state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .consistent_read(true)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .send()
        .await
    {
        Ok(r) => r.item.unwrap_or_default(),
        Err(_) => return false,
    };
    let num = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let status = item
        .get("status")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    if num("pr_number") == 0
        || (only_running && status != common::run_status::RUNNING)
        || matches!(
            status.as_str(),
            "merged" | "cancelled" | "closed" | "retried" | "archived" | "failed"
        )
    {
        return false;
    }
    let round = Round::from_item(&item)
        .unwrap_or_else(|| Round::start(kind, 0, "", "", true, chrono::Utc::now()));
    let now = chrono::Utc::now().to_rfc3339();
    let inc = Incomplete::of(&round, &run_round::errored(round.kind, error), &now);
    if write_incomplete(state, team_id, run_id, &inc).await {
        super::webhooks::emit_run(state, team_id, run_id, "completed").await;
    }
    super::add_progress_note(state, team_id, run_id, &inc.status_line()).await;
    true
}

/// The round's WIP branch, ready to write to: the one it already has when
/// it continues (`reuse`), else a fresh one at the pull request's head.
/// Returns its name.
pub(crate) async fn open_wip(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    pr_branch: &str,
    reuse: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if !reuse.is_empty() && github.get_ref(owner, repo, reuse).await.is_ok() {
        return Ok(reuse.to_string());
    }
    let wip = run_round::wip_branch(pr_branch);
    // A leftover WIP branch has no pull request, so this resets it to the
    // pull request's head.
    github.create_branch(owner, repo, &wip, pr_branch).await?;
    Ok(wip)
}

/// One file the round changed, from a compare of its start and its WIP head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Changed {
    pub path: String,
    pub removed: bool,
    /// The old path of a renamed file.
    pub previous: Option<String>,
}

/// The files a compare (`base...wip`) says the round changed.
pub(crate) fn changed_from_compare(compare: &serde_json::Value) -> Vec<Changed> {
    compare["files"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|f| {
                    let path = f["filename"].as_str()?.to_string();
                    let status = f["status"].as_str().unwrap_or("");
                    Some(Changed {
                        path,
                        removed: status == "removed",
                        previous: (status == "renamed")
                            .then(|| f["previous_filename"].as_str().map(String::from))
                            .flatten(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Files the pull request changed since the round started (someone else's
/// push) that the round changed too: landing would overwrite them.
pub(crate) fn overlap(ours: &[Changed], theirs: &[String]) -> Vec<String> {
    ours.iter()
        .flat_map(|c| std::iter::once(&c.path).chain(c.previous.iter()))
        .filter(|p| theirs.contains(p))
        .cloned()
        .collect()
}

/// How landing a round went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Landed {
    /// One commit on the pull request carries the round's changes.
    Pushed { files: usize },
    /// The round changed nothing.
    Nothing,
    /// Not landed (the WIP branch is kept): why.
    Refused(String),
}

/// What stopped a fix round from inside. The class decides the way around
/// it ([`way_around`]); none of them ends the round or the run by itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fault {
    /// The round's WIP branch isn't there (never made, or already cleaned
    /// up): the round saved nothing.
    WipMissing,
    /// The round's changes couldn't be read (a compare GitHub refused).
    Unreadable,
    /// GitHub refused the one commit (the head moved, a 409 or 422).
    PushRejected,
    /// The pull request changed the same files while the round worked.
    Clash,
    /// GitHub is refusing for a while (its limit, a 5xx, no answer).
    GitHubBusy,
    /// The sandbox or one of its tools failed.
    Sandbox,
    /// Anything else.
    Other,
}

/// The class of an error met inside a fix round, from its text.
pub(crate) fn fault_of(error: &str) -> Fault {
    let e = error.to_ascii_lowercase();
    let has = |needles: &[&str]| needles.iter().any(|n| e.contains(n));
    if has(&["changed the same files meanwhile"]) {
        Fault::Clash
    } else if has(&["no wip branch"])
        || (e.contains("404") && has(&["coderhelm-wip/", "wip branch", "/git/ref"]))
    {
        Fault::WipMissing
    } else if has(&[
        "(409",
        "(422",
        "not a fast forward",
        "not a fast-forward",
        "reference update failed",
        "couldn't be pushed",
        "update is not a fast",
    ]) {
        Fault::PushRejected
    } else if has(&[
        "rate limit",
        // The pause as the run words it (`common::github_wait::waiting_text`):
        // read as anything else, the round threw its work away and spent an
        // attempt on GitHub's limit.
        "hourly limit",
        "waiting for github",
        "(500",
        "(502",
        "(503",
        "(504",
        "timed out",
        "timeout",
        "connection reset",
        "error sending request",
    ]) {
        Fault::GitHubBusy
    } else if has(&["sandbox", "codebuild", "snapshot"]) {
        Fault::Sandbox
    } else if has(&["couldn't be read", "/compare/", "(404"]) {
        Fault::Unreadable
    } else {
        Fault::Other
    }
}

/// The way around a fault, as the round takes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WayAround {
    /// Make the WIP branch again at the pull request's head and go on: the
    /// round saved nothing, so nothing is lost.
    ReopenWip,
    /// Read the round's changes from the two trees instead of a compare.
    ReadFromTrees,
    /// Read the pull request's head again and push the one commit again.
    PushAgain,
    /// Start the round's work again from the pull request's new head.
    RestartFromHead,
    /// Wait for GitHub and go on as the same run.
    WaitForGitHub,
    /// Go on without the sandbox: CI on the pull request is the check.
    WithoutSandbox,
    /// Run the round again from a clean WIP branch.
    RetryClean,
}

pub(crate) fn way_around(f: Fault) -> WayAround {
    match f {
        Fault::WipMissing => WayAround::ReopenWip,
        Fault::Unreadable => WayAround::ReadFromTrees,
        Fault::PushRejected => WayAround::PushAgain,
        Fault::Clash => WayAround::RestartFromHead,
        Fault::GitHubBusy => WayAround::WaitForGitHub,
        Fault::Sandbox => WayAround::WithoutSandbox,
        Fault::Other => WayAround::RetryClean,
    }
}

/// How many times a round whose attempts all met a fault is run again in a
/// fresh invocation before the supervisor's ending takes over.
pub(crate) const MAX_REDRIVES: u32 = 1;

/// What a round does after an attempt met a fault it couldn't get around
/// inside the attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AfterFault {
    /// Another attempt in this invocation, from a clean WIP branch.
    RetryClean,
    /// A fresh invocation runs the round again from the start.
    Redrive,
    /// Every way around is spent: the supervisor's ending (take out what
    /// broke the checks, else end with the exact cause).
    Supervisor,
}

/// The catch-all: an attempt that met a fault is followed by a clean one
/// while the round has attempts and time, then by one fresh invocation,
/// and only then by the supervisor's ending. Never by a dead round.
pub(crate) fn after_fault(
    attempt: usize,
    max_attempts: usize,
    has_time: bool,
    redrives: u32,
) -> AfterFault {
    if attempt < max_attempts && has_time {
        AfterFault::RetryClean
    } else if redrives < MAX_REDRIVES {
        AfterFault::Redrive
    } else {
        AfterFault::Supervisor
    }
}

/// [`after_fault`] for a round that makes one attempt per invocation (a
/// review fix, a person's feedback, a security fix, planned work): it runs
/// again in a fresh invocation while it may continue, and only then is
/// left as the round that didn't finish.
pub(crate) fn after_fault_parked(may_continue: bool) -> AfterFault {
    after_fault(1, 1, false, if may_continue { 0 } else { MAX_REDRIVES })
}

/// The wait a round takes when GitHub refuses for a while, from the fault's
/// text and what this process knows of the installation's limit (`known`:
/// the cause and the seconds left). `None` for any other fault, and for a
/// GitHub fault with no limit or outage to wait out (a lone timeout): those
/// take the fault's usual way.
///
/// A round that waits is the same round afterwards: its work branch and
/// its attempt are kept (`resume::wait_round_for_github`). It used to be
/// read as "an error": the work branch was deleted, an attempt was spent,
/// and the next try started five seconds later, inside the same limit.
pub(crate) fn github_wait_of(
    error: &str,
    known: Option<(common::github_wait::Cause, u64)>,
) -> Option<(common::github_wait::Cause, u64)> {
    use common::github_wait::Cause;
    if fault_of(error) != Fault::GitHubBusy {
        return None;
    }
    if known.is_some() {
        return known;
    }
    let e = error.to_ascii_lowercase();
    let wait = crate::clients::github::OUTAGE_WAIT_SECS;
    if e.contains("rate limit") || e.contains("hourly limit") || e.contains("(429") {
        Some((Cause::RateLimit, wait))
    } else if e.contains("waiting for github")
        || ["(500", "(502", "(503", "(504"]
            .iter()
            .any(|c| e.contains(c))
    {
        Some((Cause::Outage, wait))
    } else {
        None
    }
}

/// Whether a round that met GitHub's limit may wait for it again: each run
/// waits at most `common::github_wait::MAX_WAITS` times.
pub(crate) fn may_wait_for_github(waits: u32) -> bool {
    waits < common::github_wait::MAX_WAITS
}

/// Whether the round's work branch is kept for the next try. Its work is
/// there and only the landing failed when GitHub refused the push, the
/// compare or didn't answer; any other fault starts again from the pull
/// request's head (a clash must: the work was written on an older head).
pub(crate) fn keeps_work(f: Fault) -> bool {
    matches!(
        f,
        Fault::PushRejected | Fault::Unreadable | Fault::GitHubBusy
    )
}

/// What a round does about a fault it met: its class, what comes next and
/// the line for the run page. The one decision every kind of round takes
/// (`resume`'s CI fix loop, `run_fix_round`, the feedback pass).
pub(crate) struct FaultPlan {
    pub class: Fault,
    pub next: AfterFault,
    pub note: String,
    /// The work branch goes, and the next try starts from the pull
    /// request's head.
    pub clean: bool,
}

pub(crate) fn plan_for_fault(kind: Kind, error: &str, next: AfterFault) -> FaultPlan {
    let class = fault_of(error);
    FaultPlan {
        class,
        note: fault_note(kind, class, error, &next),
        clean: !keeps_work(class) || next == AfterFault::Supervisor,
        next,
    }
}

/// A clean state for a round's next try: its work branch goes, and the
/// round starts from the pull request's head as it is now.
pub(crate) async fn start_clean(pr: &Pr<'_>, round: &mut Round) {
    if !round.wip_branch.is_empty() && round.wip_branch != pr.branch {
        let _ = pr
            .github
            .delete_branch(pr.owner, pr.repo, &round.wip_branch)
            .await;
    }
    round.wip_branch = String::new();
    round.base_sha = branch_head(pr.github, pr.owner, pr.repo, pr.branch).await;
}

/// What a fault and its way around read like on the run page.
pub(crate) fn fault_note(kind: Kind, f: Fault, error: &str, next: &AfterFault) -> String {
    let what = match f {
        Fault::WipMissing => "its work branch wasn't there, so it had saved nothing",
        Fault::Unreadable => "its changes couldn't be read from GitHub",
        Fault::PushRejected => "GitHub refused its commit",
        Fault::Clash => "the pull request changed the same files while it worked",
        Fault::GitHubBusy => "GitHub didn't answer",
        Fault::Sandbox => "the sandbox gave no result",
        Fault::Other => "it met an error",
    };
    let then = match next {
        AfterFault::RetryClean => "CoderHelm runs it again now from the pull request's head",
        AfterFault::Redrive => "CoderHelm runs it again in a fresh step",
        AfterFault::Supervisor => "its attempts are used",
    };
    format!(
        "The {} didn't land: {what} ({}). {then}.",
        kind.label(),
        common::truncate_str(error.trim(), 200)
    )
}

/// The files that differ between two trees (`before`, `after`: path and
/// blob sha): the round's changes read without a compare.
pub(crate) fn changed_from_trees(
    before: &[(String, String)],
    after: &[(String, String)],
) -> Vec<Changed> {
    let was: std::collections::HashMap<&str, &str> = before
        .iter()
        .map(|(p, s)| (p.as_str(), s.as_str()))
        .collect();
    let now: std::collections::HashMap<&str, &str> = after
        .iter()
        .map(|(p, s)| (p.as_str(), s.as_str()))
        .collect();
    let mut out: Vec<Changed> = after
        .iter()
        .filter(|(p, s)| was.get(p.as_str()) != Some(&s.as_str()))
        .map(|(p, _)| Changed {
            path: p.clone(),
            removed: false,
            previous: None,
        })
        .collect();
    out.extend(
        before
            .iter()
            .filter(|(p, _)| !now.contains_key(p.as_str()))
            .map(|(p, _)| Changed {
                path: p.clone(),
                removed: true,
                previous: None,
            }),
    );
    out.sort_by(|a, b| a.path.cmp(&b.path));
    out
}

async fn blobs(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error + Send + Sync>> {
    Ok(github
        .get_tree(owner, repo, git_ref)
        .await?
        .into_iter()
        .filter(|e| e.entry_type == "blob")
        .map(|e| (e.path, e.sha))
        .collect())
}

/// What a round changed on its WIP branch since `base_sha`. `Ok(None)`
/// when the WIP branch isn't there (the round saved nothing). A compare
/// GitHub refuses is read another way: the two trees, file by file.
async fn round_changes(
    pr: &Pr<'_>,
    wip: &str,
    base_sha: &str,
) -> Result<Option<Vec<Changed>>, String> {
    let (github, owner, repo) = (pr.github, pr.owner, pr.repo);
    let first = match github.get_diff(owner, repo, base_sha, wip).await {
        Ok(c) => return Ok(Some(changed_from_compare(&c))),
        Err(e) => e,
    };
    if let Err(e) = github.get_ref(owner, repo, wip).await {
        if crate::clients::github::is_not_found(&*e) {
            info!(wip, "The round's WIP branch isn't there: it saved nothing");
            return Ok(None);
        }
    }
    warn!(wip, error = %first, "Compare refused; reading the round's changes from the trees");
    match (
        blobs(github, owner, repo, base_sha).await,
        blobs(github, owner, repo, wip).await,
    ) {
        (Ok(before), Ok(after)) => Ok(Some(changed_from_trees(&before, &after))),
        (Err(e), _) | (_, Err(e)) => Err(format!(
            "its changes couldn't be read ({first}; reading the trees: {e})"
        )),
    }
}

/// Lands a finished round on the pull request: everything it changed on its
/// WIP branch since `base_sha`, as ONE commit with `message`, in one push.
/// Refuses (keeping the WIP branch) when the pull request changed the same
/// files meanwhile. The WIP branch is deleted once landed.
///
/// A WIP branch that isn't there is a round that saved nothing
/// ([`Landed::Nothing`]), never a refusal. A compare GitHub refuses is read
/// from the trees, and a commit GitHub refuses is pushed once more on the
/// pull request's head as it is then.
pub(crate) async fn land(pr: &Pr<'_>, wip: &str, base_sha: &str, message: &str) -> Landed {
    let (github, owner, repo) = (pr.github, pr.owner, pr.repo);
    if wip.is_empty() || wip == pr.branch || base_sha.is_empty() {
        return Landed::Refused("the round has no WIP branch to land".into());
    }
    let ours = match round_changes(pr, wip, base_sha).await {
        Ok(Some(c)) => c,
        Ok(None) => return Landed::Nothing,
        Err(why) => return Landed::Refused(why),
    };
    if ours.is_empty() {
        let _ = github.delete_branch(owner, repo, wip).await;
        return Landed::Nothing;
    }
    match github.get_ref(owner, repo, pr.branch).await {
        Ok(head) if head != base_sha => {
            let theirs: Vec<String> = github
                .get_diff(owner, repo, base_sha, &head)
                .await
                .map(|c| {
                    changed_from_compare(&c)
                        .into_iter()
                        .flat_map(|c| std::iter::once(c.path).chain(c.previous))
                        .collect()
                })
                .unwrap_or_default();
            let clash = overlap(&ours, &theirs);
            if !clash.is_empty() {
                return Landed::Refused(format!(
                    "the pull request changed the same files meanwhile ({})",
                    clash.join(", ")
                ));
            }
        }
        Ok(_) => {}
        Err(e) => {
            return Landed::Refused(format!("the pull request's head couldn't be read ({e})"))
        }
    }
    let mut ops = Vec::new();
    for c in &ours {
        if let Some(prev) = &c.previous {
            ops.push(FileOp::Delete { path: prev.clone() });
        }
        if c.removed {
            ops.push(FileOp::Delete {
                path: c.path.clone(),
            });
            continue;
        }
        match github.read_file_live(owner, repo, &c.path, wip).await {
            Ok(content) => ops.push(FileOp::Write {
                path: c.path.clone(),
                content,
            }),
            Err(e) => {
                return Landed::Refused(format!(
                    "`{}` couldn't be read from the WIP branch ({e})",
                    c.path
                ))
            }
        }
    }
    // A refused commit is pushed once more: the write reads the pull
    // request's head again, so a head that moved under the first try (the
    // files don't clash, checked above) takes the second.
    let mut pushed = github
        .batch_write(owner, repo, pr.branch, message, &ops)
        .await;
    if let Err(e) = &pushed {
        warn!(wip, error = %e, "The round's commit was refused; pushing once more on the current head");
        tokio::time::sleep(Duration::from_secs(2)).await;
        pushed = github
            .batch_write(owner, repo, pr.branch, message, &ops)
            .await;
    }
    match pushed {
        Ok(_) => {
            let _ = github.delete_branch(owner, repo, wip).await;
            Landed::Pushed { files: ours.len() }
        }
        Err(e) => Landed::Refused(format!("the one commit couldn't be pushed ({e})")),
    }
}

/// The one commit message a round lands with, in the team's commit style.
pub(crate) async fn round_message(
    state: &WorkerState,
    team_id: &str,
    ticket: &crate::models::TicketMessage,
    round: &Round,
) -> String {
    let summary = match round.kind {
        Kind::CiFix => "Fix the failing checks",
        Kind::ReviewFix => "Address the review",
        Kind::Feedback => "Address the review comments",
        Kind::SecurityFix => "Fix the security issues the check found",
        Kind::Conflict => "Merge the base branch and resolve the conflicts",
        Kind::PlannedWork => "Finish the planned work still not done",
    };
    let conventions = super::load_git_conventions(state, team_id, &ticket.ticket_id).await;
    conventions.commit_message(&super::ticket_tokens(ticket), summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// GitHub's hourly limit, as every place words it, is GitHub being
    /// busy: the round's work is kept, and the round waits for the reset
    /// instead of spending an attempt.
    #[test]
    fn githubs_limit_is_a_wait_that_keeps_the_rounds_work() {
        use common::github_wait::{waiting_text, Cause};
        let paused = waiting_text(Cause::RateLimit, 1_793_999_100);
        let raw = "HTTP status client error (403 Forbidden) for url \
                   (https://api.github.com/repos/acme/site/git/ref/heads/coderhelm/gh-41): API \
                   rate limit exceeded for installation ID 41.";
        for text in [paused.as_str(), raw] {
            assert_eq!(fault_of(text), Fault::GitHubBusy, "{text}");
            assert!(keeps_work(fault_of(text)));
            let plan = plan_for_fault(Kind::PlannedWork, text, after_fault_parked(true));
            assert!(!plan.clean, "the work branch is kept: {text}");
            assert!(!plan.note.contains("it met an error"), "{}", plan.note);
        }
        // The reset this process knows is the wait; without one, a minute.
        assert_eq!(
            github_wait_of(&paused, Some((Cause::RateLimit, 2_400))),
            Some((Cause::RateLimit, 2_400))
        );
        assert_eq!(
            github_wait_of(raw, None),
            Some((Cause::RateLimit, crate::clients::github::OUTAGE_WAIT_SECS))
        );
        assert_eq!(
            github_wait_of(&waiting_text(Cause::Outage, 1_793_999_100), None),
            Some((Cause::Outage, crate::clients::github::OUTAGE_WAIT_SECS))
        );
        // Any other fault is no wait, whatever is known of GitHub; a lone
        // timeout has nothing to wait out.
        let known = Some((Cause::RateLimit, 2_400));
        assert_eq!(github_wait_of("the model returned no text", known), None);
        assert_eq!(
            github_wait_of("(409 Conflict) reference update failed", known),
            None
        );
        assert_eq!(github_wait_of("operation timed out", None), None);
        // Bounded: a run waits at most MAX_WAITS times.
        assert!(may_wait_for_github(0));
        assert!(may_wait_for_github(common::github_wait::MAX_WAITS - 1));
        assert!(!may_wait_for_github(common::github_wait::MAX_WAITS));
    }

    #[test]
    fn a_rounds_changes_are_read_from_its_wip_compare() {
        let compare = serde_json::json!({"files": [
            {"filename": "src/tabs.tsx", "status": "added"},
            {"filename": "src/old.ts", "status": "removed"},
            {"filename": "src/new.ts", "status": "renamed", "previous_filename": "src/was.ts"},
            {"filename": "src/a.ts", "status": "modified"}
        ]});
        let c = changed_from_compare(&compare);
        assert_eq!(c.len(), 4);
        assert!(c[1].removed);
        assert_eq!(c[2].previous.as_deref(), Some("src/was.ts"));
        // Someone pushed to the PR meanwhile: only the same files block.
        assert!(overlap(&c, &["README.md".to_string()]).is_empty());
        assert_eq!(overlap(&c, &["src/a.ts".to_string()]), vec!["src/a.ts"]);
        assert_eq!(overlap(&c, &["src/was.ts".to_string()]), vec!["src/was.ts"]);
    }

    #[test]
    fn every_fault_inside_a_round_has_a_way_around_and_none_is_a_dead_round() {
        // The run's own errors, as GitHub returned them (names invented).
        let compare_404 = "HTTP status client error (404 Not Found) for url (https://api.github.com/repos/acme/site/compare/abc123...coderhelm-wip/coderhelm/gh-41-card-block): {\"message\":\"Not Found\"}";
        let ref_404 = "HTTP status client error (404 Not Found) for url (https://api.github.com/repos/acme/site/git/ref/heads/coderhelm-wip/coderhelm/gh-41-card-block)";
        for (error, fault, way) in [
            (compare_404, Fault::WipMissing, WayAround::ReopenWip),
            (ref_404, Fault::WipMissing, WayAround::ReopenWip),
            (
                "its changes couldn't be read (HTTP status client error (404 Not Found) for url (https://api.github.com/repos/acme/site/compare/abc...def))",
                Fault::Unreadable,
                WayAround::ReadFromTrees,
            ),
            (
                "the one commit couldn't be pushed (HTTP status client error (422 Unprocessable Entity): Update is not a fast forward)",
                Fault::PushRejected,
                WayAround::PushAgain,
            ),
            (
                "HTTP status client error (409 Conflict) for url (https://api.github.com/repos/acme/site/git/refs/heads/x)",
                Fault::PushRejected,
                WayAround::PushAgain,
            ),
            (
                "the pull request changed the same files meanwhile (package.json)",
                Fault::Clash,
                WayAround::RestartFromHead,
            ),
            (
                "HTTP status server error (502 Bad Gateway) for url (https://api.github.com/repos/acme/site/git/trees)",
                Fault::GitHubBusy,
                WayAround::WaitForGitHub,
            ),
            (
                "the sandbox build didn't start: CodeBuild AccountLimitExceededException",
                Fault::Sandbox,
                WayAround::WithoutSandbox,
            ),
            ("model reply had no content", Fault::Other, WayAround::RetryClean),
        ] {
            assert_eq!(fault_of(error), fault, "{error}");
            assert_eq!(way_around(fault), way, "{error}");
        }
        // The catch-all: a clean attempt while the round has attempts and
        // time, then one fresh invocation, then the supervisor's ending.
        assert_eq!(after_fault(1, 3, true, 0), AfterFault::RetryClean);
        assert_eq!(after_fault(2, 3, true, 0), AfterFault::RetryClean);
        assert_eq!(after_fault(3, 3, true, 0), AfterFault::Redrive);
        assert_eq!(after_fault(1, 3, false, 0), AfterFault::Redrive);
        assert_eq!(
            after_fault(3, 3, true, MAX_REDRIVES),
            AfterFault::Supervisor
        );
        let note = fault_note(
            Kind::CiFix,
            Fault::WipMissing,
            ref_404,
            &AfterFault::RetryClean,
        );
        assert!(
            note.contains("saved nothing") && note.contains("runs it again now"),
            "{note}"
        );
        assert!(!note.contains("didn't finish"), "{note}");
    }

    #[test]
    fn a_rounds_changes_are_read_from_the_trees_when_the_compare_is_refused() {
        let t = |v: &[(&str, &str)]| -> Vec<(String, String)> {
            v.iter()
                .map(|(p, s)| (p.to_string(), s.to_string()))
                .collect()
        };
        let before = t(&[
            ("package.json", "a1"),
            ("pnpm-lock.yaml", "b1"),
            ("old.ts", "c1"),
        ]);
        let after = t(&[
            ("package.json", "a1"),
            ("pnpm-lock.yaml", "b2"),
            ("new.ts", "d1"),
        ]);
        let c = changed_from_trees(&before, &after);
        let paths: Vec<(&str, bool)> = c.iter().map(|c| (c.path.as_str(), c.removed)).collect();
        assert_eq!(
            paths,
            vec![
                ("new.ts", false),
                ("old.ts", true),
                ("pnpm-lock.yaml", false)
            ]
        );
        assert!(changed_from_trees(&before, &before).is_empty());
    }

    #[test]
    fn a_round_starts_only_with_enough_of_its_budget_left() {
        let start = Instant::now();
        assert!(may_start(start, start));
        assert_eq!(secs_left(start, start), run_round::BUDGET_SECS);
        // After the review used most of the invocation, the fix round is
        // parked for a fresh one instead of being killed half way.
        let late = start + Duration::from_secs(run_round::BUDGET_SECS - 60);
        assert!(!may_start(start, late));
        let edge = start + Duration::from_secs(run_round::BUDGET_SECS - run_round::MIN_START_SECS);
        assert!(may_start(start, edge));
        // Past the deadline: nothing left, never a wrap-around.
        assert_eq!(secs_left(start, start + Duration::from_secs(5_000)), 0);
    }
}
