//! Staying with a pull request after the run that opened it finished:
//! through review, merge and deploy, with nobody pressing Retry.
//!
//! A finished run used to be woken only by an event that reached it: a
//! review, a comment, a failed GitHub Actions run. Nothing looked at the
//! pull request itself, so these were left as they were:
//!
//! - the base branch moved and the pull request now conflicts (found only
//!   when auto-merge tried to merge, once per pull request);
//! - a check went red after the run finished and reported through a commit
//!   status or another CI (no `workflow_run` event), or its event was lost;
//! - the run finished on a path that never asked for a review of its last
//!   commit, so CoderHelm's own reviewer still stood at "changes requested"
//!   on an older commit while the run read "Done";
//! - a person merged the pull request, and nothing watched its deploy.
//!
//! [`look`] reads one finished run's pull request and [`decide`] picks the
//! one thing to do about it, each done by the mechanism that already exists
//! for it (the conflict round, the fix loop, the reviewer's hand-off review,
//! the fix round on its findings, the post-merge deploy check). It runs when
//! a run completes and on the 15-minute sweep, so a lost event costs
//! minutes, not the pull request. Every act is claimed on the run record
//! against the commit it is about ([`Marks`]): the same commit never gets
//! the same act twice, however often it is looked at.
//!
//! Each look also writes where the pull request stands on its way to merge
//! (`common::merge_standing`), so the run page names the one true reason.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use common::merge_standing::{self, Standing};
use tracing::{info, warn};

use super::{attr_n, attr_s};
use crate::clients::github::GitHubClient;
use crate::WorkerState;

/// Finished runs are watched this long after they last changed.
pub const WATCH_DAYS: i64 = 14;
/// Pull requests one sweep reads from GitHub; the least recently looked at
/// go first, so every one is reached.
pub const SWEEP_MAX_LOOKS: usize = 40;

/// GitHub requests one look may make when the pull request changed since
/// the last one (the pull request, its base, its checks and statuses, the
/// base's when a check fails, its approvals): a look isn't started with
/// less than this left of the sweep's budget. A look at a pull request
/// that didn't change costs none (see [`Probe`]).
pub const LOOK_REQUESTS: u64 = 8;

/// What a look read of the pull request.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct Seen {
    pub open: bool,
    pub merged: bool,
    pub draft: bool,
    pub head: String,
    /// The base branch's commit the pull request is compared against.
    pub base_sha: String,
    pub merge_sha: String,
    /// GitHub says it conflicts with its base (`mergeable_state` = `dirty`).
    pub conflict: bool,
    /// GitHub says the branch's rules block the merge.
    pub blocked: bool,
    /// Checks failing on the head that don't fail on the base branch too.
    pub failing: Vec<String>,
    pub checks_running: bool,
    pub reviewer_on: bool,
    /// CoderHelm's latest review verdict and the commit it is on.
    pub own_verdict: Option<(String, String)>,
}

/// What the last look read from GitHub, kept on the run (`watch_probe`):
/// the validator of each read and what they came to. The next look asks
/// GitHub only whether any of those reads changed (conditional requests,
/// which cost nothing of the hourly budget while unchanged) and, when none
/// did, decides from this. A pull request nobody touched is looked at
/// every 15 minutes for free.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct Probe {
    /// URL and `ETag` of each read.
    pub validators: Vec<(String, String)>,
    pub seen: Seen,
    /// A person's approval on the head, when the look read approvals.
    pub human_approved: bool,
}

impl Probe {
    pub const FIELD: &'static str = "watch_probe";
    /// The most reads a probe keeps: a look that made more (a long list of
    /// reviews) is read in full every time.
    pub const MAX_VALIDATORS: usize = 12;

    pub fn parse(json: &str) -> Option<Probe> {
        serde_json::from_str::<Probe>(json)
            .ok()
            .filter(|p| !p.validators.is_empty())
    }

    /// The probe to keep after a full look, or "" when it can't stand in
    /// for one (a read without a validator, or too many).
    pub fn to_json(&self) -> String {
        if self.validators.is_empty() || self.validators.len() > Self::MAX_VALIDATORS {
            return String::new();
        }
        serde_json::to_string(self).unwrap_or_default()
    }
}

/// What was already done, each against the commit it was done for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Marks {
    /// The base commit a conflict round was started against.
    pub conflict_base: String,
    /// The head whose red checks were sent to the fix loop (or judged not
    /// this change's, see `flaky`).
    pub ci_head: String,
    /// The head a review was asked for.
    pub review_head: String,
    /// The head whose review findings got a fix round (or were deferred).
    pub findings_head: String,
    /// The merge commit whose deploy is watched.
    pub deploy_sha: String,
    /// The unfinished round (by when it stopped) that was run again after
    /// GitHub's limit ended it.
    pub limit_at: String,
}

impl Marks {
    pub const CONFLICT: &'static str = "watch_conflict_base";
    pub const CI: &'static str = "watch_ci_head";
    pub const REVIEW: &'static str = "watch_review_head";
    pub const FINDINGS: &'static str = "watch_findings_head";
    pub const DEPLOY: &'static str = "watch_deploy_sha";
    pub const LIMIT: &'static str = "watch_limit_at";

    pub fn of_item(item: &HashMap<String, AttributeValue>) -> Marks {
        let s = |k: &str| {
            item.get(k)
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default()
        };
        Marks {
            conflict_base: s(Self::CONFLICT),
            ci_head: s(Self::CI),
            review_head: s(Self::REVIEW),
            findings_head: s(Self::FINDINGS),
            deploy_sha: s(Self::DEPLOY),
            limit_at: s(Self::LIMIT),
        }
    }
}

/// The one thing to do about a finished run's pull request now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Act {
    Nothing,
    /// Merge the base in and resolve the conflict (`resume::start_conflict_round`).
    ResolveConflict,
    /// Send the red checks to the fix loop (`feedback`, as a late failed
    /// workflow run is).
    FixCi(Vec<String>),
    /// Ask CoderHelm's reviewer for its verdict on the head
    /// (`review_pr::review_for_ready`).
    ReviewHead,
    /// Take the review's findings on the head in a fix round
    /// (`resume::queue_own_review_round`).
    FixFindings,
    /// Watch the merge commit's deploy (`health_check`).
    WatchDeploy,
    /// Run again the round GitHub's rate limit ended (the round's record,
    /// by when it stopped): `resume::resume_limit_ended`.
    ResumeRound(String),
}

impl Act {
    /// The mark that claims it, and the commit it is claimed against.
    pub fn claim(&self, seen: &Seen) -> Option<(&'static str, String)> {
        match self {
            Act::Nothing => None,
            Act::ResolveConflict => Some((Marks::CONFLICT, seen.base_sha.clone())),
            Act::FixCi(_) => Some((Marks::CI, seen.head.clone())),
            Act::ReviewHead => Some((Marks::REVIEW, seen.head.clone())),
            Act::FixFindings => Some((Marks::FINDINGS, seen.head.clone())),
            Act::WatchDeploy => Some((Marks::DEPLOY, seen.merge_sha.clone())),
            Act::ResumeRound(at) => Some((Marks::LIMIT, at.clone())),
        }
    }
}

/// Run statuses whose pull request nobody else is working: the run ended
/// with its work on the pull request.
pub fn watched(status: &str) -> bool {
    matches!(
        status,
        common::run_status::COMPLETED | common::run_status::SUCCESS | "merged"
    )
}

/// What to do, first need first. A merged pull request gets its deploy
/// watched; a closed one nothing. An open one: a conflict before anything
/// (nothing else can be judged on a branch that can't merge), then red
/// checks, then the reviewer's verdict on the head: none on this commit
/// asks for one, and changes requested on this commit are fixed. Each only
/// once per commit.
pub fn decide(status: &str, seen: &Seen, marks: &Marks) -> Act {
    if !watched(status) {
        return Act::Nothing;
    }
    if seen.merged {
        return if !seen.merge_sha.is_empty() && marks.deploy_sha != seen.merge_sha {
            Act::WatchDeploy
        } else {
            Act::Nothing
        };
    }
    if !seen.open || seen.head.is_empty() {
        return Act::Nothing;
    }
    if seen.conflict {
        return if !seen.base_sha.is_empty() && marks.conflict_base != seen.base_sha {
            Act::ResolveConflict
        } else {
            Act::Nothing
        };
    }
    if !seen.failing.is_empty() && !seen.checks_running {
        return if marks.ci_head != seen.head {
            Act::FixCi(seen.failing.clone())
        } else {
            Act::Nothing
        };
    }
    if !seen.reviewer_on {
        return Act::Nothing;
    }
    match &seen.own_verdict {
        Some((verdict, sha)) if sha == &seen.head => {
            if verdict == "REQUEST_CHANGES" && marks.findings_head != seen.head {
                Act::FixFindings
            } else {
                Act::Nothing
            }
        }
        // No verdict, or one on an older commit.
        _ if marks.review_head != seen.head => Act::ReviewHead,
        _ => Act::Nothing,
    }
}

/// The round to run again before anything else is decided: one GitHub's
/// rate limit ended (`limit_ended`: when that round stopped, "" when the
/// run's last round wasn't ended by a limit), on a pull request still open,
/// once per such round. A limit is never how a round ends: before a limit
/// was waited out everywhere, a round that met one was left "didn't
/// finish" and its review's findings were deferred to a person. The sweep
/// runs only while GitHub's budget has room for it (`SweepGate`), so the
/// round starts again once the budget allows, with nobody pressing Resume.
pub fn resume_after_limit(
    status: &str,
    seen: &Seen,
    marks: &Marks,
    limit_ended: &str,
) -> Option<Act> {
    let open = seen.open && !seen.merged;
    (watched(status) && status != "merged" && open && !limit_ended.is_empty())
        .then(|| limit_ended.to_string())
        .filter(|at| marks.limit_at != *at)
        .map(Act::ResumeRound)
}

/// One finished run and its pull request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Candidate {
    pub team_id: String,
    pub run_id: String,
    pub status: String,
    pub owner: String,
    pub repo: String,
    pub pr: u64,
    pub installation_id: u64,
    pub base_branch: String,
    pub marks: Marks,
    /// When it was last looked at (RFC 3339), empty when never.
    pub looked_at: String,
    /// The standing last written, to write only a change.
    pub standing: String,
    /// The last look's [`Probe`], as stored ("" when none).
    pub probe: String,
    /// When the run's last round stopped, if GitHub's rate limit is why
    /// (`round::ended_by_limit`); "" otherwise.
    pub limit_ended: String,
}

pub fn candidate_of(item: &HashMap<String, AttributeValue>) -> Option<Candidate> {
    let s = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let n = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let pr = n("pr_number");
    let (owner, repo) = common::pr_outcome::parse_pr_url(&s("pr_url"))
        .filter(|(_, _, num)| *num == pr)
        .map(|(o, r, _)| (o, r))
        .or_else(|| {
            let full = s("repo");
            let (o, r) = full.split_once('/')?;
            Some((o.to_string(), r.to_string()))
        })?;
    let c = Candidate {
        team_id: s("team_id"),
        run_id: s("run_id"),
        status: s("status"),
        owner,
        repo,
        pr,
        installation_id: n("installation_id"),
        base_branch: s("base_branch"),
        marks: Marks::of_item(item),
        looked_at: s("watch_looked_at"),
        standing: s(merge_standing::FIELD),
        probe: s(Probe::FIELD),
        limit_ended: common::run_round::Incomplete::from_item(item)
            .filter(|inc| !inc.held && super::round::ended_by_limit(&inc.reason))
            .map(|inc| inc.at)
            .unwrap_or_default(),
    };
    (pr > 0 && c.installation_id > 0 && !c.team_id.is_empty() && !c.run_id.is_empty()).then_some(c)
}

/// The candidates a sweep reads, least recently looked at first.
pub fn order(mut all: Vec<Candidate>, max: usize) -> Vec<Candidate> {
    all.sort_by(|a, b| {
        a.looked_at
            .cmp(&b.looked_at)
            .then_with(|| a.run_id.cmp(&b.run_id))
    });
    all.truncate(max);
    all
}

/// Names of checks failing in a listing (the newest run of each name).
fn failing_names(listing: &serde_json::Value) -> (Vec<String>, bool) {
    let checks = super::reviewer_guide::checks_of(listing);
    let running = checks
        .iter()
        .any(|c| c.state == super::reviewer_guide::State::Running);
    let failing = checks
        .into_iter()
        .filter(|c| c.state == super::reviewer_guide::State::Failed)
        .map(|c| c.name)
        .collect();
    (failing, running)
}

/// Claims `field` = `value` on the run: false when it already is (the act
/// was done for this commit).
async fn claim(state: &WorkerState, c: &Candidate, field: &str, value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&c.team_id))
        .key("run_id", attr_s(&c.run_id))
        .update_expression("SET #f = :v")
        .condition_expression("attribute_exists(run_id) AND (attribute_not_exists(#f) OR #f <> :v)")
        .expression_attribute_names("#f", field)
        .expression_attribute_values(":v", attr_s(value))
        .send()
        .await
        .is_ok()
}

/// Gives a claim back when the act could not be started, so the next look
/// tries again.
async fn release(state: &WorkerState, c: &Candidate, field: &str, value: &str) {
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&c.team_id))
        .key("run_id", attr_s(&c.run_id))
        .update_expression("REMOVE #f")
        .condition_expression("#f = :v")
        .expression_attribute_names("#f", field)
        .expression_attribute_values(":v", attr_s(value))
        .send()
        .await;
}

/// Marks a head's red checks as dealt with (judged not this change's, see
/// `flaky`): the sweep doesn't send them to the fix loop.
pub(crate) async fn mark_ci_head(state: &WorkerState, team_id: &str, run_id: &str, head: &str) {
    if head.is_empty() {
        return;
    }
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET #f = :v")
        .expression_attribute_names("#f", Marks::CI)
        .expression_attribute_values(":v", attr_s(head))
        .send()
        .await;
}

async fn read(state: &WorkerState, github: &GitHubClient, c: &Candidate) -> Option<Seen> {
    let pr = github
        .get_pull_request(&c.owner, &c.repo, c.pr)
        .await
        .map_err(|e| warn!(run_id = %c.run_id, pr = c.pr, error = %e, "PR watch: could not read the pull request"))
        .ok()?;
    let text = |p: &str| {
        pr.pointer(p)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };
    let mut seen = Seen {
        open: pr["state"].as_str() == Some("open"),
        merged: pr["merged"].as_bool().unwrap_or(false),
        draft: pr["draft"].as_bool().unwrap_or(false),
        head: text("/head/sha"),
        base_sha: text("/base/sha"),
        merge_sha: text("/merge_commit_sha"),
        conflict: common::merge_gate::is_conflict(&pr),
        blocked: pr["mergeable_state"].as_str() == Some("blocked"),
        ..Default::default()
    };
    if !seen.open {
        return Some(seen);
    }
    // The base's own head, not the commit the pull request was opened on:
    // a conflict is claimed against the base as it stands.
    let base_ref = text("/base/ref");
    if let Ok(sha) = github.get_ref(&c.owner, &c.repo, &base_ref).await {
        seen.base_sha = sha;
    }
    if let Ok(listing) = github.list_ci_for_ref(&c.owner, &c.repo, &seen.head).await {
        let (failing, running) = failing_names(&listing);
        seen.checks_running = running;
        if !failing.is_empty() {
            let on_base = match github.list_ci_for_ref(&c.owner, &c.repo, &base_ref).await {
                Ok(b) => failing_names(&b).0,
                Err(_) => Vec::new(),
            };
            seen.failing = failing
                .into_iter()
                .filter(|n| !on_base.contains(n))
                .collect();
        }
    }
    seen.reviewer_on =
        super::review_pr::reviewer_enabled(state, &c.team_id, &c.owner, &c.repo).await;
    seen.own_verdict =
        super::review_pr::own_verdict(state, &c.team_id, &c.owner, &c.repo, c.pr).await;
    Some(seen)
}

async fn send_ci_fix(state: &WorkerState, c: &Candidate) -> bool {
    if state.config.feedback_queue_url.is_empty() {
        return false;
    }
    let body = serde_json::json!({
        "type": "feedback",
        "team_id": c.team_id,
        "installation_id": c.installation_id,
        "run_id": c.run_id,
        "repo_owner": c.owner,
        "repo_name": c.repo,
        "pr_number": c.pr,
        // 0: the fix loop reads the pull request's failing checks itself.
        "review_id": 0,
        "review_body": "",
        "comments": [],
        "trigger_author": "",
    });
    state
        .sqs
        .send_message()
        .queue_url(&state.config.feedback_queue_url)
        .message_body(body.to_string())
        .send()
        .await
        .is_ok()
}

/// Does `act`. False when it could not be started (its claim is given back).
async fn perform(
    state: &WorkerState,
    github: &GitHubClient,
    c: &Candidate,
    seen: &Seen,
    act: &Act,
) -> bool {
    // Whatever the watch does next rests on a description of this head.
    super::description::reconcile_run(
        state, github, &c.team_id, &c.run_id, &c.owner, &c.repo, c.pr,
    )
    .await;
    match act {
        Act::Nothing => true,
        Act::ResolveConflict => {
            super::resume::start_conflict_round(
                state,
                &c.team_id,
                c.installation_id,
                &c.owner,
                &c.repo,
                c.pr,
            )
            .await
        }
        Act::FixCi(_) => send_ci_fix(state, c).await,
        Act::ReviewHead => !matches!(
            super::review_pr::review_for_ready(
                state,
                &c.team_id,
                c.installation_id,
                (&c.owner, &c.repo, c.pr),
                &seen.head,
            )
            .await,
            super::review_pr::ReadyReviewAsk::Failed
        ),
        Act::FixFindings => {
            let body = github
                .get_pull_request(&c.owner, &c.repo, c.pr)
                .await
                .ok()
                .and_then(|p| p["body"].as_str().map(str::to_string))
                .unwrap_or_default();
            let mut items = super::own_review::listed(&body);
            if items.is_empty() {
                items.push(format!(
                    "CoderHelm's review requested changes on `{}`; its review comments on the \
                     pull request say what",
                    seen.head.get(..7).unwrap_or(&seen.head)
                ));
            }
            match super::resume::queue_own_review_round(
                state, github, &c.team_id, &c.run_id, &seen.head, &items,
            )
            .await
            {
                super::resume::OwnRound::Fixing => true,
                super::resume::OwnRound::Defer(why) => {
                    // The run's rounds are over by the supervisor's rule:
                    // what is open is deferred with the reason, on the pull
                    // request, never left as an unexplained verdict.
                    let now = super::own_review::defer_findings(&body, &items, &why);
                    if now != body {
                        if let Err(e) =
                            crate::passes::description::save(github, &c.owner, &c.repo, c.pr, &now)
                                .await
                        {
                            warn!(run_id = %c.run_id, error = %e, "PR watch: could not list the deferred findings");
                        }
                    }
                    super::add_progress_note(
                        state,
                        &c.team_id,
                        &c.run_id,
                        &super::own_review::finish_reason(&items, &why),
                    )
                    .await;
                    true
                }
                super::resume::OwnRound::Person(why) => {
                    info!(run_id = %c.run_id, why = %why, "PR watch: no fix round for the review's findings now");
                    false
                }
            }
        }
        Act::ResumeRound(_) => {
            super::resume::resume_limit_ended(state, &c.team_id, &c.run_id, c.installation_id).await
        }
        Act::WatchDeploy => {
            super::health_check::schedule_own(
                state,
                github,
                &c.team_id,
                c.installation_id,
                (&c.owner, &c.repo, c.pr),
                &c.base_branch,
                &seen.merge_sha,
            )
            .await
        }
    }
}

/// The standing shown for the pull request, from what the look read and
/// what it started.
async fn standing(
    state: &WorkerState,
    github: &GitHubClient,
    c: &Candidate,
    seen: &Seen,
    working: bool,
    // The approval the last look read, when nothing changed since.
    known_approval: Option<bool>,
) -> (Standing, bool) {
    let cfg =
        super::review_actions::OnApproveConfig::load(state, &c.team_id, &c.owner, &c.repo).await;
    let human_approved = if let Some(known) = known_approval {
        known
    } else if seen.open && cfg.auto_merge && cfg.require_human_approval {
        super::review_actions::fetch_approvals(github, &c.owner, &c.repo, c.pr, &seen.head)
            .await
            .map(|a| a.human_key_at_head())
            .unwrap_or(false)
    } else {
        false
    };
    let standing = merge_standing::decide(
        &format!("{}/{}", c.owner, c.repo),
        &merge_standing::Facts {
            merged: seen.merged,
            closed: !seen.open && !seen.merged,
            draft: seen.draft,
            head: seen.head.clone(),
            reviewer_on: seen.reviewer_on,
            own_verdict: seen.own_verdict.clone(),
            working,
            failing: seen.failing.clone(),
            checks_running: seen.checks_running,
            conflict: seen.conflict,
            auto_merge: cfg.auto_merge,
            require_human_approval: cfg.require_human_approval,
            human_approved,
            blocked_by_rules: seen.blocked && seen.failing.is_empty() && !seen.checks_running,
        },
    );
    (standing, human_approved)
}

/// Stamps the look, with the standing and the probe when they changed.
/// `None` for either leaves what is there.
async fn record_look(
    state: &WorkerState,
    c: &Candidate,
    standing: Option<&Standing>,
    probe: Option<&str>,
) {
    let mut sets = vec!["watch_looked_at = :t".to_string()];
    let mut req = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&c.team_id))
        .key("run_id", attr_s(&c.run_id))
        .condition_expression("attribute_exists(run_id)")
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()));
    if let Some(json) = standing.map(Standing::to_json).filter(|j| *j != c.standing) {
        sets.push("#ms = :ms".to_string());
        req = req
            .expression_attribute_names("#ms", merge_standing::FIELD)
            .expression_attribute_values(":ms", attr_s(&json));
    }
    if let Some(p) = probe.filter(|p| *p != c.probe) {
        sets.push("#wp = :wp".to_string());
        req = req
            .expression_attribute_names("#wp", Probe::FIELD)
            .expression_attribute_values(":wp", attr_s(p));
    }
    req = req.update_expression(format!("SET {}", sets.join(", ")));
    if let Err(e) = req.send().await {
        warn!(run_id = %c.run_id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "PR watch: could not record the look");
    }
}

/// Looks at one finished run's pull request and does the one thing it
/// needs. Returns what it did.
pub async fn look(state: &WorkerState, github: &GitHubClient, c: &Candidate) -> Act {
    // Nothing the last look read has changed: its reads stand, at no cost
    // to GitHub's budget. What CoderHelm's own records say is read again.
    let kept = match Probe::parse(&c.probe) {
        Some(p) if github.unchanged(&p.validators).await => Some(p),
        _ => None,
    };
    let (seen, mut validators) = match &kept {
        Some(p) => {
            let mut seen = p.seen.clone();
            seen.reviewer_on =
                super::review_pr::reviewer_enabled(state, &c.team_id, &c.owner, &c.repo).await;
            seen.own_verdict =
                super::review_pr::own_verdict(state, &c.team_id, &c.owner, &c.repo, c.pr).await;
            (seen, p.validators.clone())
        }
        None => {
            github.record_validators();
            let seen = read(state, github, c).await;
            let validators = github.take_validators();
            let Some(seen) = seen else {
                // GitHub refusing for a while says nothing of the pull
                // request, and the sweep's order is left as it is. Any
                // other failure moves on: the look is stamped, so the same
                // pull requests aren't the only ones a sweep reaches.
                if github.outside().is_none() {
                    record_look(state, c, None, None).await;
                }
                return Act::Nothing;
            };
            (seen, validators)
        }
    };
    let mut act = resume_after_limit(&c.status, &seen, &c.marks, &c.limit_ended)
        .unwrap_or_else(|| decide(&c.status, &seen, &c.marks));
    if let Some((field, value)) = act.claim(&seen) {
        if !claim(state, c, field, &value).await {
            act = Act::Nothing;
        } else if !perform(state, github, c, &seen, &act).await {
            release(state, c, field, &value).await;
            act = Act::Nothing;
        } else {
            info!(run_id = %c.run_id, pr = c.pr, ?act, "PR watch: picked the pull request back up");
        }
    }
    // A review asked for or a round started on this head (now or at an
    // earlier look) is CoderHelm working, not a person's turn.
    let working = matches!(
        act,
        Act::ReviewHead | Act::FixFindings | Act::ResolveConflict | Act::ResumeRound(_)
    ) || common::run_status::IN_FLIGHT.contains(&c.status.as_str());
    if kept.is_none() {
        github.record_validators();
    }
    let (s, human_approved) = standing(
        state,
        github,
        c,
        &seen,
        working,
        kept.as_ref().map(|p| p.human_approved),
    )
    .await;
    let probe = match kept {
        // Unchanged: the probe stands as it is.
        Some(_) => None,
        None => {
            validators.extend(github.take_validators());
            Some(
                Probe {
                    validators,
                    seen,
                    human_approved,
                }
                .to_json(),
            )
        }
    };
    record_look(state, c, Some(&s), probe.as_deref()).await;
    act
}

fn client(state: &WorkerState, installation_id: u64) -> Option<GitHubClient> {
    GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        installation_id,
        &state.http,
    )
    .map_err(|e| warn!(installation_id, error = %e, "PR watch: no GitHub client"))
    .ok()
}

/// A run just completed: its pull request is looked at now, so a run never
/// rests as completed with its reviewer's verdict missing on the head or
/// asking for changes. Best effort; the sweep looks again.
pub async fn after_completion(state: &WorkerState, team_id: &str, run_id: &str) {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .consistent_read(true)
        .send()
        .await
        .ok()
        .and_then(|o| o.item);
    let Some(c) = item.as_ref().and_then(candidate_of) else {
        return;
    };
    if !watched(&c.status) {
        return;
    }
    if let Some(github) = client(state, c.installation_id) {
        look(state, &github, &c).await;
    }
}

/// What one sweep did.
#[derive(Debug, Default)]
pub struct SweepReport {
    pub candidates: usize,
    pub looked_at: usize,
    /// Installations skipped this cycle for GitHub's budget.
    pub skipped_installations: usize,
    /// `owner/repo#pr: act` for each pull request picked back up.
    pub acted: Vec<String>,
}

/// Finished runs with a pull request, changed inside the watch window: each
/// pull request is looked at, least recently looked at first.
pub async fn sweep(state: &WorkerState) -> SweepReport {
    let mut report = SweepReport::default();
    let cutoff = (chrono::Utc::now() - chrono::Duration::days(WATCH_DAYS)).to_rfc3339();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    let mut all = Vec::new();
    for _ in 0..200 {
        let res = state
            .dynamo
            .scan()
            .table_name(&state.config.runs_table_name)
            .filter_expression(
                "#s IN (:done, :old_done, :merged) AND pr_number > :zero AND updated_at >= :cut \
                 AND (attribute_not_exists(pr_outcome) OR pr_outcome = :merged)",
            )
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":done", attr_s(common::run_status::COMPLETED))
            .expression_attribute_values(":old_done", attr_s(common::run_status::SUCCESS))
            .expression_attribute_values(":merged", attr_s("merged"))
            .expression_attribute_values(":zero", attr_n(0))
            .expression_attribute_values(":cut", attr_s(&cutoff))
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let page = match res {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "PR watch: scan failed");
                break;
            }
        };
        all.extend(page.items().iter().filter_map(candidate_of));
        match page.last_evaluated_key {
            Some(k) if !k.is_empty() => start = Some(k),
            _ => break,
        }
    }
    // A merged pull request whose deploy is already watched needs no look.
    all.retain(|c| c.status != "merged" || c.marks.deploy_sha.is_empty());
    report.candidates = all.len();
    let mut clients: HashMap<u64, GitHubClient> = HashMap::new();
    // Asked before each look: the sweep never spends the runs' share of
    // GitHub's budget nor more than its own hour's, and an installation it
    // skips is said once. The others' pull requests are still reached.
    let mut budget = crate::clients::github_budget::SweepGate::new(
        common::github_budget::caller::SWEEP_PR_WATCH,
    );
    for c in order(all, usize::MAX) {
        if report.looked_at >= SWEEP_MAX_LOOKS {
            break;
        }
        if !budget.allows(state, c.installation_id, LOOK_REQUESTS).await {
            continue;
        }
        if let std::collections::hash_map::Entry::Vacant(v) = clients.entry(c.installation_id) {
            match client(state, c.installation_id) {
                Some(g) => {
                    v.insert(g);
                }
                None => continue,
            }
        }
        report.looked_at += 1;
        let act = look(state, &clients[&c.installation_id], &c).await;
        if act != Act::Nothing {
            report
                .acted
                .push(format!("{}/{}#{}: {act:?}", c.owner, c.repo, c.pr));
        }
    }
    report.skipped_installations = budget.skipped();
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEAD: &str = "9f8e7d6c5b4a39281706f5e4d3c2b1a098765432";
    const OLD: &str = "1111111aaaaaaa2222222bbbbbbb3333333ccccc";
    const BASE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn open() -> Seen {
        Seen {
            open: true,
            head: HEAD.into(),
            base_sha: BASE.into(),
            reviewer_on: true,
            own_verdict: Some(("APPROVE".into(), HEAD.into())),
            ..Default::default()
        }
    }

    #[test]
    fn a_settled_pull_request_needs_nothing() {
        assert_eq!(
            decide("completed", &open(), &Marks::default()),
            Act::Nothing
        );
    }

    #[test]
    fn only_a_finished_run_is_watched() {
        let mut seen = open();
        seen.conflict = true;
        for status in [
            "running",
            "awaiting_ci",
            "needs_input",
            "cancelled",
            "closed",
            "failed",
        ] {
            assert_eq!(
                decide(status, &seen, &Marks::default()),
                Act::Nothing,
                "{status}"
            );
        }
        assert_eq!(
            decide("completed", &seen, &Marks::default()),
            Act::ResolveConflict
        );
    }

    #[test]
    fn a_conflict_is_resolved_once_per_base_commit() {
        let mut seen = open();
        seen.conflict = true;
        // Red checks and a stale verdict wait: nothing is judged on a
        // branch that can't merge.
        seen.failing = vec!["unit".into()];
        seen.own_verdict = None;
        let act = decide("completed", &seen, &Marks::default());
        assert_eq!(act, Act::ResolveConflict);
        assert_eq!(act.claim(&seen), Some((Marks::CONFLICT, BASE.to_string())));
        let done = Marks {
            conflict_base: BASE.into(),
            ..Default::default()
        };
        assert_eq!(decide("completed", &seen, &done), Act::Nothing);
        // The base moved again: a new conflict is a new round.
        seen.base_sha = "cccccccccccccccccccccccccccccccccccccccc".into();
        assert_eq!(decide("completed", &seen, &done), Act::ResolveConflict);
    }

    #[test]
    fn late_red_checks_go_to_the_fix_loop_once_per_head() {
        let mut seen = open();
        seen.failing = vec!["ci/build".into()];
        assert_eq!(
            decide("completed", &seen, &Marks::default()),
            Act::FixCi(vec!["ci/build".into()])
        );
        // Still running: the full set decides, not the first failure.
        seen.checks_running = true;
        assert_eq!(decide("completed", &seen, &Marks::default()), Act::Nothing);
        seen.checks_running = false;
        let done = Marks {
            ci_head: HEAD.into(),
            ..Default::default()
        };
        assert_eq!(decide("completed", &seen, &done), Act::Nothing);
    }

    #[test]
    fn a_verdict_missing_on_the_head_asks_for_a_review_once() {
        let mut seen = open();
        seen.own_verdict = Some(("REQUEST_CHANGES".into(), OLD.into()));
        let act = decide("completed", &seen, &Marks::default());
        assert_eq!(act, Act::ReviewHead);
        assert_eq!(act.claim(&seen), Some((Marks::REVIEW, HEAD.to_string())));
        let asked = Marks {
            review_head: HEAD.into(),
            ..Default::default()
        };
        // The same head is never reviewed twice by this path.
        assert_eq!(decide("completed", &seen, &asked), Act::Nothing);
        seen.own_verdict = None;
        assert_eq!(
            decide("completed", &seen, &Marks::default()),
            Act::ReviewHead
        );
        // No reviewer for the repository: no verdict to wait for.
        seen.reviewer_on = false;
        assert_eq!(decide("completed", &seen, &Marks::default()), Act::Nothing);
    }

    #[test]
    fn changes_requested_on_the_head_get_a_fix_round_once() {
        let mut seen = open();
        seen.own_verdict = Some(("REQUEST_CHANGES".into(), HEAD.into()));
        assert_eq!(
            decide("completed", &seen, &Marks::default()),
            Act::FixFindings
        );
        let done = Marks {
            findings_head: HEAD.into(),
            ..Default::default()
        };
        assert_eq!(decide("completed", &seen, &done), Act::Nothing);
    }

    #[test]
    fn a_merge_by_anyone_gets_its_deploy_watched_once() {
        let seen = Seen {
            merged: true,
            merge_sha: "abcabcabc".into(),
            ..Default::default()
        };
        assert_eq!(decide("merged", &seen, &Marks::default()), Act::WatchDeploy);
        let done = Marks {
            deploy_sha: "abcabcabc".into(),
            ..Default::default()
        };
        assert_eq!(decide("merged", &seen, &done), Act::Nothing);
        // Closed without merging: nothing.
        assert_eq!(
            decide("completed", &Seen::default(), &Marks::default()),
            Act::Nothing
        );
    }

    #[test]
    fn the_sweep_reaches_every_pull_request() {
        let c = |run: &str, at: &str| Candidate {
            run_id: run.into(),
            looked_at: at.into(),
            ..Default::default()
        };
        let picked = order(
            vec![
                c("r3", "2026-03-02T10:00:00Z"),
                c("r1", ""),
                c("r2", "2026-03-01T10:00:00Z"),
            ],
            2,
        );
        let ids: Vec<&str> = picked.iter().map(|c| c.run_id.as_str()).collect();
        assert_eq!(ids, vec!["r1", "r2"]);
    }
}
