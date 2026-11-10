//! Whether a finished run's pull request leaves draft, decided in one place.
//!
//! A run finished with CI green and CoderHelm's review approving. Its pull
//! request stayed a draft: CoderHelm's own sandbox checks had failed on that
//! head, the mark-ready step returned without a word, and the run page said
//! "completed". The same day, mark-ready messages that hit GitHub's rate
//! limit were logged once and dropped, and five more pull requests were
//! never marked ready. Two mark-ready paths each carried their own list of
//! holds, and every hold was silent.
//!
//! The rules:
//!
//! - [`decide`] is the one decision, and every way a pull request is marked
//!   ready goes through [`settle`]: the run's own completion
//!   (`resume::mark_pr_ready`), the mark-ready message (CI passed, a
//!   failing-test fix's recheck), an end-to-end proof that held, and the
//!   sweep. It answers [`Decision::Ready`], [`Decision::Held`] with named
//!   reasons, or [`Decision::Wait`] with what it waits for.
//! - CoderHelm defaults to progress. Its own sandbox failures and
//!   verification checklist items (all about tests, none a correctness,
//!   security or data-loss finding) don't keep the draft once the
//!   repository's CI is green on the head and CoderHelm's review approved
//!   it: the pull request is marked ready with them listed under Open items.
//!   What a person must decide keeps it a draft: security items, planned
//!   work not done, a check the change didn't pass, CoderHelm's own open
//!   review findings, a stacked change asking for a look, a person putting
//!   it back to draft, a failing-test fix that loosens a test, and redirect
//!   or link checks that fail now.
//! - A hold is never silent. It is logged at warn with the reasons, written
//!   at the top of the description, and recorded on the run
//!   (`common::draft_hold`). A hold a person must act on sets the run to
//!   `needs_input` with what to do, so the run never reads as plain
//!   "completed" while its pull request is held.
//! - Marking ready can't be lost. GitHub refusing for a while (its rate
//!   limit, a 5xx, a timeline it can't serve) sends the decision again after
//!   GitHub's reset, up to `common::github_wait::MAX_WAITS` tries; the last
//!   refusal is recorded on the run like a hold.
//! - [`sweep`] finds finished runs whose pull request is still a draft with
//!   no recorded reason, and decides again: it marks the pull request ready
//!   or records why not. A person's draft and a closed pull request are
//!   respected.
//! - A hold is only as current as the code that wrote it. When the pull
//!   request is held on a check line other code than the code running now
//!   wrote ([`recheck_due`]), the run checks it again on the head in a fresh
//!   step (`recheck`), once per code version, and the pull request waits for
//!   that instead of staying held on the old line. What the checks find then
//!   decides, here, as always.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use common::draft_hold::{DraftHold, Kind, Reason};
use common::run_status::EndedValues;
use common::run_wait::NeedsInput;
use tracing::{error, info, warn};

use super::feature::FeatureHold;
use super::test_fix::Hold as TestHold;
use super::{attr_n, attr_s};
use crate::clients::github::GitHubClient;
use crate::models::{MarkReadyMessage, WorkerMessage};
use crate::WorkerState;

/// Around the description's draft status, at its very top.
pub const STATUS_START: &str = "<!-- coderhelm:draft-status -->";
pub const STATUS_END: &str = "<!-- /coderhelm:draft-status -->";

/// A held pull request is decided again this long after the last decision
/// (the sweep): a person may have dealt with it without a push.
pub const RECHECK_HELD_SECS: i64 = 3600;
/// The sweep looks at runs finished this recently.
pub const SWEEP_WINDOW_DAYS: i64 = 14;
/// Decisions one sweep makes at most; the rest wait for the next sweep.
pub const SWEEP_MAX_DECISIONS: usize = 40;
/// The longest delay one SQS message carries.
const MAX_DELAY_SECS: u64 = common::github_wait::MAX_HOP_SECS;
/// The first wait after a refusal that named no reset.
const BASE_RETRY_SECS: u64 = 60;

// ── the decision ────────────────────────────────────────────────────────

/// Where the repository's own CI stands on the head (CoderHelm's own checks
/// left out).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ci {
    pub green: bool,
    /// Why it isn't green, in plain words; empty when green.
    pub why: String,
}

/// An end-to-end fix or new test's repeat-run proof.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum E2e {
    /// Not such a pull request, or proven.
    #[default]
    None,
    /// Its repeat runs are going.
    Running,
    /// The fix loosened the test or its repeat runs failed.
    Refused,
}

/// What [`decide`] reads, gathered from GitHub and the records.
#[derive(Debug, Clone, Default)]
pub struct Facts {
    pub closed: bool,
    pub draft: bool,
    pub body: String,
    pub title: String,
    /// An end-to-end fix or generated test whose repeat runs proved it.
    pub proven: bool,
    pub e2e: E2e,
    /// A person put it back to draft (their login).
    pub put_back_by: Option<String>,
    /// Redirect and link checks failing now.
    pub links_blocking: Vec<String>,
    /// The failing-test fix's own ready check; `None` for any other pull
    /// request.
    pub test_fix: Option<TestHold>,
    /// What CoderHelm's own feature checks still find on the head.
    pub feature: Option<FeatureHold>,
    pub ci: Ci,
    /// CoderHelm's review verdict on the head is APPROVE.
    pub approved: bool,
}

/// What happens to the pull request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Mark it ready (or it is already). `open` goes under Open items: what
    /// CoderHelm's own checks found that doesn't hold the draft.
    Ready { open: Vec<String> },
    /// It stays a draft until a person deals with each reason.
    Held { reasons: Vec<Reason> },
    /// It stays a draft while something settles by itself; CoderHelm
    /// decides again on its own (`until` epoch seconds, when known).
    Wait { why: String, until: Option<u64> },
    /// A person closed it: nothing to mark, and it is never reopened.
    Closed,
}

/// The reasons the description itself holds the draft for, each with what a
/// person can do. Every one of these is something a person must decide or
/// fix, so each stays a hold (see the module's rules).
#[cfg(test)]
pub fn body_holds(body: &str) -> Vec<Reason> {
    body_holds_on(body, false)
}

/// [`body_holds`] with the repository's CI where it stands on the head: a
/// check CoderHelm's own tools couldn't run is settled once CI is green, so it
/// no longer holds (`proof::holds_blocked_on`).
pub fn body_holds_on(body: &str, ci_green: bool) -> Vec<Reason> {
    let mut out = Vec::new();
    // Security: never shipped over without a person.
    if super::proof::holds_security_items(body) {
        out.push(Reason::new(
            "Security items are open under Open items",
            "Fix them on this branch (CoderHelm checks again on the next push), or delete each \
             security line once a person has checked it.",
        ));
    }
    // The change doesn't yet do what the ticket asked: correctness.
    if super::proof::holds_unfinished(body) {
        out.push(Reason::new(
            "Planned work is not done yet (listed under Not finished)",
            "CoderHelm keeps working it on this pull request; finish it on the branch, or remove \
             the section if the work isn't needed.",
        ));
    }
    // A check that would once have stopped the pull request (carried-over
    // commits, missing registrations, a migration that can't be released
    // safely, redirects): a named, checked problem.
    if super::proof::holds_blocked_on(body, ci_green) {
        out.push(Reason::new(
            "A check the change didn't pass is listed under Open items",
            "Fix it on this branch, or delete that line once a person has dealt with it.",
        ));
    }
    // Correctness findings of CoderHelm's own review, not yet fixed.
    if let Some(why) = super::own_review::hold(body) {
        out.push(Reason::new(
            why,
            "CoderHelm fixes them in its review rounds; fix them on the branch, or remove a \
             finding from the list once a person has judged it.",
        ));
    }
    // A stacked change asks a person to look first.
    if let Some(why) = super::stack::hold(body) {
        out.push(Reason::new(
            why,
            "Look at the change it builds on, then remove the hold line from its \"Builds on\" \
             section.",
        ));
    }
    out
}

/// The one decision. Pure, so every case is tested.
pub fn decide(f: &Facts) -> Decision {
    if f.closed {
        return Decision::Closed;
    }
    let open = match (&f.feature, f.ci.green && f.approved) {
        (Some(h), true) => h.items(),
        _ => Vec::new(),
    };
    if !f.draft {
        return Decision::Ready { open };
    }
    // A person put it back to draft: their call, and nothing CoderHelm
    // decides undoes it.
    if let Some(who) = &f.put_back_by {
        return Decision::Held {
            reasons: vec![Reason::new(
                format!("`{who}` put it back to draft"),
                "It stays a draft until a person marks it ready; CoderHelm doesn't undo that.",
            )],
        };
    }
    let mut reasons = body_holds_on(&f.body, f.ci.green);
    // Redirects and links the change adds that fail when checked now: a
    // named, checked correctness problem a reader would hit.
    if !f.links_blocking.is_empty() {
        reasons.push(Reason::new(
            format!(
                "The redirect and link checks fail now: {}",
                f.links_blocking.join("; ")
            ),
            "Fix them on this branch; the checks run again on the next push and when the pull \
             request is marked ready.",
        ));
    }
    let mut wait: Option<String> = None;
    match &f.test_fix {
        // A "fix" that loosens the test can hide the bug: a person decides.
        Some(TestHold::Refused(why)) => reasons.push(Reason::new(
            format!("This failing-test fix stays a draft: {why}"),
            "Change the fix so the test isn't loosened, or mark it ready yourself if the change \
             is right.",
        )),
        Some(TestHold::GaveUp(why)) => reasons.push(Reason::new(
            format!("This failing-test fix never cleared its ready check: {why}"),
            "Run the affected test on the branch and mark it ready yourself, or push a fix.",
        )),
        Some(TestHold::Wait(why)) => wait = Some(format!("this failing-test fix waits: {why}")),
        Some(TestHold::Go) | None => {}
    }
    match f.e2e {
        E2e::Refused => reasons.push(Reason::new(
            "The end-to-end fix was refused: it loosened the test, or its repeat runs failed \
             (the comment on the pull request says which)",
            "Push a fix that makes the test pass without loosening it, or close the pull request.",
        )),
        E2e::Running => {
            wait.get_or_insert_with(|| "its repeat runs are going".to_string());
        }
        E2e::None => {
            if !f.proven && super::resume::is_e2e_proof_pr(&f.title) {
                wait.get_or_insert_with(|| {
                    "it is an end-to-end fix or new test, which leaves draft once its repeat \
                     runs prove it"
                        .to_string()
                });
            }
        }
    }
    // CoderHelm's own sandbox and checklist findings weigh against the
    // repository's CI and the review: with both green they are open items
    // on a ready pull request, never a hold by themselves.
    if let Some(h) = &f.feature {
        if !(f.ci.green && f.approved) {
            let mut not_yet = Vec::new();
            if !f.ci.green {
                not_yet.push(if f.ci.why.is_empty() {
                    "the repository's CI isn't green on this head".to_string()
                } else {
                    f.ci.why.clone()
                });
            }
            if !f.approved {
                not_yet.push("CoderHelm's review hasn't approved this head".to_string());
            }
            reasons.push(Reason::new(
                format!(
                    "CoderHelm's own checks still find {} item(s) on this head ({}), and {}",
                    h.items().len(),
                    h.items().join("; "),
                    not_yet.join(", and ")
                ),
                "Fix them on this branch, or mark it ready yourself if they're fine. Once CI is \
                 green and the review approves, CoderHelm marks it ready with them listed.",
            ));
        }
    }
    if !reasons.is_empty() {
        return Decision::Held { reasons };
    }
    if let Some(why) = wait {
        return Decision::Wait { why, until: None };
    }
    Decision::Ready { open }
}

/// Whether a held pull request is checked again before its hold is
/// recorded: a check line written by code other than `version` holds it
/// (see `recheck`). Never for a pull request a person put back to draft, or
/// one that isn't held.
pub fn recheck_due(f: &Facts, d: &Decision, version: &str) -> bool {
    matches!(d, Decision::Held { .. })
        && f.draft
        && !f.closed
        && f.put_back_by.is_none()
        && super::proof::holds_on_other_version(&f.body, f.ci.green, version)
}

/// What the description says while the checks run again.
pub const RECHECK_WHY: &str = "CoderHelm's checks run again on this head: the check listed under                                Open items was written by an earlier version of them";

/// The description's draft status for a decision; empty when it is ready.
pub fn status_section(d: &Decision) -> String {
    match d {
        Decision::Held { reasons } => {
            let items: Vec<String> = reasons.iter().map(|r| format!("- {}", r.line())).collect();
            format!(
                "{STATUS_START}\n**This pull request stays a draft.** CoderHelm finished its \
                 work; a person needs to deal with:\n{}\n{STATUS_END}",
                items.join("\n")
            )
        }
        Decision::Wait { why, .. } => format!(
            "{STATUS_START}\n**This pull request stays a draft for now:** {}. CoderHelm checks \
             again by itself and marks it ready when that is done.\n{STATUS_END}",
            why.trim().trim_end_matches('.')
        ),
        Decision::Ready { .. } | Decision::Closed => String::new(),
    }
}

/// The description once CoderHelm couldn't mark it ready after every try.
fn failed_section(why: &str) -> String {
    format!(
        "{STATUS_START}\n**This pull request should be ready, but CoderHelm couldn't mark it:** \
         {}. Mark it ready for review yourself; CoderHelm also tries again within the hour.\n\
         {STATUS_END}",
        why.trim().trim_end_matches('.')
    )
}

/// The Open items lines for what CoderHelm's checks found on a pull request
/// it marked ready anyway, leaving out what the section already says.
pub fn open_item_lines(body: &str, open: &[String]) -> Vec<String> {
    let (owned, _) = super::completeness::take_owned(body);
    open.iter()
        .filter(|o| {
            let core = o
                .trim_start_matches("CoderHelm's sandbox check failed: ")
                .trim_start_matches("Verification checklist: ");
            // Already listed (with its old mark, or by the rules at the PR).
            !owned
                .open_items
                .lines()
                .any(|l| !l.contains(super::proof::READY_OPEN_MARK) && l.contains(core))
        })
        .map(|o| {
            format!(
                "- {}. CI is green and the review approved, so this doesn't keep the draft: \
                 look at it before merging. {}",
                o.trim().trim_end_matches('.'),
                super::proof::READY_OPEN_MARK
            )
        })
        .collect()
}

/// The description with its draft status (and, when ready, the open items)
/// set for a decision. `None` when nothing changes.
pub fn body_for(body: &str, d: &Decision) -> Option<String> {
    let mut out = super::completeness::with_section(
        body,
        super::completeness::Section::DraftStatus,
        status_section(d),
    );
    if let Decision::Ready { open } = d {
        // A check CoderHelm's own tools couldn't run no longer holds once CI
        // is green (the only way a description with one reaches Ready): its
        // line goes, so the description doesn't say it still holds.
        if let Some(b) = super::proof::without_settled_blocked(&out, true) {
            out = b;
        }
        if let Some(b) = super::proof::with_marked_items(
            &out,
            super::proof::READY_OPEN_MARK,
            open_item_lines(&out, open),
        ) {
            out = b;
        }
    }
    (out != body).then_some(out)
}

// ── settling one pull request ───────────────────────────────────────────

/// One pull request to decide.
pub struct Ctx<'a> {
    pub state: &'a WorkerState,
    pub github: &'a GitHubClient,
    pub team_id: &'a str,
    pub owner: &'a str,
    pub repo: &'a str,
    pub pr: u64,
    /// The run whose pull request it is; looked up when empty.
    pub run_id: String,
    /// Squash before marking ready, under the first commit's message when
    /// true (the run's own completion). `None`: no squash.
    pub squash_keep_first: Option<bool>,
    /// The end-to-end proof passed (its repeat runs held).
    pub proven: bool,
    pub e2e: E2e,
    /// Tries GitHub refused so far.
    pub attempt: u32,
}

impl<'a> Ctx<'a> {
    pub fn new(
        state: &'a WorkerState,
        github: &'a GitHubClient,
        team_id: &'a str,
        owner: &'a str,
        repo: &'a str,
        pr: u64,
    ) -> Self {
        Ctx {
            state,
            github,
            team_id,
            owner,
            repo,
            pr,
            run_id: String::new(),
            squash_keep_first: None,
            proven: false,
            e2e: E2e::None,
            attempt: 0,
        }
    }
}

/// GitHub didn't answer a read or the mark-ready the decision needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused {
    /// What failed, in plain words.
    pub what: String,
    /// How long to wait before the next try.
    pub wait_secs: u64,
}

impl Refused {
    fn of(what: &str, e: &(dyn std::error::Error + 'static), attempt: u32) -> Self {
        let wait_secs = match crate::clients::github::outside_wait(e) {
            Some((_, w)) => w,
            None => backoff_secs(attempt),
        };
        let reason = match crate::clients::github::outside_wait(e) {
            Some((common::github_wait::Cause::RateLimit, _)) => "GitHub's rate limit",
            Some((common::github_wait::Cause::Outage, _)) => "GitHub not answering",
            None => "GitHub refused the request",
        };
        Refused {
            what: format!("{what} ({reason})"),
            wait_secs,
        }
    }
}

/// The wait before try `attempt + 1` when GitHub named no reset: one minute,
/// doubling, at most one SQS delay.
pub fn backoff_secs(attempt: u32) -> u64 {
    BASE_RETRY_SECS
        .saturating_mul(1u64 << attempt.min(10))
        .min(MAX_DELAY_SECS)
}

/// What happened, for the caller's log and the tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ready,
    Held,
    Waiting,
    /// Held on a check older code wrote: the run checks it again.
    Rechecking,
    Closed,
    /// Sent again for later (GitHub refused for a while).
    Retrying,
    /// Every try was refused; recorded on the run.
    GaveUp,
}

/// Decides one pull request and does what the decision says: marks it
/// ready, or records why it stays a draft on the description, the run and
/// the log. Never silent, never lost.
pub async fn settle(ctx: &Ctx<'_>) -> Outcome {
    if ctx.pr == 0 {
        return Outcome::Closed;
    }
    let run_id = if ctx.run_id.is_empty() {
        super::review_pr::lookup_run_by_pr(ctx.state, ctx.team_id, ctx.owner, ctx.repo, ctx.pr)
            .await
            .unwrap_or_default()
    } else {
        ctx.run_id.clone()
    };
    let pr_url = format!(
        "https://github.com/{}/{}/pull/{}",
        ctx.owner, ctx.repo, ctx.pr
    );
    let facts = match gather(ctx).await {
        Ok(f) => f,
        Err(r) => return retry_or_give_up(ctx, &run_id, &pr_url, r).await,
    };
    let decision = decide(&facts);
    match &decision {
        Decision::Closed => {
            info!(pr = ctx.pr, "PR is closed — not marking ready");
            record_settled(ctx.state, ctx.team_id, &run_id, "closed").await;
            Outcome::Closed
        }
        Decision::Ready { open } => {
            if facts.draft {
                info!(pr = ctx.pr, open = ?open, "Marking PR ready");
                if let Err(r) = make_ready(ctx, &facts).await {
                    return retry_or_give_up(ctx, &run_id, &pr_url, r).await;
                }
                info!(pr = ctx.pr, open = open.len(), "PR marked ready");
            } else {
                info!(pr = ctx.pr, "PR already not a draft");
            }
            write_body(ctx, &facts.body, &decision).await;
            record_settled(ctx.state, ctx.team_id, &run_id, "ready").await;
            Outcome::Ready
        }
        Decision::Held { .. }
            if recheck_due(&facts, &decision, super::proof::checks_version())
                && super::recheck::start(
                    ctx.state,
                    ctx.team_id,
                    &run_id,
                    ctx.pr,
                    &facts.body,
                    super::proof::checks_version(),
                )
                .await =>
        {
            info!(pr = ctx.pr, run_id = %run_id, "The PR's hold was written by older checks: checking it again");
            let wait = Decision::Wait {
                why: RECHECK_WHY.to_string(),
                until: None,
            };
            write_body(ctx, &facts.body, &wait).await;
            if let Some(h) = DraftHold::new(
                Kind::Waiting,
                vec![Reason::new(RECHECK_WHY, "")],
                ctx.pr,
                &pr_url,
            ) {
                record_hold(ctx.state, ctx.team_id, &run_id, h).await;
            }
            Outcome::Rechecking
        }
        Decision::Held { reasons } => {
            let lines: Vec<String> = reasons.iter().map(Reason::line).collect();
            warn!(pr = ctx.pr, run_id = %run_id, reasons = ?lines, "The PR stays a draft");
            write_body(ctx, &facts.body, &decision).await;
            if let Some(h) = DraftHold::new(Kind::Held, reasons.clone(), ctx.pr, &pr_url) {
                record_hold(ctx.state, ctx.team_id, &run_id, h).await;
            }
            Outcome::Held
        }
        Decision::Wait { why, .. } => {
            info!(pr = ctx.pr, run_id = %run_id, why = %why, "The PR stays a draft for now");
            write_body(ctx, &facts.body, &decision).await;
            if let Some(h) =
                DraftHold::new(Kind::Waiting, vec![Reason::new(why, "")], ctx.pr, &pr_url)
            {
                record_hold(ctx.state, ctx.team_id, &run_id, h).await;
            }
            Outcome::Waiting
        }
    }
}

/// Reads what [`decide`] needs. A closed or ready pull request is decided
/// from the pull request alone, so nothing with side effects runs for it
/// (the failing-test check starts workflow runs, the link check rewrites
/// the description).
async fn gather(ctx: &Ctx<'_>) -> Result<Facts, Refused> {
    let gh = ctx.github;
    let read_pr = || async {
        gh.get_pull_request(ctx.owner, ctx.repo, ctx.pr)
            .await
            .map_err(|e| Refused::of("the pull request couldn't be read", &*e, ctx.attempt))
    };
    let pr = read_pr().await?;
    let mut f = Facts {
        closed: pr["state"].as_str() == Some("closed"),
        draft: pr["draft"].as_bool().unwrap_or(false),
        proven: ctx.proven,
        e2e: ctx.e2e,
        ..Default::default()
    };
    if f.closed || !f.draft {
        f.body = pr["body"].as_str().unwrap_or("").to_string();
        f.title = pr["title"].as_str().unwrap_or("").to_string();
        return Ok(f);
    }
    // A person putting it back to draft is their call. A timeline that
    // can't be read is tried again, never taken as "leave it".
    let timeline = gh
        .list_issue_timeline(ctx.owner, ctx.repo, ctx.pr)
        .await
        .map_err(|e| {
            Refused::of(
                "the pull request's timeline couldn't be read",
                &*e,
                ctx.attempt,
            )
        })?;
    f.put_back_by = crate::clients::github::human_converted_to_draft(&timeline);
    if f.put_back_by.is_none() {
        // Asking people to merge: the redirects and links are checked again
        // and the description's table updated.
        f.links_blocking = super::link_checks::refresh_pr(
            ctx.state,
            gh,
            ctx.team_id,
            ctx.owner,
            ctx.repo,
            ctx.pr,
            "",
            true,
        )
        .await
        .map(|r| r.blocking)
        .unwrap_or_default();
        f.test_fix = match super::test_fix::ready_gate(
            ctx.state,
            gh,
            ctx.team_id,
            ctx.owner,
            ctx.repo,
            ctx.pr,
        )
        .await
        {
            TestHold::Go => None,
            h => Some(h),
        };
        f.feature =
            super::feature::ready_hold(ctx.state, gh, ctx.team_id, ctx.owner, ctx.repo, ctx.pr)
                .await;
    }
    // The description as it is now (the link check may have rewritten it).
    let pr = read_pr().await?;
    f.closed = pr["state"].as_str() == Some("closed");
    f.draft = pr["draft"].as_bool().unwrap_or(f.draft);
    f.body = pr["body"].as_str().unwrap_or("").to_string();
    f.title = pr["title"].as_str().unwrap_or("").to_string();
    if reads_ci(&f) {
        let sha = pr["head"]["sha"].as_str().unwrap_or("");
        f.ci = ci_on(gh, ctx.owner, ctx.repo, sha).await.map_err(|e| {
            Refused::of(
                "the pull request's checks couldn't be read",
                &*e,
                ctx.attempt,
            )
        })?;
        f.approved = matches!(
            common::merge_gate::bot_verdict_at_head(
                &ctx.state.dynamo,
                &ctx.state.config.settings_table_name,
                ctx.team_id,
                ctx.owner,
                ctx.repo,
                ctx.pr,
                sha,
            )
            .await,
            Ok(Some(v)) if v == "APPROVE"
        );
    }
    Ok(f)
}

/// Whether the decision needs the repository's CI on the head: CoderHelm's
/// own feature findings weigh against it, and so does a description line for
/// a check CoderHelm's own tools couldn't run (CI green settles it, see
/// `proof::holds_blocked_on`). CI was once read only for the first: a pull
/// request whose one hold was such a line read CI as "not green" without
/// asking, and stayed a draft on every sweep although its CI had passed.
pub fn reads_ci(f: &Facts) -> bool {
    f.feature.is_some() || super::proof::ci_settles_a_hold(&f.body)
}

/// Whether a check is CoderHelm's own (its review check, or any check its
/// app reports): never the repository's CI.
fn own_check(c: &serde_json::Value) -> bool {
    let name = c["name"].as_str().or(c["context"].as_str()).unwrap_or("");
    name == common::review_check::CHECK_NAME
        || super::is_own_login(name)
        || super::is_own_login(c["app"]["slug"].as_str().unwrap_or(""))
}

/// The repository's own CI on a head, from its check runs and statuses.
pub fn ci_standing(check_runs: &[serde_json::Value], statuses: &[serde_json::Value]) -> Ci {
    let runs: Vec<serde_json::Value> = check_runs
        .iter()
        .filter(|c| !own_check(c))
        .cloned()
        .collect();
    let statuses: Vec<serde_json::Value> =
        statuses.iter().filter(|c| !own_check(c)).cloned().collect();
    let checks = common::e2e::check_gate::checks(&runs, &statuses);
    if checks.is_empty() {
        return Ci {
            green: false,
            why: "the repository reports no CI checks on this head".into(),
        };
    }
    let names = |v: Vec<&str>| {
        v.iter()
            .map(|n| format!("`{n}`"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let failed: Vec<&str> = checks
        .iter()
        .filter(|c| {
            c.status == "completed"
                && !matches!(c.conclusion.as_str(), "success" | "skipped" | "neutral")
        })
        .map(|c| c.name.as_str())
        .collect();
    if !failed.is_empty() {
        return Ci {
            green: false,
            why: format!("CI fails on this head: {}", names(failed)),
        };
    }
    let running: Vec<&str> = checks
        .iter()
        .filter(|c| c.status != "completed")
        .map(|c| c.name.as_str())
        .collect();
    if !running.is_empty() {
        return Ci {
            green: false,
            why: format!("CI is still running on this head: {}", names(running)),
        };
    }
    Ci {
        green: true,
        why: String::new(),
    }
}

async fn ci_on(
    gh: &GitHubClient,
    owner: &str,
    repo: &str,
    sha: &str,
) -> Result<Ci, Box<dyn std::error::Error + Send + Sync>> {
    let runs = gh.list_check_runs_for_ref(owner, repo, sha).await?;
    let statuses = gh.get_commit_status(owner, repo, sha).await?;
    Ok(ci_standing(
        runs["check_runs"].as_array().map_or(&[][..], |a| a),
        statuses["statuses"].as_array().map_or(&[][..], |a| a),
    ))
}

/// Marks the draft ready: the "Partial" title marker goes, the run's
/// history is squashed when asked (only while it is still CoderHelm's
/// draft), then GitHub's ready-for-review.
async fn make_ready(ctx: &Ctx<'_>, f: &Facts) -> Result<(), Refused> {
    let gh = ctx.github;
    let pr = gh
        .get_pull_request(ctx.owner, ctx.repo, ctx.pr)
        .await
        .map_err(|e| Refused::of("the pull request couldn't be read", &*e, ctx.attempt))?;
    // A PR reaching ready is no longer "partial".
    if let Some(clean) = f.title.strip_prefix("⚠️ Partial: ") {
        let _ = gh.update_pr_title(ctx.owner, ctx.repo, ctx.pr, clean).await;
    }
    // Final squash: the run's whole history (the implementation and every
    // fix round) as one clean commit, best-effort.
    if let (Some(keep_first), Some(head), Some(base)) = (
        ctx.squash_keep_first,
        pr["head"]["ref"].as_str(),
        pr["base"]["ref"].as_str(),
    ) {
        let title = if f.title.is_empty() {
            "Apply changes"
        } else {
            f.title.as_str()
        };
        let mut squash_msg = title
            .strip_prefix("⚠️ Partial: ")
            .unwrap_or(title)
            .to_string();
        if keep_first {
            if let Ok(commits) = gh.list_pr_commits(ctx.owner, ctx.repo, ctx.pr).await {
                if let Some(m) = super::resume::first_own_commit_message(&commits) {
                    squash_msg = m;
                }
            }
        }
        // A commit message an agent wrote may carry a stand-in key (`ABC-0`);
        // the PR title carries the real one.
        let squash_msg = common::git_conventions::normalize_message(
            &squash_msg,
            common::git_conventions::subject_key(title).as_deref(),
        );
        if let Err(e) = gh
            .squash_branch(ctx.owner, ctx.repo, head, base, &squash_msg)
            .await
        {
            warn!(pr = ctx.pr, error = %e, "Final squash before mark-ready failed — proceeding with full history");
        }
    }
    let Some(node_id) = pr["node_id"].as_str() else {
        return Err(Refused {
            what: "GitHub's answer for the pull request had no node id".into(),
            wait_secs: backoff_secs(ctx.attempt),
        });
    };
    gh.mark_pr_ready(node_id)
        .await
        .map(|_| ())
        .map_err(|e| Refused::of("GitHub didn't mark it ready", &*e, ctx.attempt))
}

async fn write_body(ctx: &Ctx<'_>, body: &str, d: &Decision) {
    let Some(new) = body_for(body, d) else {
        return;
    };
    if let Err(e) = ctx
        .github
        .update_pr_body(ctx.owner, ctx.repo, ctx.pr, &new)
        .await
    {
        warn!(pr = ctx.pr, error = %e, "Could not write the draft status to the description");
    }
}

/// GitHub refused for a while: the decision is sent again after its reset,
/// or, once every try is taken (or it can't be sent), recorded on the run.
async fn retry_or_give_up(ctx: &Ctx<'_>, run_id: &str, pr_url: &str, r: Refused) -> Outcome {
    let attempt = ctx.attempt + 1;
    if attempt <= common::github_wait::MAX_WAITS {
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        let msg = MarkReadyMessage {
            team_id: ctx.team_id.to_string(),
            installation_id: ctx.github.installation_id(),
            repo_owner: ctx.owner.to_string(),
            repo_name: ctx.repo.to_string(),
            pr_number: ctx.pr,
            run_id: run_id.to_string(),
            attempt,
            not_before: common::github_wait::until(now, r.wait_secs),
            squash_keep_first: ctx.squash_keep_first,
        };
        let delay = r.wait_secs.clamp(1, MAX_DELAY_SECS);
        if send(ctx.state, &msg, delay).await {
            warn!(pr = ctx.pr, attempt, wait_secs = r.wait_secs, what = %r.what, "Mark-ready refused by GitHub — trying again later");
            if let Some(h) = DraftHold::new(
                Kind::Waiting,
                vec![Reason::new(
                    format!(
                        "{}; CoderHelm tries again at {}",
                        r.what,
                        common::github_wait::clock(msg.not_before)
                    ),
                    "",
                )],
                ctx.pr,
                pr_url,
            ) {
                record_hold(ctx.state, ctx.team_id, run_id, h).await;
            }
            return Outcome::Retrying;
        }
    }
    error!(pr = ctx.pr, attempt = ctx.attempt, what = %r.what, "Could not mark the PR ready after every try");
    let what = format!("{} on each of {} tries", r.what, ctx.attempt.max(1));
    if let Ok(pr) = ctx
        .github
        .get_pull_request(ctx.owner, ctx.repo, ctx.pr)
        .await
    {
        let body = pr["body"].as_str().unwrap_or("");
        let new = super::completeness::with_section(
            body,
            super::completeness::Section::DraftStatus,
            failed_section(&what),
        );
        if new != body {
            let _ = ctx
                .github
                .update_pr_body(ctx.owner, ctx.repo, ctx.pr, &new)
                .await;
        }
    }
    if let Some(h) = DraftHold::new(
        Kind::Failed,
        vec![Reason::new(
            what,
            "Mark it ready for review on GitHub yourself; CoderHelm also tries again within the \
             hour.",
        )],
        ctx.pr,
        pr_url,
    ) {
        record_hold(ctx.state, ctx.team_id, run_id, h).await;
    }
    Outcome::GaveUp
}

async fn send(state: &WorkerState, msg: &MarkReadyMessage, delay: u64) -> bool {
    let Ok(body) = serde_json::to_string(&WorkerMessage::MarkReady(MarkReadyMessage {
        team_id: msg.team_id.clone(),
        installation_id: msg.installation_id,
        repo_owner: msg.repo_owner.clone(),
        repo_name: msg.repo_name.clone(),
        pr_number: msg.pr_number,
        run_id: msg.run_id.clone(),
        attempt: msg.attempt,
        not_before: msg.not_before,
        squash_keep_first: msg.squash_keep_first,
    })) else {
        return false;
    };
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .delay_seconds(delay as i32)
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            warn!(pr = msg.pr_number, error = %e, "Could not queue the next mark-ready try");
            false
        }
    }
}

/// The mark-ready message: CI passed on a finished run's branch, a
/// failing-test fix's recheck, or a try GitHub refused earlier.
pub async fn on_message(state: &WorkerState, msg: MarkReadyMessage) {
    // A wait longer than one SQS delay goes on in hops until its time.
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    if let Some(hop) = common::github_wait::hop(msg.not_before, now) {
        let pr = msg.pr_number;
        if !send(state, &msg, hop).await {
            error!(pr, "Could not send the mark-ready wait on; deciding now");
        } else {
            return;
        }
    }
    let github = match GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    ) {
        Ok(g) => g,
        Err(e) => {
            error!(pr = msg.pr_number, error = %e, "No GitHub client for mark-ready");
            return;
        }
    };
    let mut ctx = Ctx::new(
        state,
        &github,
        &msg.team_id,
        &msg.repo_owner,
        &msg.repo_name,
        msg.pr_number,
    );
    ctx.run_id = msg.run_id.clone();
    ctx.attempt = msg.attempt;
    ctx.squash_keep_first = msg.squash_keep_first;
    let outcome = settle(&ctx).await;
    info!(pr = msg.pr_number, ?outcome, "Mark-ready decided");
}

// ── the run record ──────────────────────────────────────────────────────

fn conditional_failed<E: std::fmt::Debug>(e: &aws_sdk_dynamodb::error::SdkError<E>) -> bool {
    format!("{e:?}").contains("ConditionalCheckFailed")
}

/// The run's status and hold, `None` when it can't be read.
async fn run_hold(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
) -> Option<(String, Option<DraftHold>)> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .projection_expression("#s, draft_hold")
        .expression_attribute_names("#s", "status")
        .consistent_read(true)
        .send()
        .await
        .ok()?
        .item?;
    let status = item
        .get("status")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    Some((status, DraftHold::of_item(&item)))
}

/// Records a hold on the run. A hold a person must act on sets a finished
/// run to `needs_input`, naming what to do; a run still working keeps its
/// status and gets the hold when it finishes (`after_completion`).
async fn record_hold(state: &WorkerState, team_id: &str, run_id: &str, hold: DraftHold) {
    if run_id.is_empty() {
        warn!(pr = hold.pr_number, summary = %hold.summary(), "No run found for the PR; the hold is in the description and the log only");
        return;
    }
    let before = run_hold(state, team_id, run_id).await;
    let hold = hold.continuing(before.as_ref().and_then(|(_, h)| h.as_ref()));
    let now = chrono::Utc::now().to_rfc3339();
    // `:held` is a condition value, never a status written here: the write
    // goes through `NeedsInput::needs_input`.
    let held = AttributeValue::S(common::run_status::NEEDS_INPUT.to_string());
    let (was, same) = match &before {
        Some((status, prev)) => (
            status.clone(),
            prev.as_ref()
                .is_some_and(|p| p.kind == hold.kind && p.reasons == hold.reasons),
        ),
        None => (String::new(), false),
    };
    let finished = matches!(
        was.as_str(),
        common::run_status::COMPLETED | common::run_status::SUCCESS
    );
    let rewaiting = was == common::run_status::NEEDS_INPUT && !same;
    if hold.needs_person() && (finished || rewaiting) {
        // Handing the finished run to a person is a costly decision, so it
        // goes through the stop judge's record. Nothing a model could weigh
        // is left: `decide` already kept only what a person must act on.
        let judged = super::stop_judge::cannot_proceed(
            state,
            team_id,
            &super::stop_judge::run_subject(run_id),
            super::stop_judge::Kind::Handoff,
            "draft_hold",
            &hold.summary(),
        )
        .await;
        if wait_on_person(state, team_id, run_id, &hold, &was, &judged).await {
            return;
        }
    } else if hold.needs_person() {
        // Still working (the run writes its status next and
        // `after_completion` picks the hold up), or already waiting on
        // these same reasons: the hold alone, below.
    } else {
        // A wait: a run a hold had set to needs_input is finished again.
        let res = state
            .dynamo
            .update_item()
            .table_name(&state.config.runs_table_name)
            .key("team_id", attr_s(team_id))
            .key("run_id", attr_s(run_id))
            .update_expression(
                "SET #s = :done, status_run_id = :sri, draft_hold = :h, updated_at = :t \
                 REMOVE waiting_on, draft_settled",
            )
            .condition_expression("#s = :held AND attribute_exists(draft_hold)")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":done", attr_s(common::run_status::COMPLETED))
            .expression_attribute_values(":sri", attr_s(&format!("completed#{run_id}")))
            .expression_attribute_values(":h", hold.to_attr())
            .expression_attribute_values(":t", attr_s(&now))
            .expression_attribute_values(":held", held)
            .send()
            .await;
        match res {
            Ok(_) => return,
            Err(e) if conditional_failed(&e) => {}
            Err(e) => {
                warn!(run_id, error = %e, "Could not record the draft wait on the run");
                return;
            }
        }
    }
    // Still working, or a status nothing here changes: the hold alone.
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET draft_hold = :h, updated_at = :t REMOVE draft_settled")
        .condition_expression(format!(
            "attribute_exists(run_id) AND {}",
            common::run_status::not_ended("#s")
        ))
        .expression_attribute_names("#s", "status")
        .ended_values()
        .expression_attribute_values(":h", hold.to_attr())
        .expression_attribute_values(":t", attr_s(&now))
        .send()
        .await;
    if let Err(e) = res {
        if !conditional_failed(&e) {
            warn!(run_id, error = %e, "Could not record the draft hold on the run");
        }
    }
}

/// Sets a finished run to `needs_input` on its draft hold, naming what to
/// do. `false` when the run's status isn't one this changes.
async fn wait_on_person(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    hold: &DraftHold,
    was: &str,
    _judged: &super::stop_judge::Judged,
) -> bool {
    let now = chrono::Utc::now().to_rfc3339();
    // `:held` is a condition value, never a status written here: the write
    // goes through `NeedsInput::needs_input`.
    let held = AttributeValue::S(common::run_status::NEEDS_INPUT.to_string());
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, waiting_on = :wo, draft_hold = :h, updated_at = :t \
             REMOVE draft_settled",
        )
        .condition_expression(
            "#s IN (:done, :old_done) OR (#s = :held AND attribute_exists(draft_hold))",
        )
        .expression_attribute_names("#s", "status")
        .needs_input(run_id, &hold.waiting_on())
        .expression_attribute_values(":h", hold.to_attr())
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":done", attr_s(common::run_status::COMPLETED))
        .expression_attribute_values(":old_done", attr_s(common::run_status::SUCCESS))
        .expression_attribute_values(":held", held)
        .send()
        .await;
    match res {
        Ok(_) => {
            if was != common::run_status::NEEDS_INPUT {
                super::webhooks::emit_run(state, team_id, run_id, common::run_status::NEEDS_INPUT)
                    .await;
            }
            true
        }
        Err(e) if conditional_failed(&e) => false,
        Err(e) => {
            warn!(run_id, error = %e, "Could not record the draft hold on the run");
            true
        }
    }
}

/// The pull request left draft (`ready`) or was closed: the hold goes, and
/// a run a hold had set to needs_input is finished again.
async fn record_settled(state: &WorkerState, team_id: &str, run_id: &str, how: &str) {
    if run_id.is_empty() {
        return;
    }
    let now = chrono::Utc::now().to_rfc3339();
    let held = AttributeValue::S(common::run_status::NEEDS_INPUT.to_string());
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #s = :done, status_run_id = :sri, draft_settled = :ds, updated_at = :t \
             REMOVE draft_hold, waiting_on",
        )
        .condition_expression("#s = :held AND attribute_exists(draft_hold)")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":done", attr_s(common::run_status::COMPLETED))
        .expression_attribute_values(":sri", attr_s(&format!("completed#{run_id}")))
        .expression_attribute_values(":ds", attr_s(how))
        .expression_attribute_values(":t", attr_s(&now))
        .expression_attribute_values(":held", held)
        .send()
        .await;
    match res {
        Ok(_) => return,
        Err(e) if conditional_failed(&e) => {}
        Err(e) => {
            warn!(run_id, error = %e, "Could not record that the PR is settled");
            return;
        }
    }
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(team_id))
        .key("run_id", attr_s(run_id))
        .update_expression("SET draft_settled = :ds REMOVE draft_hold")
        .condition_expression("attribute_exists(run_id)")
        .expression_attribute_values(":ds", attr_s(how))
        .send()
        .await;
    if let Err(e) = res {
        if !conditional_failed(&e) {
            warn!(run_id, error = %e, "Could not record that the PR is settled");
        }
    }
}

/// A run just finished as `completed`: a hold recorded while it was still
/// working (the decision runs before the status is written) sets it to
/// `needs_input` now.
pub async fn after_completion(state: &WorkerState, team_id: &str, run_id: &str) {
    let Some((status, Some(hold))) = run_hold(state, team_id, run_id).await else {
        return;
    };
    if status != common::run_status::COMPLETED || !hold.needs_person() {
        return;
    }
    warn!(run_id, summary = %hold.summary(), "Run finished with its PR held as a draft");
    record_hold(state, team_id, run_id, hold).await;
}

// ── the sweep ───────────────────────────────────────────────────────────

/// One finished run the sweep looks at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub team_id: String,
    pub run_id: String,
    pub status: String,
    pub owner: String,
    pub repo: String,
    pub pr: u64,
    pub installation_id: u64,
    pub hold: Option<DraftHold>,
}

/// What the sweep does with a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepStep {
    /// Decide again in full.
    Decide,
    /// Its hold is recent: only look whether the pull request left draft or
    /// was closed (a person marked it ready, or closed it).
    Peek,
    /// Nothing to do (waiting on a person for something else).
    Skip,
}

/// Pure: what the sweep does with a candidate at `now`.
pub fn sweep_step(c: &Candidate, now: chrono::DateTime<chrono::Utc>) -> SweepStep {
    let finished = matches!(
        c.status.as_str(),
        common::run_status::COMPLETED | common::run_status::SUCCESS
    );
    match (
        &c.hold,
        finished,
        c.status == common::run_status::NEEDS_INPUT,
    ) {
        // Finished with no recorded reason: the silent case. Decide.
        (None, true, _) => SweepStep::Decide,
        // Waiting on a person for something that isn't the draft.
        (None, false, _) => SweepStep::Skip,
        (Some(h), _, _) if h.stale(RECHECK_HELD_SECS, now) => SweepStep::Decide,
        (Some(_), true, _) | (Some(_), _, true) => SweepStep::Peek,
        (Some(_), false, false) => SweepStep::Skip,
    }
}

fn candidate_of(item: &HashMap<String, AttributeValue>) -> Option<Candidate> {
    let s = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
    let n = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_n().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let pr = n("pr_number");
    let (owner, repo) = s("pr_url")
        .and_then(|u| common::pr_outcome::parse_pr_url(&u))
        .filter(|(_, _, num)| *num == pr)
        .map(|(o, r, _)| (o, r))
        .or_else(|| {
            let full = s("repo")?;
            let (o, r) = full.split_once('/')?;
            Some((o.to_string(), r.to_string()))
        })?;
    (pr > 0).then(|| Candidate {
        team_id: s("team_id").unwrap_or_default(),
        run_id: s("run_id").unwrap_or_default(),
        status: s("status").unwrap_or_default(),
        owner,
        repo,
        pr,
        installation_id: n("installation_id"),
        hold: DraftHold::of_item(item),
    })
}

/// What one sweep did.
#[derive(Debug, Default)]
pub struct SweepReport {
    pub looked_at: usize,
    pub decided: usize,
    /// `owner/repo#pr: outcome` for each one decided.
    pub settled: Vec<String>,
}

/// Finished runs (in the last [`SWEEP_WINDOW_DAYS`]) whose pull request
/// isn't settled: each is decided again, so a pull request left a draft
/// with no recorded reason heals by itself. Runs with a recent hold are
/// only looked at, to notice a person marking the pull request ready.
pub async fn sweep(state: &WorkerState) -> SweepReport {
    let mut report = SweepReport::default();
    let now = chrono::Utc::now();
    let cutoff = (now - chrono::Duration::days(SWEEP_WINDOW_DAYS)).to_rfc3339();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    let mut candidates = Vec::new();
    for _ in 0..200 {
        let res = state
            .dynamo
            .scan()
            .table_name(&state.config.runs_table_name)
            .filter_expression(
                "#s IN (:done, :old_done, :held) AND pr_number > :zero AND updated_at >= :cut \
                 AND attribute_not_exists(draft_settled)",
            )
            .projection_expression(
                "team_id, run_id, #s, pr_number, pr_url, repo, installation_id, draft_hold",
            )
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":done", attr_s(common::run_status::COMPLETED))
            .expression_attribute_values(":old_done", attr_s(common::run_status::SUCCESS))
            .expression_attribute_values(
                ":held",
                AttributeValue::S(common::run_status::NEEDS_INPUT.to_string()),
            )
            .expression_attribute_values(":zero", attr_n(0))
            .expression_attribute_values(":cut", attr_s(&cutoff))
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let page = match res {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Draft sweep scan failed");
                break;
            }
        };
        candidates.extend(page.items().iter().filter_map(candidate_of));
        match page.last_evaluated_key {
            Some(k) if !k.is_empty() => start = Some(k),
            _ => break,
        }
    }
    let mut clients: HashMap<u64, GitHubClient> = HashMap::new();
    for c in candidates {
        if report.decided >= SWEEP_MAX_DECISIONS {
            break;
        }
        let step = sweep_step(&c, now);
        if step == SweepStep::Skip || c.installation_id == 0 || c.team_id.is_empty() {
            continue;
        }
        report.looked_at += 1;
        if let std::collections::hash_map::Entry::Vacant(v) = clients.entry(c.installation_id) {
            match GitHubClient::new(
                &state.secrets.github_app_id,
                &state.secrets.github_private_key,
                c.installation_id,
                &state.http,
            ) {
                Ok(g) => {
                    v.insert(g);
                }
                Err(e) => {
                    warn!(installation = c.installation_id, error = %e, "Draft sweep: no GitHub client");
                    continue;
                }
            }
        }
        let gh = &clients[&c.installation_id];
        if step == SweepStep::Peek {
            match gh.get_pull_request(&c.owner, &c.repo, c.pr).await {
                Ok(p)
                    if p["state"].as_str() == Some("open")
                        && p["draft"].as_bool() == Some(true) =>
                {
                    continue;
                }
                Ok(_) => {}
                Err(_) => continue,
            }
        }
        let mut ctx = Ctx::new(state, gh, &c.team_id, &c.owner, &c.repo, c.pr);
        ctx.run_id = c.run_id.clone();
        let outcome = settle(&ctx).await;
        report.decided += 1;
        report
            .settled
            .push(format!("{}/{}#{}: {outcome:?}", c.owner, c.repo, c.pr));
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn green_approved() -> Facts {
        Facts {
            draft: true,
            ci: Ci {
                green: true,
                why: String::new(),
            },
            approved: true,
            ..Default::default()
        }
    }

    fn sandbox_fails() -> FeatureHold {
        FeatureHold {
            sandbox_failed: vec![
                "npm run test:unit: 2 failed".into(),
                "npm run lint: 1 error".into(),
            ],
            checklist_fails: vec![],
        }
    }

    #[test]
    fn sandbox_failures_dont_hold_a_pr_whose_ci_is_green_and_review_approved() {
        let f = Facts {
            feature: Some(sandbox_fails()),
            ..green_approved()
        };
        match decide(&f) {
            Decision::Ready { open } => {
                assert_eq!(open.len(), 2);
                assert!(open[0].starts_with("CoderHelm's sandbox check failed: npm run test:unit"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_checklist_item_doesnt_hold_either() {
        let f = Facts {
            feature: Some(FeatureHold {
                sandbox_failed: vec![],
                checklist_fails: vec!["test map entry: not listed: e2e/map.json: a.spec.ts".into()],
            }),
            ..green_approved()
        };
        assert!(matches!(decide(&f), Decision::Ready { open } if open.len() == 1));
    }

    #[test]
    fn without_green_ci_or_an_approval_coderhelms_own_findings_hold_with_the_reason_named() {
        let f = Facts {
            feature: Some(sandbox_fails()),
            ci: Ci {
                green: false,
                why: "CI fails on this head: `test`".into(),
            },
            approved: true,
            draft: true,
            ..Default::default()
        };
        match decide(&f) {
            Decision::Held { reasons } => {
                assert_eq!(reasons.len(), 1);
                assert!(reasons[0].what.contains("npm run test:unit"));
                assert!(reasons[0].what.contains("CI fails on this head: `test`"));
                assert!(reasons[0].todo.contains("mark it ready yourself"));
            }
            other => panic!("{other:?}"),
        }
        let f = Facts {
            feature: Some(sandbox_fails()),
            approved: false,
            ..green_approved()
        };
        assert!(matches!(decide(&f), Decision::Held { reasons }
            if reasons[0].what.contains("review hasn't approved")));
    }

    #[test]
    fn what_a_person_must_decide_keeps_the_draft_even_with_green_ci_and_an_approval() {
        let security = format!(
            "## Summary\nx\n\n{}\n## Open items\n- The token check. {}\n{}",
            crate::passes::proof::UNVERIFIED_START,
            crate::passes::proof::SECURITY_MARK,
            crate::passes::proof::UNVERIFIED_END
        );
        let f = Facts {
            body: security,
            ..green_approved()
        };
        assert!(matches!(decide(&f), Decision::Held { reasons }
            if reasons[0].what.contains("Security items")));

        let f = Facts {
            put_back_by: Some("dana".into()),
            ..green_approved()
        };
        assert!(matches!(decide(&f), Decision::Held { reasons }
            if reasons[0].what == "`dana` put it back to draft"));

        let f = Facts {
            test_fix: Some(TestHold::Refused(
                "it adds what the failing-test rules refuse: a wait".into(),
            )),
            ..green_approved()
        };
        assert!(matches!(decide(&f), Decision::Held { .. }));

        let f = Facts {
            links_blocking: vec!["/old redirects to a 404".into()],
            ..green_approved()
        };
        assert!(matches!(decide(&f), Decision::Held { reasons }
            if reasons[0].what.contains("/old redirects to a 404")));

        let f = Facts {
            e2e: E2e::Refused,
            ..green_approved()
        };
        assert!(matches!(decide(&f), Decision::Held { .. }));

        let f = Facts {
            test_fix: Some(TestHold::GaveUp("the repeat run never passed".into())),
            ..green_approved()
        };
        assert!(matches!(decide(&f), Decision::Held { reasons }
            if reasons[0].todo.contains("mark it ready yourself")));
    }

    #[test]
    fn what_settles_by_itself_is_a_wait() {
        let f = Facts {
            test_fix: Some(TestHold::Wait("the repeat run hasn't passed yet".into())),
            ..green_approved()
        };
        assert!(matches!(decide(&f), Decision::Wait { why, .. } if why.contains("repeat run")));
        let f = Facts {
            e2e: E2e::Running,
            ..green_approved()
        };
        assert!(matches!(decide(&f), Decision::Wait { .. }));
        let f = Facts {
            title: format!("{}flaky login test", crate::passes::e2e::FIX_TICKET_PREFIX),
            ..green_approved()
        };
        assert!(matches!(decide(&f), Decision::Wait { .. }));
        let f = Facts { proven: true, ..f };
        assert!(matches!(decide(&f), Decision::Ready { .. }));
    }

    #[test]
    fn a_closed_pr_is_left_alone_and_a_ready_one_stays_ready() {
        let f = Facts {
            closed: true,
            put_back_by: Some("dana".into()),
            ..green_approved()
        };
        assert_eq!(decide(&f), Decision::Closed);
        let f = Facts {
            draft: false,
            ..green_approved()
        };
        assert_eq!(decide(&f), Decision::Ready { open: vec![] });
    }

    #[test]
    fn every_hold_is_named_on_the_description_and_goes_once_ready() {
        let held = Decision::Held {
            reasons: vec![Reason::new(
                "Security items are open under Open items",
                "Fix them.",
            )],
        };
        let body = body_for("## Summary\nx", &held).unwrap();
        assert!(body.starts_with(STATUS_START), "{body}");
        assert!(body.contains("**This pull request stays a draft.**"));
        assert!(body.contains("- Security items are open under Open items. Fix them."));
        // Written once.
        assert_eq!(body_for(&body, &held), None);
        // The status never reads as a hold marker of its own.
        assert!(body_holds(&body).is_empty());
        let ready = Decision::Ready {
            open: vec!["CoderHelm's sandbox check failed: npm test: 2 failed".into()],
        };
        let after = body_for(&body, &ready).unwrap();
        assert!(!after.contains(STATUS_START));
        assert!(after.contains("## Open items"));
        assert!(after.contains("npm test: 2 failed. CI is green and the review approved"));
        assert!(after.contains(crate::passes::proof::READY_OPEN_MARK));
        // Again: the same open item, once.
        assert_eq!(body_for(&after, &ready), None);
        let wait = Decision::Wait {
            why: "its repeat runs are going".into(),
            until: None,
        };
        assert!(body_for("x", &wait)
            .unwrap()
            .contains("stays a draft for now:** its repeat runs are going."));
    }

    #[test]
    fn an_open_item_the_rules_already_listed_isnt_listed_twice() {
        let body = format!(
            "{}\n## Open items\n- npm test: 2 failed Fix it on this branch.\n{}",
            crate::passes::proof::UNVERIFIED_START,
            crate::passes::proof::UNVERIFIED_END
        );
        let lines = open_item_lines(
            &body,
            &["CoderHelm's sandbox check failed: npm test: 2 failed".into()],
        );
        assert!(lines.is_empty());
    }

    #[test]
    fn ci_leaves_out_coderhelms_own_checks() {
        let run = |name: &str, conclusion: &str, slug: &str| {
            json!({"name": name, "status": "completed", "conclusion": conclusion,
                   "started_at": "2026-11-06T22:10:00Z", "app": {"slug": slug}})
        };
        let ci = ci_standing(
            &[
                run("build", "success", "github-actions"),
                run(common::review_check::CHECK_NAME, "failure", "coderhelm"),
            ],
            &[],
        );
        assert!(ci.green, "{ci:?}");
        let ci = ci_standing(&[run("build", "failure", "github-actions")], &[]);
        assert!(!ci.green && ci.why.contains("`build`"));
        let ci = ci_standing(
            &[run(
                common::review_check::CHECK_NAME,
                "success",
                "coderhelm",
            )],
            &[],
        );
        assert!(!ci.green && ci.why.contains("no CI checks"));
        let pending = json!({"name": "e2e", "status": "in_progress", "conclusion": null});
        assert!(ci_standing(&[pending], &[]).why.contains("still running"));
    }

    #[test]
    fn refused_tries_wait_longer_each_time_up_to_one_delay() {
        assert_eq!(backoff_secs(0), 60);
        assert_eq!(backoff_secs(1), 120);
        assert_eq!(backoff_secs(3), 480);
        assert_eq!(backoff_secs(4), MAX_DELAY_SECS);
        assert_eq!(backoff_secs(30), MAX_DELAY_SECS);
        // A rate limit waits for GitHub's reset.
        let e: Box<dyn std::error::Error + Send + Sync> =
            "HTTP status client error (403 Forbidden) for url (https://api.github.com/graphql): \
             API rate limit exceeded"
                .into();
        let r = Refused::of("GitHub didn't mark it ready", &*e, 0);
        assert!(r.what.contains("GitHub's rate limit"), "{r:?}");
        assert_eq!(r.wait_secs, 60);
    }

    #[test]
    fn the_mark_ready_message_carries_its_tries_and_stays_compatible() {
        let old = r#"{"type":"mark_ready","team_id":"T","installation_id":1,"repo_owner":"o","repo_name":"r","pr_number":5}"#;
        match serde_json::from_str::<WorkerMessage>(old).unwrap() {
            WorkerMessage::MarkReady(m) => {
                assert_eq!(m.attempt, 0);
                assert_eq!(m.not_before, 0);
                assert!(m.run_id.is_empty());
                assert_eq!(m.squash_keep_first, None);
            }
            other => panic!("{other:?}"),
        }
        let m = WorkerMessage::MarkReady(MarkReadyMessage {
            team_id: "T".into(),
            pr_number: 5,
            run_id: "01R".into(),
            attempt: 3,
            not_before: 99,
            squash_keep_first: Some(true),
            ..Default::default()
        });
        let back: WorkerMessage =
            serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        match back {
            WorkerMessage::MarkReady(b) => {
                assert_eq!((b.attempt, b.not_before, b.run_id.as_str()), (3, 99, "01R"));
                assert_eq!(b.squash_keep_first, Some(true));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_sweep_decides_silent_drafts_and_peeks_at_recent_holds() {
        let now = chrono::Utc::now();
        let c = Candidate {
            team_id: "T".into(),
            run_id: "01R".into(),
            status: "completed".into(),
            owner: "o".into(),
            repo: "r".into(),
            pr: 583,
            installation_id: 1,
            hold: None,
        };
        assert_eq!(sweep_step(&c, now), SweepStep::Decide);
        let held = DraftHold::new(Kind::Held, vec![Reason::new("a", "")], 583, "").unwrap();
        let c2 = Candidate {
            status: "needs_input".into(),
            hold: Some(held.clone()),
            ..c.clone()
        };
        assert_eq!(sweep_step(&c2, now), SweepStep::Peek);
        assert_eq!(
            sweep_step(&c2, now + chrono::Duration::seconds(RECHECK_HELD_SECS + 1)),
            SweepStep::Decide
        );
        // Waiting on a person for something that isn't the draft.
        let c3 = Candidate {
            status: "needs_input".into(),
            hold: None,
            ..c.clone()
        };
        assert_eq!(sweep_step(&c3, now), SweepStep::Skip);
    }

    #[test]
    fn a_candidate_is_read_from_its_pull_request_url() {
        let item: HashMap<String, AttributeValue> = [
            ("team_id", attr_s("T")),
            ("run_id", attr_s("01R")),
            ("status", attr_s("completed")),
            ("pr_number", attr_n(583)),
            ("pr_url", attr_s("https://github.com/acme/api/pull/583")),
            ("repo", attr_s("acme/web")),
            ("installation_id", attr_n(9)),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        let c = candidate_of(&item).unwrap();
        assert_eq!(
            (c.owner.as_str(), c.repo.as_str(), c.pr),
            ("acme", "api", 583)
        );
        assert_eq!(c.installation_id, 9);
    }
}
