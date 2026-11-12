//! A run that did the work opens its pull request, even when a check after
//! the work stopped it.
//!
//! A run once pushed every commit of a ticket's work, wrote its ticket note,
//! and ended without a pull request: a check after implementation (plan
//! coverage out of continuations, the new-feature rules, a link check, the
//! release check) returned an error, and only an error in the middle of
//! implementing was salvaged into a pull request. Every other error marked
//! the run failed, leaving the work on a branch nobody reviews.
//!
//! Now any error after implementation started, with work on the branch
//! (beyond the plan files CoderHelm commits beside it), opens a draft pull
//! request that says why the run stopped, as its open item; the run waits
//! for that pull request's review instead of failing, and says so by name
//! (`common::run_wait`): "Review pull request #n", with the open item. A stop for agent permissions (paused, an
//! approval nobody answered) still ends the run as it did.

use crate::clients::github::GitHubClient;
use crate::models::TicketMessage;
use crate::WorkerState;
use common::run_status::EndedValues;
use common::run_wait::{NeedsInput, WaitingOn};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The checkpoints after which a stopped run has work worth a pull request.
pub fn opens_pr(last_pass: &str) -> bool {
    matches!(last_pass, "implement_started" | "implement" | "security")
}

/// What the run did of its plan when it stopped: the tasks done and the
/// tasks left, each as the plan wrote it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Progress {
    pub done: Vec<String>,
    pub remaining: Vec<String>,
}

impl Progress {
    /// From the plan check's coverage of the branch.
    pub fn of(cov: &super::plan_coverage::Coverage) -> Progress {
        let line = |t: &super::plan_coverage::PlanTask| format!("`{}`: {}", t.path, t.task);
        Progress {
            done: cov.done.iter().map(line).collect(),
            remaining: cov.required_missing().into_iter().map(line).collect(),
        }
    }
}

/// The "Not finished" section of a run stopped before its work was done:
/// why it stopped (the recorded reason, in plain words), what is done, what
/// is left, and what a person can do. It keeps the pull request a draft
/// (`proof::holds_unfinished`).
pub fn not_finished(stop: &super::outcome::Stop, progress: &Progress, run_link: &str) -> String {
    let total = progress.done.len() + progress.remaining.len();
    let mut lines = vec![format!("- **Why it stopped:** {}.", stop.words())];
    if total > 0 {
        let done = if progress.done.is_empty() {
            "none".to_string()
        } else {
            progress.done.join("; ")
        };
        lines.push(format!(
            "- **Done ({} of {total}):** {}",
            progress.done.len(),
            common::truncate_str(&done, 1500)
        ));
        if !progress.remaining.is_empty() {
            lines.push(format!("- **Not done ({}):**", progress.remaining.len()));
            for (i, r) in progress.remaining.iter().take(15).enumerate() {
                lines.push(format!(
                    "  - [ ] task {} of {total}: {}",
                    progress.done.len() + i + 1,
                    common::truncate_str(r, 300)
                ));
            }
        }
    }
    lines.push(
        "- **What you can do:** finish what is left on this branch (CoderHelm picks up a push \
         or a comment here), comment with what to change, or delete this section if the work \
         left isn't needed. Then mark the pull request ready."
            .to_string(),
    );
    if !run_link.is_empty() {
        lines.push(format!("- **Run:** {run_link}"));
    }
    format!(
        "{}\n## Not finished\nThis pull request stays a draft: the run stopped before its work \
         was done.\n{}\n{}",
        super::proof::UNFINISHED_START,
        lines.join("\n"),
        super::proof::UNFINISHED_END
    )
}

/// `body` with the "Not finished" section in place: an older one replaced,
/// else right after the ticket line (or at the top).
pub fn with_unfinished(body: &str, section: &str) -> String {
    let (start, end) = (super::proof::UNFINISHED_START, super::proof::UNFINISHED_END);
    if let (Some(a), Some(b)) = (body.find(start), body.find(end)) {
        if b > a {
            return format!("{}{}{}", &body[..a], section.trim(), &body[b + end.len()..]);
        }
    }
    let first = body.lines().next().unwrap_or("").trim();
    if first.starts_with("Source ticket:") || first.starts_with("Closes #") {
        let rest = body[body.find('\n').unwrap_or(body.len())..].trim_start();
        format!("{first}\n\n{}\n\n{rest}", section.trim())
    } else {
        format!("{}\n\n{}", section.trim(), body.trim_start())
    }
}

/// The draft pull request's body: the ticket line, what is not finished
/// (when the run stopped mid-work), why the run stopped, the changes, what
/// was verified, and what the checks left open (`open`, already rendered
/// with its own heading, or empty).
pub fn body(ticket: &str, reason: &str, files: &[String], open: &str, unfinished: &str) -> String {
    let reason = reason.split_whitespace().collect::<Vec<_>>().join(" ");
    let shown: Vec<String> = files.iter().take(20).map(|f| format!("- `{f}`")).collect();
    let more = files.len().saturating_sub(shown.len());
    let open_items = if open.trim().is_empty() {
        format!("## Open items\n\n- [ ] {reason}")
    } else {
        open.trim().to_string()
    };
    let top = if unfinished.trim().is_empty() {
        String::new()
    } else {
        format!("{}\n\n", unfinished.trim())
    };
    format!(
        "{ticket}\n\n{top}## Summary\n\nCoderHelm worked on this ticket on this branch and \
         stopped before marking it ready: {reason}\n\n## Changes\n\n{}{}\n\n## Verification\n\n\
         Not yet verified as a whole: the run stopped before its checks after the work finished. \
         The repository's CI runs on this pull request.\n\n{open_items}\n\n\
         This is a draft. Push to this branch or comment on this pull request, and CoderHelm picks \
         it up from here.",
        shown.join("\n"),
        if more > 0 {
            format!("\n- and {more} more")
        } else {
            String::new()
        }
    )
}

/// The plan's progress on the branch, when the run has a plan with tasks.
async fn progress(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    branch: &str,
) -> Progress {
    let Some(plan) = super::run_plan_files(state, msg).await else {
        return Progress::default();
    };
    let tasks = super::plan_coverage::tasks_for(
        github,
        &msg.repo_owner,
        &msg.repo_name,
        branch,
        &plan.tasks,
    )
    .await;
    if tasks.is_empty() {
        return Progress::default();
    }
    match super::plan_coverage::plan_changes(
        github,
        &msg.repo_owner,
        &msg.repo_name,
        &msg.base_branch,
        branch,
    )
    .await
    {
        Some(c) => Progress::of(&c.coverage(&tasks)),
        None => Progress::default(),
    }
}

/// What [`open`] did for a stopped run with work on its branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stopped {
    /// The draft pull request is open (or was already), and the run waits
    /// for its review.
    Opened { number: u64, url: String },
    /// GitHub refused the pull request, twice: the run waits on a person
    /// with GitHub's reason in plain words, and nothing claims a pull
    /// request it doesn't have.
    Refused { why: String },
}

/// The pull request GitHub answered a create with: its number and link, or
/// why there is none in plain words. A create GitHub refused, or an answer
/// without a number, is never taken for an open pull request.
pub fn created(answer: Result<serde_json::Value, BoxError>) -> Result<(u64, String), String> {
    match answer {
        Ok(p) => match (p["number"].as_u64(), p["html_url"].as_str()) {
            (Some(n), Some(u)) if n > 0 && !u.trim().is_empty() => Ok((n, u.to_string())),
            _ => Err(
                "GitHub answered the request to open the pull request without a pull \
                      request number"
                    .to_string(),
            ),
        },
        Err(e) => Err(super::pr::refusal_words(
            "open the pull request",
            &e.to_string(),
        )),
    }
}

/// The log line for a stopped run's pull request: it names a pull request
/// only when there is one.
pub fn stop_line(s: &Stopped) -> String {
    match s {
        Stopped::Opened { number, .. } => {
            format!("Opened draft pull request #{number} for the stopped run")
        }
        Stopped::Refused { why } => format!("No pull request for the stopped run: {why}"),
    }
}

/// Opens (or finds) the draft pull request for a stopped run's branch and
/// records it on the run. Errors when the branch holds no work.
pub async fn open(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    branch: &str,
    reason: &str,
) -> Result<Stopped, BoxError> {
    // Handing the work to a person is the stop judge's call: it either
    // names what blocks, or the draft carries its assumption as an open item.
    let mut usage = crate::models::TokenUsage::default();
    let judged = super::stop_judge::decide_for_team(
        state,
        &mut usage,
        &msg.team_id,
        &super::stop_judge::run_subject(run_id),
        "",
        super::stop_judge::handoff_picture(msg, "stopped_after_work", reason, &[]),
    )
    .await;
    open_with(state, msg, run_id, branch, reason, &[], true, &judged).await
}

/// Regenerates the lockfiles a stopped run's branch calls for
/// (`generators::repair_lockfiles_with`). Returns the line for the run
/// page, or `None` when the branch changes no manifest a lockfile reads.
async fn lockfiles_before_stop(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
    run_id: &str,
    branch: &str,
) -> Option<String> {
    let provider = match crate::agent::provider::ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
    )
    .await
    {
        Ok(p) => p.for_run(
            run_id,
            &format!("{}/{}", msg.repo_owner, msg.repo_name),
            "",
            "",
        ),
        Err(e) => {
            tracing::warn!(run_id, error = %e, "Lockfiles before a stopped run's pull request: no provider scope");
            return None;
        }
    };
    let repair = super::generators::repair_lockfiles_with(
        &super::generators::LockWork {
            state,
            msg,
            github,
            branch,
            run_id,
            deadline: None,
            provider: &provider,
        },
        0,
    )
    .await;
    stop_lock_note(&repair)
}

/// What the stop says about its lockfiles: nothing when none needed
/// writing.
pub(crate) fn stop_lock_note(repair: &super::generators::LockRepair) -> Option<String> {
    if repair.written.is_empty() && repair.failed.is_empty() && repair.not_run.is_none() {
        return None;
    }
    Some(format!(
        "Before the pull request: CoderHelm {}.",
        repair.note()
    ))
}

/// [`open`], with what the checks left open going under Open items (on the
/// pull request that is already there too), and the notification only when
/// `notify` says this stop hasn't sent one.
#[allow(clippy::too_many_arguments)]
pub async fn open_with(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    branch: &str,
    reason: &str,
    items: &[super::proof::Finding],
    notify: bool,
    judged: &super::stop_judge::Judged,
) -> Result<Stopped, BoxError> {
    let mut with_assumption = items.to_vec();
    if let Some(a) = judged.assumption() {
        with_assumption.push(super::proof::Finding {
            point: super::proof::JUDGE_ASSUMPTION.to_string(),
            detail: format!("handoff: {}", a.trim()),
            settle: "Correct it in a comment on this pull request if it's wrong; otherwise \
                     delete this line."
                .to_string(),
            ..Default::default()
        });
    }
    let items = &with_assumption[..];
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;
    // A stopped run's pull request never goes up with a manifest and its
    // lockfile out of sync: the lockfiles the branch's changes call for
    // are written by their package managers first, by code. What can't be
    // written is said on the run; the stop still opens its pull request.
    if let Some(note) = lockfiles_before_stop(state, msg, &github, run_id, branch).await {
        super::add_progress_note(state, &msg.team_id, run_id, &note).await;
    }
    let diff = github
        .get_diff(&msg.repo_owner, &msg.repo_name, &msg.base_branch, branch)
        .await?;
    let changed: Vec<String> = diff["files"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|f| f["filename"].as_str().map(String::from))
        .collect();
    let work = super::run_end::work_files(&changed, &msg.ticket_id);
    if work.is_empty() {
        return Err("no work on the branch".into());
    }
    let open_text = [
        super::proof::unfinished_section(items),
        super::proof::gaps_section(items),
    ]
    .into_iter()
    .filter(|s| !s.is_empty())
    .collect::<Vec<_>>()
    .join("\n\n");
    // A stop before the work was done says so at the top, with what is done
    // and what is left; the section keeps the pull request a draft.
    let stop = super::outcome::Stop::from_recorded(reason);
    let progress = progress(state, msg, &github, branch).await;
    let mid_work = !progress.remaining.is_empty()
        || matches!(
            stop,
            super::outcome::Stop::NoProgress { .. } | super::outcome::Stop::OutOfTime(_)
        );
    // Only a need on the human-only list waits on a person
    // (`supervisor::hand_back_for`). Anything else ships: the work on the
    // branch goes on to CI and the ready decision like any finished run,
    // with what is left deferred on the checklist and named on the pull
    // request.
    let ending = super::supervisor::end(&super::supervisor::EndFacts {
        blocker: super::supervisor::hand_back_for(judged, reason),
        has_work: true,
        ladder_spent: true,
        ..Default::default()
    });
    let hand_back = match ending {
        super::supervisor::Ending::HandBack(h) => Some(h),
        _ => {
            return ship(
                state, msg, run_id, &github, branch, reason, &work, &open_text, &stop, &progress,
            )
            .await
        }
    };
    let unfinished = if mid_work {
        not_finished(
            &stop,
            &progress,
            &format!(
                "{}/runs/detail?id={run_id}",
                common::public_urls::app_url(&state.config.stage)
            ),
        )
    } else {
        String::new()
    };
    // The standard body's rules (`outcome::body_problems`): a slip is said
    // loudly, never sent silently.
    let draft_body = body(
        &super::outcome::ticket_line_for(msg),
        reason,
        &work,
        &open_text,
        &unfinished,
    );
    let problems = super::outcome::body_problems(&draft_body, &msg.ticket_id);
    if !problems.is_empty() {
        tracing::error!(run_id, problems = ?problems, "The stopped run's pull request body breaks the body rules");
    }
    let existing = github
        .find_open_pr_for_branch(&msg.repo_owner, &msg.repo_name, branch)
        .await?;
    let (number, url) = match existing {
        Some(p) => {
            let number = p["number"]
                .as_u64()
                .filter(|n| *n > 0)
                .ok_or("GitHub listed the branch's pull request without its number")?;
            // The pull request already open says what is not finished too.
            if !unfinished.is_empty() {
                let now = with_unfinished(p["body"].as_str().unwrap_or(""), &unfinished);
                if let Err(e) = github
                    .update_pr_body(&msg.repo_owner, &msg.repo_name, number, &now)
                    .await
                {
                    tracing::warn!(run_id, error = %e, "Could not say what is not finished on the open pull request");
                }
            }
            if !items.is_empty() {
                super::pr::put_open_items(
                    &github,
                    &msg.repo_owner,
                    &msg.repo_name,
                    number,
                    p["body"].as_str().unwrap_or(""),
                    items,
                )
                .await;
            }
            (number, p["html_url"].as_str().unwrap_or("").to_string())
        }
        None => {
            let conventions =
                super::load_git_conventions(state, &msg.team_id, &msg.ticket_id).await;
            let title = super::pr::pr_title(&conventions, &super::ticket_tokens(msg), msg);
            let title = common::truncate_str(&title, 72);
            // Refused once: once more with the body shortened harder, then
            // the run waits with GitHub's reason, never claiming a pull
            // request it doesn't have.
            let mut answer = created(
                github
                    .create_pull_request(
                        &msg.repo_owner,
                        &msg.repo_name,
                        title,
                        &draft_body,
                        branch,
                        &msg.base_branch,
                        true,
                    )
                    .await,
            );
            if let Err(first) = &answer {
                tracing::warn!(run_id, error = %first, "GitHub refused the stopped run's pull request; trying once more");
                let shorter = common::pr_body_fit::fit_to(
                    &draft_body,
                    common::pr_body_fit::BODY_TARGET / 2,
                    None,
                );
                answer = created(
                    github
                        .create_pull_request(
                            &msg.repo_owner,
                            &msg.repo_name,
                            title,
                            &shorter,
                            branch,
                            &msg.base_branch,
                            true,
                        )
                        .await,
                );
            }
            match answer {
                Ok(p) => p,
                Err(why) => {
                    let hand_back = hand_back.as_ref().expect("checked above");
                    record_refused(state, msg, run_id, branch, reason, &why, judged, hand_back)
                        .await;
                    if notify {
                        super::webhooks::emit_run(state, &msg.team_id, run_id, "needs_input").await;
                    }
                    return Ok(Stopped::Refused { why });
                }
            }
        }
    };
    let now = chrono::Utc::now().to_rfc3339();
    // The work is done and on the pull request: the run waits for its
    // review, named, never for "more detail" nobody was asked for.
    let detail = match judged.hold_words() {
        Some(h) => format!("{reason} Blocked on: {h}"),
        None => reason.to_string(),
    };
    let wait = review_wait(number, &url, &detail);
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(&msg.team_id))
        .key("run_id", super::attr_s(run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, waiting_on = :wo, pr_url = :pu, pr_number = :pn, \
             branch = :b, error_message = :em, updated_at = :t, current_pass = :cp",
        )
        // Never over a run a person ended.
        .condition_expression(common::run_status::not_ended("#s"))
        .ended_values()
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":cp", super::attr_s("done"))
        .needs_input(run_id, &wait, hand_back.as_ref().expect("checked above"))
        .expression_attribute_values(":pu", super::attr_s(&url))
        .expression_attribute_values(":pn", super::attr_n(number))
        .expression_attribute_values(":b", super::attr_s(branch))
        .expression_attribute_values(":em", super::attr_s(reason))
        .expression_attribute_values(":t", super::attr_s(&now))
        .send()
        .await;
    super::add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!("Stopped after the work; opened draft pull request #{number} with the reason as its open item"),
    )
    .await;
    super::run_guard::progress(
        state,
        &msg.team_id,
        run_id,
        super::run_guard::Progress::PrOpen,
    )
    .await;
    if notify {
        super::webhooks::emit_run(state, &msg.team_id, run_id, "needs_input").await;
    }
    Ok(Stopped::Opened { number, url })
}

/// What a stopped run waits for when GitHub refused its pull request: a
/// person, with GitHub's words and what Retry does.
pub fn refused_wait(why: &str) -> WaitingOn {
    WaitingOn::action(
        "Open the pull request",
        &format!(
            "The work is done and on the branch, but {why}. Press Retry: CoderHelm opens the \
             pull request from the branch without doing the work again."
        ),
        "",
    )
    .unwrap_or_default()
}

/// Records a stopped run whose pull request GitHub refused: it waits on a
/// person (never failed, never "Review pull request #0"), with the branch
/// kept and GitHub's reason as its error. Only a run the supervisor already
/// hands back gets here (`hand_back`, a reason on the human-only list); a
/// run it ships opens its pull request in [`ship`].
#[allow(clippy::too_many_arguments)]
async fn record_refused(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    branch: &str,
    reason: &str,
    why: &str,
    judged: &super::stop_judge::Judged,
    hand_back: &common::handback::HandBack,
) {
    // The stop judge's hold, when it named one, goes with GitHub's words.
    let why = match judged.hold_words() {
        Some(h) => format!("{why}. Blocked on: {h}"),
        None => why.to_string(),
    };
    let why = why.as_str();
    let wait = refused_wait(why);
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(&msg.team_id))
        .key("run_id", super::attr_s(run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, waiting_on = :wo, branch = :b, \
             error_message = :em, updated_at = :t, current_pass = :cp",
        )
        .condition_expression(common::run_status::not_ended("#s"))
        .ended_values()
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":cp", super::attr_s("done"))
        .needs_input(run_id, &wait, hand_back)
        .expression_attribute_values(":b", super::attr_s(branch))
        .expression_attribute_values(":em", super::attr_s(&format!("{reason} {why}.")))
        .expression_attribute_values(":t", super::attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await;
    if let Err(e) = res {
        tracing::warn!(run_id, error = %e, "Could not record the refused pull request on the run");
    }
    super::add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!("Stopped after the work; no pull request: {why}"),
    )
    .await;
}

/// The "Deferred" section of a run that shipped with tasks left: what
/// the run did, what it deferred and why. It doesn't hold the pull request
/// as a draft; CI and the ready decision do that, as for every run.
pub fn deferred_section(stop: &super::outcome::Stop, progress: &Progress) -> String {
    let total = progress.done.len() + progress.remaining.len();
    let mut lines = vec![format!(
        "CoderHelm shipped the work that is done. What it couldn't land is deferred, with why: \
         {}.",
        stop.words()
    )];
    if total > 0 {
        lines.push(format!("- **Done ({} of {total})**", progress.done.len()));
        for (i, r) in progress.remaining.iter().take(15).enumerate() {
            lines.push(format!(
                "- Deferred, task {} of {total}: {}",
                progress.done.len() + i + 1,
                common::truncate_str(r, 300)
            ));
        }
    }
    format!("## Deferred\n{}", lines.join("\n"))
}

/// A run stopped after its work for a reason that isn't on the human-only
/// list: the tasks left are deferred on the run's checklist, the pull
/// request opens (or is updated) with a "Deferred" section, and the run
/// goes on to CI like any finished run (`awaiting_ci`), so its CI fix loop
/// and ready decision take it from there. Never `needs_input`.
#[allow(clippy::too_many_arguments)]
async fn ship(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    github: &GitHubClient,
    branch: &str,
    reason: &str,
    work: &[String],
    open_text: &str,
    stop: &super::outcome::Stop,
    progress: &Progress,
) -> Result<Stopped, BoxError> {
    if let Some(mut plan) = super::run_plan_files(state, msg).await {
        let defer: Vec<(String, String)> = progress
            .remaining
            .iter()
            .filter_map(|r| {
                let path = r.split('`').nth(1)?.to_string();
                Some((path, format!("deferred: {}", stop.words())))
            })
            .collect();
        if !defer.is_empty() {
            plan.tasks = super::plan_coverage::with_deferred(&plan.tasks, &defer);
            super::save_checked_tasks(state, msg, run_id, &plan.tasks).await;
        }
    }
    let deferred = if progress.remaining.is_empty() {
        format!("## Deferred\n{}.", stop.words())
    } else {
        deferred_section(stop, progress)
    };
    let existing = github
        .find_open_pr_for_branch(&msg.repo_owner, &msg.repo_name, branch)
        .await?;
    let (number, url) = match existing {
        Some(p) => {
            let number = p["number"]
                .as_u64()
                .filter(|n| *n > 0)
                .ok_or("GitHub listed the branch's pull request without its number")?;
            let body = p["body"].as_str().unwrap_or("");
            let now = if body.contains("## Deferred") {
                body.to_string()
            } else {
                format!("{}\n\n{deferred}", body.trim_end())
            };
            if let Err(e) = github
                .update_pr_body(&msg.repo_owner, &msg.repo_name, number, &now)
                .await
            {
                tracing::warn!(run_id, error = %e, "Could not list what is deferred on the open pull request");
            }
            (number, p["html_url"].as_str().unwrap_or("").to_string())
        }
        None => {
            let conventions =
                super::load_git_conventions(state, &msg.team_id, &msg.ticket_id).await;
            let title = super::pr::pr_title(&conventions, &super::ticket_tokens(msg), msg);
            let body = shipped_body(
                &super::outcome::ticket_line_for(msg),
                work,
                open_text,
                &deferred,
            );
            // Named only when GitHub gave it a number; a refusal is the
            // error, in GitHub's words.
            created(
                github
                    .create_pull_request(
                        &msg.repo_owner,
                        &msg.repo_name,
                        common::truncate_str(&title, 72),
                        &body,
                        branch,
                        &msg.base_branch,
                        true,
                    )
                    .await,
            )?
        }
    };
    let usage = crate::models::TokenUsage::default();
    super::save_checkpoint(state, &msg.team_id, run_id, "pr", branch, 0, &usage).await;
    let now = chrono::Utc::now().to_rfc3339();
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(&msg.team_id))
        .key("run_id", super::attr_s(run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, pr_url = :pu, pr_number = :pn, branch = :b, \
             updated_at = :t, current_pass = :cp, deferred_reason = :dr REMOVE error_message",
        )
        .condition_expression(common::run_status::not_ended("#s"))
        .ended_values()
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", super::attr_s(common::run_status::AWAITING_CI))
        .expression_attribute_values(
            ":sri",
            super::attr_s(&format!("{}#{run_id}", common::run_status::AWAITING_CI)),
        )
        .expression_attribute_values(":pu", super::attr_s(&url))
        .expression_attribute_values(":pn", super::attr_n(number))
        .expression_attribute_values(":b", super::attr_s(branch))
        .expression_attribute_values(":t", super::attr_s(&now))
        .expression_attribute_values(":cp", super::attr_s("awaiting_ci"))
        .expression_attribute_values(":dr", super::attr_s(common::truncate_str(reason, 500)))
        .send()
        .await;
    if let Err(e) = res {
        if !format!("{e:?}").contains("ConditionalCheckFailed") {
            return Err(e.into());
        }
    }
    super::add_progress_note(
        state,
        &msg.team_id,
        run_id,
        &format!(
            "Shipped what is done on pull request #{number}; deferred: {}. CI and the ready \
             decision carry it on.",
            common::truncate_str(&stop.words(), 300)
        ),
    )
    .await;
    super::run_guard::progress(
        state,
        &msg.team_id,
        run_id,
        super::run_guard::Progress::PrOpen,
    )
    .await;
    Ok(Stopped::Opened { number, url })
}

/// The body of a pull request a run shipped with work deferred: the ticket
/// line, Summary, Changes, Verification, what the checks left open, and the
/// "Deferred" section.
pub fn shipped_body(ticket: &str, files: &[String], open: &str, deferred: &str) -> String {
    let shown: Vec<String> = files.iter().take(20).map(|f| format!("- `{f}`")).collect();
    let more = files.len().saturating_sub(shown.len());
    let open = if open.trim().is_empty() {
        String::new()
    } else {
        format!("\n\n{}", open.trim())
    };
    format!(
        "{ticket}\n\n## Summary\n\nCoderHelm did the work below on this branch and shipped it; \
         what it deferred is listed at the end, with why.\n\n## Changes\n\n{}{}\n\n\
         ## Verification\n\nThe repository's CI runs on this pull request; CoderHelm fixes what \
         fails before it is marked ready.{open}\n\n{}",
        shown.join("\n"),
        if more > 0 {
            format!("\n- and {more} more")
        } else {
            String::new()
        },
        deferred.trim()
    )
}

/// What a stopped run with a pull request waits for: its review, with the
/// reason as what is open on it.
pub fn review_wait(number: u64, url: &str, reason: &str) -> WaitingOn {
    WaitingOn::review_pr(number, url, reason)
        .or_else(|| WaitingOn::action("Review the work on the run's branch", reason, ""))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_checkpoint_after_the_work_opens_a_pr() {
        for p in ["implement_started", "implement", "security"] {
            assert!(opens_pr(p), "{p}");
        }
        for p in ["", "plan", "triage", "pr"] {
            assert!(!opens_pr(p), "{p}");
        }
    }

    #[test]
    fn the_body_says_why_and_keeps_it_open() {
        let b = body(
            "Source ticket: T-1",
            "Stopped before opening a pull request: 2 of 9 planned tasks are not done",
            &["web/a.tsx".into()],
            "",
            "",
        );
        assert!(b.starts_with("Source ticket: T-1"));
        assert!(b.contains("## Open items\n\n- [ ] Stopped before opening a pull request: 2 of 9"));
        assert!(b.contains("- `web/a.tsx`"));
        assert!(!b.contains("Closes #0"));
        // What the checks left open takes the place of the bare reason.
        let b = body(
            "Source ticket: T-1",
            "It kept continuing",
            &[],
            "## Open items\n- the vendor sends cents",
            "",
        );
        assert!(b.contains("## Open items\n- the vendor sends cents"));
        assert_eq!(b.matches("## Open items").count(), 1);
    }

    /// The stopped Jira run: seven of eight tasks done, two steps changed
    /// nothing. Its draft is the standard one: the ticket line, a specific
    /// "Not finished" section, Summary, Changes and Verification; never
    /// "Closes #0", "Partial" or "ran out of time".
    #[test]
    fn a_run_stopped_mid_work_gets_the_standard_draft_with_what_is_left() {
        let reason = "Stopped: steps 2 and 3 made no progress. The branch stayed at a8c8814 and \
                      the run was stuck at the implement work (planned tasks) while working on \
                      task 8 of 8 (`web/hooks/useStickyScroll.ts`: adapt the scroll offset). The \
                      work so far is kept on branch `coderhelm-wip/coderhelm/web-12`.";
        let progress = Progress {
            done: (1..=7)
                .map(|i| format!("`web/f{i}.tsx`: task {i}"))
                .collect(),
            remaining: vec!["`web/hooks/useStickyScroll.ts`: adapt the scroll offset".into()],
        };
        let stop = super::super::outcome::Stop::from_recorded(reason);
        let nf = not_finished(
            &stop,
            &progress,
            "https://app.example.com/runs/detail?id=r1",
        );
        let b = body(
            &super::super::outcome::ticket_line(false, 0, "WEB-12"),
            reason,
            &["web/f1.tsx".into()],
            "",
            &nf,
        );
        assert!(b.starts_with("Source ticket: WEB-12\n"), "{b}");
        assert!(super::super::proof::holds_unfinished(&b));
        assert!(b.contains("## Not finished"));
        assert!(b.contains("**Why it stopped:** steps 2 and 3 made no change to the branch while working on task 8 of 8"));
        assert!(b.contains("**Done (7 of 8):**"));
        assert!(b.contains("task 8 of 8: `web/hooks/useStickyScroll.ts`: adapt the scroll offset"));
        assert!(b.contains("**What you can do:**"));
        assert!(b.contains("https://app.example.com/runs/detail?id=r1"));
        assert!(
            super::super::outcome::body_problems(&b, "WEB-12").is_empty(),
            "{:?}",
            super::super::outcome::body_problems(&b, "WEB-12")
        );
        for never in ["#0", "Partial", "ran out of time"] {
            assert!(!b.contains(never), "{never}");
        }
    }

    const TOO_LONG: &str = "HTTP status client error (422 Unprocessable Entity) for url (https://api.github.com/repos/acme/api/pulls): {\"message\":\"Validation Failed\",\"errors\":[{\"resource\":\"Issue\",\"code\":\"custom\",\"field\":\"body\",\"message\":\"body is too long\"},{\"resource\":\"Issue\",\"code\":\"custom\",\"field\":\"body\",\"message\":\"body is too long (maximum is 65536 characters)\"}],\"status\":\"422\"}";

    /// A create GitHub refused, or answered without a number, is never an
    /// open pull request: the stop names none and waits with GitHub's words.
    #[test]
    fn a_refused_create_never_claims_a_pull_request() {
        let refused = created(Err(TOO_LONG.into())).unwrap_err();
        assert_eq!(
            refused,
            "GitHub refused to open the pull request: body is too long (maximum is 65536 characters)"
        );
        for answer in [
            serde_json::json!({}),
            serde_json::json!({"number": 0, "html_url": "https://github.com/acme/api/pull/0"}),
            serde_json::json!({"number": 12}),
        ] {
            assert!(created(Ok(answer.clone())).is_err(), "{answer}");
        }
        assert_eq!(
            created(Ok(
                serde_json::json!({"number": 602, "html_url": "https://github.com/acme/api/pull/602"})
            )),
            Ok((602, "https://github.com/acme/api/pull/602".to_string()))
        );
        let line = stop_line(&Stopped::Refused {
            why: refused.clone(),
        });
        assert!(!line.contains("Opened") && !line.contains('#'), "{line}");
        assert!(line.contains("body is too long (maximum is 65536 characters)"));
        let line = stop_line(&Stopped::Opened {
            number: 602,
            url: "u".into(),
        });
        assert_eq!(line, "Opened draft pull request #602 for the stopped run");
        // The run waits on a person, with GitHub's words and what Retry does.
        let w = refused_wait(&refused);
        assert_eq!(w.action, "Open the pull request");
        assert!(w
            .detail
            .contains("body is too long (maximum is 65536 characters)"));
        assert!(w.detail.contains("Press Retry"));
        assert!(!w.detail.contains("HTTP status"));
    }

    /// The stop path: a refused create is tried once more, shorter, and
    /// then recorded as a wait; the log names a pull request only from
    /// `Stopped::Opened`, never from a create that failed.
    #[test]
    fn the_stop_path_logs_only_a_pull_request_it_has() {
        let src = include_str!("stop_pr.rs");
        let open_with = &src[src.find("pub async fn open_with(").unwrap()
            ..src
                .find("/// What a stopped run waits for when GitHub refused")
                .unwrap()];
        assert_eq!(open_with.matches(".create_pull_request(").count(), 2);
        assert!(open_with.contains("record_refused("));
        assert!(open_with.contains("Ok(Stopped::Refused { why })"));
        assert!(
            !open_with.contains("as_u64().unwrap_or(0)"),
            "no pull request #0"
        );
        let run = include_str!("mod.rs");
        assert!(
            !run.contains("pr_url, \"Opened a draft PR"),
            "the old line logged a link it didn't check"
        );
        assert_eq!(
            run.matches("stop_pr::Stopped::Opened { number, url } => info!(")
                .count(),
            2
        );
    }

    #[test]
    fn an_open_pull_request_gets_the_section_once() {
        let nf = not_finished(
            &super::super::outcome::Stop::Other("A check errored".into()),
            &Progress::default(),
            "",
        );
        let b = with_unfinished("Source ticket: WEB-3\n\n## Summary\nx", &nf);
        assert!(
            b.starts_with("Source ticket: WEB-3\n\n<!-- coderhelm:unfinished -->"),
            "{b}"
        );
        let again = with_unfinished(&b, &nf);
        assert_eq!(again.matches("## Not finished").count(), 1);
        assert!(again.contains("## Summary\nx"));
    }

    /// The stalled Jira run, shipped: seven of eight tasks done, the eighth
    /// deferred with why. The body keeps the body rules and never holds the
    /// pull request as unfinished work for a person.
    #[test]
    fn a_run_stopped_mid_work_ships_with_the_rest_deferred() {
        let stop = super::super::outcome::Stop::from_recorded(
            "Stopped: steps 2 and 3 made no progress. The branch stayed at a8c8814 and the run \
             was stuck at the implement work (planned tasks) while working on task 8 of 8 \
             (`web/hooks/useScroll.ts`: adapt the offset).",
        );
        let progress = Progress {
            done: (1..=7)
                .map(|i| format!("`web/f{i}.tsx`: task {i}"))
                .collect(),
            remaining: vec!["`web/hooks/useScroll.ts`: adapt the offset".into()],
        };
        let d = deferred_section(&stop, &progress);
        let b = shipped_body("Source ticket: WEB-12", &["web/f1.tsx".into()], "", &d);
        assert!(
            super::super::outcome::body_problems(&b, "WEB-12").is_empty(),
            "{b}"
        );
        assert!(!super::super::proof::holds_unfinished(&b));
        assert!(b.contains("Deferred, task 8 of 8: `web/hooks/useScroll.ts`"));
        assert!(!b.contains("What you can do"));
    }
}
