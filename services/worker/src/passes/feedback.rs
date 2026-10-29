use common::run_status::EndedValues;
use serde_json::json;
use tracing::{error, info, warn};

use crate::agent::llm::{self, ToolDefinition, ToolExecutor};
use crate::agent::provider;
use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{FeedbackMessage, ReviewComment, TicketSource, TokenUsage, WorkerMessage};
use crate::passes::plan;
use crate::WorkerState;

pub async fn run(
    state: &WorkerState,
    mut msg: FeedbackMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // This round's own clock (common::run_round): the time budget starts
    // with this invocation, never with the run.
    let start = std::time::Instant::now();
    let mut usage = TokenUsage::default();

    // Load team's Anthropic API key (required), gated for this run.
    let provider = ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
    )
    .await?
    .for_run(
        &msg.run_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        "",
        &format!("Feedback on pull request #{}", msg.pr_number),
    );

    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    // Re-review on a run whose PR was closed (branch superseded, manual
    // close): silently working against a closed PR confused users. Reattach
    // to the branch's current open PR if one exists (GitHub forbids two open
    // PRs per branch), otherwise reopen the original. Also strip the
    // "⚠️ Partial:" salvage marker — someone actively re-reviewing means the
    // work is being finished.
    // A person saying the fix belongs outside the code (a setting in another
    // system) is a lesson for the team, not a request to change this code.
    // Checked first: it also holds when they closed the PR.
    if let Some(judgment) =
        super::lessons::check_comment(state, &provider, &github, &msg, &mut usage).await
    {
        info!(run_id = %msg.run_id, "Feedback says the fix is outside the code — lesson recorded, no code change");
        let closed = close_own_pr_outside(
            state,
            &github,
            &OwnPr {
                team_id: &msg.team_id,
                run_id: &msg.run_id,
                owner: &msg.repo_owner,
                repo: &msg.repo_name,
                pr_number: msg.pr_number,
            },
            &judgment.where_,
            &judgment.lesson,
        )
        .await;
        if !closed {
            finish_without_change(state, &msg).await;
        }
        return Ok(());
    }

    if !ensure_pr_open(state, &github, &mut msg).await {
        // A human closed the PR — respect it and stop.
        return Ok(());
    }

    // A user cancel must halt feedback immediately — don't process or push.
    if run_is_cancelled(state, &msg.team_id, &msg.run_id).await {
        info!(run_id = %msg.run_id, "Run is cancelled — skipping feedback");
        return Ok(());
    }

    // ONE PLATFORM WRITER PER RUN: feedback historically took no claim, so it
    // could interleave branch writes with an in-flight resume (or another
    // feedback) and the two would clobber each other. Take the run's writer
    // slot before touching the branch; if a resume holds it, wait briefly and
    // then defer via delayed re-enqueue (send_delayed_feedback) — never write
    // concurrently, and never fail the run just because a write is in progress.
    let writer_holder = format!(
        "feedback#{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let mut writer_guard = None;
    for attempt in 0..4u32 {
        writer_guard =
            super::acquire_run_writer_guard(state, &msg.team_id, &msg.run_id, &writer_holder).await;
        if writer_guard.is_some() {
            break;
        }
        info!(run_id = %msg.run_id, attempt, "run writer slot busy — waiting");
        tokio::time::sleep(std::time::Duration::from_secs(20)).await;
    }
    let _writer_guard = match writer_guard {
        Some(g) => g,
        None => {
            // An in-flight resume (or another feedback) holds the per-run writer
            // slot. This is TRANSIENT, not a failure: re-enqueue this feedback with
            // a delay so it applies once the holder releases the slot. Returning Err
            // here would mark the whole run failed (main.rs writes status=failed on
            // any feedback Err) and silently drop the review — the SQS handler acks
            // the message regardless, so it is never redelivered on its own.
            //
            // The slot's TTL frees it within ~17 min (> Lambda's 15-min max hold),
            // so normal contention clears in a few hops. MAX_FEEDBACK_DEFERS is the
            // backstop for a genuinely wedged slot: past it, fail loudly so the
            // anomaly surfaces instead of churning the queue for its 14-day retention.
            const MAX_FEEDBACK_DEFERS: u32 = 15; // ~30 min at 120s, past the slot TTL
            if msg.defer_count >= MAX_FEEDBACK_DEFERS {
                error!(
                    run_id = %msg.run_id,
                    defer_count = msg.defer_count,
                    "writer slot still busy after max feedback defers — wedged; failing so it surfaces"
                );
                return Err(
                    "writer slot did not free within ~30 min (wedged) — cannot apply feedback"
                        .into(),
                );
            }
            info!(
                run_id = %msg.run_id,
                defer_count = msg.defer_count,
                "run writer slot busy — deferring feedback via delayed re-enqueue"
            );
            if !send_delayed_feedback(state, &msg, 120).await {
                // Could not re-enqueue (queue misconfigured / SQS error). Surface it
                // instead of silently dropping the review.
                return Err("could not defer feedback (feedback queue unavailable)".into());
            }
            return Ok(());
        }
    };

    // The run's ticket and what its requester said not to do in earlier
    // rounds: a stored "one push" skips the extra update push below.
    let ticket_info = super::review_fix::Ticket::load(state, &msg.team_id, &msg.run_id).await;

    // Whether the pre-feedback conflict resolution pushed a merge commit —
    // that push triggers CI, so completing without awaiting_ci would leave
    // the CI result with no consumer (and a salvaged draft PR stuck in
    // draft forever).
    let mut conflicts_pushed = false;
    // The run's status as this feedback cycle found it; decides who owns CI.
    let mut run_status = String::new();

    // The run's ticket (id, title, issue number): commit messages made in
    // this round follow the team's commit template with its tokens.
    let mut ticket = (String::new(), String::new(), 0u64);

    // "Rebase", "update branch", "sync with main": one request, done here
    // instead of the plain conflict merge below, in one push.
    let update_ask = super::branch_update::asked(state, &provider, &msg, &mut usage).await;
    let mut update_reply: Option<String> = None;

    // Resolve merge conflicts before processing feedback (especially on re-review)
    {
        let run_record = state
            .dynamo
            .get_item()
            .table_name(&state.config.runs_table_name)
            .key(
                "team_id",
                aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
            )
            .key(
                "run_id",
                aws_sdk_dynamodb::types::AttributeValue::S(msg.run_id.clone()),
            )
            .send()
            .await
            .ok()
            .and_then(|r| r.item);
        if let Some(ref item) = run_record {
            run_status = item
                .get("status")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default();
            let text = |k: &str| {
                item.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            ticket = (
                text("ticket_id"),
                text("title"),
                item.get("issue_number")
                    .and_then(|v| v.as_n().ok())
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap_or(0),
            );
            let branch = item
                .get("branch")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default();
            // Base for the pre-fix conflict merge — resolved from the LIVE PR, not
            // the run record's possibly-stale `base_branch`. Merging the PR's real
            // base (e.g. `develop`) keeps the branch in sync WITHOUT pulling in
            // `main`-only files that would masquerade as out-of-scope changes and
            // trigger an endless revert/re-merge loop.
            let base_branch = resolve_pr_base(&github, &msg).await;
            // The start-of-round update. "One push" skips it, unless a
            // person explicitly asked for the update (then it is the push).
            let may_update =
                super::branch_update::may_update(&ticket_info.constraints, update_ask.as_ref());
            if !branch.is_empty() && !may_update {
                info!(run_id = %msg.run_id, "The requester asked for one push — not pushing a conflict merge first");
            }
            if !branch.is_empty() && may_update {
                // The run's ticket, so the merge commit carries its real key.
                let ticket_msg = crate::models::TicketMessage {
                    team_id: msg.team_id.clone(),
                    installation_id: msg.installation_id,
                    source: if ticket.2 > 0 {
                        TicketSource::Github
                    } else {
                        TicketSource::Jira
                    },
                    ticket_id: ticket.0.clone(),
                    title: ticket.1.clone(),
                    body: String::new(),
                    repo_owner: msg.repo_owner.clone(),
                    repo_name: msg.repo_name.clone(),
                    issue_number: ticket.2,
                    sender: String::new(),
                    requester: None,
                    base_branch,
                    image_attachments: vec![],
                    continuation: 0,
                    continuation_run_id: None,
                    first_attempt_ms: 0,
                    model_retries: 0,
                    ticket_update_note: None,
                    ticket_history_note: None,
                    plan_cache_key: None,
                    team_instructions: String::new(),
                    person_direction: None,
                    answers: Vec::new(),
                    new_run_id: None,
                };
                if let Some(ask) = &update_ask {
                    let (line, pushed) = super::branch_update::run(
                        state,
                        &github,
                        &provider,
                        &ticket_msg,
                        &branch,
                        ask,
                        &mut usage,
                    )
                    .await;
                    conflicts_pushed = pushed;
                    update_reply = Some(line);
                } else {
                    match super::pr::resolve_conflicts(
                        state,
                        &ticket_msg,
                        &github,
                        &branch,
                        &provider,
                        &mut usage,
                    )
                    .await
                    {
                        Ok(true) => {
                            conflicts_pushed = true;
                            info!(run_id = %msg.run_id, "Resolved merge conflicts before feedback")
                        }
                        Ok(false) => {}
                        Err(e) => {
                            warn!(run_id = %msg.run_id, error = %e, "Failed to resolve merge conflicts")
                        }
                    }
                }
            }
        }
    }

    // Updating the branch was all the person asked: the update's line is the
    // whole answer, no agent round.
    if let (Some(ask), Some(line)) = (&update_ask, &update_reply) {
        if ask.only {
            return finish_branch_update(
                state,
                &github,
                &msg,
                &ticket_info,
                line,
                conflicts_pushed,
            )
            .await;
        }
    }

    // If the gateway didn't send inline comments (PR review submitted event),
    // fetch them from the GitHub API using the review_id.
    // When review_id == 0 (re-review), fetch ALL non-bot comments and let
    // filter_unanswered determine which still need a reply.
    let comments = if msg.comments.is_empty() && msg.review_id > 0 {
        fetch_review_comments(&github, &msg).await
    } else if msg.comments.is_empty() && msg.review_id == 0 {
        fetch_all_human_comments(&github, &msg).await
    } else {
        msg.comments.clone()
    };

    // Check for "wrong repo" signal before processing normal feedback.
    // Combine review body + all comment bodies to detect the signal.
    let all_text = {
        let mut text = msg.review_body.clone();
        for c in &comments {
            text.push('\n');
            text.push_str(&c.body);
        }
        text
    };

    let team_repos = plan::fetch_team_repos(state, &msg.team_id).await;
    if let Some(target) = detect_wrong_repo(&all_text, &team_repos, &msg, state, &mut usage).await {
        info!(run_id = %msg.run_id, target = %target, "LLM confirmed wrong-repo signal");
        return handle_wrong_repo(state, &github, &msg, &target).await;
    }

    // Filter out comments the bot already replied to
    let comments = filter_unanswered(&github, &msg, comments).await;

    // On re-review (review_id == 0), check CI status and include failures in the prompt.
    // Not while the run is awaiting_ci/running: resume owns CI then (it has the
    // CI budget, stall detection and scope guard), and a second fixer here
    // raced it on the same failures.
    let resume_owns_ci = resume_owns_ci(&run_status);
    if resume_owns_ci && msg.review_id == 0 {
        info!(run_id = %msg.run_id, status = %run_status, "Run is awaiting CI / running — resume owns CI, not attaching CI failures");
    }
    let mut ci_failure_block = if msg.review_id == 0 && !resume_owns_ci {
        let branch = get_pr_branch(state, &msg).await.unwrap_or_default();
        match fetch_ci_failures(&github, &msg, &branch).await {
            Some(logs) => format!(
                "\n## CI Failures\nThe following CI checks are failing. Fix these issues as well:\n\n```\n{}\n```\n",
                if logs.len() > 12000 { format!("... (truncated)\n{}", common::tail_str(&logs, 12000)) } else { logs }
            ),
            None => String::new(),
        }
    } else {
        String::new()
    };

    // CI fixing riding along with a human comment is still an automated fix:
    // it draws from the same per-run auto-fix budget as CI-only cycles (claimed
    // below). Out of budget → answer the human, leave CI to a person.
    let ci_only_cycle = comments.is_empty() && msg.review_body.trim().is_empty();
    if !ci_failure_block.is_empty()
        && !ci_only_cycle
        && !super::claim_auto_fix_slot(state, &msg.team_id, &msg.run_id).await
    {
        info!(run_id = %msg.run_id, "Auto-fix budget spent — answering feedback without the CI fix");
        ci_failure_block.clear();
    }

    let has_ci_failures = !ci_failure_block.is_empty();

    if comments.is_empty() && msg.review_body.is_empty() && !has_ci_failures && resume_owns_ci {
        // Nothing for feedback to answer, and resume is mid-cycle on CI: leave
        // the run's status and the PR's draft state to resume.
        info!(run_id = %msg.run_id, "All comments already answered; resume owns CI — skipping feedback");
        return Ok(());
    }

    if comments.is_empty() && msg.review_body.is_empty() && !has_ci_failures {
        info!(run_id = %msg.run_id, "All comments already answered and CI green — skipping feedback");

        // Nothing left to do and CI is green — a salvaged DRAFT PR would
        // otherwise never be marked ready (only resume paths did that).
        crate::passes::resume::mark_pr_ready(
            state,
            &msg.team_id,
            &github,
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
        )
        .await;

        // Reset status back to completed so the run doesn't stay stuck at "running"
        let now = chrono::Utc::now().to_rfc3339();
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.runs_table_name)
            .key(
                "team_id",
                aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
            )
            .key(
                "run_id",
                aws_sdk_dynamodb::types::AttributeValue::S(msg.run_id.clone()),
            )
            .update_expression(
                "SET #s = :s, status_run_id = :sri, updated_at = :t \
                 REMOVE error_message, #err",
            )
            .expression_attribute_names("#s", "status")
            .expression_attribute_names("#err", "error")
            .expression_attribute_values(
                ":s",
                aws_sdk_dynamodb::types::AttributeValue::S("completed".to_string()),
            )
            .expression_attribute_values(
                ":sri",
                aws_sdk_dynamodb::types::AttributeValue::S(format!("completed#{}", msg.run_id)),
            )
            .expression_attribute_values(":t", aws_sdk_dynamodb::types::AttributeValue::S(now))
            .send()
            .await;

        return Ok(());
    }

    // CI-ONLY churn (no unanswered human comment or review to answer — the
    // all-empty case already returned above, so reaching here with both empty
    // means CI failures are the sole driver) is the automated-fix path. Draw it
    // from the durable budget so a PR that structurally can't go green (e.g.
    // codegen that can't run in the sandbox) can't churn forever. Human feedback
    // is ALWAYS honored, never budget-gated. The claim is atomic, so concurrent
    // feedback webhooks for one run can't overshoot the cap.
    if comments.is_empty()
        && msg.review_body.trim().is_empty()
        && !super::claim_auto_fix_slot(state, &msg.team_id, &msg.run_id).await
    {
        hand_off_to_human(state, &github, &msg).await;
        return Ok(());
    }

    let formatted = format_review_comments(&msg.review_body, &comments);

    // What the person asked CoderHelm not to do ("don't comment", "don't
    // merge", "one push"), read from the ticket and this thread by the
    // model; the passes obey it.
    let human_round = !(comments.is_empty() && msg.review_body.trim().is_empty());
    let constraints = if human_round {
        super::review_fix::read_constraints(
            state,
            &github,
            &provider,
            &msg.team_id,
            &msg.run_id,
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
            &ticket_info,
            &formatted,
            &mut usage,
        )
        .await
    } else {
        ticket_info.constraints.clone()
    };

    // Load voice instructions (repo-specific falls back to global)
    let voice = {
        let repo_voice = super::load_content(
            state,
            &msg.team_id,
            &format!("VOICE#REPO#{}/{}", msg.repo_owner, msg.repo_name),
        )
        .await;
        if repo_voice.is_empty() {
            super::load_content(state, &msg.team_id, "VOICE#GLOBAL").await
        } else {
            repo_voice
        }
    };
    let voice_block = if voice.is_empty() {
        String::new()
    } else {
        format!(" Match the team's voice and tone as described below:\n{voice}")
    };

    // The team's instructions, loaded once for this round: the replies, the
    // code changes and the description rewrite all follow them.
    let team_instructions =
        super::load_team_instructions(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await;
    // A failing-test fix's run keeps its failure evidence and rules.
    let team_instructions =
        super::test_fix::with_saved(state, &msg.team_id, &msg.run_id, team_instructions).await;
    let team_block = super::team_instructions_block(&team_instructions);

    // Code changes made while addressing comments follow the same test rule.
    let tests_block =
        if super::review_pr::tests_required(state, &msg.team_id, &msg.repo_owner, &msg.repo_name)
            .await
        {
            super::TESTS_REQUIRED_CODER_SECTION
        } else {
            ""
        };
    let system = format!(
        "You are a feedback agent for the {owner}/{repo} repository. \
         You respond to reviewer comments on pull requests by reading and writing files \
         directly to the PR branch. \
         If a comment requests a code change, commit it — use edit_file for targeted edits \
         (the default, and the ONLY safe option for a few lines in a large file), and write_file \
         or batch_write only for new files or whole-file rewrites — then confirm briefly. \
         If a comment asks a question, read the code and answer clearly. \
         If a comment asks to change the pull request's description (its text, a section, a \
         banner, a name in it), call update_pr_description with exactly what to change; never \
         edit files for that. CoderHelm rewrites the description and adds what changed to your \
         reply. \
         Change only what each comment asks for: code next to it that looks alike but that the \
         comment did not name stays exactly as it is. \
         Your changes land on the pull request as one commit in one push when you finish, after \
         CoderHelm checks them (scope, logs, the full test suite). \
         Your output is posted directly as a GitHub comment — write natural replies, no meta-commentary.{voice_block}{team_block}{tests_block}{logs_block}",
        logs_block = super::review_fix::logs::GUIDANCE,
        owner = msg.repo_owner,
        repo = msg.repo_name,
    );

    // The team's commit convention and the run's ticket: every commit this
    // round makes follows them, and the agent is told the real key.
    let conventions = super::load_git_conventions(state, &msg.team_id, &ticket.0).await;
    let tokens = common::git_conventions::Tokens::new(
        &if ticket.2 > 0 {
            format!("#{}", ticket.2)
        } else {
            ticket.0.clone()
        },
        &ticket.0,
        &ticket.1,
    );
    let commit_hint = conventions.commit_hint(&tokens);

    // A later step of the same round: the earlier one's commits are there.
    let continuing = if msg.round_attempt > 0 {
        "\n## Continuing this round\nAn earlier step of this round ran out of time; everything it \
         wrote is already committed on the branch. Read the files first and finish only what is \
         left; don't redo or revert it.\n"
    } else {
        ""
    };
    let prompt = format!(
        r#"A reviewer left comments on PR #{pr_number}.
{continuing}
## Review Comments
{comments}
{ci_failures}
## Instructions
For each comment:
- **Question** — read the relevant code, answer concisely
- **Change request** — fix the code (prefer edit_file for targeted changes, especially in large files; write_file only for whole-file rewrites), then say what changed, where, and what you checked (e.g. "Pointed `/a` at `/b`; `/b` returns 200."). Never reply with only "Done" or "Fixed"
- **Description change** — call update_pr_description with the change, in the person's words with every value they gave; don't commit anything for it

If there are CI failures listed above, also fix those — read the failing files, diagnose the issue, and commit a fix.

Context messages prefixed with `[username]:` are for background only — reply only to the reviewer's latest comment in each thread.

If multiple comments, write a separate reply for each, separated by `---SPLIT---` on its own line.

Rules:
- Address every comment
- Follow the reviewer's suggestions exactly unless they conflict with codebase conventions
- Don't make unrelated changes: change only what the comment names. Similar code nearby that the comment didn't point at stays as it is
- {commit_hint}
- Use GitHub markdown (backticks for code, triple-backtick for blocks)
- Keep replies SHORT — 1-2 sentences max. Just say what you did, not what was wrong or why. Example: "Fixed — reformatted to multi-line with trailing comma." Do NOT list every file or explain every change in detail."#,
        pr_number = msg.pr_number,
        comments = formatted,
        ci_failures = ci_failure_block,
    );
    // The branch update was done before this round and its answer is added
    // to the reply for the agent.
    let prompt = match &update_reply {
        Some(line) => format!(
            "{prompt}\n\n## Branch update\nThe request to bring the branch up to date with its base \
             is already handled; this answer is added to your reply: \"{line}\". Don't act on it or \
             mention it."
        ),
        None => prompt,
    };

    // Determine the PR branch — we'll use the run's known branch
    // Try to extract branch from repo metadata. For now, use a convention.
    let branch = get_pr_branch(state, &msg).await?;

    // Ticket-scope guard: CI-ONLY cycles (no unanswered human comment/review —
    // same signal as the budget gate) may only write files the PR already
    // changed; a lint/type warning in an untouched file pre-exists on base and
    // is out of scope. HUMAN-directed feedback runs unscoped — a reviewer
    // asking for an out-of-diff change is authorization, not overreach.
    // The PR's base branch — used both for scope detection and as restore_file's
    // default target so out-of-scope reverts hit the real base, not `main`.
    // Resolved from the LIVE PR (ground truth), never the run record's stored value.
    let base = resolve_pr_base(&github, &msg).await;
    // Correct a stale stored base so the rest of the system stays consistent.
    heal_run_base_branch(state, &msg, &base).await;
    let ci_only = comments.is_empty() && msg.review_body.trim().is_empty();
    // The PR's changed files: the scope of a CI-only cycle, and what the
    // team's always-applied skills scoped to paths are matched against.
    let pr_files = github
        .compare_changed_files(&msg.repo_owner, &msg.repo_name, &base, &branch)
        .await;
    let scope_guard = if ci_only {
        match &pr_files {
            Ok(files) => super::write_guard::ScopeGuard::scoped(files.clone()),
            Err(e) => {
                warn!(run_id = %msg.run_id, error = %e, "Could not list PR files — feedback cycle runs unscoped");
                super::write_guard::ScopeGuard::unscoped()
            }
        }
    } else {
        super::write_guard::ScopeGuard::unscoped()
    };

    let mut tools = feedback_tools();
    // Who may have the description changed: a person with write access or
    // higher, the same check as the comment commands.
    let may_edit_description = !msg.trigger_author.trim().is_empty()
        && github
            .has_write_access(&msg.repo_owner, &msg.repo_name, msg.trigger_author.trim())
            .await;
    if !msg.trigger_author.trim().is_empty() {
        tools.push(update_description_tool());
    }
    let mut full_system = system.clone();
    // Live systems the team connected for this repo, if any: read-only lookups
    // so the fix rests on the system's real shape.
    let live = super::live_sources::LiveSources::load(
        state,
        super::live_sources::Role::Coder,
        &msg.team_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        msg.pr_number,
    )
    .await
    .map(|l| {
        l.with_run(
            &msg.run_id,
            format!("{}/{}#{}", msg.repo_owner, msg.repo_name, msg.pr_number),
        )
    });
    if let Some(l) = &live {
        tools.extend(l.tools());
        full_system.push_str(&l.prompt_section());
    }
    // Repository history through the GitHub App: no setup.
    let team_repos = plan::fetch_team_repos(state, &msg.team_id).await;
    let history = super::github_history::GithubHistory::new(
        &msg.team_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        team_repos.clone(),
        "coder",
        msg.pr_number,
    )
    .with_run(
        &msg.run_id,
        format!("{}/{}#{}", msg.repo_owner, msg.repo_name, msg.pr_number),
    );
    tools.push(super::github_history::tool());
    full_system.push_str(super::github_history::prompt_line());
    // The code graph: who calls what a reviewer asked to change.
    let graphs = super::code_graph::RepoGraphs::load(
        state,
        &msg.team_id,
        msg.installation_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        team_repos,
        "coder",
        msg.pr_number,
    )
    .await
    .with_run(
        &msg.run_id,
        format!("{}/{}#{}", msg.repo_owner, msg.repo_name, msg.pr_number),
    );
    super::code_graph::offer(
        &graphs,
        super::code_graph::Role::Coder,
        &mut tools,
        &mut full_system,
    );
    // `check_url` on the team's own public pages.
    let web = super::web_check::WebCheck::load(
        state,
        &msg.team_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        "coder",
        msg.pr_number,
        &msg.comments.iter().fold(msg.review_body.clone(), |acc, c| {
            format!("{acc}\n{}", c.body)
        }),
    )
    .await
    .with_run(
        &msg.run_id,
        format!("{}/{}#{}", msg.repo_owner, msg.repo_name, msg.pr_number),
    );
    tools.push(super::web_check::tool());
    full_system.push_str(&web.prompt_section());
    let ops = super::ops_tools::OpsTools::load(
        state,
        &msg.team_id,
        &format!("{}/{}", msg.repo_owner, msg.repo_name),
        "coder",
        msg.pr_number,
    )
    .await
    .map(|o| {
        o.with_run(
            &msg.run_id,
            format!("{}/{}#{}", msg.repo_owner, msg.repo_name, msg.pr_number),
        )
    });
    if let Some(o) = &ops {
        tools.extend(o.tools());
        full_system.push_str(&o.prompt_section());
    }
    // The team's own written skills, read-only guidance.
    let skills = super::skills::TeamSkills::load(
        state,
        &msg.team_id,
        super::skills::Reader::Coder,
        format!("{}/{}#{}", msg.repo_owner, msg.repo_name, msg.pr_number),
        super::skills::Work::new(
            format!("{}/{}", msg.repo_owner, msg.repo_name),
            // It fixes code and replies to the comments.
            &["coder", "replies"],
            pr_files.unwrap_or_default(),
        ),
    )
    .await
    .map(|s| {
        s.with_run(Some(msg.run_id.as_str())).with_task(&format!(
            "Answering review comments on PR #{}",
            msg.pr_number
        ))
    });
    if let Some(s) = &skills {
        tools.extend(s.tools());
        s.offer(&tools);
        full_system.push_str(&s.prompt_section());
        s.record_applied(state).await;
    }

    // The round on the run record, with its deadline, while it works: the
    // stuck-run sweep waits for that deadline, not the run's age.
    if !super::round::may_start(start, std::time::Instant::now()) {
        // Too little of this invocation is left for the round: the same
        // feedback runs first in a fresh one, with its whole budget.
        info!(run_id = %msg.run_id, "Feedback round deferred to a fresh step for its full budget");
        if send_delayed_feedback(state, &msg, 5).await {
            return Ok(());
        }
    }
    // A continuing step keeps the round's starting point and its WIP branch.
    let prior = if msg.round_attempt > 0 {
        state
            .dynamo
            .get_item()
            .table_name(&state.config.runs_table_name)
            .key(
                "team_id",
                aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
            )
            .key(
                "run_id",
                aws_sdk_dynamodb::types::AttributeValue::S(msg.run_id.clone()),
            )
            .send()
            .await
            .ok()
            .and_then(|r| r.item)
            .and_then(|i| common::run_round::Round::from_item(&i))
            .filter(|r| r.kind == common::run_round::Kind::Feedback)
    } else {
        None
    };
    let mut feedback_round = {
        let base_sha = match prior.as_ref().map(|r| r.base_sha.clone()) {
            Some(b) if !b.is_empty() => b,
            _ => super::round::branch_head(&github, &msg.repo_owner, &msg.repo_name, &branch).await,
        };
        let mut r = common::run_round::Round::start(
            common::run_round::Kind::Feedback,
            0,
            &base_sha,
            &formatted,
            ci_only,
            chrono::Utc::now(),
        );
        r.attempt = msg.round_attempt + 1;
        r
    };
    // The round writes its checkpoints to its own WIP branch; the pull
    // request gets one commit when the round is done.
    feedback_round.wip_branch = super::round::open_wip(
        &github,
        &msg.repo_owner,
        &msg.repo_name,
        &branch,
        &prior.map(|r| r.wip_branch).unwrap_or_default(),
    )
    .await?;
    super::round::record(state, &msg.team_id, &msg.run_id, &feedback_round).await;

    let executor = FeedbackToolExecutor {
        github: &github,
        owner: &msg.repo_owner,
        repo: &msg.repo_name,
        branch: &feedback_round.wip_branch,
        base_branch: &base,
        files_modified: std::sync::Mutex::new(false),
        description: DescriptionRequests::new(may_edit_description),
        scope_guard,
        history: Some(&history),
        graphs: Some(&graphs),
        ops: ops.as_ref(),
        live: live.as_ref(),
        skills: skills.as_ref(),
        web: Some(&web),
        state,
        commits: (conventions, tokens),
    };

    let mut messages = vec![(
        "user".to_string(),
        vec![serde_json::json!({"type": "text", "text": prompt})],
    )];

    let model_id = provider.primary_model_id();
    let mut response = provider::converse(
        state,
        &provider,
        common::agent_policy::Agent::Coder,
        model_id,
        &full_system,
        &mut messages,
        &tools,
        &executor,
        &mut usage,
        llm::ConverseOptions {
            max_turns: 40,
            max_tokens: 16384,
            deadline: Some(super::round::deadline(start)),
            effort: None,
        },
        None,
        None,
    )
    .await?;
    if let Some(s) = &skills {
        s.drain_usage_into(&mut usage);
    }

    // Out of this step's budget with work left: what it wrote is committed.
    // The round continues in a fresh step (the comments are still
    // unanswered, so it picks them up), or the run says it didn't finish.
    // Nothing half-done is posted as a reply.
    if super::implement::reached_deadline(
        &response,
        Some(super::round::deadline(start)),
        std::time::Instant::now(),
    ) {
        if feedback_round.may_continue() {
            let mut next = msg.clone();
            next.round_attempt = feedback_round.attempt;
            if send_delayed_feedback(state, &next, 5).await {
                super::add_progress_note(
                    state,
                    &msg.team_id,
                    &msg.run_id,
                    &format!(
                        "The feedback round used its time budget with work left; it continues in a fresh step with its own budget (step {} of {}).",
                        feedback_round.attempt + 1,
                        common::run_round::MAX_ATTEMPTS
                    ),
                )
                .await;
                return Ok(());
            }
        }
        super::round::finish_incomplete(
            state,
            &msg.team_id,
            &msg.run_id,
            &super::round::Pr {
                github: &github,
                owner: &msg.repo_owner,
                repo: &msg.repo_name,
                branch: &branch,
            },
            &feedback_round,
            &common::run_round::out_of_time(feedback_round.kind, feedback_round.attempt),
        )
        .await;
        super::webhooks::emit_run(state, &msg.team_id, &msg.run_id, "completed").await;
        return Ok(());
    }

    info!(
        run_id = %msg.run_id,
        "Feedback complete: {}",
        common::truncate_str(&response, 200)
    );

    // What the round wrote is on its WIP branch. Check it (scope against
    // the threads, logs, the full test suite), give the agent one fix-up
    // turn for what the checks find, check again, then land it as the
    // round's one commit, or hold it.
    let mut checks_round = super::review_fix::Round::new(
        state,
        &github,
        &provider,
        &msg.team_id,
        &msg.run_id,
        &msg.repo_owner,
        &msg.repo_name,
        &feedback_round.wip_branch,
        msg.pr_number,
    );
    checks_round.requests = review_requests(&comments);
    checks_round.review_body = msg.review_body.clone();
    checks_round.ci_failures = has_ci_failures;
    checks_round.deadline = Some(super::round::deadline(start));
    let land_pr = super::round::Pr {
        github: &github,
        owner: &msg.repo_owner,
        repo: &msg.repo_name,
        branch: &branch,
    };
    let mut fix_pushed = false;
    let mut held: Vec<String> = Vec::new();
    let mut evidence_lines: Vec<String> = Vec::new();
    let mut dismissed: Vec<String> = Vec::new();
    let mut commit_subject = String::new();
    if *executor.files_modified.lock().unwrap() {
        let load = || {
            super::review_fix::load_wip(
                &github,
                &msg.repo_owner,
                &msg.repo_name,
                &feedback_round.base_sha,
                &feedback_round.wip_branch,
            )
        };
        let staged = load().await;
        let loaded = staged.changes();
        let first = super::review_fix::check(&checks_round, &staged, &mut usage, false).await;
        super::review_fix::write_back(
            &github,
            &msg.repo_owner,
            &msg.repo_name,
            &feedback_round.wip_branch,
            &loaded,
            &staged,
        )
        .await;
        let (staged, last) = match first.retry_prompt() {
            Some(fix_up) => {
                info!(run_id = %msg.run_id, findings = first.findings.len(), "Review fix checks found problems — one fix-up turn");
                messages.push((
                    "user".to_string(),
                    vec![serde_json::json!({"type": "text", "text": fix_up})],
                ));
                match provider::converse(
                    state,
                    &provider,
                    common::agent_policy::Agent::Coder,
                    model_id,
                    &full_system,
                    &mut messages,
                    &tools,
                    &executor,
                    &mut usage,
                    llm::ConverseOptions {
                        max_turns: 20,
                        max_tokens: 16384,
                        deadline: Some(super::round::deadline(start)),
                        effort: None,
                    },
                    None,
                    None,
                )
                .await
                {
                    Ok(again)
                        if !again.trim().is_empty()
                            && !super::implement::reached_deadline(
                                &again,
                                Some(super::round::deadline(start)),
                                std::time::Instant::now(),
                            ) =>
                    {
                        response = again
                    }
                    Ok(_) => {}
                    Err(e) => {
                        warn!(run_id = %msg.run_id, error = %e, "Fix-up turn failed — checking the round as it stands")
                    }
                }
                // What the first check took out stays on the record.
                evidence_lines.extend(
                    first
                        .evidence
                        .iter()
                        .filter(|l| l.starts_with("Scope: took out"))
                        .cloned(),
                );
                let staged = load().await;
                let loaded = staged.changes();
                let last = super::review_fix::check(&checks_round, &staged, &mut usage, true).await;
                super::review_fix::write_back(
                    &github,
                    &msg.repo_owner,
                    &msg.repo_name,
                    &feedback_round.wip_branch,
                    &loaded,
                    &staged,
                )
                .await;
                (staged, last)
            }
            None => (staged, first),
        };
        evidence_lines.extend(last.evidence.iter().cloned());
        held = last.hold.clone();
        if staged.is_empty() && held.is_empty() {
            held.push(
                "every change was outside what the review comments asked for, so nothing was pushed"
                    .to_string(),
            );
        }
        if held.is_empty() {
            let (message, how) = super::review_fix::commit_message(
                &github,
                &msg.repo_owner,
                &msg.repo_name,
                &base,
                &executor.commits.0,
                &executor.commits.1,
                &staged.messages(),
            )
            .await;
            evidence_lines.push(format!("Commit message: {how}"));
            commit_subject = message.lines().next().unwrap_or("").to_string();
            let approvers_before = github
                .list_pr_reviews(&msg.repo_owner, &msg.repo_name, msg.pr_number)
                .await
                .map(|r| super::review_fix::approvals::approvers(&r))
                .unwrap_or_default();
            // The round's own landing: one commit, one push.
            match super::round::land(
                &land_pr,
                &feedback_round.wip_branch,
                &feedback_round.base_sha,
                &message,
            )
            .await
            {
                super::round::Landed::Pushed { .. } => {
                    fix_pushed = true;
                    dismissed = super::review_fix::dismissed_after_push(
                        &github,
                        &msg.repo_owner,
                        &msg.repo_name,
                        msg.pr_number,
                        &approvers_before,
                    )
                    .await;
                }
                super::round::Landed::Nothing => {}
                super::round::Landed::Refused(why) => {
                    super::round::finish_incomplete(
                        state,
                        &msg.team_id,
                        &msg.run_id,
                        &land_pr,
                        &feedback_round,
                        &format!("its changes couldn't land on the pull request: {why}"),
                    )
                    .await;
                    super::webhooks::emit_run(state, &msg.team_id, &msg.run_id, "completed").await;
                    return Ok(());
                }
            }
        } else {
            info!(run_id = %msg.run_id, reasons = ?held, "Review fix held — not landed; its WIP branch is kept");
        }
        let head = if fix_pushed {
            super::round::branch_head(&github, &msg.repo_owner, &msg.repo_name, &branch).await
        } else {
            String::new()
        };
        let round_record = super::review_fix::evidence::Round {
            date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
            commit: head[..head.len().min(7)].to_string(),
            headline: if fix_pushed {
                commit_subject.clone()
            } else {
                held.join("; ")
            },
            lines: {
                let mut l = evidence_lines.clone();
                if !dismissed.is_empty() {
                    l.push(format!(
                        "Approvals dismissed by the push: {}; told the requester",
                        dismissed.join(", ")
                    ));
                }
                if !constraints.is_empty() {
                    l.push(format!(
                        "Requester constraints obeyed: {}",
                        constraints.lines().join(", ")
                    ));
                }
                l
            },
        };
        if fix_pushed || !held.is_empty() {
            super::review_fix::record_round(
                &github,
                &msg.repo_owner,
                &msg.repo_name,
                msg.pr_number,
                &round_record,
            )
            .await;
        }
    }
    if !dismissed.is_empty() {
        super::review_fix::mark_reapproval(state, &msg.team_id, &msg.run_id, &dismissed).await;
        super::review_fix::tell_requester(
            state,
            &github,
            &msg.team_id,
            &msg.run_id,
            &ticket_info,
            "Approval needed again",
            &super::review_fix::approvals::notice_plain(&checks_round.pr_url(), &dismissed),
        )
        .await;
    }
    // A held round's replies say so, first.
    let response = super::review_fix::held_replies(&response, &held);
    // Who must approve again, on the reply to the person who asked.
    let reapproval_note = if dismissed.is_empty() {
        String::new()
    } else {
        format!(
            "\n\n{}",
            super::review_fix::approvals::notice(&checks_round.pr_url(), &dismissed)
        )
    };

    // Reply to each review comment in its own thread.
    // Split the response by ---SPLIT--- markers to get per-comment replies.
    let sections: Vec<&str> = response
        .split("---SPLIT---")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    // The description: rewritten when the code changed (so it stays true)
    // or when a person with write access asked for a change to it, even
    // with no code change. What changed goes on the reply.
    // The round's changes landed above, as one commit in one push.
    let asked = executor.description.taken();
    let description_note = if fix_pushed || !asked.is_empty() {
        let changed = super::pr::refresh_description(
            state,
            &github,
            (&msg.team_id, msg.installation_id),
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
            &team_instructions,
            &asked,
            &provider,
            &mut usage,
        )
        .await;
        description_reply(&asked, changed.as_deref())
    } else {
        String::new()
    };

    // Redirects and links the PR changes are checked again after a push
    // (or when the evidence is a day old) and the description's table
    // updated in the same round; replies point at it, and a bare "Done" is
    // replaced by where the evidence is.
    let links = super::link_checks::refresh_pr(
        state,
        &github,
        &msg.team_id,
        &msg.repo_owner,
        &msg.repo_name,
        msg.pr_number,
        &msg.run_id,
        fix_pushed,
    )
    .await;
    // What the failing-test rules refuse in the pushed change is said on
    // the PR (the ready check keeps it a draft), unless the requester asked
    // for no PR comments.
    if fix_pushed && constraints.may_comment() {
        super::test_fix::after_feedback(
            state,
            &github,
            &msg.team_id,
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
        )
        .await;
    }
    let head_sha = if fix_pushed {
        github
            .get_pull_request(&msg.repo_owner, &msg.repo_name, msg.pr_number)
            .await
            .ok()
            .and_then(|p| p["head"]["sha"].as_str().map(String::from))
    } else {
        None
    };
    let pr_url = format!(
        "https://github.com/{}/{}/pull/{}",
        msg.repo_owner, msg.repo_name, msg.pr_number
    );
    let sections: Vec<String> = sections
        .iter()
        .map(|s| {
            super::link_checks::evidenced_reply(s, links.as_ref(), &pr_url, head_sha.as_deref())
        })
        .collect();
    let response = if sections.is_empty() {
        response
    } else {
        sections.join("\n---SPLIT---\n")
    };

    // Which team agents were asked goes once, on the last reply, after what
    // changed in the description.
    let consulted = format!(
        "{description_note}{}{reapproval_note}",
        skills
            .as_ref()
            .map(|s| s.consulted_markdown(super::skills::app_url(&state.config.stage)))
            .unwrap_or_default()
    );

    // Only match against actionable (non-context) comments for reply posting
    let actionable_comments: Vec<&ReviewComment> = comments
        .iter()
        .filter(|c| c.comment_id.is_some() && !c.is_context)
        .collect();

    // The branch update's line leads the answer to the person who asked.
    let response = match &update_reply {
        Some(line) if actionable_comments.is_empty() || !constraints.may_comment() => {
            format!("{line}\n\n{response}")
        }
        Some(line) => {
            // Inline threads carry the rest: the update gets its own reply.
            github
                .create_issue_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.pr_number,
                    &addressed(&msg.trigger_author, line),
                )
                .await?;
            response
        }
        None => response,
    };

    if !constraints.may_comment() {
        // The requester said not to post on the pull request: the replies go
        // to them on the run page and the ticket instead.
        let text = format!("{}{consulted}", response.replace("---SPLIT---", "\n\n"));
        if !actionable_comments.is_empty() || !msg.trigger_author.trim().is_empty() {
            super::review_fix::tell_requester(
                state,
                &github,
                &msg.team_id,
                &msg.run_id,
                &ticket_info,
                &format!("Review fix on pull request #{}", msg.pr_number),
                text.trim(),
            )
            .await;
        }
        info!(run_id = %msg.run_id, "Requester asked for no PR comments — replies sent to the run and the ticket");
    } else if let Some(reply) = top_level_reply(
        &msg.trigger_author,
        actionable_comments.len(),
        &response,
        &consulted,
    ) {
        // A person's top-level comment (or review body) addressed to CoderHelm
        // has no inline thread to answer in: reply on the PR, to them, so
        // nobody is left unanswered.
        github
            .create_issue_comment(&msg.repo_owner, &msg.repo_name, msg.pr_number, &reply)
            .await?;
        info!(run_id = %msg.run_id, author = %msg.trigger_author, "Replied to a top-level PR comment");
    } else if actionable_comments.is_empty() {
        // CI-only fix — the commit diff speaks for itself, no comment needed
        info!(run_id = %msg.run_id, "CI-only feedback — skipping PR comment (commit is the communication)");
    } else if sections.len() == actionable_comments.len() {
        // Matched — post each section to its corresponding comment thread
        let last = sections.len() - 1;
        for (i, (section, comment)) in sections.iter().zip(actionable_comments.iter()).enumerate() {
            let mut reply = if section.len() > 65000 {
                format!("{}\n\n*(truncated)*", common::truncate_str(section, 65000))
            } else {
                section.to_string()
            };
            if i == last {
                reply.push_str(&consulted);
            }
            github
                .reply_to_review_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.pr_number,
                    comment.comment_id.unwrap(),
                    &reply,
                )
                .await?;
        }
    } else {
        // Fallback — reply to the first review thread
        let reply_text = sections
            .first()
            .map(|s| s.to_string())
            .unwrap_or(response.clone());
        let reply = if reply_text.len() > 65000 {
            format!("{}\n\n*(truncated)*", &reply_text[..65000])
        } else {
            reply_text
        };
        let reply = format!("{reply}{consulted}");
        if let Some(comment_id) = actionable_comments.first().and_then(|c| c.comment_id) {
            github
                .reply_to_review_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.pr_number,
                    comment_id,
                    &reply,
                )
                .await?;
        }
    }

    // Note: We intentionally do NOT auto-resolve review threads.
    // The reviewer should resolve threads themselves once they're satisfied.
    // Auto-resolving kills the conversation when the reviewer wants to follow up.
    if false {
        let node_ids: Vec<&str> = actionable_comments
            .iter()
            .filter_map(|c| c.node_id.as_deref())
            .collect();
        if !node_ids.is_empty() {
            match github
                .get_review_thread_ids(&msg.repo_owner, &msg.repo_name, msg.pr_number, &node_ids)
                .await
            {
                Ok(thread_map) => {
                    let mut resolved = std::collections::HashSet::new();
                    for thread_id in thread_map.values() {
                        if resolved.insert(thread_id.clone()) {
                            if let Err(e) = github.resolve_review_thread(thread_id).await {
                                tracing::warn!(thread_id, error = %e, "Failed to resolve thread");
                            }
                        }
                    }
                    info!(
                        run_id = %msg.run_id,
                        resolved = resolved.len(),
                        "Resolved review threads after replying"
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to fetch thread IDs for resolution");
                }
            }
        }
    }

    // If feedback committed code changes — OR the pre-feedback conflict
    // resolution pushed a merge commit — set awaiting_ci so the CI result
    // has a consumer.
    // (The description was rewritten above, before the replies went out.)
    let code_pushed = fix_pushed || conflicts_pushed;
    // A push that carried CI failures to fix is a CI fix, counted with its time.
    if fix_pushed && has_ci_failures {
        super::record_ci_outcome(
            state,
            &msg.team_id,
            &msg.run_id,
            super::CiOutcome::FixCommitted,
        )
        .await;
    }
    let final_status = if code_pushed {
        "awaiting_ci"
    } else {
        "completed"
    };

    // A terminal completion must leave the PR ready for review. Every other
    // completion path marks it ready (resume review-pass, the CI-green early
    // return above, the budget hand-off); the normal feedback completion skipped
    // it, stranding the PR as a draft even though the run was done. Idempotent.
    if final_status == "completed" {
        crate::passes::resume::mark_pr_ready(
            state,
            &msg.team_id,
            &github,
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
        )
        .await;
    }

    // Update run record in runs table (including status_run_id for GSI)
    let now = chrono::Utc::now().to_rfc3339();
    let update = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
        )
        .key(
            "run_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.run_id.clone()),
        )
        .update_expression(
            "SET tokens_in = tokens_in + :ti, tokens_out = tokens_out + :to, \
             cache_read_tokens = if_not_exists(cache_read_tokens, :zero) + :crt, \
             cache_write_tokens = if_not_exists(cache_write_tokens, :zero) + :cwt, \
             cost_usd = if_not_exists(cost_usd, :zero) + :c, \
             updated_at = :t, #s = :s, current_pass = :p, status_run_id = :sri, \
             pass_history = list_append(if_not_exists(pass_history, :empty), :entry) \
             REMOVE error_message, #err, #round, #inc",
        )
        .condition_expression(common::run_status::not_ended("#s"))
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#err", "error")
        // The round finished.
        .expression_attribute_names("#round", common::run_round::FIELD)
        .expression_attribute_names("#inc", common::run_round::INCOMPLETE_FIELD)
        .ended_values()
        .expression_attribute_values(
            ":ti",
            aws_sdk_dynamodb::types::AttributeValue::N(usage.input_tokens.to_string()),
        )
        .expression_attribute_values(
            ":to",
            aws_sdk_dynamodb::types::AttributeValue::N(usage.output_tokens.to_string()),
        )
        .expression_attribute_values(
            ":crt",
            aws_sdk_dynamodb::types::AttributeValue::N(usage.cache_read_tokens.to_string()),
        )
        .expression_attribute_values(
            ":cwt",
            aws_sdk_dynamodb::types::AttributeValue::N(usage.cache_write_tokens.to_string()),
        )
        .expression_attribute_values(
            ":zero",
            aws_sdk_dynamodb::types::AttributeValue::N("0".to_string()),
        )
        .expression_attribute_values(
            ":c",
            aws_sdk_dynamodb::types::AttributeValue::N(format!("{:.4}", usage.estimated_cost())),
        )
        .expression_attribute_values(
            ":t",
            aws_sdk_dynamodb::types::AttributeValue::S(now.clone()),
        )
        .expression_attribute_values(
            ":s",
            aws_sdk_dynamodb::types::AttributeValue::S(final_status.to_string()),
        )
        .expression_attribute_values(
            ":p",
            aws_sdk_dynamodb::types::AttributeValue::S("feedback".to_string()),
        )
        .expression_attribute_values(
            ":sri",
            aws_sdk_dynamodb::types::AttributeValue::S(format!("{final_status}#{}", msg.run_id)),
        )
        .expression_attribute_values(
            ":entry",
            aws_sdk_dynamodb::types::AttributeValue::L(vec![
                aws_sdk_dynamodb::types::AttributeValue::M(
                    [
                        (
                            "pass".to_string(),
                            aws_sdk_dynamodb::types::AttributeValue::S("feedback".to_string()),
                        ),
                        (
                            "started_at".to_string(),
                            aws_sdk_dynamodb::types::AttributeValue::S(now),
                        ),
                    ]
                    .into(),
                ),
            ]),
        )
        .expression_attribute_values(":empty", aws_sdk_dynamodb::types::AttributeValue::L(vec![]))
        .send()
        .await;
    match update {
        Ok(_) => {}
        // The run was cancelled while this pass worked (a person closed the
        // PR, which a parallel pass saw first). That ends the run; it is not
        // this pass failing, and the cancel must stand.
        Err(e) if common::run_fail::is_condition_failed(&e) => {
            info!(run_id = %msg.run_id, "Run was cancelled while feedback ran — leaving it cancelled");
            return Ok(());
        }
        Err(e) => return Err(common::run_fail::error_text(&e).into()),
    }

    // The team's counters already have these tokens: each model call is
    // counted as it happens (common::usage).

    // If code was pushed, schedule a delayed resume to watch for CI
    if code_pushed {
        info!(run_id = %msg.run_id, "Feedback pushed code — scheduling CI check");
        if !state.config.ci_fix_queue_url.is_empty() {
            let resume_body = serde_json::json!({
                "type": "resume",
                "team_id": msg.team_id,
                "run_id": msg.run_id,
                "installation_id": msg.installation_id,
            });
            let _ = state
                .sqs
                .send_message()
                .queue_url(&state.config.ci_fix_queue_url)
                .message_body(resume_body.to_string())
                .delay_seconds(120)
                .send()
                .await;
        }
    }

    Ok(())
}

/// Sends feedback again after a transient model error, per
/// [`common::model_retry::requeue_delay`]. False once the policy is spent.
pub(crate) async fn requeue_after_model_error(state: &WorkerState, msg: &FeedbackMessage) -> bool {
    let Some(delay) = common::model_retry::requeue_delay(msg.model_retries) else {
        return false;
    };
    let mut next = msg.clone();
    next.model_retries = msg.model_retries + 1;
    let sent = send_delayed_feedback(state, &next, delay as i32).await;
    if sent {
        warn!(run_id = %msg.run_id, retry = next.model_retries, delay, "model_error_requeued: feedback sent again after a transient model error");
    }
    sent
}

/// Re-enqueue a feedback message to the feedback queue with an SQS delay, so a
/// feedback that lost the per-run writer-slot race retries once the holder
/// releases the slot — instead of failing the run or dropping the review.
/// Fire-and-forget: logs on failure, never propagates (the caller returns Ok).
/// Returns `true` if the feedback was re-enqueued (or intentionally skipped for a
/// cancelled run); `false` if it could not be deferred (misconfig / SQS error) so
/// the caller can surface the loss instead of silently succeeding.
async fn send_delayed_feedback(
    state: &WorkerState,
    msg: &FeedbackMessage,
    delay_seconds: i32,
) -> bool {
    if state.config.feedback_queue_url.is_empty() {
        error!("FEEDBACK_QUEUE_URL not configured — cannot defer feedback (review would be lost)");
        return false;
    }
    // Never re-arm feedback for a cancelled run — nothing to apply, no loss.
    if run_is_cancelled(state, &msg.team_id, &msg.run_id).await {
        info!(run_id = %msg.run_id, "Run cancelled — not deferring feedback");
        return true;
    }
    // Serialize back to the tagged wire form ({"type":"feedback", ...}) the worker
    // consumes. to_value on &FeedbackMessage avoids needing the struct to be Clone.
    let mut body = match serde_json::to_value(msg) {
        Ok(v) => v,
        Err(e) => {
            error!(run_id = %msg.run_id, error = %e, "Failed to serialize feedback for defer");
            return false;
        }
    };
    if let Some(obj) = body.as_object_mut() {
        obj.insert("type".to_string(), json!("feedback"));
        // Bump the defer counter so the caller's cap can bound a wedged slot.
        obj.insert("defer_count".to_string(), json!(msg.defer_count + 1));
    }
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.feedback_queue_url)
        .message_body(body.to_string())
        .delay_seconds(delay_seconds)
        .send()
        .await
    {
        Ok(_) => {
            info!(
                run_id = %msg.run_id,
                delay_seconds,
                next_defer = msg.defer_count + 1,
                "Feedback deferred via delayed re-enqueue"
            );
            true
        }
        Err(e) => {
            error!(run_id = %msg.run_id, error = %e, "Failed to defer feedback");
            false
        }
    }
}

/// The base branch to scope, restore, and merge against — read from the LIVE PR,
/// which is ground truth for what GitHub will actually merge into. A persisted
/// run-record `base_branch` can go stale and poison the whole fix loop: a run was
/// once stamped `main` while its PR was correctly opened against `develop`, and the
/// loop trusted the record — every cycle merged `main` (re-adding `main`-only files
/// that looked like out-of-scope changes) and reverted to `main` (a no-op relative
/// to the real base `develop`, so the files never actually left the diff). Reading
/// the PR's `base.ref` each cycle makes that divergence impossible. Order:
/// PR base → repo default branch → `main` (last resort, warned). Deliberately does
/// NOT read the run record's `base_branch`, which is exactly the value we distrust.
async fn resolve_pr_base(github: &GitHubClient, msg: &FeedbackMessage) -> String {
    if msg.pr_number > 0 {
        if let Ok(pr) = github
            .get_pull_request(&msg.repo_owner, &msg.repo_name, msg.pr_number)
            .await
        {
            if let Some(b) = pr["base"]["ref"].as_str().filter(|s| !s.is_empty()) {
                return b.to_string();
            }
        }
    }
    match github
        .get_default_branch(&msg.repo_owner, &msg.repo_name)
        .await
    {
        Ok(b) if !b.is_empty() => b,
        _ => {
            warn!(
                repo = %format!("{}/{}", msg.repo_owner, msg.repo_name),
                pr_number = msg.pr_number,
                "Could not resolve PR base or default branch — falling back to main"
            );
            "main".to_string()
        }
    }
}

/// Best-effort: overwrite the run record's stored `base_branch` with the
/// authoritative value resolved from the PR, so every OTHER reader (resume loop,
/// dashboard) is consistent and a stale `main` can't linger. Failures are ignored —
/// the fix loop already resolves from the PR directly and does not depend on this.
async fn heal_run_base_branch(state: &WorkerState, msg: &FeedbackMessage, base: &str) {
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
        )
        .key(
            "run_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.run_id.clone()),
        )
        .update_expression("SET base_branch = :b")
        .expression_attribute_values(
            ":b",
            aws_sdk_dynamodb::types::AttributeValue::S(base.to_string()),
        )
        .send()
        .await;
}

async fn get_pr_branch(
    state: &WorkerState,
    msg: &FeedbackMessage,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let result = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
        )
        .key(
            "run_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.run_id.clone()),
        )
        .send()
        .await?;

    let item = result.item().ok_or("Run record not found")?;
    let branch = item
        .get("branch")
        .and_then(|v| v.as_s().ok())
        .ok_or("Branch not found in run record")?;
    Ok(branch.clone())
}

fn format_review_comments(review_body: &str, comments: &[ReviewComment]) -> String {
    let mut parts = Vec::new();

    if !review_body.is_empty() {
        parts.push(format!("### Review Summary\n{review_body}\n"));
    }

    if comments.is_empty() && review_body.is_empty() {
        return "(no comments)".to_string();
    }

    for (i, c) in comments.iter().enumerate() {
        let line_info = c.line.map(|l| format!(" line {l}")).unwrap_or_default();
        parts.push(format!(
            "### Comment #{} \nFile: `{}`{}\n{}\n",
            i + 1,
            c.path,
            line_info,
            c.body,
        ));
    }

    parts.join("\n")
}

/// What each review thread asks, for the scope check: one request per
/// comment to answer, with the earlier comments of its thread (same file
/// and line) as context.
fn review_requests(comments: &[ReviewComment]) -> Vec<super::review_fix::scope::Request> {
    comments
        .iter()
        .filter(|c| !c.is_context)
        .map(|c| super::review_fix::scope::Request {
            path: c.path.clone(),
            line: c.line,
            body: c.body.clone(),
            context: comments
                .iter()
                .filter(|o| o.is_context && o.path == c.path && o.line == c.line)
                .map(|o| o.body.clone())
                .collect(),
        })
        .collect()
}

/// Fetch review comments from GitHub API for a given review.
/// Also collects prior thread context (earlier comments on the same file+line)
/// so the LLM sees the full conversation, not just the latest comment.
async fn fetch_review_comments(github: &GitHubClient, msg: &FeedbackMessage) -> Vec<ReviewComment> {
    match github
        .get_review_comments(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
    {
        Ok(api_comments) => {
            // Identify the new comments from this review
            let review_comments: Vec<&serde_json::Value> = api_comments
                .iter()
                .filter(|c| {
                    c.get("pull_request_review_id")
                        .and_then(|v| v.as_u64())
                        .map(|id| id == msg.review_id)
                        .unwrap_or(false)
                })
                .collect();

            // Collect thread keys (path + line) for the new comments
            let thread_keys: std::collections::HashSet<(String, Option<u64>)> = review_comments
                .iter()
                .map(|c| {
                    let path = c["path"].as_str().unwrap_or("").to_string();
                    let line = c["line"].as_u64();
                    (path, line)
                })
                .collect();

            // Collect new comment IDs to avoid duplicating them in context
            let new_ids: std::collections::HashSet<u64> = review_comments
                .iter()
                .filter_map(|c| c["id"].as_u64())
                .collect();

            // Find earlier comments in the same threads (context)
            let mut context: Vec<ReviewComment> = api_comments
                .iter()
                .filter(|c| {
                    let path = c["path"].as_str().unwrap_or("").to_string();
                    let line = c["line"].as_u64();
                    let id = c["id"].as_u64().unwrap_or(0);
                    thread_keys.contains(&(path, line)) && !new_ids.contains(&id)
                })
                .map(|c| {
                    let author = c["user"]["login"].as_str().unwrap_or("unknown");
                    let body = c["body"].as_str().unwrap_or("");
                    ReviewComment {
                        path: c["path"].as_str().unwrap_or("").to_string(),
                        line: c["line"].as_u64(),
                        body: format!("[{author}]: {body}"),
                        comment_id: c["id"].as_u64(),
                        node_id: c["node_id"].as_str().map(|s| s.to_string()),
                        is_context: true,
                    }
                })
                .collect();

            // Append the new review comments (these are the ones to address)
            let mut new: Vec<ReviewComment> = review_comments
                .iter()
                .map(|c| ReviewComment {
                    path: c["path"].as_str().unwrap_or("").to_string(),
                    line: c["line"].as_u64(),
                    body: c["body"].as_str().unwrap_or("").to_string(),
                    comment_id: c["id"].as_u64(),
                    node_id: c["node_id"].as_str().map(|s| s.to_string()),
                    is_context: false,
                })
                .collect();

            context.append(&mut new);
            context
        }
        Err(e) => {
            tracing::warn!("Failed to fetch review comments: {e}");
            vec![]
        }
    }
}

/// Fetch ALL non-bot review comments from a PR (used by re-review path).
/// Returns every comment from an author with standing in the repository
/// (see common::standing); an outsider's inline comment is never taken as an
/// instruction just because someone else started this pass. filter_unanswered
/// will then remove ones the bot has already replied to.
async fn fetch_all_human_comments(
    github: &GitHubClient,
    msg: &FeedbackMessage,
) -> Vec<ReviewComment> {
    match github
        .get_review_comments(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
    {
        Ok(api_comments) => api_comments
            .iter()
            .filter(|c| {
                // Skip bot-authored comments
                !c["user"]["login"]
                    .as_str()
                    .map(|l| l.contains("coderhelm"))
                    .unwrap_or(false)
            })
            .filter(|c| common::standing::shown_by_github(c))
            .map(|c| ReviewComment {
                path: c["path"].as_str().unwrap_or("").to_string(),
                line: c["line"].as_u64(),
                body: c["body"].as_str().unwrap_or("").to_string(),
                comment_id: c["id"].as_u64(),
                node_id: c["node_id"].as_str().map(|s| s.to_string()),
                is_context: false,
            })
            .collect(),
        Err(e) => {
            tracing::warn!("Failed to fetch all human comments: {e}");
            vec![]
        }
    }
}

/// Filter out comments that the bot has already replied to.
async fn filter_unanswered(
    github: &GitHubClient,
    msg: &FeedbackMessage,
    comments: Vec<ReviewComment>,
) -> Vec<ReviewComment> {
    if comments.is_empty() {
        return comments;
    }

    // Fetch all PR comments to check for existing bot replies
    let all_comments = match github
        .get_review_comments(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
    {
        Ok(c) => c,
        Err(_) => return comments, // can't check — process all
    };

    // GitHub review threads are FLAT: every reply's in_reply_to_id points at
    // the thread ROOT, even replies posted to a follow-up comment. Keying
    // "answered" on the replied-to id therefore never matched follow-ups and
    // the bot re-answered them on every re-review. Instead: a comment is
    // answered when the bot has a LATER comment (higher id) in the SAME
    // thread (thread key = in_reply_to_id, or own id for roots).
    let bot_replies: Vec<(u64, u64)> = all_comments
        .iter()
        .filter(|c| {
            c["user"]["login"]
                .as_str()
                .map(|l| l.contains("coderhelm"))
                .unwrap_or(false)
        })
        .filter_map(|c| {
            let id = c["id"].as_u64()?;
            let thread = c["in_reply_to_id"].as_u64().unwrap_or(id);
            Some((thread, id))
        })
        .collect();
    let thread_of: std::collections::HashMap<u64, u64> = all_comments
        .iter()
        .filter_map(|c| {
            let id = c["id"].as_u64()?;
            Some((id, c["in_reply_to_id"].as_u64().unwrap_or(id)))
        })
        .collect();

    let before = comments.len();
    let filtered: Vec<ReviewComment> = comments
        .into_iter()
        .filter(|c| {
            let Some(id) = c.comment_id else { return true };
            let thread = thread_of.get(&id).copied().unwrap_or(id);
            !bot_replies
                .iter()
                .any(|(t, bot_id)| *t == thread && *bot_id > id)
        })
        .collect();

    if filtered.len() < before {
        info!(
            run_id = %msg.run_id,
            skipped = before - filtered.len(),
            remaining = filtered.len(),
            "Skipped already-answered comments"
        );
    }

    filtered
}

fn feedback_tools() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "read_file".to_string(),
            description: "Read a file. Prefer read_file_lines for targeted reads.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "File path relative to repo root"}
                },
                "required": ["path"]
            }),
        },
        ToolDefinition {
            name: "read_file_lines".to_string(),
            description: "Read specific lines from a file (1-indexed, inclusive).".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "start_line": {"type": "integer"},
                    "end_line": {"type": "integer"}
                },
                "required": ["path", "start_line", "end_line"]
            }),
        },
        ToolDefinition {
            name: "search_code".to_string(),
            description: "Search for code by keyword. Returns matching file paths and fragments."
                .to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"}
                },
                "required": ["query"]
            }),
        },
        ToolDefinition {
            name: "read_tree".to_string(),
            description: "Get the full recursive file tree.".to_string(),
            input_schema: json!({"type": "object", "properties": {}}),
        },
        ToolDefinition {
            name: "edit_file".to_string(),
            description: "Make targeted edits to an existing file using search/replace — the right tool for changing a few lines, ESPECIALLY in a large file where rewriting the whole thing is impractical or unsafe. Each edit replaces one occurrence of old_text with new_text; old_text must match the file EXACTLY (whitespace, quotes, commas) and uniquely. Prefer this over write_file for review-comment fixes. If an edit keeps failing on special-character content, fall back to write_file with the COMPLETE file.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "File path relative to repo root"},
                    "edits": {
                        "type": "array",
                        "description": "Array of search/replace edits to apply in order",
                        "items": {
                            "type": "object",
                            "properties": {
                                "old_text": {"type": "string", "description": "Exact text to find (must match uniquely)"},
                                "new_text": {"type": "string", "description": "Replacement text"}
                            },
                            "required": ["old_text", "new_text"]
                        }
                    },
                    "message": {"type": "string", "description": "Commit message"}
                },
                "required": ["path", "edits", "message"]
            }),
        },
        ToolDefinition {
            name: "write_file".to_string(),
            description: "Create or update a single file.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "content": {"type": "string"},
                    "message": {"type": "string"}
                },
                "required": ["path", "content", "message"]
            }),
        },
        ToolDefinition {
            name: "batch_write".to_string(),
            description: "Atomically write multiple files in a single commit.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "message": {"type": "string"},
                    "files": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "path": {"type": "string"},
                                "content": {"type": "string"},
                                "action": {"type": "string", "enum": ["write", "delete"]}
                            },
                            "required": ["path"]
                        }
                    }
                },
                "required": ["message", "files"]
            }),
        },
        ToolDefinition {
            name: "restore_file".to_string(),
            description: "Restore a file to its EXACT content at a git ref without loading the content into your context (byte-exact, any size). Use it to recover a file a bad merge corrupted, or to REMOVE an out-of-scope change by restoring the file to the PR's base. IMPORTANT: to remove out-of-scope changes, OMIT from_ref — it defaults to this PR's base branch. Do NOT pass \"main\": this PR's base may be a different branch (e.g. `develop`), and restoring to `main` leaves the out-of-scope change in the diff. Only pass from_ref for an unusual source (a specific commit SHA).".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "File path relative to repo root"},
                    "from_ref": {"type": "string", "description": "Optional git ref to restore from (a commit SHA). Omit to restore from this PR's base branch — that is what removes an out-of-scope change."},
                    "message": {"type": "string", "description": "Commit message (optional)"}
                },
                "required": ["path"]
            }),
        },
    ]
}

/// The tool a person's comment reaches the PR description through.
fn update_description_tool() -> ToolDefinition {
    ToolDefinition {
        name: "update_pr_description".to_string(),
        description: "Change this pull request's description as the person asked (remove a \
                      banner, fix a name, add a section such as Not in this PR with an owner per \
                      item). Pass the change in the person's words, with every value they gave. \
                      CoderHelm rewrites the description once you finish (after any code you \
                      commit), keeps its shape, and adds what changed to your reply. Only people \
                      with write access to the repository can have it changed."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "change": {"type": "string", "description": "What to change in the description, with the exact values"}
            },
            "required": ["change"]
        }),
    }
}

/// What people asked to change in the PR description this round, and
/// whether the person who asked may have it changed.
struct DescriptionRequests {
    allowed: bool,
    asked: std::sync::Mutex<Vec<String>>,
}

impl DescriptionRequests {
    fn new(allowed: bool) -> Self {
        Self {
            allowed,
            asked: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Records one request and says what happens with it.
    fn ask(&self, change: &str) -> String {
        let change = change.split_whitespace().collect::<Vec<_>>().join(" ");
        if change.is_empty() {
            return "Say what to change in the description.".to_string();
        }
        if !self.allowed {
            return "Not changed: only people with write access to this repository can have the \
                    description changed. Tell the person so in your reply, politely; change \
                    nothing else for it."
                .to_string();
        }
        if let Ok(mut a) = self.asked.lock() {
            if !a.contains(&change) {
                a.push(change);
            }
        }
        "Recorded. CoderHelm rewrites the description when you finish and adds what changed to \
         your reply, so don't describe the description change yourself."
            .to_string()
    }

    /// The requests a person with write access made (none otherwise).
    fn taken(&self) -> Vec<String> {
        if !self.allowed {
            return Vec::new();
        }
        self.asked.lock().map(|a| a.clone()).unwrap_or_default()
    }
}

/// Pure: what the reply says about the description. Nothing when nobody
/// asked for a change to it (a rewrite after code changes goes unannounced,
/// as before); what changed when it was saved; that it couldn't be changed
/// when a request didn't go through.
fn description_reply(asked: &[String], changed: Option<&[String]>) -> String {
    if asked.is_empty() {
        return String::new();
    }
    match changed {
        Some(list) if !list.is_empty() => format!(
            "\n\nUpdated the description:\n{}",
            list.iter()
                .map(|c| format!("- {c}\n"))
                .collect::<String>()
                .trim_end()
        ),
        Some(_) => "\n\nUpdated the description as asked.".to_string(),
        None => "\n\nI couldn't update the description this time, so it is unchanged; ask again \
                 and I'll retry."
            .to_string(),
    }
}

struct FeedbackToolExecutor<'a> {
    github: &'a GitHubClient,
    /// What people asked to change in the PR description.
    description: DescriptionRequests,
    owner: &'a str,
    repo: &'a str,
    branch: &'a str,
    /// The PR's base branch — restore_file defaults to it so out-of-scope
    /// reverts target the real base (not a hardcoded `main`).
    base_branch: &'a str,
    files_modified: std::sync::Mutex<bool>,
    /// Ticket-scope guard: CI-only cycles may only write files the PR already
    /// changed (bounded escape). Human-directed feedback is unscoped.
    scope_guard: super::write_guard::ScopeGuard,
    /// Repository history (commits, blame, pull requests, releases, deploys).
    history: Option<&'a super::github_history::GithubHistory>,
    /// The code graph, shared with every agent that reads code.
    graphs: Option<&'a super::code_graph::RepoGraphs>,
    /// Monitoring, docs and pipeline connections (New Relic, Confluence,
    /// CloudWatch metrics, Airflow) and the AWS identity check.
    ops: Option<&'a super::ops_tools::OpsTools>,
    /// The team's connected live systems, read-only.
    live: Option<&'a super::live_sources::LiveSources>,
    /// `check_url` on the team's own public pages.
    web: Option<&'a super::web_check::WebCheck>,
    /// The team's written skills, read-only.
    skills: Option<&'a super::skills::TeamSkills>,
    state: &'a WorkerState,
    /// The team's git conventions and the ticket's tokens: every commit
    /// message follows the team's commit template.
    commits: (
        common::git_conventions::GitConventions,
        common::git_conventions::Tokens,
    ),
}

impl FeedbackToolExecutor<'_> {
    /// The commit message for the agent's own `message`, in the team's
    /// commit template when it set one.
    fn commit_message(&self, message: &str) -> String {
        self.commits.0.commit_message(&self.commits.1, message)
    }
}

#[async_trait::async_trait]
impl<'a> ToolExecutor for FeedbackToolExecutor<'a> {
    async fn execute(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(graphs) = self.graphs {
            if let Some(out) = graphs.handle(self.state, name, input).await {
                return Ok(serde_json::json!(out));
            }
        }
        if let Some(history) = self.history {
            if let Some(out) = history.handle(self.state, self.github, name, input).await {
                return Ok(serde_json::json!(out));
            }
        }
        if let Some(web) = self.web {
            if let Some(out) = web.handle(self.state, self.github, name, input).await {
                return Ok(serde_json::json!(out));
            }
        }
        if let Some(ops) = self.ops {
            if let Some(out) = ops.handle(self.state, name, input).await {
                return Ok(serde_json::json!(out));
            }
        }
        if let Some(live) = self.live {
            if let Some(out) = live.handle(self.state, name, input).await {
                return Ok(serde_json::json!(out));
            }
        }
        if let Some(skills) = self.skills {
            if let Some(out) = skills
                .handle(
                    self.state,
                    name,
                    input,
                    &skills.host(
                        self,
                        Some(super::agent_tools::RepoReads {
                            github: self.github,
                            owner: self.owner,
                            repo: self.repo,
                            git_ref: self.branch,
                            http: &self.state.http,
                        }),
                    ),
                )
                .await
            {
                return Ok(serde_json::json!(out));
            }
        }
        match name {
            "update_pr_description" => Ok(serde_json::json!(self
                .description
                .ask(input["change"].as_str().unwrap_or("")))),
            "read_tree" => {
                let tree = self
                    .github
                    .get_tree(self.owner, self.repo, self.branch)
                    .await?;
                let paths: Vec<&str> = tree
                    .iter()
                    .filter(|e| e.entry_type == "blob")
                    .map(|e| e.path.as_str())
                    .collect();
                Ok(json!(super::truncate_tree(&paths)))
            }
            "read_file" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                let content = self
                    .github
                    .read_file(self.owner, self.repo, path, self.branch)
                    .await?;
                Ok(json!(super::truncate_content(&content, path)))
            }
            "read_file_lines" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                let start = input
                    .get("start_line")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(1) as usize;
                let end = input
                    .get("end_line")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(start as u64 + 100) as usize;
                let content = self
                    .github
                    .read_file_lines(self.owner, self.repo, path, self.branch, start, end)
                    .await?;
                Ok(json!(content))
            }
            "search_code" => {
                let query = input
                    .get("query")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing query")?;
                let results = self
                    .github
                    .search_code(self.owner, self.repo, self.branch, query)
                    .await?;
                let lines: Vec<String> = results
                    .iter()
                    .map(|r| {
                        if r.matches.is_empty() {
                            r.path.clone()
                        } else {
                            format!("{}\n{}", r.path, r.matches.join("\n"))
                        }
                    })
                    .collect();
                Ok(json!(lines.join("\n---\n")))
            }
            "edit_file" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                if super::implement::is_protected_path(path) {
                    return Ok(json!(format!(
                        "Cannot modify {path}: CI/CD workflow files are protected."
                    )));
                }
                if self.scope_guard.should_block(path) {
                    return Ok(json!(super::write_guard::ScopeGuard::reject_msg(path)));
                }
                let edits = input
                    .get("edits")
                    .and_then(|v| v.as_array())
                    .ok_or("Missing edits array")?;
                let message = input
                    .get("message")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing message")?;
                let message: &str = &self.commit_message(message);
                let original = self
                    .github
                    .read_file(self.owner, self.repo, path, self.branch)
                    .await?;
                let content = match super::implement::apply_text_edits(&original, edits) {
                    Ok(c) => c,
                    Err(errors) => {
                        return Ok(json!(format!(
                            "NO edits applied to {path} — all edits in one call must succeed \
                             atomically, and {} failed: {}. Re-read the file (read_file_lines) \
                             and retry with corrected, UNIQUE old_text; only fall back to \
                             write_file with the complete file if edits keep failing.",
                            errors.len(),
                            errors.join("; ")
                        )));
                    }
                };
                // Same syntax + anti-gut guards the implement pass applies.
                if let Err(problem) =
                    super::syntax_check::validate_change(path, Some(&original), &content)
                {
                    return Ok(json!(format!(
                        "Edit REJECTED for {path} — it would break the file's syntax: {problem}. \
                         Read the file again and retry with a corrected edit."
                    )));
                }
                if let Some(problem) =
                    super::implement::validate_edit_safety(&original, &content, path)
                {
                    return Ok(json!(format!(
                        "Edit REJECTED for {path}: {problem}. Retry with a smaller, more precise edit."
                    )));
                }
                let sha = self
                    .github
                    .get_file_sha(self.owner, self.repo, path, self.branch)
                    .await
                    .ok();
                self.github
                    .write_file(
                        self.owner,
                        self.repo,
                        path,
                        &content,
                        self.branch,
                        message,
                        sha.as_deref(),
                    )
                    .await?;
                *self.files_modified.lock().unwrap() = true;
                Ok(json!(format!("Applied {} edit(s) to {path}", edits.len())))
            }
            "write_file" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                // Guard parity with the implement executor — this path had NONE
                // of the write protections, which is where post-PR churn ran.
                if super::implement::is_protected_path(path) {
                    return Ok(json!(format!(
                        "Cannot modify {path}: CI/CD workflow files are protected."
                    )));
                }
                if self.scope_guard.should_block(path) {
                    return Ok(json!(super::write_guard::ScopeGuard::reject_msg(path)));
                }
                let content = input
                    .get("content")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing content")?;
                let message = input
                    .get("message")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing message")?;
                let message: &str = &self.commit_message(message);
                if let Err(problem) = super::syntax_check::validate_change(path, None, content) {
                    return Ok(json!(format!(
                        "Write REJECTED for {path} — content has syntax problems: {problem}. \
                         Fix the content and retry."
                    )));
                }
                // Anti-stub: overwriting a substantial existing file with a
                // drastic shrink is placeholder churn, not a fix.
                if let Ok(old) = self
                    .github
                    .read_file(self.owner, self.repo, path, self.branch)
                    .await
                {
                    if let Some(problem) =
                        super::implement::validate_edit_safety(&old, content, path)
                    {
                        return Ok(json!(format!(
                            "Write REJECTED for {path}: {problem}. Write the COMPLETE file — \
                             never a stub or placeholder."
                        )));
                    }
                }
                // The blob sha is always looked up server-side — a model-supplied
                // one is a guess, and a wrong guess is a false conflict.
                let sha = self
                    .github
                    .get_file_sha(self.owner, self.repo, path, self.branch)
                    .await
                    .ok();
                self.github
                    .write_file(
                        self.owner,
                        self.repo,
                        path,
                        content,
                        self.branch,
                        message,
                        sha.as_deref(),
                    )
                    .await?;
                *self.files_modified.lock().unwrap() = true;
                Ok(json!(format!("Wrote {path}")))
            }
            "batch_write" => {
                let message = input
                    .get("message")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing message")?;
                let message: &str = &self.commit_message(message);
                let files_arr = input
                    .get("files")
                    .and_then(|v| v.as_array())
                    .ok_or("Missing files")?;
                let mut ops = Vec::new();
                for f in files_arr {
                    let path = f
                        .get("path")
                        .and_then(|v| v.as_str())
                        .ok_or("Missing file path")?;
                    // Guard parity with the implement executor (atomic: one bad
                    // file rejects the whole batch; nothing committed).
                    if super::implement::is_protected_path(path) {
                        return Ok(json!(format!(
                            "Batch write REJECTED — {path} is a protected CI/CD workflow file. \
                             Nothing was committed."
                        )));
                    }
                    if self.scope_guard.should_block(path) {
                        return Ok(json!(format!(
                            "Batch write REJECTED — {}. Nothing was committed.",
                            super::write_guard::ScopeGuard::reject_msg(path)
                        )));
                    }
                    let action = f.get("action").and_then(|v| v.as_str()).unwrap_or("write");
                    if action == "delete" {
                        ops.push(crate::clients::github::FileOp::Delete {
                            path: path.to_string(),
                        });
                    } else {
                        let content = f.get("content").and_then(|v| v.as_str()).unwrap_or("");
                        if let Err(problem) =
                            super::syntax_check::validate_change(path, None, content)
                        {
                            return Ok(json!(format!(
                                "Batch write REJECTED — {path} has syntax problems: {problem}. \
                                 Nothing was committed."
                            )));
                        }
                        if let Ok(old) = self
                            .github
                            .read_file(self.owner, self.repo, path, self.branch)
                            .await
                        {
                            if let Some(problem) =
                                super::implement::validate_edit_safety(&old, content, path)
                            {
                                return Ok(json!(format!(
                                    "Batch write REJECTED — {path}: {problem}. Never replace a \
                                     real file with a stub; write the COMPLETE file. Nothing was \
                                     committed."
                                )));
                            }
                        }
                        ops.push(crate::clients::github::FileOp::Write {
                            path: path.to_string(),
                            content: content.to_string(),
                        });
                    }
                }
                let sha = self
                    .github
                    .batch_write(self.owner, self.repo, self.branch, message, &ops)
                    .await?;
                *self.files_modified.lock().unwrap() = true;
                Ok(json!(format!(
                    "Batch commit {} — {} files",
                    &sha[..8.min(sha.len())],
                    ops.len()
                )))
            }
            "restore_file" => {
                let path = input
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing path")?;
                if super::implement::is_protected_path(path) {
                    return Ok(json!(format!(
                        "Cannot modify {path}: CI/CD workflow files are protected."
                    )));
                }
                if self.scope_guard.should_block(path) {
                    return Ok(json!(super::write_guard::ScopeGuard::reject_msg(path)));
                }
                // Default to the PR's base branch: restoring an out-of-scope file
                // to base is how you remove the change. Guessing "main" was the bug
                // on repos whose base is e.g. `develop`.
                let from_ref = input
                    .get("from_ref")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or(self.base_branch);
                let message = input
                    .get("message")
                    .and_then(|v| v.as_str())
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| format!("Restore {path} from {from_ref}"));
                let message = self.commit_message(&message);
                // Worker-side copy: bytes never enter the LLM context.
                let content = match self
                    .github
                    .read_file(self.owner, self.repo, path, from_ref)
                    .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        let hint = format!(
                            "Could not read {path} at ref '{from_ref}': {e}. Check the path and ref."
                        );
                        return Ok(json!(hint));
                    }
                };
                let sha = self
                    .github
                    .get_file_sha(self.owner, self.repo, path, self.branch)
                    .await
                    .ok();
                self.github
                    .write_file(
                        self.owner,
                        self.repo,
                        path,
                        &content,
                        self.branch,
                        &message,
                        sha.as_deref(),
                    )
                    .await?;
                *self.files_modified.lock().unwrap() = true;
                Ok(json!(format!(
                    "Restored {path} from '{from_ref}' ({} bytes) — exact copy, content not loaded into context",
                    content.len()
                )))
            }
            _ => Err(format!("Unknown tool: {name}").into()),
        }
    }
}

/// Use the light model to determine if review comments indicate "wrong repo" and extract target.
/// Returns Some(target_repo) if confirmed, None otherwise.
async fn detect_wrong_repo(
    text: &str,
    team_repos: &[String],
    msg: &FeedbackMessage,
    state: &WorkerState,
    usage: &mut TokenUsage,
) -> Option<String> {
    // Quick pre-filter: if none of the other repo names appear in the text, skip the LLM call
    let current = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let other_repos: Vec<&String> = team_repos.iter().filter(|r| *r != &current).collect();
    let has_repo_mention = other_repos.iter().any(|r| {
        let short = r.split('/').nth(1).unwrap_or(r);
        text.to_lowercase().contains(&short.to_lowercase())
    });
    if !has_repo_mention {
        return None;
    }

    let repo_list = other_repos
        .iter()
        .map(|r| r.as_str())
        .collect::<Vec<_>>()
        .join(", ");

    let provider = ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
    )
    .await
    .ok()?;

    let system = "You classify PR review comments. Respond with ONLY one line: either \
                  `WRONG_REPO: owner/repo` if the reviewer is explicitly requesting this PR be moved \
                  to a different repository, or `NO` if it's normal code review feedback. \
                  Only output WRONG_REPO if the reviewer clearly states this work belongs in another repo.";

    let prompt =
        format!("Current repo: {current}\nAvailable repos: {repo_list}\n\nReview text:\n{text}");

    let model_id = provider.primary_model_id();
    let response = provider::converse_simple(state, &provider, model_id, system, &prompt, usage)
        .await
        .ok()?;

    let response_lower = response.to_lowercase();
    if !response_lower.contains("wrong_repo") {
        return None;
    }

    // LLM confirmed wrong repo — find which repo it mentioned
    for r in team_repos {
        if r == &current {
            continue;
        }
        let short = r.split('/').nth(1).unwrap_or(r);
        if response_lower.contains(&short.to_lowercase()) || response.contains(r.as_str()) {
            return Some(r.clone());
        }
    }
    None
}

/// Handle wrong-repo: close current PR, comment, and re-trigger in the correct repo.
async fn handle_wrong_repo(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &FeedbackMessage,
    target_repo: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (target_owner, target_name) = target_repo.split_once('/').unwrap_or(("", ""));
    if target_owner.is_empty() || target_name.is_empty() {
        return Err("Invalid target repo format".into());
    }

    // Comment on the PR explaining the move
    let comment = format!(
        "🔀 Moving this work to `{target_repo}` as requested. This PR will be closed and a new one will be opened in the correct repository."
    );
    let _ = github
        .create_issue_comment(&msg.repo_owner, &msg.repo_name, msg.pr_number, &comment)
        .await;

    // Close the PR
    if let Err(e) = github
        .close_pull_request(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
    {
        warn!(pr_number = msg.pr_number, error = %e, "Failed to close PR during wrong-repo move");
    }

    // Load the original run record to get ticket info for re-trigger
    let run_record = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
        )
        .key(
            "run_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.run_id.clone()),
        )
        .send()
        .await?
        .item
        .ok_or("Run record not found")?;

    let title = run_record
        .get("title")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    let ticket_id = run_record
        .get("ticket_id")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    let ticket_source = run_record
        .get("ticket_source")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_else(|| "github".to_string());
    let issue_number = run_record
        .get("issue_number")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    let body = run_record
        .get("issue_body")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    // The new run is still for whoever asked for the first one.
    let field = |k: &str| {
        run_record
            .get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let sender = field("sender");
    let requester = common::requester::Requester::from_run(
        &field("requester_key"),
        &field("requester_name"),
        &field("requester_email"),
        &sender,
    );

    let source = match ticket_source.as_str() {
        "jira" => TicketSource::Jira,
        "alert" => TicketSource::Alert,
        _ => TicketSource::Github,
    };

    // Load image attachments from the run record (stored as JSON string)
    let image_attachments: Vec<crate::models::ImageAttachment> = run_record
        .get("image_attachments")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();

    // Send new ticket message to re-trigger in the correct repo
    let ticket_msg = WorkerMessage::Ticket(crate::models::TicketMessage {
        team_id: msg.team_id.clone(),
        installation_id: msg.installation_id,
        source,
        ticket_id: ticket_id.clone(),
        title,
        body,
        repo_owner: target_owner.to_string(),
        repo_name: target_name.to_string(),
        issue_number,
        sender,
        requester,
        base_branch: "main".to_string(),
        image_attachments,
        continuation: 0,
        continuation_run_id: None,
        first_attempt_ms: 0,
        model_retries: 0,
        ticket_update_note: None,
        ticket_history_note: None,
        plan_cache_key: None,
        team_instructions: String::new(),
        person_direction: None,
        answers: Vec::new(),
        new_run_id: None,
    });

    if state.config.ticket_queue_url.is_empty() {
        warn!("TICKET_QUEUE_URL not configured — cannot re-trigger in new repo");
        return Ok(());
    }

    let body_str = serde_json::to_string(&ticket_msg)?;
    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body_str)
        .send()
        .await?;

    // Mark old run as completed with a note
    let now = chrono::Utc::now().to_rfc3339();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
        )
        .key(
            "run_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.run_id.clone()),
        )
        .update_expression(
            "SET #s = :s, status_run_id = :sri, updated_at = :t, error_message = :em",
        )
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(
            ":s",
            aws_sdk_dynamodb::types::AttributeValue::S("completed".to_string()),
        )
        .expression_attribute_values(
            ":sri",
            aws_sdk_dynamodb::types::AttributeValue::S(format!("completed#{}", msg.run_id)),
        )
        .expression_attribute_values(":t", aws_sdk_dynamodb::types::AttributeValue::S(now))
        .expression_attribute_values(
            ":em",
            aws_sdk_dynamodb::types::AttributeValue::S(format!("Moved to {target_repo}")),
        )
        .send()
        .await;

    info!(
        run_id = %msg.run_id,
        from = %format!("{}/{}", msg.repo_owner, msg.repo_name),
        to = %target_repo,
        ticket_id = %ticket_id,
        "Wrong-repo: closed PR and re-triggered in correct repo"
    );

    Ok(())
}

/// Puts a run back to `completed` after a feedback cycle that changed nothing,
/// unless it is still working (resume owns those states).
/// CoderHelm's own pull request, by the run that opened it.
pub(crate) struct OwnPr<'a> {
    pub team_id: &'a str,
    pub run_id: &'a str,
    pub owner: &'a str,
    pub repo: &'a str,
    pub pr_number: u64,
}

/// The fix for CoderHelm's own pull request belongs outside the code (a
/// person said so, or its reviewer found the diff overrides another system's
/// data). CoderHelm closes the PR with a short note (the cause and where the
/// fix belongs), deletes its branch, tells the ticket unless the run already
/// reported the outside change there, and the run ends as needing a change
/// outside the code. A PR CoderHelm didn't open is left as it is (false).
pub(crate) async fn close_own_pr_outside(
    state: &WorkerState,
    github: &GitHubClient,
    pr_ref: &OwnPr<'_>,
    where_: &str,
    cause: &str,
) -> bool {
    use super::fix_location::{self, FixDecision, FixLocation, OutsideChange};
    let pr = match github
        .get_pull_request(pr_ref.owner, pr_ref.repo, pr_ref.pr_number)
        .await
    {
        Ok(pr) => pr,
        Err(e) => {
            warn!(run_id = %pr_ref.run_id, error = %e, "Could not read the PR to close it");
            return false;
        }
    };
    let author = pr["user"]["login"].as_str().unwrap_or("");
    if !author.contains("coderhelm") {
        return false;
    }
    if pr["state"].as_str() == Some("open") {
        let _ = github
            .create_issue_comment(
                pr_ref.owner,
                pr_ref.repo,
                pr_ref.pr_number,
                &super::lessons::closing_note(where_, cause),
            )
            .await;
        match github
            .close_pull_request(pr_ref.owner, pr_ref.repo, pr_ref.pr_number)
            .await
        {
            Ok(_) => {
                info!(run_id = %pr_ref.run_id, pr = pr_ref.pr_number, "Closed own PR — the fix belongs outside the code")
            }
            Err(e) => warn!(run_id = %pr_ref.run_id, error = %e, "Could not close own PR"),
        }
    }
    // CoderHelm's own branch only: the PR's author is CoderHelm's app (a
    // team's branch pattern can be anything, so never the prefix alone).
    if let Some(branch) = pr["head"]["ref"]
        .as_str()
        .filter(|_| super::is_own_login(pr["user"]["login"].as_str().unwrap_or("")))
    {
        if let Err(e) = github
            .delete_branch(pr_ref.owner, pr_ref.repo, branch)
            .await
        {
            warn!(run_id = %pr_ref.run_id, error = %e, "Could not delete own branch");
        }
    }

    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(pr_ref.team_id))
        .key("run_id", super::attr_s(pr_ref.run_id))
        .send()
        .await
        .ok()
        .and_then(|o| o.item);
    let field = |k: &str| {
        item.as_ref()
            .and_then(|i| i.get(k))
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let mut report = field("outside_change_report");
    if report.trim().is_empty() {
        // The planner's own outside change when it named one; otherwise
        // what the person said.
        let decision = serde_json::from_str::<FixDecision>(&field("fix_decision"))
            .ok()
            .filter(|d| d.effective_location() == FixLocation::OutsideCode)
            .unwrap_or_else(|| FixDecision {
                fix_location: FixLocation::OutsideCode,
                cause: cause.trim().to_string(),
                outside_change: Some(OutsideChange {
                    system: if where_.trim().is_empty() {
                        "the system that holds this data".to_string()
                    } else {
                        where_.trim().to_string()
                    },
                    change: cause.trim().to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            });
        let body = fix_location::outside_report(&decision, false);
        let closed_line = format!(
            "\n\nCoderHelm's pull request #{} for this was closed.",
            pr_ref.pr_number
        );
        let ticket = field("ticket_id");
        let issue_number = item
            .as_ref()
            .and_then(|i| i.get("issue_number"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0);
        match field("ticket_source").as_str() {
            "jira" if !ticket.is_empty() => {
                if let Err(e) = super::post_jira_comment(
                    state,
                    pr_ref.team_id,
                    &ticket,
                    &fix_location::jira_text(&format!("{body}{closed_line}")),
                    "clarification",
                    "Needs a change outside the code",
                )
                .await
                {
                    warn!(run_id = %pr_ref.run_id, error = %e, "Could not post the outside-the-code report on the ticket");
                }
            }
            "github" if issue_number > 0 => {
                let _ = github
                    .create_issue_comment(
                        pr_ref.owner,
                        pr_ref.repo,
                        issue_number,
                        &format!("**Needs a change outside the code**\n\n{body}{closed_line}"),
                    )
                    .await;
            }
            _ => {}
        }
        report = format!("**Needs a change outside the code**\n\n{body}");
    }

    let now = chrono::Utc::now().to_rfc3339();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(pr_ref.team_id))
        .key("run_id", super::attr_s(pr_ref.run_id))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, updated_at = :t, current_pass = :cp, \
             fix_location = :fl, outside_change_report = :rep",
        )
        .condition_expression(common::run_status::not_ended("#s"))
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(":s", super::attr_s(fix_location::STATUS_OUTSIDE))
        .expression_attribute_values(
            ":sri",
            super::attr_s(&format!(
                "{}#{}",
                fix_location::STATUS_OUTSIDE,
                pr_ref.run_id
            )),
        )
        .expression_attribute_values(":t", super::attr_s(&now))
        .expression_attribute_values(":cp", super::attr_s("done"))
        .expression_attribute_values(":fl", super::attr_s(FixLocation::OutsideCode.as_str()))
        .expression_attribute_values(":rep", super::attr_s(&report))
        .ended_values()
        .send()
        .await;
    true
}

async fn finish_without_change(state: &WorkerState, msg: &FeedbackMessage) {
    let now = chrono::Utc::now().to_rfc3339();
    let wrote = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
        )
        .key(
            "run_id",
            aws_sdk_dynamodb::types::AttributeValue::S(msg.run_id.clone()),
        )
        .update_expression("SET #s = :s, status_run_id = :sri, updated_at = :t")
        .condition_expression("#s = :running")
        .expression_attribute_names("#s", "status")
        .expression_attribute_values(
            ":s",
            aws_sdk_dynamodb::types::AttributeValue::S("completed".to_string()),
        )
        .expression_attribute_values(
            ":running",
            aws_sdk_dynamodb::types::AttributeValue::S("running".to_string()),
        )
        .expression_attribute_values(
            ":sri",
            aws_sdk_dynamodb::types::AttributeValue::S(format!("completed#{}", msg.run_id)),
        )
        .expression_attribute_values(":t", aws_sdk_dynamodb::types::AttributeValue::S(now))
        .send()
        .await
        .is_ok();
    if wrote {
        super::webhooks::emit_run(state, &msg.team_id, &msg.run_id, "completed").await;
    }
}

/// While a run is awaiting CI or running, the resume flow owns CI fixing
/// (budget, stall detection, scope guard); feedback must not fix CI too.
fn resume_owns_ci(run_status: &str) -> bool {
    matches!(run_status, "awaiting_ci" | "running")
}

/// Check CI status for a PR branch. Returns Some(logs) if there are failures, None if CI is green or absent.
async fn fetch_ci_failures(
    github: &GitHubClient,
    msg: &FeedbackMessage,
    branch: &str,
) -> Option<String> {
    if branch.is_empty() {
        return None;
    }

    let checks = github
        .list_check_runs_for_ref(&msg.repo_owner, &msg.repo_name, branch)
        .await
        .ok()?;

    let check_runs = checks["check_runs"].as_array()?;
    let failed: Vec<&serde_json::Value> = check_runs
        .iter()
        .filter(|r| {
            let conclusion = r["conclusion"].as_str().unwrap_or("");
            conclusion == "failure" || conclusion == "timed_out"
        })
        .collect();

    if failed.is_empty() {
        return None;
    }

    // Try to get workflow run logs for the first failed check
    let workflow_run_id = failed
        .iter()
        .filter_map(|r| {
            r["details_url"]
                .as_str()
                .and_then(|u| u.split("/runs/").nth(1))
                .and_then(|s| s.split('/').next())
                .and_then(|s| s.parse::<u64>().ok())
        })
        .next()
        .unwrap_or(0);

    if workflow_run_id > 0 {
        if let Ok(logs) = github
            .get_workflow_run_logs(&msg.repo_owner, &msg.repo_name, workflow_run_id)
            .await
        {
            return Some(logs);
        }
    }

    // Fallback: check run annotations (uses checks:read, not actions:read)
    if let Ok(annotations) = github
        .get_check_run_annotations(&msg.repo_owner, &msg.repo_name, branch)
        .await
    {
        if !annotations.is_empty() {
            return Some(annotations);
        }
    }

    // Last resort: just list the failed check names
    let names: Vec<&str> = failed.iter().filter_map(|r| r["name"].as_str()).collect();
    Some(format!("Failed checks: {}", names.join(", ")))
}

/// Make sure the run's PR is open before feedback work starts, reattaching or
/// reopening as needed, and drop the "⚠️ Partial:" salvage prefix from the
/// title. Best-effort: failures log and fall through (comments still land on
/// a closed PR).
/// `cancel_reason` for a run stopped because a person closed its PR.
const CANCEL_PR_CLOSED_BY_HUMAN: &str = "pr_closed_by_human";

/// Whether the run has been cancelled (consistent read — cancel must win
/// against a racing pass).
/// Hand a PR to a human after the automated-fix budget is spent.
///
/// Ordering matters for crash-safety. The RELIABLE, idempotent signals run
/// first — mark the PR ready and finalize the run (cancel-guarded, clearing the
/// stale partial/error marker) — so that even if this invocation dies mid-way,
/// the human still sees a ready PR and a redelivery just repeats these no-ops.
/// The explanatory note + PR comment run LAST, gated by an atomic
/// `handoff_notified` marker so repeated webhooks on the now-finalized run don't
/// spam the PR. If the comment is lost to a crash, the PR is already marked
/// ready — the worst case is a missing comment, never a stuck draft.
async fn hand_off_to_human(state: &WorkerState, github: &GitHubClient, msg: &FeedbackMessage) {
    use aws_sdk_dynamodb::types::AttributeValue;

    // Respect a cancel (best-effort early out; the status write below is also
    // cancel-guarded for the race window).
    if run_is_cancelled(state, &msg.team_id, &msg.run_id).await {
        return;
    }

    // (1) Reliable signal: un-draft the PR. Idempotent (no-ops if already ready).
    crate::passes::resume::mark_pr_ready(
        state,
        &msg.team_id,
        github,
        &msg.repo_owner,
        &msg.repo_name,
        msg.pr_number,
    )
    .await;

    // (2) Finalize the run — completed, stale error cleared, never clobbering a
    // cancel. Safe to repeat on redelivery.
    let now = chrono::Utc::now().to_rfc3339();
    let wrote = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(msg.team_id.clone()))
        .key("run_id", AttributeValue::S(msg.run_id.clone()))
        .update_expression(
            "SET #s = :s, status_run_id = :sri, updated_at = :t REMOVE error_message, #err",
        )
        .condition_expression(common::run_status::not_ended("#s"))
        .expression_attribute_names("#s", "status")
        .expression_attribute_names("#err", "error")
        .expression_attribute_values(":s", AttributeValue::S("completed".to_string()))
        .expression_attribute_values(
            ":sri",
            AttributeValue::S(format!("completed#{}", msg.run_id)),
        )
        .expression_attribute_values(":t", AttributeValue::S(now))
        .ended_values()
        .send()
        .await
        .is_ok();
    if wrote {
        super::webhooks::emit_run(state, &msg.team_id, &msg.run_id, "completed").await;
    }
    // When it was handed off (the first time only), for the weekly summary.
    super::record_ci_outcome(
        state,
        &msg.team_id,
        &msg.run_id,
        super::CiOutcome::HandedOff(super::GaveUp::NoChange),
    )
    .await;

    // (3) Explain ONCE. The marker write wins for exactly one invocation; a
    // redelivered/concurrent event sees the marker and skips (no PR spam). Done
    // last so a crash here can't suppress the reliable signals above.
    let first_notify = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(msg.team_id.clone()))
        .key("run_id", AttributeValue::S(msg.run_id.clone()))
        .update_expression("SET handoff_notified = :true")
        .condition_expression("attribute_not_exists(handoff_notified)")
        .expression_attribute_values(":true", AttributeValue::Bool(true))
        .send()
        .await
        .is_ok();
    if first_notify {
        warn!(run_id = %msg.run_id, "Auto-fix budget spent — handed PR to human");
        super::add_progress_note(
            state,
            &msg.team_id,
            &msg.run_id,
            "Reached the automated-fix limit without getting CI green — handed off for human review.",
        )
        .await;
        let _ = github
            .create_issue_comment(
                &msg.repo_owner,
                &msg.repo_name,
                msg.pr_number,
                "🤝 CoderHelm hit its automated-fix limit on this PR without getting CI green, so \
                 it's handed off for human review. If this needs generated files (e.g. Sanity \
                 types) regenerated, that step has to be run locally — the sandbox can't run \
                 codegen that needs project configuration.",
            )
            .await;
    }
}

/// A person ended the run (cancelled or closed it): no feedback runs on it.
async fn run_is_cancelled(state: &WorkerState, team_id: &str, run_id: &str) -> bool {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .consistent_read(true)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "run_id",
            aws_sdk_dynamodb::types::AttributeValue::S(run_id.to_string()),
        )
        .projection_expression("#s")
        .expression_attribute_names("#s", "status")
        .send()
        .await
        .ok()
        .and_then(|r| r.item)
        .and_then(|i| i.get("status").and_then(|v| v.as_s().ok()).cloned())
        .map(|s| common::run_status::ended_by_person(&s))
        .unwrap_or(false)
}

/// Prepare the run's PR for feedback. Returns `false` if the run must NOT
/// proceed — specifically when a human closed the PR (their decision wins;
/// resurrecting it and pushing commits, as the churn-era reopen did, is
/// exactly what we must not do). Returns `true` to continue.
async fn ensure_pr_open(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &mut FeedbackMessage,
) -> bool {
    let Ok(pr) = github
        .get_pull_request(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
    else {
        return true;
    };

    let closed = pr["state"].as_str() == Some("closed");
    let merged = !pr["merged_at"].is_null();
    let branch = pr["head"]["ref"].as_str().unwrap_or("").to_string();

    if merged {
        return true;
    }

    if closed {
        // Who closed it? Only reopen/reattach when CODERHELM closed it (the
        // churn-era force-reset auto-closed its own PRs). If a HUMAN closed
        // it, they made a deliberate decision — stop the run, don't reopen,
        // don't push anything.
        let closer = github
            .get_pr_closed_by(&msg.repo_owner, &msg.repo_name, msg.pr_number)
            .await;
        let closed_by_human = closer
            .as_deref()
            .map(|login| !login.to_lowercase().contains("coderhelm"))
            .unwrap_or(false);
        if closed_by_human {
            let who = closer.as_deref().unwrap_or("a user");
            info!(
                pr_number = msg.pr_number,
                closed_by = who,
                "PR was closed by a human — abandoning the run, not reopening"
            );
            super::add_progress_note(
                state,
                &msg.team_id,
                &msg.run_id,
                &format!(
                    "PR #{} was closed by {who} — stopping (a human closed it).",
                    msg.pr_number
                ),
            )
            .await;
            let now = chrono::Utc::now().to_rfc3339();
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.runs_table_name)
                .key(
                    "team_id",
                    aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
                )
                .key(
                    "run_id",
                    aws_sdk_dynamodb::types::AttributeValue::S(msg.run_id.clone()),
                )
                // Record WHY and BY WHOM, not just a note — analytics and the
                // dashboard read these fields, and the PR's outcome is known.
                .update_expression(
                    "SET #s = :s, status_run_id = :sri, current_pass = :cp, updated_at = :t, \
                     cancel_reason = :cr, cancelled_by = :cb, pr_outcome = :po",
                )
                .expression_attribute_names("#s", "status")
                .expression_attribute_values(":cr", super::attr_s(CANCEL_PR_CLOSED_BY_HUMAN))
                .expression_attribute_values(":cb", super::attr_s(who))
                .expression_attribute_values(":po", super::attr_s("closed"))
                .expression_attribute_values(
                    ":s",
                    aws_sdk_dynamodb::types::AttributeValue::S("cancelled".to_string()),
                )
                .expression_attribute_values(
                    ":sri",
                    aws_sdk_dynamodb::types::AttributeValue::S(format!("cancelled#{}", msg.run_id)),
                )
                .expression_attribute_values(
                    ":cp",
                    aws_sdk_dynamodb::types::AttributeValue::S("cancelled".to_string()),
                )
                .expression_attribute_values(":t", aws_sdk_dynamodb::types::AttributeValue::S(now))
                .send()
                .await;
            return false;
        }
    }

    if closed && !merged && !branch.is_empty() {
        // Coderhelm closed it (churn force-reset). Another PR may have taken
        // over the branch (the pre-v0.1.0 churn closed old PRs and minted new
        // ones on the same branch).
        let successor = github
            .find_open_pr_for_branch(&msg.repo_owner, &msg.repo_name, &branch)
            .await
            .ok()
            .flatten();
        if let Some(open_pr) = successor {
            let new_number = open_pr["number"].as_u64().unwrap_or(msg.pr_number);
            let new_url = open_pr["html_url"].as_str().unwrap_or("").to_string();
            info!(
                old_pr = msg.pr_number,
                new_pr = new_number,
                "Run's PR is closed — reattaching feedback to the branch's open PR"
            );
            super::add_progress_note(
                state,
                &msg.team_id,
                &msg.run_id,
                &format!(
                    "PR #{} is closed — continuing on open PR #{new_number} for the same branch",
                    msg.pr_number
                ),
            )
            .await;
            msg.pr_number = new_number;
            let _ = state
                .dynamo
                .update_item()
                .table_name(&state.config.runs_table_name)
                .key(
                    "team_id",
                    aws_sdk_dynamodb::types::AttributeValue::S(msg.team_id.clone()),
                )
                .key(
                    "run_id",
                    aws_sdk_dynamodb::types::AttributeValue::S(msg.run_id.clone()),
                )
                .update_expression("SET pr_number = :pn, pr_url = :pu")
                .expression_attribute_values(
                    ":pn",
                    aws_sdk_dynamodb::types::AttributeValue::N(new_number.to_string()),
                )
                .expression_attribute_values(
                    ":pu",
                    aws_sdk_dynamodb::types::AttributeValue::S(new_url),
                )
                .send()
                .await;
        } else if github
            .reopen_pull_request(&msg.repo_owner, &msg.repo_name, msg.pr_number)
            .await
            .is_ok()
        {
            info!(
                pr_number = msg.pr_number,
                "Reopened closed PR for re-review"
            );
            super::add_progress_note(
                state,
                &msg.team_id,
                &msg.run_id,
                &format!("Reopened PR #{} for re-review", msg.pr_number),
            )
            .await;
        } else {
            warn!(
                pr_number = msg.pr_number,
                "Could not reopen closed PR — feedback will post to it anyway"
            );
        }
    }

    // Strip the salvage marker from the title now that the PR is being
    // actively finished.
    if let Some(title) = pr["title"].as_str() {
        if let Some(clean) = title.strip_prefix("⚠️ Partial: ") {
            if github
                .update_pr_title(&msg.repo_owner, &msg.repo_name, msg.pr_number, clean)
                .await
                .is_ok()
            {
                info!(
                    pr_number = msg.pr_number,
                    "Removed Partial marker from PR title"
                );
            }
        }
    }

    true
}

#[cfg(test)]
mod review_request_tests {
    use super::*;

    fn comment(path: &str, line: u64, body: &str, context: bool) -> ReviewComment {
        ReviewComment {
            path: path.into(),
            line: Some(line),
            body: body.into(),
            comment_id: Some(line),
            node_id: None,
            is_context: context,
        }
    }

    #[test]
    fn each_comment_to_answer_is_one_request_with_its_own_thread_as_context() {
        let comments = vec![
            comment("jobs/sync.py", 10, "[alice]: why INFO?", true),
            comment("jobs/sync.py", 30, "[bob]: unrelated", true),
            comment("jobs/sync.py", 10, "Make this DEBUG", false),
            comment("api/views.py", 5, "Rename this", false),
        ];
        let r = review_requests(&comments);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].body, "Make this DEBUG");
        assert_eq!(r[0].context, vec!["[alice]: why INFO?".to_string()]);
        assert!(r[1].context.is_empty());
    }
}

#[cfg(test)]
mod resume_owns_ci_tests {
    use super::resume_owns_ci;

    #[test]
    fn only_active_runs_own_ci() {
        assert!(resume_owns_ci("awaiting_ci"));
        assert!(resume_owns_ci("running"));
        for s in [
            "completed",
            "failed",
            "needs_input",
            "cancelled",
            "closed",
            "",
        ] {
            assert!(!resume_owns_ci(s), "{s}");
        }
    }
}

#[cfg(test)]
mod write_schema_tests {
    use super::*;

    #[test]
    fn feedback_write_file_schema_has_no_sha() {
        let tools = feedback_tools();
        let w = tools.iter().find(|t| t.name == "write_file").unwrap();
        assert!(w.input_schema["properties"].get("sha").is_none());
    }
}

/// A comment addressed to the person who asked, when one did.
fn addressed(author: &str, text: &str) -> String {
    match author.trim() {
        "" => text.to_string(),
        a => format!("@{a} {text}"),
    }
}

/// A round whose only request was to bring the branch up to date: the
/// update's line answers it (in the thread it came from, else on the PR),
/// and the run waits for CI when the update pushed.
async fn finish_branch_update(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &FeedbackMessage,
    ticket: &super::review_fix::Ticket,
    line: &str,
    pushed: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let thread = msg
        .comments
        .iter()
        .filter(|c| !c.is_context)
        .find_map(|c| c.comment_id);
    match thread {
        // The requester said not to post on the pull request: the line goes
        // to them on the run page and the ticket.
        _ if !ticket.constraints.may_comment() => {
            super::review_fix::tell_requester(
                state,
                github,
                &msg.team_id,
                &msg.run_id,
                ticket,
                &format!("Branch update on pull request #{}", msg.pr_number),
                line,
            )
            .await
        }
        Some(id) => {
            github
                .reply_to_review_comment(&msg.repo_owner, &msg.repo_name, msg.pr_number, id, line)
                .await?;
        }
        None => {
            github
                .create_issue_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.pr_number,
                    &addressed(&msg.trigger_author, line),
                )
                .await?;
        }
    }
    info!(run_id = %msg.run_id, pushed, "Answered a request to update the branch");
    if !pushed {
        finish_without_change(state, msg).await;
        return Ok(());
    }
    // The pushed update runs CI: the run waits for it like any feedback push.
    use aws_sdk_dynamodb::types::AttributeValue as A;
    let now = chrono::Utc::now().to_rfc3339();
    let _ = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", A::S(msg.team_id.clone()))
        .key("run_id", A::S(msg.run_id.clone()))
        .update_expression("SET #s = :s, status_run_id = :sri, updated_at = :t")
        .condition_expression(common::run_status::not_ended("#s"))
        .expression_attribute_names("#s", "status")
        .ended_values()
        .expression_attribute_values(":s", A::S("awaiting_ci".to_string()))
        .expression_attribute_values(":sri", A::S(format!("awaiting_ci#{}", msg.run_id)))
        .expression_attribute_values(":t", A::S(now))
        .send()
        .await;
    if !state.config.ci_fix_queue_url.is_empty() {
        let resume = serde_json::json!({
            "type": "resume",
            "team_id": msg.team_id,
            "run_id": msg.run_id,
            "installation_id": msg.installation_id,
        });
        let _ = state
            .sqs
            .send_message()
            .queue_url(&state.config.ci_fix_queue_url)
            .message_body(resume.to_string())
            .delay_seconds(120)
            .send()
            .await;
    }
    Ok(())
}

/// Pure: the PR comment that answers a person's top-level comment or review
/// body, when there is no inline thread to reply in. None for automated
/// feedback (no `trigger_author`) or when inline threads carry the answer.
pub(crate) fn top_level_reply(
    trigger_author: &str,
    inline_threads: usize,
    response: &str,
    consulted: &str,
) -> Option<String> {
    let author = trigger_author.trim();
    if author.is_empty() || inline_threads > 0 {
        return None;
    }
    let text = response
        .split("---SPLIT---")
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    let text = if text.is_empty() {
        "Done — the changes are in the latest commit.".to_string()
    } else {
        common::truncate_str(&text, 60_000).to_string()
    };
    Some(format!("@{author} {text}{consulted}"))
}

#[cfg(test)]
mod description_request_tests {
    use super::{description_reply, update_description_tool, DescriptionRequests};

    #[test]
    fn a_person_with_write_access_gets_the_description_changed_without_code() {
        let d = DescriptionRequests::new(true);
        let out = d.ask("drop the warning banner");
        assert!(out.starts_with("Recorded."));
        d.ask("fix the secret name to `/app/prod/db-password`");
        d.ask("  drop the   warning banner ");
        d.ask("");
        assert_eq!(
            d.taken(),
            vec![
                "drop the warning banner".to_string(),
                "fix the secret name to `/app/prod/db-password`".to_string(),
            ]
        );
        // What changed goes on the reply, one line each.
        let changed = vec![
            "Removed the warning banner; its items are Open items at the bottom".to_string(),
            "Added Not in this PR: allowlist (network team), grant (data team)".to_string(),
        ];
        let reply = description_reply(&d.taken(), Some(&changed));
        assert_eq!(
            reply,
            "\n\nUpdated the description:\n- Removed the warning banner; its items are Open items at the bottom\n- Added Not in this PR: allowlist (network team), grant (data team)"
        );
        assert_eq!(
            description_reply(&d.taken(), Some(&[])),
            "\n\nUpdated the description as asked."
        );
        assert!(description_reply(&d.taken(), None).contains("couldn't update the description"));
    }

    #[test]
    fn without_write_access_nothing_changes_and_the_person_is_told() {
        let d = DescriptionRequests::new(false);
        let out = d.ask("drop the warning banner");
        assert!(out.starts_with("Not changed: only people with write access"));
        assert!(d.taken().is_empty());
        // No request went through: the reply says nothing about the
        // description, and a rewrite after code changes stays unannounced.
        assert_eq!(description_reply(&d.taken(), Some(&["x".to_string()])), "");
        let tool = update_description_tool();
        assert_eq!(tool.name, "update_pr_description");
        assert!(tool.description.contains("write access"));
        assert_eq!(tool.input_schema["required"][0], "change");
    }
}

#[cfg(test)]
mod top_level_reply_tests {
    use super::top_level_reply;

    #[test]
    fn a_person_s_top_level_ask_gets_an_addressed_reply() {
        let r = top_level_reply("alice", 0, "Renamed the flag.", "").unwrap();
        assert_eq!(r, "@alice Renamed the flag.");
        let r = top_level_reply("alice", 0, "one---SPLIT---two", "").unwrap();
        assert_eq!(r, "@alice one\n\ntwo");
        assert!(top_level_reply("alice", 0, "", "")
            .unwrap()
            .starts_with("@alice "));
    }

    #[test]
    fn automated_feedback_and_inline_threads_get_no_top_level_reply() {
        assert_eq!(top_level_reply("", 0, "fixed CI", ""), None);
        assert_eq!(top_level_reply("alice", 2, "fixed", ""), None);
    }
}

#[cfg(test)]
mod graph_tests {
    use super::*;

    #[test]
    fn review_replies_get_the_graph_tools() {
        use super::super::code_graph::{offer, Avail, RepoGraphs, Role, TOOLS};
        let graphs = RepoGraphs::with(
            "t",
            1,
            "acme/app",
            Avail::Building(common::code_graph::Shown::Queued),
            Vec::new(),
            "coder",
            7,
        );
        let mut tools = feedback_tools();
        let mut system = String::new();
        assert!(offer(&graphs, Role::Coder, &mut tools, &mut system));
        for name in TOOLS {
            assert!(tools.iter().any(|t| t.name == name), "{name}");
        }
        assert!(system.contains("graph_callers"));
    }
}
