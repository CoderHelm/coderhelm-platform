//! Label-triggered PR code review. A PR gains the repo's review label (or a
//! human replies to the bot) → gateway enqueues a `Review` job → this pass
//! fetches the diff, asks the model for a verdict + risk + findings against the
//! repo's rules/instructions, posts a GitHub review (APPROVE / REQUEST_CHANGES,
//! or COMMENT when GitHub forbids a self-review), persists a review record for
//! the dashboard, and — only when the repo opts in — runs post-approval actions.
//!
//! Fail-closed everywhere: any error becomes REQUEST_CHANGES, never an APPROVE.
//! Auto-merge/tag/deploy + the health guard live in `review_actions` and ship
//! OFF by default; this pass only ever *reviews* unless a repo turns them on.

use crate::agent::provider::{self, ModelProvider};
use crate::clients::github::GitHubClient;
use crate::models::{ReviewMessage, TokenUsage};
use crate::passes::{attr_n, attr_s, review_agent, review_risk};
use crate::WorkerState;
use tracing::{info, warn};

/// Per-repo reviewer config (worker-side mirror of the gateway's). OFF by
/// default — a second gate so a mis-fired enqueue still can't review a repo the
/// owner never opted in.
struct ReviewConfig {
    enabled: bool,
    killed: bool,
    instructions: String,
    /// Run the affected tests/build in the sandbox and attach pass/fail receipts.
    verify_tests: bool,
}

async fn load_config(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
) -> Result<ReviewConfig, Box<dyn std::error::Error + Send + Sync>> {
    let sk = format!("REVIEW_CONFIG#REPO#{owner}/{name}");
    // A failed read is an error (retried), never "disabled".
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await?
        .item()
        .cloned();
    let Some(item) = item else {
        return Ok(ReviewConfig {
            enabled: false,
            killed: false,
            instructions: String::new(),
            verify_tests: false,
        });
    };
    Ok(ReviewConfig {
        enabled: item
            .get("enabled")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        killed: item
            .get("killed")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
        instructions: item
            .get("instructions")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default(),
        verify_tests: item
            .get("verify_tests")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false),
    })
}

/// The repo's review trigger label IF the reviewer is enabled (and not killed),
/// else None. Lets CoderHelm self-label the PRs it opens: the reviewer gates
/// EVERY PR on the label, so a bot PR must carry it to be reviewed + self-fixed.
/// Defaults to `ch-review` when the field is unset.
pub(crate) async fn enabled_trigger_label(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
) -> Option<String> {
    let sk = format!("REVIEW_CONFIG#REPO#{owner}/{name}");
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())?;
    let enabled = item
        .get("enabled")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false);
    let killed = item
        .get("killed")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false);
    if !enabled || killed {
        return None;
    }
    Some(
        item.get("label")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "ch-review".to_string()),
    )
}

/// Team-wide (org-level) review instructions applied to EVERY repo's review, on
/// top of the per-repo config + AGENTS.md. Stored at sk `REVIEW_CONFIG#GLOBAL`.
/// Empty when unset.
async fn load_org_instructions(state: &WorkerState, team_id: &str) -> String {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s("REVIEW_CONFIG#GLOBAL"))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
        .and_then(|it| it.get("instructions").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_default()
}

const RATING_FOOTER: &str =
    "\n\n---\n_Was this review helpful? Rate it 👍 / 👎 and add notes in the \
CoderHelm dashboard so the reviewer learns. Reply **@coderhelm re-review** to re-run against the \
latest commit, or **@coderhelm <question>** to ask._";

/// Consume-once dedup for review triggers. GitHub webhooks AND SQS are both
/// at-least-once, so one user action (a comment, a label, a push) can arrive
/// twice; the gateway stamps a `dedup_key` that is stable per action and the
/// worker claims it here before any GitHub or model work.
///
/// The claim is a lease while the review runs, so an invocation that dies
/// (timeout, crash) does not block the retry, and becomes a longer "done"
/// marker once the review finished. A failed review releases it.
const REVIEW_LEASE_SECS: u64 = 16 * 60;
const REVIEW_DONE_SECS: u64 = 6 * 3600;
/// Transient failures are retried this many times before the PR is told.
const MAX_REVIEW_RETRIES: u32 = 2;
const REVIEW_RETRY_DELAY_SECS: i32 = 90;
/// Time budget for one review. The Lambda is killed at 15 minutes; the agent
/// wraps up and the sandbox stops waiting well before that.
const REVIEW_BUDGET: std::time::Duration = std::time::Duration::from_secs(12 * 60);
const REVIEW_AGENT_BUDGET: std::time::Duration = std::time::Duration::from_secs(8 * 60);

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub async fn run(state: &WorkerState, msg: ReviewMessage) -> Result<(), BoxError> {
    let cfg = load_config(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await?;
    if !cfg.enabled || cfg.killed {
        info!(
            pr = msg.pr_number,
            "Reviewer disabled/killed for repo — skipping"
        );
        return Ok(());
    }

    let claim_sk = (!msg.dedup_key.is_empty()).then(|| {
        format!(
            "REVIEWJOB#{}/{}#{:06}#{}",
            msg.repo_owner, msg.repo_name, msg.pr_number, msg.dedup_key
        )
    });
    // A retry re-uses the claim its first attempt released.
    if let Some(sk) = &claim_sk {
        let claim = common::claim::claim(
            &state.dynamo,
            &state.config.settings_table_name,
            &msg.team_id,
            sk,
            REVIEW_LEASE_SECS,
        )
        .await;
        if !claim.won_or_failed_open() {
            info!(
                pr = msg.pr_number,
                dedup_key = %msg.dedup_key,
                "Skipping duplicate review job — this trigger was already processed"
            );
            return Ok(());
        }
    }

    let deadline = std::time::Instant::now() + REVIEW_BUDGET;
    match review(state, &msg, cfg, deadline).await {
        Ok(()) => {
            if let Some(sk) = &claim_sk {
                common::claim::extend(
                    &state.dynamo,
                    &state.config.settings_table_name,
                    &msg.team_id,
                    sk,
                    REVIEW_DONE_SECS,
                )
                .await;
            }
            Ok(())
        }
        Err(e) => {
            if let Some(sk) = &claim_sk {
                common::claim::release(
                    &state.dynamo,
                    &state.config.settings_table_name,
                    &msg.team_id,
                    sk,
                )
                .await;
            }
            if msg.attempt < MAX_REVIEW_RETRIES && requeue_review(state, &msg).await {
                warn!(pr = msg.pr_number, attempt = msg.attempt, error = %e, "Review failed — retrying");
                return Ok(());
            }
            notify_review_failed(state, &msg, &e.to_string()).await;
            Err(e)
        }
    }
}

/// Re-enqueue a failed review for another attempt.
async fn requeue_review(state: &WorkerState, msg: &ReviewMessage) -> bool {
    if state.config.ticket_queue_url.is_empty() {
        return false;
    }
    let mut next = msg.clone();
    next.attempt += 1;
    let Ok(mut body) = serde_json::to_value(&next) else {
        return false;
    };
    if let Some(o) = body.as_object_mut() {
        o.insert("type".to_string(), serde_json::json!("review"));
    }
    state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body.to_string())
        .delay_seconds(REVIEW_RETRY_DELAY_SECS)
        .send()
        .await
        .is_ok()
}

/// Tell the PR that a review could not be completed, once per trigger.
async fn notify_review_failed(state: &WorkerState, msg: &ReviewMessage, error: &str) {
    let sk = format!(
        "REVIEWFAILNOTICE#{}/{}#{:06}#{}",
        msg.repo_owner, msg.repo_name, msg.pr_number, msg.dedup_key
    );
    let claim = common::claim::claim(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
        &sk,
        REVIEW_DONE_SECS,
    )
    .await;
    if !claim.won_or_failed_open() {
        return;
    }
    let Ok(github) = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    ) else {
        return;
    };
    let body = format!(
        "⚠️ I couldn't finish reviewing this PR after {} attempts (`{}`). Reply \
         **@coderhelm re-review** to try again.",
        msg.attempt + 1,
        common::truncate_str(error, 300)
    );
    let _ = github
        .create_issue_comment(&msg.repo_owner, &msg.repo_name, msg.pr_number, &body)
        .await;
}

async fn review(
    state: &WorkerState,
    msg: &ReviewMessage,
    cfg: ReviewConfig,
    deadline: std::time::Instant,
) -> Result<(), BoxError> {
    let started = std::time::Instant::now();
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    // PR metadata. head_sha may be empty on reply-triggered reviews — resolve it
    // from the PR so a reply always targets the latest commit.
    let pr = github
        .get_pull_request(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await?;

    // Coalesce rapid commits: if a newer commit has landed since this verdict
    // review was enqueued, skip it — the newer commit's own review covers the
    // latest code. Without this, a burst (e.g. an auto-fix loop pushing several
    // commits in a minute) makes the reviewer post a review per intermediate
    // commit ("asking too fast"). Reply/question reviews resolve the head fresh
    // (msg.head_sha empty) and are never skipped here.
    let current_head = pr["head"]["sha"].as_str().unwrap_or("");
    if msg.question.is_none() && !msg.head_sha.is_empty() && current_head != msg.head_sha {
        info!(
            pr = msg.pr_number,
            enqueued = %msg.head_sha,
            current = current_head,
            "Skipping superseded review — a newer commit exists"
        );
        return Ok(());
    }

    let title = pr["title"].as_str().unwrap_or("");
    let pr_body = pr["body"].as_str().unwrap_or("");
    let base = pr["base"]["sha"].as_str().unwrap_or("");
    let base_branch = pr["base"]["ref"].as_str().unwrap_or("main");
    let pr_author = pr["user"]["login"].as_str().unwrap_or("");
    let head_sha = if msg.head_sha.is_empty() {
        pr["head"]["sha"].as_str().unwrap_or("").to_string()
    } else {
        msg.head_sha.clone()
    };

    // Build the diff (base...head compare gives per-file patches), bounded.
    let compare = github
        .get_diff(&msg.repo_owner, &msg.repo_name, base, &head_sha)
        .await?;
    let diff = review_agent::format_diff(&compare, 40_000);

    // Read AGENTS.md/etc. at the PR head so a PR that edits them is reviewed
    // against its own new rules.
    let repo_instructions =
        super::load_repo_instructions_at_ref(&github, &msg.repo_owner, &msg.repo_name, &head_sha)
            .await;
    let extra = if cfg.instructions.is_empty() {
        String::new()
    } else {
        format!(
            "\n\n## Repo review focus (owner-provided)\n{}",
            cfg.instructions
        )
    };
    // Org-wide standards apply to every repo, layered under the per-repo focus.
    let org_instructions = load_org_instructions(state, &msg.team_id).await;
    let org_block = if org_instructions.trim().is_empty() {
        String::new()
    } else {
        format!("\n\n## Org-wide review standards (apply to every repo)\n{org_instructions}")
    };

    let provider = ModelProvider::load_for_team(
        &state.dynamo,
        &state.config.settings_table_name,
        &msg.team_id,
    )
    .await?;
    let mut usage = TokenUsage::default();

    // ── Reply-with-a-question mode: answer, don't vote. ──
    if let Some(question) = msg.question.as_ref().filter(|q| !q.trim().is_empty()) {
        let system = format!(
            "You are a senior engineer answering a question about an open PR in {}/{}. Use the diff \
             and PR description as ground truth; be concrete and cite file:line. If the answer \
             isn't determinable from the diff, say so.{}{}{}",
            msg.repo_owner,
            msg.repo_name,
            super::format_instructions_block(&repo_instructions),
            org_block,
            extra,
        );
        let prompt = format!(
            "PR #{}: {title}\n\n{pr_body}\n\n## Question from a reviewer\n{question}\n\n## Diff (base...head)\n{diff}",
            msg.pr_number,
        );
        let answer = provider::converse_simple(
            state,
            &provider,
            provider.heavy_model_id(),
            &system,
            &prompt,
            &mut usage,
        )
        .await
        .unwrap_or_else(|e| format!("I couldn't answer that automatically ({e})."));
        // Asked inside an inline review thread → answer in that thread.
        match msg.reply_to_comment_id {
            Some(comment_id) => {
                github
                    .reply_to_review_comment(
                        &msg.repo_owner,
                        &msg.repo_name,
                        msg.pr_number,
                        comment_id,
                        &answer,
                    )
                    .await?;
            }
            None => {
                let body = format!("{answer}{RATING_FOOTER}");
                github
                    .create_issue_comment(&msg.repo_owner, &msg.repo_name, msg.pr_number, &body)
                    .await?;
            }
        }
        store_review_record(
            state, msg, &head_sha, "QUESTION", "N/A", &answer, "COMMENT", "",
        )
        .await;
        info!(
            pr = msg.pr_number,
            in_thread = msg.reply_to_comment_id.is_some(),
            "Reviewer answered a question"
        );
        return Ok(());
    }

    // ── Verdict mode (agentic: walk the repo, structured findings, self-critique) ──
    // Fold this team's past 👎 feedback into the prompt so the reviewer learns and
    // stops repeating rejected comment styles ("leave comments to learn").
    let learning =
        load_learning_context(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await;
    let instructions_block = format!(
        "{}{org_block}{extra}{learning}",
        super::format_instructions_block(&repo_instructions)
    );
    let changed = review_agent::changed_right_lines(&compare);

    // Code graph: if this repo is indexed, compute the EXACT impacted-file set
    // for the PR's changed files and inject it into the prompt (agents under-use
    // structural tools unless the result is put in front of them), and hand the
    // agent the graph lookup tools for deeper digging.
    let graph =
        super::code_graph::Graph::open(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await;
    let graph_context = if let Some(g) = &graph {
        let changed_paths: Vec<String> = compare["files"]
            .as_array()
            .map(|fs| {
                fs.iter()
                    .filter_map(|f| f["filename"].as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let impacted = g.impacted_by(state, &changed_paths).await;
        if impacted.is_empty() {
            String::new()
        } else {
            let lines: Vec<String> = impacted
                .iter()
                .take(40)
                .map(|(f, why)| format!("- {f} — {why}"))
                .collect();
            format!(
                "\n## Impacted files (from the code graph — files that import or reference \
                 symbols defined in the changed files; check these for breakage)\n{}\n",
                lines.join("\n")
            )
        }
    } else {
        String::new()
    };

    // CI evidence: the PR's ACTUAL check results for this head, injected into
    // the prompt. A reviewer claim like "this cannot install" is refutable by a
    // green install job on the same head — that signal must be in front of the
    // model, not discoverable-in-theory. Best-effort: absent on API failure.
    let ci_context = match github
        .list_check_runs_for_ref(&msg.repo_owner, &msg.repo_name, &head_sha)
        .await
    {
        Ok(v) => {
            let runs = v["check_runs"].as_array().cloned().unwrap_or_default();
            if runs.is_empty() {
                String::new()
            } else {
                let lines: Vec<String> = runs
                    .iter()
                    .take(30)
                    .map(|r| {
                        format!(
                            "- {}: {}",
                            r["name"].as_str().unwrap_or("?"),
                            r["conclusion"]
                                .as_str()
                                .unwrap_or_else(|| r["status"].as_str().unwrap_or("?"))
                        )
                    })
                    .collect();
                format!(
                    "\n## CI results on this PR's head ({})\n{}\n",
                    &head_sha[..head_sha.len().min(7)],
                    lines.join("\n")
                )
            }
        }
        Err(e) => {
            warn!(pr = msg.pr_number, error = %e, "Could not fetch check runs for review context");
            String::new()
        }
    };
    let extra_context = format!("{graph_context}{ci_context}");

    // 1) High-recall generation with repo-walking tools.
    let output = review_agent::generate_review(
        state,
        &provider,
        &github,
        &msg.repo_owner,
        &msg.repo_name,
        &head_sha,
        title,
        pr_body,
        &diff,
        &instructions_block,
        graph.as_ref(),
        &extra_context,
        &mut usage,
        (started + REVIEW_AGENT_BUDGET).min(deadline),
    )
    .await;

    // 2) Critic pass drops weak/false findings.
    let findings =
        review_agent::critique_findings(state, &provider, &diff, output.findings, &mut usage).await;

    // 3) Map to inline comments (only diff-anchored lines) + summary bullets.
    let postable = review_agent::to_postable(&findings, &changed);

    // 4) Optional sandbox verification ("receipts"): actually run the affected
    // tests/build. A hard failure forces REQUEST_CHANGES.
    let mut verify_md = String::new();
    let mut verify_failed = false;
    if cfg.verify_tests {
        let changed_files: Vec<String> = compare["files"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|f| f["filename"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        if let Some((passed, md)) = review_agent::verify_in_sandbox(
            state,
            &github,
            &msg.repo_owner,
            &msg.repo_name,
            &head_sha,
            &changed_files,
            deadline,
        )
        .await
        {
            verify_md = md;
            verify_failed = !passed;
        }
    }

    // Verdict: a surviving blocking finding OR a failed verification forces
    // REQUEST_CHANGES; otherwise the model's verdict, fail-closed to
    // REQUEST_CHANGES on anything non-APPROVE.
    let verdict: &'static str = if postable.blocking_count > 0 || verify_failed {
        "REQUEST_CHANGES"
    } else if output.verdict.eq_ignore_ascii_case("APPROVE") {
        "APPROVE"
    } else {
        "REQUEST_CHANGES"
    };
    // Computed, explainable risk (blast-radius-weighted) — overrides the model's
    // guess and drives the displayed level.
    let risk_report = review_risk::assess(
        &github,
        &msg.repo_owner,
        &msg.repo_name,
        &head_sha,
        &compare,
    )
    .await;
    let risk = risk_report.level.to_string();

    // GitHub forbids APPROVE / REQUEST_CHANGES on your OWN PR → downgrade to COMMENT.
    let self_authored = pr_author.contains("coderhelm");
    let effective_event = if self_authored { "COMMENT" } else { verdict };
    let verdict_line = if verdict == "APPROVE" {
        "✅ **Approved**"
    } else {
        "🔴 **Changes requested**"
    };
    let summary = if output.summary.trim().is_empty() {
        "Automated review complete.".to_string()
    } else {
        output.summary.clone()
    };
    let mut full_body = format!("{verdict_line}\n\n{summary}\n\n{}", risk_report.markdown());
    if !verify_md.is_empty() {
        full_body.push_str(&format!("\n\n{verify_md}"));
    }
    if !postable.unanchored_md.is_empty() {
        full_body.push_str(&format!(
            "\n\n#### Additional findings\n{}",
            postable.unanchored_md
        ));
    }
    full_body.push_str(RATING_FOOTER);

    // Speak whenever there is news for a person. Silent only when this review
    // repeats the previous verdict AND either covers the same commit (a duplicate
    // trigger) or covers a commit CoderHelm itself pushed (its own fix loop).
    let prev = last_review_verdict(state, msg).await;
    let prev_verdict = prev.as_ref().map(|p| p.verdict.clone());
    let head_by_bot = head_commit_by_bot(&compare, &head_sha);
    let should_comment = should_post_review(
        is_explicit_trigger(&msg.trigger, msg.question.is_some()),
        prev.as_ref()
            .map(|p| (p.verdict.as_str(), p.head_sha.as_str())),
        verdict,
        &head_sha,
        head_by_bot,
    );

    // Post ONE batched review with inline comments; fall back to body-only if
    // GitHub rejects an anchor (a bad line must never drop the whole verdict).
    if should_comment {
        let posted = github
            .create_pr_review_inline(
                &msg.repo_owner,
                &msg.repo_name,
                msg.pr_number,
                &head_sha,
                effective_event,
                &full_body,
                &postable.inline,
            )
            .await;
        if let Err(e) = posted {
            warn!(pr = msg.pr_number, error = %e, "Inline review post failed — retrying body-only");
            github
                .create_pr_review(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.pr_number,
                    effective_event,
                    &full_body,
                )
                .await?;
        }
    }

    // Persist: store the summary + a compact findings digest for the dashboard.
    let record_body = {
        let mut b = summary.clone();
        for f in &findings {
            b.push_str(&format!(
                "\n\n- [{}] {}:{} — {}\n  {}",
                f.severity, f.file, f.line, f.title, f.body
            ));
        }
        b
    };
    let record_sk = store_review_record(
        state,
        msg,
        &head_sha,
        verdict,
        &risk,
        &record_body,
        effective_event,
        "",
    )
    .await;
    info!(
        pr = msg.pr_number,
        head = %head_sha,
        verdict = verdict,
        risk = %risk,
        findings = findings.len(),
        inline = postable.inline.len(),
        posted_as = effective_event,
        posted = should_comment,
        trigger = %msg.trigger,
        elapsed_s = started.elapsed().as_secs(),
        "Reviewer finished"
    );

    // ── Post-approval actions (opt-in, off by default) ──
    if verdict == "APPROVE" {
        // Self-authored (CoderHelm's own) PRs CAN auto-merge, but run_on_approve
        // forces the two-key human-approval gate on for them — the bot approving
        // its own code is never the second key. Human PRs use the repo's config.
        let report = super::review_actions::run_on_approve(
            state,
            &github,
            &msg.team_id,
            msg.installation_id,
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
            &head_sha,
            base_branch,
            self_authored,
        )
        .await;
        // The gate keeps its own single status comment on the PR, so the arming
        // itself posts nothing; the summary only goes on the dashboard record.
        if !report.summary.is_empty() {
            // An UPDATE of the record just written, never a second put: one review
            // execution must produce exactly one dashboard record.
            let upd = state
                .dynamo
                .update_item()
                .table_name(&state.config.settings_table_name)
                .key("pk", attr_s(&msg.team_id))
                .key("sk", attr_s(&record_sk))
                .update_expression("SET action_summary = :a")
                .expression_attribute_values(
                    ":a",
                    attr_s(&common::head_tail_str(&report.summary, 8_000)),
                )
                .send()
                .await;
            if let Err(e) = upd {
                warn!(pr = msg.pr_number, error = %e, "Failed to attach action summary (non-fatal)");
            }
        }
    } else if self_authored {
        // CoderHelm reviewed its OWN PR and wants changes → hand the findings to the
        // run's feedback loop so it applies the fixes. Two guards keep this review↔fix
        // loop from running away (it once flip-flopped a verdict into 67 commits / 44
        // reviews):
        //   (1) Hysteresis — if the PREVIOUS verdict was APPROVE, do NOT auto-fix this
        //       REQUEST_CHANGES. An approve→request flip on the bot's own code is
        //       almost always reviewer non-determinism, not a real regression; the
        //       findings stay visible for a human to judge.
        //   (2) Convergence cap — bound the self-review→fix rounds per PR. Past the
        //       cap, stop auto-fixing and hand off to a human (announced once).
        if prev_verdict.as_deref() == Some("APPROVE") {
            info!(
                pr = msg.pr_number,
                "Self-review flipped APPROVE→REQUEST_CHANGES — holding for a human, not re-fixing (hysteresis)"
            );
        } else if claim_self_review_round(
            state,
            &msg.team_id,
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
        )
        .await
        {
            // Under the cap — apply the fixes. A new commit re-triggers the review.
            feed_review_back_to_run(state, msg, &record_body).await;
        } else if claim_once(
            state,
            &msg.team_id,
            &format!(
                "SELFREVIEWHANDOFF#{}/{}#{:0>6}",
                msg.repo_owner, msg.repo_name, msg.pr_number
            ),
        )
        .await
        {
            // Cap reached — stop the auto-fix loop and hand to a human, once.
            let _ = github
                .create_issue_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.pr_number,
                    &format!(
                        "🛑 I've self-fixed this PR {MAX_SELF_REVIEW_FIX_ROUNDS} times without the \
                         review converging, so I'm stopping the automatic fix loop and handing it \
                         to a human. My latest findings are above."
                    ),
                )
                .await;
            info!(
                pr = msg.pr_number,
                "Self-review→fix cap reached — handed off to a human"
            );
        }
    }

    Ok(())
}

/// How many self-review → self-fix rounds CoderHelm runs on its OWN PR before it
/// gives up and hands to a human. Bounds the review↔fix loop so a flip-flopping
/// verdict can't churn commits/reviews forever (one PR hit 67 commits / 44 reviews
/// oscillating APPROVE↔REQUEST_CHANGES).
const MAX_SELF_REVIEW_FIX_ROUNDS: u32 = 3;

/// Claim one self-review→fix round for this PR (atomic conditional increment).
/// True while under MAX_SELF_REVIEW_FIX_ROUNDS (proceed with the fix); false once
/// the cap is reached — or on ANY error (fail CLOSED, like claim_auto_fix_slot:
/// never keep looping when the bound can't be confirmed). The counter expires
/// after `SELF_REVIEW_ROUNDS_TTL_SECS` so a genuinely fresh re-run of the ticket
/// later starts over; an expired counter is reset here rather than waiting for
/// DynamoDB's lazy TTL deletion.
const SELF_REVIEW_ROUNDS_TTL_SECS: u64 = 3 * 86_400;

async fn claim_self_review_round(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    repo: &str,
    pr: u64,
) -> bool {
    let sk = format!("SELFREVIEWROUNDS#{owner}/{repo}#{pr:0>6}");
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let ttl = now + SELF_REVIEW_ROUNDS_TTL_SECS;
    let counted = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .update_expression("SET #ttl = :ttl ADD rounds :one")
        .condition_expression(
            "(attribute_not_exists(rounds) OR rounds < :max) AND \
             (attribute_not_exists(#ttl) OR #ttl >= :now)",
        )
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":one", attr_n(1))
        .expression_attribute_values(":max", attr_n(u64::from(MAX_SELF_REVIEW_FIX_ROUNDS)))
        .expression_attribute_values(":ttl", attr_n(ttl))
        .expression_attribute_values(":now", attr_n(now))
        .send()
        .await;
    if counted.is_ok() {
        return true;
    }
    // The counter may only have failed because it expired: start a new one.
    state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&sk))
        .update_expression("SET #ttl = :ttl, rounds = :one")
        .condition_expression("#ttl < :now")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":one", attr_n(1))
        .expression_attribute_values(":ttl", attr_n(ttl))
        .expression_attribute_values(":now", attr_n(now))
        .send()
        .await
        .is_ok()
}

/// Perform a one-time action for a key (e.g. post the handoff notice once).
/// True only for the first caller within 7 days. Fails OPEN (acts) on a
/// transient error so a real one-time notice is never lost.
async fn claim_once(state: &WorkerState, team_id: &str, sk: &str) -> bool {
    common::claim::claim(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        sk,
        7 * 86_400,
    )
    .await
    .won_or_failed_open()
}

/// Route a self-review's requested changes into the originating run's feedback
/// loop so CoderHelm fixes them. Best-effort: no run found (PR opened outside a
/// tracked run) or no queue configured → a logged no-op, never an error.
async fn feed_review_back_to_run(state: &WorkerState, msg: &ReviewMessage, review_body: &str) {
    let Some(run_id) = lookup_run_by_pr(
        state,
        &msg.team_id,
        &msg.repo_owner,
        &msg.repo_name,
        msg.pr_number,
    )
    .await
    else {
        info!(
            pr = msg.pr_number,
            "Self-review requested changes but no run found — skipping feedback"
        );
        return;
    };
    if state.config.feedback_queue_url.is_empty() {
        warn!("FEEDBACK_QUEUE_URL not set — cannot route self-review feedback");
        return;
    }
    let body = serde_json::json!({
        "type": "feedback",
        "team_id": msg.team_id,
        "installation_id": msg.installation_id,
        "run_id": run_id,
        "repo_owner": msg.repo_owner,
        "repo_name": msg.repo_name,
        "pr_number": msg.pr_number,
        "review_id": 0,
        "review_body": format!("Automated reviewer requested changes:\n\n{review_body}"),
        "comments": [],
    });
    match state
        .sqs
        .send_message()
        .queue_url(&state.config.feedback_queue_url)
        .message_body(body.to_string())
        .send()
        .await
    {
        Ok(_) => info!(
            pr = msg.pr_number,
            run_id = %run_id,
            "Reviewer requested changes on own PR → fed back to run"
        ),
        Err(e) => {
            warn!(pr = msg.pr_number, error = %e, "Failed to enqueue self-review feedback")
        }
    }
}

/// Build a "past feedback to learn from" block from this repo's recent review
/// records — the 👎 ratings and human notes the team left. Reused so the reviewer
/// stops repeating comment styles the team rejected. Best-effort, capped, empty
/// on any error.
async fn load_learning_context(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
) -> String {
    let prefix = format!("REVIEW#{owner}/{name}#");
    let Ok(res) = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :pfx)")
        .expression_attribute_values(":pk", attr_s(team_id))
        .expression_attribute_values(":pfx", attr_s(&prefix))
        .scan_index_forward(false)
        .limit(60)
        .send()
        .await
    else {
        return String::new();
    };

    let mut notes: Vec<String> = vec![];
    for item in res.items() {
        if notes.len() >= 15 {
            break;
        }
        let comments = item
            .get("rating_comments")
            .and_then(|v| v.as_l().ok())
            .cloned()
            .unwrap_or_default();
        for c in comments {
            let Ok(s) = c.as_s() else { continue };
            let Ok(v) = serde_json::from_str::<serde_json::Value>(s) else {
                continue;
            };
            let text = v["text"].as_str().unwrap_or("").trim();
            if text.is_empty() {
                continue;
            }
            let rating = v["rating"].as_str().unwrap_or("");
            let tag = if rating == "down" { "👎" } else { "📝" };
            notes.push(format!("- {tag} {}", common::truncate_str(text, 240)));
            if notes.len() >= 15 {
                break;
            }
        }
    }
    if notes.is_empty() {
        return String::new();
    }
    format!(
        "\n\n## Past reviewer feedback from this team (learn from it — don't repeat rejected styles)\n{}",
        notes.join("\n")
    )
}

/// Newest run for a PR, via the runs-table repo-index GSI. None if untracked.
async fn lookup_run_by_pr(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    name: &str,
    pr_number: u64,
) -> Option<String> {
    let team_repo = format!("{team_id}#{owner}/{name}");
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.runs_table_name)
        .index_name("repo-index")
        .key_condition_expression("team_repo = :tr")
        .filter_expression("pr_number = :pn")
        .expression_attribute_values(":tr", attr_s(&team_repo))
        .expression_attribute_values(":pn", attr_n(pr_number))
        .scan_index_forward(false)
        .limit(50)
        .send()
        .await
        .ok()?;
    result
        .items()
        .iter()
        .find_map(|it| it.get("run_id").and_then(|v| v.as_s().ok()).cloned())
}

/// CoderHelm's most recent verdict on a PR.
struct PrevVerdict {
    verdict: String,
    head_sha: String,
}

/// The most recent verdict CoderHelm recorded for this PR (any head), or None if
/// it has never reviewed it. Records are keyed sk=REVIEW#{repo}#{pr:06}#{rfc3339},
/// so a descending query yields newest-first. Question answers are skipped.
async fn last_review_verdict(state: &WorkerState, msg: &ReviewMessage) -> Option<PrevVerdict> {
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let prefix = format!("REVIEW#{repo}#{:0>6}#", msg.pr_number);
    let resp = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :sk)")
        .expression_attribute_values(":pk", attr_s(&msg.team_id))
        .expression_attribute_values(":sk", attr_s(&prefix))
        .scan_index_forward(false)
        .limit(10)
        .send()
        .await
        .ok()?;
    resp.items().iter().find_map(|i| {
        let verdict = i.get("verdict")?.as_s().ok()?;
        if verdict != "APPROVE" && verdict != "REQUEST_CHANGES" {
            return None;
        }
        Some(PrevVerdict {
            verdict: verdict.clone(),
            head_sha: i
                .get("head_sha")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default(),
        })
    })
}

/// A person explicitly asked for this review (a reply, a question, or the
/// native "Re-request review" button).
fn is_explicit_trigger(trigger: &str, has_question: bool) -> bool {
    has_question || trigger == "reply" || trigger == "rerequest"
}

/// Pure: post this review on the PR?
///
/// Always when a person asked, when the verdict changed, or when it covers a
/// new commit a person pushed. Silent only for a repeat verdict on the same
/// commit (a duplicate trigger) or on a commit CoderHelm pushed itself (its own
/// fix loop, which would otherwise post a review per self-fix commit).
fn should_post_review(
    explicit: bool,
    prev: Option<(&str, &str)>,
    verdict: &str,
    head_sha: &str,
    head_by_bot: bool,
) -> bool {
    if explicit {
        return true;
    }
    match prev {
        None => true,
        Some((prev_verdict, _)) if prev_verdict != verdict => true,
        Some((_, prev_head)) if prev_head == head_sha => false,
        Some(_) => !head_by_bot,
    }
}

/// Pure: was the head commit authored by CoderHelm? Reads the compare
/// response's commit list (oldest first).
fn head_commit_by_bot(compare: &serde_json::Value, head_sha: &str) -> bool {
    compare["commits"]
        .as_array()
        .and_then(|cs| {
            cs.iter()
                .rev()
                .find(|c| c["sha"].as_str() == Some(head_sha))
                .or_else(|| cs.last())
        })
        .and_then(|c| c["author"]["login"].as_str())
        .is_some_and(|login| login.contains("coderhelm"))
}

/// Persist a review record to the settings table so the dashboard can list it and
/// ratings/actions can attach. Keyed pk=team_id, sk=REVIEW#{repo}#{pr:06}#{ts}.
/// Best-effort: a storage failure must never break the actual GitHub review.
/// Returns the record's sort key so follow-up steps can UPDATE this record
/// (e.g. attach the auto-merge action summary) instead of writing a second one
/// — one review execution must never produce two dashboard records.
#[allow(clippy::too_many_arguments)]
async fn store_review_record(
    state: &WorkerState,
    msg: &ReviewMessage,
    head_sha: &str,
    verdict: &str,
    risk: &str,
    body: &str,
    posted_as: &str,
    action_summary: &str,
) -> String {
    let created_at = chrono::Utc::now().to_rfc3339();
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let sk = format!("REVIEW#{repo}#{:0>6}#{created_at}", msg.pr_number);
    // Truncate the stored body so a huge review can't blow the 400KB item limit.
    let body = common::head_tail_str(body, 30_000);

    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&msg.team_id))
        .item("sk", attr_s(&sk))
        .item("record_type", attr_s("review"))
        .item("repo", attr_s(&repo))
        .item("pr_number", attr_n(msg.pr_number))
        .item("head_sha", attr_s(head_sha))
        .item("verdict", attr_s(verdict))
        .item("risk", attr_s(risk))
        .item("body", attr_s(&body))
        .item("posted_as", attr_s(posted_as))
        .item("trigger", attr_s(&msg.trigger))
        .item("thumbs_up", attr_n(0))
        .item("thumbs_down", attr_n(0))
        .item("created_at", attr_s(&created_at));
    if !action_summary.is_empty() {
        put = put.item(
            "action_summary",
            attr_s(&common::head_tail_str(action_summary, 8_000)),
        );
    }
    if let Err(e) = put.send().await {
        warn!(pr = msg.pr_number, error = %e, "Failed to persist review record (non-fatal)");
    }
    sk
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn first_review_and_verdict_changes_always_post() {
        assert!(should_post_review(false, None, "APPROVE", "h1", true));
        assert!(should_post_review(
            false,
            Some(("APPROVE", "h1")),
            "REQUEST_CHANGES",
            "h2",
            true
        ));
    }

    #[test]
    fn repeat_verdict_on_a_human_push_posts() {
        // A person pushed a fix that did not change the verdict: they must
        // still hear that the new commit was reviewed.
        assert!(should_post_review(
            false,
            Some(("REQUEST_CHANGES", "h1")),
            "REQUEST_CHANGES",
            "h2",
            false
        ));
    }

    #[test]
    fn repeat_verdict_stays_silent_for_duplicates_and_bot_pushes() {
        // Same commit reviewed again (duplicate trigger).
        assert!(!should_post_review(
            false,
            Some(("APPROVE", "h1")),
            "APPROVE",
            "h1",
            false
        ));
        // CoderHelm's own fix-loop commit.
        assert!(!should_post_review(
            false,
            Some(("APPROVE", "h1")),
            "APPROVE",
            "h2",
            true
        ));
    }

    #[test]
    fn explicit_asks_always_post() {
        assert!(is_explicit_trigger("reply", false));
        assert!(is_explicit_trigger("rerequest", false));
        assert!(is_explicit_trigger("synchronize", true));
        assert!(!is_explicit_trigger("synchronize", false));
        assert!(should_post_review(
            true,
            Some(("APPROVE", "h1")),
            "APPROVE",
            "h1",
            true
        ));
    }

    #[test]
    fn head_commit_author_is_read_from_the_compare() {
        let compare = json!({"commits": [
            {"sha": "a", "author": {"login": "thinkaxelthink"}},
            {"sha": "b", "author": {"login": "coderhelm[bot]"}},
        ]});
        assert!(head_commit_by_bot(&compare, "b"));
        assert!(!head_commit_by_bot(&compare, "a"));
        // A commit with no linked GitHub account is not the bot's.
        let unlinked = json!({"commits": [{"sha": "c", "author": null}]});
        assert!(!head_commit_by_bot(&unlinked, "c"));
    }
}
