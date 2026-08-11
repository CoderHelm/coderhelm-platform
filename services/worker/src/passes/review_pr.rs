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

async fn load_config(state: &WorkerState, team_id: &str, owner: &str, name: &str) -> ReviewConfig {
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
        .and_then(|o| o.item().cloned());
    let Some(item) = item else {
        return ReviewConfig {
            enabled: false,
            killed: false,
            instructions: String::new(),
            verify_tests: false,
        };
    };
    ReviewConfig {
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
    }
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

pub async fn run(
    state: &WorkerState,
    msg: ReviewMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = load_config(state, &msg.team_id, &msg.repo_owner, &msg.repo_name).await;
    if !cfg.enabled || cfg.killed {
        info!(
            pr = msg.pr_number,
            "Reviewer disabled/killed for repo — skipping"
        );
        return Ok(());
    }

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
        let body = format!("{answer}{RATING_FOOTER}");
        github
            .create_issue_comment(&msg.repo_owner, &msg.repo_name, msg.pr_number, &body)
            .await?;
        store_review_record(
            state, &msg, &head_sha, "QUESTION", "N/A", &answer, "COMMENT", "",
        )
        .await;
        info!(pr = msg.pr_number, "Reviewer answered a question");
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
        &mut usage,
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

    // Post ONE batched review with inline comments; fall back to body-only if
    // GitHub rejects an anchor (a bad line must never drop the whole verdict).
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
    store_review_record(
        state,
        &msg,
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
        verdict = verdict,
        risk = %risk,
        findings = findings.len(),
        inline = postable.inline.len(),
        posted_as = effective_event,
        "Reviewer posted verdict"
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
        if !report.summary.is_empty() {
            let _ = github
                .create_issue_comment(
                    &msg.repo_owner,
                    &msg.repo_name,
                    msg.pr_number,
                    &report.summary,
                )
                .await;
            store_review_record(
                state,
                &msg,
                &head_sha,
                verdict,
                &risk,
                &record_body,
                effective_event,
                &report.summary,
            )
            .await;
        }
    } else if self_authored {
        // CoderHelm reviewed its OWN PR and wants changes → hand the findings to
        // the run's feedback loop so it applies the fixes (coderhelm reviews
        // coderhelm → picks up the changes). A new commit re-triggers the review.
        feed_review_back_to_run(state, &msg, &record_body).await;
    }

    Ok(())
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

/// Persist a review record to the settings table so the dashboard can list it and
/// ratings/actions can attach. Keyed pk=team_id, sk=REVIEW#{repo}#{pr:06}#{ts}.
/// Best-effort: a storage failure must never break the actual GitHub review.
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
) {
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
}
