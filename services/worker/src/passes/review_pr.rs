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
use crate::passes::{attr_n, attr_s};
use crate::WorkerState;
use tracing::{info, warn};

/// Per-repo reviewer config (worker-side mirror of the gateway's). OFF by
/// default — a second gate so a mis-fired enqueue still can't review a repo the
/// owner never opted in.
struct ReviewConfig {
    enabled: bool,
    killed: bool,
    instructions: String,
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
    }
}

/// Parsed verdict marker + risk + body.
pub(crate) struct ReviewMeta {
    pub verdict: &'static str,
    pub risk: &'static str,
    pub body: String,
}

/// Parse the model's markers. The prompt requires:
///   line 1: `VERDICT: APPROVE` | `VERDICT: REQUEST_CHANGES`
///   line 2 (optional): `RISK: LOW|MEDIUM|HIGH`
/// A missing/garbled verdict → REQUEST_CHANGES (fail-closed: never auto-approve
/// on an ambiguous verdict). A missing risk → MEDIUM (never silently LOW).
pub(crate) fn parse_review(reply: &str) -> ReviewMeta {
    let mut lines = reply.lines();
    let first = lines.next().unwrap_or("").trim();
    let upper = first.to_ascii_uppercase();
    let verdict = if upper.starts_with("VERDICT: APPROVE") || upper == "VERDICT:APPROVE" {
        "APPROVE"
    } else {
        "REQUEST_CHANGES"
    };

    // Look at the remaining lines; an optional RISK marker may lead.
    let rest: Vec<&str> = lines.collect();
    let mut body_start = 0;
    let mut risk = "MEDIUM";
    for (i, l) in rest.iter().enumerate() {
        let t = l.trim();
        if t.is_empty() {
            continue;
        }
        let u = t.to_ascii_uppercase();
        if let Some(v) = u.strip_prefix("RISK:") {
            let v = v.trim();
            risk = if v.starts_with("LOW") {
                "LOW"
            } else if v.starts_with("HIGH") {
                "HIGH"
            } else {
                "MEDIUM"
            };
            body_start = i + 1;
        }
        break;
    }

    let body = rest[body_start..].join("\n").trim().to_string();
    let body = if body.is_empty() {
        reply.trim().to_string()
    } else {
        body
    };
    ReviewMeta {
        verdict,
        risk,
        body,
    }
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

    // Build the diff (base...head compare gives per-file patches).
    let compare = github
        .get_diff(&msg.repo_owner, &msg.repo_name, base, &head_sha)
        .await?;
    let mut diff = String::new();
    if let Some(files) = compare["files"].as_array() {
        for f in files {
            let path = f["filename"].as_str().unwrap_or("");
            let patch = f["patch"].as_str().unwrap_or("(no textual diff)");
            diff.push_str(&format!("\n### {path}\n{patch}\n"));
        }
    }
    // Keep the prompt bounded; the tail of a huge diff is less useful than the head.
    let diff = common::head_tail_str(&diff, 40_000);

    let repo_instructions =
        super::load_repo_instructions(&github, &msg.repo_owner, &msg.repo_name).await;
    let extra = if cfg.instructions.is_empty() {
        String::new()
    } else {
        format!(
            "\n\n## Repo review focus (owner-provided)\n{}",
            cfg.instructions
        )
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
             isn't determinable from the diff, say so.{}{}",
            msg.repo_owner,
            msg.repo_name,
            super::format_instructions_block(&repo_instructions),
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

    // ── Verdict mode ──
    let system = format!(
        "You are a senior code reviewer for {owner}/{repo}. Review the PR diff for correctness \
         bugs, security issues, and violations of the repo's rules — findings only, no style \
         nitpicks unless they cause bugs. Be specific: cite file and line.\n\n\
         Respond in EXACTLY this shape:\n\
         - Line 1: `VERDICT: APPROVE` (no blocking issues) or `VERDICT: REQUEST_CHANGES` \
           (one or more real bugs/risks).\n\
         - Line 2: `RISK: LOW` | `RISK: MEDIUM` | `RISK: HIGH` — the blast radius if this merges \
           (data loss / auth / prod breakage = HIGH; isolated/tested change = LOW).\n\
         - Then a concise markdown review body: the findings (file:line + why), or a short \
           confirmation if approving. If unsure, REQUEST_CHANGES.{instructions}{extra}",
        owner = msg.repo_owner,
        repo = msg.repo_name,
        instructions = super::format_instructions_block(&repo_instructions),
    );
    let prompt = format!(
        "PR #{pr}: {title}\n\n{pr_body}\n\n## Diff (base...head)\n{diff}",
        pr = msg.pr_number,
    );

    // Fail-closed: if the model call fails, request changes rather than silently
    // approving or leaving the PR unreviewed.
    let meta = match provider::converse_simple(
        state,
        &provider,
        provider.heavy_model_id(),
        &system,
        &prompt,
        &mut usage,
    )
    .await
    {
        Ok(reply) => parse_review(&reply),
        Err(e) => {
            warn!(pr = msg.pr_number, error = %e, "Review model call failed — requesting changes (fail-closed)");
            ReviewMeta {
                verdict: "REQUEST_CHANGES",
                risk: "HIGH",
                body: format!(
                    "Automated review could not complete ({e}). Requesting changes so a human takes a look."
                ),
            }
        }
    };

    // GitHub forbids APPROVE / REQUEST_CHANGES on your OWN PR. When the PR was
    // authored by the bot, downgrade to a COMMENT so the verdict is still posted.
    let self_authored = pr_author.contains("coderhelm");
    let effective_event = if self_authored {
        "COMMENT"
    } else {
        meta.verdict
    };
    let verdict_line = if meta.verdict == "APPROVE" {
        "✅ **Approved**"
    } else {
        "🔴 **Changes requested**"
    };
    let full_body = format!(
        "{verdict_line} · risk: **{}**\n\n{}{RATING_FOOTER}",
        meta.risk, meta.body
    );

    github
        .create_pr_review(
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
            effective_event,
            &full_body,
        )
        .await?;
    store_review_record(
        state,
        &msg,
        &head_sha,
        meta.verdict,
        meta.risk,
        &meta.body,
        effective_event,
        "",
    )
    .await;
    info!(
        pr = msg.pr_number,
        verdict = meta.verdict,
        risk = meta.risk,
        posted_as = effective_event,
        "Reviewer posted verdict"
    );

    // ── Post-approval actions (opt-in, off by default) ──
    if meta.verdict == "APPROVE" && !self_authored {
        let report = super::review_actions::run_on_approve(
            state,
            &github,
            &msg.team_id,
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
            &head_sha,
            base_branch,
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
                meta.verdict,
                meta.risk,
                &meta.body,
                effective_event,
                &report.summary,
            )
            .await;
        }
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::parse_review;

    #[test]
    fn approve_marker_parsed() {
        let m = parse_review("VERDICT: APPROVE\nRISK: LOW\nLooks good, no issues.");
        assert_eq!(m.verdict, "APPROVE");
        assert_eq!(m.risk, "LOW");
        assert_eq!(m.body, "Looks good, no issues.");
    }

    #[test]
    fn request_changes_marker_parsed() {
        let m = parse_review("VERDICT: REQUEST_CHANGES\nRISK: HIGH\nsrc/a.ts:10 null deref.");
        assert_eq!(m.verdict, "REQUEST_CHANGES");
        assert_eq!(m.risk, "HIGH");
        assert!(m.body.contains("null deref"));
    }

    #[test]
    fn missing_or_garbled_marker_fails_closed_to_request_changes() {
        // No verdict line at all → must NOT auto-approve.
        assert_eq!(
            parse_review("I think this is probably fine?").verdict,
            "REQUEST_CHANGES"
        );
        assert_eq!(parse_review("").verdict, "REQUEST_CHANGES");
    }

    #[test]
    fn approve_is_never_inferred_from_body_text() {
        // "approve" only counts on the verdict line, not loose in the body.
        let m = parse_review("VERDICT: REQUEST_CHANGES\nI would approve if you fix X.");
        assert_eq!(m.verdict, "REQUEST_CHANGES");
    }

    #[test]
    fn missing_risk_defaults_to_medium_not_low() {
        let m = parse_review("VERDICT: APPROVE\nNo risk line here, just prose.");
        assert_eq!(m.verdict, "APPROVE");
        assert_eq!(m.risk, "MEDIUM");
        assert!(m.body.contains("No risk line"));
    }

    #[test]
    fn risk_marker_consumed_from_body() {
        let m = parse_review("VERDICT: APPROVE\nRISK: MEDIUM\nThe actual finding.");
        assert_eq!(m.risk, "MEDIUM");
        assert_eq!(m.body, "The actual finding.");
    }
}
