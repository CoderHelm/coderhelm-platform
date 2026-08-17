//! Post-approval actions for the reviewer agent: merge → tag → health-guard.
//!
//! Everything here is OFF by default and gated behind explicit per-repo config.
//! The safety model is deliberately conservative:
//!   - `auto_merge` only runs on a bot APPROVE verdict.
//!   - `require_human_approval` (default TRUE) is a two-key rule: even with
//!     auto-merge on, a human must also have approved the PR. This is what keeps
//!     an LLM verdict from being sufficient to merge to a live branch on its own.
//!   - the merge is SHA-guarded — GitHub 409s (→ no-op) if a commit landed after
//!     the review, so unreviewed code can never be merged.
//!   - after a merge, the health guard watches CI on the base branch (and, when
//!     configured, scans CloudWatch log groups for an error spike) and posts a
//!     loud alert if the deploy looks broken. It reports; it never auto-reverts a
//!     shared branch (that is itself dangerous — a human decides the rollback).

use crate::clients::github::GitHubClient;
use crate::WorkerState;
use tracing::{info, warn};

/// SQS `DelaySeconds` hard cap (15 min). Batch windows longer than this chain
/// across multiple delayed messages.
const SQS_MAX_DELAY_SECS: u64 = 900;

/// Per-repo post-approval config, read from the same REVIEW_CONFIG item the
/// reviewer core uses. Every field defaults to the SAFE value (no action).
#[derive(Debug, Clone)]
pub struct OnApproveConfig {
    pub auto_merge: bool,
    /// "squash" | "merge" | "rebase".
    pub merge_method: String,
    /// Even with auto_merge, require a human APPROVED review too (two-key).
    pub require_human_approval: bool,
    pub auto_tag: bool,
    /// "semver" → cut the next patch release (e.g. v1.2.3 → v1.2.4); "date" →
    /// a timestamp marker tag. Default semver, so `tag_prefix` "v" produces a
    /// real release version, not a datestamp wearing a version's prefix.
    pub tag_mode: String,
    pub tag_prefix: String,
    /// Batch window, in minutes, for release tags: merges that land within the
    /// window fold into ONE release tag cut at the latest HEAD (fewer redundant
    /// prod deploys). `0` = tag immediately on every merge (no batching).
    /// Default 15.
    pub tag_batch_minutes: u32,
    /// Schedule the async, baseline-aware post-merge health check.
    pub health_check: bool,
    /// Optional label to ADD to the PR once it's cleared to merge (all approvals
    /// in), before merging. The repo's own CI is expected to react to it (e.g. a
    /// deploy-to-staging / deploy-preview workflow); the merge gate then waits for
    /// that CI to go green before merging. Empty = no label step. The value is
    /// operator-configured per repo — never hardcoded here.
    pub deploy_label: String,
}

impl Default for OnApproveConfig {
    fn default() -> Self {
        Self {
            auto_merge: false,
            merge_method: "squash".to_string(),
            require_human_approval: true,
            auto_tag: false,
            tag_mode: "semver".to_string(),
            tag_prefix: "v".to_string(),
            tag_batch_minutes: 15,
            health_check: false,
            deploy_label: String::new(),
        }
    }
}

impl OnApproveConfig {
    pub async fn load(state: &WorkerState, team_id: &str, owner: &str, name: &str) -> Self {
        let sk = format!("REVIEW_CONFIG#REPO#{owner}/{name}");
        let item = state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", super::attr_s(team_id))
            .key("sk", super::attr_s(&sk))
            .send()
            .await
            .ok()
            .and_then(|o| o.item().cloned());
        let Some(item) = item else {
            return Self::default();
        };
        let d = Self::default();
        let get_bool = |k: &str, dv: bool| {
            item.get(k)
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(dv)
        };
        let merge_method = item
            .get("merge_method")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.to_string())
            .filter(|s| matches!(s.as_str(), "squash" | "merge" | "rebase"))
            .unwrap_or(d.merge_method);
        let tag_mode = item
            .get("tag_mode")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.to_string())
            .filter(|s| matches!(s.as_str(), "semver" | "date"))
            .unwrap_or(d.tag_mode);
        let tag_prefix = item
            .get("tag_prefix")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or(d.tag_prefix);
        let deploy_label = item
            .get("deploy_label")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.trim().to_string())
            .unwrap_or(d.deploy_label);
        // Stored as a DynamoDB number. Cap at 6h so a fat-fingered value can't
        // strand a merge undeployed for days; 0 stays "immediate".
        let tag_batch_minutes = item
            .get("tag_batch_minutes")
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse::<u32>().ok())
            .map(|m| m.min(360))
            .unwrap_or(d.tag_batch_minutes);
        Self {
            auto_merge: get_bool("auto_merge", d.auto_merge),
            merge_method,
            require_human_approval: get_bool("require_human_approval", d.require_human_approval),
            auto_tag: get_bool("auto_tag", d.auto_tag),
            tag_mode,
            tag_prefix,
            tag_batch_minutes,
            health_check: get_bool("health_check", d.health_check),
            deploy_label,
        }
    }
}

/// Pure gate: may we auto-merge? Kept separate so the decision is unit-testable
/// without any network. `require_human` is passed explicitly (not read from cfg)
/// because a self-authored PR forces it on regardless of config. Fail-safe:
/// anything other than an explicit yes is a no.
pub fn should_merge(
    auto_merge: bool,
    require_human: bool,
    verdict_approved: bool,
    human_approved: bool,
) -> bool {
    if !auto_merge || !verdict_approved {
        return false;
    }
    if require_human && !human_approved {
        return false;
    }
    true
}

/// Compute the next patch release tag from existing tags. Parses tags shaped
/// `[prefix]MAJOR.MINOR.PATCH[-...]` (a bare `v` prefix is also tolerated),
/// finds the highest version, and bumps PATCH. With no existing semver tag it
/// seeds `{prefix}0.1.0`. Pure — unit-tested without any network.
pub fn next_semver_tag(existing: &[String], prefix: &str) -> String {
    fn parse(tag: &str, prefix: &str) -> Option<(u64, u64, u64)> {
        let core = tag
            .strip_prefix(prefix)
            .or_else(|| tag.strip_prefix('v'))
            .unwrap_or(tag);
        // Drop any pre-release/build suffix (e.g. -rc1, +build).
        let core = core.split(['-', '+']).next().unwrap_or(core);
        let mut it = core.split('.');
        let maj = it.next()?.parse::<u64>().ok()?;
        let min = it.next()?.parse::<u64>().ok()?;
        let patch = it.next()?.parse::<u64>().ok()?;
        // Reject trailing junk like "1.2.3.4".
        if it.next().is_some() {
            return None;
        }
        Some((maj, min, patch))
    }

    let highest = existing.iter().filter_map(|t| parse(t, prefix)).max();
    match highest {
        Some((maj, min, patch)) => format!("{prefix}{maj}.{min}.{}", patch + 1),
        None => format!("{prefix}0.1.0"),
    }
}

/// Outcome of the post-approval sequence, surfaced into the review record and a
/// PR comment.
#[derive(Debug, Default)]
pub struct ActionReport {
    /// Human-readable markdown summary for the PR comment; empty ⇒ nothing done.
    pub summary: String,
}

/// Is there at least one human APPROVED review on the PR (not the bot's)?
pub(crate) async fn human_approval_present(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    pr: u64,
) -> bool {
    match github.list_pr_reviews(owner, repo, pr).await {
        Ok(v) => {
            // LATEST-review-per-reviewer semantics (what GitHub itself uses for
            // merge eligibility). A human who approved and LATER requested
            // changes keeps the old review's state "APPROVED" in history — a
            // simple any(APPROVED) would count that stale approval as the human
            // key against the reviewer's CURRENT objection. COMMENTED reviews
            // don't supersede a verdict; DISMISSED clears one. And any
            // reviewer whose current verdict is CHANGES_REQUESTED blocks — a
            // standing objection can't be outvoted by someone else's approval.
            let mut latest: std::collections::HashMap<String, &str> =
                std::collections::HashMap::new();
            for r in v.as_array().map(|a| a.as_slice()).unwrap_or(&[]) {
                let login = r["user"]["login"].as_str().unwrap_or("");
                if login.is_empty() || login.contains("coderhelm") {
                    continue;
                }
                // list_pr_reviews returns chronological order — the last
                // meaningful state per reviewer wins. COMMENTED/PENDING never
                // supersede a verdict.
                if let s @ ("APPROVED" | "CHANGES_REQUESTED" | "DISMISSED") =
                    r["state"].as_str().unwrap_or("")
                {
                    latest.insert(login.to_string(), s);
                }
            }
            let any_approved = latest.values().any(|s| *s == "APPROVED");
            let any_blocking = latest.values().any(|s| *s == "CHANGES_REQUESTED");
            any_approved && !any_blocking
        }
        Err(e) => {
            warn!(pr, error = %e, "Could not list PR reviews — treating as no human approval");
            false
        }
    }
}

/// On an APPROVE verdict, ARM the async auto-merge gate (if the repo enabled it).
/// The actual merge happens in `await_merge` only once BOTH keys are present
/// (bot APPROVE + human approval) AND every CI check is green — it waits for
/// pending checks (e.g. a staging deploy) and never merges on failing CI.
#[allow(clippy::too_many_arguments)]
pub async fn run_on_approve(
    state: &WorkerState,
    github: &GitHubClient,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    repo: &str,
    pr_number: u64,
    head_sha: &str,
    base_branch: &str,
    self_authored: bool,
) -> ActionReport {
    let cfg = OnApproveConfig::load(state, team_id, owner, repo).await;
    let mut report = ActionReport::default();
    if !cfg.auto_merge {
        return report; // nothing configured
    }
    let _ = github; // arming reads config only; the gate re-fetches state each tick
    if super::await_merge::arm(
        state,
        team_id,
        installation_id,
        owner,
        repo,
        pr_number,
        head_sha,
        base_branch,
        self_authored,
    )
    .await
    {
        report.summary =
            "### 🚀 Auto-merge armed\n\n🤝 I'll merge this automatically once a human \
             approves **and** every CI check passes — I wait for pending checks (like the staging \
             deploy) and never merge on failing or still-running CI."
                .to_string();
    }
    report
}

/// Tag + schedule the post-merge health check after a successful merge. Returns
/// markdown lines for the merge comment. Shared by the auto-merge gate.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn post_merge_actions(
    state: &WorkerState,
    github: &GitHubClient,
    cfg: &OnApproveConfig,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    repo: &str,
    pr_number: u64,
    head_sha: &str,
    base_branch: &str,
) -> Vec<String> {
    let mut lines = vec![format!(
        "✅ Merged `{}` into `{base_branch}` via {}.",
        &head_sha[..head_sha.len().min(7)],
        cfg.merge_method
    )];

    // Release tag. In batch mode we do NOT tag here — we arm a single coalesced
    // sweep for the repo, so several merges in the window share one release tag
    // (and one prod deploy). In immediate mode (window = 0) we tag now, but
    // idempotently (skip if this commit is already tagged).
    let batched_tag = cfg.auto_tag && cfg.tag_batch_minutes > 0;
    if cfg.auto_tag {
        if batched_tag {
            schedule_tag_sweep(
                state,
                cfg,
                team_id,
                installation_id,
                owner,
                repo,
                pr_number,
                base_branch,
            )
            .await;
            lines.push(format!(
                "🏷️ Release tag batched — I'll cut one tag at the latest commit in ~{} min so merges in this window share a single release. Set the batch window to 0 to tag every merge immediately.",
                cfg.tag_batch_minutes
            ));
        } else {
            match cut_tag_if_new(github, cfg, owner, repo, head_sha).await {
                Ok(Some(tag)) => lines.push(format!("🏷️ Tagged `{tag}`.")),
                Ok(None) => {
                    lines.push("🏷️ This commit is already tagged — no new release cut.".to_string())
                }
                Err(e) => {
                    warn!(pr_number, error = %e, "Auto-tag failed");
                    lines.push(format!("⚠️ Tag failed: {e}"));
                }
            }
        }
    }

    // Health guard runs at merge time — EXCEPT for a batched tag, where the prod
    // deploy fires later off the sweep's tag, so the sweep owns the health check
    // (scheduling it here would watch the wrong, pre-deploy checks).
    if cfg.health_check && !batched_tag {
        let baseline = super::health_check::failing_checks(github, owner, repo, base_branch).await;
        if super::health_check::schedule(
            state,
            team_id,
            installation_id,
            owner,
            repo,
            pr_number,
            base_branch,
            head_sha,
            baseline,
        )
        .await
        {
            lines.push(
                "🩺 Post-merge health check scheduled — I'll watch the deploy checks and flag any NEW failures.".to_string(),
            );
        }
    }
    lines
}

/// Cut the next release tag at `sha`, unless that commit is already tagged.
/// Returns `Ok(Some(tag))` when a new tag was created, `Ok(None)` when `sha`
/// already carries a tag (idempotent no-op). That "already tagged" check is what
/// makes the batched sweep safe to run more than once for the same HEAD — a
/// duplicate sweep (from a race or a fail-open claim) becomes a clean no-op
/// instead of a redundant release.
pub(crate) async fn cut_tag_if_new(
    github: &GitHubClient,
    cfg: &OnApproveConfig,
    owner: &str,
    repo: &str,
    sha: &str,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    let tags = github
        .list_tags(owner, repo)
        .await
        .ok()
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default();
    // A freshly created tag at HEAD is always among the newest tags returned, so
    // scanning page 1 reliably catches the "already released this commit" case.
    if tags
        .iter()
        .any(|t| t["commit"]["sha"].as_str() == Some(sha))
    {
        return Ok(None);
    }
    let tag = if cfg.tag_mode == "date" {
        format!(
            "{}{}",
            cfg.tag_prefix,
            chrono::Utc::now().format("%Y%m%d-%H%M%S")
        )
    } else {
        let names: Vec<String> = tags
            .iter()
            .filter_map(|t| t["name"].as_str().map(|s| s.to_string()))
            .collect();
        next_semver_tag(&names, &cfg.tag_prefix)
    };
    github.create_tag_ref(owner, repo, &tag, sha).await?;
    Ok(Some(tag))
}

/// Ensure exactly ONE tag sweep is armed for this repo's current batch window.
/// The first merge in the window claims a per-repo marker and enqueues the
/// delayed sweep; later merges see the marker and fold in (no second sweep).
/// Returns true if THIS call armed a new sweep. Fails OPEN on a transient claim
/// error (arms anyway) — a redundant sweep is harmless (see `cut_tag_if_new`),
/// a lost release is not.
#[allow(clippy::too_many_arguments)]
async fn schedule_tag_sweep(
    state: &WorkerState,
    cfg: &OnApproveConfig,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    repo: &str,
    pr_number: u64,
    base_branch: &str,
) -> bool {
    let marker = format!("TAGSWEEP#{owner}/{repo}");
    // TTL past the window so a crashed sweep can't wedge the marker for long.
    let ttl = chrono::Utc::now().timestamp() as u64 + (cfg.tag_batch_minutes as u64 * 60) + 3_600;
    let claimed = match state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", super::attr_s(team_id))
        .item("sk", super::attr_s(&marker))
        .item("ttl", super::attr_n(ttl))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await
    {
        Ok(_) => true,
        Err(e) => {
            let conflict = e
                .as_service_error()
                .map(|se| se.is_conditional_check_failed_exception())
                .unwrap_or(false);
            if conflict {
                return false; // a sweep is already armed — fold in silently
            }
            warn!(error = %e, "Tag sweep claim errored — arming anyway (fail-open)");
            true
        }
    };
    if claimed {
        let msg = crate::models::TagSweepMessage {
            team_id: team_id.to_string(),
            installation_id,
            repo_owner: owner.to_string(),
            repo_name: repo.to_string(),
            base_branch: base_branch.to_string(),
            pr_number,
            delay_remaining_secs: cfg.tag_batch_minutes as u64 * 60,
        };
        enqueue_tag_sweep(state, &msg).await;
    }
    claimed
}

/// Send (or re-send) the sweep with an SQS delay. Windows longer than the 900s
/// SQS cap chain: each hop waits up to 900s and decrements `delay_remaining_secs`
/// until it reaches 0, when the sweep actually runs.
async fn enqueue_tag_sweep(state: &WorkerState, msg: &crate::models::TagSweepMessage) -> bool {
    if state.config.ticket_queue_url.is_empty() {
        return false;
    }
    let delay = msg.delay_remaining_secs.min(SQS_MAX_DELAY_SECS);
    let body = match serde_json::to_value(msg) {
        Ok(mut v) => {
            if let Some(o) = v.as_object_mut() {
                o.insert("type".to_string(), serde_json::json!("tag_sweep"));
            }
            v.to_string()
        }
        Err(e) => {
            warn!(error = %e, "Tag sweep: serialize failed");
            return false;
        }
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
            warn!(error = %e, "Tag sweep: enqueue failed");
            false
        }
    }
}

/// Fire (or chain) a coalesced release-tag sweep. Cuts ONE tag at the base
/// branch's current HEAD — folding in every merge that landed during the window
/// — and, when enabled, schedules the post-deploy health guard against that tag.
pub async fn run_tag_sweep(
    state: &WorkerState,
    msg: crate::models::TagSweepMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Still inside the window — wait another hop (a single SQS delay is capped).
    if msg.delay_remaining_secs > SQS_MAX_DELAY_SECS {
        let mut next = msg.clone();
        next.delay_remaining_secs -= SQS_MAX_DELAY_SECS;
        enqueue_tag_sweep(state, &next).await;
        return Ok(());
    }

    let owner = &msg.repo_owner;
    let repo = &msg.repo_name;
    let marker = format!("TAGSWEEP#{owner}/{repo}");

    // Open the NEXT window BEFORE reading HEAD/tagging: any merge that lands from
    // here on must arm a fresh sweep rather than be silently folded into this one
    // (which is about to finish). The worst case is a second sweep at the same
    // HEAD, which `cut_tag_if_new` no-ops.
    let _ = state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", super::attr_s(&msg.team_id))
        .key("sk", super::attr_s(&marker))
        .send()
        .await;

    let cfg = OnApproveConfig::load(state, &msg.team_id, owner, repo).await;
    if !cfg.auto_tag {
        info!(repo = %format!("{owner}/{repo}"), "Tag sweep: auto_tag disabled mid-window — nothing to cut");
        return Ok(());
    }

    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    // Tag the branch's CURRENT head — every merge in the window is included.
    let head = match github.get_ref(owner, repo, &msg.base_branch).await {
        Ok(sha) => sha,
        Err(e) => {
            warn!(error = %e, base = %msg.base_branch, "Tag sweep: could not resolve base-branch head");
            return Ok(());
        }
    };

    match cut_tag_if_new(&github, &cfg, owner, repo, &head).await {
        Ok(Some(tag)) => {
            info!(%tag, repo = %format!("{owner}/{repo}"), "Batched release tag cut");
            // Prod deploy fires off the tag → schedule the health guard now.
            if cfg.health_check {
                let baseline =
                    super::health_check::failing_checks(&github, owner, repo, &msg.base_branch)
                        .await;
                super::health_check::schedule(
                    state,
                    &msg.team_id,
                    msg.installation_id,
                    owner,
                    repo,
                    msg.pr_number,
                    &msg.base_branch,
                    &head,
                    baseline,
                )
                .await;
            }
        }
        Ok(None) => {
            info!(repo = %format!("{owner}/{repo}"), "Tag sweep: HEAD already tagged — skipped")
        }
        Err(e) => {
            warn!(error = %e, repo = %format!("{owner}/{repo}"), "Tag sweep: create tag failed")
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_merge_when_disabled() {
        assert!(!should_merge(false, false, true, true));
    }

    #[test]
    fn no_merge_when_verdict_not_approve() {
        assert!(!should_merge(true, false, false, true));
    }

    #[test]
    fn two_key_blocks_without_human() {
        // auto_merge on, but require_human and no human ⇒ no merge.
        assert!(!should_merge(true, true, true, false));
    }

    #[test]
    fn two_key_allows_with_human() {
        assert!(should_merge(true, true, true, true));
    }

    #[test]
    fn merges_when_human_gate_disabled() {
        assert!(should_merge(true, false, true, false));
    }

    #[test]
    fn bot_pr_forces_human_even_when_config_off() {
        // review_pr computes require_human = cfg.require_human_approval || self_authored.
        // So a bot PR (self_authored) forces require_human=true even if config is off:
        let require_human = false || true; // config off, but self-authored
        assert!(!should_merge(true, require_human, true, false)); // no human → blocked
        assert!(should_merge(true, require_human, true, true)); // human approved → ok
    }

    #[test]
    fn defaults_are_all_safe() {
        let d = OnApproveConfig::default();
        assert!(!d.auto_merge);
        assert!(!d.auto_tag);
        assert!(!d.health_check);
        assert!(d.require_human_approval);
        assert_eq!(d.tag_mode, "semver");
        assert!(!should_merge(
            d.auto_merge,
            d.require_human_approval,
            true,
            true
        ));
    }

    #[test]
    fn semver_bumps_the_highest_patch() {
        let tags = vec!["v1.2.3".into(), "v1.2.9".into(), "v1.2.4".into()];
        assert_eq!(next_semver_tag(&tags, "v"), "v1.2.10");
    }

    #[test]
    fn semver_picks_highest_across_minor_and_major() {
        let tags = vec!["v1.9.9".into(), "v2.0.1".into(), "v1.10.0".into()];
        assert_eq!(next_semver_tag(&tags, "v"), "v2.0.2");
    }

    #[test]
    fn semver_seeds_when_no_tags() {
        assert_eq!(next_semver_tag(&[], "v"), "v0.1.0");
    }

    #[test]
    fn semver_ignores_non_version_and_prerelease_tags() {
        let tags = vec![
            "nightly".into(),
            "release-candidate".into(),
            "v1.0.0-rc1".into(),
            "1.0.0".into(), // bare, no prefix — still parsed
        ];
        // Highest parseable is 1.0.0 (rc1 stripped to 1.0.0 too) → bump patch.
        assert_eq!(next_semver_tag(&tags, "v"), "v1.0.1");
    }

    #[test]
    fn semver_rejects_four_part_versions() {
        let tags = vec!["v1.2.3.4".into(), "v0.5.0".into()];
        assert_eq!(next_semver_tag(&tags, "v"), "v0.5.1");
    }

    #[test]
    fn semver_honors_custom_prefix() {
        let tags = vec!["rel-2.3.4".into()];
        assert_eq!(next_semver_tag(&tags, "rel-"), "rel-2.3.5");
    }
}
