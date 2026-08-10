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
use tracing::warn;

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
    /// Schedule the async, baseline-aware post-merge health check.
    pub health_check: bool,
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
            health_check: false,
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
        Self {
            auto_merge: get_bool("auto_merge", d.auto_merge),
            merge_method,
            require_human_approval: get_bool("require_human_approval", d.require_human_approval),
            auto_tag: get_bool("auto_tag", d.auto_tag),
            tag_mode,
            tag_prefix,
            health_check: get_bool("health_check", d.health_check),
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
        Ok(v) => v
            .as_array()
            .map(|a| a.as_slice())
            .unwrap_or(&[])
            .iter()
            .any(|r| {
                r["state"].as_str() == Some("APPROVED")
                    && !r["user"]["login"]
                        .as_str()
                        .unwrap_or("")
                        .contains("coderhelm")
            }),
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

    if cfg.auto_tag {
        let tag = if cfg.tag_mode == "date" {
            format!(
                "{}{}",
                cfg.tag_prefix,
                chrono::Utc::now().format("%Y%m%d-%H%M%S")
            )
        } else {
            let existing: Vec<String> = github
                .list_tags(owner, repo)
                .await
                .ok()
                .and_then(|v| v.as_array().cloned())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|t| t["name"].as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            next_semver_tag(&existing, &cfg.tag_prefix)
        };
        match github.create_tag_ref(owner, repo, &tag, head_sha).await {
            Ok(_) => lines.push(format!("🏷️ Tagged `{tag}`.")),
            Err(e) => {
                warn!(pr_number, error = %e, "Auto-tag failed");
                lines.push(format!("⚠️ Tag failed: {e}"));
            }
        }
    }

    if cfg.health_check {
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
