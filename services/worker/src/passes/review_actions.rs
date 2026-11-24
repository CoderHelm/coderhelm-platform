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
    /// The label that opts a person's PR into review (and so into the merge
    /// gate's status comment). Default `ch-review`.
    pub review_label: String,
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
            review_label: "ch-review".to_string(),
        }
    }
}

impl OnApproveConfig {
    /// The repo's config; a failed read falls back to the safe defaults
    /// (nothing enabled). The merge gate uses `try_load`, where a failed read
    /// must never be mistaken for "auto-merge off".
    pub async fn load(state: &WorkerState, team_id: &str, owner: &str, name: &str) -> Self {
        match Self::try_load(state, team_id, owner, name).await {
            Ok(cfg) => cfg,
            Err(e) => {
                warn!(owner, name, error = %e, "Could not read review config — using safe defaults");
                Self::default()
            }
        }
    }

    /// The repo's config. A missing item is the defaults; a failed read is an
    /// error.
    pub async fn try_load(
        state: &WorkerState,
        team_id: &str,
        owner: &str,
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let sk = format!("REVIEW_CONFIG#REPO#{owner}/{name}");
        let item = state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", super::attr_s(team_id))
            .key("sk", super::attr_s(&sk))
            .send()
            .await?
            .item()
            .cloned();
        let Some(item) = item else {
            return Ok(Self::default());
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
        let review_label = item
            .get("label")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or(d.review_label);
        Ok(Self {
            auto_merge: get_bool("auto_merge", d.auto_merge),
            merge_method,
            require_human_approval: get_bool("require_human_approval", d.require_human_approval),
            auto_tag: get_bool("auto_tag", d.auto_tag),
            tag_mode,
            tag_prefix,
            tag_batch_minutes,
            health_check: get_bool("health_check", d.health_check),
            deploy_label,
            review_label,
        })
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

/// Approval state of a PR, read from its full review list.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Approvals {
    /// People whose latest verdict is APPROVED.
    pub approved_by: Vec<String>,
    /// Of those, the ones whose approval is of the head commit.
    pub approved_at_head: Vec<String>,
    /// Reviewers (people or other bots) whose latest verdict is
    /// CHANGES_REQUESTED.
    pub blocking_by: Vec<String>,
    /// CoderHelm's own review of the head was dismissed and not posted again.
    pub bot_dismissed_at_head: bool,
    /// Each approving person's approved commit, `(login, commit)`.
    pub approved_on: Vec<(String, String)>,
    /// People whose latest review was dismissed, with the commit it was of:
    /// `(login, commit)`. A dismissal clears a verdict, so they are named
    /// when the gate waits on a person.
    pub dismissed: Vec<(String, String)>,
}

impl Approvals {
    /// The person key for merging on its own: a person approved this very
    /// commit, and nobody's standing request for changes outvotes it. A merge
    /// may reach production, so an approval of an earlier commit doesn't count
    /// even where GitHub's own rules would keep it.
    pub fn human_key_at_head(&self) -> bool {
        !self.approved_at_head.is_empty() && self.blocking_by.is_empty()
    }
}

/// Pure: approval state from GitHub's review list (oldest first).
///
/// Latest verdict per reviewer, which is what GitHub uses for merge
/// eligibility: a reviewer who approved and later requested changes is
/// blocking; COMMENTED and PENDING never supersede a verdict; DISMISSED
/// clears one. CoderHelm's own reviews are never the person key, and a
/// dismissal of its review of the head withdraws its key (fail closed).
pub fn approval_state(reviews: &serde_json::Value, head_sha: &str) -> Approvals {
    let mut latest: std::collections::BTreeMap<String, (&str, &str)> =
        std::collections::BTreeMap::new();
    let mut bot_at_head: Option<&str> = None;
    for r in reviews.as_array().map(|a| a.as_slice()).unwrap_or(&[]) {
        let login = r["user"]["login"].as_str().unwrap_or("");
        let state = r["state"].as_str().unwrap_or("");
        if login.is_empty() {
            continue;
        }
        if login.to_ascii_lowercase().contains("coderhelm") {
            if r["commit_id"].as_str() == Some(head_sha)
                && matches!(state, "APPROVED" | "CHANGES_REQUESTED" | "DISMISSED")
            {
                bot_at_head = Some(state);
            }
            continue;
        }
        if matches!(state, "APPROVED" | "CHANGES_REQUESTED" | "DISMISSED") {
            latest.insert(
                login.to_string(),
                (state, r["commit_id"].as_str().unwrap_or("")),
            );
        }
    }
    let with = |want: &str| -> Vec<String> {
        latest
            .iter()
            .filter(|(_, (s, _))| *s == want)
            .map(|(l, _)| l.clone())
            .collect()
    };
    let approved_at_head = latest
        .iter()
        .filter(|(_, (s, c))| *s == "APPROVED" && *c == head_sha)
        .map(|(l, _)| l.clone())
        .collect();
    let on = |want: &str| -> Vec<(String, String)> {
        latest
            .iter()
            .filter(|(_, (s, _))| *s == want)
            .map(|(l, (_, c))| (l.clone(), c.to_string()))
            .collect()
    };
    Approvals {
        approved_by: with("APPROVED"),
        approved_at_head,
        blocking_by: with("CHANGES_REQUESTED"),
        bot_dismissed_at_head: bot_at_head == Some("DISMISSED"),
        approved_on: on("APPROVED"),
        dismissed: on("DISMISSED"),
    }
}

/// Every review on the PR, read into its approval state. An unreadable
/// review list is an error, never "no objections".
pub(crate) async fn fetch_approvals(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    pr: u64,
    head_sha: &str,
) -> Result<Approvals, Box<dyn std::error::Error + Send + Sync>> {
    let reviews = github.list_pr_reviews(owner, repo, pr).await?;
    Ok(approval_state(&reviews, head_sha))
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
        // Recorded on the review; the PR itself shows the gate's one status
        // comment, edited in place, instead of a note per verdict.
        report.summary = "Auto-merge armed: merges once both keys are in and every CI check \
                          passes. Progress is in the PR's auto-merge status comment."
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
                false,
            )
            .await;
            lines.push(format!(
                "🏷️ Release tag batched — I'll cut one tag at the latest commit in ~{} min so merges in this window share a single release. Set the batch window to 0 to tag every merge immediately.",
                cfg.tag_batch_minutes
            ));
        } else {
            match cut_tag_if_new(github, cfg, owner, repo, head_sha).await {
                Ok(Some(tag)) => {
                    super::release_notes::after_tag(
                        state,
                        github,
                        team_id,
                        installation_id,
                        owner,
                        repo,
                        base_branch,
                        &tag,
                        head_sha,
                    )
                    .await;
                    lines.push(format!("🏷️ Tagged `{tag}`."));
                }
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
                "🩺 Post-merge health check scheduled: I'll watch this merge commit's deploy until it finishes, keep watching for 15 minutes after it goes green, and flag any NEW failures.".to_string(),
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
    // An unreadable tag list is an error: read as "no tags", the next version
    // would restart at the seed.
    let tags = github
        .list_tags(owner, repo)
        .await?
        .as_array()
        .cloned()
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
/// a lost release is not. `count`: this call is the merge's push, so a
/// window already waiting counts one more merge (CoderHelm's own merge also
/// arms, and its push then counts it, once).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn schedule_tag_sweep(
    state: &WorkerState,
    cfg: &OnApproveConfig,
    team_id: &str,
    installation_id: u64,
    owner: &str,
    repo: &str,
    pr_number: u64,
    base_branch: &str,
    count: bool,
) -> bool {
    let marker = format!("TAGSWEEP#{owner}/{repo}");
    let now = chrono::Utc::now().timestamp() as u64;
    let window = cfg.tag_batch_minutes as u64 * 60;
    // TTL past the window so a crashed sweep can't wedge the marker for long.
    let ttl = now + window + 3_600;
    let sweep_id = ulid::Ulid::new().to_string();
    // The marker also describes the waiting release for the dashboard (when
    // it fires, how many merges it holds) and carries what "Release now"
    // needs to cut it early.
    let claimed = match state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", super::attr_s(team_id))
        .item("sk", super::attr_s(&marker))
        .item("ttl", super::attr_n(ttl))
        .item("sweep_id", super::attr_s(&sweep_id))
        .item("fires_at", super::attr_n(now + window))
        .item("base_branch", super::attr_s(base_branch))
        .item("pr_number", super::attr_n(pr_number))
        .item("merges", super::attr_n(1))
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
                // A sweep is already armed — fold in, and count the merge.
                if !count {
                    return false;
                }
                let _ = state
                    .dynamo
                    .update_item()
                    .table_name(&state.config.settings_table_name)
                    .key("pk", super::attr_s(team_id))
                    .key("sk", super::attr_s(&marker))
                    .update_expression("ADD merges :one")
                    .condition_expression("attribute_exists(pk)")
                    .expression_attribute_values(":one", super::attr_n(1))
                    .send()
                    .await;
                return false;
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
            delay_remaining_secs: window,
            sweep_id,
            manual: false,
            claimed: false,
            attempt: 0,
            ..Default::default()
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
    mut msg: crate::models::TagSweepMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Still inside the window — wait another hop (a single SQS delay is capped).
    if msg.delay_remaining_secs > SQS_MAX_DELAY_SECS {
        let mut next = msg.clone();
        next.delay_remaining_secs -= SQS_MAX_DELAY_SECS;
        enqueue_tag_sweep(state, &next).await;
        return Ok(());
    }

    // A push to the default branch: the merge joins the release window.
    if !msg.merged_sha.is_empty() {
        on_merged(state, &msg).await;
        return Ok(());
    }

    let owner = &msg.repo_owner;
    let repo = &msg.repo_name;
    let marker = format!("TAGSWEEP#{owner}/{repo}");

    // Open the NEXT window BEFORE reading HEAD/tagging: any merge that lands from
    // here on must arm a fresh sweep rather than be silently folded into this one
    // (which is about to finish). Removing the marker by this window's id is
    // also the claim: when "Release now" already released the window, the
    // delayed sweep finds it gone and stops.
    if !msg.claimed && !claim_window(state, &msg.team_id, &marker, &msg.sweep_id).await {
        info!(repo = %format!("{owner}/{repo}"), "Tag sweep: window already released — skipped");
        return Ok(());
    }

    let cfg = OnApproveConfig::load(state, &msg.team_id, owner, repo).await;
    if !cfg.auto_tag && !msg.manual {
        info!(repo = %format!("{owner}/{repo}"), "Tag sweep: auto_tag disabled mid-window — nothing to cut");
        return Ok(());
    }

    // A release freeze (agents paused, until a time or until lifted): the
    // release waits for it to end, then goes on by itself.
    if !msg.manual {
        let store = crate::agent::policy::store(state);
        let now_s = chrono::Utc::now().to_rfc3339();
        if let Ok(p) = store.load(&msg.team_id).await {
            if let Some(pause) = p.paused(&now_s) {
                let wait = freeze_wait(pause.until.as_deref(), chrono::Utc::now());
                info!(repo = %format!("{owner}/{repo}"), reason = %pause.reason, wait, "Tag sweep: agents paused — the release waits");
                wait_and_retry(state, &msg, wait, msg.ci_waits).await;
                return Ok(());
            }
        }
    }

    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;

    // "Release now" with nothing waiting doesn't know the branch yet.
    if msg.base_branch.is_empty() {
        match github
            .get_default_branch(&msg.repo_owner, &msg.repo_name)
            .await
        {
            Ok(b) => msg.base_branch = b,
            Err(e) => {
                retry_or_wait(
                    state,
                    &msg,
                    &*e,
                    &format!("Couldn't read the default branch: {e}"),
                )
                .await;
                return Ok(());
            }
        }
    }

    // Tag the branch's CURRENT head — every merge in the window is included.
    let head = match github.get_ref(owner, repo, &msg.base_branch).await {
        Ok(sha) => sha,
        Err(e) => {
            warn!(error = %e, base = %msg.base_branch, "Tag sweep: could not resolve base-branch head");
            retry_or_wait(
                state,
                &msg,
                &*e,
                &format!("Couldn't read the head of {}: {e}", msg.base_branch),
            )
            .await;
            return Ok(());
        }
    };

    // Never a red head: the release waits while its checks run, and a failed
    // check stops it (said on the Releases page) until a later merge or a
    // person's "Release now".
    if !msg.manual {
        let jobs = match github.list_ci_for_ref(owner, repo, &head).await {
            Ok(v) => v["check_runs"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(super::health_check::Job::from_check_run)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
            Err(e) => {
                retry_or_wait(
                    state,
                    &msg,
                    &*e,
                    &format!("Couldn't read the checks of {}: {e}", short_sha(&head)),
                )
                .await;
                return Ok(());
            }
        };
        match head_ci(&jobs) {
            HeadCi::Green => {}
            HeadCi::Running(names) if msg.ci_waits < MAX_CI_WAITS => {
                info!(repo = %format!("{owner}/{repo}"), head = %short_sha(&head), running = ?names, "Tag sweep: the head's checks are running — the release waits");
                wait_and_retry(state, &msg, CI_WAIT_SECS, msg.ci_waits + 1).await;
                return Ok(());
            }
            HeadCi::Running(names) => {
                record_failure(
                    state,
                    &msg,
                    &format!(
                        "The checks of {} on {} were still running after {} hours ({}). CoderHelm \
                         tags only a green head.",
                        short_sha(&head),
                        msg.base_branch,
                        MAX_CI_WAITS as u64 * CI_WAIT_SECS / 3600,
                        names.join(", ")
                    ),
                )
                .await;
                return Ok(());
            }
            HeadCi::Red(names) => {
                record_failure(
                    state,
                    &msg,
                    &format!(
                        "{} is red at {}: {} failed. CoderHelm never tags a red {}; the next green \
                         merge releases everything waiting.",
                        msg.base_branch,
                        short_sha(&head),
                        names.join(", "),
                        msg.base_branch
                    ),
                )
                .await;
                return Ok(());
            }
        }
    }

    let cut = cut_tag_if_new(&github, &cfg, owner, repo, &head).await;
    if cut.is_ok() {
        clear_failure(state, &msg.team_id, owner, repo).await;
    }
    match cut {
        Ok(Some(tag)) => {
            info!(%tag, repo = %format!("{owner}/{repo}"), "Batched release tag cut");
            super::release_notes::after_tag(
                state,
                &github,
                &msg.team_id,
                msg.installation_id,
                owner,
                repo,
                &msg.base_branch,
                &tag,
                &head,
            )
            .await;
            // Prod deploy fires off the tag → schedule the health guard now.
            if cfg.health_check && msg.pr_number > 0 {
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
            warn!(error = %e, repo = %format!("{owner}/{repo}"), "Tag sweep: create tag failed");
            retry_or_wait(
                state,
                &msg,
                &*e,
                &format!("GitHub refused the release tag: {e}"),
            )
            .await;
        }
    }
    Ok(())
}

/// A merge landed on the default branch (its push, whoever merged): when
/// the repository releases merged changes (`auto_tag`), it joins the waiting
/// release window, or opens one, so one tag covers everything merged so far.
/// A window of 0 minutes releases at once.
async fn on_merged(state: &WorkerState, msg: &crate::models::TagSweepMessage) {
    let (owner, repo) = (&msg.repo_owner, &msg.repo_name);
    let cfg = OnApproveConfig::load(state, &msg.team_id, owner, repo).await;
    if !cfg.auto_tag {
        return;
    }
    // The pull request the merge came from: where the post-merge check of
    // the release reports.
    let pr_number = match GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    ) {
        Ok(g) => g
            .pulls_for_commit(owner, repo, &msg.merged_sha)
            .await
            .ok()
            .and_then(|prs| {
                prs.iter()
                    .find(|p| p["merged_at"].is_string())
                    .and_then(|p| p["number"].as_u64())
            })
            .unwrap_or(0),
        Err(_) => 0,
    };
    info!(repo = %format!("{owner}/{repo}"), sha = %short_sha(&msg.merged_sha), pr_number, "Release: a merge joins the release window");
    if cfg.tag_batch_minutes == 0 {
        let now = crate::models::TagSweepMessage {
            team_id: msg.team_id.clone(),
            installation_id: msg.installation_id,
            repo_owner: owner.clone(),
            repo_name: repo.clone(),
            base_branch: msg.base_branch.clone(),
            pr_number,
            claimed: true,
            ..Default::default()
        };
        enqueue_tag_sweep(state, &now).await;
        return;
    }
    schedule_tag_sweep(
        state,
        &cfg,
        &msg.team_id,
        msg.installation_id,
        owner,
        repo,
        pr_number,
        &msg.base_branch,
        true,
    )
    .await;
}

fn short_sha(sha: &str) -> &str {
    sha.get(..7).unwrap_or(sha)
}

/// Seconds between looks at a head whose checks still run.
const CI_WAIT_SECS: u64 = 300;
/// Looks at a running head before the release says it can't go (3 hours).
const MAX_CI_WAITS: u32 = 36;

/// The branch head's checks, as the release reads them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeadCi {
    /// Every check finished and none failed (or the head has none).
    Green,
    /// These are still running.
    Running(Vec<String>),
    /// These failed.
    Red(Vec<String>),
}

/// Pure: whether a head may be tagged. A failed check makes it red whatever
/// else runs; a job held for an environment's approval is a deploy waiting
/// on a person, not a check of the code, so it never holds the release.
pub fn head_ci(jobs: &[super::health_check::Job]) -> HeadCi {
    let red: Vec<String> = jobs
        .iter()
        .filter(|j| j.failed())
        .map(|j| j.name.clone())
        .collect();
    if !red.is_empty() {
        return HeadCi::Red(red);
    }
    let running: Vec<String> = jobs
        .iter()
        .filter(|j| !j.done() && !j.awaiting_approval())
        .map(|j| j.name.clone())
        .collect();
    if running.is_empty() {
        HeadCi::Green
    } else {
        HeadCi::Running(running)
    }
}

/// Seconds a release waits on a freeze before looking again: until its end
/// when it has one (at most the queue's longest hop chain allows, an hour),
/// else 15 minutes.
pub fn freeze_wait(until: Option<&str>, now: chrono::DateTime<chrono::Utc>) -> u64 {
    until
        .and_then(|u| chrono::DateTime::parse_from_rfc3339(u).ok())
        .map(|u| (u.with_timezone(&chrono::Utc) - now).num_seconds().max(60) as u64 + 5)
        .unwrap_or(SQS_MAX_DELAY_SECS)
        .min(3_600)
}

/// The same release again after `wait_secs`, its window already handled,
/// counting no failed attempt.
async fn wait_and_retry(
    state: &WorkerState,
    msg: &crate::models::TagSweepMessage,
    wait_secs: u64,
    ci_waits: u32,
) {
    let mut next = msg.clone();
    next.claimed = true;
    next.ci_waits = ci_waits;
    next.delay_remaining_secs = wait_secs.max(1);
    if !enqueue_tag_sweep(state, &next).await {
        record_failure(
            state,
            msg,
            "The release could not be queued again; \"Release now\" cuts it.",
        )
        .await;
    }
}

/// A failed GitHub call: GitHub's budget being held or spent is a wait, never
/// a failure (the release goes on at the reset); anything else retries with
/// a backoff, then is recorded.
async fn retry_or_wait(
    state: &WorkerState,
    msg: &crate::models::TagSweepMessage,
    e: &(dyn std::error::Error + Send + Sync + 'static),
    error: &str,
) {
    if let Some(wait) = crate::clients::github::rate_limited(e) {
        info!(repo = %format!("{}/{}", msg.repo_owner, msg.repo_name), wait, "Tag sweep: GitHub's budget is held — the release waits for the reset");
        wait_and_retry(state, msg, wait.max(60) + 5, msg.ci_waits).await;
        return;
    }
    retry_or_record(state, msg, error).await;
}

// ── Catch-up: merged but never released ────────────────────────────────────

/// How often each repository's default branch is looked at for merges no
/// release tag holds yet.
pub const CATCH_UP_EVERY_SECS: i64 = 3_600;

/// Pure: whether a repository's catch-up is due. It is, the first time,
/// after CoderHelm itself was released (its version changed since the last
/// look), and an hour after the last look.
pub fn catch_up_due(last: Option<(i64, &str)>, now: i64, version: &str) -> bool {
    match last {
        None => true,
        Some((at, v)) => v != version || now - at >= CATCH_UP_EVERY_SECS,
    }
}

/// What the catch-up does with one repository, from what it read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatchUp {
    /// The head is in a release tag already.
    Released,
    /// The repository has never released by tag: nothing to catch up.
    NoReleases,
    /// A release window is already waiting, or the release is blocked
    /// (shown on the Releases page): left to it.
    Waiting,
    /// Merged and not released: a window opens.
    Open,
}

/// Pure: the catch-up's decision. `head_in_newest` is whether the newest
/// release tag contains the head (None when it wasn't compared).
pub fn catch_up(
    head: &str,
    releases: &[(String, String)],
    head_in_newest: Option<bool>,
    window_waiting: bool,
    blocked: bool,
) -> CatchUp {
    if releases.is_empty() {
        return CatchUp::NoReleases;
    }
    if releases.iter().any(|(_, sha)| sha == head) || head_in_newest == Some(true) {
        return CatchUp::Released;
    }
    if window_waiting || blocked {
        return CatchUp::Waiting;
    }
    CatchUp::Open
}

/// Every repository whose team releases merged changes (`auto_tag`), at
/// most hourly and once after each CoderHelm release: a default-branch head
/// no release tag holds (changes merged while nothing was listening) gets a
/// release window, which then tags it by the usual rules (a green head only,
/// a freeze waits, a held budget waits). Returns the repositories opened.
pub async fn release_catch_up(state: &WorkerState) -> Vec<String> {
    let version = std::env::var("CODERHELM_VERSION").unwrap_or_default();
    let now = chrono::Utc::now().timestamp();
    let mut opened = Vec::new();
    let mut installs: std::collections::HashMap<String, Option<u64>> = Default::default();
    let mut budget = crate::clients::github_budget::SweepGate::new(
        common::github_budget::caller::SWEEP_PR_WATCH,
    );
    for c in super::review_configs::all(state, "release-catch-up").await {
        let on = c
            .item
            .get("auto_tag")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false);
        let Some((owner, repo)) = c.repo.split_once('/') else {
            continue;
        };
        if !on {
            continue;
        }
        let marker = format!("RELEASECATCHUP#{}", c.repo);
        let last = state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", super::attr_s(&c.team_id))
            .key("sk", super::attr_s(&marker))
            .send()
            .await
            .ok()
            .and_then(|o| o.item)
            .map(|i| {
                let at = i
                    .get("at")
                    .and_then(|v| v.as_n().ok())
                    .and_then(|n| n.parse::<i64>().ok())
                    .unwrap_or(0);
                let v = i
                    .get("version")
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default();
                (at, v)
            });
        if !catch_up_due(last.as_ref().map(|(a, v)| (*a, v.as_str())), now, &version) {
            continue;
        }
        let iid = match installs.get(&c.team_id) {
            Some(v) => *v,
            None => {
                let v = super::review_reminder::team_installation_id(state, &c.team_id).await;
                installs.insert(c.team_id.clone(), v);
                v
            }
        };
        let Some(iid) = iid else {
            continue;
        };
        // A held budget: looked at again on a later sweep, never marked done.
        if !budget.allows(state, iid, 4).await {
            continue;
        }
        let Ok(github) = GitHubClient::new(
            &state.secrets.github_app_id,
            &state.secrets.github_private_key,
            iid,
            &state.http,
        ) else {
            continue;
        };
        let cfg = OnApproveConfig::load(state, &c.team_id, owner, repo).await;
        let read = async {
            let branch = github.get_default_branch(owner, repo).await?;
            let head = github.get_ref(owner, repo, &branch).await?;
            let tags = github.list_tags(owner, repo).await?;
            let releases = super::release_run::releases_newest_first(&tags, &cfg.tag_prefix);
            let head_in_newest = match releases.first() {
                Some((_, sha)) if sha != &head => Some(super::release_notes::tag_on_branch(
                    &github.compare_status(owner, repo, &head, sha).await?,
                )),
                _ => None,
            };
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                branch,
                head,
                releases,
                head_in_newest,
            ))
        };
        let (branch, head, releases, head_in_newest) = match read.await {
            Ok(r) => r,
            Err(e) => {
                warn!(repo = %c.repo, error = %e, "Release catch-up: could not read the branch and its tags");
                continue;
            }
        };
        let waiting = state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", super::attr_s(&c.team_id))
            .key("sk", super::attr_s(&format!("TAGSWEEP#{}", c.repo)))
            .send()
            .await
            .ok()
            .and_then(|o| o.item)
            .is_some();
        let blocked = release_blocked(state, &c.team_id, owner, repo)
            .await
            .is_some();
        let decided = catch_up(&head, &releases, head_in_newest, waiting, blocked);
        if decided == CatchUp::Open {
            let pr = github
                .pulls_for_commit(owner, repo, &head)
                .await
                .ok()
                .and_then(|prs| {
                    prs.iter()
                        .find(|p| p["merged_at"].is_string())
                        .and_then(|p| p["number"].as_u64())
                })
                .unwrap_or(0);
            info!(repo = %c.repo, head = %short_sha(&head), "Release catch-up: merged changes no tag holds — a release window opens");
            schedule_tag_sweep(
                state, &cfg, &c.team_id, iid, owner, repo, pr, &branch, false,
            )
            .await;
            opened.push(c.repo.clone());
        }
        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.settings_table_name)
            .item("pk", super::attr_s(&c.team_id))
            .item("sk", super::attr_s(&marker))
            .item("at", super::attr_n(now as u64))
            .item("version", super::attr_s(&version))
            .item("ttl", super::attr_n((now + 30 * 86_400) as u64))
            .send()
            .await;
    }
    opened
}

/// Failed cuts retried before the failure is shown on the Releases page.
const MAX_TAG_ATTEMPTS: u32 = 3;

/// Wait before retry `attempt` (1-based): 1, 5, then 15 minutes.
fn tag_retry_delay(attempt: u32) -> u64 {
    match attempt {
        0 | 1 => 60,
        2 => 300,
        _ => SQS_MAX_DELAY_SECS,
    }
}

/// Removes the repo's waiting-release marker when it still belongs to
/// `sweep_id` (any marker for a sweep without an id). False when another
/// sweep already released this window. A failed call claims anyway: a second
/// sweep at the same HEAD cuts nothing.
async fn claim_window(state: &WorkerState, team_id: &str, marker: &str, sweep_id: &str) -> bool {
    let mut del = state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", super::attr_s(team_id))
        .key("sk", super::attr_s(marker));
    if !sweep_id.is_empty() {
        del = del
            .condition_expression("sweep_id = :id")
            .expression_attribute_values(":id", super::attr_s(sweep_id));
    }
    match del.send().await {
        Ok(_) => true,
        Err(e) => !e
            .as_service_error()
            .map(|se| se.is_conditional_check_failed_exception())
            .unwrap_or(false),
    }
}

/// A cut that failed (GitHub down, a ruleset refusing the tag): try again
/// with a backoff, and after the last try record it for the Releases page,
/// where "Release now" retries it.
async fn retry_or_record(state: &WorkerState, msg: &crate::models::TagSweepMessage, error: &str) {
    let owner = &msg.repo_owner;
    let repo = &msg.repo_name;
    if msg.attempt + 1 < MAX_TAG_ATTEMPTS {
        let mut next = msg.clone();
        next.claimed = true;
        next.attempt += 1;
        next.delay_remaining_secs = tag_retry_delay(next.attempt);
        info!(repo = %format!("{owner}/{repo}"), attempt = next.attempt, "Tag sweep: retrying the release");
        if enqueue_tag_sweep(state, &next).await {
            return;
        }
    }
    record_failure(state, msg, error).await;
}

/// Records why the release can't go for the Releases page, where "Release
/// now" retries it; the release steps of the merged pull requests say it.
async fn record_failure(state: &WorkerState, msg: &crate::models::TagSweepMessage, error: &str) {
    let owner = &msg.repo_owner;
    let repo = &msg.repo_name;
    warn!(repo = %format!("{owner}/{repo}"), error, "Tag sweep: release blocked — recorded");
    let _ = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", super::attr_s(&msg.team_id))
        .item("sk", super::attr_s(&format!("TAGFAIL#{owner}/{repo}")))
        .item("error", super::attr_s(common::truncate_str(error, 1_000)))
        .item("failed_at", super::attr_s(&chrono::Utc::now().to_rfc3339()))
        .item("base_branch", super::attr_s(&msg.base_branch))
        .item("pr_number", super::attr_n(msg.pr_number))
        .item("attempts", super::attr_n(msg.attempt + 1))
        .send()
        .await;
}

/// Why the repository's release can't go, as the Releases page shows it;
/// None when nothing is recorded.
pub(crate) async fn release_blocked(
    state: &WorkerState,
    team_id: &str,
    owner: &str,
    repo: &str,
) -> Option<String> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", super::attr_s(team_id))
        .key("sk", super::attr_s(&format!("TAGFAIL#{owner}/{repo}")))
        .send()
        .await
        .ok()?
        .item?;
    item.get("error")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.to_string())
}

async fn clear_failure(state: &WorkerState, team_id: &str, owner: &str, repo: &str) {
    let _ = state
        .dynamo
        .delete_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", super::attr_s(team_id))
        .key("sk", super::attr_s(&format!("TAGFAIL#{owner}/{repo}")))
        .send()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failed_release_backs_off_then_is_recorded() {
        assert_eq!(tag_retry_delay(1), 60);
        assert_eq!(tag_retry_delay(2), 300);
        assert_eq!(tag_retry_delay(9), SQS_MAX_DELAY_SECS);
        assert_eq!(MAX_TAG_ATTEMPTS, 3);
    }

    #[test]
    fn sweeps_armed_before_ids_still_parse() {
        let m: crate::models::TagSweepMessage = serde_json::from_value(serde_json::json!({
            "team_id": "T", "installation_id": 1, "repo_owner": "o", "repo_name": "r",
            "base_branch": "main", "pr_number": 3, "delay_remaining_secs": 0
        }))
        .unwrap();
        assert!(m.sweep_id.is_empty() && !m.manual && !m.claimed && m.attempt == 0);
    }

    fn rv(login: &str, state: &str, commit: &str) -> serde_json::Value {
        serde_json::json!({"user": {"login": login}, "state": state, "commit_id": commit})
    }

    #[test]
    fn a_later_request_for_changes_blocks_an_earlier_approval() {
        let reviews = serde_json::json!([
            rv("alice", "APPROVED", "h1"),
            rv("bob", "APPROVED", "h1"),
            rv("alice", "COMMENTED", "h1"),
            rv("bob", "CHANGES_REQUESTED", "h1"),
        ]);
        let a = approval_state(&reviews, "h1");
        assert_eq!(a.approved_by, vec!["alice".to_string()]);
        assert_eq!(a.blocking_by, vec!["bob".to_string()]);
        assert!(!a.human_key_at_head());
    }

    #[test]
    fn a_dismissal_clears_a_verdict() {
        let reviews = serde_json::json!([
            rv("bob", "CHANGES_REQUESTED", "h1"),
            rv("bob", "DISMISSED", "h1"),
            rv("alice", "APPROVED", "h2"),
        ]);
        let a = approval_state(&reviews, "h2");
        assert!(a.blocking_by.is_empty());
        assert!(a.human_key_at_head());
        // A dismissed approval is no key either.
        let dismissed = serde_json::json!([
            rv("alice", "APPROVED", "h1"),
            rv("alice", "DISMISSED", "h1"),
        ]);
        assert!(!approval_state(&dismissed, "h1").human_key_at_head());
    }

    #[test]
    fn coderhelm_s_review_is_never_the_person_key() {
        let reviews = serde_json::json!([rv("coderhelm[bot]", "APPROVED", "h1")]);
        let a = approval_state(&reviews, "h1");
        assert!(!a.human_key_at_head());
        assert!(!a.bot_dismissed_at_head);
    }

    #[test]
    fn a_dismissed_coderhelm_review_withdraws_its_key_for_that_head_only() {
        let dismissed = serde_json::json!([
            rv("coderhelm[bot]", "DISMISSED", "h1"),
            rv("alice", "APPROVED", "h1"),
        ]);
        assert!(approval_state(&dismissed, "h1").bot_dismissed_at_head);
        assert!(!approval_state(&dismissed, "h2").bot_dismissed_at_head);
        let reposted = serde_json::json!([
            rv("coderhelm[bot]", "DISMISSED", "h1"),
            rv("coderhelm[bot]", "APPROVED", "h1"),
        ]);
        assert!(!approval_state(&reposted, "h1").bot_dismissed_at_head);
    }

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
