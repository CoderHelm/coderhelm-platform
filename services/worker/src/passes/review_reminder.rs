//! Periodic review-reminder sweep. An EventBridge rule (every ~20 min) drops a
//! `{"type":"review_reminder"}` message on the ticket queue; this pass scans
//! every repo that has reminders configured, finds open PRs still waiting on a
//! reviewer, and posts a card to that repo's Microsoft Teams webhook.
//!
//! "People who need to review" = the PR's `requested_reviewers` (GitHub removes
//! a reviewer from that list once they submit a review), so a non-empty list is
//! a precise, no-extra-API signal that the PR is still waiting.
//!
//! Dedup (the "don't overpost, but don't post only once" requirement): per-PR
//! state records the last head SHA and last-notified time. A PR is re-posted
//! only when it's never been posted, a new commit landed (head changed) AND the
//! head-change floor has elapsed, or the cooldown has elapsed while it's still
//! waiting — never on every 20-min tick. The floor exists because an active PR
//! (a bot PR in a CI-fix loop, a human pushing review fixes) changes head every
//! few minutes; without it every sweep tick sees "new commits" and the channel
//! gets a card per tick. A sweep also posts at most MAX_CARDS_PER_SWEEP cards
//! per repo so first-enable on a backlog of waiting PRs can't flood the channel.

use crate::clients::github::GitHubClient;
use crate::passes::{attr_n, attr_s};
use crate::WorkerState;
use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;
use tracing::{info, warn};

const CONFIG_PREFIX: &str = "REVIEW_CONFIG#REPO#";
const DEFAULT_COOLDOWN_HOURS: i64 = 4;
/// Minimum quiet time before a head change may re-nudge. Head churn on an
/// active PR must not beat this; only the full cooldown re-nudges otherwise.
const HEAD_CHANGE_FLOOR_SECS: i64 = 3600;
/// Upper bound on cards posted to one repo's channel in a single sweep.
const MAX_CARDS_PER_SWEEP: usize = 5;

struct RepoReminder {
    team_id: String,
    repo: String, // owner/name
    webhook_url: String,
    cooldown_secs: i64,
}

/// Pure dedup decision — unit-tested without any clock or network.
/// Notify when: never notified; the head changed (new commits) and at least
/// the head-change floor has passed since the last card; or the cooldown
/// elapsed while the PR is still waiting.
pub fn should_notify(
    last_head: Option<&str>,
    last_notified_ts: Option<i64>,
    current_head: &str,
    now_ts: i64,
    cooldown_secs: i64,
) -> bool {
    match (last_head, last_notified_ts) {
        (None, _) | (_, None) => true,
        (Some(prev), Some(ts)) => {
            let elapsed = now_ts - ts;
            (prev != current_head && elapsed >= HEAD_CHANGE_FLOOR_SECS) || elapsed >= cooldown_secs
        }
    }
}

pub async fn run(state: &WorkerState) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let configs = load_reminder_configs(state).await;
    if configs.is_empty() {
        info!("Review-reminder sweep: no repos with reminders configured");
        return Ok(());
    }
    info!(
        repos = configs.len(),
        "Review-reminder sweep: scanning configured repos"
    );

    let mut install_cache: HashMap<String, u64> = HashMap::new();
    for cfg in configs {
        let iid = match install_cache.get(&cfg.team_id) {
            Some(v) => *v,
            None => match team_installation_id(state, &cfg.team_id).await {
                Some(v) => {
                    install_cache.insert(cfg.team_id.clone(), v);
                    v
                }
                None => {
                    warn!(team = %cfg.team_id, "No GitHub installation for team — skipping reminders");
                    continue;
                }
            },
        };
        if let Err(e) = sweep_repo(state, &cfg, iid).await {
            warn!(repo = %cfg.repo, error = %e, "Reminder sweep failed for repo");
        }
    }
    Ok(())
}

/// Scan the settings table for reviewer configs with reminders enabled and a
/// non-empty Teams webhook URL. Paginated; bounded so a huge table can't run the
/// Lambda forever (logs when truncated rather than silently capping).
async fn load_reminder_configs(state: &WorkerState) -> Vec<RepoReminder> {
    let mut out = vec![];
    let mut last_key: Option<HashMap<String, AttributeValue>> = None;
    for page in 0..50 {
        let mut req = state
            .dynamo
            .scan()
            .table_name(&state.config.settings_table_name)
            .filter_expression("begins_with(sk, :pfx) AND reminders_enabled = :t")
            .expression_attribute_values(":pfx", attr_s(CONFIG_PREFIX))
            .expression_attribute_values(":t", AttributeValue::Bool(true));
        if let Some(k) = last_key.take() {
            req = req.set_exclusive_start_key(Some(k));
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Reminder config scan failed");
                break;
            }
        };
        for item in resp.items() {
            let webhook_url = item
                .get("teams_webhook_url")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default();
            if webhook_url.trim().is_empty() {
                continue;
            }
            let team_id = item
                .get("pk")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default();
            let repo = item
                .get("sk")
                .and_then(|v| v.as_s().ok())
                .and_then(|sk| sk.strip_prefix(CONFIG_PREFIX).map(|s| s.to_string()))
                .unwrap_or_default();
            if team_id.is_empty() || repo.is_empty() {
                continue;
            }
            let cooldown_hours = item
                .get("reminder_cooldown_hours")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<i64>().ok())
                .filter(|h| *h > 0)
                .unwrap_or(DEFAULT_COOLDOWN_HOURS);
            out.push(RepoReminder {
                team_id,
                repo,
                webhook_url,
                cooldown_secs: cooldown_hours * 3600,
            });
        }
        match resp.last_evaluated_key() {
            Some(k) => last_key = Some(k.clone()),
            None => return out,
        }
        if page == 49 {
            warn!("Reminder config scan hit page cap (50) — some repos may be skipped this tick");
        }
    }
    out
}

/// A team's GitHub installation id, from the teams table META record.
async fn team_installation_id(state: &WorkerState, team_id: &str) -> Option<u64> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.teams_table_name)
        .key("team_id", attr_s(team_id))
        .key("sk", attr_s("META"))
        .send()
        .await
        .ok()?
        .item()?
        .get("github_installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok());
    item
}

async fn sweep_repo(
    state: &WorkerState,
    cfg: &RepoReminder,
    installation_id: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (owner, name) = cfg.repo.split_once('/').ok_or("bad repo slug")?;
    let github = GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        installation_id,
        &state.http,
    )?;

    let pulls = github.list_open_pulls(owner, name).await?;
    let Some(arr) = pulls.as_array() else {
        return Ok(());
    };
    let now_ts = chrono::Utc::now().timestamp();
    let mut posted = 0usize;

    for pr in arr {
        if posted >= MAX_CARDS_PER_SWEEP {
            info!(repo = %cfg.repo, cap = MAX_CARDS_PER_SWEEP, "Reminder card cap reached this sweep — remaining PRs wait for the next tick");
            break;
        }
        if pr["draft"].as_bool().unwrap_or(false) {
            continue;
        }
        let reviewers: Vec<String> = pr["requested_reviewers"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|r| r["login"].as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        // Only nudge PRs still waiting on a specific reviewer.
        if reviewers.is_empty() {
            continue;
        }
        let pr_number = pr["number"].as_u64().unwrap_or(0);
        let head = pr["head"]["sha"].as_str().unwrap_or("");
        if pr_number == 0 || head.is_empty() {
            continue;
        }

        let (last_head, last_ts) =
            load_reminder_state(state, &cfg.team_id, &cfg.repo, pr_number).await;
        if !should_notify(
            last_head.as_deref(),
            last_ts,
            head,
            now_ts,
            cfg.cooldown_secs,
        ) {
            continue;
        }

        let title = pr["title"].as_str().unwrap_or("(untitled)");
        let author = pr["user"]["login"].as_str().unwrap_or("");
        let url = pr["html_url"].as_str().unwrap_or("");
        let age = human_age(pr["created_at"].as_str().unwrap_or(""), now_ts);
        let card = build_card(&cfg.repo, pr_number, title, author, url, &age, &reviewers);
        match post_teams(&state.http, &cfg.webhook_url, &card).await {
            Ok(()) => {
                save_reminder_state(state, &cfg.team_id, &cfg.repo, pr_number, head, now_ts).await;
                posted += 1;
                info!(repo = %cfg.repo, pr = pr_number, reviewers = reviewers.len(), "Posted Teams review reminder");
            }
            Err(e) => warn!(repo = %cfg.repo, pr = pr_number, error = %e, "Teams post failed"),
        }
    }
    Ok(())
}

fn reminder_sk(repo: &str, pr: u64) -> String {
    format!("REVIEW_REMINDER#{repo}#{pr}")
}

async fn load_reminder_state(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    pr: u64,
) -> (Option<String>, Option<i64>) {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&reminder_sk(repo, pr)))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned());
    let Some(item) = item else {
        return (None, None);
    };
    let head = item.get("last_head").and_then(|v| v.as_s().ok()).cloned();
    let ts = item
        .get("last_notified_ts")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<i64>().ok());
    (head, ts)
}

async fn save_reminder_state(
    state: &WorkerState,
    team_id: &str,
    repo: &str,
    pr: u64,
    head: &str,
    now_ts: i64,
) {
    // A failed save means this PR re-posts next tick — that must be visible,
    // not swallowed, or the channel spams silently until someone reads Dynamo.
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&reminder_sk(repo, pr)))
        .update_expression("SET last_head = :h, last_notified_ts = :t ADD notify_count :one")
        .expression_attribute_values(":h", attr_s(head))
        .expression_attribute_values(":t", attr_n(now_ts))
        .expression_attribute_values(":one", attr_n(1))
        .send()
        .await
    {
        warn!(repo, pr, error = %e, "Reminder dedup-state save FAILED — this PR will re-post next tick");
    }
}

/// Human-readable PR age (e.g. "3 days", "5 hours", "just now") from an RFC3339
/// created_at and the current epoch seconds.
fn human_age(created_at: &str, now_ts: i64) -> String {
    let Ok(dt) = chrono::DateTime::parse_from_rfc3339(created_at) else {
        return "unknown".to_string();
    };
    let secs = (now_ts - dt.timestamp()).max(0);
    if secs < 3600 {
        "just now".to_string()
    } else if secs < 86_400 {
        let h = secs / 3600;
        format!("{h} hour{}", if h == 1 { "" } else { "s" })
    } else {
        let d = secs / 86_400;
        format!("{d} day{}", if d == 1 { "" } else { "s" })
    }
}

/// Polished Adaptive Card (v1.5) wrapped for a Teams Workflows incoming webhook:
/// a colored, bled header + FactSet metadata + an Open-PR button. Kept to the
/// v1.5 primitives Teams renders reliably (Container style tint, FactSet, emoji).
fn build_card(
    repo: &str,
    pr: u64,
    title: &str,
    author: &str,
    url: &str,
    age: &str,
    reviewers: &[String],
) -> serde_json::Value {
    let who = if reviewers.is_empty() {
        "the team".to_string()
    } else {
        reviewers
            .iter()
            .map(|r| format!("@{r}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    serde_json::json!({
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "contentUrl": null,
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.5",
                "msteams": { "width": "Full" },
                "body": [
                    {
                        "type": "Container",
                        "bleed": true,
                        "style": "warning",
                        "items": [
                            { "type": "TextBlock", "text": "⏳ Review needed", "weight": "Bolder",
                              "size": "Large", "color": "Warning", "wrap": true }
                        ]
                    },
                    {
                        "type": "Container",
                        "spacing": "Medium",
                        "items": [
                            { "type": "TextBlock", "text": title, "weight": "Bolder", "size": "Large", "wrap": true },
                            { "type": "TextBlock", "text": format!("{repo} · #{pr}"), "isSubtle": true, "spacing": "None", "wrap": true }
                        ]
                    },
                    {
                        "type": "FactSet",
                        "spacing": "Medium",
                        "facts": [
                            { "title": "Author", "value": author },
                            { "title": "Open for", "value": age },
                            { "title": "Waiting on", "value": who }
                        ]
                    }
                ],
                "actions": [
                    { "type": "Action.OpenUrl", "title": "Open PR", "url": url }
                ]
            }
        }]
    })
}

async fn post_teams(
    http: &reqwest::Client,
    webhook_url: &str,
    card: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let resp = http.post(webhook_url).json(card).send().await?;
    let status = resp.status();
    if status.is_success() {
        Ok(())
    } else {
        let body = resp.text().await.unwrap_or_default();
        Err(format!("teams webhook returned {status}: {body}").into())
    }
}

#[cfg(test)]
mod tests {
    use super::should_notify;

    const COOLDOWN: i64 = 4 * 3600;

    #[test]
    fn notifies_when_never_seen() {
        assert!(should_notify(None, None, "abc", 1000, COOLDOWN));
    }

    #[test]
    fn suppresses_within_cooldown_same_head() {
        // Notified 1h ago, same head, 4h cooldown → stay quiet.
        assert!(!should_notify(
            Some("abc"),
            Some(1000),
            "abc",
            1000 + 3600,
            COOLDOWN
        ));
    }

    #[test]
    fn renotifies_after_cooldown() {
        assert!(should_notify(
            Some("abc"),
            Some(1000),
            "abc",
            1000 + COOLDOWN,
            COOLDOWN
        ));
    }

    #[test]
    fn new_head_within_floor_stays_quiet() {
        // Active PR pushing commits every few minutes: a head change 60s after
        // the last card must NOT re-notify — this was the every-tick channel
        // spam. Only after the floor does a new head count.
        assert!(!should_notify(
            Some("abc"),
            Some(1000),
            "def",
            1000 + 60,
            COOLDOWN
        ));
    }

    #[test]
    fn new_head_after_floor_renotifies() {
        // Head changed and the 1h floor has passed (still inside the 4h
        // cooldown) → the new-commit re-nudge fires.
        assert!(should_notify(
            Some("abc"),
            Some(1000),
            "def",
            1000 + super::HEAD_CHANGE_FLOOR_SECS,
            COOLDOWN
        ));
    }

    #[test]
    fn same_head_within_floor_stays_quiet() {
        assert!(!should_notify(
            Some("abc"),
            Some(1000),
            "abc",
            1000 + 60,
            COOLDOWN
        ));
    }

    #[test]
    fn missing_timestamp_notifies() {
        assert!(should_notify(Some("abc"), None, "abc", 1000, COOLDOWN));
    }

    #[test]
    fn human_age_buckets() {
        // created_at = epoch 0; now = 3 days later.
        let three_days = 3 * 86_400;
        assert_eq!(
            super::human_age("1970-01-01T00:00:00Z", three_days),
            "3 days"
        );
        assert_eq!(
            super::human_age("1970-01-01T00:00:00Z", 2 * 3600),
            "2 hours"
        );
        assert_eq!(super::human_age("1970-01-01T00:00:00Z", 600), "just now");
        assert_eq!(super::human_age("garbage", 600), "unknown");
    }
}
