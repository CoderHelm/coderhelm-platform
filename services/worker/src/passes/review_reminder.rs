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
//! only when it's never been posted, a new commit landed (head changed), or the
//! cooldown has elapsed while it's still waiting — never on every 20-min tick.

use crate::clients::github::GitHubClient;
use crate::passes::{attr_n, attr_s};
use crate::WorkerState;
use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;
use tracing::{info, warn};

const CONFIG_PREFIX: &str = "REVIEW_CONFIG#REPO#";
const DEFAULT_COOLDOWN_HOURS: i64 = 4;

struct RepoReminder {
    team_id: String,
    repo: String, // owner/name
    webhook_url: String,
    cooldown_secs: i64,
}

/// Pure dedup decision — unit-tested without any clock or network.
/// Notify when: never notified, the head changed (new commits), or the cooldown
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
        (Some(prev), Some(ts)) => prev != current_head || now_ts - ts >= cooldown_secs,
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

    for pr in arr {
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
        let card = build_card(&cfg.repo, pr_number, title, author, url, &reviewers);
        match post_teams(&state.http, &cfg.webhook_url, &card).await {
            Ok(()) => {
                save_reminder_state(state, &cfg.team_id, &cfg.repo, pr_number, head, now_ts).await;
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
    let _ = state
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
        .await;
}

/// Adaptive Card wrapped for a Teams (Power Automate Workflows) incoming webhook.
fn build_card(
    repo: &str,
    pr: u64,
    title: &str,
    author: &str,
    url: &str,
    reviewers: &[String],
) -> serde_json::Value {
    let who = reviewers
        .iter()
        .map(|r| format!("@{r}"))
        .collect::<Vec<_>>()
        .join(", ");
    serde_json::json!({
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    { "type": "TextBlock", "weight": "Bolder", "size": "Medium", "wrap": true,
                      "text": format!("🔍 Review needed: {title}") },
                    { "type": "TextBlock", "isSubtle": true, "wrap": true,
                      "text": format!("{repo} #{pr} · opened by {author}") },
                    { "type": "TextBlock", "wrap": true,
                      "text": format!("Waiting on: {who}") }
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
    fn renotifies_immediately_on_new_head() {
        // New commit within the cooldown still re-notifies.
        assert!(should_notify(
            Some("abc"),
            Some(1000),
            "def",
            1000 + 60,
            COOLDOWN
        ));
    }

    #[test]
    fn missing_timestamp_notifies() {
        assert!(should_notify(Some("abc"), None, "abc", 1000, COOLDOWN));
    }
}
