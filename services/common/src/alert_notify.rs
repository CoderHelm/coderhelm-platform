//! Teams notifications for alerts.
//!
//! A team sets one Teams channel (a Workflows "post to a channel when a webhook
//! request is received" URL) that every alert route notifies; a route can
//! instead use its own channel or notify nowhere. Each alert posts an Adaptive
//! Card, and when CoderHelm opens a pull request for an alert a follow-up card
//! links it. Workflows webhooks can't edit a posted card, so the follow-up is
//! a new card that names the alert.

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;
use serde_json::{json, Value};
use tracing::warn;

/// Settings-table sort key of the team's alert channel.
pub const TEAM_SETTINGS_SK: &str = "ALERT_NOTIFY";

/// Route notification modes.
pub const MODE_TEAM: &str = "team";
pub const MODE_CUSTOM: &str = "custom";
pub const MODE_OFF: &str = "off";

const MAX_FACTS: usize = 8;
const MAX_FACT_VALUE: usize = 300;
const MAX_SUMMARY: usize = 900;

/// Hosts that serve Teams webhooks: Power Automate / Workflows (both URL
/// generations) and legacy incoming webhooks.
const WEBHOOK_HOST_SUFFIXES: &[&str] = &[
    ".powerplatform.com",
    ".logic.azure.com",
    ".webhook.office.com",
];

/// Pure: a Teams webhook URL the API accepts — https on a Microsoft webhook
/// host, so the server never posts to arbitrary addresses.
pub fn valid_webhook_url(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("https://") else {
        return false;
    };
    if url.len() > 1000 || url.contains(char::is_whitespace) {
        return false;
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.contains('@') {
        return false;
    }
    let host = authority
        .split(':')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    WEBHOOK_HOST_SUFFIXES.iter().any(|sfx| host.ends_with(sfx))
}

/// Pure: the channel a route's alerts go to, if any.
pub fn pick_webhook(
    route_mode: &str,
    route_url: &str,
    team_url: &str,
    team_enabled: bool,
) -> Option<String> {
    let usable = |u: &str| valid_webhook_url(u).then(|| u.to_string());
    match route_mode {
        MODE_OFF => None,
        MODE_CUSTOM => usable(route_url),
        // "team" and routes saved before notifications existed.
        _ => team_enabled.then(|| usable(team_url)).flatten(),
    }
}

/// The channel the alerts on `topic_arn` notify, read from the route and the
/// team's settings. None when notifications are off or unset.
pub async fn notify_target(
    dynamo: &DynamoClient,
    table: &str,
    team_id: &str,
    topic_arn: &str,
) -> Option<String> {
    let get = |pk: String, sk: &str| {
        dynamo
            .get_item()
            .table_name(table)
            .key("pk", AttributeValue::S(pk))
            .key("sk", AttributeValue::S(sk.to_string()))
            .send()
    };
    let (route, team) = tokio::join!(
        get(format!("ALERTTOPIC#{topic_arn}"), "ROUTE"),
        get(team_id.to_string(), TEAM_SETTINGS_SK),
    );
    let route = route.ok()?.item().cloned()?;
    if route
        .get("team_id")
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        != Some(team_id)
    {
        return None;
    }
    let team = team
        .ok()
        .and_then(|t| t.item().cloned())
        .unwrap_or_default();
    let s = |m: &std::collections::HashMap<String, AttributeValue>, k: &str| {
        m.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let enabled = team
        .get("enabled")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false);
    pick_webhook(
        &s(&route, "notify_mode"),
        &s(&route, "teams_webhook_url"),
        &s(&team, "teams_webhook_url"),
        enabled,
    )
}

/// Post a card. Retries once when Teams throttles (429). Never panics.
pub async fn post(http: &reqwest::Client, url: &str, card: &Value) -> Result<(), String> {
    for attempt in 0..2 {
        let resp = http
            .post(url)
            .timeout(std::time::Duration::from_secs(6))
            .json(card)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        if status.as_u16() == 429 && attempt == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
            continue;
        }
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "teams webhook returned {status}: {}",
            crate::truncate_str(&body, 300)
        ));
    }
    Err("teams webhook throttled".into())
}

/// How an alert reads at a glance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Alarm,
    Resolved,
    Info,
    Warning,
}

/// Pure: severity from the normalized alert's kind and title. CloudWatch
/// titles are `<STATE>: <alarm name>`.
pub fn severity(kind: &str, title: &str) -> Severity {
    if kind == "cloudwatch_alarm" {
        match title.split(':').next().unwrap_or("") {
            "ALARM" => Severity::Alarm,
            "OK" => Severity::Resolved,
            _ => Severity::Info,
        }
    } else {
        Severity::Warning
    }
}

/// Pure: split a normalized alert body into `- Label: value` facts and the
/// remaining text.
pub fn facts_and_summary(body: &str) -> (Vec<(String, String)>, String) {
    let mut facts = Vec::new();
    let mut rest = Vec::new();
    for line in body.lines() {
        let fact = line
            .strip_prefix("- ")
            .and_then(|l| l.split_once(": "))
            .filter(|(k, v)| !k.is_empty() && k.len() <= 40 && !v.trim().is_empty());
        match fact {
            Some((k, v)) if facts.len() < MAX_FACTS => facts.push((
                k.to_string(),
                crate::truncate_str(v.trim(), MAX_FACT_VALUE).to_string(),
            )),
            Some(_) => {}
            None => rest.push(line),
        }
    }
    let summary = rest.join("\n").trim().to_string();
    (
        facts,
        crate::truncate_str(&summary, MAX_SUMMARY).to_string(),
    )
}

fn wrap(card: Value) -> Value {
    json!({
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "contentUrl": null,
            "content": card
        }]
    })
}

fn header(style: &str, color: &str, text: &str) -> Value {
    json!({
        "type": "Container",
        "bleed": true,
        "style": style,
        "items": [{
            "type": "TextBlock", "text": text, "weight": "Bolder",
            "size": "Large", "color": color, "wrap": true
        }]
    })
}

pub struct AlertCard<'a> {
    pub kind: &'a str,
    pub title: &'a str,
    pub body: &'a str,
    /// One line on what CoderHelm did ("Opening a pull request in owner/repo").
    pub outcome: &'a str,
    /// The alert's page in CoderHelm.
    pub alert_url: &'a str,
    /// The alarm in the AWS console, when known.
    pub console_url: Option<&'a str>,
    /// e.g. "us-east-1 · 123456789012 · 2026-10-10 04:00 UTC"
    pub context: &'a str,
}

/// Pure: the Teams card for one alert.
pub fn alert_card(a: &AlertCard) -> Value {
    let sev = severity(a.kind, a.title);
    let name = a
        .title
        .split_once(": ")
        .filter(|_| a.kind == "cloudwatch_alarm")
        .map(|(_, n)| n)
        .unwrap_or(a.title);
    let (style, color, label) = match sev {
        Severity::Alarm => ("attention", "Attention", format!("🔴 ALARM · {name}")),
        Severity::Resolved => ("good", "Good", format!("✅ RESOLVED · {name}")),
        Severity::Info => ("emphasis", "Default", format!("⚪ {}", a.title)),
        Severity::Warning => ("warning", "Warning", format!("🟠 {name}")),
    };
    let (facts, summary) = facts_and_summary(a.body);
    let mut body = vec![
        header(style, color, &label),
        json!({ "type": "TextBlock", "text": a.context, "isSubtle": true, "spacing": "Small", "wrap": true }),
    ];
    if !summary.is_empty() {
        body.push(
            json!({ "type": "TextBlock", "text": summary, "wrap": true, "spacing": "Medium" }),
        );
    }
    if !facts.is_empty() {
        body.push(json!({
            "type": "FactSet",
            "spacing": "Medium",
            "facts": facts.iter().map(|(k, v)| json!({ "title": k, "value": v })).collect::<Vec<_>>()
        }));
    }
    if !a.outcome.is_empty() {
        body.push(json!({
            "type": "TextBlock", "text": format!("🤖 {}", a.outcome),
            "isSubtle": true, "wrap": true, "spacing": "Medium"
        }));
    }
    let mut actions =
        vec![json!({ "type": "Action.OpenUrl", "title": "View alert", "url": a.alert_url })];
    if let Some(url) = a.console_url {
        actions.push(json!({ "type": "Action.OpenUrl", "title": "Open in AWS", "url": url }));
    }
    wrap(json!({
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "msteams": { "width": "Full" },
        "body": body,
        "actions": actions
    }))
}

/// Pure: the follow-up card when CoderHelm opens a pull request for an alert.
pub fn fix_card(
    alert_title: &str,
    repo: &str,
    pr_number: u64,
    pr_title: &str,
    pr_url: &str,
    alert_url: &str,
) -> Value {
    let mut actions = vec![json!({ "type": "Action.OpenUrl", "title": "Open PR", "url": pr_url })];
    if !alert_url.is_empty() {
        actions.push(json!({ "type": "Action.OpenUrl", "title": "View alert", "url": alert_url }));
    }
    wrap(json!({
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "msteams": { "width": "Full" },
        "body": [
            header("accent", "Accent", &format!("🛠️ Fix proposed · PR #{pr_number}")),
            { "type": "TextBlock", "text": format!("For alert: {}", crate::truncate_str(alert_title, 200)),
              "isSubtle": true, "spacing": "Small", "wrap": true },
            { "type": "TextBlock", "text": crate::truncate_str(pr_title, 300), "weight": "Bolder",
              "wrap": true, "spacing": "Medium" },
            { "type": "FactSet", "facts": [
                { "title": "Repository", "value": repo },
                { "title": "Status", "value": "Waiting for review — nothing merges without a person's approval" }
            ]}
        ],
        "actions": actions
    }))
}

/// Pure: the alarm's page in the CloudWatch console.
pub fn cloudwatch_console_url(region: &str, alarm_name: &str) -> Option<String> {
    let ok_region = !region.is_empty()
        && region
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if !ok_region || alarm_name.is_empty() {
        return None;
    }
    let enc: String = alarm_name
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect();
    Some(format!(
        "https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}#alarmsV2:alarm/{enc}"
    ))
}

/// Log-and-continue wrapper: a notification never fails the caller.
pub async fn send(http: &reqwest::Client, url: &str, card: &Value, what: &str) {
    if let Err(e) = post(http, url, card).await {
        warn!(what, error = %e, "Teams alert notification failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_overrides_team_channel() {
        let team = "https://prod-1.westus.logic.azure.com/workflows/a";
        let mine = "https://x.environment.api.powerplatform.com:443/powerautomate/b";
        assert_eq!(pick_webhook("team", "", team, true).as_deref(), Some(team));
        assert_eq!(pick_webhook("", "", team, true).as_deref(), Some(team));
        assert_eq!(pick_webhook("team", "", team, false), None);
        assert_eq!(
            pick_webhook("custom", mine, team, false).as_deref(),
            Some(mine)
        );
        assert_eq!(pick_webhook("custom", "http://insecure", team, true), None);
        assert_eq!(pick_webhook("off", mine, team, true), None);
    }

    #[test]
    fn severity_from_cloudwatch_state() {
        assert_eq!(
            severity("cloudwatch_alarm", "ALARM: db-cpu"),
            Severity::Alarm
        );
        assert_eq!(
            severity("cloudwatch_alarm", "OK: db-cpu"),
            Severity::Resolved
        );
        assert_eq!(
            severity("cloudwatch_alarm", "INSUFFICIENT_DATA: db-cpu"),
            Severity::Info
        );
        assert_eq!(
            severity("chatbot_custom", "New fingerprint"),
            Severity::Warning
        );
    }

    #[test]
    fn body_splits_into_facts_and_text() {
        let (facts, summary) =
            facts_and_summary("Something happened.\n\n- Alarm: db-cpu\n- Threshold: > 80\n");
        assert_eq!(
            facts,
            vec![
                ("Alarm".into(), "db-cpu".into()),
                ("Threshold".into(), "> 80".into())
            ]
        );
        assert_eq!(summary, "Something happened.");
    }

    #[test]
    fn alarm_card_has_red_header_and_buttons() {
        let card = alert_card(&AlertCard {
            kind: "cloudwatch_alarm",
            title: "ALARM: prod-db-cpu",
            body: "- Alarm: prod-db-cpu\n- Reason: Threshold crossed",
            outcome: "No action: no match term",
            alert_url: "https://app.example/alerts/detail?id=1",
            console_url: Some("https://console.example"),
            context: "us-east-1",
        });
        let c = &card["attachments"][0]["content"];
        assert_eq!(c["version"], "1.5");
        assert_eq!(c["body"][0]["style"], "attention");
        assert_eq!(c["body"][0]["items"][0]["text"], "🔴 ALARM · prod-db-cpu");
        assert_eq!(c["actions"].as_array().unwrap().len(), 2);
        let resolved = alert_card(&AlertCard {
            title: "OK: prod-db-cpu",
            console_url: None,
            ..AlertCard {
                kind: "cloudwatch_alarm",
                title: "",
                body: "",
                outcome: "",
                alert_url: "u",
                console_url: None,
                context: "",
            }
        });
        assert_eq!(
            resolved["attachments"][0]["content"]["body"][0]["style"],
            "good"
        );
    }

    #[test]
    fn fix_card_links_the_pr() {
        let c = fix_card(
            "ALARM: x",
            "o/r",
            7,
            "Add fingerprint",
            "https://github.com/o/r/pull/7",
            "",
        );
        let actions = c["attachments"][0]["content"]["actions"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0]["url"], "https://github.com/o/r/pull/7");
    }

    #[test]
    fn console_url_is_encoded_and_region_checked() {
        assert_eq!(
            cloudwatch_console_url("us-east-1", "prod db/cpu").unwrap(),
            "https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#alarmsV2:alarm/prod%20db%2Fcpu"
        );
        assert!(cloudwatch_console_url("us-east-1.evil.com/", "x").is_none());
    }

    #[test]
    fn only_microsoft_webhook_hosts_are_accepted() {
        assert!(valid_webhook_url("https://prod-12.westus.logic.azure.com:443/workflows/abc/triggers/manual/paths/invoke?sig=x"));
        assert!(valid_webhook_url("https://default123.08.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/abc"));
        assert!(valid_webhook_url(
            "https://acme.webhook.office.com/webhookb2/abc"
        ));
        assert!(!valid_webhook_url("http://prod-1.logic.azure.com/x"));
        assert!(!valid_webhook_url("https://evil.com/x"));
        assert!(!valid_webhook_url("https://logic.azure.com.evil.com/x"));
        assert!(!valid_webhook_url(
            "https://evil.com@prod-1.logic.azure.com/x"
        ));
        assert!(!valid_webhook_url("https://evil.com#.logic.azure.com"));
    }
}
