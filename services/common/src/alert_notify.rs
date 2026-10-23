//! Chat notifications for alerts.
//!
//! A team sets one alert channel (Teams, Slack or both) that every alert route
//! notifies; a route can instead use its own channels or notify nowhere. Each
//! alert posts a message, and when CoderHelm opens a pull request for an alert
//! a follow-up message links it. Webhooks can't edit a posted message, so the
//! follow-up is a new message that names the alert. Messages are built here as
//! [`Message`]s and rendered per channel by [`crate::notify`].

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;

use crate::mcp_credentials::Cipher;
use crate::notify::{store, Block, Button, Channels, Message, Size, Spacing, Text, Tone};

/// Settings-table sort key of the team's alert channel.
pub const TEAM_SETTINGS_SK: &str = "ALERT_NOTIFY";

/// Route notification modes.
pub const MODE_TEAM: &str = "team";
pub const MODE_CUSTOM: &str = "custom";
pub const MODE_OFF: &str = "off";

const MAX_FACTS: usize = 8;
const MAX_FACT_VALUE: usize = 300;
const MAX_SUMMARY: usize = 900;

/// Pure: a Teams webhook URL the API accepts (see [`crate::notify::teams`]).
pub fn valid_webhook_url(url: &str) -> bool {
    crate::notify::teams::valid_url(url)
}

/// Pure: the channels a route's alerts go to. Empty = nowhere.
pub fn pick(route_mode: &str, route: Channels, team: Channels, team_enabled: bool) -> Channels {
    match route_mode {
        MODE_OFF => Channels::default(),
        MODE_CUSTOM => route.usable(),
        // "team" and routes saved before notifications existed.
        _ if team_enabled => team.usable(),
        _ => Channels::default(),
    }
}

/// The channels the alerts on `topic_arn` notify, read from the route and the
/// team's settings and opened. Empty when notifications are off or unset.
pub async fn notify_target(
    dynamo: &DynamoClient,
    cipher: &dyn Cipher,
    table: &str,
    team_id: &str,
    topic_arn: &str,
) -> Channels {
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
    let Some(route) = route.ok().and_then(|r| r.item().cloned()) else {
        return Channels::default();
    };
    if route
        .get("team_id")
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        != Some(team_id)
    {
        return Channels::default();
    }
    let team = team
        .ok()
        .and_then(|t| t.item().cloned())
        .unwrap_or_default();
    let mode = route
        .get("notify_mode")
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default();
    let enabled = team
        .get("enabled")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false);
    let (route_channels, team_channels) = match mode.as_str() {
        MODE_OFF => return Channels::default(),
        MODE_CUSTOM => (
            store::open_all(cipher, team_id, &route).await,
            Channels::default(),
        ),
        _ if enabled => (
            Channels::default(),
            store::open_all(cipher, team_id, &team).await,
        ),
        _ => return Channels::default(),
    };
    pick(&mode, route_channels, team_channels, enabled)
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
    } else if kind == "recovery" {
        Severity::Resolved
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

fn banner(tone: Tone, text: &str) -> Block {
    Block::Banner {
        text: text.to_string(),
        tone,
        size: Size::Medium,
    }
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

/// Facts the card already shows in its title and headline, left out of the
/// details.
const FACTS_SHOWN_ELSEWHERE: [&str; 4] = ["Alarm", "State", "Description", "Region"];

/// Pure: the alert's description as one plain line. Markdown headings and
/// blank lines are dropped, and a bare heading gives way to the sentence after
/// it ("# Airflow Task Failure\nA task has failed…" reads as the second line).
fn headline(description: &str, summary: &str) -> String {
    let text = [description, summary]
        .iter()
        .flat_map(|t| t.lines())
        .map(|l| l.trim().trim_start_matches('#').trim())
        .filter(|l| !l.is_empty())
        .collect::<Vec<_>>();
    let line = match text.as_slice() {
        [heading, next, ..] if !heading.ends_with('.') && next.len() > heading.len() => next,
        [first, ..] => first,
        [] => return String::new(),
    };
    crate::truncate_str(line, 200).to_string()
}

/// Pure: a CloudWatch threshold reason as "Metric: **5** (alarms above 1)".
fn threshold_line(facts: &[(String, String)]) -> Option<String> {
    let get = |k: &str| facts.iter().find(|(f, _)| f == k).map(|(_, v)| v.as_str());
    let value = get("Reason")?
        .split_once('[')?
        .1
        .split([' ', ']', '('])
        .next()
        .filter(|v| !v.is_empty())?;
    let (op, threshold) = get("Threshold")?.split_once(' ')?;
    let op = match op {
        "GreaterThanThreshold" => "above",
        "GreaterThanOrEqualToThreshold" => "at or above",
        "LessThanThreshold" => "below",
        "LessThanOrEqualToThreshold" => "at or below",
        _ => return None,
    };
    let num = |n: &str| n.trim_end_matches(".0").to_string();
    let reading = format!("**{}** (alarms {op} {})", num(value), num(threshold));
    Some(match get("Metric") {
        Some(m) => format!("{}: {reading}", m.rsplit('/').next().unwrap_or(m).trim()),
        None => reading,
    })
}

/// Most buttons a card shows.
const MAX_ACTIONS: usize = 5;

/// Pure: the message for one alert. CloudWatch alarms are compact: a colored
/// title, when and where, one line on what happened, the reading against its
/// threshold, and what CoderHelm did, with the raw alarm fields behind
/// Details. Other alerts are the sender's own message, so their text and
/// fields stay visible. Fields that are links become buttons.
pub fn alert_message(a: &AlertCard) -> Message {
    let sev = severity(a.kind, a.title);
    let name = a
        .title
        .split_once(": ")
        .filter(|_| a.kind == "cloudwatch_alarm")
        .map(|(_, n)| n)
        .unwrap_or(a.title);
    let (tone, label) = match sev {
        Severity::Alarm => (Tone::Attention, format!("🔴 {name}")),
        Severity::Resolved if a.kind == "cloudwatch_alarm" => {
            (Tone::Good, format!("✅ Resolved · {name}"))
        }
        Severity::Resolved => (Tone::Good, format!("✅ {name}")),
        Severity::Info => (Tone::Neutral, format!("⚪ {}", a.title)),
        Severity::Warning => (Tone::Warning, format!("🟠 {name}")),
    };
    let (all_facts, summary) = facts_and_summary(a.body);
    let (links, facts): (Vec<_>, Vec<_>) = all_facts
        .into_iter()
        .partition(|(_, v)| v.starts_with("https://") && !v.contains(char::is_whitespace));
    let compact = a.kind == "cloudwatch_alarm";
    let description = facts
        .iter()
        .find(|(k, _)| k == "Description")
        .map(|(_, v)| v.as_str())
        .unwrap_or("");
    let line = headline(description, &summary);
    let mut blocks = vec![
        banner(tone, &label),
        Block::Text(
            Text::new(a.context)
                .subtle()
                .size(Size::Small)
                .spacing(Spacing::None),
        ),
    ];
    if compact {
        if !line.is_empty() && line != name {
            blocks.push(Block::Text(Text::new(&line).spacing(Spacing::Small)));
        }
    } else if !summary.is_empty() {
        blocks.push(Block::Text(Text::new(&summary).spacing(Spacing::Small)));
    }
    if !compact && !facts.is_empty() {
        blocks.push(Block::Facts {
            facts: facts.clone(),
            spacing: Spacing::Small,
        });
    }
    if let Some(t) = threshold_line(&facts) {
        blocks.push(Block::Text(Text::new(t).spacing(Spacing::Small)));
    }
    if !a.outcome.is_empty() {
        blocks.push(Block::Text(
            Text::new(format!("🤖 {}", a.outcome))
                .subtle()
                .size(Size::Small)
                .spacing(Spacing::Small),
        ));
    }
    let details: Vec<(String, String)> = facts
        .iter()
        .filter(|_| compact)
        .filter(|(k, _)| !FACTS_SHOWN_ELSEWHERE.contains(&k.as_str()))
        .cloned()
        .collect();
    let mut buttons = vec![Button::new("View alert", a.alert_url)];
    if let Some(url) = a.console_url {
        buttons.push(Button::new("Open in AWS", url));
    }
    // One place is kept for the Details toggle.
    for (k, url) in links
        .iter()
        .take(MAX_ACTIONS.saturating_sub(buttons.len() + 1))
    {
        buttons.push(Button::new(crate::truncate_str(k, 30), url));
    }
    if !details.is_empty() {
        blocks.push(Block::Details { facts: details });
    }
    Message {
        summary: label,
        blocks,
        buttons,
        full_width: true,
    }
}

/// Pure: the follow-up message when CoderHelm opens a pull request for an
/// alert.
pub fn fix_message(
    alert_title: &str,
    repo: &str,
    pr_number: u64,
    pr_title: &str,
    pr_url: &str,
    alert_url: &str,
) -> Message {
    let mut buttons = vec![Button::new("Open PR", pr_url)];
    if !alert_url.is_empty() {
        buttons.push(Button::new("View alert", alert_url));
    }
    let title = format!("🛠️ Fix proposed · PR #{pr_number}");
    Message {
        summary: format!("{title}: {}", crate::truncate_str(pr_title, 200)),
        blocks: vec![
            banner(Tone::Accent, &title),
            Block::Text(
                Text::new(format!(
                    "For alert: {}",
                    crate::truncate_str(alert_title, 200)
                ))
                .subtle()
                .spacing(Spacing::Small),
            ),
            Block::Text(
                Text::new(crate::truncate_str(pr_title, 300))
                    .bold()
                    .spacing(Spacing::Medium),
            ),
            Block::Facts {
                facts: vec![
                    ("Repository".into(), repo.into()),
                    (
                        "Status".into(),
                        "Waiting for review — nothing merges without a person's approval".into(),
                    ),
                ],
                spacing: Spacing::Default,
            },
        ],
        buttons,
        full_width: true,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notify::ChannelKind;

    fn teams(m: &Message) -> serde_json::Value {
        crate::notify::teams::render(m)
    }

    fn golden(name: &str) -> serde_json::Value {
        let path = format!(
            "{}/src/notify/golden/{name}.json",
            env!("CARGO_MANIFEST_DIR")
        );
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    const AIRFLOW: &str = "A task has failed to run. Please check MWAA for more information.\n\
        - Alarm: Airflow Task Failures\n\
        - State: ALARM (was INSUFFICIENT_DATA)\n\
        - Description: # Airflow Task Failure\n\
        - Reason: Threshold Crossed: 1 out of the last 1 datapoints [5.0 (12/10/26 16:54:00)] was greater than the threshold (1.0)\n\
        - Metric: AmazonMWAA / TaskInstanceFailures\n\
        - Dimensions: Task=All, Environment=prod-etl, DAG=All\n\
        - Statistic: AVERAGE\n\
        - Threshold: GreaterThanThreshold 1\n";

    /// Every alert message the golden files were captured from, by name.
    fn golden_messages() -> Vec<(&'static str, Message)> {
        vec![
            ("alert_cloudwatch", alert_message(&AlertCard {
                kind: "cloudwatch_alarm",
                title: "ALARM: Airflow Task Failures",
                body: AIRFLOW,
                outcome: "CoderHelm is working on a fix in acme/etl. A follow-up card will link the pull request.",
                alert_url: "https://app.example.com/alerts/detail?id=1",
                console_url: Some("https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#alarmsV2:alarm/x"),
                context: "us-east-1 · 123456789012 · 2026-10-12 17:09 UTC",
            })),
            ("alert_resolved", alert_message(&AlertCard {
                kind: "cloudwatch_alarm",
                title: "OK: Airflow Task Failures",
                body: AIRFLOW,
                outcome: "",
                alert_url: "https://app.example.com/alerts/detail?id=2",
                console_url: None,
                context: "us-east-1",
            })),
            ("alert_info", alert_message(&AlertCard {
                kind: "cloudwatch_alarm",
                title: "INSUFFICIENT_DATA: db-cpu",
                body: "- Alarm: db-cpu\n- Reason: no data",
                outcome: "",
                alert_url: "https://app.example.com/alerts/detail?id=3",
                console_url: None,
                context: "",
            })),
            ("alert_message", alert_message(&AlertCard {
                kind: "message",
                title: "Airflow failed: nightly_load.extract",
                body: "The extract task failed after 3 tries.\n- DAG: nightly_load\n- Task: extract\n- Error: KeyError: 'id'\n- Log: https://airflow.example.com/log?dag_id=nightly_load\n- Run: https://airflow.example.com/run/1\n- Grid: https://airflow.example.com/grid\n- More: https://airflow.example.com/more",
                outcome: "Notification only: no automatic fix for this alert.",
                alert_url: "https://app.example.com/alerts/detail?id=4",
                console_url: Some("https://console.example.com"),
                context: "us-east-1 · 2026-10-12 17:09 UTC",
            })),
            ("alert_recovery", alert_message(&AlertCard {
                kind: "recovery",
                title: "Recovered: nightly_load",
                body: "All tasks succeeded.",
                outcome: "Resolves the alert posted 2026-10-12 17:09 UTC.",
                alert_url: "https://app.example.com/alerts/detail?id=5",
                console_url: None,
                context: "us-east-1",
            })),
            ("fix_with_alert", fix_message(
                "ALARM: Airflow Task Failures",
                "acme/etl",
                42,
                "Retry the extract on KeyError",
                "https://github.com/acme/etl/pull/42",
                "https://app.example.com/alerts/detail?id=1",
            )),
            ("fix_without_alert", fix_message(
                "ALARM: x",
                "acme/etl",
                7,
                "Add fingerprint",
                "https://github.com/acme/etl/pull/7",
                "",
            )),
        ]
    }

    /// The Teams card is byte for byte the card posted before Slack existed
    /// (captured from the old card builders into `notify/golden`).
    #[test]
    fn teams_cards_are_unchanged() {
        for (name, m) in golden_messages() {
            assert_eq!(
                serde_json::to_string(&teams(&m)).unwrap(),
                serde_json::to_string(&golden(name)).unwrap(),
                "{name}"
            );
        }
    }

    #[test]
    fn slack_alert_messages_carry_the_same_content() {
        for (name, m) in golden_messages() {
            let v = ChannelKind::Slack.render(&m);
            let blocks = v["blocks"].as_array().unwrap();
            assert_eq!(blocks[0]["type"], "header", "{name}");
            assert!(!v["text"].as_str().unwrap().is_empty(), "{name}");
            let actions = blocks.last().unwrap();
            assert_eq!(actions["type"], "actions", "{name}");
            // Every link button, and no Details toggle (Slack shows them).
            let titles: Vec<&str> = actions["elements"]
                .as_array()
                .unwrap()
                .iter()
                .map(|b| b["text"]["text"].as_str().unwrap())
                .collect();
            let expected: Vec<&str> = m.buttons.iter().map(|b| b.title.as_str()).collect();
            assert_eq!(titles, expected, "{name}");
        }
        let (_, cw) = golden_messages().remove(0);
        let v = ChannelKind::Slack.render(&cw).to_string();
        // The threshold line, in Slack bold, and the hidden details shown.
        assert!(
            v.contains("TaskInstanceFailures: *5* (alarms above 1)"),
            "{v}"
        );
        assert!(v.contains("*Dimensions*\\nTask=All"), "{v}");
        assert_eq!(
            ChannelKind::Slack.render(&cw)["blocks"][0]["text"]["text"],
            "🔴 Airflow Task Failures"
        );
    }

    fn ch(teams: Option<&str>, slack: Option<&str>) -> Channels {
        Channels {
            teams: teams.map(String::from),
            slack: slack.map(String::from),
        }
    }

    #[test]
    fn route_overrides_team_channel() {
        let team = "https://prod-1.westus.logic.azure.com/workflows/a";
        let mine = "https://x.environment.api.powerplatform.com:443/powerautomate/b";
        let slack = "https://hooks.slack.com/services/T0/B0/abc";
        let t = || ch(Some(team), Some(slack));
        assert_eq!(pick("team", Channels::default(), t(), true), t());
        assert_eq!(pick("", Channels::default(), t(), true), t());
        assert!(pick("team", Channels::default(), t(), false).is_empty());
        assert_eq!(
            pick("custom", ch(Some(mine), None), t(), false),
            ch(Some(mine), None)
        );
        assert_eq!(
            pick("custom", ch(None, Some(slack)), t(), true),
            ch(None, Some(slack))
        );
        assert!(pick("custom", ch(Some("http://insecure"), None), t(), true).is_empty());
        assert!(pick("off", ch(Some(mine), None), t(), true).is_empty());
        // A URL on the wrong channel's host is dropped.
        assert_eq!(
            pick(
                "team",
                Channels::default(),
                ch(Some(slack), Some(slack)),
                true
            ),
            ch(None, Some(slack))
        );
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
        let card = teams(&alert_message(&AlertCard {
            kind: "cloudwatch_alarm",
            title: "ALARM: prod-db-cpu",
            body: "- Alarm: prod-db-cpu\n- Reason: Threshold crossed",
            outcome: "No action: no match term",
            alert_url: "https://app.example/alerts/detail?id=1",
            console_url: Some("https://console.example"),
            context: "us-east-1",
        }));
        let c = &card["attachments"][0]["content"];
        assert_eq!(c["version"], "1.5");
        assert_eq!(c["body"][0]["style"], "attention");
        assert_eq!(c["body"][0]["items"][0]["text"], "🔴 prod-db-cpu");
        // View alert, Open in AWS, Details.
        assert_eq!(c["actions"].as_array().unwrap().len(), 3);
        let resolved = teams(&alert_message(&AlertCard {
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
        }));
        assert_eq!(
            resolved["attachments"][0]["content"]["body"][0]["style"],
            "good"
        );
    }

    #[test]
    fn alarm_card_is_compact_with_details_hidden() {
        let body = "A task has failed to run. Please check MWAA for more information.\n\
            - Alarm: Airflow Task Failures\n\
            - State: ALARM (was INSUFFICIENT_DATA)\n\
            - Description: # Airflow Task Failure\n\
            - Reason: Threshold Crossed: 1 out of the last 1 datapoints [5.0 (12/10/26 16:54:00)] was greater than the threshold (1.0)\n\
            - Metric: AmazonMWAA / TaskInstanceFailures\n\
            - Dimensions: Task=All, Environment=prod-etl, DAG=All\n\
            - Statistic: AVERAGE\n\
            - Threshold: GreaterThanThreshold 1\n";
        let card = teams(&alert_message(&AlertCard {
            kind: "cloudwatch_alarm",
            title: "ALARM: Airflow Task Failures",
            body,
            outcome: "",
            alert_url: "u",
            console_url: None,
            context: "us-east-1 · 2026-10-12 17:09 UTC",
        }));
        let c = &card["attachments"][0]["content"];
        let texts: Vec<&str> = c["body"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|b| b["text"].as_str())
            .collect();
        assert_eq!(
            texts,
            vec![
                "us-east-1 · 2026-10-12 17:09 UTC",
                "A task has failed to run. Please check MWAA for more information.",
                "TaskInstanceFailures: **5** (alarms above 1)",
            ]
        );
        let details = c["body"].as_array().unwrap().last().unwrap();
        assert_eq!(details["isVisible"], false);
        let shown: Vec<&str> = details["items"][0]["facts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["title"].as_str().unwrap())
            .collect();
        assert_eq!(
            shown,
            vec!["Reason", "Metric", "Dimensions", "Statistic", "Threshold"]
        );
    }

    #[test]
    fn message_alerts_keep_their_fields_and_links_become_buttons() {
        let card = teams(&alert_message(&AlertCard {
            kind: "message",
            title: "Airflow failed: nightly_load.extract",
            body: "- DAG: nightly_load\n- Task: extract\n- Error: KeyError: 'id'\n\
                   - Log: https://airflow.example/log?dag_id=nightly_load",
            outcome: "",
            alert_url: "u",
            console_url: None,
            context: "us-east-1",
        }));
        let c = &card["attachments"][0]["content"];
        let facts: Vec<&str> = c["body"]
            .as_array()
            .unwrap()
            .iter()
            .find(|b| b["type"] == "FactSet")
            .unwrap()["facts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["title"].as_str().unwrap())
            .collect();
        assert_eq!(facts, vec!["DAG", "Task", "Error"]);
        let actions: Vec<&str> = c["actions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["title"].as_str().unwrap())
            .collect();
        assert_eq!(actions, vec!["View alert", "Log"]);
    }

    #[test]
    fn fix_card_links_the_pr() {
        let c = teams(&fix_message(
            "ALARM: x",
            "o/r",
            7,
            "Add fingerprint",
            "https://github.com/o/r/pull/7",
            "",
        ));
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
