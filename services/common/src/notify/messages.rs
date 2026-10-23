//! Messages the worker sends that are not about an alert (alert messages are
//! in [`crate::alert_notify`]).

use super::{Block, Button, Message, Size, Spacing, Text, Tone};

/// Pure: "PR waiting on reviewers": a warning banner, the PR, who it waits on
/// and an Open PR button.
pub fn review_reminder(
    repo: &str,
    pr: u64,
    title: &str,
    author: &str,
    url: &str,
    age: &str,
    reviewers: &[String],
) -> Message {
    let who = if reviewers.is_empty() {
        "the team".to_string()
    } else {
        reviewers
            .iter()
            .map(|r| format!("@{r}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    Message {
        summary: format!("Review needed: {title} ({repo} #{pr})"),
        blocks: vec![
            Block::Banner {
                text: "⏳ Review needed".into(),
                tone: Tone::Warning,
                size: Size::Large,
            },
            Block::Group {
                blocks: vec![
                    Block::Text(Text::new(title).bold().size(Size::Large)),
                    Block::Text(
                        Text::new(format!("{repo} · #{pr}"))
                            .subtle()
                            .spacing(Spacing::None),
                    ),
                ],
                spacing: Spacing::Medium,
            },
            Block::Facts {
                facts: vec![
                    ("Author".into(), author.into()),
                    ("Open for".into(), age.into()),
                    ("Waiting on".into(), who),
                ],
                spacing: Spacing::Medium,
            },
        ],
        buttons: vec![Button::new("Open PR", url)],
        full_width: true,
    }
}

/// Pure: release notes for a tag failed after their retries.
pub fn release_notes_failed(owner: &str, repo: &str, tag: &str, error: &str) -> Message {
    let title = format!("⚠️ Release notes failed — {owner}/{repo} {tag}");
    Message {
        summary: title.clone(),
        blocks: vec![
            Block::Text(Text::new(title).bold().color(Tone::Attention)),
            Block::Text(Text::new(crate::truncate_str(error, 500))),
            Block::Text(
                Text::new("Send them again from CoderHelm → Releases once fixed.").subtle(),
            ),
        ],
        buttons: vec![],
        full_width: false,
    }
}

/// Pure: the message "Send test message" posts for a destination.
pub fn test_message(destination: &str, dashboard_url: &str) -> Message {
    Message {
        summary: "Test message from CoderHelm".into(),
        blocks: vec![
            Block::Banner {
                text: "✅ CoderHelm is connected".into(),
                tone: Tone::Good,
                size: Size::Medium,
            },
            Block::Text(
                Text::new(format!(
                    "This is a test message. {destination} will post to this channel."
                ))
                .spacing(Spacing::Small),
            ),
        ],
        buttons: vec![Button::new("Open CoderHelm", dashboard_url)],
        full_width: true,
    }
}

#[cfg(test)]
mod tests {
    use super::super::ChannelKind;
    use super::*;

    fn golden(name: &str) -> String {
        let path = format!(
            "{}/src/notify/golden/{name}.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        serde_json::to_string(&v).unwrap()
    }

    fn reminder(reviewers: &[String]) -> Message {
        review_reminder(
            "acme/web",
            12,
            "Add the billing page",
            "octo-dev",
            "https://github.com/acme/web/pull/12",
            "3 days",
            reviewers,
        )
    }

    #[test]
    fn the_review_reminder_teams_card_is_unchanged() {
        let m = reminder(&["ana".to_string(), "bo".to_string()]);
        let card = serde_json::to_string(&ChannelKind::Teams.render(&m)).unwrap();
        assert_eq!(card, golden("review_reminder"));
        let m = review_reminder(
            "acme/web",
            13,
            "Fix",
            "octo-dev",
            "https://github.com/acme/web/pull/13",
            "just now",
            &[],
        );
        let card = serde_json::to_string(&ChannelKind::Teams.render(&m)).unwrap();
        assert_eq!(card, golden("review_reminder_team"));
    }

    /// The card's content is unchanged; the envelope gains the
    /// `"contentUrl": null` every other card already sent.
    #[test]
    fn the_release_notes_teams_card_is_unchanged() {
        let m = release_notes_failed(
            "acme",
            "web",
            "v1.4.0",
            "Confluence returned 503: Service Unavailable",
        );
        let mut card = ChannelKind::Teams.render(&m);
        assert!(card["attachments"][0]["contentUrl"].is_null());
        card["attachments"][0]
            .as_object_mut()
            .unwrap()
            .remove("contentUrl");
        assert_eq!(
            serde_json::to_string(&card).unwrap(),
            golden("release_notes_failed")
        );
    }

    #[test]
    fn slack_review_reminder() {
        let v = ChannelKind::Slack.render(&reminder(&["ana".to_string()]));
        assert_eq!(
            v,
            serde_json::json!({
                "text": "Review needed: Add the billing page (acme/web #12)",
                "blocks": [
                    { "type": "header", "text": { "type": "plain_text", "text": "⏳ Review needed", "emoji": true } },
                    { "type": "section", "text": { "type": "mrkdwn", "text": "*Add the billing page*" } },
                    { "type": "context", "elements": [{ "type": "mrkdwn", "text": "acme/web · #12" }] },
                    { "type": "section", "fields": [
                        { "type": "mrkdwn", "text": "*Author*\nocto-dev" },
                        { "type": "mrkdwn", "text": "*Open for*\n3 days" },
                        { "type": "mrkdwn", "text": "*Waiting on*\n@ana" }
                    ]},
                    { "type": "actions", "elements": [{
                        "type": "button", "action_id": "link-0",
                        "text": { "type": "plain_text", "text": "Open PR", "emoji": true },
                        "url": "https://github.com/acme/web/pull/12"
                    }]}
                ]
            })
        );
    }

    #[test]
    fn slack_release_notes_failure() {
        let v = ChannelKind::Slack.render(&release_notes_failed(
            "acme",
            "web",
            "v1.4.0",
            "Confluence returned 503 <html>",
        ));
        let blocks = v["blocks"].as_array().unwrap();
        assert_eq!(blocks.len(), 3);
        assert_eq!(
            blocks[0]["text"]["text"],
            "*⚠️ Release notes failed — acme/web v1.4.0*"
        );
        assert_eq!(
            blocks[1]["text"]["text"],
            "Confluence returned 503 &lt;html&gt;"
        );
        assert_eq!(blocks[2]["type"], "context");
    }

    #[test]
    fn slack_test_message() {
        let v = ChannelKind::Slack.render(&test_message("Alerts", "https://app.example.com"));
        assert_eq!(v["text"], "Test message from CoderHelm");
        assert_eq!(v["blocks"][0]["type"], "header");
        assert_eq!(
            v["blocks"][2]["elements"][0]["url"],
            "https://app.example.com"
        );
        let t = ChannelKind::Teams.render(&test_message("Alerts", "https://app.example.com"));
        assert_eq!(
            t["attachments"][0]["content"]["actions"][0]["title"],
            "Open CoderHelm"
        );
    }
}
