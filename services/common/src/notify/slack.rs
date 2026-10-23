//! Slack: Block Kit for an incoming webhook (`https://hooks.slack.com/services/…`).
//!
//! The banner is a header block, lines are mrkdwn sections (secondary lines are
//! context blocks), facts are section fields, and link buttons are an actions
//! block. Slack has no hidden content, so Details are shown at the end. Slack's
//! limits are applied here: 50 blocks, 150 characters in a header, 3000 in a
//! section, 10 fields per section, 25 buttons, 75 characters on a button.

use super::{Block, Message, Size, Text};
use serde_json::{json, Value};

const HOST_PREFIX: &str = "https://hooks.slack.com/services/";
const MAX_BLOCKS: usize = 50;
const MAX_HEADER: usize = 150;
const MAX_SECTION: usize = 3000;
const MAX_FIELD: usize = 2000;
const MAX_FIELDS: usize = 10;
const MAX_CONTEXT: usize = 2000;
const MAX_BUTTONS: usize = 25;
const MAX_BUTTON_TEXT: usize = 75;
const MAX_URL: usize = 3000;

/// Pure: a Slack incoming webhook URL the API accepts:
/// `https://hooks.slack.com/services/<team>/<bot>/<secret>`, each part letters
/// and digits, so the server never posts anywhere but Slack's webhook host.
pub fn valid_url(url: &str) -> bool {
    if url.len() > 500 {
        return false;
    }
    let Some(rest) = url.strip_prefix(HOST_PREFIX) else {
        return false;
    };
    let parts: Vec<&str> = rest.split('/').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_alphanumeric()))
}

/// Pure: at most `max` characters (not bytes), with an ellipsis when cut.
fn clip(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

/// Pure: text as Slack mrkdwn. `&`, `<` and `>` are escaped (Slack reads
/// `<…>` as a link or mention), and `**bold**` becomes `*bold*`.
pub fn mrkdwn(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace("**", "*")
}

/// Pure: plain text (header and button labels): the markup removed.
fn plain(s: &str) -> String {
    s.replace("**", "")
}

fn text_block(t: &Text) -> Value {
    let body = if t.bold {
        format!("*{}*", mrkdwn(&t.text))
    } else {
        mrkdwn(&t.text)
    };
    if t.subtle || t.size == Size::Small {
        json!({
            "type": "context",
            "elements": [{ "type": "mrkdwn", "text": clip(&body, MAX_CONTEXT) }]
        })
    } else {
        json!({
            "type": "section",
            "text": { "type": "mrkdwn", "text": clip(&body, MAX_SECTION) }
        })
    }
}

fn fields(facts: &[(String, String)]) -> Vec<Value> {
    facts
        .chunks(MAX_FIELDS)
        .map(|chunk| {
            json!({
                "type": "section",
                "fields": chunk.iter().map(|(k, v)| json!({
                    "type": "mrkdwn",
                    "text": clip(&format!("*{}*\n{}", mrkdwn(k), mrkdwn(v)), MAX_FIELD),
                })).collect::<Vec<_>>()
            })
        })
        .collect()
}

fn push(out: &mut Vec<Value>, details: &mut Vec<Value>, b: &Block) {
    match b {
        Block::Banner { text, .. } => out.push(json!({
            "type": "header",
            "text": { "type": "plain_text", "text": clip(&plain(text), MAX_HEADER), "emoji": true }
        })),
        Block::Text(t) if t.text.trim().is_empty() => {}
        Block::Text(t) => out.push(text_block(t)),
        Block::Facts { facts, .. } => out.extend(fields(facts)),
        Block::Group { blocks, .. } => {
            for b in blocks {
                push(out, details, b);
            }
        }
        Block::Details { facts } if !facts.is_empty() => {
            details.push(json!({
                "type": "context",
                "elements": [{ "type": "mrkdwn", "text": "Details" }]
            }));
            details.extend(fields(facts));
        }
        Block::Details { .. } => {}
    }
}

/// Pure: the Block Kit payload for one message.
pub fn render(m: &Message) -> Value {
    let mut blocks = Vec::new();
    let mut details = Vec::new();
    for b in &m.blocks {
        push(&mut blocks, &mut details, b);
    }
    if !details.is_empty() {
        blocks.push(json!({ "type": "divider" }));
        blocks.extend(details);
    }
    let buttons: Vec<Value> = m
        .buttons
        .iter()
        .filter(|b| b.url.len() <= MAX_URL)
        .take(MAX_BUTTONS)
        .enumerate()
        .map(|(i, b)| {
            json!({
                "type": "button",
                "action_id": format!("link-{i}"),
                "text": { "type": "plain_text", "text": clip(&plain(&b.title), MAX_BUTTON_TEXT), "emoji": true },
                "url": b.url,
            })
        })
        .collect();
    // Room for the buttons: they are what a reader acts on.
    let room = MAX_BLOCKS - usize::from(!buttons.is_empty());
    blocks.truncate(room);
    if !buttons.is_empty() {
        blocks.push(json!({ "type": "actions", "elements": buttons }));
    }
    let summary = if m.summary.trim().is_empty() {
        m.blocks
            .iter()
            .find_map(|b| match b {
                Block::Banner { text, .. } => Some(plain(text)),
                _ => None,
            })
            .unwrap_or_default()
    } else {
        plain(&m.summary)
    };
    json!({ "text": clip(&summary, MAX_SECTION), "blocks": blocks })
}

#[cfg(test)]
mod tests {
    use super::super::{Button, Spacing, Tone};
    use super::*;

    #[test]
    fn only_slack_incoming_webhooks_are_accepted() {
        assert!(valid_url(
            "https://hooks.slack.com/services/T0EXAMPLE/B0EXAMPLE/abcdefghijklmnop"
        ));
        assert!(!valid_url(
            "http://hooks.slack.com/services/T0EXAMPLE/B0EXAMPLE/abcd"
        ));
        assert!(!valid_url(
            "https://hooks.slack.com.evil.com/services/T0/B0/X"
        ));
        assert!(!valid_url(
            "https://evil.com/hooks.slack.com/services/T0/B0/X"
        ));
        assert!(!valid_url("https://hooks.slack.com/services/T0/B0"));
        assert!(!valid_url("https://hooks.slack.com/services/T0/B0/X/extra"));
        assert!(!valid_url("https://hooks.slack.com/services/T0/B0/X?y=1"));
        assert!(!valid_url(
            "https://hooks.slack.com/services/@evil.com/B0/X"
        ));
        assert!(!valid_url("https://hooks.slack.com/services/T0/B0/X Y"));
        // Workflow triggers take variables, not Block Kit.
        assert!(!valid_url("https://hooks.slack.com/triggers/T0/1/abc"));
        assert!(!valid_url("https://acme.webhook.office.com/webhookb2/abc"));
        assert!(!valid_url(""));
    }

    #[test]
    fn markup_is_escaped_and_bold_converted() {
        assert_eq!(
            mrkdwn("p95 **5** > 1 & <@here>"),
            "p95 *5* &gt; 1 &amp; &lt;@here&gt;"
        );
    }

    #[test]
    fn limits_hold() {
        let long = "x".repeat(400);
        let m = Message {
            summary: String::new(),
            blocks: std::iter::once(Block::Banner {
                text: long.clone(),
                tone: Tone::Attention,
                size: Size::Medium,
            })
            .chain((0..80).map(|i| Block::Text(Text::new(format!("line {i}")))))
            .chain(std::iter::once(Block::Facts {
                facts: (0..23).map(|i| (format!("k{i}"), "v".into())).collect(),
                spacing: Spacing::Default,
            }))
            .collect(),
            buttons: (0..30)
                .map(|i| Button::new(format!("b{i}"), "https://x.example.com"))
                .collect(),
            full_width: true,
        };
        let v = render(&m);
        let blocks = v["blocks"].as_array().unwrap();
        assert_eq!(blocks.len(), MAX_BLOCKS);
        assert_eq!(
            blocks[0]["text"]["text"].as_str().unwrap().chars().count(),
            MAX_HEADER
        );
        let actions = blocks.last().unwrap();
        assert_eq!(actions["type"], "actions");
        assert_eq!(actions["elements"].as_array().unwrap().len(), MAX_BUTTONS);
        // The summary falls back to the banner.
        assert!(v["text"].as_str().unwrap().starts_with("xxx"));
    }

    #[test]
    fn facts_are_split_ten_to_a_section() {
        let f: Vec<(String, String)> = (0..12).map(|i| (format!("k{i}"), "v".into())).collect();
        let out = fields(&f);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["fields"].as_array().unwrap().len(), 10);
        assert_eq!(out[0]["fields"][0]["text"], "*k0*\nv");
    }
}
