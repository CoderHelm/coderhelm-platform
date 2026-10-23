//! Microsoft Teams: an Adaptive Card (v1.5) wrapped for a Workflows or
//! incoming webhook. Kept to the primitives Teams renders reliably: a tinted,
//! bled Container for the banner, TextBlocks, FactSets, a hidden Details
//! Container with a toggle, and OpenUrl buttons.

use super::{Block, Message, Size, Spacing, Text, Tone};
use serde_json::{json, Map, Value};

/// Hosts that serve Teams webhooks: Power Automate / Workflows (both URL
/// generations) and legacy incoming webhooks.
const WEBHOOK_HOST_SUFFIXES: &[&str] = &[
    ".powerplatform.com",
    ".logic.azure.com",
    ".webhook.office.com",
];

/// Pure: a Teams webhook URL the API accepts — https on a Microsoft webhook
/// host, so the server never posts to arbitrary addresses.
pub fn valid_url(url: &str) -> bool {
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

/// The Details container's id, the target of its toggle.
const DETAILS_ID: &str = "details";

fn tone_style(t: Tone) -> (&'static str, &'static str) {
    match t {
        Tone::Neutral => ("emphasis", "Default"),
        Tone::Attention => ("attention", "Attention"),
        Tone::Good => ("good", "Good"),
        Tone::Warning => ("warning", "Warning"),
        Tone::Accent => ("accent", "Accent"),
    }
}

fn size(s: Size) -> Option<&'static str> {
    match s {
        Size::Default => None,
        Size::Small => Some("Small"),
        Size::Medium => Some("Medium"),
        Size::Large => Some("Large"),
    }
}

fn spacing(s: Spacing) -> Option<&'static str> {
    match s {
        Spacing::Default => None,
        Spacing::None => Some("None"),
        Spacing::Small => Some("Small"),
        Spacing::Medium => Some("Medium"),
    }
}

fn text_block(t: &Text) -> Value {
    let mut m = Map::new();
    m.insert("type".into(), json!("TextBlock"));
    m.insert("text".into(), json!(t.text));
    if t.bold {
        m.insert("weight".into(), json!("Bolder"));
    }
    if let Some(s) = size(t.size) {
        m.insert("size".into(), json!(s));
    }
    if let Some(c) = t.color {
        m.insert("color".into(), json!(tone_style(c).1));
    }
    if t.subtle {
        m.insert("isSubtle".into(), json!(true));
    }
    if let Some(s) = spacing(t.spacing) {
        m.insert("spacing".into(), json!(s));
    }
    m.insert("wrap".into(), json!(true));
    Value::Object(m)
}

fn facts(f: &[(String, String)]) -> Value {
    Value::Array(
        f.iter()
            .map(|(k, v)| json!({ "title": k, "value": v }))
            .collect(),
    )
}

fn block(b: &Block) -> Value {
    match b {
        Block::Banner {
            text,
            tone,
            size: s,
        } => {
            let (style, color) = tone_style(*tone);
            json!({
                "type": "Container",
                "bleed": true,
                "style": style,
                "items": [{
                    "type": "TextBlock", "text": text, "weight": "Bolder",
                    "size": size(*s).unwrap_or("Default"), "color": color, "wrap": true
                }]
            })
        }
        Block::Text(t) => text_block(t),
        Block::Facts {
            facts: f,
            spacing: s,
        } => {
            let mut m = Map::new();
            m.insert("type".into(), json!("FactSet"));
            if let Some(s) = spacing(*s) {
                m.insert("spacing".into(), json!(s));
            }
            m.insert("facts".into(), facts(f));
            Value::Object(m)
        }
        Block::Group { blocks, spacing: s } => {
            let mut m = Map::new();
            m.insert("type".into(), json!("Container"));
            if let Some(s) = spacing(*s) {
                m.insert("spacing".into(), json!(s));
            }
            m.insert(
                "items".into(),
                Value::Array(blocks.iter().map(block).collect()),
            );
            Value::Object(m)
        }
        Block::Details { facts: f } => json!({
            "type": "Container", "id": DETAILS_ID, "isVisible": false, "spacing": "Medium",
            "items": [{ "type": "FactSet", "facts": facts(f) }]
        }),
    }
}

/// Pure: the Adaptive Card for one message, in the envelope a Teams webhook
/// takes.
pub fn render(m: &Message) -> Value {
    let body: Vec<Value> = m.blocks.iter().map(block).collect();
    let mut actions: Vec<Value> = m
        .buttons
        .iter()
        .map(|b| json!({ "type": "Action.OpenUrl", "title": b.title, "url": b.url }))
        .collect();
    if m.blocks.iter().any(|b| matches!(b, Block::Details { .. })) {
        actions.push(json!({
            "type": "Action.ToggleVisibility", "title": "Details", "targetElements": [DETAILS_ID]
        }));
    }
    let mut card = Map::new();
    card.insert(
        "$schema".into(),
        json!("http://adaptivecards.io/schemas/adaptive-card.json"),
    );
    card.insert("type".into(), json!("AdaptiveCard"));
    card.insert("version".into(), json!("1.5"));
    if m.full_width {
        card.insert("msteams".into(), json!({ "width": "Full" }));
    }
    card.insert("body".into(), Value::Array(body));
    if !actions.is_empty() {
        card.insert("actions".into(), Value::Array(actions));
    }
    json!({
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "contentUrl": null,
            "content": Value::Object(card)
        }]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_microsoft_webhook_hosts_are_accepted() {
        assert!(valid_url("https://prod-12.westus.logic.azure.com:443/workflows/abc/triggers/manual/paths/invoke?sig=x"));
        assert!(valid_url("https://default123.08.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/abc"));
        assert!(valid_url("https://acme.webhook.office.com/webhookb2/abc"));
        assert!(!valid_url("http://prod-1.logic.azure.com/x"));
        assert!(!valid_url("https://evil.com/x"));
        assert!(!valid_url("https://logic.azure.com.evil.com/x"));
        assert!(!valid_url("https://evil.com@prod-1.logic.azure.com/x"));
        assert!(!valid_url("https://evil.com#.logic.azure.com"));
        assert!(!valid_url("https://hooks.slack.com/services/T0/B0/X"));
    }

    #[test]
    fn a_card_without_buttons_has_no_actions() {
        let c = render(&Message {
            blocks: vec![Block::Text(Text::new("hi"))],
            ..Message::default()
        });
        let content = &c["attachments"][0]["content"];
        assert!(content.get("actions").is_none());
        assert!(content.get("msteams").is_none());
        assert_eq!(content["body"][0]["wrap"], true);
    }
}
