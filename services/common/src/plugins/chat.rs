//! Slack and Teams for plugins: review cards out, briefs in.
//!
//! Out, through the team's existing chat channels (`crate::notify`): when a
//! draft goes to review, a card with the draft's first lines and two
//! buttons. "Review" opens the draft; "Approve" opens it ready to approve
//! that exact version. Both land in the dashboard, where the approver is
//! signed in and every rule (not the requester, in the approver group, the
//! version unchanged) is checked; a chat button never approves anything by
//! itself. A plugin with a brief form gets a "Write a brief" card too.
//!
//! In, a brief from chat starts a run:
//!
//! - a Slack slash command (`/coderhelm brief <plugin> <text>`), verified
//!   with the team's own Slack app signing secret (`v0` HMAC over the
//!   timestamp and body, at most five minutes old);
//! - a Teams outgoing webhook message (`brief <plugin> <text>`), verified
//!   with the webhook's HMAC secret.
//!
//! Anything that fails verification is refused before it is read.

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::drafts::Artifact;
use crate::notify::{Block, Button, Message, Size, Spacing, Text, Tone};

type HmacSha256 = Hmac<Sha256>;

/// Slack requests older than this are refused (replays).
pub const SLACK_MAX_AGE_SECS: i64 = 300;

/// The dashboard's address for a stage.
pub fn dashboard(stage: &str) -> &'static str {
    match stage {
        "prod" | "production" => "https://app.coderhelm.com",
        _ => "https://app.dev.coderhelm.com",
    }
}

/// The card for a draft waiting for review.
pub fn review_card(a: &Artifact, excerpt: &str, base: &str) -> Message {
    let open = format!("{base}/plugins/draft/?id={}", a.id);
    let approve = format!("{open}&act=approve&v={}", a.version);
    let mut facts = vec![
        ("Plugin".to_string(), a.plugin_name.clone()),
        ("Kind".to_string(), a.kind.clone()),
        ("Version".to_string(), a.version.to_string()),
    ];
    if !a.channel.is_empty() {
        facts.push(("For".into(), a.channel.clone()));
    }
    Message {
        summary: format!("Draft to review: {}", a.title),
        blocks: vec![
            Block::Banner {
                text: "Draft waiting for review".into(),
                tone: Tone::Accent,
                size: Size::Large,
            },
            Block::Group {
                blocks: vec![
                    Block::Text(Text::new(&a.title).bold().size(Size::Medium)),
                    Block::Text(Text::new(crate::truncate_str(excerpt, 400)).subtle().spacing(Spacing::Small)),
                ],
                spacing: Spacing::Medium,
            },
            Block::Facts {
                facts,
                spacing: Spacing::Medium,
            },
            Block::Text(
                Text::new("Nothing is published until someone other than the requester approves this version.")
                    .subtle()
                    .spacing(Spacing::Small),
            ),
        ],
        buttons: vec![Button::new("Review", open), Button::new("Approve…", approve)],
        full_width: true,
    }
}

/// The card that invites a brief.
pub fn brief_card(plugin_id: &str, plugin_name: &str, base: &str) -> Message {
    Message {
        summary: format!("Write a brief for {plugin_name}"),
        blocks: vec![
            Block::Banner {
                text: format!("{plugin_name} is ready for a brief"),
                tone: Tone::Neutral,
                size: Size::Medium,
            },
            Block::Text(Text::new(format!(
                "Or reply here with: brief {plugin_id} <what you need>"
            ))),
        ],
        buttons: vec![Button::new(
            "Write a brief",
            format!("{base}/plugins/plugin/?id={plugin_id}#brief"),
        )],
        full_width: false,
    }
}

/// Slack's request signature: `v0=hex(HMAC(secret, "v0:<ts>:<body>"))`.
pub fn verify_slack(secret: &str, timestamp: &str, body: &[u8], signature: &str, now: i64) -> bool {
    if secret.is_empty() {
        return false;
    }
    let Ok(ts) = timestamp.parse::<i64>() else {
        return false;
    };
    if (now - ts).abs() > SLACK_MAX_AGE_SECS {
        return false;
    }
    let Some(hex_sig) = signature.strip_prefix("v0=") else {
        return false;
    };
    let Ok(sig) = hex::decode(hex_sig) else {
        return false;
    };
    let mut m = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC takes any key");
    m.update(format!("v0:{timestamp}:").as_bytes());
    m.update(body);
    m.verify_slice(&sig).is_ok()
}

/// A Teams outgoing webhook's signature: `Authorization: HMAC
/// base64(HMAC(base64decode(secret), body))`.
pub fn verify_teams(secret_b64: &str, body: &[u8], authorization: &str) -> bool {
    let b64 = base64::engine::general_purpose::STANDARD;
    let Ok(key) = b64.decode(secret_b64.trim()) else {
        return false;
    };
    if key.is_empty() {
        return false;
    }
    let Some(given) = authorization.trim().strip_prefix("HMAC ") else {
        return false;
    };
    let Ok(sig) = b64.decode(given.trim()) else {
        return false;
    };
    let mut m = HmacSha256::new_from_slice(&key).expect("HMAC takes any key");
    m.update(body);
    m.verify_slice(&sig).is_ok()
}

/// `brief <plugin> <text>` (any leading mention removed): the plugin and
/// the brief.
pub fn parse_brief(text: &str) -> Option<(String, String)> {
    let t = text.replace("<at>", " ").replace("</at>", " ");
    let mut words = t
        .split_whitespace()
        .skip_while(|w| !w.eq_ignore_ascii_case("brief"));
    words.next()?;
    let plugin = words.next()?.to_ascii_lowercase();
    let brief = words.collect::<Vec<_>>().join(" ");
    if !super::valid_id(&plugin) || brief.trim().is_empty() {
        return None;
    }
    Some((plugin, brief))
}

/// The fields of a Slack slash command body (form encoded).
pub fn slack_form(body: &str) -> std::collections::HashMap<String, String> {
    url::form_urlencoded::parse(body.as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slack_sig(secret: &str, ts: &str, body: &str) -> String {
        let mut m = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        m.update(format!("v0:{ts}:{body}").as_bytes());
        format!("v0={}", hex::encode(m.finalize().into_bytes()))
    }

    #[test]
    fn slack_requests_are_verified_and_old_ones_refused() {
        let body = "team_id=T1&user_name=dana&text=brief+marketing+Launch+post";
        let sig = slack_sig("sek", "1800000000", body);
        assert!(verify_slack(
            "sek",
            "1800000000",
            body.as_bytes(),
            &sig,
            1_800_000_010
        ));
        assert!(!verify_slack(
            "other",
            "1800000000",
            body.as_bytes(),
            &sig,
            1_800_000_010
        ));
        assert!(!verify_slack(
            "sek",
            "1800000000",
            b"tampered",
            &sig,
            1_800_000_010
        ));
        assert!(!verify_slack(
            "sek",
            "1800000000",
            body.as_bytes(),
            &sig,
            1_800_000_400
        ));
        assert!(!verify_slack(
            "",
            "1800000000",
            body.as_bytes(),
            &sig,
            1_800_000_010
        ));
        let f = slack_form(body);
        assert_eq!(
            parse_brief(&f["text"]),
            Some(("marketing".into(), "Launch post".into()))
        );
    }

    #[test]
    fn teams_messages_are_verified() {
        let b64 = base64::engine::general_purpose::STANDARD;
        let secret = b64.encode(b"0123456789abcdef");
        let body = br#"{"text":"<at>CoderHelm</at> brief marketing Webinar invite"}"#;
        let mut m = HmacSha256::new_from_slice(b"0123456789abcdef").unwrap();
        m.update(body);
        let auth = format!("HMAC {}", b64.encode(m.finalize().into_bytes()));
        assert!(verify_teams(&secret, body, &auth));
        assert!(!verify_teams(&secret, b"{}", &auth));
        assert!(!verify_teams(&secret, body, "Bearer x"));
        assert_eq!(
            parse_brief("<at>CoderHelm</at> brief marketing Webinar invite"),
            Some(("marketing".into(), "Webinar invite".into()))
        );
        assert_eq!(parse_brief("hello"), None);
        assert_eq!(parse_brief("brief Bad/Name x"), None);
    }

    #[test]
    fn review_cards_link_to_the_exact_version() {
        let a = Artifact {
            id: "art1".into(),
            plugin_name: "marketing".into(),
            kind: "social".into(),
            title: "Launch".into(),
            version: 3,
            ..Default::default()
        };
        let m = review_card(&a, "We shipped.", "https://app.coderhelm.com");
        assert!(m.buttons[1].url.ends_with("id=art1&act=approve&v=3"));
        // Renders on both channels.
        let _ = crate::notify::ChannelKind::Slack.render(&m);
        let _ = crate::notify::ChannelKind::Teams.render(&m);
    }
}
