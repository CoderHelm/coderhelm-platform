//! Messages for approval cards and sign-in prompts, in the shared chat
//! message model (`crate::notify`), so Slack and Teams render them the way
//! every other CoderHelm message renders.
//!
//! Buttons are links. An "Approve" or "Deny" button carries a signed token
//! for that approval ([`super::links`]); it opens the approval page, where
//! the person answers as themselves. Opening a link never answers anything.

use crate::notify::{Block, Button, Message, Size, Text, Tone};

use super::approvals::Approval;
use super::links::{self, Channel, LinkClaims};
use super::signin::Prompt;
use super::{api_url, dashboard_url};

/// A signed link to an approval for a channel (or a person, for email).
pub fn approval_link(
    secret: &str,
    team: &str,
    a: &Approval,
    channel: Channel,
    recipient: &str,
    action: &str,
) -> String {
    let expires_at = chrono::DateTime::parse_from_rfc3339(&a.expires_at)
        .map(|t| t.timestamp())
        .unwrap_or_else(|_| chrono::Utc::now().timestamp() + 3600);
    let token = links::sign(
        secret,
        &LinkClaims {
            team_id: team.to_string(),
            approval_id: a.id.clone(),
            recipient: recipient.to_string(),
            channel,
            expires_at,
        },
    );
    format!(
        "{}/agent-approvals/link?t={}&do={}",
        api_url(),
        urlencoding::encode(&token),
        urlencoding::encode(action)
    )
}

/// The dashboard page for an approval (signed in).
pub fn approval_page(a: &Approval) -> String {
    format!(
        "{}/settings/agents?approval={}",
        dashboard_url(),
        urlencoding::encode(&a.id)
    )
}

/// The Slack or Teams message for an approval card.
pub fn approval_message(secret: &str, team: &str, a: &Approval, channel: Channel) -> Message {
    let mut facts = vec![
        ("Agent".to_string(), a.agent.label().to_string()),
        (
            "Risk".to_string(),
            a.risk.map_or("unknown", |r| r.as_str()).to_string(),
        ),
    ];
    if let Some(r) = &a.repo {
        facts.push(("Repository".into(), r.clone()));
    }
    if !a.run_id.is_empty() {
        facts.push(("Run".into(), a.run_id.clone()));
    }
    facts.push(("Answer by".into(), a.expires_at.clone()));
    let args = serde_json::to_string_pretty(&a.args).unwrap_or_default();
    let mut blocks = vec![
        Block::Banner {
            text: "An agent is waiting for approval".into(),
            tone: Tone::Attention,
            size: Size::Medium,
        },
        Block::Text(Text::new(a.headline()).bold()),
    ];
    if !a.why.is_empty() {
        blocks.push(Block::Text(Text::new(format!("Why: {}", a.why))));
    }
    blocks.push(Block::Text(
        Text::new(format!("Asked because: {}", a.reason)).subtle(),
    ));
    blocks.push(Block::Facts {
        facts,
        spacing: Default::default(),
    });
    blocks.push(Block::Details {
        facts: vec![(
            "Arguments".into(),
            crate::truncate_str(&args, 1500).to_string(),
        )],
    });
    Message {
        summary: format!("Approval needed: {}", a.headline()),
        blocks,
        buttons: vec![
            Button::new(
                "Approve",
                approval_link(secret, team, a, channel, "", "approve"),
            ),
            Button::new("Deny", approval_link(secret, team, a, channel, "", "deny")),
        ],
        full_width: false,
    }
}

/// The Slack or Teams message for a sign-in prompt raised on a run.
pub fn sign_in_message(p: &Prompt, run_id: &str) -> Message {
    let mut blocks = vec![
        Block::Banner {
            text: if p.ask_admin {
                format!("{} isn't connected", p.server_name)
            } else {
                format!("Sign in to {}", p.server_name)
            },
            tone: Tone::Accent,
            size: Size::Medium,
        },
        Block::Text(Text::new(p.message.clone())),
    ];
    if !run_id.is_empty() {
        blocks.push(Block::Text(
            Text::new("The run carries on without it; the next one uses it once you've connected.")
                .subtle(),
        ));
    }
    Message {
        summary: p.message.clone(),
        blocks,
        buttons: vec![Button::new(
            if p.ask_admin {
                "Open settings"
            } else {
                "Connect"
            },
            format!("{}{}", dashboard_url(), p.link_path),
        )],
        full_width: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_policy::approvals::Status;
    use crate::agent_policy::model::*;

    fn card() -> Approval {
        Approval {
            id: "ap_1".into(),
            status: Status::Pending,
            agent: Agent::Coder,
            kind: CallKind::Mcp,
            tool: "linear__create_issue".into(),
            server: Some("linear".into()),
            repo: Some("acme/web".into()),
            command: None,
            run_id: "run-1".into(),
            thread_id: String::new(),
            requested_for: String::new(),
            args: serde_json::json!({"title": "Flaky test", "api_token": "(hidden)"}),
            args_hash: "h".into(),
            risk: Some(Risk::Write),
            why: "Track the flaky test".into(),
            asked_because: DecidedBy::Default { risk: Risk::Write },
            reason: "Changes ask by default.".into(),
            created_at: "2026-10-24T12:00:00.000Z".into(),
            expires_at: "2099-10-24T12:15:00.000Z".into(),
            decided_by: String::new(),
            decided_via: None,
            decided_at: None,
            choice: None,
            note: String::new(),
            rule_id: String::new(),
        }
    }

    #[test]
    fn approval_messages_carry_signed_links_that_verify() {
        let m = approval_message("s3cret", "TEAM#1", &card(), Channel::Slack);
        assert_eq!(m.buttons.len(), 2);
        let url = &m.buttons[0].url;
        assert!(url.starts_with("https://api.coderhelm.com/agent-approvals/link?t="));
        let token = url.split("t=").nth(1).unwrap().split('&').next().unwrap();
        let token = urlencoding::decode(token).unwrap();
        let c = links::verify("s3cret", &token, chrono::Utc::now().timestamp()).unwrap();
        assert_eq!(c.approval_id, "ap_1");
        assert_eq!(c.channel, Channel::Slack);
        assert!(c.recipient.is_empty());
        // Both channels render it.
        for kind in crate::notify::ChannelKind::ALL {
            let json = kind.render(&m).to_string();
            assert!(json.contains("Use create_issue on linear"));
            assert!(!json.contains("→"));
        }
    }

    /// A self-hosted install's approval cards and sign-in prompts link to
    /// the install, in every channel, never to the hosted service.
    #[test]
    fn a_self_hosted_install_links_to_itself() {
        use crate::public_urls::{hosted_mentions, pretend_self_hosted, EXAMPLE_SELF_HOST};
        pretend_self_hosted();
        let a = card();
        let p = super::super::signin::prompt(
            "figma",
            "Figma",
            super::super::signin::Need::Personal,
            true,
            "read this file",
        );
        let messages = [
            approval_message("s3cret", "TEAM#1", &a, Channel::Slack),
            approval_message("s3cret", "TEAM#1", &a, Channel::Teams),
            sign_in_message(&p, "run-1"),
        ];
        let mut out = vec![
            approval_page(&a),
            approval_link(
                "s3cret",
                "TEAM#1",
                &a,
                Channel::Email,
                "ada@example.org",
                "approve",
            ),
        ];
        for m in &messages {
            for b in &m.buttons {
                assert!(b.url.starts_with(EXAMPLE_SELF_HOST), "{}", b.url);
            }
            for kind in crate::notify::ChannelKind::ALL {
                out.push(kind.render(m).to_string());
            }
        }
        crate::public_urls::override_for_this_thread(None);
        for o in &out {
            assert_eq!(hosted_mentions(o), Vec::<String>::new(), "{o}");
        }
        assert!(out[0].starts_with(EXAMPLE_SELF_HOST));
        assert!(out[1].starts_with(EXAMPLE_SELF_HOST));
    }

    #[test]
    fn sign_in_messages_link_to_the_dashboard() {
        let p = super::super::signin::prompt(
            "figma",
            "Figma",
            super::super::signin::Need::Personal,
            false,
            "read this file",
        );
        let m = sign_in_message(&p, "run-1");
        assert_eq!(m.buttons[0].title, "Connect");
        assert!(m.buttons[0]
            .url
            .starts_with("https://app.coderhelm.com/settings/integrations/#mcp-servers"));
        assert_eq!(p.message, "Sign in to Figma to let me read this file.");
    }
}
