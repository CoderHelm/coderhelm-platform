//! Telling the team when a source's sign-in stops working.
//!
//! A source read through a member's sign-in ([`super::SignIn`]) stops when
//! that sign-in expires or is revoked, or when the member leaves the team.
//! The source then shows one sentence and a "Sign in again" button any admin
//! can press; this builds the message the team's owner and admins get once
//! (in the team's chat channels and by email) so nobody has to notice it on
//! the page first.

use crate::notify::{Block, Button, Message, Size, Text, Tone};

use super::{Source, SourceError};

/// The page a source lives on.
pub fn source_link(app_url: &str, s: &Source) -> String {
    format!(
        "{}/knowledge/base/?id={}",
        app_url.trim_end_matches('/'),
        urlencoding::encode(&s.kb_id)
    )
}

/// The chat card: what stopped, the one sentence, and the button.
pub fn signin_message(app_url: &str, s: &Source, e: &SourceError) -> Message {
    let label = s.kind.map(super::Kind::label).unwrap_or("A connected app");
    Message {
        summary: format!("\"{}\" stopped syncing from {label}", s.name),
        blocks: vec![
            Block::Banner {
                text: format!("\"{}\" stopped syncing from {label}", s.name),
                tone: Tone::Warning,
                size: Size::Medium,
            },
            Block::Text(Text::new(e.fix.clone())),
            Block::Text(
                Text::new("What was already brought in stays searchable until it syncs again.")
                    .subtle(),
            ),
        ],
        buttons: vec![Button::new("Sign in again", source_link(app_url, s))],
        full_width: false,
    }
}

/// The email: a subject and a plain-text body.
pub fn signin_email(app_url: &str, s: &Source, e: &SourceError) -> (String, String) {
    let label = s.kind.map(super::Kind::label).unwrap_or("a connected app");
    (
        format!("\"{}\" stopped syncing from {label}", s.name),
        format!(
            "The knowledge source \"{}\" stopped syncing from {label}.\n\n{}\n\nAny admin can sign in again here: {}\n\nWhat was already brought in stays searchable until it syncs again.",
            s.name,
            e.fix,
            source_link(app_url, s)
        ),
    )
}

/// Should the team be told now? Once per stop: when the source needs a new
/// sign-in and nobody has been told since it last worked.
pub fn should_alert(s: &Source) -> bool {
    let needs = s.error.as_ref().is_some_and(|e| e.action == "sign_in");
    needs && s.signin.as_ref().is_some_and(|si| si.alerted_at.is_empty())
}

#[cfg(test)]
mod tests {
    use super::super::{signin_error, Kind, SignIn};
    use super::*;

    fn source() -> Source {
        let si = SignIn {
            email: "ada@example.com".into(),
            name: "Ada".into(),
            ..Default::default()
        };
        Source {
            id: "s1".into(),
            kb_id: "kb1".into(),
            kind: Some(Kind::Sanity),
            name: "Help center".into(),
            error: Some(signin_error(&si, Kind::Sanity, String::new(), false)),
            signin: Some(si),
            ..Default::default()
        }
    }

    #[test]
    fn the_team_is_told_once_with_a_way_to_sign_in_again() {
        let mut s = source();
        assert!(should_alert(&s));
        let m = signin_message("https://app.example.com/", &s, s.error.as_ref().unwrap());
        assert_eq!(m.buttons[0].title, "Sign in again");
        assert_eq!(
            m.buttons[0].url,
            "https://app.example.com/knowledge/base/?id=kb1"
        );
        let (subject, body) =
            signin_email("https://app.example.com", &s, s.error.as_ref().unwrap());
        assert_eq!(subject, "\"Help center\" stopped syncing from Sanity");
        assert!(body.contains("Ada's Sanity sign-in stopped working."));
        s.signin.as_mut().unwrap().alerted_at = "2026-10-25T00:00:00Z".into();
        assert!(!should_alert(&s), "told once");
        let token = Source {
            error: s.error.clone(),
            ..Default::default()
        };
        assert!(!should_alert(&token), "a token source isn't a sign-in");
    }
}
