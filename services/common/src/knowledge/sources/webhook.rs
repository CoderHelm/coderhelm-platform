//! Change events from providers, and how each is checked before it starts a
//! sync.
//!
//! - Sanity: a webhook per source, at `/webhooks/knowledge/<token>`. The
//!   token in the address finds the source (only its hash is stored) and is
//!   also the signing secret: Sanity signs `"<t>.<body>"` with HMAC-SHA256
//!   (`sanity-webhook-signature: t=<ms>,v1=<base64url>`), checked here, and
//!   a signature older than [`TOLERANCE_SECS`] is refused.
//! - Notion: one address for the whole install (`/webhooks/knowledge/notion`),
//!   signed with the subscription's verification token
//!   (`X-Notion-Signature: sha256=<hex>`); the event's workspace finds the
//!   sources.
//!
//! An event only asks for a sync. What changed is always read back from the
//! provider with the source's own connection, so a forged or replayed event
//! can at most cause an early sync.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// How old a signed timestamp may be.
pub const TOLERANCE_SECS: i64 = 300;
/// The path segment Notion's install-wide webhook uses instead of a token.
pub const NOTION_PATH: &str = "notion";

/// A new webhook token: 32 random bytes, URL-safe.
pub fn new_token() -> String {
    let mut b = [0u8; 32];
    rand::rng().fill_bytes(&mut b);
    URL_SAFE_NO_PAD.encode(b)
}

/// A token as it appears in a path: URL-safe characters, the right length.
pub fn valid_token(t: &str) -> bool {
    (20..=100).contains(&t.len())
        && t.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Checks Sanity's `sanity-webhook-signature` header for `body`, signed with
/// `secret`, at `now` (unix seconds).
pub fn verify_sanity(header: &str, body: &[u8], secret: &str, now: i64) -> Result<(), String> {
    let mut t: Option<&str> = None;
    let mut sig: Option<&str> = None;
    for part in header.split(',') {
        match part.trim().split_once('=') {
            Some(("t", v)) => t = Some(v),
            Some(("v1", v)) => sig = Some(v),
            _ => {}
        }
    }
    let (t, sig) = t.zip(sig).ok_or("The signature header is malformed.")?;
    let ts: i64 = t
        .parse()
        .map_err(|_| "The signature's time isn't a number.")?;
    // Sanity sends milliseconds.
    let secs = if ts > 10_000_000_000 { ts / 1000 } else { ts };
    if (now - secs).abs() > TOLERANCE_SECS {
        return Err("The signature is too old.".into());
    }
    let expected = URL_SAFE_NO_PAD
        .decode(sig.trim_end_matches('='))
        .map_err(|_| "The signature isn't base64.")?;
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|_| "bad key")?;
    mac.update(t.as_bytes());
    mac.update(b".");
    mac.update(body);
    mac.verify_slice(&expected)
        .map_err(|_| "The signature doesn't match.".to_string())
}

/// The header Sanity would send for `body` (tests and the docs' example).
pub fn sign_sanity(body: &[u8], secret: &str, t_ms: i64) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("any key length");
    mac.update(t_ms.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    format!(
        "t={t_ms},v1={}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )
}

/// Checks Notion's `X-Notion-Signature` header for `body`, signed with the
/// subscription's verification token.
pub fn verify_notion(header: &str, body: &[u8], verification_token: &str) -> Result<(), String> {
    if verification_token.is_empty() {
        return Err("Notion webhooks aren't set up on this install.".into());
    }
    let hex_sig = header
        .trim()
        .strip_prefix("sha256=")
        .ok_or("The signature header is malformed.")?;
    let expected = (0..hex_sig.len())
        .step_by(2)
        .map(|i| {
            hex_sig
                .get(i..i + 2)
                .and_then(|b| u8::from_str_radix(b, 16).ok())
        })
        .collect::<Option<Vec<u8>>>()
        .ok_or("The signature isn't hex.")?;
    let mut mac =
        HmacSha256::new_from_slice(verification_token.as_bytes()).map_err(|_| "bad key")?;
    mac.update(body);
    mac.verify_slice(&expected)
        .map_err(|_| "The signature doesn't match.".to_string())
}

/// The workspace a Notion event is about.
pub fn notion_workspace(event: &serde_json::Value) -> Option<String> {
    event["workspace_id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(super::notion::norm)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity_signatures_are_checked() {
        let body = br#"{"_id":"p1","_type":"post","_rev":"r2"}"#;
        let now = 1_800_000_000i64;
        let h = sign_sanity(body, "tok-secret", now * 1000);
        assert!(verify_sanity(&h, body, "tok-secret", now).is_ok());
        assert!(
            verify_sanity(&h, body, "other", now).is_err(),
            "wrong secret"
        );
        assert!(
            verify_sanity(&h, b"{}", "tok-secret", now).is_err(),
            "changed body"
        );
        assert!(
            verify_sanity(&h, body, "tok-secret", now + TOLERANCE_SECS + 1).is_err(),
            "replayed later"
        );
        assert!(verify_sanity("v1=abc", body, "tok-secret", now).is_err());
        assert!(verify_sanity("t=x,v1=abc", body, "tok-secret", now).is_err());
    }

    #[test]
    fn notion_signatures_are_checked() {
        let body = br#"{"type":"page.content_updated","workspace_id":"ab-cd"}"#;
        let mut mac = HmacSha256::new_from_slice(b"vt").unwrap();
        mac.update(body);
        let hex: String = mac
            .finalize()
            .into_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let h = format!("sha256={hex}");
        assert!(verify_notion(&h, body, "vt").is_ok());
        assert!(verify_notion(&h, body, "other").is_err());
        assert!(verify_notion(&h, body, "").is_err(), "not set up");
        assert!(verify_notion("sha256=zz", body, "vt").is_err());
        assert!(verify_notion(&hex, body, "vt").is_err(), "no prefix");
        let v: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(notion_workspace(&v).as_deref(), Some("abcd"));
    }

    #[test]
    fn tokens_are_random_and_path_safe() {
        let a = new_token();
        assert_ne!(a, new_token());
        assert!(valid_token(&a));
        assert!(!valid_token("short"));
        assert!(!valid_token(&"../".repeat(10)));
    }
}
