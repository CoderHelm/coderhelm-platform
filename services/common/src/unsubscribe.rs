//! Signed unsubscribe links.
//!
//! An email carries a token that names one team, one person and one kind of
//! email. Whoever holds the token can turn that one email off for that one
//! person, and nothing else, without signing in. The token is
//! `base64url(team \n user \n expiry) . base64url(HMAC-SHA256)`; the HMAC key is
//! derived from the server secret for this purpose only, so a token can never
//! stand in for a session or anything signed with the secret itself.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// The weekly summary email.
pub const WEEKLY_SUMMARY: &str = "weekly_summary";
/// How long a link keeps working: a year, so an old email's link still works.
pub const VALID_SECS: i64 = 365 * 86_400;
/// Longest token accepted; real ones are well under this.
const MAX_TOKEN_LEN: usize = 1024;
/// Where the links point.
pub const BASE_URL: &str = "https://api.coderhelm.com/email/unsubscribe";
/// The token a test email carries when its address is not on the team.
pub const TEST_TOKEN: &str = "test";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscriber {
    pub team_id: String,
    pub user_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenError {
    Malformed,
    BadSignature,
    Expired,
}

fn mac(secret: &str, purpose: &str, payload: &str) -> HmacSha256 {
    // A key for unsubscribe links only.
    let mut kdf = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC takes any key");
    kdf.update(b"coderhelm/email-unsubscribe/v1");
    let key = kdf.finalize().into_bytes();
    let mut m = HmacSha256::new_from_slice(&key).expect("HMAC takes any key");
    m.update(purpose.as_bytes());
    m.update(b"\n");
    m.update(payload.as_bytes());
    m
}

/// A token for `purpose` that expires at `expires_at` (unix seconds).
pub fn sign(secret: &str, team_id: &str, user_id: &str, purpose: &str, expires_at: i64) -> String {
    let payload = format!("{team_id}\n{user_id}\n{expires_at}");
    let sig = mac(secret, purpose, &payload).finalize().into_bytes();
    format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(payload.as_bytes()),
        URL_SAFE_NO_PAD.encode(sig)
    )
}

/// Who a token for `purpose` belongs to, if it is genuine and current.
pub fn verify(
    secret: &str,
    token: &str,
    purpose: &str,
    now: i64,
) -> Result<Subscriber, TokenError> {
    if token.is_empty() || token.len() > MAX_TOKEN_LEN {
        return Err(TokenError::Malformed);
    }
    let (body, sig) = token.split_once('.').ok_or(TokenError::Malformed)?;
    let payload = URL_SAFE_NO_PAD
        .decode(body)
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
        .ok_or(TokenError::Malformed)?;
    let sig = URL_SAFE_NO_PAD
        .decode(sig)
        .map_err(|_| TokenError::Malformed)?;
    mac(secret, purpose, &payload)
        .verify_slice(&sig)
        .map_err(|_| TokenError::BadSignature)?;
    let mut parts = payload.split('\n');
    let (Some(team_id), Some(user_id), Some(exp), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(TokenError::Malformed);
    };
    let exp: i64 = exp.parse().map_err(|_| TokenError::Malformed)?;
    if team_id.is_empty() || user_id.is_empty() {
        return Err(TokenError::Malformed);
    }
    if now >= exp {
        return Err(TokenError::Expired);
    }
    Ok(Subscriber {
        team_id: team_id.to_string(),
        user_id: user_id.to_string(),
    })
}

/// The link for a token.
pub fn url(token: &str) -> String {
    format!("{BASE_URL}?t={token}")
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "test-secret";
    const NOW: i64 = 1_800_000_000;

    #[test]
    fn a_signed_token_names_its_team_and_person() {
        let t = sign(SECRET, "team-1", "USER#abc", WEEKLY_SUMMARY, NOW + 60);
        assert_eq!(
            verify(SECRET, &t, WEEKLY_SUMMARY, NOW),
            Ok(Subscriber {
                team_id: "team-1".into(),
                user_id: "USER#abc".into()
            })
        );
        // URL safe: no padding, no characters a query string would mangle.
        assert!(t
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.'));
    }

    #[test]
    fn another_secret_purpose_or_edit_is_refused() {
        let t = sign(SECRET, "team-1", "USER#abc", WEEKLY_SUMMARY, NOW + 60);
        assert_eq!(
            verify("other", &t, WEEKLY_SUMMARY, NOW),
            Err(TokenError::BadSignature)
        );
        assert_eq!(
            verify(SECRET, &t, "run_failed", NOW),
            Err(TokenError::BadSignature)
        );
        // Point the same signature at someone else.
        let (_, sig) = t.split_once('.').unwrap();
        let forged = format!(
            "{}.{sig}",
            URL_SAFE_NO_PAD.encode(format!("team-1\nUSER#other\n{}", NOW + 60))
        );
        assert_eq!(
            verify(SECRET, &forged, WEEKLY_SUMMARY, NOW),
            Err(TokenError::BadSignature)
        );
        // Push the expiry out.
        let later = format!(
            "{}.{sig}",
            URL_SAFE_NO_PAD.encode(format!("team-1\nUSER#abc\n{}", NOW + 999_999))
        );
        assert_eq!(
            verify(SECRET, &later, WEEKLY_SUMMARY, NOW),
            Err(TokenError::BadSignature)
        );
    }

    #[test]
    fn expired_and_malformed_tokens_are_refused() {
        let t = sign(SECRET, "team-1", "USER#abc", WEEKLY_SUMMARY, NOW);
        assert_eq!(
            verify(SECRET, &t, WEEKLY_SUMMARY, NOW),
            Err(TokenError::Expired)
        );
        for bad in ["", "nodot", "a.b", "!!!.???", TEST_TOKEN, &"x".repeat(2000)] {
            assert_eq!(
                verify(SECRET, bad, WEEKLY_SUMMARY, NOW),
                Err(TokenError::Malformed),
                "{bad}"
            );
        }
        // Genuinely signed, but not three fields.
        let payload = "team-1\nUSER#abc";
        let sig = mac(SECRET, WEEKLY_SUMMARY, payload).finalize().into_bytes();
        let short = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(payload),
            URL_SAFE_NO_PAD.encode(sig)
        );
        assert_eq!(
            verify(SECRET, &short, WEEKLY_SUMMARY, NOW),
            Err(TokenError::Malformed)
        );
    }

    #[test]
    fn the_link_carries_the_token() {
        assert_eq!(
            url("abc.def"),
            "https://api.coderhelm.com/email/unsubscribe?t=abc.def"
        );
    }
}
