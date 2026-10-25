//! Signed email approval links.
//!
//! When a draft goes to review, each approver gets an email with a link
//! that names one team, one approval, one draft version and that one
//! approver. Whoever holds the link can approve or reject exactly that
//! version as that approver, and nothing else: an edited draft is a new
//! version, so an old link is refused as stale; a stopped draft refuses
//! every link. The link expires after [`VALID_SECS`].
//!
//! The token is `base64url(team \n approval \n version \n email \n expiry)
//! . base64url(HMAC-SHA256)`, with a key derived from the server secret for
//! this purpose only, so it can never stand in for a session or for any
//! other signed link.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Three days.
pub const VALID_SECS: i64 = 3 * 86_400;
const MAX_TOKEN_LEN: usize = 1024;
/// The path the links point at, on this deployment's API (a page that asks
/// once more).
pub const PATH: &str = "/plugins/approve";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalLink {
    pub team_id: String,
    pub approval_id: String,
    pub version: u32,
    pub email: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenError {
    Malformed,
    BadSignature,
    Expired,
}

fn mac(secret: &str, payload: &str) -> HmacSha256 {
    let mut kdf = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC takes any key");
    kdf.update(b"coderhelm/plugin-approval/v1");
    let key = kdf.finalize().into_bytes();
    let mut m = HmacSha256::new_from_slice(&key).expect("HMAC takes any key");
    m.update(payload.as_bytes());
    m
}

pub fn sign(secret: &str, l: &ApprovalLink, expires_at: i64) -> String {
    let payload = format!(
        "{}\n{}\n{}\n{}\n{expires_at}",
        l.team_id,
        l.approval_id,
        l.version,
        l.email.to_ascii_lowercase()
    );
    let sig = mac(secret, &payload).finalize().into_bytes();
    format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(payload.as_bytes()),
        URL_SAFE_NO_PAD.encode(sig)
    )
}

pub fn verify(secret: &str, token: &str, now: i64) -> Result<ApprovalLink, TokenError> {
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
    mac(secret, &payload)
        .verify_slice(&sig)
        .map_err(|_| TokenError::BadSignature)?;
    let p: Vec<&str> = payload.split('\n').collect();
    let [team, approval, version, email, exp] = p.as_slice() else {
        return Err(TokenError::Malformed);
    };
    let exp: i64 = exp.parse().map_err(|_| TokenError::Malformed)?;
    let version: u32 = version.parse().map_err(|_| TokenError::Malformed)?;
    if team.is_empty() || approval.is_empty() || !email.contains('@') {
        return Err(TokenError::Malformed);
    }
    if now >= exp {
        return Err(TokenError::Expired);
    }
    Ok(ApprovalLink {
        team_id: team.to_string(),
        approval_id: approval.to_string(),
        version,
        email: email.to_string(),
    })
}

pub fn url(token: &str) -> String {
    format!(
        "{}{PATH}?t={token}",
        crate::public_urls::api_url_from_env().trim_end_matches('/')
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_800_000_000;

    fn link() -> ApprovalLink {
        ApprovalLink {
            team_id: "TEAM#a".into(),
            approval_id: "ap1".into(),
            version: 3,
            email: "Editor@A.io".into(),
        }
    }

    #[test]
    fn a_link_names_one_approval_version_and_approver() {
        let t = sign("s", &link(), NOW + 60);
        let l = verify("s", &t, NOW).unwrap();
        assert_eq!(l.version, 3);
        assert_eq!(l.email, "editor@a.io");
        assert!(t
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c)));
    }

    #[test]
    fn forged_expired_and_other_secrets_are_refused() {
        let t = sign("s", &link(), NOW + 60);
        assert_eq!(verify("other", &t, NOW), Err(TokenError::BadSignature));
        assert_eq!(verify("s", &t, NOW + 61), Err(TokenError::Expired));
        let (_, sig) = t.split_once('.').unwrap();
        let forged = format!(
            "{}.{sig}",
            URL_SAFE_NO_PAD.encode(format!("TEAM#a\nap1\n4\neditor@a.io\n{}", NOW + 60))
        );
        assert_eq!(verify("s", &forged, NOW), Err(TokenError::BadSignature));
        // An unsubscribe token (another purpose) never verifies here.
        let u = crate::unsubscribe::sign(
            "s",
            "TEAM#a",
            "USER#1",
            crate::unsubscribe::WEEKLY_SUMMARY,
            NOW + 60,
        );
        assert!(verify("s", &u, NOW).is_err());
        assert_eq!(verify("s", "nodot", NOW), Err(TokenError::Malformed));
    }
}
