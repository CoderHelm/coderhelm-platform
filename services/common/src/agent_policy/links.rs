//! Signed approval links, for the buttons on Slack and Teams messages and
//! in approval emails.
//!
//! A token names one team, one approval, one recipient and an expiry:
//! `base64url(team \n approval \n recipient \n channel \n expiry) .
//! base64url(HMAC-SHA256)`, with a key derived from the server secret for
//! this purpose only (so it can't stand in for a session or any other signed
//! value).
//!
//! - An **email** link names the person it was sent to. Opening it shows the
//!   card and Approve and Deny buttons; nothing happens on the open itself
//!   (mail scanners and link previews open links). The answer is recorded as
//!   that person, and only if they may still approve when they click.
//! - A **Slack or Teams** button goes to a channel, not a person, so its
//!   token only proves the link came from CoderHelm for that approval: it
//!   opens the approval page, where the person signs in and answers as
//!   themselves.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const MAX_TOKEN_LEN: usize = 2048;

/// Where a link was sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    Email,
    Slack,
    Teams,
}

impl Channel {
    pub fn key(self) -> &'static str {
        match self {
            Channel::Email => "email",
            Channel::Slack => "slack",
            Channel::Teams => "teams",
        }
    }
    fn parse(s: &str) -> Option<Self> {
        match s {
            "email" => Some(Channel::Email),
            "slack" => Some(Channel::Slack),
            "teams" => Some(Channel::Teams),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkClaims {
    pub team_id: String,
    pub approval_id: String,
    /// The person (email) for an email link; empty for a channel link.
    pub recipient: String,
    pub channel: Channel,
    pub expires_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkError {
    Malformed,
    BadSignature,
    Expired,
}

impl std::fmt::Display for LinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            LinkError::Malformed | LinkError::BadSignature => "This approval link isn't valid.",
            LinkError::Expired => "This approval link has expired.",
        })
    }
}

fn mac(secret: &str, payload: &str) -> HmacSha256 {
    let mut kdf = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC takes any key");
    kdf.update(b"coderhelm/agent-approval-link/v1");
    let key = kdf.finalize().into_bytes();
    let mut m = HmacSha256::new_from_slice(&key).expect("HMAC takes any key");
    m.update(payload.as_bytes());
    m
}

fn clean(s: &str) -> String {
    s.replace('\n', " ")
}

pub fn sign(secret: &str, c: &LinkClaims) -> String {
    let payload = format!(
        "{}\n{}\n{}\n{}\n{}",
        clean(&c.team_id),
        clean(&c.approval_id),
        clean(&c.recipient.to_ascii_lowercase()),
        c.channel.key(),
        c.expires_at
    );
    let sig = mac(secret, &payload).finalize().into_bytes();
    format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(payload.as_bytes()),
        URL_SAFE_NO_PAD.encode(sig)
    )
}

/// Checks a token's signature and expiry (`now` in unix seconds).
pub fn verify(secret: &str, token: &str, now: i64) -> Result<LinkClaims, LinkError> {
    if token.len() > MAX_TOKEN_LEN {
        return Err(LinkError::Malformed);
    }
    let (p, sig) = token.split_once('.').ok_or(LinkError::Malformed)?;
    let payload = URL_SAFE_NO_PAD
        .decode(p)
        .map_err(|_| LinkError::Malformed)?;
    let payload = String::from_utf8(payload).map_err(|_| LinkError::Malformed)?;
    let sig = URL_SAFE_NO_PAD
        .decode(sig)
        .map_err(|_| LinkError::Malformed)?;
    mac(secret, &payload)
        .verify_slice(&sig)
        .map_err(|_| LinkError::BadSignature)?;
    let parts: Vec<&str> = payload.split('\n').collect();
    let [team, approval, recipient, channel, exp] = parts.as_slice() else {
        return Err(LinkError::Malformed);
    };
    let expires_at: i64 = exp.parse().map_err(|_| LinkError::Malformed)?;
    if now >= expires_at {
        return Err(LinkError::Expired);
    }
    Ok(LinkClaims {
        team_id: team.to_string(),
        approval_id: approval.to_string(),
        recipient: recipient.to_string(),
        channel: Channel::parse(channel).ok_or(LinkError::Malformed)?,
        expires_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(channel: Channel) -> LinkClaims {
        LinkClaims {
            team_id: "TEAM#1".into(),
            approval_id: "ap_1".into(),
            recipient: if channel == Channel::Email {
                "Ann@X.dev".into()
            } else {
                String::new()
            },
            channel,
            expires_at: 2_000,
        }
    }

    #[test]
    fn a_signed_link_round_trips() {
        for ch in [Channel::Email, Channel::Slack, Channel::Teams] {
            let t = sign("s3cret", &claims(ch));
            let c = verify("s3cret", &t, 1_000).unwrap();
            assert_eq!(c.approval_id, "ap_1");
            assert_eq!(c.channel, ch);
        }
        assert_eq!(
            verify("s3cret", &sign("s3cret", &claims(Channel::Email)), 1)
                .unwrap()
                .recipient,
            "ann@x.dev"
        );
    }

    #[test]
    fn tampered_wrong_key_and_expired_links_fail() {
        let t = sign("s3cret", &claims(Channel::Email));
        assert_eq!(verify("other", &t, 1_000), Err(LinkError::BadSignature));
        assert_eq!(verify("s3cret", &t, 2_000), Err(LinkError::Expired));
        // Change the approval id inside the payload.
        let (p, sig) = t.split_once('.').unwrap();
        let mut payload = String::from_utf8(URL_SAFE_NO_PAD.decode(p).unwrap()).unwrap();
        payload = payload.replace("ap_1", "ap_2");
        let forged = format!("{}.{}", URL_SAFE_NO_PAD.encode(payload), sig);
        assert_eq!(
            verify("s3cret", &forged, 1_000),
            Err(LinkError::BadSignature)
        );
        assert_eq!(
            verify("s3cret", "nonsense", 1_000),
            Err(LinkError::Malformed)
        );
    }
}
