//! Signing, as the Standard Webhooks spec (standardwebhooks.com) has it.
//!
//! Every request carries three headers:
//!
//! - `webhook-id`: the event id, the same on every retry;
//! - `webhook-timestamp`: Unix seconds when this attempt was sent;
//! - `webhook-signature`: space-separated `v1,<base64 signature>` entries,
//!   one per live secret (two while a rotated secret is still accepted).
//!
//! A signature is the base64 HMAC-SHA256 of `<id>.<timestamp>.<raw body>`,
//! keyed with the secret's bytes: the part after `whsec_`, base64-decoded.
//! Any Standard Webhooks library verifies these requests.

use base64::{engine::general_purpose::STANDARD, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;

pub const SECRET_PREFIX: &str = "whsec_";
pub const ID_HEADER: &str = "webhook-id";
pub const TIMESTAMP_HEADER: &str = "webhook-timestamp";
pub const SIGNATURE_HEADER: &str = "webhook-signature";
/// How far a timestamp may be from now before a receiver should refuse it
/// (what the libraries default to).
pub const TOLERANCE_SECS: i64 = 300;

/// A new signing secret: `whsec_` and 32 random bytes in base64.
pub fn new_secret() -> String {
    use rand::RngCore;
    let mut key = [0u8; 32];
    rand::rng().fill_bytes(&mut key);
    format!("{SECRET_PREFIX}{}", STANDARD.encode(key))
}

/// The key bytes of a `whsec_` secret.
pub fn key_of(secret: &str) -> Result<Vec<u8>, String> {
    let b64 = secret.strip_prefix(SECRET_PREFIX).unwrap_or(secret);
    STANDARD
        .decode(b64.trim())
        .map_err(|_| "the signing secret isn't valid base64".to_string())
}

/// The base64 signature of one message with one secret.
pub fn signature(secret: &str, id: &str, timestamp: i64, body: &[u8]) -> Result<String, String> {
    let key = key_of(secret)?;
    let mut mac =
        Hmac::<Sha256>::new_from_slice(&key).map_err(|_| "bad signing key".to_string())?;
    mac.update(id.as_bytes());
    mac.update(b".");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    Ok(STANDARD.encode(mac.finalize().into_bytes()))
}

/// The `webhook-signature` header: a `v1,` entry per secret, space separated.
pub fn header(secrets: &[&str], id: &str, timestamp: i64, body: &[u8]) -> Result<String, String> {
    let mut parts = Vec::with_capacity(secrets.len());
    for s in secrets {
        parts.push(format!("v1,{}", signature(s, id, timestamp, body)?));
    }
    Ok(parts.join(" "))
}

/// Why a request didn't verify.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyError {
    Stale,
    NoMatch,
    Malformed,
}

/// Checks a request the way a receiver does: the timestamp within
/// [`TOLERANCE_SECS`] of `now`, and any `v1` entry matching (compared in
/// constant time).
pub fn verify(
    secret: &str,
    id: &str,
    timestamp: &str,
    signature_header: &str,
    body: &[u8],
    now: i64,
) -> Result<(), VerifyError> {
    let ts: i64 = timestamp
        .trim()
        .parse()
        .map_err(|_| VerifyError::Malformed)?;
    if (now - ts).abs() > TOLERANCE_SECS {
        return Err(VerifyError::Stale);
    }
    let expected = signature(secret, id, ts, body).map_err(|_| VerifyError::Malformed)?;
    let matched = signature_header.split_whitespace().any(|entry| {
        entry
            .strip_prefix("v1,")
            .is_some_and(|sig| subtle_eq(sig.as_bytes(), expected.as_bytes()))
    });
    if matched {
        Ok(())
    } else {
        Err(VerifyError::NoMatch)
    }
}

/// Constant time for equal lengths: every byte is compared, whatever matches.
fn subtle_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The last four characters of a secret, to tell secrets apart on the page.
pub fn hint(secret: &str) -> String {
    let chars: Vec<char> = secret.chars().collect();
    if chars.len() < 12 {
        return String::new();
    }
    format!(
        "{SECRET_PREFIX}…{}",
        chars[chars.len() - 4..].iter().collect::<String>()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // The Standard Webhooks reference test vector (the spec's libraries all
    // test against it).
    const SECRET: &str = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";
    const ID: &str = "msg_p5jXN8AQM9LWM0D4loKWxJek";
    const TS: i64 = 1614265330;
    const BODY: &[u8] = br#"{"test": 2432232314}"#;
    const EXPECTED: &str = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=";

    #[test]
    fn matches_the_standard_webhooks_test_vector() {
        assert_eq!(header(&[SECRET], ID, TS, BODY).unwrap(), EXPECTED);
        assert!(verify(SECRET, ID, &TS.to_string(), EXPECTED, BODY, TS + 10).is_ok());
    }

    #[test]
    fn verification_refuses_tampering_and_old_timestamps() {
        let ts = TS.to_string();
        assert_eq!(
            verify(SECRET, ID, &ts, EXPECTED, br#"{"test": 1}"#, TS),
            Err(VerifyError::NoMatch)
        );
        assert_eq!(
            verify(SECRET, "msg_other", &ts, EXPECTED, BODY, TS),
            Err(VerifyError::NoMatch)
        );
        assert_eq!(
            verify(SECRET, ID, &ts, EXPECTED, BODY, TS + TOLERANCE_SECS + 1),
            Err(VerifyError::Stale)
        );
        assert_eq!(
            verify(SECRET, ID, &ts, EXPECTED, BODY, TS - TOLERANCE_SECS - 1),
            Err(VerifyError::Stale)
        );
        assert_eq!(
            verify(SECRET, ID, "soon", EXPECTED, BODY, TS),
            Err(VerifyError::Malformed)
        );
        assert_eq!(
            verify(SECRET, ID, &ts, "v2,abc", BODY, TS),
            Err(VerifyError::NoMatch)
        );
    }

    #[test]
    fn two_secrets_sign_during_a_rotation_and_either_verifies() {
        let new = new_secret();
        let h = header(&[&new, SECRET], ID, TS, BODY).unwrap();
        let parts: Vec<&str> = h.split(' ').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[1], EXPECTED);
        assert!(verify(&new, ID, &TS.to_string(), &h, BODY, TS).is_ok());
        assert!(verify(SECRET, ID, &TS.to_string(), &h, BODY, TS).is_ok());
        assert_eq!(
            verify(&new_secret(), ID, &TS.to_string(), &h, BODY, TS),
            Err(VerifyError::NoMatch)
        );
    }

    #[test]
    fn new_secrets_are_32_random_bytes() {
        let a = new_secret();
        assert!(a.starts_with(SECRET_PREFIX));
        assert_eq!(key_of(&a).unwrap().len(), 32);
        assert_ne!(a, new_secret());
        assert!(key_of("whsec_not base64!").is_err());
        // From the OS-seeded CSPRNG: no repeats, no fixed bytes.
        let many: std::collections::HashSet<String> = (0..2000).map(|_| new_secret()).collect();
        assert_eq!(many.len(), 2000);
        let mut ones = 0u32;
        for s in many.iter().take(200) {
            ones += key_of(s)
                .unwrap()
                .iter()
                .map(|b| b.count_ones())
                .sum::<u32>();
        }
        let bits = 200 * 32 * 8;
        assert!(
            (bits * 45 / 100..bits * 55 / 100).contains(&ones),
            "bits look random: {ones}/{bits}"
        );
        assert!(hint(&a).starts_with("whsec_…") && hint(&a).ends_with(&a[a.len() - 4..]));
        assert_eq!(hint("short"), "");
    }
}
