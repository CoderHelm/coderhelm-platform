//! Signatures between a self-hosted install and the hosted platform.
//!
//! Three things are signed, each with HMAC-SHA256 and compared in constant
//! time:
//!
//! - **Install requests** to the hosted API (claim a name, mint a token,
//!   link an installation): the install signs with its registration secret.
//!   Headers `X-CoderHelm-Install`, `X-CoderHelm-Timestamp` and
//!   `X-CoderHelm-Signature: v1=<hex>` over
//!   `"{ts}.{METHOD}.{path}.{sha256(body) hex}"`.
//! - **Relayed events** the hosted router forwards to an install: signed with
//!   the install's relay key. Header `X-CoderHelm-Relay-Signature:
//!   t=<ts>,d=<delivery id>,v1=<hex>,v2=<hex>`: `v1` over
//!   `"{ts}.{delivery id}.{body}"`, `v2` over
//!   `"v2.{ts}.{delivery id}.{source}.{event}.{body}"` ([`sign_relay_routed`]). The
//!   install refuses anything older than [`TOLERANCE_SECS`] and any delivery
//!   id it has seen (the caller claims the id; see [`replay_key`]).
//! - **Ownership proofs**: the hosted side asks an install's CloudFront
//!   address to answer a challenge with its registration secret, which proves
//!   the registration and the distribution belong together.
//!
//! The hosted side never stores an install's secrets: both are derived from
//! one hosted master key and the install id ([`install_secret`],
//! [`relay_key`]), so revoking an install is one record and rotating the
//! master key rotates every install at once.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// How old (or how far in the future) a signed request or relayed event may be.
pub const TOLERANCE_SECS: i64 = 300;

pub const INSTALL_HEADER: &str = "x-coderhelm-install";
pub const TIMESTAMP_HEADER: &str = "x-coderhelm-timestamp";
pub const SIGNATURE_HEADER: &str = "x-coderhelm-signature";
pub const RELAY_HEADER: &str = "x-coderhelm-relay-signature";
/// The relayed event's source and type, so the install routes it without parsing first.
pub const RELAY_SOURCE_HEADER: &str = "x-coderhelm-relay-source";
pub const RELAY_EVENT_HEADER: &str = "x-coderhelm-relay-event";

fn mac(key: &[u8], parts: &[&[u8]]) -> Vec<u8> {
    // HMAC takes keys of any length; new_from_slice can't fail for HMAC.
    let mut m = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    for p in parts {
        m.update(p);
    }
    m.finalize().into_bytes().to_vec()
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The registration secret for an install, from the hosted master key.
pub fn install_secret(master: &str, install_id: &str) -> String {
    hex::encode(mac(
        master.as_bytes(),
        &[b"install:", install_id.as_bytes()],
    ))
}

/// The key the hosted router signs relayed events with for an install.
pub fn relay_key(master: &str, install_id: &str) -> String {
    hex::encode(mac(master.as_bytes(), &[b"relay:", install_id.as_bytes()]))
}

/// The capability a linked install's Jira app links carry (one per Forge
/// trigger): the hosted Jira proxy takes a call only with it, for that
/// install, that trigger and that link (`linked_at`: a new link is a new
/// token), and only while the link stands.
pub fn jira_trigger_token(
    master: &str,
    install_id: &str,
    trigger: &str,
    linked_at: &str,
) -> String {
    hex::encode(mac(
        master.as_bytes(),
        &[
            b"jira-trigger:",
            install_id.as_bytes(),
            b":",
            trigger.as_bytes(),
            b":",
            linked_at.as_bytes(),
        ],
    ))
}

/// A new install id: `ins_` and 24 random lowercase letters and digits.
pub fn new_install_id() -> String {
    use rand::Rng;
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    let tail: String = (0..24)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect();
    format!("ins_{tail}")
}

/// True for an id [`new_install_id`] makes.
pub fn is_install_id(id: &str) -> bool {
    id.strip_prefix("ins_").is_some_and(|t| {
        t.len() == 24
            && t.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    })
}

fn body_hash(body: &[u8]) -> String {
    hex::encode(Sha256::digest(body))
}

/// The `v1=<hex>` signature of an install request.
pub fn sign_request(secret: &str, ts: i64, method: &str, path: &str, body: &[u8]) -> String {
    let msg = format!(
        "{ts}.{}.{path}.{}",
        method.to_ascii_uppercase(),
        body_hash(body)
    );
    format!(
        "v1={}",
        hex::encode(mac(secret.as_bytes(), &[msg.as_bytes()]))
    )
}

/// Why a signed request or relayed event was refused, in plain words with the fix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignatureProblem {
    Missing,
    Malformed,
    Stale,
    Mismatch,
    Replayed,
}

impl SignatureProblem {
    pub fn code(self) -> &'static str {
        match self {
            Self::Missing => "signature_missing",
            Self::Malformed => "signature_malformed",
            Self::Stale => "signature_stale",
            Self::Mismatch => "signature_mismatch",
            Self::Replayed => "replayed",
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::Missing => "The request wasn't signed.",
            Self::Malformed => "The signature header isn't in the expected form.",
            Self::Stale => {
                "The request is more than 5 minutes old, or the clocks differ by that much."
            }
            Self::Mismatch => "The signature doesn't match: the keys on the two sides differ.",
            Self::Replayed => "This delivery was already received once; the copy was refused.",
        }
    }

    pub fn fix(self) -> &'static str {
        match self {
            Self::Missing | Self::Malformed => "Update CoderHelm on both sides to a release with the event relay.",
            Self::Stale => "Check that your install's clock is right; AWS keeps Lambda clocks in sync, so this is usually a delayed retry and passes on its own.",
            Self::Mismatch => "Register the install again in Settings, System, Hybrid: that issues fresh keys to both sides.",
            Self::Replayed => "Nothing to do: the first copy was handled.",
        }
    }
}

fn fresh(ts: i64, now: i64) -> bool {
    (now - ts).abs() <= TOLERANCE_SECS
}

/// Checks an install request's signature and age.
pub fn verify_request(
    secret: &str,
    ts_header: Option<&str>,
    sig_header: Option<&str>,
    method: &str,
    path: &str,
    body: &[u8],
    now: i64,
) -> Result<(), SignatureProblem> {
    let (Some(ts), Some(sig)) = (ts_header, sig_header) else {
        return Err(SignatureProblem::Missing);
    };
    let ts: i64 = ts.trim().parse().map_err(|_| SignatureProblem::Malformed)?;
    let Some(hexsig) = sig.trim().strip_prefix("v1=") else {
        return Err(SignatureProblem::Malformed);
    };
    let got = hex::decode(hexsig).map_err(|_| SignatureProblem::Malformed)?;
    if !fresh(ts, now) {
        return Err(SignatureProblem::Stale);
    }
    let want = sign_request(secret, ts, method, path, body);
    let want = hex::decode(&want[3..]).unwrap_or_default();
    if !ct_eq(&got, &want) {
        return Err(SignatureProblem::Mismatch);
    }
    Ok(())
}

/// The relay header value for one forwarded event.
pub fn sign_relay(key: &str, ts: i64, delivery_id: &str, body: &[u8]) -> String {
    let sig = mac(
        key.as_bytes(),
        &[
            ts.to_string().as_bytes(),
            b".",
            delivery_id.as_bytes(),
            b".",
            body,
        ],
    );
    format!("t={ts},d={delivery_id},v1={}", hex::encode(sig))
}

/// The relay header value with the route signed too: `v1` (the body and
/// delivery id, what older installs check) and `v2`, which also covers the
/// source and event headers the install routes on.
pub fn sign_relay_routed(
    key: &str,
    ts: i64,
    delivery_id: &str,
    source: &str,
    event: &str,
    body: &[u8],
) -> String {
    format!(
        "{},v2={}",
        sign_relay(key, ts, delivery_id, body),
        hex::encode(routed_mac(key, ts, delivery_id, source, event, body))
    )
}

fn routed_mac(key: &str, ts: i64, d: &str, source: &str, event: &str, body: &[u8]) -> Vec<u8> {
    // Source and event are fixed words (no dots), so the parts can't run together.
    mac(
        key.as_bytes(),
        &[
            b"v2.",
            ts.to_string().as_bytes(),
            b".",
            d.as_bytes(),
            b".",
            source.as_bytes(),
            b".",
            event.as_bytes(),
            b".",
            body,
        ],
    )
}

/// A verified relayed event's delivery id and timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayStamp {
    pub delivery_id: String,
    pub ts: i64,
}

/// Checks a relayed event's signature and age. Both `v1` and `v2` must
/// match, so the source and event headers are the ones that were signed.
/// Replays are the caller's second
/// step: claim [`replay_key`] once, refuse when it is already held.
pub fn verify_relay(
    key: &str,
    header: Option<&str>,
    source: &str,
    event: &str,
    body: &[u8],
    now: i64,
) -> Result<RelayStamp, SignatureProblem> {
    let Some(h) = header else {
        return Err(SignatureProblem::Missing);
    };
    let (mut ts, mut d, mut v1, mut v2) = (None, None, None, None);
    for part in h.split(',') {
        let (k, v) = part
            .trim()
            .split_once('=')
            .ok_or(SignatureProblem::Malformed)?;
        match k {
            "t" => ts = v.parse::<i64>().ok(),
            "d" => d = Some(v.to_string()),
            "v1" => v1 = hex::decode(v).ok(),
            "v2" => v2 = Some(hex::decode(v).map_err(|_| SignatureProblem::Malformed)?),
            _ => {}
        }
    }
    let (Some(ts), Some(d), Some(got)) = (ts, d, v1) else {
        return Err(SignatureProblem::Malformed);
    };
    if d.is_empty()
        || d.len() > 100
        || !d
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(SignatureProblem::Malformed);
    }
    if !fresh(ts, now) {
        return Err(SignatureProblem::Stale);
    }
    let want = mac(
        key.as_bytes(),
        &[ts.to_string().as_bytes(), b".", d.as_bytes(), b".", body],
    );
    if !ct_eq(&got, &want) {
        return Err(SignatureProblem::Mismatch);
    }
    // The route (source and event) must be signed: a v1 alone is refused.
    let Some(got2) = v2 else {
        return Err(SignatureProblem::Malformed);
    };
    if !ct_eq(&got2, &routed_mac(key, ts, &d, source, event, body)) {
        return Err(SignatureProblem::Mismatch);
    }
    Ok(RelayStamp { delivery_id: d, ts })
}

/// The once-only claim that rejects a replayed relay delivery (settings table).
pub fn replay_key(delivery_id: &str) -> String {
    format!("RELAYSEEN#{delivery_id}")
}

/// How long a seen delivery id is remembered: longer than the tolerance, so a
/// copy can't come back once its id is forgotten.
pub const REPLAY_TTL_SECS: i64 = 2 * TOLERANCE_SECS + 60;

/// The answer an install gives to an ownership challenge.
pub fn proof(secret: &str, challenge: &str) -> String {
    hex::encode(mac(secret.as_bytes(), &[b"proof:", challenge.as_bytes()]))
}

/// Whether an ownership answer is right.
pub fn check_proof(secret: &str, challenge: &str, answer: &str) -> bool {
    let Ok(got) = hex::decode(answer.trim()) else {
        return false;
    };
    let want = hex::decode(proof(secret, challenge)).unwrap_or_default();
    ct_eq(&got, &want)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MASTER: &str = "hosted-master-key-for-tests";
    const NOW: i64 = 1_800_000_000;

    #[test]
    fn derived_keys_differ_per_install_and_purpose() {
        let a = install_secret(MASTER, "ins_a");
        assert_eq!(a, install_secret(MASTER, "ins_a"));
        assert_ne!(a, install_secret(MASTER, "ins_b"));
        assert_ne!(a, relay_key(MASTER, "ins_a"));
        assert_ne!(a, install_secret("another-master", "ins_a"));
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn install_ids_have_one_shape() {
        let id = new_install_id();
        assert!(is_install_id(&id), "{id}");
        assert_ne!(id, new_install_id());
        assert!(!is_install_id("ins_short"));
        assert!(!is_install_id("abc_abcdefghijklmnopqrstuvwx"));
        assert!(!is_install_id("ins_ABCDEFGHIJKLMNOPQRSTUVWX"));
    }

    #[test]
    fn a_signed_request_verifies_and_any_change_breaks_it() {
        let s = install_secret(MASTER, "ins_a");
        let body = br#"{"name":"example"}"#;
        let sig = sign_request(&s, NOW, "post", "/hybrid/names", body);
        let ts = NOW.to_string();
        assert_eq!(
            verify_request(
                &s,
                Some(&ts),
                Some(&sig),
                "POST",
                "/hybrid/names",
                body,
                NOW + 10
            ),
            Ok(())
        );
        assert_eq!(
            verify_request(
                &s,
                Some(&ts),
                Some(&sig),
                "POST",
                "/hybrid/names",
                br#"{"name":"other"}"#,
                NOW
            ),
            Err(SignatureProblem::Mismatch)
        );
        assert_eq!(
            verify_request(
                &s,
                Some(&ts),
                Some(&sig),
                "DELETE",
                "/hybrid/names",
                body,
                NOW
            ),
            Err(SignatureProblem::Mismatch)
        );
        assert_eq!(
            verify_request(
                &s,
                Some(&ts),
                Some(&sig),
                "POST",
                "/hybrid/token",
                body,
                NOW
            ),
            Err(SignatureProblem::Mismatch)
        );
        let other = install_secret(MASTER, "ins_b");
        assert_eq!(
            verify_request(
                &other,
                Some(&ts),
                Some(&sig),
                "POST",
                "/hybrid/names",
                body,
                NOW
            ),
            Err(SignatureProblem::Mismatch)
        );
    }

    #[test]
    fn the_signature_matches_the_installer_and_a_fixed_vector() {
        // installer/test/hybrid.test.mjs asserts the same value.
        assert_eq!(
            sign_request("s3cret", 1_800_000_000, "delete", "/hybrid/installs", b""),
            "v1=ae93880dea04941037494d54f0116ec78c7f934c8a4fceed86d64fa41d24a769"
        );
    }

    #[test]
    fn requests_must_be_fresh_and_well_formed() {
        let s = install_secret(MASTER, "ins_a");
        let sig = sign_request(&s, NOW, "GET", "/x", b"");
        let ts = NOW.to_string();
        assert_eq!(
            verify_request(&s, Some(&ts), Some(&sig), "GET", "/x", b"", NOW + 301),
            Err(SignatureProblem::Stale)
        );
        assert_eq!(
            verify_request(&s, Some(&ts), Some(&sig), "GET", "/x", b"", NOW - 301),
            Err(SignatureProblem::Stale)
        );
        assert_eq!(
            verify_request(&s, None, Some(&sig), "GET", "/x", b"", NOW),
            Err(SignatureProblem::Missing)
        );
        assert_eq!(
            verify_request(&s, Some(&ts), Some("sha256=00"), "GET", "/x", b"", NOW),
            Err(SignatureProblem::Malformed)
        );
        assert_eq!(
            verify_request(&s, Some("soon"), Some(&sig), "GET", "/x", b"", NOW),
            Err(SignatureProblem::Malformed)
        );
        assert_eq!(
            verify_request(&s, Some(&ts), Some("v1=zz"), "GET", "/x", b"", NOW),
            Err(SignatureProblem::Malformed)
        );
    }

    #[test]
    fn a_relayed_event_verifies_with_its_delivery_id() {
        let k = relay_key(MASTER, "ins_a");
        let body = br#"{"action":"opened"}"#;
        let h = sign_relay_routed(
            &k,
            NOW,
            "72d3162e-cc78-11e3-81ab-4c9367dc0958",
            "github",
            "push",
            body,
        );
        let stamp = verify_relay(&k, Some(&h), "github", "push", body, NOW + 5).unwrap();
        assert_eq!(stamp.delivery_id, "72d3162e-cc78-11e3-81ab-4c9367dc0958");
        assert_eq!(stamp.ts, NOW);
        assert_eq!(
            replay_key(&stamp.delivery_id),
            "RELAYSEEN#72d3162e-cc78-11e3-81ab-4c9367dc0958"
        );
    }

    #[test]
    fn a_relayed_event_is_refused_when_changed_old_or_from_another_key() {
        let k = relay_key(MASTER, "ins_a");
        let body = b"{}";
        let h = sign_relay_routed(&k, NOW, "d1", "github", "push", body);
        assert_eq!(
            verify_relay(&k, Some(&h), "github", "push", b"{ }", NOW),
            Err(SignatureProblem::Mismatch)
        );
        assert_eq!(
            verify_relay(
                &k,
                Some(&h),
                "github",
                "push",
                body,
                NOW + TOLERANCE_SECS + 1
            ),
            Err(SignatureProblem::Stale)
        );
        assert_eq!(
            verify_relay(
                &relay_key(MASTER, "ins_b"),
                Some(&h),
                "github",
                "push",
                body,
                NOW
            ),
            Err(SignatureProblem::Mismatch)
        );
        // The delivery id is signed: swapping it breaks the signature.
        let swapped = h.replace("d=d1", "d=d2");
        assert_eq!(
            verify_relay(&k, Some(&swapped), "github", "push", body, NOW),
            Err(SignatureProblem::Mismatch)
        );
        assert_eq!(
            verify_relay(&k, None, "github", "push", body, NOW),
            Err(SignatureProblem::Missing)
        );
        assert_eq!(
            verify_relay(&k, Some("t=1"), "github", "push", body, NOW),
            Err(SignatureProblem::Malformed)
        );
        assert_eq!(
            verify_relay(&k, Some("garbage"), "github", "push", body, NOW),
            Err(SignatureProblem::Malformed)
        );
        let bad_id = sign_relay(&k, NOW, "d 1", body);
        assert_eq!(
            verify_relay(&k, Some(&bad_id), "github", "push", body, NOW),
            Err(SignatureProblem::Malformed)
        );
    }

    /// The source and event headers decide how the install handles a body,
    /// so they are signed too (`v2`). A copy with them changed, or with no
    /// `v2` at all, is refused.
    #[test]
    fn the_route_of_a_relayed_event_is_signed() {
        let k = relay_key(MASTER, "ins_a");
        let body = br#"{"action":"opened"}"#;
        let h = sign_relay_routed(&k, NOW, "d1", "github", "pull_request", body);
        assert!(h.contains(",v1=") && h.contains(",v2="));
        assert_eq!(
            verify_relay(&k, Some(&h), "github", "pull_request", body, NOW).map(|s| s.delivery_id),
            Ok("d1".to_string())
        );
        for (src, ev) in [
            ("github", "issue_comment"),
            ("jira", "pull_request"),
            ("", ""),
        ] {
            assert_eq!(
                verify_relay(&k, Some(&h), src, ev, body, NOW),
                Err(SignatureProblem::Mismatch),
                "{src} {ev}"
            );
        }
        // A v2 that doesn't match is never rescued by a good v1.
        let forged = format!("{},v2={}", sign_relay(&k, NOW, "d1", body), "00".repeat(32));
        assert_eq!(
            verify_relay(&k, Some(&forged), "github", "pull_request", body, NOW),
            Err(SignatureProblem::Mismatch)
        );
        // A v1 alone is refused: the route must be signed.
        let old = sign_relay(&k, NOW, "d1", body);
        assert_eq!(
            verify_relay(&k, Some(&old), "github", "push", body, NOW),
            Err(SignatureProblem::Malformed)
        );
    }

    #[test]
    fn the_replay_window_outlives_the_tolerance() {
        const { assert!(REPLAY_TTL_SECS > 2 * TOLERANCE_SECS) };
    }

    #[test]
    fn ownership_proofs_check_out_only_with_the_secret() {
        let s = install_secret(MASTER, "ins_a");
        let p = proof(&s, "challenge-1");
        assert!(check_proof(&s, "challenge-1", &p));
        assert!(!check_proof(&s, "challenge-2", &p));
        assert!(!check_proof(
            &install_secret(MASTER, "ins_b"),
            "challenge-1",
            &p
        ));
        assert!(!check_proof(&s, "challenge-1", "not hex"));
    }

    #[test]
    fn every_problem_has_words_and_a_fix() {
        for p in [
            SignatureProblem::Missing,
            SignatureProblem::Malformed,
            SignatureProblem::Stale,
            SignatureProblem::Mismatch,
            SignatureProblem::Replayed,
        ] {
            assert!(p.message().ends_with('.') && p.fix().ends_with('.'));
        }
    }
}
