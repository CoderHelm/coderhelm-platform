//! Proof that a message to a hybrid install comes from CoderHelm.
//!
//! The relay key and registration secret ([`super::sign`]) prove that the
//! hosted side and the install share a key. This module proves something
//! stronger: that CoderHelm itself sent the message. The hosted side signs
//! with an Ed25519 key held in AWS KMS (the private key never leaves KMS),
//! and the install checks the signature with CoderHelm's public keys, which
//! come pinned in the release it runs ([`TrustedKeys::from_env`],
//! `HYBRID_HOSTED_KEYS`, set from `infra/lib/hosted-keys.ts`). The release
//! itself is signed, so the trust comes from the release signature, never
//! from anything the network says.
//!
//! What is signed (each part on its own line, so no part can run into the
//! next): the format, the key id, the install the message is for, the time,
//! the delivery id, where it came from, its type, and the body's sha256.
//!
//! - Relayed events: header `X-CoderHelm-Hosted-Signature` next to the relay
//!   key's `X-CoderHelm-Relay-Signature`; the delivery id is the relay's.
//! - Answers to an install's own calls (tokens, names, email, links): the
//!   install sends a fresh `X-CoderHelm-Request-Id`, and CoderHelm signs the
//!   answer with that id as the delivery id, source [`RESPONSE_SOURCE`] and
//!   the status, method and path as the type, so an old answer can't be
//!   passed off as a new one.
//!
//! Keys rotate by release: a release may trust two keys at once, the next
//! release adds CoderHelm's next key before the hosted side switches to it,
//! and a key is retired by a release that leaves it out.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ed25519_dalek::{Signature, VerifyingKey};
use sha2::{Digest, Sha256};

use super::sign::{SignatureProblem, TOLERANCE_SECS};

/// The signature on a message from CoderHelm.
pub const HOSTED_HEADER: &str = "x-coderhelm-hosted-signature";
/// The id an install puts on each call, which CoderHelm's answer signs.
pub const REQUEST_ID_HEADER: &str = "x-coderhelm-request-id";
/// The source of a signed answer to an install's call.
pub const RESPONSE_SOURCE: &str = "response";
/// The pinned public keys on an install (comma-separated, base64url).
pub const TRUSTED_KEYS_ENV: &str = "HYBRID_HOSTED_KEYS";
/// The KMS key (id, ARN or alias) the hosted side signs with.
pub const SIGNING_KEY_ENV: &str = "HYBRID_SIGNING_KEY";

const FORMAT: &str = "coderhelm-hosted-v1";

/// What a signature covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message<'a> {
    pub install_id: &'a str,
    pub ts: i64,
    pub delivery_id: &'a str,
    pub source: &'a str,
    pub event: &'a str,
    pub body: &'a [u8],
}

/// A short id for a public key: the first 16 hex characters of its sha256.
pub fn kid_of(public_key: &[u8; 32]) -> String {
    hex::encode(Sha256::digest(public_key))[..16].to_string()
}

/// The full fingerprint of a public key (sha256, hex), for people to compare.
pub fn fingerprint(public_key: &[u8; 32]) -> String {
    hex::encode(Sha256::digest(public_key))
}

/// A part that may appear in the signed text: no line breaks.
fn plain(v: &str) -> bool {
    !v.contains('\n') && !v.contains('\r')
}

/// The exact bytes signed, or None when a part holds a line break.
pub fn canonical(kid: &str, m: &Message<'_>) -> Option<Vec<u8>> {
    if ![kid, m.install_id, m.delivery_id, m.source, m.event]
        .iter()
        .all(|v| plain(v))
    {
        return None;
    }
    Some(
        format!(
            "{FORMAT}\n{kid}\n{}\n{}\n{}\n{}\n{}\n{}",
            m.install_id,
            m.ts,
            m.delivery_id,
            m.source,
            m.event,
            hex::encode(Sha256::digest(m.body))
        )
        .into_bytes(),
    )
}

/// The header value for a signature.
pub fn header(kid: &str, m: &Message<'_>, signature: &[u8]) -> String {
    format!(
        "k={kid},t={},d={},v1={}",
        m.ts,
        m.delivery_id,
        URL_SAFE_NO_PAD.encode(signature)
    )
}

/// The public keys an install trusts as CoderHelm's, by key id.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct TrustedKeys {
    keys: Vec<(String, [u8; 32])>,
}

impl std::fmt::Debug for TrustedKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.keys.iter().map(|(k, _)| k))
            .finish()
    }
}

impl TrustedKeys {
    /// From `HYBRID_HOSTED_KEYS`: raw Ed25519 public keys, base64url,
    /// comma-separated. Empty means this release pins no key yet.
    pub fn parse(v: &str) -> Result<Self, String> {
        let mut keys = Vec::new();
        for part in v.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let raw = URL_SAFE_NO_PAD
                .decode(part.trim_end_matches('='))
                .map_err(|_| format!("{part} isn't base64url"))?;
            let key: [u8; 32] = raw
                .try_into()
                .map_err(|_| format!("{part} isn't a 32-byte Ed25519 public key"))?;
            VerifyingKey::from_bytes(&key)
                .map_err(|_| format!("{part} isn't a valid Ed25519 public key"))?;
            let kid = kid_of(&key);
            if !keys.iter().any(|(k, _)| *k == kid) {
                keys.push((kid, key));
            }
        }
        Ok(Self { keys })
    }

    /// The keys this release pins (none when unset or unreadable: a broken
    /// value is logged, and the release then behaves as one without keys).
    pub fn from_env() -> Self {
        let v = std::env::var(TRUSTED_KEYS_ENV).unwrap_or_default();
        Self::parse(&v).unwrap_or_else(|e| {
            tracing::error!(error = %e, "hybrid: {TRUSTED_KEYS_ENV} is unreadable");
            Self::default()
        })
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Each pinned key's id and full fingerprint.
    pub fn describe(&self) -> Vec<(String, String)> {
        self.keys
            .iter()
            .map(|(k, key)| (k.clone(), fingerprint(key)))
            .collect()
    }

    /// Checks a message's CoderHelm signature, age and route. Replays are the
    /// caller's second step (claim the delivery id once), as for the relay.
    pub fn verify(
        &self,
        header: Option<&str>,
        install_id: &str,
        source: &str,
        event: &str,
        body: &[u8],
        now: i64,
    ) -> Result<Stamp, SignatureProblem> {
        let Some(h) = header.filter(|h| !h.trim().is_empty()) else {
            return Err(SignatureProblem::Missing);
        };
        let (mut kid, mut ts, mut d, mut sig) = (None, None, None, None);
        for part in h.split(',') {
            let (k, v) = part
                .trim()
                .split_once('=')
                .ok_or(SignatureProblem::Malformed)?;
            match k {
                "k" => kid = Some(v.to_string()),
                "t" => ts = Some(v.parse::<i64>().map_err(|_| SignatureProblem::Malformed)?),
                "d" => d = Some(v.to_string()),
                "v1" => {
                    sig = Some(
                        URL_SAFE_NO_PAD
                            .decode(v)
                            .map_err(|_| SignatureProblem::Malformed)?,
                    )
                }
                _ => {}
            }
        }
        let (Some(kid), Some(ts), Some(d), Some(sig)) = (kid, ts, d, sig) else {
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
        let sig = Signature::from_slice(&sig).map_err(|_| SignatureProblem::Malformed)?;
        if (now - ts).abs() > TOLERANCE_SECS {
            return Err(SignatureProblem::Stale);
        }
        let Some((_, key)) = self.keys.iter().find(|(k, _)| *k == kid) else {
            return Err(SignatureProblem::UnknownKey);
        };
        let key = VerifyingKey::from_bytes(key).map_err(|_| SignatureProblem::UnknownKey)?;
        let m = Message {
            install_id,
            ts,
            delivery_id: &d,
            source,
            event,
            body,
        };
        let msg = canonical(&kid, &m).ok_or(SignatureProblem::Malformed)?;
        key.verify_strict(&msg, &sig)
            .map_err(|_| SignatureProblem::Mismatch)?;
        Ok(Stamp {
            kid,
            delivery_id: d,
            ts,
        })
    }
}

/// A verified CoderHelm signature's key id, delivery id and time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stamp {
    pub kid: String,
    pub delivery_id: String,
    pub ts: i64,
}

/// The type a signed answer carries: its status, method and path (without
/// the query), so an answer to one call can't stand in for another.
pub fn response_event(status: u16, method: &str, path: &str) -> String {
    let path = path.split('?').next().unwrap_or("");
    format!("{status} {} {path}", method.to_ascii_uppercase())
}

/// A fresh id for one call (the delivery id its answer is signed with).
pub fn new_request_id() -> String {
    use rand::Rng;
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    let tail: String = (0..32)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect();
    format!("req_{tail}")
}

/// Where the hosted side's signatures come from.
#[derive(Clone)]
enum Backend {
    Kms {
        client: aws_sdk_kms::Client,
        key_id: String,
    },
    /// Tests only: a key in memory.
    #[cfg(test)]
    Local(ed25519_dalek::SigningKey),
}

/// The hosted side's signer: an Ed25519 key in AWS KMS.
#[derive(Clone)]
pub struct HostedSigner {
    backend: Backend,
    kid: std::sync::Arc<tokio::sync::OnceCell<String>>,
}

impl std::fmt::Debug for HostedSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostedSigner").finish_non_exhaustive()
    }
}

impl HostedSigner {
    /// The hosted side's signer when `HYBRID_SIGNING_KEY` names a KMS key.
    pub fn from_env(client: &aws_sdk_kms::Client) -> Option<Self> {
        let key_id = std::env::var(SIGNING_KEY_ENV)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())?;
        Some(Self {
            backend: Backend::Kms {
                client: client.clone(),
                key_id,
            },
            kid: Default::default(),
        })
    }

    #[cfg(test)]
    pub fn local(key: ed25519_dalek::SigningKey) -> Self {
        Self {
            backend: Backend::Local(key),
            kid: Default::default(),
        }
    }

    /// The signing key's id (read once from its public key).
    pub async fn kid(&self) -> Result<String, String> {
        self.kid
            .get_or_try_init(|| async {
                let key = self.public_key().await?;
                Ok::<_, String>(kid_of(&key))
            })
            .await
            .cloned()
    }

    /// The raw 32-byte public key.
    pub async fn public_key(&self) -> Result<[u8; 32], String> {
        match &self.backend {
            Backend::Kms { client, key_id } => {
                let out = client
                    .get_public_key()
                    .key_id(key_id)
                    .send()
                    .await
                    .map_err(|e| format!("KMS GetPublicKey failed: {}", e.into_service_error()))?;
                let der = out
                    .public_key()
                    .map(|b| b.as_ref().to_vec())
                    .unwrap_or_default();
                raw_from_spki(&der)
            }
            #[cfg(test)]
            Backend::Local(k) => Ok(k.verifying_key().to_bytes()),
        }
    }

    async fn sign_bytes(&self, msg: &[u8]) -> Result<Vec<u8>, String> {
        match &self.backend {
            Backend::Kms { client, key_id } => {
                let out = client
                    .sign()
                    .key_id(key_id)
                    .message(aws_sdk_kms::primitives::Blob::new(msg))
                    .message_type(aws_sdk_kms::types::MessageType::Raw)
                    .signing_algorithm(aws_sdk_kms::types::SigningAlgorithmSpec::Ed25519Sha512)
                    .send()
                    .await
                    .map_err(|e| format!("KMS Sign failed: {}", e.into_service_error()))?;
                Ok(out
                    .signature()
                    .map(|b| b.as_ref().to_vec())
                    .unwrap_or_default())
            }
            #[cfg(test)]
            Backend::Local(k) => {
                use ed25519_dalek::Signer;
                Ok(k.sign(msg).to_bytes().to_vec())
            }
        }
    }

    /// The header value for one message.
    pub async fn sign(&self, m: &Message<'_>) -> Result<String, String> {
        let kid = self.kid().await?;
        let msg = canonical(&kid, m).ok_or("a signed part holds a line break")?;
        let sig = self.sign_bytes(&msg).await?;
        if sig.len() != 64 {
            return Err(format!("KMS answered a {}-byte signature", sig.len()));
        }
        Ok(header(&kid, m, &sig))
    }
}

static SIGNER: std::sync::OnceLock<Option<HostedSigner>> = std::sync::OnceLock::new();

/// Sets the hosted side's signer once at start (from `HYBRID_SIGNING_KEY`;
/// none on a self-hosted install).
pub fn init_signer(client: &aws_sdk_kms::Client) {
    let _ = SIGNER.set(HostedSigner::from_env(client));
}

/// The hosted side's signer, when this deployment has one.
pub fn signer() -> Option<HostedSigner> {
    SIGNER.get().and_then(|s| s.clone())
}

static TRUSTED: std::sync::OnceLock<TrustedKeys> = std::sync::OnceLock::new();

/// The keys this install's release pins (read once).
pub fn trusted() -> &'static TrustedKeys {
    TRUSTED.get_or_init(TrustedKeys::from_env)
}

/// The raw key inside an Ed25519 SubjectPublicKeyInfo (what KMS answers):
/// a fixed 12-byte prefix and the 32 key bytes.
pub fn raw_from_spki(der: &[u8]) -> Result<[u8; 32], String> {
    const PREFIX: [u8; 12] = [
        0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
    ];
    if der.len() != 44 || der[..12] != PREFIX {
        return Err("the public key isn't an Ed25519 key".into());
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&der[12..]);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    const INSTALL: &str = "ins_abcdefghijklmnopqrstuvwx";
    const NOW: i64 = 1_761_436_800;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn pinned(keys: &[&SigningKey]) -> TrustedKeys {
        let v: Vec<String> = keys
            .iter()
            .map(|k| URL_SAFE_NO_PAD.encode(k.verifying_key().to_bytes()))
            .collect();
        TrustedKeys::parse(&v.join(",")).unwrap()
    }

    fn msg<'a>(body: &'a [u8]) -> Message<'a> {
        Message {
            install_id: INSTALL,
            ts: NOW,
            delivery_id: "72d3162e-cc78-11e3",
            source: "github",
            event: "pull_request",
            body,
        }
    }

    async fn signed(k: &SigningKey, m: &Message<'_>) -> String {
        HostedSigner::local(k.clone()).sign(m).await.unwrap()
    }

    #[tokio::test]
    async fn a_coderhelm_signature_is_accepted() {
        let k = key(1);
        let h = signed(&k, &msg(b"{}")).await;
        let s = pinned(&[&k])
            .verify(Some(&h), INSTALL, "github", "pull_request", b"{}", NOW)
            .unwrap();
        assert_eq!(s.delivery_id, "72d3162e-cc78-11e3");
        assert_eq!(s.kid, kid_of(&k.verifying_key().to_bytes()));
    }

    #[tokio::test]
    async fn another_key_is_refused() {
        let h = signed(&key(2), &msg(b"{}")).await;
        assert_eq!(
            pinned(&[&key(1)]).verify(Some(&h), INSTALL, "github", "pull_request", b"{}", NOW),
            Err(SignatureProblem::UnknownKey)
        );
    }

    #[tokio::test]
    async fn a_pinned_kid_with_another_keys_signature_is_refused() {
        // Claim key 1's id, sign with key 2.
        let m = msg(b"{}");
        let kid1 = kid_of(&key(1).verifying_key().to_bytes());
        let forged = {
            use ed25519_dalek::Signer;
            let sig = key(2).sign(&canonical(&kid1, &m).unwrap());
            header(&kid1, &m, &sig.to_bytes())
        };
        assert_eq!(
            pinned(&[&key(1)]).verify(Some(&forged), INSTALL, "github", "pull_request", b"{}", NOW),
            Err(SignatureProblem::Mismatch)
        );
    }

    #[tokio::test]
    async fn any_change_to_what_was_signed_is_refused() {
        let k = key(1);
        let keys = pinned(&[&k]);
        let h = signed(&k, &msg(b"{\"a\":1}")).await;
        let bad = |install: &str, source: &str, event: &str, body: &[u8]| {
            keys.verify(Some(&h), install, source, event, body, NOW)
        };
        assert_eq!(
            bad(INSTALL, "github", "pull_request", b"{\"a\":2}"),
            Err(SignatureProblem::Mismatch)
        );
        assert_eq!(
            bad(
                "ins_bbbbbbbbbbbbbbbbbbbbbbbb",
                "github",
                "pull_request",
                b"{\"a\":1}"
            ),
            Err(SignatureProblem::Mismatch)
        );
        assert_eq!(
            bad(INSTALL, "jira", "pull_request", b"{\"a\":1}"),
            Err(SignatureProblem::Mismatch)
        );
        assert_eq!(
            bad(INSTALL, "github", "push", b"{\"a\":1}"),
            Err(SignatureProblem::Mismatch)
        );
        // Another delivery id or time in the header.
        let moved = h.replace("d=72d3162e-cc78-11e3", "d=other-id");
        assert_eq!(
            keys.verify(
                Some(&moved),
                INSTALL,
                "github",
                "pull_request",
                b"{\"a\":1}",
                NOW
            ),
            Err(SignatureProblem::Mismatch)
        );
        let later = h.replace(&format!("t={NOW}"), &format!("t={}", NOW + 10));
        assert_eq!(
            keys.verify(
                Some(&later),
                INSTALL,
                "github",
                "pull_request",
                b"{\"a\":1}",
                NOW
            ),
            Err(SignatureProblem::Mismatch)
        );
    }

    #[tokio::test]
    async fn a_stale_signature_is_refused() {
        let k = key(1);
        let h = signed(&k, &msg(b"{}")).await;
        assert_eq!(
            pinned(&[&k]).verify(
                Some(&h),
                INSTALL,
                "github",
                "pull_request",
                b"{}",
                NOW + TOLERANCE_SECS + 1
            ),
            Err(SignatureProblem::Stale)
        );
    }

    #[tokio::test]
    async fn two_pinned_keys_both_verify_during_a_rotation() {
        let (old, next) = (key(1), key(3));
        let keys = pinned(&[&old, &next]);
        for k in [&old, &next] {
            let h = signed(k, &msg(b"{}")).await;
            assert!(keys
                .verify(Some(&h), INSTALL, "github", "pull_request", b"{}", NOW)
                .is_ok());
        }
        // A release that leaves the old key out refuses it.
        let h = signed(&old, &msg(b"{}")).await;
        assert_eq!(
            pinned(&[&next]).verify(Some(&h), INSTALL, "github", "pull_request", b"{}", NOW),
            Err(SignatureProblem::UnknownKey)
        );
    }

    #[test]
    fn missing_or_malformed_headers_are_refused() {
        let keys = pinned(&[&key(1)]);
        let v = |h: Option<&str>| keys.verify(h, INSTALL, "github", "pull_request", b"{}", NOW);
        assert_eq!(v(None), Err(SignatureProblem::Missing));
        assert_eq!(v(Some(" ")), Err(SignatureProblem::Missing));
        assert_eq!(v(Some("nonsense")), Err(SignatureProblem::Malformed));
        assert_eq!(v(Some("k=a,t=1,d=x")), Err(SignatureProblem::Malformed));
        assert_eq!(
            v(Some("k=a,t=1,d=x\ny,v1=AAAA")),
            Err(SignatureProblem::Malformed)
        );
    }

    #[test]
    fn line_breaks_never_reach_the_signed_text() {
        let mut m = msg(b"{}");
        m.event = "push\nother";
        assert!(canonical("abc", &m).is_none());
    }

    #[test]
    fn pinned_keys_parse_strictly_and_never_print_the_key() {
        let k = key(1).verifying_key().to_bytes();
        let b64 = URL_SAFE_NO_PAD.encode(k);
        let t = TrustedKeys::parse(&format!(" {b64} ,{b64},")).unwrap();
        assert_eq!(t.describe().len(), 1, "the same key once");
        assert_eq!(t.describe()[0].1, fingerprint(&k));
        assert!(!format!("{t:?}").contains(&b64));
        assert!(TrustedKeys::parse("").unwrap().is_empty());
        assert!(TrustedKeys::parse("not base64!").is_err());
        assert!(TrustedKeys::parse(&URL_SAFE_NO_PAD.encode([1u8; 16])).is_err());
    }

    #[test]
    fn kms_public_keys_unwrap_to_the_raw_key() {
        let raw = key(4).verifying_key().to_bytes();
        let mut der = vec![
            0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
        ];
        der.extend_from_slice(&raw);
        assert_eq!(raw_from_spki(&der).unwrap(), raw);
        assert!(raw_from_spki(&der[..40]).is_err());
        let mut p256 = der.clone();
        p256[8] = 0x71;
        assert!(raw_from_spki(&p256).is_err());
    }

    #[test]
    fn an_answer_signs_its_status_method_and_path() {
        assert_eq!(
            response_event(200, "post", "/hybrid/github/token?x=1"),
            "200 POST /hybrid/github/token"
        );
        let a = new_request_id();
        assert!(a.starts_with("req_") && a.len() == 36);
        assert_ne!(a, new_request_id());
    }

    #[test]
    fn the_signer_never_prints_its_key() {
        let s = HostedSigner::local(key(9));
        assert!(!format!("{s:?}").contains("9"));
    }
}
