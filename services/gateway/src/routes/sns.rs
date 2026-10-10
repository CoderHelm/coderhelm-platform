//! Verify that a message posted to an HTTPS endpoint really came from AWS SNS.
//!
//! SNS signs every message with the private key of a certificate it serves
//! from `https://sns.<region>.amazonaws.com/…pem`. Verification: the
//! certificate URL must be an SNS host, the certificate's RSA key must verify
//! the signature over the canonical string of the message's fields
//! (SignatureVersion 1 = SHA1, 2 = SHA256, PKCS#1 v1.5).

use rsa::pkcs1v15::{Signature, VerifyingKey};
use rsa::pkcs8::DecodePublicKey;
use rsa::signature::Verifier;
use rsa::RsaPublicKey;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use x509_cert::der::{DecodePem, Encode};
use x509_cert::Certificate;

/// The fields of an SNS HTTP(S) message that matter here.
#[derive(Debug, Deserialize, Clone)]
pub struct SnsMessage {
    #[serde(rename = "Type")]
    pub kind: String,
    #[serde(rename = "MessageId")]
    pub message_id: String,
    #[serde(rename = "TopicArn")]
    pub topic_arn: String,
    #[serde(rename = "Subject", default)]
    pub subject: Option<String>,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Timestamp")]
    pub timestamp: String,
    #[serde(rename = "Token", default)]
    pub token: Option<String>,
    #[serde(rename = "SubscribeURL", default)]
    pub subscribe_url: Option<String>,
    #[serde(rename = "SignatureVersion")]
    pub signature_version: String,
    #[serde(rename = "Signature")]
    pub signature: String,
    #[serde(rename = "SigningCertURL")]
    pub signing_cert_url: String,
}

/// Is this URL served by SNS itself? Only such URLs are fetched (signing
/// certificates) or visited (subscription confirmation).
pub fn is_sns_url(url: &str, path_suffix: Option<&str>) -> bool {
    let Some(rest) = url.strip_prefix("https://") else {
        return false;
    };
    let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
    let host_ok = host
        .strip_prefix("sns.")
        .and_then(|h| {
            h.strip_suffix(".amazonaws.com")
                .or_else(|| h.strip_suffix(".amazonaws.com.cn"))
        })
        .is_some_and(|region| {
            !region.is_empty()
                && region
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        });
    host_ok && path_suffix.is_none_or(|sfx| path.split('?').next().unwrap_or("").ends_with(sfx))
}

/// Pure: the string SNS signs, per message type.
pub fn canonical_string(m: &SnsMessage) -> Option<String> {
    let mut fields: Vec<(&str, &str)> = vec![("Message", &m.message), ("MessageId", &m.message_id)];
    match m.kind.as_str() {
        "Notification" => {
            if let Some(subject) = m.subject.as_deref() {
                fields.push(("Subject", subject));
            }
            fields.push(("Timestamp", &m.timestamp));
            fields.push(("TopicArn", &m.topic_arn));
            fields.push(("Type", &m.kind));
        }
        "SubscriptionConfirmation" | "UnsubscribeConfirmation" => {
            fields.push(("SubscribeURL", m.subscribe_url.as_deref()?));
            fields.push(("Timestamp", &m.timestamp));
            fields.push(("Token", m.token.as_deref()?));
            fields.push(("TopicArn", &m.topic_arn));
            fields.push(("Type", &m.kind));
        }
        _ => return None,
    }
    let mut out = String::new();
    for (k, v) in fields {
        out.push_str(k);
        out.push('\n');
        out.push_str(v);
        out.push('\n');
    }
    Some(out)
}

/// Signing certificates by URL; they rotate rarely and are reused for every
/// message, so each warm Lambda fetches each one once.
fn cert_cache() -> &'static Mutex<HashMap<String, RsaPublicKey>> {
    static CACHE: OnceLock<Mutex<HashMap<String, RsaPublicKey>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn signing_key(http: &reqwest::Client, url: &str) -> Result<RsaPublicKey, String> {
    if let Some(k) = cert_cache().lock().ok().and_then(|c| c.get(url).cloned()) {
        return Ok(k);
    }
    let pem = http
        .get(url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| format!("fetch signing cert: {e}"))?
        .text()
        .await
        .map_err(|e| format!("read signing cert: {e}"))?;
    let key = public_key_from_pem(&pem)?;
    if let Ok(mut c) = cert_cache().lock() {
        c.insert(url.to_string(), key.clone());
    }
    Ok(key)
}

fn public_key_from_pem(pem: &str) -> Result<RsaPublicKey, String> {
    let cert = Certificate::from_pem(pem.as_bytes()).map_err(|e| format!("parse cert: {e}"))?;
    let spki = cert
        .tbs_certificate
        .subject_public_key_info
        .to_der()
        .map_err(|e| format!("encode key: {e}"))?;
    RsaPublicKey::from_public_key_der(&spki).map_err(|e| format!("rsa key: {e}"))
}

/// Pure: verify `signature_b64` over `canonical` with `key`.
pub fn verify_with_key(
    key: &RsaPublicKey,
    version: &str,
    canonical: &str,
    signature_b64: &str,
) -> Result<(), String> {
    use base64::Engine;
    let sig_bytes = base64::engine::general_purpose::STANDARD
        .decode(signature_b64)
        .map_err(|e| format!("signature encoding: {e}"))?;
    let sig = Signature::try_from(sig_bytes.as_slice()).map_err(|e| format!("signature: {e}"))?;
    let ok = match version {
        "1" => VerifyingKey::<sha1::Sha1>::new(key.clone()).verify(canonical.as_bytes(), &sig),
        "2" => VerifyingKey::<sha2::Sha256>::new(key.clone()).verify(canonical.as_bytes(), &sig),
        other => return Err(format!("unsupported SignatureVersion {other}")),
    };
    ok.map_err(|_| "signature does not match".to_string())
}

/// Verify a message end to end. Err explains why it was rejected.
pub async fn verify(http: &reqwest::Client, m: &SnsMessage) -> Result<(), String> {
    if !is_sns_url(&m.signing_cert_url, Some(".pem")) {
        return Err(format!(
            "signing cert URL is not SNS: {}",
            m.signing_cert_url
        ));
    }
    let canonical = canonical_string(m).ok_or("unsupported message type or missing fields")?;
    let key = signing_key(http, &m.signing_cert_url).await?;
    verify_with_key(&key, &m.signature_version, &canonical, &m.signature)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs1v15::SigningKey;
    use rsa::signature::{SignatureEncoding, Signer};

    fn msg(kind: &str) -> SnsMessage {
        SnsMessage {
            kind: kind.into(),
            message_id: "m-1".into(),
            topic_arn: "arn:aws:sns:us-east-1:111122223333:alarms".into(),
            subject: Some("ALARM: x".into()),
            message: "{\"AlarmName\":\"x\"}".into(),
            timestamp: "2026-10-09T23:00:00.000Z".into(),
            token: Some("tok".into()),
            subscribe_url: Some(
                "https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription".into(),
            ),
            signature_version: "2".into(),
            signature: String::new(),
            signing_cert_url:
                "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-abc.pem".into(),
        }
    }

    #[test]
    fn only_sns_hosts_are_trusted() {
        assert!(is_sns_url(
            "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-1.pem",
            Some(".pem")
        ));
        assert!(is_sns_url(
            "https://sns.cn-north-1.amazonaws.com.cn/x.pem",
            Some(".pem")
        ));
        assert!(!is_sns_url(
            "http://sns.us-east-1.amazonaws.com/x.pem",
            Some(".pem")
        ));
        assert!(!is_sns_url(
            "https://sns.us-east-1.amazonaws.com.evil.com/x.pem",
            Some(".pem")
        ));
        assert!(!is_sns_url(
            "https://evil.com/sns.us-east-1.amazonaws.com/x.pem",
            Some(".pem")
        ));
        assert!(!is_sns_url(
            "https://sns.us-east-1.amazonaws.com/x.txt",
            Some(".pem")
        ));
        assert!(is_sns_url(
            "https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&Token=t",
            None
        ));
    }

    #[test]
    fn canonical_strings_follow_the_sns_field_order() {
        let n = canonical_string(&msg("Notification")).unwrap();
        assert_eq!(
            n,
            "Message\n{\"AlarmName\":\"x\"}\nMessageId\nm-1\nSubject\nALARM: x\nTimestamp\n2026-10-09T23:00:00.000Z\nTopicArn\narn:aws:sns:us-east-1:111122223333:alarms\nType\nNotification\n"
        );
        let mut no_subject = msg("Notification");
        no_subject.subject = None;
        assert!(!canonical_string(&no_subject).unwrap().contains("Subject"));
        let c = canonical_string(&msg("SubscriptionConfirmation")).unwrap();
        assert!(c.contains("SubscribeURL\n") && c.contains("Token\ntok\n"));
        assert!(canonical_string(&msg("Bogus")).is_none());
    }

    #[test]
    fn signatures_verify_and_tampering_fails() {
        use base64::Engine;
        let mut rng = rand08::thread_rng();
        let private = rsa::RsaPrivateKey::new(&mut rng, 1024).unwrap();
        let public = private.to_public_key();
        let m = msg("Notification");
        let canonical = canonical_string(&m).unwrap();
        for (version, sig) in [
            (
                "1",
                SigningKey::<sha1::Sha1>::new(private.clone())
                    .sign(canonical.as_bytes())
                    .to_vec(),
            ),
            (
                "2",
                SigningKey::<sha2::Sha256>::new(private.clone())
                    .sign(canonical.as_bytes())
                    .to_vec(),
            ),
        ] {
            let b64 = base64::engine::general_purpose::STANDARD.encode(&sig);
            assert!(
                verify_with_key(&public, version, &canonical, &b64).is_ok(),
                "v{version}"
            );
            let tampered = canonical.replace("m-1", "m-2");
            assert!(verify_with_key(&public, version, &tampered, &b64).is_err());
        }
        assert!(verify_with_key(&public, "3", &canonical, "AAAA").is_err());
    }
}
