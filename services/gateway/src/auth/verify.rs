use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Verify GitHub webhook signature (HMAC-SHA256).
/// Header: X-Hub-Signature-256: sha256=<hex>
pub fn verify_github_signature(secret: &str, body: &[u8], signature_header: &str) -> bool {
    let expected_prefix = "sha256=";
    let hex_sig = match signature_header.strip_prefix(expected_prefix) {
        Some(s) => s,
        None => return false,
    };

    let Ok(received_bytes) = hex::decode(hex_sig) else {
        return false;
    };

    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(body);
    let computed = mac.finalize().into_bytes();

    computed.as_slice().ct_eq(&received_bytes).into()
}

/// Verify Jira webhook signature (HMAC-SHA256).
/// Header: X-Hub-Signature: sha256=<hex>
pub fn verify_jira_signature(secret: &str, body: &[u8], signature_header: &str) -> bool {
    // Same HMAC-SHA256 scheme
    verify_github_signature(secret, body, signature_header)
}

/// Verify Stripe webhook signature.
/// Uses Stripe's `t=timestamp,v1=signature` format in Stripe-Signature header.
pub fn verify_stripe_signature(
    secret: &str,
    body: &[u8],
    signature_header: &str,
    tolerance_secs: u64,
) -> bool {
    let mut timestamp: Option<&str> = None;
    let mut signatures: Vec<&str> = Vec::new();

    for part in signature_header.split(',') {
        if let Some(t) = part.strip_prefix("t=") {
            timestamp = Some(t);
        } else if let Some(v) = part.strip_prefix("v1=") {
            signatures.push(v);
        }
    }

    let Some(ts_str) = timestamp else {
        return false;
    };
    let Ok(ts) = ts_str.parse::<u64>() else {
        return false;
    };

    // Check timestamp tolerance
    let now = chrono::Utc::now().timestamp() as u64;
    if now.abs_diff(ts) > tolerance_secs {
        return false;
    }

    // Compute expected signature: HMAC-SHA256(secret, "timestamp.body")
    let signed_payload = format!("{ts_str}.");
    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(signed_payload.as_bytes());
    mac.update(body);
    let computed = hex::encode(mac.finalize().into_bytes());

    // Check if any v1 signature matches
    signatures
        .iter()
        .any(|sig| computed.as_bytes().ct_eq(sig.as_bytes()).into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify_github_signature_valid() {
        let secret = "test-secret";
        let body = b"test body";
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let sig = hex::encode(mac.finalize().into_bytes());
        let header = format!("sha256={sig}");
        assert!(verify_github_signature(secret, body, &header));
    }

    #[test]
    fn test_verify_github_signature_invalid() {
        assert!(!verify_github_signature(
            "secret",
            b"body",
            "sha256=deadbeef"
        ));
    }

    #[test]
    fn test_verify_github_signature_bad_prefix() {
        assert!(!verify_github_signature("secret", b"body", "md5=deadbeef"));
    }
}
