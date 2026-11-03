//! Hybrid self-hosting: a self-hosted install that uses CoderHelm's hosted
//! pieces where that is easier, and nothing else.
//!
//! - [`names`]: `{name}.coderhelm.com` addresses, the records the hosted side
//!   may write for them.
//! - [`name_check`]: the 15-minute check that every name still points only
//!   where CoderHelm put it and still reaches its install.
//! - [`hosted_sig`]: CoderHelm's own signature on what it sends an install
//!   (Ed25519 in AWS KMS; the install checks it with keys its release pins).
//! - [`sign`]: the signatures between an install and the hosted side (install
//!   requests, relayed events, ownership proofs), with replay protection.
//! - [`broker`]: what the hosted token broker may mint for an install.
//! - [`client`]: the install's registration and its signed calls to the hosted side.
//! - [`relay`]: forwarding one event to an install, with retries.
//! - [`integrations`]: which integrations work through CoderHelm's apps, and
//!   what that means for each.
//!
//! The trust model, in short: the install keeps its code, runs and data.
//! CoderHelm's servers see relayed event payloads in transit (held only while
//! a forward is retried, at most 24 hours), keep delivery records (ids,
//! types, status) for 14 days, hold the
//! hosted apps' keys and, for the OAuth integrations an install chose, the
//! encrypted sign-ins. docs/self-host/hybrid.md says the same in full.

pub mod broker;
pub mod client;
pub mod hosted_sig;
pub mod integrations;
pub mod name_check;
pub mod names;
pub mod relay;
pub mod sign;

/// The hosted platform's API, which hybrid installs talk to.
pub const DEFAULT_HOSTED_URL: &str = "https://api.coderhelm.com";

/// The hosted URL this install uses (`HYBRID_URL`, else the default). Tests
/// and staging point it elsewhere.
pub fn hosted_url() -> String {
    std::env::var("HYBRID_URL")
        .ok()
        .map(|v| v.trim().trim_end_matches('/').to_string())
        .filter(|v| v.starts_with("https://") || v.starts_with("http://localhost"))
        .unwrap_or_else(|| DEFAULT_HOSTED_URL.to_string())
}

/// Whether the install's config turned hybrid off (`hybridUrl: null`).
pub fn turned_off() -> bool {
    std::env::var("HYBRID_URL").is_ok_and(|v| v.trim() == "off")
}

/// Whether this deployment is the hosted side of hybrid (it has the master
/// key and serves `/hybrid/*`). Self-hosted installs never are.
pub fn is_hosted_side(master_key: Option<&str>) -> bool {
    master_key.is_some_and(|k| k.trim().len() >= 32)
}

/// The master key from its own secret's value: the key itself, or a JSON
/// object holding it as `hybrid_master_key`. None when neither is a real key.
pub fn master_key_from_secret(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let key = if raw.starts_with('{') {
        serde_json::from_str::<serde_json::Value>(raw)
            .ok()?
            .get("hybrid_master_key")?
            .as_str()?
            .trim()
            .to_string()
    } else {
        raw.to_string()
    };
    is_hosted_side(Some(&key)).then_some(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_master_key_reads_from_its_own_secret_plain_or_json() {
        let k = "k".repeat(40);
        assert_eq!(master_key_from_secret(&k).as_deref(), Some(k.as_str()));
        assert_eq!(
            master_key_from_secret(&format!("  {k}\n")).as_deref(),
            Some(k.as_str())
        );
        let j = format!("{{\"hybrid_master_key\":\"{k}\"}}");
        assert_eq!(master_key_from_secret(&j).as_deref(), Some(k.as_str()));
        assert_eq!(master_key_from_secret("short"), None);
        assert_eq!(master_key_from_secret("{\"other\":\"x\"}"), None);
        assert_eq!(master_key_from_secret("{not json"), None);
    }

    #[test]
    fn the_hosted_side_needs_a_real_master_key() {
        assert!(!is_hosted_side(None));
        assert!(!is_hosted_side(Some("short")));
        assert!(is_hosted_side(Some(&"k".repeat(32))));
    }
}
