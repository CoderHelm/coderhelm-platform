//! Hybrid self-hosting: a self-hosted install that uses CoderHelm's hosted
//! pieces where that is easier, and nothing else.
//!
//! - [`names`]: `{name}.coderhelm.com` addresses, the records the hosted side
//!   may write for them.
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
pub mod integrations;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_hosted_side_needs_a_real_master_key() {
        assert!(!is_hosted_side(None));
        assert!(!is_hosted_side(Some("short")));
        assert!(is_hosted_side(Some(&"k".repeat(32))));
    }
}
