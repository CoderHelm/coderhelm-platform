//! Logic shared between the gateway and worker services.

use sha2::{Digest, Sha256};

/// Stable content hash for a ticket's context: title + body + image keys.
///
/// The gateway computes this at webhook time to decide whether a ticket
/// update actually changed anything worth re-running; the worker stores it
/// on the run record and uses it for plan-cache invalidation. Both sides
/// MUST agree on the value, which is why this lives in a shared crate and
/// uses SHA-256 rather than std's DefaultHasher (whose output is not
/// guaranteed stable across Rust releases or compilations).
pub fn ticket_context_hash(title: &str, body: &str, image_keys: &[String]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(title.as_bytes());
    hasher.update([0u8]);
    hasher.update(body.as_bytes());
    for key in image_keys {
        hasher.update([0u8]);
        hasher.update(key.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_stable_and_sensitive() {
        let a = ticket_context_hash("t", "b", &["k1".into()]);
        assert_eq!(a, ticket_context_hash("t", "b", &["k1".into()]));
        assert_ne!(a, ticket_context_hash("t", "b2", &["k1".into()]));
        assert_ne!(a, ticket_context_hash("t", "b", &[]));
        // Field boundaries matter: ("ab","c") != ("a","bc")
        assert_ne!(
            ticket_context_hash("ab", "c", &[]),
            ticket_context_hash("a", "bc", &[])
        );
    }
}
