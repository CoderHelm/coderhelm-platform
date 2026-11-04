//! A re-review started by an edit of a pull request's description.
//!
//! A review that asks the author to confirm facts ("Confirm before merge")
//! or requests changes is often answered in the description: the author adds
//! what the real system returned. That edit is the answer, so it starts a
//! review of the same commit with the new description. The gateway decides
//! whether an edit counts and queues the review; the worker reviews the
//! description as it is when the job runs. Both use the keys here.

use sha2::{Digest, Sha256};

/// The review trigger for an edited description.
pub const TRIGGER: &str = "description";

/// How long a description review waits in the queue, so a burst of edits
/// settles first and one review reads the last of them.
pub const SETTLE_SECS: i32 = 120;

/// How long the first edit of a burst holds the queued slot. Shorter than
/// [`SETTLE_SECS`]: an edit after it ends may land after the queued review
/// read the description, and then queues its own (which the worker drops
/// when it finds that text already reviewed).
pub const SETTLE_CLAIM_SECS: u64 = 110;

/// How long a reviewed (commit, description) pair is not reviewed again.
pub const SEEN_TTL_SECS: u64 = 14 * 24 * 3600;

/// The description's words with what doesn't change its meaning removed:
/// runs of whitespace are one space, and every task-list box reads as
/// unchecked, so ticking a box or re-wrapping a line is not an edit.
pub fn normalized(body: &str) -> String {
    let unticked = body
        .replace("[x]", "[ ]")
        .replace("[X]", "[ ]")
        .replace("[]", "[ ]");
    unticked.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Whether an edit from `old` to `new` changed what the description says.
pub fn changed(old: &str, new: &str) -> bool {
    normalized(old) != normalized(new)
}

/// A short hash of the normalized description.
pub fn body_hash(body: &str) -> String {
    let digest = Sha256::digest(normalized(body).as_bytes());
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

/// The marker for a (commit, description) pair already queued or reviewed.
pub fn seen_sk(owner: &str, name: &str, pr: u64, head: &str, hash: &str) -> String {
    format!("REVIEWDESC#{owner}/{name}#{pr:06}#{head}#{hash}")
}

/// The slot the first edit of a burst holds while its review waits.
pub fn settle_sk(owner: &str, name: &str, pr: u64, head: &str) -> String {
    format!("REVIEWDESCWAIT#{owner}/{name}#{pr:06}#{head}")
}

/// The review job's dedup key for one (commit, description) pair.
pub fn dedup_key(head: &str, hash: &str) -> String {
    format!("desc#{head}#{hash}")
}

/// The line a review started by a description edit opens with.
pub const SUMMARY_NOTE: &str = "Re-reviewed after the description was updated.";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whitespace_and_checkboxes_are_not_changes() {
        let old = "## Verification\n- [ ] ran against a restore\n\nok";
        assert!(!changed(
            old,
            "## Verification\n- [x] ran against  a restore\n\n\nok  "
        ));
        assert!(!changed(
            old,
            "## Verification\r\n- [X] ran against a restore\r\nok"
        ));
        assert!(!changed("a\nb", "a b"));
    }

    #[test]
    fn new_words_are_changes() {
        let old = "## Verification\n- unit tests";
        let new = "## Verification\n- unit tests\n- restore of real data: 4,210 rows, 0 errors";
        assert!(changed(old, new));
        assert!(changed("", "Ran it on real data."));
    }

    #[test]
    fn the_hash_follows_meaning_not_formatting() {
        assert_eq!(body_hash("a  b\n- [x] c"), body_hash("a b - [ ] c"));
        assert_ne!(body_hash("a b"), body_hash("a b c"));
        assert_eq!(body_hash("x").len(), 16);
    }

    #[test]
    fn keys_name_the_commit_and_the_text() {
        assert_eq!(
            seen_sk("acme", "web", 42, "abc", "f00d"),
            "REVIEWDESC#acme/web#000042#abc#f00d"
        );
        assert_eq!(
            settle_sk("acme", "web", 42, "abc"),
            "REVIEWDESCWAIT#acme/web#000042#abc"
        );
        assert_eq!(dedup_key("abc", "f00d"), "desc#abc#f00d");
        assert!(SETTLE_CLAIM_SECS < SETTLE_SECS as u64);
    }
}
