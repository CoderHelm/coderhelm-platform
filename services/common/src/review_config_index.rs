//! The teams that have a per-repository reviewer config
//! (`pk = <team>`, `sk = REVIEW_CONFIG#REPO#<owner/repo>`), kept as a
//! [`settings_index`](crate::settings_index) so the review sweeps read the
//! configs by key.
//!
//! A member is the config's own partition key, so a team is found however
//! it is recorded elsewhere (with or without a record in the teams table).
//! The gateway adds the team when it saves a config; the daily rebuild finds
//! any config saved before the index existed.

use aws_sdk_dynamodb::types::AttributeValue;

use crate::settings_index::{Item, Spec};

/// `INDEX#REVIEW_CONFIG`.
pub const NAME: &str = "REVIEW_CONFIG";
pub const CONFIG_PREFIX: &str = "REVIEW_CONFIG#REPO#";

/// The repository a config's sort key names, when it is one.
pub fn repo_of(sk: &str) -> Option<&str> {
    sk.strip_prefix(CONFIG_PREFIX).filter(|r| r.contains('/'))
}

/// The team a record stands for in the index: its partition key, when the
/// record is a repository config.
pub fn member_of(item: &Item) -> Option<String> {
    let sk = item.get("sk")?.as_s().ok()?;
    repo_of(sk)?;
    let pk = item.get("pk")?.as_s().ok()?;
    (!pk.is_empty() && !pk.starts_with("INDEX#")).then(|| pk.clone())
}

/// How the daily rebuild finds every config.
pub fn spec() -> Spec<'static> {
    Spec {
        name: NAME,
        filter: "begins_with(sk, :pfx)",
        values: vec![(":pfx", AttributeValue::S(CONFIG_PREFIX.to_string()))],
        member_of,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn item(pk: &str, sk: &str) -> Item {
        HashMap::from([
            ("pk".to_string(), AttributeValue::S(pk.to_string())),
            ("sk".to_string(), AttributeValue::S(sk.to_string())),
        ])
    }

    #[test]
    fn a_repository_config_stands_for_its_own_team() {
        // A team known only by its members (no teams-table record) is still
        // indexed: the member is the config's partition key.
        assert_eq!(
            member_of(&item("TEAM#members-only", "REVIEW_CONFIG#REPO#acme/web")).as_deref(),
            Some("TEAM#members-only")
        );
    }

    #[test]
    fn other_records_are_not_members() {
        assert_eq!(member_of(&item("TEAM#a", "REVIEW_CONFIG#GLOBAL")), None);
        assert_eq!(member_of(&item("TEAM#a", "REVIEW_CONFIG#REPO#nope")), None);
        assert_eq!(member_of(&item("TEAM#a", "ANALYTICS#deploys")), None);
        assert_eq!(
            member_of(&item("INDEX#REVIEW_CONFIG", "REVIEW_CONFIG#REPO#a/b")),
            None
        );
    }

    #[test]
    fn the_index_has_its_own_partition() {
        assert_eq!(
            crate::settings_index::partition(NAME),
            "INDEX#REVIEW_CONFIG"
        );
    }
}
