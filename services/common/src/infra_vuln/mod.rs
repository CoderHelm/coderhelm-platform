//! Infrastructure vulnerabilities: what a team runs on AWS (AMIs and
//! kernels, node groups, databases and caches, container base images),
//! matched by version against public advisories, with AWS's own split of
//! who has to act.
//!
//! - [`feeds`]: the public sources, read once a day for every team and kept
//!   under [`GLOBAL_PK`] in the settings table: Amazon Linux advisories
//!   (ALAS) for AL2 and AL2023, Bottlerocket advisories, AWS security
//!   bulletins with each page's per-service status, the Lambda runtime
//!   deprecation table and AWS's public "latest AMI" parameters. A source
//!   that can't be read or parsed keeps its last good copy
//!   ([`feeds::FeedStatus`]).
//!
//! - [`inventory`] and [`raw`]: what a team runs, read once a day through
//!   the optional "Infrastructure vulnerabilities" permissions of its
//!   CoderHelmLogReader role (`RES#` records in the AWS insights table).
//!
//! Nothing here calls a model: every match is a version comparison.

pub mod feeds;
pub mod inventory;
pub mod raw;
pub mod store;

/// Global records live under the same partition as the CISA list.
pub const GLOBAL_PK: &str = crate::security_fixes::GLOBAL_PK;

/// One status row per feed: when it last parsed, and the last error.
pub const FEED_STATUS_PREFIX: &str = "INFRA_FEED#";
/// One row per Amazon Linux or Bottlerocket advisory.
pub const ADVISORY_PREFIX: &str = "INFRA_ADV#";
/// One row per AWS security bulletin.
pub const BULLETIN_PREFIX: &str = "INFRA_BULLETIN#";
/// The Lambda runtime table, one row.
pub const RUNTIMES_SK: &str = "INFRA_RUNTIMES";
/// AWS's public "latest AMI" parameters, one row.
pub const LATEST_IMAGES_SK: &str = "INFRA_LATEST_IMAGES";
/// The day's read of the feeds, claimed once (`INFRA_FEEDS#<yyyymmdd>`).
pub const SYNC_CLAIM_PREFIX: &str = "INFRA_FEEDS#";

/// Advisories and bulletins are kept this long after their last read.
pub const KEEP_DAYS: i64 = 730;

pub fn feed_status_sk(feed: &str) -> String {
    format!("{FEED_STATUS_PREFIX}{feed}")
}

pub fn advisory_sk(id: &str) -> String {
    format!("{ADVISORY_PREFIX}{id}")
}

pub fn bulletin_sk(id: &str) -> String {
    format!("{BULLETIN_PREFIX}{}", id.to_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_sit_under_the_global_partition() {
        assert_eq!(GLOBAL_PK, "SECURITY#GLOBAL");
        assert_eq!(feed_status_sk("alas2023"), "INFRA_FEED#alas2023");
        assert_eq!(
            advisory_sk("ALAS2023-2026-3365"),
            "INFRA_ADV#ALAS2023-2026-3365"
        );
        assert_eq!(bulletin_sk("2026-160-aws"), "INFRA_BULLETIN#2026-160-AWS");
    }
}
