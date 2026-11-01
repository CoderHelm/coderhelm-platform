//! A small, time-ordered record of every deploy CoderHelm itself started.
//!
//! CoderHelm starts a deploy by adding a label the repo's own CI reacts to
//! (`CI:DEPLOY_STAGING`, `deploy:staging`, the repo's configured deploy
//! label): on its own PR when it opens it, from a review, or at the merge
//! gate. Each label it adds that way writes one of these, keyed by time, so a
//! period is one range query. Deploys from before recording are copied once
//! from GitHub's label history (`BACKFILL_MARKER_SK`).

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

pub const PREFIX: &str = "DSTAT#";
/// Set on a team once its earlier deploys were copied from GitHub.
pub const BACKFILL_MARKER_SK: &str = "ANALYTICS#deploy-stats-backfilled#v1";
/// Set when the copy was last asked for, so a page load asks at most hourly.
pub const BACKFILL_QUEUED_SK: &str = "ANALYTICS#deploy-stats-backfill-queued";
/// How far back the copy looks.
pub const BACKFILL_DAYS: i64 = 60;
/// Same lifetime as review stats: well past the 90-day analytics window.
const KEEP_DAYS: i64 = 400;

/// Where a deploy label sends the PR, from its name.
pub fn target(label: &str) -> &'static str {
    let l = label.to_ascii_lowercase();
    if l.contains("prod") {
        "prod"
    } else if l.contains("stag") {
        "staging"
    } else if l.contains("preview") {
        "preview"
    } else {
        "other"
    }
}

/// Whether adding this label starts a deploy: the repo's configured deploy
/// labels, or any label naming a deploy.
pub fn is_deploy_label(label: &str, configured: &[String]) -> bool {
    let l = label.trim();
    !l.is_empty()
        && (configured.iter().any(|c| c.trim().eq_ignore_ascii_case(l))
            || l.to_ascii_lowercase().contains("deploy"))
}

/// The configured deploy labels, from the repo's comma-separated setting.
pub fn configured(deploy_label: &str) -> Vec<String> {
    deploy_label
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

pub fn stat_sk(created_at: &str, repo: &str, pr: u64, label: &str) -> String {
    format!("{PREFIX}{created_at}#{repo}#{pr:0>6}#{label}")
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

/// One deploy CoderHelm started. `head` is the PR's head commit when known
/// (a deploy is counted once per label and commit); `source` says where it
/// was started: `pr_open`, `review`, `merge_gate` or `backfill`.
pub fn stat_item(
    team_id: &str,
    created_at: &str,
    repo: &str,
    pr: u64,
    label: &str,
    head: &str,
    source: &str,
) -> HashMap<String, AttributeValue> {
    let ttl = chrono::DateTime::parse_from_rfc3339(created_at)
        .map(|d| d.timestamp())
        .unwrap_or_else(|_| chrono::Utc::now().timestamp())
        + KEEP_DAYS * 86_400;
    let mut item = HashMap::from([
        ("pk".to_string(), s(team_id)),
        ("sk".to_string(), s(&stat_sk(created_at, repo, pr, label))),
        ("record_type".to_string(), s("deploy_stat")),
        ("created_at".to_string(), s(created_at)),
        ("repo".to_string(), s(repo)),
        ("pr_number".to_string(), AttributeValue::N(pr.to_string())),
        ("label".to_string(), s(label)),
        ("target".to_string(), s(target(label))),
        ("source".to_string(), s(source)),
        ("ttl".to_string(), AttributeValue::N(ttl.to_string())),
    ]);
    if !head.is_empty() {
        item.insert("head_sha".to_string(), s(head));
    }
    item
}

/// What makes two records the same deploy: one label on one PR commit. A
/// record without a commit (copied from GitHub) is its own deploy: GitHub
/// only records a label added when the PR didn't have it.
pub fn dedupe_key(repo: &str, pr: u64, label: &str, head: &str, sk: &str) -> String {
    if head.is_empty() {
        sk.to_string()
    } else {
        format!(
            "{}#{pr}#{}#{head}",
            repo.to_lowercase(),
            label.to_lowercase()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deploy_labels_and_targets() {
        let cfg = configured(" ship-it , ");
        assert_eq!(cfg, vec!["ship-it".to_string()]);
        assert!(is_deploy_label("CI:DEPLOY_STAGING", &[]));
        assert!(is_deploy_label("deploy:preview", &[]));
        assert!(is_deploy_label("Ship-It", &cfg));
        assert!(!is_deploy_label("E2E:join", &[]));
        assert!(!is_deploy_label("coderhelm-review", &[]));
        assert!(!is_deploy_label("  ", &cfg));
        assert_eq!(target("CI:DEPLOY_STAGING"), "staging");
        assert_eq!(target("CI:DEPLOY_PRODUCTION"), "prod");
        assert_eq!(target("deploy:preview"), "preview");
        assert_eq!(target("ship-it"), "other");
    }

    #[test]
    fn items_sort_by_time_and_carry_the_target() {
        let i = stat_item(
            "TEAM#t",
            "2026-10-30T01:02:03Z",
            "acme/web",
            42,
            "CI:DEPLOY_STAGING",
            "abc",
            "review",
        );
        assert_eq!(
            i["sk"].as_s().unwrap(),
            "DSTAT#2026-10-30T01:02:03Z#acme/web#000042#CI:DEPLOY_STAGING"
        );
        assert_eq!(i["target"].as_s().unwrap(), "staging");
        assert_eq!(i["head_sha"].as_s().unwrap(), "abc");
        let b = stat_item(
            "TEAM#t",
            "2026-10-30T01:02:03Z",
            "acme/web",
            42,
            "x-deploy",
            "",
            "backfill",
        );
        assert!(!b.contains_key("head_sha"));
    }

    #[test]
    fn a_relabel_on_the_same_commit_is_one_deploy() {
        let a = dedupe_key("Acme/Web", 4, "CI:DEPLOY_STAGING", "abc", "sk1");
        let b = dedupe_key("acme/web", 4, "ci:deploy_staging", "abc", "sk2");
        assert_eq!(a, b);
        assert_ne!(
            a,
            dedupe_key("acme/web", 4, "CI:DEPLOY_STAGING", "def", "sk3")
        );
        assert_ne!(
            dedupe_key("acme/web", 4, "l", "", "sk1"),
            dedupe_key("acme/web", 4, "l", "", "sk2")
        );
    }
}
