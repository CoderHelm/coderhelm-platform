//! A small, time-ordered record of every review, for analytics.
//!
//! Review records are keyed by repo and pull request, so counting the reviews
//! of the last 30 days would mean reading every review ever written (bodies
//! included). Each review also writes one of these: no body, keyed by time, so
//! a period is one range query.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

pub const PREFIX: &str = "RSTAT#";
/// Set on a team once every review record has a stats record carrying what
/// was posted on GitHub. The first copy (`ANALYTICS#review-stats-backfilled`)
/// dropped writes DynamoDB throttled and never recorded `posted_as`, so the
/// repair runs again under this key.
pub const REPAIR_MARKER_SK: &str = "ANALYTICS#review-stats-repaired#v2";
/// Stats records outlive the 90-day analytics window by a wide margin.
const KEEP_DAYS: i64 = 400;

/// What one review found and how earlier findings ended up.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReviewCounts {
    pub findings: usize,
    /// Earlier findings this review looked at again.
    pub prior_open: usize,
    /// Earlier findings the code now fixes: acted on.
    pub prior_fixed: usize,
    /// Earlier findings a person answered or resolved without a code change.
    pub prior_settled_by_person: usize,
    /// Whether the review was posted (a re-review with nothing new is not).
    pub posted: bool,
    /// Whether the counts are known. Reviews copied from before stats records
    /// existed know their findings but not what became of earlier ones.
    pub priors_known: bool,
}

pub fn stat_sk(created_at: &str, repo: &str, pr: u64) -> String {
    format!("{PREFIX}{created_at}#{repo}#{pr:0>6}")
}

/// The stats key for a review record's key (`REVIEW#{repo}#{pr}#{created_at}`).
pub fn stat_sk_from_review_sk(review_sk: &str) -> Option<String> {
    let rest = review_sk.strip_prefix("REVIEW#")?;
    let mut parts = rest.rsplitn(3, '#');
    let created_at = parts.next()?;
    let pr: u64 = parts.next()?.parse().ok()?;
    let repo = parts.next()?;
    if repo.is_empty() || created_at.is_empty() {
        return None;
    }
    Some(stat_sk(created_at, repo, pr))
}

/// Findings in a stored review body: each is written as `\n\n- [severity] ...`.
pub fn count_findings(body: &str) -> usize {
    body.matches("\n\n- [").count()
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}
fn n(v: impl ToString) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

#[allow(clippy::too_many_arguments)]
pub fn stat_item(
    team_id: &str,
    created_at: &str,
    repo: &str,
    pr: u64,
    verdict: &str,
    posted_as: &str,
    risk: &str,
    trigger: &str,
    counts: &ReviewCounts,
) -> HashMap<String, AttributeValue> {
    let ttl = chrono::DateTime::parse_from_rfc3339(created_at)
        .map(|d| d.timestamp())
        .unwrap_or_else(|_| chrono::Utc::now().timestamp())
        + KEEP_DAYS * 86_400;
    let mut item = HashMap::from([
        ("pk".to_string(), s(team_id)),
        ("sk".to_string(), s(&stat_sk(created_at, repo, pr))),
        ("record_type".to_string(), s("review_stat")),
        ("created_at".to_string(), s(created_at)),
        ("repo".to_string(), s(repo)),
        ("pr_number".to_string(), n(pr)),
        ("verdict".to_string(), s(verdict)),
        ("risk".to_string(), s(risk)),
        ("trigger".to_string(), s(trigger)),
        ("findings".to_string(), n(counts.findings)),
        ("posted".to_string(), AttributeValue::Bool(counts.posted)),
        ("thumbs_up".to_string(), n(0)),
        ("thumbs_down".to_string(), n(0)),
        ("ttl".to_string(), n(ttl)),
    ]);
    // What GitHub shows: an approval, changes requested, or a plain comment
    // (the verdict can be an approval posted as a comment).
    if !posted_as.is_empty() {
        item.insert("posted_as".to_string(), s(posted_as));
    }
    if counts.priors_known {
        item.insert("prior_open".to_string(), n(counts.prior_open));
        item.insert("prior_fixed".to_string(), n(counts.prior_fixed));
        item.insert(
            "prior_settled_by_person".to_string(),
            n(counts.prior_settled_by_person),
        );
    }
    item
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_sort_by_time_and_map_from_review_keys() {
        let sk = stat_sk("2026-01-02T03:04:05Z", "acme/web", 42);
        assert_eq!(sk, "RSTAT#2026-01-02T03:04:05Z#acme/web#000042");
        assert_eq!(
            stat_sk_from_review_sk("REVIEW#acme/web#000042#2026-01-02T03:04:05Z").as_deref(),
            Some(sk.as_str())
        );
        assert_eq!(stat_sk_from_review_sk("REVIEW#acme/web#x#2026"), None);
        assert_eq!(stat_sk_from_review_sk("OTHER#a#1#b"), None);
        assert!(
            stat_sk("2026-01-01T00:00:00Z", "a/b", 1) < stat_sk("2026-01-02T00:00:00Z", "a/a", 1)
        );
    }

    #[test]
    fn findings_are_counted_from_the_stored_digest() {
        let body = "Summary.\n\n- [high] a.rs:1 — x\n  y\n\n- [low] b.rs:2 — z\n  w";
        assert_eq!(count_findings(body), 2);
        assert_eq!(count_findings("Looks good."), 0);
    }

    #[test]
    fn unknown_prior_counts_are_left_out() {
        let known = ReviewCounts {
            findings: 2,
            prior_open: 3,
            prior_fixed: 1,
            prior_settled_by_person: 1,
            posted: true,
            priors_known: true,
        };
        let item = stat_item(
            "TEAM#1",
            "2026-01-01T00:00:00Z",
            "a/b",
            7,
            "APPROVE",
            "COMMENT",
            "low",
            "labeled",
            &known,
        );
        assert_eq!(item["prior_fixed"], AttributeValue::N("1".into()));
        assert_eq!(item["verdict"], AttributeValue::S("APPROVE".into()));
        assert_eq!(item["posted_as"], AttributeValue::S("COMMENT".into()));
        let unknown = ReviewCounts {
            findings: 2,
            ..Default::default()
        };
        let item = stat_item(
            "TEAM#1",
            "2026-01-01T00:00:00Z",
            "a/b",
            7,
            "COMMENT",
            "",
            "low",
            "labeled",
            &unknown,
        );
        assert!(!item.contains_key("prior_fixed"));
        assert!(!item.contains_key("posted_as"));
        assert_eq!(item["findings"], AttributeValue::N("2".into()));
    }
}
