//! What happened to a pull request Coderhelm opened, read from GitHub's PR
//! object: merged (when, by whom), closed without merging, or still open.
//! Shared by the merge webhook and the backfill so both record the same
//! fields the analytics read.

use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Merged {
        merged_at: String,
        opened_at: String,
        merged_by: String,
    },
    Closed {
        closed_at: String,
        opened_at: String,
    },
    Open,
}

/// Reads the outcome from a GitHub pull request object.
pub fn outcome(pr: &Value) -> Outcome {
    let s = |k: &str| pr[k].as_str().unwrap_or("").to_string();
    if let Some(merged_at) = pr["merged_at"].as_str().filter(|v| !v.is_empty()) {
        return Outcome::Merged {
            merged_at: merged_at.to_string(),
            opened_at: s("created_at"),
            merged_by: pr["merged_by"]["login"].as_str().unwrap_or("").to_string(),
        };
    }
    if pr["state"].as_str() == Some("closed") {
        return Outcome::Closed {
            closed_at: s("closed_at"),
            opened_at: s("created_at"),
        };
    }
    Outcome::Open
}

/// Commits on a pull request that a person pushed (not Coderhelm, not a bot).
pub fn human_commit_count(commits: &[Value]) -> u64 {
    commits
        .iter()
        .filter(|c| {
            let login = c["author"]["login"]
                .as_str()
                .or(c["committer"]["login"].as_str())
                .unwrap_or("");
            let is_bot = c["author"]["type"].as_str() == Some("Bot") || login.ends_with("[bot]");
            !login.contains("coderhelm") && !is_bot && !login.is_empty()
        })
        .count() as u64
}

/// `(owner, repo, number)` from a PR's html URL.
pub fn parse_pr_url(url: &str) -> Option<(String, String, u64)> {
    let rest = url.strip_prefix("https://github.com/")?;
    let mut parts = rest.split('/');
    let owner = parts.next().filter(|s| !s.is_empty())?;
    let repo = parts.next().filter(|s| !s.is_empty())?;
    if parts.next()? != "pull" {
        return None;
    }
    let n = parts.next()?.parse().ok()?;
    Some((owner.to_string(), repo.to_string(), n))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reads_merged_closed_and_open() {
        let merged = json!({"state":"closed","merged_at":"2026-10-14T20:01:00Z","created_at":"2026-10-14T18:55:00Z","merged_by":{"login":"pat"}});
        assert_eq!(
            outcome(&merged),
            Outcome::Merged {
                merged_at: "2026-10-14T20:01:00Z".into(),
                opened_at: "2026-10-14T18:55:00Z".into(),
                merged_by: "pat".into()
            }
        );
        let closed = json!({"state":"closed","merged_at":null,"closed_at":"2026-10-15T09:00:00Z","created_at":"2026-10-14T18:55:00Z"});
        assert_eq!(
            outcome(&closed),
            Outcome::Closed {
                closed_at: "2026-10-15T09:00:00Z".into(),
                opened_at: "2026-10-14T18:55:00Z".into()
            }
        );
        assert_eq!(
            outcome(&json!({"state":"open","merged_at":null})),
            Outcome::Open
        );
    }

    #[test]
    fn counts_only_people() {
        let commits = vec![
            json!({"author":{"login":"coderhelm[bot]","type":"Bot"}}),
            json!({"author":{"login":"alice","type":"User"}}),
            json!({"author":null,"committer":{"login":"bob"}}),
            json!({"author":{"login":"dependabot[bot]","type":"Bot"}}),
        ];
        assert_eq!(human_commit_count(&commits), 2);
        assert_eq!(human_commit_count(&[]), 0);
    }

    #[test]
    fn parses_pr_urls() {
        assert_eq!(
            parse_pr_url("https://github.com/acme/web/pull/311"),
            Some(("acme".into(), "web".into(), 311))
        );
        assert_eq!(parse_pr_url("https://github.com/acme/web/issues/3"), None);
        assert_eq!(parse_pr_url("https://example.com/acme/web/pull/3"), None);
    }
}
