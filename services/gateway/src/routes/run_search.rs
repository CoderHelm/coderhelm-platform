//! Search and filters for the runs list.
//!
//! Runs are keyed `team_id` + `run_id` (a ULID, so newest first), and nothing
//! indexes their titles, so a search reads the team's runs newest first and
//! matches each one here. DynamoDB's `contains` is case-sensitive and cannot
//! compare a ticket's number, which is why the matching is not a
//! FilterExpression. One request reads at most `SCAN_BUDGET` runs; when it
//! stops early, `next` is the last run it read, so "Load more" searches on
//! from there.

use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;

pub type Item = HashMap<String, AttributeValue>;

/// Most runs one request reads, matched or not.
pub const SCAN_BUDGET: usize = 1000;

/// The attributes the list shows or matches on; run items also carry notes
/// and pass history, which the list never needs.
pub const LIST_ATTRS: &[&str] = &[
    "run_id",
    "status",
    "ticket_source",
    "ticket_id",
    "title",
    "repo",
    "pr_url",
    "pr_number",
    "pr_numbers",
    "cost_usd",
    "tokens_in",
    "tokens_out",
    "duration_s",
    "current_pass",
    "created_at",
];

#[derive(Debug, Default)]
pub struct RunFilter {
    /// Lowercased words of the search; a run must match every one.
    terms: Vec<String>,
    /// Exact statuses to keep; empty keeps every status but `archived`.
    statuses: Vec<String>,
    source: Option<String>,
    /// One repo to keep, lowercased (exact match).
    repo: Option<String>,
}

impl RunFilter {
    pub fn new(q: Option<&str>, status: Option<&str>, source: Option<&str>) -> Self {
        let terms = q
            .unwrap_or("")
            .chars()
            .take(200)
            .collect::<String>()
            .to_lowercase()
            .split_whitespace()
            .take(8)
            .map(str::to_string)
            .collect();
        let statuses = status
            .unwrap_or("")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let source = source.map(str::to_string).filter(|s| !s.is_empty());
        Self {
            terms,
            statuses,
            source,
            repo: None,
        }
    }

    /// Keep only runs of one repo (`owner/name`, any case). Empty keeps all.
    pub fn with_repo(mut self, repo: Option<&str>) -> Self {
        self.repo = repo
            .map(|r| r.trim().to_lowercase())
            .filter(|r| !r.is_empty() && r.len() <= 200);
        self
    }

    pub fn matches(&self, item: &Item) -> bool {
        let status = s(item, "status");
        if self.statuses.is_empty() {
            // Archived runs only show when asked for by status.
            if status == "archived" {
                return false;
            }
        } else if !self.statuses.iter().any(|w| w == status) {
            return false;
        }
        if let Some(source) = &self.source {
            if s(item, "ticket_source") != source {
                return false;
            }
        }
        if let Some(repo) = &self.repo {
            if s(item, "repo").to_lowercase() != *repo {
                return false;
            }
        }
        if self.terms.is_empty() {
            return true;
        }
        let ticket = s(item, "ticket_id").to_lowercase();
        let title = s(item, "title").to_lowercase();
        let repo = s(item, "repo").to_lowercase();
        let status_words = status.replace('_', " ");
        let prs = pr_numbers(item);
        self.terms.iter().all(|term| {
            // "8532" and "#8532" are a ticket or PR number: "ABC-8532" and
            // PR 8532 match, "ABC-18532" does not.
            let digits = term.strip_prefix('#').unwrap_or(term);
            if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
                return ticket_number(&ticket) == Some(digits)
                    || digits.parse::<u64>().is_ok_and(|n| prs.contains(&n))
                    || title.contains(term.as_str());
            }
            ticket.contains(term.as_str())
                || title.contains(term.as_str())
                || repo.contains(term.as_str())
                || status.contains(term.as_str())
                || status_words.contains(term.as_str())
        })
    }
}

/// The number at the end of a ticket id: "abc-123" and "acme/web#123" → "123".
fn ticket_number(ticket: &str) -> Option<&str> {
    let (_, n) = ticket.rsplit_once(['-', '#'])?;
    (!n.is_empty() && n.bytes().all(|b| b.is_ascii_digit())).then_some(n)
}

fn pr_numbers(item: &Item) -> Vec<u64> {
    let mut out: Vec<u64> = item
        .get("pr_numbers")
        .and_then(|v| v.as_l().ok())
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_n().ok()?.parse().ok())
        .collect();
    out.extend(
        item.get("pr_number")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok()),
    );
    out
}

fn s<'a>(item: &'a Item, key: &str) -> &'a str {
    item.get(key)
        .and_then(|v| v.as_s().ok())
        .map(|s| s.as_str())
        .unwrap_or("")
}

/// Pure: walk one read page, keeping matches until `want` are found or
/// `budget` runs are read. Returns the matches and how many runs were read;
/// fewer than `items.len()` means the request stops inside this page.
pub fn take_matches<'a>(
    items: &'a [Item],
    filter: &RunFilter,
    want: usize,
    budget: usize,
) -> (Vec<&'a Item>, usize) {
    let mut found = Vec::new();
    let mut read = 0;
    for item in items {
        if found.len() >= want || read >= budget {
            break;
        }
        read += 1;
        if filter.matches(item) {
            found.push(item);
        }
    }
    (found, read)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(id: &str, status: &str, ticket: &str, title: &str, repo: &str, pr: Option<u64>) -> Item {
        let mut item = HashMap::from([
            ("run_id".to_string(), AttributeValue::S(id.into())),
            ("status".to_string(), AttributeValue::S(status.into())),
            (
                "ticket_source".to_string(),
                AttributeValue::S("jira".into()),
            ),
            ("ticket_id".to_string(), AttributeValue::S(ticket.into())),
            ("title".to_string(), AttributeValue::S(title.into())),
            ("repo".to_string(), AttributeValue::S(repo.into())),
        ]);
        if let Some(n) = pr {
            item.insert("pr_number".into(), AttributeValue::N(n.to_string()));
        }
        item
    }

    fn q(text: &str) -> RunFilter {
        RunFilter::new(Some(text), None, None)
    }

    #[test]
    fn a_ticket_number_alone_matches_its_key_but_not_a_longer_number() {
        let r = run(
            "1",
            "completed",
            "ABC-8532",
            "Fix the login page",
            "acme/web",
            None,
        );
        assert!(q("8532").matches(&r));
        assert!(q("#8532").matches(&r));
        assert!(q("abc-8532").matches(&r));
        assert!(q("ABC-8532").matches(&r));
        assert!(!q("853").matches(&r));
        let longer = run("2", "completed", "ABC-18532", "Other", "acme/web", None);
        assert!(!q("8532").matches(&longer));
    }

    #[test]
    fn a_github_issue_number_matches_too() {
        let r = run(
            "1",
            "completed",
            "acme/web#42",
            "Add dark mode",
            "acme/web",
            None,
        );
        assert!(q("42").matches(&r));
        assert!(q("#42").matches(&r));
    }

    #[test]
    fn matches_title_repo_and_status_case_insensitively() {
        let r = run(
            "1",
            "needs_input",
            "ABC-1",
            "Show the Class Schedule",
            "acme/web",
            None,
        );
        assert!(q("class schedule").matches(&r));
        assert!(q("ACME/WEB").matches(&r));
        assert!(q("needs input").matches(&r));
        assert!(q("needs_input").matches(&r));
        assert!(!q("schedule billing").matches(&r));
    }

    #[test]
    fn matches_pr_numbers() {
        let r = run("1", "merged", "ABC-1", "Title", "acme/web", Some(101));
        assert!(q("101").matches(&r));
        assert!(q("#101").matches(&r));
        assert!(!q("10").matches(&r));
        let mut multi = run("2", "completed", "ABC-2", "Title", "acme/api", None);
        multi.insert(
            "pr_numbers".into(),
            AttributeValue::L(vec![
                AttributeValue::N("7".into()),
                AttributeValue::N("9".into()),
            ]),
        );
        assert!(q("9").matches(&multi));
    }

    #[test]
    fn numbers_in_titles_still_match() {
        let r = run(
            "1",
            "completed",
            "ABC-1",
            "Upgrade to Node 22",
            "acme/web",
            None,
        );
        assert!(q("22").matches(&r));
    }

    #[test]
    fn archived_runs_stay_hidden_unless_asked_for_by_status() {
        let r = run("1", "archived", "ABC-1", "Old", "acme/web", None);
        assert!(!RunFilter::new(None, None, None).matches(&r));
        assert!(!q("old").matches(&r));
        assert!(RunFilter::new(None, Some("archived"), None).matches(&r));
    }

    #[test]
    fn status_and_source_filters_combine_with_the_search() {
        let r = run("1", "failed", "ABC-1", "Login", "acme/web", None);
        assert!(RunFilter::new(Some("login"), Some("failed,cancelled"), None).matches(&r));
        assert!(!RunFilter::new(Some("login"), Some("completed"), None).matches(&r));
        assert!(RunFilter::new(Some("login"), None, Some("jira")).matches(&r));
        assert!(!RunFilter::new(Some("login"), None, Some("github")).matches(&r));
    }

    #[test]
    fn the_repo_filter_keeps_one_repo_exactly() {
        let web = run("1", "failed", "ABC-1", "Login", "acme/web", None);
        let web2 = run("2", "failed", "ABC-2", "Login", "acme/web2", None);
        let f = RunFilter::new(None, None, None).with_repo(Some("Acme/Web"));
        assert!(f.matches(&web));
        assert!(!f.matches(&web2));
        let all = RunFilter::new(None, None, None).with_repo(Some(""));
        assert!(all.matches(&web2));
        let both = RunFilter::new(Some("login"), Some("failed"), None).with_repo(Some("acme/web"));
        assert!(both.matches(&web));
    }

    #[test]
    fn stops_at_the_wanted_count_or_the_budget() {
        let items: Vec<Item> = (0..10)
            .map(|i| {
                let title = if i % 2 == 0 { "login" } else { "billing" };
                run(
                    &i.to_string(),
                    "completed",
                    "ABC-1",
                    title,
                    "acme/web",
                    None,
                )
            })
            .collect();
        let f = q("login");
        // Two matches wanted: runs 0 and 2, after reading three.
        let (found, read) = take_matches(&items, &f, 2, 100);
        assert_eq!(found.len(), 2);
        assert_eq!(read, 3);
        // The budget stops the walk first.
        let (found, read) = take_matches(&items, &f, 50, 4);
        assert_eq!(found.len(), 2);
        assert_eq!(read, 4);
        // A full page read without enough matches.
        let (found, read) = take_matches(&items, &f, 50, 100);
        assert_eq!(found.len(), 5);
        assert_eq!(read, 10);
    }
}
