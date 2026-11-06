//! A ticket that is one part of a numbered series ("part 4 of 4"): the
//! planner reads the earlier parts' pull requests before it plans, so it
//! builds on what they added instead of starting blind.
//!
//! The ticket's history already holds every pull request that names this
//! ticket, and the earlier-attempts search finds ones that name a ticket
//! this one names or read almost the same. A sibling part usually does
//! neither: its title shares the series' words but names another part, and
//! its key is another ticket. This finds those: pull requests in the run's
//! repository that name another part of the same series, or a nearby key of
//! the same project, and share the series' words.

use std::collections::BTreeSet;

use tracing::warn;

use crate::clients::github::GitHubClient;
use crate::passes::feature::prior::{similarity, ticket_keys, words};
use crate::passes::ticket_history::EarlierPr;

/// Most sibling pull requests read for one ticket.
const MAX_SIBLINGS: usize = 6;
/// Word overlap with the series' title at or above which a pull request
/// that names another part counts.
const SERIES_WORDS: f64 = 0.2;

/// Where a ticket sits in its series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Part {
    pub part: u32,
    pub total: u32,
}

/// The series marker a text carries: `part 2 of 4`, `part 2/4`, `(2/4)`,
/// `[2 of 4]`. `None` without one, or with a part outside its total.
pub fn marker(text: &str) -> Option<Part> {
    let re = regex::Regex::new(
        r"(?i)(?:\bpart\s*(\d{1,2})\s*(?:of|/)\s*(\d{1,2})\b|[\(\[]\s*(\d{1,2})\s*(?:of|/)\s*(\d{1,2})\s*[\)\]])",
    )
    .ok()?;
    let c = re.captures(text)?;
    let n = |i: usize, j: usize| {
        c.get(i)
            .or_else(|| c.get(j))
            .and_then(|m| m.as_str().parse::<u32>().ok())
    };
    let part = n(1, 3)?;
    let total = n(2, 4)?;
    (total >= 2 && (1..=total).contains(&part)).then_some(Part { part, total })
}

/// The ticket's series marker, from its title first, then its body.
pub fn of_ticket(title: &str, body: &str) -> Option<Part> {
    marker(title).or_else(|| marker(body))
}

/// The series' words: the title's significant words without the marker.
pub fn series_words(title: &str) -> BTreeSet<String> {
    let re = regex::Regex::new(
        r"(?i)\bpart\s*\d{1,2}\s*(?:of|/)\s*\d{1,2}\b|[\(\[]\s*\d{1,2}\s*(?:of|/)\s*\d{1,2}\s*[\)\]]",
    );
    let mut stripped = match re {
        Ok(re) => re.replace_all(title, " ").to_string(),
        Err(_) => title.to_string(),
    };
    // A ticket key names the ticket, not the series.
    for k in ticket_keys(&stripped.clone()) {
        stripped = stripped.replace(&k, " ");
    }
    let mut w = words(&stripped);
    w.remove("part");
    w
}

/// A key's project and number (`ABC-12` is `("ABC", 12)`).
fn key_parts(key: &str) -> Option<(&str, u64)> {
    let (p, n) = key.rsplit_once('-')?;
    Some((p, n.parse().ok()?))
}

/// Whether a pull request (its title, branch and description) is another
/// part of this ticket's series: it names another part of a series of the
/// same length and shares the series' words, or it names a key of the same
/// project within the series' span and shares them.
pub fn is_sibling(
    ticket_id: &str,
    me: Part,
    own_words: &BTreeSet<String>,
    pr_title: &str,
    pr_branch: &str,
    pr_body: &str,
) -> bool {
    let head: String = pr_body.chars().take(1500).collect();
    let text = format!("{pr_title}\n{pr_branch}\n{head}");
    if !ticket_id.is_empty() && crate::passes::mentions_ticket(&text, ticket_id) {
        // This ticket's own pull request: the ticket's history reads it.
        return false;
    }
    let alike = similarity(own_words, &series_words(pr_title)) >= SERIES_WORDS;
    if !alike {
        return false;
    }
    if let Some(p) = marker(&text) {
        return p.total == me.total && p.part != me.part;
    }
    let Some((project, n)) = key_parts(ticket_id) else {
        return false;
    };
    let span = u64::from(me.total) * 2;
    ticket_keys(&text)
        .iter()
        .any(|k| key_parts(k).is_some_and(|(p, m)| p == project && m != n && m.abs_diff(n) <= span))
}

/// Finds the other parts' pull requests in the run's repository, oldest
/// part first. Best-effort and bounded: a list that can't be read adds
/// nothing.
pub async fn find(
    github: &GitHubClient,
    owner: &str,
    name: &str,
    ticket_id: &str,
    title: &str,
    body: &str,
) -> Vec<EarlierPr> {
    let Some(me) = of_ticket(title, body) else {
        return Vec::new();
    };
    let own = series_words(title);
    let repo = format!("{owner}/{name}");
    let list = match github.list_pull_requests(owner, name, "all", 100).await {
        Ok(v) => v,
        Err(e) => {
            warn!(repo, error = %e, "Could not list pull requests for the ticket's series");
            return Vec::new();
        }
    };
    let mut out: Vec<EarlierPr> = Vec::new();
    for p in list.as_array().into_iter().flatten() {
        let number = p["number"].as_u64().unwrap_or(0);
        if number == 0
            || !is_sibling(
                ticket_id,
                me,
                &own,
                p["title"].as_str().unwrap_or(""),
                p["head"]["ref"].as_str().unwrap_or(""),
                p["body"].as_str().unwrap_or(""),
            )
        {
            continue;
        }
        if let Some(full) = crate::passes::ticket_history::read_pr(github, &repo, number).await {
            out.push(full);
        }
        if out.len() >= MAX_SIBLINGS {
            break;
        }
    }
    out.sort_by_key(|p| {
        marker(&format!("{}\n{}", p.title, p.body))
            .map(|m| m.part)
            .unwrap_or(u32::MAX)
    });
    out
}

/// What the planner is told about the series. Empty when the ticket isn't
/// part of one.
pub fn planner_note(me: Option<Part>, repo: &str, siblings: &[EarlierPr]) -> String {
    let Some(me) = me else {
        return String::new();
    };
    let mut out = format!(
        "\n\n## The other parts of this ticket's series\nThis ticket is part {} of {}. ",
        me.part, me.total
    );
    if siblings.is_empty() {
        out.push_str(&format!(
            "No pull request for the other parts was found among the last 100 in {repo}. Before \
             planning new code, search the code for what the earlier parts would have added (the \
             fields, components or settings this part builds on) and build on what is there.\n"
        ));
        return out;
    }
    out.push_str(
        "Their pull requests are below. Build on what they added: read their changed files with \
         your tools before planning, reuse their fields, names and patterns, and don't redo or \
         undo their work. Say in design.md what this part adds on top of them.\n",
    );
    for p in siblings {
        let part = marker(&format!("{}\n{}", p.title, p.body))
            .map(|m| format!("part {} of {}, ", m.part, m.total))
            .unwrap_or_default();
        out.push_str(&format!(
            "- #{} \"{}\" ({part}{}, branch `{}`): {}\n",
            p.number,
            p.title.trim(),
            p.state,
            p.branch,
            common::truncate_str(
                &p.body.split_whitespace().collect::<Vec<_>>().join(" "),
                600
            )
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_series_marker_is_read_in_its_usual_forms() {
        let p = |part, total| Some(Part { part, total });
        assert_eq!(marker("Title size setting (Part 4 of 4)"), p(4, 4));
        assert_eq!(marker("part 2/3: the schema"), p(2, 3));
        assert_eq!(marker("Venue blocks [1/4]"), p(1, 4));
        assert_eq!(marker("Venue blocks (3 of 4)"), p(3, 4));
        assert_eq!(marker("No series here, 4 of 4 tests pass"), None);
        assert_eq!(marker("part 5 of 4"), None);
        assert_eq!(of_ticket("Add a setting", "This is part 4 of 4."), p(4, 4));
    }

    #[test]
    fn the_other_parts_are_found_and_unrelated_prs_are_not() {
        let title = "Venue page blocks: add a Title Size setting (Section / Page) (Part 4 of 4)";
        let me = of_ticket(title, "").unwrap();
        let own = series_words(title);
        assert!(!own.contains("part"));
        // Another part of the same series, by its marker.
        assert!(is_sibling(
            "WEB-104",
            me,
            &own,
            "WEB-101: Venue page blocks: add the block schemas (Part 1 of 4)",
            "coderhelm/web-101",
            "",
        ));
        // By a nearby key of the same project, with no marker in the PR.
        assert!(is_sibling(
            "WEB-104",
            me,
            &own,
            "WEB-103: Venue page blocks title setting",
            "coderhelm/web-103",
            "",
        ));
        // This ticket's own PR is the history's, not a sibling.
        assert!(!is_sibling(
            "WEB-104",
            me,
            &own,
            "WEB-104: Venue page blocks title size (Part 4 of 4)",
            "coderhelm/web-104",
            "",
        ));
        // Another series, or unrelated work, is not.
        assert!(!is_sibling(
            "WEB-104",
            me,
            &own,
            "WEB-90: Checkout redesign (Part 1 of 4)",
            "x",
            "",
        ));
        assert!(!is_sibling(
            "WEB-104",
            me,
            &own,
            "WEB-102: Venue page blocks (Part 1 of 3)",
            "x",
            "",
        ));
        assert!(!is_sibling(
            "WEB-104",
            me,
            &own,
            "WEB-180: Venue page blocks title setting",
            "x",
            "",
        ));
    }

    #[test]
    fn the_planner_is_told_to_build_on_the_earlier_parts() {
        assert_eq!(planner_note(None, "a/b", &[]), "");
        let me = Some(Part { part: 4, total: 4 });
        let none = planner_note(me, "a/b", &[]);
        assert!(none.contains("part 4 of 4"), "{none}");
        assert!(none.contains("search the code"), "{none}");
        let pr = EarlierPr {
            repo: "a/b".into(),
            number: 12,
            title: "WEB-101: Venue page blocks schemas (Part 1 of 4)".into(),
            branch: "coderhelm/web-101".into(),
            state: "merged".into(),
            body: "Adds the block schemas.".into(),
            ..Default::default()
        };
        let note = planner_note(me, "a/b", &[pr]);
        assert!(note.contains("Build on what they added"), "{note}");
        assert!(
            note.contains("- #12 \"WEB-101: Venue page blocks schemas (Part 1 of 4)\" (part 1 of 4, merged, branch `coderhelm/web-101`): Adds the block schemas."),
            "{note}"
        );
    }
}
