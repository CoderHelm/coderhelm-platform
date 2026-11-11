//! The base branch a ticket asks for.
//!
//! A ticket can say which branch its work belongs on: "Open the PR against
//! `release-2`, not `develop`", "Branch from feature/checkout", "Base
//! branch: integration". Teams write this for long-lived feature or
//! release branches that have no open pull request of their own, so
//! [`super::stack`] (which only stacks on open pull requests) never sees
//! them. Without this, the run started from the repository's default
//! branch anyway: its pull request targeted the wrong branch, compared
//! against a base that lacked the feature's earlier work, rebuilt that work
//! a second time, or failed to open at all.
//!
//! The rules:
//!
//! - Only explicit phrasing counts: a pull request opened, raised or
//!   targeted "against", "into", "onto" or "to" a branch; branching "from"
//!   or "off" a branch; "base branch" or "target branch" followed by a
//!   name. A clause that negates it ("do not open the PR against
//!   develop", "never branch from main") names a branch to avoid, never
//!   one to use.
//! - The name must be a branch the repository has. A ticket that names a
//!   branch by a longer or shorter path (`feature/checkout` for
//!   `checkout`, or the reverse) uses the one branch that matches it by its
//!   last path segment. No match, or more than one, changes nothing and the
//!   run says so.
//! - Two different branches named: nothing is guessed.

use std::sync::LazyLock;

use regex::Regex;

use crate::clients::github::GitHubClient;

/// What a ticket's named base comes to against the repository's branches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The ticket names no base branch, or names the default branch.
    None,
    /// The branch the run starts from and its pull request targets.
    Use(String),
    /// The ticket names a base the repository doesn't have (the names, as
    /// written).
    Missing(Vec<String>),
    /// The ticket names more than one base branch (the branches).
    Ambiguous(Vec<String>),
}

impl Outcome {
    /// What the run tells people about it, or None when there is nothing to
    /// say.
    pub fn note(&self, default: &str) -> Option<String> {
        match self {
            Outcome::None => None,
            Outcome::Use(b) => Some(format!(
                "The ticket asks for `{b}` as the base branch: the branch starts from it and \
                 the pull request targets it."
            )),
            Outcome::Missing(names) => Some(format!(
                "The ticket names {} as the base branch, which this repository doesn't have. \
                 The pull request targets `{default}`; correct the branch name in the ticket \
                 and retry if that is wrong.",
                quoted(names)
            )),
            Outcome::Ambiguous(names) => Some(format!(
                "The ticket names more than one base branch ({}). The pull request targets \
                 `{default}`; leave one in the ticket and retry if that is wrong.",
                quoted(names)
            )),
        }
    }
}

fn quoted(names: &[String]) -> String {
    names
        .iter()
        .map(|n| format!("`{n}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A branch name as written, with optional quotes or backticks around it.
const NAME: &str = r#"(?P<q>[`'"])?(?P<name>[A-Za-z0-9][A-Za-z0-9._/\-]*[A-Za-z0-9])[`'"]?"#;

/// The phrasings that name a base branch.
static PHRASES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        // open / raise / create / submit / target the PR against X
        r"(?i)\b(?:open|opening|raise|raising|create|creating|submit|submitting|target|targeting|point|pointing|file|base)\s+(?:the\s+|this\s+|a\s+|your\s+|each\s+|all\s+)?(?:prs?|pull\s+requests?|mrs?|merge\s+requests?)\s+(?:against|into|onto|off)\s+(?:the\s+)?",
        // the PR (should) go(es) against X / PR against X
        r"(?i)\b(?:prs?|pull\s+requests?)\s+(?:should\s+|must\s+|will\s+)?(?:go(?:es)?\s+|be\s+opened\s+|be\s+raised\s+|targets?\s+)?(?:against|into|onto)\s+(?:the\s+)?",
        // branch from / off X
        r"(?i)\b(?:branch(?:ed|ing)?)\s+(?:it\s+|this\s+)?(?:off\s+of|off|from)\s+(?:the\s+)?",
        // base branch: X / target branch is X / base branch should be X
        r"(?i)\b(?:base|target)\s+branch\s*(?::|=|\bis\b|\bshould\s+be\b|\bmust\s+be\b|\bwill\s+be\b)\s*",
    ]
    .iter()
    .map(|p| Regex::new(&format!("{p}{NAME}")).expect("ticket base phrase"))
    .collect()
});

/// Words before a match in its clause that make it a branch to avoid.
static NEGATION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:not|never|don'?t|do\s+not|avoid|instead\s+of|rather\s+than|nor|except)\b")
        .expect("negation")
});

/// Words that read as a name after the phrases but aren't one.
const NOT_NAMES: &[&str] = &[
    "a",
    "an",
    "and",
    "or",
    "the",
    "it",
    "this",
    "that",
    "there",
    "here",
    "your",
    "our",
    "its",
    "each",
    "every",
    "any",
    "same",
    "new",
    "base",
    "default",
    "current",
    "latest",
    "remote",
    "upstream",
    "scratch",
    "branch",
    "mainline",
    "repo",
    "repository",
    "now",
    "not",
    "never",
    "github",
];

/// The base branches a ticket names, as written and in order, without
/// repeats (case aside).
#[cfg(test)]
fn named(text: &str) -> Vec<String> {
    named_with_shape(text).into_iter().map(|(n, _)| n).collect()
}

/// [`named`], each with whether it reads as a branch name on its own:
/// quoted, or shaped like one (a `/`, `-`, `_`, `.` or a digit). Only those
/// are reported when the repository lacks them, so a plain word that one of
/// the phrasings happened to catch never produces a note.
fn named_with_shape(text: &str) -> Vec<(String, bool)> {
    // Every phrasing's hits, in the order the ticket says them.
    let mut hits: Vec<(usize, String, bool)> = Vec::new();
    for re in PHRASES.iter() {
        for caps in re.captures_iter(text) {
            let (Some(whole), Some(name)) = (caps.get(0), caps.name("name")) else {
                continue;
            };
            let clause_start = text[..whole.start()]
                .rfind(['.', ';', '\n', '!', '?'])
                .map(|i| i + 1)
                .unwrap_or(0);
            if NEGATION.is_match(&text[clause_start..whole.start()]) {
                continue;
            }
            // "the feature branch", "a release branch": a description, not a name.
            if text[name.end()..]
                .trim_start_matches(['`', '\'', '"'])
                .trim_start()
                .to_ascii_lowercase()
                .starts_with("branch")
            {
                continue;
            }
            let raw = name.as_str().trim_end_matches(['.', '/', '-']);
            let n = raw.strip_prefix("origin/").unwrap_or(raw);
            if n.is_empty() || NOT_NAMES.contains(&n.to_ascii_lowercase().as_str()) {
                continue;
            }
            let shaped = caps.name("q").is_some()
                || n.contains(['/', '-', '_', '.'])
                || n.chars().any(|c| c.is_ascii_digit());
            hits.push((whole.start(), n.to_string(), shaped));
        }
    }
    hits.sort_by_key(|(at, _, _)| *at);
    let mut out: Vec<(String, bool)> = Vec::new();
    for (_, n, shaped) in hits {
        if !out.iter().any(|(o, _)| o.eq_ignore_ascii_case(&n)) {
            out.push((n, shaped));
        }
    }
    out
}

fn last_segment(name: &str) -> &str {
    name.rsplit('/').next().unwrap_or(name)
}

/// The repository branch a name written in a ticket means: the exact
/// branch, else the one branch equal to it case aside, else the one branch
/// whose last path segment is the name's (or whose name is the name's last
/// segment).
pub fn resolve_one<'a>(name: &str, branches: &'a [String]) -> Option<&'a str> {
    if let Some(b) = branches.iter().find(|b| b.as_str() == name) {
        return Some(b);
    }
    let unique = |hits: Vec<&'a String>| (hits.len() == 1).then(|| hits[0].as_str());
    if let Some(b) = unique(
        branches
            .iter()
            .filter(|b| b.eq_ignore_ascii_case(name))
            .collect(),
    ) {
        return Some(b);
    }
    let seg = last_segment(name);
    unique(
        branches
            .iter()
            .filter(|b| last_segment(b).eq_ignore_ascii_case(seg) || b.eq_ignore_ascii_case(seg))
            .collect(),
    )
}

/// What the names a ticket gives come to against the repository's
/// branches and its default branch.
pub fn resolve(names: &[(String, bool)], branches: &[String], default: &str) -> Outcome {
    let mut found: Vec<String> = Vec::new();
    let mut missing: Vec<String> = Vec::new();
    for (n, shaped) in names {
        match resolve_one(n, branches) {
            Some(b) => {
                if !found.iter().any(|f| f == b) {
                    found.push(b.to_string());
                }
            }
            None if *shaped => missing.push(n.clone()),
            None => {}
        }
    }
    match found.len() {
        0 if missing.is_empty() => Outcome::None,
        0 => Outcome::Missing(missing),
        1 if found[0] == default => Outcome::None,
        1 => Outcome::Use(found.remove(0)),
        _ => Outcome::Ambiguous(found),
    }
}

/// The base branch the ticket asks for, read from its title and body and
/// checked against the repository's branches. A branch listing that fails
/// changes nothing.
pub async fn for_ticket(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    title: &str,
    body: &str,
    default: &str,
) -> Outcome {
    let names = named_with_shape(&format!("{title}\n{body}"));
    if names.is_empty() {
        return Outcome::None;
    }
    match github.list_branches(owner, repo, 1000).await {
        Ok(branches) => resolve(&names, &branches, default),
        Err(e) => {
            tracing::warn!(owner, repo, error = %e, "Could not list branches for the ticket's base");
            Outcome::None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn branches(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn reads_the_phrasings_teams_use() {
        assert_eq!(
            named("Open the PR against `release-2`, NOT `develop`."),
            vec!["release-2"]
        );
        assert_eq!(
            named("Open the PR against feature/checkout, never develop."),
            vec!["feature/checkout"]
        );
        assert_eq!(
            named("Branch from feature/checkout."),
            vec!["feature/checkout"]
        );
        assert_eq!(named("Base branch: integration"), vec!["integration"]);
        assert_eq!(named("Target branch is 'staging'"), vec!["staging"]);
        assert_eq!(
            named("PRs should go against hotfix/1.4"),
            vec!["hotfix/1.4"]
        );
        assert_eq!(named("Please branch off of origin/next"), vec!["next"]);
    }

    #[test]
    fn a_negated_clause_names_a_branch_to_avoid() {
        let ticket = "Branch from feature/checkout.\n\
                      Open the PR against feature/checkout.\n\
                      Do not branch from, compare against, or open the PR against develop.";
        assert_eq!(named(ticket), vec!["feature/checkout"]);
        assert!(named("Never open the PR against main.").is_empty());
        assert!(named("Branch from and open the PR against `release-2`.") == vec!["release-2"]);
        assert!(named("Don't branch from develop").is_empty());
    }

    #[test]
    fn descriptions_and_plain_prose_name_nothing() {
        assert!(named("Open the PR against the feature branch.").is_empty());
        assert!(named("Start from scratch on the layout.").is_empty());
        assert!(named("The button should work from the home screen.").is_empty());
        assert!(named("Merge into main after QA signs off.").is_empty());
        assert!(named("Create a PR to fix the crash on launch.").is_empty());
        assert!(named("Raise a PR on GitHub when done.").is_empty());
        assert!(named("Add the file the base branch doesn't have yet.").is_empty());
        assert!(named("").is_empty());
    }

    #[test]
    fn repeats_count_once() {
        let ticket = "Branch from Release-2. Open the PR against release-2.";
        assert_eq!(named(ticket), vec!["Release-2"]);
    }

    #[test]
    fn only_branch_shaped_or_quoted_names_are_reported_missing() {
        let shaped = named_with_shape("Open the PR against `staging`. Branch from checkout.");
        assert_eq!(
            shaped,
            vec![
                ("staging".to_string(), true),
                ("checkout".to_string(), false)
            ]
        );
        assert_eq!(
            named_with_shape("Branch from release-3"),
            vec![("release-3".to_string(), true)]
        );
    }

    #[test]
    fn a_name_resolves_to_the_one_branch_it_means() {
        let b = branches(&["develop", "main", "checkout", "release/2"]);
        assert_eq!(resolve_one("checkout", &b), Some("checkout"));
        assert_eq!(resolve_one("feature/checkout", &b), Some("checkout"));
        assert_eq!(resolve_one("2", &b), Some("release/2"));
        assert_eq!(resolve_one("Develop", &b), Some("develop"));
        assert_eq!(resolve_one("feature/payments", &b), None);
        let two = branches(&["a/checkout", "b/checkout"]);
        assert_eq!(resolve_one("checkout", &two), None);
    }

    #[test]
    fn outcomes() {
        let b = branches(&["develop", "checkout"]);
        let n = |names: &[(&str, bool)]| -> Vec<(String, bool)> {
            names.iter().map(|(s, q)| (s.to_string(), *q)).collect()
        };
        assert_eq!(resolve(&[], &b, "develop"), Outcome::None);
        assert_eq!(
            resolve(&n(&[("feature/checkout", true)]), &b, "develop"),
            Outcome::Use("checkout".into())
        );
        assert_eq!(
            resolve(&n(&[("develop", false)]), &b, "develop"),
            Outcome::None
        );
        assert_eq!(
            resolve(&n(&[("feature/payments", true)]), &b, "develop"),
            Outcome::Missing(vec!["feature/payments".into()])
        );
        // A plain word the repository lacks is no branch name: nothing to say.
        assert_eq!(
            resolve(&n(&[("checked", false)]), &b, "develop"),
            Outcome::None
        );
        assert_eq!(
            resolve(
                &n(&[("checkout", false), ("develop", false)]),
                &b,
                "develop"
            ),
            Outcome::Ambiguous(vec!["checkout".into(), "develop".into()])
        );
        // One found and one missing: the found one is used.
        assert_eq!(
            resolve(&n(&[("checkout", false), ("no-such", true)]), &b, "develop"),
            Outcome::Use("checkout".into())
        );
    }

    #[test]
    fn notes_say_what_happened() {
        assert!(Outcome::None.note("main").is_none());
        let used = Outcome::Use("checkout".into()).note("main").unwrap();
        assert!(used.contains("`checkout`"));
        let missing = Outcome::Missing(vec!["feature/x".into()])
            .note("main")
            .unwrap();
        assert!(missing.contains("`feature/x`") && missing.contains("`main`"));
    }
}
