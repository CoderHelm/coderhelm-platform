//! The rounds on one pull request agree: a planned-work round never builds
//! again what a review took out, unless the ticket asks for it.
//!
//! The check before the pull request compares the branch with the plan. A
//! review round (CoderHelm's own review, or a person's review comments)
//! may take out something the plan put in. The next check then lists that
//! task as planned work not done, a planned-work round builds it again,
//! the review asks for it to be taken out again, and so on. One pull
//! request went build, take out, build, take out in four commits; seven of
//! the week's branches had the same back and forth.
//!
//! A senior engineer settles it once, by the ticket:
//!
//! - A planned item whose work a review commit on the pull request took out
//!   ([`taken_out`]) is not built again ([`Sorted::skipped`]), and the run
//!   says which commit took it out.
//! - When an acceptance criterion of the ticket asks for that item, the
//!   ticket wins: it is built, and the item carries the criterion, so the
//!   round's reply to the review says why it stays ([`Sorted::build`]).

use std::collections::HashMap;

/// What a review commit removed that earlier commits on the pull request
/// had added, in one file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TakenOut {
    pub path: String,
    /// The review commit.
    pub sha: String,
    /// The removed lines, trimmed.
    pub lines: Vec<String>,
}

/// Fewer removed lines than this in a file is a trim, not taking work out.
pub const MIN_LINES: usize = 3;

/// Whether a commit is CoderHelm answering a review: its own review's fix
/// round or a person's review comments (`round::round_message`).
pub fn answers_review(message: &str) -> bool {
    message
        .lines()
        .next()
        .is_some_and(|l| l.contains("Address the review"))
}

/// The lines a patch adds (`'+'`) or removes (`'-'`), trimmed, short ones
/// (a lone brace) left out.
fn patch_lines(patch: &str, sign: char) -> Vec<String> {
    patch
        .lines()
        .filter(|l| !l.starts_with("+++") && !l.starts_with("---"))
        .filter_map(|l| l.strip_prefix(sign))
        .map(str::trim)
        .filter(|l| l.len() > 3)
        .map(str::to_string)
        .collect()
}

/// What a review commit took out: `before` is the pull request's change up
/// to the commit's parent and `review` the commit, each `(path, patch)`. A
/// file counts when the commit removed at least [`MIN_LINES`] lines the
/// pull request had added there.
pub fn taken_out(
    before: &[(String, String)],
    review: &[(String, String)],
    sha: &str,
) -> Vec<TakenOut> {
    let mut added: HashMap<&str, Vec<String>> = HashMap::new();
    for (path, patch) in before {
        added
            .entry(path.as_str())
            .or_default()
            .extend(patch_lines(patch, '+'));
    }
    let mut out = Vec::new();
    for (path, patch) in review {
        let Some(had) = added.get(path.as_str()) else {
            continue;
        };
        let mut pool = had.clone();
        let mut lines = Vec::new();
        for r in patch_lines(patch, '-') {
            if let Some(i) = pool.iter().position(|a| *a == r) {
                pool.swap_remove(i);
                lines.push(r);
            }
        }
        if lines.len() >= MIN_LINES {
            out.push(TakenOut {
                path: path.clone(),
                sha: sha.to_string(),
                lines,
            });
        }
    }
    out
}

/// The code an item quotes in backticks that is no file path: what the
/// item asks to add (`h-svh!`, `frameFitsWindow`).
fn quoted_code(item: &str) -> Vec<String> {
    item.split('`')
        .skip(1)
        .step_by(2)
        .map(str::trim)
        .filter(|q| q.len() >= 3 && super::stack::named_paths(q).is_empty())
        .map(|q| q.trim_matches(|c| c == '"' || c == '\'').to_string())
        .filter(|q| !q.is_empty())
        .collect()
}

/// The review take-out an item asks to build again: one in a file the item
/// names that removed what the item quotes (or, when it quotes no code,
/// any take-out in that file).
pub fn undone_by_review<'a>(item: &str, taken: &'a [TakenOut]) -> Option<&'a TakenOut> {
    let paths = super::stack::named_paths(item);
    let code = quoted_code(item);
    taken.iter().find(|t| {
        paths.contains(&t.path)
            && (code.is_empty() || code.iter().any(|c| t.lines.iter().any(|l| l.contains(c))))
    })
}

/// The acceptance criterion an item serves: the one it shares the most
/// words with, at least two.
pub fn criterion_for<'a>(item: &str, criteria: &'a [String]) -> Option<&'a String> {
    let words = super::minimal_change::stems(item);
    criteria
        .iter()
        .map(|c| {
            let mut cw = super::minimal_change::stems(c);
            cw.sort();
            cw.dedup();
            (cw.iter().filter(|w| words.contains(w)).count(), c)
        })
        .filter(|(n, _)| *n >= 2)
        .max_by_key(|(n, _)| *n)
        .map(|(_, c)| c)
}

/// The planned items, sorted against what the reviews took out.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Sorted {
    /// Built by the next planned-work round. An item a review took out
    /// that a criterion asks for carries the criterion.
    pub build: Vec<String>,
    /// Not built again, with the commit that took it out (for the run's
    /// note).
    pub skipped: Vec<(String, String)>,
}

/// The line added to an item the ticket keeps over a review's take-out.
pub fn kept_line(sha: &str, criterion: &str) -> String {
    format!(
        "(A review took this out in {}; the ticket's acceptance criterion \"{}\" asks for it, so it stays. Say so in the reply to that review finding.)",
        common::truncate_str(sha, 7),
        common::truncate_str(criterion.trim(), 160)
    )
}

/// Sorts the items a planned-work round would build.
pub fn sort(items: &[String], taken: &[TakenOut], criteria: &[String]) -> Sorted {
    let mut out = Sorted::default();
    for item in items {
        match undone_by_review(item, taken) {
            None => out.build.push(item.clone()),
            Some(t) => match criterion_for(item, criteria) {
                Some(c) => out.build.push(format!("{item} {}", kept_line(&t.sha, c))),
                None => out.skipped.push((item.clone(), t.sha.clone())),
            },
        }
    }
    out
}

/// [`sort`] on the check's findings: planned work a review took out is
/// dropped (returned with its commit), and an item a criterion keeps
/// carries [`kept_line`]. Other findings pass as they are.
pub fn sort_findings(
    findings: Vec<super::proof::Finding>,
    taken: &[TakenOut],
    criteria: &[String],
) -> (Vec<super::proof::Finding>, Vec<(String, String)>) {
    if taken.is_empty() {
        return (findings, Vec::new());
    }
    let mut keep = Vec::new();
    let mut skipped = Vec::new();
    for mut f in findings {
        if f.point.trim() != "planned_work" {
            keep.push(f);
            continue;
        }
        let item = format!("{} {}", f.detail, f.settle);
        match undone_by_review(&item, taken) {
            None => keep.push(f),
            Some(t) => match criterion_for(&item, criteria) {
                Some(c) => {
                    f.detail = format!("{} {}", f.detail.trim(), kept_line(&t.sha, c));
                    keep.push(f);
                }
                None => skipped.push((f.detail.clone(), t.sha.clone())),
            },
        }
    }
    (keep, skipped)
}

/// The run's note for the items not built again.
pub fn skipped_note(skipped: &[(String, String)]) -> String {
    let list: Vec<String> = skipped
        .iter()
        .map(|(i, sha)| {
            format!(
                "- {} (taken out by the review in {})",
                common::truncate_str(i, 200),
                common::truncate_str(sha, 7)
            )
        })
        .collect();
    format!(
        "Not built again: a review took this planned work out and no acceptance criterion asks for it:\n{}",
        list.join("\n")
    )
}

/// What the reviews on pull request `number` took out: each review commit
/// against the pull request's change up to its parent. Empty when GitHub
/// can't say (nothing is skipped then).
pub async fn read(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    number: u64,
    base_branch: &str,
) -> Vec<TakenOut> {
    let Ok(commits) = github.list_pr_commits(owner, repo, number).await else {
        return Vec::new();
    };
    let files_of = |v: &serde_json::Value| -> Vec<(String, String)> {
        v["files"]
            .as_array()
            .map(|fs| {
                fs.iter()
                    .map(|f| {
                        (
                            f["filename"].as_str().unwrap_or("").to_string(),
                            f["patch"].as_str().unwrap_or("").to_string(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    let mut out = Vec::new();
    for c in commits
        .iter()
        .filter(|c| answers_review(c["commit"]["message"].as_str().unwrap_or("")))
    {
        let sha = c["sha"].as_str().unwrap_or("");
        let parent = c["parents"][0]["sha"].as_str().unwrap_or("");
        if sha.is_empty() || parent.is_empty() {
            continue;
        }
        let (Ok(before), Ok(own)) = (
            github.get_diff(owner, repo, base_branch, parent).await,
            github.commit_changes(owner, repo, sha).await,
        ) else {
            continue;
        };
        let own: Vec<(String, String)> = own.into_iter().map(|f| (f.path, f.patch)).collect();
        out.extend(taken_out(&files_of(&before), &own, sha));
    }
    out
}
