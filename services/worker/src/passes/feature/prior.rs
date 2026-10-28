//! Rule 2: earlier attempts at the same feature, read before planning.
//!
//! The ticket's history already holds every pull request that names the
//! ticket. A feature is often tried before under another ticket (a parent,
//! a related one) or with no pull request at all, on a branch that's still
//! there. Before planning, this looks for:
//!
//! - pull requests and branches that name the ticket or a ticket its text
//!   names (a parent, a related ticket);
//! - pull requests whose title is close to the ticket's;
//! - branches whose name is close to the ticket's title.
//!
//! Each attempt's commits are read with their files. A commit that neither
//! names the ticket nor shares a word or a file with the attempt's own work
//! is unrelated (a merge of the default branch, a fix for something else
//! picked up on the way) and is never carried over: the planner is told so,
//! and the check before the pull request ([`carried_unrelated`]) refuses a
//! branch that holds files only those commits touched. The run always starts
//! from the default branch ([`fresh_base`]). Where an attempt and the ticket
//! differ, the ticket wins, and the plan lists the differences under
//! [`DIFFERENCES_HEADING`].

use std::collections::{BTreeSet, HashSet};

use tracing::warn;

use crate::clients::github::GitHubClient;
use crate::passes::ticket_history::EarlierPr;

/// The design.md heading the plan lists differences under.
pub const DIFFERENCES_HEADING: &str = "## Differences from earlier attempts";
/// Most attempts read for one ticket.
const MAX_ATTEMPTS: usize = 5;
/// Most commits read per attempt.
const MAX_COMMITS: usize = 15;
/// Title similarity at or above which a pull request or branch counts.
const SIMILAR: f64 = 0.6;

/// How an attempt is linked to the ticket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Link {
    /// Names the ticket itself.
    Ticket,
    /// Names a ticket the ticket's text names.
    Related(String),
    /// Its title or branch is close to the ticket's title (percent).
    Similar(u32),
}

impl Link {
    fn words(&self) -> String {
        match self {
            Link::Ticket => "names this ticket".into(),
            Link::Related(k) => format!("names the related ticket {k}"),
            Link::Similar(p) => format!("its title is {p}% like this ticket's"),
        }
    }
}

/// One commit of an attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptCommit {
    pub sha: String,
    pub message: String,
    pub files: Vec<String>,
    /// A merge commit (two or more parents).
    pub merge: bool,
}

impl AttemptCommit {
    fn short(&self) -> &str {
        self.sha.get(..7).unwrap_or(&self.sha)
    }

    fn subject(&self) -> &str {
        self.message.lines().next().unwrap_or("").trim()
    }
}

/// An earlier attempt: a pull request, or a branch with none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attempt {
    pub repo: String,
    pub branch: String,
    /// 0 for a branch with no pull request.
    pub pr: u64,
    pub title: String,
    /// `open`, `merged`, `closed` (without merging) or `branch`.
    pub state: String,
    /// Why it ended, as far as its record says (who closed it, reviews).
    pub why_ended: String,
    pub link: Link,
    /// Whether the branch is still there.
    pub branch_present: bool,
    pub commits: Vec<AttemptCommit>,
}

impl Attempt {
    pub fn name(&self) -> String {
        if self.pr > 0 {
            format!("#{} ({})", self.pr, self.repo)
        } else {
            format!("branch `{}` ({})", self.branch, self.repo)
        }
    }

    /// How the plan may name it: the pull request number or the branch.
    fn marks(&self) -> Vec<String> {
        let mut m = Vec::new();
        if self.pr > 0 {
            m.push(format!("#{}", self.pr));
            m.push(format!("/pull/{}", self.pr));
        }
        if !self.branch.is_empty() {
            m.push(self.branch.clone());
        }
        m
    }
}

/// What the run found before planning: the attempts, the ticket's keys
/// (its own first) and its title's words.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Found {
    pub attempts: Vec<Attempt>,
    pub keys: Vec<String>,
    pub words: BTreeSet<String>,
}

impl Found {
    pub fn is_empty(&self) -> bool {
        self.attempts.is_empty()
    }

    pub fn note(&self) -> String {
        planner_note(&self.attempts, &self.keys, &self.words)
    }

    /// One line per attempt, for the pull request.
    pub fn pr_lines(&self) -> Vec<String> {
        self.attempts
            .iter()
            .map(|a| {
                let (own, unrelated) = classify(a, &self.keys, &self.words);
                let mut l = format!("{} \"{}\": {}", a.name(), a.title.trim(), a.state);
                if !a.why_ended.trim().is_empty() && a.state != "branch" {
                    l.push_str(&format!(
                        " ({})",
                        common::truncate_str(a.why_ended.trim(), 200)
                    ));
                }
                if !own.is_empty() {
                    l.push_str(&format!("; its own work touched {} file(s)", own.len()));
                }
                if !unrelated.is_empty() {
                    l.push_str(&format!(
                        "; {} unrelated commit(s) not carried over",
                        unrelated.len()
                    ));
                }
                l
            })
            .collect()
    }
}

/// Jira-style keys a text names (`ABC-123`), in order, deduped.
pub fn ticket_keys(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let start_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
        if start_ok && bytes[i].is_ascii_uppercase() {
            let mut j = i;
            while j < bytes.len() && (bytes[j].is_ascii_uppercase() || bytes[j].is_ascii_digit()) {
                j += 1;
            }
            let prefix_len = j - i;
            if (2..=10).contains(&prefix_len) && j < bytes.len() && bytes[j] == b'-' {
                let mut k = j + 1;
                while k < bytes.len() && bytes[k].is_ascii_digit() {
                    k += 1;
                }
                let end_ok = k == bytes.len() || !bytes[k].is_ascii_alphanumeric();
                if k > j + 1 && end_ok {
                    let key = text[i..k].to_string();
                    if !out.contains(&key) {
                        out.push(key);
                    }
                    i = k;
                    continue;
                }
            }
            i = j.max(i + 1);
            continue;
        }
        i += 1;
    }
    out
}

const STOP: &[&str] = &[
    "the",
    "and",
    "for",
    "with",
    "from",
    "into",
    "this",
    "that",
    "add",
    "adds",
    "new",
    "make",
    "allow",
    "support",
    "should",
    "when",
    "page",
    "feat",
    "feature",
    "fix",
    "chore",
    "coderhelm",
    "update",
    "ticket",
];

/// A title's significant words: lowercase, three letters or more, no stop
/// words. Splits on anything that isn't a letter or digit, so a branch name
/// (`feat/quote-block`) reads like a title.
pub fn words(text: &str) -> BTreeSet<String> {
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .map(str::to_ascii_lowercase)
        .filter(|w| {
            w.len() >= 3 && !STOP.contains(&w.as_str()) && !w.chars().all(|c| c.is_ascii_digit())
        })
        .collect()
}

/// How alike two word sets are (shared over all), 0 to 1.
pub fn similarity(a: &BTreeSet<String>, b: &BTreeSet<String>) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let shared = a.intersection(b).count() as f64;
    shared / a.union(b).count() as f64
}

fn names_key(text: &str, key: &str) -> bool {
    !key.is_empty() && crate::passes::mentions_ticket(text, key)
}

/// How a pull request or branch is linked to the ticket, if it is.
pub fn link_for(
    texts: &[&str],
    title: &str,
    ticket_id: &str,
    related: &[String],
    ticket_words: &BTreeSet<String>,
) -> Option<Link> {
    if texts.iter().any(|t| names_key(t, ticket_id)) {
        return Some(Link::Ticket);
    }
    for k in related {
        if texts.iter().any(|t| names_key(t, k)) {
            return Some(Link::Related(k.clone()));
        }
    }
    let s = similarity(ticket_words, &words(title));
    (s >= SIMILAR).then(|| Link::Similar((s * 100.0).round() as u32))
}

/// The files an attempt's own work touched, and its commits that are
/// unrelated to the ticket. A commit is the attempt's own when its message
/// names the ticket (or a related one) or shares a word with the ticket's
/// title, or when it touches a file such a commit touched or whose path
/// shares a word with the title. A merge commit is never the attempt's own.
/// When no commit qualifies, every non-merge commit is taken as its own (an
/// attempt nothing can be told apart in).
pub fn classify(
    a: &Attempt,
    keys: &[String],
    ticket_words: &BTreeSet<String>,
) -> (Vec<String>, Vec<AttemptCommit>) {
    let own_by_words = |c: &AttemptCommit| {
        keys.iter().any(|k| names_key(&c.message, k))
            || !words(c.subject()).is_disjoint(ticket_words)
            || c.files.iter().any(|f| !words(f).is_disjoint(ticket_words))
    };
    let mut own: Vec<&AttemptCommit> = a
        .commits
        .iter()
        .filter(|c| !c.merge && own_by_words(c))
        .collect();
    if own.is_empty() {
        own = a.commits.iter().filter(|c| !c.merge).collect();
    }
    let mut files: BTreeSet<String> = own.iter().flat_map(|c| c.files.iter().cloned()).collect();
    // A commit that touches the attempt's own files is part of it.
    loop {
        let before = files.len();
        for c in a.commits.iter().filter(|c| !c.merge) {
            if c.files.iter().any(|f| files.contains(f)) {
                files.extend(c.files.iter().cloned());
            }
        }
        if files.len() == before {
            break;
        }
    }
    let unrelated: Vec<AttemptCommit> = a
        .commits
        .iter()
        .filter(|c| c.merge || !c.files.iter().any(|f| files.contains(f)))
        .cloned()
        .collect();
    (files.into_iter().collect(), unrelated)
}

/// The base a run's branch is made from: always the default branch, never an
/// earlier attempt's branch.
pub fn fresh_base<'a>(base: &'a str, default_branch: &'a str, attempts: &[Attempt]) -> &'a str {
    if !default_branch.is_empty() && attempts.iter().any(|a| a.branch == base) {
        default_branch
    } else {
        base
    }
}

/// What the planner is told. Empty without attempts.
pub fn planner_note(
    attempts: &[Attempt],
    keys: &[String],
    ticket_words: &BTreeSet<String>,
) -> String {
    if attempts.is_empty() {
        return String::new();
    }
    let mut out = String::from(
        "\n\n## Earlier attempts at this feature (read before planning)\n\
         These pull requests and branches look like earlier work on the same feature. For each: \
         say what it did and why it ended, reuse what fits (files, helpers, tests it named), and \
         never carry over its unrelated commits. Your branch starts fresh from the default \
         branch; nothing is copied from these branches unless your tasks write it. Where an \
         attempt and this ticket differ, the ticket wins: list every difference in design.md \
         under `",
    );
    out.push_str(DIFFERENCES_HEADING);
    out.push_str("`, naming each attempt by its pull request number or branch.\n");
    for a in attempts {
        let (files, unrelated) = classify(a, keys, ticket_words);
        out.push_str(&format!(
            "- {} \"{}\" on branch `{}`: {}; {}{}.\n",
            a.name(),
            a.title.trim(),
            a.branch,
            a.state,
            a.link.words(),
            if a.branch_present {
                ""
            } else {
                "; the branch is gone"
            }
        ));
        if !a.why_ended.trim().is_empty() {
            out.push_str(&format!("  Why it ended: {}\n", a.why_ended.trim()));
        }
        if !files.is_empty() {
            out.push_str(&format!(
                "  Its own work touched: {}\n",
                files
                    .iter()
                    .take(25)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        for c in unrelated.iter().take(10) {
            out.push_str(&format!(
                "  UNRELATED commit {} \"{}\"{} touching {}: do not carry it over.\n",
                c.short(),
                c.subject(),
                if c.merge { " (merge)" } else { "" },
                if c.files.is_empty() {
                    "no files".to_string()
                } else {
                    c.files
                        .iter()
                        .take(8)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            ));
        }
    }
    out
}

/// What the plan leaves out about the attempts: the differences heading, or
/// an attempt it never names.
pub fn plan_gaps(design: &str, plan_text: &str, attempts: &[Attempt]) -> Vec<String> {
    if attempts.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    if !design.contains(DIFFERENCES_HEADING) {
        out.push(format!(
            "design.md has no `{DIFFERENCES_HEADING}` section: list where the earlier attempts and \
             this ticket differ (the ticket wins), what each did, why it ended and what you reuse."
        ));
    }
    for a in attempts {
        if !a.marks().iter().any(|m| plan_text.contains(m.as_str())) {
            out.push(format!(
                "The plan never names the earlier attempt {}: say what it did, why it ended, and \
                 what you reuse from it.",
                a.name()
            ));
        }
    }
    out
}

/// Files the run's branch changes that only an earlier attempt's unrelated
/// commits touched, and that no plan task names: carried over, never asked
/// for. As (path, the attempt and commit it came from).
pub fn carried_unrelated(
    changed: &[String],
    planned: &[String],
    attempts: &[Attempt],
    keys: &[String],
    ticket_words: &BTreeSet<String>,
) -> Vec<(String, String)> {
    let planned: HashSet<&str> = planned.iter().map(String::as_str).collect();
    let mut out = Vec::new();
    for a in attempts {
        let (own, unrelated) = classify(a, keys, ticket_words);
        for c in &unrelated {
            for f in &c.files {
                if changed.contains(f)
                    && !planned.contains(f.as_str())
                    && !own.contains(f)
                    && !out.iter().any(|(p, _): &(String, String)| p == f)
                {
                    out.push((
                        f.clone(),
                        format!("{} commit {} \"{}\"", a.name(), c.short(), c.subject()),
                    ));
                }
            }
        }
    }
    out
}

/// The commits of a branch or pull request, with their files.
async fn read_commits(
    github: &GitHubClient,
    owner: &str,
    name: &str,
    default_branch: &str,
    branch: &str,
    pr: u64,
    branch_present: bool,
) -> Vec<AttemptCommit> {
    let raw = if branch_present {
        github
            .compare_commits(owner, name, default_branch, branch, MAX_COMMITS)
            .await
    } else if pr > 0 {
        github.list_pr_commits(owner, name, pr).await
    } else {
        return Vec::new();
    };
    let raw = match raw {
        Ok(v) => v,
        Err(e) => {
            warn!(branch, pr, error = %e, "Could not read an earlier attempt's commits");
            return Vec::new();
        }
    };
    let mut out = Vec::new();
    for c in raw.iter().take(MAX_COMMITS) {
        let sha = c["sha"].as_str().unwrap_or("").to_string();
        if sha.is_empty() {
            continue;
        }
        let files = github
            .commit_files(owner, name, &sha)
            .await
            .unwrap_or_default();
        out.push(AttemptCommit {
            message: c["commit"]["message"].as_str().unwrap_or("").to_string(),
            merge: c["parents"].as_array().is_some_and(|p| p.len() > 1),
            sha,
            files,
        });
    }
    out
}

fn why_ended(p: &EarlierPr) -> String {
    let mut parts = Vec::new();
    match p.state.as_str() {
        "closed" if !p.closed_by.is_empty() => parts.push(format!(
            "closed without merging by {} on {}",
            p.closed_by,
            p.closed_on()
        )),
        "closed" => parts.push("closed without merging".to_string()),
        "merged" => parts.push("merged".to_string()),
        _ => parts.push("still open".to_string()),
    }
    if let Some((author, state, text)) = p.reviews.last() {
        parts.push(format!(
            "last review by {author} ({state}): {}",
            common::truncate_str(text.trim(), 300)
        ));
    }
    if let Some((author, text)) = p.comments.last() {
        parts.push(format!(
            "last comment by {author}: {}",
            common::truncate_str(text.trim(), 300)
        ));
    }
    parts.join("; ")
}

/// What the run looks for, from the ticket.
pub struct Search<'a> {
    pub owner: &'a str,
    pub name: &'a str,
    pub default_branch: &'a str,
    pub ticket_id: &'a str,
    pub title: &'a str,
    pub body: &'a str,
    /// The run's own branch: it is reset from the default branch anyway.
    pub own_branch: &'a str,
}

impl Search<'_> {
    /// The ticket and the tickets its text names.
    pub fn keys(&self) -> Vec<String> {
        let mut k = vec![self.ticket_id.to_string()];
        for x in ticket_keys(&format!("{}\n{}", self.title, self.body)) {
            if !k.contains(&x) {
                k.push(x);
            }
        }
        k.retain(|x| !x.is_empty());
        k
    }

    pub fn ticket_words(&self) -> BTreeSet<String> {
        words(self.title)
    }
}

/// Finds the earlier attempts in the run's repository. `known` are the pull
/// requests the ticket's history already read in full. Best-effort and
/// bounded: a list that can't be read adds nothing.
pub async fn gather(github: &GitHubClient, s: &Search<'_>, known: &[EarlierPr]) -> Vec<Attempt> {
    let repo = format!("{}/{}", s.owner, s.name);
    let keys = s.keys();
    let related: Vec<String> = keys.iter().skip(1).cloned().collect();
    let tw = s.ticket_words();
    let branches: Vec<String> = github
        .list_branches(s.owner, s.name, 500)
        .await
        .unwrap_or_else(|e| {
            warn!(repo, error = %e, "Could not list branches for earlier attempts");
            Vec::new()
        });
    let mut out: Vec<Attempt> = Vec::new();
    // An open pull request isn't an earlier attempt: the run's open-PR
    // handling works on it (or asks) before anything is planned.
    let add_pr = |p: &EarlierPr, link: Link, out: &mut Vec<Attempt>| {
        if p.state == "open" || out.iter().any(|a| a.pr == p.number) {
            return;
        }
        out.push(Attempt {
            repo: repo.clone(),
            branch: p.branch.clone(),
            pr: p.number,
            title: p.title.clone(),
            state: p.state.clone(),
            why_ended: why_ended(p),
            link,
            branch_present: branches.contains(&p.branch),
            commits: Vec::new(),
        });
    };
    for p in known.iter().filter(|p| p.repo == repo) {
        add_pr(p, Link::Ticket, &mut out);
    }
    // Pull requests that name a related ticket, or read like this one.
    if let Ok(v) = github.list_pull_requests(s.owner, s.name, "all", 100).await {
        for p in v.as_array().into_iter().flatten() {
            let number = p["number"].as_u64().unwrap_or(0);
            let title = p["title"].as_str().unwrap_or("");
            let branch = p["head"]["ref"].as_str().unwrap_or("");
            let body = p["body"].as_str().unwrap_or("");
            if number == 0 || out.iter().any(|a| a.pr == number) {
                continue;
            }
            let Some(link) = link_for(&[title, branch, body], title, s.ticket_id, &related, &tw)
            else {
                continue;
            };
            if let Some(full) = crate::passes::ticket_history::read_pr(github, &repo, number).await
            {
                add_pr(&full, link, &mut out);
            }
            if out.len() >= MAX_ATTEMPTS {
                break;
            }
        }
    }
    // Branches with no pull request that name the ticket or read like it.
    for b in &branches {
        if out.len() >= MAX_ATTEMPTS {
            break;
        }
        if b == s.default_branch || b == s.own_branch || out.iter().any(|a| &a.branch == b) {
            continue;
        }
        if let Some(link) = link_for(&[b], b, s.ticket_id, &related, &tw) {
            out.push(Attempt {
                repo: repo.clone(),
                branch: b.clone(),
                pr: 0,
                title: b.clone(),
                state: "branch".into(),
                why_ended: "a branch with no pull request".into(),
                link,
                branch_present: true,
                commits: Vec::new(),
            });
        }
    }
    out.truncate(MAX_ATTEMPTS);
    for a in &mut out {
        a.commits = read_commits(
            github,
            s.owner,
            s.name,
            s.default_branch,
            &a.branch,
            a.pr,
            a.branch_present,
        )
        .await;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit(sha: &str, msg: &str, files: &[&str]) -> AttemptCommit {
        AttemptCommit {
            sha: sha.into(),
            message: msg.into(),
            files: files.iter().map(|f| f.to_string()).collect(),
            merge: false,
        }
    }

    fn attempt(commits: Vec<AttemptCommit>) -> Attempt {
        Attempt {
            repo: "o/web".into(),
            branch: "feat/quote-block".into(),
            pr: 41,
            title: "Quote block for pages".into(),
            state: "closed".into(),
            why_ended: "closed without merging by sam on 2026-09-01".into(),
            link: Link::Similar(75),
            branch_present: true,
            commits,
        }
    }

    #[test]
    fn reads_ticket_keys_from_text() {
        assert_eq!(
            ticket_keys("Parent: WEB-12. Related to OPS-7 and WEB-12; not abc-1 or X-9"),
            vec!["WEB-12", "OPS-7"]
        );
        assert!(ticket_keys("no keys here, utf-8 is not one").is_empty());
    }

    #[test]
    fn links_by_ticket_then_related_then_similar_title() {
        let tw = words("Add a quote block to content pages");
        let rel = vec!["WEB-12".to_string()];
        assert_eq!(
            link_for(&["WEB-40 quote"], "x", "WEB-40", &rel, &tw),
            Some(Link::Ticket)
        );
        assert_eq!(
            link_for(&["feat/WEB-12-quote"], "x", "WEB-40", &rel, &tw),
            Some(Link::Related("WEB-12".into()))
        );
        assert!(matches!(
            link_for(
                &["feat/quote-block-content"],
                "feat/quote-block-content",
                "WEB-40",
                &rel,
                &tw
            ),
            Some(Link::Similar(_))
        ));
        assert_eq!(
            link_for(&["fix/login"], "fix/login", "WEB-40", &rel, &tw),
            None
        );
    }

    #[test]
    fn unrelated_commits_are_told_apart_from_the_attempts_own_work() {
        let mut merge = commit("m1", "Merge branch 'main'", &["src/pay/checkout.ts"]);
        merge.merge = true;
        let a = attempt(vec![
            commit("a1", "WEB-40 add quote block", &["src/blocks/Quote.tsx"]),
            commit(
                "a2",
                "Register it",
                &["src/blocks/index.ts", "src/blocks/Quote.tsx"],
            ),
            commit("b1", "Fix login redirect", &["src/auth/login.ts"]),
            merge,
        ]);
        let keys = vec!["WEB-40".to_string()];
        let tw = words("Quote block");
        let (own, unrelated) = classify(&a, &keys, &tw);
        assert_eq!(own, vec!["src/blocks/Quote.tsx", "src/blocks/index.ts"]);
        let shas: Vec<&str> = unrelated.iter().map(|c| c.sha.as_str()).collect();
        assert_eq!(shas, vec!["b1", "m1"]);

        let note = planner_note(std::slice::from_ref(&a), &keys, &tw);
        assert!(note.contains(DIFFERENCES_HEADING));
        assert!(note.contains("UNRELATED commit b1"));
        assert!(note.contains("src/auth/login.ts"));
        assert!(note.contains("closed without merging by sam"));

        // A branch that carries the login fix over is caught; a planned file
        // isn't.
        let changed = vec![
            "src/blocks/Quote.tsx".to_string(),
            "src/auth/login.ts".to_string(),
            "src/pay/checkout.ts".to_string(),
        ];
        let carried = carried_unrelated(
            &changed,
            &["src/pay/checkout.ts".to_string()],
            &[a],
            &keys,
            &tw,
        );
        assert_eq!(carried.len(), 1);
        assert_eq!(carried[0].0, "src/auth/login.ts");
    }

    #[test]
    fn the_run_always_starts_from_the_default_branch() {
        let a = attempt(vec![]);
        assert_eq!(
            fresh_base("feat/quote-block", "main", std::slice::from_ref(&a)),
            "main"
        );
        assert_eq!(fresh_base("main", "main", &[a]), "main");
        assert_eq!(fresh_base("develop", "main", &[]), "develop");
    }

    #[test]
    fn the_plan_must_list_differences_and_name_each_attempt() {
        let a = attempt(vec![]);
        let gaps = plan_gaps("## Design\nstuff", "stuff", std::slice::from_ref(&a));
        assert_eq!(gaps.len(), 2);
        let design = format!("{DIFFERENCES_HEADING}\n- #41 used a modal; the ticket asks inline");
        assert!(plan_gaps(&design, &design, &[a]).is_empty());
        assert!(plan_gaps("", "", &[]).is_empty());
    }
}
