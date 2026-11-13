//! Building on another open pull request instead of copying it.
//!
//! A run once started its branch from the default branch while the ticket
//! depended on another open (work-in-progress) pull request, and the
//! branch's first commits re-created that pull request's commits under new
//! hashes. The rules here:
//!
//! - [`dependency`]: a ticket that names the open pull request it depends on
//!   (a link or `#123` in a line that says it depends on, builds on, is
//!   blocked by or comes after it), or the ticket key of one, starts its
//!   branch from that pull request's branch, and its pull request targets
//!   that branch (stacked). It states the dependency and the merge order.
//!   Two more signals need no dependency words: a ticket titled as part of a
//!   series (`[Name 2/4]`, `(2/4) …`, `Name (2/4)`, `Part 2 of 4: …`) builds
//!   on the latest earlier part that still has
//!   an open pull request ([`series`]), and a ticket that names a file the
//!   base branch doesn't have yet builds on the one open pull request that
//!   adds it ([`by_files`]).
//! - [`copied`]: before the pull request opens, the branch's commits are
//!   compared with every other open pull request's commits by patch id (the
//!   change itself, whitespace and line numbers aside). A branch that
//!   re-created another pull request's commits (any open pull request of
//!   the repository, CoderHelm's or a person's) is moved onto that pull
//!   request's branch without the copies and stacks on it ([`before_pr`]):
//!   its own change is rebuilt there file by file as one commit
//!   ([`super::restack`]), however many commits it has and without merging
//!   anything first. Copies usually come from the dependency's branch being
//!   rebased after the run started on it: its commits got new hashes and
//!   the run's branch still holds the old ones. A branch kept from an
//!   earlier attempt is rebuilt the same way before any implementation
//!   ([`at_start`]), and a force push to a branch CoderHelm's open pull
//!   requests stack on rebuilds them ([`super::restack::after_forced_push`]).
//!   The pull request always opens; only a missing GitHub App permission
//!   stops the run, naming it.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};

use regex::Regex;
use serde_json::Value;

use crate::clients::github::{CommitFile, GitHubClient};

use super::restack;

/// The open pull request a branch builds on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Dependency {
    pub number: u64,
    pub branch: String,
    pub title: String,
    /// How it was found (the ticket's line, or the commits it copied).
    pub why: String,
    /// Files both changed whose two versions couldn't be merged: the
    /// rebuilt commit keeps the dependency's, a labelled follow-up commit
    /// puts this change's back. The pull request stays a draft.
    pub unresolved: Vec<String>,
    /// The branch repeats the dependency's commits and couldn't be rebuilt
    /// on it: the pull request targets the original base, as a draft.
    pub not_stacked: bool,
}

impl Dependency {
    /// The pull request section: what it builds on and the merge order.
    pub fn section(&self) -> String {
        let (n, b) = (self.number, &self.branch);
        let t = self.title.replace('"', "'");
        let why = self.why.trim();
        let mut out = if self.not_stacked {
            format!(
                "## Repeats #{n}\n\nThis pull request carries commits of #{n} (`{b}`, \"{t}\") \
                 under other hashes, so its diff shows them too. {why}\n\n\
                 Merge order: #{n} first, then this one. It stays a draft until it is rebuilt on \
                 `{b}`: running the ticket again does that."
            )
        } else {
            format!(
                "## Builds on #{n}\n\nThis pull request is stacked on #{n} (`{b}`, \"{t}\"): its \
                 base is that branch, so its diff shows only this ticket's change. {why}\n\n\
                 Merge order: #{n} first, then this one. When #{n} merges, GitHub moves this pull \
                 request's base to #{n}'s base; if it then needs a rebase, comment \"rebase\" on \
                 it and CoderHelm brings it up to date."
            )
        };
        if !self.unresolved.is_empty() {
            out.push_str(&format!(
                "\n\n### Needs a look\n\n#{n} also changed these files, and the two versions \
                 couldn't be merged. The rebuilt commit keeps #{n}'s version; the commit after it, \
                 labelled \"Follow-up\", puts this change's back. Check that each keeps #{n}'s \
                 work before merging; the pull request stays a draft until then.\n"
            ));
            for p in &self.unresolved {
                out.push_str(&format!("\n- `{p}`"));
            }
        }
        let hold = if self.not_stacked || !self.unresolved.is_empty() {
            format!("\n{HOLD}")
        } else {
            String::new()
        };
        format!("{SECTION_START}\n{out}{hold}\n{SECTION_END}")
    }
}

pub const SECTION_START: &str = "<!-- coderhelm:stack -->";
pub const SECTION_END: &str = "<!-- /coderhelm:stack -->";
/// In the stack section when a person has to look before it merges.
const HOLD: &str = "<!-- coderhelm:stack-hold -->";

/// Why a pull request stays a draft: its stack section asks a person to
/// look first (files both pull requests changed, or a branch that still
/// repeats the other pull request's commits).
pub fn hold(body: &str) -> Option<String> {
    let (a, b) = (body.find(SECTION_START)?, body.find(SECTION_END)?);
    (b > a && body[a..b].contains(HOLD)).then(|| {
        "the stacked change asks for a look first (see its \"Builds on\" section)".to_string()
    })
}

/// Words that make a line say the ticket depends on what it names.
const DEPENDS: &[&str] = &[
    "depends on",
    "dependent on",
    "depend on",
    "blocked by",
    "builds on",
    "build on",
    "built on",
    "on top of",
    "stacked on",
    "after",
    "requires",
    "needs",
    "follow-up to",
    "follows",
    "wip",
];

fn says_depends(line: &str) -> bool {
    let l = line.to_ascii_lowercase();
    DEPENDS.iter().any(|w| {
        l.match_indices(w).any(|(i, _)| {
            let before = l[..i].chars().next_back();
            let after = l[i + w.len()..].chars().next();
            !before.is_some_and(|c| c.is_ascii_alphanumeric())
                && !after.is_some_and(|c| c.is_ascii_alphanumeric())
        })
    })
}

/// What a ticket names as its dependencies: pull request numbers of this
/// repository, and other tickets' keys, from the lines that say it depends
/// on them.
pub fn named(text: &str, owner: &str, repo: &str, own_ticket: &str) -> (Vec<u64>, Vec<String>) {
    static KEY: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let key = KEY.get_or_init(|| Regex::new(r"\b([A-Z][A-Z0-9]{1,9}-\d{1,6})\b").unwrap());
    let url = Regex::new(&format!(
        r"(?i)github\.com/{}/{}/pull/(\d+)",
        regex::escape(owner),
        regex::escape(repo)
    ))
    .unwrap();
    static NUM: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let num = NUM.get_or_init(|| Regex::new(r"(?i)(?:^|[\s(])(?:pr\s*)?#(\d{1,7})\b").unwrap());
    let mut prs = Vec::new();
    let mut keys = Vec::new();
    for line in text.lines() {
        if !says_depends(line) {
            continue;
        }
        for c in url.captures_iter(line).chain(num.captures_iter(line)) {
            if let Ok(n) = c[1].parse::<u64>() {
                if !prs.contains(&n) {
                    prs.push(n);
                }
            }
        }
        for c in key.captures_iter(line) {
            let k = c[1].to_string();
            if !k.eq_ignore_ascii_case(own_ticket) && !keys.contains(&k) {
                keys.push(k);
            }
        }
    }
    (prs, keys)
}

/// The open pull request (from `open`, the open pulls listing) a ticket
/// depends on: one it names by number, else one whose title or branch
/// carries a ticket key it names. Only pull requests from this repository
/// (a fork's branch can't be a base), never the run's own branch.
pub fn pick(
    open: &[Value],
    prs: &[u64],
    keys: &[String],
    owner: &str,
    repo: &str,
    own_branch: &str,
) -> Option<Dependency> {
    let full = format!("{owner}/{repo}").to_ascii_lowercase();
    let usable = |p: &&Value| {
        p["head"]["repo"]["full_name"]
            .as_str()
            .is_some_and(|f| f.eq_ignore_ascii_case(&full))
            && p["head"]["ref"].as_str().is_some_and(|b| b != own_branch)
    };
    let dep = |p: &Value, why: String| Dependency {
        number: p["number"].as_u64().unwrap_or(0),
        branch: p["head"]["ref"].as_str().unwrap_or("").to_string(),
        title: p["title"].as_str().unwrap_or("").to_string(),
        why,
        ..Default::default()
    };
    for n in prs {
        if let Some(p) = open
            .iter()
            .filter(usable)
            .find(|p| p["number"].as_u64() == Some(*n))
        {
            return Some(dep(p, format!("The ticket says it depends on #{n}.")));
        }
    }
    for k in keys {
        let kl = k.to_ascii_lowercase();
        if let Some(p) = open.iter().filter(usable).find(|p| {
            p["title"]
                .as_str()
                .is_some_and(|t| t.to_ascii_lowercase().contains(&kl))
                || p["head"]["ref"]
                    .as_str()
                    .is_some_and(|b| b.to_ascii_lowercase().contains(&kl))
        }) {
            return Some(dep(
                p,
                format!("The ticket says it depends on {k}, whose open pull request this is."),
            ));
        }
    }
    None
}

/// A ticket title's place in a series.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Series {
    /// The series' name, lowercased: written inside the marker
    /// (`[Titles 2/4]`) or as a label before it (`Titles (2/4): …`).
    /// None when the marker carries no name (`(2/4) Section titles`).
    pub name: Option<String>,
    pub part: u32,
    pub total: u32,
    /// The title's other words (lowercase, four letters or more, common
    /// words left out): how nameless parts are matched.
    pub words: Vec<String>,
}

/// Common words that never tie two titles together.
const COMMON: &[&str] = &[
    "this", "that", "with", "from", "into", "have", "will", "make", "should", "when", "then",
    "than", "more", "less", "also", "only", "each", "them", "they", "their", "there", "page",
    "pages", "part", "about", "after", "before", "where", "which", "what", "your", "some",
    "update", "change", "changes", "add", "adds", "fix", "fixes", "use", "uses",
];

fn title_words(text: &str) -> Vec<String> {
    let mut out: Vec<String> = text
        .split(|c: char| !c.is_ascii_alphanumeric())
        .map(|w| w.to_ascii_lowercase())
        .filter(|w| w.len() >= 4 && !w.chars().all(|c| c.is_ascii_digit()))
        .filter(|w| !COMMON.contains(&w.as_str()))
        .collect();
    out.sort();
    out.dedup();
    out
}

/// A label before a marker is the series' name only when a separator
/// follows the marker (`Titles (2/4): …`, `Titles [2/4] - …`); a ticket
/// key prefix (`ABC-12:`) is left out.
fn label_name(before: &str, after: &str) -> Option<String> {
    let sep = after
        .trim_start()
        .starts_with([':', '-', '\u{2013}', '\u{2014}', '|']);
    if !sep {
        return None;
    }
    static KEY: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let key = KEY.get_or_init(|| Regex::new(r"^\s*[A-Z][A-Z0-9]+-\d+\s*[:\-]?\s*").unwrap());
    let label = key.replace(before, "");
    let label = label.trim().trim_end_matches([',', ':', '-']).trim();
    (!label.is_empty() && label.len() <= 40 && label.chars().any(|c| c.is_ascii_alphabetic()))
        .then(|| label.to_ascii_lowercase())
}

/// A ticket title's place in a series, from any of these shapes:
/// `[Name 2/4]` or `(Name 2/4)`; `[2/4]` or `(2/4)`, with the name from a
/// label before it when a separator follows (`Titles (2/4): …`); `Part 2
/// of 4`. None for part 0 or a part past the total.
pub fn series(title: &str) -> Option<Series> {
    static NAMED: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    static BARE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    static PART: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let named = NAMED.get_or_init(|| {
        Regex::new(r"[\[(]\s*([A-Za-z][^\[\]()/]*?)\s+(\d{1,3})\s*/\s*(\d{1,3})\s*[\])]").unwrap()
    });
    let bare =
        BARE.get_or_init(|| Regex::new(r"[\[(]\s*(\d{1,3})\s*/\s*(\d{1,3})\s*[\])]").unwrap());
    let part =
        PART.get_or_init(|| Regex::new(r"(?i)\bpart\s+(\d{1,3})\s+of\s+(\d{1,3})\b").unwrap());
    let (name, n, m, range) = if let Some(c) = named.captures(title) {
        let r = c.get(0)?.range();
        (
            Some(c[1].trim().to_ascii_lowercase()),
            c[2].parse().ok()?,
            c[3].parse().ok()?,
            r,
        )
    } else if let Some(c) = bare.captures(title) {
        let r = c.get(0)?.range();
        let name = label_name(&title[..r.start], &title[r.end..]);
        (name, c[1].parse().ok()?, c[2].parse().ok()?, r)
    } else {
        let c = part.captures(title)?;
        let r = c.get(0)?.range();
        let name = label_name(&title[..r.start], &title[r.end..]);
        (name, c[1].parse().ok()?, c[2].parse().ok()?, r)
    };
    let (n, m): (u32, u32) = (n, m);
    if !(n >= 1 && n <= m && m >= 2) {
        return None;
    }
    let rest = format!("{} {}", &title[..range.start], &title[range.end..]);
    Some(Series {
        name,
        part: n,
        total: m,
        words: title_words(&rest),
    })
}

/// Whether two titles' series are the same series: same total, and the same
/// name; a nameless one matches a named one whose name is among its words,
/// and two nameless ones match when their titles share a word.
fn same_series(a: &Series, b: &Series) -> bool {
    if a.total != b.total {
        return false;
    }
    let in_words = |name: &str, words: &[String]| {
        let w = title_words(name);
        !w.is_empty() && w.iter().all(|x| words.contains(x))
    };
    match (&a.name, &b.name) {
        (Some(x), Some(y)) => x == y,
        (Some(x), None) => in_words(x, &b.words),
        (None, Some(y)) => in_words(y, &a.words),
        (None, None) => a.words.iter().any(|w| b.words.contains(w)),
    }
}

/// The open pull request of the latest earlier part of the same series
/// (see [`same_series`]; a lower part number), from this repository and
/// never the run's own branch. Two open pull requests for that part: no
/// guess.
pub fn pick_series(
    open: &[Value],
    title: &str,
    owner: &str,
    repo: &str,
    own_branch: &str,
) -> Option<Dependency> {
    let mine = series(title)?;
    let (n, m) = (mine.part, mine.total);
    let full = format!("{owner}/{repo}").to_ascii_lowercase();
    let parts: Vec<(u32, &Value)> = open
        .iter()
        .filter(|p| {
            p["head"]["repo"]["full_name"]
                .as_str()
                .is_some_and(|f| f.eq_ignore_ascii_case(&full))
                && p["head"]["ref"].as_str().is_some_and(|b| b != own_branch)
        })
        .filter_map(|p| {
            let theirs = series(p["title"].as_str()?)?;
            (theirs.part < n && same_series(&mine, &theirs)).then_some((theirs.part, p))
        })
        .collect();
    let k = parts.iter().map(|(k, _)| *k).max()?;
    let latest: Vec<&Value> = parts
        .iter()
        .filter(|(x, _)| *x == k)
        .map(|(_, p)| *p)
        .collect();
    if latest.len() != 1 {
        return None;
    }
    let p = latest[0];
    Some(Dependency {
        number: p["number"].as_u64().unwrap_or(0),
        branch: p["head"]["ref"].as_str().unwrap_or("").to_string(),
        title: p["title"].as_str().unwrap_or("").to_string(),
        why: format!("This ticket is part {n} of {m}; part {k}'s pull request is still open."),
        ..Default::default()
    })
}

/// Repository paths a ticket names (`apps/web/src/ui/heading.tsx`): a
/// relative path with a directory and an extension, at most 10, in order.
pub fn named_paths(text: &str) -> Vec<String> {
    static PATH: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = PATH.get_or_init(|| {
        Regex::new(
            r"(?:^|[\s(`'\x22\[])((?:[A-Za-z0-9_@.\-]+/)+[A-Za-z0-9_.\-\[\]]+\.[A-Za-z0-9]{1,8})\b",
        )
        .unwrap()
    });
    let mut out: Vec<String> = Vec::new();
    for c in re.captures_iter(text) {
        let p = c[1].trim_start_matches("./").to_string();
        if p.contains("://") || p.starts_with('.') || p.contains("..") || out.contains(&p) {
            continue;
        }
        out.push(p);
        if out.len() == 10 {
            break;
        }
    }
    out
}

/// The one open pull request that adds the files a ticket names and the
/// base branch lacks: `changes` is each candidate (from this repository,
/// never the run's own branch) with the paths it changes. The pull request
/// covering the most missing paths wins; a tie is no dependency.
pub fn by_files(missing: &[String], changes: &[(Value, Vec<String>)]) -> Option<Dependency> {
    if missing.is_empty() {
        return None;
    }
    let mut best: Option<(usize, &Value, &String)> = None;
    let mut tie = false;
    for (p, files) in changes {
        let hit: Vec<&String> = missing.iter().filter(|m| files.contains(m)).collect();
        if hit.is_empty() {
            continue;
        }
        match best {
            Some((n, _, _)) if hit.len() < n => {}
            Some((n, _, _)) if hit.len() == n => tie = true,
            _ => {
                best = Some((hit.len(), p, hit[0]));
                tie = false;
            }
        }
    }
    if tie {
        return None;
    }
    best.map(|(_, p, path)| Dependency {
        number: p["number"].as_u64().unwrap_or(0),
        branch: p["head"]["ref"].as_str().unwrap_or("").to_string(),
        title: p["title"].as_str().unwrap_or("").to_string(),
        why: format!(
            "The ticket names `{path}`, which the base branch doesn't have yet; this pull request adds it."
        ),
        ..Default::default()
    })
}

/// The open pull request the ticket depends on, if any: one it names in a
/// line that says so, else the latest open earlier part of its series,
/// else the one open pull request that adds a file it names and `base`
/// lacks.
#[allow(clippy::too_many_arguments)]
pub async fn dependency(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    ticket_id: &str,
    title: &str,
    text: &str,
    base: &str,
    own_branch: &str,
) -> Option<Dependency> {
    let (prs, keys) = named(text, owner, repo, ticket_id);
    let paths = named_paths(text);
    if prs.is_empty() && keys.is_empty() && series(title).is_none() && paths.is_empty() {
        return None;
    }
    let open = github.list_open_pulls(owner, repo).await.ok()?;
    let open = open.as_array()?;
    if let Some(d) = pick(open, &prs, &keys, owner, repo, own_branch) {
        return Some(d);
    }
    if let Some(d) = pick_series(open, title, owner, repo, own_branch) {
        return Some(d);
    }
    let mut missing = Vec::new();
    for p in &paths {
        if !github.file_exists(owner, repo, p, base).await {
            missing.push(p.clone());
        }
    }
    if missing.is_empty() {
        return None;
    }
    let full = format!("{owner}/{repo}").to_ascii_lowercase();
    let mut changes = Vec::new();
    for p in open
        .iter()
        .filter(|p| {
            p["head"]["repo"]["full_name"]
                .as_str()
                .is_some_and(|f| f.eq_ignore_ascii_case(&full))
                && p["head"]["ref"].as_str().is_some_and(|b| b != own_branch)
        })
        .take(MAX_PRS)
    {
        let Some(n) = p["number"].as_u64() else {
            continue;
        };
        if let Ok(files) = github.list_pull_files(owner, repo, n).await {
            changes.push((p.clone(), files.into_iter().map(|(f, _)| f).collect()));
        }
    }
    by_files(&missing, &changes)
}

/// Why `restore_file` refuses a source: only the pull request's own base
/// branch or a commit (a hex SHA) may be restored from. Restoring a file
/// from another branch copies that branch's work under new commits, which
/// is how a run once re-created another open pull request's commits; a
/// ticket that needs another branch's work is stacked on it when the run
/// starts ([`for_ticket`]).
pub fn restore_refusal(from_ref: &str, base: &str) -> Option<String> {
    let r = from_ref.trim();
    let is_sha = (7..=40).contains(&r.len()) && r.chars().all(|c| c.is_ascii_hexdigit());
    if r == base || is_sha {
        return None;
    }
    Some(format!(
        "Not restored: `{r}` isn't this pull request's base (`{base}`) or a commit. Copying files \
         from another branch re-creates its work under new commits. Leave from_ref out to restore \
         from the base; if this ticket needs another open pull request's changes, say so in the \
         pull request (it should be stacked on that pull request's branch) instead of copying them."
    ))
}

/// Why a run refuses to open its pull request: its branch copied another
/// open pull request's commits and couldn't be stacked on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused(pub String);

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stopped before opening a pull request: {}", self.0)
    }
}

impl std::error::Error for Refused {}

/// At the start of a run: the open pull request the ticket depends on
/// (never one of this ticket's own earlier attempts) becomes the base its
/// branch starts from and its pull request targets.
pub async fn for_ticket(
    github: &GitHubClient,
    msg: &mut crate::models::TicketMessage,
    own_attempts: &[String],
) -> Option<Dependency> {
    let text = format!("{}\n{}", msg.title, msg.body);
    let d = dependency(
        github,
        &msg.repo_owner,
        &msg.repo_name,
        &msg.ticket_id,
        &msg.title,
        &text,
        &msg.base_branch,
        "",
    )
    .await
    .filter(|d| !own_attempts.contains(&d.branch))?;
    msg.base_branch = d.branch.clone();
    Some(d)
}

/// Whether a step of a run looks for the base an earlier step stacked it
/// on: only a later step of the same run whose message still names a base
/// other than the repository's default.
pub fn keeps_base(same_run: bool, incoming: &str, default: &str) -> bool {
    same_run && super::plan_coverage::stacked_base(incoming, default)
}

/// A later step of the same run keeps the base an earlier step stacked it
/// on, while that base is still an open pull request's branch in this
/// repository. Every step used to start again from the default branch: the
/// branch, already built on the other pull request, then read as copying
/// its commits, was rebuilt and force-pushed in every step, and the plan
/// check measured against a base that flipped between steps.
pub async fn kept(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base: &str,
) -> Option<Dependency> {
    let open = github.list_open_pulls(owner, repo).await.ok()?;
    let full = format!("{owner}/{repo}").to_ascii_lowercase();
    let p = open.as_array()?.iter().find(|p| {
        p["head"]["ref"].as_str() == Some(base)
            && p["head"]["repo"]["full_name"]
                .as_str()
                .is_some_and(|f| f.eq_ignore_ascii_case(&full))
    })?;
    Some(Dependency {
        number: p["number"].as_u64().unwrap_or(0),
        branch: base.to_string(),
        title: p["title"].as_str().unwrap_or("").to_string(),
        why: "An earlier step of this run stacked it on this pull request's branch.".to_string(),
        ..Default::default()
    })
}

/// Right after the run's branch is ready, before any implementation: a
/// branch kept from an earlier attempt (or retried) may carry copies of
/// another open pull request's commits (that pull request's branch was
/// rebased since). Its own change is rebuilt on that branch file by file
/// now, so the run builds on it; when that can't be done the pull request
/// step tries again.
pub async fn at_start(
    state: &crate::WorkerState,
    github: &GitHubClient,
    msg: &mut crate::models::TicketMessage,
    branch: &str,
    provider: &crate::agent::provider::ModelProvider,
    usage: &mut crate::models::TokenUsage,
    stacked: bool,
) -> Option<Dependency> {
    let (owner, repo) = (msg.repo_owner.clone(), msg.repo_name.clone());
    // A branch an earlier step already stacked on the pull request's branch,
    // which holds that branch's head as it is now, has nothing to rebuild:
    // a rebuild would make the same tree again under new commits, force
    // push it, and make every later check measure a branch that only moved.
    if stacked && already_on(github, &owner, &repo, &msg.base_branch, branch).await {
        return None;
    }
    let c = copied(github, &owner, &repo, &msg.base_branch, branch).await?;
    tracing::info!(
        branch,
        dep = c.dep.number,
        copies = ?c.commits,
        "Copies of an open pull request's commits found on the branch"
    );
    let mut merger = restack::ModelMerger {
        state,
        provider,
        usage,
        theirs: format!("pull request #{}'s branch", c.dep.number),
    };
    match rebuild_on(
        github,
        &owner,
        &repo,
        &msg.base_branch,
        branch,
        &c,
        &mut merger,
    )
    .await
    {
        Ok(r) => {
            msg.base_branch = c.dep.branch.clone();
            Some(Dependency {
                why: rebuilt_why(&c, &r),
                unresolved: r.unresolved,
                ..c.dep
            })
        }
        Err(h) => {
            tracing::info!(
                branch,
                dep = c.dep.number,
                why = %h.say(&format!("{owner}/{repo}")),
                "Copies of an open pull request's commits stay until the pull request step"
            );
            None
        }
    }
}

/// Whether `branch` already holds `base`'s head (it is not behind it).
async fn already_on(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base: &str,
    branch: &str,
) -> bool {
    match github.get_diff(owner, repo, base, branch).await {
        Ok(c) => holds_head(&c),
        Err(_) => false,
    }
}

/// A compare of `<base>...<branch>` says the branch holds the base's head.
pub fn holds_head(compare: &serde_json::Value) -> bool {
    compare["behind_by"].as_u64() == Some(0)
}

/// Right before the pull request opens: a branch that copied another open
/// pull request's commits gets its own change rebuilt on that pull
/// request's branch, file by file, as one commit (`restack`): no commit is
/// replayed and nothing has to be merged first, so neither the number of
/// commits nor a merge can stop it. Files both changed are merged by the
/// model; one it can't merge keeps the other pull request's version plus a
/// labelled follow-up commit with this change's, and the pull request
/// opens as a draft naming it. When the rebuild fails anyway (GitHub kept
/// failing), the pull request still opens against the original base, as a
/// draft that says so. Only a missing permission stops the run. Returns the
/// dependency the pull request states.
pub async fn before_pr(
    state: &crate::WorkerState,
    github: &GitHubClient,
    msg: &mut crate::models::TicketMessage,
    branch: &str,
    from_ticket: Option<Dependency>,
    provider: &crate::agent::provider::ModelProvider,
    usage: &mut crate::models::TokenUsage,
) -> Result<Option<Dependency>, Refused> {
    let (owner, repo) = (msg.repo_owner.clone(), msg.repo_name.clone());
    let Some(c) = copied(github, &owner, &repo, &msg.base_branch, branch).await else {
        return Ok(from_ticket.filter(|d| d.branch == msg.base_branch));
    };
    let mut merger = restack::ModelMerger {
        state,
        provider,
        usage,
        theirs: format!("pull request #{}'s branch", c.dep.number),
    };
    match rebuild_on(
        github,
        &owner,
        &repo,
        &msg.base_branch,
        branch,
        &c,
        &mut merger,
    )
    .await
    {
        Ok(r) => {
            msg.base_branch = c.dep.branch.clone();
            Ok(Some(Dependency {
                why: rebuilt_why(&c, &r),
                unresolved: r.unresolved,
                ..c.dep
            }))
        }
        Err(h @ restack::Halt::Permission { .. }) => Err(Refused(format!(
            "{} The work is kept on branch `{branch}`.",
            h.say(&format!("{owner}/{repo}"))
        ))),
        Err(restack::Halt::Failed(why)) => {
            tracing::warn!(
                branch,
                dep = c.dep.number,
                why,
                "Couldn't rebuild the change on the open pull request's branch; opening it as a draft"
            );
            Ok(Some(Dependency {
                why: not_rebuilt_why(&c, &why),
                not_stacked: true,
                ..c.dep
            }))
        }
    }
}

/// The branch's own change rebuilt on the dependency's head ([`restack`]).
async fn rebuild_on<M: restack::Merger>(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base: &str,
    branch: &str,
    c: &Copy,
    merger: &mut M,
) -> Result<restack::Rebuilt, restack::Halt> {
    let gh = restack::Gh {
        github,
        owner,
        repo,
        known: &c.files,
    };
    let message = flat_message(c);
    restack::rebuild(
        &gh,
        merger,
        &restack::Job {
            branch,
            onto: &c.dep.branch,
            own: restack::Own::NotCopies {
                base,
                copies: &c.commits,
            },
            message: &message,
            onto_pr: c.dep.number,
        },
    )
    .await
}

/// What happened, for the stack section: the copies dropped, the change
/// rebuilt on the dependency, the files merged.
pub fn rebuilt_why(c: &Copy, r: &restack::Rebuilt) -> String {
    let n = c.dep.number;
    let mut out = format!(
        "The branch had re-created {} of #{n}'s commits under other hashes; its own change \
         ({} commit(s), {} file(s)) was rebuilt as one commit on `{}`, so those commits come from \
         #{n} itself.",
        c.commits.len(),
        r.own_commits,
        r.paths,
        c.dep.branch,
    );
    if !r.merged.is_empty() {
        let list: Vec<String> = r.merged.iter().map(|p| format!("`{p}`")).collect();
        out.push_str(&format!(
            " #{n} had also changed {}; the two versions were merged, keeping both.",
            list.join(", ")
        ));
    }
    out
}

/// Why the pull request isn't stacked, for its section: the copies, and
/// why rebuilding failed.
pub fn not_rebuilt_why(c: &Copy, why: &str) -> String {
    let short: Vec<&str> = c.commits.iter().map(|s| &s[..7.min(s.len())]).collect();
    format!(
        "The branch repeats {} of #{}'s commits ({}); rebuilding its own change on `{}` didn't \
         work this time ({why}).",
        c.commits.len(),
        c.dep.number,
        short.join(", "),
        c.dep.branch,
    )
}

/// The messages of `listed`'s commits (a compare's, oldest first) that
/// aren't in `copies` and aren't merge commits.
pub fn own_messages(listed: &[Value], copies: &[String]) -> Vec<String> {
    listed
        .iter()
        .filter(|c| c["parents"].as_array().is_none_or(|p| p.len() <= 1))
        .filter(|c| {
            c["sha"]
                .as_str()
                .is_some_and(|s| !copies.iter().any(|x| x == s))
        })
        .filter_map(|c| c.pointer("/commit/message").and_then(|m| m.as_str()))
        .map(|m| m.trim().to_string())
        .filter(|m| !m.is_empty())
        .collect()
}

/// The message of the one commit the run's change becomes on top of the
/// dependency's branch: its first own commit's (already in the team's
/// convention), then every own commit's first line.
pub fn flat_message(c: &Copy) -> String {
    let head = c
        .own
        .first()
        .and_then(|m| m.lines().next())
        .unwrap_or("Rebuild this change")
        .to_string();
    let mut out = format!(
        "{head}\n\nRebuilt on #{} (`{}`) file by file, without the {} commit(s) copied from it.",
        c.dep.number,
        c.dep.branch,
        c.commits.len()
    );
    if c.own.len() > 1 {
        out.push_str("\n\nThis change's commits:\n");
        for m in &c.own {
            out.push_str(&format!("- {}\n", m.lines().next().unwrap_or("")));
        }
    }
    out.trim_end().to_string()
}

// ── Patch ids ───────────────────────────────────────────────────────────────

/// A commit's patch id: the same for the same change whatever its hash,
/// parent, message, line numbers or whitespace (like `git patch-id`).
pub fn patch_id(files: &[(String, String)]) -> Option<u64> {
    let mut by_path: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for (path, patch) in files {
        let lines: Vec<String> = patch
            .lines()
            .filter(|l| {
                (l.starts_with('+') || l.starts_with('-'))
                    && !l.starts_with("+++")
                    && !l.starts_with("---")
            })
            .map(|l| {
                let (sign, rest) = l.split_at(1);
                format!(
                    "{sign}{}",
                    rest.chars()
                        .filter(|c| !c.is_whitespace())
                        .collect::<String>()
                )
            })
            .collect();
        by_path.entry(path.as_str()).or_default().extend(lines);
    }
    if by_path.values().all(|l| l.is_empty()) {
        return None;
    }
    let mut h = std::collections::hash_map::DefaultHasher::new();
    by_path.hash(&mut h);
    Some(h.finish())
}

/// Another open pull request's commits the branch re-created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Copy {
    pub dep: Dependency,
    /// The branch's commits whose change is one of that pull request's.
    pub commits: Vec<String>,
    /// The messages of the branch's other commits (merge commits aside):
    /// its own work.
    pub own: Vec<String>,
    /// The files of the branch's commits, as read for their patch ids.
    pub files: BTreeMap<String, Vec<CommitFile>>,
}

/// The branch's commits that match another pull request's, by patch id:
/// `ours` and `theirs` are (sha, patch id).
pub fn matches(ours: &[(String, u64)], theirs: &[(String, u64)]) -> Vec<String> {
    let ids: BTreeSet<u64> = theirs.iter().map(|(_, i)| *i).collect();
    ours.iter()
        .filter(|(_, i)| ids.contains(i))
        .map(|(s, _)| s.clone())
        .collect()
}

const MAX_PRS: usize = 40;

/// Each commit's patch id, and its files as read.
async fn ids(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    shas: &[String],
) -> (Vec<(String, u64)>, BTreeMap<String, Vec<CommitFile>>) {
    let mut out = Vec::new();
    let mut read = BTreeMap::new();
    for sha in shas {
        if let Ok(files) = github.commit_changes(owner, repo, sha).await {
            let patches: Vec<(String, String)> = files
                .iter()
                .map(|f| (f.path.clone(), f.patch.clone()))
                .collect();
            if let Some(id) = patch_id(&patches) {
                out.push((sha.clone(), id));
            }
            read.insert(sha.clone(), files);
        }
    }
    (out, read)
}

/// The open pull request whose commits the branch re-created the most of,
/// when any (only pull requests that change one of the branch's files are
/// read).
pub async fn copied(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base: &str,
    branch: &str,
) -> Option<Copy> {
    // Every commit, however many: a copy past a cut-off would read as the
    // branch's own work.
    let listed = github
        .compare_all_commits(owner, repo, base, branch)
        .await
        .ok()?;
    let ours_shas: Vec<String> = listed
        .iter()
        .filter_map(|c| c["sha"].as_str().map(String::from))
        .collect();
    if ours_shas.is_empty() {
        return None;
    }
    let (ours, files) = ids(github, owner, repo, &ours_shas).await;
    let our_files: BTreeSet<String> = github
        .compare_changed_files(owner, repo, base, branch)
        .await
        .ok()?
        .into_iter()
        .collect();
    let open = github.list_open_pulls(owner, repo).await.ok()?;
    let full = format!("{owner}/{repo}").to_ascii_lowercase();
    let mut best: Option<Copy> = None;
    for p in open.as_array()?.iter().take(MAX_PRS) {
        let head = p["head"]["ref"].as_str().unwrap_or("");
        let same_repo = p["head"]["repo"]["full_name"]
            .as_str()
            .is_some_and(|f| f.eq_ignore_ascii_case(&full));
        if head == branch || !same_repo {
            continue;
        }
        let n = p["number"].as_u64().unwrap_or(0);
        let touches = github
            .list_pull_files(owner, repo, n)
            .await
            .map(|f| f.iter().any(|(path, _)| our_files.contains(path)))
            .unwrap_or(false);
        if !touches {
            continue;
        }
        let their_shas: Vec<String> = github
            .list_pr_commits(owner, repo, n)
            .await
            .unwrap_or_default()
            .iter()
            .filter_map(|c| c["sha"].as_str().map(String::from))
            .collect();
        let (theirs, _) = ids(github, owner, repo, &their_shas).await;
        let m = matches(&ours, &theirs);
        if !m.is_empty() && best.as_ref().is_none_or(|b| m.len() > b.commits.len()) {
            best = Some(Copy {
                dep: Dependency {
                    number: n,
                    branch: head.to_string(),
                    title: p["title"].as_str().unwrap_or("").to_string(),
                    ..Default::default()
                },
                own: own_messages(&listed, &m),
                commits: m,
                files: BTreeMap::new(),
            });
        }
    }
    best.map(|c| Copy { files, ..c })
}

/// The body with the stack section replaced (or added after the first
/// paragraph's line, the ticket line).
pub fn put_section(body: &str, section: &str) -> String {
    if let (Some(a), Some(b)) = (body.find(SECTION_START), body.find(SECTION_END)) {
        if b > a {
            return format!("{}{section}{}", &body[..a], &body[b + SECTION_END.len()..]);
        }
    }
    format!("{}\n\n{section}", body.trim_end())
}

/// States the dependency in the open pull request.
pub async fn note_pr(github: &GitHubClient, owner: &str, repo: &str, pr: u64, dep: &Dependency) {
    let Ok(p) = github.get_pull_request(owner, repo, pr).await else {
        return;
    };
    let body = p["body"].as_str().unwrap_or("");
    let updated = put_section(body, &dep.section());
    if updated != body {
        if let Err(e) = crate::passes::description::save(github, owner, repo, pr, &updated).await {
            tracing::warn!(pr, error = %e, "Could not state the stacked pull request's dependency");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ticket_names_what_it_depends_on() {
        let text = "Replace the two flags with one field.\n\
                    Depends on https://github.com/acme/web/pull/41 (the schema work).\n\
                    Similar to #7.\n\
                    Blocked by ABC-12.";
        let (prs, keys) = named(text, "acme", "web", "ABC-99");
        assert_eq!(prs, vec![41]);
        assert_eq!(keys, vec!["ABC-12".to_string()]);
        // A mention without a dependency word is not a dependency.
        assert!(!prs.contains(&7));
        // The ticket's own key never is.
        assert!(named("builds on ABC-99", "acme", "web", "ABC-99")
            .1
            .is_empty());
        assert_eq!(named("This needs PR #5 first", "a", "b", "").0, vec![5]);
        assert!(named("Dependency injection #3", "a", "b", "").0.is_empty());
    }

    fn pr(n: u64, branch: &str, title: &str, repo: &str) -> Value {
        serde_json::json!({
            "number": n,
            "title": title,
            "head": {"ref": branch, "repo": {"full_name": repo}},
        })
    }

    #[test]
    fn the_dependency_is_an_open_pr_of_this_repo() {
        let open = vec![
            pr(41, "fork-branch", "Schema", "someone/web"),
            pr(42, "abc-12-schema", "ABC-12 schema work", "acme/web"),
            pr(43, "coderhelm/abc-99", "This ticket", "acme/web"),
        ];
        // #41 is a fork's branch: not a base.
        assert!(pick(&open, &[41], &[], "acme", "web", "coderhelm/abc-99").is_none());
        let d = pick(
            &open,
            &[],
            &["ABC-12".into()],
            "acme",
            "web",
            "coderhelm/abc-99",
        )
        .unwrap();
        assert_eq!((d.number, d.branch.as_str()), (42, "abc-12-schema"));
        let s = d.section();
        assert!(s.contains("Merge order: #42 first, then this one."));
        // The run's own branch is never its own dependency.
        assert!(pick(&open, &[43], &[], "acme", "web", "coderhelm/abc-99").is_none());
    }

    #[test]
    fn a_series_title_gives_its_name_part_and_total() {
        let nm = |t: &str| series(t).map(|s| (s.name, s.part, s.total));
        assert_eq!(
            nm("[Titles 2/4] Sections are too big"),
            Some((Some("titles".into()), 2, 4))
        );
        assert_eq!(
            nm("ABC-12: [Nav polish 3 / 5] Menu"),
            Some((Some("nav polish".into()), 3, 5))
        );
        assert_eq!(
            nm("(Titles 2/4) Sections"),
            Some((Some("titles".into()), 2, 4))
        );
        assert_eq!(nm("(2/4) Section titles can be bigger"), Some((None, 2, 4)));
        assert_eq!(nm("[2/4] Section titles"), Some((None, 2, 4)));
        assert_eq!(
            nm("Titles (2/4): Sections"),
            Some((Some("titles".into()), 2, 4))
        );
        assert_eq!(
            nm("ABC-7: Titles [3/4] - Body"),
            Some((Some("titles".into()), 3, 4))
        );
        assert_eq!(nm("Section titles (2/4)"), Some((None, 2, 4)));
        assert_eq!(nm("Part 2 of 4: Section titles"), Some((None, 2, 4)));
        assert_eq!(
            nm("Titles, part 2 of 4: Sections"),
            Some((Some("titles".into()), 2, 4))
        );
        assert_eq!(nm("[Titles 5/4] too far"), None);
        assert_eq!(nm("[Titles 0/4] zero"), None);
        assert_eq!(nm("Ratio 1/1 (1/1)"), None);
        assert_eq!(nm("No series here"), None);
        assert_eq!(nm("Support 24/7 chat"), None);
    }

    #[test]
    fn other_title_shapes_stack_on_the_same_series() {
        let open = vec![
            pr(
                20,
                "coderhelm/abc-1",
                "[Titles 1/4] Add a shared Heading component",
                "acme/web",
            ),
            pr(21, "other", "(1/4) Button colors use tokens", "acme/web"),
            pr(
                22,
                "coderhelm/abc-9",
                "Part 1 of 3: Section titles",
                "acme/web",
            ),
        ];
        // A nameless part matches the named series whose name is in its title.
        let d = pick_series(
            &open,
            "(2/4) Section titles can be bigger",
            "acme",
            "web",
            "x",
        )
        .unwrap();
        assert_eq!(d.number, 20);
        // Two nameless parts match on a shared word and the same total.
        let d = pick_series(&open, "(2/4) Button borders use tokens", "acme", "web", "x").unwrap();
        assert_eq!(d.number, 21);
        let d = pick_series(
            &open,
            "Part 2 of 3: Section titles on venues",
            "acme",
            "web",
            "x",
        )
        .unwrap();
        assert_eq!(d.number, 22);
        // A label before the marker names the series.
        let d = pick_series(&open, "Titles (2/4): Sections", "acme", "web", "x").unwrap();
        assert_eq!(d.number, 20);
        // Nothing in common: no stacking.
        assert!(pick_series(
            &open,
            "(2/4) Footer links open in a new tab",
            "acme",
            "web",
            "x"
        )
        .is_none());
        // Two open pull requests for the same earlier part: no guess.
        let tie = vec![
            pr(30, "a", "(1/2) Menu spacing", "acme/web"),
            pr(31, "b", "(1/2) Menu colors", "acme/web"),
        ];
        assert!(pick_series(&tie, "(2/2) Menu fonts", "acme", "web", "x").is_none());
    }

    #[test]
    fn a_later_part_stacks_on_the_latest_open_earlier_part() {
        let open = vec![
            pr(
                10,
                "coderhelm/abc-1",
                "[Titles 1/4] Shared heading",
                "acme/web",
            ),
            pr(11, "coderhelm/abc-3", "[Titles 3/4] Body text", "acme/web"),
            pr(12, "other", "[Buttons 1/4] Not this series", "acme/web"),
            pr(
                13,
                "coderhelm/abc-0",
                "[Titles 1/3] Different total",
                "acme/web",
            ),
            pr(14, "fork", "[Titles 2/4] A fork", "someone/web"),
        ];
        let d = pick_series(
            &open,
            "[Titles 2/4] Sections",
            "acme",
            "web",
            "coderhelm/abc-2",
        )
        .unwrap();
        assert_eq!((d.number, d.branch.as_str()), (10, "coderhelm/abc-1"));
        assert!(d.why.contains("part 2 of 4"));
        // Part 4 builds on part 3, the latest earlier part still open.
        let d = pick_series(
            &open,
            "[Titles 4/4] Footer",
            "acme",
            "web",
            "coderhelm/abc-4",
        )
        .unwrap();
        assert_eq!(d.number, 11);
        // The first part, or a series whose earlier parts merged (no open pull
        // request), starts from the default branch.
        assert!(pick_series(&open, "[Titles 1/4] Shared heading", "acme", "web", "x").is_none());
        let merged: Vec<Value> = open.iter().filter(|p| p["number"] != 10).cloned().collect();
        assert!(pick_series(&merged, "[Titles 2/4] Sections", "acme", "web", "x").is_none());
        // Never the run's own branch.
        assert!(pick_series(&open, "[Titles 2/4] S", "acme", "web", "coderhelm/abc-1").is_none());
    }

    #[test]
    fn a_ticket_names_repository_paths() {
        let text = "Switch to the shared Heading (apps/web/src/components/ui/heading.tsx).\n\
                    Don't change `apps/web/src/components/ui/heading.tsx` or e2e/test-map.json.\n\
                    See https://example.com/docs/page.html and ../outside/x.ts and v1.2.";
        assert_eq!(
            named_paths(text),
            vec![
                "apps/web/src/components/ui/heading.tsx".to_string(),
                "e2e/test-map.json".to_string()
            ]
        );
    }

    #[test]
    fn a_named_file_only_an_open_pr_adds_makes_that_pr_the_base() {
        let missing = vec!["web/ui/heading.tsx".to_string()];
        let a = (
            pr(20, "coderhelm/abc-1", "Shared heading", "acme/web"),
            vec![
                "web/ui/heading.tsx".to_string(),
                "web/ui/heading.test.tsx".to_string(),
            ],
        );
        let b = (
            pr(21, "unrelated", "Other work", "acme/web"),
            vec!["web/x.ts".to_string()],
        );
        let d = by_files(&missing, &[a.clone(), b.clone()]).unwrap();
        assert_eq!(d.number, 20);
        assert!(d.why.contains("`web/ui/heading.tsx`"));
        // Two pull requests adding the same file: no guess.
        let c = (
            pr(22, "another", "Also a heading", "acme/web"),
            vec!["web/ui/heading.tsx".to_string()],
        );
        assert!(by_files(&missing, &[a.clone(), c]).is_none());
        // Nothing missing on the base: no dependency.
        assert!(by_files(&[], &[a, b]).is_none());
    }

    #[test]
    fn restore_file_only_restores_from_the_base_or_a_commit() {
        assert!(restore_refusal("main", "main").is_none());
        assert!(restore_refusal("coderhelm/abc-1", "coderhelm/abc-1").is_none());
        assert!(restore_refusal("4d17cbb", "main").is_none());
        assert!(restore_refusal("86f5835aa1b2c3d4e5f60718293a4b5c6d7e8f90", "main").is_none());
        let no = restore_refusal("coderhelm/abc-1", "main").unwrap();
        assert!(no.contains("isn't this pull request's base"));
        assert!(restore_refusal("develop", "main").is_some());
    }

    #[test]
    fn a_correctly_stacked_branch_copies_nothing() {
        // Stacked on the dependency, the branch's commits (compared against
        // its base, the dependency's branch) are only its own changes.
        let theirs = vec![("d1".to_string(), 1u64), ("d2".to_string(), 2u64)];
        let ours = vec![("o1".to_string(), 3u64), ("o2".to_string(), 4u64)];
        assert!(matches(&ours, &theirs).is_empty());
        // Copied commits (same change, new hash) are what the guard catches.
        let copied = vec![("c1".to_string(), 1u64), ("o1".to_string(), 3u64)];
        assert_eq!(matches(&copied, &theirs), vec!["c1".to_string()]);
    }

    #[test]
    fn patch_ids_match_the_same_change_under_a_new_hash() {
        let a = vec![(
            "web/a.ts".to_string(),
            "@@ -1,3 +1,3 @@\n context\n-const x = 1\n+const x = 2\n".to_string(),
        )];
        let b = vec![(
            "web/a.ts".to_string(),
            "@@ -10,3 +12,3 @@\n other context\n-const x = 1\n+const  x = 2\n".to_string(),
        )];
        let c = vec![(
            "web/a.ts".to_string(),
            "@@ -1,3 +1,3 @@\n-const x = 1\n+const x = 3\n".to_string(),
        )];
        assert_eq!(patch_id(&a), patch_id(&b));
        assert_ne!(patch_id(&a), patch_id(&c));
        assert_eq!(patch_id(&[("x".into(), String::new())]), None);
        let ours = vec![
            ("s1".to_string(), patch_id(&a).unwrap()),
            ("s2".to_string(), patch_id(&c).unwrap()),
        ];
        let theirs = vec![("t1".to_string(), patch_id(&b).unwrap())];
        assert_eq!(matches(&ours, &theirs), vec!["s1".to_string()]);
    }

    fn listed(sha: &str, parents: &[&str], message: &str) -> Value {
        serde_json::json!({
            "sha": sha,
            "parents": parents.iter().map(|p| serde_json::json!({"sha": p})).collect::<Vec<_>>(),
            "commit": {"message": message},
        })
    }

    fn copy_of_42() -> Copy {
        // acme/web: the run's branch still holds the old hashes of #42's
        // commits after #42's branch was rebased, then its own two commits.
        let listed = vec![
            listed("c1aaaaaaa", &["b0"], "ABC-12: add the schema"),
            listed("c2bbbbbbb", &["c1aaaaaaa"], "ABC-12: migrate rows"),
            listed("m1", &["c2bbbbbbb", "d9"], "Merge abc-12-schema"),
            listed(
                "o1",
                &["m1"],
                "ABC-99: one field for the two flags\n\nBody.",
            ),
            listed("o2", &["o1"], "ABC-99: tests"),
        ];
        let commits = vec!["c1aaaaaaa".to_string(), "c2bbbbbbb".to_string()];
        Copy {
            dep: Dependency {
                number: 42,
                branch: "abc-12-schema".into(),
                title: "ABC-12 schema work".into(),
                ..Default::default()
            },
            own: own_messages(&listed, &commits),
            commits,
            files: BTreeMap::new(),
        }
    }

    #[test]
    fn the_branch_s_own_work_is_what_isn_t_copied_or_merged() {
        let c = copy_of_42();
        assert_eq!(
            c.own,
            vec![
                "ABC-99: one field for the two flags\n\nBody.".to_string(),
                "ABC-99: tests".to_string()
            ]
        );
        // Rebuilt as one commit: the first own subject, then each one's.
        let m = flat_message(&c);
        assert!(m.starts_with("ABC-99: one field for the two flags\n\n"));
        assert!(
            m.contains("Rebuilt on #42 (`abc-12-schema`) file by file, without the 2 commit(s)")
        );
        assert!(m.contains("- ABC-99: tests"));
        assert!(!m.contains("Body."));
        // One own commit: no list.
        let mut one = c.clone();
        one.own.truncate(1);
        assert!(!flat_message(&one).contains("This change's commits"));
    }

    #[test]
    fn a_rebuilt_branch_says_what_happened_and_stacks() {
        let c = copy_of_42();
        let r = restack::Rebuilt {
            own_commits: 140,
            paths: 9,
            merged: vec!["web/rows.ts".into()],
            ..Default::default()
        };
        let why = rebuilt_why(&c, &r);
        assert!(why.contains("re-created 2 of #42's commits"));
        assert!(
            why.contains("(140 commit(s), 9 file(s)) was rebuilt as one commit on `abc-12-schema`")
        );
        assert!(why.contains("#42 had also changed `web/rows.ts`; the two versions were merged"));
        let d = Dependency { why, ..c.dep };
        let s = d.section();
        assert!(s.contains("stacked on #42 (`abc-12-schema`"));
        assert!(!s.contains("Needs a look"));
        // Nothing to look at: it may leave draft.
        assert_eq!(hold(&put_section("Body", &s)), None);
    }

    #[test]
    fn files_that_couldn_t_be_merged_keep_the_pull_request_a_draft() {
        let c = copy_of_42();
        let d = Dependency {
            why: "w".into(),
            unresolved: vec!["web/rows.ts".into()],
            ..c.dep.clone()
        };
        let s = d.section();
        assert!(s.contains("### Needs a look"));
        assert!(s.contains("- `web/rows.ts`"));
        assert!(s.contains("labelled \"Follow-up\""));
        let body = put_section("## Summary\n\nOne field.", &s);
        assert!(hold(&body).is_some());
        // A branch that couldn't be rebuilt opens anyway, as a draft that
        // says why, against the original base.
        let d = Dependency {
            why: not_rebuilt_why(&c, "reading the branch's head kept failing on GitHub (502)"),
            not_stacked: true,
            ..c.dep
        };
        let s = d.section();
        assert!(s.starts_with(&format!("{SECTION_START}\n## Repeats #42")));
        assert!(s.contains("repeats 2 of #42's commits (c1aaaaa, c2bbbbb)"));
        assert!(s.contains("running the ticket again does that"));
        assert!(hold(&put_section("Body", &s)).is_some());
        // A hold marker outside the section is no hold.
        assert_eq!(hold("Body <!-- coderhelm:stack-hold -->"), None);
    }

    #[test]
    fn stopping_names_the_missing_permission_and_where_the_work_is() {
        let h = restack::Halt::Permission {
            permission: "Contents: read and write".into(),
            doing: "moving the branch".into(),
        };
        let r = Refused(format!(
            "{} The work is kept on branch `coderhelm/feature-b`.",
            h.say("acme/web")
        ))
        .to_string();
        assert!(r.starts_with(
            "Stopped before opening a pull request: CoderHelm's GitHub App needs the \"Contents: \
             read and write\" permission on acme/web for moving the branch."
        ));
        assert!(r.ends_with("The work is kept on branch `coderhelm/feature-b`."));
    }

    #[test]
    fn the_section_is_replaced_in_place() {
        let d = Dependency {
            number: 4,
            branch: "b".into(),
            title: "t".into(),
            why: "w".into(),
            ..Default::default()
        };
        let once = put_section("Body", &d.section());
        let twice = put_section(&once, &d.section());
        assert_eq!(once, twice);
    }
}
