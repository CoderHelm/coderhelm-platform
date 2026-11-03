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
//!   its own commits are replayed with the copies left out, or, when they
//!   don't replay cleanly, that branch is merged in (the model resolves
//!   conflicts) and the change becomes one commit on top of it. Copies
//!   usually come from the dependency's branch being rebased after the run
//!   started on it: its commits got new hashes and the run's branch still
//!   holds the old ones. A branch kept from an earlier attempt is checked
//!   before any implementation ([`at_start`]). The run stops only when
//!   neither way works, saying what the person can do.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};

use regex::Regex;
use serde_json::Value;

use crate::clients::github::{GitHubClient, RebaseResult};

/// The open pull request a branch builds on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Dependency {
    pub number: u64,
    pub branch: String,
    pub title: String,
    /// How it was found (the ticket's line, or the commits it copied).
    pub why: String,
}

impl Dependency {
    /// The pull request section: what it builds on and the merge order.
    pub fn section(&self) -> String {
        format!(
            "{SECTION_START}\n## Builds on #{n}\n\nThis pull request is stacked on #{n} (`{b}`, \
             \"{t}\"): its base is that branch, so its diff shows only this ticket's change. {why}\n\n\
             Merge order: #{n} first, then this one. When #{n} merges, GitHub moves this pull \
             request's base to #{n}'s base; CoderHelm rebases it if needed.\n{SECTION_END}",
            n = self.number,
            b = self.branch,
            t = self.title.replace('"', "'"),
            why = self.why.trim(),
        )
    }
}

pub const SECTION_START: &str = "<!-- coderhelm:stack -->";
pub const SECTION_END: &str = "<!-- /coderhelm:stack -->";

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

/// Right after the run's branch is ready, before any implementation: a
/// branch kept from an earlier attempt may carry copies of another open
/// pull request's commits (that pull request's branch was rebased since).
/// When the branch's own commits replay cleanly onto that pull request's
/// branch, the copies are dropped now and the run stacks on it; otherwise
/// the pull request step deals with them.
pub async fn at_start(
    github: &GitHubClient,
    msg: &mut crate::models::TicketMessage,
    branch: &str,
) -> Option<Dependency> {
    let c = copied(
        github,
        &msg.repo_owner,
        &msg.repo_name,
        &msg.base_branch,
        branch,
    )
    .await?;
    match drop_copies(github, &msg.repo_owner, &msg.repo_name, branch, &c).await {
        Ok(()) => {
            msg.base_branch = c.dep.branch.clone();
            Some(Dependency {
                why: dropped_why(&c),
                ..c.dep
            })
        }
        Err(why) => {
            tracing::info!(
                branch,
                dep = c.dep.number,
                why,
                "Copies of an open pull request's commits stay until the pull request step"
            );
            None
        }
    }
}

/// Right before the pull request opens: a branch that copied another open
/// pull request's commits is moved onto that pull request's branch without
/// the copies, and the pull request is stacked on it. First the branch's
/// own commits are replayed onto it (`git rebase --onto`, copies left
/// out); when they don't replay cleanly, that branch is merged into the
/// run's branch (conflicts are resolved by the model, the head is never
/// reset) and the merged code becomes one commit on top of it. Only when
/// both fail does the run stop, saying what the person can do. Returns the
/// dependency the pull request states (the ticket's, or the one found by
/// its commits).
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
    // Both ways build on the dependency's branch: the merge resolves
    // conflicts against it.
    let base = std::mem::replace(&mut msg.base_branch, c.dep.branch.clone());
    let msg_ref: &crate::models::TicketMessage = msg;
    let outcome = recover(drop_copies(github, &owner, &repo, branch, &c), || {
        tracing::info!(
            branch,
            dep = c.dep.number,
            "Own commits don't replay onto the open pull request's branch; merging it in"
        );
        merge_and_flatten(state, github, msg_ref, branch, &c, provider, usage)
    })
    .await;
    match outcome {
        Ok(how) => Ok(Some(Dependency {
            why: how.why(&c),
            ..c.dep
        })),
        Err((replayed, merged)) => {
            msg.base_branch = base;
            Err(Refused(refusal(&c, branch, &replayed, &merged)))
        }
    }
}

/// How a branch's copies of another pull request's commits were dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recovered {
    /// Its own commits replayed onto that pull request's branch.
    Replayed,
    /// That branch merged in, the change rebuilt as one commit on it.
    Rebuilt,
}

impl Recovered {
    fn why(self, c: &Copy) -> String {
        match self {
            Recovered::Replayed => dropped_why(c),
            Recovered::Rebuilt => format!(
                "The branch had re-created {} of its commits; #{n}'s branch was merged in and \
                 this change rebuilt as one commit on top of it, so those commits come from #{n} \
                 itself.",
                c.commits.len(),
                n = c.dep.number
            ),
        }
    }
}

/// The order of the recovery: replaying the branch's own commits first;
/// merging the dependency in (the model resolves conflicts) only when that
/// fails. Both reasons when neither works.
pub async fn recover<M, F>(
    replay: impl std::future::Future<Output = Result<(), String>>,
    merge: M,
) -> Result<Recovered, (String, String)>
where
    M: FnOnce() -> F,
    F: std::future::Future<Output = Result<(), String>>,
{
    let replayed = match replay.await {
        Ok(()) => return Ok(Recovered::Replayed),
        Err(why) => why,
    };
    match merge().await {
        Ok(()) => Ok(Recovered::Rebuilt),
        Err(merged) => Err((replayed, merged)),
    }
}

fn dropped_why(c: &Copy) -> String {
    format!(
        "The branch had re-created {} of its commits; they were dropped and its own commits \
         replayed onto `{}`, so those commits come from #{} itself.",
        c.commits.len(),
        c.dep.branch,
        c.dep.number
    )
}

/// The branch's own commits replayed onto the dependency's branch, the
/// copies left out; why not when that can't be done.
async fn drop_copies(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    branch: &str,
    c: &Copy,
) -> Result<(), String> {
    let lease = github
        .get_ref(owner, repo, branch)
        .await
        .map_err(|e| format!("its head couldn't be read ({e})"))?;
    match github
        .rebase_onto(owner, repo, branch, &c.dep.branch, &c.commits, &lease)
        .await
    {
        Ok(RebaseResult::Rebased(_)) | Ok(RebaseResult::UpToDate) => Ok(()),
        Ok(RebaseResult::NotClean(why)) => Err(why),
        Ok(RebaseResult::Moved) => Err("the branch moved while it was being rebased".to_string()),
        Err(e) => Err(format!("the rebase failed ({e})")),
    }
}

/// The dependency's branch (`msg.base_branch` by now) merged into the
/// run's branch, conflicts resolved by the model, then the merged code as
/// one commit on top of the dependency's branch.
async fn merge_and_flatten(
    state: &crate::WorkerState,
    github: &GitHubClient,
    msg: &crate::models::TicketMessage,
    branch: &str,
    c: &Copy,
    provider: &crate::agent::provider::ModelProvider,
    usage: &mut crate::models::TokenUsage,
) -> Result<(), String> {
    if let Err(e) = super::pr::resolve_conflicts(state, msg, github, branch, provider, usage).await
    {
        return Err(format!("merging it in failed ({e})"));
    }
    let lease = github
        .get_ref(&msg.repo_owner, &msg.repo_name, branch)
        .await
        .map_err(|e| format!("its head couldn't be read ({e})"))?;
    match github
        .flatten_onto(
            &msg.repo_owner,
            &msg.repo_name,
            branch,
            &c.dep.branch,
            &flat_message(c),
            &lease,
        )
        .await
    {
        Ok(RebaseResult::Rebased(_)) | Ok(RebaseResult::UpToDate) => Ok(()),
        Ok(RebaseResult::NotClean(why)) => Err(why),
        Ok(RebaseResult::Moved) => Err("the branch moved while it was being rebuilt".to_string()),
        Err(e) => Err(format!("rebuilding it failed ({e})")),
    }
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
        "{head}\n\nRebuilt on #{} (`{}`) without the {} commit(s) copied from it.",
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

/// Why the run stops, in plain words with what the person can do: both
/// ways of building on the dependency failed.
pub fn refusal(c: &Copy, branch: &str, replayed: &str, merged: &str) -> String {
    let short: Vec<&str> = c.commits.iter().map(|s| &s[..7.min(s.len())]).collect();
    format!(
        "the branch repeats {k} commit(s) of open pull request #{n} (`{b}`) under other hashes \
         ({shas}). Dropping them didn't work ({replayed}), and neither did merging #{n}'s branch \
         in and rebuilding this change on it ({merged}). The work is kept on branch `{branch}`. \
         To finish it: merge or close #{n} and run the ticket again, or move `{branch}` onto \
         `{b}` yourself (`git rebase --onto {b}`, keeping only this change's commits) and open \
         the pull request against `{b}`.",
        k = c.commits.len(),
        n = c.dep.number,
        b = c.dep.branch,
        shas = short.join(", "),
    )
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

const MAX_COMMITS: usize = 60;
const MAX_PRS: usize = 40;

async fn ids(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    shas: &[String],
) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    for sha in shas.iter().take(MAX_COMMITS) {
        if let Ok(files) = github.commit_patches(owner, repo, sha).await {
            if let Some(id) = patch_id(&files) {
                out.push((sha.clone(), id));
            }
        }
    }
    out
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
    let listed = github
        .compare_commits(owner, repo, base, branch, MAX_COMMITS)
        .await
        .ok()?;
    let ours_shas: Vec<String> = listed
        .iter()
        .filter_map(|c| c["sha"].as_str().map(String::from))
        .collect();
    if ours_shas.is_empty() {
        return None;
    }
    let ours = ids(github, owner, repo, &ours_shas).await;
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
        let theirs = ids(github, owner, repo, &their_shas).await;
        let m = matches(&ours, &theirs);
        if !m.is_empty() && best.as_ref().is_none_or(|b| m.len() > b.commits.len()) {
            best = Some(Copy {
                dep: Dependency {
                    number: n,
                    branch: head.to_string(),
                    title: p["title"].as_str().unwrap_or("").to_string(),
                    why: String::new(),
                },
                own: own_messages(&listed, &m),
                commits: m,
            });
        }
    }
    best
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
        if let Err(e) = github.update_pr_body(owner, repo, pr, &updated).await {
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
                why: String::new(),
            },
            own: own_messages(&listed, &commits),
            commits,
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
        assert!(m.contains("Rebuilt on #42 (`abc-12-schema`) without the 2 commit(s)"));
        assert!(m.contains("- ABC-99: tests"));
        assert!(!m.contains("Body."));
        // One own commit: no list.
        let mut one = c.clone();
        one.own.truncate(1);
        assert!(!flat_message(&one).contains("This change's commits"));
    }

    #[test]
    fn a_stacked_dependency_says_its_copies_were_dropped() {
        let c = copy_of_42();
        let why = dropped_why(&c);
        assert!(why.contains("re-created 2 of its commits"));
        assert!(why.contains("replayed onto `abc-12-schema`"));
        let d = Dependency { why, ..c.dep };
        assert!(d.section().contains("stacked on #42 (`abc-12-schema`"));
    }

    #[tokio::test]
    async fn copies_are_dropped_by_replaying_first_and_merging_only_on_a_conflict() {
        // The own commits replay onto the dependency: no merge, no model.
        let merged = std::cell::Cell::new(false);
        let how = recover(async { Ok(()) }, || async {
            merged.set(true);
            Ok(())
        })
        .await;
        assert_eq!(how, Ok(Recovered::Replayed));
        assert!(!merged.get());
        // A commit conflicts with the dependency's head: merged in instead.
        let how = recover(
            async { Err("commit o1 conflicts with the base".to_string()) },
            || async { Ok(()) },
        )
        .await;
        assert_eq!(how, Ok(Recovered::Rebuilt));
        let c = copy_of_42();
        assert!(Recovered::Rebuilt
            .why(&c)
            .contains("#42's branch was merged in"));
        // Neither works: both reasons, for the message.
        let how = recover(async { Err("a".to_string()) }, || async {
            Err("b".to_string())
        })
        .await;
        assert_eq!(how, Err(("a".to_string(), "b".to_string())));
    }

    #[test]
    fn stopping_says_what_the_person_can_do() {
        let c = copy_of_42();
        let r = Refused(refusal(
            &c,
            "coderhelm/abc-99",
            "commit o1 conflicts with the base",
            "`abc-12-schema` isn't merged into the branch yet",
        ))
        .to_string();
        assert!(r.starts_with("Stopped before opening a pull request: the branch repeats 2"));
        assert!(r.contains("(c1aaaaa, c2bbbbb)"));
        assert!(r.contains("commit o1 conflicts with the base"));
        assert!(r.contains("The work is kept on branch `coderhelm/abc-99`."));
        assert!(r.contains("merge or close #42 and run the ticket again"));
        assert!(r.contains("open the pull request against `abc-12-schema`"));
    }

    #[test]
    fn the_section_is_replaced_in_place() {
        let d = Dependency {
            number: 4,
            branch: "b".into(),
            title: "t".into(),
            why: "w".into(),
        };
        let once = put_section("Body", &d.section());
        let twice = put_section(&once, &d.section());
        assert_eq!(once, twice);
    }
}
