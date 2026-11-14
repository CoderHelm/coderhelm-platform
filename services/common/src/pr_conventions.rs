//! How a repository writes its pull request titles and commit subjects,
//! detected and never guessed.
//!
//! A dependency pull request was titled `fix(deps): ...` in a repository
//! whose merged pull requests all read `<TICKET-KEY>: <summary>`, and its
//! commits carried the prefix `KEY-1:`, which is no ticket.
//!
//! [`detect`] reads the convention from, in this order: the team's own
//! configuration, the repository's guide, its lint or check configuration
//! (commitlint, a pull request title check), then the recent merged pull
//! requests and base-branch commits (enough samples, a clear majority,
//! merges and bots left out). The evidence is kept with the answer.
//! [`subject`] writes a title or commit subject in that form. A key in it is
//! always a real one ([`Key`]): the run's own ticket, or one that exists in
//! the connected tracker. When the form needs a key and there is none, the
//! closest valid form is used and the pull request says so once; a key is
//! never invented. The same convention is applied to the pull request title
//! (which GitHub squashes a multi-commit pull request to) and to commits.

use std::sync::OnceLock;

use regex::Regex;

/// Fewer samples than this teach nothing.
pub const MIN_SAMPLES: usize = 8;
/// The share of samples a form needs to be the convention.
pub const MAJORITY_PERCENT: usize = 60;
/// The share of samples one ticket key position needs.
pub const KEY_PERCENT: usize = 40;
/// A title's length CoderHelm aims for (it becomes the squash commit's
/// subject), and the most it ever writes.
pub const TITLE_TARGET: usize = 72;
pub const TITLE_MAX: usize = 80;
/// How many recent merged pull requests or commits are read.
pub const SAMPLE_SIZE: usize = 30;

/// How a key is set off from the summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sep {
    /// `OPS-12: summary`
    Colon,
    /// `OPS-12 summary`
    Space,
    /// `[OPS-12] summary`
    Bracket,
    /// `summary (OPS-12)`
    Suffix,
}

/// The form of a title or commit subject.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Form {
    /// A tracker key, then the summary.
    TicketKey(Sep),
    /// `type(scope): summary` (Conventional Commits).
    Conventional,
    /// A plain sentence.
    #[default]
    Plain,
}

/// Where the convention was read from, strongest first.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum Source {
    TeamConfig,
    RepoGuide,
    LintConfig,
    MergedPullRequests,
    BaseCommits,
    /// Nothing said: CoderHelm's own default.
    #[default]
    Default,
}

/// A detected convention, with what it rests on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Convention {
    pub form: Form,
    pub source: Source,
    /// What was read, in words (`24 of the last 30 merged pull requests`).
    pub evidence: String,
    /// The Conventional Commits scopes people used, most used first.
    pub scopes: Vec<String>,
    /// Whether people start the summary in lower case (`None`: nothing
    /// read; the form's own habit then).
    pub lower: Option<bool>,
}

/// Which text is being written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Title,
    Commit,
}

/// One recent title or commit subject.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Sample {
    pub text: String,
    /// Written by a bot (a dependency bot, CoderHelm itself).
    pub bot: bool,
}

/// Everything a detection reads.
#[derive(Debug, Clone, Default)]
pub struct Inputs<'a> {
    /// The team set its own template for this kind.
    pub team_template: bool,
    /// The repository's guide files (AGENTS.md, CONTRIBUTING.md, ...).
    pub guide: &'a str,
    /// commitlint configuration: `(path, text)`.
    pub commitlint: Option<(&'a str, &'a str)>,
    /// Workflows that check pull request titles: `(path, text)`.
    pub title_checks: Vec<(&'a str, &'a str)>,
    /// Release tooling that reads commit subjects (`release-please-config.json`,
    /// a semantic-release configuration): its path.
    pub release_tooling: Option<&'a str>,
    /// Titles of recent merged pull requests, newest first.
    pub merged: &'a [Sample],
    /// Subjects of recent base-branch commits, newest first.
    pub commits: &'a [Sample],
}

fn key_lead() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"^(\[)?([A-Z][A-Z0-9]*-\d+)(\])?(:)?\s+\S").expect("valid"))
}

fn key_suffix() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"\S\s+\(([A-Z][A-Z0-9]*-\d+)\)$").expect("valid"))
}

fn conventional_lead() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| {
        Regex::new(
            r"^(feat|fix|chore|docs|refactor|test|tests|perf|build|ci|style|revert)(\(([^)]*)\))?!?: +\S",
        )
        .expect("valid")
    })
}

/// The form one subject is written in.
pub fn form_of(subject: &str) -> Form {
    let s = subject.trim();
    if let Some(c) = key_lead().captures(s) {
        return Form::TicketKey(if c.get(1).is_some() && c.get(3).is_some() {
            Sep::Bracket
        } else if c.get(4).is_some() {
            Sep::Colon
        } else {
            Sep::Space
        });
    }
    if conventional_lead().is_match(s) {
        return Form::Conventional;
    }
    if key_suffix().is_match(s) {
        return Form::TicketKey(Sep::Suffix);
    }
    Form::Plain
}

/// The summary a subject carries after its key or type.
fn summary_part(subject: &str) -> String {
    bare(subject).2
}

fn is_noise(s: &Sample) -> bool {
    let t = s.text.trim_start();
    s.bot
        || t.is_empty()
        || t.starts_with("Merge pull request")
        || t.starts_with("Merge branch")
        || t.starts_with("Merge remote-tracking")
        || t.starts_with("Revert \"")
}

/// The form most samples share, when enough people's samples say one:
/// `(form, how many agree, how many were read)`. Conventional Commits needs
/// a clear majority; a ticket key needs one position (`KEY-1: `, `[KEY-1] `,
/// `KEY-1 `, ` (KEY-1)`) in [`KEY_PERCENT`] of the samples.
fn majority(samples: &[Sample]) -> Option<(Form, usize, usize)> {
    let read: Vec<Form> = people(samples).map(|s| form_of(&s.text)).collect();
    if read.len() < MIN_SAMPLES {
        return None;
    }
    let count = |want: fn(&Form) -> bool| read.iter().filter(|f| want(f)).count();
    let conventional = count(|f| matches!(f, Form::Conventional));
    let plain = count(|f| matches!(f, Form::Plain));
    let share = |n: usize, percent: usize| n * 100 >= percent * read.len();
    let (sep, keyed) = [Sep::Colon, Sep::Space, Sep::Bracket, Sep::Suffix]
        .into_iter()
        .map(|s| (s, read.iter().filter(|f| **f == Form::TicketKey(s)).count()))
        .max_by_key(|(_, n)| *n)
        .unwrap_or((Sep::Colon, 0));
    if share(conventional, MAJORITY_PERCENT) {
        Some((Form::Conventional, conventional, read.len()))
    } else if keyed > 0 && share(keyed, KEY_PERCENT) {
        Some((Form::TicketKey(sep), keyed, read.len()))
    } else if share(plain, MAJORITY_PERCENT) {
        Some((Form::Plain, plain, read.len()))
    } else {
        None
    }
}

/// The samples people wrote, newest first, at most [`SAMPLE_SIZE`].
fn people(samples: &[Sample]) -> impl Iterator<Item = &Sample> {
    samples.iter().filter(|s| !is_noise(s)).take(SAMPLE_SIZE)
}

/// The scopes people's Conventional Commits subjects use, most used first.
fn scopes_of(samples: &[Sample]) -> Vec<String> {
    let mut counts: Vec<(String, usize)> = Vec::new();
    for s in people(samples) {
        if let Some(scope) = bare(&s.text).1 {
            match counts.iter_mut().find(|(x, _)| *x == scope) {
                Some(c) => c.1 += 1,
                None => counts.push((scope, 1)),
            }
        }
    }
    counts.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
    counts.into_iter().map(|(s, _)| s).collect()
}

/// Whether most of the samples in `form` start their summary in lower case.
fn lower_of(samples: &[Sample], form: Form) -> Option<bool> {
    let firsts: Vec<bool> = people(samples)
        .filter(|s| match (form_of(&s.text), form) {
            (Form::TicketKey(_), Form::TicketKey(_)) => true,
            (a, b) => a == b,
        })
        .filter_map(|s| summary_part(&s.text).chars().next())
        .filter(|c| c.is_alphabetic())
        .map(|c| c.is_lowercase())
        .collect();
    if firsts.is_empty() {
        return None;
    }
    Some(firsts.iter().filter(|l| **l).count() * 2 > firsts.len())
}

/// The form a guide states for `kind`, when it states one: a line about
/// commit messages or pull request titles that names Conventional Commits
/// or shows the form in backticks.
fn stated_in_guide(guide: &str, kind: Kind) -> Option<(Form, String)> {
    for line in guide.lines() {
        let l = line.to_lowercase();
        let about = match kind {
            Kind::Title => {
                l.contains("pr title") || l.contains("pull request title") || l.contains("titles")
            }
            Kind::Commit => l.contains("commit"),
        };
        if !about {
            continue;
        }
        if l.contains("conventional commit") {
            return Some((Form::Conventional, one_line(line)));
        }
        let mut ticks = line.split('`');
        ticks.next();
        while let Some(inside) = ticks.next() {
            // An example's key may be a sample: only its shape is read.
            let shaped = inside
                .replace("<KEY>", "ABC-123")
                .replace("<TICKET-KEY>", "ABC-123")
                .replace("<TICKET>", "ABC-123")
                .replace("{ticket}", "ABC-123")
                .replace("<type>", "fix")
                .replace("XXXX", "1234");
            match form_of(&shaped) {
                Form::Plain => {}
                f => return Some((f, one_line(line))),
            }
            ticks.next();
        }
    }
    None
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The form a lint or check configuration enforces.
fn enforced(i: &Inputs<'_>, kind: Kind) -> Option<(Form, String)> {
    if kind == Kind::Title {
        for (path, text) in &i.title_checks {
            let l = text.to_lowercase();
            if l.contains("action-semantic-pull-request") || l.contains("conventional") {
                return Some((
                    Form::Conventional,
                    format!("`{path}` checks pull request titles as Conventional Commits"),
                ));
            }
            if l.contains("jira") || text.contains("[A-Z]+-") {
                return Some((
                    Form::TicketKey(Sep::Colon),
                    format!("`{path}` checks pull request titles for a ticket key"),
                ));
            }
        }
    }
    if let Some(path) = i.release_tooling {
        return Some((
            Form::Conventional,
            format!("`{path}` reads Conventional Commits subjects for releases"),
        ));
    }
    let (path, text) = i.commitlint?;
    let l = text.to_lowercase();
    // commitlint lints commits; it speaks for titles only where they are
    // squashed into the commit, which the title checks above cover.
    if kind == Kind::Commit || i.title_checks.is_empty() {
        if text.contains("[A-Z]+-") || l.contains("issueprefixes") || l.contains("jira") {
            return Some((
                Form::TicketKey(Sep::Colon),
                format!("`{path}` requires a ticket key"),
            ));
        }
        if l.contains("config-conventional") || l.contains("type-enum") {
            return Some((
                Form::Conventional,
                format!("`{path}` enforces Conventional Commits"),
            ));
        }
    }
    None
}

/// The convention for `kind`, from the strongest source that says one,
/// with the scopes and the case people use.
pub fn detect(kind: Kind, i: &Inputs<'_>) -> Convention {
    let mut c = detect_form(kind, i);
    let samples: Vec<Sample> = match kind {
        Kind::Title => i.merged.iter().chain(i.commits).cloned().collect(),
        Kind::Commit => i.commits.iter().chain(i.merged).cloned().collect(),
    };
    c.scopes = scopes_of(&samples);
    c.lower = lower_of(&samples, c.form);
    c
}

fn detect_form(kind: Kind, i: &Inputs<'_>) -> Convention {
    if i.team_template {
        return Convention {
            form: Form::Plain,
            source: Source::TeamConfig,
            evidence: "the team's own template in Settings".to_string(),
            ..Default::default()
        };
    }
    if let Some((form, line)) = stated_in_guide(i.guide, kind) {
        return Convention {
            form,
            source: Source::RepoGuide,
            evidence: format!(
                "the repository's guide says: \"{}\"",
                crate::truncate_str(&line, 160)
            ),
            ..Default::default()
        };
    }
    if let Some((form, evidence)) = enforced(i, kind) {
        return Convention {
            form,
            source: Source::LintConfig,
            evidence,
            ..Default::default()
        };
    }
    let (first, second) = match kind {
        Kind::Title => (
            (i.merged, Source::MergedPullRequests, "merged pull requests"),
            (i.commits, Source::BaseCommits, "base-branch commits"),
        ),
        Kind::Commit => (
            (i.commits, Source::BaseCommits, "base-branch commits"),
            (i.merged, Source::MergedPullRequests, "merged pull requests"),
        ),
    };
    for (samples, source, what) in [first, second] {
        if let Some((form, agree, read)) = majority(samples) {
            return Convention {
                form,
                source,
                evidence: format!("{agree} of the last {read} {what} written by people"),
                ..Default::default()
            };
        }
    }
    Convention {
        form: Form::Plain,
        source: Source::Default,
        evidence: "no configuration, guide or clear majority in recent history".to_string(),
        ..Default::default()
    }
}

/// The scope people used that fits the changed paths: one that names a
/// directory or file the change touches. `None` when none fits.
pub fn scope_for(scopes: &[String], paths: &[String]) -> Option<String> {
    scopes
        .iter()
        .find(|scope| {
            let s = scope.to_lowercase();
            paths.iter().any(|p| {
                p.to_lowercase()
                    .split(['/', '.', '-', '_'])
                    .any(|seg| seg == s)
            })
        })
        .cloned()
}

/// The key a subject may carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Key {
    /// A real key: the run's own ticket, or one found (or made) in the
    /// connected tracker.
    Real(String),
    /// There is none, and why one could not be made.
    None(String),
}

/// Whether `candidate` may be written as a key: it is the run's own ticket,
/// or the tracker says it exists. A stand-in never is, whatever anyone says.
pub fn real_key(candidate: &str, run_ticket: Option<&str>, in_tracker: Option<bool>) -> bool {
    if !crate::git_conventions::is_ticket_key(candidate) {
        return false;
    }
    if run_ticket.is_some_and(|k| k.eq_ignore_ascii_case(candidate)) {
        return true;
    }
    in_tracker == Some(true) && !crate::git_conventions::is_stand_in_key(candidate)
}

/// A subject without the key or type it starts with: `(type, scope, rest)`.
pub fn bare(subject: &str) -> (Option<String>, Option<String>, String) {
    let mut s = subject.trim().to_string();
    let mut ty = None;
    let mut scope = None;
    for _ in 0..2 {
        if let Some(c) = key_lead().captures(&s) {
            let end = c.get(0).map(|m| m.end() - 1).unwrap_or(0);
            s = s[end..].trim_start().to_string();
        }
        if let Some(c) = conventional_lead().captures(&s) {
            ty = Some(c[1].to_string());
            scope = c
                .get(3)
                .map(|m| m.as_str().to_string())
                .filter(|x| !x.is_empty());
            let end = c.get(0).map(|m| m.end() - 1).unwrap_or(0);
            s = s[end..].trim_start().to_string();
        }
    }
    // A placeholder key at the start is never kept.
    let s = crate::git_conventions::normalize_subject(&s, None, false);
    (ty, scope, s)
}

fn upper_first(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

fn lower_first(s: &str) -> String {
    let second_upper = s.chars().nth(1).is_some_and(|c| c.is_uppercase());
    let mut c = s.chars();
    match c.next() {
        Some(f) if !second_upper => f.to_lowercase().collect::<String>() + c.as_str(),
        _ => s.to_string(),
    }
}

/// A subject written in the convention, and what to say once on the pull
/// request when the form could not be met in full.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Written {
    pub text: String,
    pub note: Option<String>,
}

/// `subject` (in whatever form it was first written) in the convention's
/// form. The key is `key` or nothing: with a form that needs a key and no
/// real one, the summary stands alone and the note says why.
pub fn subject(c: &Convention, key: &Key, subject: &str) -> Written {
    subject_for(c, key, subject, &[])
}

/// [`subject`] for a change to `paths`: a Conventional Commits subject with
/// no scope of its own takes the scope people used that fits them.
pub fn subject_for(c: &Convention, key: &Key, subject: &str, paths: &[String]) -> Written {
    let (ty, scope, rest) = bare(subject);
    let rest = tidy_summary(&dependency_summary(&rest).unwrap_or(rest));
    let case = |s: &str, lower_by_default: bool| {
        if c.lower.unwrap_or(lower_by_default) {
            lower_first(s)
        } else {
            upper_first(s)
        }
    };
    let text = match (c.form, key) {
        (Form::TicketKey(sep), Key::Real(k)) => {
            let rest = case(&rest, false);
            match sep {
                Sep::Colon => format!("{k}: {rest}"),
                Sep::Space => format!("{k} {rest}"),
                Sep::Bracket => format!("[{k}] {rest}"),
                Sep::Suffix => format!("{rest} ({k})"),
            }
        }
        (Form::Conventional, _) => {
            let scope = scope.or_else(|| scope_for(&c.scopes, paths));
            format!(
                "{}{}: {}",
                ty.as_deref().unwrap_or("chore"),
                scope.map(|s| format!("({s})")).unwrap_or_default(),
                case(&rest, true)
            )
        }
        (Form::TicketKey(_), Key::None(_)) | (Form::Plain, _) => case(&rest, false),
    };
    let note = match (c.form, key) {
        (Form::TicketKey(_), Key::None(why)) => Some(format!(
            "Titles and commits in this repository start with a ticket key ({}). This run has no ticket and CoderHelm did not make one ({}), so they carry none: add the key to the title before merging if one is required.",
            c.evidence,
            why.trim_end_matches('.')
        )),
        _ => None,
    };
    Written {
        text: fit_title(&text),
        note,
    }
}

/// Verbs a title may start with, in their imperative form.
const VERBS: &[&str] = &[
    "add",
    "allow",
    "apply",
    "avoid",
    "block",
    "bump",
    "cache",
    "change",
    "check",
    "clean",
    "close",
    "convert",
    "create",
    "debounce",
    "default",
    "delete",
    "deprecate",
    "disable",
    "document",
    "drop",
    "enable",
    "ensure",
    "expose",
    "extract",
    "fix",
    "guard",
    "handle",
    "hide",
    "implement",
    "improve",
    "introduce",
    "keep",
    "limit",
    "load",
    "log",
    "make",
    "mark",
    "merge",
    "migrate",
    "move",
    "open",
    "pin",
    "prevent",
    "read",
    "record",
    "reduce",
    "refactor",
    "remove",
    "rename",
    "render",
    "reorder",
    "replace",
    "restore",
    "retry",
    "return",
    "revert",
    "run",
    "scope",
    "send",
    "set",
    "show",
    "skip",
    "sort",
    "split",
    "start",
    "stop",
    "store",
    "support",
    "switch",
    "test",
    "track",
    "trim",
    "update",
    "upgrade",
    "use",
    "validate",
    "warn",
    "write",
];

/// The imperative of `word` when it is a known verb in another form
/// (`Adds`, `Adding`, `Added` are `Add`), keeping its case.
fn imperative(word: &str) -> Option<String> {
    let w = word.to_lowercase();
    if VERBS.contains(&w.as_str()) {
        return None;
    }
    let mut stems: Vec<String> = Vec::new();
    if let Some(s) = w.strip_suffix("ies") {
        stems.push(format!("{s}y"));
    }
    for suffix in ["es", "s", "ed", "d", "ing"] {
        if let Some(s) = w.strip_suffix(suffix) {
            stems.push(s.to_string());
            stems.push(format!("{s}e"));
            // `Splitting`, `Stopped`: the doubled consonant.
            let mut c = s.chars();
            if let (Some(a), Some(b)) = (c.next_back(), c.next_back()) {
                if a == b {
                    stems.push(s[..s.len() - a.len_utf8()].to_string());
                }
            }
        }
    }
    let base = stems.into_iter().find(|s| VERBS.contains(&s.as_str()))?;
    Some(if word.chars().next().is_some_and(char::is_uppercase) {
        upper_first(&base)
    } else {
        base
    })
}

/// Whether a character is an emoji or pictograph.
fn is_emoji(c: char) -> bool {
    let n = c as u32;
    (0x1F000..=0x1FAFF).contains(&n) || (0x2600..=0x27BF).contains(&n) || n == 0xFE0F
}

/// A title's summary as the rules want it: no agent name, emoji, `[bot]`,
/// `[AI]` or `WIP`, an imperative verb first, no closing stop.
pub fn tidy_summary(rest: &str) -> String {
    let t: String = rest.chars().filter(|c| !is_emoji(*c)).collect();
    let mut words: Vec<String> = t
        .split_whitespace()
        .filter(|w| !is_banned_word(w))
        .map(str::to_string)
        .collect();
    if words.first().is_some_and(|w| w == ":" || w == "-") {
        words.remove(0);
    }
    if let Some(first) = words.first_mut() {
        if let Some(v) = imperative(first) {
            *first = v;
        }
    }
    words
        .join(" ")
        .trim_start_matches([':', '-', ' '])
        .trim_end_matches(['.', ' '])
        .trim()
        .to_string()
}

/// A word a title never carries: the agent's name, `[bot]`, `[AI]`, `WIP`.
fn is_banned_word(w: &str) -> bool {
    let bare = w
        .trim_matches(|c: char| !c.is_alphanumeric())
        .to_ascii_lowercase();
    let bracketed = w.starts_with('[') && w.ends_with(']');
    bare == "coderhelm" || bare == "wip" || (bracketed && (bare == "bot" || bare == "ai"))
}

/// `title` within [`TITLE_TARGET`] characters when it can be cut at a word
/// (never an ellipsis), and never over [`TITLE_MAX`].
pub fn fit_title(title: &str) -> String {
    let t = title.trim();
    if t.chars().count() <= TITLE_TARGET {
        return t.to_string();
    }
    let mut out = String::new();
    for w in t.split_whitespace() {
        if out.chars().count() + w.chars().count() + 1 > TITLE_TARGET {
            break;
        }
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(w);
    }
    let out = out
        .trim_end_matches([',', ';', ':', '(', '-', ' '])
        .trim_end_matches(" and")
        .trim_end_matches(" for")
        .trim_end_matches(" to")
        .to_string();
    if out.is_empty() {
        t.chars().take(TITLE_MAX).collect()
    } else {
        out
    }
}

/// A dependency update's title: `Update next to 15.1.0` for one package,
/// `Update 48 npm dependencies (3 major)` for many, in the Conventional
/// Commits form `chore(deps): update ...` where the repository uses it.
pub fn dependency_title(
    conventional: bool,
    ecosystem: &str,
    packages: usize,
    majors: usize,
    single: Option<(&str, &str)>,
) -> String {
    let eco = if ecosystem.trim().is_empty() {
        String::new()
    } else {
        format!("{} ", ecosystem.trim())
    };
    let body = match single {
        Some((package, to)) => format!("update {package} to {to}"),
        None => format!(
            "update {packages} {eco}{}{}",
            if conventional {
                if packages == 1 {
                    "package"
                } else {
                    "packages"
                }
            } else if packages == 1 {
                "dependency"
            } else {
                "dependencies"
            },
            if majors > 0 {
                format!(" ({majors} major)")
            } else {
                String::new()
            }
        ),
    };
    if conventional {
        format!("chore(deps): {body}")
    } else {
        upper_first(&body)
    }
}

/// A security bundle's summary (`security updates for web (3 packages, 4
/// advisories)`) as a change a title describes: `update 3 packages to fix 4
/// security advisories`.
fn dependency_summary(rest: &str) -> Option<String> {
    let l = rest.to_lowercase();
    let at = l.find("security updates for ")?;
    let eco = rest[..at].trim();
    let open = rest.rfind('(')?;
    let inside = rest[open + 1..].trim_end_matches(')');
    let mut packages = None;
    let mut advisories = None;
    for part in inside.split(',') {
        let mut w = part.split_whitespace();
        let n: usize = w.next()?.parse().ok()?;
        match w.next()? {
            x if x.starts_with("package") => packages = Some(n),
            x if x.starts_with("advisor") => advisories = Some(n),
            _ => {}
        }
    }
    let packages = packages?;
    let eco = if eco.is_empty() {
        String::new()
    } else {
        format!("{eco} ")
    };
    Some(match advisories {
        Some(a) => format!(
            "Update {packages} {eco}package{} to fix {a} security advisor{}",
            if packages == 1 { "" } else { "s" },
            if a == 1 { "y" } else { "ies" }
        ),
        None => format!(
            "Update {packages} {eco}package{} for security advisories",
            if packages == 1 { "" } else { "s" }
        ),
    })
}

/// What a finished title does wrong against the rules, if anything: its
/// length, a closing stop, a first word that is not an imperative verb,
/// an agent name, emoji, `[bot]`, `[AI]` or `WIP`, the form the repository
/// uses, and a branch without the ticket key.
pub fn title_defects(title: &str, c: &Convention, key: Option<&str>, branch: &str) -> Vec<String> {
    let mut d = Vec::new();
    let n = title.chars().count();
    if n > TITLE_MAX {
        d.push(format!("{n} characters, over {TITLE_MAX}"));
    } else if n > TITLE_TARGET {
        d.push(format!("{n} characters, over {TITLE_TARGET}"));
    }
    if title.trim_end().ends_with('.') {
        d.push("ends with a stop".into());
    }
    let rest = bare(title).2;
    let first = rest.split_whitespace().next().unwrap_or("");
    if imperative(first).is_some() {
        d.push(format!("`{first}` is not imperative"));
    } else if !VERBS.contains(&first.to_lowercase().as_str()) {
        d.push(format!("`{first}` is not a known verb"));
    }
    for w in title.split_whitespace().filter(|w| is_banned_word(w)) {
        d.push(format!("contains {w}"));
    }
    let lower = title.to_lowercase();
    if key.is_some_and(|k| lower.contains(&format!("implement {}", k.to_lowercase()))) {
        d.push("names the ticket instead of the change".into());
    }
    if title.chars().any(is_emoji) {
        d.push("contains an emoji".into());
    }
    if let Some(v) = violation(
        c,
        title,
        &key.map(|k| vec![k.to_string()]).unwrap_or_default(),
    ) {
        d.push(v);
    }
    if let Some(k) = key.filter(|k| crate::git_conventions::is_ticket_key(k)) {
        if !branch.to_lowercase().contains(&k.to_lowercase()) {
            d.push(format!("the branch `{branch}` does not carry {k}"));
        }
    }
    d
}

/// What a title or commit subject does wrong against the convention, if
/// anything: for a check of text already written.
pub fn violation(c: &Convention, text: &str, real: &[String]) -> Option<String> {
    let placeholders = crate::placeholders::found(text.lines().next().unwrap_or(""), real);
    if !placeholders.is_empty() {
        return Some(format!(
            "`{}` is a placeholder, not a ticket key",
            placeholders.join("`, `")
        ));
    }
    let form = form_of(text);
    let same = match (c.form, form) {
        (Form::TicketKey(_), Form::TicketKey(_)) => true,
        (a, b) => a == b,
    };
    if c.source == Source::Default || c.source == Source::TeamConfig || same {
        return None;
    }
    let name = |f: Form| match f {
        Form::TicketKey(_) => "a ticket key and a summary",
        Form::Conventional => "a Conventional Commits type and a summary",
        Form::Plain => "a plain sentence",
    };
    Some(format!(
        "this repository writes {} ({}); this is {}",
        name(c.form),
        c.evidence,
        name(form)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn people(v: &[&str]) -> Vec<Sample> {
        v.iter()
            .map(|t| Sample {
                text: t.to_string(),
                bot: false,
            })
            .collect()
    }

    fn keyed_history() -> Vec<Sample> {
        let mut v = people(&[
            "OPS-412: Lower the sync log level",
            "OPS-409: Add retry to the poller",
            "Merge pull request #9 from acme/branch",
            "OPS-401: Fix the empty page",
            "OPS-399: Handle empty pages",
            "OPS-398: Cache the token",
            "Tidy the readme",
            "OPS-390: Drop the old flag",
            "OPS-388: Rename the queue",
            "OPS-381: Split the worker",
        ]);
        v.push(Sample {
            text: "fix(deps): bump glob-walk from 2.1.0 to 2.1.4".into(),
            bot: true,
        });
        v
    }

    #[test]
    fn the_convention_is_inferred_from_what_people_merged() {
        let merged = keyed_history();
        let c = detect(
            Kind::Title,
            &Inputs {
                merged: &merged,
                ..Default::default()
            },
        );
        assert_eq!(c.form, Form::TicketKey(Sep::Colon));
        assert_eq!(c.source, Source::MergedPullRequests);
        assert_eq!(
            c.evidence,
            "8 of the last 9 merged pull requests written by people"
        );
        // Too few samples, or no clear majority, teach nothing.
        let few = people(&["OPS-1: a", "OPS-2: b", "OPS-3: c"]);
        assert_eq!(
            detect(
                Kind::Title,
                &Inputs {
                    merged: &few,
                    ..Default::default()
                }
            )
            .source,
            Source::Default
        );
        let mixed = people(&[
            "OPS-1: a", "fix: b", "Do c", "OPS-2: d", "feat: e", "Do f", "OPS-3: g", "fix: h",
            "Do i",
        ]);
        assert_eq!(
            detect(
                Kind::Title,
                &Inputs {
                    merged: &mixed,
                    ..Default::default()
                }
            )
            .source,
            Source::Default
        );
    }

    #[test]
    fn explicit_sources_come_before_inference() {
        let merged = keyed_history();
        let guide = "## Commits\n\nCommit messages follow Conventional Commits.\nPull request titles read `<KEY>: <summary>`.\n";
        let i = Inputs {
            guide,
            merged: &merged,
            commits: &merged,
            ..Default::default()
        };
        let commit = detect(Kind::Commit, &i);
        assert_eq!(
            (commit.form, commit.source),
            (Form::Conventional, Source::RepoGuide)
        );
        let title = detect(Kind::Title, &i);
        assert_eq!(
            (title.form, title.source),
            (Form::TicketKey(Sep::Colon), Source::RepoGuide)
        );
        // Lint configuration, with no guide.
        let lint = Inputs {
            commitlint: Some((
                "commitlint.config.js",
                "module.exports = { extends: ['@commitlint/config-conventional'] }",
            )),
            merged: &merged,
            commits: &merged,
            ..Default::default()
        };
        let c = detect(Kind::Commit, &lint);
        assert_eq!((c.form, c.source), (Form::Conventional, Source::LintConfig));
        assert!(c.evidence.contains("commitlint.config.js"));
        // The team's own template is the last word.
        let team = Inputs {
            team_template: true,
            ..lint.clone()
        };
        assert_eq!(detect(Kind::Commit, &team).source, Source::TeamConfig);
    }

    #[test]
    fn a_subject_is_rewritten_into_the_form_with_a_real_key_or_none() {
        let keyed = Convention {
            form: Form::TicketKey(Sep::Colon),
            source: Source::MergedPullRequests,
            evidence: "8 of the last 9 merged pull requests written by people".into(),
            ..Default::default()
        };
        let was = "fix(deps): bump glob-walk 2.1.0 to 2.1.4 [SECURITY] (CVE-2026-31337)";
        let w = subject(&keyed, &Key::Real("OPS-431".into()), was);
        assert_eq!(
            w.text,
            "OPS-431: Bump glob-walk 2.1.0 to 2.1.4 [SECURITY] (CVE-2026-31337)"
        );
        assert_eq!(w.note, None);
        // No key: the closest valid form, said once, and nothing invented.
        let w = subject(
            &keyed,
            &Key::None("no tracker is connected with permission to create tickets".into()),
            was,
        );
        assert_eq!(
            w.text,
            "Bump glob-walk 2.1.0 to 2.1.4 [SECURITY] (CVE-2026-31337)"
        );
        let note = w.note.unwrap();
        assert!(note.contains("start with a ticket key"), "{note}");
        assert!(note.contains("no tracker is connected"), "{note}");
        // A placeholder somebody wrote is never carried over.
        let w = subject(
            &keyed,
            &Key::None("x".into()),
            "KEY-1: fix(deps): bump glob-walk",
        );
        assert_eq!(w.text, "Bump glob-walk");
        let conventional = Convention {
            form: Form::Conventional,
            source: Source::LintConfig,
            evidence: "x".into(),
            ..Default::default()
        };
        assert_eq!(
            subject(
                &conventional,
                &Key::Real("OPS-431".into()),
                "OPS-431: Bump glob-walk"
            )
            .text,
            "chore: bump glob-walk"
        );
        assert_eq!(
            subject(&conventional, &Key::None("x".into()), was).text,
            was
        );
    }

    #[test]
    fn a_key_is_real_only_as_the_runs_ticket_or_in_the_tracker() {
        assert!(real_key("OPS-431", Some("OPS-431"), None));
        assert!(real_key("OPS-17", None, Some(true)));
        assert!(!real_key("OPS-17", None, Some(false)));
        assert!(!real_key("OPS-17", None, None));
        assert!(!real_key("KEY-1", None, Some(true)));
        assert!(!real_key("#42", Some("#42"), None));
    }

    #[test]
    fn written_text_is_checked_against_the_convention() {
        let keyed = Convention {
            form: Form::TicketKey(Sep::Colon),
            source: Source::MergedPullRequests,
            evidence: "8 of the last 9 merged pull requests written by people".into(),
            ..Default::default()
        };
        let v = violation(
            &keyed,
            "fix(deps): security updates for web (14 packages)",
            &[],
        )
        .unwrap();
        assert!(v.contains("a ticket key and a summary"), "{v}");
        let v = violation(&keyed, "KEY-1: fix(deps): glob-walk 2.1.0 to 2.1.4", &[]).unwrap();
        assert_eq!(v, "`KEY-1` is a placeholder, not a ticket key");
        assert_eq!(violation(&keyed, "OPS-431: Bump glob-walk", &[]), None);
    }

    #[test]
    fn a_ticket_key_in_one_position_needs_forty_percent() {
        // 4 of 9 with the key at the end, the rest plain or mixed.
        let merged = people(&[
            "Lower the sync log level (OPS-412)",
            "Add retry to the poller (OPS-409)",
            "Fix the empty page (OPS-401)",
            "Cache the token (OPS-398)",
            "Tidy the readme",
            "OPS-390: Drop the old flag",
            "fix: rename the queue",
            "Split the worker",
            "[OPS-381] Move the cron",
        ]);
        let c = detect(
            Kind::Title,
            &Inputs {
                merged: &merged,
                ..Default::default()
            },
        );
        assert_eq!(c.form, Form::TicketKey(Sep::Suffix));
        assert_eq!(
            c.evidence,
            "4 of the last 9 merged pull requests written by people"
        );
        let w = subject(&c, &Key::Real("OPS-431".into()), "OPS-431: Adds the retry.");
        assert_eq!(w.text, "Add the retry (OPS-431)");
    }

    #[test]
    fn release_tooling_enforces_conventional_commits_with_the_scope_and_case_people_use() {
        let merged = people(&[
            "feat(studio): add the hero warning",
            "fix(api): handle an empty page",
            "fix(studio): keep the draft",
            "Tidy the readme",
            "chore: bump the lockfile",
            "OPS-1: something",
            "docs: explain the deploy",
            "Plain words here",
        ]);
        let c = detect(
            Kind::Title,
            &Inputs {
                release_tooling: Some("release-please-config.json"),
                merged: &merged,
                ..Default::default()
            },
        );
        assert_eq!((c.form, c.source), (Form::Conventional, Source::LintConfig));
        assert!(
            c.evidence.contains("release-please-config.json"),
            "{}",
            c.evidence
        );
        assert_eq!(c.scopes, vec!["studio".to_string(), "api".to_string()]);
        assert_eq!(c.lower, Some(true));
        let paths = vec!["apps/studio/schemaTypes/hero.ts".to_string()];
        let w = subject_for(
            &c,
            &Key::None("x".into()),
            "feat: Warns when Hero subtitle exceeds 160 characters.",
            &paths,
        );
        assert_eq!(
            w.text,
            "feat(studio): warn when Hero subtitle exceeds 160 characters"
        );
        // No scope fits: none is made up.
        let w = subject_for(
            &c,
            &Key::None("x".into()),
            "feat: add a flag",
            &["src/x.rs".to_string()],
        );
        assert_eq!(w.text, "feat: add a flag");
    }

    #[test]
    fn a_title_is_imperative_short_and_carries_no_agent_marks() {
        assert_eq!(
            tidy_summary("Adding retries to the poller."),
            "Add retries to the poller"
        );
        assert_eq!(tidy_summary("Fixed the empty page"), "Fix the empty page");
        assert_eq!(
            tidy_summary("Updates next to 15.1.0"),
            "Update next to 15.1.0"
        );
        assert_eq!(tidy_summary("Splitting the worker"), "Split the worker");
        assert_eq!(
            tidy_summary("[WIP] CoderHelm: Warns when the subtitle is long \u{1F680} [bot]"),
            "Warn when the subtitle is long"
        );
        let long = "OPS-12: Add a warning when the Hero subtitle exceeds one hundred and sixty characters in Studio";
        let fit = fit_title(long);
        assert!(fit.chars().count() <= TITLE_TARGET, "{fit}");
        assert!(!fit.contains('\u{2026}') && !fit.ends_with("..."), "{fit}");
        let plain = Convention::default();
        assert_eq!(
            title_defects(
                "OPS-12: Adds the warning.",
                &plain,
                Some("OPS-12"),
                "coderhelm/feature-warning"
            ),
            vec![
                "ends with a stop".to_string(),
                "`Adds` is not imperative".to_string(),
                "the branch `coderhelm/feature-warning` does not carry OPS-12".to_string(),
            ]
        );
        assert_eq!(
            title_defects(
                "OPS-12: Warn when the Hero subtitle exceeds 160 characters",
                &plain,
                Some("OPS-12"),
                "coderhelm/OPS-12-hero-subtitle-warning"
            ),
            Vec::<String>::new()
        );
        assert!(
            title_defects("WIP Implement OPS-12 [bot]", &plain, Some("OPS-12"), "x")
                .iter()
                .any(|d| d.contains("names the ticket"))
        );
    }

    #[test]
    fn a_dependency_title_says_how_many_and_how_many_major() {
        assert_eq!(
            dependency_title(true, "npm", 48, 3, None),
            "chore(deps): update 48 npm packages (3 major)"
        );
        assert_eq!(
            dependency_title(false, "npm", 48, 3, None),
            "Update 48 npm dependencies (3 major)"
        );
        assert_eq!(
            dependency_title(false, "npm", 1, 1, Some(("next", "15.1.0"))),
            "Update next to 15.1.0"
        );
        // A security bundle's title, as a change.
        let keyed = Convention {
            form: Form::TicketKey(Sep::Colon),
            ..Default::default()
        };
        assert_eq!(
            subject(
                &keyed,
                &Key::Real("OPS-9".into()),
                "fix(deps): npm security updates for web (14 packages, 31 advisories)"
            )
            .text,
            "OPS-9: Update 14 npm packages to fix 31 security advisories"
        );
    }
}
