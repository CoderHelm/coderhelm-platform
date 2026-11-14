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
}

/// The form of a title or commit subject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Form {
    /// A tracker key, then the summary.
    TicketKey(Sep),
    /// `type(scope): summary` (Conventional Commits).
    Conventional,
    /// A plain sentence.
    Plain,
}

/// Where the convention was read from, strongest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Source {
    TeamConfig,
    RepoGuide,
    LintConfig,
    MergedPullRequests,
    BaseCommits,
    /// Nothing said: CoderHelm's own default.
    Default,
}

/// A detected convention, with what it rests on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Convention {
    pub form: Form,
    pub source: Source,
    /// What was read, in words (`24 of the last 30 merged pull requests`).
    pub evidence: String,
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
    /// Titles of recent merged pull requests, newest first.
    pub merged: &'a [Sample],
    /// Subjects of recent base-branch commits, newest first.
    pub commits: &'a [Sample],
}

fn key_lead() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"^(\[)?([A-Z][A-Z0-9]*-\d+)(\])?(:)?\s+\S").expect("valid"))
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
    Form::Plain
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
/// `(form, how many agree, how many were read)`.
fn majority(samples: &[Sample]) -> Option<(Form, usize, usize)> {
    let read: Vec<Form> = samples
        .iter()
        .filter(|s| !is_noise(s))
        .take(SAMPLE_SIZE)
        .map(|s| form_of(&s.text))
        .collect();
    if read.len() < MIN_SAMPLES {
        return None;
    }
    let count = |want: fn(&Form) -> bool| read.iter().filter(|f| want(f)).count();
    let keyed = count(|f| matches!(f, Form::TicketKey(_)));
    let conventional = count(|f| matches!(f, Form::Conventional));
    let plain = count(|f| matches!(f, Form::Plain));
    let clear = |n: usize| n * 100 >= MAJORITY_PERCENT * read.len();
    if clear(keyed) {
        let sep = [Sep::Colon, Sep::Space, Sep::Bracket]
            .into_iter()
            .max_by_key(|s| read.iter().filter(|f| **f == Form::TicketKey(*s)).count())
            .unwrap_or(Sep::Colon);
        Some((Form::TicketKey(sep), keyed, read.len()))
    } else if clear(conventional) {
        Some((Form::Conventional, conventional, read.len()))
    } else if clear(plain) {
        Some((Form::Plain, plain, read.len()))
    } else {
        None
    }
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

/// The convention for `kind`, from the strongest source that says one.
pub fn detect(kind: Kind, i: &Inputs<'_>) -> Convention {
    if i.team_template {
        return Convention {
            form: Form::Plain,
            source: Source::TeamConfig,
            evidence: "the team's own template in Settings".to_string(),
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
        };
    }
    if let Some((form, evidence)) = enforced(i, kind) {
        return Convention {
            form,
            source: Source::LintConfig,
            evidence,
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
            };
        }
    }
    Convention {
        form: Form::Plain,
        source: Source::Default,
        evidence: "no configuration, guide or clear majority in recent history".to_string(),
    }
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
    let (ty, scope, rest) = bare(subject);
    let text = match (c.form, key) {
        (Form::TicketKey(sep), Key::Real(k)) => {
            let rest = upper_first(&rest);
            match sep {
                Sep::Colon => format!("{k}: {rest}"),
                Sep::Space => format!("{k} {rest}"),
                Sep::Bracket => format!("[{k}] {rest}"),
            }
        }
        (Form::Conventional, _) => format!(
            "{}{}: {}",
            ty.as_deref().unwrap_or("chore"),
            scope.map(|s| format!("({s})")).unwrap_or_default(),
            lower_first(&rest)
        ),
        (Form::TicketKey(_), Key::None(_)) | (Form::Plain, _) => upper_first(&rest),
    };
    let note = match (c.form, key) {
        (Form::TicketKey(_), Key::None(why)) => Some(format!(
            "Titles and commits in this repository start with a ticket key ({}). This run has no ticket and CoderHelm did not make one ({}), so they carry none: add the key to the title before merging if one is required.",
            c.evidence,
            why.trim_end_matches('.')
        )),
        _ => None,
    };
    Written { text, note }
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
}
