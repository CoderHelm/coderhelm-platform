//! A team's git conventions: the templates CoderHelm names its branches,
//! writes its commit messages and titles its pull requests with. Stored on
//! the settings table (`pk` = team id, `sk` = [`SETTINGS_SK`]); a template
//! left empty keeps CoderHelm's default. The gateway validates templates with
//! [`validate`] before saving; the worker renders them with [`Tokens`].
//!
//! Whatever a team's branch pattern, CoderHelm's own pull requests are
//! recognised by who opened them and by the run records, never by a branch
//! prefix alone: [`DEFAULT_BRANCH_PREFIX`] is only the fast path for branches
//! made before the pattern could change.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};

/// The settings record's sort key.
pub const SETTINGS_SK: &str = "SETTINGS#GIT_CONVENTIONS";

/// Branch template when the team set none (every branch before this setting).
pub const DEFAULT_BRANCH: &str = "coderhelm/{ticket_lower}";
/// The prefix every default branch starts with.
pub const DEFAULT_BRANCH_PREFIX: &str = "coderhelm/";
/// Commit message template when the team set none.
pub const DEFAULT_COMMIT: &str = "{ticket}: {title}";
/// PR title template when the team set none.
pub const DEFAULT_PR_TITLE: &str = "{ticket}: {title}";

/// The tokens a template may use.
pub const TOKENS: &[&str] = &["ticket", "ticket_lower", "slug", "summary", "title"];

/// Longest template accepted.
pub const MAX_TEMPLATE_CHARS: usize = 120;
/// Longest branch name produced.
pub const MAX_BRANCH_CHARS: usize = 100;
/// Longest commit subject line produced.
pub const MAX_COMMIT_CHARS: usize = 72;
/// Longest PR title produced (longer ones end in "...").
pub const MAX_PR_TITLE_CHARS: usize = 72;
/// Longest `{slug}`.
pub const MAX_SLUG_CHARS: usize = 40;

/// The three templates; `None` keeps the default.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitConventions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_title: Option<String>,
}

impl GitConventions {
    /// Reads the settings record; a missing or blank field is the default.
    pub fn from_item(item: &HashMap<String, AttributeValue>) -> Self {
        let field = |k: &str| {
            item.get(k)
                .and_then(|v| v.as_s().ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        };
        Self {
            branch: field("branch"),
            commit: field("commit"),
            pr_title: field("pr_title"),
        }
    }

    pub fn branch_template(&self) -> &str {
        self.branch.as_deref().unwrap_or(DEFAULT_BRANCH)
    }

    /// The branches a run for this ticket may already be on, newest pattern
    /// first: the team's template, then the default. `suffix` is appended to
    /// each (a multi-repo run's `-{repo}`).
    pub fn branch_candidates(&self, tokens: &Tokens, suffix: &str) -> Vec<String> {
        let mut out = vec![branch(self.branch_template(), tokens, suffix)];
        let default = branch(DEFAULT_BRANCH, tokens, suffix);
        if !out.contains(&default) {
            out.push(default);
        }
        out
    }

    /// The commit message for a commit described by `summary` (an agent's
    /// own message, a ticket title). The subject's ticket key is the run's
    /// real one whatever the agent wrote: a placeholder (`ABC-0`,
    /// `ABC-XXXX`, `{ticket}`) becomes the run's key, and a subject with no
    /// key gets it when the convention puts the key in commits (the team's
    /// template names `{ticket}`, or the default does). A run without a
    /// tracker key (a GitHub issue) never gets one invented; a placeholder is
    /// dropped instead. With a team template the agent's subject, without
    /// any key, is its `{summary}`; without one the message is kept as
    /// written apart from the key, body included.
    pub fn commit_message(&self, tokens: &Tokens, summary: &str) -> String {
        let key = tokens.key();
        let (first, body) = match summary.split_once('\n') {
            Some((f, b)) => (f, Some(b)),
            None => (summary, None),
        };
        match &self.commit {
            Some(t) if names_ticket(t) => {
                let bare = without_key(first, key);
                commit(t, &tokens.with_summary(&bare))
            }
            Some(t) => commit(
                t,
                &tokens.with_summary(&normalize_subject(first, key, false)),
            ),
            None => {
                let subject = normalize_subject(first, key, names_ticket(DEFAULT_COMMIT));
                match body {
                    Some(b) => format!("{subject}\n{b}"),
                    None => subject,
                }
            }
        }
    }

    /// How a commit subject this run makes reads, for the agent's prompt:
    /// the team's template (or the default) with the run's real key filled
    /// in and `…` for the summary. A run without a tracker key is told to
    /// put none in.
    pub fn commit_hint(&self, tokens: &Tokens) -> String {
        match tokens.key() {
            Some(key) => {
                let example = match &self.commit {
                    Some(t) => commit(t, &tokens.with_summary("…")),
                    None => format!("{key}: …"),
                };
                format!(
                    "Commit subject: `{example}`. This run's ticket key is `{key}`: write it \
                     exactly, never a placeholder such as `{project}-0`, `{project}-XXXX` or \
                     `{{ticket}}`.",
                    project = key.split('-').next().unwrap_or(key),
                )
            }
            None => "Commit subject: a short summary of the change. This run has no ticket \
                     key: put none in the subject and never make one up."
                .to_string(),
        }
    }
}

/// Whether a template puts the ticket in what it renders.
fn names_ticket(template: &str) -> bool {
    template.contains("{ticket}") || template.contains("{ticket_lower}")
}

/// Whether `s` is a tracker key as people write it: a project of ASCII
/// letters and digits that starts with a letter, `-`, then the number
/// (`ABC-123`, `OPS2-7`). A GitHub reference (`#42`) is not one.
pub fn is_ticket_key(s: &str) -> bool {
    let Some((project, number)) = s.split_once('-') else {
        return false;
    };
    project
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic())
        && project.chars().all(|c| c.is_ascii_alphanumeric())
        && !number.is_empty()
        && number.chars().all(|c| c.is_ascii_digit())
}

/// Project names people use as stand-ins in an example key.
const STAND_IN_PROJECTS: &[&str] = &[
    "ABC", "XYZ", "XXX", "PROJ", "PROJECT", "KEY", "TICKET", "ISSUE", "JIRA", "TASK", "STORY",
    "FOO", "ID",
];

/// Words that stand for the ticket inside braces or angle brackets
/// (`{ticket}`, `<TICKET-ID>`).
const STAND_IN_WORDS: &[&str] = &[
    "ticket",
    "ticket_id",
    "ticketid",
    "ticket-id",
    "ticket_key",
    "ticket-key",
    "ticket_lower",
    "key",
    "jira",
    "jira_key",
    "jira-key",
    "issue",
    "issue_key",
    "issue-key",
    "id",
];

/// A ticket key, or a stand-in for one, at the start of a commit subject.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Lead {
    /// Where the subject's own text starts after the key and its separator.
    rest: usize,
    /// The key as written, without brackets.
    key: String,
    /// It stands in for a key rather than naming one.
    stand_in: bool,
}

/// Whether the number part of a key is an example rather than a ticket's:
/// letters or marks (`XXXX`, `NNN`, `####`, `…`), all zeros, or the counting
/// sequence and nines people type as examples (`123`, `1234`, `9999`).
fn stand_in_number(number: &str) -> bool {
    if number.is_empty() {
        return true;
    }
    if !number.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }
    let counting = "1234567890".starts_with(number) && number.len() >= 3;
    number.chars().all(|c| c == '0') || counting || number.chars().all(|c| c == '9')
}

/// The key (or stand-in) a commit subject starts with, if any. `run_key` is
/// the run's real key: an exact match is never a stand-in.
fn lead(subject: &str, run_key: Option<&str>) -> Option<Lead> {
    let start = subject.len() - subject.trim_start().len();
    let s = &subject[start..];
    let close_of = |c: char| match c {
        '{' => Some('}'),
        '<' => Some('>'),
        '[' => Some(']'),
        '(' => Some(')'),
        _ => None,
    };
    let (inner, after, bracketed) = match s.chars().next().and_then(close_of) {
        Some(close) => {
            let end = s[1..].find(close)? + 1;
            (&s[1..end], end + 1, true)
        }
        None => {
            let end = s
                .find(|c: char| c == ':' || c.is_whitespace())
                .unwrap_or(s.len());
            (&s[..end], end, false)
        }
    };
    let inner_t = inner.trim();
    if inner_t.is_empty() || inner_t.len() > 32 {
        return None;
    }
    let is_run_key = run_key.is_some_and(|k| inner_t.eq_ignore_ascii_case(k));
    // Tracker projects are written in capitals; a lowercase word with a
    // dash (`utf-8`, `pre-commit`) is prose, unless it is the run's own
    // project written in lowercase (a `{ticket_lower}` template).
    let run_project = run_key.and_then(|k| k.split_once('-')).map(|(p, _)| p);
    let key_like_project = |project: &str| {
        !project.chars().any(|c| c.is_ascii_lowercase())
            || run_project.is_some_and(|p| p.eq_ignore_ascii_case(project))
    };
    let stand_in = if is_run_key {
        false
    } else if bracketed
        && STAND_IN_WORDS
            .iter()
            .any(|w| inner_t.eq_ignore_ascii_case(w))
    {
        true
    } else {
        let (project, number) = inner_t.split_once('-')?;
        let project_ok = project
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic())
            && project.chars().all(|c| c.is_ascii_alphanumeric())
            && key_like_project(project);
        // A key's number part: digits, or the marks examples are written with.
        let number_ok = number.chars().all(|c| {
            c.is_ascii_digit() || matches!(c, 'X' | 'x' | 'N' | 'n' | '#' | '?' | '*' | '…' | '.')
        });
        if !project_ok || !number_ok || number.is_empty() {
            return None;
        }
        stand_in_number(number)
            || STAND_IN_PROJECTS
                .iter()
                .any(|p| project.eq_ignore_ascii_case(p))
    };
    if !stand_in && !is_ticket_key(inner_t) {
        return None;
    }
    // The separator: `:`, ` - `, or plain space.
    let mut rest = start + after;
    let tail = &subject[rest..];
    let skipped = tail.len()
        - tail
            .trim_start_matches([':', ' ', '\t', '-', '–', '—'])
            .len();
    rest += skipped;
    Some(Lead {
        rest,
        key: inner_t.to_string(),
        stand_in,
    })
}

/// A commit subject with the run's real key: a stand-in key at its start is
/// replaced by `key` (or dropped when the run has none), and when `prefix`
/// is set and the subject names no key, `key` is put in front. A real key
/// that differs from the run's is left: the subject is about that ticket.
pub fn normalize_subject(subject: &str, key: Option<&str>, prefix: bool) -> String {
    let subject = subject.trim_end();
    match lead(subject, key) {
        Some(l) if l.stand_in => {
            let rest = &subject[l.rest..];
            match key {
                Some(k) if !rest.is_empty() => format!("{k}: {rest}"),
                Some(k) => k.to_string(),
                None => rest.to_string(),
            }
        }
        Some(_) => subject.trim_start().to_string(),
        None => match key {
            Some(k) if prefix && !subject.trim().is_empty() => {
                format!("{k}: {}", subject.trim_start())
            }
            _ => subject.trim_start().to_string(),
        },
    }
}

/// The real tracker key a subject starts with (a PR title CoderHelm wrote
/// from the ticket), if any; never a stand-in.
pub fn subject_key(subject: &str) -> Option<String> {
    lead(subject, None)
        .filter(|l| !l.stand_in && is_ticket_key(&l.key))
        .map(|l| l.key)
}

/// A whole commit message with its subject normalized as in
/// [`normalize_subject`] (no key added), its body kept.
pub fn normalize_message(message: &str, key: Option<&str>) -> String {
    match message.split_once('\n') {
        Some((first, body)) => format!("{}\n{body}", normalize_subject(first, key, false)),
        None => normalize_subject(message, key, false),
    }
}

/// The subject without the run's key or a stand-in for it at its start, for
/// a template that puts the key in itself.
fn without_key(subject: &str, key: Option<&str>) -> String {
    match lead(subject, key) {
        Some(l) if l.stand_in || key.is_some_and(|k| l.key.eq_ignore_ascii_case(k)) => {
            subject[l.rest..].trim().to_string()
        }
        _ => subject.trim().to_string(),
    }
}

/// What a template's tokens stand for, for one ticket.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tokens {
    /// The ticket as people write it (`ABC-123`, `#42`).
    pub ticket: String,
    /// The ticket's id in lowercase (`abc-123`, `gh-42`).
    pub ticket_lower: String,
    /// A short kebab-case summary of the title, ASCII, at most
    /// [`MAX_SLUG_CHARS`].
    pub slug: String,
    /// A one-line lowercase summary (for commits).
    pub summary: String,
    /// The ticket's title as written.
    pub title: String,
}

impl Tokens {
    /// `reference` is how the ticket is written (`ABC-123`, `#42`), `id` its
    /// stored id (`ABC-123`, `GH-42`).
    pub fn new(reference: &str, id: &str, title: &str) -> Self {
        let title = title.lines().next().unwrap_or("").trim().to_string();
        Self {
            ticket: reference.trim().to_string(),
            ticket_lower: id.trim().to_lowercase(),
            slug: slug(&title),
            summary: summary(&title),
            title,
        }
    }

    /// The run's tracker key (`ABC-123`), when its ticket has one. `None`
    /// for a GitHub issue (`#42`) or a run with no ticket.
    pub fn key(&self) -> Option<&str> {
        is_ticket_key(&self.ticket).then_some(self.ticket.as_str())
    }

    /// The same ticket, with `{summary}` taken from `text`.
    pub fn with_summary(&self, text: &str) -> Self {
        Self {
            summary: summary(text),
            ..self.clone()
        }
    }

    fn get(&self, name: &str) -> Option<&str> {
        Some(match name {
            "ticket" => &self.ticket,
            "ticket_lower" => &self.ticket_lower,
            "slug" => &self.slug,
            "summary" => &self.summary,
            "title" => &self.title,
            _ => return None,
        })
    }
}

/// A short kebab-case form of `text`: ASCII letters and digits, words joined
/// by `-`, at most [`MAX_SLUG_CHARS`], cut at a word.
pub fn slug(text: &str) -> String {
    let mut words: Vec<String> = Vec::new();
    let mut cur = String::new();
    for c in text.chars() {
        if c.is_ascii_alphanumeric() {
            cur.push(c.to_ascii_lowercase());
        } else if c == '\'' || c == '\u{2019}' {
            // "don't" stays one word.
        } else if !cur.is_empty() {
            words.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        words.push(cur);
    }
    let mut out = String::new();
    for w in words {
        let extra = if out.is_empty() { w.len() } else { w.len() + 1 };
        if out.len() + extra > MAX_SLUG_CHARS {
            if out.is_empty() {
                out = w[..MAX_SLUG_CHARS].to_string();
            }
            break;
        }
        if !out.is_empty() {
            out.push('-');
        }
        out.push_str(&w);
    }
    out
}

/// A one-line lowercase summary of `text`: its first line, the first letter
/// lowercased (an acronym such as `API` is kept), no trailing period.
pub fn summary(text: &str) -> String {
    let line = text
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("");
    let line = line.trim_end_matches('.').trim_end();
    let mut chars = line.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let second_upper = line.chars().nth(1).is_some_and(|c| c.is_uppercase());
    if second_upper {
        line.to_string()
    } else {
        first.to_lowercase().chain(chars).collect()
    }
}

fn render(template: &str, tokens: &Tokens, each: impl Fn(&str) -> String) -> String {
    let mut out = String::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        match after.find('}') {
            Some(close) => {
                let name = &after[..close];
                match tokens.get(name) {
                    Some(v) => out.push_str(&each(v)),
                    None => out.push_str(&rest[open..open + close + 2]),
                }
                rest = &after[close + 1..];
            }
            None => {
                out.push_str(&rest[open..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

/// The text cut to at most `max` characters, at a word when one is near.
fn cut(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let head: String = text.chars().take(max).collect();
    match head.rfind(' ') {
        Some(i) if i > max / 2 => head[..i].trim_end().to_string(),
        _ => head.trim_end().to_string(),
    }
}

/// A token's value made safe inside a branch name.
fn ref_part(value: &str) -> String {
    let mut out = String::new();
    for c in value.chars() {
        let c = if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
            c
        } else {
            '-'
        };
        if c == '-' && out.ends_with('-') {
            continue;
        }
        if c == '.' && out.ends_with('.') {
            continue;
        }
        out.push(c);
    }
    out.trim_matches(|c| c == '-' || c == '.').to_string()
}

/// The branch name for `template`, plus `suffix`. Token values are made
/// ref-safe; the template's own text was checked by [`validate`].
pub fn branch(template: &str, tokens: &Tokens, suffix: &str) -> String {
    let mut name = render(template, tokens, ref_part);
    let suffix = ref_part(suffix);
    if !suffix.is_empty() {
        name.push('-');
        name.push_str(&suffix);
    }
    while name.contains("//") {
        name = name.replace("//", "/");
    }
    let mut name = cut(&name, MAX_BRANCH_CHARS);
    while name.ends_with(['/', '.', '-']) || name.ends_with(".lock") {
        if name.ends_with(".lock") {
            name.truncate(name.len() - 5);
        } else {
            name.pop();
        }
    }
    name
}

/// The commit subject for `template`: one line, at most [`MAX_COMMIT_CHARS`].
pub fn commit(template: &str, tokens: &Tokens) -> String {
    let line = render(template, tokens, |v| v.replace(['\n', '\r'], " "));
    cut(line.trim(), MAX_COMMIT_CHARS)
}

/// The PR title for `template`: one line; past [`MAX_PR_TITLE_CHARS`] it is
/// cut and ends in "...".
pub fn pr_title(template: &str, tokens: &Tokens) -> String {
    let line = render(template, tokens, |v| v.replace(['\n', '\r'], " "));
    let line = line.trim();
    if line.chars().count() > MAX_PR_TITLE_CHARS {
        let head: String = line.chars().take(MAX_PR_TITLE_CHARS - 3).collect();
        format!("{}...", head.trim_end())
    } else {
        line.to_string()
    }
}

/// Whether `name` is a valid git branch name (the rules of
/// `git check-ref-format --branch` that a template can break).
pub fn is_valid_branch(name: &str) -> bool {
    if name.is_empty() || name.len() > 255 || name == "@" || name.starts_with('-') {
        return false;
    }
    if name.starts_with('/') || name.ends_with('/') || name.ends_with('.') {
        return false;
    }
    if name.contains("..") || name.contains("//") || name.contains("@{") {
        return false;
    }
    if name
        .chars()
        .any(|c| c.is_control() || c == ' ' || "~^:?*[\\".contains(c))
    {
        return false;
    }
    name.split('/')
        .all(|part| !part.is_empty() && !part.starts_with('.') && !part.ends_with(".lock"))
}

/// Which template is being checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Branch,
    Commit,
    PrTitle,
}

/// Checks a template before it is saved: one line, known tokens only, at
/// most [`MAX_TEMPLATE_CHARS`]; a branch template names the ticket and
/// produces a valid branch name. The error says what to change.
pub fn validate(kind: Kind, template: &str) -> Result<(), String> {
    let t = template.trim();
    if t.is_empty() {
        return Ok(());
    }
    if t.chars().count() > MAX_TEMPLATE_CHARS {
        return Err(format!(
            "Keep it to {MAX_TEMPLATE_CHARS} characters or fewer."
        ));
    }
    if t.contains(['\n', '\r']) {
        return Err("Use a single line.".into());
    }
    let mut rest = t;
    let mut used: Vec<&str> = Vec::new();
    while let Some(open) = rest.find(['{', '}']) {
        if rest[open..].starts_with('}') {
            return Err("A `}` has no matching `{`.".into());
        }
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            return Err("A `{` is never closed.".into());
        };
        let name = &after[..close];
        if !TOKENS.contains(&name) {
            return Err(format!(
                "`{{{name}}}` is not a token. Use {}.",
                TOKENS
                    .iter()
                    .map(|t| format!("`{{{t}}}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        used.push(name);
        rest = &after[close + 1..];
    }
    if kind == Kind::Branch {
        if !used.iter().any(|u| *u == "ticket" || *u == "ticket_lower") {
            return Err(
                "A branch name needs `{ticket}` or `{ticket_lower}`, so each ticket gets its own branch."
                    .into(),
            );
        }
        // The template's own text must already be valid: token values are
        // made safe, the text around them is not changed.
        let literal = render(t, &Tokens::new("x", "x", "x"), |_| "x".to_string());
        if !is_valid_branch(&literal) {
            return Err(
                "That isn't a valid git branch name: no spaces, `..`, `//`, `~^:?*[\\`, or a leading or trailing `/` or `.`."
                    .into(),
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens() -> Tokens {
        Tokens::new(
            "ABC-123",
            "ABC-123",
            "Fix the Login redirect after SSO sign-in.",
        )
    }

    #[test]
    fn renders_every_token() {
        let t = tokens();
        assert_eq!(t.slug, "fix-the-login-redirect-after-sso-sign-in");
        assert_eq!(t.summary, "fix the Login redirect after SSO sign-in");
        assert_eq!(
            branch("feature/{ticket}-{slug}", &t, ""),
            "feature/ABC-123-fix-the-login-redirect-after-sso-sign-in"
        );
        assert_eq!(branch(DEFAULT_BRANCH, &t, ""), "coderhelm/abc-123");
        assert_eq!(
            commit("{ticket_lower}: {summary}", &t),
            "abc-123: fix the Login redirect after SSO sign-in"
        );
        assert_eq!(
            pr_title("[{ticket}] {title}", &t),
            "[ABC-123] Fix the Login redirect after SSO sign-in."
        );
    }

    #[test]
    fn the_defaults_match_what_coderhelm_did_before() {
        let t = Tokens::new("#42", "GH-42", "Fix bug");
        let c = GitConventions::default();
        assert_eq!(branch(c.branch_template(), &t, ""), "coderhelm/gh-42");
        assert_eq!(pr_title(DEFAULT_PR_TITLE, &t), "#42: Fix bug");
        assert_eq!(commit(DEFAULT_COMMIT, &t), "#42: Fix bug");
        // Without a team template an agent's commit message is kept.
        assert_eq!(c.commit_message(&t, "Add the retry"), "Add the retry");
    }

    #[test]
    fn slug_is_short_ascii_and_cut_at_a_word() {
        assert_eq!(
            slug("Don't double-charge  café orders!"),
            "dont-double-charge-caf-orders"
        );
        let long = slug("Make the nightly export resilient to vendor timeouts and retries");
        assert!(long.len() <= MAX_SLUG_CHARS, "{long}");
        assert_eq!(long, "make-the-nightly-export-resilient-to");
        assert_eq!(slug("x".repeat(60).as_str()).len(), MAX_SLUG_CHARS);
        assert_eq!(slug("¿¿"), "");
    }

    #[test]
    fn summary_keeps_acronyms_and_drops_the_period() {
        assert_eq!(summary("Add retries."), "add retries");
        assert_eq!(summary("API keys rotate"), "API keys rotate");
        assert_eq!(summary("\n  Second line wins\nthird"), "second line wins");
        assert_eq!(summary(""), "");
    }

    #[test]
    fn commit_subjects_are_cut_to_72() {
        let t = Tokens::new("ABC-1", "ABC-1", &"word ".repeat(40));
        let c = commit("{ticket}: {summary}", &t);
        assert!(c.chars().count() <= MAX_COMMIT_CHARS, "{c}");
        assert!(c.starts_with("ABC-1: word word"));
        let p = pr_title("{ticket}: {title}", &t);
        assert_eq!(p.chars().count(), MAX_PR_TITLE_CHARS.min(p.chars().count()));
        assert!(p.ends_with("..."));
    }

    #[test]
    fn branch_names_stay_valid_whatever_the_ticket_says() {
        let t = Tokens::new("#7", "GH-7", "Fix: a/b..c ~weird^ [title]");
        for tpl in [
            "{ticket}/{slug}",
            "fix/{ticket_lower}-{summary}",
            "{ticket_lower}",
        ] {
            let b = branch(tpl, &t, "-web");
            assert!(is_valid_branch(&b), "{tpl} -> {b}");
        }
        assert_eq!(branch("{ticket}/{slug}", &t, ""), "7/fix-a-b-c-weird-title");
        assert_eq!(branch(DEFAULT_BRANCH, &t, "-web"), "coderhelm/gh-7-web");
        let long = Tokens::new("A-1", "A-1", &"long title words ".repeat(20));
        let b = branch("very/long/prefix/{ticket}-{title}", &long, "");
        assert!(b.len() <= MAX_BRANCH_CHARS && is_valid_branch(&b), "{b}");
    }

    #[test]
    fn rerun_lookup_tries_the_teams_pattern_then_the_default() {
        let t = tokens();
        let custom = GitConventions {
            branch: Some("fix/{ticket_lower}-{slug}".into()),
            ..Default::default()
        };
        assert_eq!(
            custom.branch_candidates(&t, ""),
            vec![
                "fix/abc-123-fix-the-login-redirect-after-sso-sign-in".to_string(),
                "coderhelm/abc-123".to_string(),
            ]
        );
        assert_eq!(
            GitConventions::default().branch_candidates(&t, "-api"),
            vec!["coderhelm/abc-123-api".to_string()]
        );
    }

    #[test]
    fn a_team_commit_template_wraps_the_agents_message() {
        let c = GitConventions {
            commit: Some("{ticket}: {summary}".into()),
            ..Default::default()
        };
        assert_eq!(
            c.commit_message(&tokens(), "Add the retry to the export job."),
            "ABC-123: add the retry to the export job"
        );
    }

    fn jira(key: &str) -> Tokens {
        Tokens::new(key, key, "Tidy the settings screen")
    }

    #[test]
    fn a_placeholder_key_becomes_the_runs_real_key() {
        let c = GitConventions::default();
        let t = jira("ABC-8121");
        for written in [
            "ABC-0: use the shared button",
            "ABC-0000: use the shared button",
            "ABC-XXXX: use the shared button",
            "ABC-XXX use the shared button",
            "ABC-NNNN: use the shared button",
            "ABC-####: use the shared button",
            "ABC-123: use the shared button",
            "ABC-…: use the shared button",
            "TICKET-1: use the shared button",
            "PROJ-42 - use the shared button",
            "{ticket}: use the shared button",
            "<ticket>: use the shared button",
            "[TICKET-ID] use the shared button",
            "use the shared button",
        ] {
            assert_eq!(
                c.commit_message(&t, written),
                "ABC-8121: use the shared button",
                "{written}"
            );
        }
        // The real key stays as written, in either case.
        assert_eq!(
            c.commit_message(&t, "ABC-8121: use the shared button"),
            "ABC-8121: use the shared button"
        );
        assert_eq!(
            c.commit_message(&t, "abc-8121: use the shared button"),
            "abc-8121: use the shared button"
        );
        // `ABC-123` is this run's own ticket here, not an example.
        assert_eq!(
            c.commit_message(&jira("ABC-123"), "ABC-123: fix"),
            "ABC-123: fix"
        );
        // Another real ticket is named on purpose.
        assert_eq!(
            c.commit_message(&t, "OPS-4411: bump the pool size"),
            "OPS-4411: bump the pool size"
        );
        // Prose with a dash is not a key.
        assert_eq!(
            c.commit_message(&t, "utf-8 decode the export"),
            "ABC-8121: utf-8 decode the export"
        );
        // The body is kept.
        assert_eq!(
            c.commit_message(&t, "ABC-0: fix\n\nWhy: the old one broke."),
            "ABC-8121: fix\n\nWhy: the old one broke."
        );
    }

    #[test]
    fn a_run_without_a_ticket_key_never_gets_one() {
        let c = GitConventions::default();
        let t = Tokens::new("#42", "GH-42", "Fix bug");
        assert_eq!(t.key(), None);
        assert_eq!(c.commit_message(&t, "ABC-0: fix the bug"), "fix the bug");
        assert_eq!(c.commit_message(&t, "{ticket}: fix the bug"), "fix the bug");
        assert_eq!(c.commit_message(&t, "fix the bug"), "fix the bug");
        let hint = c.commit_hint(&t);
        assert!(hint.contains("no ticket key"), "{hint}");
        // CoderHelm's own E2E tickets have no tracker key either.
        assert_eq!(
            Tokens::new("E2E-FIX-01ABC", "E2E-FIX-01ABC", "x").key(),
            None
        );
    }

    #[test]
    fn a_team_template_gets_the_subject_without_any_key() {
        let c = GitConventions {
            commit: Some("{ticket}: {summary}".into()),
            ..Default::default()
        };
        let t = jira("ABC-8121");
        for written in [
            "ABC-0000: Use the shared button.",
            "ABC-8121: Use the shared button.",
            "Use the shared button.",
        ] {
            assert_eq!(
                c.commit_message(&t, written),
                "ABC-8121: use the shared button",
                "{written}"
            );
        }
        // A template without the ticket still never keeps a placeholder.
        let plain = GitConventions {
            commit: Some("{summary}".into()),
            ..Default::default()
        };
        assert_eq!(
            plain.commit_message(&t, "ABC-0: fix it"),
            "ABC-8121: fix it"
        );
    }

    #[test]
    fn the_agent_is_told_the_real_key() {
        let t = jira("ABC-8121");
        let hint = GitConventions::default().commit_hint(&t);
        assert!(hint.starts_with("Commit subject: `ABC-8121: …`"), "{hint}");
        assert!(hint.contains("`ABC-0`"), "{hint}");
        let team = GitConventions {
            commit: Some("[{ticket}] {summary}".into()),
            ..Default::default()
        };
        assert!(team
            .commit_hint(&t)
            .starts_with("Commit subject: `[ABC-8121] …`"));
    }

    #[test]
    fn tracker_keys_are_read_strictly() {
        assert!(is_ticket_key("ABC-123"));
        assert!(is_ticket_key("OPS2-7"));
        assert!(!is_ticket_key("#42"));
        assert!(!is_ticket_key("ABC-"));
        assert!(!is_ticket_key("ABC-12a"));
        assert!(!is_ticket_key("1AB-12"));
        assert!(!is_ticket_key("E2E-FIX-01ABC"));
    }

    #[test]
    fn validation_allows_known_tokens_only() {
        assert!(validate(Kind::Branch, "").is_ok());
        assert!(validate(Kind::Branch, "feature/{ticket}-{slug}").is_ok());
        assert!(validate(Kind::Commit, "{ticket}: {summary}").is_ok());
        assert!(validate(Kind::PrTitle, "[{ticket}] {title}").is_ok());
        let e = validate(Kind::Commit, "{ticket}: {nope}").unwrap_err();
        assert!(e.contains("`{nope}` is not a token"), "{e}");
        assert!(validate(Kind::Commit, "{ticket").is_err());
        assert!(validate(Kind::Commit, "ticket}").is_err());
        assert!(validate(Kind::Commit, "a\nb").is_err());
        assert!(validate(Kind::PrTitle, &"x".repeat(MAX_TEMPLATE_CHARS + 1)).is_err());
    }

    #[test]
    fn validation_rejects_branch_templates_git_would() {
        assert!(validate(Kind::Branch, "fix/{slug}")
            .unwrap_err()
            .contains("needs `{ticket}`"));
        for bad in [
            "fix it/{ticket}",
            "fix..{ticket}",
            "/{ticket}",
            "{ticket}/",
            "fix//{ticket}",
            "fix~{ticket}",
            ".hidden/{ticket}",
            "{ticket}.lock",
            "-{ticket}",
        ] {
            assert!(validate(Kind::Branch, bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn reads_the_settings_record() {
        let mut item = HashMap::new();
        item.insert(
            "branch".to_string(),
            AttributeValue::S("fix/{ticket}".into()),
        );
        item.insert("commit".to_string(), AttributeValue::S("  ".into()));
        let c = GitConventions::from_item(&item);
        assert_eq!(c.branch.as_deref(), Some("fix/{ticket}"));
        assert_eq!(c.commit, None);
        assert_eq!(c.pr_title, None);
    }
}
