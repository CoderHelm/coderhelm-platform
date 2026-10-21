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
    /// own message, a ticket title). Without a team template the summary is
    /// kept as written.
    pub fn commit_message(&self, tokens: &Tokens, summary: &str) -> String {
        match &self.commit {
            Some(t) => commit(t, &tokens.with_summary(summary)),
            None => summary.to_string(),
        }
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
