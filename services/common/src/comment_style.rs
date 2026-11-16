//! How a CoderHelm comment reads: a senior engineer's short note.
//!
//! A pull request once got a comment that was one long line listing a
//! dozen deploy workflows (Lambdas, for a web page change) with their file
//! paths, then the release plan pasted in ("1. Deploy the change to
//! production. How: wait for ... CoderHelm does this after the merge."). A
//! second comment pasted the outside-work plan ("Done outside the code:
//! ...; CoderHelm does the other 4 step(s) itself"), asked a person to read
//! an end-to-end result and to run a flow in two browsers, repeated the same
//! missing permission under each item, and ended with "This replaces what I
//! wrote at 03:28 UTC" and a History block. The owner: "they don't make
//! sense", "sounds like a bot", "super spammy".
//!
//! Every comment goes through [`render`] and then [`lint`] before the
//! conversation layer writes it (`conversation::speak_with`):
//!
//! - [`render`] cleans what a writer upstream got wrong, deterministically:
//!   history blocks and "This replaces what I wrote" go, pasted plan lines
//!   ("How:", "After step", "Gate:", "How it deploys", "Done outside the
//!   code", "CoderHelm does ... itself") go, template plurals ("step(s)")
//!   are written out, workflow file paths in parentheses go, a sentence
//!   said once is not said again, collapsed sections go (details belong in
//!   the pull request description), and the note is cut to [`MAX_LINES`]
//!   lines of at most [`MAX_LINE_CHARS`] characters.
//! - [`lint`] is the check: a text that still breaks a rule is never
//!   posted; the layer keeps quiet and logs the defect.

/// Lines a comment shows, at most.
pub const MAX_LINES: usize = 5;
/// Characters one line holds, at most.
pub const MAX_LINE_CHARS: usize = 320;

/// The line that closes a comment cut to [`MAX_LINES`].
pub fn more_line(markdown: bool) -> &'static str {
    if markdown {
        "The rest is in the pull request description."
    } else {
        "The rest is on the run page."
    }
}

/// Words that never appear in a comment, and why.
pub const BANNED: &[(&str, &str)] = &[
    (
        "This replaces what I wrote",
        "names an earlier version of itself",
    ),
    ("History:", "carries a history block"),
    ("Retried at ", "carries a history line"),
    ("How it deploys", "carries a deploy map"),
    ("Done outside the code", "pastes plan status"),
    ("CoderHelm does the other", "pastes plan status"),
    ("CoderHelm does this after", "pastes plan status"),
    (" How: ", "pastes plan text"),
    ("After step ", "pastes plan text"),
    ("Gate: ", "pastes plan text"),
    (
        "<details>",
        "folds details into a comment (they go in the description)",
    ),
];

/// What breaks the style.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Problem {
    /// More than [`MAX_LINES`] lines.
    TooManyLines(usize),
    /// A line longer than [`MAX_LINE_CHARS`] (its number, 1-based).
    LongLine(usize),
    /// A banned phrase, and why.
    Banned(&'static str, &'static str),
    /// A template plural such as "step(s)".
    TemplatePlural(String),
    /// More than one workflow file path, or a list of file paths.
    PathDump(usize),
    /// The same sentence twice.
    Repeated(String),
    /// An arrow glyph.
    Arrow,
}

impl std::fmt::Display for Problem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Problem::TooManyLines(n) => write!(f, "{n} lines (at most {MAX_LINES})"),
            Problem::LongLine(n) => write!(f, "line {n} is over {MAX_LINE_CHARS} characters"),
            Problem::Banned(p, why) => write!(f, "\"{p}\": {why}"),
            Problem::TemplatePlural(w) => write!(f, "template plural \"{w}\""),
            Problem::PathDump(n) => write!(f, "{n} file paths"),
            Problem::Repeated(s) => write!(f, "says \"{s}\" twice"),
            Problem::Arrow => write!(f, "an arrow glyph"),
        }
    }
}

/// Lines a reader sees: not empty, not a hidden marker, not a quote of a
/// person's comment (a reply opens on one).
fn shown_lines(text: &str) -> Vec<&str> {
    text.lines()
        .map(str::trim_end)
        .filter(|l| !l.trim().is_empty())
        .filter(|l| !l.trim_start().starts_with("<!--"))
        .filter(|l| !l.trim_start().starts_with('>'))
        .collect()
}

/// Template plurals in `text` ("step(s)", "item(s)").
fn template_plurals(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(i) = text[from..].find("(s)") {
        let at = from + i;
        let word: String = text[..at]
            .chars()
            .rev()
            .take_while(|c| c.is_alphabetic())
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        if !word.is_empty() {
            out.push(format!("{word}(s)"));
        }
        from = at + 3;
    }
    out
}

/// A token that reads as a file path: a slash and a file extension.
fn is_path(token: &str) -> bool {
    let t = token.trim_matches(|c: char| "`'\"(),;:.".contains(c));
    if t.starts_with("http") || !t.contains('/') {
        return false;
    }
    t.rsplit('/')
        .next()
        .and_then(|name| name.rsplit_once('.'))
        .is_some_and(|(stem, ext)| {
            !stem.is_empty()
                && (1..=5).contains(&ext.len())
                && ext.chars().all(char::is_alphanumeric)
        })
}

/// Lower-case words of a sentence, for telling repeats apart.
fn norm(s: &str) -> String {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect::<Vec<_>>()
        .join(" ")
}

/// The sentences and clauses of a line (split after ". ", "; " and ": "),
/// each with its separator.
fn pieces(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        cur.push(chars[i]);
        let ends =
            matches!(chars[i], '.' | ';' | ':') && chars.get(i + 1).is_some_and(|c| *c == ' ');
        if ends {
            out.push(std::mem::take(&mut cur));
        }
        i += 1;
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// A sentence long enough that saying it twice is a repeat, not a short
/// shared phrase.
fn counts_as_sentence(n: &str) -> bool {
    n.split(' ').count() >= 5
}

/// Every way `text` breaks the style; empty when it keeps it.
pub fn lint(text: &str) -> Vec<Problem> {
    let mut out = Vec::new();
    let lines = shown_lines(text);
    if lines.len() > MAX_LINES {
        out.push(Problem::TooManyLines(lines.len()));
    }
    for (i, l) in lines.iter().enumerate() {
        if l.chars().count() > MAX_LINE_CHARS {
            out.push(Problem::LongLine(i + 1));
        }
    }
    for (p, why) in BANNED {
        if text.contains(p) {
            out.push(Problem::Banned(p, why));
        }
    }
    for w in template_plurals(text) {
        out.push(Problem::TemplatePlural(w));
    }
    let workflows = text.matches(".github/workflows/").count();
    let paths = text.split_whitespace().filter(|t| is_path(t)).count();
    if workflows > 1 || paths > 2 {
        out.push(Problem::PathDump(workflows.max(paths)));
    }
    let mut seen = std::collections::BTreeSet::new();
    for l in &lines {
        for p in pieces(l) {
            let n = norm(&p);
            if counts_as_sentence(&n) && !seen.insert(n) {
                out.push(Problem::Repeated(p.trim().to_string()));
            }
        }
    }
    if crate::conversation::has_arrow(text) {
        out.push(Problem::Arrow);
    }
    out
}

/// Whether a line is plan text or plan status pasted into a comment.
fn plan_line(l: &str) -> bool {
    let t = l
        .trim()
        .trim_start_matches(['-', '*', ' '])
        .trim_start_matches("**")
        .trim();
    t.starts_with("Done outside the code")
        || t.starts_with("CoderHelm does the other")
        || t.starts_with("How it deploys")
        || t.starts_with("Retried at ")
        || t.contains(" How: ")
        || t.contains("After step ")
        || t.starts_with("Gate: ")
}

/// Whether a line only says nothing is needed (a status for nobody).
fn nothing_line(l: &str) -> bool {
    let t = l.trim().replace("**", "");
    let t = t.trim();
    t.eq_ignore_ascii_case("Needed from you: Nothing.")
        || t.eq_ignore_ascii_case("Needed from you: Nothing")
        || t.eq_ignore_ascii_case("Nothing.")
}

/// A heading line ("**Needed from you:**") with nothing on it.
fn bare_heading(l: &str) -> bool {
    let t = l.trim();
    (t.starts_with("**") && t.ends_with(":**")) || (t.starts_with('#') && !t.contains(". "))
}

/// Writes out "step(s)": "1 step(s)" is "1 step", any other count (or none)
/// "steps".
fn fix_plurals(line: &str) -> String {
    let mut out = String::new();
    let mut rest = line;
    while let Some(i) = rest.find("(s)") {
        let head = &rest[..i];
        let word_len = head
            .chars()
            .rev()
            .take_while(|c| c.is_alphabetic())
            .map(char::len_utf8)
            .sum::<usize>();
        let before_word = head[..head.len() - word_len].trim_end();
        let one = before_word
            .rsplit(|c: char| c.is_whitespace() || c == '(')
            .next()
            .is_some_and(|w| w == "1" || w.eq_ignore_ascii_case("one"));
        out.push_str(head);
        if word_len > 0 && !one {
            out.push('s');
        }
        rest = &rest[i + 3..];
    }
    out.push_str(rest);
    out
}

/// Takes out parenthesised groups that hold a workflow file path
/// ("(`.github/workflows/deploy.yml`)").
fn drop_workflow_parens(line: &str) -> String {
    let mut out = line.to_string();
    while let Some(p) = out.find(".github/workflows/") {
        let open = out[..p].rfind('(');
        let close = out[p..].find(')').map(|c| p + c);
        match (open, close) {
            (Some(o), Some(c)) if !out[o..p].contains(')') => {
                let start = if o > 0 && out[..o].ends_with(' ') {
                    o - 1
                } else {
                    o
                };
                out.replace_range(start..=c, "");
            }
            _ => break,
        }
    }
    out
}

/// Cuts a line where it starts naming an earlier version of itself.
fn drop_replaces(line: &str) -> String {
    match line.find("This replaces what I wrote") {
        Some(at) => line[..at].trim_end().to_string(),
        None => line.to_string(),
    }
}

/// Removes sentences that are plan status ("CoderHelm does this after the
/// merge.").
fn drop_plan_sentences(line: &str) -> String {
    pieces(line)
        .into_iter()
        .filter(|p| {
            let t = p.trim();
            !(t.starts_with("CoderHelm does this after")
                || t.starts_with("CoderHelm does the other"))
        })
        .collect::<String>()
        .trim_end()
        .to_string()
}

/// The comment as it is posted: cleaned and cut to the style (see the
/// module docs). `markdown`: GitHub (true) or Jira (false).
pub fn render(body: &str, markdown: bool) -> String {
    render_within(body, markdown, MAX_LINES)
}

/// [`render`] in at most `max_lines` lines (a reply's text, under the line
/// that names the person).
pub fn render_within(body: &str, markdown: bool, max_lines: usize) -> String {
    let max_lines = max_lines.clamp(1, MAX_LINES);
    let text = crate::conversation::strip_marker(body);
    // Collapsed sections are details: the description holds them.
    let text = match text.find("<details>") {
        Some(at) => text[..at].to_string(),
        None => text,
    };
    let text = crate::conversation::no_arrows(&text);
    let mut seen = std::collections::BTreeSet::new();
    let mut lines: Vec<String> = Vec::new();
    for raw in text.lines() {
        let t = raw.trim();
        if t.starts_with("History:") {
            break;
        }
        if t.is_empty() || t.starts_with("<!--") || plan_line(t) || nothing_line(t) {
            continue;
        }
        let l = drop_replaces(raw.trim_end());
        let l = drop_plan_sentences(&l);
        let l = fix_plurals(&l);
        let l = drop_workflow_parens(&l);
        // A sentence said once is not said again.
        let all = pieces(&l);
        let n_all = all.len();
        let mut kept: Vec<String> = all
            .into_iter()
            .filter(|p| {
                let n = norm(p);
                !counts_as_sentence(&n) || seen.insert(n)
            })
            .collect();
        // A lead-in left without what it introduced ("CoderHelm could
        // not:") goes too.
        if kept.len() < n_all {
            while kept.last().is_some_and(|p| p.trim_end().ends_with(':')) {
                kept.pop();
            }
        }
        let kept: String = kept.concat();
        let kept = kept.trim_end().trim_end_matches(';').trim_end().to_string();
        let lead = kept.trim().trim_start_matches(['-', '*', ' ']);
        if lead.is_empty() || lead.chars().all(|c| c.is_ascii_digit() || c == '.') {
            continue;
        }
        lines.push(kept);
    }
    // A heading left with nothing under it says nothing.
    let mut out: Vec<String> = Vec::new();
    for (i, l) in lines.iter().enumerate() {
        if bare_heading(l) && lines.get(i + 1).is_none_or(|n| bare_heading(n)) {
            continue;
        }
        out.push(crate::conversation::cut_words(l, MAX_LINE_CHARS));
    }
    if out.len() > max_lines {
        out.truncate(max_lines - 1);
        out.push(more_line(markdown).to_string());
    }
    out.join("\n")
}

/// [`render`], then [`lint`]: the text to post, or why it can't be.
pub fn clean(body: &str, markdown: bool) -> Result<String, Vec<Problem>> {
    let text = render(body, markdown);
    let problems = lint(&text);
    if problems.is_empty() {
        Ok(text)
    } else {
        Err(problems)
    }
}

/// The two comments a pull request got, with generic names: fixtures for
/// the tests here and in `conversation`.
#[cfg(test)]
pub mod tests_fixtures {
    /// The deploy comment.
    pub const DEPLOY_BEFORE: &str = "How it deploys: Staging deploys on merge to `main` (`Deploy Thumbnail Lambda`, .github/workflows/deploy-lambda-thumbnail.yml); Staging deploys on merge to `main` (`Deploy Upload Lambda`, .github/workflows/deploy-lambda-upload.yml); Staging deploys on merge to `main` (`Deploy Web App`, .github/workflows/deploy-web.yml); Production deploys on a `v*` tag (`Release`, .github/workflows/release.yml).\n1. Deploy the change to production. How: wait for `Deploy Thumbnail Lambda` (.github/workflows/deploy-lambda-thumbnail.yml), which runs on merge to `main`. CoderHelm does this after the merge.";

    /// The outside-work comment.
    pub const OUTSIDE_BEFORE: &str = "Done outside the code: Open the PR ready for review.\nCoderHelm does the other 4 step(s) itself.\n**Needed from you:**\n1. Read the Deploy Preview e2e result. If the frame was refused, the page header blocks it. CoderHelm could not: Page Not Found did not run on `abc1234`; not done: CoderHelm's GitHub App is not allowed to start workflows in this repository. To let it: give the app \"Actions: write\" (a repository admin)\n2. Run the registration flow inside the iframe in a fresh browser context in Chromium and WebKit. CoderHelm could not: CoderHelm's GitHub App is not allowed to start workflows in this repository. To let it: give the app \"Actions: write\" (a repository admin)\n\nThis replaces what I wrote at 03:28 UTC (\"Where: the booking portal; Page Not Found\").\n\nHistory:\n- 03:28 UTC: Where: the booking portal";
}

#[cfg(test)]
mod tests {
    use super::tests_fixtures::*;
    use super::*;

    #[test]
    fn the_deploy_comment_breaks_the_style_and_render_empties_it() {
        let p = lint(DEPLOY_BEFORE);
        assert!(
            p.iter()
                .any(|x| matches!(x, Problem::Banned("How it deploys", _))),
            "{p:?}"
        );
        assert!(p.iter().any(|x| matches!(x, Problem::PathDump(_))), "{p:?}");
        assert!(p.iter().any(|x| matches!(x, Problem::LongLine(_))), "{p:?}");
        // All of it is plan text for nobody: nothing is left to post.
        assert_eq!(render(DEPLOY_BEFORE, true), "");
    }

    #[test]
    fn the_outside_comment_breaks_the_style_and_render_fixes_it() {
        let p = lint(OUTSIDE_BEFORE);
        for want in [
            "This replaces what I wrote",
            "History:",
            "Done outside the code",
        ] {
            assert!(
                p.iter()
                    .any(|x| matches!(x, Problem::Banned(b, _) if *b == want)),
                "{want}: {p:?}"
            );
        }
        assert!(
            p.iter().any(|x| matches!(x, Problem::TemplatePlural(_))),
            "{p:?}"
        );
        assert!(p.iter().any(|x| matches!(x, Problem::Repeated(_))), "{p:?}");
        let after = render(OUTSIDE_BEFORE, true);
        assert!(lint(&after).is_empty(), "{after}\n{:?}", lint(&after));
        assert!(!after.contains("step(s)"), "{after}");
        assert!(!after.contains("History"), "{after}");
        assert!(!after.contains("This replaces"), "{after}");
        assert_eq!(
            after.matches("not allowed to start workflows").count(),
            1,
            "{after}"
        );
    }

    #[test]
    fn template_plurals_are_written_out() {
        assert_eq!(
            fix_plurals("CoderHelm did 1 step(s)."),
            "CoderHelm did 1 step."
        );
        assert_eq!(
            fix_plurals("CoderHelm did 4 step(s)."),
            "CoderHelm did 4 steps."
        );
        assert_eq!(fix_plurals("the item(s) left"), "the items left");
        assert!(lint("Two file(s) changed.").contains(&Problem::TemplatePlural("file(s)".into())));
    }

    #[test]
    fn workflow_paths_in_parentheses_go() {
        assert_eq!(
            drop_workflow_parens(
                "Wait for `Deploy Web` (.github/workflows/deploy-web.yml) to pass."
            ),
            "Wait for `Deploy Web` to pass."
        );
        assert_eq!(
            drop_workflow_parens("Wait for (`Deploy Web`, `.github/workflows/deploy-web.yml`)."),
            "Wait for."
        );
        // One path a person must open stays, and passes the lint.
        let one = "Open `.github/workflows/deploy-web.yml` and approve the environment.";
        assert!(lint(one).is_empty(), "{:?}", lint(one));
    }

    #[test]
    fn a_long_comment_is_cut_to_five_lines() {
        let body = (1..=9)
            .map(|i| format!("- Item number {i} for a person on the team to look at."))
            .collect::<Vec<_>>()
            .join("\n");
        let out = render(&body, true);
        assert_eq!(out.lines().count(), MAX_LINES, "{out}");
        assert!(out.ends_with(more_line(true)), "{out}");
        assert!(lint(&out).is_empty(), "{:?}", lint(&out));
        assert!(render(&body, false).ends_with(more_line(false)));
    }

    #[test]
    fn a_collapsed_section_and_marker_are_not_shown() {
        let body = "Tests failed on the merge commit.\n\n<details><summary>Log</summary>\n\nlots\n</details>";
        assert_eq!(render(body, true), "Tests failed on the merge commit.");
        assert!(!lint("a\n<details>x</details>").is_empty());
    }

    #[test]
    fn a_short_plain_note_passes_unchanged() {
        let body = "**Needed from you:** @owner, approve the production deploy: the team requires an approval.";
        assert_eq!(render(body, true), body);
        assert!(lint(body).is_empty());
    }

    #[test]
    fn nothing_needed_alone_says_nothing() {
        assert_eq!(render("**Needed from you:** Nothing.", true), "");
        assert_eq!(render("**Needed from you:**\n", true), "");
    }

    #[test]
    fn path_dumps_and_arrows_are_caught() {
        assert!(lint("Changed a/b.ts, c/d.ts and e/f.ts.")
            .iter()
            .any(|p| matches!(p, Problem::PathDump(3))));
        assert!(lint("Build \u{2192} deploy").contains(&Problem::Arrow));
    }
}
