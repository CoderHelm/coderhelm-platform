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
//!
//! A person once wrote "@CoderHelm rebase with main" on a pull request and
//! got back a quote of that very comment, an "@name, replying to your
//! comment" header, a second @-mention, "you asked for a rebase with main:",
//! two clauses on how the merge was done, and a link to the pull request the
//! reply sat on. The owner called it insane. What a colleague writes is one
//! line: "Merged main in (15401ad)."
//!
//! So every comment also passes the thread rules, which need to know where
//! it is posted ([`Place`]): [`tidy`] fixes them, [`lint_on`] checks them.
//!
//! - no "replying to" header, ever (a reply that answers an older comment
//!   quotes it instead: `conversation::reply_body`);
//! - a quote and a header never both;
//! - one @-mention of a person at most;
//! - never a link to the thread the comment is posted on;
//! - never the person's request said back to them ("you asked for ...").

use crate::conversation::Place;
use regex::Regex;
use std::sync::LazyLock;

/// Lines a comment shows, at most.
pub const MAX_LINES: usize = 5;
/// Lines a reply to a person shows, at most (a quote of an older comment
/// aside).
pub const REPLY_LINES: usize = 3;
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
    /// A "replying to" header.
    ReplyHeader,
    /// A quote of the person's comment and a header both.
    QuoteAndHeader,
    /// The same person @-mentioned more than once.
    MentionTwice(String),
    /// A link to the thread the comment is posted on.
    SelfLink(String),
    /// The person's request said back to them.
    EchoesRequest(String),
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
            Problem::ReplyHeader => write!(f, "a \"replying to\" header"),
            Problem::QuoteAndHeader => write!(f, "a quote and a header both"),
            Problem::MentionTwice(n) => write!(f, "mentions @{n} more than once"),
            Problem::SelfLink(u) => write!(f, "links to the thread it is posted on ({u})"),
            Problem::EchoesRequest(s) => write!(f, "says the request back (\"{s}\")"),
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

// ── The thread rules ──────────────────────────────────────────────────

static MD_LINK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[([^\]\n]*)\]\(([^)\s]+)\)").unwrap());
static BARE_URL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"https?://[^\s)\]>"'`]+"#).unwrap());
static HASH_REF: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?:^|[^\w&/#\[])((?:(?:this |the )?(?:pull request|pr|issue) )?#(\d+))\b")
        .unwrap()
});
static MENTION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:^|[^\w`/@.])@([A-Za-z0-9][A-Za-z0-9-]*(?:\[bot\])?)").unwrap()
});
/// A link to the thread, taken out with the words that lead to it ("it's
/// pushed to", "in", "see").
static SELF_RUN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)[\s,;:-]*(?:\b(?:and|it's|it is|this is|is|are|was|now|pushed|posted|opened|up|see|in|on|to|at|into|via|under|here|over|from|for)\b[\s,]*)*\x{E000}[^\x{E001}]*\x{E001}",
    )
    .unwrap()
});

/// Opens what [`tidy`] marks as a link to the thread.
const SELF_OPEN: char = '\u{E000}';
/// Closes it.
const SELF_CLOSE: char = '\u{E001}';

/// Words that open a sentence that says the request back.
const ECHO: &[&str] = &[
    "you asked",
    "you've asked",
    "you have asked",
    "you requested",
    "you wanted",
    "you want ",
    "you'd like",
    "you would like",
    "you're asking",
    "you are asking",
    "as you asked",
    "as you requested",
    "as requested",
    "per your request",
    "your request",
    "your comment asks",
    "your comment asked",
];

/// A nod that says nothing ("Understood.", "Sure,"). "Got it." is not one
/// here: it opens CoderHelm's own ticket acknowledgement, which is how that
/// comment is told apart from a person's (`ticket_comments::ACK_OPENING`).
const NOD: &[&str] = &["understood", "sure", "will do", "on it", "noted"];

/// Whether `url` points at the thread a comment on `place` sits on: the
/// pull request or issue itself (any anchor on it), or the Jira ticket. A
/// pull request's files or a commit on it are code, not the thread.
pub fn links_here(url: &str, place: &Place) -> bool {
    let u = url
        .trim()
        .trim_end_matches(['.', ',', ';', ':', '!', '?'])
        .to_ascii_lowercase();
    match place {
        Place::Github { repo, number } => {
            let rest = u
                .strip_prefix("https://")
                .or_else(|| u.strip_prefix("http://"))
                .unwrap_or(&u);
            let Some((_, path)) = rest.split_once('/') else {
                return false;
            };
            let path = path
                .split(['#', '?'])
                .next()
                .unwrap_or("")
                .trim_end_matches('/');
            let repo = repo.to_ascii_lowercase();
            path == format!("{repo}/pull/{number}") || path == format!("{repo}/issues/{number}")
        }
        Place::Jira { ticket } => {
            let t = format!("/browse/{}", ticket.to_ascii_lowercase());
            u.find(&t).is_some_and(|at| {
                !u[at + t.len()..].starts_with(|c: char| c.is_ascii_alphanumeric() || c == '-')
            })
        }
    }
}

fn wrap(s: &str) -> String {
    format!("{SELF_OPEN}{s}{SELF_CLOSE}")
}

/// `f` applied to the parts of `s` outside the self markers.
fn outside_marks(s: &str, f: impl Fn(&str) -> String) -> String {
    let mut out = String::new();
    let mut rest = s;
    while let Some(at) = rest.find(SELF_OPEN) {
        out.push_str(&f(&rest[..at]));
        let end = rest[at..]
            .find(SELF_CLOSE)
            .map_or(rest.len(), |e| at + e + SELF_CLOSE.len_utf8());
        out.push_str(&rest[at..end]);
        rest = &rest[end..];
    }
    out.push_str(&f(rest));
    out
}

/// The line with every link to the thread wrapped in the self markers.
fn mark_self(line: &str, place: &Place) -> String {
    let out = MD_LINK
        .replace_all(line, |c: &regex::Captures| {
            if links_here(&c[2], place) {
                wrap(&c[1])
            } else {
                c[0].to_string()
            }
        })
        .to_string();
    let out = outside_marks(&out, |seg| {
        BARE_URL
            .replace_all(seg, |c: &regex::Captures| {
                let m = &c[0];
                let u = m.trim_end_matches(['.', ',', ';', ':', '!', '?']);
                if links_here(u, place) {
                    format!("{}{}", wrap(u), &m[u.len()..])
                } else {
                    m.to_string()
                }
            })
            .to_string()
    });
    match place {
        Place::Github { number, .. } => outside_marks(&out, |seg| {
            HASH_REF
                .replace_all(seg, |c: &regex::Captures| {
                    let whole = &c[0];
                    let r = c.get(1).map(|m| m.as_str()).unwrap_or("");
                    if c[2].parse::<u64>().ok() == Some(*number) {
                        whole.replacen(r, &wrap(r), 1)
                    } else {
                        whole.to_string()
                    }
                })
                .to_string()
        }),
        Place::Jira { .. } => out,
    }
}

/// A line's sentences, each with its end mark (a dot in a path, a link or
/// code stays; "1." opening a list item is not a sentence).
fn sentences(line: &str) -> Vec<String> {
    let chars: Vec<char> = line.chars().collect();
    let mut out = Vec::new();
    let mut cur = String::new();
    let (mut tick, mut paren) = (false, 0i32);
    for (i, &c) in chars.iter().enumerate() {
        cur.push(c);
        match c {
            '`' => tick = !tick,
            '(' | '[' if !tick => paren += 1,
            ')' | ']' if !tick => paren -= 1,
            _ => {}
        }
        let ends = !tick
            && paren <= 0
            && matches!(c, '.' | '!' | '?')
            && chars.get(i + 1).is_none_or(|n| n.is_whitespace());
        let list_number = cur
            .trim()
            .trim_end_matches('.')
            .chars()
            .all(|d| d.is_ascii_digit());
        if ends && !list_number {
            out.push(std::mem::take(&mut cur).trim().to_string());
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out.into_iter().filter(|s| !s.is_empty()).collect()
}

/// The sentence without the name it opens on ("@sam, ", "Sam: "), and how
/// long that name was.
fn after_address(s: &str) -> (&str, usize) {
    let t = s.trim_start();
    let first = t.split_whitespace().next().unwrap_or("");
    let addressed = first.starts_with('@')
        || (first.ends_with([',', ':'])
            && first.len() > 1
            && first.chars().next().is_some_and(char::is_uppercase));
    if addressed && t.len() > first.len() {
        let rest = t[first.len()..].trim_start_matches([',', ':']).trim_start();
        (rest, t.len() - rest.len())
    } else {
        (t, 0)
    }
}

fn capitalized(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

/// Whether a sentence is a header naming what it replies to ("@sam,
/// replying to your comment.", "I read kim's comment too.").
fn is_header(s: &str) -> bool {
    let (rest, _) = after_address(s);
    let l = rest.to_lowercase();
    l.starts_with("replying to")
        || l.contains(", replying to ")
        || l.starts_with("in reply to")
        || l.contains(" in reply to ")
        || (l.starts_with("i read ") && l.contains("comment"))
}

/// Whether a sentence opens by saying the request back.
fn echoes(s: &str) -> bool {
    let (rest, _) = after_address(s);
    let l = rest.to_lowercase();
    ECHO.iter().any(|e| l.starts_with(e))
}

/// Whether any sentence of `text` says a person's request back to them.
pub fn says_back(text: &str) -> bool {
    text.lines().flat_map(sentences).any(|s| echoes(&s))
}

/// The sentence without the request said back to the person: what follows
/// the echo ("you asked for a rebase: the branch ..." keeps "The branch
/// ..."), or nothing when it is all echo or a bare nod.
fn without_echo(s: &str) -> Option<String> {
    let (rest, _) = after_address(s);
    let l = rest.to_lowercase();
    let bare = l.trim_end_matches(['.', '!']);
    if NOD.contains(&bare) {
        return None;
    }
    if let Some(n) = NOD
        .iter()
        .find(|n| l.starts_with(*n) && l[n.len()..].starts_with([',', ':', ' ']))
    {
        if l[n.len()..].starts_with([',', ':']) {
            return Some(capitalized(rest.get(n.len() + 1..)?.trim_start()));
        }
    }
    if !echoes(s) {
        return Some(s.to_string());
    }
    let cut = [
        ": ",
        "; ",
        " - ",
        " \u{2014} ",
        ", so ",
        ", and ",
        ", which ",
        ", ",
    ]
    .iter()
    .filter_map(|sep| l.find(sep).map(|at| (at, sep.len())))
    .min_by_key(|(at, _)| *at)?;
    let after = rest.get(cut.0 + cut.1..)?.trim_start();
    let after = after
        .strip_prefix("so ")
        .or_else(|| after.strip_prefix("and "))
        .unwrap_or(after);
    let after = capitalized(after.trim());
    (after.split_whitespace().count() >= 2).then_some(after)
}

/// The sentence without its links to the thread (and the words leading to
/// them), or nothing when that leaves no sentence.
fn without_self(s: &str) -> Option<String> {
    let Some(at) = s.find(SELF_OPEN) else {
        return Some(s.to_string());
    };
    // A sentence that opens on the thread is about the thread.
    if s[..at]
        .trim_matches(|c: char| !c.is_alphanumeric())
        .is_empty()
    {
        return None;
    }
    let out = SELF_RUN.replace_all(s, "").to_string();
    let out = out.replace(" .", ".").replace(" ,", ",");
    let words = out
        .split_whitespace()
        .filter(|w| w.chars().any(char::is_alphanumeric))
        .count();
    (words >= 2 && !out.contains(SELF_OPEN)).then_some(out)
}

/// Whether byte `at` of `s` is inside an inline code span.
fn in_code(s: &str, at: usize) -> bool {
    s[..at].matches('`').count() % 2 == 1
}

/// The sentence with a person already mentioned named without the `@`
/// (or left out where it only addressed them).
fn one_mention(s: &str, seen: &mut std::collections::BTreeSet<String>) -> String {
    let mut out = String::new();
    let mut last = 0;
    for c in MENTION.captures_iter(s) {
        let m = c.get(1).expect("login");
        let at = m.start() - 1;
        if in_code(s, at) {
            continue;
        }
        let login = m.as_str().to_ascii_lowercase();
        if seen.insert(login) {
            continue;
        }
        out.push_str(&s[last..at]);
        if s[..at].trim().is_empty() {
            // It only addressed them: "@sam, done." is "Done."
            let rest = s[m.end()..].trim_start_matches([',', ':']).trim_start();
            out.push_str(&capitalized(rest));
            return out;
        }
        out.push_str(m.as_str());
        last = m.end();
    }
    out.push_str(&s[last..]);
    out
}

/// The comment with the thread rules applied (see the module docs): no
/// header, one @-mention of a person, no link to the thread it is posted
/// on, no request said back. Quotes, code and hidden markers stay as they
/// are.
pub fn tidy(body: &str, place: &Place) -> String {
    let mut seen = std::collections::BTreeSet::new();
    let mut out: Vec<String> = Vec::new();
    let mut fence = false;
    for line in body.lines() {
        let t = line.trim_start();
        if t.starts_with("```") {
            fence = !fence;
            out.push(line.to_string());
            continue;
        }
        if fence || t.starts_with('>') || t.starts_with("<!--") || t.is_empty() {
            out.push(line.to_string());
            continue;
        }
        let indent = &line[..line.len() - t.len()];
        let marked = mark_self(line, place);
        let mut kept: Vec<String> = Vec::new();
        for s in sentences(&marked) {
            if is_header(&s) {
                continue;
            }
            let Some(s) = without_echo(&s) else { continue };
            let Some(s) = without_self(&s) else { continue };
            let s = one_mention(&s, &mut seen);
            if !s.trim().is_empty() {
                kept.push(s);
            }
        }
        if kept.is_empty() {
            continue;
        }
        let joined = kept.join(" ");
        // Anything the rules didn't take out keeps its own words.
        let joined = joined.replace([SELF_OPEN, SELF_CLOSE], "");
        out.push(format!("{indent}{joined}"));
    }
    // No blank runs left behind, none at either end.
    let mut tidy: Vec<String> = Vec::new();
    for l in out {
        if l.trim().is_empty() && tidy.last().is_none_or(|p| p.trim().is_empty()) {
            continue;
        }
        tidy.push(l);
    }
    while tidy.last().is_some_and(|l| l.trim().is_empty()) {
        tidy.pop();
    }
    tidy.join("\n")
}

/// [`lint`] and the thread rules for a comment posted on `place`.
pub fn lint_on(text: &str, place: &Place) -> Vec<Problem> {
    let mut out = lint(text);
    let mut quoted = false;
    let mut header = false;
    let mut mentions: std::collections::BTreeMap<String, usize> = Default::default();
    let mut fence = false;
    for line in text.lines() {
        let t = line.trim_start();
        if t.starts_with("```") {
            fence = !fence;
            continue;
        }
        if fence || t.starts_with("<!--") {
            continue;
        }
        if t.starts_with('>') {
            quoted = true;
            continue;
        }
        for s in sentences(t) {
            if is_header(&s) {
                header = true;
            }
            if echoes(&s) {
                out.push(Problem::EchoesRequest(crate::conversation::cut_words(
                    &s, 80,
                )));
            }
        }
        for c in MENTION.captures_iter(t) {
            let m = c.get(1).expect("login");
            if !in_code(t, m.start() - 1) {
                *mentions.entry(m.as_str().to_ascii_lowercase()).or_default() += 1;
            }
        }
        let marked = mark_self(t, place);
        let mut rest = marked.as_str();
        while let Some(at) = rest.find(SELF_OPEN) {
            let end = rest[at..].find(SELF_CLOSE).map_or(rest.len(), |e| at + e);
            out.push(Problem::SelfLink(
                rest[at + SELF_OPEN.len_utf8()..end].to_string(),
            ));
            rest = &rest[(end + SELF_CLOSE.len_utf8()).min(rest.len())..];
        }
    }
    if header {
        out.push(Problem::ReplyHeader);
        if quoted {
            out.push(Problem::QuoteAndHeader);
        }
    }
    for (who, n) in mentions {
        if n > 1 {
            out.push(Problem::MentionTwice(who));
        }
    }
    out
}

/// What is posted on `place`: [`tidy`], then [`render_within`] in
/// `max_lines`, then [`lint_on`]. The text, or why it can't be posted.
pub fn for_thread(body: &str, place: &Place, max_lines: usize) -> Result<String, Vec<Problem>> {
    let text = render_within(&tidy(body, place), place.markdown(), max_lines);
    let problems = lint_on(&text, place);
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

    // ── The thread rules ──

    fn pr() -> Place {
        Place::github("acme", "web", 1547)
    }

    /// The reply the owner called insane, as the old layer and the model
    /// wrote it (names changed).
    const INSANE: &str = "@sam, replying to [your comment](https://github.com/acme/web/pull/1547#issuecomment-9).\n\n@sam you asked for a rebase with main: the branch is now up to date with main as a merge commit (15401ad), because force pushes aren't allowed here, so the result is the same code as a rebase. It's pushed to [pull request #1547](https://github.com/acme/web/pull/1547).";

    #[test]
    fn the_insane_reply_breaks_every_thread_rule() {
        let p = lint_on(INSANE, &pr());
        assert!(p.contains(&Problem::ReplyHeader), "{p:?}");
        assert!(p.contains(&Problem::MentionTwice("sam".into())), "{p:?}");
        assert!(p.iter().any(|x| matches!(x, Problem::SelfLink(_))), "{p:?}");
        assert!(
            p.iter().any(|x| matches!(x, Problem::EchoesRequest(_))),
            "{p:?}"
        );
        let quoted = format!("> @CoderHelm rebase with main\n\n{INSANE}");
        assert!(lint_on(&quoted, &pr()).contains(&Problem::QuoteAndHeader));
    }

    #[test]
    fn tidy_takes_out_the_header_the_echo_the_second_mention_and_the_self_link() {
        let out = tidy(INSANE, &pr());
        assert_eq!(
            out,
            "The branch is now up to date with main as a merge commit (15401ad), because force pushes aren't allowed here, so the result is the same code as a rebase."
        );
        assert!(
            lint_on(&out, &pr()).is_empty(),
            "{:?}",
            lint_on(&out, &pr())
        );
    }

    #[test]
    fn a_link_to_the_thread_goes_and_links_elsewhere_stay() {
        let here = pr();
        assert!(links_here("https://github.com/acme/web/pull/1547", &here));
        assert!(links_here(
            "https://github.com/ACME/web/pull/1547/#issuecomment-3",
            &here
        ));
        assert!(links_here("https://github.com/acme/web/issues/1547", &here));
        assert!(!links_here("https://github.com/acme/web/pull/1548", &here));
        assert!(!links_here(
            "https://github.com/acme/web/pull/1547/files",
            &here
        ));
        assert!(!links_here(
            "https://github.com/acme/web/commit/15401ad",
            &here
        ));
        let jira = Place::jira("ABC-12");
        assert!(links_here(
            "https://acme.atlassian.net/browse/ABC-12",
            &jira
        ));
        assert!(!links_here(
            "https://acme.atlassian.net/browse/ABC-123",
            &jira
        ));

        assert_eq!(
            tidy(
                "Fixed the null check in 15401ad, pushed to https://github.com/acme/web/pull/1547.",
                &here
            ),
            "Fixed the null check in 15401ad."
        );
        assert_eq!(tidy("Done in #1547.", &here), "");
        // The re-approval notice ends on the link: on the pull request it
        // goes whole, on a ticket it stays.
        let n = "Nothing was merged. https://github.com/acme/web/pull/1547";
        assert_eq!(tidy(n, &here), "Nothing was merged.");
        assert_eq!(tidy(n, &Place::jira("ABC-12")), n);
        assert_eq!(
            tidy(
                "Renamed the flag. See [#1547](https://github.com/acme/web/pull/1547).",
                &here
            ),
            "Renamed the flag."
        );
        // Another pull request, a commit and the run page stay.
        let other = "Same fix as #41: [15401ad](https://github.com/acme/web/commit/15401ad), run at https://app.example/runs/1.";
        assert_eq!(tidy(other, &here), other);
        // On the ticket, its own link goes; the pull request's stays.
        assert_eq!(
            tidy("Opened https://github.com/acme/web/pull/52 for https://acme.atlassian.net/browse/ABC-12.", &jira),
            "Opened https://github.com/acme/web/pull/52."
        );
    }

    #[test]
    fn a_person_is_mentioned_once_at_most() {
        assert_eq!(
            tidy("@kim, done. @kim the docs page too.", &pr()),
            "@kim, done. The docs page too."
        );
        assert_eq!(
            tidy("@kim and @lee need to approve; @kim first.", &pr()),
            "@kim and @lee need to approve; kim first."
        );
        // A mention in a quote, code or an address is left alone.
        let quoted = "> @kim wrote this\n\n@kim, done. Run `npm i @kim/pkg`.";
        assert_eq!(tidy(quoted, &pr()), quoted);
    }

    #[test]
    fn the_request_is_never_said_back() {
        assert_eq!(
            tidy(
                "You asked for a rebase, so I merged main in (15401ad).",
                &pr()
            ),
            "I merged main in (15401ad)."
        );
        assert_eq!(
            tidy("As requested, renamed the flag.", &pr()),
            "Renamed the flag."
        );
        assert_eq!(
            tidy("Understood. Renamed the flag.", &pr()),
            "Renamed the flag."
        );
        assert_eq!(tidy("You want the docs updated.", &pr()), "");
        // CoderHelm's own ticket acknowledgement keeps the words it is
        // recognized by.
        let ack = crate::ticket_comments::ack_reply();
        assert_eq!(tidy(&ack, &Place::jira("ABC-1")), ack);
        assert!(says_back("Kim, you asked for the docs page too."));
        assert!(!says_back("Updated the docs page."));
    }

    #[test]
    fn a_header_goes_whatever_wrote_it() {
        for h in [
            "@sam, replying to your comment.",
            "Sam Lee, replying to your comment (\"Use staging.\").",
            "I read kim's comment too.",
        ] {
            assert_eq!(
                tidy(&format!("{h} Switched to staging."), &pr()),
                "Switched to staging.",
                "{h}"
            );
        }
    }

    #[test]
    fn plain_comments_pass_unchanged() {
        for c in [
            "Merged main in (15401ad).",
            "Merged main in (15401ad). Force pushes are off here, so it's a merge instead of a rebase.",
            "**Needed from you:** @owner, approve the production deploy: the team requires an approval.",
            "1. Approve the deploy.\n2. Re-run the job.",
            "<!-- coderhelm:merge-status -->\n**Auto-merge** \u{b7} waiting",
        ] {
            assert_eq!(tidy(c, &pr()), c);
            assert!(lint_on(c, &pr()).is_empty(), "{c}: {:?}", lint_on(c, &pr()));
        }
    }
}
