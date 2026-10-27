//! How an example reads: a one-line title, a short cleaned summary and the
//! reason it is an example.
//!
//! Examples are made from pull request bodies and review comments, which
//! carry markup meant for GitHub: HTML comments (`<!-- coderhelm:... -->`),
//! callout markers (`> [!WARNING]`), section labels (`**Summary**`) and
//! boilerplate lines (`Source ticket: X`, `Closes #4`). [`summary`] keeps the
//! markdown a reader wants (lists, code, emphasis) and drops the rest;
//! [`plain`] drops all markup, for titles, search and prompts. Examples kept
//! before titles were stored are read through [`legacy_split`], so old and
//! new ones look the same.

use regex::Regex;
use std::sync::LazyLock;

/// Longest title, in characters.
pub const MAX_TITLE: usize = 120;
/// Longest summary kept with an example, in characters.
pub const MAX_SUMMARY: usize = 600;

static UNVERIFIED_BLOCK: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?s)<!--\s*coderhelm:unverified\s*-->.*?(<!--\s*/coderhelm:unverified\s*-->|\z)")
        .unwrap()
});
static COMMENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?s)<!--.*?(-->|\z)").unwrap());
static TAG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"</?[A-Za-z][A-Za-z0-9-]*(\s[^<>]*)?/?>").unwrap());
static CALLOUT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^\s*>\s*\[!(note|tip|important|warning|caution)\]\s*$").unwrap()
});
static BOILERPLATE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)^\s*(source ticket\s*:|(closes|fixes|resolves|refs?|related to)\s*:?\s*([\w.-]+/[\w.-]+)?#\d+\s*$|(closes|fixes|resolves)\s*:?\s*[A-Z][A-Z0-9]+-\d+\s*$)",
    )
    .unwrap()
});
/// A section label on its own (`**Summary**`, `## Changes`, `Risk:`).
static LABEL_LINE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^\s*(#{1,6}\s*)?(\*\*|__)?(summary|problem|changes?|risk|verification|why|what|context|notes?|details?|open items|testing|tests?)(\*\*|__)?\s*:?\s*$").unwrap()
});
/// A section label leading its text (`**Summary** x`, `Problem: x`).
static LABEL_LEAD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^\s*(\*\*|__)(summary|problem|changes?|risk|verification|why|what|context|notes?)(:)?(\*\*|__)\s*:?\s*").unwrap()
});
static HEADING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\s*#{1,6}\s+").unwrap());
static QUOTE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\s*>\s?").unwrap());
static BULLET: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s*([-*+]|\d+[.)])\s+(\[[ xX]\]\s+)?").unwrap());
static IMAGE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"!\[([^\]]*)\]\([^)]*\)").unwrap());
static LINK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\[([^\]]+)\]\([^)]*\)").unwrap());
static STRONG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(\*\*|__)([^*_]+?)(\*\*|__)").unwrap());
static EM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(^|[\s(])\*([^*\s][^*]*?)\*([\s).,;:!?]|$)").unwrap());
static CODE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"`+([^`]*)`+").unwrap());
static SPACES: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[ \t]+").unwrap());
static BLANKS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\n{3,}").unwrap());
/// A conventional-commit prefix (`fix`, `feat(api)!`), never a title on its own.
static COMMIT_TYPE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z]+(\([^)]*\))?!?$").unwrap());

/// Markdown with GitHub-only markup and boilerplate removed. Lists, code,
/// emphasis and headings stay; HTML comments and tags, callout markers and
/// `Source ticket:`/`Closes #N` lines go.
pub fn clean(md: &str) -> String {
    let md = md.replace("\r\n", "\n");
    let md = UNVERIFIED_BLOCK.replace_all(&md, "");
    let md = COMMENT.replace_all(&md, "");
    let md = TAG.replace_all(&md, "");
    let mut out: Vec<String> = vec![];
    for line in md.lines() {
        if CALLOUT.is_match(line) || BOILERPLATE.is_match(line) {
            continue;
        }
        let line = line.trim_end();
        // A quote line left empty by a removed marker.
        if QUOTE.is_match(line) && QUOTE.replace(line, "").trim().is_empty() {
            continue;
        }
        out.push(line.to_string());
    }
    BLANKS
        .replace_all(out.join("\n").trim(), "\n\n")
        .into_owned()
}

/// One line of text with every mark removed.
pub fn plain(md: &str) -> String {
    let mut lines: Vec<String> = vec![];
    let mut fenced = false;
    for line in clean(md).lines() {
        if line.trim_start().starts_with("```") || line.trim_start().starts_with("~~~") {
            fenced = !fenced;
            continue;
        }
        if fenced || LABEL_LINE.is_match(line) {
            continue;
        }
        let l = HEADING.replace(line, "");
        let l = QUOTE.replace(&l, "");
        let l = BULLET.replace(&l, "");
        let l = LABEL_LEAD.replace(&l, "");
        let l = IMAGE.replace_all(&l, "$1");
        let l = LINK.replace_all(&l, "$1");
        let l = STRONG.replace_all(&l, "$2");
        let l = EM.replace_all(&l, "$1$2$3");
        let l = CODE.replace_all(&l, "$1");
        let l = l.trim();
        if !l.is_empty() {
            lines.push(l.to_string());
        }
    }
    SPACES.replace_all(&lines.join(" "), " ").trim().to_string()
}

/// `s` cut to at most `max` characters at a word, with "…" when cut.
pub fn cut_words(s: &str, max: usize) -> String {
    let s = s.trim();
    if s.chars().count() <= max {
        return s.to_string();
    }
    let hard: String = s.chars().take(max.saturating_sub(1)).collect();
    let at = hard
        .rfind(char::is_whitespace)
        .filter(|&i| i >= hard.len() / 2)
        .unwrap_or(hard.len());
    format!(
        "{}…",
        hard[..at].trim_end_matches(|c: char| c.is_whitespace() || ",;:-".contains(c))
    )
}

/// The first sentence worth reading, as a title.
pub fn first_sentence(md: &str) -> String {
    let p = plain(md);
    let end = p
        .char_indices()
        .find(|&(i, c)| {
            matches!(c, '.' | '?' | '!') && p[i + c.len_utf8()..].starts_with(char::is_whitespace)
        })
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(p.len());
    cut_words(p[..end].trim_end_matches('.'), MAX_TITLE)
}

/// A title from a pull request title or a review comment's heading; when
/// that is empty, the body's first sentence.
pub fn title(raw_title: &str, body: &str) -> String {
    let t = plain(raw_title);
    if t.is_empty() {
        first_sentence(body)
    } else {
        cut_words(&t, MAX_TITLE)
    }
}

/// The cleaned markdown, cut at a word to [`MAX_SUMMARY`] characters, with
/// an open code fence closed.
pub fn summary(body: &str) -> String {
    summary_of(&clean(body), false)
}

fn summary_of(cleaned: &str, was_cut: bool) -> String {
    let mut s = cleaned.to_string();
    // Leading section labels add nothing in a summary of a few lines.
    while let Some(first) = s.lines().next() {
        if LABEL_LINE.is_match(first) {
            s = s[first.len()..].trim_start().to_string();
        } else {
            break;
        }
    }
    let s = LABEL_LEAD.replace(&s, "").into_owned();
    let mut out = if was_cut {
        // The stored text ended mid-word: drop the broken word.
        let at = s.rfind(char::is_whitespace).unwrap_or(s.len());
        format!("{}…", s[..at].trim_end())
    } else {
        cut_words(&s, MAX_SUMMARY)
    };
    if out
        .lines()
        .filter(|l| l.trim_start().starts_with("```"))
        .count()
        % 2
        == 1
    {
        out.push_str("\n```");
    }
    out
}

/// Splits an example's stored text, kept as `title: body` before titles
/// were stored, into a title and a cleaned summary. `body_cap` is the length
/// the body was cut to when stored (and [`super::exemplars::MAX_TEXT`] the
/// whole text's), so a body cut mid-word ends in "…".
pub fn legacy_split(text: &str, body_cap: usize) -> (String, String) {
    let near = |len: usize, cap: usize| len + 8 >= cap && len <= cap;
    let whole_cut = near(text.len(), super::exemplars::MAX_TEXT);
    let mut from = 0;
    let mut split = None;
    while let Some(i) = text[from..].find(": ") {
        let at = from + i;
        let head = text[..at].trim();
        if COMMIT_TYPE.is_match(head) || (head.contains('(') && !head.contains(')')) {
            from = at + 2;
            continue;
        }
        if head.chars().count() <= 160 && !head.contains('\n') {
            split = Some(at);
        }
        break;
    }
    let (t, s) = match split {
        Some(at) => {
            let body = &text[at + 2..];
            (
                title(&text[..at], body),
                summary_of(&clean(body), whole_cut || near(body.len(), body_cap)),
            )
        }
        None => (
            first_sentence(text),
            summary_of(&clean(text), whole_cut || near(text.len(), body_cap)),
        ),
    };
    // A summary that only repeats the title adds nothing.
    let s = if plain(&s).trim_end_matches('.') == t {
        String::new()
    } else {
        s
    };
    (t, s)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PR_BODY: &str = "Source ticket: OPS-1\n\n<!-- coderhelm:unverified -->\n> [!WARNING]\n> **Not yet verified**\n> - **Waiting on CI:** lint <!-- coderhelm:ci -->\n<!-- /coderhelm:unverified -->\n\n**Summary** Retry the import once with the same key.\n\n## Changes\n- Add `retry_once` to the client\n- Keep the key\n\nCloses #4\n<details><summary>Logs</summary>ok</details>";

    #[test]
    fn clean_drops_comments_callouts_and_boilerplate() {
        let c = clean(PR_BODY);
        assert!(!c.contains("<!--"), "{c}");
        assert!(!c.contains("[!WARNING]"), "{c}");
        assert!(!c.contains("Not yet verified"), "{c}");
        assert!(!c.contains("Source ticket"), "{c}");
        assert!(!c.contains("Closes #4"), "{c}");
        assert!(!c.contains("<details>"), "{c}");
        assert!(c.contains("- Add `retry_once` to the client"), "{c}");
        assert!(c.contains("## Changes"), "{c}");
    }

    #[test]
    fn plain_has_no_marks() {
        let p = plain(PR_BODY);
        assert!(
            p.starts_with("Retry the import once with the same key."),
            "{p}"
        );
        assert!(
            !p.contains("**") && !p.contains('`') && !p.contains('#'),
            "{p}"
        );
        assert!(!p.contains("Changes"), "{p}");
        assert_eq!(
            plain("See [the docs](https://x.y) and *this*."),
            "See the docs and this."
        );
        assert_eq!(plain("keep snake_case_names"), "keep snake_case_names");
    }

    #[test]
    fn titles_are_one_clean_line() {
        assert_eq!(
            title("**Missing test** for `retry`", ""),
            "Missing test for retry"
        );
        assert_eq!(
            title("", PR_BODY),
            "Retry the import once with the same key"
        );
        let long = "word ".repeat(60);
        let t = title(&long, "");
        assert!(t.chars().count() <= MAX_TITLE && t.ends_with('…'), "{t}");
        assert!(!t.contains("wor…"), "cut at a word: {t}");
    }

    #[test]
    fn summaries_are_short_markdown() {
        let s = summary(PR_BODY);
        assert!(s.starts_with("Retry the import once"), "{s}");
        assert!(s.contains("- Add `retry_once`"), "{s}");
        let long = format!("```\n{}", "code line\n".repeat(200));
        let s = summary(&long);
        assert!(s.chars().count() <= MAX_SUMMARY + 4);
        assert!(s.ends_with("```"), "an open fence is closed: {s}");
    }

    #[test]
    fn legacy_text_splits_into_title_and_summary() {
        let text = format!("fix(api): retry payments: {PR_BODY}");
        let (t, s) = legacy_split(&text, 700);
        assert_eq!(t, "fix(api): retry payments");
        assert!(s.starts_with("Retry the import once"), "{s}");
        assert!(!s.contains("<!--"));

        // Cut mid-word when stored: the broken word goes.
        let cut = format!(
            "Missing test: {}",
            "the retry path is never exercised ".repeat(20)
        );
        let cut = &cut[.."Missing test: ".len() + 400];
        let (t, s) = legacy_split(cut, 400);
        assert_eq!(t, "Missing test");
        assert!(s.ends_with('…'), "{s}");
        let last = s.trim_end_matches('…').split_whitespace().last().unwrap();
        assert!(
            ["the", "retry", "path", "is", "never", "exercised"].contains(&last),
            "{s}"
        );

        // No title prefix: the first sentence is the title.
        let (t, s) = legacy_split(
            "Asked to rename a variable. The team names it this way.",
            700,
        );
        assert_eq!(t, "Asked to rename a variable");
        assert!(s.contains("The team names it"));
        let (t, s) = legacy_split("text bad", 700);
        assert_eq!((t.as_str(), s.as_str()), ("text bad", ""));
    }
}
