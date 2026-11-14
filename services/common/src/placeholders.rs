//! Placeholder tokens: text that stands in for a real value and must never
//! be learned, or written into a commit, a title or a description.
//!
//! A dependency pull request's commits were prefixed `KEY-1:`. No ticket
//! had that key. The line a review fix round writes into the description to
//! say how it chose its commit message described a ticket-key convention
//! with the example `KEY-1: `; the description is read back by the code
//! that learns from what happened to a pull request, and nothing stopped an
//! example from being kept as a rule.
//!
//! [`found`] names every placeholder in a text: a sample ticket key (a
//! stand-in project such as `KEY` or `ABC`, a stand-in number such as
//! `XXXX` or `0000`, or a `-1` or `-123` key that is not a real one),
//! `XXXX`, `<summary>`, `{ticket}`, `TODO`. A lesson or learned convention
//! that holds one is rejected when learned and removed when found
//! (`rewards::lessons::checkable`, `reviewer_prefs::clean`,
//! `worker::passes::lesson_cleanup`).

use std::sync::OnceLock;

use regex::Regex;

fn key_shape() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"\b([A-Z][A-Z0-9]{1,9})-(\d+|[XxNn#]{2,})\b").expect("valid"))
}

fn marks() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| {
        Regex::new(r"(?i)\b(X{4,}|TODO|TBD|FIXME)\b|<([A-Za-z][A-Za-z _-]{1,30})>|\{([a-z][a-z_-]{1,24})\}")
            .expect("valid")
    })
}

/// Names shaped like a key that are not tickets.
const NOT_PROJECTS: &[&str] = &[
    "UTF", "SHA", "ISO", "HTTP", "CVE", "GHSA", "RFC", "TLS", "SSL", "AES", "MD", "IPV", "ES",
    "RSA", "HS", "RS", "PEP", "BCP", "CWE", "ECMA", "IEEE", "COVID", "BASE", "UTC", "GMT", "CP",
    "KOI", "EUC", "GB", "US", "EU", "AP", "CA", "SA", "ME", "AF", "IL",
];

/// HTML that is not a placeholder.
const TAGS: &[&str] = &[
    "details",
    "sub",
    "sup",
    "br",
    "b",
    "i",
    "em",
    "strong",
    "code",
    "pre",
    "a",
    "p",
    "div",
    "span",
    "img",
    "kbd",
    "ul",
    "ol",
    "li",
    "table",
    "tr",
    "td",
    "th",
    "hr",
    "h1",
    "h2",
    "h3",
    "h4",
    "u",
    "s",
    "del",
    "ins",
    "q",
    "blockquote",
];

/// Every placeholder token in `text`. `real` are keys known to exist (the
/// run's own ticket, keys found in the tracker): never placeholders.
pub fn found(text: &str, real: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |t: &str| {
        if !out.iter().any(|x: &String| x == t) {
            out.push(t.to_string());
        }
    };
    for c in key_shape().captures_iter(text) {
        let whole = c.get(0).map(|m| m.as_str()).unwrap_or("");
        let (project, number) = (&c[1], &c[2]);
        if NOT_PROJECTS.contains(&project) || real.iter().any(|k| k.eq_ignore_ascii_case(whole)) {
            continue;
        }
        let sample_number = matches!(number, "1" | "123");
        if crate::git_conventions::is_stand_in_key(whole) || sample_number {
            push(whole);
        }
    }
    for c in marks().captures_iter(text) {
        if let Some(m) = c.get(1) {
            // Already named as part of a sample key.
            let within = text[..m.start()].ends_with('-');
            if !within {
                push(m.as_str());
            }
        } else if let Some(m) = c.get(2) {
            let inner = m.as_str().trim().to_ascii_lowercase();
            let whole = c.get(0).expect("the match");
            // A type parameter (`Vec<String>`) or real HTML is not one.
            let generic = text[..whole.start()]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_alphanumeric() || c == '_');
            let html = TAGS.contains(&inner.split(' ').next().unwrap_or(""))
                || text.to_ascii_lowercase().contains(&format!("</{inner}>"));
            if !generic && !html {
                push(whole.as_str());
            }
        } else if let Some(m) = c.get(3) {
            if crate::git_conventions::is_stand_in_word(m.as_str())
                || crate::git_conventions::TOKENS.contains(&m.as_str())
            {
                push(c.get(0).map(|m| m.as_str()).unwrap_or(""));
            }
        }
    }
    out
}

/// True when `text` holds a placeholder.
pub fn has(text: &str, real: &[String]) -> bool {
    !found(text, real).is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_keys_and_template_marks_are_found() {
        assert_eq!(
            found("Prefix every commit subject with `KEY-1: `.", &[]),
            vec!["KEY-1"]
        );
        assert_eq!(
            found("Title: ABC-123: <summary>", &[]),
            vec!["ABC-123", "<summary>"]
        );
        assert_eq!(found("Use OPS-XXXX in the subject", &[]), vec!["OPS-XXXX"]);
        assert_eq!(found("{ticket}: {title}", &[]), vec!["{ticket}", "{title}"]);
        assert_eq!(found("TODO: say why", &[]), vec!["TODO"]);
        assert_eq!(found("ticket XXXX", &[]), vec!["XXXX"]);
    }

    #[test]
    fn real_keys_and_ordinary_text_are_not() {
        // The first ticket of a real project exists.
        assert!(found("OPS-1: Lower the log level", &["OPS-1".to_string()]).is_empty());
        assert!(found("OPS-482: Lower the log level", &[]).is_empty());
        for t in [
            "Encode as UTF-8 and hash with SHA-1.",
            "Fixes CVE-2026-1 in `glob-walk`.",
            "<details><summary>All rows</summary>",
            "Call `walk({ depth })` with `Vec<String>`.",
            "Return early when `n == 0`.",
        ] {
            assert!(found(t, &[]).is_empty(), "{t}: {:?}", found(t, &[]));
        }
        // Not real: nothing says the key exists.
        assert_eq!(found("OPS-1: x", &[]), vec!["OPS-1"]);
    }
}
