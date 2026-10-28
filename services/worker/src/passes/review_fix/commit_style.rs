//! A review fix round is one commit, and its message reads like the
//! repository's own: learned from the base branch's recent history (a
//! ticket-key prefix, a conventional-commit type, one line or a body,
//! co-author lines or none). The team's commit template, when it set one,
//! comes first.

use std::sync::OnceLock;

use regex::Regex;

/// Fewer messages than this teach nothing; the team's conventions decide.
pub const MIN_SAMPLES: usize = 5;

/// How the repository writes commit messages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Style {
    /// Subjects start with a tracker key; the text between the key and the
    /// subject (`": "`, `" "`, `"] "` for `[KEY] `).
    pub key_prefix: Option<KeyPrefix>,
    /// Subjects start with a conventional-commit type (`fix: `).
    pub conventional: bool,
    /// Messages are a subject only.
    pub single_line: bool,
    /// Messages carry `Co-authored-by:` lines.
    pub co_authors: bool,
    /// How many messages it was learned from.
    pub samples: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyPrefix {
    /// `KEY-1: subject`
    Colon,
    /// `KEY-1 subject`
    Space,
    /// `[KEY-1] subject`
    Bracket,
}

fn key_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"^(\[)?([A-Z][A-Z0-9]*-\d+)(\])?(:)?\s").unwrap())
}

fn conventional_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| {
        Regex::new(
            r"^(feat|fix|chore|docs|refactor|test|tests|perf|build|ci|style|revert)(\([^)]*\))?!?: ",
        )
        .unwrap()
    })
}

/// Whether a commit is a merge or an automated one, which teaches nothing
/// about how people write.
fn is_noise(message: &str) -> bool {
    let s = message.trim_start();
    s.starts_with("Merge pull request")
        || s.starts_with("Merge branch")
        || s.starts_with("Merge remote-tracking")
        || s.starts_with("Revert \"")
        || s.to_ascii_lowercase().starts_with("bump ")
}

/// Learns the style from recent commit messages (newest first).
pub fn learn(messages: &[String]) -> Style {
    let msgs: Vec<&str> = messages
        .iter()
        .map(|m| m.trim())
        .filter(|m| !m.is_empty() && !is_noise(m))
        .collect();
    let n = msgs.len();
    if n == 0 {
        return Style::default();
    }
    let share = |k: usize| k * 100 / n;
    let mut colon = 0;
    let mut space = 0;
    let mut bracket = 0;
    for m in &msgs {
        if let Some(c) = key_re().captures(m) {
            if c.get(1).is_some() && c.get(3).is_some() {
                bracket += 1;
            } else if c.get(4).is_some() {
                colon += 1;
            } else if c.get(1).is_none() && c.get(3).is_none() {
                space += 1;
            }
        }
    }
    let keyed = colon + space + bracket;
    let key_prefix = (share(keyed) >= 50).then_some({
        if bracket >= colon && bracket >= space {
            KeyPrefix::Bracket
        } else if colon >= space {
            KeyPrefix::Colon
        } else {
            KeyPrefix::Space
        }
    });
    let conventional = share(
        msgs.iter()
            .filter(|m| conventional_re().is_match(m))
            .count(),
    ) >= 50;
    let with_body = msgs
        .iter()
        .filter(|m| {
            m.lines()
                .skip(1)
                .any(|l| !l.trim().is_empty() && !is_trailer(l))
        })
        .count();
    let co = msgs
        .iter()
        .filter(|m| m.to_ascii_lowercase().contains("co-authored-by:"))
        .count();
    Style {
        key_prefix,
        conventional,
        single_line: share(n - with_body) >= 70,
        co_authors: share(co) >= 30,
        samples: n,
    }
}

/// A `Key: value` trailer line (`Signed-off-by:`, `Co-authored-by:`).
fn is_trailer(line: &str) -> bool {
    let l = line.trim().to_ascii_lowercase();
    l.starts_with("co-authored-by:")
        || l.starts_with("signed-off-by:")
        || l.starts_with("reviewed-by:")
        || l.starts_with("change-id:")
}

/// The subject without any key or type the agent already put in front.
fn bare(subject: &str) -> String {
    let mut s = subject.trim().to_string();
    if let Some(c) = key_re().captures(&s) {
        s = s[c.get(0).unwrap().end()..].trim_start().to_string();
    }
    if let Some(c) = conventional_re().captures(&s) {
        s = s[c.get(0).unwrap().end()..].trim_start().to_string();
    }
    s
}

/// The one commit message of a round, in the repository's style, from the
/// agent's messages (first is the subject, the rest a body when the
/// repository writes bodies). `key` is the run's tracker key, if any.
pub fn compose(style: &Style, key: Option<&str>, agent_messages: &[String]) -> String {
    let mut subjects: Vec<String> = agent_messages
        .iter()
        .map(|m| bare(m.lines().next().unwrap_or("")))
        .filter(|s| !s.is_empty())
        .collect();
    subjects.dedup();
    let first = subjects
        .first()
        .cloned()
        .unwrap_or_else(|| "Address review comments".to_string());
    let mut subject = first;
    if style.conventional {
        let lower = subject
            .chars()
            .next()
            .map(|c| c.to_lowercase().collect::<String>() + &subject[c.len_utf8()..])
            .unwrap_or_default();
        subject = format!("fix: {lower}");
    }
    if let (Some(prefix), Some(k)) = (&style.key_prefix, key) {
        subject = match prefix {
            KeyPrefix::Colon => format!("{k}: {subject}"),
            KeyPrefix::Space => format!("{k} {subject}"),
            KeyPrefix::Bracket => format!("[{k}] {subject}"),
        };
    }
    if style.single_line || subjects.len() < 2 {
        return subject;
    }
    let body: Vec<String> = subjects[1..].iter().map(|s| format!("- {s}")).collect();
    format!("{subject}\n\n{}", body.join("\n"))
}

/// The message without co-author lines, unless the repository uses them.
pub fn without_co_authors(style: &Style, message: &str) -> String {
    if style.co_authors {
        return message.to_string();
    }
    let kept: Vec<&str> = message
        .lines()
        .filter(|l| !l.trim().to_ascii_lowercase().starts_with("co-authored-by:"))
        .collect();
    kept.join("\n").trim_end().to_string()
}

/// One line for the evidence: what the style was learned from.
pub fn describe(style: &Style) -> String {
    if style.samples < MIN_SAMPLES {
        return "too few recent commits to learn from; the team's commit convention".to_string();
    }
    let mut parts = Vec::new();
    match style.key_prefix {
        Some(KeyPrefix::Colon) => parts.push("ticket key prefix `KEY-1: `"),
        Some(KeyPrefix::Space) => parts.push("ticket key prefix `KEY-1 `"),
        Some(KeyPrefix::Bracket) => parts.push("ticket key prefix `[KEY-1] `"),
        None => parts.push("no ticket key prefix"),
    }
    if style.conventional {
        parts.push("conventional type");
    }
    parts.push(if style.single_line {
        "single line"
    } else {
        "subject and body"
    });
    if !style.co_authors {
        parts.push("no co-author lines");
    }
    format!(
        "{} (learned from the last {} commits)",
        parts.join(", "),
        style.samples
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn a_ticket_key_single_line_history_gives_a_keyed_single_line() {
        let style = learn(&s(&[
            "OPS-12: Lower the sync log level",
            "OPS-11: Add retry to the poller",
            "Merge pull request #9 from org/branch",
            "OPS-10: Fix typo",
            "OPS-9: Bump client",
            "OPS-8: Handle empty pages",
        ]));
        assert_eq!(style.key_prefix, Some(KeyPrefix::Colon));
        assert!(style.single_line);
        assert!(!style.co_authors);
        assert_eq!(style.samples, 5);
        let m = compose(
            &style,
            Some("OPS-13"),
            &s(&["OPS-13: Log unchanged runs at DEBUG", "Add a caplog test"]),
        );
        assert_eq!(m, "OPS-13: Log unchanged runs at DEBUG");
    }

    #[test]
    fn bracketed_keys_and_conventional_types_are_followed() {
        let style = learn(&s(&[
            "[AB-1] fix: a",
            "[AB-2] feat: b",
            "[AB-3] fix: c",
            "[AB-4] chore: d",
            "[AB-5] fix: e",
        ]));
        assert_eq!(style.key_prefix, Some(KeyPrefix::Bracket));
        // The type comes after the bracket, so the subject does not start with one.
        assert!(!style.conventional);
        let conv = learn(&s(&[
            "fix: a",
            "feat: b",
            "fix(api): c",
            "docs: d",
            "chore: e",
        ]));
        assert!(conv.conventional);
        assert_eq!(conv.key_prefix, None);
        assert_eq!(
            compose(&conv, Some("AB-9"), &s(&["Keep the alarm's log text"])),
            "fix: keep the alarm's log text"
        );
    }

    #[test]
    fn a_history_with_bodies_gets_a_body_and_co_authors_only_when_used() {
        let style = learn(&s(&[
            "Add x\n\nWhy x.\n\nCo-authored-by: A <a@example.com>",
            "Add y\n\nWhy y.",
            "Add z\n\nWhy z.\n\nCo-authored-by: B <b@example.com>",
            "Add w\n\nWhy w.",
            "Add v\n\nWhy v.",
        ]));
        assert!(!style.single_line);
        assert!(style.co_authors);
        let m = compose(&style, None, &s(&["Change one", "Change two"]));
        assert_eq!(m, "Change one\n\n- Change two");
        let plain = Style::default();
        assert_eq!(
            without_co_authors(&plain, "Fix\n\nCo-authored-by: Bot <b@example.com>"),
            "Fix"
        );
        assert!(
            without_co_authors(&style, "Fix\n\nCo-authored-by: X <x@example.com>")
                .contains("Co-authored-by")
        );
    }

    #[test]
    fn no_key_is_invented_and_an_agent_key_is_not_doubled() {
        let style = learn(&s(&["K-1: a", "K-2: b", "K-3: c", "K-4: d", "K-5: e"]));
        assert_eq!(compose(&style, None, &s(&["K-9: Fix"])), "Fix");
        assert_eq!(compose(&style, Some("K-9"), &s(&["K-9: Fix"])), "K-9: Fix");
        assert_eq!(
            compose(&style, Some("K-9"), &[]),
            "K-9: Address review comments"
        );
    }

    #[test]
    fn the_description_says_what_was_learned() {
        let style = learn(&s(&["K-1: a", "K-2: b", "K-3: c", "K-4: d", "K-5: e"]));
        assert_eq!(
            describe(&style),
            "ticket key prefix `KEY-1: `, single line, no co-author lines (learned from the last 5 commits)"
        );
        assert!(describe(&Style::default()).starts_with("too few"));
    }
}
