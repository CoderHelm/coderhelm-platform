//! The last read of a description before it is saved, as its reader would
//! read it.
//!
//! Many code paths each append a section and none of them reads the result.
//! This step does, once, with the facts and the assembled text in hand. It
//! asks a model the questions a reviewer or ticket owner asks first ("what
//! is this, how do I check it") and lets it name written sections that tell
//! a reader nothing. It can be fluent; it cannot invent:
//!
//! - it only fills two slots of the lead, the summary and the check. The
//!   risk, what is needed from the reader, the anomalies, every table, count,
//!   check result and step are rendered by code from the facts and never
//!   pass through the model;
//! - [`accept`] takes a reply only as far as it can be verified: the summary
//!   is run through the same fact check as every other sentence
//!   (`pr_claims`), may name only code and numbers that the description or
//!   the facts already hold, and carries no raw machinery; the check must be
//!   a command the description already gives, word for word;
//! - a section it asks to drop is dropped only when a person or a model
//!   wrote it: nothing CoderHelm renders from records, and never the
//!   explanation of the change or its risk;
//! - when the reply fails any of that, or there is no model, the slots fall
//!   back to what the description already said ([`Slots::from_body`]) and
//!   then to the records.

use serde::Deserialize;

use crate::pr_facts::{Facts, LEAD_END, LEAD_START};

/// The longest summary taken, in characters.
pub const MAX_SUMMARY_CHARS: usize = 260;
/// The longest check taken.
pub const MAX_CHECK_CHARS: usize = 160;

/// Sections a reply may never drop.
const KEPT_SECTIONS: &[&str] = &["changes", "change", "risk", "open items"];

/// What the reader pass fills.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Slots {
    /// What the change is, in one or two plain sentences.
    pub summary: String,
    /// The one command or step that checks it.
    pub check: String,
    /// Written sections to drop, by heading.
    pub drop: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct Reply {
    #[serde(default)]
    summary: String,
    #[serde(default)]
    check: String,
    #[serde(default)]
    drop: Vec<String>,
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The system prompt.
pub const SYSTEM: &str = "You read a pull request description the way its reviewer will, before it is saved. Answer only from the text and facts you are given. Plain words, short sentences, no internal tool names, no raw errors, no identifiers the reader does not need.";

/// What the model is asked: the facts in brief, the written text, and the
/// exact shape of the answer.
pub fn prompt(facts: &Facts, written: &str) -> String {
    let files: Vec<&str> = facts
        .files
        .iter()
        .take(40)
        .map(|f| f.path.as_str())
        .collect();
    let checks: Vec<String> = facts
        .checks
        .iter()
        .map(|c| format!("{} ({:?})", c.name, c.outcome))
        .collect();
    format!(
        "## Recorded facts (true; never contradict them)\n\
         - Ticket or finding: {why}\n\
         - Files changed ({n}): {files}\n\
         - Checks recorded on the head: {checks}\n\n\
         ## The description as written\n{written}\n\n\
         ## Answer\n\
         Return ONLY a JSON object:\n\
         {{\"summary\": \"what this change does, in one or two short plain sentences a reviewer can read alone; no ticket restated, no rationale\",\n \
         \"check\": \"the one command from the description that checks the change, copied exactly, or empty when it gives none\",\n \
         \"drop\": [\"the heading of each written section that tells a reviewer nothing they need (an empty template section, background that restates the ticket); never Changes or Risk\"]}}\n\
         Use only names, numbers and commands that appear above.",
        why = facts
            .why
            .as_ref()
            .map(|w| format!("{} {}", w.reference, w.title))
            .unwrap_or_else(|| "none".to_string()),
        n = facts.files.len(),
        files = files.join(", "),
        checks = if checks.is_empty() {
            "none".to_string()
        } else {
            checks.join(", ")
        },
    )
}

fn json_of(reply: &str) -> Option<Reply> {
    let a = reply.find('{')?;
    let b = reply.rfind('}')?;
    serde_json::from_str(reply.get(a..=b)?).ok()
}

/// Whether every backticked name and every number of `text` is in `known`
/// (the description as written, and the facts).
fn only_known(text: &str, known: &str) -> bool {
    let mut ticks = text.split('`');
    ticks.next();
    while let Some(inside) = ticks.next() {
        if !inside.trim().is_empty() && !known.contains(inside.trim()) {
            return false;
        }
        ticks.next();
    }
    text.split(|c: char| !c.is_ascii_digit())
        .filter(|n| !n.is_empty())
        .all(|n| known.contains(n))
}

fn summary_ok(summary: &str, facts: &Facts, known: &str) -> Option<String> {
    let s = one_line(summary);
    if s.split_whitespace().count() < 3 || s.chars().count() > MAX_SUMMARY_CHARS {
        return None;
    }
    if !crate::pr_layout::machinery(&s).is_empty() || !only_known(&s, known) {
        return None;
    }
    if s.starts_with('#') || s.starts_with('|') || s.starts_with("<!--") {
        return None;
    }
    // The same check as every other sentence: a statement the facts do not
    // bear out is corrected, and a summary it empties is refused.
    let (checked, _) = crate::pr_claims::enforce(&s, facts);
    let checked = one_line(&checked);
    (checked.split_whitespace().count() >= 3).then_some(checked)
}

fn check_ok(check: &str, written: &str) -> Option<String> {
    let c = one_line(check.trim().trim_matches('`'));
    if c.is_empty() || c.chars().count() > MAX_CHECK_CHARS {
        return None;
    }
    // Chosen from the description, never made up.
    (crate::pr_steps::is_check_command(&c) && written.contains(&c)).then(|| format!("`{c}`"))
}

/// What of a model's `reply` may be used, verified against `facts` and the
/// description as `written`. A slot that fails comes back empty: the caller
/// falls back to [`Slots::from_body`] and the records.
pub fn accept(reply: &str, facts: &Facts, written: &str) -> Slots {
    let Some(r) = json_of(reply) else {
        return Slots::default();
    };
    let mut known = written.to_string();
    for f in &facts.files {
        known.push(' ');
        known.push_str(&f.path);
    }
    if let Some(w) = &facts.why {
        known.push_str(&format!(" {} {}", w.reference, w.title));
    }
    Slots {
        summary: summary_ok(&r.summary, facts, &known).unwrap_or_default(),
        check: check_ok(&r.check, written).unwrap_or_default(),
        drop: r
            .drop
            .iter()
            .map(|d| one_line(d.trim_start_matches('#')))
            .filter(|d| !d.is_empty() && !KEPT_SECTIONS.contains(&d.to_lowercase().as_str()))
            .collect(),
    }
}

impl Slots {
    /// The slots a description already carries in its lead (what an earlier
    /// reader pass or the records put there), so a rebuild that makes no
    /// model call keeps them. They are verified again like a new reply.
    pub fn from_body(body: &str, facts: &Facts) -> Slots {
        let Some(a) = body.find(LEAD_START) else {
            return Slots::default();
        };
        let Some(b) = body[a..].find(LEAD_END) else {
            return Slots::default();
        };
        let lead = &body[a + LEAD_START.len()..a + b];
        let first = lead
            .lines()
            .map(str::trim)
            .find(|l| !l.is_empty())
            .filter(|l| !l.starts_with("**"))
            .unwrap_or("");
        let check = lead
            .lines()
            .find_map(|l| l.trim().strip_prefix("**Check:**"))
            .and_then(|l| {
                let l = l.trim();
                let cmd = l.strip_prefix('`')?.split('`').next()?;
                crate::pr_steps::is_check_command(cmd).then(|| format!("`{cmd}`"))
            })
            .unwrap_or_default();
        let known = format!("{body} {first}");
        Slots {
            summary: summary_ok(first, facts, &known).unwrap_or_default(),
            check,
            drop: Vec::new(),
        }
    }
}

/// `body` without the written sections named in `drop`. Only a `##` section
/// outside CoderHelm's own blocks goes, heading and all; a block CoderHelm
/// renders is never touched.
pub fn without_sections(body: &str, drop: &[String]) -> String {
    if drop.is_empty() {
        return body.to_string();
    }
    let wanted: Vec<String> = drop.iter().map(|d| d.to_lowercase()).collect();
    let lines: Vec<&str> = body.lines().collect();
    let mut out: Vec<&str> = Vec::new();
    let mut skipping = false;
    let mut block: Option<String> = None;
    let mut in_code = false;
    for (i, l) in lines.iter().enumerate() {
        let t = l.trim();
        if t.starts_with("```") {
            in_code = !in_code;
        }
        if in_code {
            if !skipping {
                out.push(l);
            }
            continue;
        }
        if let Some(close) = &block {
            out.push(l);
            if t == close {
                block = None;
            }
            continue;
        }
        if let Some(name) = t
            .strip_prefix("<!-- coderhelm:")
            .and_then(|r| r.strip_suffix("-->"))
            .map(str::trim)
            .filter(|n| !n.contains(' '))
        {
            let close = format!("<!-- /coderhelm:{name} -->");
            if lines[i + 1..].iter().any(|x| x.trim() == close) {
                skipping = false;
                block = Some(close);
                out.push(l);
                continue;
            }
        }
        if t.starts_with("## ") || t.starts_with("# ") {
            let title = t
                .trim_start_matches('#')
                .trim()
                .trim_matches('*')
                .trim()
                .to_lowercase();
            skipping = wanted.contains(&title) && !KEPT_SECTIONS.contains(&title.as_str());
        } else if t.starts_with("<!--") || t.starts_with("<sub>") {
            // A footer line ends whatever was being dropped.
            skipping = false;
        }
        if !skipping {
            out.push(l);
        }
    }
    let mut text = out.join("\n");
    if body.ends_with('\n') {
        text.push('\n');
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pr_facts::{CheckFact, FileChange, Outcome, Why};

    fn facts() -> Facts {
        Facts {
            head: "abc1234def".into(),
            why: Some(Why {
                reference: "OPS-41".into(),
                title: "Export poller gives up on the first timeout".into(),
                url: String::new(),
            }),
            files: vec![FileChange {
                path: "src/export/poller.rs".into(),
                status: "modified".into(),
                previous: String::new(),
            }],
            files_complete: true,
            checks: vec![CheckFact {
                name: "unit tests".into(),
                outcome: Outcome::Failed,
                url: String::new(),
            }],
            checks_read: true,
            ..Default::default()
        }
    }

    const WRITTEN: &str = "## Summary\n\nThe export poller now retries a timed-out poll 3 times in `poll_once` before it gives up.\n\n## Background\n\nThe ticket asks for retries.\n\n## Changes\n\n* `src/export/poller.rs`: the retry loop.\n\n## Verification\n\n1. Run `cargo test -p export`.\n";

    #[test]
    fn a_reply_is_taken_only_as_far_as_it_can_be_verified() {
        let good = r##"Here you go: {"summary": "Retries a timed-out export poll 3 times before giving up.", "check": "cargo test -p export", "drop": ["Background", "Changes", "# Risk"]}"##;
        let s = accept(good, &facts(), WRITTEN);
        assert_eq!(
            s.summary,
            "Retries a timed-out export poll 3 times before giving up."
        );
        assert_eq!(s.check, "`cargo test -p export`");
        // The explanation of the change and its risk are never dropped.
        assert_eq!(s.drop, vec!["Background"]);
    }

    #[test]
    fn a_reply_cannot_invent() {
        let f = facts();
        // A number and a name the description does not hold.
        let s = accept(
            r#"{"summary": "Retries the poll 5 times in `poll_forever`.", "check": "make verify"}"#,
            &f,
            WRITTEN,
        );
        assert_eq!(s, Slots::default());
        // A claim the facts refute (the suite failed) is taken out by the
        // same check as every other sentence.
        let s = accept(
            r#"{"summary": "Retries a timed-out export poll 3 times. All tests pass."}"#,
            &f,
            WRITTEN,
        );
        assert_eq!(s.summary, "Retries a timed-out export poll 3 times.");
        // Raw machinery, a blanket word alone, or no JSON at all.
        for bad in [
            r#"{"summary": "Calls query_content and gets a 403 Forbidden back from the API."}"#,
            r#"{"summary": "No impact."}"#,
            "I could not read the description.",
        ] {
            assert_eq!(accept(bad, &f, WRITTEN).summary, "", "{bad}");
        }
    }

    #[test]
    fn a_rebuild_without_a_model_keeps_the_slots_it_can_still_verify() {
        let body = format!(
            "{LEAD_START}\nRetries a timed-out export poll 3 times before giving up.\n\n**Check:** `cargo test -p export`; 0 of 1 recorded checks passed on `abc1234`.\n**Needed from you:** Nothing.\n{LEAD_END}\n\n{WRITTEN}"
        );
        let s = Slots::from_body(&body, &facts());
        assert_eq!(
            s.summary,
            "Retries a timed-out export poll 3 times before giving up."
        );
        assert_eq!(s.check, "`cargo test -p export`");
        assert_eq!(Slots::from_body(WRITTEN, &facts()), Slots::default());
    }

    #[test]
    fn only_written_sections_are_dropped() {
        let body = format!(
            "{WRITTEN}\n<!-- coderhelm:settled -->\n## Background\n- kept: a record\n<!-- /coderhelm:settled -->\n\n<sub>credit</sub>\n"
        );
        let out = without_sections(&body, &["Background".to_string(), "Changes".to_string()]);
        assert!(!out.contains("The ticket asks for retries."), "{out}");
        assert!(out.contains("## Changes"), "{out}");
        assert!(out.contains("- kept: a record"), "{out}");
        assert!(out.contains("## Verification"), "{out}");
        assert!(out.contains("<sub>credit</sub>"), "{out}");
        assert_eq!(without_sections(&body, &[]), body);
    }
}
