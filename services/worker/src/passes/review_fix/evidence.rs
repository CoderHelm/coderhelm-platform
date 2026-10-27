//! The PR description's record of what each review fix round checked
//! before its push: the scope check, the log checks, the full test suite
//! (its exact command and counts), the commit message's style, and any
//! approval the push dismissed. Newest round first, the last few kept.

pub const START: &str = "<!-- coderhelm:review-fixes -->";
pub const END: &str = "<!-- /coderhelm:review-fixes -->";
const HEADING: &str = "**Review fixes**";

/// Rounds kept in the description.
pub const MAX_ROUNDS: usize = 5;

/// One round.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Round {
    /// `2026-10-27`
    pub date: String,
    /// The pushed commit, short; empty when nothing was pushed.
    pub commit: String,
    /// The commit subject, or why nothing was pushed.
    pub headline: String,
    pub lines: Vec<String>,
}

impl Round {
    fn render(&self) -> String {
        let what = if self.commit.is_empty() {
            format!("not pushed: {}", one_line(&self.headline))
        } else {
            format!("commit `{}`: {}", self.commit, one_line(&self.headline))
        };
        let mut out = format!("* {}, {what}\n", self.date);
        for l in &self.lines {
            out.push_str(&format!("  * {}\n", one_line(l)));
        }
        out
    }
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The rounds already in a section, as their rendered text.
fn rounds_in(section: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for line in section.lines() {
        if line.starts_with(START) || line.starts_with(END) || line.trim() == HEADING {
            continue;
        }
        if line.starts_with("* ") {
            out.push(format!("{line}\n"));
        } else if line.starts_with("  ") {
            if let Some(last) = out.last_mut() {
                last.push_str(line);
                last.push('\n');
            }
        }
    }
    out
}

/// The section with `round` added first and older rounds after it.
pub fn section(existing: &str, round: &Round) -> String {
    let mut rounds = vec![round.render()];
    rounds.extend(rounds_in(existing));
    rounds.truncate(MAX_ROUNDS);
    format!("{START}\n{HEADING}\n{}{END}", rounds.concat())
}

/// The review fixes section of a description, if it has one.
pub fn current(body: &str) -> String {
    super::super::completeness::split_section(body, START, END).0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round(n: usize) -> Round {
        Round {
            date: "2026-10-27".into(),
            commit: format!("abc{n}"),
            headline: format!("Fix {n}"),
            lines: vec![
                "Scope: kept 2 hunks".into(),
                "Full test suite passed: `make test`\n(10 passed)".into(),
            ],
        }
    }

    #[test]
    fn rounds_stack_newest_first_and_old_ones_drop_off() {
        let mut s = section("", &round(1));
        assert!(s.starts_with(START) && s.ends_with(END));
        assert!(s.contains("* 2026-10-27, commit `abc1`: Fix 1\n  * Scope: kept 2 hunks\n"));
        assert!(s.contains("  * Full test suite passed: `make test` (10 passed)\n"));
        for n in 2..=7 {
            s = section(&s, &round(n));
        }
        assert_eq!(s.matches("\n* ").count(), MAX_ROUNDS);
        assert!(s.find("abc7").unwrap() < s.find("abc6").unwrap());
        assert!(!s.contains("abc2"));
    }

    #[test]
    fn a_held_round_says_it_was_not_pushed() {
        let r = Round {
            date: "2026-10-27".into(),
            commit: String::new(),
            headline: "the full test suite fails".into(),
            lines: vec![],
        };
        assert!(section("", &r).contains("* 2026-10-27, not pushed: the full test suite fails"));
    }

    #[test]
    fn the_section_is_found_in_a_description_and_kept_by_the_rewrite() {
        let s = section("", &round(1));
        let body = format!("**Summary** x\n\n{s}\n\n**Risk** Low");
        assert_eq!(current(&body).trim_end(), s);
        let (owned, rest) = crate::passes::completeness::take_owned(&body);
        assert_eq!(owned.review_fixes, s);
        assert!(!rest.contains("Review fixes"));
        let back = crate::passes::completeness::assemble(&rest, &owned);
        assert!(back.contains(&s));
        assert!(back.find(START).unwrap() < back.find("**Risk**").unwrap());
    }
}
