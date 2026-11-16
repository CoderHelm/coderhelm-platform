//! What a failed end-to-end run says, read before anything is written
//! about it.
//!
//! A pull request once said CoderHelm "couldn't read the log" of a failed
//! end-to-end run while the workflow's own comment on the pull request
//! named the failing test and its error. The rules:
//!
//! - [`read`] takes every source there is (the e2e workflow's comment on
//!   the pull request, the check run's annotations, the job's log) and
//!   returns the failing tests and the error from the first that names
//!   them. It returns [`Unreadable`] only when every source is empty: "the
//!   log couldn't be read" can't be said while a comment or an annotation
//!   exists.
//! - [`cause`] says whether this pull request caused it: the same check
//!   failing on the base branch is not this change's; a failing spec the
//!   change touched, or a check that passes on the base, is.

use serde::{Deserialize, Serialize};

/// What there is to read about one failed check.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Sources {
    /// The comment the e2e workflow posted on the pull request.
    pub comment: String,
    /// The check run's annotations: (path, title, message).
    pub annotations: Vec<(String, String, String)>,
    /// The tail of the job's log.
    pub log: String,
}

impl Sources {
    pub fn is_empty(&self) -> bool {
        self.comment.trim().is_empty() && self.annotations.is_empty() && self.log.trim().is_empty()
    }
}

/// Every source was empty: the only time a failure may be called unread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unreadable;

/// Whether this pull request caused the failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "why")]
pub enum Cause {
    ThisChange(String),
    NotThisChange(String),
    Unknown(String),
}

/// A failed end-to-end check, as its own report says.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureRead {
    pub check: String,
    /// The failing tests, as the report names them (`file › title`).
    pub tests: Vec<String>,
    /// The first error line.
    pub error: String,
    /// Where it was read: `comment`, `annotations` or `log`.
    pub source: String,
    pub cause: Cause,
}

impl FailureRead {
    /// One line for the pull request.
    pub fn line(&self) -> String {
        let tests = if self.tests.is_empty() {
            String::new()
        } else {
            format!(
                ": {}",
                self.tests
                    .iter()
                    .take(3)
                    .map(|t| format!("`{t}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let err = if self.error.is_empty() {
            String::new()
        } else {
            format!(" ({})", crate::truncate_str(&self.error, 160))
        };
        let cause = match &self.cause {
            Cause::ThisChange(w) => format!("This change caused it: {w}"),
            Cause::NotThisChange(w) => format!("Not this change's: {w}"),
            Cause::Unknown(w) => format!("Whether this change caused it isn't known yet: {w}"),
        };
        format!(
            "`{}` failed{tests}{err}, read from its {}. {cause}.",
            self.check, self.source
        )
    }
}

/// A failing test named in a report line: Playwright's `✘ … › file:line ›
/// title`, a numbered `1) [project] › …`, or Cypress' `1) suite title`.
fn test_of(line: &str) -> Option<String> {
    let t = line.trim();
    let t = t
        .trim_start_matches(['✘', '×', '✗', '-', '*', ' '])
        .trim_start_matches(|c: char| c.is_ascii_digit())
        .trim_start_matches([')', '.', ' ']);
    if !t.contains(" › ") && !t.contains(" > ") {
        return None;
    }
    let sep = if t.contains(" › ") { " › " } else { " > " };
    let parts: Vec<&str> = t
        .split(sep)
        .map(str::trim)
        .filter(|p| !p.starts_with('[') && !p.is_empty())
        .collect();
    let file = parts
        .iter()
        .find(|p| p.contains(".spec.") || p.contains(".test.") || p.contains(".cy."))?;
    let file = file.split(':').next().unwrap_or(file);
    let title = parts
        .last()
        .filter(|l| !l.contains(file))
        .copied()
        .unwrap_or("");
    Some(if title.is_empty() {
        file.to_string()
    } else {
        format!("{file} › {title}")
    })
}

fn error_of(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .find(|l| {
            let low = l.to_ascii_lowercase();
            low.starts_with("error:")
                || low.starts_with("timeouterror")
                || low.starts_with("expect(")
                || low.contains("assertionerror")
                || low.starts_with("expected")
        })
        .map(|l| l.replace('`', "'"))
        .unwrap_or_default()
}

fn tests_in(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for l in text.lines() {
        let low = l.to_ascii_lowercase();
        // Passing lines and the summary don't name failures.
        if low.contains("✓") || low.contains(" passed") || low.contains("flaky") {
            continue;
        }
        if let Some(t) = test_of(l) {
            if !out.contains(&t) {
                out.push(t);
            }
        }
    }
    out
}

/// The failing tests and error, from the first source that names them.
pub fn read(check: &str, s: &Sources) -> Result<FailureRead, Unreadable> {
    if s.is_empty() {
        return Err(Unreadable);
    }
    let make = |tests: Vec<String>, error: String, source: &str| FailureRead {
        check: check.to_string(),
        tests,
        error,
        source: source.to_string(),
        cause: Cause::Unknown("not decided yet".into()),
    };
    let from_comment = (tests_in(&s.comment), error_of(&s.comment));
    if !from_comment.0.is_empty() || !from_comment.1.is_empty() {
        return Ok(make(
            from_comment.0,
            from_comment.1,
            "comment on the pull request",
        ));
    }
    if !s.annotations.is_empty() {
        let mut tests = Vec::new();
        let mut error = String::new();
        for (path, title, message) in &s.annotations {
            let named = test_of(title).unwrap_or_else(|| {
                if path.is_empty() {
                    title.clone()
                } else {
                    format!("{path} › {title}")
                }
            });
            if !named.trim().is_empty() && !tests.contains(&named) {
                tests.push(named);
            }
            if error.is_empty() {
                error = error_of(message);
                if error.is_empty() {
                    error = message.lines().next().unwrap_or("").trim().to_string();
                }
            }
        }
        return Ok(make(tests, error, "check annotations"));
    }
    if !s.log.trim().is_empty() {
        return Ok(make(tests_in(&s.log), error_of(&s.log), "job log"));
    }
    // A comment that names nothing is still read: it is quoted as is.
    Ok(make(
        Vec::new(),
        s.comment
            .lines()
            .find(|l| !l.trim().is_empty())
            .unwrap_or("")
            .trim()
            .to_string(),
        "comment on the pull request",
    ))
}

/// Whether the pull request caused it: `base_failed` is the same check's
/// result on the base branch's head (None when it didn't run there).
pub fn cause(f: &FailureRead, changed: &[String], base_failed: Option<bool>) -> Cause {
    if base_failed == Some(true) {
        return Cause::NotThisChange(format!("`{}` fails on the base branch too", f.check));
    }
    let touched: Vec<&String> = f
        .tests
        .iter()
        .filter(|t| {
            let file = t.split(" › ").next().unwrap_or(t);
            changed
                .iter()
                .any(|c| c.ends_with(file) || file.ends_with(c.as_str()))
        })
        .collect();
    if let Some(t) = touched.first() {
        return Cause::ThisChange(format!(
            "the failing spec `{t}` is one this change adds or edits"
        ));
    }
    if base_failed == Some(false) {
        return Cause::ThisChange(format!(
            "`{}` passes on the base branch and fails with this change",
            f.check
        ));
    }
    Cause::Unknown(format!(
        "`{}` hasn't run on the base branch to compare",
        f.check
    ))
}

/// Text that says a failure couldn't be read: refused while the failure
/// has a comment, an annotation or a log to read.
pub fn says_unreadable(text: &str) -> bool {
    let t = text.to_ascii_lowercase();
    [
        "couldn't read the log",
        "could not read the log",
        "couldn't read its log",
        "could not read its log",
        "log couldn't be read",
        "log could not be read",
        "log isn't readable",
        "couldn't read the e2e",
        "could not read the e2e",
    ]
    .iter()
    .any(|p| t.contains(p))
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMMENT: &str = "## E2E results\n\n❌ 1 failed, 41 passed\n\n  1) [chromium] › tests/checkout/registration.spec.ts:88:7 › Camp registration › pays with a saved card\n\n    Error: expect(locator).toBeVisible() failed\n    Locator: getByTestId('payment-confirmation')\n";

    /// A pull request said it couldn't read the log while the e2e
    /// workflow's comment named the failing test and its error.
    #[test]
    fn the_comment_names_the_failing_test_and_its_error() {
        let s = Sources {
            comment: COMMENT.into(),
            ..Default::default()
        };
        let f = read("E2E on preview", &s).unwrap();
        assert_eq!(
            f.tests,
            vec!["tests/checkout/registration.spec.ts › pays with a saved card".to_string()]
        );
        assert!(f.error.starts_with("Error: expect(locator).toBeVisible()"));
        assert_eq!(f.source, "comment on the pull request");
        // "Unreadable" only when there is nothing at all.
        assert_eq!(read("x", &Sources::default()), Err(Unreadable));
        assert!(says_unreadable(
            "CoderHelm couldn't read the log of this run"
        ));
    }

    #[test]
    fn annotations_are_read_when_there_is_no_comment() {
        let s = Sources {
            annotations: vec![(
                "e2e/tests/login.spec.ts".into(),
                "[chromium] › tests/login.spec.ts:12:5 › Login › shows an error".into(),
                "Error: Timed out 5000ms waiting for expect(locator).toHaveText()".into(),
            )],
            ..Default::default()
        };
        let f = read("E2E", &s).unwrap();
        assert_eq!(f.source, "check annotations");
        assert_eq!(
            f.tests,
            vec!["tests/login.spec.ts › shows an error".to_string()]
        );
        assert!(f.error.contains("Timed out"));
    }

    #[test]
    fn the_cause_comes_from_the_base_and_the_changed_specs() {
        let f = read(
            "E2E on preview",
            &Sources {
                comment: COMMENT.into(),
                ..Default::default()
            },
        )
        .unwrap();
        let none: Vec<String> = Vec::new();
        assert!(matches!(
            cause(&f, &none, Some(true)),
            Cause::NotThisChange(_)
        ));
        assert!(matches!(
            cause(&f, &none, Some(false)),
            Cause::ThisChange(_)
        ));
        assert!(matches!(cause(&f, &none, None), Cause::Unknown(_)));
        let changed = vec!["e2e/tests/checkout/registration.spec.ts".to_string()];
        assert!(matches!(cause(&f, &changed, None), Cause::ThisChange(_)));
    }
}
