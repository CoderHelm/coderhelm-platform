//! The reviewer reads a red check test by test, from the failing test's own
//! error, before it says anything about whose failure it is.
//!
//! A vendor portal's end-to-end check was red on a pull request and on the
//! base branch. The reviewer was told only "this check fails on the base
//! branch too" by the check's name, and wrote that an unrelated
//! phone-pricing test was "broken on main". The test that failed was the
//! camp-selection test in the spec the pull request changed, and it failed
//! because the vendor had opened next year's registration: an outside
//! change, this branch's to fix.
//!
//! The rules:
//!
//! - [`CiCompare`] holds the failing tests on the head (name, spec, the
//!   first line of their error, from the failing checks' annotations) and
//!   the failing tests on the base branch's head. A check the base fails too
//!   is the base's by name only when it isn't an end-to-end check; an
//!   end-to-end check is compared test by test ([`CiCompare::base_failing`]).
//! - The prompt lists the failing tests, the ones in specs the change
//!   touches first, each with its own error, and which also fail on the
//!   base ([`CiCompare::context`]).
//! - [`hold`] replaces every sentence of a finding or the summary that
//!   blames the base branch, flakiness or something outside the change for a
//!   failing test, unless the test fails on the base's head too; and every
//!   sentence that asks to revert part of the change to make a check pass,
//!   unless the test passes on the base's head and fails on this one. It is
//!   the reviewer's side of the coder's revert gate and blame hold
//!   (`ci_repro`): the reviewer never asserts what those would refuse.

use super::review_agent::Finding;

/// One failing test, from a failing check's annotations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FailingTest {
    pub check: String,
    /// The annotated file (the spec), when there is one.
    pub spec: String,
    /// The test's title (`describe › test`).
    pub name: String,
    /// The first line of its error.
    pub error: String,
}

impl FailingTest {
    /// The last part of its title (the test itself, without its describe).
    fn short(&self) -> &str {
        self.name.rsplit(" › ").next().unwrap_or(&self.name).trim()
    }

    /// Whether `text` names this test.
    fn named_in(&self, text: &str) -> bool {
        let t = text.to_ascii_lowercase();
        let short = self.short().to_ascii_lowercase();
        (short.len() >= 8 && t.contains(&short))
            || (!self.name.is_empty() && t.contains(&self.name.to_ascii_lowercase()))
    }
}

/// The failing tests in a GitHub annotations listing
/// (`GitHubClient::get_check_run_annotations`: `--- check ---` headers, then
/// `level: path:line: message` with the message's later lines after it).
pub fn parse_annotations(text: &str) -> Vec<FailingTest> {
    let start = regex::Regex::new(r"^(failure|error|warning|notice): (\S*):(\d+): ?(.*)$").ok();
    let mut out: Vec<FailingTest> = Vec::new();
    let mut check = String::new();
    // The annotation being read: (check, path, level, message lines).
    let mut open: Option<(String, String, String, Vec<String>)> = None;
    let close = |open: &mut Option<(String, String, String, Vec<String>)>,
                 out: &mut Vec<FailingTest>| {
        if let Some((check, path, level, lines)) = open.take() {
            if level != "failure" && level != "error" {
                return;
            }
            let message = lines.join("\n");
            let name = super::outside_exec::failing_tests(&message)
                .into_iter()
                .next()
                .or_else(|| {
                    let first = lines.first().map(|l| l.trim()).unwrap_or("");
                    first.contains(" › ").then(|| title_of(first))
                });
            let Some(name) = name else {
                return;
            };
            let name = title_of(&name);
            let error = lines
                .iter()
                .map(|l| l.trim())
                .find(|l| {
                    l.starts_with("Error")
                        || l.starts_with("expect(")
                        || l.contains("Timeout")
                        || l.starts_with("TimeoutError")
                })
                .or_else(|| {
                    lines
                        .iter()
                        .map(|l| l.trim())
                        .find(|l| !l.is_empty() && !l.contains(" › "))
                })
                .unwrap_or("")
                .to_string();
            if !out.iter().any(|t| t.name == name && t.check == check) {
                out.push(FailingTest {
                    check,
                    spec: path,
                    name,
                    error: common::truncate_str(&error, 240).to_string(),
                });
            }
        }
    };
    for line in text.lines() {
        let t = line.trim();
        if let Some(name) = t.strip_prefix("--- ").and_then(|r| r.strip_suffix(" ---")) {
            close(&mut open, &mut out);
            check = name.to_string();
            continue;
        }
        if let Some(c) = start.as_ref().and_then(|re| re.captures(line)) {
            close(&mut open, &mut out);
            open = Some((
                check.clone(),
                c[2].to_string(),
                c[1].to_string(),
                vec![c[4].to_string()],
            ));
            continue;
        }
        if let Some((_, _, _, lines)) = open.as_mut() {
            lines.push(line.to_string());
        }
    }
    close(&mut open, &mut out);
    out
}

/// A Playwright line's test title: what follows the spec segment
/// (`[chromium] › a.spec.ts:12:5 › Suite › test` gives `Suite › test`),
/// without its retry note and trailing rule.
fn title_of(line: &str) -> String {
    let line = line
        .trim()
        .trim_start_matches(|c: char| c.is_ascii_digit() || c == ')' || c == ' ');
    let parts: Vec<&str> = line.split(" › ").map(str::trim).collect();
    let after_spec = parts
        .iter()
        .rposition(|p| p.contains(".spec.") || p.contains(".test.") || p.starts_with('['))
        .map(|i| &parts[i + 1..])
        .unwrap_or(&parts[..]);
    let title = if after_spec.is_empty() {
        parts.join(" › ")
    } else {
        after_spec.join(" › ")
    };
    title
        .split(" (retry #")
        .next()
        .unwrap_or("")
        .trim_end_matches(['─', ' '])
        .trim()
        .to_string()
}

/// A red check, test by test, on the head and on the base branch's head.
#[derive(Debug, Clone, Default)]
pub struct CiCompare {
    pub head: String,
    pub base: String,
    /// Failing tests on the head.
    pub head_failing: Vec<FailingTest>,
    /// Failing tests on the base branch's head.
    pub base_failing_tests: Vec<FailingTest>,
    /// The base head's failing checks, by name.
    pub base_failing_checks: Vec<String>,
    /// Whether the base head's end-to-end checks ran to the end (so a test
    /// not failing there passed there).
    pub base_e2e_ran: bool,
    /// The paths the pull request changes (its merge-base diff).
    pub touched: Vec<String>,
}

impl CiCompare {
    /// From the head's and the base's annotations and the base's check runs.
    pub fn read(
        head: &str,
        base: &str,
        head_notes: &str,
        base_notes: &str,
        base_runs: &serde_json::Value,
    ) -> Self {
        let runs = base_runs["check_runs"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let e2e: Vec<&serde_json::Value> = runs
            .iter()
            .filter(|r| super::e2e_reads::is_e2e_name(r["name"].as_str().unwrap_or("")))
            .collect();
        CiCompare {
            head: head.to_string(),
            base: base.to_string(),
            head_failing: parse_annotations(head_notes),
            base_failing_tests: parse_annotations(base_notes),
            base_failing_checks: super::review_policy::failing_checks(base_runs),
            base_e2e_ran: !e2e.is_empty()
                && e2e.iter().all(|r| {
                    r["status"].as_str() == Some("completed")
                        && r["conclusion"].as_str() != Some("skipped")
                }),
            touched: Vec::new(),
        }
    }

    /// With the paths the pull request changes.
    pub fn touching(mut self, touched: Vec<String>) -> Self {
        self.touched = touched;
        self
    }

    /// Whether `t` is in a spec the pull request changes.
    pub fn in_touched_spec(&self, t: &FailingTest) -> bool {
        touches(&self.touched, &t.spec)
    }

    /// Whether the base's head fails `t` too.
    pub fn on_base_too(&self, t: &FailingTest) -> bool {
        self.base_failing_tests.iter().any(|b| b.name == t.name)
    }

    /// Whether `t` passed on the base's head and fails on this one: the
    /// change made it fail.
    pub fn caused_here(&self, t: &FailingTest) -> bool {
        self.base_e2e_ran && !self.on_base_too(t)
    }

    /// What fails on the base branch too, for the blocking policy: a
    /// failing check by name when it isn't an end-to-end check, and the
    /// end-to-end tests that fail on both heads by name. An end-to-end check
    /// red on both is never the base's by its name alone.
    pub fn base_failing(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .base_failing_checks
            .iter()
            .filter(|c| !super::e2e_reads::is_e2e_name(c))
            .cloned()
            .collect();
        for t in self.head_failing.iter().filter(|t| self.on_base_too(t)) {
            if !out.contains(&t.name) {
                out.push(t.name.clone());
            }
        }
        out
    }

    /// The head's failing tests, the ones in a spec the change touches
    /// first.
    pub fn ordered(&self) -> Vec<(&FailingTest, bool)> {
        let mut out: Vec<(&FailingTest, bool)> = self
            .head_failing
            .iter()
            .map(|t| (t, self.in_touched_spec(t)))
            .collect();
        out.sort_by_key(|(_, touched)| !*touched);
        out
    }

    /// The reviewer's section on the red checks.
    pub fn context(&self) -> String {
        let short = |s: &str| s[..s.len().min(7)].to_string();
        let mut s = String::new();
        let other: Vec<&String> = self
            .base_failing_checks
            .iter()
            .filter(|c| !super::e2e_reads::is_e2e_name(c))
            .collect();
        if !other.is_empty() {
            s.push_str(&format!(
                "\n## Checks failing on the base branch too ({})\n{}\nThese fail without this \
                 PR's change: never block on them. Mention one as a note at most, with \
                 `caused_by_diff`: false.\n",
                short(&self.base),
                other
                    .iter()
                    .map(|n| format!("- {n}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            ));
        }
        if self.head_failing.is_empty() {
            return s;
        }
        s.push_str(&format!(
            "\n## Failing tests on this head ({}), from their own errors\n",
            short(&self.head)
        ));
        for (t, touched) in self.ordered() {
            let mark = if touched {
                " (a spec this change touches)"
            } else {
                ""
            };
            let base = if self.on_base_too(t) {
                format!("fails on the base branch's head {} too", short(&self.base))
            } else if self.base_e2e_ran {
                format!("passes on the base branch's head {}", short(&self.base))
            } else {
                "not compared: the base branch's run didn't finish".to_string()
            };
            s.push_str(&format!(
                "- `{}` in `{}`{mark}: {} ({base})\n",
                t.name,
                if t.spec.is_empty() { &t.check } else { &t.spec },
                if t.error.is_empty() {
                    "no error line in its annotation"
                } else {
                    &t.error
                }
            ));
        }
        s.push_str(TRIAGE_RULES);
        s
    }
}

/// How the reviewer reads a red check.
pub const TRIAGE_RULES: &str = "Start from the tests in specs this change touches, and name a \
     failure by the failing test and its own error above, never another test. A test that \
     fails on the base branch's head too isn't caused by this change's code; before calling it \
     pre-existing or broken on the base branch, read its error for an outside change (another \
     company's page, data, a date): then the test or the code it checks is this branch's to fix, \
     and you say so. Never ask to revert part of the change to make a check pass unless the \
     test passes on the base branch's head and fails on this one.\n";

fn touches(touched: &[String], spec: &str) -> bool {
    let spec = spec.trim_start_matches("./");
    !spec.is_empty()
        && touched
            .iter()
            .any(|p| p == spec || p.ends_with(&format!("/{spec}")) || spec.ends_with(p.as_str()))
}

/// Whether `sentence` is about a failing check or test.
fn about_a_failure(s: &str) -> bool {
    regex::Regex::new(r"\b(tests?|checks?|ci|e2e|end-to-end|specs?|suite|fail\w*|red|green)\b")
        .map(|re| re.is_match(s))
        .unwrap_or(false)
}

/// Whether `sentence` says a failure is the base branch's, flaky,
/// pre-existing or outside the change.
pub fn blames(sentence: &str) -> bool {
    let s = sentence.to_ascii_lowercase();
    const WORDS: &[&str] = &[
        "broken on main",
        "broken on the base",
        "fails on main",
        "failing on main",
        "red on main",
        "on main too",
        "on main as well",
        "on the base branch too",
        "on the base branch as well",
        "pre-existing",
        "preexisting",
        "already failing",
        "already broken",
        "already red",
        "flaky",
        "flake",
        "not caused by this",
        "isn't caused by this",
        "is not caused by this",
        "not this change's",
        "not this pr's",
        "unrelated to this change",
        "unrelated to this pr",
        "unrelated to the change",
        "unrelated test",
        "unrelated failure",
        "known failure",
    ];
    WORDS.iter().any(|w| s.contains(w)) && about_a_failure(&s)
}

/// Whether `sentence` asks to revert or take out part of the change to make
/// a check pass.
pub fn asks_revert(sentence: &str) -> bool {
    let s = sentence.to_ascii_lowercase();
    let revert = [
        "revert",
        "roll back",
        "rollback",
        "back out",
        "put back",
        "take out",
    ]
    .iter()
    .any(|w| s.contains(w));
    revert && about_a_failure(&s)
}

/// What replaces a blame with no test-level comparison behind it.
pub const BLAME_HELD: &str = "Whose failure this is isn't shown: the failing test doesn't fail \
     the same way on the base branch's head.";

/// What replaces a blame of a test in a spec the pull request changes that
/// fails on the base branch too: an outside change broke it, and its fix
/// belongs on this branch.
pub const TOUCHED_HELD: &str = "That test fails on the base branch's head too and is in a spec \
     this change touches: something outside the code changed under it, and its fix (the test, or \
     the code it checks) belongs on this branch.";

/// What replaces a revert asked for with no comparison behind it.
pub const REVERT_HELD: &str = "Reverting part of the change isn't shown to fix the check: the \
     failing test doesn't pass on the base branch's head.";

/// The head's failing tests a sentence names by title.
fn named<'a>(cmp: &'a CiCompare, sentence: &str) -> Vec<&'a FailingTest> {
    cmp.head_failing
        .iter()
        .filter(|t| t.named_in(sentence))
        .collect()
}

/// What a blame sentence may say: `None` when it is backed (it names a
/// failing test, every one it names fails on the base's head too, and none
/// is in a spec the change touches), else what replaces it. A blame that
/// names no failing test (another test, or only a check's name) is never
/// backed.
fn blame_held(cmp: &CiCompare, sentence: &str) -> Option<&'static str> {
    let tests = named(cmp, sentence);
    if tests.is_empty() || !tests.iter().all(|t| cmp.on_base_too(t)) {
        return Some(BLAME_HELD);
    }
    if tests.iter().any(|t| cmp.in_touched_spec(t)) {
        return Some(TOUCHED_HELD);
    }
    None
}

/// Whether a revert sentence is backed: a test it names (or, when it names
/// none, every failing test) passes on the base's head and fails on this
/// one.
fn revert_backed(cmp: &CiCompare, sentence: &str) -> bool {
    let tests = named(cmp, sentence);
    if tests.is_empty() {
        !cmp.head_failing.is_empty() && cmp.head_failing.iter().all(|t| cmp.caused_here(t))
    } else {
        tests.iter().any(|t| cmp.caused_here(t))
    }
}

/// `text` with its unbacked blame and revert sentences replaced. Returns
/// how many were.
pub fn hold_text(text: &mut String, cmp: &CiCompare) -> usize {
    let mut n = 0;
    let mut out = String::with_capacity(text.len());
    for (i, line) in text.split('\n').enumerate() {
        if i > 0 {
            out.push('\n');
        }
        for sentence in sentences(line) {
            let held = if blames(sentence) {
                blame_held(cmp, sentence)
            } else if asks_revert(sentence) && !revert_backed(cmp, sentence) {
                Some(REVERT_HELD)
            } else {
                None
            };
            match held {
                Some(h) => {
                    n += 1;
                    let lead: String = sentence
                        .chars()
                        .take_while(|c| c.is_whitespace() || matches!(c, '-' | '*'))
                        .collect();
                    let trail = if sentence.ends_with(' ') { " " } else { "" };
                    out.push_str(&format!("{lead}{h}{trail}"));
                }
                None => out.push_str(sentence),
            }
        }
    }
    if n > 0 {
        *text = out;
    }
    n
}

/// A line's sentences, each with its own end and trailing space.
fn sentences(line: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let b = line.as_bytes();
    for (i, c) in b.iter().enumerate() {
        if matches!(c, b'.' | b'!' | b'?') && (i + 1 == b.len() || b[i + 1] == b' ') {
            let end = (i + 2).min(b.len());
            out.push(&line[start..end]);
            start = end;
        }
    }
    if start < line.len() {
        out.push(&line[start..]);
    }
    out
}

/// Holds the findings' and the summary's unbacked blame and revert
/// sentences ([`hold_text`]). A finding that rests on such a claim (it says
/// the failure isn't the change's) no longer says it isn't the diff's.
/// Returns how many sentences were held.
pub fn hold(findings: &mut [Finding], summary: &mut String, cmp: &CiCompare) -> usize {
    let mut n = hold_text(summary, cmp);
    for f in findings.iter_mut() {
        let title = hold_text(&mut f.title, cmp);
        let body = hold_text(&mut f.body, cmp);
        if title + body > 0 && f.caused_by_diff == Some(false) {
            f.caused_by_diff = None;
        }
        n += title + body;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const HEAD: &str = "\n--- Playwright e2e (all tests) ---\nfailure: e2e/tests/browse/portal.spec.ts:120: 1) [chromium] › browse/portal.spec.ts:120:7 › Portal › choosing a camp adds it to the cart while registration is open ──\n\n    Error: Choosing a camp did not add it to the order summary\n    expect(locator).not.toHaveText(expected) failed\n";

    #[test]
    fn annotations_give_each_failing_test_with_its_own_error() {
        let t = parse_annotations(HEAD);
        assert_eq!(t.len(), 1);
        assert_eq!(
            t[0].name,
            "Portal › choosing a camp adds it to the cart while registration is open"
        );
        assert_eq!(t[0].spec, "e2e/tests/browse/portal.spec.ts");
        assert!(t[0].error.starts_with("Error: Choosing a camp"));
        assert_eq!(t[0].check, "Playwright e2e (all tests)");
    }

    #[test]
    fn an_end_to_end_check_red_on_both_is_compared_test_by_test() {
        let base_runs = json!({"check_runs": [
            {"name": "Playwright e2e (all tests)", "status": "completed", "conclusion": "failure"},
            {"name": "Lint", "status": "completed", "conclusion": "failure"}
        ]});
        let base_notes = "\n--- Playwright e2e (all tests) ---\nfailure: e2e/tests/browse/pricing.spec.ts:40: 1) [chromium] › browse/pricing.spec.ts:40:3 › Pricing › phone pricing shows both rates\n    Error: timeout\n";
        let cmp = CiCompare::read("h1", "b1", HEAD, base_notes, &base_runs)
            .touching(vec!["e2e/tests/browse/portal.spec.ts".into()]);
        // The check's name is not the base's; the lint check still is.
        assert_eq!(cmp.base_failing(), vec!["Lint".to_string()]);
        assert!(cmp.caused_here(&cmp.head_failing[0]));
        // A blame of a test that isn't failing here is held; so is one that
        // only names the check.
        let mut s = "The phone pricing shows both rates test is broken on main. Fixed the copy."
            .to_string();
        assert_eq!(hold_text(&mut s, &cmp), 1);
        assert_eq!(s, format!("{BLAME_HELD} Fixed the copy."));
        let mut c = "Playwright e2e fails on main too, so it's pre-existing.".to_string();
        assert_eq!(hold_text(&mut c, &cmp), 1);
        // A revert of the change the failing test passes without is backed.
        let mut r = "Revert the cart step: choosing a camp adds it to the cart while registration is open fails only here.".to_string();
        assert_eq!(hold_text(&mut r, &cmp), 0);
        // Code words alone are no blame.
        let mut code = "This guards a pre-existing hole in the parser.".to_string();
        assert_eq!(hold_text(&mut code, &cmp), 0);
    }
}
