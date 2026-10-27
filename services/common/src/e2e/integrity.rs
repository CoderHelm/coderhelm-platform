//! Test integrity: a fix may not make a test pass by asking less of it.
//!
//! Read from the diff's structure (strings and comments taken out, see
//! [`super::fix_guard::code_of`]), in test files only:
//!
//! - **Removed assertions**: more `expect`/`assert` calls taken out than put
//!   back.
//! - **Skipped or expected to fail**: `.skip`, `.fixme`, `test.fail`, `xit`,
//!   `pytest.mark.skip`, `@Disabled`, `this.skip()`.
//! - **Broadened matchers**: an exact matcher (`toBe`, `toEqual`,
//!   `toHaveText`, `toHaveCount`, …) replaced by a looser one (`toContain`,
//!   `toMatch`, `toMatchObject`, `toBeTruthy`, `expect.anything()`, …).
//! - **Lowered counts**: `toHaveCount(5)` becoming `toHaveCount(3)`, a lower
//!   `toBeGreaterThan` bound, a higher `toBeLessThan` bound.
//! - **Swallowed failures**: an assertion wrapped in `try`/`catch`, a
//!   `.catch(…)` on an assertion, or `expect.soft`.
//!
//! These are refused unless the run's evidence shows the expectation itself
//! was wrong.

use serde::{Deserialize, Serialize};

use super::fix_guard::code_of;
use super::triage::is_test_file;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Loosening {
    RemovedAssertion,
    Skipped,
    BroadenedMatcher,
    LoweredCount,
    SwallowedFailure,
}

impl Loosening {
    pub fn words(self) -> &'static str {
        match self {
            Loosening::RemovedAssertion => "removes an assertion",
            Loosening::Skipped => "skips the test or marks it as expected to fail",
            Loosening::BroadenedMatcher => "replaces an exact check with a looser one",
            Loosening::LoweredCount => "lowers an expected count",
            Loosening::SwallowedFailure => "lets a failing assertion pass silently",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Weakening {
    pub path: String,
    pub kind: Loosening,
    pub line: String,
}

impl Weakening {
    pub fn sentence(&self) -> String {
        format!(
            "`{}` {} (`{}`)",
            self.path,
            self.kind.words(),
            crate::truncate_str(&self.line, 160)
        )
    }
}

const STRICT: &[&str] = &[
    ".toBe(",
    ".toEqual(",
    ".toStrictEqual(",
    ".toHaveText(",
    ".toHaveCount(",
    ".toHaveLength(",
    ".toHaveURL(",
    ".toHaveValue(",
    ".toHaveAttribute(",
    ".should('have.text'",
    ".should(\"have.text\"",
    "assertEqual(",
    "assertEquals(",
];

const LOOSE: &[&str] = &[
    ".toContain(",
    ".toContainText(",
    ".toMatch(",
    ".toMatchObject(",
    ".toBeTruthy(",
    ".toBeDefined(",
    ".toBeGreaterThan",
    ".toBeLessThan",
    "expect.anything(",
    "expect.any(",
    ".toBeVisible(",
    "assertIn(",
    "assertTrue(",
];

const SKIPS: &[&str] = &[
    "test.skip(",
    "test.fixme(",
    "test.fail(",
    "it.skip(",
    "describe.skip(",
    "describe.fixme(",
    "test.describe.skip(",
    "test.describe.fixme(",
    "xit(",
    "xdescribe(",
    "xtest(",
    "this.skip(",
    "pytest.skip(",
    "@pytest.mark.skip",
    "@pytest.mark.xfail",
    "@Disabled",
    "@Ignore",
];

fn is_assertion(code: &str) -> bool {
    code.contains("expect(")
        || code.contains("expect.soft(")
        || code.trim_start().starts_with("assert")
        || code.contains(".assert")
        || code.contains("assert_")
        || code.contains(".should(")
}

fn count(code: &str, list: &[&str]) -> usize {
    list.iter().filter(|m| code.contains(*m)).count()
}

/// `(matcher, number)` for the count matchers on a line.
fn counts(code: &str) -> Vec<(&'static str, i64)> {
    let mut out = Vec::new();
    for m in [
        ".toHaveCount(",
        ".toHaveLength(",
        ".toBeGreaterThan(",
        ".toBeGreaterThanOrEqual(",
        ".toBeLessThan(",
        ".toBeLessThanOrEqual(",
    ] {
        if let Some(i) = code.find(m) {
            let rest = code[i + m.len()..].trim_start();
            let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(n) = digits.parse() {
                out.push((m, n));
            }
        }
    }
    out
}

/// A hunk's added lines (code, raw) and removed code lines.
type HunkLines = (Vec<(String, String)>, Vec<String>);

/// One hunk's added and removed code lines (raw added lines kept for the
/// message).
fn hunks(path: &str, patch: &str) -> Vec<HunkLines> {
    let mut out: Vec<HunkLines> = Vec::new();
    for l in patch.lines() {
        if l.starts_with("@@") || out.is_empty() {
            out.push((Vec::new(), Vec::new()));
            if l.starts_with("@@") {
                continue;
            }
        }
        if l.starts_with("+++") || l.starts_with("---") {
            continue;
        }
        let Some(h) = out.last_mut() else { continue };
        if let Some(b) = l.strip_prefix('+') {
            h.0.push((code_of(path, b), b.trim().to_string()));
        } else if let Some(b) = l.strip_prefix('-') {
            h.1.push(code_of(path, b));
        }
    }
    out
}

/// How `files` (path, status, patch) loosen their tests.
pub fn weakenings(files: &[(String, String, String)]) -> Vec<Weakening> {
    let mut out = Vec::new();
    for (path, status, patch) in files {
        if !is_test_file(path) {
            continue;
        }
        let push = |out: &mut Vec<Weakening>, kind, line: &str| {
            if !out
                .iter()
                .any(|w: &Weakening| w.path == *path && w.kind == kind)
            {
                out.push(Weakening {
                    path: path.clone(),
                    kind,
                    line: line.to_string(),
                });
            }
        };
        if status == "removed" {
            push(&mut out, Loosening::RemovedAssertion, "the whole file");
            continue;
        }
        let all = hunks(path, patch);
        let (mut removed_asserts, mut added_asserts) = (0usize, 0usize);
        let mut last_removed = String::new();
        for (added, removed) in &all {
            removed_asserts += removed.iter().filter(|c| is_assertion(c)).count();
            added_asserts += added.iter().filter(|(c, _)| is_assertion(c)).count();
            if let Some(r) = removed.iter().rev().find(|c| is_assertion(c)) {
                last_removed = r.clone();
            }
            for (code, raw) in added {
                if SKIPS.iter().any(|s| code.contains(s)) {
                    push(&mut out, Loosening::Skipped, raw);
                }
                if code.contains("expect.soft(") || (is_assertion(code) && code.contains(".catch("))
                {
                    push(&mut out, Loosening::SwallowedFailure, raw);
                }
            }
            // An assertion moved inside a try whose catch was added too.
            let tries = added
                .iter()
                .any(|(c, _)| c.starts_with("try") || c.contains(" try {"));
            let catches = added.iter().any(|(c, _)| c.contains("catch"));
            if tries && catches && added.iter().any(|(c, _)| is_assertion(c)) {
                let raw = added
                    .iter()
                    .find(|(c, _)| c.contains("catch"))
                    .map(|(_, r)| r.as_str())
                    .unwrap_or("");
                push(&mut out, Loosening::SwallowedFailure, raw);
            }
            let strict_removed: usize = removed.iter().map(|c| count(c, STRICT)).sum();
            let strict_added: usize = added.iter().map(|(c, _)| count(c, STRICT)).sum();
            let loose_removed: usize = removed.iter().map(|c| count(c, LOOSE)).sum();
            let loose_added: usize = added.iter().map(|(c, _)| count(c, LOOSE)).sum();
            if strict_removed > strict_added && loose_added > loose_removed {
                let raw = added
                    .iter()
                    .find(|(c, _)| count(c, LOOSE) > 0)
                    .map(|(_, r)| r.as_str())
                    .unwrap_or("");
                push(&mut out, Loosening::BroadenedMatcher, raw);
            }
            let before: Vec<(&str, i64)> = removed.iter().flat_map(|c| counts(c)).collect();
            for (code, raw) in added {
                for (m, n) in counts(code) {
                    let Some(old) = before
                        .iter()
                        .filter(|(bm, _)| *bm == m)
                        .map(|(_, v)| *v)
                        .next()
                    else {
                        continue;
                    };
                    let lower_is_looser = !m.starts_with(".toBeLessThan");
                    if (lower_is_looser && n < old) || (!lower_is_looser && n > old) {
                        push(&mut out, Loosening::LoweredCount, raw);
                    }
                }
            }
        }
        if removed_asserts > added_asserts {
            push(&mut out, Loosening::RemovedAssertion, &last_removed);
        }
    }
    out
}

/// The fix round's instructions for loosened tests.
pub fn fix_feedback(found: &[Weakening]) -> String {
    let mut s = String::from(
        "The change loosens tests, which the rules refuse unless the evidence shows the expectation itself was wrong:\n",
    );
    for w in found {
        s.push_str(&format!("- {}\n", w.sentence()));
    }
    s.push_str(
        "\nPut the assertions back as they were and fix the cause. If a lookup shows the expectation itself is wrong (the data or the product changed on purpose), say so in your notes on a line starting `Expectation wrong:` that names the lookup id (L1, T2, …) showing it, and change the expectation to exactly what the evidence shows.\n",
    );
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(path: &str, patch: &str) -> Vec<(String, String, String)> {
        vec![(path.into(), "modified".into(), patch.into())]
    }

    fn kinds(path: &str, patch: &str) -> Vec<Loosening> {
        weakenings(&f(path, patch))
            .into_iter()
            .map(|w| w.kind)
            .collect()
    }

    #[test]
    fn deleted_and_skipped_tests_are_found() {
        assert_eq!(
            kinds(
                "e2e/a.spec.ts",
                "-  await expect(rows).toHaveCount(3);\n   await page.goto('/');"
            ),
            vec![Loosening::RemovedAssertion]
        );
        assert_eq!(
            kinds("e2e/a.spec.ts", "+test.fixme('deletes', async () => {"),
            vec![Loosening::Skipped]
        );
        assert_eq!(
            kinds("e2e/a.spec.ts", "+  test.fail();"),
            vec![Loosening::Skipped]
        );
        assert_eq!(
            kinds("tests/test_a.py", "+@pytest.mark.skip(reason='later')"),
            vec![Loosening::Skipped]
        );
        let gone = weakenings(&[("e2e/a.spec.ts".into(), "removed".into(), String::new())]);
        assert_eq!(gone[0].kind, Loosening::RemovedAssertion);
    }

    #[test]
    fn broadened_matchers_and_lowered_counts_are_found() {
        assert_eq!(
            kinds("e2e/a.spec.ts", "-  await expect(total).toHaveText('12 items');\n+  await expect(total).toContainText('items');"),
            vec![Loosening::BroadenedMatcher]
        );
        assert_eq!(
            kinds("tests/a.test.ts", "-  expect(res).toEqual({ id: 1, name: 'a' });\n+  expect(res).toMatchObject({ id: 1 });"),
            vec![Loosening::BroadenedMatcher]
        );
        assert_eq!(
            kinds(
                "e2e/a.spec.ts",
                "-  await expect(rows).toHaveCount(5);\n+  await expect(rows).toHaveCount(3);"
            ),
            vec![Loosening::LoweredCount]
        );
        assert_eq!(
            kinds(
                "e2e/a.spec.ts",
                "-  expect(ms).toBeLessThan(500);\n+  expect(ms).toBeLessThan(2000);"
            ),
            vec![Loosening::LoweredCount]
        );
        // A stricter count is fine.
        assert!(kinds(
            "e2e/a.spec.ts",
            "-  await expect(rows).toHaveCount(3);\n+  await expect(rows).toHaveCount(5);"
        )
        .is_empty());
    }

    #[test]
    fn swallowed_failures_are_found() {
        assert_eq!(
            kinds("e2e/a.spec.ts", "+  try {\n   await expect(dialog).toBeHidden();\n+    await expect(dialog).toBeHidden();\n+  } catch (e) {}"),
            vec![Loosening::SwallowedFailure]
        );
        assert_eq!(
            kinds(
                "e2e/a.spec.ts",
                "+  await expect.soft(total).toHaveText('3');"
            ),
            vec![Loosening::SwallowedFailure]
        );
        assert_eq!(
            kinds(
                "e2e/a.spec.ts",
                "+  await expect(total).toHaveText('3').catch(() => {});"
            ),
            vec![Loosening::SwallowedFailure]
        );
    }

    #[test]
    fn honest_changes_and_app_code_are_left_alone() {
        // A clearer message on the same check.
        assert!(kinds(
            "e2e/a.spec.ts",
            "-  await expect(rows).toHaveCount(3);\n+  await expect(rows, 'rows after filtering by status').toHaveCount(3);"
        )
        .is_empty());
        // An added check.
        assert!(kinds("e2e/a.spec.ts", "+  await expect(dialog).toBeHidden();").is_empty());
        // A comment mentioning skip.
        assert!(kinds("e2e/a.spec.ts", "+  // test.skip( was here").is_empty());
        // App code is not a test.
        assert!(kinds("src/list.ts", "-  expect(x).toBe(1);").is_empty());
        let fb = fix_feedback(&weakenings(&f(
            "e2e/a.spec.ts",
            "+test.skip('x', () => {})",
        )));
        assert!(fb.contains("Expectation wrong:"));
    }
}
