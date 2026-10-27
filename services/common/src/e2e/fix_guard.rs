//! What a fix for a failing or flaky test may not add to test code without
//! evidence, read from the diff's structure (calls and settings, with strings
//! and comments taken out), not from what the description claims.
//!
//! - **Waits without a reason** (rule: get the failure first): a fixed wait
//!   (`waitForTimeout`, `cy.wait(500)`, `browser.pause`, `sleep`,
//!   `setTimeout` in a test), a poll (`expect.poll`, `toPass`, `waitUntil`,
//!   `waitForFunction`, `setInterval`), more retries (`retries: 2`,
//!   `jest.retryTimes`, `--retries`, `pytest.mark.flaky`) or a longer
//!   timeout (`timeout: 30000`, `test.setTimeout`, `test.slow()`,
//!   `defaultCommandTimeout`) is refused unless the failing run's report shows
//!   the test failed on time: a timeout, or waiting for something that never
//!   came.
//! - **Shared helpers that walk every row** (rule: keep shared helpers
//!   cheap): a page-object or helper file that gains a loop or a poll over a
//!   whole collection (`.all()`, `$$`, `findElements`, `.each`,
//!   `allTextContents`, a `count()` loop) is refused: the helper should wait
//!   on the one element it reads.

use serde::{Deserialize, Serialize};

/// What kind of change a finding is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    FixedWait,
    Poll,
    Retries,
    Timeout,
    RowWalk,
}

impl Kind {
    pub fn words(self) -> &'static str {
        match self {
            Kind::FixedWait => "a fixed wait",
            Kind::Poll => "a poll",
            Kind::Retries => "more retries",
            Kind::Timeout => "a longer timeout",
            Kind::RowWalk => "a loop over every row in a shared helper",
        }
    }
}

/// One added line the rules refuse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finding {
    pub path: String,
    pub kind: Kind,
    /// The added line, trimmed.
    pub line: String,
    /// What it does, in a few words (`waitForTimeout`, `retries 0 to 2`).
    pub what: String,
}

impl Finding {
    pub fn sentence(&self) -> String {
        format!(
            "`{}` adds {} (`{}`: {})",
            self.path,
            self.kind.words(),
            crate::truncate_str(&self.line, 160),
            self.what
        )
    }
}

fn ext(path: &str) -> &str {
    path.rsplit('/')
        .next()
        .and_then(|n| n.rsplit_once('.'))
        .map(|(_, e)| e)
        .unwrap_or("")
}

fn file_name(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_lowercase()
}

/// A test runner's configuration file.
pub fn is_test_config(path: &str) -> bool {
    let n = file_name(path);
    [
        "playwright.config.",
        "cypress.config.",
        "jest.config.",
        "vitest.config.",
        "wdio.conf.",
        "karma.conf.",
        ".mocharc",
        "codecept.conf.",
        "nightwatch.conf.",
    ]
    .iter()
    .any(|p| n.starts_with(p))
        || matches!(n.as_str(), "pytest.ini" | "conftest.py")
}

/// Test code: specs, their support files (page objects, helpers, fixtures)
/// and test runner settings.
pub fn is_test_code(path: &str) -> bool {
    if is_test_config(path) || super::triage::is_test_file(path) {
        return true;
    }
    let l = path.to_lowercase();
    l.split('/').any(|s| {
        matches!(
            s,
            "page-objects"
                | "pageobjects"
                | "page_objects"
                | "pom"
                | "test-utils"
                | "test_utils"
                | "testutils"
                | "test-helpers"
                | "test_helpers"
                | "support"
                | "fixtures"
        )
    })
}

/// A spec: a file of tests, as opposed to code the specs share.
pub fn is_spec(path: &str) -> bool {
    let n = file_name(path);
    [".test.", ".spec.", "_test.", "_spec.", ".e2e.", ".cy."]
        .iter()
        .any(|m| n.contains(m))
        || n.starts_with("test_")
        || n.ends_with(".feature")
        || n.ends_with("test.java")
        || n.ends_with("test.kt")
}

/// Code shared by many tests: page objects, helpers, fixtures, steps.
pub fn is_shared_helper(path: &str) -> bool {
    is_test_code(path) && !is_spec(path) && !is_test_config(path)
}

fn hash_comments(path: &str) -> bool {
    matches!(
        ext(path),
        "py" | "rb" | "ini" | "cfg" | "toml" | "yml" | "yaml" | "sh"
    )
}

/// The code on a line with string contents and comments taken out, so a
/// word in a message or a comment never counts.
pub fn code_of(path: &str, line: &str) -> String {
    let hash = hash_comments(path);
    let mut out = String::with_capacity(line.len());
    let mut quote: Option<char> = None;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match quote {
            Some(q) => {
                if c == '\\' {
                    chars.next();
                } else if c == q {
                    quote = None;
                    out.push(c);
                }
            }
            None => {
                if c == '"' || c == '\'' || c == '`' {
                    quote = Some(c);
                    out.push(c);
                } else if (c == '/' && chars.peek() == Some(&'/') && !hash) || (c == '#' && hash) {
                    break;
                } else {
                    out.push(c);
                }
            }
        }
    }
    let t = out.trim();
    // Block comment lines.
    if t.starts_with("/*") || t.starts_with('*') || t.starts_with("<!--") {
        return String::new();
    }
    t.to_string()
}

fn is_ident(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '$'
}

/// Whether `code` calls `name(` (`name` may include a receiver: `cy.wait`),
/// as a whole word. Returns the text right after the `(`.
fn call<'a>(code: &'a str, name: &str) -> Option<&'a str> {
    let pat = format!("{name}(");
    let mut from = 0;
    while let Some(i) = code[from..].find(&pat) {
        let at = from + i;
        // A method (`.toPass`) follows anything; a function name must start
        // a word.
        let before = code[..at].chars().last();
        if name.starts_with('.') || before.is_none_or(|c| !is_ident(c)) {
            return Some(&code[at + pat.len()..]);
        }
        from = at + pat.len();
    }
    None
}

fn starts_with_number(s: &str) -> bool {
    s.trim_start()
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_digit())
}

/// The first number in `s`, `_` allowed as a separator.
fn number(s: &str) -> Option<u64> {
    let s = s.trim_start();
    let digits: String = s
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '_')
        .filter(|c| *c != '_')
        .collect();
    digits.parse().ok()
}

const TIMEOUT_KEYS: &[&str] = &[
    "timeout",
    "actionTimeout",
    "navigationTimeout",
    "expectTimeout",
    "defaultCommandTimeout",
    "pageLoadTimeout",
    "requestTimeout",
    "responseTimeout",
    "execTimeout",
    "waitforTimeout",
    "testTimeout",
    "hookTimeout",
    "slowTestThreshold",
];

/// `key: 123` / `key = 123` settings on a line: (key, value).
fn settings(code: &str, keys: &[&str]) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    for key in keys {
        let mut from = 0;
        while let Some(i) = code[from..].find(key) {
            let at = from + i;
            from = at + key.len();
            let before = code[..at].chars().last();
            let after = &code[at + key.len()..];
            if before.is_some_and(is_ident) || after.chars().next().is_some_and(is_ident) {
                continue;
            }
            let rest = after.trim_start();
            let Some(rest) = rest.strip_prefix(':').or_else(|| rest.strip_prefix('=')) else {
                continue;
            };
            if rest.starts_with('=') {
                continue; // a comparison
            }
            if let Some(n) = number(rest) {
                out.push((key.to_string(), n));
            }
        }
    }
    out
}

/// Retry settings on a line: (key, value). Flags without a value
/// (`pytest.mark.flaky`) read as one retry.
fn retry_settings(code: &str) -> Vec<(String, u64)> {
    let mut out = settings(code, &["retries", "reruns", "retry", "maxRetries"]);
    for name in [".retries", ".retryTimes"] {
        if let Some(rest) = call(code, name) {
            if let Some(n) = number(rest) {
                out.push(("retries".into(), n));
            }
        }
    }
    for flag in ["--retries", "--reruns"] {
        if let Some(i) = code.find(flag) {
            let rest = code[i + flag.len()..].trim_start_matches(['=', ' ']);
            if let Some(n) = number(rest) {
                out.push(("retries".into(), n));
            }
        }
    }
    if code.contains("mark.flaky") || code.trim_start().starts_with("@flaky") {
        out.push(("flaky".into(), 1));
    }
    out
}

/// Timeout settings and calls on a line: (key, value).
pub fn timeout_settings(code: &str) -> Vec<(String, u64)> {
    let mut out = settings(code, TIMEOUT_KEYS);
    for name in [
        "test.setTimeout",
        "jest.setTimeout",
        "this.timeout",
        "implicitly_wait",
        "implicitlyWait",
        "set_page_load_timeout",
    ] {
        if let Some(rest) = call(code, name) {
            out.push((name.to_string(), number(rest).unwrap_or(1)));
        }
    }
    if call(code, "test.slow").is_some() {
        out.push(("test.slow".into(), 3));
    }
    out
}

/// What a wait on the line is, if it is one.
pub fn wait_on(code: &str) -> Option<(Kind, String)> {
    for name in [
        "waitForTimeout",
        "browser.pause",
        "driver.sleep",
        "page.waitForTimeout",
    ] {
        if call(code, name).is_some() {
            return Some((Kind::FixedWait, name.to_string()));
        }
    }
    if let Some(rest) = call(code, "cy.wait") {
        if starts_with_number(rest) {
            return Some((Kind::FixedWait, "cy.wait with a number".into()));
        }
    }
    for name in [
        "sleep",
        "time.sleep",
        "Thread.sleep",
        "asyncio.sleep",
        "time.Sleep",
        "usleep",
    ] {
        if call(code, name).is_some() {
            return Some((Kind::FixedWait, name.to_string()));
        }
    }
    for name in ["delay", "wait", "pause", "timeout"] {
        if let Some(rest) = call(code, name) {
            if starts_with_number(rest) {
                return Some((Kind::FixedWait, format!("{name} with a number")));
            }
        }
    }
    let set_timeout = call(code, "setTimeout").is_some()
        && call(code, "test.setTimeout").is_none()
        && call(code, "jest.setTimeout").is_none();
    if set_timeout {
        return Some((Kind::FixedWait, "setTimeout".into()));
    }
    for name in [
        "expect.poll",
        ".toPass",
        "waitUntil",
        "wait_until",
        "cy.waitUntil",
        "waitForFunction",
        "setInterval",
    ] {
        if call(code, name).is_some() {
            return Some((Kind::Poll, name.trim_start_matches('.').to_string()));
        }
    }
    None
}

/// The added and removed code lines of each hunk of a patch.
fn hunks(path: &str, patch: &str) -> Vec<(Vec<String>, Vec<String>)> {
    let mut out: Vec<(Vec<String>, Vec<String>)> = Vec::new();
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
        let Some(last) = out.last_mut() else { continue };
        if let Some(b) = l.strip_prefix('+') {
            last.0.push(b.to_string());
        } else if let Some(b) = l.strip_prefix('-') {
            last.1.push(code_of(path, b));
        }
    }
    out
}

fn highest(values: &[(String, u64)], key: &str) -> Option<u64> {
    values
        .iter()
        .filter(|(k, _)| k == key)
        .map(|(_, v)| *v)
        .max()
}

/// Waits, polls, retries and timeouts that `files` (path, status, patch) add
/// to test code.
pub fn added_waits(files: &[(String, String, String)]) -> Vec<Finding> {
    let mut out = Vec::new();
    for (path, _, patch) in files {
        if !is_test_code(path) {
            continue;
        }
        let all = hunks(path, patch);
        let removed: Vec<&String> = all.iter().flat_map(|(_, r)| r).collect();
        let removed_retries: Vec<(String, u64)> =
            removed.iter().flat_map(|c| retry_settings(c)).collect();
        let removed_timeouts: Vec<(String, u64)> =
            removed.iter().flat_map(|c| timeout_settings(c)).collect();
        let removed_waits: Vec<String> = removed
            .iter()
            .filter_map(|c| wait_on(c).map(|(_, w)| w))
            .collect();
        let mut seen: std::collections::HashSet<(Kind, String)> = Default::default();
        for (added, _) in &all {
            for raw in added {
                let code = code_of(path, raw);
                if code.is_empty() {
                    continue;
                }
                let line = raw.trim().to_string();
                if let Some((kind, what)) = wait_on(&code) {
                    // Moving an existing wait (same call, removed elsewhere)
                    // adds nothing.
                    if !removed_waits.contains(&what) {
                        out.push(Finding {
                            path: path.clone(),
                            kind,
                            line: line.clone(),
                            what,
                        });
                        continue;
                    }
                }
                for (k, v) in retry_settings(&code) {
                    let was = highest(&removed_retries, &k);
                    if was.is_none_or(|w| v > w)
                        && v > 0
                        && seen.insert((Kind::Retries, line.clone()))
                    {
                        out.push(Finding {
                            path: path.clone(),
                            kind: Kind::Retries,
                            line: line.clone(),
                            what: match was {
                                Some(w) => format!("{k} {w} to {v}"),
                                None => format!("{k} {v}"),
                            },
                        });
                    }
                }
                for (k, v) in timeout_settings(&code) {
                    let was = highest(&removed_timeouts, &k);
                    if was.is_none_or(|w| v > w) && seen.insert((Kind::Timeout, line.clone())) {
                        out.push(Finding {
                            path: path.clone(),
                            kind: Kind::Timeout,
                            line: line.clone(),
                            what: match was {
                                Some(w) => format!("{k} {w} to {v}"),
                                None => format!("{k} {v}"),
                            },
                        });
                    }
                }
            }
        }
    }
    out
}

/// A read of a whole collection of elements.
fn collection_read(code: &str) -> Option<&'static str> {
    [
        (".all()", ".all()"),
        ("$$(", "$$"),
        ("$$eval(", "$$eval"),
        (".evaluateAll(", "evaluateAll"),
        ("findElements(", "findElements"),
        ("find_elements(", "find_elements"),
        ("querySelectorAll(", "querySelectorAll"),
        (".allTextContents(", "allTextContents"),
        (".allInnerTexts(", "allInnerTexts"),
        (".count()", "count()"),
        (".each(", "each"),
    ]
    .iter()
    .find(|(p, _)| code.contains(p))
    .map(|(_, w)| *w)
}

/// A loop or a poll.
fn loop_on(code: &str) -> bool {
    let c = code.trim_start();
    c.starts_with("for ")
        || c.starts_with("for(")
        || c.starts_with("while ")
        || c.starts_with("while(")
        || c.starts_with("do {")
        || code.contains("for await")
        || code.contains(".forEach(")
        || code.contains(".each(")
        || matches!(wait_on(code), Some((Kind::Poll, _)))
        || (code.contains(".map(") && code.contains("await"))
}

/// Loops and polls over whole collections that `files` add to shared
/// helpers (page objects, helpers, fixtures). A hunk counts when its added
/// lines both loop (or poll) and read a whole collection.
pub fn row_walks(files: &[(String, String, String)]) -> Vec<Finding> {
    let mut out = Vec::new();
    for (path, _, patch) in files {
        if !is_shared_helper(path) {
            continue;
        }
        for (added, _) in hunks(path, patch) {
            let codes: Vec<(String, String)> = added
                .iter()
                .map(|raw| (code_of(path, raw), raw.trim().to_string()))
                .filter(|(c, _)| !c.is_empty())
                .collect();
            let looping = codes.iter().find(|(c, _)| loop_on(c));
            let reading = codes.iter().find_map(|(c, _)| collection_read(c));
            if let (Some((_, line)), Some(what)) = (looping, reading) {
                out.push(Finding {
                    path: path.clone(),
                    kind: Kind::RowWalk,
                    line: line.clone(),
                    what: format!("loops over `{what}` on every call"),
                });
            }
        }
    }
    out
}

/// What the rules refuse in a diff, given whether the failing run's report
/// showed a timing failure (`timing_evidence`). Waits are allowed with that
/// evidence; a row walk in a shared helper never is.
pub fn refused(files: &[(String, String, String)], timing_evidence: bool) -> Vec<Finding> {
    let mut out = if timing_evidence {
        Vec::new()
    } else {
        added_waits(files)
    };
    out.extend(row_walks(files));
    out
}

/// The fix round's instructions for refused findings.
pub fn fix_feedback(findings: &[Finding], timing_evidence: bool) -> String {
    let mut s = String::from(
        "The change can't open as a pull request yet. It adds to test code what the rules for failing and flaky tests refuse:\n",
    );
    for f in findings {
        s.push_str(&format!("- {}\n", f.sentence()));
    }
    if findings.iter().any(|f| f.kind != Kind::RowWalk) && !timing_evidence {
        s.push_str(
            "\nNo report of the failing run shows the test failed on time (a timeout, or waiting for something that never came), so a wait, poll, retry or longer timeout is a guess. Remove them. Without failure evidence, change only what makes the next failure tell its cause: a clearer assertion message, the state captured at the failure (what the page showed, which dialog was open).\n",
        );
    }
    if findings.iter().any(|f| f.kind == Kind::RowWalk) {
        s.push_str(
            "\nA shared helper runs in many tests: never loop or poll over every row or card. Wait on the one element the helper reads (a locator filtered to that row), with the framework's own auto-waiting assertion.\n",
        );
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(path: &str, patch: &str) -> Vec<(String, String, String)> {
        vec![(path.into(), "modified".into(), patch.into())]
    }

    #[test]
    fn fixed_waits_and_polls_in_tests_are_found() {
        let cases = [
            (
                "e2e/items.spec.ts",
                "+  await page.waitForTimeout(500);",
                Kind::FixedWait,
            ),
            (
                "tests/cart.test.js",
                "+  await new Promise(r => setTimeout(r, 200));",
                Kind::FixedWait,
            ),
            ("cypress/e2e/a.cy.ts", "+  cy.wait(1000)", Kind::FixedWait),
            (
                "test/specs/login.e2e.js",
                "+    await browser.pause(300)",
                Kind::FixedWait,
            ),
            ("tests/test_login.py", "+    time.sleep(2)", Kind::FixedWait),
            (
                "e2e/items.spec.ts",
                "+  await expect.poll(() => rows.count()).toBe(3);",
                Kind::Poll,
            ),
            (
                "e2e/items.spec.ts",
                "+  await expect(async () => { await go(); }).toPass();",
                Kind::Poll,
            ),
            (
                "e2e/items.spec.ts",
                "+  await page.waitForFunction(() => window.ready);",
                Kind::Poll,
            ),
        ];
        for (path, patch, kind) in cases {
            let got = added_waits(&f(path, patch));
            assert_eq!(got.len(), 1, "{path}: {patch}");
            assert_eq!(got[0].kind, kind, "{patch}");
        }
    }

    #[test]
    fn retries_and_timeouts_count_only_when_raised() {
        let cfg = "playwright.config.ts";
        let up = added_waits(&f(cfg, "-  retries: 0,\n+  retries: 2,"));
        assert_eq!(up.len(), 1);
        assert_eq!(up[0].kind, Kind::Retries);
        assert_eq!(up[0].what, "retries 0 to 2");
        assert!(added_waits(&f(cfg, "-  retries: 2,\n+  retries: 1,")).is_empty());
        let t = added_waits(&f(cfg, "-  timeout: 30_000,\n+  timeout: 60_000,"));
        assert_eq!(t[0].kind, Kind::Timeout);
        assert_eq!(t[0].what, "timeout 30000 to 60000");
        let opt = added_waits(&f(
            "e2e/a.spec.ts",
            "+  await expect(dialog).toBeVisible({ timeout: 15000 });",
        ));
        assert_eq!(opt[0].kind, Kind::Timeout);
        assert_eq!(
            added_waits(&f("e2e/a.spec.ts", "+  test.slow();"))[0].kind,
            Kind::Timeout
        );
        assert_eq!(
            added_waits(&f("tests/a.test.ts", "+jest.retryTimes(3)"))[0].kind,
            Kind::Retries
        );
        assert_eq!(
            added_waits(&f("tests/test_a.py", "+@pytest.mark.flaky(reruns=2)"))[0].kind,
            Kind::Retries
        );
        assert_eq!(
            added_waits(&f(
                "cypress.config.ts",
                "+    defaultCommandTimeout: 10000,"
            ))[0]
                .kind,
            Kind::Timeout
        );
    }

    #[test]
    fn strings_comments_app_code_and_moved_waits_are_left_alone() {
        assert!(
            added_waits(&f("e2e/a.spec.ts", "+  // await page.waitForTimeout(500)")).is_empty()
        );
        assert!(added_waits(&f(
            "e2e/a.spec.ts",
            "+  log('waitForTimeout(500) removed');"
        ))
        .is_empty());
        assert!(added_waits(&f("src/app/retry.ts", "+  setTimeout(flush, 100);")).is_empty());
        assert!(added_waits(&f("e2e/a.spec.ts", "+  cy.wait('@save')")).is_empty());
        assert!(added_waits(&f("e2e/a.spec.ts", "+  await waitForSave(page);")).is_empty());
        assert!(added_waits(&f(
            "e2e/a.spec.ts",
            "@@ -1 +1 @@\n-  await page.waitForTimeout(500);\n@@ -9 +9 @@\n+  await page.waitForTimeout(500);"
        ))
        .is_empty());
        // A comparison is not a setting.
        assert!(added_waits(&f("e2e/a.spec.ts", "+  if (timeout == 5) {}")).is_empty());
    }

    #[test]
    fn helpers_are_told_from_specs() {
        assert!(is_shared_helper("e2e/pages/items-page.ts"));
        assert!(is_shared_helper("tests/support/commands.ts"));
        assert!(is_shared_helper("packages/web-tests/page-objects/table.ts"));
        assert!(!is_shared_helper("e2e/items.spec.ts"));
        assert!(!is_shared_helper("playwright.config.ts"));
        assert!(!is_shared_helper("src/components/table.tsx"));
        assert!(is_test_code("playwright.config.ts"));
    }

    #[test]
    fn helpers_that_walk_every_row_are_refused() {
        let walk = "@@ -10,3 +10,8 @@\n+  async rowNamed(name: string) {\n+    for (const row of await this.rows.all()) {\n+      if ((await row.textContent())?.includes(name)) return row;\n+    }\n+  }";
        let got = row_walks(&f("e2e/pages/items-page.ts", walk));
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].kind, Kind::RowWalk);
        assert!(got[0].what.contains(".all()"));
        let poll = "+    await expect.poll(async () => (await this.cards.allTextContents()).join()).toContain(name);";
        assert_eq!(row_walks(&f("e2e/pages/cards.ts", poll)).len(), 1);
        let cy = "+  cy.get('[data-row]').each(($r) => { names.push($r.text()) })";
        assert_eq!(row_walks(&f("cypress/support/commands.ts", cy)).len(), 1);
        // Waiting on the one element is the right shape.
        let one = "+  const row = this.rows.filter({ hasText: name });\n+  await expect(row).toBeVisible();";
        assert!(row_walks(&f("e2e/pages/items-page.ts", one)).is_empty());
        // The same loop in a spec is not a shared helper.
        assert!(row_walks(&f("e2e/items.spec.ts", walk)).is_empty());
    }

    #[test]
    fn evidence_of_a_timing_failure_allows_waits_but_never_row_walks() {
        let files = vec![
            (
                "e2e/a.spec.ts".to_string(),
                "modified".to_string(),
                "+  await expect(dialog).toBeVisible({ timeout: 15000 });".to_string(),
            ),
            (
                "e2e/pages/items.ts".to_string(),
                "modified".to_string(),
                "+  for (const r of await this.rows.all()) {}".to_string(),
            ),
        ];
        assert_eq!(refused(&files, false).len(), 2);
        let with = refused(&files, true);
        assert_eq!(with.len(), 1);
        assert_eq!(with[0].kind, Kind::RowWalk);
        let fb = fix_feedback(&refused(&files, false), false);
        assert!(fb.contains("is a guess") && fb.contains("one element"));
    }
}
