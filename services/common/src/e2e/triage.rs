//! Why an E2E test failed on a PR, in one calm line, and the checks that keep
//! an automated fix honest.

use serde::{Deserialize, Serialize};

use super::coverage::{matches, TestMap};
use super::model::{Outcome, ParsedTest, State, TestRecord};
use super::signature::top_frames;

/// What a failure on a PR most likely means.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// The test was already known to be flaky (or is quarantined).
    KnownFlaky,
    /// Many tests failed together: the environment broke, not the change.
    BadRun,
    /// It fails on the default branch too.
    FailingOnMain,
    /// Nothing explains it but the change.
    Caused,
}

/// Decides from what was known before this run.
pub fn verdict(before: Option<&TestRecord>, bad_run: bool, default_branch: &str) -> Verdict {
    if bad_run {
        return Verdict::BadRun;
    }
    let Some(r) = before else {
        return Verdict::Caused;
    };
    if r.health == State::Broken {
        return Verdict::FailingOnMain;
    }
    if r.health == State::Flaky || r.quarantined {
        return Verdict::KnownFlaky;
    }
    let recent_main: Vec<_> = r
        .history
        .iter()
        .rev()
        .filter(|e| e.branch == default_branch && !e.bad_run && e.outcome != Outcome::Skipped)
        .take(3)
        .collect();
    if recent_main.iter().any(|e| e.outcome == Outcome::Failed) {
        return Verdict::FailingOnMain;
    }
    if recent_main.iter().any(|e| e.outcome == Outcome::Flaky) {
        return Verdict::KnownFlaky;
    }
    Verdict::Caused
}

/// Changed files that the failure points at: files in its stack, or files in
/// the test map area its tags name. Only app code counts: a spec or helper
/// the change shares with the failing test is named in every error and is
/// no evidence the change caused it.
pub fn related_changes(t: &ParsedTest, changed: &[String], map: Option<&TestMap>) -> Vec<String> {
    let changed: Vec<String> = changed
        .iter()
        .filter(|f| !super::fix_guard::is_test_code(f))
        .cloned()
        .collect();
    let changed = &changed[..];
    let mut hits: Vec<String> = Vec::new();
    for a in &t.attempts {
        let Some(e) = &a.error else { continue };
        // The stack isn't kept, but the message often names the file.
        for f in changed {
            let name = f.rsplit('/').next().unwrap_or(f);
            if name.len() > 3 && e.message.contains(name) && !hits.contains(f) {
                hits.push(f.clone());
            }
        }
    }
    if let Some(m) = map {
        for area in m
            .areas
            .iter()
            .filter(|a| t.tags.iter().any(|tag| tag.eq_ignore_ascii_case(&a.tag)))
        {
            for f in changed {
                if area.paths.iter().any(|g| matches(g, f)) && !hits.contains(f) {
                    hits.push(f.clone());
                }
            }
        }
    }
    hits.truncate(5);
    hits
}

/// Files a stack trace passes through, for matching against a diff.
pub fn stack_files(stack: &str) -> Vec<String> {
    top_frames(stack, 8)
        .into_iter()
        .map(|f| f.rsplit(' ').next().unwrap_or(&f).to_string())
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Line {
    pub test_id: String,
    pub title: String,
    pub verdict: Verdict,
    pub text: String,
    #[serde(default)]
    pub error: String,
    #[serde(default)]
    pub related: Vec<String>,
}

fn short(s: &str, n: usize) -> String {
    let one = s.lines().next().unwrap_or("").trim();
    let t = crate::truncate_str(one, n);
    if t.len() < one.len() {
        format!("{t}…")
    } else {
        t.to_string()
    }
}

/// One calm line for a failing test.
pub fn line(
    t: &ParsedTest,
    v: Verdict,
    before: Option<&TestRecord>,
    run: (usize, usize),
    related: Vec<String>,
    default_branch: &str,
) -> Line {
    let title = t.full_title();
    let error = t
        .attempts
        .iter()
        .rev()
        .find_map(|a| a.error.as_ref().map(|e| short(&e.message, 160)))
        .unwrap_or_default();
    let text = match v {
        Verdict::KnownFlaky => {
            let stats = before.map(|r| super::stats::stats(r, chrono::Utc::now()));
            match stats {
                Some(s) if s.runs_30d > 0 => format!(
                    "`{title}` failed, but it is known to be flaky: it failed or flipped in {:.0}% of its last {} runs.",
                    (s.fail_rate_30d + s.flake_rate_30d) * 100.0,
                    s.runs_30d
                ),
                _ => format!("`{title}` failed, but it is known to be flaky."),
            }
        }
        Verdict::BadRun => format!(
            "`{title}` failed in a run where {} of {} tests failed together, which points to the environment rather than this change.",
            run.1, run.0
        ),
        Verdict::FailingOnMain => format!("`{title}` is failing on `{default_branch}` too, so this change didn't cause it."),
        Verdict::Caused => {
            if related.is_empty() {
                format!("`{title}` failed and nothing outside this change explains it. Error: `{error}`")
            } else {
                format!(
                    "`{title}` failed and looks related to this change ({}). Error: `{error}`",
                    related.iter().map(|f| format!("`{f}`")).collect::<Vec<_>>().join(", ")
                )
            }
        }
    };
    Line {
        test_id: t.id(),
        title,
        verdict: v,
        text,
        error,
        related,
    }
}

// ── keeping automated fixes honest ────────────────────────────────────────

const ASSERTION_MARKERS: &[&str] = &[
    "expect(",
    "assert",
    ".should(",
    ".should.",
    "toBe",
    "toHave",
    "toEqual",
    "toContain",
    "toMatch",
    "verify(",
    "Then ",
];

const SKIP_MARKERS: &[&str] = &[
    "test.skip",
    "test.fixme",
    "it.skip",
    "describe.skip",
    "xit(",
    "xdescribe(",
    "test.only",
    "it.only",
    "@skip",
    "@ignore",
    "@Disabled",
    "pytest.mark.skip",
];

/// Whether a path is a test (by folder or by file name), not app code.
pub fn is_test_file(path: &str) -> bool {
    let l = path.to_lowercase();
    let segs: Vec<&str> = l.split('/').collect();
    let name = segs.last().copied().unwrap_or("");
    segs[..segs.len().saturating_sub(1)].iter().any(|s| {
        matches!(
            *s,
            "e2e"
                | "test"
                | "tests"
                | "__tests__"
                | "spec"
                | "specs"
                | "features"
                | "cypress"
                | "playwright"
        )
    }) || [".test.", ".spec.", "_test.", "_spec.", ".e2e."]
        .iter()
        .any(|m| name.contains(m))
        || name.ends_with("test.java")
        || name.ends_with("test.kt")
        || name.ends_with(".feature")
}

fn is_assertion(line: &str) -> bool {
    let l = line.trim();
    !l.starts_with("//") && !l.starts_with('#') && ASSERTION_MARKERS.iter().any(|m| l.contains(m))
}

/// Why a diff weakens tests, or `None` when it doesn't. `files` are
/// (path, unified patch). A test file that loses assertions overall, gains a
/// skip, or is deleted is refused; a fix must make the test pass honestly.
pub fn weakens_tests(files: &[(String, String, String)]) -> Option<String> {
    for (path, status, patch) in files {
        if !is_test_file(path) {
            continue;
        }
        if status == "removed" {
            return Some(format!("it deletes the test file `{path}`"));
        }
        let (mut removed, mut added) = (0, 0);
        for l in patch.lines() {
            if l.starts_with("+++") || l.starts_with("---") {
                continue;
            }
            if let Some(body) = l.strip_prefix('-') {
                if is_assertion(body) {
                    removed += 1;
                }
            } else if let Some(body) = l.strip_prefix('+') {
                if is_assertion(body) {
                    added += 1;
                }
                if SKIP_MARKERS.iter().any(|m| body.contains(m)) {
                    return Some(format!("it skips a test in `{path}`"));
                }
            }
        }
        if removed > added {
            return Some(format!(
                "it removes {} assertion(s) from `{path}`",
                removed - added
            ));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::e2e::model::{Attempt, AttemptError, AttemptStatus, Framework, HistoryEntry};

    fn rec(health: State, history: Vec<(Outcome, &str)>) -> TestRecord {
        TestRecord {
            id: "x".into(),
            repo: "acme/web".into(),
            framework: Framework::Playwright,
            file: "a".into(),
            title: "t".into(),
            project: None,
            tags: vec![],
            health,
            quarantined: false,
            quarantined_since: None,
            quarantined_by: None,
            never_quarantine: false,
            owner: None,
            cause: None,
            last_flip_at: None,
            runs_since_flip: 0,
            fix_merged_at: None,
            ticket_url: None,
            fix_pr_url: None,
            fix_status: None,
            fix_branch: None,
            fix_phase_at: None,
            workflow_path: None,
            last_seen: String::new(),
            history: history
                .into_iter()
                .enumerate()
                .map(|(i, (o, b))| HistoryEntry {
                    run_key: format!("r{i}"),
                    sha: format!("s{i}"),
                    branch: b.into(),
                    pr: None,
                    at: chrono::Utc::now().to_rfc3339(),
                    outcome: o,
                    bad_run: false,
                    attempts: vec![],
                })
                .collect(),
            events: vec![],
        }
    }

    #[test]
    fn verdicts_follow_what_was_known() {
        assert_eq!(verdict(None, false, "main"), Verdict::Caused);
        assert_eq!(verdict(None, true, "main"), Verdict::BadRun);
        assert_eq!(
            verdict(Some(&rec(State::Flaky, vec![])), false, "main"),
            Verdict::KnownFlaky
        );
        assert_eq!(
            verdict(Some(&rec(State::Broken, vec![])), false, "main"),
            Verdict::FailingOnMain
        );
        assert_eq!(
            verdict(
                Some(&rec(
                    State::Healthy,
                    vec![(Outcome::Passed, "main"), (Outcome::Failed, "main")]
                )),
                false,
                "main"
            ),
            Verdict::FailingOnMain
        );
        assert_eq!(
            verdict(
                Some(&rec(
                    State::Healthy,
                    vec![(Outcome::Failed, "pr-3"), (Outcome::Passed, "main")]
                )),
                false,
                "main"
            ),
            Verdict::Caused
        );
        let mut q = rec(State::Healthy, vec![]);
        q.quarantined = true;
        assert_eq!(verdict(Some(&q), false, "main"), Verdict::KnownFlaky);
    }

    fn failing(msg: &str, tags: &[&str]) -> ParsedTest {
        ParsedTest {
            framework: Framework::Playwright,
            file: "specs/pay.spec.ts".into(),
            suite: vec!["Checkout".into()],
            title: "pays".into(),
            project: None,
            tags: tags.iter().map(|t| t.to_string()).collect(),
            attempts: vec![Attempt {
                retry: 0,
                status: AttemptStatus::Failed,
                duration_ms: 1,
                error: Some(AttemptError {
                    message: msg.into(),
                    signature: "s".into(),
                    selector: None,
                }),
                trace_url: None,
                video_url: None,
                screenshot_url: None,
            }],
        }
    }

    #[test]
    fn related_changes_come_from_the_message_and_the_map() {
        let map = crate::e2e::coverage::parse_test_map(
            r#"{"areas": {"checkout": {"tag": "@checkout", "paths": ["src/checkout/**"]}}}"#,
        )
        .unwrap();
        let changed = vec![
            "src/checkout/Pay.tsx".to_string(),
            "src/home/Hero.tsx".to_string(),
            "README.md".to_string(),
        ];
        let t = failing("TypeError in Hero.tsx line 4", &["checkout"]);
        let hits = related_changes(&t, &changed, Some(&map));
        assert_eq!(hits, vec!["src/home/Hero.tsx", "src/checkout/Pay.tsx"]);
        let l = line(&t, Verdict::Caused, None, (10, 1), hits, "main");
        assert!(l.text.starts_with(
            "`Checkout › pays` failed and looks related to this change (`src/home/Hero.tsx`"
        ));
        assert!(l.text.contains("Error: `TypeError in Hero.tsx line 4`"));
        let bad = line(&t, Verdict::BadRun, None, (40, 12), vec![], "main");
        assert!(bad.text.contains("12 of 40 tests failed together"));
    }

    #[test]
    fn a_spec_the_change_shares_with_the_failing_test_is_not_related() {
        let changed = vec![
            "e2e/classes.spec.ts".to_string(),
            "e2e/pages/classes.ts".to_string(),
            "src/billing/Invoice.tsx".to_string(),
        ];
        let t = failing(
            "expect(locator).toBeVisible() failed at e2e/classes.spec.ts:42 in classes.ts",
            &[],
        );
        assert!(related_changes(&t, &changed, None).is_empty());
    }

    #[test]
    fn weakened_tests_are_caught() {
        let ok_patch = "@@\n-    await page.waitForTimeout(500);\n+    await expect(page.getByTestId('pay')).toBeVisible();\n     expect(total).toBe(10);";
        assert_eq!(
            weakens_tests(&[(
                "e2e/specs/pay.spec.ts".into(),
                "modified".into(),
                ok_patch.into()
            )]),
            None
        );
        let removed = "@@\n-    await expect(page.getByText('Paid')).toBeVisible();\n+    await page.waitForTimeout(5000);";
        assert!(weakens_tests(&[(
            "e2e/specs/pay.spec.ts".into(),
            "modified".into(),
            removed.into()
        )])
        .unwrap()
        .contains("removes 1 assertion"));
        let skipped = "@@\n-test('pays', async () => {\n+test.skip('pays', async () => {";
        assert!(weakens_tests(&[(
            "e2e/specs/pay.spec.ts".into(),
            "modified".into(),
            skipped.into()
        )])
        .unwrap()
        .contains("skips"));
        assert!(weakens_tests(&[(
            "e2e/specs/pay.spec.ts".into(),
            "removed".into(),
            String::new()
        )])
        .unwrap()
        .contains("deletes"));
        // App code losing an assert-like word isn't a test change.
        let app = "@@\n-  assert(ready)";
        assert_eq!(
            weakens_tests(&[("src/lib/pay.ts".into(), "modified".into(), app.into())]),
            None
        );
        assert!(is_test_file("e2e/specs/pay.spec.ts"));
        assert!(is_test_file("apps/web/src/lib/pay.test.ts"));
        assert!(is_test_file("test/features/Login/login.feature"));
        assert!(!is_test_file("src/lib/latest.ts"));
        assert!(!is_test_file("src/contest/entry.tsx"));
        let comment = "@@\n-    // expect(x).toBe(1)";
        assert_eq!(
            weakens_tests(&[("e2e/a.spec.ts".into(), "modified".into(), comment.into())]),
            None
        );
    }
}
