//! What a failing test's report says about the failure: the real error, the
//! line it stopped at, the step that failed, and the page as it was (the
//! dialogs open, the elements shown) when it failed.
//!
//! A fix for a flaky test starts from these facts, quoted in the pull request,
//! never from a guess. Playwright's HTML report keeps them in the zip embedded
//! in its `index.html` (`report.json` plus one JSON part per spec file); the
//! page state is the `error-context` attachment, a Markdown file in the
//! report's `data/` folder; traces sit in `data/` too and the trace viewer in
//! `trace/`. Playwright's JSON reporter, JUnit XML and Cucumber JSON carry the
//! error and stack only.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::model::ParsedTest;

/// Error text kept per failure.
const MAX_ERROR: usize = 1_500;
/// Code frame kept per failure.
const MAX_SNIPPET: usize = 1_200;
/// Page state kept per failure.
const MAX_PAGE: usize = 2_500;
/// Failures kept from one report.
pub const MAX_FAILURES: usize = 40;

/// One failed attempt of one test, as its report tells it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureExtract {
    pub test: String,
    pub file: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default)]
    pub run_id: u64,
    #[serde(default)]
    pub attempt: u32,
    /// Playwright's retry index of the attempt (0 = first try).
    #[serde(default)]
    pub retry: u32,
    /// `failed` | `timedOut`.
    pub status: String,
    /// The error, without terminal colours.
    pub error: String,
    /// `file:line:column` the error points at.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub location: String,
    /// The code frame around that line.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub snippet: String,
    /// The step that failed (`Click getByRole('button', { name: 'Save' })`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub failed_step: String,
    /// The page at the failure, as Playwright's page snapshot (YAML).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub page_state: String,
    /// Dialogs open at the failure (`dialog "Delete item"`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dialogs: Vec<String>,
    /// Report-relative paths of the page-state file, trace, screenshot and
    /// video (`data/<hash>.md`, `data/<hash>.zip`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<String>,
    /// Where the report was read (`s3://…/index.html`, an artifact name).
    #[serde(default)]
    pub source: String,
}

static ANSI: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r"\x1b\[[0-9;]*[A-Za-z]").expect("valid pattern")
});

/// Text without terminal colour codes.
pub fn plain(s: &str) -> String {
    ANSI.replace_all(s, "").into_owned()
}

fn clip(s: &str, n: usize) -> String {
    crate::truncate_str(s.trim(), n).to_string()
}

/// The deepest step with an error, as `title`.
fn failed_step(steps: &Value) -> Option<String> {
    for s in steps.as_array().into_iter().flatten() {
        if let Some(inner) = failed_step(&s["steps"]) {
            return Some(inner);
        }
        if s.get("error").is_some_and(|e| !e.is_null()) {
            return s["title"].as_str().map(str::to_string);
        }
    }
    None
}

/// `file:line:column` from the last `at …` line of an error's text.
fn location_in(text: &str) -> String {
    text.lines()
        .rev()
        .map(str::trim)
        .find_map(|l| l.strip_prefix("at "))
        .map(|l| {
            let l = l.trim_end_matches(')');
            let l = l.rsplit('(').next().unwrap_or(l);
            // Keep from the last path segment that looks like a test folder.
            l.rsplit_once('/')
                .map(|(_, f)| f.to_string())
                .unwrap_or_else(|| l.to_string())
        })
        .unwrap_or_default()
}

fn attachments_of(result: &Value) -> Vec<String> {
    result["attachments"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|a| a["path"].as_str())
        .filter(|p| !p.contains(".."))
        .map(str::to_string)
        .collect()
}

/// The `error-context` attachment's path (the page at the failure).
pub fn context_path(f: &FailureExtract) -> Option<&str> {
    f.attachments
        .iter()
        .map(String::as_str)
        .find(|p| p.ends_with(".md"))
}

fn from_pw_result(test: &Value, file: &str, r: &Value) -> Option<FailureExtract> {
    let status = r["status"].as_str().unwrap_or("");
    if !matches!(status, "failed" | "timedOut") {
        return None;
    }
    let err = r["errors"]
        .get(0)
        .filter(|e| !e.is_null())
        .or_else(|| r.get("error").filter(|e| e.is_object()));
    let message = err
        .and_then(|e| e["message"].as_str().or(e.as_str()))
        .unwrap_or(if status == "timedOut" {
            "Test timed out"
        } else {
            "Failed"
        });
    let message = plain(message);
    let snippet = err
        .and_then(|e| e["codeframe"].as_str().or(e["snippet"].as_str()))
        .map(plain)
        .unwrap_or_default();
    // The message carries the code frame after a blank line: keep the error.
    let error = message
        .split("\n\n  ")
        .next()
        .unwrap_or(&message)
        .to_string();
    Some(FailureExtract {
        test: test["title"].as_str().unwrap_or("").to_string(),
        file: test["location"]["file"]
            .as_str()
            .unwrap_or(file)
            .to_string(),
        project: test["projectName"]
            .as_str()
            .filter(|p| !p.is_empty())
            .map(str::to_string),
        retry: r["retry"].as_u64().unwrap_or(0) as u32,
        status: status.to_string(),
        error: clip(&error, MAX_ERROR),
        location: location_in(&message),
        snippet: clip(&snippet, MAX_SNIPPET),
        failed_step: failed_step(&r["steps"]).unwrap_or_default(),
        attachments: attachments_of(r),
        ..Default::default()
    })
}

/// The failures in a Playwright HTML report (`index.html`). `None` when the
/// file isn't one.
pub fn playwright_html_failures(bytes: &[u8]) -> Option<Vec<FailureExtract>> {
    use std::io::Read;
    let zip_bytes = super::parse::html_report_zip(bytes)?;
    let mut zip = zip::ZipArchive::new(std::io::Cursor::new(zip_bytes)).ok()?;
    let mut read = |name: &str| -> Option<Value> {
        let f = zip.by_name(name).ok()?;
        let mut buf = Vec::new();
        f.take(super::parse::MAX_REPORT_BYTES as u64)
            .read_to_end(&mut buf)
            .ok()?;
        serde_json::from_slice(&buf).ok()
    };
    let report = read("report.json")?;
    let mut out = Vec::new();
    for summary in report["files"].as_array().into_iter().flatten() {
        let Some(id) = summary["fileId"].as_str() else {
            continue;
        };
        let Some(part) = read(&format!("{id}.json")) else {
            continue;
        };
        let file = part["fileName"].as_str().unwrap_or("");
        for t in part["tests"].as_array().into_iter().flatten() {
            for r in t["results"].as_array().into_iter().flatten() {
                if out.len() >= MAX_FAILURES {
                    return Some(out);
                }
                if let Some(f) = from_pw_result(t, file, r) {
                    out.push(f);
                }
            }
        }
    }
    Some(out)
}

fn pw_json_walk(suite: &Value, out: &mut Vec<FailureExtract>) {
    let file = suite["file"].as_str().unwrap_or("");
    for spec in suite["specs"].as_array().into_iter().flatten() {
        for test in spec["tests"].as_array().into_iter().flatten() {
            let t = serde_json::json!({
                "title": spec["title"],
                "location": { "file": spec["file"].as_str().unwrap_or(file) },
                "projectName": test["projectName"],
            });
            for r in test["results"].as_array().into_iter().flatten() {
                if out.len() >= MAX_FAILURES {
                    return;
                }
                if let Some(f) = from_pw_result(&t, file, r) {
                    out.push(f);
                }
            }
        }
    }
    for child in suite["suites"].as_array().into_iter().flatten() {
        pw_json_walk(child, out);
    }
}

/// The failures in a Playwright JSON report.
pub fn playwright_json_failures(report: &Value) -> Vec<FailureExtract> {
    let mut out = Vec::new();
    for s in report["suites"].as_array().into_iter().flatten() {
        pw_json_walk(s, &mut out);
    }
    out
}

/// The failures among parsed tests (JUnit, Cucumber): error text only.
pub fn parsed_failures(tests: &[ParsedTest]) -> Vec<FailureExtract> {
    let mut out = Vec::new();
    for t in tests {
        for a in t.attempts.iter().filter(|a| a.status.is_failure()) {
            if out.len() >= MAX_FAILURES {
                return out;
            }
            let message = a
                .error
                .as_ref()
                .map(|e| e.message.clone())
                .unwrap_or_default();
            out.push(FailureExtract {
                test: t.full_title(),
                file: t.file.clone(),
                project: t.project.clone(),
                retry: a.retry,
                status: if a.status == super::model::AttemptStatus::TimedOut {
                    "timedOut".into()
                } else {
                    "failed".into()
                },
                location: location_in(&message),
                error: clip(&plain(&message), MAX_ERROR),
                ..Default::default()
            });
        }
    }
    out
}

/// The failures in any report Coderhelm reads, by its file name.
pub fn failures_in(name: &str, bytes: &[u8]) -> Option<Vec<FailureExtract>> {
    let lower = name.to_lowercase();
    if lower.ends_with(".html") {
        return playwright_html_failures(bytes);
    }
    if lower.ends_with(".json") {
        let v: Value = serde_json::from_slice(bytes).ok()?;
        if v.get("suites").is_some() && v.get("config").is_some() {
            return Some(playwright_json_failures(&v));
        }
    }
    let tests = super::parse::parse_any(name, bytes, &Default::default())?;
    Some(parsed_failures(&tests))
}

/// The page snapshot in an `error-context` file, and the dialogs open in it.
pub fn page_state(markdown: &str) -> (String, Vec<String>) {
    // The snapshot is the first fenced block after "Page snapshot"; older
    // files are the snapshot alone.
    let after = markdown
        .find("Page snapshot")
        .map(|i| &markdown[i..])
        .unwrap_or(markdown);
    let snapshot = match after.find("```") {
        Some(i) => {
            let body = &after[i + 3..];
            let body = body.split_once('\n').map(|(_, b)| b).unwrap_or(body);
            body.split("```").next().unwrap_or(body)
        }
        None => after,
    };
    let dialogs: Vec<String> = snapshot
        .lines()
        .map(str::trim)
        .filter_map(|l| l.strip_prefix("- "))
        .filter(|l| l.starts_with("dialog") || l.starts_with("alertdialog"))
        .map(|l| l.trim_end_matches(':').to_string())
        .take(10)
        .collect();
    (clip(snapshot, MAX_PAGE), dialogs)
}

/// Whether the failure is about time: a timeout, or waiting for something
/// that didn't happen. Only such evidence justifies a wait in test code.
pub fn is_timing(f: &FailureExtract) -> bool {
    let e = f.error.to_lowercase();
    f.status == "timedOut"
        || [
            "timeout",
            "timed out",
            "waiting for",
            "exceeded while",
            "not visible",
            "to be visible",
            "tobevisible",
            "not stable",
            "detached from the dom",
            "intercepts pointer events",
            "element is not attached",
        ]
        .iter()
        .any(|w| e.contains(w))
}

/// Whether the failure is of one of `names` (a test title, a part of one, or
/// its spec file). Empty `names` matches every failure.
pub fn is_of(f: &FailureExtract, names: &[String]) -> bool {
    if names.is_empty() {
        return true;
    }
    let title = f.test.to_lowercase();
    let file = f.file.to_lowercase();
    names.iter().map(|n| n.trim().to_lowercase()).any(|n| {
        !n.is_empty()
            && (title.contains(&n) || n.contains(&title) && !title.is_empty() || file.ends_with(&n))
    })
}

/// The failures as Markdown, quoted for agents and the pull request.
pub fn markdown(failures: &[FailureExtract]) -> String {
    let mut out = String::new();
    for f in failures {
        out.push_str(&format!(
            "- `{}` in `{}`{}, run {} attempt {}{}: {}\n",
            f.test,
            f.file,
            f.project
                .as_deref()
                .map(|p| format!(" ({p})"))
                .unwrap_or_default(),
            f.run_id,
            f.attempt.max(1),
            if f.retry > 0 {
                format!(" (retry {})", f.retry)
            } else {
                String::new()
            },
            f.status
        ));
        out.push_str(&format!(
            "  ```\n  {}\n  ```\n",
            f.error.replace('\n', "\n  ")
        ));
        if !f.location.is_empty() {
            out.push_str(&format!("  at `{}`\n", f.location));
        }
        if !f.failed_step.is_empty() {
            out.push_str(&format!("  failed step: {}\n", f.failed_step));
        }
        if !f.dialogs.is_empty() {
            out.push_str(&format!(
                "  open at the failure: {}\n",
                f.dialogs.join("; ")
            ));
        }
        if !f.page_state.is_empty() {
            out.push_str(&format!(
                "  page at the failure:\n  ```yaml\n  {}\n  ```\n",
                f.page_state.replace('\n', "\n  ")
            ));
        }
        if !f.source.is_empty() {
            out.push_str(&format!("  report: {}\n", f.source));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn html_report() -> Vec<u8> {
        use base64::{engine::general_purpose::STANDARD, Engine};
        use std::io::Write;
        let mut buf = std::io::Cursor::new(Vec::new());
        {
            let mut z = zip::ZipWriter::new(&mut buf);
            let o: zip::write::SimpleFileOptions = Default::default();
            z.start_file("report.json", o).unwrap();
            z.write_all(include_bytes!("testdata/pw_html_report.json"))
                .unwrap();
            let file: Value =
                serde_json::from_str(include_str!("testdata/pw_html_file.json")).unwrap();
            z.start_file(format!("{}.json", file["fileId"].as_str().unwrap()), o)
                .unwrap();
            z.write_all(include_bytes!("testdata/pw_html_file.json"))
                .unwrap();
            z.finish().unwrap();
        }
        format!(
            "<html><template id=\"playwrightReportBase64\">data:application/zip;base64,{}</template></html>",
            STANDARD.encode(buf.into_inner())
        )
        .into_bytes()
    }

    #[test]
    fn html_reports_give_each_failed_attempt_with_its_error_and_page_file() {
        let fails = playwright_html_failures(&html_report()).unwrap();
        // "flaky once" failed once, "always fails" twice.
        assert_eq!(fails.len(), 3);
        let first = &fails[0];
        assert_eq!(first.test, "flaky once");
        assert_eq!(first.status, "failed");
        assert!(first
            .error
            .starts_with("Error: expect(received).toBe(expected)"));
        assert!(!first.error.contains('\u{1b}'));
        assert!(first.snippet.contains("> 5 |"));
        assert_eq!(first.location, "sample.spec.ts:5:63");
        assert_eq!(first.failed_step, "Expect \"toBe\"");
        assert_eq!(
            context_path(first),
            Some("data/fb514be06904c765250f1cd80540174c3de63df0.md")
        );
        assert!(fails[1].error.contains("price shown"));
        assert_eq!(fails[2].retry, 1);
        assert!(failures_in("report/index.html", &html_report()).is_some());
        assert!(playwright_html_failures(b"<html></html>").is_none());
    }

    #[test]
    fn page_state_finds_open_dialogs() {
        let md = "# Test info\n\n- Name: deletes\n\n# Page snapshot\n\n```yaml\n- banner:\n  - link \"Home\"\n- dialog \"Delete item\":\n  - heading \"Are you sure?\" [level=2]\n  - button \"Cancel\"\n  - button \"Delete\"\n- alertdialog \"Unsaved changes\"\n```\n";
        let (state, dialogs) = page_state(md);
        assert!(state.starts_with("- banner:"));
        assert!(!state.contains("```"));
        assert_eq!(
            dialogs,
            vec!["dialog \"Delete item\"", "alertdialog \"Unsaved changes\""]
        );
        let (bare, none) = page_state("- main:\n  - button \"Go\"");
        assert!(bare.contains("button"));
        assert!(none.is_empty());
    }

    #[test]
    fn timing_failures_are_told_from_assertion_failures() {
        let mut f = FailureExtract {
            error: "Error: expect(locator).toBeVisible() failed\nTimeout: 5000ms\nwaiting for getByRole('dialog')".into(),
            status: "failed".into(),
            ..Default::default()
        };
        assert!(is_timing(&f));
        f.error = "Error: expect(received).toBe(expected)\nExpected: 3\nReceived: 2".into();
        assert!(!is_timing(&f));
        f.status = "timedOut".into();
        assert!(is_timing(&f));
    }

    #[test]
    fn failures_are_matched_to_the_tests_a_ticket_names() {
        let f = FailureExtract {
            test: "deletes an item".into(),
            file: "tests/items.spec.ts".into(),
            ..Default::default()
        };
        assert!(is_of(&f, &[]));
        assert!(is_of(&f, &["Deletes an item".into()]));
        assert!(is_of(&f, &["items.spec.ts".into()]));
        assert!(is_of(&f, &["checkout › deletes an item".into()]));
        assert!(!is_of(&f, &["creates an item".into()]));
    }

    #[test]
    fn json_and_junit_reports_give_their_errors() {
        let v: Value = serde_json::json!({
            "config": {},
            "suites": [{ "file": "a.spec.ts", "specs": [{ "title": "t", "file": "a.spec.ts", "tests": [{
                "projectName": "chromium",
                "results": [{ "status": "timedOut", "retry": 0, "errors": [{ "message": "Test timeout of 30000ms exceeded." }] }]
            }]}]}]
        });
        let f = playwright_json_failures(&v);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].project.as_deref(), Some("chromium"));
        assert!(is_timing(&f[0]));
        let xml = r#"<testsuite name="s"><testcase name="logs in" classname="auth" file="auth.test.ts"><failure message="boom">stack</failure></testcase></testsuite>"#;
        let j = failures_in("junit.xml", xml.as_bytes()).unwrap();
        assert_eq!(j.len(), 1);
        assert_eq!(j[0].error, "boom");
        let md = markdown(&j);
        assert!(md.contains("boom") && md.contains("logs in"));
    }
}
