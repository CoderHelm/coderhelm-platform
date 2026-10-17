//! Reads test reports into [`ParsedTest`]s, keeping every attempt.
//!
//! Supported: Playwright's JSON reporter, Playwright's HTML report (it embeds
//! the same results, so repos that only upload the HTML report need no
//! change), JUnit XML (including the
//! `flakyFailure` / `rerunFailure` elements retrying runners write) and
//! Cucumber JSON (WebdriverIO's cucumber reporter). A test that appears more
//! than once in one report (some reporters write each retry as its own entry)
//! is folded into one test with several attempts.

use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use serde_json::Value;

use super::model::{Attempt, AttemptError, AttemptStatus, Framework, ParsedTest};
use super::signature;

/// Largest report read. Bigger files are skipped, not truncated.
pub const MAX_REPORT_BYTES: usize = 50 * 1024 * 1024;
/// Most tests taken from one report.
const MAX_TESTS: usize = 20_000;
const MAX_MESSAGE: usize = 2_000;

/// Where attachments can be found: the run's page, since per-file links into
/// an artifact zip aren't possible.
#[derive(Debug, Clone, Default)]
pub struct Links {
    pub artifacts_url: Option<String>,
}

/// Terminal colour codes, which Playwright writes into error messages.
static ANSI: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r"\x1b\[[0-9;]*[A-Za-z]").expect("valid pattern")
});

fn error(message: &str, stack: &str) -> AttemptError {
    let message = ANSI.replace_all(message, "");
    let message = message.as_ref();
    AttemptError {
        message: crate::truncate_str(message.trim(), MAX_MESSAGE).to_string(),
        signature: signature::signature(message, stack),
        selector: signature::selector(message),
    }
}

/// Recognises the report type from its content and parses it. `None` when the
/// file isn't a report we understand.
pub fn parse_any(name: &str, bytes: &[u8], links: &Links) -> Option<Vec<ParsedTest>> {
    if bytes.len() > MAX_REPORT_BYTES {
        return None;
    }
    let lower = name.to_lowercase();
    if lower.ends_with(".html") {
        return parse_playwright_html(bytes, links);
    }
    if lower.ends_with(".xml") {
        let text = std::str::from_utf8(bytes).ok()?;
        if !text.contains("<testsuite") && !text.contains("<testcase") {
            return None;
        }
        return parse_junit(text).ok();
    }
    if lower.ends_with(".json") {
        let v: Value = serde_json::from_slice(bytes).ok()?;
        if v.get("suites").is_some() && v.get("config").is_some() {
            return Some(parse_playwright(&v, links));
        }
        if v.as_array()
            .is_some_and(|a| a.iter().any(|f| f.get("elements").is_some()))
        {
            return Some(parse_cucumber(&v));
        }
    }
    None
}

// ── Playwright ────────────────────────────────────────────────────────────

fn pw_status(s: &str) -> AttemptStatus {
    match s {
        "passed" => AttemptStatus::Passed,
        "timedOut" => AttemptStatus::TimedOut,
        "skipped" => AttemptStatus::Skipped,
        "interrupted" => AttemptStatus::Interrupted,
        _ => AttemptStatus::Failed,
    }
}

fn pw_attempt(r: &Value, links: &Links) -> Attempt {
    let status = pw_status(r["status"].as_str().unwrap_or("failed"));
    let err = r["error"]
        .as_object()
        .map(|_| &r["error"])
        .or_else(|| r["errors"].get(0));
    let error = if status.is_failure() {
        err.map(|e| {
            error(
                e["message"].as_str().unwrap_or("Failed"),
                e["stack"].as_str().unwrap_or(""),
            )
        })
        .or_else(|| {
            Some(error(
                if status == AttemptStatus::TimedOut {
                    "Test timed out"
                } else {
                    "Failed"
                },
                "",
            ))
        })
    } else {
        None
    };
    let has = |kind: &str| {
        r["attachments"]
            .as_array()
            .is_some_and(|a| a.iter().any(|x| x["name"].as_str() == Some(kind)))
    };
    let link = |kind: &str| {
        if has(kind) {
            links.artifacts_url.clone()
        } else {
            None
        }
    };
    Attempt {
        retry: r["retry"].as_u64().unwrap_or(0) as u32,
        status,
        duration_ms: r["duration"].as_f64().unwrap_or(0.0).max(0.0) as u64,
        error,
        trace_url: link("trace"),
        video_url: link("video"),
        screenshot_url: link("screenshot"),
    }
}

fn pw_walk(suite: &Value, path: &mut Vec<String>, links: &Links, out: &mut Vec<ParsedTest>) {
    let title = suite["title"].as_str().unwrap_or("");
    let file = suite["file"].as_str().unwrap_or("");
    // The top suite of each file is the file itself, not a describe block.
    let pushed = !title.is_empty() && title != file;
    if pushed {
        path.push(title.to_string());
    }
    for spec in suite["specs"].as_array().into_iter().flatten() {
        let spec_file = spec["file"].as_str().unwrap_or(file).to_string();
        let tags: Vec<String> = spec["tags"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|t| t.as_str().map(|s| s.trim_start_matches('@').to_string()))
            .collect();
        for test in spec["tests"].as_array().into_iter().flatten() {
            if out.len() >= MAX_TESTS {
                return;
            }
            let attempts: Vec<Attempt> = test["results"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|r| pw_attempt(r, links))
                .collect();
            let attempts = if attempts.is_empty() {
                // Declared but never run (e.g. `test.skip`).
                vec![Attempt {
                    retry: 0,
                    status: AttemptStatus::Skipped,
                    duration_ms: 0,
                    error: None,
                    trace_url: None,
                    video_url: None,
                    screenshot_url: None,
                }]
            } else {
                attempts
            };
            out.push(ParsedTest {
                framework: Framework::Playwright,
                file: spec_file.clone(),
                suite: path.clone(),
                title: spec["title"].as_str().unwrap_or("").to_string(),
                project: test["projectName"]
                    .as_str()
                    .filter(|p| !p.is_empty())
                    .map(str::to_string),
                tags: tags.clone(),
                attempts,
            });
        }
    }
    for child in suite["suites"].as_array().into_iter().flatten() {
        pw_walk(child, path, links, out);
    }
    if pushed {
        path.pop();
    }
}

pub fn parse_playwright(report: &Value, links: &Links) -> Vec<ParsedTest> {
    let mut out = Vec::new();
    for suite in report["suites"].as_array().into_iter().flatten() {
        pw_walk(suite, &mut Vec::new(), links, &mut out);
    }
    fold_duplicates(out)
}

// ── Playwright HTML report ────────────────────────────────────────────────

/// Most bytes read from any one file inside an HTML report's embedded zip.
const MAX_HTML_PART: u64 = 50 * 1024 * 1024;

/// The zip of results Playwright embeds in its HTML report, as a
/// `data:application/zip;base64,` URL (in a script variable in older versions,
/// a `<template>` element in newer ones).
fn html_report_zip(bytes: &[u8]) -> Option<Vec<u8>> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    let text = std::str::from_utf8(bytes).ok()?;
    const MARK: &str = "data:application/zip;base64,";
    let rest = &text[text.find(MARK)? + MARK.len()..];
    let end = rest.find(['<', '"', '`', '\'']).unwrap_or(rest.len());
    STANDARD.decode(rest[..end].trim()).ok()
}

/// Reads the tests from a Playwright HTML report (`index.html`). Each test
/// keeps every attempt, like the JSON reporter, and gets the same id.
pub fn parse_playwright_html(bytes: &[u8], links: &Links) -> Option<Vec<ParsedTest>> {
    use std::io::Read;
    let zip_bytes = html_report_zip(bytes)?;
    let mut zip = zip::ZipArchive::new(std::io::Cursor::new(zip_bytes)).ok()?;
    let mut read = |name: &str| -> Option<Value> {
        let f = zip.by_name(name).ok()?;
        if f.size() > MAX_HTML_PART {
            return None;
        }
        let mut buf = Vec::new();
        f.take(MAX_HTML_PART).read_to_end(&mut buf).ok()?;
        serde_json::from_slice(&buf).ok()
    };
    let report = read("report.json")?;
    let mut out = Vec::new();
    for summary in report["files"].as_array().into_iter().flatten() {
        let Some(file_id) = summary["fileId"].as_str() else {
            continue;
        };
        // The per-file part has every attempt; the summary has only the last.
        let Some(file) = read(&format!("{file_id}.json")) else {
            continue;
        };
        let file_name = file["fileName"].as_str().unwrap_or("");
        for t in file["tests"].as_array().into_iter().flatten() {
            if out.len() >= MAX_TESTS {
                break;
            }
            let mut attempts: Vec<Attempt> = t["results"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|r| pw_attempt(r, links))
                .collect();
            if attempts.is_empty() {
                attempts.push(Attempt {
                    retry: 0,
                    status: AttemptStatus::Skipped,
                    duration_ms: 0,
                    error: None,
                    trace_url: None,
                    video_url: None,
                    screenshot_url: None,
                });
            }
            out.push(ParsedTest {
                framework: Framework::Playwright,
                file: t["location"]["file"]
                    .as_str()
                    .unwrap_or(file_name)
                    .to_string(),
                suite: t["path"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|p| p.as_str().map(str::to_string))
                    .collect(),
                title: t["title"].as_str().unwrap_or("").to_string(),
                project: t["projectName"]
                    .as_str()
                    .filter(|p| !p.is_empty())
                    .map(str::to_string),
                tags: t["tags"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|x| x.as_str().map(|s| s.trim_start_matches('@').to_string()))
                    .collect(),
                attempts,
            });
        }
    }
    Some(fold_duplicates(out))
}

// ── JUnit XML ─────────────────────────────────────────────────────────────

fn attr(e: &BytesStart, name: &str) -> Option<String> {
    e.attributes()
        .flatten()
        .find(|a| a.key.as_ref() == name.as_bytes())
        .and_then(|a| a.unescape_value().ok().map(|v| v.into_owned()))
}

#[derive(Default)]
struct JCase {
    file: String,
    suite: Vec<String>,
    title: String,
    time_ms: u64,
    /// (kind, message, body): kind is failure | error | skipped | flaky | rerun
    marks: Vec<(String, String, String)>,
}

fn junit_case(c: JCase) -> ParsedTest {
    let failed = |retry: u32, msg: &str, body: &str, duration_ms: u64| Attempt {
        retry,
        status: AttemptStatus::Failed,
        duration_ms,
        error: Some(error(if msg.is_empty() { body } else { msg }, body)),
        trace_url: None,
        video_url: None,
        screenshot_url: None,
    };
    let mut attempts = Vec::new();
    let is = |k: &str| c.marks.iter().any(|(kind, _, _)| kind == k);
    if is("skipped") && !is("failure") && !is("error") {
        attempts.push(Attempt {
            retry: 0,
            status: AttemptStatus::Skipped,
            duration_ms: 0,
            error: None,
            trace_url: None,
            video_url: None,
            screenshot_url: None,
        });
    } else if is("failure") || is("error") {
        // The main failure, then every rerun that also failed.
        let mut retry = 0;
        for (kind, msg, body) in &c.marks {
            if kind == "failure" || kind == "error" {
                attempts.push(failed(retry, msg, body, c.time_ms));
                retry += 1;
            }
        }
        for (kind, msg, body) in &c.marks {
            if kind == "rerun" {
                attempts.push(failed(retry, msg, body, 0));
                retry += 1;
            }
        }
    } else {
        // Passed in the end; each flaky mark is an earlier failed attempt.
        let mut retry = 0;
        for (kind, msg, body) in &c.marks {
            if kind == "flaky" {
                attempts.push(failed(retry, msg, body, 0));
                retry += 1;
            }
        }
        attempts.push(Attempt {
            retry,
            status: AttemptStatus::Passed,
            duration_ms: c.time_ms,
            error: None,
            trace_url: None,
            video_url: None,
            screenshot_url: None,
        });
    }
    ParsedTest {
        framework: Framework::Junit,
        file: c.file,
        suite: c.suite,
        title: c.title,
        project: None,
        tags: Vec::new(),
        attempts,
    }
}

pub fn parse_junit(xml: &str) -> Result<Vec<ParsedTest>, String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut out = Vec::new();
    let mut suites: Vec<(String, String)> = Vec::new(); // (name, file)
    let mut case: Option<JCase> = None;
    let mut mark: Option<(String, String, String)> = None;

    let mark_kind = |name: &[u8]| -> Option<&'static str> {
        match name {
            b"failure" => Some("failure"),
            b"error" => Some("error"),
            b"skipped" => Some("skipped"),
            b"flakyFailure" | b"flakyError" => Some("flaky"),
            b"rerunFailure" | b"rerunError" => Some("rerun"),
            _ => None,
        }
    };
    let start_case = |e: &BytesStart, suites: &[(String, String)]| {
        let (sname, sfile) = suites.last().cloned().unwrap_or_default();
        let file = attr(e, "file")
            .or_else(|| (!sfile.is_empty()).then_some(sfile.clone()))
            .or_else(|| attr(e, "classname"))
            .unwrap_or_default();
        let mut suite = Vec::new();
        if !sname.is_empty() && sname != file {
            suite.push(sname);
        }
        JCase {
            file,
            suite,
            title: attr(e, "name").unwrap_or_default(),
            time_ms: attr(e, "time")
                .and_then(|t| t.parse::<f64>().ok())
                .map(|t| (t * 1000.0) as u64)
                .unwrap_or(0),
            marks: Vec::new(),
        }
    };

    loop {
        match reader.read_event().map_err(|e| format!("junit xml: {e}"))? {
            Event::Start(e) => match e.name().as_ref() {
                b"testsuite" => suites.push((
                    attr(&e, "name").unwrap_or_default(),
                    attr(&e, "file").unwrap_or_default(),
                )),
                b"testcase" => case = Some(start_case(&e, &suites)),
                n => {
                    if let (Some(kind), true) = (mark_kind(n), case.is_some()) {
                        mark = Some((
                            kind.to_string(),
                            attr(&e, "message").unwrap_or_default(),
                            String::new(),
                        ));
                    }
                }
            },
            Event::Empty(e) => match e.name().as_ref() {
                b"testcase" => {
                    if out.len() < MAX_TESTS {
                        out.push(junit_case(start_case(&e, &suites)));
                    }
                }
                n => {
                    if let (Some(kind), Some(c)) = (mark_kind(n), case.as_mut()) {
                        c.marks.push((
                            kind.to_string(),
                            attr(&e, "message").unwrap_or_default(),
                            String::new(),
                        ));
                    }
                }
            },
            Event::Text(t) => {
                if let Some(m) = mark.as_mut() {
                    if let Ok(s) = t.unescape() {
                        m.2.push_str(&s);
                    }
                }
            }
            Event::CData(t) => {
                if let Some(m) = mark.as_mut() {
                    m.2.push_str(&String::from_utf8_lossy(t.as_ref()));
                }
            }
            Event::End(e) => match e.name().as_ref() {
                b"testsuite" => {
                    suites.pop();
                }
                b"testcase" => {
                    if let Some(c) = case.take() {
                        if out.len() < MAX_TESTS {
                            out.push(junit_case(c));
                        }
                    }
                }
                n => {
                    if mark_kind(n).is_some() {
                        if let (Some(m), Some(c)) = (mark.take(), case.as_mut()) {
                            c.marks.push(m);
                        }
                    }
                }
            },
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(fold_duplicates(out))
}

// ── Cucumber JSON ─────────────────────────────────────────────────────────

pub fn parse_cucumber(report: &Value) -> Vec<ParsedTest> {
    let mut out = Vec::new();
    for feature in report.as_array().into_iter().flatten() {
        let file = feature["uri"].as_str().unwrap_or("").to_string();
        let feature_name = feature["name"].as_str().unwrap_or("").to_string();
        let mut background_error: Option<AttemptError> = None;
        for el in feature["elements"].as_array().into_iter().flatten() {
            if out.len() >= MAX_TESTS {
                break;
            }
            let steps: Vec<&Value> = ["before", "steps", "after"]
                .iter()
                .flat_map(|k| el[*k].as_array().into_iter().flatten())
                .collect();
            let duration_ns: f64 = steps
                .iter()
                .filter_map(|s| s["result"]["duration"].as_f64())
                .sum();
            let failed_step = steps
                .iter()
                .find(|s| s["result"]["status"].as_str() == Some("failed"));
            let step_error = failed_step.map(|s| {
                let msg = s["result"]["error_message"]
                    .as_str()
                    .unwrap_or("Step failed");
                let step = format!(
                    "{}{}",
                    s["keyword"].as_str().unwrap_or(""),
                    s["name"].as_str().unwrap_or("")
                );
                let first = msg.lines().next().unwrap_or(msg);
                error(&format!("{first} (step: {})", step.trim()), msg)
            });
            if el["type"].as_str() == Some("background") {
                background_error = step_error;
                continue;
            }
            let error = step_error.or_else(|| background_error.take());
            let all_skipped = !steps.is_empty()
                && steps.iter().all(|s| {
                    matches!(
                        s["result"]["status"].as_str(),
                        Some("skipped") | Some("pending") | Some("undefined")
                    )
                });
            let status = if error.is_some() {
                AttemptStatus::Failed
            } else if all_skipped {
                AttemptStatus::Skipped
            } else {
                AttemptStatus::Passed
            };
            out.push(ParsedTest {
                framework: Framework::Cucumber,
                file: file.clone(),
                suite: if feature_name.is_empty() {
                    vec![]
                } else {
                    vec![feature_name.clone()]
                },
                title: el["name"].as_str().unwrap_or("").to_string(),
                project: None,
                tags: el["tags"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|t| {
                        t["name"]
                            .as_str()
                            .map(|n| n.trim_start_matches('@').to_string())
                    })
                    .collect(),
                attempts: vec![Attempt {
                    retry: 0,
                    status,
                    duration_ms: (duration_ns / 1_000_000.0) as u64,
                    error,
                    trace_url: None,
                    video_url: None,
                    screenshot_url: None,
                }],
            });
        }
    }
    fold_duplicates(out)
}

/// Folds repeats of the same test into one, in order, numbering the attempts.
pub fn fold_duplicates(tests: Vec<ParsedTest>) -> Vec<ParsedTest> {
    let mut order: Vec<String> = Vec::new();
    let mut by_id: std::collections::HashMap<String, ParsedTest> = std::collections::HashMap::new();
    for t in tests {
        let id = t.id();
        match by_id.get_mut(&id) {
            Some(existing) => existing.attempts.extend(t.attempts),
            None => {
                order.push(id.clone());
                by_id.insert(id, t);
            }
        }
    }
    order
        .into_iter()
        .filter_map(|id| by_id.remove(&id))
        .map(|mut t| {
            for (i, a) in t.attempts.iter_mut().enumerate() {
                a.retry = i as u32;
            }
            t
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::e2e::model::Outcome;
    use serde_json::json;

    fn links() -> Links {
        Links {
            artifacts_url: Some("https://github.com/acme/web/actions/runs/1#artifacts".into()),
        }
    }

    #[test]
    fn playwright_keeps_every_attempt_suite_and_project() {
        let report = json!({
            "config": {"rootDir": "/w/e2e"},
            "suites": [{
                "title": "specs/join.spec.ts", "file": "specs/join.spec.ts",
                "specs": [],
                "suites": [{
                    "title": "Online join", "file": "specs/join.spec.ts",
                    "specs": [{
                        "title": "pays by card", "file": "specs/join.spec.ts", "tags": ["@join"],
                        "tests": [{
                            "projectName": "chromium",
                            "results": [
                                {"retry": 0, "status": "failed", "duration": 1200.5,
                                 "error": {"message": "Timed out 15000ms waiting for locator('#pay')", "stack": "    at x (/w/e2e/specs/join.spec.ts:3:1)"},
                                 "attachments": [{"name": "trace", "path": "/tmp/trace.zip"}]},
                                {"retry": 1, "status": "passed", "duration": 900, "attachments": []}
                            ]
                        }]
                    }, {
                        "title": "is skipped", "file": "specs/join.spec.ts",
                        "tests": [{"projectName": "chromium", "results": []}]
                    }]
                }]
            }]
        });
        let tests = parse_any("results.json", report.to_string().as_bytes(), &links()).unwrap();
        assert_eq!(tests.len(), 2);
        let t = &tests[0];
        assert_eq!(t.framework, Framework::Playwright);
        assert_eq!(t.suite, vec!["Online join"]);
        assert_eq!(t.project.as_deref(), Some("chromium"));
        assert_eq!(t.tags, vec!["join"]);
        assert_eq!(t.attempts.len(), 2);
        assert_eq!(Outcome::of(&t.attempts), Outcome::Flaky);
        let e = t.attempts[0].error.as_ref().unwrap();
        assert_eq!(e.selector.as_deref(), Some("#pay"));
        assert!(t.attempts[0].trace_url.is_some());
        assert!(t.attempts[1].trace_url.is_none());
        assert_eq!(t.attempts[0].duration_ms, 1200);
        assert_eq!(Outcome::of(&tests[1].attempts), Outcome::Skipped);
        assert_eq!(t.full_title(), "Online join › pays by card");
    }

    #[test]
    fn junit_reads_flaky_and_rerun_failures() {
        let xml = r#"<?xml version="1.0"?>
<testsuites>
  <testsuite name="CheckoutTest" file="src/test/CheckoutTest.java">
    <testcase name="passes" classname="CheckoutTest" time="0.5"/>
    <testcase name="flips" classname="CheckoutTest" time="1.2">
      <flakyFailure message="expected 1 but was 2" type="AssertionError">at CheckoutTest.flips(CheckoutTest.java:10)</flakyFailure>
    </testcase>
    <testcase name="breaks" classname="CheckoutTest" time="2">
      <failure message="boom"><![CDATA[stack here]]></failure>
      <rerunFailure message="boom again"/>
    </testcase>
    <testcase name="later" classname="CheckoutTest"><skipped/></testcase>
  </testsuite>
</testsuites>"#;
        let tests = parse_any("TEST-x.xml", xml.as_bytes(), &Links::default()).unwrap();
        let by = |n: &str| tests.iter().find(|t| t.title == n).unwrap();
        assert_eq!(tests.len(), 4);
        assert_eq!(Outcome::of(&by("passes").attempts), Outcome::Passed);
        assert_eq!(by("passes").attempts[0].duration_ms, 500);
        assert_eq!(by("passes").file, "src/test/CheckoutTest.java");
        assert_eq!(by("passes").suite, vec!["CheckoutTest"]);
        let flips = &by("flips").attempts;
        assert_eq!(flips.len(), 2);
        assert_eq!(Outcome::of(flips), Outcome::Flaky);
        assert_eq!(
            flips[0].error.as_ref().unwrap().message,
            "expected 1 but was 2"
        );
        let breaks = &by("breaks").attempts;
        assert_eq!(breaks.len(), 2);
        assert_eq!(Outcome::of(breaks), Outcome::Failed);
        assert_eq!(Outcome::of(&by("later").attempts), Outcome::Skipped);
    }

    #[test]
    fn cucumber_scenarios_and_retries() {
        let report = json!([{
            "uri": "test/features/Login/login.feature", "name": "Login",
            "elements": [
                {"type": "scenario", "name": "Signs in", "tags": [{"name": "@smoke"}],
                 "steps": [{"keyword": "Given ", "name": "I open the app", "result": {"status": "passed", "duration": 2000000000u64}},
                           {"keyword": "Then ", "name": "I see home", "result": {"status": "failed", "duration": 1000000000u64, "error_message": "element ('~Home') still not displayed\n at x"}}]},
                {"type": "scenario", "name": "Signs in", "tags": [{"name": "@smoke"}],
                 "steps": [{"keyword": "Given ", "name": "I open the app", "result": {"status": "passed", "duration": 1000000000u64}}]},
                {"type": "scenario", "name": "Pending one",
                 "steps": [{"keyword": "Given ", "name": "x", "result": {"status": "skipped"}}]}
            ]
        }]);
        let tests = parse_any(
            "cucumber.json",
            report.to_string().as_bytes(),
            &Links::default(),
        )
        .unwrap();
        assert_eq!(tests.len(), 2);
        let t = &tests[0];
        assert_eq!(t.framework, Framework::Cucumber);
        assert_eq!(t.tags, vec!["smoke"]);
        assert_eq!(t.attempts.len(), 2, "the retry is folded in");
        assert_eq!(t.attempts[1].retry, 1);
        assert_eq!(Outcome::of(&t.attempts), Outcome::Flaky);
        assert_eq!(t.attempts[0].duration_ms, 3000);
        let e = t.attempts[0].error.as_ref().unwrap();
        assert!(e.message.contains("(step: Then I see home)"));
        assert_eq!(e.selector.as_deref(), Some("~Home"));
        assert_eq!(Outcome::of(&tests[1].attempts), Outcome::Skipped);
    }

    #[test]
    fn unknown_files_are_ignored() {
        assert!(parse_any("package.json", br#"{"name":"x"}"#, &Links::default()).is_none());
        assert!(parse_any("pom.xml", b"<project/>", &Links::default()).is_none());
        assert!(parse_any("notes.txt", b"hello", &Links::default()).is_none());
        assert!(parse_any("bad.json", b"{not json", &Links::default()).is_none());
    }

    /// A real Playwright 1.64 HTML report's embedded results (one passing,
    /// one flaky and one failing test, retries 1), wrapped the way the report
    /// wraps them.
    fn html_report(template: bool) -> Vec<u8> {
        use base64::{engine::general_purpose::STANDARD, Engine};
        use std::io::Write;
        let report = include_str!("testdata/pw_html_report.json");
        let file = include_str!("testdata/pw_html_file.json");
        let file_id = serde_json::from_str::<Value>(file).unwrap()["fileId"]
            .as_str()
            .unwrap()
            .to_string();
        let mut buf = Vec::new();
        {
            let mut z = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            let o = zip::write::SimpleFileOptions::default();
            z.start_file("report.json", o).unwrap();
            z.write_all(report.as_bytes()).unwrap();
            z.start_file(format!("{file_id}.json"), o).unwrap();
            z.write_all(file.as_bytes()).unwrap();
            z.finish().unwrap();
        }
        let data = format!("data:application/zip;base64,{}", STANDARD.encode(&buf));
        let html = if template {
            format!("<html><body><template id=\"playwrightReportBase64\">{data}</template></body></html>")
        } else {
            format!("<script>window.playwrightReportBase64 = \"{data}\";</script>")
        };
        html.into_bytes()
    }

    #[test]
    fn a_playwright_html_report_gives_every_attempt() {
        for template in [true, false] {
            let tests = parse_any(
                "playwright-report-1/index.html",
                &html_report(template),
                &Links::default(),
            )
            .unwrap();
            assert_eq!(tests.len(), 3);
            let by = |t: &str| tests.iter().find(|x| x.title.starts_with(t)).unwrap();
            assert_eq!(by("passes").attempts.len(), 1);
            let flaky = by("flaky once");
            assert_eq!(
                flaky.attempts.iter().map(|a| a.status).collect::<Vec<_>>(),
                vec![AttemptStatus::Failed, AttemptStatus::Passed]
            );
            assert_eq!(flaky.suite, vec!["checkout".to_string()]);
            assert_eq!(flaky.file, "sample.spec.ts");
            let failing = by("always fails");
            assert_eq!(failing.attempts.len(), 2);
            assert_eq!(failing.tags, vec!["smoke".to_string()]);
            let msg = &failing.attempts[0].error.as_ref().unwrap().message;
            assert!(msg.starts_with("Error: price shown"), "{msg}");
            assert!(!msg.contains('\x1b'), "colour codes are stripped");
        }
        // The same test from the HTML report and the JSON reporter has one id.
        let json = r#"{"config":{},"suites":[{"title":"sample.spec.ts","file":"sample.spec.ts","specs":[],"suites":[{"title":"checkout","file":"sample.spec.ts","specs":[{"title":"passes","file":"sample.spec.ts","tags":[],"tests":[{"projectName":"","results":[{"retry":0,"status":"passed","duration":3}]}]}]}]}]}"#;
        let from_json = parse_any("results.json", json.as_bytes(), &Links::default()).unwrap();
        let from_html = parse_any("index.html", &html_report(true), &Links::default()).unwrap();
        assert_eq!(
            from_json[0].id(),
            from_html.iter().find(|t| t.title == "passes").unwrap().id()
        );
        // An HTML page with no embedded report is not a report.
        assert!(parse_any("index.html", b"<html>hello</html>", &Links::default()).is_none());
    }
}
