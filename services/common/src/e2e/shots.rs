//! Screenshots the end-to-end reports already carry, for the Before and
//! After table of CoderHelm's own pull request.
//!
//! A pull request whose whole change was visual showed no screenshot, while
//! the team's end-to-end run on it had taken some. The reports CoderHelm
//! reads (`parse`, `attribution`) keep them:
//!
//! - Cucumber JSON (WebdriverIO, Appium): images embedded in a scenario's
//!   steps and hooks, base64 in the report itself;
//! - Playwright JSON: image attachments of a test's result, a file in the
//!   run's artifacts (`path`) or the image itself (`body`);
//! - Playwright's HTML report: image attachments under its `data/` folder.
//!
//! [`shots_in`] reads them per test, keyed by the test's id
//! ([`ParsedTest::id`]), and [`pick`] chooses the tests that show the change:
//! a test the change edits, a test the repository's test map ties to the
//! changed files, or a test whose name shares a word with a changed UI file.
//! The worker commits the images to the ref CoderHelm keeps its pull
//! request screenshots on in the repository itself (`refs/coderhelm/screenshots`,
//! next to the preview's page captures, at [`ref_path`]) and links them by
//! commit, so only people who can read the repository see them.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::coverage::TestMap;
use super::model::{test_id, Framework, HistoryEntry, Outcome, ParsedTest};

/// Largest image kept.
pub const MAX_IMAGE: usize = 5 * 1024 * 1024;

/// One test's screenshot in one report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shot {
    /// The test's id ([`ParsedTest::id`]).
    pub test_id: String,
    pub image: Image,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Image {
    /// The image itself (a Cucumber embedding, a Playwright `body`).
    Inline(Vec<u8>),
    /// A file of the run, as the report wrote its path.
    File(String),
}

/// The image's type, from its first bytes. Only PNG, JPEG and WebP are
/// kept.
pub fn image_type(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(&[0x89, b'P', b'N', b'G']) {
        Some("image/png")
    } else if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        Some("image/jpeg")
    } else if bytes.len() > 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        Some("image/webp")
    } else {
        None
    }
}

/// The file extension for an image type.
pub fn extension(content_type: &str) -> &'static str {
    match content_type {
        "image/jpeg" => "jpg",
        "image/webp" => "webp",
        _ => "png",
    }
}

fn is_image(content_type: &str) -> bool {
    matches!(
        content_type.trim().to_ascii_lowercase().as_str(),
        "image/png" | "image/jpeg" | "image/jpg" | "image/webp"
    )
}

fn decoded(data: &str) -> Option<Vec<u8>> {
    let clean: String = data.chars().filter(|c| !c.is_whitespace()).collect();
    let bytes = STANDARD.decode(clean).ok()?;
    (bytes.len() <= MAX_IMAGE && image_type(&bytes).is_some()).then_some(bytes)
}

/// The screenshots in one report file, the last one of each test (its last
/// attempt, its final state). Empty when the file is not a report this
/// reads or carries no image.
pub fn shots_in(name: &str, bytes: &[u8]) -> Vec<Shot> {
    if bytes.len() > super::parse::MAX_REPORT_BYTES {
        return Vec::new();
    }
    let lower = name.to_lowercase();
    let found = if lower.ends_with(".html") {
        playwright_html(bytes)
    } else if lower.ends_with(".json") {
        match serde_json::from_slice::<Value>(bytes) {
            Ok(v) if v.get("suites").is_some() && v.get("config").is_some() => playwright(&v),
            Ok(v)
                if v.as_array()
                    .is_some_and(|a| a.iter().any(|f| f.get("elements").is_some())) =>
            {
                cucumber(&v)
            }
            _ => Vec::new(),
        }
    } else {
        Vec::new()
    };
    last_per_test(found)
}

fn last_per_test(found: Vec<Shot>) -> Vec<Shot> {
    let mut out: Vec<Shot> = Vec::new();
    for s in found {
        match out.iter_mut().find(|x| x.test_id == s.test_id) {
            Some(x) => *x = s,
            None => out.push(s),
        }
    }
    out
}

// ── Cucumber JSON ─────────────────────────────────────────────────────────

fn cucumber(report: &Value) -> Vec<Shot> {
    let mut out = Vec::new();
    for feature in report.as_array().into_iter().flatten() {
        let file = feature["uri"].as_str().unwrap_or("");
        let feature_name = feature["name"].as_str().unwrap_or("");
        let suite: Vec<String> = if feature_name.is_empty() {
            vec![]
        } else {
            vec![feature_name.to_string()]
        };
        for el in feature["elements"].as_array().into_iter().flatten() {
            if el["type"].as_str() == Some("background") {
                continue;
            }
            let last = ["before", "steps", "after"]
                .iter()
                .flat_map(|k| el[*k].as_array().into_iter().flatten())
                .flat_map(|s| s["embeddings"].as_array().into_iter().flatten())
                .filter(|e| {
                    is_image(
                        e["mime_type"]
                            .as_str()
                            .or_else(|| e["media"]["type"].as_str())
                            .unwrap_or(""),
                    )
                })
                .filter_map(|e| e["data"].as_str().and_then(decoded))
                .next_back();
            if let Some(bytes) = last {
                out.push(Shot {
                    test_id: test_id(
                        Framework::Cucumber,
                        file,
                        &suite,
                        el["name"].as_str().unwrap_or(""),
                        None,
                    ),
                    image: Image::Inline(bytes),
                });
            }
        }
    }
    out
}

// ── Playwright ────────────────────────────────────────────────────────────

/// The last image attachment of a test's last result.
fn pw_image(results: &Value) -> Option<Image> {
    let last = results.as_array()?.last()?;
    last["attachments"]
        .as_array()?
        .iter()
        .filter(|a| is_image(a["contentType"].as_str().unwrap_or("")))
        .filter_map(|a| match (a["body"].as_str(), a["path"].as_str()) {
            (Some(b), _) if !b.is_empty() => decoded(b).map(Image::Inline),
            (_, Some(p)) if !p.is_empty() && !p.contains("..") => Some(Image::File(p.to_string())),
            _ => None,
        })
        .next_back()
}

fn project_of(test: &Value) -> Option<&str> {
    test["projectName"].as_str().filter(|p| !p.is_empty())
}

/// The same walk as `parse::parse_playwright`, so ids match.
fn pw_walk(suite: &Value, path: &mut Vec<String>, out: &mut Vec<Shot>) {
    let title = suite["title"].as_str().unwrap_or("");
    let file = suite["file"].as_str().unwrap_or("");
    let pushed = !title.is_empty() && title != file;
    if pushed {
        path.push(title.to_string());
    }
    for spec in suite["specs"].as_array().into_iter().flatten() {
        let spec_file = spec["file"].as_str().unwrap_or(file);
        for test in spec["tests"].as_array().into_iter().flatten() {
            if let Some(image) = pw_image(&test["results"]) {
                out.push(Shot {
                    test_id: test_id(
                        Framework::Playwright,
                        spec_file,
                        path,
                        spec["title"].as_str().unwrap_or(""),
                        project_of(test),
                    ),
                    image,
                });
            }
        }
    }
    for child in suite["suites"].as_array().into_iter().flatten() {
        pw_walk(child, path, out);
    }
    if pushed {
        path.pop();
    }
}

fn playwright(report: &Value) -> Vec<Shot> {
    let mut out = Vec::new();
    for suite in report["suites"].as_array().into_iter().flatten() {
        pw_walk(suite, &mut Vec::new(), &mut out);
    }
    out
}

/// The same reading as `parse::parse_playwright_html`, so ids match. The
/// attachments' paths are relative to the report (`data/<hash>.png`).
fn playwright_html(bytes: &[u8]) -> Vec<Shot> {
    use std::io::Read;
    let Some(zip_bytes) = super::parse::html_report_zip(bytes) else {
        return Vec::new();
    };
    let Ok(mut zip) = zip::ZipArchive::new(std::io::Cursor::new(zip_bytes)) else {
        return Vec::new();
    };
    let limit = super::parse::MAX_REPORT_BYTES as u64;
    let mut read = |name: &str| -> Option<Value> {
        let f = zip.by_name(name).ok()?;
        if f.size() > limit {
            return None;
        }
        let mut buf = Vec::new();
        f.take(limit).read_to_end(&mut buf).ok()?;
        serde_json::from_slice(&buf).ok()
    };
    let Some(report) = read("report.json") else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for summary in report["files"].as_array().into_iter().flatten() {
        let Some(file) = summary["fileId"]
            .as_str()
            .and_then(|id| read(&format!("{id}.json")))
        else {
            continue;
        };
        let file_name = file["fileName"].as_str().unwrap_or("");
        for t in file["tests"].as_array().into_iter().flatten() {
            let Some(image) = pw_image(&t["results"]) else {
                continue;
            };
            let suite: Vec<String> = t["path"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|p| p.as_str().map(str::to_string))
                .collect();
            out.push(Shot {
                test_id: test_id(
                    Framework::Playwright,
                    t["location"]["file"].as_str().unwrap_or(file_name),
                    &suite,
                    t["title"].as_str().unwrap_or(""),
                    project_of(t),
                ),
                image,
            });
        }
    }
    out
}

/// The run's file an image attachment names. `report` is the report's own
/// name in the run's folder, `names` every file there. A relative path is
/// read next to the report; an absolute one (the runner's disk) is the file
/// whose name ends with the most of its trailing folders, at least the
/// image's own folder: every Playwright test writes `test-finished-1.png`,
/// in a folder of its own.
pub fn resolve(report: &str, path: &str, names: &[String]) -> Option<String> {
    let p = path.replace('\\', "/");
    if p.split('/').any(|s| s == "..") {
        return None;
    }
    let absolute = p.starts_with('/') || p.get(1..2) == Some(":");
    if !absolute {
        let dir = report
            .rsplit_once('/')
            .map(|(d, _)| format!("{d}/"))
            .unwrap_or_default();
        let near = format!("{dir}{}", p.trim_start_matches("./"));
        if names.contains(&near) {
            return Some(near);
        }
    }
    let segs: Vec<&str> = p.split('/').filter(|s| !s.is_empty()).collect();
    for k in (2..=segs.len()).rev() {
        let tail = segs[segs.len() - k..].join("/");
        if let Some(n) = names
            .iter()
            .find(|n| **n == tail || n.ends_with(&format!("/{tail}")))
        {
            return Some(n.clone());
        }
    }
    None
}

// ── Which tests show the change ───────────────────────────────────────────

/// Why a test's screenshot is shown for a change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Why {
    /// The change edits the test itself.
    TestChanged,
    /// The repository's test map ties the test's area to a changed file.
    Mapped,
    /// The test's name shares this word with a changed UI file.
    Named(String),
}

impl Why {
    /// Stronger evidence first.
    fn rank(&self) -> u8 {
        match self {
            Why::TestChanged => 0,
            Why::Mapped => 1,
            Why::Named(_) => 2,
        }
    }

    pub fn words(&self) -> String {
        match self {
            Why::TestChanged => "this change edits the test".into(),
            Why::Mapped => "the repository's test map ties it to the changed files".into(),
            Why::Named(w) => format!("the test and a changed UI file share the word \"{w}\""),
        }
    }
}

/// Folder and file names that say nothing about what a file shows.
const GENERIC: &[&str] = &[
    "src",
    "app",
    "apps",
    "lib",
    "libs",
    "packages",
    "package",
    "components",
    "component",
    "screens",
    "screen",
    "pages",
    "page",
    "views",
    "view",
    "styles",
    "style",
    "index",
    "main",
    "common",
    "shared",
    "utils",
    "util",
    "hooks",
    "assets",
    "features",
    "feature",
    "layout",
    "layouts",
    "containers",
    "container",
    "modules",
    "module",
    "default",
    "mobile",
    "native",
    "client",
    "public",
    "test",
    "tests",
    "spec",
    "specs",
    "e2e",
    "ui",
    "tsx",
    "jsx",
    "scss",
    "css",
    "vue",
    "svelte",
    "html",
    "styled",
    "theme",
    "types",
    "button",
    "text",
    "item",
    "list",
    "card",
    "modal",
];

/// The words of a path or title: split on anything but letters and digits
/// and at camelCase humps, lowercased, at least four letters, none of
/// [`GENERIC`].
fn words(s: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut prev_lower = false;
    let mut push = |w: &mut String| {
        let l = w.to_ascii_lowercase();
        if l.len() >= 4 && !GENERIC.contains(&l.as_str()) && !out.contains(&l) {
            out.push(l);
        }
        w.clear();
    };
    for c in s.chars() {
        if !c.is_ascii_alphanumeric() {
            push(&mut cur);
            prev_lower = false;
            continue;
        }
        if c.is_ascii_uppercase() && prev_lower {
            push(&mut cur);
        }
        prev_lower = c.is_ascii_lowercase() || c.is_ascii_digit();
        cur.push(c);
    }
    push(&mut cur);
    out
}

fn same_file(a: &str, b: &str) -> bool {
    let (a, b) = (a.trim_start_matches("./"), b.trim_start_matches("./"));
    !a.is_empty()
        && !b.is_empty()
        && (a == b || a.ends_with(&format!("/{b}")) || b.ends_with(&format!("/{a}")))
}

/// Why `t`'s screenshot shows the change to `changed`, if it does.
pub fn why_shown(t: &ParsedTest, changed: &[String], map: Option<&TestMap>) -> Option<Why> {
    if changed.iter().any(|f| same_file(f, &t.file)) {
        return Some(Why::TestChanged);
    }
    if map.is_some() && !super::triage::related_changes(t, changed, map).is_empty() {
        return Some(Why::Mapped);
    }
    let named = words(&format!("{} {}", t.full_title(), t.file));
    changed
        .iter()
        .filter(|f| crate::pr_facts::is_ui(f))
        .flat_map(|f| words(f))
        .find(|w| named.contains(w))
        .map(Why::Named)
}

/// The tests whose screenshots show the change, best first: the tests that
/// passed in the end, a test the change edits before one the test map
/// names, before one that only shares a word.
pub fn pick<'a>(
    tests: &'a [ParsedTest],
    changed: &[String],
    map: Option<&TestMap>,
) -> Vec<(&'a ParsedTest, Why)> {
    let mut out: Vec<(&ParsedTest, Why)> = tests
        .iter()
        .filter(|t| matches!(Outcome::of(&t.attempts), Outcome::Passed | Outcome::Flaky))
        .filter_map(|t| why_shown(t, changed, map).map(|w| (t, w)))
        .collect();
    out.sort_by_key(|(_, w)| w.rank());
    out
}

// ── What the description shows ────────────────────────────────────────────

/// One side of the table: the kept image and the run it came from.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Side {
    /// The image's address on the screenshots ref, at the commit that
    /// added it.
    pub src: String,
    /// The run's page; empty for an upload with no run page.
    #[serde(default)]
    pub run_url: String,
}

/// The screenshots kept for one head of a pull request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Kept {
    pub pr: u64,
    /// The test's full title.
    pub test: String,
    /// Why this test shows the change ([`Why::words`]).
    pub why: String,
    /// The branch the Before side was taken on.
    pub base: String,
    /// The test on the base branch, when a run there kept one.
    #[serde(default)]
    pub before: Option<Side>,
    /// The test on the head.
    pub after: Side,
}

fn md_link(text: &str, url: &str) -> String {
    if url.starts_with("https://") {
        format!("[{text}]({url})")
    } else {
        text.to_string()
    }
}

/// The Before and After table for `k`.
pub fn screens(k: &Kept) -> crate::pr_body::Screens {
    let test = k.test.replace('`', "'");
    let base: &str = if k.base.is_empty() {
        "the base branch"
    } else {
        &k.base
    };
    let (before, before_alt) = match &k.before {
        Some(s) => (
            s.src.clone(),
            format!("{test} on {base}, before this change"),
        ),
        None => (
            String::new(),
            format!("No passing run on {base} with a screenshot of this test yet"),
        ),
    };
    let runs = [
        k.before
            .as_ref()
            .map(|s| md_link(&format!("{base}'s run"), &s.run_url)),
        Some(md_link("this head's run", &k.after.run_url)),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(", ");
    crate::pr_body::Screens::Images {
        before,
        before_alt,
        after: k.after.src.clone(),
        after_alt: format!("{test} on this head"),
        caption: format!(
            "Screenshots of the end-to-end test `{test}`, chosen because {}: {runs}.",
            k.why.trim_end_matches('.')
        ),
    }
}

/// The newest run of a test on `branch` that passed: where its Before
/// screenshot is read.
pub fn newest_passing<'a>(history: &'a [HistoryEntry], branch: &str) -> Option<&'a HistoryEntry> {
    history.iter().rev().find(|e| {
        e.branch == branch && !e.bad_run && matches!(e.outcome, Outcome::Passed | Outcome::Flaky)
    })
}

/// Where an image of test `test` from run `run` is kept on the screenshots
/// ref, beside the preview's captures of the same head (`pr-<n>/<head>/`);
/// two tests or two runs never share a path.
pub fn ref_path(pr: u64, sha: &str, run: &str, test: &str, side: &str, ext: &str) -> String {
    let test = hex::encode(Sha256::digest(test.as_bytes()));
    let run: String = run
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!(
        "pr-{pr}/{}/e2e-{}-{run}-{side}.{ext}",
        sha.get(..12).unwrap_or(sha),
        &test[..16]
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::e2e::model::{Attempt, AttemptStatus};
    use serde_json::json;

    const PNG: &[u8] = &[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 1, 2, 3];

    fn b64(b: &[u8]) -> String {
        STANDARD.encode(b)
    }

    fn ids(report: &str, bytes: &[u8]) -> Vec<String> {
        let tests = super::super::parse::parse_any(report, bytes, &Default::default()).unwrap();
        tests.iter().map(ParsedTest::id).collect()
    }

    #[test]
    fn cucumber_embeddings_give_each_scenario_its_last_image_under_the_test_id() {
        let report = json!([{
            "uri": "features/checkout.feature",
            "name": "Checkout",
            "elements": [
                {"type": "background", "name": "", "steps": [
                    {"keyword": "Given ", "name": "signed in", "result": {"status": "passed"},
                     "embeddings": [{"mime_type": "image/png", "data": b64(PNG)}]}
                ]},
                {"type": "scenario", "name": "Pays by card", "steps": [
                    {"keyword": "When ", "name": "pay", "result": {"status": "passed"},
                     "embeddings": [{"mime_type": "image/png", "data": b64(b"\x89PNG first")}]},
                    {"keyword": "Then ", "name": "paid", "result": {"status": "passed"},
                     "embeddings": [{"mime_type": "text/plain", "data": b64(b"log")}]}
                ], "after": [
                    {"keyword": "After", "result": {"status": "passed"},
                     "embeddings": [{"media": {"type": "image/png"}, "data": b64(PNG)}]}
                ]},
                {"type": "scenario", "name": "No image", "steps": [
                    {"keyword": "When ", "name": "x", "result": {"status": "passed"}}
                ]}
            ]
        }]);
        let bytes = serde_json::to_vec(&report).unwrap();
        let shots = shots_in("wdio/cucumber.json", &bytes);
        assert_eq!(shots.len(), 1);
        assert_eq!(shots[0].image, Image::Inline(PNG.to_vec()));
        // The id is the one the parser gives the scenario.
        assert!(ids("wdio/cucumber.json", &bytes).contains(&shots[0].test_id));
    }

    #[test]
    fn playwright_attachments_are_a_body_or_a_file_of_the_last_attempt() {
        let report = json!({
            "config": {},
            "suites": [{"title": "home.spec.ts", "file": "home.spec.ts", "specs": [], "suites": [{
                "title": "Home", "file": "home.spec.ts",
                "specs": [
                    {"title": "shows the hero", "file": "home.spec.ts", "tests": [{
                        "projectName": "chromium",
                        "results": [
                            {"retry": 0, "status": "failed", "attachments": [
                                {"name": "screenshot", "contentType": "image/png", "path": "/runner/test-results/home-hero/test-failed-1.png"}]},
                            {"retry": 1, "status": "passed", "attachments": [
                                {"name": "trace", "contentType": "application/zip", "path": "/runner/test-results/home-hero/trace.zip"},
                                {"name": "screenshot", "contentType": "image/png", "path": "/runner/test-results/home-hero/test-finished-1.png"}]}
                        ]}]},
                    {"title": "inline", "file": "home.spec.ts", "tests": [{
                        "projectName": "chromium",
                        "results": [{"retry": 0, "status": "passed", "attachments": [
                            {"name": "after", "contentType": "image/png", "body": b64(PNG)}]}]}]}
                ]
            }]}]
        });
        let bytes = serde_json::to_vec(&report).unwrap();
        let shots = shots_in("results.json", &bytes);
        assert_eq!(shots.len(), 2);
        assert_eq!(
            shots[0].image,
            Image::File("/runner/test-results/home-hero/test-finished-1.png".into())
        );
        assert_eq!(shots[1].image, Image::Inline(PNG.to_vec()));
        let parsed = ids("results.json", &bytes);
        assert!(shots.iter().all(|s| parsed.contains(&s.test_id)));
    }

    #[test]
    fn an_attachment_is_found_next_to_its_report_or_by_its_own_folder() {
        let names: Vec<String> = [
            "report/index.html",
            "report/data/abc.png",
            "results/home-hero/test-finished-1.png",
            "results/cart-total/test-finished-1.png",
        ]
        .map(String::from)
        .to_vec();
        assert_eq!(
            resolve("report/index.html", "data/abc.png", &names).as_deref(),
            Some("report/data/abc.png")
        );
        assert_eq!(
            resolve(
                "results/results.json",
                "/home/runner/work/app/test-results/cart-total/test-finished-1.png",
                &names
            )
            .as_deref(),
            Some("results/cart-total/test-finished-1.png")
        );
        // The file name alone is every test's; it never decides.
        assert_eq!(
            resolve("r.json", "/elsewhere/test-finished-1.png", &names),
            None
        );
        assert_eq!(resolve("r.json", "../secret/a.png", &names), None);
    }

    fn passed(file: &str, suite: &[&str], title: &str) -> ParsedTest {
        ParsedTest {
            framework: Framework::Playwright,
            file: file.into(),
            suite: suite.iter().map(|s| s.to_string()).collect(),
            title: title.into(),
            project: None,
            tags: vec![],
            attempts: vec![Attempt {
                retry: 0,
                status: AttemptStatus::Passed,
                duration_ms: 1,
                error: None,
                trace_url: None,
                video_url: None,
                screenshot_url: None,
            }],
        }
    }

    #[test]
    fn the_test_the_change_edits_comes_before_one_that_shares_a_word() {
        let mut failed = passed("e2e/checkout.spec.ts", &["Checkout"], "fails");
        failed.attempts[0].status = AttemptStatus::Failed;
        let tests = vec![
            passed("e2e/home.spec.ts", &["Home"], "shows the hero"),
            passed("e2e/checkout.spec.ts", &["Checkout"], "pays by card"),
            failed,
            passed("e2e/profile.spec.ts", &["Profile"], "edits the name"),
        ];
        let changed: Vec<String> = [
            "src/screens/Checkout/PayButton.tsx",
            "e2e/profile.spec.ts",
            "README.md",
        ]
        .map(String::from)
        .to_vec();
        let picked = pick(&tests, &changed, None);
        let titles: Vec<&str> = picked.iter().map(|(t, _)| t.title.as_str()).collect();
        assert_eq!(titles, vec!["edits the name", "pays by card"]);
        assert_eq!(picked[0].1, Why::TestChanged);
        assert_eq!(picked[1].1, Why::Named("checkout".into()));
    }

    #[test]
    fn generic_folders_and_non_ui_files_never_match() {
        let tests = vec![passed("e2e/screens.spec.ts", &["Components"], "index page")];
        let changed: Vec<String> = ["src/components/index.tsx", "src/api/screens.ts"]
            .map(String::from)
            .to_vec();
        assert!(pick(&tests, &changed, None).is_empty());
    }

    #[test]
    fn words_split_camel_case_and_drop_short_and_generic_ones() {
        assert_eq!(
            words("src/screens/CheckoutSummary/PayButton.tsx"),
            vec!["checkout", "summary"]
        );
    }

    #[test]
    fn the_table_names_the_test_its_runs_and_why() {
        let k = Kept {
            pr: 7,
            test: "Checkout › pays by `card`".into(),
            why: Why::Named("checkout".into()).words(),
            base: "main".into(),
            before: Some(Side {
                src: "https://github.com/o/r/blob/c1/pr-7/abc/e2e-x-1-1-before.png?raw=true".into(),
                run_url: "https://github.com/o/r/actions/runs/1".into(),
            }),
            after: Side {
                src: "https://github.com/o/r/blob/c1/pr-7/abc/e2e-x-2-1-after.png?raw=true".into(),
                run_url: "https://github.com/o/r/actions/runs/2".into(),
            },
        };
        let s = screens(&k);
        let crate::pr_body::Screens::Images {
            before,
            after,
            caption,
            after_alt,
            ..
        } = s
        else {
            panic!("images");
        };
        assert!(before.ends_with("before.png?raw=true"));
        assert!(after.ends_with("after.png?raw=true"));
        assert_eq!(after_alt, "Checkout › pays by 'card' on this head");
        assert_eq!(
            caption,
            "Screenshots of the end-to-end test `Checkout › pays by 'card'`, chosen because the test and a changed UI file share the word \"checkout\": [main's run](https://github.com/o/r/actions/runs/1), [this head's run](https://github.com/o/r/actions/runs/2)."
        );
        let none = screens(&Kept {
            before: None,
            ..k.clone()
        });
        let crate::pr_body::Screens::Images {
            before, before_alt, ..
        } = none
        else {
            panic!("images");
        };
        assert!(before.is_empty());
        assert_eq!(
            before_alt,
            "No passing run on main with a screenshot of this test yet"
        );
    }

    #[test]
    fn each_test_and_run_has_its_own_path_beside_the_preview_captures() {
        let path = ref_path(7, "0123456789abcdef", "12-1", "t", "after", "png");
        // sha256("t") = e3b98a4da31a127d...
        assert_eq!(
            path,
            "pr-7/0123456789ab/e2e-e3b98a4da31a127d-12-1-after.png"
        );
        assert_ne!(
            path,
            ref_path(7, "0123456789abcdef", "12-1", "u", "after", "png")
        );
        assert_ne!(
            path,
            ref_path(7, "0123456789abcdef", "13-1", "t", "after", "png")
        );
        // An upload's run key is kept to letters, digits and dashes.
        assert_eq!(
            ref_path(7, "0123456789abcdef", "up-9/../x", "t", "before", "jpg"),
            "pr-7/0123456789ab/e2e-e3b98a4da31a127d-up-9____x-before.jpg"
        );
    }

    #[test]
    fn before_comes_from_the_newest_passing_run_on_the_pull_requests_base() {
        let e = |run: &str, branch: &str, outcome, bad_run| HistoryEntry {
            run_key: run.into(),
            sha: "s".into(),
            branch: branch.into(),
            pr: None,
            at: "t".into(),
            outcome,
            bad_run,
            attempts: vec![],
        };
        let history = vec![
            e("1-1", "develop", Outcome::Passed, false),
            e("2-1", "develop", Outcome::Failed, false),
            e("3-1", "develop", Outcome::Passed, true),
            e("4-1", "main", Outcome::Passed, false),
        ];
        // A pull request into develop never shows main's newer run.
        assert_eq!(
            newest_passing(&history, "develop").map(|e| e.run_key.as_str()),
            Some("1-1")
        );
        assert_eq!(
            newest_passing(&history, "main").map(|e| e.run_key.as_str()),
            Some("4-1")
        );
        assert_eq!(newest_passing(&history, "release"), None);
        let k = Kept {
            base: "develop".into(),
            ..Default::default()
        };
        let crate::pr_body::Screens::Images { before_alt, .. } = screens(&k) else {
            panic!("images");
        };
        assert_eq!(
            before_alt,
            "No passing run on develop with a screenshot of this test yet"
        );
    }

    #[test]
    fn only_real_images_are_kept() {
        assert_eq!(image_type(PNG), Some("image/png"));
        assert_eq!(image_type(&[0xFF, 0xD8, 0xFF, 0xE0]), Some("image/jpeg"));
        assert_eq!(image_type(b"<svg onload=alert(1)>"), None);
        assert_eq!(decoded(&b64(b"<html>")), None);
    }
}
