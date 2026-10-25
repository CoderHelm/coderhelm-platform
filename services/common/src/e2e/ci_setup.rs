//! Setting a repo up to send E2E results: finding the workflow that runs the
//! tests, telling what is already there, and adding what is missing.
//!
//! - [`pick_workflow`] reads every workflow file and finds the job and step
//!   that run the E2E tests, the test runner, and whether results are
//!   uploaded and quarantined tests are skipped ([`WorkflowFacts`]).
//! - [`plan`] adds the missing steps to that workflow's text. Positions come
//!   from a real YAML parser (`yaml_rust2` events with their marks); the new
//!   lines are spliced into the original text, so comments, order, quoting
//!   and blank lines stay as they were. The result is parsed again and
//!   checked before it is used. Running it on its own output changes nothing.
//! - [`unified_diff`] shows the change before a pull request is opened.
//!
//! Everything here is pure: callers read the files from GitHub and write the
//! result back.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};
use serde_yaml::Value as Yaml;
use yaml_rust2::parser::{Event, MarkedEventReceiver, Parser};
use yaml_rust2::scanner::{Marker, TScalarStyle};

/// The branch a setup pull request is opened from.
pub const SETUP_BRANCH: &str = "coderhelm/e2e-results-setup";
/// The artifact the upload step writes. Any artifact is read; the name only
/// makes the step easy to recognise.
pub const UPLOAD_ARTIFACT: &str = "coderhelm-e2e-results";
pub const UPLOAD_STEP_NAME: &str = "Upload E2E results for Coderhelm";
pub const QUARANTINE_STEP_ID: &str = "coderhelm_quarantine";
pub const QUARANTINE_STEP_NAME: &str = "Get quarantined tests from Coderhelm";
/// Where the quarantine step asks for the list: this install's API (a
/// self-hosted install has its own address).
pub fn quarantine_url() -> String {
    format!(
        "{}/webhooks/ci/tests/quarantine",
        crate::public_urls::api_url_from_env()
    )
}
/// Where workflow files live.
pub const WORKFLOW_DIR: &str = ".github/workflows";
/// Config files that say which reporter Playwright writes.
pub const PLAYWRIGHT_CONFIGS: &[&str] = &[
    "playwright.config.ts",
    "playwright.config.js",
    "playwright.config.mjs",
    "playwright.config.cjs",
];
pub const VITEST_CONFIGS: &[&str] = &[
    "vitest.config.ts",
    "vitest.config.js",
    "vitest.config.mts",
    "vite.config.ts",
    "vite.config.js",
];
pub const JEST_CONFIGS: &[&str] = &["jest.config.ts", "jest.config.js", "jest.config.cjs"];

// ── runners ──────────────────────────────────────────────────────────────

/// A test runner whose results Coderhelm reads. Playwright writes its own
/// JSON or HTML report; the rest write JUnit XML.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Runner {
    Playwright,
    Jest,
    Vitest,
    Cypress,
    /// Any other runner that writes JUnit XML.
    Junit,
}

impl Runner {
    pub const ALL: [Runner; 5] = [
        Runner::Playwright,
        Runner::Jest,
        Runner::Vitest,
        Runner::Cypress,
        Runner::Junit,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Runner::Playwright => "playwright",
            Runner::Jest => "jest",
            Runner::Vitest => "vitest",
            Runner::Cypress => "cypress",
            Runner::Junit => "junit",
        }
    }

    /// The folders the upload step keeps for this runner.
    pub fn result_paths(self) -> &'static [&'static str] {
        match self {
            // The HTML report embeds every result; the JSON reporter writes
            // into test-results/ by the snippet's convention.
            Runner::Playwright => &["playwright-report/", "test-results/"],
            Runner::Jest => &["junit.xml", "reports/", "test-results/"],
            Runner::Vitest => &["junit.xml", "test-results/"],
            Runner::Cypress => &["cypress/results/", "results/"],
            Runner::Junit => &["**/TEST-*.xml", "**/junit*.xml", "test-results/"],
        }
    }

    /// Quarantined tests can be skipped by the CI step only for Playwright:
    /// the quarantine call answers with a `--grep-invert` pattern.
    pub fn can_skip_quarantined(self) -> bool {
        self == Runner::Playwright
    }

    /// Words in a command that run this runner.
    fn markers(self) -> &'static [&'static str] {
        match self {
            Runner::Playwright => &["playwright test"],
            Runner::Cypress => &["cypress run"],
            Runner::Vitest => &["vitest"],
            Runner::Jest => &["jest"],
            Runner::Junit => &[
                "mvn test",
                "mvn verify",
                "gradle test",
                "gradlew test",
                "pytest",
                "go test",
            ],
        }
    }
}

/// The scripts of a package.json, or an empty map.
pub fn package_scripts(package_json: Option<&str>) -> Map<String, Json> {
    package_json
        .and_then(|t| serde_json::from_str::<Json>(t).ok())
        .and_then(|v| v["scripts"].as_object().cloned())
        .unwrap_or_default()
}

/// The runner a package.json depends on, most specific first.
pub fn runner_from_package(package_json: &str) -> Option<Runner> {
    let v: Json = serde_json::from_str(package_json).ok()?;
    let has = |name: &str| {
        ["dependencies", "devDependencies"]
            .iter()
            .any(|k| v[*k].get(name).is_some())
    };
    if has("@playwright/test") || has("playwright") {
        Some(Runner::Playwright)
    } else if has("cypress") {
        Some(Runner::Cypress)
    } else if has("vitest") {
        Some(Runner::Vitest)
    } else if has("jest") {
        Some(Runner::Jest)
    } else {
        None
    }
}

/// The runner a shell command runs, following `npm run x` into package.json
/// scripts once.
pub fn runner_of_command(cmd: &str, scripts: &Map<String, Json>) -> Option<Runner> {
    let direct = |text: &str| {
        Runner::ALL
            .into_iter()
            .find(|r| r.markers().iter().any(|m| contains_word(text, m)))
    };
    if let Some(r) = direct(cmd) {
        return Some(r);
    }
    script_names(cmd)
        .iter()
        .filter_map(|s| scripts.get(s).and_then(Json::as_str))
        .find_map(direct)
}

/// `text` contains `word` with no letter or digit glued to either side.
fn contains_word(text: &str, word: &str) -> bool {
    let bytes = text.as_bytes();
    text.match_indices(word).any(|(i, _)| {
        let before = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
        let end = i + word.len();
        let after = end >= bytes.len() || !bytes[end].is_ascii_alphanumeric();
        before && after
    })
}

/// Script names a command runs: `npm run e2e`, `npm test`, `yarn e2e`,
/// `pnpm run e2e`, `pnpm e2e`, `bun run e2e`.
fn script_names(cmd: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in cmd.lines() {
        let words: Vec<&str> = line.split_whitespace().collect();
        for (i, w) in words.iter().enumerate() {
            let next = |k: usize| words.get(i + k).map(|s| s.to_string());
            match *w {
                "npm" | "pnpm" | "bun" => match words.get(i + 1).copied() {
                    Some("run") | Some("run-script") => out.extend(next(2)),
                    Some("test") | Some("t") => out.push("test".into()),
                    Some(s) if *w == "pnpm" && !s.starts_with('-') => out.push(s.to_string()),
                    _ => {}
                },
                "yarn" => match words.get(i + 1).copied() {
                    Some("run") => out.extend(next(2)),
                    Some(s) if !s.starts_with('-') => out.push(s.to_string()),
                    _ => {}
                },
                _ => {}
            }
        }
    }
    out
}

/// Whether the runner's config writes a report Coderhelm reads. `None` when
/// there is no config to look at.
pub fn reporter_writes_results(
    runner: Runner,
    config: Option<&str>,
    package_json: Option<&str>,
) -> Option<bool> {
    match runner {
        Runner::Playwright => {
            let c = config?;
            // The HTML report embeds the results; JSON and JUnit are read as is.
            Some(["html", "json", "junit"].iter().any(|r| {
                c.contains(&format!("'{r}'"))
                    || c.contains(&format!("\"{r}\""))
                    || c.contains(&format!("`{r}`"))
            }))
        }
        Runner::Jest => {
            let pkg = package_json.unwrap_or("");
            Some(pkg.contains("jest-junit") || config.is_some_and(|c| c.contains("jest-junit")))
        }
        Runner::Vitest => config.map(|c| c.contains("junit")),
        Runner::Cypress => config.map(|c| c.contains("junit")),
        Runner::Junit => Some(true),
    }
}

// ── reading a workflow ───────────────────────────────────────────────────

/// The job and step that run the E2E tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestJob {
    /// The job's key under `jobs:`.
    pub job_id: String,
    /// Its `name:`, when it has one.
    pub job_name: Option<String>,
    pub step_index: usize,
    pub step_name: Option<String>,
    pub runner: Runner,
    /// Runs in a matrix, so each copy needs its own artifact name.
    pub matrix: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadState {
    Missing,
    /// Uploads, but not when tests fail (no `if: !cancelled()` or `always()`).
    OnlyOnSuccess,
    /// Uploads something else than the results.
    OtherFiles,
    Found,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowFacts {
    pub job: TestJob,
    pub upload: UploadState,
    /// The upload step's paths, as written.
    pub upload_paths: Vec<String>,
    /// The quarantine step is in the job.
    pub quarantine: bool,
    /// Coderhelm's own upload step is in the job.
    pub ours: bool,
}

fn as_str(v: &Yaml) -> Option<&str> {
    v.as_str()
}

fn get<'a>(v: &'a Yaml, key: &str) -> Option<&'a Yaml> {
    v.as_mapping()?.get(Yaml::String(key.to_string()))
}

fn step_run(step: &Yaml) -> String {
    get(step, "run").and_then(as_str).unwrap_or("").to_string()
}

fn step_uses(step: &Yaml) -> String {
    get(step, "uses").and_then(as_str).unwrap_or("").to_string()
}

/// The runner a step runs, from its command or its action.
fn step_runner(step: &Yaml, scripts: &Map<String, Json>) -> Option<Runner> {
    let uses = step_uses(step);
    if uses.starts_with("cypress-io/github-action") {
        return Some(Runner::Cypress);
    }
    runner_of_command(&step_run(step), scripts)
}

/// Finds the job and step that run E2E tests. Playwright and Cypress jobs
/// come first, then jobs whose name says e2e, then any test step.
pub fn find_test_job(yaml: &str, scripts: &Map<String, Json>) -> Option<TestJob> {
    let doc: Yaml = serde_yaml::from_str(yaml).ok()?;
    let jobs = get(&doc, "jobs")?.as_mapping()?;
    let mut best: Option<(u32, TestJob)> = None;
    for (id, job) in jobs {
        let Some(id) = id.as_str() else { continue };
        let Some(steps) = get(job, "steps").and_then(Yaml::as_sequence) else {
            continue;
        };
        let job_name = get(job, "name").and_then(as_str).map(str::to_string);
        for (i, step) in steps.iter().enumerate() {
            let Some(runner) = step_runner(step, scripts) else {
                continue;
            };
            let e2e_named = [id, job_name.as_deref().unwrap_or("")]
                .iter()
                .any(|n| n.to_lowercase().contains("e2e"));
            let score = match runner {
                Runner::Playwright | Runner::Cypress => 3,
                _ if e2e_named => 2,
                _ => 1,
            };
            if best.as_ref().is_none_or(|(s, _)| score > *s) {
                best = Some((
                    score,
                    TestJob {
                        job_id: id.to_string(),
                        job_name: job_name.clone(),
                        step_index: i,
                        step_name: get(step, "name").and_then(as_str).map(str::to_string),
                        runner,
                        matrix: get(job, "strategy")
                            .and_then(|s| get(s, "matrix"))
                            .is_some(),
                    },
                ));
            }
        }
    }
    best.map(|(_, j)| j)
}

/// Whether an upload path keeps the runner's results.
fn path_covers(runner: Runner, path: &str) -> bool {
    let p = path.trim().trim_start_matches("./");
    if p == "." || p == "*" || p == "**" {
        return true;
    }
    match runner {
        Runner::Playwright => {
            p.starts_with("playwright-report")
                || p.starts_with("test-results")
                || p.ends_with(".json")
        }
        _ => {
            p.ends_with(".xml")
                || p.contains("junit")
                || p.contains("results")
                || p.contains("reports")
        }
    }
}

fn runs_on_failure(cond: &str) -> bool {
    let c = cond.replace(' ', "");
    c.contains("always()") || c.contains("!cancelled()") || c.contains("failure()")
}

/// What a workflow already does for the test job, or `None` when it runs no
/// tests.
pub fn inspect(yaml: &str, scripts: &Map<String, Json>) -> Option<WorkflowFacts> {
    let job = find_test_job(yaml, scripts)?;
    let doc: Yaml = serde_yaml::from_str(yaml).ok()?;
    let steps = get(get(get(&doc, "jobs")?, &job.job_id)?, "steps")?.as_sequence()?;
    let mut upload = UploadState::Missing;
    let mut upload_paths = Vec::new();
    let mut ours = false;
    for step in &steps[job.step_index + 1..] {
        if !step_uses(step).starts_with("actions/upload-artifact") {
            continue;
        }
        let with = get(step, "with");
        let paths: Vec<String> = with
            .and_then(|w| get(w, "path"))
            .and_then(as_str)
            .unwrap_or("")
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('!'))
            .map(str::to_string)
            .collect();
        let name = get(step, "name").and_then(as_str).unwrap_or("");
        let artifact = with
            .and_then(|w| get(w, "name"))
            .and_then(as_str)
            .unwrap_or("");
        if name == UPLOAD_STEP_NAME || artifact.starts_with(UPLOAD_ARTIFACT) {
            ours = true;
        }
        let covers = paths.iter().any(|p| path_covers(job.runner, p));
        let on_failure = get(step, "if")
            .and_then(as_str)
            .is_some_and(runs_on_failure);
        let state = match (covers, on_failure) {
            (true, true) => UploadState::Found,
            (true, false) => UploadState::OnlyOnSuccess,
            (false, _) => UploadState::OtherFiles,
        };
        let rank = |s: UploadState| match s {
            UploadState::Missing => 0,
            UploadState::OtherFiles => 1,
            UploadState::OnlyOnSuccess => 2,
            UploadState::Found => 3,
        };
        if rank(state) > rank(upload) {
            upload = state;
            upload_paths = paths;
        }
    }
    let quarantine = steps.iter().any(|s| {
        step_run(s).contains("/ci/tests/quarantine")
            || get(s, "id").and_then(as_str) == Some(QUARANTINE_STEP_ID)
    });
    Some(WorkflowFacts {
        job,
        upload,
        upload_paths,
        quarantine,
        ours,
    })
}

/// The workflow that runs the E2E tests, among `(path, text)` pairs. Files
/// that don't parse are skipped.
pub fn pick_workflow(
    files: &[(String, String)],
    scripts: &Map<String, Json>,
) -> Option<(String, WorkflowFacts)> {
    let rank = |f: &WorkflowFacts, path: &str| {
        let runner = matches!(f.job.runner, Runner::Playwright | Runner::Cypress) as u8;
        let named = path.to_lowercase().contains("e2e") as u8;
        (runner, named)
    };
    files
        .iter()
        .filter_map(|(p, t)| inspect(t, scripts).map(|f| (p.clone(), f)))
        .max_by(|(pa, a), (pb, b)| rank(a, pa).cmp(&rank(b, pb)).then(pb.cmp(pa)))
}

// ── the YAML tree with positions ─────────────────────────────────────────

#[derive(Debug, Clone)]
enum Node {
    Scalar {
        value: String,
        style: TScalarStyle,
        at: Marker,
    },
    Map {
        at: Marker,
        flow: bool,
        entries: Vec<(Node, Node)>,
    },
    Seq {
        at: Marker,
        end: Marker,
        items: Vec<Node>,
    },
    Alias,
}

impl Node {
    fn at(&self) -> Option<Marker> {
        match self {
            Node::Scalar { at, .. } | Node::Map { at, .. } | Node::Seq { at, .. } => Some(*at),
            Node::Alias => None,
        }
    }

    fn get(&self, key: &str) -> Option<&Node> {
        match self {
            Node::Map { entries, .. } => entries.iter().find_map(|(k, v)| match k {
                Node::Scalar { value, .. } if value == key => Some(v),
                _ => None,
            }),
            _ => None,
        }
    }

    fn key_node(&self, key: &str) -> Option<&Node> {
        match self {
            Node::Map { entries, .. } => entries.iter().find_map(|(k, _)| match k {
                Node::Scalar { value, .. } if value == key => Some(k),
                _ => None,
            }),
            _ => None,
        }
    }
}

#[derive(Default)]
struct Events(Vec<(Event, Marker)>);

impl MarkedEventReceiver for Events {
    fn on_event(&mut self, ev: Event, mark: Marker) {
        self.0.push((ev, mark));
    }
}

fn build(events: &[(Event, Marker)], i: &mut usize, text: &[char]) -> Option<Node> {
    let (ev, at) = events.get(*i)?.clone();
    *i += 1;
    match ev {
        Event::Scalar(value, style, _, _) => Some(Node::Scalar { value, style, at }),
        Event::Alias(_) => Some(Node::Alias),
        Event::MappingStart(_, _) => {
            let flow = text.get(at.index()) == Some(&'{');
            let mut entries: Vec<(Node, Node)> = Vec::new();
            loop {
                if let Some((Event::MappingEnd, _)) = events.get(*i) {
                    *i += 1;
                    // A block mapping's start mark sits after its first key;
                    // the key's own mark is where the mapping begins.
                    let at = entries
                        .first()
                        .and_then(|(k, _): &(Node, Node)| k.at())
                        .unwrap_or(at);
                    return Some(Node::Map { at, flow, entries });
                }
                let k = build(events, i, text)?;
                let v = build(events, i, text)?;
                entries.push((k, v));
            }
        }
        Event::SequenceStart(_, _) => {
            let mut items = Vec::new();
            loop {
                if let Some((Event::SequenceEnd, end)) = events.get(*i) {
                    let end = *end;
                    *i += 1;
                    return Some(Node::Seq { at, end, items });
                }
                items.push(build(events, i, text)?);
            }
        }
        _ => None,
    }
}

fn parse_marked(yaml: &str) -> Result<Node, String> {
    let mut rx = Events::default();
    Parser::new_from_str(yaml)
        .load(&mut rx, false)
        .map_err(|e| format!("The workflow isn't valid YAML: {e}"))?;
    let text: Vec<char> = yaml.chars().collect();
    let start =
        rx.0.iter()
            .position(|(e, _)| {
                matches!(
                    e,
                    Event::MappingStart(..) | Event::SequenceStart(..) | Event::Scalar(..)
                )
            })
            .ok_or("The workflow is empty.")?;
    let mut i = start;
    build(&rx.0, &mut i, &text).ok_or_else(|| "The workflow couldn't be read.".to_string())
}

// ── editing ──────────────────────────────────────────────────────────────

/// What a setup pull request should do.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Options {
    /// Add a step that uploads results when tests fail too.
    #[serde(default = "yes")]
    pub upload: bool,
    /// Add the step that skips quarantined tests (Playwright only).
    #[serde(default)]
    pub quarantine: bool,
    /// Fix an upload step that exists but sends nothing: run it when tests
    /// fail, and add Coderhelm's own step when the paths don't match.
    #[serde(default)]
    pub repair: bool,
}

fn yes() -> bool {
    true
}

/// A planned change to one workflow file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    pub yaml: String,
    /// "upload", "upload_if", "quarantine", "permissions"
    pub added: Vec<String>,
    /// Plain sentences for the pull request body.
    pub notes: Vec<String>,
}

impl Plan {
    pub fn changed(&self) -> bool {
        !self.added.is_empty()
    }
}

struct Lines {
    lines: Vec<String>,
    /// (line index, text) to insert before that line, in order.
    inserts: Vec<(usize, Vec<String>)>,
    /// (line index, char column, text) to insert inside a line.
    splices: Vec<(usize, usize, String)>,
}

impl Lines {
    fn new(yaml: &str) -> Self {
        let mut lines: Vec<String> = yaml.split('\n').map(str::to_string).collect();
        if lines.last().is_some_and(|l| l.is_empty()) && yaml.ends_with('\n') {
            lines.pop();
        }
        Lines {
            lines,
            inserts: Vec::new(),
            splices: Vec::new(),
        }
    }

    fn indent_of(&self, line: usize) -> usize {
        self.lines
            .get(line)
            .map(|l| l.chars().take_while(|c| *c == ' ').count())
            .unwrap_or(0)
    }

    fn is_blank_or_comment_at(&self, line: usize, max_indent: usize) -> bool {
        let Some(l) = self.lines.get(line) else {
            return true;
        };
        let t = l.trim_start();
        t.is_empty() || (t.starts_with('#') && self.indent_of(line) <= max_indent)
    }

    fn finish(mut self, trailing_newline: bool) -> String {
        for (line, col, text) in std::mem::take(&mut self.splices) {
            let l = &mut self.lines[line];
            let byte = l.char_indices().nth(col).map_or(l.len(), |(b, _)| b);
            l.insert_str(byte, &text);
        }
        let mut inserts = std::mem::take(&mut self.inserts);
        // Stable: inserts at the same line keep the order they were added.
        inserts.sort_by_key(|(at, _)| *at);
        let mut out: Vec<String> = Vec::with_capacity(self.lines.len() + 16);
        let mut next = inserts.into_iter().peekable();
        for (i, l) in self.lines.into_iter().enumerate() {
            while next.peek().is_some_and(|(at, _)| *at == i) {
                out.extend(next.next().unwrap().1);
            }
            out.push(l);
        }
        for (_, rest) in next {
            out.extend(rest);
        }
        let mut s = out.join("\n");
        if trailing_newline {
            s.push('\n');
        }
        s
    }
}

/// 0-based line of a mark.
fn line_of(m: Marker) -> usize {
    m.line().saturating_sub(1)
}

/// The column of the `-` that starts a sequence item whose first key is at
/// `key_col` on `line`.
fn dash_col(lines: &Lines, line: usize, key_col: usize) -> usize {
    let chars: Vec<char> = lines.lines[line].chars().collect();
    let mut c = key_col.min(chars.len());
    while c > 0 {
        c -= 1;
        if chars[c] == '-' {
            return c;
        }
    }
    key_col.saturating_sub(2)
}

/// The last line of a sequence item: up to the next item (or the sequence's
/// end), without the blank lines and comments in between, which belong to
/// what follows.
fn item_last_line(lines: &Lines, items: &[Node], i: usize, seq_end: Marker, dash: usize) -> usize {
    let start = items[i].at().map(line_of).unwrap_or(0);
    let boundary = match items.get(i + 1).and_then(Node::at) {
        Some(m) => line_of(m),
        None => line_of(seq_end).min(lines.lines.len()),
    };
    let mut last = boundary.saturating_sub(1).max(start);
    while last > start && lines.is_blank_or_comment_at(last, dash) {
        last -= 1;
    }
    last
}

/// The first line of a sequence item, with the comments right above it.
fn item_first_line(lines: &Lines, item: &Node, dash: usize) -> usize {
    let mut first = item.at().map(line_of).unwrap_or(0);
    while first > 0 {
        let prev = first - 1;
        let t = lines.lines[prev].trim_start();
        if t.starts_with('#') && lines.indent_of(prev) == dash {
            first = prev;
        } else {
            break;
        }
    }
    first
}

fn upload_step(runner: Runner, matrix: bool, dash: usize, key: usize) -> Vec<String> {
    let d = " ".repeat(dash);
    let k = " ".repeat(key);
    let name = if matrix {
        format!("{UPLOAD_ARTIFACT}-${{{{ strategy.job-index }}}}")
    } else {
        UPLOAD_ARTIFACT.to_string()
    };
    let mut out = vec![
        format!("{d}- name: {UPLOAD_STEP_NAME}"),
        format!("{k}# Keeps the results of failed runs too, which are the ones that matter."),
        format!("{k}if: ${{{{ !cancelled() }}}}"),
        format!("{k}uses: actions/upload-artifact@v4"),
        format!("{k}with:"),
        format!("{k}  name: {name}"),
        format!("{k}  path: |"),
    ];
    out.extend(runner.result_paths().iter().map(|p| format!("{k}    {p}")));
    out.push(format!("{k}  if-no-files-found: ignore"));
    out.push(format!("{k}  retention-days: 7"));
    out
}

fn quarantine_step(dash: usize, key: usize) -> Vec<String> {
    let url = quarantine_url();
    let d = " ".repeat(dash);
    let k = " ".repeat(key);
    vec![
        format!("{d}- name: {QUARANTINE_STEP_NAME}"),
        format!("{k}# Asks Coderhelm which tests are quarantined, signed with this run's GitHub token."),
        format!("{k}# If Coderhelm can't be reached, every test runs as before."),
        format!("{k}id: {QUARANTINE_STEP_ID}"),
        format!("{k}continue-on-error: true"),
        format!("{k}run: |"),
        format!("{k}  TOKEN=$(curl -sf -H \"Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN\" \"$ACTIONS_ID_TOKEN_REQUEST_URL&audience=coderhelm\" | jq -r .value) || TOKEN=\"\""),
        format!("{k}  GREP=$(curl -sf -H \"Authorization: Bearer $TOKEN\" {url} | jq -r '.playwright_grep_invert // \"\"') || GREP=\"\""),
        format!("{k}  echo \"grep=$GREP\" >> \"$GITHUB_OUTPUT\""),
    ]
}

/// What goes after the test command so it skips quarantined tests. Empty
/// when nothing is quarantined, so the command runs as before.
fn grep_flag(cmd_line: &str) -> String {
    let flag = "${QUARANTINED:+--grep-invert \"$QUARANTINED\"}";
    let words: Vec<&str> = cmd_line.split_whitespace().collect();
    let npm_script = words
        .windows(2)
        .any(|w| w[0] == "npm" && matches!(w[1], "run" | "run-script" | "test" | "t"));
    if npm_script && !cmd_line.contains(" -- ") {
        format!(" -- {flag}")
    } else {
        format!(" {flag}")
    }
}

/// The source line (and char column of its end) of the command that runs
/// the tests inside a step's `run:`. `None` when it can't be placed safely.
fn command_position(
    lines: &Lines,
    run: &Node,
    runner: Runner,
    scripts: &Map<String, Json>,
) -> Option<(usize, usize, String)> {
    let Node::Scalar { value, style, at } = run else {
        return None;
    };
    let runs_tests = |l: &str| runner_of_command(l, scripts) == Some(runner);
    match style {
        TScalarStyle::Plain if !value.contains('\n') => {
            if !runs_tests(value) {
                return None;
            }
            let line = line_of(*at);
            Some((line, at.col() + value.chars().count(), value.clone()))
        }
        TScalarStyle::Literal => {
            let first = line_of(*at) + 1;
            let content_indent = lines.indent_of(first);
            let mut i = first;
            while i < lines.lines.len() {
                let l = &lines.lines[i];
                if !l.trim().is_empty() && lines.indent_of(i) < content_indent {
                    break;
                }
                if runs_tests(l.trim()) {
                    // Follow `\` line continuations to the command's end.
                    let mut end = i;
                    while lines.lines[end].trim_end().ends_with('\\') && end + 1 < lines.lines.len()
                    {
                        end += 1;
                    }
                    let text = lines.lines[end].trim_end().to_string();
                    if text.contains(" #") {
                        return None;
                    }
                    return Some((end, text.chars().count(), lines.lines[i].trim().to_string()));
                }
                i += 1;
            }
            None
        }
        _ => None,
    }
}

/// Adds what `opts` asks for to a workflow, leaving everything else as it
/// was. Returns the same text (and nothing in `added`) when it is all there.
pub fn plan(yaml: &str, scripts: &Map<String, Json>, opts: Options) -> Result<Plan, String> {
    let facts = inspect(yaml, scripts).ok_or("No job in this workflow runs tests.")?;
    let root = parse_marked(yaml)?;
    let job_node = root
        .get("jobs")
        .and_then(|j| j.get(&facts.job.job_id))
        .ok_or("The test job couldn't be found.")?;
    let Some(Node::Seq {
        items,
        end: seq_end,
        ..
    }) = job_node.get("steps")
    else {
        return Err("The test job's steps aren't a list.".into());
    };
    let step = &items[facts.job.step_index];
    let step_at = step.at().ok_or("The test step couldn't be read.")?;
    if matches!(step, Node::Map { flow: true, .. }) {
        return Err("The test step is written on one line; add the steps by hand.".into());
    }
    let mut lines = Lines::new(yaml);
    let key = step_at.col();
    let dash = dash_col(&lines, line_of(step_at), key);
    let mut added = Vec::new();
    let mut notes = Vec::new();
    let runner = facts.job.runner;

    // Upload.
    let mut deferred: Vec<(usize, Vec<String>)> = Vec::new();
    let mut add_upload = (opts.upload && facts.upload == UploadState::Missing)
        || (opts.repair && facts.upload == UploadState::OtherFiles && !facts.ours);
    if opts.repair && facts.upload == UploadState::OnlyOnSuccess {
        // Run the existing upload step when tests fail too.
        let up = items[facts.job.step_index + 1..]
            .iter()
            .enumerate()
            .find(|(_, s)| matches!(s.get("uses"), Some(Node::Scalar { value, .. }) if value.starts_with("actions/upload-artifact")))
            .map(|(k, s)| (facts.job.step_index + 1 + k, s));
        if let Some((idx, up)) = up {
            if up.get("if").is_none() {
                let last = item_last_line(&lines, items, idx, *seq_end, dash);
                let ukey = up.at().map_or(key, |m| m.col());
                lines.inserts.push((
                    last + 1,
                    vec![format!("{}if: ${{{{ !cancelled() }}}}", " ".repeat(ukey))],
                ));
                added.push("upload_if".to_string());
                notes.push("The existing upload step now runs when tests fail too.".to_string());
            } else if !facts.ours {
                notes.push("The existing upload step has its own condition, so it was left alone and a second one was added.".into());
                add_upload = true;
            }
        }
    }
    if add_upload {
        // Pushed after the quarantine edits, so an `env:` added to the test
        // step stays on the test step and the upload step follows it.
        let last = item_last_line(&lines, items, facts.job.step_index, *seq_end, dash);
        deferred.push((last + 1, upload_step(runner, facts.job.matrix, dash, key)));
        added.push("upload".into());
        notes.push(format!(
            "Uploads {} after the test step, also when tests fail.",
            runner.result_paths().join(", ")
        ));
    }

    // Quarantine.
    if opts.quarantine && !facts.quarantine {
        if !runner.can_skip_quarantined() {
            return Err("Skipping quarantined tests in CI works with Playwright only.".into());
        }
        let run = step
            .get("run")
            .ok_or("The test step has no command to change.")?;
        let (cmd_line, cmd_col, cmd_text) = command_position(&lines, run, runner, scripts).ok_or(
            "The test command couldn't be changed safely; add the quarantine step by hand.",
        )?;
        // The step that asks Coderhelm, right before the test step.
        let first = item_first_line(&lines, step, dash);
        lines.inserts.push((first, quarantine_step(dash, key)));
        // The flag on the test command.
        lines
            .splices
            .push((cmd_line, cmd_col, grep_flag(&cmd_text)));
        // The pattern, handed to the test step.
        let env_line = format!("QUARANTINED: ${{{{ steps.{QUARANTINE_STEP_ID}.outputs.grep }}}}");
        match step.get("env") {
            Some(Node::Map {
                flow: false,
                entries,
                ..
            }) if !entries.is_empty() => {
                let first_key = entries[0].0.at().ok_or("env couldn't be read")?;
                lines.inserts.push((
                    line_of(first_key),
                    vec![format!("{}{env_line}", " ".repeat(first_key.col()))],
                ));
            }
            None => {
                let last = item_last_line(&lines, items, facts.job.step_index, *seq_end, dash);
                let k = " ".repeat(key);
                lines.inserts.push((
                    last + 1,
                    vec![format!("{k}env:"), format!("{k}  {env_line}")],
                ));
            }
            Some(_) => {
                return Err(
                    "The test step's env is written inline; add the quarantine step by hand."
                        .into(),
                )
            }
        }
        added.push("quarantine".into());
        notes.push("Asks Coderhelm for quarantined tests before the test step and skips them. If Coderhelm can't be reached, every test runs.".into());

        // The job may ask GitHub for a signed token.
        let perm_note = add_id_token_permission(&mut lines, &root, job_node, &facts.job.job_id)?;
        if let Some(n) = perm_note {
            added.push("permissions".into());
            notes.push(n);
        }
    }

    lines.inserts.extend(deferred);

    if added.is_empty() {
        return Ok(Plan {
            yaml: yaml.to_string(),
            added,
            notes,
        });
    }
    let out = lines.finish(yaml.ends_with('\n'));
    verify(yaml, &out, &facts, &added)?;
    Ok(Plan {
        yaml: out,
        added,
        notes,
    })
}

/// Lets the test job ask GitHub for an OIDC token. Adds `id-token: write` to
/// the job's `permissions:` (or the workflow's), or gives the job its own.
fn add_id_token_permission(
    lines: &mut Lines,
    root: &Node,
    job: &Node,
    job_id: &str,
) -> Result<Option<String>, String> {
    let has_write = |p: &Node| matches!(p.get("id-token"), Some(Node::Scalar { value, .. }) if value == "write");
    let target = match (job.get("permissions"), root.get("permissions")) {
        (Some(p), _) => Some(p),
        (None, Some(p)) => Some(p),
        (None, None) => None,
    };
    match target {
        Some(Node::Scalar { value, .. }) if value == "write-all" => Ok(None),
        Some(Node::Scalar { .. }) => Err(
            "The workflow sets its permissions in one word; add `id-token: write` by hand.".into(),
        ),
        Some(
            p @ Node::Map {
                flow: false,
                entries,
                ..
            },
        ) => {
            if has_write(p) {
                return Ok(None);
            }
            if let Some(v) = p.get("id-token") {
                // `id-token: read` or `none`: change the value in place.
                if let Node::Scalar {
                    value: old,
                    style: TScalarStyle::Plain,
                    at,
                } = v
                {
                    let l = &mut lines.lines[line_of(*at)];
                    let start = l.char_indices().nth(at.col()).map_or(l.len(), |(b, _)| b);
                    l.replace_range(start..start + old.len(), "write");
                    return Ok(Some(
                        "Lets the job ask GitHub for a signed token (`id-token: write`).".into(),
                    ));
                }
                return Err("`id-token` is set in a way that couldn't be changed safely.".into());
            }
            let first = entries
                .first()
                .and_then(|(k, _)| k.at())
                .ok_or("permissions couldn't be read")?;
            lines.inserts.push((
                line_of(first),
                vec![format!("{}id-token: write", " ".repeat(first.col()))],
            ));
            Ok(Some(
                "Lets the job ask GitHub for a signed token (`id-token: write`).".into(),
            ))
        }
        Some(Node::Map { .. }) => Err(
            "The workflow's permissions are written inline; add `id-token: write` by hand.".into(),
        ),
        Some(_) => Err("The workflow's permissions couldn't be read.".into()),
        None => {
            // No permissions anywhere: the job gets its own, with the read
            // access checkout needs.
            let key = job
                .key_node("steps")
                .and_then(Node::at)
                .ok_or_else(|| format!("The job {job_id} couldn't be read."))?;
            let k = " ".repeat(key.col());
            lines.inserts.push((
                line_of(key),
                vec![
                    format!("{k}permissions:"),
                    format!("{k}  contents: read"),
                    format!("{k}  id-token: write # lets the job ask GitHub for a signed token Coderhelm checks"),
                ],
            ));
            Ok(Some(
                "Gives the job `contents: read` and `id-token: write`. If the job needs more access, add it to this block.".into(),
            ))
        }
    }
}

/// The edited text parses, the test job gained exactly the steps that were
/// added, and nothing else in the file changed meaning.
fn verify(old: &str, new: &str, facts: &WorkflowFacts, added: &[String]) -> Result<(), String> {
    let a: Yaml = serde_yaml::from_str(old).map_err(|e| e.to_string())?;
    let b: Yaml = serde_yaml::from_str(new)
        .map_err(|e| format!("The change didn't produce valid YAML: {e}"))?;
    let steps = |d: &Yaml| {
        get(get(get(d, "jobs")?, &facts.job.job_id)?, "steps")
            .and_then(Yaml::as_sequence)
            .cloned()
    };
    let (sa, sb) = (steps(&a).unwrap_or_default(), steps(&b).unwrap_or_default());
    let new_steps = added
        .iter()
        .filter(|x| *x == "upload" || *x == "quarantine")
        .count();
    if sb.len() != sa.len() + new_steps {
        return Err("The change didn't land in the test job as planned.".into());
    }
    // Every other job is untouched.
    let jobs = |d: &Yaml| {
        get(d, "jobs")
            .and_then(Yaml::as_mapping)
            .cloned()
            .unwrap_or_default()
    };
    let (ja, jb) = (jobs(&a), jobs(&b));
    for (k, v) in &ja {
        if k.as_str() != Some(facts.job.job_id.as_str()) && jb.get(k) != Some(v) {
            return Err("The change touched another job.".into());
        }
    }
    // Every original step is still there, in order, with its keys (the test
    // step may gain `env`).
    let mut j = 0;
    for s in &sa {
        let name = get(s, "name").cloned();
        let uses = get(s, "uses").cloned();
        while j < sb.len()
            && (get(&sb[j], "name").cloned() != name || get(&sb[j], "uses").cloned() != uses)
        {
            j += 1;
        }
        if j == sb.len() {
            return Err("The change reordered or lost a step.".into());
        }
        j += 1;
    }
    Ok(())
}

// ── diff ─────────────────────────────────────────────────────────────────

/// A unified diff of one file, three lines of context.
pub fn unified_diff(path: &str, old: &str, new: &str) -> String {
    let a: Vec<&str> = old.lines().collect();
    let b: Vec<&str> = new.lines().collect();
    // Longest common subsequence over lines; workflow files are small.
    let (n, m) = (a.len(), b.len());
    let mut lcs = vec![vec![0u32; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            lcs[i][j] = if a[i] == b[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    // (kind, old line, new line, text): ' ' keep, '-' remove, '+' add
    let mut ops: Vec<(char, usize, usize, &str)> = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < n || j < m {
        if i < n && j < m && a[i] == b[j] {
            ops.push((' ', i, j, a[i]));
            i += 1;
            j += 1;
        } else if j < m && (i == n || lcs[i][j + 1] >= lcs[i + 1][j]) {
            ops.push(('+', i, j, b[j]));
            j += 1;
        } else {
            ops.push(('-', i, j, a[i]));
            i += 1;
        }
    }
    let changed: Vec<usize> = ops
        .iter()
        .enumerate()
        .filter(|(_, o)| o.0 != ' ')
        .map(|(k, _)| k)
        .collect();
    if changed.is_empty() {
        return String::new();
    }
    let mut out = format!("--- a/{path}\n+++ b/{path}\n");
    let mut k = 0;
    while k < changed.len() {
        let start = changed[k].saturating_sub(3);
        let mut end = (changed[k] + 3).min(ops.len() - 1);
        while k + 1 < changed.len() && changed[k + 1] <= end + 4 {
            k += 1;
            end = (changed[k] + 3).min(ops.len() - 1);
        }
        let hunk = &ops[start..=end];
        let old_start = hunk[0].1 + 1;
        let new_start = hunk[0].2 + 1;
        let old_len = hunk.iter().filter(|o| o.0 != '+').count();
        let new_len = hunk.iter().filter(|o| o.0 != '-').count();
        out.push_str(&format!(
            "@@ -{old_start},{old_len} +{new_start},{new_len} @@\n"
        ));
        for (kind, _, _, text) in hunk {
            out.push(*kind);
            out.push_str(text);
            out.push('\n');
        }
        k += 1;
    }
    out
}

// ── what has been seen ───────────────────────────────────────────────────

/// Runs of the E2E workflow without results after which the step counts as
/// "added but nothing received".
pub const EMPTY_RUNS_BEFORE_WARNING: u32 = 2;

/// What reading one workflow's runs found, kept per repo.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowSeen {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_results_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_results_url: Option<String>,
    #[serde(default)]
    pub last_tests: u32,
    /// Runs in a row since the last results that sent none.
    #[serde(default)]
    pub empty_runs: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_empty_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_empty_url: Option<String>,
    /// The empty run had a test job that was skipped.
    #[serde(default)]
    pub last_empty_conclusion: String,
}

/// Results and quarantine calls seen for one repo. Kept next to the repo.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CiState {
    /// By workflow file; uploads signed by CI are under "upload".
    #[serde(default)]
    pub workflows: std::collections::BTreeMap<String, WorkflowSeen>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quarantine_fetched_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quarantine_run_url: Option<String>,
}

impl CiState {
    /// Records one finished run of a workflow: `tests` read, or `None` when
    /// it sent no results.
    pub fn note_run(
        &mut self,
        workflow: &str,
        url: &str,
        at: &str,
        conclusion: &str,
        tests: Option<u32>,
    ) {
        let key = if workflow.is_empty() {
            "upload"
        } else {
            workflow
        };
        let w = self.workflows.entry(key.to_string()).or_default();
        match tests {
            Some(n) => {
                w.last_results_at = Some(at.to_string());
                w.last_results_url = Some(url.to_string()).filter(|u| !u.is_empty());
                w.last_tests = n;
                w.empty_runs = 0;
            }
            None => {
                w.empty_runs += 1;
                w.last_empty_at = Some(at.to_string());
                w.last_empty_url = Some(url.to_string()).filter(|u| !u.is_empty());
                w.last_empty_conclusion = conclusion.to_string();
            }
        }
    }

    /// The newest results from any workflow, for repos whose results come
    /// from somewhere else than the detected workflow.
    pub fn newest_results(&self) -> Option<&WorkflowSeen> {
        self.workflows
            .values()
            .filter(|w| w.last_results_at.is_some())
            .max_by(|a, b| a.last_results_at.cmp(&b.last_results_at))
    }
}

/// A setup pull request Coderhelm opened (or is opening) for a repo.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetupPr {
    /// "queued", "reading", "opening", "open", "merged", "closed",
    /// "nothing_to_do" or "failed".
    pub status: String,
    /// "setup" or "repair".
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub quarantine: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub number: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_sha: Option<String>,
    #[serde(default)]
    pub workflow_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub actor: String,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merged_at: Option<String>,
}

impl SetupPr {
    /// Still being opened, or open and waiting for review.
    pub fn is_open(&self) -> bool {
        matches!(
            self.status.as_str(),
            "queued" | "reading" | "opening" | "open"
        )
    }
}

/// What a setup job asks the worker to do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetupRequest {
    pub installation_id: u64,
    pub default_branch: String,
    pub workflow_path: String,
    pub options: Options,
}

/// The overall state of one repo's setup, as the dashboard shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupStatus {
    /// No workflow runs tests: nothing to do.
    NoTests,
    /// The steps are missing and no pull request is open.
    NeedsSetup,
    /// A setup pull request is being opened or waits for review.
    InProgress,
    /// The steps are on the default branch; the next run will tell.
    Waiting,
    /// The steps are there but runs keep arriving without results.
    NothingReceived,
    /// Real results arrived.
    SetUp,
}

/// Decides a repo's state. Green ([`SetupStatus::SetUp`]) only once real
/// results have arrived, never because the YAML looks right.
pub fn status_of(
    facts: Option<&WorkflowFacts>,
    seen: Option<&WorkflowSeen>,
    any_results: bool,
    pr_open: bool,
) -> SetupStatus {
    let empty = seen.map_or(0, |s| s.empty_runs);
    let step_there = facts.is_some_and(|f| f.upload != UploadState::Missing);
    if step_there && empty >= EMPTY_RUNS_BEFORE_WARNING {
        return SetupStatus::NothingReceived;
    }
    if any_results {
        return SetupStatus::SetUp;
    }
    if pr_open {
        return SetupStatus::InProgress;
    }
    match facts {
        None => SetupStatus::NoTests,
        Some(_) if step_there => SetupStatus::Waiting,
        Some(_) => SetupStatus::NeedsSetup,
    }
}

/// The likely reason a workflow with an upload step sends nothing, in plain
/// words, and whether a pull request can fix it.
pub fn likely_cause(
    facts: &WorkflowFacts,
    seen: &WorkflowSeen,
    reporter_ok: Option<bool>,
) -> (String, bool) {
    if seen.last_empty_conclusion == "skipped" {
        return (
            format!(
                "The {} job was skipped in the last runs, so no results were written. Check its `if:` and the events that start the workflow.",
                facts.job.job_name.as_deref().unwrap_or(&facts.job.job_id)
            ),
            false,
        );
    }
    match facts.upload {
        UploadState::OnlyOnSuccess => (
            "The upload step runs only when every test passes. Failed runs, the ones that matter, send nothing.".into(),
            true,
        ),
        UploadState::OtherFiles => (
            format!(
                "The upload step keeps {} but the results are written elsewhere. The artifact path doesn't match.",
                if facts.upload_paths.is_empty() { "other files".to_string() } else { facts.upload_paths.join(", ") }
            ),
            !facts.ours,
        ),
        _ if reporter_ok == Some(false) => (
            format!(
                "{} doesn't write a results file, so the uploaded folder is empty. Add the reporter below.",
                reporter_snippet(facts.job.runner).0
            ),
            false,
        ),
        _ => (
            "The artifact arrives without a report Coderhelm can read. Check that the reporter writes into the uploaded folder.".into(),
            false,
        ),
    }
}

// ── the pull request ─────────────────────────────────────────────────────

/// Everything needed to open a setup pull request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrRequest {
    pub branch: String,
    pub base: String,
    pub path: String,
    pub title: String,
    pub body: String,
    pub commit_message: String,
}

/// The pull request for a planned change.
pub fn pr_request(
    path: &str,
    base: &str,
    facts: &WorkflowFacts,
    plan: &Plan,
    repair: bool,
) -> PrRequest {
    let job = facts.job.job_name.as_deref().unwrap_or(&facts.job.job_id);
    let quarantine = plan.added.iter().any(|a| a == "quarantine");
    let title = if repair {
        "Fix the E2E results upload for Coderhelm".to_string()
    } else if quarantine {
        "Send E2E results to Coderhelm and skip quarantined tests".to_string()
    } else {
        "Send E2E results to Coderhelm".to_string()
    };
    let mut body = format!(
        "## Summary\n\nChanges `{path}` so Coderhelm can read the results of the **{job}** job{}.\n\n## Changes\n\n",
        if quarantine { " and keep quarantined tests from failing the build" } else { "" }
    );
    for n in &plan.notes {
        body.push_str(&format!("- {n}\n"));
    }
    body.push_str(
        "\nNothing else in the workflow changes: comments, order and every other job stay as they were.\n\n## Verification\n\nAfter this merges, the next run of the job sends its results. The Tests page in Coderhelm shows them arriving.\n",
    );
    PrRequest {
        branch: SETUP_BRANCH.to_string(),
        base: base.to_string(),
        path: path.to_string(),
        title: title.clone(),
        body,
        commit_message: title,
    }
}

// ── manual snippets ──────────────────────────────────────────────────────

/// The upload step on its own, for people who add it by hand.
pub fn upload_snippet(runner: Runner, matrix: bool) -> String {
    upload_step(runner, matrix, 0, 2).join("\n")
}

/// The quarantine step and the changed test command, for people who add
/// them by hand.
pub fn quarantine_snippet(test_command: Option<&str>) -> String {
    let mut s = quarantine_step(0, 2).join("\n");
    let cmd = test_command.unwrap_or("npx playwright test");
    s.push_str(&format!(
        "\n# then, on your test step:\n- env:\n    QUARANTINED: ${{{{ steps.{QUARANTINE_STEP_ID}.outputs.grep }}}}\n  run: {cmd}{}",
        grep_flag(cmd)
    ));
    s
}

/// The reporter setting that writes a results file, for people who add it
/// by hand: (file, snippet).
pub fn reporter_snippet(runner: Runner) -> (&'static str, &'static str) {
    match runner {
        Runner::Playwright => (
            "playwright.config.ts",
            "reporter: [[\"html\", { open: \"never\" }], [\"json\", { outputFile: \"test-results/results.json\" }]],",
        ),
        Runner::Jest => (
            "jest.config.js",
            "reporters: [\"default\", [\"jest-junit\", { outputDirectory: \"test-results\" }]],",
        ),
        Runner::Vitest => (
            "vitest.config.ts",
            "test: { reporters: [\"default\", \"junit\"], outputFile: { junit: \"test-results/junit.xml\" } },",
        ),
        Runner::Cypress => (
            "cypress.config.ts",
            "reporter: \"junit\",\nreporterOptions: { mochaFile: \"cypress/results/results-[hash].xml\" },",
        ),
        Runner::Junit => ("your test command", "# write JUnit XML into test-results/"),
    }
}

/// The test command of the found test step, for snippets.
pub fn test_command(yaml: &str, job: &TestJob) -> Option<String> {
    let doc: Yaml = serde_yaml::from_str(yaml).ok()?;
    let step = get(get(get(&doc, "jobs")?, &job.job_id)?, "steps")?
        .as_sequence()?
        .get(job.step_index)?
        .clone();
    let run = step_run(&step);
    run.lines()
        .map(str::trim)
        .find(|l| {
            runner_of_command(l, &Map::new()).is_some()
                || l.starts_with("npm")
                || l.starts_with("pnpm")
                || l.starts_with("yarn")
        })
        .map(str::to_string)
}

#[cfg(test)]
#[path = "ci_setup_tests.rs"]
mod tests;
