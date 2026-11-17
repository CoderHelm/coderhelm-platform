//! What a fix must show before it is committed and pushed, read from the
//! commit itself, never from what the coder says about it.
//!
//! Three pull requests went wrong the same way. A CI fix round pushed while
//! the coder's own run of the touched tests failed: the sandbox's test step
//! ran another package's suite with `--passWithNoTests`, so "2 of 2 checks
//! passed" while `vitest` on the changed files exited 1, four times. A commit
//! titled "Revert the change that broke CI" put a source file back and left
//! the tests that expected the reverted behaviour in place. Twice the branch
//! was rolled back to an older green head as the "fix".
//!
//! The gates, each a function here and each called where the commit is made:
//!
//! - **Checks before every push** ([`verdict`], [`package_steps`]): the
//!   commands come from the repository itself, the scripts of the package
//!   that owns each touched test file and the steps its CI runs. A red
//!   result holds the push; a fix nothing ran on is not pushed; a sandbox
//!   that could not install pushes with an `Unverified:` line in the commit.
//! - **A commit message matches its diff** ([`message_problems`],
//!   [`true_message`]): a message that says revert, remove or drop and adds
//!   test lines, one that says add and only deletes, or one that names a
//!   file the commit doesn't touch, is rewritten from the diff.
//! - **A revert takes its tests with it** ([`removed_symbols`],
//!   [`still_expected`]): a definition the commit removes that a test still
//!   names holds the push until the test goes in the same commit.
//! - **Two fixes per failing test** ([`FixLedger`]): a test still red after
//!   two pushed fixes stops the fix rounds with the test and its error named.
//!   Never a rollback to an older head.
//! - **No logs is no reason to stop** ([`logs_unread_brief`],
//!   [`hold_no_logs`]): when CI's job log can't be read, the fixer is given
//!   the repository's own commands to reproduce the failure locally, and a
//!   sentence that blames the missing log is replaced.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

// ── Checks before every push ───────────────────────────────────────────────

/// What the sandbox's last check run on the commit's tree came to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Checked {
    /// A sandbox can run the repository's checks for this run.
    pub sandbox: bool,
    /// The commit touches test files.
    pub touched_tests: bool,
    pub tests_ran: bool,
    pub tests_passed: bool,
    pub types_ran: bool,
    pub types_passed: bool,
    /// Why the last attempt gave no result (the install failed, the build
    /// had no result).
    pub failure: Option<String>,
    /// The tree changed after the last check run, or none ran.
    pub owed: bool,
}

/// Whether a fix may be pushed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// The checks ran on this tree and passed.
    Verified,
    /// The checks ran on this tree and failed: never pushed.
    Red(String),
    /// The checks could not run (no sandbox, the install failed): pushed
    /// with an `Unverified:` line in the commit saying why.
    Unverified(String),
    /// The tree changed after the last check run: the checks run first.
    Owed,
}

/// The push verdict for a fix, from its last check run.
pub fn verdict(c: &Checked) -> Verdict {
    if !c.sandbox {
        return Verdict::Unverified("no sandbox could run the repository's checks".into());
    }
    if let Some(why) = c.failure.as_deref().filter(|w| !w.trim().is_empty()) {
        return Verdict::Unverified(format!("the sandbox gave no result ({})", why.trim()));
    }
    if c.owed {
        return Verdict::Owed;
    }
    let mut red = Vec::new();
    if c.types_ran && !c.types_passed {
        red.push("the type check");
    }
    if c.tests_ran && !c.tests_passed {
        red.push("the touched tests");
    }
    if !red.is_empty() {
        return Verdict::Red(format!("{} failed in the sandbox", red.join(" and ")));
    }
    if c.touched_tests && !c.tests_ran {
        return Verdict::Unverified(
            "no unit suite of the repository ran for this change (no test script covers the \
             changed package)"
                .into(),
        );
    }
    Verdict::Verified
}

/// The line an unverified commit carries.
pub fn unverified_line(why: &str) -> String {
    format!(
        "Unverified: {}. CI is the first run of this change.",
        why.trim()
    )
}

/// A check the sandbox runs before a push, from the repository's own files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkgStep {
    /// `tests`, `types` or `lint`.
    pub kind: &'static str,
    /// The package folder ("" for the root).
    pub dir: String,
    pub cmd: String,
}

/// A unit or component test file, by its name.
pub fn is_test_file(path: &str) -> bool {
    let p = path.to_lowercase();
    let base = p.rsplit('/').next().unwrap_or(&p);
    base.contains(".test.") || base.contains(".spec.") || p.contains("__tests__/")
}

/// A test that needs a browser or deployed services: never a sandbox step.
fn is_browser_test(path: &str, script: &str) -> bool {
    let p = path.to_lowercase();
    let s = script.to_lowercase();
    p.starts_with("e2e/")
        || p.contains("/e2e/")
        || ["playwright", "cypress", "e2e"]
            .iter()
            .any(|w| s.contains(w))
}

/// A source file a unit suite can cover (not a doc, an asset or a config).
pub fn is_code_file(path: &str) -> bool {
    let p = path.to_lowercase();
    [
        ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".vue", ".svelte",
    ]
    .iter()
    .any(|e| p.ends_with(e))
}

/// The package manager the repository's lockfiles name.
pub fn package_manager(root_files: &[String]) -> &'static str {
    let has = |f: &str| root_files.iter().any(|p| p == f);
    if has("pnpm-lock.yaml") {
        "pnpm"
    } else if has("yarn.lock") {
        "yarn"
    } else if has("bun.lockb") || has("bun.lock") {
        "bun"
    } else {
        "npm"
    }
}

/// `run <script>` with arguments, as each package manager passes them on.
fn run_script(pm: &str, script: &str, args: &str) -> String {
    let args = args.trim();
    match (pm, args.is_empty()) {
        (_, true) => format!("{pm} run {script}"),
        ("npm", false) => format!("npm run {script} -- {args}"),
        (_, false) => format!("{pm} run {script} {args}"),
    }
}

/// The folder of the nearest manifest above `path` (`manifests`: folder,
/// "" for the root).
fn owner_dir<'a>(path: &str, dirs: &'a [String]) -> Option<&'a String> {
    dirs.iter()
        .filter(|d| d.is_empty() || path.starts_with(&format!("{d}/")))
        .max_by_key(|d| d.len())
}

/// The checks to run before a push, read from the repository's own files:
/// for each package that owns a touched file (`manifests`: package folder
/// and its `package.json`), its test script on the touched test files, and
/// its type-check and lint scripts. `root_files`: the files at the
/// repository root (the lockfile names the package manager). Browser and
/// end-to-end tests are left to CI. Nothing is hard-coded per repository:
/// a package without such a script gets no step for it.
pub fn package_steps(
    touched: &[String],
    manifests: &[(String, String)],
    root_files: &[String],
) -> Vec<PkgStep> {
    let pm = package_manager(root_files);
    let dirs: Vec<String> = manifests.iter().map(|(d, _)| d.clone()).collect();
    let mut by_dir: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for path in touched {
        if let Some(d) = owner_dir(path, &dirs) {
            by_dir.entry(d.clone()).or_default().push(path.clone());
        }
    }
    let mut out = Vec::new();
    for (dir, files) in by_dir {
        let Some((_, manifest)) = manifests.iter().find(|(d, _)| *d == dir) else {
            continue;
        };
        let scripts: BTreeMap<String, String> = serde_json::from_str::<serde_json::Value>(manifest)
            .ok()
            .and_then(|v| v.get("scripts").cloned())
            .and_then(|s| serde_json::from_value(s).ok())
            .unwrap_or_default();
        let cd = |cmd: String| {
            if dir.is_empty() {
                cmd
            } else {
                format!("cd {dir} && {cmd}")
            }
        };
        let rel = |p: &str| {
            if dir.is_empty() {
                p.to_string()
            } else {
                p.strip_prefix(&format!("{dir}/")).unwrap_or(p).to_string()
            }
        };
        let safe = |p: &str| {
            p.chars()
                .all(|c| c.is_ascii_alphanumeric() || "./_-[]()".contains(c))
        };
        // The test script: the unit one when the package splits them.
        let test = ["test:unit", "test"]
            .iter()
            .find_map(|n| scripts.get(*n).map(|s| (*n, s.as_str())))
            .filter(|(_, s)| !s.trim().is_empty());
        let tests: Vec<String> = files
            .iter()
            .filter(|f| is_test_file(f))
            .filter(|f| !is_browser_test(&rel(f), test.map_or("", |(_, s)| s)))
            .map(|f| rel(f))
            .filter(|f| safe(f))
            .collect();
        // A change to code that isn't a test reaches callers outside the
        // diff (a shared helper's other users): the package's whole unit
        // suite runs, the command CI runs for it. Only a change to nothing
        // but test files runs just those files.
        let code_changed = files
            .iter()
            .any(|f| !is_test_file(f) && is_code_file(f) && !is_browser_test(&rel(f), ""));
        if let (Some((name, body)), true) = (test, code_changed || !tests.is_empty()) {
            let low = body.to_lowercase();
            let files = if code_changed {
                String::new()
            } else {
                tests.join(" ")
            };
            let watch = low.trim_start().starts_with("vitest") && !low.contains(" run");
            let cmd = if watch {
                // `vitest` alone watches; `vitest run` runs once.
                format!("{pm} exec vitest run {files}")
                    .trim_end()
                    .to_string()
            } else {
                run_script(pm, name, &files)
            };
            out.push(PkgStep {
                kind: if code_changed { "suite" } else { "tests" },
                dir: dir.clone(),
                cmd: cd(format!("CI=true {cmd}")),
            });
        }
        if let Some(name) = ["check-types", "typecheck", "type-check", "types", "tsc"]
            .iter()
            .find(|n| scripts.contains_key(**n))
        {
            out.push(PkgStep {
                kind: "types",
                dir: dir.clone(),
                cmd: cd(run_script(pm, name, "")),
            });
        }
        if scripts.contains_key("lint") {
            out.push(PkgStep {
                kind: "lint",
                dir: dir.clone(),
                cmd: cd(run_script(pm, "lint", "")),
            });
        }
    }
    out
}

/// The folders whose `package.json` decides the steps for `touched`: each
/// file's folder and every folder above it, root included.
pub fn manifest_dirs(touched: &[String]) -> Vec<String> {
    let mut out: Vec<String> = vec![String::new()];
    for p in touched {
        let mut parts: Vec<&str> = p.split('/').collect();
        parts.pop();
        while !parts.is_empty() {
            let d = parts.join("/");
            if !out.contains(&d) {
                out.push(d);
            }
            parts.pop();
        }
    }
    out
}

// ── A commit message matches its diff ──────────────────────────────────────

/// One file of a commit, as the compare API gives it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileChange {
    pub path: String,
    /// `added`, `modified`, `removed`, `renamed`.
    pub status: String,
    pub additions: u64,
    pub deletions: u64,
    pub patch: String,
}

impl FileChange {
    /// From one entry of a compare's `files`.
    pub fn from_compare(f: &serde_json::Value) -> Option<Self> {
        Some(Self {
            path: f["filename"].as_str()?.to_string(),
            status: f["status"].as_str().unwrap_or("modified").to_string(),
            additions: f["additions"].as_u64().unwrap_or(0),
            deletions: f["deletions"].as_u64().unwrap_or(0),
            patch: f["patch"].as_str().unwrap_or("").to_string(),
        })
    }
}

/// Every file of a compare.
pub fn changes_of(compare: &serde_json::Value) -> Vec<FileChange> {
    compare["files"]
        .as_array()
        .map(|a| a.iter().filter_map(FileChange::from_compare).collect())
        .unwrap_or_default()
}

const TAKE_WORDS: &[&str] = &[
    "revert",
    "remove",
    "drop",
    "delete",
    "take out",
    "roll back",
];
const ADD_WORDS: &[&str] = &["add ", "adds ", "create", "introduce", "new "];

fn says(message: &str, words: &[&str]) -> bool {
    let low = format!("{} ", message.lines().next().unwrap_or("").to_lowercase());
    words.iter().any(|w| low.contains(w))
}

/// File names a message names: backticked paths or words with an extension.
fn named_files(message: &str) -> Vec<String> {
    let re = regex::Regex::new(r"[A-Za-z0-9_\-/\.\[\]]+\.(?:tsx?|jsx?|mjs|cjs|rs|py|go|rb|java|kt|swift|css|scss|json|ya?ml|md|sql|tf)\b")
        .expect("valid regex");
    let mut out: Vec<String> = re
        .find_iter(message)
        .map(|m| m.as_str().trim_start_matches("./").to_string())
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Where a commit's message says something its diff doesn't do. Empty when
/// they agree.
pub fn message_problems(message: &str, files: &[FileChange]) -> Vec<String> {
    let mut out = Vec::new();
    if files.is_empty() {
        out.push("the commit changes no file".to_string());
        return out;
    }
    let adds: u64 = files.iter().map(|f| f.additions).sum();
    let dels: u64 = files.iter().map(|f| f.deletions).sum();
    if says(message, TAKE_WORDS) {
        let grown: Vec<&str> = files
            .iter()
            .filter(|f| is_test_file(&f.path))
            .filter(|f| f.status == "added" || f.additions > f.deletions)
            .map(|f| f.path.as_str())
            .collect();
        if !grown.is_empty() {
            out.push(format!(
                "it says it takes something out, and it adds test lines to {}",
                grown.join(", ")
            ));
        }
        if adds > dels {
            out.push(format!(
                "it says it takes something out, and the diff adds more than it removes (+{adds}/-{dels})"
            ));
        }
    } else if says(message, ADD_WORDS) && adds == 0 {
        out.push(format!(
            "it says it adds, and the diff only removes (-{dels})"
        ));
    }
    for named in named_files(message) {
        let hit = files.iter().any(|f| {
            f.path == named || f.path.ends_with(&format!("/{named}")) || named.ends_with(&f.path)
        });
        if !hit {
            out.push(format!(
                "it names `{named}`, which the commit doesn't change"
            ));
        }
    }
    out
}

/// A ticket key at the start of a subject (`ACME-12: `, `[ACME-12] `).
fn ticket_prefix(subject: &str) -> &str {
    let re = regex::Regex::new(r"^\[?[A-Z][A-Z0-9]+-\d+\]?:?\s+").expect("valid regex");
    re.find(subject).map_or("", |m| m.as_str())
}

/// The subject a diff gives itself: what it does to how many files.
pub fn subject_from_diff(files: &[FileChange]) -> String {
    let adds: u64 = files.iter().map(|f| f.additions).sum();
    let dels: u64 = files.iter().map(|f| f.deletions).sum();
    let names: Vec<&str> = files
        .iter()
        .take(3)
        .map(|f| f.path.rsplit('/').next().unwrap_or(&f.path))
        .collect();
    let more = if files.len() > 3 {
        format!(" and {} more", files.len() - 3)
    } else {
        String::new()
    };
    let verb = if files.iter().all(|f| f.status == "removed") {
        "Remove"
    } else if files.iter().all(|f| f.status == "added") {
        "Add"
    } else {
        "Change"
    };
    format!("{verb} {}{more} (+{adds}/-{dels})", names.join(", "))
}

/// The message a commit lands with: `message` when it agrees with the diff,
/// else a subject read from the diff (keeping a ticket key) with what the
/// first message said and why it was replaced.
pub fn true_message(message: &str, files: &[FileChange]) -> String {
    let problems = message_problems(message, files);
    if problems.is_empty() || files.is_empty() {
        return message.to_string();
    }
    let subject = message.lines().next().unwrap_or("").trim();
    let prefix = ticket_prefix(subject);
    let mut body: Vec<String> = files
        .iter()
        .map(|f| {
            format!(
                "- {} {} (+{}/-{})",
                f.status, f.path, f.additions, f.deletions
            )
        })
        .collect();
    body.push(String::new());
    body.push(format!(
        "The message \"{}\" was replaced: {}.",
        subject,
        problems.join("; ")
    ));
    format!(
        "{prefix}{}\n\n{}",
        subject_from_diff(files),
        body.join("\n")
    )
}

// ── A revert takes its tests with it ───────────────────────────────────────

/// Definitions a commit removes from code that isn't a test, with the file
/// they left: a name defined on a removed line and on no added line.
pub fn removed_symbols(files: &[FileChange]) -> Vec<(String, String)> {
    let def = regex::Regex::new(
        r"^\s*(?:export\s+)?(?:default\s+)?(?:pub(?:\([a-z]+\))?\s+)?(?:async\s+)?(?:function\*?|const|let|class|interface|type|enum|def|fn|struct)\s+([A-Za-z_][A-Za-z0-9_]{2,})",
    )
    .expect("valid regex");
    let mut out = Vec::new();
    for f in files.iter().filter(|f| !is_test_file(&f.path)) {
        let mut gone = Vec::new();
        let mut kept = Vec::new();
        for line in f.patch.lines() {
            let (sign, rest) = line.split_at(line.len().min(1));
            if line.starts_with("---") || line.starts_with("+++") {
                continue;
            }
            if let Some(c) = def.captures(rest) {
                match sign {
                    "-" => gone.push(c[1].to_string()),
                    "+" => kept.push(c[1].to_string()),
                    _ => {}
                }
            }
        }
        for name in gone {
            if !kept.contains(&name) && !out.iter().any(|(n, _)| *n == name) {
                out.push((name, f.path.clone()));
            }
        }
    }
    out
}

/// A test that still names a definition the commit removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stale {
    pub symbol: String,
    pub from: String,
    pub test: String,
}

impl Stale {
    pub fn line(&self) -> String {
        format!(
            "`{}` still names `{}`, which this commit removes from `{}`: revert or update that test in the same commit",
            self.test, self.symbol, self.from
        )
    }
}

/// The tests (`tests`: path and content after the commit) that still name
/// a removed definition. A whole word, so `fooBar` doesn't match `foo`.
pub fn still_expected(removed: &[(String, String)], tests: &[(String, String)]) -> Vec<Stale> {
    let mut out = Vec::new();
    for (symbol, from) in removed {
        let Ok(re) = regex::Regex::new(&format!(r"\b{}\b", regex::escape(symbol))) else {
            continue;
        };
        for (test, content) in tests {
            if re.is_match(content) {
                out.push(Stale {
                    symbol: symbol.clone(),
                    from: from.clone(),
                    test: test.clone(),
                });
            }
        }
    }
    out
}

/// The test files worth reading for [`still_expected`]: the ones the commit
/// touches, and the ones whose name carries a changed source file's stem.
pub fn tests_to_read(files: &[FileChange], tree: &[String]) -> Vec<String> {
    let stems: Vec<String> = files
        .iter()
        .filter(|f| !is_test_file(&f.path))
        .filter_map(|f| f.path.rsplit('/').next())
        .filter_map(|b| b.split('.').next())
        .filter(|s| s.len() >= 3)
        .map(str::to_lowercase)
        .collect();
    let mut out: Vec<String> = files
        .iter()
        .filter(|f| is_test_file(&f.path) && f.status != "removed")
        .map(|f| f.path.clone())
        .collect();
    for p in tree.iter().filter(|p| is_test_file(p)) {
        let base = p.rsplit('/').next().unwrap_or(p).to_lowercase();
        if stems.iter().any(|s| base.contains(s.as_str())) && !out.contains(p) {
            out.push(p.clone());
        }
    }
    out.truncate(25);
    out
}

// ── Two fixes per failing test ─────────────────────────────────────────────

/// Pushed fixes for a failing test before the fix rounds stop on it.
pub const MAX_FIXES_PER_TEST: u32 = 2;

/// How many fixes were pushed for each failing test (or, when the logs name
/// no test, each failing check), kept on the run record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixLedger(pub BTreeMap<String, u32>);

/// Where the fix rounds stop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stop {
    pub test: String,
    pub fixes: u32,
}

impl FixLedger {
    pub fn from_json(s: &str) -> Self {
        serde_json::from_str(s).unwrap_or_default()
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".into())
    }

    /// One fix pushed for each of `failing`.
    pub fn record_push(&mut self, failing: &[String]) {
        for t in failing {
            *self.0.entry(t.clone()).or_insert(0) += 1;
        }
    }

    /// The first test still failing after [`MAX_FIXES_PER_TEST`] pushed
    /// fixes, when one is.
    pub fn stop(&self, failing: &[String]) -> Option<Stop> {
        failing.iter().find_map(|t| {
            let n = self.0.get(t).copied().unwrap_or(0);
            (n >= MAX_FIXES_PER_TEST).then(|| Stop {
                test: t.clone(),
                fixes: n,
            })
        })
    }
}

impl Stop {
    /// What the pull request and the run say: the test, its error, that
    /// nothing more is pushed and nothing was rolled back.
    pub fn text(&self, error: &str) -> String {
        let error = error.trim();
        let error = if error.is_empty() {
            "the error is in the check's log".to_string()
        } else {
            format!("`{}`", crate::truncate_str(error, 300))
        };
        format!(
            "Stopped after {} pushed fixes for the failing test `{}`, still failing with {}. \
             No more fixes are pushed and nothing was rolled back: the branch keeps its work. \
             The next step is a person's look at this test and its error.",
            self.fixes, self.test, error
        )
    }
}

/// The failing tests a CI log names: vitest and jest (`FAIL file > name`,
/// `✕`/`×` lines), Playwright (`✘ … › name`), pytest (`FAILED path::name`),
/// cargo (`test name ... FAILED`) and go (`--- FAIL: Name`). Timestamps of
/// the Actions log are dropped. Empty when none is named.
pub fn failing_tests(logs: &str) -> Vec<String> {
    let ts = regex::Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\s*")
        .expect("valid regex");
    let ansi = regex::Regex::new(r"\x1b\[[0-9;]*m").expect("valid regex");
    let pats = [
        regex::Regex::new(r"^\s*FAIL\s+(\S+\.(?:test|spec)\.[jt]sx?\s*>\s*.+?)\s*$").unwrap(),
        regex::Regex::new(r"^\s*(?:✘|×|✕)\s+(?:\d+\s+)?(?:\[[^\]]+\]\s*›\s*)?(.+?)(?:\s+\(\d+(?:\.\d+)?m?s\))?\s*$").unwrap(),
        regex::Regex::new(r"^\s*FAILED\s+(\S+::\S+)").unwrap(),
        regex::Regex::new(r"^\s*test\s+(\S+)\s+\.\.\.\s+FAILED").unwrap(),
        regex::Regex::new(r"^\s*--- FAIL:\s+(\S+)").unwrap(),
    ];
    let mut out: Vec<String> = Vec::new();
    for raw in logs.lines() {
        let line = ansi.replace_all(raw, "");
        let line = ts.replace(&line, "");
        for p in &pats {
            if let Some(c) = p.captures(&line) {
                let name = c[1].split_whitespace().collect::<Vec<_>>().join(" ");
                if !name.is_empty() && name.len() < 300 && !out.contains(&name) {
                    out.push(name);
                }
                break;
            }
        }
    }
    out
}

// ── No logs is no reason to stop ───────────────────────────────────────────

/// What the fixer is told when CI's job logs can't be read: reproduce the
/// failure with the repository's own commands first (`commands`: the
/// install and test steps of the CI job, or the package scripts).
pub fn logs_unread_brief(failing_checks: &[String], commands: &[String]) -> String {
    let checks = if failing_checks.is_empty() {
        "the failing check".to_string()
    } else {
        failing_checks.join(", ")
    };
    let how = if commands.is_empty() {
        "the repository's own test and check commands (its package scripts and the steps of the \
         CI job)"
            .to_string()
    } else {
        commands
            .iter()
            .map(|c| format!("`{c}`"))
            .collect::<Vec<_>>()
            .join(", then ")
    };
    format!(
        "\n\nCI's job log for {checks} couldn't be read. That is no reason to stop or to guess: \
         reproduce the failure in the sandbox first with {how} (run_checks or bash), read that \
         output, and fix from it. Never write that the log couldn't be read as the reason for a \
         result."
    )
}

/// Whether a sentence gives an unreadable CI log as its reason.
pub fn blames_logs(sentence: &str) -> bool {
    let low = sentence.to_lowercase();
    let cant = [
        "could not",
        "couldn't",
        "can't",
        "cannot",
        "unable to",
        "no access to",
        "not able to",
    ];
    let read = ["read", "fetch", "access", "see", "download", "open", "get"];
    let logs = ["log", "logs", "job output", "ci output"];
    cant.iter().any(|c| {
        low.match_indices(c).any(|(i, _)| {
            let after = &low[i..(i + 60).min(low.len())];
            read.iter().any(|r| after.contains(r)) && logs.iter().any(|l| after.contains(l))
        })
    })
}

/// A text with each sentence that blames an unreadable log replaced by what
/// to do instead (`repro`: the command that reproduces it locally). The
/// flag says whether anything was replaced.
pub fn hold_no_logs(text: &str, repro: &str) -> (String, bool) {
    let mut changed = false;
    let parts: Vec<String> = text
        .split_inclusive(['.', '\n'])
        .map(|s| {
            if blames_logs(s) {
                changed = true;
                let lead = &s[..s.len() - s.trim_start().len()];
                let end = if s.ends_with('\n') { "\n" } else { "" };
                if repro.trim().is_empty() {
                    format!(
                        "{lead}CI's job log wasn't readable, so the failure is reproduced with \
                         the repository's own commands in the sandbox.{end}"
                    )
                } else {
                    format!(
                        "{lead}CI's job log wasn't readable, so the failure is reproduced with \
                         `{}` in the sandbox.{end}",
                        repro.trim()
                    )
                }
            } else {
                s.to_string()
            }
        })
        .collect();
    (parts.concat().trim_end().to_string(), changed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fc(path: &str, status: &str, a: u64, d: u64, patch: &str) -> FileChange {
        FileChange {
            path: path.into(),
            status: status.into(),
            additions: a,
            deletions: d,
            patch: patch.into(),
        }
    }

    #[test]
    fn a_red_or_unrun_fix_is_never_pushed_and_a_failed_install_says_unverified() {
        let ok = Checked {
            sandbox: true,
            touched_tests: true,
            tests_ran: true,
            tests_passed: true,
            types_ran: true,
            types_passed: true,
            ..Default::default()
        };
        assert_eq!(verdict(&ok), Verdict::Verified);
        let red = Checked {
            tests_passed: false,
            ..ok.clone()
        };
        assert!(matches!(verdict(&red), Verdict::Red(w) if w.contains("touched tests")));
        let owed = Checked {
            owed: true,
            ..ok.clone()
        };
        assert_eq!(verdict(&owed), Verdict::Owed);
        let install = Checked {
            failure: Some("pnpm install exited 1".into()),
            ..ok.clone()
        };
        let Verdict::Unverified(why) = verdict(&install) else {
            panic!("a failed install is unverified");
        };
        assert!(unverified_line(&why).starts_with("Unverified: the sandbox gave no result"));
        let none = Checked {
            tests_ran: false,
            tests_passed: false,
            ..ok
        };
        assert!(matches!(verdict(&none), Verdict::Unverified(_)));
    }

    #[test]
    fn the_steps_come_from_the_package_that_owns_the_touched_tests() {
        let web =
            r#"{"scripts":{"test":"vitest","lint":"next lint","check-types":"tsc --noEmit"}}"#;
        let lambda = r#"{"scripts":{"test":"jest"}}"#;
        let root = r#"{"scripts":{"build":"turbo build"}}"#;
        let manifests = vec![
            (String::new(), root.to_string()),
            ("apps/web".to_string(), web.to_string()),
            ("lambdas/resizer".to_string(), lambda.to_string()),
        ];
        let touched = vec![
            "apps/web/src/utils/filters.ts".to_string(),
            "apps/web/src/test/utils/filters.test.ts".to_string(),
            "e2e/tests/camps.spec.ts".to_string(),
        ];
        let steps = package_steps(&touched, &manifests, &["pnpm-lock.yaml".into()]);
        assert_eq!(
            steps,
            vec![
                // A helper changed: the package's whole suite, as CI runs it.
                PkgStep {
                    kind: "suite",
                    dir: "apps/web".into(),
                    cmd: "cd apps/web && CI=true pnpm exec vitest run".into(),
                },
                PkgStep {
                    kind: "types",
                    dir: "apps/web".into(),
                    cmd: "cd apps/web && pnpm run check-types".into(),
                },
                PkgStep {
                    kind: "lint",
                    dir: "apps/web".into(),
                    cmd: "cd apps/web && pnpm run lint".into(),
                },
            ]
        );
        // The lambda's suite never runs for a web change; an npm repository
        // passes the files after `--`.
        let steps = package_steps(
            &["lambdas/resizer/src/a.test.js".to_string()],
            &manifests,
            &[],
        );
        assert_eq!(
            steps[0].cmd,
            "cd lambdas/resizer && CI=true npm run test -- src/a.test.js"
        );
        assert_eq!(steps[0].kind, "tests");
        // Only test files touched: just those run.
        let only = package_steps(
            &["apps/web/src/test/utils/filters.test.ts".to_string()],
            &manifests,
            &["pnpm-lock.yaml".into()],
        );
        assert_eq!(
            only[0].cmd,
            "cd apps/web && CI=true pnpm exec vitest run src/test/utils/filters.test.ts"
        );
        assert_eq!(
            manifest_dirs(&["apps/web/src/a.ts".into()]),
            vec!["", "apps/web/src", "apps/web", "apps"]
        );
    }

    #[test]
    fn a_revert_message_that_adds_test_lines_is_rewritten_from_the_diff() {
        let files = vec![
            fc("apps/web/src/utils/filters.ts", "modified", 2, 30, ""),
            fc(
                "apps/web/src/test/utils/filters.test.ts",
                "modified",
                40,
                0,
                "",
            ),
        ];
        let msg = "Revert the change that broke CI; it is deferred on the pull request";
        let p = message_problems(msg, &files);
        assert!(p.iter().any(|x| x.contains("adds test lines")), "{p:?}");
        let fixed = true_message(msg, &files);
        assert!(
            fixed.starts_with("Change filters.ts, filters.test.ts (+42/-30)"),
            "{fixed}"
        );
        assert!(fixed.contains("was replaced"));
        // A ticket key stays; a message that agrees stays as written.
        let fixed = true_message("ACME-7: Remove the old filter", &files);
        assert!(fixed.starts_with("ACME-7: Change"), "{fixed}");
        let agree = vec![fc("src/a.ts", "modified", 0, 12, "")];
        assert_eq!(
            true_message("Remove the old filter", &agree),
            "Remove the old filter"
        );
        assert!(message_problems("Add the filter", &agree)[0].contains("only removes"));
        assert!(message_problems("Fix `src/b.ts`", &agree)[0].contains("src/b.ts"));
        assert_eq!(
            message_problems("x", &[]),
            vec!["the commit changes no file"]
        );
    }

    #[test]
    fn a_removed_definition_a_test_still_names_holds_the_push() {
        let patch = "@@ -1,9 +1,3 @@\n-export function dedupeBySlug(items) {\n-  return items;\n-}\n export const keep = 1;\n+export const other = 2;";
        let files = vec![fc("src/utils/filters.ts", "modified", 1, 3, patch)];
        let removed = removed_symbols(&files);
        assert_eq!(
            removed,
            vec![("dedupeBySlug".into(), "src/utils/filters.ts".into())]
        );
        let tests = vec![
            (
                "src/test/filters.test.ts".to_string(),
                "import { dedupeBySlug } from '../utils/filters';".to_string(),
            ),
            (
                "src/test/other.test.ts".to_string(),
                "dedupeBySlugs()".to_string(),
            ),
        ];
        let stale = still_expected(&removed, &tests);
        assert_eq!(stale.len(), 1);
        assert!(stale[0].line().contains("same commit"));
        let tree = vec![
            "src/test/filters.test.ts".to_string(),
            "src/test/unrelated.test.ts".to_string(),
        ];
        assert_eq!(
            tests_to_read(&files, &tree),
            vec!["src/test/filters.test.ts"]
        );
    }

    #[test]
    fn a_test_red_after_two_pushed_fixes_stops_with_its_name() {
        let logs = "2026-11-16T23:37:45.1Z  FAIL  src/test/app/global-error.test.tsx > global-error > reports the error\n\
                    2026-11-16T23:37:45.2Z  ✘  3 [chromium] › tests/camps.spec.ts:12:3 › selecting a camp adds it (12.3s)\n\
                    FAILED tests/test_sync.py::test_upsert - assert 1 == 2\n\
                    test sync::keeps_rows ... FAILED\n\
                    --- FAIL: TestKeep (0.00s)";
        let t = failing_tests(logs);
        assert_eq!(
            t,
            vec![
                "src/test/app/global-error.test.tsx > global-error > reports the error",
                "tests/camps.spec.ts:12:3 › selecting a camp adds it",
                "tests/test_sync.py::test_upsert",
                "sync::keeps_rows",
                "TestKeep",
            ]
        );
        let mut l = FixLedger::default();
        let one = vec![t[0].clone()];
        assert_eq!(l.stop(&one), None);
        l.record_push(&one);
        assert_eq!(l.stop(&one), None);
        l.record_push(&one);
        let stop = l.stop(&one).expect("two pushed fixes stop");
        let text = stop.text("In HTML, <html> cannot be a child of <div>.");
        assert!(text.contains("global-error.test.tsx") && text.contains("<html>"));
        assert!(text.contains("nothing was rolled back"));
        assert_eq!(FixLedger::from_json(&l.to_json()), l);
        assert_eq!(l.stop(&["another".into()]), None);
    }

    #[test]
    fn an_unread_log_is_reproduced_never_a_reason() {
        assert!(blames_logs("I could not read the CI logs, so I left it."));
        assert!(blames_logs("Can't access the job logs for this check"));
        assert!(!blames_logs("The test reads the log file from disk."));
        let (t, changed) = hold_no_logs(
            "Updated the selector. I could not read the logs, so the cause is unconfirmed.",
            "cd apps/web && pnpm exec vitest run src/a.test.ts",
        );
        assert!(changed);
        assert!(
            t.starts_with("Updated the selector. CI's job log wasn't readable"),
            "{t}"
        );
        assert!(!t.contains("could not read"));
        let b = logs_unread_brief(
            &["Test Suite".into()],
            &["pnpm install".into(), "pnpm test".into()],
        );
        assert!(b.contains("`pnpm install`, then `pnpm test`"));
    }
}
