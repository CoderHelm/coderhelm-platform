//! The repository's whole test suite, run the way the repository runs it,
//! before a review fix is pushed. Not only the touched tests: a change to a
//! log line once passed its own test and broke another one that asserted
//! the old text. How the suite runs is read from the repository itself (CI
//! workflow steps, AGENTS.md or CONTRIBUTING, a Makefile, package scripts,
//! tox or nox, a script that loops over test files with its environment such
//! as `PYTHONHASHSEED`), never guessed from the stack alone.

use std::io::Read;
use std::sync::OnceLock;

use regex::Regex;
use serde_json::{json, Value};

/// Files that say how a repository runs its tests, read in this order.
pub const SOURCES: &[&str] = &[
    "AGENTS.md",
    "CLAUDE.md",
    "CONTRIBUTING.md",
    "docs/CONTRIBUTING.md",
    ".github/CONTRIBUTING.md",
    "Makefile",
    "makefile",
    "justfile",
    "Taskfile.yml",
    "package.json",
    "tox.ini",
    "noxfile.py",
    "pyproject.toml",
    "setup.cfg",
    "pytest.ini",
    "Cargo.toml",
    "go.mod",
    "Gemfile",
    "build.gradle",
    "pom.xml",
    ".gitlab-ci.yml",
    "README.md",
];

/// Most characters read from one source file.
pub const MAX_SOURCE: usize = 6_000;

/// What the discovery is told.
pub const SYSTEM: &str = "You find how a repository runs its FULL test suite, exactly as the \
repository itself runs it, from its files: CI workflow steps (the job that runs the tests on \
pull requests), AGENTS.md / CLAUDE.md / CONTRIBUTING, a Makefile or justfile target, package \
scripts, tox or nox sessions, or a script that loops over test files. Copy the commands \
exactly, with the environment they set (for example `PYTHONHASHSEED=0`, `CI=true`, a settings \
module) and any loop over test files written as one shell command. `setup` installs what the \
tests need (the CI step's install commands, the lockfile's package manager). `command` runs \
every test, not a subset: no file filters, no `-k`, no single package, unless the repository \
itself always runs it that way. Leave out end-to-end suites that need a browser, a deployed \
site or real cloud services, and name each one in `left_out`. `source` names the file and step \
it came from. When nothing in the files says how the tests run and there are no tests, set \
found to false and say why.";

pub fn schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "found": {"type": "boolean"},
            "setup": {"type": "string"},
            "command": {"type": "string"},
            "source": {"type": "string"},
            "left_out": {"type": "array", "items": {"type": "string"}},
            "reason": {"type": "string"}
        },
        "required": ["found", "setup", "command", "source", "left_out", "reason"],
        "additionalProperties": false
    })
}

/// How the repository runs its full suite.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Suite {
    pub setup: String,
    pub command: String,
    pub source: String,
    pub left_out: Vec<String>,
}

impl Suite {
    /// The one shell command the sandbox runs.
    pub fn script(&self) -> String {
        if self.setup.trim().is_empty() {
            self.command.trim().to_string()
        } else {
            format!("{} && {}", self.setup.trim(), self.command.trim())
        }
    }
}

/// The discovery's answer read: the suite, or why there is none.
pub fn parse(answer: &Value) -> Result<Suite, String> {
    let s = |k: &str| {
        answer
            .get(k)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string()
    };
    if answer.get("found").and_then(|f| f.as_bool()) != Some(true) || s("command").is_empty() {
        let why = s("reason");
        return Err(if why.is_empty() {
            "the repository's files don't say how its tests run".to_string()
        } else {
            why
        });
    }
    Ok(Suite {
        setup: s("setup"),
        command: s("command"),
        source: s("source"),
        left_out: answer
            .get("left_out")
            .and_then(|l| l.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str())
                    .map(|v| v.trim().to_string())
                    .filter(|v| !v.is_empty())
                    .collect()
            })
            .unwrap_or_default(),
    })
}

/// The discovery prompt from the files read (path, text).
pub fn prompt(files: &[(String, String)]) -> String {
    let mut out = String::from("## The repository's files\n");
    for (path, text) in files {
        out.push_str(&format!(
            "\n### {path}\n```\n{}\n```\n",
            common::head_tail_str(text, MAX_SOURCE)
        ));
    }
    out
}

/// Test counts read from a run's output.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub passed: u64,
    pub failed: u64,
    pub skipped: u64,
}

impl Counts {
    pub fn describe(&self) -> String {
        format!(
            "{} passed, {} failed, {} skipped",
            self.passed, self.failed, self.skipped
        )
    }
}

fn re(cell: &'static OnceLock<Regex>, pattern: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pattern).unwrap())
}

/// The counts the common runners print, summed over every summary line
/// (a loop over test files prints one per file). None when no runner's
/// summary is in the output.
pub fn counts(output: &str) -> Option<Counts> {
    static PYTEST: OnceLock<Regex> = OnceLock::new();
    static PYTEST_PART: OnceLock<Regex> = OnceLock::new();
    static JEST: OnceLock<Regex> = OnceLock::new();
    static JEST_PART: OnceLock<Regex> = OnceLock::new();
    static CARGO: OnceLock<Regex> = OnceLock::new();
    static UNITTEST: OnceLock<Regex> = OnceLock::new();
    static UNITTEST_FAIL: OnceLock<Regex> = OnceLock::new();
    static GO: OnceLock<Regex> = OnceLock::new();
    static MOCHA: OnceLock<Regex> = OnceLock::new();
    static RSPEC: OnceLock<Regex> = OnceLock::new();

    let mut c = Counts::default();
    let mut seen = false;
    let num = |s: &str| s.parse::<u64>().unwrap_or(0);

    // pytest: "==== 12 passed, 1 failed, 2 skipped in 3.2s ===="
    let pytest = re(
        &PYTEST,
        r"(?m)^=+ (.*\d+ (?:passed|failed|errors?|skipped)[^=]*) in [\d.]+s",
    );
    let part = re(
        &PYTEST_PART,
        r"(\d+) (passed|failed|errors?|skipped|xfailed|xpassed)",
    );
    for line in pytest.captures_iter(output) {
        seen = true;
        for p in part.captures_iter(&line[1]) {
            let n = num(&p[1]);
            match &p[2] {
                "passed" | "xpassed" | "xfailed" => c.passed += n,
                "skipped" => c.skipped += n,
                _ => c.failed += n,
            }
        }
    }
    // Jest / Vitest: "Tests:       1 failed, 41 passed, 42 total" / "Tests  41 passed (41)"
    let jest = re(&JEST, r"(?m)^\s*Tests:?\s+(.*(?:passed|failed|skipped).*)$");
    let jpart = re(&JEST_PART, r"(\d+) (passed|failed|skipped|todo)");
    for line in jest.captures_iter(output) {
        seen = true;
        for p in jpart.captures_iter(&line[1]) {
            let n = num(&p[1]);
            match &p[2] {
                "passed" => c.passed += n,
                "failed" => c.failed += n,
                _ => c.skipped += n,
            }
        }
    }
    // cargo: "test result: ok. 12 passed; 0 failed; 1 ignored; ..."
    let cargo = re(
        &CARGO,
        r"test result: \w+\. (\d+) passed; (\d+) failed; (\d+) ignored",
    );
    for m in cargo.captures_iter(output) {
        seen = true;
        c.passed += num(&m[1]);
        c.failed += num(&m[2]);
        c.skipped += num(&m[3]);
    }
    // unittest: "Ran 12 tests in 0.3s" then "OK" or "FAILED (failures=1, errors=2, skipped=1)"
    let ran = re(&UNITTEST, r"(?m)^Ran (\d+) tests? in [\d.]+s");
    let ufail = re(
        &UNITTEST_FAIL,
        r"(?m)^(?:FAILED|OK) ?\(?((?:\w+=\d+(?:, )?)*)\)?$",
    );
    let runs: Vec<u64> = ran.captures_iter(output).map(|m| num(&m[1])).collect();
    if !runs.is_empty() {
        seen = true;
        let mut bad = 0;
        let mut skipped = 0;
        for m in ufail.captures_iter(output) {
            for kv in m[1].split(", ").filter(|s| !s.is_empty()) {
                if let Some((k, v)) = kv.split_once('=') {
                    match k {
                        "failures" | "errors" | "unexpected_successes" => bad += num(v),
                        "skipped" | "expected_failures" => skipped += num(v),
                        _ => {}
                    }
                }
            }
        }
        let total: u64 = runs.iter().sum();
        c.failed += bad;
        c.skipped += skipped;
        c.passed += total.saturating_sub(bad + skipped);
    }
    // go test -v: "--- PASS: TestX", "--- FAIL: TestY", "--- SKIP: TestZ"
    let go = re(&GO, r"(?m)^\s*--- (PASS|FAIL|SKIP): ");
    for m in go.captures_iter(output) {
        seen = true;
        match &m[1] {
            "PASS" => c.passed += 1,
            "FAIL" => c.failed += 1,
            _ => c.skipped += 1,
        }
    }
    // mocha: "  12 passing (30ms)", "  1 failing", "  2 pending"
    let mocha = re(&MOCHA, r"(?m)^\s+(\d+) (passing|failing|pending)\b");
    for m in mocha.captures_iter(output) {
        seen = true;
        let n = num(&m[1]);
        match &m[2] {
            "passing" => c.passed += n,
            "failing" => c.failed += n,
            _ => c.skipped += n,
        }
    }
    // rspec: "42 examples, 1 failure, 2 pending"
    let rspec = re(
        &RSPEC,
        r"(?m)^(\d+) examples?, (\d+) failures?(?:, (\d+) pending)?",
    );
    for m in rspec.captures_iter(output) {
        seen = true;
        let total = num(&m[1]);
        let failed = num(&m[2]);
        let pending = m.get(3).map(|p| num(p.as_str())).unwrap_or(0);
        c.failed += failed;
        c.skipped += pending;
        c.passed += total.saturating_sub(failed + pending);
    }
    seen.then_some(c)
}

/// What one full-suite run found, for the evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Ran and passed.
    Passed {
        command: String,
        source: String,
        counts: Option<Counts>,
    },
    /// Ran and failed.
    Failed {
        command: String,
        source: String,
        counts: Option<Counts>,
        exit: Option<i32>,
        tail: String,
    },
    /// Could not run; why.
    NotRun(String),
}

impl Outcome {
    /// One evidence line: the exact command and the counts.
    pub fn line(&self) -> String {
        let counts = |c: &Option<Counts>| match c {
            Some(c) => c.describe(),
            None => "the runner printed no counts".to_string(),
        };
        match self {
            Outcome::Passed {
                command,
                source,
                counts: c,
            } => format!(
                "Full test suite passed: `{}` ({}), from {source}",
                one_line(command),
                counts(c)
            ),
            Outcome::Failed {
                command,
                source,
                counts: c,
                exit,
                ..
            } => format!(
                "Full test suite FAILED (exit {}): `{}` ({}), from {source}",
                exit.map(|e| e.to_string()).unwrap_or_else(|| "?".into()),
                one_line(command),
                counts(c)
            ),
            Outcome::NotRun(why) => format!("Full test suite not run: {why}"),
        }
    }
}

fn one_line(s: &str) -> String {
    let l = s.split_whitespace().collect::<Vec<_>>().join(" ");
    common::truncate_str(&l, 300).to_string()
}

/// A GitHub tarball (gzip, one top folder) with the staged files written
/// over it and removed files left out, gzipped again: the source the
/// sandbox checks before anything is pushed.
pub fn overlay_tarball(
    tarball: &[u8],
    writes: &[(String, Option<String>)],
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut raw = Vec::new();
    flate2::read::GzDecoder::new(tarball).read_to_end(&mut raw)?;
    let mut archive = tar::Archive::new(raw.as_slice());
    let mut out = tar::Builder::new(Vec::new());
    let mut prefix: Option<String> = None;
    let mut written: std::collections::BTreeSet<String> = Default::default();
    for entry in archive.entries()? {
        let mut entry = entry?;
        if matches!(
            entry.header().entry_type(),
            tar::EntryType::XGlobalHeader | tar::EntryType::XHeader
        ) {
            continue;
        }
        let path = entry.path()?.to_string_lossy().to_string();
        let (top, rel) = match path.split_once('/') {
            Some((t, r)) => (t.to_string(), r.to_string()),
            None => (path.clone(), String::new()),
        };
        if prefix.is_none() && !top.is_empty() && top != "pax_global_header" {
            prefix = Some(top.clone());
        }
        let staged = writes.iter().find(|(p, _)| *p == rel);
        match staged {
            Some((_, None)) => continue,
            Some((p, Some(content))) if entry.header().entry_type().is_file() => {
                let mut h = entry.header().clone();
                h.set_size(content.len() as u64);
                h.set_cksum();
                out.append_data(&mut h, &path, content.as_bytes())?;
                written.insert(p.clone());
            }
            _ => {
                let mut h = entry.header().clone();
                let mut data = Vec::new();
                entry.read_to_end(&mut data)?;
                out.append_data(&mut h, &path, data.as_slice())?;
            }
        }
    }
    let prefix = prefix.ok_or("the tarball has no top folder")?;
    for (p, content) in writes {
        let Some(content) = content else { continue };
        if written.contains(p) {
            continue;
        }
        let mut h = tar::Header::new_gnu();
        h.set_size(content.len() as u64);
        h.set_mode(0o644);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_cksum();
        out.append_data(&mut h, format!("{prefix}/{p}"), content.as_bytes())?;
    }
    let tar_bytes = out.into_inner()?;
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    std::io::Write::write_all(&mut gz, &tar_bytes)?;
    Ok(gz.finish()?)
}

/// A short, stable name for the staged state, so each checked state is its
/// own sandbox source.
pub fn state_tag(writes: &[(String, Option<String>)]) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    writes.hash(&mut h);
    format!("{:016x}", h.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_discovery_answer_reads_into_one_command() {
        let s = parse(&json!({
            "found": true,
            "setup": "pip install -r requirements.txt",
            "command": "for f in tests/test_*.py; do PYTHONHASHSEED=0 python -m pytest \"$f\" || exit 1; done",
            "source": ".github/workflows/ci.yml, job test",
            "left_out": ["e2e (needs a browser)", ""],
            "reason": ""
        }))
        .unwrap();
        assert!(s
            .script()
            .starts_with("pip install -r requirements.txt && for f in"));
        assert!(s.script().contains("PYTHONHASHSEED=0"));
        assert_eq!(s.left_out, vec!["e2e (needs a browser)".to_string()]);
        let none = parse(
            &json!({"found": false, "setup": "", "command": "", "source": "", "left_out": [], "reason": "no tests in the repository"}),
        );
        assert_eq!(none.unwrap_err(), "no tests in the repository");
        assert!(parse(&json!({"found": true, "command": "  "})).is_err());
    }

    #[test]
    fn counts_are_read_from_each_runner_and_summed_over_a_loop() {
        let pytest =
            "==== 3 passed, 1 skipped in 0.12s ====\n==== 2 passed, 1 failed in 0.3s ====\n";
        assert_eq!(
            counts(pytest),
            Some(Counts {
                passed: 5,
                failed: 1,
                skipped: 1
            })
        );
        let jest = "Tests:       1 failed, 41 passed, 42 total\n";
        assert_eq!(
            counts(jest),
            Some(Counts {
                passed: 41,
                failed: 1,
                skipped: 0
            })
        );
        let vitest = " Tests  12 passed (12)\n";
        assert_eq!(counts(vitest).unwrap().passed, 12);
        let cargo = "test result: ok. 10 passed; 0 failed; 2 ignored; 0 measured\ntest result: ok. 5 passed; 0 failed; 0 ignored; 0 measured\n";
        assert_eq!(
            counts(cargo),
            Some(Counts {
                passed: 15,
                failed: 0,
                skipped: 2
            })
        );
        let unittest = "Ran 10 tests in 0.5s\n\nFAILED (failures=1, skipped=2)\n";
        assert_eq!(
            counts(unittest),
            Some(Counts {
                passed: 7,
                failed: 1,
                skipped: 2
            })
        );
        let go =
            "--- PASS: TestA (0.00s)\n--- FAIL: TestB (0.01s)\n    --- PASS: TestB/sub (0.00s)\n";
        assert_eq!(
            counts(go),
            Some(Counts {
                passed: 2,
                failed: 1,
                skipped: 0
            })
        );
        let rspec = "42 examples, 1 failure, 2 pending\n";
        assert_eq!(counts(rspec).unwrap().passed, 39);
        assert_eq!(counts("compiling...\ndone\n"), None);
    }

    #[test]
    fn the_evidence_line_names_the_exact_command_and_counts() {
        let o = Outcome::Passed {
            command: "make test".into(),
            source: "Makefile, target test".into(),
            counts: Some(Counts {
                passed: 120,
                failed: 0,
                skipped: 3,
            }),
        };
        assert_eq!(
            o.line(),
            "Full test suite passed: `make test` (120 passed, 0 failed, 3 skipped), from Makefile, target test"
        );
        assert!(Outcome::NotRun("sandbox off".into())
            .line()
            .ends_with("sandbox off"));
    }

    fn tarball(files: &[(&str, &str)]) -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());
        for (p, c) in files {
            let mut h = tar::Header::new_gnu();
            h.set_size(c.len() as u64);
            h.set_mode(0o644);
            h.set_entry_type(tar::EntryType::Regular);
            h.set_cksum();
            b.append_data(&mut h, format!("org-repo-abc123/{p}"), c.as_bytes())
                .unwrap();
        }
        let raw = b.into_inner().unwrap();
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        std::io::Write::write_all(&mut gz, &raw).unwrap();
        gz.finish().unwrap()
    }

    fn read_back(gz: &[u8]) -> Vec<(String, String)> {
        let mut raw = Vec::new();
        flate2::read::GzDecoder::new(gz)
            .read_to_end(&mut raw)
            .unwrap();
        let mut a = tar::Archive::new(raw.as_slice());
        let mut out = Vec::new();
        for e in a.entries().unwrap() {
            let mut e = e.unwrap();
            let p = e.path().unwrap().to_string_lossy().to_string();
            let mut s = String::new();
            e.read_to_string(&mut s).unwrap();
            out.push((p, s));
        }
        out
    }

    #[test]
    fn the_overlay_writes_removes_and_adds_files_under_the_top_folder() {
        let src = tarball(&[("a.py", "old"), ("b.py", "keep"), ("c.py", "gone")]);
        let out = overlay_tarball(
            &src,
            &[
                ("a.py".into(), Some("new".into())),
                ("c.py".into(), None),
                ("tests/test_d.py".into(), Some("added".into())),
            ],
        )
        .unwrap();
        let files = read_back(&out);
        assert!(files.contains(&("org-repo-abc123/a.py".into(), "new".into())));
        assert!(files.contains(&("org-repo-abc123/b.py".into(), "keep".into())));
        assert!(files.contains(&("org-repo-abc123/tests/test_d.py".into(), "added".into())));
        assert!(!files.iter().any(|(p, _)| p.ends_with("c.py")));
    }

    #[test]
    fn each_staged_state_gets_its_own_tag() {
        let a = state_tag(&[("a".into(), Some("1".into()))]);
        let b = state_tag(&[("a".into(), Some("2".into()))]);
        assert_ne!(a, b);
        assert_eq!(a, state_tag(&[("a".into(), Some("1".into()))]));
    }
}
