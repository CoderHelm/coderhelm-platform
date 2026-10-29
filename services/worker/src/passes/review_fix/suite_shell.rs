//! The full test suite as shell the sandbox can run, and what a run of it
//! says.
//!
//! A CI workflow is YAML, not shell: a `uses:` step is a GitHub Action. The
//! suite's setup once carried a step written as words ("install uv with
//! Python 3.11 and activate a venv (astral-sh/setup-uv)"), the sandbox's
//! `/bin/sh` failed on the "(", and the syntax error was reported as the
//! repository's tests failing, which held a finished fix. So:
//!
//! - a `uses:` step becomes real shell from [`translate`], or is left out
//!   with a note; [`sanitize`] replaces any step still written as words;
//! - the suite runs under bash, its syntax checked first (`bash -n`), with
//!   its setup and its tests marked apart ([`sandbox_command`]);
//! - [`read_run`] tells a broken command (a syntax error, a missing tool, a
//!   failed setup, an exit 2 within seconds with no counts) from tests that
//!   ran and failed. A broken command is CoderHelm's own failure: the round
//!   falls back to another command, never holds the fix for it.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use base64::Engine as _;
use regex::Regex;

use super::suite::{counts, Counts};

/// Printed when the derived script is not valid bash.
pub const SYNTAX_MARKER: &str = "===CODERHELM_SUITE_SYNTAX===";
/// Printed after the setup, with its exit code.
pub const SETUP_MARKER: &str = "===CODERHELM_SUITE_SETUP exit=";
/// Printed after the test command, with its exit code and how long it ran.
pub const RAN_MARKER: &str = "===CODERHELM_SUITE_RAN exit=";

/// A test command that exits this fast with no counts never reached the
/// tests.
pub const SETUP_SECS: u64 = 10;

/// One `uses:` step of a workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsesStep {
    /// `owner/name`, lowercased, without the `@ref` or a sub-path.
    pub action: String,
    /// Its `with:` inputs that are plain values (expressions left out).
    pub with: BTreeMap<String, String>,
}

impl UsesStep {
    /// The part after the owner (`setup-uv`).
    fn name(&self) -> &str {
        self.action
            .split_once('/')
            .map(|(_, n)| n)
            .unwrap_or(&self.action)
    }
}

/// What a `uses:` step becomes in the sandbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Translation {
    /// This shell, in the setup.
    Shell(String),
    /// Left out, and why.
    Dropped(String),
}

/// The `uses:` steps of one workflow, in order, each action once.
pub fn uses_steps(workflow: &str) -> Vec<UsesStep> {
    let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(workflow) else {
        return Vec::new();
    };
    let mut out: Vec<UsesStep> = Vec::new();
    let Some(jobs) = doc.get("jobs").and_then(|j| j.as_mapping()) else {
        return out;
    };
    for (_, job) in jobs {
        let Some(steps) = job.get("steps").and_then(|s| s.as_sequence()) else {
            continue;
        };
        for step in steps {
            let Some(uses) = step.get("uses").and_then(|u| u.as_str()) else {
                continue;
            };
            let action = action_name(uses);
            if action.is_empty() || out.iter().any(|s| s.action == action) {
                continue;
            }
            let mut with = BTreeMap::new();
            if let Some(w) = step.get("with").and_then(|w| w.as_mapping()) {
                for (k, v) in w {
                    let Some(k) = k.as_str() else { continue };
                    let v = match v {
                        serde_yaml::Value::String(s) => s.clone(),
                        serde_yaml::Value::Number(n) => n.to_string(),
                        serde_yaml::Value::Bool(b) => b.to_string(),
                        _ => continue,
                    };
                    if !v.contains("${{") {
                        with.insert(k.to_string(), v.trim().to_string());
                    }
                }
            }
            out.push(UsesStep { action, with });
        }
    }
    out
}

fn action_name(uses: &str) -> String {
    let base = uses.split('@').next().unwrap_or("").trim();
    if base.starts_with("./") || base.starts_with("docker://") {
        return base.to_ascii_lowercase();
    }
    base.split('/')
        .take(2)
        .collect::<Vec<_>>()
        .join("/")
        .to_ascii_lowercase()
}

/// A version input safe to put in a command (`3.11`, `20.x`, `0.4.18`).
fn version(step: &UsesStep, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|k| step.with.get(*k))
        .map(|v| v.trim().trim_start_matches('v').to_string())
        .find(|v| {
            !v.is_empty()
                && v.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+' | '_'))
        })
}

/// The shell a supported action becomes in the sandbox, or why it is left
/// out. The sandbox already has the source, its own toolchains, and sets
/// Node from the repository.
pub fn translate(step: &UsesStep) -> Translation {
    let a = step.action.as_str();
    let tick = format!("`{a}`");
    match a {
        "actions/checkout"
        | "actions/cache"
        | "actions/upload-artifact"
        | "actions/download-artifact" => Translation::Dropped(format!(
            "{tick} left out: the sandbox already has the source and keeps no cache or artifacts"
        )),
        "astral-sh/setup-uv" => {
            let mut sh =
                "python3 -m pip install -q uv && export PATH=\"$HOME/.local/bin:$PATH\"".to_string();
            if let Some(v) = version(step, &["python-version"]) {
                sh.push_str(&format!(
                    " && uv venv -q --python {v} .venv && . .venv/bin/activate"
                ));
            } else if step.with.get("activate-environment").map(String::as_str) == Some("true") {
                sh.push_str(" && uv venv -q .venv && . .venv/bin/activate");
            }
            Translation::Shell(sh)
        }
        "actions/setup-python" => Translation::Shell(
            "python3 -m venv .venv && . .venv/bin/activate && python -m pip install -q --upgrade pip"
                .to_string(),
        ),
        "actions/setup-node" => match version(step, &["node-version"]) {
            Some(v) => Translation::Shell(format!(
                "{{ n {v} >/dev/null 2>&1 || true; hash -r; }}"
            )),
            None => Translation::Dropped(format!(
                "{tick} left out: the sandbox sets Node from the repository"
            )),
        },
        "pnpm/action-setup" => Translation::Shell(format!(
            "npm install -g -s pnpm@{}",
            version(step, &["version"]).unwrap_or_else(|| "latest".into())
        )),
        "oven-sh/setup-bun" => Translation::Shell(format!(
            "npm install -g -s bun@{}",
            version(step, &["bun-version"]).unwrap_or_else(|| "latest".into())
        )),
        "actions/setup-go"
        | "actions/setup-java"
        | "actions/setup-dotnet"
        | "ruby/setup-ruby"
        | "dtolnay/rust-toolchain"
        | "actions-rs/toolchain"
        | "hashicorp/setup-terraform" => Translation::Dropped(format!(
            "{tick} left out: the sandbox image's own toolchain is used"
        )),
        _ => Translation::Dropped(format!(
            "{tick} left out: a GitHub Action with no sandbox translation"
        )),
    }
}

/// What the discovery is told about the `uses:` steps of the workflows
/// read: the exact shell for each, or that it is left out.
pub fn uses_prompt(files: &[(String, String)]) -> String {
    let steps = workflow_steps(files);
    if steps.is_empty() {
        return String::new();
    }
    let mut out = String::from(
        "\n## The workflows' `uses:` steps in the sandbox\nA `uses:` step is a GitHub Action, \
         not shell. Put exactly this shell in `setup` for it, or leave it out; never describe \
         a step in words.\n",
    );
    for s in &steps {
        match translate(s) {
            Translation::Shell(sh) => out.push_str(&format!("- `{}`: `{sh}`\n", s.action)),
            Translation::Dropped(why) => out.push_str(&format!("- {why}\n")),
        }
    }
    out
}

/// Every `uses:` step of the workflow files among `files`.
pub fn workflow_steps(files: &[(String, String)]) -> Vec<UsesStep> {
    let mut out: Vec<UsesStep> = Vec::new();
    for (path, text) in files {
        if !path.starts_with(".github/workflows/") {
            continue;
        }
        for s in uses_steps(text) {
            if !out.iter().any(|o| o.action == s.action) {
                out.push(s);
            }
        }
    }
    out
}

/// A command cut at its top-level `&&`, `||`, `;` and newlines (outside
/// quotes and parentheses): each piece with the separator after it.
fn pieces(cmd: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let (mut sq, mut dq, mut depth) = (false, false, 0i32);
    let chars: Vec<char> = cmd.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        if !sq && !dq && depth == 0 {
            let sep = match (c, next) {
                ('&', Some('&')) => Some("&&"),
                ('|', Some('|')) => Some("||"),
                (';', _) => Some(";"),
                ('\n', _) => Some("\n"),
                _ => None,
            };
            if let Some(sep) = sep {
                out.push((cur.clone(), sep.to_string()));
                cur.clear();
                i += sep.len();
                continue;
            }
        }
        match c {
            '\\' if !sq => {
                cur.push(c);
                if let Some(n) = next {
                    cur.push(n);
                    i += 1;
                }
            }
            '\'' if !dq => {
                sq = !sq;
                cur.push(c)
            }
            '"' if !sq => {
                dq = !dq;
                cur.push(c)
            }
            '(' if !sq && !dq => {
                depth += 1;
                cur.push(c)
            }
            ')' if !sq && !dq => {
                depth -= 1;
                cur.push(c)
            }
            _ => cur.push(c),
        }
        i += 1;
    }
    out.push((cur, String::new()));
    out
}

/// The step a piece of a command describes in words: it names a workflow
/// action (`astral-sh/setup-uv`, `setup-uv`) or says `uses:`.
fn names_action<'a>(piece: &str, steps: &'a [UsesStep]) -> Option<Option<&'a UsesStep>> {
    let p = piece.to_ascii_lowercase();
    for s in steps {
        let token = |t: &str| {
            p.match_indices(t).any(|(at, _)| {
                let before = p[..at].chars().last();
                let after = p[at + t.len()..].chars().next();
                let edge =
                    |c: Option<char>| c.is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '-'));
                edge(before) && edge(after)
            })
        };
        if token(&s.action) || (s.name().contains('-') && token(s.name())) {
            return Some(Some(s));
        }
    }
    p.contains("uses:").then_some(None)
}

/// The suite's setup and command with every workflow step written as words
/// replaced by its shell ([`translate`]) or left out, and a note for each.
pub fn sanitize(text: &str, steps: &[UsesStep], notes: &mut Vec<String>) -> String {
    let mut kept: Vec<(String, String)> = Vec::new();
    for (piece, sep) in pieces(text) {
        match names_action(&piece, steps) {
            None => kept.push((piece, sep)),
            Some(step) => {
                let what = common::truncate_str(piece.trim(), 120).to_string();
                match step.map(translate) {
                    Some(Translation::Shell(sh)) => {
                        notes.push(format!(
                            "`{what}` was a workflow step written as words; ran `{sh}` for it"
                        ));
                        if !kept.iter().any(|(k, _)| k.trim() == sh) {
                            kept.push((format!(" {sh} "), sep));
                        }
                    }
                    Some(Translation::Dropped(why)) => {
                        notes.push(format!("`{what}`: {why}"));
                    }
                    None => notes.push(format!(
                        "`{what}` was a workflow step written as words, with no shell for it; left out"
                    )),
                }
            }
        }
    }
    // Pieces left empty by a removal take their separator with them.
    let kept: Vec<(String, String)> = kept
        .into_iter()
        .filter(|(p, _)| !p.trim().is_empty())
        .collect();
    let mut out = String::new();
    for (n, (piece, sep)) in kept.iter().enumerate() {
        out.push_str(piece);
        if n + 1 < kept.len() {
            out.push_str(if sep.is_empty() { "&&" } else { sep });
        }
    }
    out.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace(" ; ", "; ")
        .trim()
        .to_string()
}

/// The bash script the sandbox runs: the setup in this shell (so a venv it
/// activates stays active), then the tests in a subshell (an `exit` in a
/// loop ends only them), each with its marker.
pub fn sandbox_script(setup: &str, command: &str) -> String {
    let mut s = String::from("set +e\n");
    if !setup.trim().is_empty() {
        s.push_str(setup.trim());
        s.push_str("\n__ch_setup=$?\n");
        s.push_str(&format!("echo \"{SETUP_MARKER}$__ch_setup===\"\n"));
        s.push_str("if [ \"$__ch_setup\" -ne 0 ]; then exit 98; fi\n");
    }
    s.push_str("__ch_t0=$(date +%s)\n(\n");
    s.push_str(command.trim());
    s.push_str("\n)\n__ch_rc=$?\n");
    s.push_str(&format!(
        "echo \"{RAN_MARKER}$__ch_rc secs=$(( $(date +%s) - __ch_t0 ))===\"\n"
    ));
    s.push_str("exit $__ch_rc\n");
    s
}

/// What the sandbox's `/bin/sh` is handed: the script, base64 so no quote
/// in it can break the wrapper, written out, checked with `bash -n`, and
/// run under bash.
pub fn sandbox_command(setup: &str, command: &str) -> String {
    let b64 = base64::engine::general_purpose::STANDARD.encode(sandbox_script(setup, command));
    format!(
        "printf '%s' '{b64}' | base64 -d > /tmp/coderhelm_suite.sh && \
         if bash -n /tmp/coderhelm_suite.sh; then bash /tmp/coderhelm_suite.sh; \
         else echo \"{SYNTAX_MARKER}\"; exit 96; fi"
    )
}

/// `bash -n` on the script here, before a build is spent on it. Err holds
/// bash's own message. Ok when the script is valid, or when this host has
/// no bash to ask (the sandbox checks again).
pub fn bash_syntax(setup: &str, command: &str) -> Result<(), String> {
    let out = std::process::Command::new("bash")
        .arg("-n")
        .arg("-c")
        .arg(sandbox_script(setup, command))
        .output();
    match out {
        Ok(o) if !o.status.success() => Err(String::from_utf8_lossy(&o.stderr)
            .lines()
            .next()
            .unwrap_or("bash -n failed")
            .trim()
            .to_string()),
        _ => Ok(()),
    }
}

/// What one run of a suite command came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Run {
    Passed {
        counts: Option<Counts>,
    },
    Failed {
        counts: Option<Counts>,
        exit: Option<i32>,
    },
    /// The command CoderHelm built broke before the tests ran: why. Never
    /// a test failure.
    Broken {
        why: String,
        /// Whether the setup finished (exit 0): a later command may keep it.
        setup_ok: bool,
    },
}

fn re(cell: &'static OnceLock<Regex>, pattern: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pattern).unwrap())
}

fn marker_number(output: &str, marker: &str) -> Option<i64> {
    output
        .lines()
        .rev()
        .find_map(|l| l.trim().strip_prefix(marker))
        .and_then(|rest| {
            rest.chars()
                .take_while(|c| c.is_ascii_digit() || *c == '-')
                .collect::<String>()
                .parse()
                .ok()
        })
}

/// The line a shell printed for its own failure (`Syntax error: "("
/// unexpected`, `uv: command not found`).
fn shell_error(output: &str) -> Option<String> {
    static SHELL: OnceLock<Regex> = OnceLock::new();
    let r = re(
        &SHELL,
        r"(?mi)^.*(syntax error|: command not found|: not found$|unexpected EOF|bad substitution).*$",
    );
    r.find_iter(output)
        .last()
        .map(|m| common::truncate_str(m.as_str().trim(), 200).to_string())
}

/// What a finished run's output says: its markers, the runner's counts,
/// its exit code.
pub fn read_run(output: &str, exit: Option<i32>) -> Run {
    let printed = |m: &str| output.lines().any(|l| l.trim().starts_with(m));
    if output.lines().any(|l| l.trim() == SYNTAX_MARKER) || exit == Some(96) && !printed(RAN_MARKER)
    {
        return Run::Broken {
            why: format!(
                "the command CoderHelm built is not valid bash ({})",
                shell_error(output).unwrap_or_else(|| "bash -n failed".into())
            ),
            setup_ok: false,
        };
    }
    let setup = marker_number(output, SETUP_MARKER);
    if let Some(code) = setup.filter(|c| *c != 0) {
        return Run::Broken {
            why: format!(
                "its setup failed (exit {code}) before any test ran{}",
                shell_error(output)
                    .map(|e| format!(": {e}"))
                    .unwrap_or_default()
            ),
            setup_ok: false,
        };
    }
    let setup_ok = setup == Some(0);
    let found = counts(output);
    if exit == Some(0) {
        return match found {
            // A loop that `break`s on a failure exits 0: the counts say.
            Some(c) if c.failed > 0 => Run::Failed {
                counts: found,
                exit,
            },
            _ => Run::Passed { counts: found },
        };
    }
    if found.is_some() {
        return Run::Failed {
            counts: found,
            exit,
        };
    }
    let code = exit.map(|e| e.to_string()).unwrap_or_else(|| "?".into());
    if !printed(RAN_MARKER) {
        return Run::Broken {
            why: format!(
                "the command stopped before its tests finished (exit {code}) and the runner \
                 printed no counts{}",
                shell_error(output)
                    .map(|e| format!(": {e}"))
                    .unwrap_or_default()
            ),
            setup_ok,
        };
    }
    if matches!(exit, Some(126) | Some(127)) || shell_error(output).is_some() {
        return Run::Broken {
            why: format!(
                "a tool the command needs is missing or the shell rejected it (exit {code}, the \
                 runner printed no counts){}",
                shell_error(output)
                    .map(|e| format!(": {e}"))
                    .unwrap_or_default()
            ),
            setup_ok,
        };
    }
    let secs = output
        .lines()
        .rev()
        .find_map(|l| {
            l.trim()
                .strip_prefix(RAN_MARKER)
                .and_then(|r| r.split_once("secs="))
                .and_then(|(_, s)| s.trim_end_matches('=').parse::<u64>().ok())
        })
        .unwrap_or(u64::MAX);
    if exit == Some(2) && secs <= SETUP_SECS {
        return Run::Broken {
            why: format!(
                "exit 2 within {secs}s and the runner printed no counts: the command broke \
                 before the tests ran"
            ),
            setup_ok,
        };
    }
    Run::Failed {
        counts: found,
        exit,
    }
}

/// The test command the requester named, from what they wrote (the round's
/// review comments first, then the ticket): the first span in backticks
/// that runs tests. None when they named none.
pub fn requester_command(texts: &[&str]) -> Option<String> {
    static FENCED: OnceLock<Regex> = OnceLock::new();
    static INLINE: OnceLock<Regex> = OnceLock::new();
    static TESTY: OnceLock<Regex> = OnceLock::new();
    let fenced = re(&FENCED, r"(?s)```[A-Za-z]*\n(.*?)```");
    let inline = re(&INLINE, r"`([^`\n]+)`");
    let testy = re(
        &TESTY,
        r"\b(pytest|tests?|tox|nox|jest|vitest|mocha|rspec|unittest|phpunit|ctest)\b",
    );
    for text in texts {
        let mut spans: Vec<(usize, String)> = fenced
            .captures_iter(text)
            .map(|c| (c.get(0).unwrap().start(), c[1].trim().to_string()))
            .collect();
        let without_fences = fenced.replace_all(text, |c: &regex::Captures| " ".repeat(c[0].len()));
        spans.extend(
            inline
                .captures_iter(&without_fences)
                .map(|c| (c.get(0).unwrap().start(), c[1].trim().to_string())),
        );
        spans.sort_by_key(|(at, _)| *at);
        for (_, span) in spans {
            let span = span
                .lines()
                .map(|l| l.trim().trim_start_matches("$ "))
                .filter(|l| !l.is_empty())
                .collect::<Vec<_>>()
                .join("\n");
            let one_word = !span.contains(char::is_whitespace);
            let bare = matches!(span.as_str(), "pytest" | "tox" | "nox" | "rspec");
            if span.len() > 600 || (one_word && !bare) || !testy.is_match(&span) {
                continue;
            }
            return Some(span);
        }
    }
    None
}

/// The simplest command that runs the repository's tests, from the files
/// read (path, text). None when nothing says how.
pub fn simplest_command(files: &[(String, String)]) -> Option<String> {
    let has = |p: &str| files.iter().find(|(path, _)| path == p).map(|(_, t)| t);
    if let Some(m) = has("Makefile").or_else(|| has("makefile")) {
        if m.lines().any(|l| l.starts_with("test:")) {
            return Some("make test".into());
        }
    }
    if let Some(p) = has("package.json") {
        let v: serde_json::Value = serde_json::from_str(p).unwrap_or_default();
        if let Some(t) = v["scripts"]["test"].as_str() {
            if !t.contains("no test specified") {
                return Some("npm test".into());
            }
        }
    }
    let pytest = has("pytest.ini").is_some()
        || has("pyproject.toml").is_some_and(|t| t.contains("[tool.pytest"))
        || has("setup.cfg").is_some_and(|t| t.contains("[tool:pytest]"))
        || has("tox.ini").is_some_and(|t| t.contains("[pytest]"));
    if pytest {
        return Some("python -m pytest".into());
    }
    if has("Cargo.toml").is_some() {
        return Some("cargo test".into());
    }
    if has("go.mod").is_some() {
        return Some("go test ./...".into());
    }
    if has("Gemfile").is_some_and(|t| t.contains("rspec")) {
        return Some("bundle exec rspec".into());
    }
    if has("pyproject.toml").is_some() || has("setup.cfg").is_some() {
        return Some("python -m unittest discover".into());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A generic Python repository's workflow, the shape that broke.
    pub const UV_WORKFLOW: &str = "name: tests\non: [pull_request]\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n      - run: sudo apt-get update -qq && sudo apt-get install -yqq libpq-dev pkg-config\n      - uses: astral-sh/setup-uv@v5\n        with:\n          python-version: \"3.11\"\n          activate-environment: true\n      - run: uv pip install -r requirements.txt\n      - name: Run tests\n        run: for t in tests/test_*.py; do PYTHONHASHSEED=0 python \"$t\" || exit 1; done\n";

    fn files() -> Vec<(String, String)> {
        vec![(".github/workflows/tests.yml".into(), UV_WORKFLOW.into())]
    }

    #[test]
    fn uses_steps_are_read_with_their_inputs() {
        let s = uses_steps(UV_WORKFLOW);
        assert_eq!(s.len(), 2);
        assert_eq!(s[1].action, "astral-sh/setup-uv");
        assert_eq!(s[1].with.get("python-version").unwrap(), "3.11");
        assert!(matches!(translate(&s[0]), Translation::Dropped(_)));
        match translate(&s[1]) {
            Translation::Shell(sh) => {
                assert!(sh.contains("uv venv -q --python 3.11"), "{sh}");
                assert!(sh.contains(". .venv/bin/activate"), "{sh}");
            }
            other => panic!("{other:?}"),
        }
        let p = uses_prompt(&files());
        assert!(p.contains("never describe a step in words"), "{p}");
        assert!(p.contains("python3 -m pip install -q uv"), "{p}");
    }

    #[test]
    fn a_setup_uv_step_written_as_words_becomes_valid_bash() {
        let steps = workflow_steps(&files());
        let setup = "sudo apt-get update -qq && sudo apt-get install -yqq libpq-dev pkg-config; \
                     install uv with Python 3.11 and activate a venv (astral-sh/setup-uv); \
                     uv pip install -r requirements.txt";
        // As it was: not valid shell.
        assert!(bash_syntax(setup, "true").is_err());
        let mut notes = Vec::new();
        let fixed = sanitize(setup, &steps, &mut notes);
        assert!(!fixed.contains("activate a venv"), "{fixed}");
        assert!(!fixed.contains("astral-sh"), "{fixed}");
        assert!(fixed.contains("uv venv -q --python 3.11 .venv"), "{fixed}");
        assert!(fixed.starts_with("sudo apt-get update -qq"), "{fixed}");
        assert!(
            fixed.ends_with("uv pip install -r requirements.txt"),
            "{fixed}"
        );
        assert_eq!(notes.len(), 1, "{notes:?}");
        let cmd = "for t in tests/test_*.py; do PYTHONHASHSEED=0 python \"$t\" || exit 1; done";
        assert_eq!(bash_syntax(&fixed, cmd), Ok(()));
        // The loop is left as it was.
        assert_eq!(sanitize(cmd, &steps, &mut notes), cmd);
        // A step that is left out goes with its separator.
        let mut n2 = Vec::new();
        assert_eq!(
            sanitize(
                "actions/checkout && pip install -r requirements.txt",
                &steps,
                &mut n2
            ),
            "pip install -r requirements.txt"
        );
        assert!(n2[0].contains("left out"), "{n2:?}");
    }

    #[test]
    fn the_sandbox_command_is_valid_for_sh_and_runs_the_script_under_bash() {
        let setup = "python3 -m venv .venv && . .venv/bin/activate";
        let cmd = "for t in tests/test_*.py; do PYTHONHASHSEED=0 python \"$t\" || break; done";
        let wrapped = sandbox_command(setup, cmd);
        let sh = std::process::Command::new("sh")
            .args(["-n", "-c", &wrapped])
            .status()
            .unwrap();
        assert!(sh.success());
        assert!(wrapped.contains("bash -n /tmp/coderhelm_suite.sh"));
        let script = sandbox_script(setup, cmd);
        assert!(script.contains(SETUP_MARKER) && script.contains(RAN_MARKER));
        assert_eq!(bash_syntax(setup, cmd), Ok(()));
    }

    #[test]
    fn the_script_marks_setup_and_tests_apart_when_run() {
        let run = |setup: &str, cmd: &str| {
            let o = std::process::Command::new("bash")
                .args(["-c", &sandbox_script(setup, cmd)])
                .output()
                .unwrap();
            (
                String::from_utf8_lossy(&o.stdout).to_string()
                    + &String::from_utf8_lossy(&o.stderr),
                o.status.code(),
            )
        };
        let (out, code) = run("false", "echo never");
        assert_eq!(code, Some(98));
        assert!(matches!(
            read_run(&out, code),
            Run::Broken {
                setup_ok: false,
                ..
            }
        ));
        let (out, code) = run(
            "true",
            "echo 'Ran 3 tests in 0.1s'; echo; echo 'FAILED (failures=1)'; exit 1",
        );
        assert_eq!(code, Some(1));
        assert!(matches!(read_run(&out, code), Run::Failed { .. }), "{out}");
        let (out, code) = run("true", "coderhelm-no-such-tool --run");
        assert_eq!(code, Some(127));
        assert!(
            matches!(read_run(&out, code), Run::Broken { setup_ok: true, .. }),
            "{out}"
        );
    }

    #[test]
    fn the_prod_output_is_a_broken_command_not_failing_tests() {
        let out = "===CODERHELM_CHECKS_START===\nv20.20.2\n/codebuild/output/tmp/script.sh: 1: eval: Syntax error: \"(\" unexpected\n===CODERHELM_CHECKS_END exit=2===\n";
        match read_run(out, Some(2)) {
            Run::Broken { why, .. } => {
                assert!(why.contains("printed no counts"), "{why}");
                assert!(why.contains("Syntax error"), "{why}");
            }
            other => panic!("{other:?}"),
        }
        let syntax = format!("{SYNTAX_MARKER}\n");
        assert!(matches!(read_run(&syntax, Some(96)), Run::Broken { .. }));
        let quick = format!("{SETUP_MARKER}0===\nusage: foo\n{RAN_MARKER}2 secs=1===\n");
        assert!(matches!(
            read_run(&quick, Some(2)),
            Run::Broken { setup_ok: true, .. }
        ));
        let slow = format!("{SETUP_MARKER}0===\nboom\n{RAN_MARKER}2 secs=40===\n");
        assert!(matches!(read_run(&slow, Some(2)), Run::Failed { .. }));
        let masked =
            format!("Ran 4 tests in 0.2s\n\nFAILED (failures=1)\n{RAN_MARKER}0 secs=3===\n");
        assert!(matches!(read_run(&masked, Some(0)), Run::Failed { .. }));
        let ok = format!("Ran 4 tests in 0.2s\n\nOK\n{RAN_MARKER}0 secs=3===\n");
        assert!(matches!(read_run(&ok, Some(0)), Run::Passed { .. }));
    }

    #[test]
    fn the_requester_command_is_read_from_backticks() {
        let review = "Please make the change. Run the tests with \
                      `for t in tests/test_*.py; do PYTHONHASHSEED=0 python \"$t\" || break; done` \
                      before pushing; see `test_parse` for the case.";
        assert_eq!(
            requester_command(&["", review]).unwrap(),
            "for t in tests/test_*.py; do PYTHONHASHSEED=0 python \"$t\" || break; done"
        );
        let fenced = "Use this:\n```bash\n$ make test\n```\n";
        assert_eq!(requester_command(&[fenced]).unwrap(), "make test");
        assert_eq!(
            requester_command(&["rename `test_parse` in `src/app.py`"]),
            None
        );
        assert_eq!(requester_command(&["no commands here"]), None);
    }

    #[test]
    fn the_simplest_command_comes_from_the_repository_files() {
        let f = |p: &str, t: &str| vec![(p.to_string(), t.to_string())];
        assert_eq!(
            simplest_command(&f("pyproject.toml", "[tool.pytest.ini_options]\n")).unwrap(),
            "python -m pytest"
        );
        assert_eq!(
            simplest_command(&f("Makefile", "test:\n\tpytest\n")).unwrap(),
            "make test"
        );
        assert_eq!(
            simplest_command(&f(
                "package.json",
                r#"{"scripts":{"test":"echo \"Error: no test specified\" && exit 1"}}"#
            )),
            None
        );
        assert_eq!(simplest_command(&[]), None);
    }
}
