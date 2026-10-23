//! The checks a repository's CI gates a pull request on, and running them in
//! the sandbox one step at a time. A team rule said "run lint, type-check and
//! the tests before pushing", the sandbox ran only the type-check and the
//! touched tests, and the PR opened with a warning that lint never ran, which
//! CI then passed. So the sandbox runs CI's own lint command too, each check as
//! its own step so one failing never hides another's result, and the run
//! records which kinds of check ran and what CI will still run on the PR.
//!
//! What CI runs is read for any stack, not only JavaScript: a Terraform
//! repository's PR workflows ran `terraform fmt -recursive -check` and
//! `terraform plan`, the reading only knew npm-style commands, so CI looked
//! like it checked nothing and the PR told a person to run both by hand. Each
//! `run:` command of a workflow a pull request triggers is matched against
//! [`TOOLS`], a table of known check tools across stacks, and kept with its
//! exact text so the judge and the PR name the command CI runs.

use serde_yaml::Value;

/// A kind of check a repository's CI runs on a pull request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CheckKind {
    /// A type-check, or a validation of the same role (`terraform validate`).
    Typecheck,
    Build,
    Lint,
    Tests,
    /// A formatter in check mode (`terraform fmt -check`, `cargo fmt --check`).
    Format,
    /// A preview of what a deploy would change (`terraform plan`). Needs cloud
    /// credentials, so the sandbox never runs it.
    Plan,
    /// A security or policy scan (`tfsec`, `checkov`).
    Security,
}

impl CheckKind {
    /// The name people and the judge read, and the judge writes back.
    pub fn name(self) -> &'static str {
        match self {
            CheckKind::Typecheck => "typecheck",
            CheckKind::Build => "build",
            CheckKind::Lint => "lint",
            CheckKind::Tests => "tests",
            CheckKind::Format => "format",
            CheckKind::Plan => "plan",
            CheckKind::Security => "security",
        }
    }

    /// The kind a judge's `check` value names, if any.
    pub fn parse(s: &str) -> Option<CheckKind> {
        match s
            .trim()
            .to_lowercase()
            .replace(['-', ' ', '_'], "")
            .as_str()
        {
            "typecheck" | "types" | "checktypes" | "validate" | "validation" => {
                Some(CheckKind::Typecheck)
            }
            "build" | "compile" => Some(CheckKind::Build),
            "lint" | "linter" => Some(CheckKind::Lint),
            "tests" | "test" | "unittests" | "testsuite" => Some(CheckKind::Tests),
            "format" | "fmt" | "formatting" | "formatcheck" => Some(CheckKind::Format),
            "plan" | "preview" => Some(CheckKind::Plan),
            "security" | "securityscan" | "scan" | "sast" => Some(CheckKind::Security),
            _ => None,
        }
    }
}

/// A known check tool: the program and subcommands that run it, the flags
/// that put it in check mode, and what kind of check it is. Adding a tool is
/// one row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tool {
    /// How the tool is named to people and matched between commands: a
    /// finding asking for `terraform fmt -check` and CI's
    /// `terraform fmt -recursive -check` are the same tool.
    pub name: &'static str,
    /// The program, then its subcommands in order. Flags may sit between
    /// them; build tools that take several tasks (gradle, maven, terragrunt)
    /// may have other tasks between them too.
    pub words: &'static [&'static str],
    /// The command must carry one of these flags (a formatter's check mode:
    /// without it, `terraform fmt` rewrites files and checks nothing). Empty
    /// when any invocation is the check.
    pub any_flag: &'static [&'static str],
    pub kind: CheckKind,
    /// The sandbox has the tool and it needs no credentials or services, so
    /// the sandbox runs CI's own command for it (format and lint only).
    pub sandbox: bool,
}

const fn tool(
    name: &'static str,
    words: &'static [&'static str],
    any_flag: &'static [&'static str],
    kind: CheckKind,
    sandbox: bool,
) -> Tool {
    Tool {
        name,
        words,
        any_flag,
        kind,
        sandbox,
    }
}

use CheckKind::{Build, Format, Lint, Plan, Security, Tests, Typecheck};

/// Known check tools across common stacks. First match wins, so a row with
/// required flags comes before a broader row for the same program.
#[rustfmt::skip]
pub const TOOLS: &[Tool] = &[
    // Terraform and OpenTofu
    tool("terraform fmt", &["terraform", "fmt"], &["-check", "--check"], Format, true),
    tool("terraform validate", &["terraform", "validate"], &[], Typecheck, false),
    tool("terraform plan", &["terraform", "plan"], &[], Plan, false),
    tool("tofu fmt", &["tofu", "fmt"], &["-check", "--check"], Format, false),
    tool("tofu validate", &["tofu", "validate"], &[], Typecheck, false),
    tool("tofu plan", &["tofu", "plan"], &[], Plan, false),
    tool("terragrunt hclfmt", &["terragrunt", "hclfmt"], &["--check", "--terragrunt-check"], Format, false),
    tool("terragrunt validate", &["terragrunt", "validate"], &[], Typecheck, false),
    tool("terragrunt plan", &["terragrunt", "plan"], &[], Plan, false),
    tool("tflint", &["tflint"], &[], Lint, false),
    tool("tfsec", &["tfsec"], &[], Security, false),
    tool("checkov", &["checkov"], &[], Security, false),
    tool("trivy config", &["trivy", "config"], &[], Security, false),
    tool("terrascan", &["terrascan", "scan"], &[], Security, false),
    // Go
    tool("gofmt", &["gofmt"], &["-l", "-d"], Format, true),
    tool("go vet", &["go", "vet"], &[], Lint, true),
    tool("golangci-lint", &["golangci-lint"], &[], Lint, true),
    tool("staticcheck", &["staticcheck"], &[], Lint, false),
    tool("go test", &["go", "test"], &[], Tests, false),
    tool("go build", &["go", "build"], &[], Build, false),
    // Python
    tool("ruff format", &["ruff", "format"], &["--check", "--diff"], Format, true),
    tool("ruff", &["ruff", "check"], &[], Lint, true),
    tool("black", &["black"], &["--check", "--diff"], Format, true),
    tool("isort", &["isort"], &["--check", "--check-only", "-c", "--diff"], Format, true),
    tool("flake8", &["flake8"], &[], Lint, true),
    tool("pylint", &["pylint"], &[], Lint, false),
    tool("mypy", &["mypy"], &[], Typecheck, false),
    tool("pyright", &["pyright"], &[], Typecheck, false),
    tool("pytest", &["pytest"], &[], Tests, false),
    tool("unittest", &["unittest"], &[], Tests, false),
    tool("tox", &["tox"], &[], Tests, false),
    tool("nox", &["nox"], &[], Tests, false),
    // Rust
    tool("cargo fmt", &["cargo", "fmt"], &["--check"], Format, true),
    tool("cargo clippy", &["cargo", "clippy"], &[], Lint, true),
    tool("cargo test", &["cargo", "test"], &[], Tests, false),
    tool("cargo nextest", &["cargo", "nextest"], &[], Tests, false),
    tool("cargo check", &["cargo", "check"], &[], Typecheck, false),
    tool("cargo build", &["cargo", "build"], &[], Build, false),
    // JavaScript and TypeScript (package scripts are read by the heuristics
    // in `classify_command`)
    tool("prettier", &["prettier"], &["--check", "-c", "--list-different", "-l"], Format, true),
    tool("eslint", &["eslint"], &[], Lint, true),
    tool("biome", &["biome", "check"], &[], Lint, true),
    tool("biome", &["biome", "ci"], &[], Lint, true),
    tool("biome", &["biome", "lint"], &[], Lint, true),
    tool("tsc", &["tsc"], &["--noemit"], Typecheck, false),
    tool("vitest", &["vitest"], &[], Tests, false),
    tool("jest", &["jest"], &[], Tests, false),
    tool("mocha", &["mocha"], &[], Tests, false),
    tool("playwright", &["playwright", "test"], &[], Tests, false),
    tool("cypress", &["cypress", "run"], &[], Tests, false),
    // Java and Kotlin
    tool("gradle spotlessCheck", &["gradle", "spotlesscheck"], &[], Format, false),
    tool("gradle spotlessCheck", &["gradlew", "spotlesscheck"], &[], Format, false),
    tool("gradle ktlintCheck", &["gradle", "ktlintcheck"], &[], Lint, false),
    tool("gradle ktlintCheck", &["gradlew", "ktlintcheck"], &[], Lint, false),
    tool("gradle check", &["gradle", "check"], &[], Tests, false),
    tool("gradle check", &["gradlew", "check"], &[], Tests, false),
    tool("gradle test", &["gradle", "test"], &[], Tests, false),
    tool("gradle test", &["gradlew", "test"], &[], Tests, false),
    tool("gradle build", &["gradle", "build"], &[], Build, false),
    tool("gradle build", &["gradlew", "build"], &[], Build, false),
    tool("maven spotless", &["mvn", "spotless:check"], &[], Format, false),
    tool("maven spotless", &["mvnw", "spotless:check"], &[], Format, false),
    tool("maven verify", &["mvn", "verify"], &[], Tests, false),
    tool("maven verify", &["mvnw", "verify"], &[], Tests, false),
    tool("maven test", &["mvn", "test"], &[], Tests, false),
    tool("maven test", &["mvnw", "test"], &[], Tests, false),
    tool("maven package", &["mvn", "package"], &[], Build, false),
    tool("maven package", &["mvnw", "package"], &[], Build, false),
    tool("ktlint", &["ktlint"], &[], Lint, false),
    // Ruby
    tool("rubocop", &["rubocop"], &[], Lint, false),
    tool("standardrb", &["standardrb"], &[], Lint, false),
    tool("rspec", &["rspec"], &[], Tests, false),
    tool("rake test", &["rake", "test"], &[], Tests, false),
    tool("rake spec", &["rake", "spec"], &[], Tests, false),
    tool("rails test", &["rails", "test"], &[], Tests, false),
    // Infrastructure and general
    tool("pulumi preview", &["pulumi", "preview"], &[], Plan, false),
    tool("cdk diff", &["cdk", "diff"], &[], Plan, false),
    tool("cdk synth", &["cdk", "synth"], &[], Build, false),
    tool("helm lint", &["helm", "lint"], &[], Lint, false),
    tool("shellcheck", &["shellcheck"], &[], Lint, false),
    tool("hadolint", &["hadolint"], &[], Lint, false),
    tool("actionlint", &["actionlint"], &[], Lint, false),
    tool("pre-commit", &["pre-commit", "run"], &[], Lint, false),
    tool("make lint", &["make", "lint"], &[], Lint, false),
    tool("make test", &["make", "test"], &[], Tests, false),
];

/// Programs whose later words are tasks that may come with other tasks in
/// between (`./gradlew clean check`, `mvn -P ci clean verify`).
const TASK_RUNNERS: &[&str] = &[
    "gradle",
    "gradlew",
    "mvn",
    "mvnw",
    "terragrunt",
    "make",
    "rake",
];

/// Leading words that only run the next command another way: they are
/// dropped before the program is read. Each entry is the words, then whether
/// flags right after them are dropped too.
const WRAPPERS: &[&[&str]] = &[
    &["npx"],
    &["bunx"],
    &["pnpx"],
    &["uvx"],
    &["pnpm", "exec"],
    &["pnpm", "dlx"],
    &["yarn", "exec"],
    &["yarn", "dlx"],
    &["npm", "exec"],
    &["poetry", "run"],
    &["uv", "run"],
    &["pipenv", "run"],
    &["pdm", "run"],
    &["hatch", "run"],
    &["rye", "run"],
    &["pipx", "run"],
    &["bundle", "exec"],
    &["python", "-m"],
    &["python3", "-m"],
    &["py", "-m"],
    &["sudo"],
    &["time"],
    &["exec"],
    &["then"],
    &["do"],
    &["else"],
    &["!"],
];

/// A command's words, ready to match against [`TOOLS`]: lowercased, quotes
/// and command-substitution marks removed, leading env assignments,
/// `test -z` and wrappers (`npx`, `poetry run`, `python -m`) dropped, and the
/// program read by its file name (`./gradlew` is `gradlew`).
fn words_of(cmd: &str) -> Vec<String> {
    let flat: String = cmd
        .to_lowercase()
        .replace("$(", " ")
        .chars()
        .map(|c| match c {
            '"' | '\'' | '`' | '(' | ')' => ' ',
            c => c,
        })
        .collect();
    let mut words: Vec<String> = flat.split_whitespace().map(str::to_string).collect();
    loop {
        let before = words.len();
        // `FOO=bar cmd`
        while words
            .first()
            .is_some_and(|w| w.contains('=') && !w.starts_with('-'))
        {
            words.remove(0);
        }
        // `test -z "$(gofmt -l .)"`, `[ -z ... ]`
        if matches!(words.first().map(String::as_str), Some("test" | "["))
            && matches!(words.get(1).map(String::as_str), Some("-z" | "-n"))
        {
            words.drain(..2);
        }
        for w in WRAPPERS {
            if words.len() > w.len() && words.iter().zip(w.iter()).all(|(a, b)| a == b) {
                words.drain(..w.len());
                while words.first().is_some_and(|w| w.starts_with('-')) {
                    words.remove(0);
                }
                break;
            }
        }
        if words.len() == before {
            break;
        }
    }
    if let Some(first) = words.first_mut() {
        if let Some(base) = first.rsplit('/').next() {
            *first = base.to_string();
        }
    }
    words
}

/// Whether `words` (from [`words_of`]) run `t`.
fn runs_tool(words: &[String], t: &Tool) -> bool {
    let Some((program, subs)) = t.words.split_first() else {
        return false;
    };
    if words.first().map(String::as_str) != Some(*program) {
        return false;
    }
    let gaps = TASK_RUNNERS.contains(program);
    let mut at = 1;
    for sub in subs {
        loop {
            let Some(w) = words.get(at) else {
                return false;
            };
            at += 1;
            if w == sub {
                break;
            }
            if !(gaps || w.starts_with('-') || w.starts_with('+')) {
                return false;
            }
        }
    }
    t.any_flag.is_empty()
        || words.iter().skip(1).any(|w| {
            t.any_flag
                .iter()
                .any(|f| w == f || w.starts_with(&format!("{f}=")))
        })
}

/// What a single shell command runs, as a check: the tool's row, or a
/// package-manager script read by the JavaScript heuristics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Matched {
    pub kind: CheckKind,
    /// The tool's name (see [`Tool::name`]); `<kind> script` for a package
    /// script (`pnpm lint` and `npm run lint` are the same check).
    pub tool: String,
    /// The sandbox can run CI's own command for it.
    pub sandbox: bool,
}

/// The check one shell command runs, if it is one. The tool table first;
/// then package-manager scripts (`pnpm lint`, `npm run check-types`), whose
/// names only the repository knows.
pub fn classify_command(cmd: &str) -> Option<Matched> {
    let cmd = cmd.trim();
    if cmd.is_empty() || cmd.starts_with('#') {
        return None;
    }
    let words = words_of(cmd);
    if let Some(t) = TOOLS.iter().find(|t| runs_tool(&words, t)) {
        return Some(Matched {
            kind: t.kind,
            tool: t.name.to_string(),
            sandbox: t.sandbox,
        });
    }
    let script = |kind: CheckKind, sandbox: bool| {
        Some(Matched {
            kind,
            tool: format!("{} script", kind.name()),
            sandbox,
        })
    };
    if is_format_script(cmd) {
        script(Format, true)
    } else if super::is_typecheck_cmd(cmd) {
        script(Typecheck, false)
    } else if is_lint_cmd(cmd) {
        script(Lint, true)
    } else if is_any_test_cmd(cmd) {
        script(Tests, false)
    } else if is_build_cmd(cmd) {
        script(Build, false)
    } else {
        None
    }
}

/// A package-manager script that checks formatting (`pnpm format:check`).
fn is_format_script(line: &str) -> bool {
    let low = line.to_lowercase();
    let tokens: Vec<&str> = low.split_whitespace().collect();
    ["pnpm", "npm", "yarn", "bun", "turbo"].contains(&tokens.first().copied().unwrap_or(""))
        && tokens.iter().any(|t| {
            matches!(
                *t,
                "format:check" | "fmt:check" | "prettier:check" | "check-format" | "format-check"
            )
        })
}

/// One step of a sandbox check run: a command and the kinds of check it is.
#[derive(Debug, Clone, PartialEq)]
pub struct Step {
    pub kinds: Vec<CheckKind>,
    pub cmd: String,
    /// What the step covers when that's narrower than the kind says (the
    /// tests step runs only the test files the change touched).
    pub scope: Option<String>,
}

impl Step {
    /// The step's name in the summary: its kinds, or `install` for a step
    /// that only installs dependencies.
    pub fn label(&self) -> String {
        if self.kinds.is_empty() {
            "install".to_string()
        } else {
            self.kinds
                .iter()
                .map(|k| k.name())
                .collect::<Vec<_>>()
                .join(" + ")
        }
    }

    /// A step whose result is reported but never holds the agent: lint and
    /// format can fail on a config quirk that has nothing to do with the
    /// change.
    pub fn advisory(&self) -> bool {
        !self.kinds.is_empty()
            && self
                .kinds
                .iter()
                .all(|k| matches!(k, CheckKind::Lint | CheckKind::Format))
    }
}

/// The kinds of check a derived check command (install + type-check/build)
/// covers, read from its own `&&`-joined parts.
pub fn kinds_of(cmd: &str) -> Vec<CheckKind> {
    let mut kinds = Vec::new();
    for part in cmd.split("&&").map(str::trim) {
        let low = part.to_lowercase();
        let kind = if super::is_typecheck_cmd(part) || low.starts_with("npx --no-install tsc") {
            Some(CheckKind::Typecheck)
        } else if low.contains(" build")
            || low.starts_with("build")
            || low.contains("compileall")
            || low.contains("--no-run")
            || low.contains("go vet")
        {
            Some(CheckKind::Build)
        } else if low.contains("terraform validate") {
            Some(CheckKind::Typecheck)
        } else {
            None
        };
        if let Some(k) = kind {
            if !kinds.contains(&k) {
                kinds.push(k);
            }
        }
    }
    kinds
}

/// The output marker each step's exit code is reported under.
const STEP_MARK: &str = "CODERHELM_STEP";

/// One shell script that runs every step on its own, so a failing step never
/// stops or hides the next: each step's output goes to its own log, a failed
/// step's log tail is printed, then one `CODERHELM_STEP <n> <exit>` line per
/// step, last, where the sandbox's output tail always keeps it. Exits non-zero
/// when any step failed.
pub fn script(steps: &[Step]) -> String {
    // The sandbox keeps the last ~6000 characters of output.
    let per_step = 5000 / steps.len().max(1);
    let mut out = String::new();
    for (i, s) in steps.iter().enumerate() {
        let n = i + 1;
        out.push_str(&format!(
            "( {} ) > /tmp/coderhelm-step-{n}.log 2>&1; ch_s{n}=$?\n",
            s.cmd
        ));
    }
    for (i, s) in steps.iter().enumerate() {
        let n = i + 1;
        out.push_str(&format!(
            "if [ $ch_s{n} -ne 0 ]; then echo \"--- {label} failed (exit $ch_s{n}), last output ---\"; \
             tail -c {per_step} /tmp/coderhelm-step-{n}.log; echo; fi\n",
            label = s.label(),
        ));
    }
    out.push_str("echo \"=== CoderHelm check summary ===\"\n");
    for n in 1..=steps.len() {
        out.push_str(&format!("echo \"{STEP_MARK} {n} $ch_s{n}\"\n"));
    }
    let all: Vec<String> = (1..=steps.len())
        .map(|n| format!("[ $ch_s{n} -eq 0 ]"))
        .collect();
    out.push_str(&if all.is_empty() {
        "true".to_string()
    } else {
        all.join(" && ")
    });
    out
}

/// Each step's exit code from a script run's output, in step order. `None`
/// for a step whose line isn't there (the run timed out or was cut short).
pub fn step_results(output: &str, steps: usize) -> Vec<Option<i32>> {
    let mut codes = vec![None; steps];
    for line in output.lines() {
        let mut parts = line.split_whitespace();
        if parts.next() != Some(STEP_MARK) {
            continue;
        }
        let (Some(n), Some(code)) = (
            parts.next().and_then(|n| n.parse::<usize>().ok()),
            parts.next().and_then(|c| c.parse::<i32>().ok()),
        ) else {
            continue;
        };
        if (1..=steps).contains(&n) {
            codes[n - 1] = Some(code);
        }
    }
    codes
}

/// What one step came to, as a line for the agent, the judge and the PR.
pub fn result_line(step: &Step, code: Option<i32>) -> String {
    let verdict = match code {
        Some(0) => "PASS".to_string(),
        Some(c) => format!("FAIL (exit {c})"),
        None => "no result (the run stopped before this step reported)".to_string(),
    };
    let scope = step
        .scope
        .as_deref()
        .map(|s| format!(" ({s})"))
        .unwrap_or_default();
    format!(
        "- {}: {verdict}, `{}`{scope}",
        step.label(),
        common::truncate_str(&step.cmd, 300)
    )
}

/// A single shell line that runs a linter through a package manager or a
/// known linter binary. Not `lint-staged` or `commitlint`, which lint git
/// state the sandbox doesn't have.
pub fn is_lint_cmd(line: &str) -> bool {
    let low = line.to_lowercase();
    if low.starts_with('#') || low.contains("lint-staged") || low.contains("commitlint") {
        return false;
    }
    let tokens: Vec<&str> = low
        .split_whitespace()
        .map(|t| t.trim_matches(|c| c == '"' || c == '\''))
        .collect();
    let looks_lint = tokens.iter().any(|t| {
        *t == "lint"
            || t.starts_with("lint:")
            || t.ends_with(":lint")
            || *t == "eslint"
            || *t == "clippy"
            || *t == "golangci-lint"
            || *t == "flake8"
            || *t == "ruff"
    });
    let is_cmd = ["pnpm", "npm", "yarn", "npx", "turbo", "bun", "cargo"]
        .iter()
        .any(|pm| tokens.first() == Some(pm))
        || ["eslint", "golangci-lint", "flake8", "ruff"]
            .iter()
            .any(|bin| tokens.first() == Some(bin));
    looks_lint && is_cmd
}

/// A single shell line that runs a build, by a package manager or toolchain.
fn is_build_cmd(line: &str) -> bool {
    let low = line.to_lowercase();
    let tokens: Vec<&str> = low.split_whitespace().collect();
    let first = tokens.first().copied().unwrap_or("");
    (["pnpm", "npm", "yarn", "turbo", "bun"].contains(&first)
        && tokens
            .iter()
            .any(|t| *t == "build" || t.ends_with(":build")))
        || low.starts_with("cargo build")
        || low.starts_with("go build")
}

/// A single shell line that runs a test suite: a unit suite `is_test_cmd`
/// accepts, or a toolchain's own test command.
fn is_any_test_cmd(line: &str) -> bool {
    let low = line.to_lowercase();
    super::is_test_cmd(line)
        || low.starts_with("cargo test")
        || low.starts_with("go test")
        || low.starts_with("pytest")
        || low.starts_with("python -m pytest")
}

/// One workflow file: its path in the repository and its text.
#[derive(Debug, Clone, PartialEq)]
pub struct Workflow {
    pub path: String,
    pub text: String,
}

/// One check command CI runs on a pull request, as the workflow writes it.
#[derive(Debug, Clone, PartialEq)]
pub struct CiCommand {
    /// Its id in this run (`C1`): the judge names it, code checks it exists.
    pub id: String,
    pub kind: CheckKind,
    /// The tool's name (see [`Matched::tool`]).
    pub tool: String,
    /// The command, exactly as the workflow runs it.
    pub cmd: String,
    /// The workflow file it is in.
    pub workflow: String,
    /// The job it runs in.
    pub job: String,
    /// The directory it runs in, when the workflow sets one.
    pub dir: Option<String>,
    /// The sandbox can run it as it is (see [`Tool::sandbox`]).
    pub sandbox: bool,
}

impl CiCommand {
    /// The command as people read it: `cmd` (in `dir`).
    pub fn display(&self) -> String {
        match &self.dir {
            Some(d) => format!("`{}` (in `{d}`)", self.cmd),
            None => format!("`{}`", self.cmd),
        }
    }

    /// The line the judge reads: id, kind, tool, command, where it runs.
    pub fn judge_line(&self) -> String {
        let job = if self.job.is_empty() {
            String::new()
        } else {
            format!(", job `{}`", self.job)
        };
        format!(
            "- [{}] {} ({}): {} (workflow `{}`{job})",
            self.id,
            self.kind.name(),
            self.tool,
            self.display(),
            self.workflow,
        )
    }
}

/// The most check commands kept: a judge prompt stays short.
const MAX_CI_COMMANDS: usize = 40;

/// The single commands of a `run:` script: continuation lines joined,
/// comments and blank lines dropped, and each line split at `&&`, `||`, `;`
/// and `|`.
pub fn script_commands(script: &str) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    let mut pending = String::new();
    for raw in script.lines() {
        let line = raw.trim();
        if let Some(head) = line.strip_suffix('\\') {
            pending.push_str(head.trim());
            pending.push(' ');
            continue;
        }
        pending.push_str(line);
        lines.push(std::mem::take(&mut pending));
    }
    if !pending.trim().is_empty() {
        lines.push(pending);
    }
    let mut out = Vec::new();
    for line in lines {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = match line.find(" #") {
            Some(i) => &line[..i],
            None => line,
        };
        for part in line
            .replace("&&", "\n")
            .replace("||", "\n")
            .replace([';', '|'], "\n")
            .lines()
        {
            let part = part.trim();
            if !part.is_empty() {
                out.push(part.to_string());
            }
        }
    }
    out
}

/// A key of a YAML mapping, by name. `on` is read as `true` by YAML 1.1
/// readers, so `on` is looked up both ways.
fn get<'a>(v: &'a Value, key: &str) -> Option<&'a Value> {
    let map = v.as_mapping()?;
    map.get(Value::String(key.to_string()))
        .or_else(|| (key == "on").then(|| map.get(Value::Bool(true))).flatten())
}

fn strings(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Sequence(items)) => items
            .iter()
            .filter_map(|i| i.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    }
}

/// Whether a GitHub branch filter pattern matches `branch`: `*` stops at
/// `/`, `**` doesn't, `?` is one character.
fn glob_matches(pattern: &str, branch: &str) -> bool {
    fn go(p: &[u8], s: &[u8]) -> bool {
        match p.first() {
            None => s.is_empty(),
            Some(b'*') if p.get(1) == Some(&b'*') => (0..=s.len()).any(|i| go(&p[2..], &s[i..])),
            Some(b'*') => (0..=s.len())
                .take_while(|&i| i == 0 || s[i - 1] != b'/')
                .any(|i| go(&p[1..], &s[i..])),
            Some(b'?') => !s.is_empty() && go(&p[1..], &s[1..]),
            Some(c) => s.first() == Some(c) && go(&p[1..], &s[1..]),
        }
    }
    go(pattern.as_bytes(), branch.as_bytes())
}

/// Whether a branch filter (`branches`, with `!` negations, or
/// `branches-ignore`) lets a pull request into `base` through.
fn branch_allowed(trigger: &Value, base: &str) -> bool {
    let only = strings(get(trigger, "branches"));
    if !only.is_empty() {
        let mut allowed = false;
        for p in &only {
            match p.strip_prefix('!') {
                Some(neg) if glob_matches(neg, base) => allowed = false,
                None if glob_matches(p, base) => allowed = true,
                _ => {}
            }
        }
        return allowed;
    }
    !strings(get(trigger, "branches-ignore"))
        .iter()
        .any(|p| glob_matches(p, base))
}

/// Whether a workflow's `on:` runs it when a pull request into `base` opens
/// or gets a commit: a `pull_request` (or `pull_request_target`) trigger
/// whose `types` (default opened, synchronize, reopened) include one of
/// those, and whose branch filter lets `base` through. An empty `base` skips
/// the branch filter.
fn runs_on_pull_request(on: &Value, base: &str) -> bool {
    const EVENTS: [&str; 2] = ["pull_request", "pull_request_target"];
    match on {
        Value::String(s) => EVENTS.contains(&s.as_str()),
        Value::Sequence(_) => strings(Some(on))
            .iter()
            .any(|s| EVENTS.contains(&s.as_str())),
        Value::Mapping(_) => EVENTS.iter().any(|e| match get(on, e) {
            None => false,
            Some(t @ Value::Mapping(_)) => {
                let types = strings(get(t, "types"));
                let on_change = types.is_empty()
                    || types
                        .iter()
                        .any(|ty| matches!(ty.as_str(), "opened" | "synchronize" | "reopened"));
                on_change && (base.is_empty() || branch_allowed(t, base))
            }
            // `pull_request:` with nothing under it.
            Some(_) => true,
        }),
        _ => false,
    }
}

/// The check commands one workflow runs on a pull request into `base`:
/// every job's `run:` steps (multi-line `run: |` scripts split into single
/// commands), each matched against the tool table. Empty when a pull request
/// doesn't trigger it. A workflow YAML can't read is scanned line by line,
/// as long as it names `pull_request`.
fn workflow_commands(wf: &Workflow, base: &str) -> Vec<CiCommand> {
    let file = wf.path.rsplit('/').next().unwrap_or(&wf.path).to_string();
    let mut found: Vec<(String, Option<String>, String)> = Vec::new();
    match serde_yaml::from_str::<Value>(&wf.text) {
        Ok(doc) => {
            let Some(on) = get(&doc, "on") else {
                return Vec::new();
            };
            if !runs_on_pull_request(on, base) {
                return Vec::new();
            }
            let Some(jobs) = get(&doc, "jobs").and_then(Value::as_mapping) else {
                return Vec::new();
            };
            for (name, job) in jobs {
                let job_name = name.as_str().unwrap_or_default().to_string();
                let job_dir = get(job, "defaults")
                    .and_then(|d| get(d, "run"))
                    .and_then(|r| get(r, "working-directory"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let Some(steps) = get(job, "steps").and_then(Value::as_sequence) else {
                    continue;
                };
                for step in steps {
                    let Some(run) = get(step, "run").and_then(Value::as_str) else {
                        continue;
                    };
                    let dir = get(step, "working-directory")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        .or_else(|| job_dir.clone());
                    for cmd in script_commands(run) {
                        found.push((job_name.clone(), dir.clone(), cmd));
                    }
                }
            }
        }
        Err(_) if wf.text.contains("pull_request") => {
            for raw in wf.text.lines() {
                let line = raw.trim().trim_start_matches("- ").trim();
                let Some(run) = line.strip_prefix("run:") else {
                    // A line of a `run: |` block: read as a command itself.
                    found.push((String::new(), None, line.to_string()));
                    continue;
                };
                let run = run.trim().trim_matches(|c| c == '"' || c == '\'');
                for cmd in script_commands(run) {
                    found.push((String::new(), None, cmd));
                }
            }
        }
        Err(_) => {}
    }
    found
        .into_iter()
        .filter_map(|(job, dir, cmd)| {
            let m = classify_command(&cmd)?;
            Some(CiCommand {
                id: String::new(),
                kind: m.kind,
                tool: m.tool,
                cmd,
                workflow: file.clone(),
                job,
                dir: dir.filter(|d| !d.trim().is_empty() && d.trim() != "."),
                sandbox: m.sandbox,
            })
        })
        .collect()
}

/// Every check command the repository's CI runs on a pull request into
/// `base`, from its workflow files, each with an id (`C1`, `C2`, ...). Only
/// workflows a pull request triggers count: their result shows on the PR, so
/// they settle what didn't run before it. A command a workflow repeats (in
/// the same directory) is kept once.
pub fn pr_commands(workflows: &[Workflow], base: &str) -> Vec<CiCommand> {
    let mut out: Vec<CiCommand> = Vec::new();
    for wf in workflows {
        for c in workflow_commands(wf, base) {
            if out.len() >= MAX_CI_COMMANDS {
                break;
            }
            if !out.iter().any(|o| o.cmd == c.cmd && o.dir == c.dir) {
                out.push(c);
            }
        }
    }
    for (i, c) in out.iter_mut().enumerate() {
        c.id = format!("C{}", i + 1);
    }
    out
}

/// The kinds of check `commands` cover, sorted.
pub fn kinds_from(commands: &[CiCommand]) -> Vec<CheckKind> {
    let mut kinds: Vec<CheckKind> = Vec::new();
    for c in commands {
        if !kinds.contains(&c.kind) {
            kinds.push(c.kind);
        }
    }
    kinds.sort();
    kinds
}

/// The kinds of check the repository's CI runs on a pull request, from its
/// workflow files.
#[cfg(test)]
pub fn gates_from_workflows(workflows: &[Workflow], base: &str) -> Vec<CheckKind> {
    kinds_from(&pr_commands(workflows, base))
}

/// The sandbox steps that run CI's own format and lint commands, as CI
/// writes them: one step per kind (format, then lint), its commands joined
/// with `&&`, each in the directory CI runs it in. Only commands of tools the
/// sandbox has and that need no credentials ([`Tool::sandbox`]).
pub fn mirrored_steps(commands: &[CiCommand]) -> Vec<Step> {
    let mut steps = Vec::new();
    for kind in [CheckKind::Format, CheckKind::Lint] {
        let cmds: Vec<String> = commands
            .iter()
            .filter(|c| c.kind == kind && c.sandbox)
            .map(|c| match &c.dir {
                Some(d) if is_plain_path(d) => format!("(cd '{d}' && {})", c.cmd),
                _ => c.cmd.clone(),
            })
            .collect();
        if !cmds.is_empty() {
            steps.push(Step {
                kinds: vec![kind],
                cmd: cmds.join(" && "),
                scope: Some(format!("the repo's CI {} command", kind.name())),
            });
        }
    }
    steps
}

/// A relative path made only of safe characters, fit to quote in a shell
/// command.
pub fn is_plain_path(p: &str) -> bool {
    !p.is_empty()
        && !p.starts_with('/')
        && !p.split('/').any(|seg| seg == "..")
        && p.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/'))
}

/// The texts of the repository's workflow files at `git_ref`.
pub async fn read_workflows(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Vec<Workflow> {
    let Ok(entries) = github
        .list_directory(owner, repo, ".github/workflows", git_ref)
        .await
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries {
        if !(entry.name.ends_with(".yml") || entry.name.ends_with(".yaml")) {
            continue;
        }
        if let Ok(text) = github.read_file(owner, repo, &entry.path, git_ref).await {
            out.push(Workflow {
                path: entry.path,
                text,
            });
        }
    }
    out
}

/// Every check command the repository's CI runs on a pull request into
/// `base` (read from the workflows at `base`).
pub async fn detect_ci(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    base: &str,
) -> Vec<CiCommand> {
    pr_commands(&read_workflows(github, owner, repo, base).await, base)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step(kinds: &[CheckKind], cmd: &str) -> Step {
        Step {
            kinds: kinds.to_vec(),
            cmd: cmd.into(),
            scope: None,
        }
    }

    fn wf(path: &str, text: &str) -> Workflow {
        Workflow {
            path: path.into(),
            text: text.into(),
        }
    }

    fn kind_of(cmd: &str) -> Option<CheckKind> {
        classify_command(cmd).map(|m| m.kind)
    }

    fn tool_of(cmd: &str) -> Option<String> {
        classify_command(cmd).map(|m| m.tool)
    }

    #[test]
    fn finds_ci_lint_commands_but_not_git_state_linters() {
        assert!(is_lint_cmd("pnpm lint"));
        assert!(is_lint_cmd("pnpm run lint"));
        assert!(is_lint_cmd("pnpm --filter web lint:ci"));
        assert!(is_lint_cmd("npx eslint ."));
        assert!(is_lint_cmd("cargo clippy --workspace -- -D warnings"));
        assert!(is_lint_cmd("ruff check ."));
        assert!(!is_lint_cmd("npx lint-staged"));
        assert!(!is_lint_cmd("npx commitlint --from HEAD~1"));
        assert!(!is_lint_cmd("# pnpm lint"));
        assert!(!is_lint_cmd("name: Lint"));
        assert!(!is_lint_cmd("pnpm test"));
    }

    #[test]
    fn the_tool_table_reads_check_commands_across_stacks() {
        let cases: &[(&str, CheckKind, &str)] = &[
            // Terraform
            ("terraform fmt -recursive -check", Format, "terraform fmt"),
            (
                "terraform -chdir=envs/prod fmt -check -diff",
                Format,
                "terraform fmt",
            ),
            (
                "terraform validate -no-color",
                Typecheck,
                "terraform validate",
            ),
            (
                "terraform plan -input=false -out=tfplan",
                Plan,
                "terraform plan",
            ),
            ("tflint --recursive", Lint, "tflint"),
            ("tfsec .", Security, "tfsec"),
            ("checkov -d .", Security, "checkov"),
            ("terragrunt run-all plan", Plan, "terragrunt plan"),
            // Go
            ("test -z \"$(gofmt -l .)\"", Format, "gofmt"),
            ("go vet ./...", Lint, "go vet"),
            ("golangci-lint run --timeout 5m", Lint, "golangci-lint"),
            ("go test -race ./...", Tests, "go test"),
            // Python
            ("ruff check .", Lint, "ruff"),
            ("ruff format --check .", Format, "ruff format"),
            ("poetry run black --check .", Format, "black"),
            ("flake8 src", Lint, "flake8"),
            ("uv run mypy src", Typecheck, "mypy"),
            ("python -m pytest -q", Tests, "pytest"),
            // Rust
            ("cargo fmt --all -- --check", Format, "cargo fmt"),
            ("cargo +nightly fmt --check", Format, "cargo fmt"),
            (
                "cargo clippy --workspace -- -D warnings",
                Lint,
                "cargo clippy",
            ),
            ("cargo test --workspace", Tests, "cargo test"),
            // JavaScript
            ("npx prettier --check .", Format, "prettier"),
            ("npx eslint .", Lint, "eslint"),
            ("npx tsc --noEmit", Typecheck, "tsc"),
            ("pnpm lint", Lint, "lint script"),
            (
                "pnpm --filter web check-types",
                Typecheck,
                "typecheck script",
            ),
            ("pnpm format:check", Format, "format script"),
            ("pnpm test:run", Tests, "tests script"),
            // Java and Kotlin
            ("./gradlew clean check", Tests, "gradle check"),
            ("gradle test", Tests, "gradle test"),
            ("mvn -B -P ci clean verify", Tests, "maven verify"),
            // Ruby
            ("bundle exec rubocop", Lint, "rubocop"),
            ("bundle exec rspec", Tests, "rspec"),
        ];
        for (cmd, kind, tool) in cases {
            assert_eq!(kind_of(cmd), Some(*kind), "{cmd}");
            assert_eq!(tool_of(cmd).as_deref(), Some(*tool), "{cmd}");
        }
        // Writers and setup are not checks.
        for cmd in [
            "terraform fmt -recursive",
            "terraform init -input=false",
            "terraform apply -auto-approve tfplan",
            "cargo fmt --all",
            "black .",
            "echo done",
            "npm ci",
            "# terraform plan",
        ] {
            assert_eq!(kind_of(cmd), None, "{cmd}");
        }
        // A finding's command and CI's command are the same tool.
        assert_eq!(
            tool_of("terraform fmt -check"),
            tool_of("terraform fmt -recursive -check")
        );
        assert_eq!(tool_of("pnpm lint"), tool_of("npm run lint"));
    }

    #[test]
    fn the_sandbox_never_mirrors_a_command_that_needs_credentials() {
        for t in TOOLS {
            if t.sandbox {
                assert!(
                    matches!(t.kind, Format | Lint),
                    "{} runs in the sandbox but is {}",
                    t.name,
                    t.kind.name()
                );
            }
            if t.kind == Plan {
                assert!(!t.sandbox, "{}", t.name);
            }
        }
    }

    #[test]
    fn a_run_script_splits_into_single_commands() {
        let script = "terraform init -input=false \\\n  -backend-config=env.hcl\n# a comment\n\nterraform fmt -recursive -check && terraform validate\necho ok | tee out; go vet ./... # trailing\n";
        assert_eq!(
            script_commands(script),
            vec![
                "terraform init -input=false -backend-config=env.hcl",
                "terraform fmt -recursive -check",
                "terraform validate",
                "echo ok",
                "tee out",
                "go vet ./...",
            ]
        );
    }

    #[test]
    fn reads_pull_request_workflows_with_run_blocks_types_and_branches() {
        let lint = wf(
            ".github/workflows/lint.yml",
            r#"name: Lint
on:
  pull_request:
    types: [opened, synchronize, reopened, labeled]
jobs:
  fmt:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: 1.9.8
      - name: Format
        run: |
          terraform fmt -recursive -check
"#,
        );
        let plan = wf(
            ".github/workflows/plan.yml",
            r#"on:
  pull_request:
    branches: [main]
jobs:
  plan:
    strategy:
      matrix:
        env: [staging, prod]
    defaults:
      run:
        working-directory: envs/${{ matrix.env }}
    steps:
      - run: terraform init -input=false
      - run: |
          terraform plan -input=false \
            -out=tfplan
"#,
        );
        let deploy = wf(
            ".github/workflows/deploy.yml",
            "on:\n  push:\n    branches: [main]\njobs:\n  apply:\n    steps:\n      - run: terraform plan\n",
        );
        let closed_only = wf(
            ".github/workflows/cleanup.yml",
            "on:\n  pull_request:\n    types: [closed]\njobs:\n  c:\n    steps:\n      - run: cargo test\n",
        );
        let all = [lint.clone(), plan.clone(), deploy, closed_only];
        let cmds = pr_commands(&all, "main");
        let got: Vec<(&str, CheckKind, &str, Option<&str>)> = cmds
            .iter()
            .map(|c| (c.id.as_str(), c.kind, c.cmd.as_str(), c.dir.as_deref()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("C1", Format, "terraform fmt -recursive -check", None),
                (
                    "C2",
                    Plan,
                    "terraform plan -input=false -out=tfplan",
                    Some("envs/${{ matrix.env }}")
                ),
            ]
        );
        assert_eq!(cmds[0].workflow, "lint.yml");
        assert_eq!(cmds[0].job, "fmt");
        assert_eq!(gates_from_workflows(&all, "main"), vec![Format, Plan]);
        // The plan workflow gates only pull requests into main.
        assert_eq!(gates_from_workflows(&[plan], "release"), vec![]);
        // The judge reads the command, not just its kind.
        assert_eq!(
            cmds[0].judge_line(),
            "- [C1] format (terraform fmt): `terraform fmt -recursive -check` (workflow `lint.yml`, job `fmt`)"
        );
    }

    #[test]
    fn ci_gates_come_only_from_workflows_a_pull_request_triggers() {
        let pr = "on:\n  pull_request:\njobs:\n  lint:\n    steps:\n      - run: pnpm lint\n      - run: pnpm check-types\n      - run: pnpm test:run\n      - run: pnpm build\n";
        let push_only = "on:\n  push:\n    branches: [main]\njobs:\n  e:\n    steps:\n      - run: cargo test\n";
        let as_list =
            "on: [push, pull_request]\njobs:\n  e:\n    steps:\n      - run: cargo fmt --check\n";
        assert_eq!(
            gates_from_workflows(&[wf("a.yml", pr), wf("b.yml", push_only)], "main"),
            vec![
                CheckKind::Typecheck,
                CheckKind::Build,
                CheckKind::Lint,
                CheckKind::Tests
            ]
        );
        assert_eq!(
            gates_from_workflows(&[wf("b.yml", push_only)], "main"),
            vec![]
        );
        assert_eq!(
            gates_from_workflows(&[wf("c.yml", as_list)], "main"),
            vec![CheckKind::Format]
        );
        let lint = mirrored_steps(&pr_commands(&[wf("a.yml", pr)], "main"));
        assert_eq!(
            lint,
            vec![Step {
                kinds: vec![CheckKind::Lint],
                cmd: "pnpm lint".into(),
                scope: Some("the repo's CI lint command".into()),
            }]
        );
        assert_eq!(mirrored_steps(&[]), vec![]);
    }

    #[test]
    fn the_sandbox_mirrors_ci_format_and_lint_but_never_plan() {
        let text = "on: pull_request\njobs:\n  tf:\n    defaults:\n      run:\n        working-directory: infra\n    steps:\n      - run: terraform fmt -recursive -check\n      - run: tflint\n      - run: terraform plan\n";
        let steps = mirrored_steps(&pr_commands(&[wf("tf.yml", text)], "main"));
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].kinds, vec![CheckKind::Format]);
        assert_eq!(
            steps[0].cmd,
            "(cd 'infra' && terraform fmt -recursive -check)"
        );
        assert!(steps[0].advisory());
    }

    #[test]
    fn branch_filters_follow_github_globs() {
        assert!(glob_matches("main", "main"));
        assert!(glob_matches("release/*", "release/1.2"));
        assert!(!glob_matches("release/*", "release/1/2"));
        assert!(glob_matches("release/**", "release/1/2"));
        assert!(!glob_matches("main", "mainline"));
    }

    #[test]
    fn a_derived_check_command_says_what_kinds_it_covers() {
        assert_eq!(
            kinds_of("corepack enable && pnpm install --frozen-lockfile && pnpm --filter web check-types"),
            vec![CheckKind::Typecheck]
        );
        assert_eq!(
            kinds_of("npm ci --no-audit --no-fund && npm run typecheck && npm run build"),
            vec![CheckKind::Typecheck, CheckKind::Build]
        );
        assert_eq!(
            kinds_of("cargo build --locked && cargo test --no-run --locked"),
            vec![CheckKind::Build]
        );
        assert_eq!(kinds_of("npm ci --no-audit --no-fund"), vec![]);
        assert_eq!(step(&[], "npm ci").label(), "install");
    }

    #[test]
    fn each_step_runs_on_its_own_and_reports_its_exit_code() {
        let steps = [
            step(&[CheckKind::Typecheck], "pnpm install && pnpm check-types"),
            step(&[CheckKind::Lint], "pnpm lint"),
        ];
        let s = script(&steps);
        // No `&&` between steps: a failing lint never stops the next step.
        assert!(s.contains(
            "( pnpm install && pnpm check-types ) > /tmp/coderhelm-step-1.log 2>&1; ch_s1=$?\n"
        ));
        assert!(s.contains("( pnpm lint ) > /tmp/coderhelm-step-2.log 2>&1; ch_s2=$?\n"));
        assert!(s.contains("--- lint failed (exit $ch_s2), last output ---"));
        assert!(s.contains("echo \"CODERHELM_STEP 2 $ch_s2\""));
        assert!(s.ends_with("[ $ch_s1 -eq 0 ] && [ $ch_s2 -eq 0 ]"));

        let out = "noise\n=== CoderHelm check summary ===\nCODERHELM_STEP 1 0\nCODERHELM_STEP 2 1\nCODERHELM_STEP 9 0\n";
        assert_eq!(step_results(out, 2), vec![Some(0), Some(1)]);
        assert_eq!(step_results("timed out", 2), vec![None, None]);
    }

    #[test]
    fn a_result_line_names_the_check_its_command_and_its_scope() {
        let mut t = step(
            &[CheckKind::Tests],
            "pnpm test -- --passWithNoTests a.test.ts",
        );
        t.scope = Some("only the test files this change touched".into());
        assert_eq!(
            result_line(&t, Some(0)),
            "- tests: PASS, `pnpm test -- --passWithNoTests a.test.ts` (only the test files this change touched)"
        );
        assert_eq!(
            result_line(&step(&[CheckKind::Lint], "pnpm lint"), Some(2)),
            "- lint: FAIL (exit 2), `pnpm lint`"
        );
        assert!(result_line(&step(&[CheckKind::Lint], "pnpm lint"), None).contains("no result"));
    }

    #[test]
    fn the_judges_check_names_read_back_as_kinds() {
        assert_eq!(CheckKind::parse("lint"), Some(CheckKind::Lint));
        assert_eq!(CheckKind::parse("Type-check"), Some(CheckKind::Typecheck));
        assert_eq!(CheckKind::parse("validate"), Some(CheckKind::Typecheck));
        assert_eq!(CheckKind::parse("test"), Some(CheckKind::Tests));
        assert_eq!(CheckKind::parse("fmt"), Some(CheckKind::Format));
        assert_eq!(CheckKind::parse("plan"), Some(CheckKind::Plan));
        assert_eq!(CheckKind::parse(""), None);
        for k in [
            CheckKind::Typecheck,
            CheckKind::Build,
            CheckKind::Lint,
            CheckKind::Tests,
            CheckKind::Format,
            CheckKind::Plan,
            CheckKind::Security,
        ] {
            assert_eq!(CheckKind::parse(k.name()), Some(k));
        }
    }
}
