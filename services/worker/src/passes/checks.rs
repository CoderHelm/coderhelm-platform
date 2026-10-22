//! The checks a repository's CI gates a pull request on, and running them in
//! the sandbox one step at a time. A team rule said "run lint, type-check and
//! the tests before pushing", the sandbox ran only the type-check and the
//! touched tests, and the PR opened with a warning that lint never ran, which
//! CI then passed. So the sandbox runs CI's own lint command too, each check as
//! its own step so one failing never hides another's result, and the run
//! records which kinds of check ran and what CI will still run on the PR.

/// A kind of check a repository's CI runs on a pull request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CheckKind {
    Typecheck,
    Build,
    Lint,
    Tests,
}

impl CheckKind {
    /// The name people and the judge read, and the judge writes back.
    pub fn name(self) -> &'static str {
        match self {
            CheckKind::Typecheck => "typecheck",
            CheckKind::Build => "build",
            CheckKind::Lint => "lint",
            CheckKind::Tests => "tests",
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
            "typecheck" | "types" | "checktypes" => Some(CheckKind::Typecheck),
            "build" | "compile" => Some(CheckKind::Build),
            "lint" | "linter" => Some(CheckKind::Lint),
            "tests" | "test" | "unittests" | "testsuite" => Some(CheckKind::Tests),
            _ => None,
        }
    }
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

/// The `run:` command lines of a workflow, with YAML prefixes and quotes
/// stripped (the same reading `detect_ci_typecheck` does).
fn run_lines(yaml: &str) -> impl Iterator<Item = &str> {
    yaml.lines().map(|raw| {
        let line = raw.trim().trim_start_matches("- ").trim();
        let line = line.strip_prefix("run:").unwrap_or(line).trim();
        line.trim_matches(|c| c == '"' || c == '\'').trim()
    })
}

/// A single shell line that runs a linter. Not `lint-staged` or
/// `commitlint`, which lint git state the sandbox doesn't have.
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

/// The kinds of check the repository's CI runs on a pull request, from the
/// texts of its workflow files. Only workflows a pull request triggers count:
/// their result shows on the PR, so they settle what didn't run before it.
pub fn gates_from_workflows(yamls: &[String]) -> Vec<CheckKind> {
    let mut kinds = Vec::new();
    for yaml in yamls.iter().filter(|y| y.contains("pull_request")) {
        for line in run_lines(yaml) {
            let kind = if super::is_typecheck_cmd(line) {
                Some(CheckKind::Typecheck)
            } else if is_lint_cmd(line) {
                Some(CheckKind::Lint)
            } else if is_any_test_cmd(line) {
                Some(CheckKind::Tests)
            } else if is_build_cmd(line) {
                Some(CheckKind::Build)
            } else {
                None
            };
            if let Some(k) = kind {
                if !kinds.contains(&k) {
                    kinds.push(k);
                }
            }
        }
    }
    kinds.sort();
    kinds
}

/// Every distinct lint command the workflows run, joined with `&&`, or
/// `None` when they run none.
pub fn lint_from_workflows(yamls: &[String]) -> Option<String> {
    let mut cmds: Vec<&str> = Vec::new();
    for yaml in yamls {
        for line in run_lines(yaml) {
            if is_lint_cmd(line) && !cmds.contains(&line) {
                cmds.push(line);
            }
        }
    }
    (!cmds.is_empty()).then(|| cmds.join(" && "))
}

/// The texts of the repository's workflow files at `git_ref`.
pub async fn read_workflows(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Vec<String> {
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
        if let Ok(yaml) = github.read_file(owner, repo, &entry.path, git_ref).await {
            out.push(yaml);
        }
    }
    out
}

/// The repository's own CI lint command (the faithful replica, as
/// `detect_ci_typecheck` is for the type-check), for the sandbox to run as
/// its own step.
pub async fn detect_ci_lint(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Option<String> {
    lint_from_workflows(&read_workflows(github, owner, repo, git_ref).await)
}

/// The kinds of check the repository's CI runs on a pull request.
pub async fn detect_ci_gates(
    github: &crate::clients::github::GitHubClient,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Vec<CheckKind> {
    gates_from_workflows(&read_workflows(github, owner, repo, git_ref).await)
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
    fn ci_gates_come_only_from_workflows_a_pull_request_triggers() {
        let pr = "on:\n  pull_request:\njobs:\n  lint:\n    steps:\n      - run: pnpm lint\n      - run: pnpm check-types\n      - run: pnpm test:run\n      - run: pnpm build\n".to_string();
        let push_only = "on:\n  push:\n    branches: [main]\njobs:\n  e:\n    steps:\n      - run: cargo test\n".to_string();
        assert_eq!(
            gates_from_workflows(&[pr.clone(), push_only.clone()]),
            vec![
                CheckKind::Typecheck,
                CheckKind::Build,
                CheckKind::Lint,
                CheckKind::Tests
            ]
        );
        assert_eq!(gates_from_workflows(&[push_only]), vec![]);
        assert_eq!(lint_from_workflows(&[pr]), Some("pnpm lint".to_string()));
        assert_eq!(lint_from_workflows(&[]), None);
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
        assert_eq!(CheckKind::parse("test"), Some(CheckKind::Tests));
        assert_eq!(CheckKind::parse(""), None);
        for k in [
            CheckKind::Typecheck,
            CheckKind::Build,
            CheckKind::Lint,
            CheckKind::Tests,
        ] {
            assert_eq!(CheckKind::parse(k.name()), Some(k));
        }
    }
}
