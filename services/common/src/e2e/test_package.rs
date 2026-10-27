//! Checking test code where it lives.
//!
//! End-to-end tests often sit in a package of their own inside the repository
//! (`e2e/` with its own `package.json`, `tsconfig.json` and lockfile), outside
//! the main workspace. The repository's own checks never type-check or lint
//! it, so a fix there has to be checked in that package: install its
//! dependencies on their own (ignoring the parent workspace), type-check, lint
//! the changed files, then run the affected spec many times in a row
//! (Playwright's `--repeat-each`, a loop for runners without one) against the
//! team's preview or staging address, and count the passes.
//!
//! Pure: which packages a change touches and the commands that check them.
//! The worker runs the commands in its sandbox.

use serde::{Deserialize, Serialize};

/// The marker a repeat loop prints, read back by [`parse_repeat`].
pub const REPEAT_MARK: &str = "CODERHELM_REPEAT";

/// How many times the affected spec runs.
pub const REPEAT_RUNS: u32 = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Manager {
    Npm,
    Pnpm,
    Yarn,
    Bun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Runner {
    Playwright,
    Cypress,
    Vitest,
    Jest,
    Wdio,
}

impl Runner {
    pub fn name(self) -> &'static str {
        match self {
            Runner::Playwright => "Playwright",
            Runner::Cypress => "Cypress",
            Runner::Vitest => "Vitest",
            Runner::Jest => "Jest",
            Runner::Wdio => "WebdriverIO",
        }
    }
}

/// A package a change touches that the main workspace doesn't cover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestPackage {
    /// The package's folder, relative to the repository root.
    pub dir: String,
    pub manager: Manager,
    /// The lockfile the package has, if any.
    pub lockfile: Option<String>,
    pub has_tsconfig: bool,
    /// It has an ESLint setup (a config file or ESLint in its dependencies).
    pub lint: bool,
    pub runner: Option<Runner>,
    /// Changed files, relative to `dir`.
    pub files: Vec<String>,
}

const LOCKFILES: &[(&str, Manager)] = &[
    ("package-lock.json", Manager::Npm),
    ("npm-shrinkwrap.json", Manager::Npm),
    ("pnpm-lock.yaml", Manager::Pnpm),
    ("yarn.lock", Manager::Yarn),
    ("bun.lockb", Manager::Bun),
    ("bun.lock", Manager::Bun),
];

fn parent(path: &str) -> &str {
    path.rsplit_once('/').map(|(d, _)| d).unwrap_or("")
}

fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_string()
    } else {
        format!("{dir}/{name}")
    }
}

/// The nearest folder above `file` (or its own) with a `package.json`;
/// `""` is the root.
pub fn package_dir(tree: &[String], file: &str) -> Option<String> {
    let mut dir = parent(file).to_string();
    loop {
        if tree.iter().any(|p| *p == join(&dir, "package.json")) {
            return Some(dir);
        }
        if dir.is_empty() {
            return None;
        }
        dir = parent(&dir).to_string();
    }
}

/// Whether a workspace glob (`packages/*`, `apps/**`, `e2e`) covers `dir`.
fn glob_covers(glob: &str, dir: &str) -> bool {
    let g = glob.trim().trim_start_matches("./").trim_end_matches('/');
    if g.is_empty() || g.starts_with('!') {
        return false;
    }
    if let Some(base) = g.strip_suffix("/**") {
        return dir == base || dir.starts_with(&format!("{base}/"));
    }
    let gs: Vec<&str> = g.split('/').collect();
    let ds: Vec<&str> = dir.split('/').collect();
    gs.len() == ds.len()
        && gs.iter().zip(&ds).all(|(g, d)| {
            *g == "*" || g == d || g.strip_suffix('*').is_some_and(|p| d.starts_with(p))
        })
}

/// The workspace globs in a root `package.json` (`workspaces` as a list or
/// as `{ packages: [...] }`) or a `pnpm-workspace.yaml`.
pub fn workspace_globs(
    root_package_json: Option<&str>,
    pnpm_workspace: Option<&str>,
) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(v) =
        root_package_json.and_then(|t| serde_json::from_str::<serde_json::Value>(t).ok())
    {
        let ws = &v["workspaces"];
        let list = ws.as_array().or_else(|| ws["packages"].as_array());
        out.extend(
            list.into_iter()
                .flatten()
                .filter_map(|x| x.as_str().map(str::to_string)),
        );
    }
    if let Some(y) = pnpm_workspace.and_then(|t| serde_yaml::from_str::<serde_yaml::Value>(t).ok())
    {
        if let Some(list) = y.get("packages").and_then(|p| p.as_sequence()) {
            out.extend(list.iter().filter_map(|x| x.as_str().map(str::to_string)));
        }
    }
    out
}

fn runner_of(package_json: &str) -> Option<Runner> {
    let v: serde_json::Value = serde_json::from_str(package_json).ok()?;
    let has = |name: &str| {
        ["dependencies", "devDependencies"]
            .iter()
            .any(|k| v[*k].get(name).is_some())
    };
    if has("@playwright/test") || has("playwright") {
        Some(Runner::Playwright)
    } else if has("cypress") {
        Some(Runner::Cypress)
    } else if has("@wdio/cli") || has("webdriverio") {
        Some(Runner::Wdio)
    } else if has("vitest") {
        Some(Runner::Vitest)
    } else if has("jest") {
        Some(Runner::Jest)
    } else {
        None
    }
}

fn has_eslint(tree: &[String], dir: &str, package_json: &str) -> bool {
    let configs = [
        "eslint.config.js",
        "eslint.config.mjs",
        "eslint.config.cjs",
        "eslint.config.ts",
        ".eslintrc",
        ".eslintrc.js",
        ".eslintrc.cjs",
        ".eslintrc.json",
        ".eslintrc.yml",
    ];
    configs
        .iter()
        .any(|c| tree.iter().any(|p| *p == join(dir, c)))
        || package_json.contains("\"eslint\"")
}

/// The packages outside the main workspace that `changed` files sit in.
/// `read` gives a file's text (each package's `package.json`). A package is
/// outside when it has its own lockfile, or the root names no workspace that
/// covers it.
pub fn packages_outside(
    tree: &[String],
    changed: &[String],
    root_globs: &[String],
    read: &dyn Fn(&str) -> Option<String>,
) -> Vec<TestPackage> {
    let mut out: Vec<TestPackage> = Vec::new();
    for file in changed {
        let Some(dir) = package_dir(tree, file) else {
            continue;
        };
        if dir.is_empty() {
            continue;
        }
        let rel = file
            .strip_prefix(&format!("{dir}/"))
            .unwrap_or(file)
            .to_string();
        if let Some(p) = out.iter_mut().find(|p| p.dir == dir) {
            p.files.push(rel);
            continue;
        }
        let lock = LOCKFILES
            .iter()
            .find(|(name, _)| tree.iter().any(|p| *p == join(&dir, name)));
        let covered = root_globs.iter().any(|g| glob_covers(g, &dir));
        if lock.is_none() && covered {
            continue;
        }
        let pj = read(&join(&dir, "package.json")).unwrap_or_default();
        out.push(TestPackage {
            dir: dir.clone(),
            manager: lock.map(|(_, m)| *m).unwrap_or(Manager::Npm),
            lockfile: lock.map(|(n, _)| n.to_string()),
            has_tsconfig: tree.iter().any(|p| *p == join(&dir, "tsconfig.json")),
            lint: has_eslint(tree, &dir, &pj),
            runner: runner_of(&pj),
            files: vec![rel],
        });
    }
    out
}

/// Single-quotes a path for the shell; `None` when it can't be quoted safely.
fn q(s: &str) -> Option<String> {
    let ok = !s.is_empty()
        && !s.starts_with('-')
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "/._-@+[]()".contains(c));
    ok.then(|| format!("'{s}'"))
}

/// Installs the package's dependencies on their own, never the parent
/// workspace's.
pub fn install_cmd(p: &TestPackage) -> String {
    let frozen = p.lockfile.is_some();
    match p.manager {
        Manager::Npm if frozen => "npm ci --no-audit --no-fund --workspaces=false".into(),
        Manager::Npm => "npm install --no-audit --no-fund --workspaces=false".into(),
        Manager::Pnpm => format!(
            "pnpm install --ignore-workspace{}",
            if frozen { " --frozen-lockfile" } else { "" }
        ),
        Manager::Yarn => format!(
            "yarn install --ignore-engines{}",
            if frozen { " --frozen-lockfile" } else { "" }
        ),
        Manager::Bun => format!(
            "bun install{}",
            if frozen { " --frozen-lockfile" } else { "" }
        ),
    }
}

fn is_script(f: &str) -> bool {
    [".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".mts", ".cts"]
        .iter()
        .any(|e| f.ends_with(e))
}

/// One check step: what it is (`install`, `typecheck`, `lint`) and its
/// command, run from the repository root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub kind: &'static str,
    pub cmd: String,
}

/// Install, type-check and lint for a package; `None` when its folder can't
/// be quoted for the shell.
pub fn check_steps(p: &TestPackage) -> Option<Vec<Step>> {
    let dir = q(&p.dir)?;
    let mut steps = vec![Step {
        kind: "install",
        cmd: format!("cd {dir} && {}", install_cmd(p)),
    }];
    if p.has_tsconfig {
        steps.push(Step {
            kind: "typecheck",
            cmd: format!("cd {dir} && npx --no-install tsc --noEmit -p tsconfig.json"),
        });
    }
    let lintable: Vec<String> = p
        .files
        .iter()
        .filter(|f| is_script(f))
        .filter_map(|f| q(f))
        .collect();
    if p.lint && !lintable.is_empty() {
        steps.push(Step {
            kind: "lint",
            cmd: format!("cd {dir} && npx --no-install eslint {}", lintable.join(" ")),
        });
    }
    Some(steps)
}

/// The command that runs `specs` (relative to the package) `n` times
/// against `target`, and prints the counts. `None` when the runner isn't
/// known or a path can't be quoted.
pub fn repeat_cmd(
    p: &TestPackage,
    specs: &[String],
    n: u32,
    target: Option<&str>,
) -> Option<String> {
    let dir = q(&p.dir)?;
    let runner = p.runner?;
    let files: Vec<String> = specs.iter().map(|s| q(s)).collect::<Option<_>>()?;
    if files.is_empty() || n == 0 {
        return None;
    }
    let target = match target {
        Some(t) => {
            super::report_source::check_target(t).ok()?;
            Some(t)
        }
        None => None,
    };
    let env = |vars: &[&str]| -> String {
        target
            .map(|t| {
                vars.iter()
                    .map(|v| format!("{v}='{t}' "))
                    .collect::<String>()
            })
            .unwrap_or_default()
    };
    let list = files.join(" ");
    Some(match runner {
        Runner::Playwright => format!(
            "cd {dir} && npx --no-install playwright install --with-deps chromium >/dev/null 2>&1; \
             {}npx --no-install playwright test {list} --repeat-each={n} --retries=0 --reporter=line",
            env(&["BASE_URL", "PLAYWRIGHT_BASE_URL", "PLAYWRIGHT_TEST_BASE_URL"])
        ),
        other => {
            let run = match other {
                Runner::Cypress => format!(
                    "{}npx --no-install cypress run --spec {}",
                    env(&["CYPRESS_BASE_URL", "BASE_URL"]),
                    files.join(",")
                ),
                Runner::Vitest => format!("{}npx --no-install vitest run {list}", env(&["BASE_URL"])),
                Runner::Jest => format!("{}npx --no-install jest {list}", env(&["BASE_URL"])),
                Runner::Wdio => format!(
                    "{}npx --no-install wdio run wdio.conf.ts --spec {list}",
                    env(&["BASE_URL"])
                ),
                Runner::Playwright => unreachable!(),
            };
            format!(
                "cd {dir} && p=0; f=0; for i in $(seq 1 {n}); do if {run} >/dev/null 2>&1; then p=$((p+1)); else f=$((f+1)); fi; done; \
                 echo \"{REPEAT_MARK} passed=$p failed=$f\"; [ $f -eq 0 ]"
            )
        }
    })
}

/// What a repeat run counted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Repeat {
    pub passed: u32,
    pub failed: u32,
    pub flaky: u32,
}

impl Repeat {
    pub fn total(&self) -> u32 {
        self.passed + self.failed + self.flaky
    }
    pub fn green(&self) -> bool {
        self.total() > 0 && self.failed == 0 && self.flaky == 0
    }
    pub fn line(&self) -> String {
        format!("{}/{} passed", self.passed, self.total())
    }
}

/// The counts in a repeat run's output: the loop's marker, or Playwright's
/// summary (`20 passed`, `2 failed`, `1 flaky`).
pub fn parse_repeat(output: &str) -> Option<Repeat> {
    let mut r = Repeat::default();
    let mut found = false;
    for line in output.lines() {
        let l = line.trim();
        if let Some(rest) = l.strip_prefix(REPEAT_MARK) {
            for part in rest.split_whitespace() {
                match part.split_once('=') {
                    Some(("passed", n)) => r.passed = n.parse().unwrap_or(0),
                    Some(("failed", n)) => r.failed = n.parse().unwrap_or(0),
                    _ => {}
                }
            }
            return Some(Repeat { flaky: 0, ..r });
        }
        let mut words = l.split_whitespace();
        let (Some(n), Some(what)) = (words.next(), words.next()) else {
            continue;
        };
        let Ok(n) = n.parse::<u32>() else { continue };
        match what {
            "passed" => r.passed = n,
            "failed" => r.failed = n,
            "flaky" => r.flaky = n,
            _ => continue,
        }
        found = true;
    }
    found.then_some(r)
}

/// The marker each step's exit code is printed with.
pub const STEP_MARK: &str = "CODERHELM_TEST_STEP";

/// One shell script for the sandbox: every step in order (each one's output
/// in its own log, so one failing never hides another), the tail of each
/// failing step, the repeat run's summary lines, and one marker per step
/// with its exit code. It exits non-zero when any step failed.
pub fn sandbox_script(steps: &[Step], repeat: Option<&str>) -> String {
    let mut all: Vec<(String, String)> = steps
        .iter()
        .map(|s| (s.kind.to_string(), s.cmd.clone()))
        .collect();
    if let Some(r) = repeat {
        all.push(("repeat".into(), r.to_string()));
    }
    let per = 4_000 / all.len().max(1);
    let mut out = String::new();
    for (i, (_, cmd)) in all.iter().enumerate() {
        let n = i + 1;
        out.push_str(&format!(
            "( {cmd} ) > /tmp/ch-test-{n}.log 2>&1; ch_t{n}=$?\n"
        ));
    }
    for (i, (kind, _)) in all.iter().enumerate() {
        let n = i + 1;
        if kind == "repeat" {
            out.push_str(&format!(
                "grep -E '^[[:space:]]*[0-9]+ (passed|failed|flaky)|{REPEAT_MARK}' /tmp/ch-test-{n}.log\n"
            ));
        }
        out.push_str(&format!(
            "if [ $ch_t{n} -ne 0 ]; then echo \"--- {kind} failed (exit $ch_t{n}) ---\"; tail -c {per} /tmp/ch-test-{n}.log; echo; fi\n"
        ));
    }
    for n in 1..=all.len() {
        out.push_str(&format!("echo \"{STEP_MARK} {n} $ch_t{n}\"\n"));
    }
    let checks: Vec<String> = (1..=all.len())
        .map(|n| format!("[ $ch_t{n} -eq 0 ]"))
        .collect();
    out.push_str(&if checks.is_empty() {
        "true".to_string()
    } else {
        checks.join(" && ")
    });
    out
}

/// Each step's exit code from the script's output (`None`: no result).
pub fn step_codes(output: &str, steps: usize) -> Vec<Option<i32>> {
    let mut codes = vec![None; steps];
    for line in output.lines() {
        let mut parts = line.split_whitespace();
        if parts.next() != Some(STEP_MARK) {
            continue;
        }
        let (Some(n), Some(c)) = (
            parts.next().and_then(|n| n.parse::<usize>().ok()),
            parts.next().and_then(|c| c.parse::<i32>().ok()),
        ) else {
            continue;
        };
        if (1..=steps).contains(&n) {
            codes[n - 1] = Some(c);
        }
    }
    codes
}

/// The files a spec or helper imports by a relative path (`./pages/items`,
/// `../support/db`), resolved against `tree`: the components a test uses.
pub fn relative_imports(file: &str, content: &str, tree: &[String]) -> Vec<String> {
    let dir = parent(file);
    let mut out: Vec<String> = Vec::new();
    for line in content.lines() {
        let l = line.trim();
        if !(l.starts_with("import ") || l.contains("require(") || l.starts_with("export ")) {
            continue;
        }
        for q in ['\'', '"'] {
            let parts: Vec<&str> = l.split(q).collect();
            for spec in parts.iter().skip(1).step_by(2) {
                if !(spec.starts_with("./") || spec.starts_with("../")) {
                    continue;
                }
                let mut segs: Vec<&str> = if dir.is_empty() {
                    Vec::new()
                } else {
                    dir.split('/').collect()
                };
                for s in spec.split('/') {
                    match s {
                        "." | "" => {}
                        ".." => {
                            segs.pop();
                        }
                        other => segs.push(other),
                    }
                }
                let base = segs.join("/");
                let candidates = [
                    base.clone(),
                    format!("{base}.ts"),
                    format!("{base}.tsx"),
                    format!("{base}.js"),
                    format!("{base}.jsx"),
                    format!("{base}.mjs"),
                    format!("{base}/index.ts"),
                    format!("{base}/index.js"),
                ];
                if let Some(hit) = candidates.iter().find(|c| tree.contains(c)) {
                    if !out.contains(hit) {
                        out.push(hit.clone());
                    }
                }
            }
        }
        if out.len() >= 20 {
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(paths: &[&str]) -> Vec<String> {
        paths.iter().map(|s| s.to_string()).collect()
    }

    const PW: &str = r#"{"devDependencies":{"@playwright/test":"1.48.0","eslint":"9.0.0"}}"#;

    #[test]
    fn a_test_package_with_its_own_lockfile_is_outside_the_workspace() {
        let t = tree(&[
            "package.json",
            "package-lock.json",
            "e2e/package.json",
            "e2e/package-lock.json",
            "e2e/tsconfig.json",
            "e2e/pages/items.ts",
            "e2e/tests/items.spec.ts",
            "src/app.ts",
        ]);
        let changed = vec![
            "e2e/pages/items.ts".to_string(),
            "e2e/tests/items.spec.ts".into(),
            "src/app.ts".into(),
        ];
        let read = |p: &str| (p == "e2e/package.json").then(|| PW.to_string());
        let pk = packages_outside(&t, &changed, &["packages/*".into()], &read);
        assert_eq!(pk.len(), 1);
        let p = &pk[0];
        assert_eq!(p.dir, "e2e");
        assert_eq!(p.manager, Manager::Npm);
        assert!(p.has_tsconfig && p.lint);
        assert_eq!(p.runner, Some(Runner::Playwright));
        assert_eq!(p.files, vec!["pages/items.ts", "tests/items.spec.ts"]);
        let steps = check_steps(p).unwrap();
        assert_eq!(
            steps[0].cmd,
            "cd 'e2e' && npm ci --no-audit --no-fund --workspaces=false"
        );
        assert_eq!(steps[1].kind, "typecheck");
        assert_eq!(
            steps[2].cmd,
            "cd 'e2e' && npx --no-install eslint 'pages/items.ts' 'tests/items.spec.ts'"
        );
    }

    #[test]
    fn a_workspace_member_without_a_lockfile_is_checked_by_the_workspace() {
        let t = tree(&[
            "package.json",
            "packages/web/package.json",
            "packages/web/a.spec.ts",
        ]);
        let changed = vec!["packages/web/a.spec.ts".to_string()];
        let globs = workspace_globs(Some(r#"{"workspaces":["packages/*"]}"#), None);
        assert!(packages_outside(&t, &changed, &globs, &|_| None).is_empty());
        // Not named by the workspace: checked on its own.
        assert_eq!(packages_outside(&t, &changed, &[], &|_| None).len(), 1);
        let pnpm = workspace_globs(None, Some("packages:\n  - 'apps/**'\n"));
        assert_eq!(pnpm, vec!["apps/**"]);
        assert!(glob_covers("apps/**", "apps/x/y"));
        assert!(glob_covers("packages/web-*", "packages/web-tests"));
        assert!(!glob_covers("packages/*", "e2e"));
        let obj = workspace_globs(Some(r#"{"workspaces":{"packages":["e2e"]}}"#), None);
        assert_eq!(obj, vec!["e2e"]);
    }

    #[test]
    fn installs_ignore_the_parent_workspace_for_every_manager() {
        let mut p = TestPackage {
            dir: "tests/e2e".into(),
            manager: Manager::Pnpm,
            lockfile: Some("pnpm-lock.yaml".into()),
            has_tsconfig: false,
            lint: false,
            runner: None,
            files: vec!["a.spec.ts".into()],
        };
        assert_eq!(
            install_cmd(&p),
            "pnpm install --ignore-workspace --frozen-lockfile"
        );
        p.manager = Manager::Yarn;
        assert!(install_cmd(&p).contains("--frozen-lockfile"));
        p.manager = Manager::Npm;
        p.lockfile = None;
        assert!(install_cmd(&p).contains("--workspaces=false"));
        assert_eq!(check_steps(&p).unwrap().len(), 1);
        p.dir = "bad dir; rm -rf /".into();
        assert!(check_steps(&p).is_none());
    }

    #[test]
    fn repeat_commands_use_the_runner_and_the_target() {
        let mut p = TestPackage {
            dir: "e2e".into(),
            manager: Manager::Npm,
            lockfile: None,
            has_tsconfig: true,
            lint: true,
            runner: Some(Runner::Playwright),
            files: vec![],
        };
        let specs = vec!["tests/items.spec.ts".to_string()];
        let pw = repeat_cmd(&p, &specs, 20, Some("https://staging.example.com")).unwrap();
        assert!(pw.contains("--repeat-each=20 --retries=0"));
        assert!(pw.contains("PLAYWRIGHT_BASE_URL='https://staging.example.com'"));
        assert!(repeat_cmd(&p, &specs, 20, Some("https://x.example.com/$(id)")).is_none());
        p.runner = Some(Runner::Cypress);
        let cy = repeat_cmd(&p, &specs, 5, None).unwrap();
        assert!(cy.contains("seq 1 5") && cy.contains("cypress run --spec 'tests/items.spec.ts'"));
        assert!(cy.contains(REPEAT_MARK));
        p.runner = None;
        assert!(repeat_cmd(&p, &specs, 5, None).is_none());
    }

    #[test]
    fn repeat_counts_are_read_from_the_output() {
        let pw = "Running 20 tests using 4 workers\n  1 flaky\n  19 passed (41.2s)\n";
        let r = parse_repeat(pw).unwrap();
        assert_eq!((r.passed, r.failed, r.flaky), (19, 0, 1));
        assert!(!r.green());
        let ok = parse_repeat("  20 passed (30s)").unwrap();
        assert!(ok.green());
        assert_eq!(ok.line(), "20/20 passed");
        let lp = parse_repeat("noise\nCODERHELM_REPEAT passed=4 failed=1\n").unwrap();
        assert_eq!((lp.passed, lp.failed), (4, 1));
        assert!(parse_repeat("nothing here").is_none());
    }

    #[test]
    fn the_sandbox_script_reports_each_step_and_the_repeat_counts() {
        let steps = vec![
            Step {
                kind: "install",
                cmd: "cd 'e2e' && npm ci".into(),
            },
            Step {
                kind: "typecheck",
                cmd: "cd 'e2e' && npx --no-install tsc --noEmit -p tsconfig.json".into(),
            },
        ];
        let sc = sandbox_script(
            &steps,
            Some("cd 'e2e' && npx --no-install playwright test 'a.spec.ts' --repeat-each=20"),
        );
        assert!(sc.contains("ch_t3=$?"));
        assert!(sc.contains("grep -E"));
        assert!(sc.ends_with("[ $ch_t1 -eq 0 ] && [ $ch_t2 -eq 0 ] && [ $ch_t3 -eq 0 ]"));
        let out = "  20 passed (40s)\nCODERHELM_TEST_STEP 1 0\nCODERHELM_TEST_STEP 2 2\nCODERHELM_TEST_STEP 3 0\n";
        assert_eq!(step_codes(out, 3), vec![Some(0), Some(2), Some(0)]);
        assert_eq!(step_codes("", 2), vec![None, None]);
        assert!(parse_repeat(out).unwrap().green());
    }

    #[test]
    fn a_spec_s_components_are_its_relative_imports() {
        let t = tree(&[
            "e2e/tests/items.spec.ts",
            "e2e/pages/items-page.ts",
            "e2e/support/index.ts",
            "e2e/fixtures.js",
        ]);
        let spec = "import { test } from '@playwright/test';\nimport { ItemsPage } from '../pages/items-page';\nimport '../support';\nconst f = require(\"../fixtures\");\nconst x = './not-an-import';";
        assert_eq!(
            relative_imports("e2e/tests/items.spec.ts", spec, &t),
            vec![
                "e2e/pages/items-page.ts",
                "e2e/support/index.ts",
                "e2e/fixtures.js"
            ]
        );
    }
}
