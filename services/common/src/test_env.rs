//! How a repository's CI installs and runs its tests, found once and reused
//! by every sandbox command of the run.
//!
//! A data-pipeline run reinstalled its test dependencies eighteen times in
//! one run, about 25 minutes of sandbox builds. Each round started over: it
//! ran the test file bare, read the CI workflow, then tried `pip install`
//! lines until one worked. Each sandbox command is a fresh build, so nothing
//! carried over.
//!
//! Now:
//!
//! - [`from_workflows`] reads the CI job that runs the tests: its working
//!   directory, Python and Node versions, the install steps before its first
//!   test step, the test steps, and the plain environment values it sets.
//! - [`TestEnv::fingerprint`] names that install, from its steps and the
//!   dependency manifests' blob ids. A changed requirements or lock file is
//!   a new fingerprint and a fresh install.
//! - [`TestEnv::wrap`] puts a command inside the environment. With a cached
//!   environment (`ENV_CACHE_GET_URL`) it is restored, and a Python install is
//!   not run again; without one the install runs once and the result is
//!   saved (`ENV_CACHE_PUT_URL`) for every later command, round and proof
//!   step of the run. Python installs go into one virtual environment and
//!   Node package managers keep their download store there, so the archive
//!   is one folder.
//! - [`TestEnv::coder_note`] tells the coder the environment is ready, so it
//!   runs the tests directly instead of reinstalling.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Where the environment lives in the sandbox; a test may move it.
pub const ENV_ROOT: &str = "/tmp/coderhelm-env";

/// The printed lines that say what the wrapper did.
pub const RESTORED: &str = "===CODERHELM_ENV_RESTORED===";
pub const INSTALLED: &str = "===CODERHELM_ENV_INSTALLED===";
pub const SAVED: &str = "===CODERHELM_ENV_SAVED===";
/// Printed with the install's exit code when CI's install failed: what
/// follows ran without it, so a test result after it says nothing.
pub const INSTALL_FAILED: &str = "===CODERHELM_ENV_INSTALL_FAILED===";

/// The language whose packages the install fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Python,
    Node,
    #[default]
    Other,
}

/// How the repository's CI installs and runs its tests.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TestEnv {
    /// The workflow and job it came from.
    pub source: String,
    /// The folder the steps run in; "" for the repository root.
    pub dir: String,
    pub kind: Kind,
    pub python: String,
    pub node: String,
    /// The pnpm version CI's `pnpm/action-setup` pins ("" when none).
    pub pnpm: String,
    /// Plain `NAME=value` settings of the job (never an expression).
    pub env: Vec<(String, String)>,
    pub install: Vec<String>,
    pub test: Vec<String>,
    /// Files whose content decides the install (requirements, locks).
    pub manifests: Vec<String>,
}

const INSTALL_WORDS: &[&str] = &[
    "pip install",
    "pip3 install",
    "uv pip install",
    "uv sync",
    "poetry install",
    "pipenv install",
    "npm ci",
    "npm install",
    "pnpm install",
    "yarn install",
    "bun install",
    "corepack enable",
];

const LOCKS: &[&str] = &[
    "requirements.txt",
    "requirement.txt",
    "requirements-dev.txt",
    "constraints.txt",
    "pyproject.toml",
    "poetry.lock",
    "uv.lock",
    "Pipfile.lock",
    "package.json",
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "bun.lockb",
];

fn s(v: Option<&serde_yaml::Value>) -> String {
    match v {
        Some(serde_yaml::Value::String(x)) => x.trim().to_string(),
        Some(serde_yaml::Value::Number(n)) => n.to_string(),
        _ => String::new(),
    }
}

fn plain_env(v: Option<&serde_yaml::Value>) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let Some(m) = v.and_then(|x| x.as_mapping()) {
        for (k, val) in m {
            let (k, val) = (s(Some(k)), s(Some(val)));
            let safe_key =
                !k.is_empty() && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            let low = k.to_ascii_lowercase();
            let secretish = ["token", "secret", "password", "key", "auth"]
                .iter()
                .any(|w| low.contains(w));
            if safe_key && !secretish && !val.contains("${{") && !val.contains('\n') {
                out.push((k, val));
            }
        }
    }
    out
}

fn lines_of(run: &str) -> Vec<String> {
    run.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect()
}

fn is_install(line: &str) -> bool {
    let l = line.to_ascii_lowercase();
    INSTALL_WORDS.iter().any(|w| l.contains(w))
}

/// Files an install line reads (`-r`, `-c`, `--constraint`, `--requirement`).
fn named_files(line: &str) -> Vec<String> {
    let t: Vec<&str> = line.split_whitespace().collect();
    let mut out = Vec::new();
    for (i, w) in t.iter().enumerate() {
        let next = t.get(i + 1).map(|x| x.trim_matches(['"', '\'']));
        match *w {
            "-r" | "-c" | "--constraint" | "--requirement" => {
                if let Some(f) = next.filter(|f| !f.contains("://") && !f.contains('$')) {
                    out.push(f.to_string());
                }
            }
            _ => {}
        }
    }
    out
}

fn join(dir: &str, f: &str) -> String {
    let f = f.trim_start_matches("./");
    if dir.is_empty() || f.starts_with('/') {
        f.to_string()
    } else {
        format!("{}/{f}", dir.trim_end_matches('/'))
    }
}

/// The repository's test environment, from its workflow files
/// (`(path, text)`), with no files to aim it at: [`from_workflows_for`].
pub fn from_workflows(workflows: &[(String, String)], tree: &[String]) -> Option<TestEnv> {
    from_workflows_for(workflows, tree, &[])
}

/// The `paths` filters of a workflow's `pull_request` and `push` triggers.
/// Empty when it runs on every change.
fn path_filters(doc: &serde_yaml::Value) -> Vec<String> {
    let Some(on) = doc.get("on") else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for event in ["pull_request", "push"] {
        let Some(e) = on.get(event) else {
            continue;
        };
        if let Some(list) = e.get("paths").and_then(|p| p.as_sequence()) {
            out.extend(list.iter().map(|p| s(Some(p))).filter(|p| !p.is_empty()));
        }
    }
    out
}

/// How well a job fits the files a run changes: 0 aims at them (its
/// workflow's `paths` filter or its working directory covers one), 1 runs
/// on every change from the repository root, 2 is another package's job
/// (a filter or a folder none of the files is in). `None` with no files:
/// every job fits, unfiltered ones first.
fn fit(filters: &[String], dir: &str, focus: &[String]) -> u8 {
    let in_dir = |f: &String| dir.is_empty() || f.starts_with(&format!("{dir}/"));
    if focus.is_empty() {
        return if filters.is_empty() && dir.is_empty() {
            1
        } else {
            2
        };
    }
    let filtered_in = !filters.is_empty()
        && focus.iter().any(|f| {
            filters.iter().any(|g| {
                !g.starts_with('!') && crate::skill_apply::glob_match(g.trim_start_matches("./"), f)
            })
        });
    if !filters.is_empty() && !filtered_in {
        return 2;
    }
    if !focus.iter().any(in_dir) {
        return 2;
    }
    if filtered_in || !dir.is_empty() {
        0
    } else {
        1
    }
}

/// The repository's test environment, from its workflow files
/// (`(path, text)`): the job whose tests run for `focus` (the files the run
/// changes or plans to), its setup before its first test step, and its test
/// steps. `None` when no job runs tests.
///
/// A monorepo's first workflow is often another package's (a lambda's
/// deploy, filtered to its own folder): its `npm ci` and `npm test` say
/// nothing about a web change, and told to the coder as "the test
/// environment" they send every sandbox command through the wrong install.
/// So a job whose workflow filters to other paths, or that runs in a folder
/// none of the files is in, is taken only when nothing fits better; with no
/// files, a job that runs on every change from the root comes first.
pub fn from_workflows_for(
    workflows: &[(String, String)],
    tree: &[String],
    focus: &[String],
) -> Option<TestEnv> {
    let focus: Vec<String> = focus
        .iter()
        .map(|f| f.trim_start_matches("./").to_string())
        .filter(|f| !f.is_empty())
        .collect();
    let mut best: Option<(u8, TestEnv)> = None;
    for (path, text) in workflows {
        let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(text) else {
            continue;
        };
        let Some(jobs) = doc.get("jobs").and_then(|j| j.as_mapping()) else {
            continue;
        };
        let filters = path_filters(&doc);
        for (name, job) in jobs {
            let Some(steps) = job.get("steps").and_then(|x| x.as_sequence()) else {
                continue;
            };
            let mut env = TestEnv {
                source: format!("{path} ({})", s(Some(name))),
                dir: s(job
                    .get("defaults")
                    .and_then(|d| d.get("run"))
                    .and_then(|r| r.get("working-directory")))
                .trim_start_matches("./")
                .trim_end_matches('/')
                .to_string(),
                env: plain_env(doc.get("env")),
                ..Default::default()
            };
            env.env.extend(plain_env(job.get("env")));
            let mut seen_test = false;
            for step in steps {
                let uses = s(step.get("uses")).to_ascii_lowercase();
                if uses.starts_with("actions/setup-python") {
                    env.python = s(step.get("with").and_then(|w| w.get("python-version")));
                }
                if uses.starts_with("actions/setup-node") {
                    env.node = s(step.get("with").and_then(|w| w.get("node-version")));
                }
                if uses.starts_with("pnpm/action-setup") {
                    env.pnpm = s(step.get("with").and_then(|w| w.get("version")));
                }
                let run = s(step.get("run"));
                if run.is_empty() || run.contains("${{") {
                    continue;
                }
                let dir = s(step.get("working-directory"))
                    .trim_start_matches("./")
                    .to_string();
                // A step that runs tests keeps its whole script (a loop's
                // body, a counter it sets), so a test file can be run the
                // way the step runs it (`TestEnv::file_command`).
                let step_tests = lines_of(&run)
                    .iter()
                    .any(|l| crate::ci_cover::is_check_command(l) && !is_install(l));
                for line in lines_of(&run) {
                    let line = if dir.is_empty() {
                        line
                    } else {
                        format!("(cd {dir} && {line})")
                    };
                    if step_tests && !is_install(&line) {
                        seen_test = true;
                        env.test.push(line);
                        env.env.extend(plain_env(step.get("env")));
                    } else if is_install(&line) && !seen_test {
                        env.install.push(line);
                    }
                }
            }
            if env.test.is_empty() {
                continue;
            }
            let all = format!("{} {}", env.install.join(" "), env.test.join(" ")).to_lowercase();
            env.kind = if !env.python.is_empty()
                || ["pip", "pytest", "python", "poetry", "uv "]
                    .iter()
                    .any(|w| all.contains(w))
            {
                Kind::Python
            } else if !env.node.is_empty()
                || ["npm", "pnpm", "yarn", "npx", "vitest", "jest"]
                    .iter()
                    .any(|w| all.contains(w))
            {
                Kind::Node
            } else {
                Kind::Other
            };
            let mut manifests: Vec<String> = env
                .install
                .iter()
                .flat_map(|l| named_files(l))
                .map(|f| join(&env.dir, &f))
                .collect();
            for l in LOCKS {
                let p = join(&env.dir, l);
                if tree.iter().any(|t| t == &p) {
                    manifests.push(p);
                }
            }
            manifests.sort();
            manifests.dedup();
            env.manifests = manifests;
            env.env.sort();
            env.env.dedup();
            // A job whose test steps all run in one folder belongs there.
            let dir = if env.dir.is_empty() {
                common_step_dir(&env.test)
            } else {
                env.dir.clone()
            };
            let rank = fit(&filters, &dir, &focus);
            if best.as_ref().is_none_or(|(r, _)| rank < *r) {
                best = Some((rank, env));
            }
            if rank == 0 {
                return best.map(|(_, e)| e);
            }
        }
    }
    best.map(|(_, e)| e)
}

/// The folder every test step `cd`s into (`(cd <dir> && ...)`), when they
/// all share one; "" otherwise.
fn common_step_dir(test: &[String]) -> String {
    let dirs: Vec<&str> = test
        .iter()
        .filter_map(|t| t.strip_prefix("(cd ")?.split(" && ").next())
        .map(|d| d.trim_end_matches('/'))
        .collect();
    match dirs.first() {
        Some(d) if dirs.len() == test.len() && dirs.iter().all(|x| x == d) => d.to_string(),
        _ => String::new(),
    }
}

/// Interpreters that run a test file given its path.
const SCRIPT_RUNNERS: &[&str] = &["python", "python3", "node", "bash", "sh"];

/// A test step without its `(cd <dir> && ...)` wrapper: (dir, command).
fn unwrap_cd(line: &str) -> (String, String) {
    if let Some(rest) = line.strip_prefix("(cd ") {
        if let Some((dir, cmd)) = rest.split_once(" && ") {
            let cmd = cmd.strip_suffix(')').unwrap_or(cmd);
            return (
                dir.trim().trim_end_matches('/').to_string(),
                cmd.trim().to_string(),
            );
        }
    }
    (String::new(), line.trim().to_string())
}

/// A shell loop's head, `for <var> in <globs>; do`: its variable and globs.
fn loop_head(line: &str) -> Option<(String, Vec<String>)> {
    let rest = line.trim().strip_prefix("for ")?;
    let (var, rest) = rest.split_once(" in ")?;
    let list = rest.split(';').next()?.trim_end_matches(" do").trim();
    let globs: Vec<String> = list
        .split_whitespace()
        .map(|g| {
            g.trim_matches(['"', '\''])
                .trim_start_matches("./")
                .to_string()
        })
        .filter(|g| !g.is_empty() && !g.contains('$'))
        .collect();
    let var = var.trim();
    (!var.is_empty() && !globs.is_empty()).then(|| (var.to_string(), globs))
}

/// The interpreter a loop body runs the loop's file with: `python "$t"`
/// gives `python`.
fn runs_var(line: &str, var: &str) -> Option<String> {
    let line = line.trim();
    for form in [
        format!("\"${var}\""),
        format!("\"${{{var}}}\""),
        format!("${{{var}}}"),
        format!("${var}"),
    ] {
        if let Some(i) = line.find(&form) {
            let runner = line[..i].trim();
            let first = runner.split_whitespace().next().unwrap_or("");
            if SCRIPT_RUNNERS.contains(&first) && !runner.contains(['|', ';', '&']) {
                return Some(runner.to_string());
            }
        }
    }
    None
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Whether a path is safe to put on a command line as it is.
fn safe_path(p: &str) -> bool {
    !p.is_empty()
        && !p.starts_with('-')
        && p.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '/'))
        && !p.contains("..")
}

/// A test command narrowed to one test file (`file`, relative to the folder
/// the command runs in), or `None` when its runner can't take one. The one
/// place CoderHelm narrows a runner: pytest and `python -m unittest` take
/// the path, Vitest and Jest take it as a filter (they print "no test files
/// found" when it matches nothing, never a pass), `go test` takes the
/// file's package, `cargo test` an integration test's target. A runner's
/// own flags after it are dropped with the rest of the line.
pub fn narrow(cmd: &str, file: &str) -> Option<String> {
    if !safe_path(file) {
        return None;
    }
    let words: Vec<&str> = cmd.split_whitespace().collect();
    let low: Vec<String> = words.iter().map(|w| w.to_ascii_lowercase()).collect();
    // Up to and including word `i`, never across a `&&`, `;` or `|`.
    let prefix = |i: usize| -> String {
        let start = words[..i]
            .iter()
            .rposition(|w| matches!(*w, "&&" | ";" | "||" | "|"))
            .map_or(0, |p| p + 1);
        words[start..=i].join(" ")
    };
    for (i, w) in low.iter().enumerate() {
        let w = w.trim_matches(['"', '\'']);
        let base = w.rsplit('/').next().unwrap_or(w);
        if base == "pytest" || (base == "unittest" && i > 0 && low[i - 1] == "-m") {
            return Some(format!("{} {file}", prefix(i)));
        }
        if base == "vitest" {
            let run = low.get(i + 1).is_some_and(|n| n == "run");
            let end = if run { i + 1 } else { i };
            let run = if run { "" } else { " run" };
            return Some(format!("{}{run} {file}", prefix(end)));
        }
        if base == "jest" {
            return Some(format!("{} {file}", prefix(i)));
        }
        if w == "go" && low.get(i + 1).is_some_and(|n| n == "test") && file.ends_with("_test.go") {
            let dir = file.rsplit_once('/').map_or(".", |(d, _)| d);
            let pre = prefix(i + 1);
            return Some(format!("{pre} ./{dir}/"));
        }
        if w == "cargo" && low.get(i + 1).is_some_and(|n| n == "test") {
            // An integration test, `tests/<name>.rs`, is a target of its own.
            let name = file
                .strip_prefix("tests/")
                .and_then(|n| n.strip_suffix(".rs"))
                .filter(|n| !n.contains('/'))?;
            return Some(format!("{} --test {name}", prefix(i + 1)));
        }
    }
    None
}

/// What a package script or a make target runs, from the manifests:
/// `npm test`, `npm run <s>`, `pnpm [--filter <name>] [run] <s>`, `yarn
/// [workspace <name>] <s>`, `bun run <s>`, `make <target>`, run in `dir`.
/// `(folder, command)` with the command the script or target holds, so a
/// runner in it can be narrowed. `read` gives a file's text; `tree` finds a
/// workspace package by name. `None` when it is no script call or the
/// script can't be read.
pub fn resolve_script(
    line: &str,
    dir: &str,
    tree: &[String],
    read: &dyn Fn(&str) -> Option<String>,
) -> Option<(String, String)> {
    resolve_depth(line, dir, tree, read, 0)
}

fn package_dir_named(
    tree: &[String],
    read: &dyn Fn(&str) -> Option<String>,
    name: &str,
) -> Option<String> {
    let name = name.trim_matches(['"', '\'']);
    if name.starts_with("./") || name.contains('/') && !name.starts_with('@') {
        return Some(
            name.trim_start_matches("./")
                .trim_end_matches('/')
                .to_string(),
        );
    }
    tree.iter()
        .filter(|p| *p == "package.json" || p.ends_with("/package.json"))
        .filter(|p| !p.contains("node_modules/"))
        .find(|p| {
            read(p)
                .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
                .is_some_and(|v| v["name"].as_str() == Some(name))
        })
        .map(|p| {
            p.trim_end_matches("package.json")
                .trim_end_matches('/')
                .to_string()
        })
}

fn resolve_depth(
    line: &str,
    dir: &str,
    tree: &[String],
    read: &dyn Fn(&str) -> Option<String>,
    depth: u8,
) -> Option<(String, String)> {
    if depth > 3 {
        return None;
    }
    let words: Vec<&str> = line.split_whitespace().collect();
    let (tool, rest) = words.split_first()?;
    let mut at = dir.to_string();
    let mut args: Vec<&str> = Vec::new();
    let mut i = 0;
    while i < rest.len() {
        let w = rest[i];
        match (*tool, w) {
            ("pnpm", "--filter" | "-F") | ("npm", "--workspace" | "-w") => {
                at = package_dir_named(tree, read, rest.get(i + 1)?)?;
                i += 2;
                continue;
            }
            ("pnpm" | "npm" | "yarn", "-C" | "--dir" | "--prefix" | "--cwd") | ("make", "-C") => {
                let d = rest
                    .get(i + 1)?
                    .trim_matches(['"', '\''])
                    .trim_end_matches('/');
                at = join(&at, d);
                i += 2;
                continue;
            }
            ("yarn", "workspace") => {
                at = package_dir_named(tree, read, rest.get(i + 1)?)?;
                i += 2;
                continue;
            }
            _ if w.starts_with("--filter=") && *tool == "pnpm" => {
                at = package_dir_named(tree, read, &w["--filter=".len()..])?;
            }
            _ if w.starts_with('-') => {}
            _ => args.push(w),
        }
        i += 1;
    }
    let script = match (*tool, args.as_slice()) {
        ("make", [target, ..]) => {
            let mk = read(&join(&at, "Makefile"))?;
            let recipe = make_recipe(&mk, target)?;
            return resolve_depth(&recipe, &at, tree, read, depth + 1).or(Some((at, recipe)));
        }
        ("make", []) => return None,
        ("npm" | "pnpm" | "yarn" | "bun", ["run" | "run-script", s, ..]) => s.to_string(),
        ("npm" | "pnpm" | "yarn", ["test" | "t", ..]) => "test".to_string(),
        ("pnpm" | "yarn", [s, ..])
            if !matches!(*s, "install" | "add" | "exec" | "dlx" | "i" | "ci" | "x") =>
        {
            s.to_string()
        }
        _ => return None,
    };
    let pj = read(&join(&at, "package.json"))?;
    let v: serde_json::Value = serde_json::from_str(&pj).ok()?;
    let body = v["scripts"][script.as_str()].as_str()?.trim().to_string();
    if body.is_empty() {
        return None;
    }
    resolve_depth(&body, &at, tree, read, depth + 1).or(Some((at, body)))
}

/// A script's command run from a shell: a Node runner a package script
/// finds in `node_modules/.bin` (`vitest`, `jest`) goes through `npx`.
fn via_bin(cmd: &str) -> String {
    let first = cmd.split_whitespace().next().unwrap_or("");
    if matches!(
        first,
        "vitest" | "jest" | "mocha" | "ava" | "tsx" | "ts-node"
    ) {
        format!("npx --no-install {cmd}")
    } else {
        cmd.to_string()
    }
}

fn in_folder(at: &str, cmd: &str) -> String {
    if at.is_empty() || at == "." {
        cmd.to_string()
    } else {
        format!("(cd {at} && {cmd})")
    }
}

/// The first recipe line of `target` in a Makefile that runs a check, its
/// `@`/`-` prefix dropped.
fn make_recipe(makefile: &str, target: &str) -> Option<String> {
    let mut lines = makefile.lines();
    while let Some(l) = lines.next() {
        let Some((name, _)) = l.split_once(':') else {
            continue;
        };
        if l.starts_with('\t') || name.split_whitespace().all(|n| n != target) {
            continue;
        }
        for r in lines.by_ref() {
            if !r.starts_with('\t') {
                if r.trim().is_empty() {
                    continue;
                }
                break;
            }
            let r = r.trim().trim_start_matches(['@', '-']).trim();
            let says = matches!(
                r.split_whitespace().next(),
                Some("echo" | "printf" | "true")
            );
            if !says && crate::ci_cover::is_check_command(r) {
                return Some(r.to_string());
            }
        }
        return None;
    }
    None
}

/// The files [`TestEnv::resolve_scripts`] and [`package_test_command`] may
/// read: the manifests and Makefiles of the folders the test steps run in,
/// the workspace's package manifests (a `--filter` names a package), and
/// each test file's nearest manifest. At most 40.
pub fn script_files(env: Option<&TestEnv>, tree: &[String], files: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut add = |p: String| {
        if tree.contains(&p) && !out.contains(&p) && out.len() < 40 {
            out.push(p);
        }
    };
    if let Some(e) = env {
        let mut dirs = vec![e.dir.clone()];
        dirs.extend(e.test.iter().map(|t| unwrap_cd(t).0));
        for d in dirs {
            add(join(&d, "package.json"));
            add(join(&d, "Makefile"));
        }
    }
    for f in files {
        if let Some(d) = crate::e2e::test_package::package_dir(tree, f) {
            add(join(&d, "package.json"));
        }
    }
    for p in tree
        .iter()
        .filter(|p| p.ends_with("/package.json") && !p.contains("node_modules/"))
    {
        add(p.clone());
    }
    out
}

/// The test file run by its own package's test script, narrowed to it: the
/// narrowest command that still runs it when CI's test step can't be
/// narrowed (a `turbo` or a root script running every package). `None`
/// when the package has no test script with a runner that takes a file.
pub fn package_test_command(
    file: &str,
    tree: &[String],
    read: &dyn Fn(&str) -> Option<String>,
) -> Option<String> {
    let dir = crate::e2e::test_package::package_dir(tree, file)?;
    let pj = read(&join(&dir, "package.json"))?;
    let v: serde_json::Value = serde_json::from_str(&pj).ok()?;
    let rel = if dir.is_empty() {
        file
    } else {
        file.strip_prefix(&format!("{dir}/"))?
    };
    for script in ["test:unit", "test:run", "test"] {
        let Some(body) = v["scripts"][script].as_str() else {
            continue;
        };
        let (at, body) =
            resolve_depth(body, &dir, tree, read, 1).unwrap_or((dir.clone(), body.to_string()));
        let rel = if at == dir {
            rel.to_string()
        } else {
            match file.strip_prefix(&format!("{at}/")) {
                Some(r) => r.to_string(),
                None => continue,
            }
        };
        if let Some(c) = narrow(&via_bin(&body), &rel) {
            return Some(in_folder(&at, &c));
        }
    }
    None
}

impl TestEnv {
    /// Whether this is another package's job for `focus` (the files a run
    /// changes): its tests run in a folder none of them is in.
    pub fn misses(&self, focus: &[String]) -> bool {
        let dir = if self.dir.is_empty() {
            common_step_dir(&self.test)
        } else {
            self.dir.clone()
        };
        !dir.is_empty()
            && !focus.is_empty()
            && !focus
                .iter()
                .any(|f| f.trim_start_matches("./").starts_with(&format!("{dir}/")))
    }

    /// Each test step that calls a package script or a make target
    /// (`pnpm --filter web test`, `npm run test:unit`, `make test`) put as
    /// the command the script holds, in its folder, so its runner can be
    /// narrowed to one file ([`Self::file_command`]). `read` gives the
    /// manifests' text ([`script_files`]).
    pub fn resolve_scripts(&mut self, tree: &[String], read: &dyn Fn(&str) -> Option<String>) {
        for line in self.test.iter_mut() {
            let (dir, cmd) = unwrap_cd(line);
            let dir = if dir.is_empty() {
                self.dir.clone()
            } else {
                dir
            };
            if let Some((at, body)) = resolve_script(&cmd, &dir, tree, read) {
                let at = at
                    .strip_prefix(&format!("{}/", self.dir))
                    .map(str::to_string)
                    .unwrap_or(if at == self.dir { String::new() } else { at });
                *line = in_folder(&at, &via_bin(&body));
            }
        }
    }

    /// The install's name: its steps, versions and the manifests' blob ids
    /// (`shas`: `(path, blob sha)` from the tree at the head).
    pub fn fingerprint(&self, shas: &[(String, String)]) -> String {
        let mut h = Sha256::new();
        h.update(self.dir.as_bytes());
        h.update(self.python.as_bytes());
        h.update(self.node.as_bytes());
        h.update(self.pnpm.as_bytes());
        for l in &self.install {
            h.update(l.as_bytes());
            h.update(b"\n");
        }
        for m in &self.manifests {
            let sha = shas
                .iter()
                .find(|(p, _)| p == m)
                .map(|(_, s)| s.as_str())
                .unwrap_or("-");
            h.update(format!("{m}={sha}\n").as_bytes());
        }
        hex::encode(&h.finalize()[..12])
    }

    /// Whether a restored environment is complete: a Python install lives
    /// whole in the virtual environment; a Node install keeps only the
    /// package store there and runs again (fast, from the store).
    pub fn restore_is_complete(&self) -> bool {
        self.kind == Kind::Python
    }

    fn activate(&self) -> String {
        let mut out = vec![format!(
            "export E=\"${{CODERHELM_ENV:-{ENV_ROOT}}}\"; mkdir -p \"$E\""
        )];
        for (k, v) in &self.env {
            out.push(format!("export {k}={}", quote(v)));
        }
        match self.kind {
            Kind::Python => {
                if !self.python.is_empty() {
                    out.push(format!(
                        "(command -v pyenv >/dev/null 2>&1 && pyenv global {} >/dev/null 2>&1) || true",
                        quote(&self.python)
                    ));
                }
                out.push(
                    "[ -x \"$E/venv/bin/python\" ] || python3 -m venv \"$E/venv\"; . \"$E/venv/bin/activate\""
                        .to_string(),
                );
            }
            Kind::Node => {
                out.push(
                    "export npm_config_cache=\"$E/npm\" npm_config_store_dir=\"$E/pnpm-store\" YARN_CACHE_FOLDER=\"$E/yarn\""
                        .to_string(),
                );
                if !self.node.is_empty() {
                    out.push(format!(
                        "n {} >/dev/null 2>&1 || true; hash -r",
                        quote(&self.node)
                    ));
                }
                // The package manager CI sets up with an action (no `run`
                // line installs it), at the version it pins, before any
                // install line calls it.
                let all = format!("{} {}", self.install.join(" "), self.test.join(" "));
                for (tool, pinned) in [("pnpm", &self.pnpm), ("yarn", &String::new())] {
                    if !all.split_whitespace().any(|w| w == tool) {
                        continue;
                    }
                    let at = if pinned.is_empty() {
                        String::new()
                    } else {
                        format!("@{pinned}")
                    };
                    out.push(format!(
                        "command -v {tool} >/dev/null 2>&1 || corepack enable {tool} >/dev/null 2>&1 || npm install -g {} >/dev/null 2>&1 || true; hash -r",
                        quote(&format!("{tool}{at}"))
                    ));
                }
            }
            Kind::Other => {}
        }
        out.join("\n")
    }

    /// `command` run inside the environment. The script restores the cached
    /// environment when `ENV_CACHE_GET_URL` is set and fetches, installs
    /// otherwise (or after a Node restore), saves a fresh install when
    /// `ENV_CACHE_PUT_URL` is set, then runs `command` and exits with its
    /// code.
    pub fn wrap(&self, command: &str) -> String {
        self.wrap_with(command, self.restore_is_complete())
    }

    /// [`Self::wrap`], saying whether a restored environment is complete.
    pub fn wrap_with(&self, command: &str, complete: bool) -> String {
        let install = if self.install.is_empty() {
            "true".to_string()
        } else {
            self.install.join(" && ")
        };
        let cd = if self.dir.is_empty() {
            String::new()
        } else {
            format!("cd {} || exit 1\n", quote(&self.dir))
        };
        let complete = if complete { "1" } else { "" };
        format!(
            r#"export E="${{CODERHELM_ENV:-{ENV_ROOT}}}"; mkdir -p "$E"
ROOT="$PWD"
RESTORED=""
if [ -n "$ENV_CACHE_GET_URL" ] && curl -fsS --retry 2 --noproxy "*" -o /tmp/coderhelm-env.tgz "$ENV_CACHE_GET_URL" 2>/dev/null && tar xzf /tmp/coderhelm-env.tgz -C "$E"; then
  RESTORED=1; echo "{RESTORED}"
fi
{activate}
{cd}if [ -z "$RESTORED" ] || [ -z "{complete}" ]; then
  ( {install} ) >/tmp/coderhelm-install.log 2>&1; ICODE=$?; tail -5 /tmp/coderhelm-install.log
  if [ $ICODE -ne 0 ]; then echo "{INSTALL_FAILED} $ICODE"; tail -c 1500 /tmp/coderhelm-install.log; fi
  if [ $ICODE -eq 0 ] && [ -z "$RESTORED" ]; then
    echo "{INSTALLED}"
    if [ -n "$ENV_CACHE_PUT_URL" ] && tar czf /tmp/coderhelm-env-out.tgz -C "$E" . && curl -fsS --retry 2 --noproxy "*" -T /tmp/coderhelm-env-out.tgz "$ENV_CACHE_PUT_URL"; then echo "{SAVED}"; fi
  fi
fi
cd "$ROOT"
bash -c {cmd}"#,
            activate = self.activate(),
            cmd = quote(command),
        )
    }

    /// The command that runs one test file (`file`, from the repository
    /// root) the way CI runs its tests, or `None` when no test step can be
    /// narrowed to it. CI may run a runner (`pytest`, `python -m
    /// unittest`), a loop over test files (`for t in tests/test_*.py; do
    /// python "$t"`), or the file by name. A data-pipeline repository ran
    /// its tests with that loop, no runner CoderHelm knew, so a bug fix's
    /// new test was never run on the base and the pull request said
    /// nothing about it.
    pub fn file_command(&self, file: &str) -> Option<String> {
        let safe = !file.is_empty()
            && file
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '/'))
            && !file.contains("..");
        if !safe {
            return None;
        }
        let steps: Vec<(String, String)> = self.test.iter().map(|l| unwrap_cd(l)).collect();
        let in_dir = |dir: &str, cmd: String| -> Option<String> {
            if dir.is_empty() {
                return Some(cmd);
            }
            let rel = file.strip_prefix(&format!("{}/", dir.trim_end_matches('/')))?;
            Some(format!("(cd {dir} && {})", cmd.replace(file, rel)))
        };
        // A runner that takes file paths (`narrow`).
        // The wrapper runs the command from the repository root: a job's
        // working directory and a step's folder are both cd'd into.
        let job = self.dir.trim_end_matches('/');
        for (dir, line) in &steps {
            let at = join(job, dir);
            let rel = if at.is_empty() {
                Some(file)
            } else {
                file.strip_prefix(&format!("{}/", at.trim_end_matches('/')))
            };
            if let Some(c) = rel.and_then(|r| narrow(line, r)) {
                return Some(in_folder(&at, &c));
            }
        }
        // A loop over test files, its body running each one.
        for (i, (dir, line)) in steps.iter().enumerate() {
            let Some((var, globs)) = loop_head(line) else {
                continue;
            };
            let rel = if dir.is_empty() {
                file
            } else {
                match file.strip_prefix(&format!("{dir}/")) {
                    Some(r) => r,
                    None => continue,
                }
            };
            if !globs.iter().any(|g| {
                crate::skill_apply::glob_match(g, rel) && g.contains('/') == rel.contains('/')
            }) {
                continue;
            }
            // The body: the rest of this line after `do`, then the lines
            // that follow it.
            let after_do = line.split_once("; do").map(|(_, b)| b.to_string());
            let body = after_do
                .into_iter()
                .chain(steps.iter().skip(i + 1).map(|(_, l)| l.clone()));
            for b in body {
                if let Some(runner) = runs_var(&b, &var) {
                    return in_dir(dir, format!("{runner} {file}"));
                }
                if b.trim_start().starts_with("done") {
                    break;
                }
            }
        }
        // The file by name.
        for (dir, line) in &steps {
            let words: Vec<&str> = line.split_whitespace().collect();
            if words.len() == 2
                && SCRIPT_RUNNERS.contains(&words[0])
                && (format!("{dir}/{}", words[1]).trim_start_matches('/') == file
                    || words[1] == file)
            {
                return in_dir(dir, line.replace(words[1], file));
            }
        }
        None
    }

    /// What the coder is told: the environment is ready in every command,
    /// and how the tests run.
    pub fn coder_note(&self) -> String {
        format!(
            "\n\n## The test environment (from CI: {})\nEvery sandbox command you run starts with this repository's test environment installed (CoderHelm installs it once per run and reuses it in every round). Don't reinstall dependencies: run the tests directly{}:\n{}\n",
            self.source,
            if self.dir.is_empty() {
                String::new()
            } else {
                format!(" from `{}`", self.dir)
            },
            self.test
                .iter()
                .map(|t| format!("- `{t}`"))
                .collect::<Vec<_>>()
                .join("\n")
        )
    }

    /// One line for the run's notes.
    pub fn summary(&self) -> String {
        format!(
            "Test environment from {}: install `{}`; tests `{}`",
            self.source,
            if self.install.is_empty() {
                "nothing".to_string()
            } else {
                self.install.join(" && ")
            },
            self.test.join(" && ")
        )
    }
}

/// The S3 key of a cached environment: one per team, repository and
/// fingerprint, so later rounds and runs of the same install reuse it.
pub fn cache_key(team: &str, repo: &str, fingerprint: &str) -> String {
    let safe = |x: &str| -> String {
        x.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    };
    format!("env-cache/{}/{}/{fingerprint}.tgz", safe(team), safe(repo))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PY: &str = r#"
name: Tests
on: [pull_request]
env:
  PYTHONHASHSEED: "0"
  GH_TOKEN: ${{ secrets.X }}
jobs:
  tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: |
          pip install 'apache-airflow[postgres]==2.10.3' -c dags/constraints-3.11.txt
          pip install -r requirement.txt
      - run: for t in tests/test_*.py; do python "$t" || exit 1; done
        name: Run tests
      - run: python -m pytest tests
"#;

    #[test]
    fn the_test_job_is_read_from_ci() {
        let tree = vec![
            "requirement.txt".to_string(),
            "dags/constraints-3.11.txt".to_string(),
        ];
        let e =
            from_workflows(&[(".github/workflows/tests.yml".into(), PY.into())], &tree).unwrap();
        assert_eq!(e.kind, Kind::Python);
        assert_eq!(e.python, "3.11");
        assert_eq!(e.install.len(), 2);
        assert!(e.test.iter().any(|t| t.contains("pytest")));
        assert_eq!(e.env, vec![("PYTHONHASHSEED".to_string(), "0".to_string())]);
        assert_eq!(
            e.manifests,
            vec!["dags/constraints-3.11.txt", "requirement.txt"]
        );
        // A changed requirements file is a new install.
        let a = e.fingerprint(&[("requirement.txt".into(), "aaa".into())]);
        let b = e.fingerprint(&[("requirement.txt".into(), "bbb".into())]);
        assert_ne!(a, b);
        assert_eq!(
            a,
            e.fingerprint(&[("requirement.txt".into(), "aaa".into())])
        );
        assert!(e.coder_note().contains("Don't reinstall"));
        assert!(cache_key("TEAM#x", "o/r", &a).starts_with("env-cache/TEAM_x/o_r/"));
    }

    #[test]
    fn a_node_job_with_a_working_directory() {
        let wf = "jobs:\n  web:\n    defaults:\n      run:\n        working-directory: ./apps/web\n    steps:\n      - uses: actions/setup-node@v4\n        with:\n          node-version: 20\n      - run: corepack enable && pnpm install --frozen-lockfile\n      - run: pnpm vitest run\n";
        let e = from_workflows(
            &[("ci.yml".into(), wf.into())],
            &["apps/web/pnpm-lock.yaml".to_string()],
        )
        .unwrap();
        assert_eq!(e.kind, Kind::Node);
        assert_eq!(e.dir, "apps/web");
        assert_eq!(e.node, "20");
        assert_eq!(e.manifests, vec!["apps/web/pnpm-lock.yaml"]);
        assert!(!e.restore_is_complete());
        assert!(e.wrap("pnpm vitest run").contains("cd 'apps/web'"));
        // No job with a test step: nothing.
        assert!(from_workflows(
            &[(
                "d.yml".into(),
                "jobs:\n  d:\n    steps:\n      - run: echo hi\n".into()
            )],
            &[]
        )
        .is_none());
    }

    /// A data-pipeline repository's CI: a loop runs each test file with
    /// `python`, no runner CoderHelm knew.
    const LOOP: &str = r#"
name: Tests
on: [pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: uv pip install --quiet "apache-airflow[postgres]==2.10.3" -r requirement.txt
      - name: Run tests
        env:
          PYTHONHASHSEED: "0"
        run: |
          failed=0
          for t in tests/test_*.py; do
            echo "::group::$t"
            python "$t" || failed=1
            echo "::endgroup::"
          done
          exit $failed
"#;

    #[test]
    fn one_test_file_runs_the_way_ci_runs_its_tests() {
        let e = from_workflows(&[("t.yml".into(), LOOP.into())], &[]).unwrap();
        assert!(
            e.test.iter().any(|t| t.contains("python \"$t\"")),
            "{:?}",
            e.test
        );
        assert_eq!(
            e.file_command("tests/test_camp_sport.py").as_deref(),
            Some("python tests/test_camp_sport.py")
        );
        // A file the loop doesn't run, and a path that isn't safe.
        assert_eq!(e.file_command("dags/camp_sport.py"), None);
        assert_eq!(e.file_command("tests/test_a.py; rm -rf /"), None);
        // A runner takes the file.
        let e = from_workflows(&[("t.yml".into(), PY.into())], &[]).unwrap();
        assert_eq!(
            e.file_command("tests/test_a.py").as_deref(),
            Some("python -m pytest tests/test_a.py")
        );
        // A one-line loop, in a folder.
        let mut e = TestEnv {
            test: vec![
                "(cd etl && for t in tests/test_*.py; do python3 \"$t\" || exit 1; done)".into(),
            ],
            ..Default::default()
        };
        assert_eq!(
            e.file_command("etl/tests/test_a.py").as_deref(),
            Some("(cd etl && python3 tests/test_a.py)")
        );
        assert_eq!(e.file_command("tests/test_a.py"), None);
        e.test = vec!["npm run build".into()];
        assert_eq!(e.file_command("tests/test_a.py"), None);
    }

    #[test]
    fn every_runner_is_narrowed_to_one_file_in_one_place() {
        assert_eq!(
            narrow("python -m pytest tests -q", "tests/test_a.py").as_deref(),
            Some("python -m pytest tests/test_a.py")
        );
        assert_eq!(
            narrow("python -m unittest discover", "tests/test_a.py").as_deref(),
            Some("python -m unittest tests/test_a.py")
        );
        assert_eq!(
            narrow("pnpm exec vitest run --coverage", "src/a.test.ts").as_deref(),
            Some("pnpm exec vitest run src/a.test.ts")
        );
        assert_eq!(
            narrow("npx --no-install vitest", "src/a.test.ts").as_deref(),
            Some("npx --no-install vitest run src/a.test.ts")
        );
        assert_eq!(
            narrow("npx jest --ci", "src/a.test.ts").as_deref(),
            Some("npx jest src/a.test.ts")
        );
        assert_eq!(
            narrow("go test ./... -race", "pkg/cart/total_test.go").as_deref(),
            Some("go test ./pkg/cart/")
        );
        assert_eq!(
            narrow("cargo test --workspace", "tests/cart.rs").as_deref(),
            Some("cargo test --test cart")
        );
        // Never across a chain, never a file it can't take.
        assert_eq!(
            narrow("tsc --noEmit && vitest run", "a.test.ts").as_deref(),
            Some("vitest run a.test.ts")
        );
        assert_eq!(narrow("cargo test", "src/lib.rs"), None);
        assert_eq!(narrow("npm run build", "a.test.ts"), None);
        assert_eq!(narrow("pytest", "a; rm -rf /"), None);
    }

    #[test]
    fn package_scripts_and_make_targets_are_read_to_their_runner() {
        let tree: Vec<String> = [
            "package.json",
            "apps/web/package.json",
            "apps/web/src/a.test.ts",
            "Makefile",
            "tests/test_a.py",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let read = |p: &str| -> Option<String> {
            match p {
                "package.json" => {
                    Some(r#"{"name":"root","scripts":{"test":"turbo run test"}}"#.into())
                }
                "apps/web/package.json" => Some(
                    r#"{"name":"web","scripts":{"test":"vitest","test:run":"vitest run"}}"#.into(),
                ),
                "Makefile" => Some(
                    "lint:\n\truff .\ntest: deps\n\t@echo testing\n\tpython -m pytest -q tests\n"
                        .into(),
                ),
                _ => None,
            }
        };
        assert_eq!(
            resolve_script("pnpm --filter web test", "", &tree, &read),
            Some(("apps/web".into(), "vitest".into()))
        );
        assert_eq!(
            resolve_script("pnpm -F web run test:run", "", &tree, &read),
            Some(("apps/web".into(), "vitest run".into()))
        );
        assert_eq!(
            resolve_script("make test", "", &tree, &read),
            Some((String::new(), "python -m pytest -q tests".into()))
        );
        assert_eq!(resolve_script("pnpm install", "", &tree, &read), None);
        // A CI step that calls a script is narrowed through it.
        let mut e = TestEnv {
            test: vec!["pnpm --filter web test".into(), "make test".into()],
            ..Default::default()
        };
        e.resolve_scripts(&tree, &read);
        assert_eq!(
            e.file_command("apps/web/src/a.test.ts").as_deref(),
            Some("(cd apps/web && npx --no-install vitest run src/a.test.ts)")
        );
        assert_eq!(
            e.file_command("tests/test_a.py").as_deref(),
            Some("python -m pytest tests/test_a.py")
        );
        // CI runs every package through turbo: the file's own package's
        // script runs it.
        assert_eq!(
            package_test_command("apps/web/src/a.test.ts", &tree, &read).as_deref(),
            Some("(cd apps/web && npx --no-install vitest run src/a.test.ts)")
        );
        let wanted = script_files(Some(&e), &tree, &["apps/web/src/a.test.ts".into()]);
        assert!(wanted.contains(&"apps/web/package.json".to_string()));
        assert!(wanted.contains(&"Makefile".to_string()));
    }

    #[test]
    fn a_failed_install_and_the_pinned_manager_are_in_the_script() {
        let e = TestEnv {
            kind: Kind::Node,
            pnpm: "10.28.2".into(),
            install: vec!["pnpm install --frozen-lockfile".into()],
            test: vec!["pnpm test".into()],
            ..Default::default()
        };
        let w = e.wrap("true");
        assert!(w.contains(INSTALL_FAILED));
        assert!(w.contains("npm install -g 'pnpm@10.28.2'"));
        assert!(w.find("corepack enable pnpm").unwrap() < w.find("pnpm install --frozen").unwrap());
        let wf = "jobs:\n  t:\n    steps:\n      - uses: pnpm/action-setup@v4\n        with:\n          version: 10.28.2\n      - run: pnpm install --frozen-lockfile\n      - run: pnpm --filter web test\n";
        let e = from_workflows(&[("ci.yml".into(), wf.into())], &[]).unwrap();
        assert_eq!(e.pnpm, "10.28.2");
    }

    /// The second round restores the first round's install instead of
    /// installing again. Runs the real script with bash, tar and curl (file
    /// URLs stand in for the presigned ones).
    #[test]
    fn a_second_round_restores_instead_of_reinstalling() {
        for tool in ["bash", "tar", "curl"] {
            if std::process::Command::new(tool)
                .arg("--version")
                .output()
                .is_err()
            {
                return;
            }
        }
        let dir = std::env::temp_dir().join(format!("coderhelm-env-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("repo")).unwrap();
        let log = dir.join("installs.log");
        let cache = dir.join("cache.tgz");
        let env = TestEnv {
            source: "ci.yml (tests)".into(),
            kind: Kind::Other,
            install: vec![format!(
                "echo install >> {} && echo ready > \"$E/marker\"",
                log.display()
            )],
            test: vec!["true".into()],
            ..Default::default()
        };
        // A complete restore (as for Python), without needing Python here.
        let script = env.wrap_with("cat \"$E/marker\"", true);
        let run = |get: Option<&std::path::Path>, put: Option<&std::path::Path>, env_root: &str| {
            let mut c = std::process::Command::new("bash");
            c.arg("-c")
                .arg(&script)
                .current_dir(dir.join("repo"))
                .env("CODERHELM_ENV", dir.join(env_root));
            match get {
                Some(p) => c.env("ENV_CACHE_GET_URL", format!("file://{}", p.display())),
                None => c.env_remove("ENV_CACHE_GET_URL"),
            };
            match put {
                Some(p) => c.env("ENV_CACHE_PUT_URL", format!("file://{}", p.display())),
                None => c.env_remove("ENV_CACHE_PUT_URL"),
            };
            let o = c.output().unwrap();
            String::from_utf8_lossy(&o.stdout).to_string()
        };
        // Round one: nothing cached, so it installs and saves.
        let first = run(None, Some(&cache), "env1");
        assert!(first.contains(INSTALLED), "{first}");
        assert!(first.contains(SAVED), "{first}");
        assert!(first.contains("ready"), "{first}");
        // Round two, a fresh build: restored, not installed again.
        let second = run(Some(&cache), None, "env2");
        assert!(second.contains(RESTORED), "{second}");
        assert!(!second.contains(INSTALLED), "{second}");
        assert!(second.contains("ready"), "{second}");
        let installs = std::fs::read_to_string(&log).unwrap();
        assert_eq!(installs.lines().count(), 1, "installed {installs}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
