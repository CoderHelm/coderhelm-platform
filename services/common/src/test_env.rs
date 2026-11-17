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

    /// The install's name: its steps, versions and the manifests' blob ids
    /// (`shas`: `(path, blob sha)` from the tree at the head).
    pub fn fingerprint(&self, shas: &[(String, String)]) -> String {
        let mut h = Sha256::new();
        h.update(self.dir.as_bytes());
        h.update(self.python.as_bytes());
        h.update(self.node.as_bytes());
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
        // A runner that takes file paths.
        for (dir, line) in &steps {
            let low = line.to_ascii_lowercase();
            for runner in ["pytest", "-m unittest"] {
                if let Some(i) = low.find(runner) {
                    let prefix = line[..i + runner.len()].trim();
                    if let Some(c) = in_dir(dir, format!("{prefix} {file}")) {
                        return Some(c);
                    }
                }
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
