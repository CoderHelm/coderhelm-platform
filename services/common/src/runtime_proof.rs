//! Proof that a repository still builds and starts after its dependencies
//! change.
//!
//! A dependency run of 64 packages finished with its checks passed, four of
//! its bumps crossing a major version through overrides. The sandbox had run
//! the install, the type check, the linter and the unit tests. The
//! application's build never ran and the application was never started, so
//! nothing showed that the parents of the overridden packages work on the new
//! major.
//!
//! The rules here:
//!
//! - [`discover`] finds what to run. The build is the build command the
//!   repository's own CI runs (a workflow a pull request triggers first, any
//!   workflow otherwise), else the manifest's build script, else the
//!   toolchain's own build ([`MANAGERS`], one row per package manager). The
//!   start command is the one the repository declares: a `Procfile` web
//!   entry, else the manifest's `start` script. Nothing is guessed: what
//!   isn't found is said, with why.
//! - [`script`] is the one sandbox command: install from the lockfile, each
//!   build command on its own, then the start command in the background, a
//!   wait for it to answer on a port, and a request for `/` and the common
//!   health paths. Each part reports on its own line ([`parse`]), so a
//!   failing part never hides another.
//! - The same script runs on the base branch first. [`blamed`] names only
//!   what passes on the base branch and fails with the bumps: a build that
//!   needs a secret the sandbox doesn't have fails on both and is never
//!   blamed on a bump.
//! - [`report`] writes what ran and what it came to. A part that did not run
//!   says so with the reason; nothing is claimed that did not run.
//!
//! - Every project the bumps can break is built, not only the one with the
//!   most bumps. In a workspace, [`add_projects`] adds the build of each
//!   project whose dependencies bring in a changed package line (read from
//!   the lockfile, `reachability::lock_projects`), whether or not any CI job
//!   builds it; [`add_lockfile`] adds the project of every other lockfile
//!   the run bumped. A project that is not built, or whose build fails on
//!   the base branch too, is named as not proven with the reason
//!   ([`unproven_projects`]); a project build that passes on the base
//!   branch and fails with the bumps is blamed like any other.
//!
//! Everything here is pure; the sandbox run is the worker's
//! (`worker::passes::dep_fix`).

use serde::{Deserialize, Serialize};

use crate::e2e::test_package::{ensure_manager, Manager as Js};

/// The line the script ends with.
pub const MARK: &str = "CODERHELM_PROOF";
/// One build command's exit code: `CODERHELM_PROOF_STEP build <n> <exit>`.
pub const STEP_MARK: &str = "CODERHELM_PROOF_STEP";
/// One requested path's status: `CODERHELM_PROOF_PAGE <path> <status>`.
pub const PAGE_MARK: &str = "CODERHELM_PROOF_PAGE";
/// The port the start command is given as `PORT`.
pub const PORT: u16 = 38411;
/// Ports asked when the application ignores `PORT` and the sandbox image
/// can't list listening sockets: the defaults of common servers.
pub const COMMON_PORTS: &[u16] = &[3000, 8080, 8000, 5000, 4173, 5173, 4321, 4200];
/// How long the start command gets to answer, in two-second polls.
pub const START_POLLS: u32 = 45;
/// Build commands one proof runs for the main project.
pub const MAX_BUILDS: usize = 3;
/// Builds of other affected projects one proof runs beside them: the whole
/// proof is one sandbox run inside one step's time.
pub const MAX_PROJECT_BUILDS: usize = 5;
/// The paths requested once the application answers: its root page and the
/// health endpoints servers commonly have.
pub const PATHS: &[&str] = &["/", "/healthz", "/health", "/api/health"];

/// One package manager: how a project of it installs, runs a manifest
/// script and builds. Adding a manager is one row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManagerRow {
    pub name: &'static str,
    pub lockfiles: &'static [&'static str],
    /// Installs exactly what the lockfile says.
    pub install: &'static str,
    /// The JavaScript manager made available first (corepack), when one.
    pub js: Option<Js>,
    /// The manifest whose scripts name a build and a start (`package.json`).
    pub scripts_in: Option<&'static str>,
    /// Runs a manifest script; the script's name follows.
    pub run_script: Option<&'static str>,
    /// The toolchain's own build, when CI and the manifest name none.
    pub build: Option<&'static str>,
}

const fn row(
    name: &'static str,
    lockfiles: &'static [&'static str],
    install: &'static str,
    js: Option<Js>,
    build: Option<&'static str>,
) -> ManagerRow {
    ManagerRow {
        name,
        lockfiles,
        install,
        js,
        scripts_in: None,
        run_script: None,
        build,
    }
}

const fn js_row(
    name: &'static str,
    lockfiles: &'static [&'static str],
    install: &'static str,
    js: Js,
    run_script: &'static str,
) -> ManagerRow {
    ManagerRow {
        name,
        lockfiles,
        install,
        js: Some(js),
        scripts_in: Some("package.json"),
        run_script: Some(run_script),
        build: None,
    }
}

/// The package managers CoderHelm writes lockfiles for
/// (`lockfile_sync::lock_manager` names the same ones).
#[rustfmt::skip]
pub const MANAGERS: &[ManagerRow] = &[
    js_row("npm", &["package-lock.json", "npm-shrinkwrap.json"], "npm ci --no-audit --no-fund", Js::Npm, "npm run"),
    js_row("pnpm", &["pnpm-lock.yaml"], "pnpm install --frozen-lockfile", Js::Pnpm, "pnpm run"),
    js_row("yarn", &["yarn.lock"], "yarn install --frozen-lockfile", Js::Yarn, "yarn run"),
    js_row("bun", &["bun.lock", "bun.lockb"], "bun install --frozen-lockfile", Js::Bun, "bun run"),
    row("cargo", &["Cargo.lock"], "cargo fetch --locked", None, Some("cargo build --locked")),
    row("poetry", &["poetry.lock"], "(command -v poetry >/dev/null 2>&1 || pip install -q poetry) && poetry install --no-interaction", None, Some("python -m compileall -q .")),
    row("uv", &["uv.lock"], "(command -v uv >/dev/null 2>&1 || pip install -q uv) && uv sync --frozen", None, Some("python -m compileall -q .")),
    row("pipenv", &["Pipfile.lock"], "(command -v pipenv >/dev/null 2>&1 || pip install -q pipenv) && pipenv sync", None, Some("python -m compileall -q .")),
    row("bundler", &["Gemfile.lock"], "bundle install", None, None),
    row("go", &["go.sum"], "go mod download", None, Some("go build ./...")),
];

/// The manager of a lockfile.
pub fn manager_of(lockfile: &str) -> Option<&'static ManagerRow> {
    let name = lockfile.rsplit('/').next().unwrap_or(lockfile);
    MANAGERS.iter().find(|m| m.lockfiles.contains(&name))
}

/// A build command a CI workflow runs.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CiBuild {
    pub cmd: String,
    /// The directory the workflow runs it in, when it sets one.
    pub dir: Option<String>,
    pub workflow: String,
    pub job: String,
    /// A pull request triggers the workflow.
    pub on_pull_request: bool,
}

/// One command of the proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ProofCmd {
    pub cmd: String,
    /// Where it runs, from the repository root (`""`: the root).
    pub dir: String,
    /// Where the command was found, in words (`` `ci.yml`, job `build` ``).
    pub source: String,
    /// The build of another affected project: it is judged on its own and
    /// never keeps the main project's start from running.
    pub extra: bool,
}

/// What a proof runs, found once per run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ProofPlan {
    /// The project the bumps are in (the lockfile's folder).
    pub dir: String,
    pub install: String,
    pub builds: Vec<ProofCmd>,
    pub start: Option<ProofCmd>,
    /// Why no build command was found; empty when one was.
    pub no_build: String,
    /// Why no start command was found; empty when one was.
    pub no_start: String,
    /// Other lockfiles the run bumped packages in, whose projects this
    /// proof does not build.
    pub not_covered: Vec<String>,
    /// The other projects the bumps can break: each workspace project that
    /// takes a changed package line, and the project of each other lockfile
    /// with bumps. Each is built, or named as not proven with why.
    pub projects: Vec<ProjectProof>,
    /// Why the workspace projects that take the changed packages could not
    /// be worked out; empty when they were (or the project is no workspace).
    pub projects_unknown: String,
}

/// One project the bumps can break, beside the main one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ProjectProof {
    /// Its folder from the repository root.
    pub dir: String,
    /// Why it is affected, in words (`takes glob-walk@2.3.1`).
    pub because: String,
    /// Its builds, as indices into the plan's `builds`.
    pub builds: Vec<usize>,
    /// Why it is not built here; empty when it is.
    pub not_built: String,
}

/// A workspace project whose dependencies bring in a changed package line.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AffectedProject {
    /// Its folder from the repository root.
    pub dir: String,
    /// The changed packages it installs, as `name@version`.
    pub uses: Vec<String>,
    /// Its scripts manifest (`package.json`), when it has one.
    pub manifest: Option<String>,
}

fn because(uses: &[String]) -> String {
    let shown: Vec<String> = uses.iter().take(4).map(|u| format!("`{u}`")).collect();
    format!(
        "takes {}{}",
        shown.join(", "),
        if uses.len() > shown.len() {
            format!(" and {} more changed packages", uses.len() - shown.len())
        } else {
            String::new()
        }
    )
}

fn project_builds(plan: &ProofPlan) -> usize {
    plan.projects.iter().map(|p| p.builds.len()).sum()
}

/// Adds to `plan` (the proof of `lockfile`'s project) every workspace
/// project the bumps can break: a project a build of the plan already runs
/// in is covered by it; one whose manifest declares a `build` script gets
/// that build, whether or not CI builds it; one that declares none is named
/// as not built. The lockfile's own folder is the plan's main project.
pub fn add_projects(plan: &mut ProofPlan, lockfile: &str, projects: &[AffectedProject]) {
    let Some(m) = manager_of(lockfile) else {
        return;
    };
    for p in projects {
        if p.dir == plan.dir || plan.projects.iter().any(|x| x.dir == p.dir) {
            continue;
        }
        let mut proof = ProjectProof {
            dir: p.dir.clone(),
            because: because(&p.uses),
            ..Default::default()
        };
        let covered: Vec<usize> = plan
            .builds
            .iter()
            .enumerate()
            .filter(|(_, b)| b.dir == p.dir)
            .map(|(i, _)| i)
            .collect();
        let scripted = p
            .manifest
            .as_deref()
            .and_then(|t| script_of(t, "build"))
            .is_some();
        if !covered.is_empty() {
            proof.builds = covered;
        } else if !is_plain_path(&p.dir) {
            proof.not_built = "its folder can't be named in a sandbox command".into();
        } else if let (true, Some(run), Some(name)) = (scripted, m.run_script, m.scripts_in) {
            if project_builds(plan) >= MAX_PROJECT_BUILDS {
                proof.not_built = format!(
                    "one proof builds at most {MAX_PROJECT_BUILDS} projects beside the main one"
                );
            } else {
                plan.builds.push(ProofCmd {
                    cmd: format!("{run} build"),
                    dir: p.dir.clone(),
                    source: format!("the `build` script of `{}`", under(&p.dir, name)),
                    extra: true,
                });
                proof.builds = vec![plan.builds.len() - 1];
            }
        } else {
            proof.not_built = match m.scripts_in {
                Some(name) => format!("`{}` declares no `build` script", under(&p.dir, name)),
                None => format!("{} has no build per project", m.name),
            };
        }
        plan.projects.push(proof);
    }
}

/// Adds to `plan` the project of another lockfile the run bumped packages
/// in, as `other` (its own [`discover`]) found it: each of its builds runs
/// after that project's own install. A project with no build is named as
/// not built, with why.
pub fn add_lockfile(plan: &mut ProofPlan, lockfile: &str, bumps: usize, other: &ProofPlan) {
    let mut proof = ProjectProof {
        dir: other.dir.clone(),
        because: format!("has its own lockfile `{lockfile}` with {bumps} of the bumps"),
        ..Default::default()
    };
    if other.builds.is_empty() {
        proof.not_built = other.no_build.clone();
    } else if project_builds(plan) + other.builds.len() > MAX_PROJECT_BUILDS {
        proof.not_built =
            format!("one proof builds at most {MAX_PROJECT_BUILDS} projects beside the main one");
    } else {
        for b in &other.builds {
            // The project's own install first, in the lockfile's folder.
            let install = if b.dir == other.dir {
                other.install.clone()
            } else {
                format!("( cd \"$CH_ROOT\" && {}{} )", cd(&other.dir), other.install)
            };
            plan.builds.push(ProofCmd {
                cmd: format!("{install} && {}", b.cmd),
                dir: b.dir.clone(),
                source: format!("{}, after the install of `{lockfile}`", b.source),
                extra: true,
            });
            proof.builds.push(plan.builds.len() - 1);
        }
    }
    plan.projects.push(proof);
}

/// The workspace folders a root `package.json` names (`workspaces`, as a
/// list or under `packages`), among the folders of `tree` that hold a
/// `package.json`. `root`: the folder of the root manifest. Folders are
/// returned from `root`. `*` matches one folder name, `**` any depth.
pub fn workspace_dirs(root_manifest: &str, tree: &[String], root: &str) -> Vec<String> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(root_manifest) else {
        return vec![];
    };
    let ws = &v["workspaces"];
    let globs: Vec<&str> = ws
        .as_array()
        .or_else(|| ws["packages"].as_array())
        .map(|a| a.iter().filter_map(|g| g.as_str()).collect())
        .unwrap_or_default();
    fn matches(glob: &[&str], dir: &[&str]) -> bool {
        match (glob.first(), dir.first()) {
            (None, None) => true,
            (Some(&"**"), _) => {
                matches(&glob[1..], dir) || (!dir.is_empty() && matches(glob, &dir[1..]))
            }
            (Some(g), Some(d)) => {
                let one = match g.split_once('*') {
                    Some((pre, post)) => {
                        d.len() >= pre.len() + post.len() && d.starts_with(pre) && d.ends_with(post)
                    }
                    None => g == d,
                };
                one && matches(&glob[1..], &dir[1..])
            }
            _ => false,
        }
    }
    let prefix = if root.is_empty() {
        String::new()
    } else {
        format!("{root}/")
    };
    let mut out: Vec<String> = tree
        .iter()
        .filter_map(|p| p.strip_prefix(prefix.as_str()))
        .filter_map(|p| p.strip_suffix("/package.json"))
        .filter(|d| !d.split('/').any(|s| s == "node_modules"))
        .filter(|d| {
            let segs: Vec<&str> = d.split('/').collect();
            globs.iter().any(|g| {
                let g = g.trim_start_matches("./").trim_end_matches('/');
                !g.starts_with('!') && matches(&g.split('/').collect::<Vec<_>>(), &segs)
            })
        })
        .map(String::from)
        .collect();
    out.sort();
    out.dedup();
    out
}

/// The projects the proof does not prove, each with why: one that is not
/// built, one whose build gave no result, and one whose build fails on the
/// base branch too (the sandbox has no secrets or services, so that says
/// nothing about the bumps). A project whose build passes with the bumps,
/// or fails only with them, is not here: the first is proven, the second
/// is blamed ([`blamed`]).
pub fn unproven_projects(
    plan: &ProofPlan,
    base: Option<&ProofResult>,
    head: Option<&ProofResult>,
) -> Vec<(String, String)> {
    let code = |r: Option<&ProofResult>, i: usize| -> Option<i32> {
        r.filter(|r| r.ran)
            .and_then(|r| r.builds.get(i).copied().flatten())
    };
    let mut out = vec![];
    for p in &plan.projects {
        if !p.not_built.is_empty() {
            out.push((p.dir.clone(), p.not_built.clone()));
            continue;
        }
        let why = p.builds.iter().find_map(|&i| {
            let cmd = plan.builds.get(i).map(|b| b.cmd.as_str()).unwrap_or("");
            match (code(base, i), code(head, i)) {
                (_, Some(0)) => None,
                (Some(0), Some(_)) => None,
                (Some(b), Some(_)) => Some(format!(
                    "its build `{cmd}` fails on the base branch too in the sandbox (exit {b}),                      which has no secrets or services, so the result says nothing about the bumps"
                )),
                (None, Some(c)) => Some(format!(
                    "its build `{cmd}` fails (exit {c}) and did not run on the base branch, so                      the bumps can't be told from what was already failing"
                )),
                (_, None) => Some(format!("its build `{cmd}` gave no result in the sandbox")),
            }
        });
        if let Some(why) = why {
            out.push((p.dir.clone(), why));
        }
    }
    out
}

impl ProofPlan {
    /// Nothing to run: no build and no start command was found.
    pub fn is_empty(&self) -> bool {
        self.builds.is_empty() && self.start.is_none()
    }
}

fn dir_of(path: &str) -> &str {
    path.rsplit_once('/').map(|(d, _)| d).unwrap_or("")
}

/// A relative path made only of characters safe to quote in a shell line.
pub fn is_plain_path(p: &str) -> bool {
    !p.starts_with('/')
        && !p.split('/').any(|seg| seg == "..")
        && p.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/' | '@'))
}

fn inside(dir: &str, project: &str) -> bool {
    project.is_empty() || dir == project || dir.starts_with(&format!("{project}/"))
}

fn file<'a>(files: &'a [(String, String)], path: &str) -> Option<&'a str> {
    files
        .iter()
        .find(|(p, _)| p == path)
        .map(|(_, t)| t.as_str())
}

fn under(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_string()
    } else {
        format!("{dir}/{name}")
    }
}

/// The files [`discover`] reads for a lockfile, from the repository root:
/// the project's `Procfile` and its scripts manifest.
pub fn files_to_read(lockfile: &str) -> Vec<String> {
    let dir = dir_of(lockfile);
    let mut v = vec![under(dir, "Procfile")];
    if let Some(m) = manager_of(lockfile).and_then(|m| m.scripts_in) {
        v.push(under(dir, m));
    }
    v
}

/// A manifest script by name (`package.json` `scripts`).
fn script_of(manifest: &str, name: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(manifest).ok()?;
    let s = v.get("scripts")?.get(name)?.as_str()?.trim();
    (!s.is_empty()).then(|| s.to_string())
}

/// The `web` entry of a `Procfile`.
pub fn procfile_web(text: &str) -> Option<String> {
    text.lines().find_map(|l| {
        let cmd = l.trim().strip_prefix("web:")?.trim();
        (!cmd.is_empty()).then(|| cmd.to_string())
    })
}

/// What a proof of the project of `lockfile` runs. `ci`: the build commands
/// the repository's workflows run; `files`: the files [`files_to_read`]
/// names that exist, with their text.
pub fn discover(lockfile: &str, ci: &[CiBuild], files: &[(String, String)]) -> ProofPlan {
    let dir = dir_of(lockfile).to_string();
    let mut plan = ProofPlan {
        dir: dir.clone(),
        ..Default::default()
    };
    let Some(m) = manager_of(lockfile) else {
        plan.no_build = format!("`{lockfile}` is not a lockfile CoderHelm builds from");
        plan.no_start = plan.no_build.clone();
        return plan;
    };
    if !is_plain_path(&dir) {
        plan.no_build = format!("the project folder `{dir}` can't be named in a sandbox command");
        plan.no_start = plan.no_build.clone();
        return plan;
    }
    plan.install = format!("{}{}", m.js.map(ensure_manager).unwrap_or(""), m.install);
    // The build CI runs: what gates a pull request first, else what any
    // workflow builds (a deploy workflow's build is still the real build).
    let usable = |c: &&CiBuild| {
        let d = c.dir.as_deref().unwrap_or("").trim_matches('/');
        // A command written with a workflow expression can't run in a shell.
        is_plain_path(d) && inside(d, &dir) && !c.cmd.trim().is_empty() && !c.cmd.contains("${{")
    };
    let mut from_ci: Vec<&CiBuild> = ci
        .iter()
        .filter(|c| c.on_pull_request)
        .filter(usable)
        .collect();
    if from_ci.is_empty() {
        from_ci = ci.iter().filter(usable).collect();
    }
    for c in from_ci {
        let d = c.dir.as_deref().unwrap_or("").trim_matches('/').to_string();
        let d = if d.is_empty() { dir.clone() } else { d };
        if plan.builds.iter().any(|b| b.cmd == c.cmd && b.dir == d) {
            continue;
        }
        plan.builds.push(ProofCmd {
            cmd: c.cmd.clone(),
            dir: d,
            source: if c.job.is_empty() {
                format!("the CI workflow `{}`", c.workflow)
            } else {
                format!("the CI workflow `{}`, job `{}`", c.workflow, c.job)
            },
            extra: false,
        });
        if plan.builds.len() >= MAX_BUILDS {
            break;
        }
    }
    let manifest = m.scripts_in.and_then(|n| file(files, &under(&dir, n)));
    if plan.builds.is_empty() {
        let scripted = match (m.run_script, m.scripts_in, manifest) {
            (Some(run), Some(name), Some(text)) => script_of(text, "build").map(|_| ProofCmd {
                cmd: format!("{run} build"),
                dir: dir.clone(),
                source: format!("the `build` script of `{}`", under(&dir, name)),
                extra: false,
            }),
            _ => None,
        };
        let own = m.build.map(|b| ProofCmd {
            cmd: b.to_string(),
            dir: dir.clone(),
            source: format!("{}'s own build", m.name),
            extra: false,
        });
        match scripted.or(own) {
            Some(b) => plan.builds.push(b),
            None => {
                plan.no_build = match m.scripts_in {
                    Some(name) => format!(
                        "no workflow runs a build command and `{}` has no `build` script",
                        under(&dir, name)
                    ),
                    None => format!(
                        "no workflow runs a build command and {} has no build of its own",
                        m.name
                    ),
                }
            }
        }
    }
    // The start command the repository declares, never a guessed one.
    let procfile = file(files, &under(&dir, "Procfile")).and_then(procfile_web);
    plan.start = match (procfile, m.run_script, m.scripts_in, manifest) {
        (Some(cmd), _, _, _) => Some(ProofCmd {
            cmd,
            dir: dir.clone(),
            source: format!("the `web` entry of `{}`", under(&dir, "Procfile")),
            extra: false,
        }),
        (None, Some(run), Some(name), Some(text)) => script_of(text, "start").map(|_| ProofCmd {
            cmd: format!("{run} start"),
            dir: dir.clone(),
            source: format!("the `start` script of `{}`", under(&dir, name)),
            extra: false,
        }),
        _ => None,
    };
    if plan.start.is_none() {
        plan.no_start = match m.scripts_in {
            Some(name) => format!(
                "the repository declares no start command (no `web` entry in `{}`, no `start` script in `{}`)",
                under(&dir, "Procfile"),
                under(&dir, name)
            ),
            None => format!(
                "the repository declares no start command (no `web` entry in `{}`)",
                under(&dir, "Procfile")
            ),
        };
    }
    plan
}

fn cd(dir: &str) -> String {
    if dir.is_empty() {
        String::new()
    } else {
        format!("cd '{dir}' && ")
    }
}

/// The one sandbox command of a proof (see the module notes). It always
/// exits 0; the verdict is read from its marker lines.
pub fn script(plan: &ProofPlan) -> String {
    let mut s = String::new();
    if plan.builds.iter().any(|b| b.cmd.contains("$CH_ROOT")) {
        s.push_str("export CH_ROOT=\"$(pwd)\"\n");
    }
    s.push_str(&format!(
        "( {}{} ) > /tmp/coderhelm-proof-install.log 2>&1; ch_install=$?\n",
        cd(&plan.dir),
        plan.install
    ));
    s.push_str("if [ $ch_install -ne 0 ]; then echo \"--- install failed (exit $ch_install), last output ---\"; tail -c 1200 /tmp/coderhelm-proof-install.log; echo; fi\n");
    s.push_str("ch_built=$ch_install\n");
    for (i, b) in plan.builds.iter().enumerate() {
        let n = i + 1;
        s.push_str(&format!(
            "if [ $ch_install -eq 0 ]; then ( {}{} ) > /tmp/coderhelm-proof-build-{n}.log 2>&1; ch_b{n}=$?; \
             if [ $ch_b{n} -ne 0 ]; then {gate}echo \"--- build {n} failed (exit $ch_b{n}), last output ---\"; tail -c 1200 /tmp/coderhelm-proof-build-{n}.log; echo; fi; \
             echo \"{STEP_MARK} build {n} $ch_b{n}\"; fi\n",
            cd(&b.dir),
            b.cmd,
            gate = if b.extra { "" } else { "ch_built=1; " },
        ));
    }
    s.push_str("ch_start=skipped; ch_port=0\n");
    if let Some(start) = &plan.start {
        let common: Vec<String> = COMMON_PORTS.iter().map(u16::to_string).collect();
        // Listening sockets, when the image can list them: the port the
        // application chose is found even when it ignores PORT. The egress
        // proxy's own port is never the application's.
        s.push_str("ch_ports() { if command -v ss >/dev/null 2>&1; then ss -ltnH 2>/dev/null | awk '{print $4}' | sed 's/.*://' | grep -E '^[0-9]+$' | grep -v '^3128$' | sort -un | tr '\\n' ' '; fi; }\n");
        s.push_str("ch_killtree() { for ch_c in $(pgrep -P \"$1\" 2>/dev/null); do ch_killtree \"$ch_c\"; done; kill \"$1\" 2>/dev/null; }\n");
        s.push_str("if [ $ch_built -eq 0 ]; then\n");
        s.push_str("  ch_before=\" $(ch_ports) \"\n");
        s.push_str(&format!(
            "  ( {}export PORT={PORT} && {} ) > /tmp/coderhelm-proof-start.log 2>&1 &\n",
            cd(&start.dir),
            start.cmd
        ));
        s.push_str("  ch_pid=$!; ch_start=silent\n");
        s.push_str(&format!("  for ch_i in $(seq 1 {START_POLLS}); do\n"));
        s.push_str("    if ! kill -0 $ch_pid 2>/dev/null; then wait $ch_pid; ch_start=\"exited:$?\"; break; fi\n");
        s.push_str(&format!(
            "    for ch_p in {PORT} $(ch_ports) {}; do\n",
            common.join(" ")
        ));
        s.push_str("      case \"$ch_before\" in *\" $ch_p \"*) continue;; esac\n");
        s.push_str("      ch_code=$(curl -s -o /dev/null -m 3 --noproxy '*' -w '%{http_code}' \"http://127.0.0.1:$ch_p/\" 2>/dev/null)\n");
        s.push_str("      if [ -n \"$ch_code\" ] && [ \"$ch_code\" != \"000\" ]; then ch_start=up; ch_port=$ch_p; break 2; fi\n");
        s.push_str("    done\n    sleep 2\n  done\n");
        s.push_str("  if [ \"$ch_start\" = up ]; then\n");
        s.push_str(&format!("    for ch_path in {}; do\n", PATHS.join(" ")));
        s.push_str("      ch_code=$(curl -s -o /dev/null -m 20 --noproxy '*' -w '%{http_code}' \"http://127.0.0.1:$ch_port$ch_path\" 2>/dev/null)\n");
        s.push_str(&format!(
            "      echo \"{PAGE_MARK} $ch_path ${{ch_code:-000}}\"\n    done\n"
        ));
        s.push_str("  else\n    echo \"--- start did not answer ($ch_start), last output ---\"; tail -c 1200 /tmp/coderhelm-proof-start.log; echo\n  fi\n");
        s.push_str("  ch_killtree $ch_pid\nfi\n");
    }
    s.push_str(&format!(
        "echo \"{MARK} install=$ch_install start=$ch_start port=$ch_port\"\ntrue"
    ));
    s
}

/// What one proof run came to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ProofResult {
    /// The sandbox ran the script to its last line (false: no result, not a
    /// verdict).
    pub ran: bool,
    pub install: i32,
    /// Each build command's exit code, in the plan's order (`None`: it did
    /// not run).
    pub builds: Vec<Option<i32>>,
    /// `up`, `silent` (running, never answered), `exited:<code>` or
    /// `skipped` (no start command, or the build failed first).
    pub start: String,
    pub port: u16,
    /// Each requested path and the status it answered (0: no answer).
    pub pages: Vec<(String, u16)>,
    /// The output's tail, for a reason.
    pub tail: String,
}

impl ProofResult {
    /// No result, with why.
    pub fn no_result(why: &str) -> Self {
        ProofResult {
            tail: why.to_string(),
            ..Default::default()
        }
    }

    pub fn built(&self) -> bool {
        self.install == 0 && !self.builds.is_empty() && self.builds.iter().all(|b| *b == Some(0))
    }

    pub fn started(&self) -> bool {
        self.start == "up"
    }
}

/// A status that is a successful answer (2xx, or a redirect).
pub fn page_ok(status: u16) -> bool {
    (200..400).contains(&status)
}

/// A proof's result from its output; `ran` is false without the last line.
pub fn parse(output: &str, plan: &ProofPlan) -> ProofResult {
    let mut r = ProofResult {
        builds: vec![None; plan.builds.len()],
        start: "skipped".into(),
        tail: crate::tail_str(output, 2500).to_string(),
        ..Default::default()
    };
    for line in output.lines() {
        let mut parts = line.split_whitespace();
        match parts.next() {
            Some(STEP_MARK) => {
                let (Some("build"), Some(n), Some(code)) = (
                    parts.next(),
                    parts.next().and_then(|n| n.parse::<usize>().ok()),
                    parts.next().and_then(|c| c.parse::<i32>().ok()),
                ) else {
                    continue;
                };
                if (1..=r.builds.len()).contains(&n) {
                    r.builds[n - 1] = Some(code);
                }
            }
            Some(PAGE_MARK) => {
                let (Some(path), Some(code)) = (
                    parts.next(),
                    parts.next().and_then(|c| c.parse::<u16>().ok()),
                ) else {
                    continue;
                };
                r.pages.retain(|(p, _)| p != path);
                r.pages.push((path.to_string(), code));
            }
            Some(MARK) => {
                let mut install = None;
                for p in parts {
                    if let Some(v) = p.strip_prefix("install=") {
                        install = v.parse::<i32>().ok();
                    } else if let Some(v) = p.strip_prefix("start=") {
                        r.start = v.to_string();
                    } else if let Some(v) = p.strip_prefix("port=") {
                        r.port = v.parse().unwrap_or(0);
                    }
                }
                if let Some(i) = install {
                    r.install = i;
                    r.ran = true;
                }
            }
            _ => {}
        }
    }
    r
}

fn start_words(start: &str) -> String {
    match start.strip_prefix("exited:") {
        Some(code) => format!("exited with {code} before answering"),
        None if start == "silent" => format!(
            "ran for {} seconds without answering on a port",
            START_POLLS * 2
        ),
        None => "did not run".to_string(),
    }
}

/// What passes on the base branch and fails with the bumps, in words; empty
/// when the bumps break nothing the proof can see. Never names a part that
/// fails on the base branch too, or one either run gave no result for.
pub fn blamed(plan: &ProofPlan, base: &ProofResult, head: &ProofResult) -> Vec<String> {
    let mut out = vec![];
    if !base.ran || !head.ran {
        return out;
    }
    if base.install == 0 && head.install != 0 {
        out.push(format!(
            "the install `{}` fails (exit {})",
            plan.install, head.install
        ));
        return out;
    }
    for (i, b) in plan.builds.iter().enumerate() {
        let (was, now) = (
            base.builds.get(i).copied().flatten(),
            head.builds.get(i).copied().flatten(),
        );
        if was == Some(0) {
            if let Some(code) = now.filter(|c| *c != 0) {
                out.push(format!("the build `{}` fails (exit {code})", b.cmd));
            }
        }
    }
    if !out.is_empty() {
        return out;
    }
    if let Some(start) = &plan.start {
        if base.started() && !head.started() && head.start != "skipped" {
            out.push(format!(
                "the application no longer starts: `{}` {}",
                start.cmd,
                start_words(&head.start)
            ));
            return out;
        }
        if base.started() && head.started() {
            for (path, was) in &base.pages {
                let now = head
                    .pages
                    .iter()
                    .find(|(p, _)| p == path)
                    .map(|(_, c)| *c)
                    .unwrap_or(0);
                if page_ok(*was) && !page_ok(now) {
                    out.push(format!(
                        "`GET {path}` answers {} ({was} on the base branch)",
                        if now == 0 {
                            "nothing".to_string()
                        } else {
                            now.to_string()
                        }
                    ));
                }
            }
        }
    }
    out
}

fn build_line(b: &ProofCmd, base: Option<i32>, head: Option<i32>) -> String {
    let verdict = match (base, head) {
        (_, Some(0)) => "passes with these bumps".to_string(),
        (Some(0), Some(c)) => format!("fails with these bumps (exit {c}); it passes on the base branch"),
        (Some(b), Some(_)) => format!(
            "fails on the base branch too (exit {b}; not caused by this change), so it proves nothing about the bumps"
        ),
        (None, Some(c)) => format!("fails (exit {c}); it did not run on the base branch"),
        (_, None) => "did not run (the install failed first)".to_string(),
    };
    format!("- Build `{}` (from {}): {verdict}.\n", b.cmd, b.source)
}

/// The pull request's account of the proof: each command with where it was
/// found and what it came to, the pages requested with their answers, and
/// what did not run with why.
pub fn report(plan: &ProofPlan, base: Option<&ProofResult>, head: Option<&ProofResult>) -> String {
    let mut s = String::new();
    if !plan.not_covered.is_empty() {
        s.push_str(&format!(
            "- Covers the project of `{}` only (where most of the bumps are); the bumps in {} are not built or started here.\n",
            if plan.dir.is_empty() { "the repository root" } else { plan.dir.as_str() },
            plan.not_covered
                .iter()
                .map(|l| format!("`{l}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !plan.projects_unknown.is_empty() {
        s.push_str(&format!(
            "- Which workspace projects take the changed packages is not known ({}), so only the builds below were run; a project they don't cover is not proven here.\n",
            plan.projects_unknown
        ));
    }
    for p in &plan.projects {
        if p.not_built.is_empty() {
            s.push_str(&format!(
                "- Project `{}` {}: built here ({}).\n",
                p.dir,
                p.because,
                p.builds
                    .iter()
                    .filter_map(|&i| plan.builds.get(i))
                    .map(|b| format!("`{}`", b.cmd))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }
    for (dir, why) in unproven_projects(plan, base, head) {
        let because = plan
            .projects
            .iter()
            .find(|p| p.dir == dir)
            .map(|p| p.because.as_str())
            .unwrap_or("");
        s.push_str(&format!(
            "- Project `{dir}` {because}: **not proven here**: {why}. CI on this pull request, or a build by hand, is the check.\n"
        ));
    }
    let head = match head {
        Some(h) if h.ran => h,
        other => {
            let why = other
                .map(|h| crate::truncate_str(first_line(&h.tail), 200).to_string())
                .filter(|w| !w.is_empty())
                .unwrap_or_else(|| "the sandbox gave no result".to_string());
            for b in &plan.builds {
                s.push_str(&format!(
                    "- Build `{}` (from {}): no result ({why}). Not proven here; CI on this pull request is the check.\n",
                    b.cmd, b.source
                ));
            }
            if let Some(st) = &plan.start {
                s.push_str(&format!(
                    "- Start `{}` (from {}): not run (the build gave no result).\n",
                    st.cmd, st.source
                ));
            }
            return s;
        }
    };
    let base = base.filter(|b| b.ran);
    if head.install != 0 {
        s.push_str(&format!(
            "- Install `{}`: fails (exit {}){}; the build and the start did not run.\n",
            plan.install,
            head.install,
            match base {
                Some(b) if b.install != 0 => " on the base branch too (not caused by this change)",
                _ => "",
            }
        ));
        return s;
    }
    for (i, b) in plan.builds.iter().enumerate() {
        s.push_str(&build_line(
            b,
            base.and_then(|x| x.builds.get(i).copied().flatten()),
            head.builds.get(i).copied().flatten(),
        ));
    }
    if plan.builds.is_empty() {
        s.push_str(&format!("- Build: not run: {}.\n", plan.no_build));
    }
    match &plan.start {
        None => s.push_str(&format!("- Start: not run: {}.\n", plan.no_start)),
        Some(st) if head.start == "skipped" => s.push_str(&format!(
            "- Start `{}` (from {}): not run (the build failed first).\n",
            st.cmd, st.source
        )),
        Some(st) if head.started() => {
            let pages: Vec<String> = head
                .pages
                .iter()
                .filter(|(p, c)| {
                    // A health path the application doesn't have says
                    // nothing; the root page is always shown.
                    p == "/" || *c != 404
                })
                .map(|(p, c)| {
                    if *c == 0 {
                        format!("`GET {p}` no answer")
                    } else {
                        format!("`GET {p}` {c}")
                    }
                })
                .collect();
            let good = head.pages.iter().filter(|(_, c)| page_ok(*c)).count();
            s.push_str(&format!(
                "- Start `{}` (from {}): the application started in the sandbox and answered on port {}. {}.{}\n",
                st.cmd,
                st.source,
                head.port,
                pages.join(", "),
                if good == 0 {
                    " No requested path returned a successful status, so only the start is proven, not a page."
                } else {
                    ""
                }
            ));
        }
        Some(st) => {
            let on_base = base.is_some_and(|b| !b.started());
            s.push_str(&format!(
                "- Start `{}` (from {}): {}{}.\n",
                st.cmd,
                st.source,
                start_words(&head.start),
                if on_base {
                    ", on the base branch too (the sandbox has no secrets or services, so this is not caused by the bumps and proves nothing about them)"
                } else {
                    ""
                }
            ));
        }
    }
    s
}

fn first_line(s: &str) -> &str {
    s.lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ci(cmd: &str, dir: Option<&str>, pr: bool) -> CiBuild {
        CiBuild {
            cmd: cmd.into(),
            dir: dir.map(String::from),
            workflow: if pr { "ci.yml" } else { "deploy.yml" }.into(),
            job: "build".into(),
            on_pull_request: pr,
        }
    }

    fn pkg(scripts: &str) -> Vec<(String, String)> {
        vec![(
            "package.json".to_string(),
            format!("{{\"name\":\"shop\",\"scripts\":{scripts}}}"),
        )]
    }

    // ── Every project the bumps can break ──────────────────────────────

    fn affected(dir: &str, uses: &[&str], scripts: Option<&str>) -> AffectedProject {
        AffectedProject {
            dir: dir.into(),
            uses: uses.iter().map(|u| u.to_string()).collect(),
            manifest: scripts.map(|s| format!("{{\"scripts\":{s}}}")),
        }
    }

    /// The site is what CI builds; the studio has a build script and no CI
    /// job; the shared package declares no build.
    fn workspace_plan() -> ProofPlan {
        let mut plan = discover(
            "pnpm-lock.yaml",
            &[ci("pnpm --filter web build", Some("apps/web"), true)],
            &pkg(r#"{"dev":"turbo dev"}"#),
        );
        add_projects(
            &mut plan,
            "pnpm-lock.yaml",
            &[
                affected(
                    "apps/web",
                    &["path-match@3.1.2"],
                    Some(r#"{"build":"next build"}"#),
                ),
                affected(
                    "apps/studio",
                    &["glob-walk@2.3.1"],
                    Some(r#"{"build":"studio build"}"#),
                ),
                affected(
                    "packages/ui",
                    &["path-match@3.1.2"],
                    Some(r#"{"lint":"eslint ."}"#),
                ),
            ],
        );
        plan
    }

    #[test]
    fn a_project_no_ci_job_builds_is_built_when_its_manifest_declares_a_build() {
        let plan = workspace_plan();
        let cmds: Vec<(&str, &str)> = plan
            .builds
            .iter()
            .map(|b| (b.cmd.as_str(), b.dir.as_str()))
            .collect();
        assert_eq!(
            cmds,
            vec![
                ("pnpm --filter web build", "apps/web"),
                ("pnpm run build", "apps/studio"),
            ]
        );
        assert_eq!(
            plan.builds[1].source,
            "the `build` script of `apps/studio/package.json`"
        );
        // The site is covered by the build CI runs in its folder.
        assert_eq!(plan.projects[0].builds, vec![0]);
        assert_eq!(plan.projects[1].builds, vec![1]);
        assert_eq!(
            plan.projects[2].not_built,
            "`packages/ui/package.json` declares no `build` script"
        );
        let sh = script(&plan);
        assert!(sh.contains("( cd 'apps/studio' && pnpm run build )"));
        // The studio's build failing never keeps the site's start from
        // running: only the main project's builds gate it.
        let studio = sh.lines().find(|l| l.contains("apps/studio")).unwrap();
        assert!(!studio.contains("ch_built=1"), "{studio}");
        let web = sh.lines().find(|l| l.contains("--filter web")).unwrap();
        assert!(web.contains("ch_built=1"), "{web}");
    }

    #[test]
    fn a_project_build_only_the_bumps_break_is_blamed_and_feeds_the_take_out() {
        let plan = workspace_plan();
        let base = ProofResult {
            ran: true,
            builds: vec![Some(0), Some(0)],
            start: "skipped".into(),
            ..Default::default()
        };
        let head = ProofResult {
            builds: vec![Some(0), Some(1)],
            ..base.clone()
        };
        assert_eq!(
            blamed(&plan, &base, &head),
            vec!["the build `pnpm run build` fails (exit 1)".to_string()]
        );
        // Blamed, so not listed as unproven: only the project with no build is.
        let open = unproven_projects(&plan, Some(&base), Some(&head));
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].0, "packages/ui");
    }

    #[test]
    fn a_project_that_cannot_be_built_in_the_sandbox_is_named_as_not_proven() {
        let plan = workspace_plan();
        // The studio's build needs a token the sandbox does not have: it
        // fails on the base branch too.
        let base = ProofResult {
            ran: true,
            builds: vec![Some(0), Some(1)],
            start: "skipped".into(),
            ..Default::default()
        };
        let head = base.clone();
        assert!(
            blamed(&plan, &base, &head).is_empty(),
            "never blamed on a bump"
        );
        let open = unproven_projects(&plan, Some(&base), Some(&head));
        assert_eq!(open.len(), 2);
        assert_eq!(open[0].0, "apps/studio");
        assert!(
            open[0].1.contains("fails on the base branch too"),
            "{}",
            open[0].1
        );
        let r = report(&plan, Some(&base), Some(&head));
        assert!(
            r.contains("- Project `apps/studio` takes `glob-walk@2.3.1`: **not proven here**: its build `pnpm run build` fails on the base branch too"),
            "{r}"
        );
        assert!(
            r.contains("- Project `packages/ui` takes `path-match@3.1.2`: **not proven here**: `packages/ui/package.json` declares no `build` script"),
            "{r}"
        );
        assert!(r.contains("- Project `apps/web` takes `path-match@3.1.2`: built here (`pnpm --filter web build`)."), "{r}");
        // No result at all: every built project is unproven, and said so.
        let none = unproven_projects(&plan, Some(&base), None);
        assert!(none
            .iter()
            .any(|(d, w)| d == "apps/web" && w.contains("gave no result")));
    }

    #[test]
    fn the_project_of_another_lockfile_is_built_after_its_own_install() {
        let mut plan = discover("package-lock.json", &[], &pkg(r#"{"build":"vite build"}"#));
        let studio = discover(
            "studio/package-lock.json",
            &[],
            &[(
                "studio/package.json".to_string(),
                r#"{"scripts":{"build":"studio build"}}"#.to_string(),
            )],
        );
        add_lockfile(&mut plan, "studio/package-lock.json", 3, &studio);
        let tools = discover("tools/package-lock.json", &[], &[]);
        add_lockfile(&mut plan, "tools/package-lock.json", 1, &tools);
        assert_eq!(plan.builds.len(), 2);
        assert_eq!(plan.builds[1].dir, "studio");
        assert!(
            plan.builds[1]
                .cmd
                .ends_with("npm ci --no-audit --no-fund && npm run build"),
            "{}",
            plan.builds[1].cmd
        );
        assert_eq!(
            plan.projects[0].because,
            "has its own lockfile `studio/package-lock.json` with 3 of the bumps"
        );
        assert!(
            plan.projects[1]
                .not_built
                .contains("`tools/package.json` has no `build` script"),
            "{}",
            plan.projects[1].not_built
        );
        assert!(plan.not_covered.is_empty());
        // The cap is said, never silently dropped.
        let mut many = discover("pnpm-lock.yaml", &[], &pkg(r#"{"build":"x"}"#));
        let all: Vec<AffectedProject> = (0..MAX_PROJECT_BUILDS + 2)
            .map(|i| {
                affected(
                    &format!("apps/a{i}"),
                    &["glob-walk@2.3.1"],
                    Some(r#"{"build":"x"}"#),
                )
            })
            .collect();
        add_projects(&mut many, "pnpm-lock.yaml", &all);
        assert_eq!(many.builds.len(), 1 + MAX_PROJECT_BUILDS);
        assert_eq!(
            many.projects
                .iter()
                .filter(|p| p.not_built.contains("at most"))
                .count(),
            2
        );
    }

    #[test]
    fn workspace_folders_are_read_from_the_root_manifest() {
        let tree: Vec<String> = [
            "package.json",
            "apps/web/package.json",
            "apps/studio/package.json",
            "apps/web/node_modules/x/package.json",
            "packages/ui/package.json",
            "packages/deep/inner/package.json",
            "tools/release/package.json",
        ]
        .iter()
        .map(|p| p.to_string())
        .collect();
        assert_eq!(
            workspace_dirs(r#"{"workspaces":["apps/*","packages/**"]}"#, &tree, ""),
            vec![
                "apps/studio",
                "apps/web",
                "packages/deep/inner",
                "packages/ui"
            ]
        );
        assert_eq!(
            workspace_dirs(
                r#"{"workspaces":{"packages":["tools/release"]}}"#,
                &tree,
                ""
            ),
            vec!["tools/release"]
        );
        assert!(workspace_dirs(r#"{"name":"x"}"#, &tree, "").is_empty());
    }

    #[test]
    fn the_build_is_the_one_ci_runs_on_a_pull_request() {
        let plan = discover(
            "pnpm-lock.yaml",
            &[
                ci("pnpm deploy:build", None, false),
                ci("pnpm --filter web build", None, true),
            ],
            &pkg(r#"{"build":"next build","start":"next start"}"#),
        );
        assert_eq!(plan.builds.len(), 1);
        assert_eq!(plan.builds[0].cmd, "pnpm --filter web build");
        assert_eq!(
            plan.builds[0].source,
            "the CI workflow `ci.yml`, job `build`"
        );
        assert!(plan.install.ends_with("pnpm install --frozen-lockfile"));
        let start = plan.start.unwrap();
        assert_eq!(start.cmd, "pnpm run start");
        assert_eq!(start.source, "the `start` script of `package.json`");
    }

    #[test]
    fn a_deploy_workflows_build_is_used_when_no_pull_request_workflow_builds() {
        let plan = discover(
            "package-lock.json",
            &[ci("npm run build:prod", None, false)],
            &pkg(r#"{"build":"vite build"}"#),
        );
        assert_eq!(plan.builds[0].cmd, "npm run build:prod");
        assert!(plan.builds[0].source.contains("deploy.yml"));
        // No start script and no Procfile: said, never guessed.
        assert_eq!(plan.start, None);
        assert!(plan
            .no_start
            .contains("no `start` script in `package.json`"));
    }

    #[test]
    fn without_a_ci_build_the_manifests_script_then_the_toolchains_own_build() {
        let plan = discover("yarn.lock", &[], &pkg(r#"{"build":"tsc -b"}"#));
        assert_eq!(plan.builds[0].cmd, "yarn run build");
        assert_eq!(
            plan.builds[0].source,
            "the `build` script of `package.json`"
        );
        let none = discover("yarn.lock", &[], &pkg(r#"{"test":"jest"}"#));
        assert!(none.builds.is_empty() && none.is_empty());
        assert!(
            none.no_build.contains("no `build` script"),
            "{}",
            none.no_build
        );
        // Other managers build with their own toolchain: one table row each.
        for (lock, build) in [
            ("Cargo.lock", "cargo build --locked"),
            ("services/api/go.sum", "go build ./..."),
            ("poetry.lock", "python -m compileall -q ."),
            ("uv.lock", "python -m compileall -q ."),
            ("Pipfile.lock", "python -m compileall -q ."),
        ] {
            let p = discover(lock, &[], &[]);
            assert_eq!(p.builds[0].cmd, build, "{lock}");
            assert!(!p.install.is_empty(), "{lock}");
        }
        assert_eq!(
            discover("services/api/go.sum", &[], &[]).dir,
            "services/api"
        );
        // Every lockfile CoderHelm writes has a row.
        for lock in [
            "package-lock.json",
            "pnpm-lock.yaml",
            "yarn.lock",
            "bun.lock",
            "Cargo.lock",
            "poetry.lock",
            "uv.lock",
            "Pipfile.lock",
            "Gemfile.lock",
            "go.sum",
        ] {
            assert_eq!(
                manager_of(lock).map(|m| m.name),
                crate::lockfile_sync::lock_manager(lock),
                "{lock}"
            );
        }
    }

    #[test]
    fn a_nested_project_takes_only_the_builds_of_its_own_folder() {
        let files = vec![
            (
                "apps/web/package.json".to_string(),
                r#"{"scripts":{"build":"next build","start":"next start"}}"#.to_string(),
            ),
            (
                "apps/web/Procfile".to_string(),
                "release: ./migrate\nweb: node server.js\n".to_string(),
            ),
        ];
        let plan = discover(
            "apps/web/package-lock.json",
            &[
                ci("npm run build", Some("apps/admin"), true),
                ci("npm run build", Some("apps/web"), true),
                ci("npm run build", Some("../outside"), true),
            ],
            &files,
        );
        assert_eq!(plan.builds.len(), 1);
        assert_eq!(plan.builds[0].dir, "apps/web");
        // A Procfile's web entry is the declared start.
        let start = plan.start.unwrap();
        assert_eq!(start.cmd, "node server.js");
        assert_eq!(start.source, "the `web` entry of `apps/web/Procfile`");
        assert_eq!(
            files_to_read("apps/web/package-lock.json"),
            vec!["apps/web/Procfile", "apps/web/package.json"]
        );
    }

    fn plan() -> ProofPlan {
        discover(
            "pnpm-lock.yaml",
            &[ci("pnpm build", None, true)],
            &pkg(r#"{"build":"next build","start":"next start"}"#),
        )
    }

    #[test]
    fn the_script_reports_each_part_on_its_own_line_and_always_exits_zero() {
        let s = script(&plan());
        assert!(s.contains("pnpm install --frozen-lockfile"));
        assert!(s.contains("( pnpm build ) > /tmp/coderhelm-proof-build-1.log"));
        assert!(s.contains("export PORT=38411 && pnpm run start"));
        assert!(s.contains("CODERHELM_PROOF_STEP build 1 $ch_b1"));
        assert!(s.contains("for ch_path in / /healthz /health /api/health; do"));
        // Local requests never go through the team's allowlist proxy.
        assert!(s.contains("--noproxy '*'"));
        assert!(s.ends_with("true"));
        // Without a start command nothing is started.
        let mut no_start = plan();
        no_start.start = None;
        assert!(!script(&no_start).contains("export PORT="));
    }

    const GREEN: &str = "\
CODERHELM_PROOF_STEP build 1 0
CODERHELM_PROOF_PAGE / 200
CODERHELM_PROOF_PAGE /healthz 404
CODERHELM_PROOF_PAGE /health 404
CODERHELM_PROOF_PAGE /api/health 200
CODERHELM_PROOF install=0 start=up port=38411
";

    #[test]
    fn a_green_proof_reads_built_started_and_its_pages() {
        let p = plan();
        let r = parse(GREEN, &p);
        assert!(r.ran && r.built() && r.started());
        assert_eq!(r.port, 38411);
        assert_eq!(r.pages.len(), 4);
        assert!(blamed(&p, &r, &r).is_empty());
        let text = report(&p, Some(&r), Some(&r));
        assert!(text.contains("Build `pnpm build` (from the CI workflow `ci.yml`, job `build`): passes with these bumps."), "{text}");
        assert!(text.contains("the application started in the sandbox and answered on port 38411. `GET /` 200, `GET /api/health` 200."), "{text}");
        // Health paths the application doesn't have are left out.
        assert!(!text.contains("/healthz"), "{text}");
    }

    #[test]
    fn output_cut_short_is_no_result_never_a_verdict() {
        let p = plan();
        let cut = parse("CODERHELM_PROOF_STEP build 1 0\n", &p);
        assert!(!cut.ran);
        let green = parse(GREEN, &p);
        assert!(blamed(&p, &green, &cut).is_empty());
        let text = report(&p, Some(&green), Some(&cut));
        assert!(text.contains("no result"), "{text}");
        assert!(text.contains("Not proven here"), "{text}");
        assert!(!text.contains("passes"), "{text}");
    }

    #[test]
    fn only_what_passes_on_the_base_and_fails_with_the_bumps_is_blamed() {
        let p = plan();
        let green = parse(GREEN, &p);
        let build_red = parse(
            "CODERHELM_PROOF_STEP build 1 1\nCODERHELM_PROOF install=0 start=skipped port=0\n",
            &p,
        );
        assert_eq!(
            blamed(&p, &green, &build_red),
            vec!["the build `pnpm build` fails (exit 1)"]
        );
        // The base fails the same way: a secret the sandbox lacks, not a bump.
        assert!(blamed(&p, &build_red, &build_red).is_empty());
        let text = report(&p, Some(&build_red), Some(&build_red));
        assert!(text.contains("fails on the base branch too"), "{text}");
        assert!(text.contains("not run (the build failed first)"), "{text}");

        let crashed = parse(
            "CODERHELM_PROOF_STEP build 1 0\nCODERHELM_PROOF install=0 start=exited:1 port=0\n",
            &p,
        );
        assert_eq!(
            blamed(&p, &green, &crashed),
            vec![
                "the application no longer starts: `pnpm run start` exited with 1 before answering"
            ]
        );
        assert!(blamed(&p, &crashed, &crashed).is_empty());
        let text = report(&p, Some(&crashed), Some(&crashed));
        assert!(text.contains("on the base branch too"), "{text}");

        let page_red = parse(&GREEN.replace("/api/health 200", "/api/health 500"), &p);
        assert_eq!(
            blamed(&p, &green, &page_red),
            vec!["`GET /api/health` answers 500 (200 on the base branch)"]
        );
        // A path that was already failing on the base is never blamed.
        assert!(blamed(&p, &page_red, &page_red).is_empty());
    }

    #[test]
    fn what_did_not_run_is_said_with_why() {
        let mut p = discover("yarn.lock", &[], &pkg(r#"{"build":"tsc -b"}"#));
        let r = parse(
            "CODERHELM_PROOF_STEP build 1 0\nCODERHELM_PROOF install=0 start=skipped port=0\n",
            &p,
        );
        let text = report(&p, Some(&r), Some(&r));
        assert!(
            text.contains("- Start: not run: the repository declares no start command"),
            "{text}"
        );
        p.builds.clear();
        p.no_build = "no workflow runs a build command".into();
        assert!(report(&p, Some(&r), Some(&r))
            .contains("- Build: not run: no workflow runs a build command."));
    }

    #[test]
    fn a_procfile_names_the_web_command() {
        assert_eq!(
            procfile_web("worker: bundle exec sidekiq\nweb: bundle exec puma -C config/puma.rb\n"),
            Some("bundle exec puma -C config/puma.rb".into())
        );
        assert_eq!(procfile_web("worker: x\n"), None);
    }
}
