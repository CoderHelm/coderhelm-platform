//! What a dependency change is verified with, whatever CI happens to run.
//!
//! A dependency pull request recorded no type check because the
//! repository's CI has none, and CoderHelm's checks mirrored CI. The
//! reviewer ran `tsc --noEmit` themselves, and a frozen-lockfile install
//! with the package manager version the manifest declares and again with
//! the one the deployment image installs.
//!
//! The set here is found in the project, never in its CI:
//!
//! - [`ROWS`] has one row per package manager: its frozen-lockfile install
//!   with the flag written out (a package manager decides by itself whether
//!   it is in CI), how it runs a tool, and how each check is found: a
//!   manifest script, a configuration file, a table in a configuration
//!   file, or the toolchain's own command. Adding a manager is one row.
//! - [`discover`] reads one project into a [`Plan`]: the frozen install,
//!   the same install with the package manager version the deployment image
//!   pins when that differs from the manifest's, the type check when the
//!   project is typed, the build, the lint, the unit tests and the
//!   production-scoped audit. A check the project does not have is named
//!   with why (`no tsconfig.json: the project is not typed`).
//! - [`fit`] puts the set into the time one sandbox build has, in priority
//!   order, and names what does not fit and why. Nothing is claimed for a
//!   check that did not run.
//! - [`script`] is the one sandbox command; [`read`] turns its marked lines
//!   into a [`Verified`] record of one head. [`blamed`] compares it with
//!   the same set on the base branch, so a failure the base already has is
//!   never blamed on the change.
//!
//! Where the commands come from: `pnpm install --frozen-lockfile`,
//! `pnpm audit --prod`, `npm audit --omit=dev` and `uv sync --locked` were
//! read from the tools' own documentation. The other rows are the commands
//! this codebase already runs (`runtime_proof::MANAGERS`, `dep_audit`,
//! `worker::passes::checks::TOOLS`) or each toolchain's standard ones.
//!
//! Everything here is pure; the sandbox run is the worker's
//! (`worker::passes::dep_fix`).

use serde::{Deserialize, Serialize};

use crate::dep_fix::b64;
use crate::e2e::test_package::{ensure_manager, Manager as Js};

/// One kind of check of the minimum set.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// The frozen-lockfile install with the package manager version the
    /// manifest declares (or the sandbox's, when it declares none).
    #[default]
    Install,
    /// The same install with the version the deployment image pins.
    DeployInstall,
    Typecheck,
    Build,
    Lint,
    Tests,
    /// The package manager's audit of production dependencies.
    Audit,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Kind::Install => "frozen install",
            Kind::DeployInstall => "frozen install, deployment version",
            Kind::Typecheck => "type check",
            Kind::Build => "build",
            Kind::Lint => "lint",
            Kind::Tests => "unit tests",
            Kind::Audit => "production audit",
        }
    }

    /// What is kept first when the time is short: lower is kept longer.
    /// The install carries every other check; the type check is the
    /// cheapest way to see a changed signature; the audit costs seconds.
    pub fn priority(self) -> u8 {
        match self {
            Kind::Install => 0,
            Kind::Typecheck => 1,
            Kind::Audit => 2,
            Kind::Build => 3,
            Kind::Tests => 4,
            Kind::Lint => 5,
            Kind::DeployInstall => 6,
        }
    }

    /// The seconds a check of this kind is given when nothing is known of
    /// the project: what it is budgeted, and half of its time limit.
    pub fn seconds(self) -> u32 {
        match self {
            Kind::Install => 150,
            Kind::DeployInstall => 120,
            Kind::Typecheck => 75,
            Kind::Build => 150,
            Kind::Lint => 45,
            Kind::Tests => 150,
            Kind::Audit => 30,
        }
    }

    /// It needs the install to have passed.
    fn needs_install(self) -> bool {
        !matches!(self, Kind::Install | Kind::DeployInstall | Kind::Audit)
    }

    /// A failure of it can be the change's (lint fails on configuration
    /// quirks, and an audit that finds advisories is a result).
    pub fn blames(self) -> bool {
        !matches!(self, Kind::Lint | Kind::Audit)
    }
}

/// How one check is found in a project. The first way that applies wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Found {
    pub kind: Kind,
    /// Manifest script names; the first one the manifest has is run.
    pub scripts: &'static [&'static str],
    /// Configuration files whose presence means the project has the check.
    pub files: &'static [&'static str],
    /// A file and a text in it that mean the same (`pyproject.toml`,
    /// `[tool.mypy]`).
    pub marks: &'static [(&'static str, &'static str)],
    /// The tool's command when a file or a mark found it.
    pub cmd: &'static str,
    /// The toolchain's own check: it applies to every project.
    pub always: bool,
}

const fn by_script(kind: Kind, scripts: &'static [&'static str]) -> Found {
    Found {
        kind,
        scripts,
        files: &[],
        marks: &[],
        cmd: "",
        always: false,
    }
}

const fn by_config(
    kind: Kind,
    scripts: &'static [&'static str],
    files: &'static [&'static str],
    marks: &'static [(&'static str, &'static str)],
    cmd: &'static str,
) -> Found {
    Found {
        kind,
        scripts,
        files,
        marks,
        cmd,
        always: false,
    }
}

const fn own(kind: Kind, cmd: &'static str) -> Found {
    Found {
        kind,
        scripts: &[],
        files: &[],
        marks: &[],
        cmd,
        always: true,
    }
}

/// One package manager's minimum set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Row {
    pub manager: &'static str,
    pub lockfiles: &'static [&'static str],
    /// Installs exactly what the lockfile says and fails when the lockfile
    /// and the manifest disagree, with the flag written out.
    pub install: &'static str,
    /// Makes the manager available first, when it may be missing.
    pub ensure: &'static str,
    /// The manifest whose scripts name checks.
    pub manifest: Option<&'static str>,
    /// Runs a manifest script; the script's name follows.
    pub run_script: &'static str,
    /// Runs a tool of the project; the tool's command follows.
    pub run_tool: &'static str,
    pub checks: &'static [Found],
    /// Runs the install with an exact version of the manager (`{v}` is the
    /// version); empty when the sandbox has no way to.
    pub at_version: &'static str,
    /// Why a kind the row never finds does not apply to its projects.
    pub never: &'static [(Kind, &'static str)],
}

const TS_SCRIPTS: &[&str] = &["typecheck", "type-check", "check-types", "types", "tsc"];
const ESLINT: &[&str] = &[
    ".eslintrc",
    ".eslintrc.js",
    ".eslintrc.cjs",
    ".eslintrc.json",
    ".eslintrc.yml",
    ".eslintrc.yaml",
    "eslint.config.js",
    "eslint.config.mjs",
    "eslint.config.cjs",
    "eslint.config.ts",
];
const VITEST: &[&str] = &[
    "vitest.config.ts",
    "vitest.config.mts",
    "vitest.config.js",
    "vitest.config.mjs",
];
const JEST: &[&str] = &[
    "jest.config.js",
    "jest.config.ts",
    "jest.config.cjs",
    "jest.config.mjs",
    "jest.config.json",
];

/// The checks of a JavaScript or TypeScript project, whatever its manager.
const JS_CHECKS: &[Found] = &[
    by_config(
        Kind::Typecheck,
        TS_SCRIPTS,
        &["tsconfig.json"],
        &[],
        "npx --no-install tsc --noEmit",
    ),
    by_script(Kind::Build, &["build"]),
    by_config(
        Kind::Lint,
        &["lint"],
        ESLINT,
        &[],
        "npx --no-install eslint .",
    ),
    by_config(
        Kind::Lint,
        &[],
        &["biome.json", "biome.jsonc"],
        &[],
        "npx --no-install biome ci .",
    ),
    by_config(
        Kind::Tests,
        &["test:unit", "test:ci", "test"],
        VITEST,
        &[],
        "npx --no-install vitest run",
    ),
    by_config(Kind::Tests, &[], JEST, &[], "npx --no-install jest --ci"),
];

/// The checks of a Python project, whatever its manager.
const PY_CHECKS: &[Found] = &[
    by_config(
        Kind::Typecheck,
        &[],
        &["mypy.ini", ".mypy.ini"],
        &[("pyproject.toml", "[tool.mypy]"), ("setup.cfg", "[mypy]")],
        "mypy .",
    ),
    by_config(
        Kind::Typecheck,
        &[],
        &["pyrightconfig.json"],
        &[("pyproject.toml", "[tool.pyright]")],
        "pyright",
    ),
    own(Kind::Build, "python -m compileall -q ."),
    by_config(
        Kind::Lint,
        &[],
        &["ruff.toml", ".ruff.toml"],
        &[("pyproject.toml", "[tool.ruff")],
        "ruff check .",
    ),
    by_config(
        Kind::Lint,
        &[],
        &[".flake8"],
        &[("setup.cfg", "[flake8]"), ("tox.ini", "[flake8]")],
        "flake8",
    ),
    by_config(
        Kind::Tests,
        &[],
        &["pytest.ini", "conftest.py", "tests/conftest.py"],
        &[
            ("pyproject.toml", "[tool.pytest"),
            ("setup.cfg", "[tool:pytest]"),
            ("tox.ini", "[pytest]"),
        ],
        "pytest -q",
    ),
];

const NOT_TYPED_PY: &str = "no mypy or pyright configuration: the project is not type-checked";

const fn js(
    manager: &'static str,
    lockfiles: &'static [&'static str],
    install: &'static str,
    run_script: &'static str,
    at_version: &'static str,
) -> Row {
    Row {
        manager,
        lockfiles,
        install,
        // Made available by `ensure_manager`, per manager, in `discover`.
        ensure: "",
        manifest: Some("package.json"),
        run_script,
        run_tool: "",
        checks: JS_CHECKS,
        at_version,
        never: &[],
    }
}

const fn py(
    manager: &'static str,
    lockfiles: &'static [&'static str],
    install: &'static str,
    ensure: &'static str,
    run_tool: &'static str,
) -> Row {
    Row {
        manager,
        lockfiles,
        install,
        ensure,
        manifest: None,
        run_script: "",
        run_tool,
        checks: PY_CHECKS,
        at_version: "",
        never: &[(Kind::Typecheck, NOT_TYPED_PY)],
    }
}

/// The minimum set of each package manager CoderHelm writes lockfiles for
/// (`lockfile_sync::lock_manager` names the same ones).
pub const ROWS: &[Row] = &[
    js(
        "npm",
        &["package-lock.json", "npm-shrinkwrap.json"],
        "npm ci --no-audit --no-fund",
        "npm run",
        "npx -y npm@{v} ci --no-audit --no-fund --ignore-scripts",
    ),
    js(
        "pnpm",
        &["pnpm-lock.yaml"],
        "pnpm install --frozen-lockfile",
        "pnpm run",
        "npx -y pnpm@{v} install --frozen-lockfile --ignore-scripts",
    ),
    js(
        "yarn",
        &["yarn.lock"],
        "yarn install --frozen-lockfile",
        "yarn run",
        "npx -y yarn@{v} install --frozen-lockfile --ignore-scripts",
    ),
    js(
        "bun",
        &["bun.lock", "bun.lockb"],
        "bun install --frozen-lockfile",
        "bun run",
        "",
    ),
    Row {
        manager: "cargo",
        lockfiles: &["Cargo.lock"],
        install: "cargo fetch --locked",
        ensure: "",
        manifest: None,
        run_script: "",
        run_tool: "",
        checks: &[
            own(Kind::Typecheck, "cargo check --locked --all-targets"),
            own(Kind::Build, "cargo build --locked"),
            own(
                Kind::Lint,
                "cargo clippy --locked --all-targets -- -D warnings",
            ),
            own(Kind::Tests, "cargo test --locked"),
        ],
        at_version: "",
        never: &[],
    },
    py(
        "poetry",
        &["poetry.lock"],
        "poetry install --no-interaction",
        "(command -v poetry >/dev/null 2>&1 || pip install -q poetry) && ",
        "poetry run ",
    ),
    py(
        "uv",
        &["uv.lock"],
        "uv sync --locked",
        "(command -v uv >/dev/null 2>&1 || pip install -q uv) && ",
        "uv run ",
    ),
    py(
        "pipenv",
        &["Pipfile.lock"],
        "pipenv sync --dev",
        "(command -v pipenv >/dev/null 2>&1 || pip install -q pipenv) && ",
        "pipenv run ",
    ),
    Row {
        manager: "bundler",
        lockfiles: &["Gemfile.lock"],
        install: "BUNDLE_FROZEN=true bundle install",
        ensure: "",
        manifest: None,
        run_script: "",
        run_tool: "bundle exec ",
        checks: &[
            by_config(Kind::Typecheck, &[], &["sorbet/config"], &[], "srb tc"),
            by_config(Kind::Lint, &[], &[".rubocop.yml"], &[], "rubocop"),
            by_config(
                Kind::Tests,
                &[],
                &[".rspec", "spec/spec_helper.rb"],
                &[],
                "rspec",
            ),
            by_config(Kind::Tests, &[], &["test/test_helper.rb"], &[], "rake test"),
        ],
        at_version: "",
        never: &[
            (
                Kind::Typecheck,
                "no `sorbet/config`: the project is not typed",
            ),
            (Kind::Build, "a Ruby project has no build step"),
        ],
    },
    Row {
        manager: "go",
        lockfiles: &["go.sum"],
        install: "go mod download && go mod verify",
        ensure: "",
        manifest: None,
        run_script: "",
        run_tool: "",
        checks: &[
            own(Kind::Typecheck, "go vet ./..."),
            own(Kind::Build, "go build ./..."),
            by_config(
                Kind::Lint,
                &[],
                &[".golangci.yml", ".golangci.yaml", ".golangci.toml"],
                &[],
                "golangci-lint run",
            ),
            own(Kind::Tests, "go test ./..."),
        ],
        at_version: "",
        never: &[],
    },
];

/// The row of a lockfile's manager.
pub fn row_of(lockfile: &str) -> Option<&'static Row> {
    let name = lockfile.rsplit('/').next().unwrap_or(lockfile);
    ROWS.iter().find(|r| r.lockfiles.contains(&name))
}

fn dir_of(path: &str) -> &str {
    path.rsplit_once('/').map(|(d, _)| d).unwrap_or("")
}

fn under(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_string()
    } else {
        format!("{dir}/{name}")
    }
}

/// A deployment image's file, by name.
fn is_dockerfile(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    name == "Dockerfile" || name.starts_with("Dockerfile.") || name.ends_with(".Dockerfile")
}

/// Dockerfiles one project's plan reads at most.
const MAX_DOCKERFILES: usize = 4;

/// The files [`discover`] reads for the project of `lockfile`, of those in
/// `tree`: its manifest, the configuration files a row's marks name, and
/// the Dockerfiles in its folder and at the repository root.
pub fn files_to_read(lockfile: &str, tree: &[String]) -> Vec<String> {
    let Some(row) = row_of(lockfile) else {
        return vec![];
    };
    let dir = dir_of(lockfile);
    let mut want: Vec<String> = vec![];
    if let Some(m) = row.manifest {
        want.push(under(dir, m));
    }
    for f in row.checks {
        for (file, _) in f.marks {
            want.push(under(dir, file));
        }
    }
    let mut docker: Vec<&String> = tree
        .iter()
        .filter(|p| is_dockerfile(p) && (dir_of(p) == dir || dir_of(p).is_empty()))
        .collect();
    // The project's own first, then the root's.
    docker.sort_by_key(|p| (dir_of(p) != dir, p.len()));
    want.extend(docker.into_iter().take(MAX_DOCKERFILES).cloned());
    want.sort();
    want.dedup();
    want.retain(|p| tree.contains(p));
    want
}

/// A version number at the start of `s` (`9.1.0`, `1.8`).
fn version_at(s: &str) -> Option<String> {
    let v: String = s
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let v = v.trim_end_matches('.').to_string();
    (v.contains('.') && v.chars().next().is_some_and(|c| c.is_ascii_digit())).then_some(v)
}

/// The package manager version a manifest declares (`packageManager:
/// "pnpm@9.1.0"`), when it names `manager`.
pub fn declared_version(manifest: &str, manager: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(manifest).ok()?;
    let pm = v.get("packageManager")?.as_str()?;
    let (name, version) = pm.split_once('@')?;
    (name == manager).then(|| version_at(version)).flatten()
}

/// The version of `manager` a Dockerfile installs: `<manager>@<version>`
/// (`npm install -g pnpm@9.1.0`, `corepack prepare pnpm@9.1.0`),
/// `<manager>==<version>` (`pip install poetry==1.8.2`), or a
/// `<MANAGER>_VERSION` it sets. `None` when it names none.
pub fn image_version(dockerfile: &str, manager: &str) -> Option<String> {
    let upper = format!("{}_VERSION", manager.to_ascii_uppercase());
    for line in dockerfile.lines() {
        let l = line.trim();
        if l.starts_with('#') {
            continue;
        }
        for sep in ["@", "=="] {
            let needle = format!("{manager}{sep}");
            let mut from = 0;
            while let Some(i) = l[from..].find(&needle) {
                let at = from + i;
                let before = l[..at].chars().next_back();
                let word_start = before.is_none_or(|c| !c.is_ascii_alphanumeric() && c != '-');
                if word_start {
                    if let Some(v) = version_at(&l[at + needle.len()..]) {
                        return Some(v);
                    }
                }
                from = at + needle.len();
            }
        }
        if let Some(i) = l.find(&upper) {
            let rest = l[i + upper.len()..].trim_start_matches(['=', ' ', '"', '\'']);
            if let Some(v) = version_at(rest) {
                return Some(v);
            }
        }
    }
    None
}

/// One check of a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Check {
    pub kind: Kind,
    /// The command as people read it.
    pub shown: String,
    /// The full command, run in the project's folder.
    pub cmd: String,
    /// Where it was found, in words.
    pub found: String,
}

/// What a project is verified with, found once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Plan {
    pub lockfile: String,
    pub manager: String,
    /// The project's folder from the repository root.
    pub dir: String,
    pub checks: Vec<Check>,
    /// The kinds the project does not have, each with why.
    pub not_applicable: Vec<(Kind, String)>,
    /// The package manager version the manifest declares; empty for none.
    pub declared: String,
    /// The version the deployment image pins and the file that pins it;
    /// empty for none.
    pub image: String,
    pub image_file: String,
    /// Why nothing can be verified; empty when something can.
    pub none: String,
}

impl Plan {
    pub fn is_empty(&self) -> bool {
        self.checks.is_empty()
    }
}

/// A folder name a shell command can carry.
fn is_plain_path(p: &str) -> bool {
    p.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '_' | '-' | '.' | '@'))
        && !p.contains("..")
}

fn script_of<'a>(manifest: &'a serde_json::Value, name: &str) -> Option<&'a str> {
    let s = manifest.get("scripts")?.get(name)?.as_str()?;
    // The placeholder a new package.json comes with tests nothing.
    (!s.trim().is_empty() && !s.contains("no test specified")).then_some(s)
}

/// What the project of `lockfile` is verified with. `tree`: the
/// repository's files; `files`: the ones [`files_to_read`] named, with
/// their text; `audit`: the production-scoped audit of the lockfile's
/// manager (shown, command), when it has one.
pub fn discover(
    lockfile: &str,
    tree: &[String],
    files: &[(String, String)],
    audit: Option<(&str, &str)>,
) -> Plan {
    let dir = dir_of(lockfile).to_string();
    let mut plan = Plan {
        lockfile: lockfile.to_string(),
        dir: dir.clone(),
        ..Default::default()
    };
    let Some(row) = row_of(lockfile) else {
        plan.none = format!("`{lockfile}` is not a lockfile CoderHelm installs from");
        return plan;
    };
    plan.manager = row.manager.to_string();
    if !is_plain_path(&dir) {
        plan.none = format!("the project folder `{dir}` can't be named in a sandbox command");
        return plan;
    }
    let text = |name: &str| -> Option<&str> {
        let path = under(&dir, name);
        files
            .iter()
            .find(|(p, _)| *p == path)
            .map(|(_, t)| t.as_str())
    };
    let has_file = |name: &str| tree.contains(&under(&dir, name));
    let manifest_text = row.manifest.and_then(text);
    let manifest: Option<serde_json::Value> =
        manifest_text.and_then(|t| serde_json::from_str(t).ok());
    // Yarn 2 and later installs with `--immutable`.
    let berry = row.manager == "yarn" && has_file(".yarnrc.yml");
    let install = if berry {
        "yarn install --immutable"
    } else {
        row.install
    };
    let ensure = match row.manager {
        "pnpm" => ensure_manager(Js::Pnpm),
        "yarn" => ensure_manager(Js::Yarn),
        "bun" => ensure_manager(Js::Bun),
        _ => row.ensure,
    };
    plan.declared = manifest_text
        .and_then(|t| declared_version(t, row.manager))
        .unwrap_or_default();
    plan.checks.push(Check {
        kind: Kind::Install,
        shown: install.to_string(),
        cmd: format!("{ensure}{install}"),
        found: if plan.declared.is_empty() {
            format!(
                "{}'s frozen-lockfile install, with the sandbox's {} (the manifest declares no version)",
                row.manager, row.manager
            )
        } else {
            format!(
                "{}'s frozen-lockfile install, with {} {} as `{}` declares",
                row.manager,
                row.manager,
                plan.declared,
                under(&dir, row.manifest.unwrap_or("the manifest"))
            )
        },
    });
    // The version the deployment image installs, when it pins another.
    let pinned = files
        .iter()
        .filter(|(p, _)| is_dockerfile(p))
        .find_map(|(p, t)| image_version(t, row.manager).map(|v| (p.clone(), v)));
    if let Some((file, v)) = pinned {
        plan.image = v.clone();
        plan.image_file = file.clone();
        if v == plan.declared {
            plan.not_applicable.push((
                Kind::DeployInstall,
                format!(
                    "`{file}` installs {} {v}, the version the manifest declares: one install covers both",
                    row.manager
                ),
            ));
        } else if row.at_version.is_empty() || berry {
            plan.not_applicable.push((
                Kind::DeployInstall,
                format!(
                    "`{file}` installs {} {v}; CoderHelm's sandbox has no way to run that exact version of {}, so the frozen install was not run with it",
                    row.manager, row.manager
                ),
            ));
        } else {
            let cmd = row.at_version.replace("{v}", &v);
            plan.checks.push(Check {
                kind: Kind::DeployInstall,
                shown: cmd.clone(),
                cmd,
                found: format!("`{file}` installs {} {v}", row.manager),
            });
        }
    } else {
        plan.not_applicable.push((
            Kind::DeployInstall,
            "no Dockerfile of the project pins a package manager version".to_string(),
        ));
    }
    for kind in [Kind::Typecheck, Kind::Build, Kind::Lint, Kind::Tests] {
        let mut found: Option<Check> = None;
        for f in row.checks.iter().filter(|f| f.kind == kind) {
            let by_file = f.files.iter().find(|n| has_file(n));
            let by_mark = f
                .marks
                .iter()
                .find(|(file, needle)| text(file).is_some_and(|t| t.contains(needle)));
            let script = manifest
                .as_ref()
                .and_then(|m| f.scripts.iter().find(|s| script_of(m, s).is_some()));
            // The project has the check when its configuration is there
            // (whatever CI or the scripts do with it), or a script runs it.
            let applies = f.always || by_file.is_some() || by_mark.is_some() || script.is_some();
            if !applies {
                continue;
            }
            let (shown, source) = match (script, by_file, by_mark) {
                (Some(s), _, _) => (
                    format!("{} {s}", row.run_script),
                    format!(
                        "the `{s}` script of `{}`",
                        under(&dir, row.manifest.unwrap_or("the manifest"))
                    ),
                ),
                (None, Some(file), _) => (
                    format!("{}{}", row.run_tool, f.cmd),
                    format!("`{}` is there and no script runs it", under(&dir, file)),
                ),
                (None, None, Some((file, needle))) => (
                    format!("{}{}", row.run_tool, f.cmd),
                    format!("`{needle}` in `{}`", under(&dir, file)),
                ),
                (None, None, None) => (
                    format!("{}{}", row.run_tool, f.cmd),
                    format!("{}'s own {}", row.manager, kind.name()),
                ),
            };
            found = Some(Check {
                kind,
                cmd: shown.clone(),
                shown,
                found: source,
            });
            break;
        }
        match found {
            Some(c) => plan.checks.push(c),
            None => {
                let why = row
                    .never
                    .iter()
                    .find(|(k, _)| *k == kind)
                    .map(|(_, w)| w.to_string())
                    .unwrap_or_else(|| match (kind, row.manifest) {
                        (Kind::Typecheck, Some(_)) => {
                            "no `tsconfig.json`: the project is not typed".to_string()
                        }
                        (_, Some(m)) => format!(
                            "`{}` has no script for it and the project has no configuration file of a tool that runs it",
                            under(&dir, m)
                        ),
                        _ => "the project has no configuration file of a tool that runs it"
                            .to_string(),
                    });
                plan.not_applicable.push((kind, why));
            }
        }
    }
    match audit {
        Some((shown, cmd)) => plan.checks.push(Check {
            kind: Kind::Audit,
            shown: shown.to_string(),
            cmd: cmd.to_string(),
            found: format!("{}'s audit of production dependencies", row.manager),
        }),
        None => plan.not_applicable.push((
            Kind::Audit,
            format!(
                "CoderHelm has no production-scoped audit for {}; the audit of every dependency runs in the settle stage",
                row.manager
            ),
        )),
    }
    plan
}

// ── The time one sandbox build has ──────────────────────────────────────

/// What a sandbox build spends before the first check: the snapshot of
/// the branch and the image's start.
pub const OVERHEAD_SECS: u32 = 60;

/// What fits in `secs` of one sandbox build: the checks to run (indices
/// into `plan.checks`, in the plan's order) and the ones left out, each
/// with why. Checks are kept in priority order ([`Kind::priority`]); the
/// install comes first, and without room for it nothing runs.
pub fn fit(plan: &Plan, secs: u32) -> (Vec<usize>, Vec<(usize, String)>) {
    let room = secs.saturating_sub(OVERHEAD_SECS);
    let mut order: Vec<usize> = (0..plan.checks.len()).collect();
    order.sort_by_key(|&i| (plan.checks[i].kind.priority(), i));
    let mut used = 0u32;
    let mut run: Vec<usize> = vec![];
    let mut left: Vec<(usize, String)> = vec![];
    let mut installed = true;
    for i in order {
        let k = plan.checks[i].kind;
        let cost = k.seconds();
        if k.needs_install() && !installed {
            left.push((
                i,
                "the install it needs did not fit in this sandbox build".to_string(),
            ));
            continue;
        }
        if used + cost <= room {
            used += cost;
            run.push(i);
        } else {
            if k == Kind::Install {
                installed = false;
            }
            left.push((
                i,
                format!(
                    "the sandbox build had {} seconds left for checks and this one is given {}; the checks before it in priority took {}",
                    room, cost, used
                ),
            ));
        }
    }
    run.sort();
    left.sort_by_key(|(i, _)| *i);
    (run, left)
}

// ── The command and its output ──────────────────────────────────────────

/// What each check's line in the output starts with.
pub const MARK: &str = "CODERHELM_VERIFY";
/// The line that says which version of the package manager installed.
pub const PM_MARK: &str = "CODERHELM_VERIFY_PM";
/// The exit code `timeout` gives a command it stopped.
const TIMED_OUT: i32 = 124;

/// The command that prints a manager's own version.
fn version_cmd(manager: &str) -> Option<&'static str> {
    Some(match manager {
        "npm" => "npm --version",
        "pnpm" => "pnpm --version",
        "yarn" => "yarn --version",
        "bun" => "bun --version",
        "poetry" => "poetry --version",
        "uv" => "uv --version",
        "pipenv" => "pipenv --version",
        "cargo" => "cargo --version",
        "bundler" => "bundle --version",
        "go" => "go version",
        _ => return None,
    })
}

/// The one sandbox command that runs the checks `run` of `plan`, each on
/// its own with a time limit, so a failing or slow one never hides the
/// next. A failed check's last output is printed, then one
/// `CODERHELM_VERIFY <index> <exit code>` line per check, last, where the
/// sandbox's kept output always holds it (`skip` for one whose install
/// failed). `after_audit` is printed by the audit's own reducer
/// (`dep_audit::listing`), given the audit's output file. Always exits 0.
pub fn script(plan: &Plan, run: &[usize], audit_reducer: &str) -> String {
    let mut s = String::new();
    let per = 2400 / run.len().max(1);
    let install = run
        .iter()
        .copied()
        .find(|&i| plan.checks[i].kind == Kind::Install);
    let cd = if plan.dir.is_empty() {
        String::new()
    } else {
        format!("cd '{}' && ", plan.dir)
    };
    for &i in run {
        let c = &plan.checks[i];
        let limit = c.kind.seconds() * 2;
        let body = format!("{cd}{}", c.cmd);
        let one = format!(
            "echo '{}' | base64 -d > /tmp/coderhelm-verify-{i}.sh; \
             timeout {limit} sh /tmp/coderhelm-verify-{i}.sh > /tmp/coderhelm-verify-{i}.log 2>&1; ch_v{i}=$?",
            b64(body.as_bytes())
        );
        match install {
            Some(ins) if c.kind.needs_install() => s.push_str(&format!(
                "if [ \"$ch_v{ins}\" = 0 ]; then {one}; else ch_v{i}=skip; fi\n"
            )),
            _ => s.push_str(&format!("{one}\n")),
        }
    }
    for &i in run {
        let c = &plan.checks[i];
        if c.kind == Kind::Audit {
            continue;
        }
        s.push_str(&format!(
            "if [ \"$ch_v{i}\" != 0 ] && [ \"$ch_v{i}\" != skip ]; then echo \"--- {} failed (exit $ch_v{i}), last output ---\"; tail -c {per} /tmp/coderhelm-verify-{i}.log; echo; fi\n",
            c.kind.name()
        ));
    }
    if let Some(i) = run
        .iter()
        .copied()
        .find(|&i| plan.checks[i].kind == Kind::Audit)
    {
        if !audit_reducer.is_empty() {
            s.push_str(&audit_reducer.replace("{out}", &format!("/tmp/coderhelm-verify-{i}.log")));
            s.push('\n');
        }
    }
    if let Some(v) = version_cmd(&plan.manager) {
        s.push_str(&format!(
            "echo \"{PM_MARK} $( ({cd}{v}) 2>/dev/null | head -n 1 | tr -cd '0-9A-Za-z. -' | cut -c1-40)\"\n"
        ));
    }
    for &i in run {
        s.push_str(&format!("echo \"{MARK} {i} $ch_v{i}\"\n"));
    }
    s.push_str("true");
    s
}

/// What one check came to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case", tag = "is")]
pub enum Status {
    Passed,
    Failed {
        exit: i32,
    },
    /// Stopped at its time limit, in seconds.
    TimedOut {
        after: u32,
    },
    /// Not run, with why: never a pass and never a failure.
    #[default]
    NotRun,
}

/// One check with its result on one head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Outcome {
    pub kind: Kind,
    pub shown: String,
    pub found: String,
    pub status: Status,
    /// Why it did not run; empty when it did.
    pub why: String,
}

impl Outcome {
    pub fn ran(&self) -> bool {
        !matches!(self.status, Status::NotRun)
    }
    pub fn passed(&self) -> bool {
        matches!(self.status, Status::Passed)
    }
    /// It ran to its end and failed. A check stopped at its time limit has
    /// no result: never a failure to blame on a change.
    pub fn failed(&self) -> bool {
        matches!(self.status, Status::Failed { .. })
    }
}

/// The minimum set's results on one commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Verified {
    /// The commit the checks ran on.
    pub sha: String,
    pub lockfile: String,
    pub manager: String,
    /// The package manager version that installed, as it printed it.
    pub used: String,
    pub declared: String,
    pub image: String,
    pub image_file: String,
    pub outcomes: Vec<Outcome>,
    pub not_applicable: Vec<(Kind, String)>,
    /// The sandbox gave a result (false: nothing below is a verdict).
    pub ran: bool,
    /// The end of the output, for a reason.
    pub tail: String,
}

/// The record when the sandbox gave no result: every check not run, with
/// why.
pub fn no_result(plan: &Plan, sha: &str, why: &str) -> Verified {
    read(plan, sha, &[], &[], "").with_reason(why)
}

impl Verified {
    /// Every check not run, for this one reason.
    pub fn with_reason(mut self, why: &str) -> Self {
        for o in &mut self.outcomes {
            o.why = why.to_string();
        }
        self.tail = why.to_string();
        self
    }

    pub fn outcome(&self, kind: Kind) -> Option<&Outcome> {
        self.outcomes.iter().find(|o| o.kind == kind)
    }

    /// The kinds of the set with no result on this commit, each with why.
    pub fn not_run(&self) -> Vec<(Kind, String)> {
        self.outcomes
            .iter()
            .filter(|o| !o.ran())
            .map(|o| (o.kind, o.why.clone()))
            .collect()
    }
}

/// A [`script`] run's results. `run`: the checks the script ran; `left`:
/// the ones [`fit`] left out, with why. A check the script ran that has no
/// line (the build was cut short) did not run.
pub fn read(
    plan: &Plan,
    sha: &str,
    run: &[usize],
    left: &[(usize, String)],
    output: &str,
) -> Verified {
    let mut codes: Vec<Option<String>> = vec![None; plan.checks.len()];
    let mut used = String::new();
    for line in output.lines() {
        let mut parts = line.split_whitespace();
        match parts.next() {
            Some(MARK) => {
                if let (Some(i), Some(code)) = (
                    parts.next().and_then(|n| n.parse::<usize>().ok()),
                    parts.next(),
                ) {
                    if i < codes.len() {
                        codes[i] = Some(code.to_string());
                    }
                }
            }
            Some(PM_MARK) => used = parts.collect::<Vec<_>>().join(" "),
            _ => {}
        }
    }
    let any = codes.iter().any(Option::is_some);
    let outcomes = plan
        .checks
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let (status, why) = match (codes[i].as_deref(), run.contains(&i)) {
                (Some("skip"), _) => (
                    Status::NotRun,
                    "the frozen install failed, so it could not run".to_string(),
                ),
                (Some(code), _) => match code.parse::<i32>() {
                    Ok(0) => (Status::Passed, String::new()),
                    Ok(TIMED_OUT) => (
                        Status::TimedOut {
                            after: c.kind.seconds() * 2,
                        },
                        String::new(),
                    ),
                    Ok(exit) => (Status::Failed { exit }, String::new()),
                    Err(_) => (Status::NotRun, "its result could not be read".to_string()),
                },
                (None, true) => (
                    Status::NotRun,
                    "the sandbox build ended before it reported".to_string(),
                ),
                (None, false) => (
                    Status::NotRun,
                    left.iter()
                        .find(|(l, _)| *l == i)
                        .map(|(_, w)| w.clone())
                        .unwrap_or_else(|| "it was not started".to_string()),
                ),
            };
            Outcome {
                kind: c.kind,
                shown: c.shown.clone(),
                found: c.found.clone(),
                status,
                why,
            }
        })
        .collect();
    Verified {
        sha: sha.to_string(),
        lockfile: plan.lockfile.clone(),
        manager: plan.manager.clone(),
        used,
        declared: plan.declared.clone(),
        image: plan.image.clone(),
        image_file: plan.image_file.clone(),
        outcomes,
        not_applicable: plan.not_applicable.clone(),
        ran: any,
        tail: crate::tail_str(output, 2000).to_string(),
    }
}

/// The checks that fail on the head and pass on the base branch, of the
/// kinds a change can be blamed for. Without a base result nothing is
/// blamed: a failure is then shown as not compared.
pub fn blamed<'a>(head: &'a Verified, base: Option<&Verified>) -> Vec<&'a Outcome> {
    let Some(base) = base else {
        return vec![];
    };
    head.outcomes
        .iter()
        .filter(|o| o.kind.blames() && o.failed())
        .filter(|o| base.outcome(o.kind).is_some_and(Outcome::passed))
        .collect()
}

/// Whether the base branch needs a run: the head has a failure a change
/// can be blamed for.
pub fn needs_base(head: &Verified) -> bool {
    head.outcomes.iter().any(|o| o.kind.blames() && o.failed())
}

fn short(sha: &str) -> &str {
    &sha[..sha.len().min(7)]
}

/// The record as the pull request states it: every check with the command
/// that ran and what it came to on this commit, what the base branch did
/// where the head failed, what the project does not have, and what did not
/// run with why.
pub fn section(head: &Verified, base: Option<&Verified>) -> String {
    let mut s = format!(
        "**Verified in the sandbox on commit `{}`** (the set a dependency change needs, found in `{}`'s project, whatever CI runs):\n",
        short(&head.sha),
        head.lockfile
    );
    if !head.ran {
        s.push_str(&format!(
            "- No result: {}. Nothing here claims a check passed.\n",
            crate::truncate_str(
                head.tail.lines().next().unwrap_or("the sandbox gave none"),
                200
            )
        ));
        return s;
    }
    for o in &head.outcomes {
        let verdict = match &o.status {
            // An audit that lists advisories is a result, not a failure:
            // they are counted with the advisories of this commit.
            Status::Passed if o.kind == Kind::Audit => {
                "lists no advisory in production dependencies".to_string()
            }
            Status::Failed { exit } if o.kind == Kind::Audit => format!(
                "lists advisories (exit {exit}); they are counted with the advisories of this commit"
            ),
            Status::Passed => "passes".to_string(),
            Status::Failed { exit } => {
                let on_base = base.and_then(|b| b.outcome(o.kind));
                match on_base {
                    Some(b) if b.passed() => {
                        format!("fails (exit {exit}); it passes on the base branch")
                    }
                    Some(b) if b.ran() => format!(
                        "fails (exit {exit}); it fails on the base branch too, so not caused by this change"
                    ),
                    _ => format!("fails (exit {exit}); not compared with the base branch"),
                }
            }
            Status::TimedOut { after } => {
                format!("stopped at its time limit of {after} seconds: no result")
            }
            Status::NotRun => format!("not run: {}", o.why),
        };
        let with = match o.kind {
            Kind::Install if !head.used.is_empty() => format!(" (ran with {})", head.used),
            _ => String::new(),
        };
        s.push_str(&format!(
            "- {}: `{}`{with}: {verdict}. Found as {}.\n",
            o.kind.name(),
            o.shown,
            o.found
        ));
    }
    for (k, why) in &head.not_applicable {
        s.push_str(&format!("- {}: does not apply: {why}.\n", k.name()));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(paths: &[&str]) -> Vec<String> {
        paths.iter().map(|p| p.to_string()).collect()
    }

    fn files(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(p, t)| (p.to_string(), t.to_string()))
            .collect()
    }

    const PNPM_AUDIT: Option<(&str, &str)> =
        Some(("pnpm audit --prod --json", "pnpm audit --prod --json"));

    /// The review's project: typed, with a build and tests, a CI with no
    /// type check, and a Dockerfile that installs another pnpm.
    fn typed_service() -> Plan {
        let t = tree(&[
            "package.json",
            "pnpm-lock.yaml",
            "tsconfig.json",
            "Dockerfile",
            "src/main.ts",
        ]);
        let f = files(&[
            (
                "package.json",
                r#"{"packageManager":"pnpm@9.12.3+sha512.abc","scripts":{"build":"nest build","test":"jest","lint":"eslint ."}}"#,
            ),
            (
                "Dockerfile",
                "FROM node:22-slim\nRUN npm install -g pnpm@9.1.0\nCOPY . .\nRUN pnpm install --frozen-lockfile\n",
            ),
        ]);
        discover("pnpm-lock.yaml", &t, &f, PNPM_AUDIT)
    }

    fn kinds(p: &Plan) -> Vec<Kind> {
        p.checks.iter().map(|c| c.kind).collect()
    }

    #[test]
    fn a_typed_project_is_type_checked_though_no_script_or_ci_job_does() {
        let p = typed_service();
        assert_eq!(
            kinds(&p),
            vec![
                Kind::Install,
                Kind::DeployInstall,
                Kind::Typecheck,
                Kind::Build,
                Kind::Lint,
                Kind::Tests,
                Kind::Audit
            ]
        );
        let tc = &p.checks[2];
        assert_eq!(tc.shown, "npx --no-install tsc --noEmit");
        assert_eq!(tc.found, "`tsconfig.json` is there and no script runs it");
        // The frozen flag is written out, with the manifest's version and
        // then with the image's.
        assert!(p.checks[0].cmd.ends_with("pnpm install --frozen-lockfile"));
        assert!(p.checks[0]
            .found
            .contains("with pnpm 9.12.3 as `package.json` declares"));
        assert_eq!(
            p.checks[1].shown,
            "npx -y pnpm@9.1.0 install --frozen-lockfile --ignore-scripts"
        );
        assert_eq!(p.checks[1].found, "`Dockerfile` installs pnpm 9.1.0");
        assert_eq!(p.checks[3].shown, "pnpm run build");
        assert_eq!(p.checks[5].shown, "pnpm run test");
        assert_eq!(p.checks[6].shown, "pnpm audit --prod --json");
    }

    #[test]
    fn a_check_the_project_does_not_have_is_named_with_why() {
        let t = tree(&["package.json", "package-lock.json", "index.js"]);
        let f = files(&[(
            "package.json",
            r#"{"scripts":{"test":"echo \"Error: no test specified\" && exit 1"}}"#,
        )]);
        let p = discover("package-lock.json", &t, &f, None);
        assert_eq!(kinds(&p), vec![Kind::Install]);
        assert_eq!(p.checks[0].shown, "npm ci --no-audit --no-fund");
        let why = |k: Kind| {
            p.not_applicable
                .iter()
                .find(|(x, _)| *x == k)
                .map(|(_, w)| w.as_str())
                .unwrap()
        };
        assert_eq!(
            why(Kind::Typecheck),
            "no `tsconfig.json`: the project is not typed"
        );
        assert!(why(Kind::Tests).contains("has no script for it"));
        assert!(why(Kind::DeployInstall).contains("no Dockerfile"));
        assert!(why(Kind::Audit).contains("no production-scoped audit"));
    }

    #[test]
    fn the_image_version_is_read_from_every_way_a_dockerfile_pins_it() {
        assert_eq!(
            image_version("RUN corepack prepare pnpm@9.1.0 --activate", "pnpm"),
            Some("9.1.0".into())
        );
        assert_eq!(
            image_version("ENV PNPM_VERSION=8.15.4\nRUN curl x", "pnpm"),
            Some("8.15.4".into())
        );
        assert_eq!(
            image_version("ARG POETRY_VERSION=\"1.8.2\"", "poetry"),
            Some("1.8.2".into())
        );
        assert_eq!(
            image_version("RUN pip install poetry==1.8.2", "poetry"),
            Some("1.8.2".into())
        );
        // Another package's name that ends with the manager's is not it.
        assert_eq!(image_version("RUN npm i -g not-pnpm@1.2.3", "pnpm"), None);
        assert_eq!(image_version("# pnpm@9.1.0\nRUN true", "pnpm"), None);
        assert_eq!(
            declared_version(r#"{"packageManager":"yarn@1.22.19"}"#, "yarn"),
            Some("1.22.19".into())
        );
        assert_eq!(
            declared_version(r#"{"packageManager":"yarn@1.22.19"}"#, "pnpm"),
            None
        );
    }

    #[test]
    fn the_same_image_version_needs_one_install_and_an_unrunnable_one_is_said() {
        let t = tree(&["package.json", "pnpm-lock.yaml", "Dockerfile"]);
        let same = files(&[
            ("package.json", r#"{"packageManager":"pnpm@9.1.0"}"#),
            ("Dockerfile", "RUN npm i -g pnpm@9.1.0"),
        ]);
        let p = discover("pnpm-lock.yaml", &t, &same, None);
        assert!(!kinds(&p).contains(&Kind::DeployInstall));
        assert!(p.not_applicable[0].1.contains("one install covers both"));
        // A Python image that pins another Poetry: named, never run as if.
        let t = tree(&["pyproject.toml", "poetry.lock", "Dockerfile"]);
        let f = files(&[
            ("pyproject.toml", "[tool.mypy]\nstrict = true\n"),
            ("Dockerfile", "RUN pip install poetry==1.8.2"),
        ]);
        let p = discover("poetry.lock", &t, &f, None);
        assert!(!kinds(&p).contains(&Kind::DeployInstall));
        let (_, why) = &p.not_applicable[0];
        assert!(why.contains("installs poetry 1.8.2"));
        assert!(why.contains("was not run with it"));
    }

    #[test]
    fn every_ecosystem_finds_its_checks_in_the_project() {
        // Python: typed by a table in pyproject.toml, tests by conftest.
        let t = tree(&["pyproject.toml", "uv.lock", "conftest.py", "app/main.py"]);
        let f = files(&[(
            "pyproject.toml",
            "[tool.pyright]\ninclude = [\"app\"]\n[tool.ruff.lint]\nselect = [\"E\"]\n",
        )]);
        let p = discover("uv.lock", &t, &f, None);
        let shown: Vec<&str> = p.checks.iter().map(|c| c.shown.as_str()).collect();
        assert_eq!(
            shown,
            vec![
                "uv sync --locked",
                "uv run pyright",
                "uv run python -m compileall -q .",
                "uv run ruff check .",
                "uv run pytest -q"
            ]
        );
        assert_eq!(p.checks[1].found, "`[tool.pyright]` in `pyproject.toml`");
        // An untyped Python project says so.
        let p = discover("uv.lock", &tree(&["pyproject.toml", "uv.lock"]), &[], None);
        assert!(p
            .not_applicable
            .iter()
            .any(|(k, w)| *k == Kind::Typecheck && w.contains("not type-checked")));
        // Rust and Go: the toolchain's own, always.
        let p = discover(
            "Cargo.lock",
            &tree(&["Cargo.lock", "Cargo.toml"]),
            &[],
            None,
        );
        let shown: Vec<&str> = p.checks.iter().map(|c| c.shown.as_str()).collect();
        assert_eq!(
            shown,
            vec![
                "cargo fetch --locked",
                "cargo check --locked --all-targets",
                "cargo build --locked",
                "cargo clippy --locked --all-targets -- -D warnings",
                "cargo test --locked"
            ]
        );
        let p = discover(
            "svc/go.sum",
            &tree(&["svc/go.sum", "svc/go.mod"]),
            &[],
            None,
        );
        assert_eq!(p.dir, "svc");
        assert_eq!(p.checks[1].shown, "go vet ./...");
        assert!(p.not_applicable.iter().any(|(k, _)| *k == Kind::Lint));
        // Ruby: found by its configuration files.
        let p = discover(
            "Gemfile.lock",
            &tree(&["Gemfile.lock", ".rubocop.yml", ".rspec"]),
            &[],
            None,
        );
        let shown: Vec<&str> = p.checks.iter().map(|c| c.shown.as_str()).collect();
        assert_eq!(
            shown,
            vec![
                "BUNDLE_FROZEN=true bundle install",
                "bundle exec rubocop",
                "bundle exec rspec"
            ]
        );
        // Yarn 2 and later installs immutably.
        let p = discover(
            "yarn.lock",
            &tree(&["yarn.lock", "package.json", ".yarnrc.yml"]),
            &files(&[("package.json", "{}")]),
            None,
        );
        assert_eq!(p.checks[0].shown, "yarn install --immutable");
        // Every manager a lockfile is written for has a row.
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
                row_of(lock).map(|r| r.manager),
                crate::lockfile_sync::lock_manager(lock),
                "{lock}"
            );
        }
    }

    #[test]
    fn the_files_read_are_the_manifest_the_marked_files_and_the_dockerfiles() {
        let t = tree(&[
            "apps/api/package.json",
            "apps/api/pnpm-lock.yaml",
            "apps/api/Dockerfile",
            "Dockerfile",
            "apps/web/Dockerfile",
        ]);
        assert_eq!(
            files_to_read("apps/api/pnpm-lock.yaml", &t),
            vec!["Dockerfile", "apps/api/Dockerfile", "apps/api/package.json"]
        );
    }

    #[test]
    fn what_does_not_fit_is_left_out_in_priority_order_and_named() {
        let p = typed_service();
        // A full window: everything.
        let (run, left) = fit(&p, 900);
        assert_eq!(run, vec![0, 1, 2, 3, 4, 5, 6]);
        assert!(left.is_empty());
        // Short: the install, the type check and the audit stay; the
        // deployment install is the first to go, then lint, tests, build.
        let (run, left) = fit(&p, 60 + 150 + 75 + 30 + 150);
        let kept: Vec<Kind> = run.iter().map(|&i| p.checks[i].kind).collect();
        assert_eq!(
            kept,
            vec![Kind::Install, Kind::Typecheck, Kind::Build, Kind::Audit]
        );
        let gone: Vec<Kind> = left.iter().map(|(i, _)| p.checks[*i].kind).collect();
        assert_eq!(gone, vec![Kind::DeployInstall, Kind::Lint, Kind::Tests]);
        assert!(left[0].1.contains("seconds left for checks"));
        // No room for the install: nothing that needs it runs, and each
        // says so. The audit needs no install.
        let (run, left) = fit(&p, 60 + 40);
        assert_eq!(
            run.iter().map(|&i| p.checks[i].kind).collect::<Vec<_>>(),
            vec![Kind::Audit]
        );
        assert!(left
            .iter()
            .any(|(i, w)| p.checks[*i].kind == Kind::Typecheck && w.contains("install it needs")));
    }

    #[test]
    fn each_result_is_recorded_against_the_commit_and_nothing_unrun_is_claimed() {
        let p = typed_service();
        let (run, left) = fit(&p, 60 + 150 + 75 + 30 + 150);
        let cmd = script(&p, &run, "");
        // Each command travels encoded, with its own time limit; the marks
        // come last.
        assert!(cmd.contains("timeout 300 sh /tmp/coderhelm-verify-0.sh"));
        assert!(cmd.contains("if [ \"$ch_v0\" = 0 ]; then"));
        assert!(cmd.trim_end().ends_with("true"));
        assert!(!cmd.contains("tsc --noEmit"));
        let out = "CODERHELM_VERIFY_PM 9.12.3\nCODERHELM_VERIFY 0 0\nCODERHELM_VERIFY 2 2\nCODERHELM_VERIFY 3 124\nCODERHELM_VERIFY 6 1\n";
        let v = read(&p, "abc1234def", &run, &left, out);
        assert!(v.ran);
        assert_eq!(v.used, "9.12.3");
        assert!(v.outcome(Kind::Install).unwrap().passed());
        assert_eq!(
            v.outcome(Kind::Typecheck).unwrap().status,
            Status::Failed { exit: 2 }
        );
        assert_eq!(
            v.outcome(Kind::Build).unwrap().status,
            Status::TimedOut { after: 300 }
        );
        // What did not fit is not run, with why; never a pass.
        let not_run: Vec<Kind> = v.not_run().into_iter().map(|(k, _)| k).collect();
        assert_eq!(not_run, vec![Kind::DeployInstall, Kind::Lint, Kind::Tests]);
        let text = section(&v, None);
        assert!(text.contains("on commit `abc1234`"));
        assert!(text.contains(
            "- type check: `npx --no-install tsc --noEmit`: fails (exit 2); not compared with the base branch."
        ));
        assert!(text.contains("- unit tests: `pnpm run test`: not run: the sandbox build had"));
        assert!(text.contains("stopped at its time limit of 300 seconds: no result"));
        assert!(!text.contains("unit tests: `pnpm run test`: passes"));
        // A build cut short: what it ran and did not report is not run.
        let cut = read(&p, "abc", &run, &left, "CODERHELM_VERIFY 0 0\n");
        assert_eq!(
            cut.outcome(Kind::Typecheck).unwrap().why,
            "the sandbox build ended before it reported"
        );
        // A failed install: the checks that need it say so.
        let failed = read(&p, "abc", &run, &left, "CODERHELM_VERIFY 0 1\nCODERHELM_VERIFY 2 skip\nCODERHELM_VERIFY 3 skip\nCODERHELM_VERIFY 6 0\n");
        assert_eq!(
            failed.outcome(Kind::Typecheck).unwrap().why,
            "the frozen install failed, so it could not run"
        );
        // No output at all: no result, and the section claims nothing.
        let none = no_result(&p, "abc", "the sandbox build timed out");
        assert!(!none.ran);
        assert!(section(&none, None).contains("Nothing here claims a check passed"));
    }

    #[test]
    fn a_failure_the_base_branch_has_too_is_never_blamed_on_the_change() {
        let p = typed_service();
        let all: Vec<usize> = (0..p.checks.len()).collect();
        let head = read(
            &p,
            "head",
            &all,
            &[],
            "CODERHELM_VERIFY 0 0\nCODERHELM_VERIFY 1 0\nCODERHELM_VERIFY 2 2\nCODERHELM_VERIFY 3 0\nCODERHELM_VERIFY 4 1\nCODERHELM_VERIFY 5 1\nCODERHELM_VERIFY 6 1\n",
        );
        assert!(needs_base(&head));
        // Without the base nothing is blamed.
        assert!(blamed(&head, None).is_empty());
        let base = read(
            &p,
            "base",
            &all,
            &[],
            "CODERHELM_VERIFY 0 0\nCODERHELM_VERIFY 1 0\nCODERHELM_VERIFY 2 0\nCODERHELM_VERIFY 3 0\nCODERHELM_VERIFY 4 0\nCODERHELM_VERIFY 5 1\nCODERHELM_VERIFY 6 1\n",
        );
        // The type check passes on the base and fails with the bumps:
        // blamed. The tests fail on both: not. Lint is never blamed, and an
        // audit that finds advisories is a result.
        let b: Vec<Kind> = blamed(&head, Some(&base)).iter().map(|o| o.kind).collect();
        assert_eq!(b, vec![Kind::Typecheck]);
        let text = section(&head, Some(&base));
        assert!(text.contains("type check: `npx --no-install tsc --noEmit`: fails (exit 2); it passes on the base branch"));
        assert!(text.contains("it fails on the base branch too, so not caused by this change"));
        let green = read(
            &p,
            "head",
            &all,
            &[],
            "CODERHELM_VERIFY 0 0\nCODERHELM_VERIFY 2 0\n",
        );
        assert!(!needs_base(&green));
        // A check stopped at its time limit has no result: nothing to
        // blame, and no base run asked for.
        let slow = read(
            &p,
            "head",
            &all,
            &[],
            "CODERHELM_VERIFY 0 0\nCODERHELM_VERIFY 5 124\n",
        );
        assert!(!needs_base(&slow));
        assert!(blamed(&slow, Some(&base)).is_empty());
    }
}
