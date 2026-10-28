//! Rule 2: know where the tests run before judging their data.
//!
//! Before a test-related run judges data or behaviour it reads the test
//! suite's environment: the runner config (base address and the variables
//! that override it), the suite's env files (only keys, and addresses with
//! any credentials removed; never other values), its setup files and the
//! data and fixtures it reads ([`read`]). That is recorded in the run's
//! evidence ([`TestEnvironment::markdown`]) and every agent evaluates data
//! in that environment. A difference between environments (production and
//! staging content) is not a contradiction, and a stop that cites one is
//! refused (see [`super::screen`]).

use serde::{Deserialize, Serialize};

use crate::e2e::test_package;

/// Runner config files, by name.
pub const CONFIG_NAMES: &[&str] = &[
    "playwright.config.ts",
    "playwright.config.js",
    "playwright.config.mjs",
    "playwright.config.cjs",
    "cypress.config.ts",
    "cypress.config.js",
    "cypress.config.mjs",
    "cypress.config.cjs",
    "cypress.json",
    "wdio.conf.ts",
    "wdio.conf.js",
    "vitest.config.ts",
    "vitest.config.js",
    "jest.config.ts",
    "jest.config.js",
];

/// Folder names whose files are test data.
const DATA_DIRS: &[&str] = &[
    "fixtures",
    "fixture",
    "test-data",
    "testdata",
    "test_data",
    "data",
    "seed",
    "seeds",
    "mocks",
];

const DATA_EXTS: &[&str] = &[".json", ".csv", ".yaml", ".yml", ".sql", ".ts", ".js"];

/// Config keys that point the suite at its setup or stored data.
const SETUP_KEYS: &[&str] = &[
    "globalSetup",
    "globalTeardown",
    "storageState",
    "setupFiles",
    "setupNodeEvents",
    "fixturesFolder",
    "testDataDir",
];

const MAX_DATA: usize = 15;
const MAX_VARS: usize = 20;

/// The rule every agent on a test-related run follows.
pub const RULE: &str = "Evaluate data and behaviour in the environment above: the base address \
the suite runs against (and the variables that override it) and the data and fixtures it reads. \
Look records up there, not in production. A difference between environments (production and \
staging content, two datasets) is not a contradiction and never a reason to stop or ask; a stop \
that cites one is refused.";

/// A value read from a file, with where.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Setting {
    pub value: String,
    /// `path:line`.
    pub at: String,
}

/// Where the tests run and what they read.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestEnvironment {
    /// The runner configs read.
    #[serde(default)]
    pub config_files: Vec<String>,
    /// Base addresses written in the configs.
    #[serde(default)]
    pub base_urls: Vec<Setting>,
    /// Variables that override the base address.
    #[serde(default)]
    pub overrides: Vec<Setting>,
    /// Every other variable the configs read.
    #[serde(default)]
    pub env_vars: Vec<Setting>,
    /// What the suite's env files set: `KEY=address` for addresses (no
    /// credentials), `KEY` alone for anything else.
    #[serde(default)]
    pub env_files: Vec<Setting>,
    /// Setup and stored-state settings in the configs.
    #[serde(default)]
    pub setup: Vec<Setting>,
    /// Data and fixture files the suite reads.
    #[serde(default)]
    pub data: Vec<String>,
    /// The team's test address for the repository (Tests settings), the one
    /// CoderHelm's repeat runs use.
    #[serde(default)]
    pub target_url: String,
}

fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() || dir == "." {
        name.to_string()
    } else {
        format!("{dir}/{name}")
    }
}

/// The suite folders for `hints` (spec files, failing test files): each
/// one's package folder, and the root.
pub fn suite_dirs(tree: &[String], hints: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for h in hints {
        let h = h.trim_start_matches("./");
        let found = tree
            .iter()
            .find(|t| *t == h || t.ends_with(&format!("/{h}")))
            .map(String::as_str)
            .unwrap_or(h);
        if let Some(d) = test_package::package_dir(tree, found) {
            if !out.contains(&d) {
                out.push(d);
            }
        }
    }
    if !out.iter().any(String::is_empty) {
        out.push(String::new());
    }
    out
}

/// The runner configs in `dirs` that exist in `tree`.
pub fn config_paths(tree: &[String], dirs: &[String]) -> Vec<String> {
    dirs.iter()
        .flat_map(|d| CONFIG_NAMES.iter().map(move |n| join(d, n)))
        .filter(|p| tree.contains(p))
        .collect()
}

/// The env files (`.env`, `.env.*`) in `dirs` that exist in `tree`.
pub fn env_paths(tree: &[String], dirs: &[String]) -> Vec<String> {
    tree.iter()
        .filter(|p| {
            let (dir, name) = p.rsplit_once('/').unwrap_or(("", p.as_str()));
            dirs.iter().any(|d| d == dir) && (name == ".env" || name.starts_with(".env."))
        })
        .cloned()
        .collect()
}

/// String literals on a line.
fn literals(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = line.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if !matches!(c, '\'' | '"' | '`') {
            continue;
        }
        let rest = &line[i + 1..];
        if let Some(end) = rest.find(c) {
            out.push(rest[..end].to_string());
            for _ in 0..rest[..=end].chars().count() {
                chars.next();
            }
        }
    }
    out
}

/// Environment variables a line reads (`process.env.X`,
/// `process.env['X']`, `import.meta.env.X`, `Cypress.env('X')`).
fn env_reads(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    for marker in ["process.env.", "import.meta.env."] {
        for part in line.split(marker).skip(1) {
            let name: String = part
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                out.push(name);
            }
        }
    }
    for marker in ["process.env[", "Cypress.env("] {
        for part in line.split(marker).skip(1) {
            if let Some(name) = literals(part).into_iter().next() {
                if !name.is_empty() {
                    out.push(name);
                }
            }
        }
    }
    out
}

/// An address with any `user:password@` removed; `None` when it isn't one.
fn clean_url(s: &str) -> Option<String> {
    let s = s.trim();
    let (scheme, rest) = s.split_once("://")?;
    if !matches!(scheme, "http" | "https") || rest.is_empty() {
        return None;
    }
    let (host, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let host = host.rsplit_once('@').map(|(_, h)| h).unwrap_or(host);
    Some(format!("{scheme}://{host}{path}"))
}

fn push(list: &mut Vec<Setting>, value: String, at: String, max: usize) {
    if list.len() < max && !list.iter().any(|s| s.value == value) {
        list.push(Setting { value, at });
    }
}

/// Reads the suite's environment: `configs` and `envs` as (path, text),
/// `tree` for its data files, `dirs` the suite folders.
pub fn read(
    configs: &[(String, String)],
    envs: &[(String, String)],
    tree: &[String],
    dirs: &[String],
    target_url: &str,
) -> TestEnvironment {
    let mut env = TestEnvironment {
        config_files: configs.iter().map(|(p, _)| p.clone()).collect(),
        target_url: target_url.trim().to_string(),
        ..Default::default()
    };
    for (path, text) in configs {
        for (i, line) in text.lines().enumerate() {
            let at = format!("{path}:{}", i + 1);
            let lower = line.to_lowercase();
            let vars = env_reads(line);
            if lower.contains("baseurl") {
                for l in literals(line) {
                    if let Some(u) = clean_url(&l) {
                        push(&mut env.base_urls, u, at.clone(), MAX_VARS);
                    }
                }
                for v in &vars {
                    push(&mut env.overrides, v.clone(), at.clone(), MAX_VARS);
                }
                continue;
            }
            if let Some(k) = SETUP_KEYS.iter().find(|k| line.contains(*k)) {
                let shown = literals(line)
                    .into_iter()
                    .next()
                    .map(|v| format!("{k}: {v}"))
                    .unwrap_or_else(|| (*k).to_string());
                push(&mut env.setup, shown, at.clone(), MAX_VARS);
            }
            for v in vars {
                if !env.overrides.iter().any(|o| o.value == v) {
                    push(&mut env.env_vars, v, at.clone(), MAX_VARS);
                }
            }
        }
    }
    for (path, text) in envs {
        for (i, line) in text.lines().enumerate() {
            let l = line.trim().trim_start_matches("export ").trim();
            if l.starts_with('#') {
                continue;
            }
            let Some((key, value)) = l.split_once('=') else {
                continue;
            };
            let key = key.trim();
            if key.is_empty() || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                continue;
            }
            let value = value.trim().trim_matches(|c| c == '"' || c == '\'');
            let shown = match clean_url(value) {
                Some(u) => format!("{key}={u}"),
                None => key.to_string(),
            };
            push(
                &mut env.env_files,
                shown,
                format!("{path}:{}", i + 1),
                MAX_VARS,
            );
        }
    }
    for p in tree {
        if env.data.len() >= MAX_DATA {
            break;
        }
        let in_suite = dirs
            .iter()
            .any(|d| d.is_empty() || p.starts_with(&format!("{d}/")));
        let segs: Vec<&str> = p.split('/').collect();
        let data_dir = segs[..segs.len().saturating_sub(1)]
            .iter()
            .any(|s| DATA_DIRS.contains(s));
        let data_ext = DATA_EXTS.iter().any(|e| p.ends_with(e));
        let suite_only = dirs
            .iter()
            .any(|d| !d.is_empty() && p.starts_with(&format!("{d}/")))
            || segs
                .iter()
                .any(|s| matches!(*s, "e2e" | "tests" | "test" | "cypress" | "playwright"));
        if in_suite && data_dir && data_ext && suite_only {
            env.data.push(p.clone());
        }
    }
    env
}

impl TestEnvironment {
    /// Nothing was found.
    pub fn is_empty(&self) -> bool {
        self.config_files.is_empty()
            && self.env_files.is_empty()
            && self.data.is_empty()
            && self.target_url.is_empty()
    }

    /// Where the tests run, in a few words.
    pub fn target_line(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if !self.target_url.is_empty() {
            parts.push(format!("{} (the team's test address)", self.target_url));
        }
        for b in &self.base_urls {
            parts.push(format!("{} ({})", b.value, b.at));
        }
        if !self.overrides.is_empty() {
            parts.push(format!(
                "overridden by {}",
                self.overrides
                    .iter()
                    .map(|o| format!("`{}`", o.value))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if parts.is_empty() {
            "not set in the config".to_string()
        } else {
            parts.join("; ")
        }
    }

    /// One line for the run's progress notes.
    pub fn summary_note(&self) -> String {
        format!(
            "Test environment read: base address {}; {} config file(s), {} env setting(s), {} data file(s)",
            self.target_line(),
            self.config_files.len(),
            self.env_files.len(),
            self.data.len()
        )
    }

    /// The environment and the rule, for the run evidence and every agent.
    pub fn markdown(&self) -> String {
        let mut s = String::from("## Where the tests run\n");
        s.push_str(&format!("- Base address: {}\n", self.target_line()));
        if !self.config_files.is_empty() {
            s.push_str(&format!(
                "- Runner config: {}\n",
                self.config_files
                    .iter()
                    .map(|c| format!("`{c}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        let list = |label: &str, items: &[Setting], s: &mut String| {
            if !items.is_empty() {
                s.push_str(&format!(
                    "- {label}: {}\n",
                    items
                        .iter()
                        .map(|i| format!("`{}` ({})", i.value, i.at))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        };
        list("Variables the config reads", &self.env_vars, &mut s);
        list("Env files set", &self.env_files, &mut s);
        list("Setup and stored state", &self.setup, &mut s);
        if !self.data.is_empty() {
            s.push_str(&format!(
                "- Data and fixtures: {}\n",
                self.data
                    .iter()
                    .map(|d| format!("`{d}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        s.push('\n');
        s.push_str(RULE);
        s.push('\n');
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PW_CONFIG: &str = include_str!("testdata/playwright.config.ts.txt");
    const ENV_FILE: &str = include_str!("testdata/env.test.txt");

    fn tree() -> Vec<String> {
        [
            "package.json",
            "e2e/package.json",
            "e2e/playwright.config.ts",
            "e2e/.env.test",
            "e2e/tests/cards.spec.ts",
            "e2e/fixtures/cards.json",
            "e2e/fixtures/README.md",
            "e2e/support/data/users.csv",
            "src/data/prices.json",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn the_suite_folder_and_its_config_are_found() {
        let t = tree();
        let dirs = suite_dirs(&t, &["tests/cards.spec.ts".to_string()]);
        assert_eq!(dirs, vec!["e2e".to_string(), String::new()]);
        assert_eq!(config_paths(&t, &dirs), vec!["e2e/playwright.config.ts"]);
        assert_eq!(env_paths(&t, &dirs), vec!["e2e/.env.test"]);
    }

    #[test]
    fn base_address_overrides_data_and_fixtures_are_read() {
        let t = tree();
        let dirs = vec!["e2e".to_string(), String::new()];
        let env = read(
            &[("e2e/playwright.config.ts".into(), PW_CONFIG.into())],
            &[("e2e/.env.test".into(), ENV_FILE.into())],
            &t,
            &dirs,
            "https://staging.example.test",
        );
        assert_eq!(env.base_urls.len(), 1);
        assert_eq!(env.base_urls[0].value, "https://staging.example.test");
        assert!(env.base_urls[0].at.starts_with("e2e/playwright.config.ts:"));
        assert_eq!(
            env.overrides
                .iter()
                .map(|o| o.value.as_str())
                .collect::<Vec<_>>(),
            vec!["E2E_BASE_URL"]
        );
        assert!(env.env_vars.iter().any(|v| v.value == "TEST_DATASET"));
        assert!(env.setup.iter().any(|s| s.value.starts_with("globalSetup")));
        assert!(env
            .setup
            .iter()
            .any(|s| s.value.starts_with("storageState")));
        // Env files: addresses without credentials, other values never kept.
        assert!(env
            .env_files
            .iter()
            .any(|e| e.value == "E2E_BASE_URL=https://staging.example.test/app"));
        assert!(env.env_files.iter().any(|e| e.value == "API_TOKEN"));
        assert!(!env.env_files.iter().any(|e| e.value.contains("s3cr3t")));
        assert!(!env.env_files.iter().any(|e| e.value.contains("user:")));
        // Data the suite reads, not the app's own data folder.
        assert!(env.data.contains(&"e2e/fixtures/cards.json".to_string()));
        assert!(env.data.contains(&"e2e/support/data/users.csv".to_string()));
        assert!(!env.data.contains(&"src/data/prices.json".to_string()));
        assert!(!env.data.contains(&"e2e/fixtures/README.md".to_string()));
        let md = env.markdown();
        assert!(md.contains("https://staging.example.test (the team's test address)"));
        assert!(md.contains("`E2E_BASE_URL`"));
        assert!(md.contains("not a contradiction"));
        assert!(env.summary_note().contains("1 config file(s)"));
    }

    #[test]
    fn nothing_read_is_empty() {
        let env = read(&[], &[], &[], &[String::new()], "");
        assert!(env.is_empty());
        assert_eq!(env.target_line(), "not set in the config");
    }
}
