//! Which CI job covers the changed code.
//!
//! A web pull request's Open items once told a reader to run `npm test` in
//! an unrelated serverless function's folder: the line came from the list
//! of every workflow in the repository, not from the jobs that test the
//! changed package. [`covering_jobs`] maps the changed paths to the jobs
//! that run on them (the workflow's `paths` filters and branches, and each
//! job's `working-directory`), [`line`] says which job checks the change or
//! that none does, and [`command_fits`] rejects a check command aimed at a
//! folder the change doesn't touch. "How to check" and Open items are built
//! from this mapping, never from a repository-wide list.

use serde::{Deserialize, Serialize};

/// One CI job that runs on the changed code.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoveringJob {
    pub workflow: String,
    /// The check's name (the job's `name`, else its key).
    pub check: String,
    /// The folder its steps run in; "" for the repository's root.
    pub dir: String,
    /// The test-like commands it runs, as written.
    pub commands: Vec<String>,
}

fn s(v: Option<&serde_yaml::Value>) -> String {
    v.and_then(|x| x.as_str()).unwrap_or("").trim().to_string()
}

fn norm_dir(d: &str) -> String {
    d.trim()
        .trim_start_matches("./")
        .trim_end_matches('/')
        .to_string()
}

/// A command that runs tests, lint or type checks.
pub fn is_check_command(cmd: &str) -> bool {
    let c = cmd.to_ascii_lowercase();
    [
        " test",
        "test ",
        "vitest",
        "jest",
        "pytest",
        "playwright",
        "cypress",
        "lint",
        "tsc",
        "typecheck",
        "type-check",
        "cargo test",
        "go test",
    ]
    .iter()
    .any(|w| c.contains(w))
}

/// Whether a changed path lies in `dir` ("" is the whole repository).
fn under(dir: &str, path: &str) -> bool {
    dir.is_empty() || path == dir || path.starts_with(&format!("{dir}/"))
}

/// The jobs of pull request workflows that run on the changed paths. A
/// job whose steps run in a folder covers only changes in that folder; a
/// job at the root covers the change when its workflow starts for it.
pub fn covering_jobs(
    workflows: &[(String, String)],
    changed: &[String],
    base: &str,
) -> Vec<CoveringJob> {
    let mut out = Vec::new();
    for (path, yaml) in workflows {
        if !crate::preview::triggers(yaml).applies(changed, base) {
            continue;
        }
        let Ok(v) = serde_yaml::from_str::<serde_yaml::Value>(yaml) else {
            continue;
        };
        let wf_dir = s(v
            .get("defaults")
            .and_then(|d| d.get("run"))
            .and_then(|r| r.get("working-directory")));
        let Some(jobs) = v.get("jobs").and_then(|j| j.as_mapping()) else {
            continue;
        };
        for (k, job) in jobs {
            let Some(id) = k.as_str() else { continue };
            let name = s(job.get("name"));
            let check = if name.is_empty() || name.contains("${{") {
                id.to_string()
            } else {
                name
            };
            let job_dir = s(job
                .get("defaults")
                .and_then(|d| d.get("run"))
                .and_then(|r| r.get("working-directory")));
            let mut dirs: Vec<String> = Vec::new();
            let mut commands: Vec<String> = Vec::new();
            for step in job
                .get("steps")
                .and_then(|x| x.as_sequence())
                .into_iter()
                .flatten()
            {
                let run = s(step.get("run"));
                if run.is_empty() || !is_check_command(&run) {
                    continue;
                }
                let step_dir = s(step.get("working-directory"));
                let dir = [&step_dir, &job_dir, &wf_dir]
                    .into_iter()
                    .find(|d| !d.is_empty() && !d.contains("${{"))
                    .map(|d| norm_dir(d))
                    .unwrap_or_default();
                // `cd <dir> && …` in the command itself.
                let dir = cd_dir(&run).unwrap_or(dir);
                if !dirs.contains(&dir) {
                    dirs.push(dir);
                }
                commands.push(run.lines().next().unwrap_or("").trim().to_string());
            }
            if commands.is_empty() {
                continue;
            }
            if let Some(dir) = dirs.iter().find(|d| changed.iter().any(|c| under(d, c))) {
                out.push(CoveringJob {
                    workflow: path.clone(),
                    check: check.clone(),
                    dir: dir.clone(),
                    commands,
                });
            }
        }
    }
    out
}

/// The folder a command `cd`s into first, if any.
fn cd_dir(cmd: &str) -> Option<String> {
    let t = cmd.trim();
    let rest = t.strip_prefix("cd ")?;
    let dir = rest
        .split(|c: char| c.is_whitespace() || c == '&' || c == ';')
        .next()?
        .trim_matches(['\'', '"']);
    (!dir.is_empty() && !dir.contains('$')).then(|| norm_dir(dir))
}

/// The folders a command names to run in: `cd X`, `--prefix X`,
/// `--dir X`, `-C X`, `--filter ./X`, `working-directory: X`.
pub fn command_dirs(cmd: &str) -> Vec<String> {
    let mut out = Vec::new();
    let toks: Vec<&str> = cmd
        .split(|c: char| c.is_whitespace() || c == '&' || c == ';' || c == '`')
        .filter(|t| !t.is_empty())
        .collect();
    for (i, t) in toks.iter().enumerate() {
        let next = toks.get(i + 1).map(|n| n.trim_matches(['\'', '"']));
        let dir = match *t {
            "cd" | "--prefix" | "--dir" | "-C" | "--cwd" | "working-directory:" => next,
            "--filter" => next.filter(|n| n.starts_with("./")),
            _ => None,
        };
        if let Some(d) = dir.filter(|d| !d.is_empty() && *d != "." && !d.contains('$')) {
            let d = norm_dir(d);
            if !out.contains(&d) {
                out.push(d);
            }
        }
    }
    out
}

/// Whether a check command fits the change: every folder it names to run
/// in holds a changed path. A command naming no folder runs at the root
/// and fits.
pub fn command_fits(cmd: &str, changed: &[String]) -> bool {
    command_dirs(cmd)
        .iter()
        .all(|d| changed.iter().any(|c| under(d, c)))
}

/// The line that names the CI job checking the change, or says none does.
pub fn line(jobs: &[CoveringJob]) -> String {
    if jobs.is_empty() {
        return "No CI job covers this change: no pull request workflow runs tests on the changed paths".into();
    }
    let parts: Vec<String> = jobs
        .iter()
        .take(3)
        .map(|j| {
            let at = if j.dir.is_empty() {
                String::new()
            } else {
                format!(" in `{}`", j.dir)
            };
            format!(
                "`{}`{at} runs `{}`",
                j.check,
                j.commands.first().map(String::as_str).unwrap_or("")
            )
        })
        .collect();
    format!("CI checks this change: {}", parts.join("; "))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(x: &[&str]) -> Vec<String> {
        x.iter().map(|s| s.to_string()).collect()
    }

    const WEB: &str = r#"
name: Web
on:
  pull_request:
    paths: ["apps/web/**"]
jobs:
  unit:
    name: Web unit tests
    defaults:
      run:
        working-directory: apps/web
    steps:
      - run: npm ci
      - run: npm test
"#;

    const FUNCTION: &str = r#"
name: Function
on:
  pull_request:
jobs:
  fn_tests:
    name: Function tests
    steps:
      - run: npm ci
        working-directory: lambda/uploader
      - run: npm test
        working-directory: lambda/uploader
"#;

    fn wfs() -> Vec<(String, String)> {
        vec![
            ("web.yml".into(), WEB.into()),
            ("fn.yml".into(), FUNCTION.into()),
        ]
    }

    /// A web change's Open items once cited `npm test` in an unrelated
    /// function's folder. The job that covers the change is the web one.
    #[test]
    fn the_job_that_tests_the_changed_package_is_named_never_another_folders() {
        let changed = v(&["apps/web/src/components/form.tsx"]);
        let jobs = covering_jobs(&wfs(), &changed, "main");
        assert_eq!(jobs.len(), 1, "{jobs:?}");
        assert_eq!(jobs[0].check, "Web unit tests");
        assert_eq!(jobs[0].dir, "apps/web");
        assert!(line(&jobs).contains("`Web unit tests` in `apps/web` runs `npm test`"));
        assert!(!command_fits("cd lambda/uploader && npm test", &changed));
        assert!(!command_fits("npm --prefix lambda/uploader test", &changed));
        assert!(command_fits("cd apps/web && npm test", &changed));
        assert!(command_fits("npm test", &changed));
    }

    #[test]
    fn a_change_no_job_tests_says_so() {
        let changed = v(&["docs/guide.md"]);
        let jobs = covering_jobs(&wfs(), &changed, "main");
        assert!(jobs.is_empty());
        assert!(line(&jobs).starts_with("No CI job covers this change"));
    }
}
