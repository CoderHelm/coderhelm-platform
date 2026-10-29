//! Rule 1: the run's own tools failing is never a question.
//!
//! A missing dependency, a command that isn't installed, an install that
//! failed or a sandbox error is CoderHelm's environment, not something a
//! person can answer. The run classifies each stop reason ([`signature`],
//! [`from_output`], [`from_check_lines`], [`from_text`]); for a tooling
//! failure it installs the package's dependencies with the package's own
//! manager and tries the command again ([`repair_steps`]). When that still
//! fails the run ends as a tooling failure naming the exact command and
//! error ([`ToolingFailure::reason`]), never "needs your answer".

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::e2e::test_package::{self, Manager, Step, TestPackage};

/// Every tooling failure's reason starts with this, so the run record and
/// the run page can tell it from any other failure.
pub const PREFIX: &str = "Tooling failure:";

/// The run's `stop_kind` for a tooling failure.
pub const STOP_KIND: &str = "tooling";

/// Longest error kept.
const MAX_ERROR_CHARS: usize = 600;
/// Longest command kept.
const MAX_COMMAND_CHARS: usize = 300;

/// A command the run itself ran that failed for reasons of its own
/// environment.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolingFailure {
    /// The exact command.
    pub command: String,
    /// The error line it printed.
    pub error: String,
    /// What the run tried before giving up, one line each.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tried: Vec<String>,
}

impl ToolingFailure {
    pub fn new(command: &str, error: &str) -> Self {
        ToolingFailure {
            command: crate::truncate_str(&one_line(command).replace('`', "'"), MAX_COMMAND_CHARS)
                .to_string(),
            error: crate::truncate_str(&one_line(error), MAX_ERROR_CHARS).to_string(),
            tried: Vec::new(),
        }
    }

    /// The run's failure reason: `Tooling failure: `cmd` failed: error`.
    pub fn reason(&self) -> String {
        let command = if self.command.is_empty() {
            "(command not recorded)"
        } else {
            &self.command
        };
        let mut s = format!("{PREFIX} `{command}` failed: {}", self.error);
        if !self.tried.is_empty() {
            s.push_str(&format!(" Tried first: {}.", self.tried.join("; ")));
        }
        s
    }

    /// Reads a reason [`reason`](Self::reason) wrote; `None` for any other.
    pub fn from_reason(reason: &str) -> Option<Self> {
        let rest = reason.trim().strip_prefix(PREFIX)?.trim_start();
        let rest = rest.strip_prefix('`')?;
        let (command, rest) = rest.split_once('`')?;
        let rest = rest.trim_start().strip_prefix("failed:")?.trim();
        let (error, tried) = match rest.split_once(" Tried first: ") {
            Some((e, t)) => (
                e,
                t.trim_end_matches('.')
                    .split("; ")
                    .map(str::to_string)
                    .collect(),
            ),
            None => (rest, Vec::new()),
        };
        Some(ToolingFailure {
            command: command.to_string(),
            error: error.trim().to_string(),
            tried,
        })
    }

    /// For the ticket and the run page.
    pub fn markdown(&self) -> String {
        let mut s = format!(
            "**Tooling failure.** CoderHelm's own environment failed, so this is not a question \
             for the ticket.\n\nCommand:\n```\n{}\n```\nError:\n```\n{}\n```\n",
            self.command, self.error
        );
        if !self.tried.is_empty() {
            s.push_str("\nTried first:\n");
            for t in &self.tried {
                s.push_str(&format!("- {t}\n"));
            }
        }
        s
    }
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The module a "Cannot find module 'x'" line names, when it is a package
/// (not a relative import or a path alias): a missing dependency.
fn missing_package(lower_line: &str, line: &str) -> Option<String> {
    let at = lower_line.find("cannot find module ")?;
    let rest = line.get(at + "cannot find module ".len()..)?;
    let quote = rest.chars().next().filter(|c| matches!(c, '\'' | '"'))?;
    let name: String = rest.get(1..)?.chars().take_while(|c| *c != quote).collect();
    let first = name.chars().next()?;
    let package = (first.is_ascii_alphanumeric() || first == '@')
        && !name.starts_with("@/")
        && !name.starts_with("~/");
    package.then_some(name)
}

/// The line the sandbox's network allowlist proxy prints for a host it
/// refused (`CODERHELM_EGRESS_BLOCKED <host>`).
pub const EGRESS_BLOCKED: &str = "CODERHELM_EGRESS_BLOCKED";

/// Where a team allows a domain, as the run names it.
pub const EGRESS_SETTING: &str =
    "the Network list in Settings, Agent permissions, Advanced, Sandbox";

/// The hosts the sandbox's network allowlist refused, in the order the
/// build printed them, once each. Only a line of its own counts: the build
/// log also echoes the script that greps for the marker.
pub fn blocked_hosts(output: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for line in output.lines() {
        let Some(rest) = line.trim().strip_prefix(EGRESS_BLOCKED) else {
            continue;
        };
        let Some(host) = rest.strip_prefix(' ').map(str::trim) else {
            continue;
        };
        let plain = !host.is_empty()
            && host.len() <= 253
            && host
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':'));
        let host = host.to_ascii_lowercase();
        if plain && !out.contains(&host) {
            out.push(host);
        }
    }
    out
}

/// The first line of `output` that says a network call failed (a proxy
/// that refused the tunnel, a refused or unresolved connection), trimmed.
pub fn network_error(output: &str) -> Option<String> {
    output
        .lines()
        .map(str::trim)
        .find(|l| {
            let lower = l.to_lowercase();
            !lower.starts_with(&EGRESS_BLOCKED.to_lowercase())
                && (lower.contains("tunneling socket")
                    || lower.contains("econnrefused")
                    || lower.contains("enotfound")
                    || lower.contains("econnreset")
                    || lower.contains("etimedout")
                    || lower.contains("eai_again")
                    || lower.contains("getaddrinfo")
                    || lower.contains("socket hang up")
                    || lower.contains("fetch failed")
                    || lower.contains("403 forbidden")
                    || lower.contains("proxy authentication")
                    || (lower.contains("statuscode=403") && lower.contains("socket"))
                    || lower.contains("could not resolve host")
                    || lower.contains("failed to connect to"))
        })
        .map(|l| crate::truncate_str(l, 240).to_string())
}

/// What a refused host comes to, in the run's words: `who` couldn't reach
/// it (blocked by the sandbox network allowlist), the error it printed, and
/// how to allow it.
pub fn blocked_message(who: &str, hosts: &[String], error: Option<&str>) -> String {
    let mut names = hosts.iter().take(3).cloned().collect::<Vec<_>>().join(", ");
    if hosts.len() > 3 {
        names.push_str(&format!(" and {} more", hosts.len() - 3));
    }
    let mut s = format!("{who} couldn't reach {names} (blocked by the sandbox network allowlist)");
    if let Some(e) = error.filter(|e| !e.trim().is_empty()) {
        s.push_str(&format!(": {}", e.trim()));
    }
    let it = if hosts.len() == 1 { "it" } else { "them" };
    s.push_str(&format!(
        ". This is the sandbox's network setting, not the change. To allow {it}, add {names} to \
         {EGRESS_SETTING}."
    ));
    s
}

/// A failed command whose output shows the sandbox refused a host: the
/// tooling failure that names the host, the command and its error. `who`
/// says what was running ("the generator", "the check").
pub fn egress_failure(who: &str, command: &str, output: &str) -> Option<ToolingFailure> {
    let hosts = blocked_hosts(output);
    if hosts.is_empty() {
        return None;
    }
    let error = network_error(output);
    Some(ToolingFailure::new(
        command,
        &blocked_message(who, &hosts, error.as_deref()),
    ))
}

/// The first line of `output` that says the run's tooling failed, trimmed;
/// `None` when nothing does. A host the sandbox refused, with a network
/// error in the same output, comes first: the command failed because it
/// couldn't connect, not because of the code.
pub fn signature(output: &str) -> Option<String> {
    let hosts = blocked_hosts(output);
    if !hosts.is_empty() {
        if let Some(err) = network_error(output) {
            return Some(
                crate::truncate_str(
                    &blocked_message("The command", &hosts, Some(&err)),
                    MAX_ERROR_CHARS,
                )
                .to_string(),
            );
        }
    }
    for line in output.lines() {
        let l = line.trim();
        if l.is_empty() {
            continue;
        }
        let lower = l.to_lowercase();
        let hit = lower.contains("command not found")
            || (lower.starts_with("sh:") && lower.ends_with("not found"))
            || (lower.starts_with("sh:") && lower.contains("permission denied"))
            || lower.contains("could not determine executable to run")
            || lower.starts_with("npm err!")
            || lower.starts_with("npm error")
            || lower.contains("err_pnpm_")
            || lower.contains("an unexpected error occurred")
            || lower.contains("lockfile needs to be updated")
            || lower.contains("err_module_not_found")
            || lower.contains("cannot find type definition file for")
            || (lower.contains("enoent")
                && (lower.contains("spawn") || lower.contains("node_modules")))
            || missing_package(&lower, l).is_some();
        if hit {
            return Some(crate::truncate_str(l, MAX_ERROR_CHARS).to_string());
        }
    }
    None
}

/// A failed command's result: a tooling failure when it exited 127 (not
/// installed) or printed a tooling error.
pub fn from_output(command: &str, output: &str, exit: Option<i32>) -> Option<ToolingFailure> {
    match (signature(output), exit) {
        (Some(line), _) => Some(ToolingFailure::new(command, &line)),
        (None, Some(127)) => Some(ToolingFailure::new(
            command,
            "exit 127: the command is not installed in the run's environment",
        )),
        _ => None,
    }
}

/// One sandbox step line (`- typecheck: FAIL (exit 2), `cmd``) as
/// (exit code, command); `None` for a passing or unreadable line.
fn failed_step(line: &str) -> Option<(i32, String)> {
    let at = line.find("FAIL (exit ")?;
    let code: String = line[at + "FAIL (exit ".len()..]
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '-')
        .collect();
    let code = code.parse().ok()?;
    let rest = &line[at..];
    let start = rest.find('`')?;
    let end = rest[start + 1..].find('`')?;
    Some((code, rest[start + 1..start + 1 + end].to_string()))
}

/// The tooling failure in a sandbox check run: its step lines and its
/// output. A step that exited 127 is one; otherwise a tooling error in the
/// output is put on the failing step it follows in the output (the first
/// failing step when the output doesn't say).
pub fn from_check_lines(lines: &[String], output: &str) -> Option<ToolingFailure> {
    let failed: Vec<(i32, String)> = lines.iter().filter_map(|l| failed_step(l)).collect();
    if let Some((_, cmd)) = failed.iter().find(|(c, _)| *c == 127) {
        return from_output(cmd, output, Some(127));
    }
    let line = signature(output)?;
    let cmd = failed
        .iter()
        .find(|(_, cmd)| {
            output
                .find(cmd.as_str())
                .zip(output.find(&line))
                .is_some_and(|(c, l)| c < l)
        })
        .or(failed.first())
        .map(|(_, c)| c.clone())?;
    Some(ToolingFailure::new(&cmd, &line))
}

/// A tooling failure quoted in a stop's own words (a question, a reason):
/// the error line, and the command in backticks before it when there is one.
pub fn from_text(text: &str) -> Option<ToolingFailure> {
    let line = signature(text)?;
    // The last complete `...` up to the end of the error's line.
    let end = text
        .find(&line)
        .map(|i| i + line.len())
        .unwrap_or(text.len());
    let parts: Vec<&str> = text[..end].split('`').collect();
    let closed = if parts.len() % 2 == 1 {
        parts.len()
    } else {
        parts.len() - 1
    };
    let command = (1..closed)
        .step_by(2)
        .map(|i| parts[i])
        .rfind(|c| !c.trim().is_empty() && !c.contains('\n'))
        .unwrap_or("");
    Some(ToolingFailure::new(command, &line))
}

/// The output a failing step printed in a [`test_package::sandbox_script`]
/// run: the text after its `--- kind failed (exit N) ---` line, up to the
/// next step's marker. Empty when it printed none.
pub fn failed_section<'a>(output: &'a str, kind: &str) -> &'a str {
    let head = format!("--- {kind} failed (exit ");
    let Some(i) = output.find(&head) else {
        return "";
    };
    let rest = &output[i..];
    let body = rest.find('\n').map(|n| &rest[n + 1..]).unwrap_or("");
    let end = [body.find("\n--- "), body.find(test_package::STEP_MARK)]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(body.len());
    &body[..end]
}

/// The tooling failure in a package check run (install, type-check, lint
/// steps): an install that failed is one whatever it printed; a later step
/// is one when it printed a tooling error or exited 127.
pub fn from_steps(steps: &[Step], codes: &[Option<i32>], output: &str) -> Option<ToolingFailure> {
    for (s, c) in steps.iter().zip(codes) {
        let Some(code) = c.filter(|c| *c != 0) else {
            continue;
        };
        let section = failed_section(output, s.kind);
        if let Some(f) = from_output(&s.cmd, section, Some(code)) {
            return Some(f);
        }
        if s.kind == "install" {
            return Some(ToolingFailure::new(
                &s.cmd,
                &format!("the install exited {code}"),
            ));
        }
    }
    None
}

/// The text of every tool result in an agent's conversation log.
pub fn tool_outputs(log: &[Value]) -> Vec<String> {
    let mut out = Vec::new();
    for m in log {
        for b in m["content"].as_array().into_iter().flatten() {
            if b["type"] != "tool_result" {
                continue;
            }
            match &b["content"] {
                Value::String(s) => out.push(s.clone()),
                Value::Array(parts) => out.extend(
                    parts
                        .iter()
                        .filter_map(|p| p["text"].as_str().map(str::to_string)),
                ),
                other if !other.is_null() => out.push(other.to_string()),
                _ => {}
            }
        }
    }
    out
}

/// The folder a command runs in (`cd 'dir' && ...` or `cd dir && ...`),
/// `None` when it doesn't change folder.
pub fn command_dir(cmd: &str) -> Option<String> {
    let rest = cmd.trim().strip_prefix("cd ")?;
    let (dir, _) = rest.split_once("&&")?;
    let dir = dir.trim().trim_matches(|c| c == '\'' || c == '"');
    (!dir.is_empty() && dir != ".").then(|| dir.to_string())
}

/// How the package's own manager runs a binary it installed.
pub fn exec_prefix(m: Manager) -> &'static str {
    match m {
        Manager::Npm => "npx --no-install ",
        Manager::Pnpm => "pnpm exec ",
        Manager::Yarn => "yarn ",
        Manager::Bun => "bunx ",
    }
}

/// The failed command run through the package's own manager.
pub fn with_manager(cmd: &str, m: Manager) -> String {
    cmd.replace("npx --no-install ", exec_prefix(m))
}

fn install_line(m: Manager, frozen: bool, isolated: bool) -> String {
    match m {
        Manager::Npm if frozen => format!(
            "npm ci --no-audit --no-fund{}",
            if isolated { " --workspaces=false" } else { "" }
        ),
        Manager::Npm => format!(
            "npm install --no-audit --no-fund{}",
            if isolated { " --workspaces=false" } else { "" }
        ),
        Manager::Pnpm => format!(
            "pnpm install{}{}",
            if isolated { " --ignore-workspace" } else { "" },
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

/// The repair for a tooling failure in package `p` (its `lockfile` as a
/// path from the repository root, see [`rooted`]): install its
/// dependencies with its own manager (where its lockfile is: in the package
/// on its own, or from the workspace root), falling back to an install that
/// refreshes the lockfile when the frozen one fails, then the failed
/// command again, run through the package's manager.
pub fn repair_steps(p: &TestPackage, failed: &str) -> Vec<Step> {
    let quote = |d: &str| format!("'{}'", d.replace('\'', ""));
    let own_lock = p
        .lockfile
        .as_deref()
        .is_some_and(|l| p.dir == "." || l.starts_with(&format!("{}/", p.dir)));
    // A package with its own lockfile sits outside the workspace: its
    // install must not reach for the parent's.
    let isolated = p.dir != "." && own_lock;
    let at = if own_lock || p.lockfile.is_none() {
        p.dir.clone()
    } else {
        ".".to_string()
    };
    let cd = if at == "." {
        String::new()
    } else {
        format!("cd {} && ", quote(&at))
    };
    let frozen = p.lockfile.is_some();
    let install = if frozen {
        format!(
            "{cd}({} || {})",
            install_line(p.manager, true, isolated),
            install_line(p.manager, false, isolated)
        )
    } else {
        format!("{cd}{}", install_line(p.manager, false, isolated))
    };
    let mut retry = with_manager(failed, p.manager);
    if command_dir(&retry).is_none() && p.dir != "." {
        retry = format!("cd {} && {retry}", quote(&p.dir));
    }
    vec![
        Step {
            kind: "install",
            cmd: install,
        },
        Step {
            kind: "retry",
            cmd: retry,
        },
    ]
}

/// `p` with its lockfile as a path from the repository root:
/// [`test_package::packages_outside`] names the file in the package's own
/// folder.
pub fn rooted(mut p: TestPackage) -> TestPackage {
    if let Some(l) = p.lockfile.as_mut() {
        if p.dir != "." && !l.contains('/') {
            *l = format!("{}/{l}", p.dir);
        }
    }
    p
}

/// A package's check steps run the way its own manager runs them: the
/// install falls back to refreshing the lockfile when the frozen one fails,
/// and binaries run through the manager (`pnpm exec`, `yarn`, `bunx`).
pub fn hardened(p: &TestPackage, steps: Vec<Step>) -> Vec<Step> {
    let p = rooted(p.clone());
    let install = repair_steps(&p, "true")
        .into_iter()
        .next()
        .map(|s| s.cmd)
        .unwrap_or_default();
    steps
        .into_iter()
        .map(|s| {
            if s.kind == "install" && !install.is_empty() {
                Step {
                    kind: s.kind,
                    cmd: install.clone(),
                }
            } else {
                Step {
                    kind: s.kind,
                    cmd: with_manager(&s.cmd, p.manager),
                }
            }
        })
        .collect()
}

/// What a repair came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Repair {
    /// The command passes now.
    Fixed { tried: Vec<String> },
    /// It still fails: the failure to end the run with.
    Failed(ToolingFailure),
}

/// Reads a repair run (the steps [`repair_steps`] gave, through
/// [`test_package::sandbox_script`]): fixed when the retry passed,
/// otherwise the failure with the newest error and what was tried.
pub fn repair_result(steps: &[Step], output: &str, original: &ToolingFailure) -> Repair {
    let codes = test_package::step_codes(output, steps.len());
    let tried: Vec<String> = steps
        .iter()
        .zip(&codes)
        .map(|(s, c)| {
            let verdict = match c {
                Some(0) => "passed".to_string(),
                Some(c) => format!("failed (exit {c})"),
                None => "no result".to_string(),
            };
            format!("`{}` {verdict}", s.cmd.replace('`', "'"))
        })
        .collect();
    if codes.last().copied().flatten() == Some(0) {
        return Repair::Fixed { tried };
    }
    let mut f = match (steps.last(), codes.last().copied().flatten()) {
        (Some(retry), code) => from_output(&retry.cmd, output, code).unwrap_or_else(|| {
            let mut f = original.clone();
            if let Some(line) = signature(output) {
                f.error = line;
            }
            f
        }),
        (None, _) => original.clone(),
    };
    f.tried = tried;
    Repair::Failed(f)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHECK_OUTPUT: &str = include_str!("testdata/tooling_check_output.txt");
    const NPX_OUTPUT: &str = include_str!("testdata/tooling_npx_missing.txt");
    const CODE_ERROR: &str = include_str!("testdata/code_type_error.txt");

    #[test]
    fn missing_packages_and_commands_are_tooling() {
        let line = signature(CHECK_OUTPUT).unwrap();
        assert!(
            line.contains("Cannot find module '@playwright/test'"),
            "{line}"
        );
        assert!(signature(NPX_OUTPUT)
            .unwrap()
            .contains("could not determine executable to run"));
        assert!(signature("sh: 1: tsc: not found").is_some());
        assert!(signature("bash: pnpm: command not found").is_some());
        assert!(
            signature(" ERR_PNPM_OUTDATED_LOCKFILE  Cannot install with frozen-lockfile").is_some()
        );
        assert!(signature("npm ERR! code ENOENT").is_some());
    }

    #[test]
    fn a_real_code_error_is_not_tooling() {
        assert_eq!(signature(CODE_ERROR), None);
        // A relative import or a path alias that doesn't resolve is the code's.
        assert_eq!(
            signature("src/a.ts(1,20): error TS2307: Cannot find module './helpers' or its corresponding type declarations."),
            None
        );
        assert_eq!(
            signature("error TS2307: Cannot find module '@/lib/x'."),
            None
        );
        assert_eq!(from_output("npm test", CODE_ERROR, Some(1)), None);
    }

    #[test]
    fn exit_127_is_tooling_even_without_words() {
        let f = from_output("cd 'e2e' && npx --no-install eslint a.ts", "", Some(127)).unwrap();
        assert_eq!(f.command, "cd 'e2e' && npx --no-install eslint a.ts");
        assert!(f.error.contains("127"));
    }

    #[test]
    fn the_failing_step_is_named_with_its_exact_command() {
        let lines = vec![
            "- install: PASS, `cd 'e2e' && npm ci --no-audit --no-fund --workspaces=false`".to_string(),
            "- typecheck: FAIL (exit 2), `cd 'e2e' && npx --no-install tsc --noEmit -p tsconfig.json`".to_string(),
            "- lint: FAIL (exit 1), `cd 'e2e' && npx --no-install eslint tests/cards.spec.ts`".to_string(),
        ];
        let f = from_check_lines(&lines, CHECK_OUTPUT).unwrap();
        assert_eq!(
            f.command,
            "cd 'e2e' && npx --no-install tsc --noEmit -p tsconfig.json"
        );
        assert!(f.error.contains("@playwright/test"));
        let only_lint =
            vec!["- lint: FAIL (exit 127), `cd 'e2e' && npx --no-install eslint x.ts`".to_string()];
        assert!(from_check_lines(&only_lint, "")
            .unwrap()
            .command
            .contains("eslint"));
        assert_eq!(from_check_lines(&lines, CODE_ERROR), None);
    }

    #[test]
    fn a_question_quoting_a_tooling_error_is_a_tooling_stop() {
        let q = "The type-check `cd 'e2e' && npx --no-install tsc --noEmit` failed with:\nerror TS2307: Cannot find module '@playwright/test' or its corresponding type declarations.\nShould I add the dependency?";
        let f = from_text(q).unwrap();
        assert_eq!(f.command, "cd 'e2e' && npx --no-install tsc --noEmit");
        assert!(f.error.contains("@playwright/test"));
        assert_eq!(from_text("Which environment gets the new limit?"), None);
    }

    #[test]
    fn the_reason_names_command_and_error_and_reads_back() {
        let mut f =
            ToolingFailure::new("cd 'e2e' && npx --no-install tsc", "sh: 1: tsc: not found");
        f.tried = vec!["`cd 'e2e' && npm install` passed".into()];
        let r = f.reason();
        assert!(r.starts_with(PREFIX));
        assert!(r.contains("`cd 'e2e' && npx --no-install tsc`"));
        assert!(r.contains("tsc: not found"));
        assert_eq!(ToolingFailure::from_reason(&r), Some(f.clone()));
        assert_eq!(ToolingFailure::from_reason("The plan failed"), None);
        assert!(f.markdown().contains("not a question"));
    }

    fn pkg(dir: &str, manager: Manager, lock: Option<&str>) -> TestPackage {
        TestPackage {
            dir: dir.into(),
            manager,
            lockfile: lock.map(str::to_string),
            has_tsconfig: true,
            lint: true,
            runner: None,
            files: vec![],
        }
    }

    #[test]
    fn the_repair_installs_with_the_packages_own_manager_then_retries() {
        let p = pkg("e2e", Manager::Pnpm, Some("e2e/pnpm-lock.yaml"));
        let steps = repair_steps(
            &p,
            "cd 'e2e' && npx --no-install tsc --noEmit -p tsconfig.json",
        );
        assert_eq!(steps[0].kind, "install");
        assert_eq!(
            steps[0].cmd,
            "cd 'e2e' && (pnpm install --ignore-workspace --frozen-lockfile || pnpm install --ignore-workspace)"
        );
        assert_eq!(
            steps[1].cmd,
            "cd 'e2e' && pnpm exec tsc --noEmit -p tsconfig.json"
        );

        // A workspace member installs from the root's lockfile.
        let member = pkg("apps/web", Manager::Yarn, Some("yarn.lock"));
        let steps = repair_steps(&member, "npx --no-install eslint src/a.ts");
        assert_eq!(
            steps[0].cmd,
            "(yarn install --ignore-engines --frozen-lockfile || yarn install --ignore-engines)"
        );
        assert_eq!(steps[1].cmd, "cd 'apps/web' && yarn eslint src/a.ts");

        // No lockfile: a plain install where the package is.
        let loose = pkg("tests", Manager::Npm, None);
        assert_eq!(
            repair_steps(&loose, "cd 'tests' && npx --no-install tsc")[0].cmd,
            "cd 'tests' && npm install --no-audit --no-fund"
        );
        assert_eq!(command_dir("cd 'e2e' && tsc"), Some("e2e".into()));
        assert_eq!(command_dir("npm test"), None);
    }

    #[test]
    fn package_checks_run_through_the_packages_own_manager() {
        // As `packages_outside` names it: the lockfile in the package's folder.
        let p = pkg("e2e", Manager::Pnpm, Some("pnpm-lock.yaml"));
        let steps = hardened(
            &p,
            test_package::check_steps(&TestPackage {
                files: vec!["tests/a.spec.ts".into()],
                ..p.clone()
            })
            .unwrap(),
        );
        assert_eq!(
            steps[0].cmd,
            "cd 'e2e' && (pnpm install --ignore-workspace --frozen-lockfile || pnpm install --ignore-workspace)"
        );
        assert_eq!(
            steps[1].cmd,
            "cd 'e2e' && pnpm exec tsc --noEmit -p tsconfig.json"
        );
        assert_eq!(
            steps[2].cmd,
            "cd 'e2e' && pnpm exec eslint 'tests/a.spec.ts'"
        );
    }

    #[test]
    fn a_repair_is_fixed_only_when_the_retry_passes() {
        let p = pkg("e2e", Manager::Npm, Some("e2e/package-lock.json"));
        let original =
            ToolingFailure::new("cd 'e2e' && npx --no-install tsc", "sh: 1: tsc: not found");
        let steps = repair_steps(&p, &original.command);
        let ok = format!(
            "{} 1 0\n{} 2 0\n",
            test_package::STEP_MARK,
            test_package::STEP_MARK
        );
        assert!(matches!(
            repair_result(&steps, &ok, &original),
            Repair::Fixed { .. }
        ));
        let bad = format!(
            "npm ERR! code E404\n{} 1 1\n{} 2 127\n",
            test_package::STEP_MARK,
            test_package::STEP_MARK
        );
        match repair_result(&steps, &bad, &original) {
            Repair::Failed(f) => {
                assert_eq!(f.command, steps[1].cmd);
                assert!(f.error.contains("npm ERR!"), "{}", f.error);
                assert_eq!(f.tried.len(), 2);
                assert!(f.tried[0].contains("failed (exit 1)"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_package_check_run_names_its_tooling_failure() {
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
        let mark = test_package::STEP_MARK;
        // The install passed; the type-check can't find the runner's types.
        let out = format!(
            "--- typecheck failed (exit 2) ---\ntests/a.spec.ts(1,30): error TS2307: Cannot find module '@playwright/test' or its corresponding type declarations.\n\n{mark} 1 0\n{mark} 2 2\n"
        );
        let codes = test_package::step_codes(&out, 2);
        let f = from_steps(&steps, &codes, &out).unwrap();
        assert_eq!(f.command, steps[1].cmd);
        assert!(f.error.contains("TS2307"));
        assert!(failed_section(&out, "typecheck").contains("TS2307"));
        assert_eq!(failed_section(&out, "lint"), "");
        // An install that failed is tooling whatever it printed.
        let out =
            format!("--- install failed (exit 1) ---\nsomething odd\n{mark} 1 1\n{mark} 2 0\n");
        let codes = test_package::step_codes(&out, 2);
        let f = from_steps(&steps, &codes, &out).unwrap();
        assert_eq!(f.command, "cd 'e2e' && npm ci");
        assert_eq!(f.error, "the install exited 1");
        // A real type error in the test code is the code's.
        let out = format!(
            "--- typecheck failed (exit 2) ---\ntests/a.spec.ts(3,7): error TS2322: Type 'string' is not assignable to type 'number'.\n{mark} 1 0\n{mark} 2 2\n"
        );
        let codes = test_package::step_codes(&out, 2);
        assert_eq!(from_steps(&steps, &codes, &out), None);
    }

    #[test]
    fn tool_results_are_read_from_the_log() {
        let log = vec![serde_json::json!({
            "role": "user",
            "content": [
                {"type": "tool_result", "tool_use_id": "a", "content": "sh: 1: tsc: not found"},
                {"type": "tool_result", "tool_use_id": "b", "content": [{"type": "text", "text": "ok"}]},
                {"type": "text", "text": "not a result"}
            ]
        })];
        assert_eq!(tool_outputs(&log), vec!["sh: 1: tsc: not found", "ok"]);
    }

    /// A generator's sandbox log when the allowlist refused its API host.
    const BLOCKED_LOG: &str = "[Container] Running command if [ -f /tmp/egress.log ]; then grep CODERHELM_EGRESS_BLOCKED /tmp/egress.log | sort -u | head -20; fi\n\
         > sanity schema extract --enforce-required-fields\n\
         SchemaExtractionError: tunneling socket could not be established, statusCode=403\n\
         ===CODERHELM_CODEGEN_END exit=1===\n\
         CODERHELM_EGRESS_BLOCKED abc123.api.example.io\n\
         CODERHELM_EGRESS_BLOCKED abc123.api.example.io\n";

    #[test]
    fn a_refused_host_is_read_from_its_own_line_only() {
        assert_eq!(blocked_hosts(BLOCKED_LOG), vec!["abc123.api.example.io"]);
        assert!(blocked_hosts("grep CODERHELM_EGRESS_BLOCKED /tmp/egress.log").is_empty());
        assert!(blocked_hosts("CODERHELM_EGRESS_BLOCKED $(rm -rf /)").is_empty());
        assert!(blocked_hosts("all good").is_empty());
    }

    #[test]
    fn a_blocked_generator_is_a_tooling_failure_naming_the_host() {
        let f = egress_failure(
            "the generator",
            "cd apps/web && pnpm run typegen",
            BLOCKED_LOG,
        )
        .unwrap();
        assert_eq!(f.command, "cd apps/web && pnpm run typegen");
        assert!(f.error.starts_with(
            "the generator couldn't reach abc123.api.example.io (blocked by the sandbox network \
             allowlist): SchemaExtractionError: tunneling socket could not be established"
        ));
        assert!(f.error.contains(EGRESS_SETTING));
        assert!(!f.error.contains("regenerated"));
        // The run page reads it back from the run's reason.
        assert_eq!(ToolingFailure::from_reason(&f.reason()), Some(f.clone()));
        assert_eq!(egress_failure("the generator", "x", "error TS2322"), None);
    }

    #[test]
    fn a_failing_check_that_could_not_connect_is_tooling_not_the_change() {
        let lines = vec!["- test: FAIL (exit 1), `npm test`".to_string()];
        let out = "FAIL src/api.test.ts\nError: connect ECONNREFUSED 10.0.0.1:443\nCODERHELM_EGRESS_BLOCKED api.vendor.example\n";
        let f = from_check_lines(&lines, out).unwrap();
        assert_eq!(f.command, "npm test");
        assert!(f.error.contains(
            "couldn't reach api.vendor.example (blocked by the sandbox network allowlist)"
        ));
        // A refused host beside a real code error, with no failed connection,
        // stays the code's.
        let code = format!("{CODE_ERROR}\nCODERHELM_EGRESS_BLOCKED telemetry.example.com\n");
        assert_eq!(from_check_lines(&lines, &code), None);
    }

    #[test]
    fn many_refused_hosts_are_named_briefly() {
        let hosts: Vec<String> = (1..=5).map(|i| format!("h{i}.example.com")).collect();
        let m = blocked_message("the check", &hosts, None);
        assert!(m.starts_with(
            "the check couldn't reach h1.example.com, h2.example.com, h3.example.com and 2 more"
        ));
        assert!(m.contains("To allow them"));
    }
}
