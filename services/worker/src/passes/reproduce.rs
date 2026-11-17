//! The planner reproduces a bug on the base before it plans code
//! (`common::repro_first` has the rules and why).
//!
//! - [`Reproducer`]: the planner's `reproduce_on_base` tool. It runs one
//!   call or command in the sandbox on the base branch's head, in the run's
//!   test environment, and returns its exit code and output. Each run is
//!   kept ([`Reproducer::attempts`]), so the plan check can hold the plan's
//!   `## Reproduce` to a command that ran and to what it printed.
//! - [`plan_gaps`]: the plan check's lines: the reproduction, the fix in
//!   the mechanism it landed in, and questions looked up before they are
//!   decided with a default. A gap goes back with the plan's one send-back;
//!   what is still open stays on the decision.

use std::sync::Mutex;

use common::repro_first::{self, Attempt};
use serde_json::{json, Value};

use super::plan::PlanResult;
use crate::agent::llm::ToolDefinition;

/// Runs of the tool a plan may make.
pub const MAX_RUNS: usize = 6;
const OUT_MARK: &str = "===CODERHELM_REPRO_OUT===";
const EXIT_MARK: &str = "===CODERHELM_REPRO_EXIT";
/// The output kept of one run.
const KEEP: usize = 3000;

/// The tool's definition.
pub fn tool() -> ToolDefinition {
    ToolDefinition {
        name: repro_first::TOOL.to_string(),
        description: format!(
            "Run one call or command on the BASE branch, before any change, in a sandbox with the \
             repository's test environment installed, and get its exit code and output. Use it to \
             reproduce a bug: put the ticket's exact names or ids (and each variant the data holds) \
             through the code path the bug is in, for example `python -c \"from dags.utils.common \
             import get_override; print(get_override('Camp-Half Day'))\"`, or run the nearest \
             existing test. Quote the command and its output in `## Reproduce`. Read-only: \
             nothing it writes is kept. At most {MAX_RUNS} runs."
        ),
        input_schema: json!({
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The shell command, run from the repository root"
                },
                "inputs": {
                    "type": "string",
                    "description": "The exact names or ids it feeds the code, and where each came from (the ticket, a lookup)"
                }
            },
            "required": ["command"]
        }),
    }
}

/// The planner's lines about the tool.
pub fn prompt_section(can_run: bool) -> String {
    if can_run {
        format!(
            "\n\n## Reproduce a bug before planning (`{}`)\nFor a bug, before you plan code: take the exact names or ids from the ticket, find every variant the real data holds with a read-only lookup (a `LIKE` on the name), and run them through the current code on the base with `{}`. Write `## Reproduce` in design.md with `Inputs:`, `Ran on base:` (the command, as you ran it), `Output:` (what it printed, quoted) and `Wrong line:` (`path:line` and the code on it, backticked). The fix goes in that line's mechanism.\n",
            repro_first::TOOL,
            repro_first::TOOL
        )
    } else {
        "\n\n## Reproduce a bug before planning\nFor a bug, write `## Reproduce` in design.md: `Inputs:` (the ticket's exact names or ids, with every variant a read-only lookup finds), `Unverified:` (no sandbox here to run the code on the base) and `Wrong line:` (`path:line` and the code on it, backticked). The fix goes in that line's mechanism.\n".to_string()
    }
}

/// A command's output from the script's: the exit code and what it printed.
pub fn read_output(output: &str) -> (Option<i32>, String) {
    let exit = output.lines().rev().find_map(|l| {
        l.trim()
            .strip_prefix(EXIT_MARK)
            .and_then(|r| r.trim().parse::<i32>().ok())
    });
    let body = output
        .split_once(OUT_MARK)
        .map(|(_, b)| b)
        .unwrap_or(output);
    let body = body.split(EXIT_MARK).next().unwrap_or(body).trim();
    let start = body.len().saturating_sub(KEEP);
    let start = (start..=body.len())
        .find(|i| body.is_char_boundary(*i))
        .unwrap_or(body.len());
    (exit, body[start..].to_string())
}

/// The sandbox script for one command: its output and exit code, always
/// printed (a call that prints `None` and exits 0 is the reproduction).
pub fn script(command: &str) -> String {
    format!(
        "( {command} ) > /tmp/ch-repro.log 2>&1; ch_r=$?\necho \"{OUT_MARK}\"\ntail -c {KEEP} /tmp/ch-repro.log\necho\necho \"{EXIT_MARK} $ch_r\"\ntrue"
    )
}

/// The planner's tool, on one run.
pub struct Reproducer<'a> {
    pub runner: super::test_bug::Runner<'a>,
    pub base_branch: &'a str,
    attempts: Mutex<Vec<Attempt>>,
}

impl<'a> Reproducer<'a> {
    pub fn new(runner: super::test_bug::Runner<'a>, base_branch: &'a str) -> Self {
        Self {
            runner,
            base_branch,
            attempts: Mutex::new(Vec::new()),
        }
    }

    /// Whether a sandbox is set up to run on the base.
    pub fn can_run(&self) -> bool {
        let c = &self.runner.state.config;
        !c.sandbox_bucket_name.is_empty() && !c.sandbox_project_name.is_empty()
    }

    /// Every run so far.
    pub fn attempts(&self) -> Vec<Attempt> {
        self.attempts.lock().map(|a| a.clone()).unwrap_or_default()
    }

    /// The tool's answer, or `None` for another tool.
    pub async fn handle(&self, name: &str, input: &Value) -> Option<String> {
        if name != repro_first::TOOL {
            return None;
        }
        let command = input["command"].as_str().unwrap_or("").trim().to_string();
        if command.is_empty() {
            return Some("No command given.".into());
        }
        if self.attempts().len() >= MAX_RUNS {
            return Some(format!(
                "Not run: this plan has used its {MAX_RUNS} runs. Write `## Reproduce` from the runs you made."
            ));
        }
        let r = &self.runner;
        let attempt = match r.github.get_ref(r.owner, r.repo, self.base_branch).await {
            Err(e) => Attempt {
                command: command.clone(),
                error: format!("the base branch couldn't be read ({e})"),
                ..Default::default()
            },
            Ok(sha) => match r.run_script_at(&sha, &[&command], &script(&command)).await {
                Ok(out) => {
                    let (exit, output) = read_output(&out);
                    Attempt {
                        command: command.clone(),
                        exit,
                        output,
                        error: String::new(),
                    }
                }
                Err(e) => Attempt {
                    command: command.clone(),
                    error: e,
                    ..Default::default()
                },
            },
        };
        let answer = if attempt.error.is_empty() {
            format!(
                "Ran on the base ({}): exit {}\nOutput:\n{}",
                self.base_branch,
                attempt
                    .exit
                    .map_or("unknown".to_string(), |c| c.to_string()),
                attempt.output
            )
        } else {
            format!("Didn't run: {}", attempt.error)
        };
        if let Ok(mut a) = self.attempts.lock() {
            a.push(attempt);
        }
        Some(answer)
    }
}

/// The plan's out-of-scope entries decided with a default and its question
/// lines: what a lookup may have to settle first.
fn questions(files: &PlanResult) -> Vec<String> {
    let mut out: Vec<String> =
        common::scope_ledger::Ledger::from_plan(&files.proposal, &files.design, "")
            .entries
            .into_iter()
            .filter(|e| e.why.to_lowercase().contains("default"))
            .map(|e| format!("{}: {}", e.item, e.why))
            .collect();
    for l in format!("{}\n{}", files.proposal, files.design).lines() {
        let t = l.trim().trim_start_matches(['-', '*']).trim();
        if t.ends_with('?') && !out.iter().any(|o| o.contains(t)) {
            out.push(t.to_string());
        }
    }
    out
}

/// What the plan check sends back about reproducing, the mechanism and
/// questions. `is_bug` from triage; `ticket` the ticket's words; `attempts`
/// the tool's runs.
pub fn plan_gaps(
    files: &PlanResult,
    is_bug: bool,
    ticket: &str,
    attempts: &[Attempt],
    can_run: bool,
    can_look_up: bool,
) -> Vec<String> {
    let t = files.tasks.trim_start();
    if t.is_empty() || super::plan_sections::directive(t) {
        return Vec::new();
    }
    let mut out = Vec::new();
    if is_bug {
        out.extend(repro_first::reproduce_gaps(
            &files.design,
            ticket,
            attempts,
            can_run,
            can_look_up,
        ));
        let smallest =
            super::feature::reuse::section(&files.design, super::plan_shape::SMALLEST_CHANGE)
                .unwrap_or("");
        out.extend(repro_first::reuse_gaps(
            &files.design,
            smallest,
            &super::plan_shape::proposing_text(files),
        ));
    }
    out.extend(repro_first::question_gaps(&questions(files)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_output_is_read_whatever_the_exit() {
        let out = "===CODERHELM_ENV_RESTORED===\ninstall noise\n===CODERHELM_REPRO_OUT===\nNone\n\n===CODERHELM_REPRO_EXIT 0\n";
        assert_eq!(read_output(out), (Some(0), "None".to_string()));
        let (exit, body) = read_output(
            "===CODERHELM_REPRO_OUT===\nTraceback\nImportError\n===CODERHELM_REPRO_EXIT 1",
        );
        assert_eq!(exit, Some(1));
        assert!(body.ends_with("ImportError"));
        assert_eq!(read_output("nothing").0, None);
        let s = script("python -c 'print(1)'");
        assert!(s.starts_with("( python -c 'print(1)' )") && s.contains(EXIT_MARK));
        assert_eq!(tool().name, "reproduce_on_base");
    }
}
