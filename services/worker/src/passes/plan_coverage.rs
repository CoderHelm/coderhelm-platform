//! Before the first pull request opens, the plan is done: task by task.
//!
//! A complex ticket's plan listed several new files (a block, its schema,
//! its tabs, its tests). The first implement pass changed one file, ran out
//! of its time, and the pipeline went on: the proof check listed what was
//! missing as "gaps ... under Open items" and the pull request opened as if
//! the work were done. Review rounds then tried to build the rest.
//!
//! The plan's task checklist (`- [ ] path: what to do`, which the planner is
//! required to write) is compared with what the branch actually changes, one
//! task at a time ([`coverage`]). Planned tasks that aren't done get another
//! implement round aimed at exactly those tasks while there is time
//! ([`Next::Round`]); a run out of time continues in a fresh invocation from
//! its branch. If tasks the plan needs are still not done, the pull request
//! doesn't open: the run stops with a plain status naming them
//! ([`stop_reason`]). Only tasks the plan itself marks optional may be left
//! out, and the pull request then says so under Open items, with the reason
//! ([`optional_findings`]).

use tracing::{info, warn};

use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TokenUsage};
use crate::passes::plan::PlanResult;
use crate::passes::proof::{Finding, RunEvidence};
use crate::passes::FileCache;
use crate::WorkerState;

/// How many extra implement rounds missing planned tasks get in one
/// invocation, before the run continues in a fresh one.
pub const MAX_ROUNDS: u32 = 2;

/// One task of the plan's checklist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanTask {
    /// The file the task changes (or a directory, ending in `/`).
    pub path: String,
    /// What the task does, as the plan wrote it.
    pub task: String,
    /// The task creates the file.
    pub is_new: bool,
    /// The plan says the task may be left out.
    pub optional: bool,
}

/// Words that mark a task the plan leaves to judgment: never a reason to
/// hold the pull request.
const OPTIONAL_MARKS: &[&str] = &[
    "(optional)",
    "optional:",
    "if needed",
    "if necessary",
    "if required",
    "nice to have",
    "nice-to-have",
    "no change needed",
    "no changes needed",
    "verify only",
    "read only",
    "read-only",
];

fn is_optional(desc: &str) -> bool {
    let d = desc.trim().to_lowercase();
    d.starts_with("optional")
        || d.starts_with("consider ")
        || d.starts_with("maybe ")
        || OPTIONAL_MARKS.iter().any(|m| d.contains(m))
}

/// Every task of the checklist that names a file, in order. Lines that don't
/// follow the `- [ ] path: description` format are not tasks this check can
/// hold the change to, and are skipped.
pub fn plan_tasks(tasks_md: &str) -> Vec<PlanTask> {
    let mut out = Vec::new();
    for line in tasks_md.lines() {
        let l = line.trim_start();
        let Some(rest) = l
            .strip_prefix("- [ ]")
            .or_else(|| l.strip_prefix("- [x]"))
            .or_else(|| l.strip_prefix("- [X]"))
            .or_else(|| l.strip_prefix("* [ ]"))
        else {
            continue;
        };
        let Some((head, desc)) = rest.split_once(':') else {
            continue;
        };
        let path = head.trim().trim_matches(|c| c == '`' || c == '*').trim();
        let looks_like_path = !path.is_empty()
            && path.len() < 300
            && !path.chars().any(char::is_whitespace)
            && (path.contains('/') || path.contains('.'))
            && !path.contains("://");
        if !looks_like_path {
            continue;
        }
        let d = desc.to_lowercase();
        let is_new = d.contains("create")
            || d.contains("new file")
            || d.contains("(new)")
            || d.trim_start().starts_with("new ")
            || d.trim_start().starts_with("add new");
        out.push(PlanTask {
            path: path.trim_start_matches("./").to_string(),
            task: desc.trim().to_string(),
            is_new,
            optional: is_optional(desc),
        });
    }
    out
}

/// Which planned tasks the branch did and which it didn't.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Coverage {
    pub done: Vec<PlanTask>,
    pub missing: Vec<PlanTask>,
}

impl Coverage {
    /// How many tasks the plan has.
    pub fn total(&self) -> usize {
        self.done.len() + self.missing.len()
    }

    /// Tasks the plan needs that aren't done.
    pub fn required_missing(&self) -> Vec<&PlanTask> {
        self.missing.iter().filter(|t| !t.optional).collect()
    }

    /// Tasks the plan marks optional that aren't done.
    pub fn optional_missing(&self) -> Vec<&PlanTask> {
        self.missing.iter().filter(|t| t.optional).collect()
    }
}

fn norm(p: &str) -> &str {
    p.trim().trim_start_matches("./").trim_start_matches('/')
}

/// Whether the branch changes the task's file (or, for a directory, a file
/// in it). A deleted file shows in the diff too, so a removal counts.
fn path_changed(path: &str, changed: &[String]) -> bool {
    let path = norm(path);
    if path.ends_with('/') {
        return changed.iter().any(|c| norm(c).starts_with(path));
    }
    changed.iter().any(|c| norm(c) == path)
}

/// Each planned task, done when the branch changes its file. Structural, not
/// the coder's account: a task whose file the diff doesn't touch is not done
/// whatever the coder's summary says.
pub fn coverage(tasks: &[PlanTask], changed: &[String]) -> Coverage {
    let mut cov = Coverage::default();
    for t in tasks {
        if path_changed(&t.path, changed) {
            cov.done.push(t.clone());
        } else {
            cov.missing.push(t.clone());
        }
    }
    cov
}

/// What follows a coverage check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Next {
    /// Every task is done (or the plan has no checklist to hold it to).
    Complete,
    /// Only tasks the plan marks optional are left: the pull request opens
    /// and names them under Open items.
    OpenWithOptional,
    /// Run another implement round aimed at the missing tasks.
    Round,
    /// Tasks the plan needs are not done and this invocation has no round
    /// left: continue in a fresh invocation, or stop without a pull request.
    Unfinished,
}

/// Decides what follows a coverage check.
pub fn next(cov: &Coverage, rounds_done: u32, has_time: bool) -> Next {
    if cov.required_missing().is_empty() {
        return if cov.optional_missing().is_empty() {
            Next::Complete
        } else {
            Next::OpenWithOptional
        };
    }
    if rounds_done < MAX_ROUNDS && has_time {
        Next::Round
    } else {
        Next::Unfinished
    }
}

fn task_line(t: &PlanTask) -> String {
    format!("- [ ] `{}`: {}", t.path, t.task)
}

/// What the coder is told in a round: exactly the tasks still to do.
pub fn round_feedback(cov: &Coverage) -> String {
    let todo: Vec<String> = cov.required_missing().into_iter().map(task_line).collect();
    let done: Vec<String> = cov
        .done
        .iter()
        .map(|t| format!("- [x] `{}`", t.path))
        .collect();
    format!(
        "The plan is not done yet: the branch doesn't change the file of {} of its {} tasks. \
         Do these tasks now, in order, writing each task's files as soon as that task is done. \
         Read what is already on the branch first and build on it; don't redo finished tasks.\n\n\
         Still to do:\n{}\n\nAlready done on the branch:\n{}",
        todo.len(),
        cov.total(),
        todo.join("\n"),
        if done.is_empty() {
            "(none)".to_string()
        } else {
            done.join("\n")
        }
    )
}

/// The run's status when it stops with planned work not done: plain, and
/// naming the tasks (the run record keeps up to 500 characters; the full
/// list is in [`progress_note`]).
pub fn stop_reason(cov: &Coverage, branch: &str) -> String {
    let missing = cov.required_missing();
    let paths: Vec<String> = missing.iter().map(|t| format!("`{}`", t.path)).collect();
    let mut list = String::new();
    for (i, p) in paths.iter().enumerate() {
        if list.len() + p.len() > 280 {
            list.push_str(&format!(" and {} more", paths.len() - i));
            break;
        }
        if i > 0 {
            list.push_str(", ");
        }
        list.push_str(p);
    }
    format!(
        "Stopped before opening a pull request: {} of {} planned tasks are not done ({list}). \
         The work so far is on branch `{branch}`; Retry continues from the plan.",
        missing.len(),
        cov.total(),
    )
}

/// A progress note with every task and whether it's done.
pub fn progress_note(cov: &Coverage) -> String {
    let mut lines = vec![format!(
        "Plan check: {} of {} planned tasks done",
        cov.done.len(),
        cov.total()
    )];
    for t in &cov.done {
        lines.push(format!("- [x] {}", t.path));
    }
    for t in &cov.missing {
        let note = if t.optional { " (optional)" } else { "" };
        lines.push(format!("- [ ] {}{note}", t.path));
    }
    lines.join("\n")
}

/// The optional tasks left out, as Open items for a person, each with why.
pub fn optional_findings(cov: &Coverage) -> Vec<Finding> {
    cov.optional_missing()
        .into_iter()
        .map(|t| Finding {
            point: "optional_task".to_string(),
            detail: format!("`{}`: {}", t.path, t.task),
            settle: "The plan marks this task optional, so the pull request opens without it; \
                     do it here or in a follow-up if it's wanted."
                .to_string(),
            ..Default::default()
        })
        .collect()
}

/// The plan's checklist with only the done tasks checked: what the run page
/// and a retry read, instead of every task ticked when the pass ended.
pub fn checked_tasks_md(tasks_md: &str, cov: &Coverage) -> String {
    tasks_md
        .lines()
        .map(|line| {
            let done = plan_tasks(line)
                .first()
                .is_some_and(|t| cov.done.iter().any(|d| d.path == t.path));
            if done {
                line.replacen("- [ ]", "- [x]", 1)
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The files the branch changes against its base, or `None` when GitHub
/// can't say (the check then can't hold the change and steps aside).
pub async fn changed_files(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base: &str,
    branch: &str,
) -> Option<Vec<String>> {
    match github.get_diff(owner, repo, base, branch).await {
        Ok(d) => Some(
            d["files"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|f| f["filename"].as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
        ),
        Err(e) => {
            warn!(error = %e, "Plan check could not read the branch's diff");
            None
        }
    }
}

/// Everything a plan round needs, as the pipeline has it.
pub struct Work<'a> {
    pub state: &'a WorkerState,
    pub msg: &'a TicketMessage,
    pub github: &'a GitHubClient,
    /// The plan, with this repository's tasks.
    pub plan: &'a PlanResult,
    pub branch: &'a str,
    pub rules: &'a [String],
    pub repo_instructions: &'a str,
    pub provider: &'a ModelProvider,
    pub file_cache: &'a FileCache,
    pub run_id: &'a str,
    pub deadline: Option<std::time::Instant>,
}

/// Checks the plan against the branch and runs the implement rounds missing
/// tasks earn while there is time. Returns the coverage it ended with, or
/// `None` when the plan has no checklist or the diff can't be read.
/// `evidence` gains what the rounds looked up.
pub async fn close_gaps(
    w: &Work<'_>,
    evidence: &mut RunEvidence,
    usage: &mut TokenUsage,
    has_time: impl Fn() -> bool,
) -> Option<Coverage> {
    let tasks = plan_tasks(&w.plan.tasks);
    if tasks.is_empty() {
        return None;
    }
    let mut rounds = 0;
    loop {
        let changed = changed_files(
            w.github,
            &w.msg.repo_owner,
            &w.msg.repo_name,
            &w.msg.base_branch,
            w.branch,
        )
        .await?;
        let cov = coverage(&tasks, &changed);
        info!(
            run_id = w.run_id,
            done = cov.done.len(),
            missing = cov.missing.len(),
            rounds,
            "Plan check"
        );
        if next(&cov, rounds, has_time()) != Next::Round {
            return Some(cov);
        }
        rounds += 1;
        super::add_progress_note(
            w.state,
            &w.msg.team_id,
            w.run_id,
            &format!(
                "{}\nWorking on the {} task(s) not done yet (round {rounds})",
                progress_note(&cov),
                cov.required_missing().len()
            ),
        )
        .await;
        if super::check_cancelled(w.state, &w.msg.team_id, w.run_id)
            .await
            .is_err()
        {
            return Some(cov);
        }
        let _ = super::update_pass(w.state, &w.msg.team_id, w.run_id, "implement").await;
        let pass_start = std::time::Instant::now();
        let usage_before = usage.clone();
        let result = super::implement::run(
            w.state,
            w.msg,
            w.github,
            w.plan,
            w.branch,
            w.rules,
            w.repo_instructions,
            Some(&round_feedback(&cov)),
            "medium",
            w.provider,
            usage,
            w.file_cache,
            Some(w.run_id),
            w.deadline,
            // Planned files the PR doesn't change yet are exactly what this
            // round must write.
            false,
        )
        .await;
        super::write_pass_trace(
            w.state,
            &w.msg.team_id,
            w.run_id,
            "implement:plan_round",
            pass_start,
            &usage_before,
            usage,
            None,
        )
        .await;
        match result {
            Ok(r) => {
                let timed_out = r.timed_out;
                evidence.merge(r.evidence);
                if timed_out {
                    // No time for another round here; what it wrote counts.
                    rounds = MAX_ROUNDS;
                }
            }
            Err(e) => {
                warn!(run_id = w.run_id, error = %e, "Plan round failed");
                rounds = MAX_ROUNDS;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PLAN: &str = "## Tasks\n\
        - [ ] `src/blocks/gallery/index.ts`: Create the gallery block component (new file)\n\
        - [ ] `src/blocks/gallery/schema.ts`: Create the block's schema\n\
        - [ ] `src/blocks/gallery/tabs.tsx`: Add the tabs for each section\n\
        - [ ] `src/blocks/registry.ts`: Register the gallery block\n\
        - [ ] `src/blocks/gallery/index.test.ts`: Add tests for the block\n\
        - [ ] `docs/blocks.md`: (optional) Mention the block in the docs\n\
        - [ ] Run the type-check\n";

    fn changed(paths: &[&str]) -> Vec<String> {
        paths.iter().map(|p| p.to_string()).collect()
    }

    #[test]
    fn reads_each_task_with_its_file_and_whether_it_is_optional() {
        let tasks = plan_tasks(PLAN);
        assert_eq!(tasks.len(), 6, "a line without a file is not a task here");
        assert_eq!(tasks[0].path, "src/blocks/gallery/index.ts");
        assert!(tasks[0].is_new);
        assert!(!tasks[0].optional);
        assert_eq!(tasks[3].task, "Register the gallery block");
        assert!(tasks[5].optional);
        for opt in [
            "Optional: add a story",
            "Update the README if needed",
            "No change needed, just confirm the export",
            "consider adding a cache",
        ] {
            assert!(is_optional(opt), "{opt}");
        }
        assert!(!is_optional("Create the block's schema"));
    }

    #[test]
    fn a_pass_that_changed_one_file_of_a_six_task_plan_is_not_done() {
        // The observed run: one file changed, the rest of the plan missing.
        let cov = coverage(&plan_tasks(PLAN), &changed(&["src/blocks/registry.ts"]));
        assert_eq!(cov.done.len(), 1);
        assert_eq!(cov.required_missing().len(), 4);
        assert_eq!(cov.optional_missing().len(), 1);
        assert_eq!(next(&cov, 0, true), Next::Round);
        assert_eq!(next(&cov, 1, true), Next::Round);
        // Out of rounds or time: never "open the PR anyway".
        assert_eq!(next(&cov, MAX_ROUNDS, true), Next::Unfinished);
        assert_eq!(next(&cov, 0, false), Next::Unfinished);
    }

    #[test]
    fn only_optional_tasks_left_opens_with_them_named() {
        let all_but_docs = changed(&[
            "src/blocks/gallery/index.ts",
            "./src/blocks/gallery/schema.ts",
            "src/blocks/gallery/tabs.tsx",
            "src/blocks/registry.ts",
            "src/blocks/gallery/index.test.ts",
        ]);
        let cov = coverage(&plan_tasks(PLAN), &all_but_docs);
        assert_eq!(next(&cov, 0, true), Next::OpenWithOptional);
        let items = optional_findings(&cov);
        assert_eq!(items.len(), 1);
        assert!(items[0].detail.contains("docs/blocks.md"));
        assert!(items[0].settle.contains("optional"));
        // An optional task is a person's to decide, never a fix round.
        assert_eq!(items[0].settler(), crate::passes::proof::Settler::Owner);

        let mut everything = all_but_docs.clone();
        everything.push("docs/blocks.md".into());
        assert_eq!(
            next(&coverage(&plan_tasks(PLAN), &everything), 0, true),
            Next::Complete
        );
    }

    #[test]
    fn a_directory_task_is_done_by_any_file_in_it() {
        let tasks = plan_tasks("- [ ] `src/blocks/gallery/`: Add the block's files\n");
        assert_eq!(tasks.len(), 1);
        let cov = coverage(&tasks, &changed(&["src/blocks/gallery/a.ts"]));
        assert!(cov.missing.is_empty());
        let cov = coverage(&tasks, &changed(&["src/blocks/other/a.ts"]));
        assert_eq!(cov.missing.len(), 1);
    }

    #[test]
    fn the_round_is_told_exactly_what_is_left() {
        let cov = coverage(&plan_tasks(PLAN), &changed(&["src/blocks/registry.ts"]));
        let fb = round_feedback(&cov);
        assert!(fb.contains("src/blocks/gallery/schema.ts"));
        assert!(fb.contains("- [x] `src/blocks/registry.ts`"));
        // Optional tasks are not pushed on the round.
        assert!(!fb.contains("docs/blocks.md"));
        assert!(fb.contains("as soon as that task is done"));
    }

    #[test]
    fn the_stop_status_names_the_missing_tasks_plainly() {
        let cov = coverage(&plan_tasks(PLAN), &changed(&["src/blocks/registry.ts"]));
        let r = stop_reason(&cov, "feature/gallery");
        assert!(r.starts_with("Stopped before opening a pull request: 4 of 6 planned tasks"));
        assert!(r.contains("`src/blocks/gallery/index.ts`"));
        assert!(r.contains("`feature/gallery`"));
        assert!(!r.contains("docs/blocks.md"));
        assert!(r.len() <= 500, "fits the run record's reason");
        let note = progress_note(&cov);
        assert!(note.contains("1 of 6 planned tasks done"));
        assert!(note.contains("- [ ] docs/blocks.md (optional)"));
    }

    #[test]
    fn a_long_list_is_cut_but_counted() {
        let plan: String = (0..40)
            .map(|i| format!("- [ ] `src/very/long/path/to/module_{i}/file_{i}.ts`: Create it\n"))
            .collect();
        let cov = coverage(&plan_tasks(&plan), &[]);
        let r = stop_reason(&cov, "b");
        assert!(r.contains("more)"));
        assert!(r.len() <= 500);
    }

    #[test]
    fn only_done_tasks_are_checked_off() {
        let cov = coverage(&plan_tasks(PLAN), &changed(&["src/blocks/registry.ts"]));
        let md = checked_tasks_md(PLAN, &cov);
        assert!(md.contains("- [x] `src/blocks/registry.ts`"));
        assert!(md.contains("- [ ] `src/blocks/gallery/schema.ts`"));
        assert!(md.contains("- [ ] Run the type-check"));
    }
}
