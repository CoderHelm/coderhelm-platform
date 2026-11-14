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
//!
//! A task whose file a code generator writes (a generated types file, an
//! extracted schema) is not the coder's to write: the generator check
//! (`generators::gate`) runs the generator before the pull request opens
//! and blocks it when its output is out of date. Such a task never holds
//! the plan check ([`mark_generated`]): a run once looped through every
//! continuation on two generated files the generator, run each step, left
//! unchanged, and then ended saying it changed nothing.

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
    /// A code generator writes the file: the generator check decides it.
    pub generated: bool,
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
    // A task the supervisor deferred with its reason (`with_deferred`).
    "(deferred:",
];

/// What the coder says when it judged a planned file needs no change.
const NO_CHANGE_MARKS: &[&str] = &[
    "needs no change",
    "need no change",
    "no change needed",
    "no changes needed",
    "needs no edit",
    "doesn't need a change",
    "doesn't need any change",
    "does not need a change",
    "does not need any change",
    "doesn't need changing",
    "nothing to change",
    // How coders actually say it (a run looped two steps on a file its
    // coder twice said "needed no code change" and "stays as it is").
    "needed no change",
    "need no code change",
    "needs no code change",
    "needed no code change",
    "no code change is needed",
    "no code change needed",
    "no change is needed",
    "no changes are needed",
    "requires no change",
    "required no change",
    "stays as it is",
    "stays as is",
    "left as it is",
    "left unchanged",
    "stays unchanged",
];

/// The line a coder writes to settle a planned task without a change
/// (`round_feedback` asks for it): `No change needed: <path>: <why>`.
pub const NO_CHANGE_LINE: &str = "No change needed:";

/// The coder's own line saying `path` needs no change, when it wrote one: the
/// line names the file (its path or, when that is how the coder wrote it,
/// its file name) and says no change is needed.
pub fn declined_by_coder(notes: &str, path: &str) -> Option<String> {
    let name = path.rsplit('/').next().unwrap_or(path).to_lowercase();
    if name.is_empty() {
        return None;
    }
    notes.lines().find_map(|l| {
        let low = l.to_lowercase();
        (low.contains(&name) && NO_CHANGE_MARKS.iter().any(|m| low.contains(m))).then(|| {
            let t = l
                .trim()
                .trim_start_matches(['-', '*', ' '])
                .replace("**", "");
            common::truncate_str(t.trim(), 300).to_string()
        })
    })
}

fn is_optional(desc: &str) -> bool {
    let d = desc.trim().to_lowercase();
    d.starts_with("optional")
        || d.starts_with("consider ")
        || d.starts_with("maybe ")
        || OPTIONAL_MARKS.iter().any(|m| d.contains(m))
}

/// Words that say the task's file is a generator's output.
const GENERATED_MARKS: &[&str] = &[
    "regenerate",
    "re-generate",
    "generated by",
    "(generated)",
    "generated file",
    "auto-generated",
    "autogenerated",
    "schema extract",
];

fn is_generated_desc(desc: &str) -> bool {
    let d = desc.to_lowercase();
    GENERATED_MARKS.iter().any(|m| d.contains(m))
}

/// Marks the tasks whose file a detected generator writes (`outputs`:
/// the generators' output globs).
pub fn mark_generated(tasks: &mut [PlanTask], outputs: &[String]) {
    for t in tasks.iter_mut() {
        if outputs
            .iter()
            .any(|g| super::generators::glob_match(g, &t.path))
        {
            t.generated = true;
        }
    }
}

/// The plan's tasks for `branch`, with the ones a detected generator
/// writes marked.
pub async fn tasks_for(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    branch: &str,
    tasks_md: &str,
) -> Vec<PlanTask> {
    let mut tasks = plan_tasks(tasks_md);
    if tasks.is_empty() {
        return tasks;
    }
    let files = super::generators::load(github, owner, repo, branch).await;
    let outputs: Vec<String> = super::generators::detect(&files)
        .into_iter()
        .flat_map(|g| g.outputs)
        .collect();
    mark_generated(&mut tasks, &outputs);
    tasks
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
            path: norm(path),
            task: desc.trim().to_string(),
            is_new,
            optional: is_optional(desc),
            generated: is_generated_desc(desc),
        });
    }
    out
}

/// Which planned tasks the branch did and which it didn't.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Coverage {
    pub done: Vec<PlanTask>,
    pub missing: Vec<PlanTask>,
    /// The done tasks whose file only the branch this one is stacked on
    /// changes: the pull request ships on top of it, so they are done there,
    /// not in this branch's own diff.
    pub on_base: Vec<String>,
}

impl Coverage {
    /// How many tasks the plan has.
    pub fn total(&self) -> usize {
        self.done.len() + self.missing.len()
    }

    /// Tasks the plan needs that aren't done (a generator's output is the
    /// generator check's, not the coder's).
    pub fn required_missing(&self) -> Vec<&PlanTask> {
        self.missing
            .iter()
            .filter(|t| !t.optional && !t.generated)
            .collect()
    }

    /// Generated files the plan names that the branch doesn't change yet:
    /// the generator check runs their generator before the pull request.
    pub fn generated_missing(&self) -> Vec<&PlanTask> {
        self.missing
            .iter()
            .filter(|t| t.generated && !t.optional)
            .collect()
    }

    /// Tasks the plan marks optional that aren't done.
    pub fn optional_missing(&self) -> Vec<&PlanTask> {
        self.missing.iter().filter(|t| t.optional).collect()
    }
}

/// Drops the missing tasks that are not core to the ticket: a
/// documentation file (README, CHANGELOG, docs) the ticket doesn't name.
/// They become optional (named under Open items) and never hold the run; a
/// dependency run once spent five steps on a README table no one asked for
/// and stopped asking a person. Returns the tasks it dropped.
pub fn drop_non_core(cov: &mut Coverage, ticket_body: &str) -> Vec<PlanTask> {
    let mut dropped = vec![];
    for t in cov.missing.iter_mut() {
        if !t.optional && !t.generated && !common::dep_fix::core_to_dep_fix(&t.path, ticket_body) {
            t.optional = true;
            dropped.push(t.clone());
        }
    }
    dropped
}

/// One spelling for a path (`..` resolved), the one the diff uses.
fn norm(p: &str) -> String {
    super::generators::normalize_path(p)
}

/// Whether the branch changes the task's file (or, for a directory, a file
/// in it). A deleted file shows in the diff too, so a removal counts.
fn path_changed(path: &str, changed: &[String]) -> bool {
    let path = norm(path);
    if path.ends_with('/') {
        return changed.iter().any(|c| norm(c).starts_with(&path));
    }
    changed.iter().any(|c| norm(c) == path)
}

/// The coverage once the generator check has settled generated files:
/// a generated task whose file its generator regenerated and committed, or
/// verified up to date (`settled`: those generators' output globs), is
/// done. The plan check, the proof check and the generator check then agree.
pub fn settle_generated(mut cov: Coverage, settled: &[String]) -> Coverage {
    let (now_done, still): (Vec<PlanTask>, Vec<PlanTask>) =
        cov.missing.into_iter().partition(|t| {
            t.generated
                && settled
                    .iter()
                    .any(|g| super::generators::glob_match(g, &t.path))
        });
    cov.done.extend(now_done);
    cov.missing = still;
    cov
}

/// Each planned task, done when the branch changes its file. Structural, not
/// the coder's account: a task whose file the diff doesn't touch is not done
/// whatever the coder's summary says. Every plan check in the run measures
/// with [`plan_changes`] (a stacked base adds its changes); this is that
/// measure with no stacked base, kept for the tests.
#[cfg(test)]
pub fn coverage(tasks: &[PlanTask], changed: &[String]) -> Coverage {
    stacked_coverage(tasks, changed, &[])
}

/// [`coverage`] for a branch stacked on another open pull request's branch:
/// `on_base` is what that branch changes against the repository's default
/// branch. A task whose file only it changes is done there: the pull request
/// ships on top of it, and the coder, reading the branch, finds the file
/// already in its planned form. Counting such a task as missing sent a run
/// round after round with nothing to write until it was stopped.
pub fn stacked_coverage(tasks: &[PlanTask], changed: &[String], on_base: &[String]) -> Coverage {
    let mut cov = Coverage::default();
    for t in tasks {
        if path_changed(&t.path, changed) {
            cov.done.push(t.clone());
        } else if path_changed(&t.path, on_base) {
            cov.on_base.push(t.path.clone());
            cov.done.push(t.clone());
        } else {
            cov.missing.push(t.clone());
        }
    }
    cov
}

/// What the plan check reads: the branch's own diff against its base and,
/// when that base is another open pull request's branch (not the
/// repository's default), the files that branch changes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanChanges {
    pub own: Vec<String>,
    pub on_base: Vec<String>,
}

impl PlanChanges {
    pub fn coverage(&self, tasks: &[PlanTask]) -> Coverage {
        stacked_coverage(tasks, &self.own, &self.on_base)
    }
}

/// Whether `base` is another branch the run is stacked on, not the
/// repository's default branch.
pub fn stacked_base(base: &str, default: &str) -> bool {
    !default.is_empty() && !base.is_empty() && base != default
}

/// [`changed_files`] plus, for a stacked base, what that base changes
/// against the repository's default branch. `None` when the branch's own
/// diff can't be read; a base whose diff can't be read adds nothing.
pub async fn plan_changes(
    github: &GitHubClient,
    owner: &str,
    repo: &str,
    base: &str,
    branch: &str,
) -> Option<PlanChanges> {
    let own = changed_files(github, owner, repo, base, branch).await?;
    let default = github
        .get_default_branch(owner, repo)
        .await
        .unwrap_or_default();
    let on_base = if stacked_base(base, &default) {
        changed_files(github, owner, repo, &default, base)
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    Some(PlanChanges { own, on_base })
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
         Still to do:\n{}\n\nAlready done on the branch:\n{}\n\n{}",
        todo.len(),
        cov.total(),
        todo.join("\n"),
        if done.is_empty() {
            "(none)".to_string()
        } else {
            done.join("\n")
        },
        settle_without_change()
    )
}

/// How a task is settled without a change, said the same way every time.
fn settle_without_change() -> String {
    format!(
        "If, after reading a task's file, it truly needs no change (the code already does what \
         the task asks), don't edit it: write one line `{NO_CHANGE_LINE} <path>: <why, with the \
         code that already does it>` in your final message. That settles the task; it is listed \
         on the pull request for a person to check."
    )
}

/// A different approach after a step changed nothing: one task only (the
/// first one left), what the branch already changes, and the two ways to
/// finish it (the change, or the no-change line). `diff_files` is what the
/// branch changes so far.
pub fn narrowed_feedback(cov: &Coverage, diff_files: &[String]) -> Option<String> {
    let missing = cov.required_missing();
    let first = missing.first()?;
    let n = cov.done.len() + 1;
    let files = if diff_files.is_empty() {
        "(nothing yet)".to_string()
    } else {
        diff_files
            .iter()
            .take(40)
            .map(|f| format!("- `{f}`"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    Some(format!(
        "The last step changed nothing on the branch while this task was open, so this step \
         takes a different approach: one task only. Start fresh from the plan and the branch as \
         it is now; don't re-read or redo finished work.\n\n\
         The task (task {n} of {total}):\n{task}\n\n\
         The branch already changes these files (the other tasks are done there):\n{files}\n\n\
         Do exactly one of these, then stop:\n\
         1. Make the change the task asks for in `{path}` and save the file.\n\
         2. If the file already does what the task asks, write the line \
         `{NO_CHANGE_LINE} {path}: <why, with the code that already does it>` in your final \
         message. Saying it needs no change any other way leaves the task open, and the run \
         stops after this step.{more}",
        total = cov.total(),
        task = task_line(first),
        path = first.path,
        more = if missing.len() > 1 {
            format!(
                "\n\n{} more task(s) are left after this one; the next step takes them.",
                missing.len() - 1
            )
        } else {
            String::new()
        }
    ))
}

/// Marks the tasks whose file the coder said needs no change (in `notes`)
/// and the branch doesn't change: optional from now on, with the coder's
/// line. Returns the paths it settled.
pub fn settle_declined(tasks: &mut [PlanTask], cov: &Coverage, notes: &str) -> Vec<String> {
    let missing: Vec<String> = cov
        .required_missing()
        .iter()
        .map(|t| t.path.clone())
        .collect();
    let mut settled = Vec::new();
    for t in tasks.iter_mut().filter(|t| missing.contains(&t.path)) {
        if let Some(why) = declined_by_coder(notes, &t.path) {
            info!(path = %t.path, "The coder found a planned file needs no change");
            t.optional = true;
            t.task = format!("{} (the coder found it needs no change: {why})", t.task);
            settled.push(t.path.clone());
        }
    }
    settled
}

/// The plan's checklist with each settled task's line saying so, so a later
/// step reads the task as optional (`is_optional` matches "no change
/// needed") instead of asking for it again.
pub fn with_declined(tasks_md: &str, tasks: &[PlanTask]) -> String {
    tasks_md
        .lines()
        .map(|line| {
            let Some(task) = plan_tasks(line).into_iter().next() else {
                return line.to_string();
            };
            match tasks.iter().find(|t| t.path == task.path) {
                Some(t) if t.optional && !task.optional => {
                    let why = t
                        .task
                        .split_once("(the coder found it needs no change: ")
                        .map(|(_, w)| w.trim_end_matches(')').trim())
                        .unwrap_or("");
                    format!(
                        "{line} (no change needed: {})",
                        common::truncate_str(why, 300)
                    )
                }
                _ => line.to_string(),
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The plan's checklist with each deferred task's line carrying its reason
/// (`(deferred: ...)`), so every later check reads it as settled and the
/// pull request lists it with why. `defer` holds (path, reason) pairs; the
/// reason starts with `deferred:`.
pub fn with_deferred(tasks_md: &str, defer: &[(String, String)]) -> String {
    tasks_md
        .lines()
        .map(|line| {
            let Some(task) = plan_tasks(line).into_iter().next() else {
                return line.to_string();
            };
            match defer.iter().find(|(p, _)| *p == task.path) {
                Some((_, why)) if !task.optional => {
                    let why = why.trim().trim_start_matches("deferred:").trim();
                    format!("{line} (deferred: {})", common::truncate_str(why, 300))
                }
                _ => line.to_string(),
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The run's status when it stops with planned work not done: plain, and
/// naming the tasks (the run record keeps up to 500 characters; the full
/// list is in [`progress_note`]).
pub fn stop_reason(cov: &Coverage, branch: &str) -> String {
    stop_reason_why(cov, branch, "")
}

/// [`stop_reason`] with why the tasks aren't done, when the run knows.
pub fn stop_reason_why(cov: &Coverage, branch: &str, why: &str) -> String {
    let missing = cov.required_missing();
    let paths: Vec<String> = missing.iter().map(|t| format!("`{}`", t.path)).collect();
    let why = why.trim().trim_end_matches('.');
    let room = 280usize.saturating_sub(why.len());
    let mut list = String::new();
    for (i, p) in paths.iter().enumerate() {
        if list.len() + p.len() > room {
            list.push_str(&format!(" and {} more", paths.len() - i));
            break;
        }
        if i > 0 {
            list.push_str(", ");
        }
        list.push_str(p);
    }
    format!(
        "Stopped before opening a pull request: {} of {} planned tasks are not done ({list}){}. \
         The work so far is on branch `{branch}`; Retry continues from the plan.",
        missing.len(),
        cov.total(),
        if why.is_empty() {
            String::new()
        } else {
            format!(": {why}")
        },
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
        let note = if cov.on_base.contains(&t.path) {
            " (changed by the branch this one is stacked on)"
        } else {
            ""
        };
        lines.push(format!("- [x] {}{note}", t.path));
    }
    for t in &cov.missing {
        let note = if t.optional {
            " (optional)"
        } else if t.generated {
            " (generated: the generator check runs it before the pull request)"
        } else {
            ""
        };
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

/// The outside steps the plan check counts: none of them is done by the
/// branch. A step handed to a person counts as not done, with its recorded
/// reason; one handed out with no reason is not covered at all.
pub fn outside_findings(d: Option<&super::fix_location::FixDecision>) -> Vec<Finding> {
    let Some(c) = d
        .filter(|d| d.effective_location() == super::fix_location::FixLocation::Both)
        .and_then(|d| d.outside_change.as_ref())
    else {
        return Vec::new();
    };
    let h = super::outside_work::held(c);
    let mut out = Vec::new();
    for l in h.unjustified {
        out.push(Finding {
            point: "planned_work".to_string(),
            detail: format!("Not covered: {l}"),
            settle: "The plan gives this to a person without saying why CoderHelm couldn't do \
                     it or how to grant the access: do it in the change or let CoderHelm do it, \
                     or record the missing permission and how to grant it."
                .to_string(),
            ..Default::default()
        });
    }
    for l in h.by_person {
        out.push(Finding {
            point: "planned_work".to_string(),
            detail: format!("Not done by this run (a person's step): {l}"),
            settle: "Done when its owner makes it and the confirm query returns what the note \
                     expects."
                .to_string(),
            ..Default::default()
        });
    }
    for l in h.not_done {
        out.push(Finding {
            point: "planned_work".to_string(),
            detail: format!("Not done by this run: {l}"),
            settle: "Done when the access it names is granted and the run is retried, or its \
                     owner makes it and the confirm query returns what the note expects."
                .to_string(),
            ..Default::default()
        });
    }
    out
}

/// A progress note counting the outside steps.
pub fn outside_note(d: Option<&super::fix_location::FixDecision>) -> String {
    let Some(c) = d.and_then(|d| d.outside_change.as_ref()) else {
        return String::new();
    };
    let h = super::outside_work::held(c);
    format!(
        "Plan check (outside the code): {} step(s) for a person with a recorded reason, {} handed out with no reason (not covered)",
        h.by_person.len(),
        h.unjustified.len()
    )
}

/// The plan's checklist with only the done tasks checked: what the run page
/// and a retry read, instead of every task ticked when the pass ended.
pub fn checked_tasks_md(tasks_md: &str, cov: &Coverage) -> String {
    tasks_md
        .lines()
        .map(|line| {
            // A line that isn't a task with a file stays as written.
            let Some(task) = plan_tasks(line).into_iter().next() else {
                return line.to_string();
            };
            let done = cov.done.iter().any(|d| d.path == task.path);
            if done {
                line.replacen("- [ ]", "- [x]", 1)
            } else {
                // The plan check is the one place a task is unchecked: its
                // file was reverted, or the branch never changed it.
                line.replacen("- [x]", "- [ ]", 1)
                    .replacen("- [X]", "- [ ]", 1)
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
    let mut tasks = tasks_for(
        w.github,
        &w.msg.repo_owner,
        &w.msg.repo_name,
        w.branch,
        &w.plan.tasks,
    )
    .await;
    if tasks.is_empty() {
        return None;
    }
    let mut rounds = 0;
    // The implement pass may already have said a planned file needs no
    // change: that settles the task before any round asks for it again.
    let mut notes = evidence.notes.clone();
    loop {
        let cov = plan_changes(
            w.github,
            &w.msg.repo_owner,
            &w.msg.repo_name,
            &w.msg.base_branch,
            w.branch,
        )
        .await?
        .coverage(&tasks);
        let settled = settle_declined(&mut tasks, &cov, &notes);
        notes.clear();
        let cov = if settled.is_empty() {
            cov
        } else {
            let mut c = cov;
            for t in c.missing.iter_mut() {
                if let Some(n) = tasks.iter().find(|n| n.path == t.path) {
                    *t = n.clone();
                }
            }
            c
        };
        info!(
            run_id = w.run_id,
            done = cov.done.len(),
            missing = cov.missing.len(),
            on_base = cov.on_base.len(),
            settled = settled.len(),
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
            // round must write: the plan's task files are its files.
            super::write_guard::RoundFor::PlannedWork,
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
                // Told to do a task, the coder said why its file needs no
                // change: that is its judgment, listed on the pull request
                // for a person, never another round or step asking the same
                // thing (settled at the top of the loop).
                notes = r.evidence.notes.clone();
                super::write_conversation_log(
                    w.state,
                    &w.msg.team_id,
                    w.run_id,
                    "implement_plan_round",
                    &r.conversation_log,
                )
                .await;
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

    /// The production run: 32 of 33 tasks done, the open one the generated
    /// types the studio's typegen writes to `../web/...`. Once the
    /// generator check settles it, the plan check agrees it is done.
    #[test]
    fn a_settled_generated_task_is_done() {
        let tasks_md = "- [ ] apps/studio/schemaTypes/blocks/hero.ts: add the block\n\
                        - [ ] apps/web/src/lib/sanity/sanity.types.ts: (generated: the generator check runs it before the pull request)\n";
        let mut tasks = plan_tasks(tasks_md);
        mark_generated(
            &mut tasks,
            &["apps/studio/../web/src/lib/sanity/sanity.types.ts".to_string()],
        );
        assert!(tasks[1].generated);
        let cov = coverage(
            &tasks,
            &["apps/studio/schemaTypes/blocks/hero.ts".to_string()],
        );
        assert_eq!(cov.generated_missing().len(), 1);
        assert!(cov.required_missing().is_empty());
        let settled = vec!["apps/web/src/lib/sanity/sanity.types.ts".to_string()];
        let cov = settle_generated(cov, &settled);
        assert_eq!(cov.done.len(), 2);
        assert!(cov.missing.is_empty());
        assert!(checked_tasks_md(tasks_md, &cov).matches("- [x]").count() == 2);
        // A path spelled with `..` in the diff counts as the task's file.
        let cov = coverage(
            &tasks,
            &["apps/studio/../web/src/lib/sanity/sanity.types.ts".to_string()],
        );
        assert!(cov.done.iter().any(|t| t.path.ends_with("sanity.types.ts")));
        // Nothing settled: still the generator check's.
        let cov = settle_generated(coverage(&tasks, &[]), &[]);
        assert_eq!(cov.generated_missing().len(), 1);
    }

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

    /// The observed run: every task done but a generated types file and an
    /// extracted schema, which the generator left unchanged in each step.
    /// The plan check hands them to the generator check instead of
    /// continuing on them until the run ends.
    #[test]
    fn generated_files_never_hold_the_plan_check() {
        let plan = "- [ ] `studio/schemaTypes/blocks/hero.ts`: Create the block schema\n\
            - [ ] `studio/schema.json`: Regenerate with schema extract\n\
            - [ ] `web/src/sanity.types.ts`: Update the types for the new block\n";
        let mut tasks = plan_tasks(plan);
        assert!(!tasks[0].generated);
        assert!(tasks[1].generated, "the task says so");
        assert!(!tasks[2].generated, "only the generator's outputs say so");
        mark_generated(&mut tasks, &["web/**/sanity.types.ts".to_string()]);
        assert!(tasks[2].generated);
        let cov = coverage(&tasks, &changed(&["studio/schemaTypes/blocks/hero.ts"]));
        assert!(cov.required_missing().is_empty());
        assert_eq!(cov.generated_missing().len(), 2);
        assert_eq!(next(&cov, MAX_ROUNDS, false), Next::Complete);
        let note = progress_note(&cov);
        assert!(note.contains("- [ ] web/src/sanity.types.ts (generated"));
        // Not pushed on a coder round either.
        assert!(!round_feedback(&cov).contains("sanity.types.ts"));
    }

    #[test]
    fn the_stop_status_can_say_why() {
        let cov = coverage(&plan_tasks(PLAN), &changed(&["src/blocks/registry.ts"]));
        let r = stop_reason_why(&cov, "b", "the last step changed no file for them.");
        assert!(r.contains("(`src/blocks/gallery/index.ts`"));
        assert!(r.contains(": the last step changed no file for them. The work so far"));
        let plan: String = (0..40)
            .map(|i| format!("- [ ] `src/very/long/path/to/module_{i}/file_{i}.ts`: Create it\n"))
            .collect();
        let long = stop_reason_why(
            &coverage(&plan_tasks(&plan), &[]),
            "coderhelm-wip/feature/a-long-branch-name",
            "the last step changed no file for them",
        );
        assert!(long.len() <= 500, "{}", long.len());
    }

    #[test]
    fn only_done_tasks_are_checked_off() {
        let cov = coverage(&plan_tasks(PLAN), &changed(&["src/blocks/registry.ts"]));
        let md = checked_tasks_md(PLAN, &cov);
        assert!(md.contains("- [x] `src/blocks/registry.ts`"));
        assert!(md.contains("- [ ] `src/blocks/gallery/schema.ts`"));
        assert!(md.contains("- [ ] Run the type-check"));
    }

    #[test]
    fn an_outside_step_without_a_reason_is_not_covered() {
        use crate::passes::fix_location::{FixDecision, FixLocation, OutsideChange};
        use crate::passes::outside_work::NoteStep;
        let d = FixDecision {
            fix_location: FixLocation::Both,
            outside_change: Some(OutsideChange {
                system: "the CMS".into(),
                change: "Publish the block".into(),
                steps: vec![
                    NoteStep {
                        step: "Publish on production".into(),
                        by: "person".into(),
                        owner: "an editor".into(),
                        ..Default::default()
                    },
                    NoteStep {
                        step: "Approve the production copy".into(),
                        by: "person".into(),
                        owner: "an editor".into(),
                        why_person: "production content needs an editor's approval".into(),
                        grant: "none: CoderHelm never writes production".into(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        };
        let f = outside_findings(Some(&d));
        assert_eq!(f.len(), 2);
        assert!(f[0]
            .detail
            .starts_with("Not covered: 1. Publish on production"));
        assert!(f[1]
            .detail
            .contains("Why a person: production content needs an editor's approval"));
        assert!(outside_note(Some(&d)).contains("1 handed out with no reason"));
        assert!(outside_findings(None).is_empty());
    }

    #[test]
    fn a_coderhelm_step_that_ended_not_done_is_an_open_item() {
        use crate::passes::fix_location::{FixDecision, FixLocation, OutsideChange};
        use crate::passes::outside_work::NoteStep;
        let d = FixDecision {
            fix_location: FixLocation::Both,
            outside_change: Some(OutsideChange {
                system: "the CMS".into(),
                change: "Replace the hero".into(),
                steps: vec![NoteStep {
                    step: "Replace the hero on production".into(),
                    by: "coderhelm".into(),
                    kind: "content".into(),
                    target: "production".into(),
                    result: "not done: no production CMS write connection exists".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        let f = outside_findings(Some(&d));
        assert_eq!(f.len(), 1);
        assert!(f[0]
            .detail
            .starts_with("Not done by this run: 1. Replace the hero on production"));
        assert!(f[0].detail.contains("no production CMS write connection"));
    }

    #[test]
    fn a_deferred_task_is_settled_and_listed_with_why() {
        let md = "- [ ] `src/a.rs`: fix the parser\n- [ ] `README.md`: note the new flag";
        let out = with_deferred(
            md,
            &[(
                "README.md".to_string(),
                "deferred: this non-core task resisted 3 different approaches".to_string(),
            )],
        );
        let tasks = plan_tasks(&out);
        assert!(!tasks[0].optional);
        assert!(tasks[1].optional, "{out}");
        assert!(tasks[1].task.contains("resisted 3 different approaches"));
        // Deferring twice doesn't stack the reason.
        assert_eq!(
            with_deferred(&out, &[("README.md".into(), "deferred: again".into())]),
            out
        );
    }
}
