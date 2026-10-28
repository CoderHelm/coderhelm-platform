//! Rule 1: what each plan task does to its file, stated by the plan.
//!
//! A feature adds files. The repo check used to read "the plan names files
//! that don't exist" as a made-up plan and planned again, so a new-feature
//! run re-planned because its new files didn't exist yet. Every task now says
//! what it does to its path, after the colon:
//!
//! ```text
//! - [ ] src/blocks/Quote.tsx: [create] The quote block component
//! - [ ] src/blocks/index.ts: [modify] Register the quote block
//! - [ ] src/old/Legacy.tsx: [delete] Remove the unused block
//! - [ ] public/robots.txt: [replace] Write the file again from scratch
//! ```
//!
//! Only `modify`, `delete` and `replace` paths must exist; a `create` path
//! must not (a plan that overwrites an existing file says `replace`). A task
//! without a tag keeps the old reading of its words, and the check holds
//! untagged paths to the old threshold.

use std::collections::HashMap;

/// What a task does to its file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileAction {
    Create,
    Modify,
    Delete,
    /// Overwrites an existing file from scratch.
    Replace,
}

impl FileAction {
    pub fn as_str(self) -> &'static str {
        match self {
            FileAction::Create => "create",
            FileAction::Modify => "modify",
            FileAction::Delete => "delete",
            FileAction::Replace => "replace",
        }
    }

    /// Whether the path must exist before the change.
    pub fn needs_existing(self) -> bool {
        !matches!(self, FileAction::Create)
    }

    fn parse(tag: &str) -> Option<FileAction> {
        match tag.trim().to_ascii_lowercase().as_str() {
            "create" | "new" | "add" => Some(FileAction::Create),
            "modify" | "edit" | "update" | "change" => Some(FileAction::Modify),
            "delete" | "remove" => Some(FileAction::Delete),
            "replace" | "overwrite" | "rewrite" => Some(FileAction::Replace),
            _ => None,
        }
    }
}

/// One file the plan changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedFile {
    pub path: String,
    /// The task's tag. `None`: the task has none.
    pub action: Option<FileAction>,
    /// The untagged task's words read as creating the file (the old reading).
    pub reads_new: bool,
}

impl PlannedFile {
    /// The file is new: tagged `create`, or untagged and worded as new.
    pub fn is_new(&self) -> bool {
        match self.action {
            Some(a) => a == FileAction::Create,
            None => self.reads_new,
        }
    }
}

/// The `[tag]` a task description starts with, and the rest.
fn split_tag(desc: &str) -> (Option<FileAction>, &str) {
    let d = desc.trim_start();
    if let Some(rest) = d.strip_prefix('[') {
        if let Some((tag, after)) = rest.split_once(']') {
            if let Some(a) = FileAction::parse(tag) {
                return (Some(a), after.trim_start());
            }
        }
    }
    (None, d)
}

fn reads_new(desc: &str) -> bool {
    let d = desc.to_lowercase();
    d.contains("create")
        || d.contains("new file")
        || d.contains("(new)")
        || d.trim_start().starts_with("new ")
        || d.trim_start().starts_with("add new")
}

/// The path a checklist line names and its description, for
/// `- [ ] path: description` (and `- [ ] [create] path: description`, a tag
/// written before the path).
fn task_line(line: &str) -> Option<(String, Option<FileAction>, String)> {
    let l = line.trim_start();
    let rest = l
        .strip_prefix("- [ ]")
        .or_else(|| l.strip_prefix("- [x]"))
        .or_else(|| l.strip_prefix("- [X]"))
        .or_else(|| l.strip_prefix("* [ ]"))?;
    let (pre_tag, rest) = split_tag(rest);
    let (head, desc) = rest.split_once(':')?;
    let path = head.trim().trim_matches(|c| c == '`' || c == '*').trim();
    let looks_like_path = !path.is_empty()
        && path.len() < 300
        && !path.chars().any(char::is_whitespace)
        && (path.contains('/') || path.contains('.'))
        && !path.contains("://");
    if !looks_like_path {
        return None;
    }
    let (tag, desc) = split_tag(desc);
    Some((
        path.trim_start_matches("./").to_string(),
        tag.or(pre_tag),
        desc.trim().to_string(),
    ))
}

/// Every file the checklist changes, deduped, in order. A path named by
/// several tasks takes the first tag given; `create` wins over an untagged
/// reading.
pub fn planned_files(tasks: &str) -> Vec<PlannedFile> {
    let mut out: Vec<PlannedFile> = Vec::new();
    for line in tasks.lines() {
        let Some((path, action, desc)) = task_line(line) else {
            continue;
        };
        let new = reads_new(&desc);
        match out.iter_mut().find(|f| f.path == path) {
            Some(f) => {
                f.action = f.action.or(action);
                f.reads_new |= new;
            }
            None => out.push(PlannedFile {
                path,
                action,
                reads_new: new,
            }),
        }
    }
    out
}

/// What is wrong with one planned path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Problem {
    /// Tagged modify, delete or replace, but the file isn't there.
    Missing { path: String, action: FileAction },
    /// Tagged create, but the file is already there.
    AlreadyExists { path: String },
}

impl Problem {
    pub fn line(&self) -> String {
        match self {
            Problem::Missing { path, action } => format!(
                "`{path}` is tagged [{}] but doesn't exist (tag it [create] if the task adds it)",
                action.as_str()
            ),
            Problem::AlreadyExists { path } => format!(
                "`{path}` is tagged [create] but already exists (tag it [modify], or [replace] to write it again)"
            ),
        }
    }
}

/// The problems with the tagged paths, given which paths exist (`exists`:
/// path to whether it is there; a path not in the map wasn't looked up and is
/// skipped). Untagged paths are never a problem here.
pub fn check(files: &[PlannedFile], exists: &HashMap<String, bool>) -> Vec<Problem> {
    let mut out = Vec::new();
    for f in files {
        let (Some(action), Some(&there)) = (f.action, exists.get(&f.path)) else {
            continue;
        };
        if action.needs_existing() && !there {
            out.push(Problem::Missing {
                path: f.path.clone(),
                action,
            });
        } else if action == FileAction::Create && there {
            out.push(Problem::AlreadyExists {
                path: f.path.clone(),
            });
        }
    }
    out
}

/// The paths to look up: every path that must exist (tagged or untagged and
/// not new), then the tagged `create` paths, each list capped at `cap`.
pub fn paths_to_look_up(files: &[PlannedFile], cap: usize) -> (Vec<String>, Vec<String>) {
    let existing: Vec<String> = files
        .iter()
        .filter(|f| !f.is_new())
        .map(|f| f.path.clone())
        .take(cap)
        .collect();
    let created: Vec<String> = files
        .iter()
        .filter(|f| f.action == Some(FileAction::Create))
        .map(|f| f.path.clone())
        .take(cap)
        .collect();
    (existing, created)
}

/// The note a re-plan gets.
pub fn replan_note(missing: &[String], problems: &[Problem], repo: &str) -> String {
    let mut out = String::from("\n\n---\n\n**Planning note (from CoderHelm):** ");
    if !missing.is_empty() {
        out.push_str(&format!(
            "the previous plan for this ticket named files that don't exist in {repo}: {}. ",
            missing.join(", ")
        ));
    }
    if !problems.is_empty() {
        out.push_str("Its task tags don't match the repository: ");
        out.push_str(
            &problems
                .iter()
                .map(Problem::line)
                .collect::<Vec<_>>()
                .join("; "),
        );
        out.push_str(". ");
    }
    out.push_str(
        "Look the paths up with your tools. Tag every task with what it does to its file \
         (`- [ ] path: [create] ...`, `[modify]`, `[delete]` or `[replace]`): new files are \
         expected for a feature and are tagged [create]; only [modify], [delete] and [replace] \
         paths must exist. If the change doesn't belong in this repository, say so instead of \
         planning code.",
    );
    out
}

/// What the planner is told about the checklist's tags.
pub const PLANNER_FORMAT: &str = "`- [ ] <file_path>: [<action>] <description>`, where <action> \
     is `create` (a new file; the path must not exist yet), `modify`, `delete` or `replace` \
     (overwrite an existing file); e.g. `- [ ] src/resolvers/join.ts: [modify] Add optional \
     billingAddress param to OnlineJoin mutation`, `- [ ] src/blocks/Quote.tsx: [create] The \
     quote block component`. New files are expected for a feature: tag them [create].";

#[cfg(test)]
mod tests {
    use super::*;

    const TASKS: &str = "\
- [ ] src/blocks/Quote.tsx: [create] The quote block component
- [ ] src/blocks/index.ts: [modify] Register the quote block
- [ ] `src/old/Legacy.tsx`: [delete] Remove the old block
- [ ] public/robots.txt: [replace] Write it again
- [ ] src/lib/format.ts: Add a date helper
- [ ] src/blocks/Card.tsx: Create the card block
- [ ] [create] src/blocks/Card.test.tsx: Tests for the card block
- [ ] Not a path: something
";

    #[test]
    fn reads_each_tasks_action_from_its_tag() {
        let f = planned_files(TASKS);
        let by = |p: &str| f.iter().find(|x| x.path == p).unwrap().clone();
        assert_eq!(by("src/blocks/Quote.tsx").action, Some(FileAction::Create));
        assert_eq!(by("src/blocks/index.ts").action, Some(FileAction::Modify));
        assert_eq!(by("src/old/Legacy.tsx").action, Some(FileAction::Delete));
        assert_eq!(by("public/robots.txt").action, Some(FileAction::Replace));
        assert_eq!(by("src/lib/format.ts").action, None);
        assert!(!by("src/lib/format.ts").is_new());
        // Untagged, worded as new: the old reading still counts.
        assert!(by("src/blocks/Card.tsx").is_new());
        // A tag before the path is read too.
        assert_eq!(
            by("src/blocks/Card.test.tsx").action,
            Some(FileAction::Create)
        );
        assert_eq!(f.len(), 7);
    }

    #[test]
    fn new_files_of_a_feature_are_not_a_problem() {
        // The real case: a feature's plan named files that don't exist yet.
        let files = planned_files(
            "- [ ] src/blocks/Quote.tsx: [create] The quote block\n\
             - [ ] src/blocks/Quote.test.tsx: [create] Its tests\n\
             - [ ] src/blocks/index.ts: [modify] Register it\n",
        );
        let exists = HashMap::from([
            ("src/blocks/Quote.tsx".to_string(), false),
            ("src/blocks/Quote.test.tsx".to_string(), false),
            ("src/blocks/index.ts".to_string(), true),
        ]);
        assert!(check(&files, &exists).is_empty());
        let (existing, created) = paths_to_look_up(&files, 10);
        assert_eq!(existing, vec!["src/blocks/index.ts"]);
        assert_eq!(created.len(), 2);
    }

    #[test]
    fn a_modify_path_must_exist_and_a_create_path_must_not() {
        let files = planned_files(
            "- [ ] src/a.ts: [modify] Change it\n\
             - [ ] src/b.ts: [create] Add it\n\
             - [ ] src/c.ts: [replace] Rewrite it\n\
             - [ ] src/d.ts: Untagged and missing\n",
        );
        let exists = HashMap::from([
            ("src/a.ts".to_string(), false),
            ("src/b.ts".to_string(), true),
            ("src/c.ts".to_string(), true),
            ("src/d.ts".to_string(), false),
        ]);
        let p = check(&files, &exists);
        assert_eq!(
            p,
            vec![
                Problem::Missing {
                    path: "src/a.ts".into(),
                    action: FileAction::Modify
                },
                Problem::AlreadyExists {
                    path: "src/b.ts".into()
                },
            ]
        );
        let note = replan_note(&[], &p, "o/r");
        assert!(note.contains("[replace]"));
        assert!(note.contains("`src/b.ts` is tagged [create] but already exists"));
    }

    #[test]
    fn a_path_not_looked_up_is_skipped() {
        let files = planned_files("- [ ] src/a.ts: [modify] Change it\n");
        assert!(check(&files, &HashMap::new()).is_empty());
    }
}
