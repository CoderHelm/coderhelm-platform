//! The planner's answer, read file by file.
//!
//! A run once lost a good plan to one cut answer. The plan check sent the
//! plan back with two open items; the planner's revision reached the output
//! limit partway through its last block, wrote only that block again on the
//! next turn ("the four files above are unchanged"), and the worker read
//! only that last turn: no tasks, no design, and the run failed telling the
//! person to retry it.
//!
//! Now the answer is read per block:
//!
//! - [`read`]: every known block in the text (the four openspec files and
//!   the decision), each the last one written in full; a block whose fence
//!   never closed, or that a new block of a known name interrupted, is cut
//!   and never read as complete.
//! - [`merge`]: a revision replaces only the files it wrote in full; a file
//!   it left out, cut or wrote empty keeps the last valid one. A revision
//!   never empties a valid plan.
//! - [`missing`]: what a usable plan still lacks, file by file, for a repair
//!   that asks for just those files.
//! - [`unusable`]: why a plan can't be built from, in a sentence, or `None`.

use super::plan::PlanResult;

/// The openspec files, in the order a plan writes them.
pub const FILES: [&str; 4] = ["proposal.md", "design.md", "tasks.md", "spec.md"];

/// The decision block's fence name.
pub const DECISION: &str = "fix_location.json";

const NAMES: [&str; 5] = [FILES[0], FILES[1], FILES[2], FILES[3], DECISION];

/// Repairs of missing files asked of the planner before the plan is
/// judged unusable (the second at a higher effort).
pub const MAX_REPAIRS: u32 = 2;

/// How a block stands in an answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Missing,
    /// Its fence opened and never closed (the output limit, usually).
    Cut,
    Complete,
}

/// The known blocks of one answer: for each name, the last block written in
/// full, else the last cut one.
#[derive(Clone, Debug, Default)]
pub struct Answer {
    blocks: Vec<(&'static str, State, String)>,
}

impl Answer {
    pub fn state(&self, name: &str) -> State {
        self.blocks
            .iter()
            .find(|(n, _, _)| *n == name)
            .map_or(State::Missing, |(_, s, _)| *s)
    }

    /// The block's text when it was written in full, else empty.
    pub fn complete(&self, name: &str) -> &str {
        self.blocks
            .iter()
            .find(|(n, s, _)| *n == name && *s == State::Complete)
            .map_or("", |(_, _, b)| b.trim())
    }

    /// The decision block as the decision's own parser reads it: closed
    /// when it was written in full, open when it was cut, absent when
    /// missing.
    pub fn decision_text(&self) -> String {
        match self.blocks.iter().find(|(n, _, _)| *n == DECISION) {
            Some((_, State::Complete, b)) => format!("```{DECISION}\n{}\n```", b.trim()),
            Some((_, _, b)) => format!("```{DECISION}\n{}", b.trim()),
            None => String::new(),
        }
    }

    fn set(&mut self, name: &'static str, state: State, body: String) {
        // A complete block is never replaced by a later cut one.
        match self.blocks.iter_mut().find(|(n, _, _)| *n == name) {
            Some(slot) if state == State::Complete || slot.1 != State::Complete => {
                *slot = (name, state, body)
            }
            Some(_) => {}
            None => self.blocks.push((name, state, body)),
        }
    }
}

/// The known block a fence line opens, if any: "```tasks.md".
fn opens(line: &str) -> Option<&'static str> {
    let rest = line.trim().strip_prefix("```")?.trim();
    NAMES.iter().copied().find(|n| *n == rest)
}

/// Reads every known block in an answer. Inside a block, a fence with a
/// language (```ts) opens a nested code block and a bare ``` closes it; a
/// bare ``` with nothing nested closes the block.
pub fn read(text: &str) -> Answer {
    let mut out = Answer::default();
    let mut open: Option<(&'static str, Vec<&str>, usize)> = None;
    for line in text.lines() {
        if let Some(name) = opens(line) {
            if let Some((prev, body, _)) = open.take() {
                out.set(prev, State::Cut, body.join("\n"));
            }
            open = Some((name, Vec::new(), 0));
            continue;
        }
        let Some((name, body, depth)) = open.as_mut() else {
            continue;
        };
        let t = line.trim();
        if t == "```" {
            if *depth == 0 {
                let (name, body) = (*name, body.join("\n"));
                open = None;
                out.set(name, State::Complete, body);
                continue;
            }
            *depth -= 1;
        } else if t.starts_with("```") {
            *depth += 1;
        } else if *depth == 0 && t.ends_with("```") {
            // The closing fence written on the last line of the block.
            let (name, mut body) = (*name, body.clone());
            body.push(line.trim_end().trim_end_matches('`'));
            open = None;
            out.set(name, State::Complete, body.join("\n"));
            continue;
        }
        body.push(line);
    }
    if let Some((name, body, _)) = open {
        out.set(name, State::Cut, body.join("\n"));
    }
    out
}

/// Tasks that are an answer in themselves: nothing to change, a question,
/// or a fix outside the code.
pub fn directive(tasks: &str) -> bool {
    let t = tasks.trim_start();
    t.starts_with("NO_CHANGES_NEEDED")
        || t.starts_with("CLARIFICATION_NEEDED")
        || t.starts_with(super::fix_location::NO_CODE_CHANGE)
}

/// Whether one file of a plan is usable as written: not empty, and a task
/// list holds at least one task (or is a directive).
pub fn valid(name: &str, body: &str) -> bool {
    let b = body.trim();
    if b.is_empty() {
        return false;
    }
    if name == "tasks.md" {
        return directive(b) || b.lines().any(|l| l.trim_start().starts_with("- ["));
    }
    true
}

fn file(p: &PlanResult, name: &str) -> String {
    match name {
        "proposal.md" => p.proposal.clone(),
        "design.md" => p.design.clone(),
        "tasks.md" => p.tasks.clone(),
        _ => p.spec.clone(),
    }
}

fn set_file(p: &mut PlanResult, name: &str, body: String) {
    match name {
        "proposal.md" => p.proposal = body,
        "design.md" => p.design = body,
        "tasks.md" => p.tasks = body,
        _ => p.spec = body,
    }
}

/// The files a plan of work must have before it is built: the proposal,
/// the design and the tasks (the spec is asked for, never required). A
/// directive needs only its tasks.
pub fn missing(p: &PlanResult) -> Vec<&'static str> {
    if directive(&p.tasks) {
        return Vec::new();
    }
    ["proposal.md", "design.md", "tasks.md"]
        .into_iter()
        .filter(|n| !valid(n, &file(p, n)))
        .collect()
}

/// The files a repair asks for: the required ones [`missing`] names, and
/// the spec when it is empty too.
pub fn to_repair(p: &PlanResult) -> Vec<&'static str> {
    let mut out = missing(p);
    if !out.is_empty() && p.spec.trim().is_empty() {
        out.push("spec.md");
    }
    out
}

/// Why a plan can't be built from, in a sentence; `None` when it can.
pub fn unusable(p: &PlanResult) -> Option<String> {
    let m = missing(p);
    if m.is_empty() {
        return None;
    }
    Some(format!(
        "the plan has no usable {}",
        m.iter()
            .map(|n| format!("`{n}`"))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

/// A revision over the plan before it. Each file is the revision's when
/// it wrote that file in full and it is valid, else the earlier plan's:
/// a file the revision left out, cut off or wrote empty stays as it was.
/// The decision is the revision's when it wrote one that reads. The names
/// of the files kept from before because the revision lost them (cut or
/// empty, not merely left out) come back with it.
pub fn merge(
    prev: PlanResult,
    revised: PlanResult,
    answer: &Answer,
) -> (PlanResult, Vec<&'static str>) {
    let mut out = revised;
    let mut lost = Vec::new();
    for name in FILES {
        let new = file(&out, name);
        if valid(name, &new) {
            continue;
        }
        let old = file(&prev, name);
        // A directive's tasks stand alone: the earlier tasks never come back
        // under it, and its empty files stay empty.
        if directive(&out.tasks) && name != "tasks.md" {
            continue;
        }
        if valid(name, &old) || (name == "spec.md" && !old.trim().is_empty()) {
            if answer.state(name) != State::Missing || !new.trim().is_empty() {
                lost.push(name);
            }
            set_file(&mut out, name, old);
        }
    }
    out.repo_tasks = super::plan::repo_tasks_of(&out.tasks);
    if out.fix.is_none() {
        out.fix = prev.fix;
    }
    (out, lost)
}

/// What the planner is asked when files are missing from its plan: only
/// those files, each whole in its own fence.
pub fn repair_prompt(names: &[&str], answer: &Answer) -> String {
    let list = names
        .iter()
        .map(|n| {
            let why = match answer.state(n) {
                State::Cut => "was cut off before its fence closed",
                State::Complete => "has no usable content (a task list needs `- [ ]` tasks)",
                State::Missing => "is missing",
            };
            format!("- `{n}` {why}")
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "Your plan is not complete yet:\n{list}\n\nWrite only these files now, each whole in \
         its own fence (```name ... ```), in the same format as before, and nothing else: no \
         other file, no decision block, no notes. Keep each one short; the files not listed are \
         kept as they are."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(tasks: &str) -> PlanResult {
        PlanResult {
            proposal: "P".into(),
            design: "D".into(),
            tasks: tasks.into(),
            spec: "S".into(),
            repo_tasks: vec![],
            fix: None,
        }
    }

    const FULL: &str = "```proposal.md\nP1\n```\n```design.md\nD1\n```\n```tasks.md\n- [ ] a.ts: [modify] A\n```\n```spec.md\nS1\n```\n```fix_location.json\n{\"fix_location\":\"code\"}\n```";

    #[test]
    fn every_block_is_read_whole() {
        let a = read(FULL);
        for n in NAMES {
            assert_eq!(a.state(n), State::Complete, "{n}");
        }
        assert_eq!(a.complete("tasks.md"), "- [ ] a.ts: [modify] A");
        assert!(a.decision_text().ends_with("\n```"));
    }

    #[test]
    fn a_nested_code_block_stays_inside_its_file() {
        let a = read("```design.md\nUse this:\n```ts\nconst a = 1;\n```\nThen that.\n```\n");
        assert_eq!(a.state("design.md"), State::Complete);
        assert_eq!(
            a.complete("design.md"),
            "Use this:\n```ts\nconst a = 1;\n```\nThen that."
        );
    }

    #[test]
    fn a_cut_block_is_never_complete() {
        // Cut at the end of the text.
        let a = read("```proposal.md\nP\n```\n```tasks.md\n- [ ] a.ts: [mod");
        assert_eq!(a.state("proposal.md"), State::Complete);
        assert_eq!(a.state("tasks.md"), State::Cut);
        assert_eq!(a.complete("tasks.md"), "");
        // Cut by a block of a known name written again from its start: the
        // later, whole one counts.
        let a = read(
            "```fix_location.json\n{\"cause\": \"New woMy last answer was cut.\n```fix_location.json\n{\"fix_location\":\"both\"}\n```",
        );
        assert_eq!(a.state(DECISION), State::Complete);
        assert_eq!(a.complete(DECISION), "{\"fix_location\":\"both\"}");
        // A whole block is never replaced by a later cut one.
        let a = read("```spec.md\nS\n```\n```spec.md\nS2 cut");
        assert_eq!(a.complete("spec.md"), "S");
        assert_eq!(read("no fences").state("tasks.md"), State::Missing);
        // A fence closed on the block's last line still closes it.
        let a = read("```tasks.md\n- [ ] a.ts: [modify] A```\n");
        assert_eq!(a.complete("tasks.md"), "- [ ] a.ts: [modify] A");
    }

    #[test]
    fn a_task_list_needs_tasks() {
        assert!(valid("tasks.md", "- [ ] a.ts: [modify] A"));
        assert!(valid("tasks.md", "NO_CHANGES_NEEDED: already there"));
        assert!(valid(
            "tasks.md",
            "CLARIFICATION_NEEDED:\n- question: which?"
        ));
        assert!(!valid("tasks.md", "I will now write the tasks."));
        assert!(!valid("design.md", "  "));
        assert!(valid("design.md", "D"));
    }

    #[test]
    fn missing_names_each_required_file() {
        assert!(missing(&plan("- [ ] a.ts: [modify] A")).is_empty());
        let mut p = plan("");
        p.design.clear();
        p.spec.clear();
        assert_eq!(missing(&p), vec!["design.md", "tasks.md"]);
        assert_eq!(to_repair(&p), vec!["design.md", "tasks.md", "spec.md"]);
        assert_eq!(
            unusable(&p).unwrap(),
            "the plan has no usable `design.md`, `tasks.md`"
        );
        // A directive needs nothing else.
        let mut d = plan("NO_CHANGES_NEEDED: done");
        d.design.clear();
        assert!(unusable(&d).is_none());
    }

    #[test]
    fn a_revision_never_empties_a_valid_plan() {
        let prev = plan("- [ ] a.ts: [modify] A");
        // The revision wrote only its decision, after a cut.
        let text = "```fix_location.json\n{\"fix_location\":\"code\"}\n```";
        let a = read(text);
        let revised = PlanResult {
            proposal: String::new(),
            design: String::new(),
            tasks: String::new(),
            spec: String::new(),
            repo_tasks: vec![],
            fix: None,
        };
        let (out, lost) = merge(prev, revised, &a);
        assert_eq!(out.tasks, "- [ ] a.ts: [modify] A");
        assert_eq!(out.design, "D");
        assert!(lost.is_empty(), "left out is not lost: {lost:?}");

        // A revision that changes only the tasks keeps the rest.
        let prev = plan("- [ ] a.ts: [modify] A");
        let mut revised = plan("- [ ] a.ts: [modify] A\n- [ ] b.ts: [create] B");
        revised.design.clear();
        revised.proposal.clear();
        revised.spec.clear();
        let a = read("```tasks.md\n- [ ] a.ts: [modify] A\n- [ ] b.ts: [create] B\n```");
        let (out, _) = merge(prev, revised, &a);
        assert!(out.tasks.contains("b.ts"));
        assert_eq!(out.design, "D");

        // A cut file is lost and named; the earlier one stands.
        let prev = plan("- [ ] a.ts: [modify] A");
        let mut revised = plan("");
        revised.design.clear();
        let a = read("```design.md\nD2 cut");
        let (out, lost) = merge(prev, revised, &a);
        assert_eq!(out.design, "D");
        assert_eq!(lost, vec!["design.md"]);
    }

    #[test]
    fn a_directive_revision_stands_alone() {
        let prev = plan("- [ ] a.ts: [modify] A");
        let mut revised = plan("NO_CHANGES_NEEDED: already in place");
        revised.design.clear();
        let a = read("```tasks.md\nNO_CHANGES_NEEDED: already in place\n```");
        let (out, _) = merge(prev, revised, &a);
        assert!(out.tasks.starts_with("NO_CHANGES_NEEDED"));
        assert_eq!(out.design, "");
    }

    #[test]
    fn a_repair_asks_for_only_the_missing_files() {
        let a = read("```tasks.md\n- [ ] a.ts: [mod");
        let p = repair_prompt(&["design.md", "tasks.md"], &a);
        assert!(p.contains("`design.md` is missing"));
        assert!(p.contains("`tasks.md` was cut off"));
        assert!(p.contains("Write only these files"));
        assert!(!p.contains("edit_file"));
    }
}
