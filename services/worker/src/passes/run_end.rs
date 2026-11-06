//! How a run ends when an implement step changes no file.
//!
//! A run's work can span several steps: a step that runs out of its time
//! continues in a fresh one, and each step only counts the files it wrote
//! itself. A run once built fourteen files over three steps; its fourth step
//! found nothing left to write, and the run ended as "I ran but produced no
//! file changes" with two generic questions ("Which files or components
//! should change?", "What should happen instead?"), although the branch held
//! all the work and the ticket named every file.
//!
//! The rules here:
//!
//! - The run's work is the branch's diff against the base it started from
//!   ([`work_files`]), summed over every step: never the last step alone. A
//!   step that changed nothing on a branch that holds the work goes on to
//!   the plan check and the pull request ([`Empty::Carry`]).
//! - With nothing on the branch and a plan that names its files, the run
//!   builds once more, told every planned task ([`build_round_feedback`]).
//! - Still nothing: a question the coder itself wrote (naming what it
//!   couldn't decide) is asked ([`Empty::Ask`]); otherwise, when the ticket
//!   or the plan already names the files, the run stops naming the planned
//!   tasks that aren't built ([`Empty::Unbuilt`]). It never asks the generic
//!   questions then.
//! - The generic questions ([`Empty::Generic`]) are only for a ticket that
//!   names no file and a plan with no file tasks.
//! - "Wrote no file" means the coder made no write. Writes GitHub refused
//!   for a while (its rate limit, a 5xx), or a diff it couldn't show for
//!   that reason, are a wait ([`Empty::Wait`]): the run pauses and goes on
//!   by itself (`common::github_wait`). Writes GitHub rejected for a real
//!   reason are reported with that reason ([`Empty::Rejected`]). Neither is
//!   ever called CoderHelm's miss.

use super::plan_coverage::PlanTask;
use common::github_wait::Cause;

/// What became of the writes a pass tried, beyond the files it wrote.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Writes {
    /// Write calls GitHub refused for a while: how many, why, and until
    /// when (epoch seconds) the last one said to wait.
    pub refused: usize,
    pub refused_for: Option<(Cause, u64)>,
    /// Write calls GitHub rejected for a real reason, as `path: reason`.
    pub rejected: Vec<String>,
}

impl Writes {
    /// One write call GitHub refused for a while.
    pub fn refuse(&mut self, cause: Cause, until: u64) {
        self.refused += 1;
        let later = self.refused_for.map_or(until, |(_, u)| u.max(until));
        self.refused_for = Some((cause, later));
    }

    /// One write call GitHub rejected: `what` names the file(s), `why` is
    /// GitHub's answer.
    pub fn reject(&mut self, what: &str, why: &str) {
        if self.rejected.len() < 5 {
            self.rejected
                .push(format!("{what}: {}", common::truncate_str(why.trim(), 300)));
        }
    }

    pub fn merge(&mut self, other: Writes) {
        self.refused += other.refused;
        if let Some((c, u)) = other.refused_for {
            let later = self.refused_for.map_or(u, |(_, x)| x.max(u));
            self.refused_for = Some((c, later));
        }
        for r in other.rejected {
            if self.rejected.len() < 5 {
                self.rejected.push(r);
            }
        }
    }
}

/// Why GitHub couldn't be read or written just now, when it was for a
/// while: the cause and until when (epoch seconds).
pub type Outside = Option<(Cause, u64)>;

/// What a step that changed no file leads to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Empty {
    /// The branch holds the run's work (these files): judge that.
    Carry(Vec<String>),
    /// Ask the coder's own questions (each names what it couldn't decide).
    Ask(Vec<String>),
    /// Nothing built although the ticket or plan names the files: stop
    /// with this reason, no question.
    Unbuilt(String),
    /// The ticket names no file and the plan has no file tasks: ask what
    /// should change.
    Generic,
    /// GitHub refused the writes (or the branch's diff) for a while: the run
    /// waits until then (epoch seconds) and goes on by itself.
    Wait(Cause, u64),
    /// GitHub rejected the writes for a real reason: stop, saying it.
    Rejected(String),
}

/// The files the branch changes that are the run's work: everything but the
/// plan files CoderHelm commits beside it (`openspec/specs/<ticket>/`).
pub fn work_files(changed: &[String], ticket_id: &str) -> Vec<String> {
    let spec_dir = format!("openspec/specs/{}/", ticket_id.to_lowercase());
    changed
        .iter()
        .filter(|p| {
            let p = p.trim_start_matches("./");
            ticket_id.is_empty() || !p.to_lowercase().starts_with(&spec_dir)
        })
        .cloned()
        .collect()
}

/// The questions the coder ended its account with: lines ending in `?`
/// that say what it couldn't decide. At most three.
pub fn coder_questions(notes: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for line in notes.lines() {
        let q = line
            .trim()
            .trim_start_matches(['-', '*', '>', ' '])
            .trim_start_matches(|c: char| c.is_ascii_digit() || c == '.' || c == ')')
            .trim()
            .trim_start_matches("Question:")
            .trim_start_matches("QUESTION:")
            .trim();
        // A question worth asking names something: a few words at least.
        if q.ends_with('?') && q.split_whitespace().count() >= 5 && q.len() <= 400 {
            let q = q.to_string();
            if !out.contains(&q) {
                out.push(q);
            }
        }
        if out.len() == 3 {
            break;
        }
    }
    out
}

/// Whether the ticket's text names a file (`dir/name.ext`).
pub fn ticket_names_files(text: &str) -> bool {
    text.split(|c: char| c.is_whitespace() || "`'\"()[]<>,;".contains(c))
        .map(|t| t.trim_end_matches(['.', ':']))
        .any(|t| {
            let Some((dir, file)) = t.rsplit_once('/') else {
                return false;
            };
            !dir.is_empty()
                && !t.contains("://")
                && file.rsplit_once('.').is_some_and(|(stem, ext)| {
                    !stem.is_empty()
                        && (1..=5).contains(&ext.len())
                        && ext.chars().all(|c| c.is_ascii_alphanumeric())
                })
        })
}

/// What a step that changed no file leads to. `work`: the branch's work
/// files (`None` when GitHub couldn't say: then nothing is claimed either
/// way and the plan decides); `tasks`: the plan's file tasks;
/// `questions`: the coder's own; `ticket_names_files`: the ticket's text
/// names a file; `writes`: the writes GitHub refused or rejected;
/// `outside`: GitHub is refusing requests for a while right now.
pub fn judge(
    work: Option<&[String]>,
    tasks: &[PlanTask],
    questions: &[String],
    ticket_names_files: bool,
    writes: &Writes,
    outside: Outside,
) -> Empty {
    if let Some(w) = work.filter(|w| !w.is_empty()) {
        return Empty::Carry(w.to_vec());
    }
    // Writes GitHub refused for a while, or a branch GitHub couldn't show
    // while it refuses: wait for it, never "wrote no file".
    if let Some((cause, until)) = writes.refused_for.or(outside.filter(|_| work.is_none())) {
        return Empty::Wait(cause, until);
    }
    if !writes.rejected.is_empty() {
        return Empty::Rejected(rejected_reason(&writes.rejected));
    }
    if !questions.is_empty() {
        return Empty::Ask(questions.to_vec());
    }
    let required: Vec<&PlanTask> = tasks
        .iter()
        .filter(|t| !t.optional && !t.generated)
        .collect();
    if !required.is_empty() || ticket_names_files {
        return Empty::Unbuilt(unbuilt_reason(&required, work.is_none()));
    }
    Empty::Generic
}

fn rejected_reason(rejected: &[String]) -> String {
    format!(
        "Stopped without a pull request: GitHub rejected the coder's writes to the branch: {}. \
         The change was written but couldn't be saved; once that is fixed, Retry saves it.",
        rejected.join("; ")
    )
}

fn unbuilt_reason(required: &[&PlanTask], unread: bool) -> String {
    let mut list = String::new();
    for (i, t) in required.iter().enumerate() {
        let p = format!("`{}`", t.path);
        if list.len() + p.len() > 260 {
            list.push_str(&format!(" and {} more", required.len() - i));
            break;
        }
        if i > 0 {
            list.push_str(", ");
        }
        list.push_str(&p);
    }
    let what = if required.is_empty() {
        "the files the ticket names".to_string()
    } else {
        format!("{} planned task(s): {list}", required.len())
    };
    if unread {
        // Whether the work is there is unknown: never blame anyone for it.
        return format!(
            "Stopped without a pull request: the coder made no write in this step and GitHub \
             couldn't show the branch's diff, so whether the earlier work is there is unknown \
             ({what}). Retry checks the branch again and builds what is missing."
        );
    }
    format!(
        "Stopped without a pull request: the coder made no write to the branch, although the \
         ticket and plan say what to build ({what}). That is CoderHelm's miss, not missing \
         detail: Retry builds it from the same plan."
    )
}

/// What the extra build round is told when a step wrote nothing and the
/// branch holds nothing: every planned task, and to ask only what it
/// truly can't decide, after building the rest.
pub fn build_round_feedback(tasks: &[PlanTask]) -> String {
    let todo: Vec<String> = tasks
        .iter()
        .filter(|t| !t.optional && !t.generated)
        .map(|t| format!("- [ ] `{}`: {}", t.path, t.task))
        .collect();
    format!(
        "Your last pass wrote no file and the branch has none of this change yet. The ticket and \
         plan say what to build: do every task below now, writing each file as soon as it is \
         done.\n\n{}\n\nIf one detail truly can't be decided from the ticket, the code or a \
         lookup, build everything else first, then end your summary with one question per open \
         detail, naming the file, field or option it decides (for example: \"Should the `size` \
         option of `src/blocks/card.ts` default to `small` or `large`?\").",
        todo.join("\n")
    )
}

/// The coder's questions as the run's questions, each saying the rest was
/// built or tried first.
pub fn questions_from(qs: &[String]) -> Vec<common::run_questions::Question> {
    qs.iter()
        .enumerate()
        .map(|(i, q)| common::run_questions::Question {
            id: format!("q{}", i + 1),
            question: q.clone(),
            why: "The coder couldn't decide this from the ticket, the code or a lookup, and the \
                  change depends on it."
                .to_string(),
            suggestions: Vec::new(),
            decision: String::new(),
            evidence: Vec::new(),
            ..Default::default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::plan_coverage::plan_tasks;

    const PLAN: &str = "- [ ] `web/src/blocks/hero.tsx`: Create the hero block\n\
        - [ ] `web/src/blocks/hero.test.tsx`: Add tests for each tab option\n\
        - [ ] `e2e/test-map.json`: Register the new spec\n\
        - [ ] `studio/schema.json`: Regenerate with schema extract\n";

    fn none() -> Writes {
        Writes::default()
    }

    fn files(p: &[&str]) -> Vec<String> {
        p.iter().map(|s| s.to_string()).collect()
    }

    /// The observed run: the last step wrote nothing, the branch held the
    /// work of the earlier steps.
    #[test]
    fn a_step_that_wrote_nothing_on_a_branch_with_the_work_carries_on() {
        let branch = files(&[
            "openspec/specs/abc-12/tasks.md",
            "web/src/blocks/hero.tsx",
            "web/src/blocks/hero.test.tsx",
            "e2e/test-map.json",
        ]);
        let work = work_files(&branch, "ABC-12");
        assert_eq!(work.len(), 3, "the plan files are not the work");
        let out = judge(Some(&work), &plan_tasks(PLAN), &[], true, &none(), None);
        assert_eq!(out, Empty::Carry(work));
    }

    #[test]
    fn only_the_plan_files_on_the_branch_is_no_work() {
        let work = work_files(&files(&["openspec/specs/abc-12/proposal.md"]), "ABC-12");
        assert!(work.is_empty());
        // The ticket and plan name the files: never the generic questions.
        match judge(Some(&work), &plan_tasks(PLAN), &[], true, &none(), None) {
            Empty::Unbuilt(r) => {
                assert!(r.contains("3 planned task(s)"), "{r}");
                assert!(r.contains("`web/src/blocks/hero.tsx`"));
                assert!(
                    !r.contains("schema.json"),
                    "a generated file isn't the coder's"
                );
                assert!(r.contains("Retry"));
                assert!(r.len() <= 500, "fits the run record's reason");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_coders_own_question_is_asked_not_the_generic_ones() {
        let notes = "Built the block.\n\nOpen question:\n- Should the `layout` field of \
            `web/src/blocks/hero.tsx` default to `grid` or `carousel`?\n- ok?\n";
        let qs = coder_questions(notes);
        assert_eq!(
            qs,
            vec![
                "Should the `layout` field of `web/src/blocks/hero.tsx` default to `grid` or `carousel`?"
                    .to_string()
            ],
            "a bare 'ok?' is not a question worth asking"
        );
        assert_eq!(
            judge(Some(&[]), &plan_tasks(PLAN), &qs, true, &none(), None),
            Empty::Ask(qs.clone())
        );
        let asked = questions_from(&qs);
        assert_eq!(asked[0].id, "q1");
        assert!(asked[0].question.contains("`layout`"));
    }

    #[test]
    fn generic_questions_only_when_nothing_names_a_file() {
        assert_eq!(
            judge(Some(&[]), &[], &[], false, &none(), None),
            Empty::Generic
        );
        assert!(matches!(
            judge(Some(&[]), &[], &[], true, &none(), None),
            Empty::Unbuilt(_)
        ));
        // An unreadable diff never claims "no changes" for a planned ticket.
        assert!(matches!(
            judge(None, &plan_tasks(PLAN), &[], false, &none(), None),
            Empty::Unbuilt(r) if r.contains("couldn't show") && !r.contains("CoderHelm's miss")
        ));
    }

    /// The incident: every write was refused with GitHub's rate limit and
    /// the diff couldn't be read either. That is a wait, never "the coder
    /// wrote no file", never CoderHelm's miss.
    #[test]
    fn writes_github_refused_for_a_while_are_a_wait_not_no_file() {
        let mut writes = Writes::default();
        for _ in 0..29 {
            writes.refuse(Cause::RateLimit, 1_793_999_100);
        }
        assert_eq!(writes.refused, 29);
        let out = judge(None, &plan_tasks(PLAN), &[], true, &writes, None);
        assert_eq!(out, Empty::Wait(Cause::RateLimit, 1_793_999_100));
        // No write was refused, but GitHub is refusing while the diff is
        // unread: also a wait.
        let out = judge(
            None,
            &plan_tasks(PLAN),
            &[],
            true,
            &none(),
            Some((Cause::Outage, 7)),
        );
        assert_eq!(out, Empty::Wait(Cause::Outage, 7));
        // With the work readable on the branch, the run carries on.
        let work = files(&["web/src/blocks/hero.tsx"]);
        assert_eq!(
            judge(Some(&work), &plan_tasks(PLAN), &[], true, &writes, None),
            Empty::Carry(work)
        );
    }

    #[test]
    fn writes_github_rejected_for_a_real_reason_say_that_reason() {
        let mut writes = Writes::default();
        writes.reject(
            "web/src/blocks/hero.tsx",
            "HTTP status client error (403 Forbidden) for url (...): Resource not accessible by integration",
        );
        match judge(Some(&[]), &plan_tasks(PLAN), &[], true, &writes, None) {
            Empty::Rejected(r) => {
                assert!(r.contains("GitHub rejected the coder's writes"), "{r}");
                assert!(r.contains("Resource not accessible by integration"));
                assert!(!r.contains("CoderHelm's miss"));
                assert!(!r.contains("wrote no file"));
            }
            other => panic!("{other:?}"),
        }
        // Only a step with no write at all is "no write".
        assert!(matches!(
            judge(Some(&[]), &plan_tasks(PLAN), &[], true, &none(), None),
            Empty::Unbuilt(r) if r.contains("made no write")
        ));
    }

    #[test]
    fn a_ticket_that_names_a_file_is_read_as_such() {
        assert!(ticket_names_files(
            "Add the block in `web/src/blocks/hero.tsx` and register it."
        ));
        assert!(ticket_names_files("Update e2e/test-map.json."));
        assert!(!ticket_names_files(
            "See https://example.com/docs/page.html"
        ));
        assert!(!ticket_names_files(
            "The hero should show three tabs and/or a title."
        ));
    }

    #[test]
    fn the_build_round_names_every_task_and_how_to_ask() {
        let fb = build_round_feedback(&plan_tasks(PLAN));
        assert!(fb.contains("`web/src/blocks/hero.tsx`"));
        assert!(fb.contains("`e2e/test-map.json`"));
        assert!(!fb.contains("schema.json"));
        assert!(fb.contains("build everything else first"));
        assert!(fb.contains("naming the file, field or option"));
    }
}
