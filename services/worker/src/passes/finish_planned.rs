//! Planned work left after the pull request opens is finished by CoderHelm.
//!
//! When the check before the pull request still finds planned work not done
//! after its fix round, the pull request opens as a draft with that work
//! listed at the top under "Not finished" (`proof::unfinished_section`). It
//! used to stay there for a person to pick up.
//!
//! Now the run keeps working it, in planned-work rounds on the pull request
//! (`run_round::Kind::PlannedWork`), each in a fresh invocation with its own
//! budget, the same as every other round:
//!
//! - A round is told the tasks still listed ([`round_feedback`]), builds
//!   them on its own WIP branch and lands them on the pull request as one
//!   commit.
//! - The check judges the branch again. The "Not finished" section is
//!   rewritten to what is still not done, or removed when nothing is.
//! - Nothing left: the pull request is marked ready ([`Next::Ready`]).
//! - Progress (a task finished, or the round pushed changes): another round
//!   ([`Next::Again`]). The runaway ceiling is the same as a run's steps.
//! - Two rounds in a row with no progress stop the work ([`Next::Stop`]):
//!   the run says which tasks are still not done and why, its status is
//!   `needs_input` (never `completed`), and the pull request stays a draft.
//!
//! The progress rule is the one ticket runs use between steps
//! (`steps::stalled_after`, `steps::MAX_STALLED_STEPS`).

use common::run_round::{Kind, Round};

use super::proof::{UNFINISHED_END, UNFINISHED_START};
use super::steps;

/// The planned tasks a description still lists under "Not finished": one
/// per item line, without the bullet. Empty when the section isn't there.
pub fn listed(body: &str) -> Vec<String> {
    let Some(start) = body.find(UNFINISHED_START) else {
        return Vec::new();
    };
    let rest = &body[start + UNFINISHED_START.len()..];
    let inner = rest.find(UNFINISHED_END).map_or(rest, |end| &rest[..end]);
    inner
        .lines()
        .map(str::trim)
        .filter_map(|l| l.strip_prefix("- ").or_else(|| l.strip_prefix("* ")))
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

/// What a planned-work round is told: every task still listed, built now,
/// in full.
pub fn round_feedback(items: &[String]) -> String {
    let todo: Vec<String> = items.iter().map(|i| format!("- [ ] {i}")).collect();
    format!(
        "This pull request is open as a draft because planned work is still not done. The \
         check before it listed the tasks below under \"Not finished\". Build every one of \
         them now on this branch, in full, following the plan and the code already on the \
         branch. Write each file as soon as it is done. Change the other files only where a \
         task needs it.\n\n{}",
        todo.join("\n")
    )
}

/// The first planned-work round, parked for a fresh invocation after the
/// pull request opens. `None` when nothing is listed.
pub fn first_round(section: &str, now: chrono::DateTime<chrono::Utc>) -> Option<Round> {
    let items = listed(section);
    if items.is_empty() {
        return None;
    }
    // Not scoped: planned work may need files the branch doesn't change yet.
    Some(
        Round::start(
            Kind::PlannedWork,
            1,
            "",
            &round_feedback(&items),
            false,
            now,
        )
        .parked(false),
    )
}

/// The round after `prev`, for the tasks still listed.
pub fn next_round(
    prev: &Round,
    items: &[String],
    stalled: u32,
    now: chrono::DateTime<chrono::Utc>,
) -> Round {
    let mut r = Round::start(
        Kind::PlannedWork,
        prev.number.saturating_add(1),
        "",
        &round_feedback(items),
        false,
        now,
    )
    .parked(false);
    r.stalled = stalled;
    r
}

/// What follows a planned-work round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next {
    /// Nothing is listed any more: the pull request may be marked ready.
    Ready,
    /// Another round, with this many stalled rounds before it.
    Again { stalled: u32 },
    /// The work stops: this reason names the tasks and why.
    Stop(String),
}

/// What follows a round. `before`: the tasks listed when it started;
/// `after`: the tasks the check lists now (`None` when the check couldn't
/// answer: nothing is claimed done then); `pushed_files`: the files the
/// round's commit changed.
pub fn next(
    before: &[String],
    after: Option<&[String]>,
    pushed_files: usize,
    round: &Round,
) -> Next {
    let after = after.unwrap_or(before);
    if after.is_empty() {
        return Next::Ready;
    }
    let progress = after.len() < before.len() || pushed_files > 0;
    if progress {
        // The same runaway ceiling as a ticket run's steps: never reached by
        // real work, but no loop runs forever.
        if round.number >= steps::MAX_RUN_STEPS {
            return Next::Stop(stop_reason(
                after,
                &format!(
                    "the work reached the safety ceiling of {} planned-work rounds",
                    steps::MAX_RUN_STEPS
                ),
            ));
        }
        return Next::Again { stalled: 0 };
    }
    let stalled = round.stalled.saturating_add(1);
    if steps::stops_for_stall(stalled) {
        let last = round.number.max(2);
        return Next::Stop(stop_reason(
            after,
            &format!(
                "planned-work rounds {} and {last} made no progress: no task was finished and \
                 nothing was pushed",
                last - 1
            ),
        ));
    }
    Next::Again { stalled }
}

/// The run's status after a round: waiting on CI while there are new
/// commits or another round is parked, completed once nothing is left and
/// nothing new was pushed, and `needs_input` when the work stopped (never
/// `completed`: the tasks are still not done).
pub fn run_status(next: &Next, pushed_files: usize) -> &'static str {
    use common::run_status::{AWAITING_CI, COMPLETED, NEEDS_INPUT};
    match next {
        Next::Ready if pushed_files > 0 => AWAITING_CI,
        Next::Ready => COMPLETED,
        Next::Again { .. } => AWAITING_CI,
        Next::Stop(_) => NEEDS_INPUT,
    }
}

/// Why the work stopped, with the exact tasks still not done.
pub fn stop_reason(items: &[String], why: &str) -> String {
    let list: Vec<String> = items.iter().map(|i| format!("- {i}")).collect();
    format!(
        "Stopped with planned work not done: {why}. Still not done:\n{}\nThe pull request \
         stays a draft with these listed under \"Not finished\".",
        list.join("\n")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::proof::{unfinished_section, Finding};

    fn planned(detail: &str) -> Finding {
        Finding {
            point: "planned_work".into(),
            detail: detail.into(),
            settle: String::new(),
            ..Default::default()
        }
    }

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-10-28T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    #[test]
    fn the_listed_tasks_come_from_the_section_only() {
        let section = unfinished_section(&[
            planned("`apps/web/src/blocks/card.tsx` is not written."),
            planned("The card block is not registered in `apps/studio/schema/index.ts`."),
        ]);
        let body = format!("{section}\n\n## Summary\n- not a task\n");
        let items = listed(&body);
        assert_eq!(items.len(), 2, "{items:?}");
        assert!(items[0].contains("card.tsx"));
        assert!(listed("## Summary\n- a line\n").is_empty());
    }

    #[test]
    fn the_first_round_is_parked_unscoped_with_every_task() {
        let section =
            unfinished_section(&[planned("`apps/web/src/blocks/card.tsx` is not written.")]);
        let r = first_round(&section, now()).unwrap();
        assert_eq!(r.kind, Kind::PlannedWork);
        assert_eq!(r.number, 1);
        assert_eq!(r.state, common::run_round::State::Pending);
        assert!(!r.scoped);
        assert!(r.feedback.contains("card.tsx"));
        assert!(first_round("", now()).is_none());
    }

    #[test]
    fn rounds_go_on_while_they_make_progress_and_stop_after_two_stalled() {
        let three: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let two = three[..2].to_vec();
        let mut r = first_round(
            &unfinished_section(&[planned("a"), planned("b"), planned("c")]),
            now(),
        )
        .unwrap();
        // A task finished: progress.
        assert_eq!(next(&three, Some(&two), 0, &r), Next::Again { stalled: 0 });
        // Pushed changes, nothing finished yet: progress (the head moved).
        assert_eq!(next(&two, Some(&two), 3, &r), Next::Again { stalled: 0 });
        // No change at all: one stalled round, then the second stops.
        assert_eq!(next(&two, Some(&two), 0, &r), Next::Again { stalled: 1 });
        r = next_round(&r, &two, 1, now());
        match next(&two, Some(&two), 0, &r) {
            Next::Stop(why) => {
                assert!(why.contains("rounds 1 and 2 made no progress"), "{why}");
                assert!(why.contains("- a\n- b"), "{why}");
            }
            other => panic!("{other:?}"),
        }
        // A check that couldn't answer claims nothing done.
        assert_eq!(
            next(&two, None, 0, &r).clone(),
            next(&two, Some(&two), 0, &r)
        );
        // Nothing left: ready.
        assert_eq!(next(&two, Some(&[]), 0, &r), Next::Ready);
    }

    #[test]
    fn a_progressing_round_continues_up_to_the_runaway_ceiling_only() {
        let items = vec!["a".to_string(), "b".to_string()];
        let mut r = first_round(&unfinished_section(&[planned("a"), planned("b")]), now()).unwrap();
        for _ in 1..steps::MAX_RUN_STEPS {
            assert_eq!(
                next(&items, Some(&items), 1, &r),
                Next::Again { stalled: 0 }
            );
            r = next_round(&r, &items, 0, now());
        }
        assert!(matches!(next(&items, Some(&items), 1, &r), Next::Stop(_)));
    }
}
