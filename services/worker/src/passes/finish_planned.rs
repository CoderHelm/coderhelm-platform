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
//! - Progress (a task finished, or the round pushed changes with no edit
//!   left refused, `supervisor::progressed`): another round
//!   ([`Next::Again`]). The runaway ceiling is the same as a run's steps.
//! - A round that ends with an edit refused and no task finished made no
//!   progress, whatever else it pushed: the next round is told which edits
//!   were refused and how to make them ([`next_round`]).
//! - A round that never ran its work (its steps ran out, a fault stopped
//!   it before it landed, [`Ran::Unfinished`]) is not a stall: it runs again
//!   with the stalls it had. One the team's usage limit refused
//!   ([`Ran::Limited`]) never ran either: the run waits on the limit
//!   ([`Next::WaitLimit`]), the ending every run takes at the limit.
//! - Two rounds in a row with no progress stop the work ([`Next::Stop`]):
//!   the run says once which tasks are still not done and why (the refused
//!   files by name), its status is `needs_input` (never `completed`), and
//!   the pull request stays a draft.
//!
//! The progress rule is the one ticket runs use between steps
//! (`steps::stalled_after`, `steps::MAX_STALLED_STEPS`).

use common::run_round::{Kind, Round};

use super::proof::{UNFINISHED_END, UNFINISHED_START};
use super::steps;
use super::write_guard::Refusal;

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
    first_round_of(&listed(section), now)
}

/// [`first_round`] for these items (the section's, after
/// `round_agree::sort`).
pub fn first_round_of(items: &[String], now: chrono::DateTime<chrono::Utc>) -> Option<Round> {
    if items.is_empty() {
        return None;
    }
    // Recorded as not scoped (no pull-request-files boundary): a planned-work
    // round is bounded by the plan, whose task files the branch may not
    // change yet (`write_guard::RoundFor::of_round`).
    Some(Round::start(Kind::PlannedWork, 1, "", &round_feedback(items), false, now).parked(false))
}

/// The round after `prev`, for the tasks still listed. It is told the
/// edits `prev` was refused, and how to make them.
pub fn next_round(
    prev: &Round,
    items: &[String],
    stalled: u32,
    refused: &[Refusal],
    now: chrono::DateTime<chrono::Utc>,
) -> Round {
    let mut r = Round::start(
        Kind::PlannedWork,
        prev.number.saturating_add(1),
        "",
        &format!(
            "{}{}",
            round_feedback(items),
            super::write_guard::refused_note(refused)
        ),
        false,
        now,
    )
    .parked(false);
    r.stalled = stalled;
    r
}

/// A round's refusals split into the files a review fix settled (the round
/// was refused for undoing that fix, `round_agree::UNDO_MARK`) and every
/// other refusal.
pub fn split_undone(refused: &[Refusal]) -> (Vec<Refusal>, Vec<Refusal>) {
    refused
        .iter()
        .cloned()
        .partition(|r| r.why.starts_with(super::round_agree::UNDO_MARK))
}

/// `items` without the planned work in a file a review fix settled: the
/// review decided that file, so the plan's version of it is not work left.
pub fn drop_settled(items: Vec<String>, settled: &[Refusal]) -> Vec<String> {
    if settled.is_empty() {
        return items;
    }
    items
        .into_iter()
        .filter(|i| {
            let paths = super::stack::named_paths(i);
            !settled.iter().any(|s| paths.contains(&s.path))
        })
        .collect()
}

/// How a planned-work round ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ran {
    /// It ran to its end; its commit changed this many files.
    Finished { files: usize },
    /// It never finished its work: its steps ran out, or a fault stopped it
    /// before it landed anything. Not a stall: nothing was judged.
    Unfinished,
    /// The team's usage limit refused its model calls: it never ran. The
    /// limit's words.
    Limited(String),
}

/// What follows a planned-work round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next {
    /// Nothing is listed any more: the pull request may be marked ready.
    Ready,
    /// Another round, with this many stalled rounds before it.
    Again { stalled: u32 },
    /// The round never ran for the team's usage limit: the run waits on the
    /// limit (the usage-limit ending every run takes), its stalls unchanged.
    WaitLimit(String),
    /// The work stops: this reason names the tasks and why.
    Stop(String),
}

/// What follows a round. `before`: the tasks listed when it started;
/// `after`: the tasks the check lists now (`None` when the check couldn't
/// answer: nothing is claimed done then); `ran`: how the round ended;
/// `refused`: the edits the round was refused and never made. Only a round
/// that ran to its end with no progress counts as stalled.
pub fn next(
    before: &[String],
    after: Option<&[String]>,
    ran: &Ran,
    refused: &[Refusal],
    round: &Round,
) -> Next {
    let after = after.unwrap_or(before);
    if after.is_empty() {
        return Next::Ready;
    }
    let pushed_files = match ran {
        Ran::Finished { files } => *files,
        Ran::Limited(why) => return Next::WaitLimit(why.clone()),
        // A round that didn't run its work is run again with the stalls it
        // had, under the same runaway ceiling.
        Ran::Unfinished if round.number >= steps::MAX_RUN_STEPS => {
            return Next::Stop(stop_reason(
                after,
                &format!(
                    "the work reached the safety ceiling of {} planned-work rounds",
                    steps::MAX_RUN_STEPS
                ),
            ));
        }
        Ran::Unfinished => {
            return Next::Again {
                stalled: round.stalled,
            }
        }
    };
    let progress =
        super::supervisor::progressed(after.len() < before.len(), pushed_files, refused.len());
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
        let what = if refused.is_empty() {
            "nothing was pushed".to_string()
        } else {
            format!(
                "the edits the work needs were refused ({})",
                refused
                    .iter()
                    .map(|r| format!("`{}`: {}", r.path, r.why))
                    .collect::<Vec<_>>()
                    .join("; ")
            )
        };
        return Next::Stop(stop_reason(
            after,
            &format!(
                "planned-work rounds {} and {last} made no progress: no task was finished and \
                 {what}",
                last - 1
            ),
        ));
    }
    Next::Again { stalled }
}

/// The run's status after a round: waiting on CI while there are new
/// commits or another round is parked, completed once nothing is left and
/// nothing new was pushed, and `needs_input` when the work stopped or waits
/// on the usage limit (never `completed`: the tasks are still not done).
pub fn status_after(next: &Next, pushed_files: usize) -> &'static str {
    use common::run_status::{AWAITING_CI, COMPLETED, NEEDS_INPUT};
    match next {
        Next::Ready if pushed_files > 0 => AWAITING_CI,
        Next::Ready => COMPLETED,
        Next::Again { .. } => AWAITING_CI,
        Next::WaitLimit(_) | Next::Stop(_) => NEEDS_INPUT,
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
        assert_eq!(
            next(&three, Some(&two), &Ran::Finished { files: 0 }, &[], &r),
            Next::Again { stalled: 0 }
        );
        // Pushed changes, nothing finished yet: progress (the head moved).
        assert_eq!(
            next(&two, Some(&two), &Ran::Finished { files: 3 }, &[], &r),
            Next::Again { stalled: 0 }
        );
        // No change at all: one stalled round, then the second stops.
        assert_eq!(
            next(&two, Some(&two), &Ran::Finished { files: 0 }, &[], &r),
            Next::Again { stalled: 1 }
        );
        r = next_round(&r, &two, 1, &[], now());
        match next(&two, Some(&two), &Ran::Finished { files: 0 }, &[], &r) {
            Next::Stop(why) => {
                assert!(why.contains("rounds 1 and 2 made no progress"), "{why}");
                assert!(why.contains("- a\n- b"), "{why}");
            }
            other => panic!("{other:?}"),
        }
        // A check that couldn't answer claims nothing done.
        assert_eq!(
            next(&two, None, &Ran::Finished { files: 0 }, &[], &r).clone(),
            next(&two, Some(&two), &Ran::Finished { files: 0 }, &[], &r)
        );
        // Nothing left: ready.
        assert_eq!(
            next(&two, Some(&[]), &Ran::Finished { files: 0 }, &[], &r),
            Next::Ready
        );
    }

    /// A round that ends with an edit refused and no task finished made no
    /// progress, whatever else it pushed: the next round is told what was
    /// refused, and a second such round stops with the files by name.
    #[test]
    fn a_round_with_a_refused_edit_and_nothing_finished_stalls_then_stops_once() {
        let items = vec!["`src/lib/metadata.ts` has no metadata for the gift route.".to_string()];
        let refused = vec![Refusal {
            path: "src/lib/metadata.ts".into(),
            why: "outside the files of the planned tasks still not done".into(),
        }];
        let r = first_round(&unfinished_section(&[planned(&items[0])]), now()).unwrap();
        // It pushed other files: still no progress, because the edit the
        // task needs is the one it couldn't make.
        assert_eq!(
            next(
                &items,
                Some(&items),
                &Ran::Finished { files: 4 },
                &refused,
                &r
            ),
            Next::Again { stalled: 1 }
        );
        // The same push with nothing refused is progress.
        assert_eq!(
            next(&items, Some(&items), &Ran::Finished { files: 4 }, &[], &r),
            Next::Again { stalled: 0 }
        );
        // A task finished is progress whatever was refused.
        assert_eq!(
            next(
                &["a".to_string(), "b".to_string()],
                Some(&items),
                &Ran::Finished { files: 1 },
                &refused,
                &r
            ),
            Next::Again { stalled: 0 }
        );
        // The next round is told, once, what was refused and how to make it.
        let second = next_round(&r, &items, 1, &refused, now());
        assert!(second.feedback.contains("Edits the last round was refused"));
        assert!(second.feedback.contains("`src/lib/metadata.ts`"));
        assert!(second.feedback.contains("outside_plan"));
        // Refused again: the work stops and says which file, once.
        match next(
            &items,
            Some(&items),
            &Ran::Finished { files: 2 },
            &refused,
            &second,
        ) {
            Next::Stop(why) => {
                assert!(why.contains("rounds 1 and 2 made no progress"), "{why}");
                assert!(
                    why.contains("`src/lib/metadata.ts`: outside the files"),
                    "{why}"
                );
                assert_eq!(why.matches("src/lib/metadata.ts").count(), 2, "{why}");
            }
            other => panic!("{other:?}"),
        }
    }

    /// A round refused for undoing a review fix met the review's decision:
    /// the planned work in that file is dropped and the round is not a
    /// stall, so the plan and the review never stop the run between them.
    #[test]
    fn work_a_review_fix_settled_is_dropped_not_stalled_on() {
        let refusal =
            crate::passes::round_agree::undo_refusal(&[crate::passes::round_agree::Undo {
                sha: "abc1234def".into(),
                path: "e2e/theme.spec.ts".into(),
                lines: vec!["test('three cases')".into(); 3],
            }]);
        let refused: Vec<Refusal> = crate::passes::round_agree::undone_files(&refusal)
            .into_iter()
            .map(|t| Refusal {
                path: t.path,
                why: format!("{} ({})", crate::passes::round_agree::UNDO_MARK, t.sha),
            })
            .collect();
        assert_eq!(refused.len(), 1);
        assert_eq!(refused[0].path, "e2e/theme.spec.ts");
        let (settled, other) = split_undone(&refused);
        assert_eq!((settled.len(), other.len()), (1, 0));
        let items = vec![
            "The task for e2e/theme.spec.ts is only partly done: Test 2 and Test 3 are missing"
                .to_string(),
        ];
        let after = drop_settled(items.clone(), &settled);
        assert!(after.is_empty());
        let r = first_round(&unfinished_section(&[planned(&items[0])]), now()).unwrap();
        assert_eq!(
            next(
                &items,
                Some(&after),
                &Ran::Finished { files: 0 },
                &other,
                &r
            ),
            Next::Ready
        );
        // Work in another file stays.
        let elsewhere = vec!["`src/page.tsx` is not written.".to_string()];
        assert_eq!(drop_settled(elsewhere.clone(), &settled), elsewhere);
    }

    /// A round that never ran (the usage limit refused its calls, or it
    /// stopped before it landed anything) is not a stall: two in a row
    /// never stop the work.
    #[test]
    fn rounds_that_never_ran_are_not_stalls() {
        let items = vec!["a".to_string(), "b".to_string()];
        let r = first_round(&unfinished_section(&[planned("a"), planned("b")]), now()).unwrap();
        let limit = "Monthly cache read tokens limit reached (9.1M of 9M).".to_string();
        let mut round = r.clone();
        for _ in 0..2 {
            assert_eq!(
                next(
                    &items,
                    Some(&items),
                    &Ran::Limited(limit.clone()),
                    &[],
                    &round
                ),
                Next::WaitLimit(limit.clone())
            );
            let again = next(&items, Some(&items), &Ran::Unfinished, &[], &round);
            assert_eq!(again, Next::Again { stalled: 0 });
            round = next_round(&round, &items, 0, &[], now());
        }
        assert_eq!(
            status_after(&Next::WaitLimit(limit), 0),
            common::run_status::NEEDS_INPUT
        );
        // The stalls a round had stay as they were.
        let mut stalled = r.clone();
        stalled.stalled = 1;
        assert_eq!(
            next(&items, Some(&items), &Ran::Unfinished, &[], &stalled),
            Next::Again { stalled: 1 }
        );
    }

    #[test]
    fn a_progressing_round_continues_up_to_the_runaway_ceiling_only() {
        let items = vec!["a".to_string(), "b".to_string()];
        let mut r = first_round(&unfinished_section(&[planned("a"), planned("b")]), now()).unwrap();
        for _ in 1..steps::MAX_RUN_STEPS {
            assert_eq!(
                next(&items, Some(&items), &Ran::Finished { files: 1 }, &[], &r),
                Next::Again { stalled: 0 }
            );
            r = next_round(&r, &items, 0, &[], now());
        }
        assert!(matches!(
            next(&items, Some(&items), &Ran::Finished { files: 1 }, &[], &r),
            Next::Stop(_)
        ));
    }
}
