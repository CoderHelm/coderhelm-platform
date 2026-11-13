//! Security fixes: a finding's `fixing` and `pr_open` always say what is
//! true on GitHub and in the run's record.
//!
//! A finding is claimed twice on its way to fixed: by a fix run (`fixing`,
//! with the run's ticket) and by the pull request the run opens (`pr_open`,
//! with the pull request). Either claim can end without the fix: a person
//! closes the pull request, the run is cancelled, fails or is closed, the
//! branch is deleted (GitHub closes its pull request). A claim nothing
//! releases is a finding nobody can ask a fix for: the dashboard's checkbox
//! is off for a finding in a pull request or a run.
//!
//! The rules here are pure: what GitHub says about a pull request
//! ([`PrNow`]) or what became of a run ([`RunNow`]) goes in, and the
//! finding comes out as it should stand, with the line for its timeline.
//! Three callers read the facts and apply the rules (the worker's
//! `passes::security_release`): a pull request webhook, a run a person
//! cancelled or closed, and the scheduled sweep, which finds what the first
//! two missed and what was stale before they existed. Applying a fact twice
//! changes nothing the second time.

use chrono::{DateTime, Duration, Utc};

use crate::security_fixes::{parse_time, Finding, FindingState, PrRef, Status};

/// Days automatic fixes wait after a pull request closed unmerged or a
/// person ended the fix run: the team said no for now. A person asking for
/// a fix never waits on it.
pub const HOLD_DAYS_CLOSED: i64 = 7;
/// Days automatic fixes wait after a fix run ended by itself with no pull
/// request (it failed): tried again tomorrow.
pub const HOLD_DAYS_RUN_ENDED: i64 = 1;
/// Hours a `fixing` finding with no run on record waits before it is
/// released: the run's message may still be queued.
pub const NO_RUN_GRACE_HOURS: i64 = 12;
/// Pull requests one sweep reads per team at most; the rest are read on
/// the following sweeps, in turn.
pub const MAX_PRS_PER_SWEEP: usize = 100;
/// Fix runs one sweep reads per team at most.
pub const MAX_TICKETS_PER_SWEEP: usize = 50;
/// Seconds between two sweeps: what the turn-taking counts in.
pub const SWEEP_SECS: i64 = 1_200;

/// Who closed a pull request without merging it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Closer {
    /// Their GitHub login, or their name when they closed it from
    /// CoderHelm; empty when GitHub does not say.
    pub by: String,
    /// A person closed it (on GitHub, or by closing the run in CoderHelm).
    /// False when CoderHelm closed it for a reason of its own.
    pub person: bool,
    /// When, as GitHub wrote it.
    pub at: Option<String>,
}

/// What GitHub says about a pull request now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrNow {
    Open,
    Merged,
    Closed(Closer),
    /// GitHub answers that there is no such pull request (its repository
    /// was deleted or moved, or the app no longer has it): released like a
    /// closed one, since no fix is coming from it.
    Gone,
    /// GitHub could not be read: the finding is left as it is and read
    /// again next time. Never taken for open, closed or merged.
    Unknown,
}

/// What became of the fix run a `fixing` finding waits on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunNow {
    /// Queued or working.
    Working,
    /// Over with no pull request to show for it.
    Ended {
        /// The run record's status (`cancelled`, `failed`, ...).
        status: String,
        /// The person who ended it, when one did.
        by: Option<String>,
    },
    /// No run of the ticket since the finding was claimed.
    Missing,
    /// The run records could not be read: nothing changes.
    Unknown,
}

/// What a rule did to a finding: the line for its timeline, with a link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub text: String,
    pub link: Option<String>,
}

fn change(text: String, link: &str) -> Option<Change> {
    Some(Change {
        text,
        link: (!link.is_empty()).then(|| link.to_string()),
    })
}

/// Whether the hold a closed pull request or an ended run left is still on.
pub fn held(st: &FindingState, now: DateTime<Utc>) -> bool {
    st.hold_until
        .as_deref()
        .and_then(parse_time)
        .is_some_and(|t| t > now)
}

/// Whether a finding's pull request is one the sweep reads: recorded as
/// open, or closed unmerged with its hold still on (a person may reopen
/// it, and the finding then belongs to it again).
pub fn watches_pr(f: &Finding, st: &FindingState, now: DateTime<Utc>) -> bool {
    match &f.pr {
        Some(p) if p.state == "open" => true,
        Some(p) => p.state == "closed" && f.status == Status::Open && held(st, now),
        None => false,
    }
}

/// Brings a finding in line with its pull request. `None` when nothing
/// changed (so a second call with the same fact writes nothing).
///
/// - Merged: the finding is fixed.
/// - Closed unmerged: it is open again, its claim released, with who closed
///   the pull request and when kept on the pull request's record. Automatic
///   fixes wait [`HOLD_DAYS_CLOSED`] days; a person asking never waits.
/// - Open again after it was closed (a person reopened it): the finding is
///   in the pull request again and the hold is lifted.
/// - Unknown: left alone.
///
/// Only a finding that is `pr_open` changes status; one a person dismissed,
/// or one fixed another way, keeps its status and only its pull request's
/// recorded state follows GitHub.
pub fn settle_pr(
    f: &mut Finding,
    st: &mut FindingState,
    pr_now: &PrNow,
    now: DateTime<Utc>,
) -> Option<Change> {
    let pr = f.pr.clone()?;
    let now_s = now.to_rfc3339();
    let gone = PrNow::Closed(Closer::default());
    let pr_now = match pr_now {
        PrNow::Gone => &gone,
        other => other,
    };
    let is_gone = |c: &Closer| !c.person && c.by.is_empty() && c.at.is_none();
    match pr_now {
        PrNow::Unknown | PrNow::Gone => None,
        PrNow::Merged => {
            if pr.state == "merged" {
                return None;
            }
            f.pr = Some(PrRef {
                state: "merged".into(),
                closed_at: None,
                closed_by: None,
                ..pr.clone()
            });
            if matches!(f.status, Status::PrOpen | Status::Open | Status::Fixing) {
                f.status = Status::Fixed;
                f.waiting = None;
                st.merged_at = Some(now_s.clone());
                st.hold_until = None;
            }
            f.updated_at = now_s;
            change(format!("PR #{} merged", pr.number), &pr.url)
        }
        PrNow::Closed(closer) => {
            if pr.state != "open" {
                return None;
            }
            f.pr = Some(PrRef {
                state: "closed".into(),
                closed_at: Some(closer.at.clone().unwrap_or_else(|| now_s.clone())),
                closed_by: (!closer.by.is_empty()).then(|| closer.by.clone()),
                closed_by_person: closer.person,
                ..pr.clone()
            });
            let was_in_it = f.status == Status::PrOpen;
            if was_in_it {
                f.status = Status::Open;
                f.waiting = None;
                st.ticket = None;
                st.bundled = false;
                st.claimed_at = None;
                st.hold_until = Some((now + Duration::days(HOLD_DAYS_CLOSED)).to_rfc3339());
            }
            f.updated_at = now_s;
            let who = match (closer.person, closer.by.is_empty()) {
                (true, false) => format!(" by {}", closer.by),
                (false, _) => " by CoderHelm".to_string(),
                (true, true) => String::new(),
            };
            let then = if was_in_it {
                format!(
                    "; automatic fixes wait {HOLD_DAYS_CLOSED} days. Ask for a fix to open a new pull request now"
                )
            } else {
                String::new()
            };
            let what = if is_gone(closer) {
                format!("PR #{} is no longer on GitHub", pr.number)
            } else {
                format!("PR #{} was closed without merging{who}", pr.number)
            };
            change(format!("{what}{then}"), &pr.url)
        }
        PrNow::Open => {
            if pr.state != "closed" {
                return None;
            }
            // Reopened. A finding that moved on since (a new run, another
            // pull request, fixed, dismissed) is not pulled back.
            if f.status != Status::Open || st.ticket.is_some() || st.requested_by.is_some() {
                return None;
            }
            f.pr = Some(PrRef {
                state: "open".into(),
                ci: Some("pending".into()),
                closed_at: None,
                closed_by: None,
                closed_by_person: false,
                ..pr.clone()
            });
            f.status = Status::PrOpen;
            f.waiting = None;
            st.hold_until = None;
            f.updated_at = now_s;
            change(
                format!("PR #{} was reopened: the fix is in it again", pr.number),
                &pr.url,
            )
        }
    }
}

/// Days a run waiting on a person's answer keeps its findings.
pub const WAITING_RUN_DAYS: i64 = 7;
/// Hours a run recorded as queued or working keeps its findings after its
/// record last moved: a run whose worker died never ends on its own record.
pub const SILENT_RUN_HOURS: i64 = 26;

/// What a run record says became of the run: working while it is in flight
/// (or waits on a person's answer) and its record still moves; ended
/// otherwise. `by`: who cancelled or closed it.
pub fn run_now_of(
    status: &str,
    updated_at: &str,
    by: Option<String>,
    now: DateTime<Utc>,
) -> RunNow {
    use crate::run_status as rs;
    let age = parse_time(updated_at).map(|t| now - t);
    let within = |d: Duration| age.is_none_or(|a| a < d);
    let working = if rs::IN_FLIGHT.contains(&status) {
        within(Duration::hours(SILENT_RUN_HOURS))
    } else if matches!(status, rs::NEEDS_INPUT | rs::NEEDS_OUTSIDE_CHANGE) {
        within(Duration::days(WAITING_RUN_DAYS))
    } else {
        false
    };
    if working {
        RunNow::Working
    } else {
        RunNow::Ended {
            status: status.to_string(),
            by: by.filter(|b| !b.is_empty()),
        }
    }
}

/// When a `fixing` finding was claimed by its run: recorded at the claim,
/// or (a finding claimed before that was kept) its last write.
pub fn claimed_at(f: &Finding, st: &FindingState) -> Option<DateTime<Utc>> {
    st.claimed_at
        .as_deref()
        .and_then(parse_time)
        .or_else(|| parse_time(&f.updated_at))
}

/// Brings a `fixing` finding in line with its run. `open_pr`: the open pull
/// request on the run's branch, when there is one (the finding was never
/// told). `None` when nothing changed.
///
/// - A pull request is open on the branch: the finding is in it.
/// - The run is queued or working: left alone.
/// - The run ended with no pull request: the finding is open again. A run a
///   person cancelled or closed holds automatic fixes [`HOLD_DAYS_CLOSED`]
///   days; one that ended by itself, [`HOLD_DAYS_RUN_ENDED`] day.
/// - No run on record: released once [`NO_RUN_GRACE_HOURS`] have passed.
pub fn settle_run(
    f: &mut Finding,
    st: &mut FindingState,
    run: &RunNow,
    open_pr: Option<&PrRef>,
    now: DateTime<Utc>,
) -> Option<Change> {
    if f.status != Status::Fixing {
        return None;
    }
    let now_s = now.to_rfc3339();
    if let Some(pr) = open_pr {
        f.status = Status::PrOpen;
        f.pr = Some(PrRef {
            ci: Some("pending".into()),
            state: "open".into(),
            adopted: false,
            closed_at: None,
            closed_by: None,
            closed_by_person: false,
            ..pr.clone()
        });
        f.waiting = None;
        st.pr_opened_at.get_or_insert_with(|| now_s.clone());
        f.updated_at = now_s;
        return change(format!("PR #{} opened", pr.number), &pr.url);
    }
    let (days, text, waiting) = match run {
        RunNow::Working | RunNow::Unknown => return None,
        RunNow::Ended { status, by } => {
            if crate::run_status::ended_by_person(status) {
                let who = by
                    .as_deref()
                    .filter(|b| !b.is_empty())
                    .map(|b| format!(" by {b}"))
                    .unwrap_or_default();
                let word = if status == crate::run_status::CLOSED {
                    "closed"
                } else {
                    "cancelled"
                };
                (
                    HOLD_DAYS_CLOSED,
                    format!(
                        "The fix run was {word}{who} before it opened a pull request; automatic fixes wait {HOLD_DAYS_CLOSED} days. Ask for a fix to start one now"
                    ),
                    Some(format!(
                        "The fix run was {word}{who}. Ask for a fix to start a new one."
                    )),
                )
            } else {
                (
                    HOLD_DAYS_RUN_ENDED,
                    "The fix run ended without a pull request; it is tried again tomorrow"
                        .to_string(),
                    None,
                )
            }
        }
        RunNow::Missing => {
            let waited =
                claimed_at(f, st).is_none_or(|t| now - t > Duration::hours(NO_RUN_GRACE_HOURS));
            if !waited {
                return None;
            }
            (
                HOLD_DAYS_RUN_ENDED,
                "The fix run ended without a pull request; it is tried again tomorrow".to_string(),
                None,
            )
        }
    };
    f.status = Status::Open;
    f.waiting = waiting;
    st.ticket = None;
    st.bundled = false;
    st.claimed_at = None;
    st.hold_until = Some((now + Duration::days(days)).to_rfc3339());
    f.updated_at = now_s;
    change(text, "")
}

/// The `limit` items of `all` this sweep reads, in turn: every sweep starts
/// where the last one stopped, so a team with more than fit in one sweep
/// has each read within `ceil(n / limit)` sweeps and none is starved.
pub fn turn<T: Clone>(all: &[T], limit: usize, now: DateTime<Utc>) -> Vec<T> {
    if all.len() <= limit || limit == 0 {
        return all.to_vec();
    }
    let sweep = (now.timestamp() / SWEEP_SECS).max(0) as usize;
    let start = (sweep % all.len().div_ceil(limit)) * limit;
    all.iter()
        .cycle()
        .skip(start)
        .take(limit)
        .cloned()
        .collect()
}

/// What a person's Retry of a security fix run does, from the unit's
/// findings as they stand: `Some(ids)` when the run's pull request is gone
/// (closed unmerged, or never opened and the run is over), with the
/// findings a new pull request can still fix, so the retry is a new run on
/// the plan as it is now; `None` when the unit still has an open pull
/// request or a run working (the earlier run is taken up as before).
pub fn retry_ids(members: &[(Finding, FindingState)]) -> Option<Vec<String>> {
    let busy = members.iter().any(|(f, _)| {
        f.status == Status::Fixing || f.pr.as_ref().is_some_and(|p| p.state == "open")
    });
    if busy {
        return None;
    }
    let ids: Vec<String> = members
        .iter()
        .filter(|(f, _)| matches!(f.status, Status::Open | Status::NoFix))
        .filter(|(f, _)| f.fixed_version.is_some())
        .map(|(f, _)| f.id.clone())
        .collect();
    (!ids.is_empty()).then_some(ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(t: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(t).unwrap().with_timezone(&Utc)
    }

    fn now() -> DateTime<Utc> {
        at("2026-11-13T10:00:00Z")
    }

    fn in_pr(id: &str, number: u64) -> (Finding, FindingState) {
        (
            Finding {
                id: id.into(),
                repo: "acme/web".into(),
                package: "left-pad".into(),
                version: "1.0.0".into(),
                fixed_version: Some("1.0.1".into()),
                status: Status::PrOpen,
                pr: Some(PrRef {
                    number,
                    url: format!("https://github.com/acme/web/pull/{number}"),
                    ci: Some("passing".into()),
                    state: "open".into(),
                    ..Default::default()
                }),
                updated_at: "2026-11-12T09:00:00Z".into(),
                ..Default::default()
            },
            FindingState {
                ticket: Some("SEC-AAAAAAAAAAAA".into()),
                bundled: true,
                ..Default::default()
            },
        )
    }

    fn fixing(id: &str) -> (Finding, FindingState) {
        let (mut f, mut st) = in_pr(id, 1);
        f.status = Status::Fixing;
        f.pr = None;
        st.claimed_at = Some("2026-11-13T09:00:00Z".into());
        (f, st)
    }

    fn person(by: &str) -> PrNow {
        PrNow::Closed(Closer {
            by: by.into(),
            person: true,
            at: Some("2026-11-13T09:30:00Z".into()),
        })
    }

    #[test]
    fn a_pull_request_a_person_closed_gives_its_finding_back_with_who_and_when() {
        let (mut f, mut st) = in_pr("a", 12);
        let c = settle_pr(&mut f, &mut st, &person("octo"), now()).unwrap();
        assert_eq!(f.status, Status::Open);
        let pr = f.pr.clone().unwrap();
        assert_eq!(
            (
                pr.number,
                pr.state.as_str(),
                pr.closed_by.as_deref(),
                pr.closed_by_person
            ),
            (12, "closed", Some("octo"), true)
        );
        assert_eq!(pr.closed_at.as_deref(), Some("2026-11-13T09:30:00Z"));
        // The claim is gone, and automatic fixes wait a week.
        assert_eq!((st.ticket.clone(), st.bundled), (None, false));
        assert_eq!(
            st.hold_until.as_deref().and_then(parse_time),
            Some(now() + Duration::days(7))
        );
        assert!(held(&st, now()));
        assert!(c.text.contains("PR #12 was closed without merging by octo"));
        assert!(c.text.contains("Ask for a fix"));
        assert_eq!(
            c.link.as_deref(),
            Some("https://github.com/acme/web/pull/12")
        );
        assert_eq!(f.updated_at, now().to_rfc3339());
    }

    #[test]
    fn the_same_fact_twice_changes_nothing_the_second_time() {
        for fact in [person("octo"), PrNow::Merged] {
            let (mut f, mut st) = in_pr("a", 12);
            assert!(settle_pr(&mut f, &mut st, &fact, now()).is_some());
            let after = (f.clone(), st.clone());
            assert!(settle_pr(&mut f, &mut st, &fact, now() + Duration::minutes(20)).is_none());
            assert_eq!((f, st), after);
        }
        // An open pull request recorded as open: nothing to do.
        let (mut f, mut st) = in_pr("a", 12);
        let before = (f.clone(), st.clone());
        assert!(settle_pr(&mut f, &mut st, &PrNow::Open, now()).is_none());
        assert_eq!((f, st), before);
    }

    #[test]
    fn a_merge_fixes_the_finding() {
        let (mut f, mut st) = in_pr("a", 12);
        let c = settle_pr(&mut f, &mut st, &PrNow::Merged, now()).unwrap();
        assert_eq!(f.status, Status::Fixed);
        assert_eq!(f.pr.unwrap().state, "merged");
        assert!(st.merged_at.is_some());
        assert_eq!(c.text, "PR #12 merged");
    }

    #[test]
    fn github_unreadable_is_never_taken_for_open_closed_or_merged() {
        let (mut f, mut st) = in_pr("a", 12);
        let before = (f.clone(), st.clone());
        assert!(settle_pr(&mut f, &mut st, &PrNow::Unknown, now()).is_none());
        assert_eq!((f, st), before);
    }

    #[test]
    fn a_pull_request_coderhelm_closed_says_so_and_still_releases() {
        let (mut f, mut st) = in_pr("a", 12);
        let by_us = PrNow::Closed(Closer {
            by: "coderhelm[bot]".into(),
            person: false,
            at: None,
        });
        let c = settle_pr(&mut f, &mut st, &by_us, now()).unwrap();
        assert_eq!(f.status, Status::Open);
        assert!(c.text.contains("by CoderHelm"), "{}", c.text);
        assert!(!f.pr.unwrap().closed_by_person);
        assert!(held(&st, now()));
    }

    #[test]
    fn a_dismissed_finding_keeps_its_status_when_its_pull_request_closes() {
        let (mut f, mut st) = in_pr("a", 12);
        f.status = Status::Dismissed;
        let c = settle_pr(&mut f, &mut st, &person("octo"), now()).unwrap();
        assert_eq!(f.status, Status::Dismissed);
        assert_eq!(f.pr.unwrap().state, "closed");
        assert_eq!(st.hold_until, None);
        assert!(!c.text.contains("Ask for a fix"));
    }

    #[test]
    fn a_reopened_pull_request_takes_its_finding_back_unless_it_moved_on() {
        let (mut f, mut st) = in_pr("a", 12);
        settle_pr(&mut f, &mut st, &person("octo"), now()).unwrap();
        assert!(watches_pr(&f, &st, now()));
        let later = now() + Duration::hours(1);
        let c = settle_pr(&mut f, &mut st, &PrNow::Open, later).unwrap();
        assert_eq!(f.status, Status::PrOpen);
        let pr = f.pr.clone().unwrap();
        assert_eq!(
            (pr.state.as_str(), pr.closed_by, pr.closed_at),
            ("open", None, None)
        );
        assert_eq!(st.hold_until, None);
        assert!(c.text.contains("reopened"));

        // A person asked for a new fix in between: the old pull request
        // coming back does not take the finding from the new run.
        let (mut f, mut st) = in_pr("b", 12);
        settle_pr(&mut f, &mut st, &person("octo"), now()).unwrap();
        st.requested_by = Some("dev@example.com".into());
        assert!(settle_pr(&mut f, &mut st, &PrNow::Open, later).is_none());
        let (mut f, mut st) = in_pr("c", 12);
        settle_pr(&mut f, &mut st, &person("octo"), now()).unwrap();
        f.status = Status::Fixing;
        st.ticket = Some("SEC-BBBBBBBBBBBB".into());
        assert!(settle_pr(&mut f, &mut st, &PrNow::Open, later).is_none());
        assert_eq!(f.status, Status::Fixing);
    }

    #[test]
    fn a_closed_pull_request_is_watched_only_while_its_hold_is_on() {
        let (mut f, mut st) = in_pr("a", 12);
        settle_pr(&mut f, &mut st, &person("octo"), now()).unwrap();
        assert!(watches_pr(&f, &st, now() + Duration::days(6)));
        assert!(!watches_pr(&f, &st, now() + Duration::days(8)));
        let (mut f, mut st) = in_pr("m", 12);
        settle_pr(&mut f, &mut st, &PrNow::Merged, now()).unwrap();
        assert!(!watches_pr(&f, &st, now()));
    }

    #[test]
    fn a_run_a_person_cancelled_releases_what_it_claimed_and_says_who() {
        let (mut f, mut st) = fixing("a");
        let run = RunNow::Ended {
            status: "cancelled".into(),
            by: Some("dev@example.com".into()),
        };
        let c = settle_run(&mut f, &mut st, &run, None, now()).unwrap();
        assert_eq!(f.status, Status::Open);
        assert_eq!(
            (st.ticket.clone(), st.bundled, st.claimed_at.clone()),
            (None, false, None)
        );
        assert_eq!(
            st.hold_until.as_deref().and_then(parse_time),
            Some(now() + Duration::days(7))
        );
        assert!(c.text.contains("cancelled by dev@example.com"));
        assert_eq!(
            f.waiting.as_deref(),
            Some("The fix run was cancelled by dev@example.com. Ask for a fix to start a new one.")
        );
        // Again: nothing.
        assert!(settle_run(&mut f, &mut st, &run, None, now()).is_none());
    }

    #[test]
    fn a_run_that_failed_is_tried_again_tomorrow_and_a_working_one_is_left_alone() {
        let (mut f, mut st) = fixing("a");
        for run in [RunNow::Working, RunNow::Unknown] {
            assert!(settle_run(&mut f, &mut st, &run, None, now()).is_none());
            assert_eq!(f.status, Status::Fixing);
        }
        let failed = RunNow::Ended {
            status: "failed".into(),
            by: None,
        };
        let c = settle_run(&mut f, &mut st, &failed, None, now()).unwrap();
        assert_eq!(f.status, Status::Open);
        assert_eq!(
            st.hold_until.as_deref().and_then(parse_time),
            Some(now() + Duration::days(1))
        );
        assert_eq!(f.waiting, None);
        assert!(c.text.contains("tried again tomorrow"));
    }

    #[test]
    fn a_finding_with_no_run_on_record_waits_out_the_queue_first() {
        let (mut f, mut st) = fixing("a");
        // Claimed an hour ago: the run's message may still be queued.
        assert!(settle_run(&mut f, &mut st, &RunNow::Missing, None, now()).is_none());
        let later = now() + Duration::hours(12);
        assert!(settle_run(&mut f, &mut st, &RunNow::Missing, None, later).is_some());
        assert_eq!(f.status, Status::Open);
        // A finding claimed before the claim time was kept: its last write.
        let (mut f, mut st) = fixing("b");
        st.claimed_at = None;
        f.updated_at = "2026-11-12T09:00:00Z".into();
        assert!(settle_run(&mut f, &mut st, &RunNow::Missing, None, now()).is_some());
    }

    #[test]
    fn a_pull_request_open_on_the_runs_branch_takes_the_finding_whatever_the_run_says() {
        let (mut f, mut st) = fixing("a");
        let pr = PrRef {
            number: 31,
            url: "https://github.com/acme/web/pull/31".into(),
            ..Default::default()
        };
        let ended = RunNow::Ended {
            status: "failed".into(),
            by: None,
        };
        let c = settle_run(&mut f, &mut st, &ended, Some(&pr), now()).unwrap();
        assert_eq!(f.status, Status::PrOpen);
        assert_eq!(
            f.pr.as_ref().map(|p| (p.number, p.state.as_str())),
            Some((31, "open"))
        );
        assert_eq!(st.ticket.as_deref(), Some("SEC-AAAAAAAAAAAA"));
        assert_eq!(c.text, "PR #31 opened");
    }

    #[test]
    fn a_run_record_reads_as_working_only_while_it_is_in_flight_and_moving() {
        let n = now();
        let recent = "2026-11-13T09:50:00Z";
        for s in ["pending", "queued", "running", "awaiting_ci", "needs_input"] {
            assert_eq!(run_now_of(s, recent, None, n), RunNow::Working, "{s}");
        }
        for s in ["cancelled", "closed", "failed", "completed", "retried"] {
            assert!(
                matches!(run_now_of(s, recent, None, n), RunNow::Ended { .. }),
                "{s}"
            );
        }
        assert_eq!(
            run_now_of("cancelled", recent, Some("dev@example.com".into()), n),
            RunNow::Ended {
                status: "cancelled".into(),
                by: Some("dev@example.com".into())
            }
        );
        // A run whose worker died two days ago never holds its findings.
        assert!(matches!(
            run_now_of("running", "2026-11-11T09:00:00Z", None, n),
            RunNow::Ended { .. }
        ));
    }

    #[test]
    fn a_pull_request_that_is_gone_releases_its_finding_too() {
        let (mut f, mut st) = in_pr("a", 12);
        let c = settle_pr(&mut f, &mut st, &PrNow::Gone, now()).unwrap();
        assert_eq!(f.status, Status::Open);
        assert_eq!(f.pr.as_ref().unwrap().state, "closed");
        assert!(
            c.text.starts_with("PR #12 is no longer on GitHub"),
            "{}",
            c.text
        );
        assert!(settle_pr(&mut f, &mut st, &PrNow::Gone, now()).is_none());
    }

    #[test]
    fn every_pull_request_gets_its_turn_when_more_wait_than_one_sweep_reads() {
        let all: Vec<u32> = (0..250).collect();
        assert_eq!(turn(&all[..80], 100, now()), all[..80].to_vec());
        let mut seen = std::collections::BTreeSet::new();
        for sweep in 0..3 {
            let t = turn(&all, 100, now() + Duration::seconds(sweep * SWEEP_SECS));
            assert_eq!(t.len(), 100);
            seen.extend(t);
        }
        assert_eq!(seen.len(), 250, "three sweeps read all 250");
    }

    #[test]
    fn a_retry_after_the_pull_request_closed_is_a_new_run_on_what_is_still_open() {
        let (mut a, mut sa) = in_pr("a", 12);
        let (mut b, mut sb) = in_pr("b", 12);
        // Still in the open pull request: the earlier run is taken up.
        assert_eq!(
            retry_ids(&[(a.clone(), sa.clone()), (b.clone(), sb.clone())]),
            None
        );
        settle_pr(&mut a, &mut sa, &person("octo"), now()).unwrap();
        settle_pr(&mut b, &mut sb, &person("octo"), now()).unwrap();
        // One was fixed another way since: only the other is asked for.
        b.status = Status::Fixed;
        assert_eq!(
            retry_ids(&[(a.clone(), sa.clone()), (b, sb)]),
            Some(vec!["a".to_string()])
        );
        // A run already working on it: nothing new starts.
        let (c, sc) = fixing("c");
        assert_eq!(retry_ids(&[(a, sa), (c, sc)]), None);
        assert_eq!(retry_ids(&[]), None);
    }
}
