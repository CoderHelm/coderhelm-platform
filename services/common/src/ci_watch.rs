//! A run waiting on CI never polls forever without progress.
//!
//! A run finished its work and waited for its pull request's checks. One
//! workflow passed; another never left `queued`. Every two minutes the run
//! asked GitHub again, found the same check still running, and armed
//! another check; a webhook resume in between armed a second chain, so it
//! checked about once a minute. After eight hours and almost 500 checks
//! nothing had changed and nothing said which check it was waiting on. The
//! 24-hour cap measured from the run's last write, which every check
//! refreshed, so it never came.
//!
//! The rules, each enforced in code:
//!
//! - **Progress is the pending set.** [`pending_of`] names the checks still
//!   queued or running on the head. [`step`] keeps when that set last
//!   changed ([`Wait`]); any change (a check started, finished, a new head)
//!   is progress and starts the clock again.
//! - **No progress for [`STALL_SECS`] is a stall.** The first stall
//!   re-runs the stuck checks' workflows once ([`Step::Rerun`]); a second
//!   stall stops the wait ([`Step::Stop`]): the run finishes with its pull
//!   request open, the stuck checks named with how long they waited
//!   ([`stop_note`]), and the ready decision holds the pull request until
//!   they pass.
//! - **One wait chain per run.** [`may_arm`]: a delayed check is armed only
//!   when none is due, so a webhook resume never starts a second chain.
//! - **No wait is left with no chain.** A hand-off clears the due check it
//!   armed, so a later wait can arm its own. A run still waiting on CI with
//!   nothing due ([`parked`]) is picked back up by the self-heal sweep, on
//!   each release and every 15 minutes, a bounded number of times.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The run record attribute that keeps the wait, as JSON.
pub const FIELD: &str = "ci_wait";
/// The run record attribute that says when the next delayed check is due
/// (unix seconds).
pub const DUE_FIELD: &str = "ci_poll_due";
/// How long the pending set may stay the same before it is a stall.
pub const STALL_SECS: i64 = 3600;
/// A delayed check this close to due counts as due (queue timing).
pub const DUE_SLACK_SECS: i64 = 20;

/// Where the wait stands.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Wait {
    pub head: String,
    /// The checks still queued or running, as "name (status)", sorted.
    pub pending: Vec<String>,
    /// When the pending set last changed (unix seconds).
    pub since: i64,
    /// The stuck checks were re-run once already.
    #[serde(default)]
    pub reran: bool,
}

/// What the wait does now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Progress, or not long enough: check again later.
    Wait,
    /// The first stall: re-run the stuck checks' workflows once.
    Rerun,
    /// The second stall: stop waiting, name what is stuck.
    Stop,
}

/// The checks still queued or running in a merged check-runs body (as
/// `list_ci_for_ref` returns it), as "name (status)", sorted.
pub fn pending_of(checks: &Value) -> Vec<String> {
    let mut out: Vec<String> = checks["check_runs"]
        .as_array()
        .map(|rs| {
            rs.iter()
                .filter_map(|r| {
                    let s = r["status"].as_str().unwrap_or("");
                    (s == "queued" || s == "in_progress").then(|| {
                        format!("{} ({s})", r["name"].as_str().unwrap_or("a check").trim())
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out.dedup();
    out
}

/// The workflow runs behind the pending checks (from each check's
/// `details_url`, `/actions/runs/<id>/...`): what a re-run starts again.
/// A commit status from outside Actions has none.
pub fn pending_run_ids(checks: &Value) -> Vec<u64> {
    let mut out: Vec<u64> = checks["check_runs"]
        .as_array()
        .map(|rs| {
            rs.iter()
                .filter(|r| matches!(r["status"].as_str(), Some("queued") | Some("in_progress")))
                .filter_map(|r| {
                    let url = r["details_url"].as_str()?;
                    let rest = url.split("/actions/runs/").nth(1)?;
                    rest.split(|c: char| !c.is_ascii_digit())
                        .next()?
                        .parse()
                        .ok()
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort_unstable();
    out.dedup();
    out
}

/// The wait after one more look: `prev` as kept, the head and pending set
/// now, at `now` (unix seconds).
pub fn step(prev: Option<&Wait>, head: &str, pending: &[String], now: i64) -> (Wait, Step) {
    let fresh = || Wait {
        head: head.to_string(),
        pending: pending.to_vec(),
        since: now,
        reran: false,
    };
    let Some(p) = prev.filter(|p| p.head == head && p.pending == pending) else {
        return (fresh(), Step::Wait);
    };
    if now - p.since < STALL_SECS {
        return (p.clone(), Step::Wait);
    }
    if !p.reran {
        let mut w = p.clone();
        w.reran = true;
        w.since = now;
        return (w, Step::Rerun);
    }
    (p.clone(), Step::Stop)
}

/// The wait for `head` with nothing pending on it: the kept wait when it is
/// already that, else a fresh one. A wait kept for an older head, or with
/// checks pending that have since ended, is dropped: a run once showed a
/// day-old head's checks as still running while its pull request had
/// moved on.
pub fn for_head(prev: Option<&Wait>, head: &str, now: i64) -> Wait {
    match prev {
        Some(p) if p.head == head && p.pending.is_empty() => p.clone(),
        _ => Wait {
            head: head.to_string(),
            pending: Vec::new(),
            since: now,
            reran: false,
        },
    }
}

/// Whether a delayed check may be armed now: none is due later than `now`.
pub fn may_arm(due: Option<i64>, now: i64) -> bool {
    due.is_none_or(|d| d <= now + DUE_SLACK_SECS)
}

/// How long a run waiting on CI may go with no delayed check due and no
/// write before it counts as parked: no chain is left to look at it again.
/// Longer than the queue's longest delay (15 minutes) plus a resume's own
/// time, so a live chain is never taken for a parked one.
pub const PARKED_AFTER_SECS: i64 = 3600;
/// How many times one run is picked back up from a parked wait. Past it,
/// the stuck-run sweep's own wall cap ends the run.
pub const MAX_PARKED_REDRIVES: u32 = 3;
/// The run record attribute counting those pick-ups.
pub const REDRIVES_FIELD: &str = "ci_parked_redrives";
/// The note on the run when it is picked back up.
pub const REDRIVE_NOTE: &str = "The run's wait on its checks had stopped with nothing set to \
     look again; CoderHelm picked it back up and looks at the pull request now.";

/// What the parked-wait sweep reads of a run waiting on CI.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WaitState {
    /// When its next delayed check is due ([`DUE_FIELD`]), if one was armed.
    pub due: Option<i64>,
    /// Its last write (unix seconds).
    pub updated: i64,
    /// It waits for GitHub's limit to reset: that chain arms no check.
    pub waits_for_github: bool,
    /// Until when an invocation holds the run's writer slot.
    pub writer_until: Option<i64>,
    /// How many times it was picked back up already ([`REDRIVES_FIELD`]).
    pub redrives: u32,
}

/// Whether a run waiting on CI is parked at `now`: no delayed check due
/// within [`PARKED_AFTER_SECS`], no write in that long, no wait for GitHub,
/// nothing holding it, and picked back up fewer than
/// [`MAX_PARKED_REDRIVES`] times. A wait handed off before the hand-off
/// cleared its due check, or one whose queued check was lost, has nothing
/// left to look at it again; only a sweep reaches it.
pub fn parked(w: &WaitState, now: i64) -> bool {
    let quiet = |t: i64| t + PARKED_AFTER_SECS <= now;
    !w.waits_for_github
        && w.redrives < MAX_PARKED_REDRIVES
        && w.writer_until.is_none_or(|u| u <= now)
        && w.due.is_none_or(quiet)
        && quiet(w.updated)
}

/// The run's note when the wait stops.
pub fn stop_note(w: &Wait, now: i64) -> String {
    let mins = ((now - w.since).max(0) + STALL_SECS) / 60;
    format!(
        "CI made no progress for {mins} minutes on {}: {} stayed queued or running, and a \
         re-run didn't move it. CoderHelm stopped waiting and finished the run with the pull \
         request open; the ready decision holds it until those checks pass.",
        w.head.chars().take(7).collect::<String>(),
        if w.pending.is_empty() {
            "a check".to_string()
        } else {
            w.pending.join(", ")
        }
    )
}

/// The run's note when the stuck checks are re-run.
pub fn rerun_note(w: &Wait, started: usize) -> String {
    format!(
        "CI made no progress for {} minutes: {} stayed queued or running. CoderHelm {}.",
        STALL_SECS / 60,
        w.pending.join(", "),
        if started > 0 {
            format!("started {started} of their workflow runs again")
        } else {
            "couldn't start them again (not GitHub Actions runs, or GitHub refused)".to_string()
        }
    )
}

pub fn to_json(w: &Wait) -> String {
    serde_json::to_string(w).unwrap_or_default()
}

pub fn from_json(s: &str) -> Option<Wait> {
    serde_json::from_str(s).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn checks() -> Value {
        json!({"total_count": 3, "check_runs": [
            {"name": "Web App Lint", "status": "completed", "conclusion": "success"},
            {"name": "E2E on preview", "status": "queued",
             "details_url": "https://github.com/o/r/actions/runs/4512/job/77"},
            {"name": "outside/status", "status": "in_progress", "details_url": "https://ci.example/x"}
        ]})
    }

    #[test]
    fn the_pending_set_and_its_runs_are_read() {
        assert_eq!(
            pending_of(&checks()),
            vec![
                "E2E on preview (queued)".to_string(),
                "outside/status (in_progress)".to_string()
            ]
        );
        assert_eq!(pending_run_ids(&checks()), vec![4512]);
    }

    #[test]
    fn no_progress_reruns_once_then_stops() {
        let p = pending_of(&checks());
        let (w, s) = step(None, "abc", &p, 1000);
        assert_eq!(s, Step::Wait);
        let (w, s) = step(Some(&w), "abc", &p, 1000 + STALL_SECS - 1);
        assert_eq!(s, Step::Wait);
        let (w, s) = step(Some(&w), "abc", &p, 1000 + STALL_SECS);
        assert_eq!(s, Step::Rerun);
        assert!(w.reran);
        let (w2, s) = step(Some(&w), "abc", &p, 1000 + STALL_SECS + 60);
        assert_eq!(s, Step::Wait);
        let (_, s) = step(Some(&w2), "abc", &p, 1000 + 2 * STALL_SECS);
        assert_eq!(s, Step::Stop);
        assert!(stop_note(&w2, 1000 + 2 * STALL_SECS).contains("E2E on preview (queued)"));
    }

    #[test]
    fn any_change_is_progress() {
        let p = pending_of(&checks());
        let (w, _) = step(None, "abc", &p, 0);
        let (w2, s) = step(Some(&w), "abc", &p[..1], STALL_SECS * 3);
        assert_eq!(s, Step::Wait);
        assert_eq!(w2.since, STALL_SECS * 3);
        let (_, s) = step(Some(&w), "def", &p, STALL_SECS * 3);
        assert_eq!(s, Step::Wait);
    }

    /// A wait kept for an older head (its checks "in progress" for a day)
    /// never stands for the pull request's current head.
    #[test]
    fn the_wait_follows_the_head_and_drops_stale_pending() {
        let old = Wait {
            head: "old1234".into(),
            pending: vec!["E2E on preview (in_progress)".into()],
            since: 10,
            reran: false,
        };
        let now = for_head(Some(&old), "new5678", 500);
        assert_eq!(now.head, "new5678");
        assert!(now.pending.is_empty());
        assert_eq!(now.since, 500);
        // The same head, still nothing pending: kept as it was.
        assert_eq!(for_head(Some(&now), "new5678", 900), now);
        // The same head whose checks have ended: the pending list goes.
        let ended = Wait {
            head: "new5678".into(),
            ..old.clone()
        };
        assert!(for_head(Some(&ended), "new5678", 900).pending.is_empty());
    }

    /// A wait with no check due and no write for an hour is parked; a live
    /// chain, a wait for GitHub, a held run, or one picked up three times
    /// already is not.
    #[test]
    fn a_wait_with_nothing_due_is_parked() {
        let now = 100_000;
        let stale = now - PARKED_AFTER_SECS - 5;
        let parked_wait = WaitState {
            due: Some(stale),
            updated: stale,
            ..Default::default()
        };
        assert!(parked(&parked_wait, now));
        assert!(parked(
            &WaitState {
                due: None,
                ..parked_wait
            },
            now
        ));
        // A check due soon, or one due a few minutes ago: a live chain.
        for due in [now + 120, now - 300] {
            assert!(!parked(
                &WaitState {
                    due: Some(due),
                    ..parked_wait
                },
                now
            ));
        }
        // Written lately: still moving.
        assert!(!parked(
            &WaitState {
                updated: now - 60,
                ..parked_wait
            },
            now
        ));
        assert!(!parked(
            &WaitState {
                waits_for_github: true,
                ..parked_wait
            },
            now
        ));
        assert!(!parked(
            &WaitState {
                writer_until: Some(now + 30),
                ..parked_wait
            },
            now
        ));
        assert!(parked(
            &WaitState {
                writer_until: Some(now - 30),
                ..parked_wait
            },
            now
        ));
        assert!(!parked(
            &WaitState {
                redrives: MAX_PARKED_REDRIVES,
                ..parked_wait
            },
            now
        ));
    }

    #[test]
    fn one_chain_is_armed_at_a_time() {
        assert!(may_arm(None, 100));
        assert!(may_arm(Some(90), 100));
        assert!(may_arm(Some(110), 100));
        assert!(!may_arm(Some(220), 100));
    }
}
