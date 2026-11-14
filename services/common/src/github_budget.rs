//! One GitHub budget per installation, spent in order of who is waiting.
//!
//! GitHub gives an app installation a fixed number of REST requests an hour
//! (5,000 at the least). An install once used all of it: sweeps on timers
//! (a look at every finished run's pull request every 15 minutes, a read of
//! every pull request a security finding records, a description rebuilt
//! after every push) spent it beside the runs, and when it was gone a fix
//! round that had worked for two hours was left unfinished and its review
//! findings were deferred to a person. Nothing said where the requests went.
//!
//! The rules:
//!
//! - **Every request is counted** by who made it ([`caller`]): a run's own
//!   work, a fix round, a review, each named sweep. The worker's GitHub
//!   client counts where every request leaves (`send_tracked`), and the
//!   counts go on the analytics table by installation and hour
//!   ([`counts_key`]) and in one `github_budget` log line per message.
//! - **Work a person waits on comes first** ([`Priority::Foreground`]): a
//!   run's steps, its fix rounds, a review. It may spend the whole budget.
//! - **Sweeps come last** ([`Priority::Background`]). A sweep spends only
//!   while at least [`RUN_RESERVE_PCT`] percent of the hour's budget is left
//!   (the rest is the runs'), and never more than its own [`hourly_cap`] an
//!   hour. The caps add up to less than the background share of the smallest
//!   budget, so every sweep together can't reach the runs' share.
//! - **A spent limit sends nothing**: once GitHub said the hour's budget is
//!   gone, no caller sends another request until the reset ([`decide`]); the
//!   request comes back as the limit it would have met, and the caller waits
//!   (`common::github_wait`).
//! - **A conditional request that comes back unchanged costs nothing**
//!   (GitHub doesn't count a 304), so it isn't counted here either.

/// Percent of the hourly budget background work never spends: it stops
/// while less than this is left.
pub const RUN_RESERVE_PCT: u64 = 50;

/// The smallest hourly budget GitHub gives an installation.
pub const SMALLEST_LIMIT: u64 = 5_000;

/// Who a request is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// A person is waiting on it: a run's steps, a fix round, a review.
    Foreground,
    /// A sweep on a timer, or work nobody is waiting on.
    Background,
}

/// The labels requests are counted under.
pub mod caller {
    /// A ticket run's own steps (plan, implement, the pull request).
    pub const RUN: &str = "run";
    /// A round on an open pull request: CI fix, review fix, feedback.
    pub const FIX_ROUND: &str = "fix_round";
    /// A pull request review.
    pub const REVIEW: &str = "review";
    /// The ready decision of a pull request.
    pub const READY: &str = "ready";
    /// Merge, release steps, the post-merge check, restacking.
    pub const MERGE: &str = "merge";
    /// End-to-end results and actions.
    pub const TESTS: &str = "tests";
    /// Onboarding, setup, skills, plugins a person started.
    pub const SETUP: &str = "setup";
    /// A security webhook event.
    pub const SECURITY_EVENT: &str = "security_event";
    /// The plan and brain chats' tools.
    pub const CHAT: &str = "chat";
    /// The description of a pull request rebuilt after CoderHelm's push.
    pub const DESCRIPTION: &str = "description";
    /// Anything not labelled (a spawned task).
    pub const OTHER: &str = "other";

    /// Reviews stuck without a verdict.
    pub const SWEEP_REVIEW: &str = "sweep:review";
    /// Finished runs whose pull request is still a draft.
    pub const SWEEP_DRAFTS: &str = "sweep:drafts";
    /// Finished runs' pull requests through merge and deploy.
    pub const SWEEP_PR_WATCH: &str = "sweep:pr_watch";
    /// Deploy counts.
    pub const SWEEP_DEPLOYS: &str = "sweep:deploys";
    /// Reminders for reviews nobody answered.
    pub const SWEEP_REVIEW_REMINDER: &str = "sweep:review_reminder";
    /// Merged and closed pull requests' outcomes.
    pub const SWEEP_PR_OUTCOMES: &str = "sweep:pr_outcomes";
    /// Skill repositories.
    pub const SWEEP_SKILLS: &str = "sweep:skills";
    /// Stale pull requests.
    pub const SWEEP_STALE_PRS: &str = "sweep:stale_prs";
    /// Agent files gone out of date.
    pub const SWEEP_AGENT_FILES: &str = "sweep:agent_files";
    /// Security findings: sources, the reconcile of their pull requests.
    pub const SWEEP_SECURITY: &str = "sweep:security";
    /// Plugin sources.
    pub const SWEEP_PLUGINS: &str = "sweep:plugins";
    /// Knowledge sources.
    pub const SWEEP_KNOWLEDGE: &str = "sweep:knowledge";
    /// Automatic fixes of log findings.
    pub const SWEEP_LOG_AUTOFIX: &str = "sweep:log_autofix";
    /// The code graph index.
    pub const SWEEP_GRAPH: &str = "sweep:graph";
    /// Lessons and outcomes learned after a pull request closed.
    pub const SWEEP_LEARNING: &str = "sweep:learning";
    /// The weekly summary.
    pub const SWEEP_WEEKLY: &str = "sweep:weekly";
    /// Quarantined tests: releases, followed fixes and removals.
    pub const SWEEP_QUARANTINE: &str = "sweep:quarantine";

    /// Every background caller with its hourly cap of counted requests per
    /// installation (see [`super::hourly_cap`]).
    pub const BACKGROUND: &[(&str, u64)] = &[
        (SWEEP_REVIEW, 120),
        (SWEEP_DRAFTS, 120),
        (SWEEP_PR_WATCH, 240),
        (SWEEP_DEPLOYS, 300),
        (SWEEP_REVIEW_REMINDER, 120),
        (SWEEP_PR_OUTCOMES, 120),
        (SWEEP_SKILLS, 60),
        (SWEEP_STALE_PRS, 200),
        (SWEEP_AGENT_FILES, 60),
        (SWEEP_SECURITY, 300),
        (SWEEP_PLUGINS, 60),
        (SWEEP_KNOWLEDGE, 200),
        (SWEEP_LOG_AUTOFIX, 60),
        (SWEEP_GRAPH, 250),
        (SWEEP_LEARNING, 100),
        (SWEEP_WEEKLY, 100),
        (SWEEP_QUARANTINE, 90),
    ];
}

/// Whether `caller`'s requests are for someone waiting. An unknown label is
/// foreground: work is never held because nobody labelled it.
pub fn priority(caller: &str) -> Priority {
    if caller::BACKGROUND.iter().any(|(c, _)| *c == caller) {
        Priority::Background
    } else {
        Priority::Foreground
    }
}

/// The most counted requests a background caller makes for one installation
/// in one hour. None for foreground work, which has no cap of its own.
pub fn hourly_cap(caller: &str) -> Option<u64> {
    caller::BACKGROUND
        .iter()
        .find(|(c, _)| *c == caller)
        .map(|(_, cap)| *cap)
}

/// GitHub's hourly REST budget for one installation, as its last response
/// said: requests left, of how many, and when it refills (epoch seconds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    pub remaining: u64,
    pub limit: u64,
    pub reset: u64,
}

impl Budget {
    /// Requests background work may still spend before it reaches the runs'
    /// share. 0 once the reserve is touched.
    pub fn background_left(&self) -> u64 {
        self.remaining
            .saturating_sub(self.limit * RUN_RESERVE_PCT / 100)
    }

    /// Whether this reading is over: the hour it describes has reset, and
    /// the next response tells the new budget.
    pub fn is_past(&self, now: u64) -> bool {
        self.reset > 0 && self.reset <= now
    }
}

/// Why a request isn't sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hold {
    /// GitHub said the hour's budget is gone: nothing is sent until the
    /// reset, whoever asks.
    LimitSpent,
    /// What is left of the hour is the runs' share.
    ReservedForRuns,
    /// The caller made its hour's requests for this installation.
    HourlyCap,
}

impl Hold {
    pub fn as_str(self) -> &'static str {
        match self {
            Hold::LimitSpent => "GitHub's hourly limit is spent",
            Hold::ReservedForRuns => "what is left of GitHub's hourly budget is kept for runs",
            Hold::HourlyCap => "it made its requests for this hour",
        }
    }
}

/// Whether a request may be sent now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Spend {
    Go,
    /// Not sent: why, and how many seconds until it may be.
    Hold {
        why: Hold,
        wait_secs: u64,
    },
}

/// The start of the hour after `now` (epoch seconds).
fn next_hour(now: u64) -> u64 {
    (now / 3600 + 1) * 3600
}

/// The one decision every request takes before it is sent.
///
/// `limited_until`: until when GitHub said the installation's hourly budget
/// is gone (epoch seconds), when it did. `spent_this_hour`: counted requests
/// `caller` already made for the installation in the hour of `now`.
pub fn decide(
    caller: &str,
    budget: Option<Budget>,
    limited_until: Option<u64>,
    spent_this_hour: u64,
    now: u64,
) -> Spend {
    if let Some(until) = limited_until.filter(|u| *u > now) {
        return Spend::Hold {
            why: Hold::LimitSpent,
            wait_secs: until - now,
        };
    }
    if priority(caller) == Priority::Foreground {
        return Spend::Go;
    }
    if let Some(b) = budget.filter(|b| !b.is_past(now)) {
        if b.background_left() == 0 {
            return Spend::Hold {
                why: Hold::ReservedForRuns,
                wait_secs: b.reset.saturating_sub(now).max(60),
            };
        }
    }
    if hourly_cap(caller).is_some_and(|cap| spent_this_hour >= cap) {
        return Spend::Hold {
            why: Hold::HourlyCap,
            wait_secs: next_hour(now) - now,
        };
    }
    Spend::Go
}

/// How many more counted requests a background caller may make for an
/// installation right now: what is left of its hourly cap, and never into
/// the runs' share. A sweep sizes its batch with this. `u64::MAX` for
/// foreground work.
pub fn allowance(
    caller: &str,
    budget: Option<Budget>,
    limited_until: Option<u64>,
    spent_this_hour: u64,
    now: u64,
) -> u64 {
    match decide(caller, budget, limited_until, spent_this_hour, now) {
        Spend::Hold { .. } => 0,
        Spend::Go => {
            let Some(cap) = hourly_cap(caller) else {
                return u64::MAX;
            };
            let of_cap = cap.saturating_sub(spent_this_hour);
            match budget.filter(|b| !b.is_past(now)) {
                Some(b) => of_cap.min(b.background_left()),
                None => of_cap,
            }
        }
    }
}

/// The one line a sweep logs when it skips an installation's cycle.
pub fn skip_line(caller: &str, why: Hold, wait_secs: u64) -> String {
    format!(
        "{caller} skipped this cycle for the installation: {}; it looks again in about {} min",
        why.as_str(),
        wait_secs.div_ceil(60).max(1)
    )
}

/// The analytics table's partition for an installation's GitHub budget.
pub fn partition(installation_id: u64) -> String {
    format!("GITHUB#{installation_id}")
}

/// The sort key of an hour's request counts (`now`: epoch seconds).
pub fn counts_key(now: u64) -> String {
    let t = chrono::DateTime::from_timestamp(now as i64, 0).unwrap_or_default();
    format!("HOUR#{}", t.format("%Y-%m-%dT%H"))
}

/// The sort key of the installation's last known budget.
pub const STATE_KEY: &str = "STATE";

/// How long an hour's counts are kept (seconds).
pub const COUNTS_TTL_SECS: u64 = 14 * 24 * 3600;

/// The attribute an hour's record counts `caller`'s requests under.
pub fn count_attr(caller: &str) -> String {
    format!(
        "req_{}",
        caller
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect::<String>()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_794_000_000;

    fn budget(remaining: u64) -> Option<Budget> {
        Some(Budget {
            remaining,
            limit: 5_000,
            reset: NOW + 1_800,
        })
    }

    #[test]
    fn work_a_person_waits_on_spends_the_whole_budget() {
        for c in [
            caller::RUN,
            caller::FIX_ROUND,
            caller::REVIEW,
            caller::READY,
            caller::DESCRIPTION,
            caller::OTHER,
            "a label nobody listed",
        ] {
            assert_eq!(priority(c), Priority::Foreground, "{c}");
            assert_eq!(decide(c, budget(1), None, 9_999, NOW), Spend::Go, "{c}");
            assert_eq!(hourly_cap(c), None);
            assert_eq!(allowance(c, budget(1), None, 0, NOW), u64::MAX);
        }
    }

    #[test]
    fn a_sweep_never_spends_the_share_kept_for_runs() {
        for (c, _) in caller::BACKGROUND {
            assert_eq!(priority(c), Priority::Background);
            // Half the budget left: one more request would be the runs'.
            assert_eq!(
                decide(c, budget(2_500), None, 0, NOW),
                Spend::Hold {
                    why: Hold::ReservedForRuns,
                    wait_secs: 1_800
                },
                "{c}"
            );
            assert_eq!(decide(c, budget(2_501), None, 0, NOW), Spend::Go, "{c}");
            assert_eq!(allowance(c, budget(2_500), None, 0, NOW), 0);
            // A batch is sized to stop at the reserve.
            assert_eq!(allowance(c, budget(2_510), None, 0, NOW), 10);
        }
        // A reading from before the reset says nothing about this hour.
        let old = Some(Budget {
            remaining: 3,
            limit: 5_000,
            reset: NOW - 10,
        });
        assert_eq!(decide(caller::SWEEP_PR_WATCH, old, None, 0, NOW), Spend::Go);
        // No reading yet: the first response tells it.
        assert_eq!(
            decide(caller::SWEEP_PR_WATCH, None, None, 0, NOW),
            Spend::Go
        );
    }

    #[test]
    fn each_sweep_has_an_hourly_cap_and_all_of_them_fit_the_background_share() {
        let cap = hourly_cap(caller::SWEEP_PR_WATCH).unwrap();
        assert_eq!(cap, 240);
        assert_eq!(
            decide(caller::SWEEP_PR_WATCH, budget(4_900), None, cap - 1, NOW),
            Spend::Go
        );
        let held = decide(caller::SWEEP_PR_WATCH, budget(4_900), None, cap, NOW);
        assert!(
            matches!(held, Spend::Hold { why: Hold::HourlyCap, wait_secs } if wait_secs <= 3_600),
            "{held:?}"
        );
        assert_eq!(
            allowance(caller::SWEEP_PR_WATCH, budget(4_900), None, cap - 7, NOW),
            7
        );
        // Every sweep at its cap, on the smallest budget GitHub gives, still
        // leaves the runs their share.
        let all: u64 = caller::BACKGROUND.iter().map(|(_, c)| c).sum();
        assert!(
            all <= SMALLEST_LIMIT * (100 - RUN_RESERVE_PCT) / 100,
            "{all}"
        );
        // No label is listed twice.
        let mut names: Vec<&str> = caller::BACKGROUND.iter().map(|(c, _)| *c).collect();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), caller::BACKGROUND.len());
    }

    #[test]
    fn a_spent_limit_sends_nothing_for_anyone_until_the_reset() {
        for c in [caller::RUN, caller::FIX_ROUND, caller::SWEEP_SECURITY] {
            assert_eq!(
                decide(c, budget(0), Some(NOW + 731), 0, NOW),
                Spend::Hold {
                    why: Hold::LimitSpent,
                    wait_secs: 731
                },
                "{c}"
            );
            // Past the reset it is over.
            assert_eq!(decide(c, None, Some(NOW - 1), 0, NOW), Spend::Go, "{c}");
        }
    }

    #[test]
    fn a_skip_is_one_plain_line() {
        let line = skip_line(caller::SWEEP_PR_WATCH, Hold::ReservedForRuns, 1_790);
        assert_eq!(
            line,
            "sweep:pr_watch skipped this cycle for the installation: what is left of GitHub's \
             hourly budget is kept for runs; it looks again in about 30 min"
        );
    }

    #[test]
    fn counts_are_kept_by_installation_and_hour() {
        assert_eq!(partition(7), "GITHUB#7");
        // 2026-11-06 21:05:00 UTC.
        assert_eq!(counts_key(1_793_999_100), "HOUR#2026-11-06T21");
        assert_eq!(count_attr(caller::SWEEP_PR_WATCH), "req_sweep_pr_watch");
        assert_eq!(count_attr(caller::RUN), "req_run");
    }
}
