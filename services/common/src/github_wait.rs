//! A run that GitHub can't serve for a while waits, then goes on by itself.
//!
//! A ticket run once hit the installation's hourly GitHub limit in a fix
//! round: fifteen `batch_write` and fourteen `edit_file` calls came back
//! "403 ... API rate limit exceeded", the branch's diff couldn't be read
//! either, and the run ended "the coder wrote no file ... That is CoderHelm's
//! miss". The coder had written; GitHub had refused, for an hour.
//!
//! The rules:
//!
//! - GitHub refusing for a while (its rate limit, a 5xx, a dropped
//!   connection) is never the coder's fault and never ends a run. The tool
//!   call gets [`tool_result`], the pass ends, and the run is sent again with
//!   an SQS delay until GitHub's reset ([`hop`] chains delays past the 900
//!   seconds one message holds). It goes on from its checkpoint, plan and
//!   branch, as the same run.
//! - The run page says [`waiting_text`] while it waits.
//! - The wait isn't work: it adds no step, no worked time and no pass to the
//!   run's progress guard or its ceiling.
//! - [`MAX_WAITS`] bounds it: a GitHub that refuses through that many waits
//!   ends the run with [`gave_up_reason`], which says it was GitHub.

use serde::{Deserialize, Serialize};

/// The longest delay one SQS message carries.
pub const MAX_HOP_SECS: u64 = 900;

/// Waits one run takes for GitHub before it stops (about eight hours of
/// hourly limits): far above a real limit, which resets within the hour.
pub const MAX_WAITS: u32 = 8;

/// Seconds added after GitHub's reset time, so the run starts after it.
pub const RESET_SLACK_SECS: u64 = 5;

/// Why GitHub can't be used right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Cause {
    /// The installation's rate limit (hourly, or a secondary limit).
    #[default]
    RateLimit,
    /// GitHub failed for a moment: a 5xx, or no answer at all.
    Outage,
}

/// A run waiting for GitHub, carried on the run's next message.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitHubWait {
    /// When the run may go on (epoch seconds). 0 when it isn't waiting.
    #[serde(default)]
    pub until: u64,
    /// Waits this run took so far.
    #[serde(default)]
    pub waits: u32,
    /// The pass the run was in when it paused: going on with it again is not
    /// a new pass for the progress guard.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pass: String,
    #[serde(default)]
    pub cause: Cause,
}

impl GitHubWait {
    /// Nothing to carry: the run never waited.
    pub fn is_idle(&self) -> bool {
        self.until == 0 && self.waits == 0
    }
}

/// [`GitHubWait::is_idle`] for a boxed field (serde passes the field).
#[allow(clippy::borrowed_box)]
pub fn is_idle_boxed(w: &Box<GitHubWait>) -> bool {
    w.is_idle()
}

/// The delay of the next hop of a wait that ends at `until`, at `now`: None
/// once it is (about) over and the run should go on.
pub fn hop(until: u64, now: u64) -> Option<u64> {
    let left = until.saturating_sub(now);
    (left > RESET_SLACK_SECS).then(|| left.min(MAX_HOP_SECS))
}

/// When a wait of `wait_secs` from `now` ends, with the slack.
pub fn until(now: u64, wait_secs: u64) -> u64 {
    now + wait_secs.max(1) + RESET_SLACK_SECS
}

/// `HH:MM UTC` of an epoch time.
pub fn clock(epoch: u64) -> String {
    chrono::DateTime::from_timestamp(epoch as i64, 0)
        .map(|t| t.format("%H:%M UTC").to_string())
        .unwrap_or_else(|| "soon".to_string())
}

/// What the run page says while the run waits.
pub fn waiting_text(cause: Cause, until: u64) -> String {
    match cause {
        Cause::RateLimit => format!(
            "Waiting for GitHub's hourly limit to reset at {}; continues by itself",
            clock(until)
        ),
        Cause::Outage => format!(
            "Waiting for GitHub to answer again; tries at {} and continues by itself",
            clock(until)
        ),
    }
}

/// The tool result the model gets for a call GitHub refused this way, in
/// place of the raw error it would retry.
pub fn tool_result(cause: Cause, until: u64) -> String {
    let what = match cause {
        Cause::RateLimit => "GitHub's rate limit is reached",
        Cause::Outage => "GitHub isn't answering right now",
    };
    format!(
        "{what}; the run will pause and continue at {}. Do not retry this or any other call: \
         nothing is wrong with your change, and the work so far is kept.",
        clock(until)
    )
}

/// What a tool call the loop didn't run (after GitHub refused an earlier one
/// in the same turn) returns.
pub fn not_run(cause: Cause, until: u64) -> String {
    format!("Not run: {}", tool_result(cause, until))
}

/// Why a run stops when GitHub kept refusing through every wait. It names
/// GitHub, never the coder.
pub fn gave_up_reason(cause: Cause, waits: u32) -> String {
    let what = match cause {
        Cause::RateLimit => "GitHub's rate limit for this installation",
        Cause::Outage => "GitHub failing to answer",
    };
    format!(
        "Stopped after waiting {waits} times for {what}: GitHub refused the run's requests each \
         time. Nothing was wrong with the work, which is kept on the branch. Retry goes on from \
         it once GitHub is back."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wait_past_one_delay_is_sent_on_in_hops() {
        let now = 1_000_000;
        let until = until(now, 3_000);
        assert_eq!(until, now + 3_005);
        let mut t = now;
        let mut hops = Vec::new();
        while let Some(h) = hop(until, t) {
            assert!(h <= MAX_HOP_SECS);
            hops.push(h);
            t += h;
        }
        assert_eq!(hops, vec![900, 900, 900, 305]);
        assert!(hop(until, t).is_none());
        // A wait that is already over goes on at once.
        assert!(hop(now, now + 1).is_none());
        assert!(hop(now + 3, now).is_none(), "within the slack: go on");
    }

    #[test]
    fn the_waiting_text_names_the_reset_in_plain_words() {
        // 2026-11-06 21:05:00 UTC.
        let reset = 1_793_999_100;
        assert_eq!(clock(reset), "21:05 UTC");
        assert_eq!(
            waiting_text(Cause::RateLimit, reset),
            "Waiting for GitHub's hourly limit to reset at 21:05 UTC; continues by itself"
        );
        assert!(waiting_text(Cause::Outage, reset).contains("continues by itself"));
    }

    #[test]
    fn the_tool_result_tells_the_model_not_to_retry() {
        let reset = 1_793_999_100;
        let r = tool_result(Cause::RateLimit, reset);
        assert!(r.starts_with(
            "GitHub's rate limit is reached; the run will pause and continue at 21:05 UTC"
        ));
        assert!(r.contains("Do not retry"));
        assert!(!r.contains("403"));
        assert!(not_run(Cause::RateLimit, reset).starts_with("Not run: GitHub's rate limit"));
    }

    #[test]
    fn giving_up_names_github_never_the_coder() {
        let r = gave_up_reason(Cause::RateLimit, MAX_WAITS);
        assert!(r.contains("GitHub's rate limit"));
        assert!(!r.contains("CoderHelm's miss"));
        assert!(!r.to_lowercase().contains("coder wrote no file"));
    }

    #[test]
    fn an_idle_wait_is_not_carried() {
        assert!(GitHubWait::default().is_idle());
        let w = GitHubWait {
            until: 5,
            waits: 1,
            pass: "implement".into(),
            cause: Cause::RateLimit,
        };
        let back: GitHubWait = serde_json::from_str(&serde_json::to_string(&w).unwrap()).unwrap();
        assert_eq!(back, w);
        let old: GitHubWait = serde_json::from_str("{}").unwrap();
        assert!(old.is_idle());
    }
}
