//! Work CoderHelm started on its own pull request and hasn't landed, and
//! what a person's mention on that pull request does about it.
//!
//! A fix round writes to its own WIP branch (`run_round::wip_branch`) and
//! lands as one commit when its checks pass. When the checks hold it (or the
//! round runs out of time), the work waits on that branch. A person who then
//! mentions CoderHelm on the pull request wants that work finished: a bare
//! `@coderhelm`, or one that says "do it", "redo", "continue", "apply" or the
//! like. Before this module a bare mention was always a re-review, so the
//! held work was never landed and a fresh review was posted instead.
//!
//! The rules:
//!
//! - Outstanding work comes first. A bare mention on CoderHelm's own pull
//!   request, while a round's work is held or unfinished, resumes that
//!   round: the checks run again on the WIP with the current setup, and the
//!   work lands when they pass (or is fixed, then lands).
//! - A mention with words is read by a model, never by keywords: an ask to
//!   continue or to change the code runs a fix round (on the held WIP when
//!   there is one), a question gets an answer, and only an ask for a review
//!   gets a review.
//! - A re-review happens when the comment asks for one, or when no work is
//!   outstanding.
//! - The person is told what CoderHelm is doing ([`Outstanding::pickup_line`]),
//!   through whatever channel the requester allowed.

use crate::run_round::{Incomplete, Kind};

/// A round's work on CoderHelm's own pull request that hasn't landed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Outstanding {
    /// The branch holding the work.
    pub wip_branch: String,
    /// Where the round started (the pull request's head then).
    pub base_sha: String,
    /// When the round stopped (RFC 3339), empty when unknown.
    pub since: String,
    /// Why it didn't land, in a person's words; empty when unknown.
    pub reason: String,
    /// What the round was asked to do; empty when not kept.
    pub requests: String,
    /// What the round was for, when the record says.
    pub kind: Option<Kind>,
    /// The work is done and only held by the checks (false: the round ran
    /// out of time or hit an error part way).
    pub held: bool,
}

impl Outstanding {
    /// The unfinished round on the run record, when its work is on a WIP
    /// branch.
    pub fn from_incomplete(inc: &Incomplete) -> Option<Outstanding> {
        if inc.wip_branch.trim().is_empty() {
            return None;
        }
        Some(Outstanding {
            wip_branch: inc.wip_branch.clone(),
            base_sha: inc.base_sha.clone(),
            since: inc.at.clone(),
            reason: inc.reason.clone(),
            requests: inc.feedback.clone(),
            kind: inc.kind,
            held: inc.held,
        })
    }

    /// A WIP branch the run record doesn't name (a round held before the
    /// record kept held rounds), from GitHub's compare of the pull request's
    /// head with it (`head...wip`). Work it has that the pull request lacks
    /// is outstanding; the round's start is where the two branches meet.
    pub fn from_wip_compare(wip_branch: &str, compare: &serde_json::Value) -> Option<Outstanding> {
        let ahead = compare["ahead_by"].as_u64().unwrap_or(0);
        let files = compare["files"].as_array().map_or(0, |f| f.len());
        let base = compare["merge_base_commit"]["sha"].as_str().unwrap_or("");
        if ahead == 0 || files == 0 || base.is_empty() {
            return None;
        }
        let since = compare["commits"]
            .as_array()
            .and_then(|c| c.last())
            .and_then(|c| c["commit"]["committer"]["date"].as_str())
            .unwrap_or("")
            .to_string();
        Some(Outstanding {
            wip_branch: wip_branch.to_string(),
            base_sha: base.to_string(),
            since,
            held: true,
            ..Default::default()
        })
    }

    /// When the work stopped, as a person reads it ("2026-10-29 21:34
    /// UTC"), or None when unknown.
    pub fn when(&self) -> Option<String> {
        chrono::DateTime::parse_from_rfc3339(self.since.trim())
            .ok()
            .map(|t| {
                t.with_timezone(&chrono::Utc)
                    .format("%Y-%m-%d %H:%M UTC")
                    .to_string()
            })
    }

    /// What CoderHelm tells the person who mentioned it.
    pub fn pickup_line(&self) -> String {
        let from = self
            .when()
            .map(|w| format!(" from {w}"))
            .unwrap_or_default();
        if self.held {
            format!(
                "Picking up the held changes{from}: re-running the checks with the current \
                 setup, then landing them."
            )
        } else {
            format!(
                "Picking up the unfinished fix round{from}: finishing it, re-running the \
                 checks, then landing it."
            )
        }
    }

    /// The outstanding work, for the model that reads what a comment asks.
    pub fn note(&self) -> String {
        let from = self
            .when()
            .map(|w| format!(" since {w}"))
            .unwrap_or_default();
        let why = if self.reason.trim().is_empty() {
            String::new()
        } else {
            format!(" ({})", crate::truncate_str(self.reason.trim(), 300))
        };
        if self.held {
            format!(
                "CoderHelm finished a fix round on this pull request, but its checks held the \
                 changes from landing{why}; they wait on a work branch{from}, not landed."
            )
        } else {
            format!(
                "CoderHelm's last fix round on this pull request didn't finish{why}; its work \
                 waits on a work branch{from}, not landed."
            )
        }
    }
}

impl Outstanding {
    /// What the resumed round is asked: the outstanding work first (its
    /// work is already on the branch), then what it was asked, then the new
    /// comment. The scope check judges the held work against all of it.
    pub fn round_body(&self, comment: &str) -> String {
        let mut s = format!(
            "## Outstanding work to land first\n{}\nThe changes for these requests are already on \
             the branch. Read them, finish only what is still missing, and don't redo or revert \
             them. CoderHelm checks them again with the current setup and lands them.\n",
            self.note()
        );
        if !self.requests.trim().is_empty() {
            s.push_str(&format!(
                "\n## What that round was asked\n{}\n",
                self.requests.trim()
            ));
        }
        let words = comment.trim();
        if !words.is_empty() {
            s.push_str(&format!("\n## The new comment\n{words}\n"));
        }
        s
    }
}

/// What a held round was asked when the record doesn't keep it: the last
/// review a person submitted with a body, at or before the round stopped
/// (`since`, RFC 3339; any time when unknown), from GitHub's list of the
/// pull request's reviews (oldest first).
pub fn requests_from_reviews(reviews: &serde_json::Value, since: &str) -> String {
    let cutoff = chrono::DateTime::parse_from_rfc3339(since.trim()).ok();
    reviews
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or_default()
        .iter()
        .filter(|r| {
            let login = r["user"]["login"].as_str().unwrap_or("");
            r["user"]["type"].as_str() != Some("Bot") && !login.contains("coderhelm")
        })
        .filter(|r| !r["body"].as_str().unwrap_or("").trim().is_empty())
        .rfind(|r| match (cutoff, r["submitted_at"].as_str()) {
            (Some(c), Some(at)) => chrono::DateTime::parse_from_rfc3339(at).is_ok_and(|t| t <= c),
            _ => true,
        })
        .and_then(|r| r["body"].as_str())
        .unwrap_or("")
        .trim()
        .to_string()
}

/// What a person's mention on CoderHelm's own pull request does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MentionRoute {
    /// Resume the outstanding round: check its work again and land it.
    ResumeWork,
    /// A formal review of the head.
    Review,
    /// Words the model reads for what they ask (a fix round, an answer, a
    /// review), told about the outstanding work.
    Ask,
}

/// Pure. `bare`: the mention carries no words. `asks_review`: the words are
/// exactly a review command ("re-review", "review again"). `outstanding`:
/// a round's work waits unlanded.
pub fn mention_route(bare: bool, asks_review: bool, outstanding: bool) -> MentionRoute {
    if bare {
        if outstanding {
            MentionRoute::ResumeWork
        } else {
            MentionRoute::Review
        }
    } else if asks_review {
        MentionRoute::Review
    } else {
        MentionRoute::Ask
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_round::Round;

    #[test]
    fn a_bare_mention_resumes_outstanding_work_and_reviews_otherwise() {
        assert_eq!(mention_route(true, false, true), MentionRoute::ResumeWork);
        assert_eq!(mention_route(true, false, false), MentionRoute::Review);
        // An explicit review command is a review, work or not.
        assert_eq!(mention_route(false, true, true), MentionRoute::Review);
        assert_eq!(mention_route(false, true, false), MentionRoute::Review);
        // Words are read for meaning.
        assert_eq!(mention_route(false, false, true), MentionRoute::Ask);
        assert_eq!(mention_route(false, false, false), MentionRoute::Ask);
    }

    #[test]
    fn a_held_round_on_the_record_is_outstanding() {
        let now = chrono::Utc::now();
        let mut r = Round::start(Kind::Feedback, 0, "abc", "1. Fix the tie", false, now);
        r.wip_branch = "coderhelm-wip/coderhelm/fix-1".into();
        let inc = Incomplete::held(
            &r,
            "Full test suite FAILED (exit 2)",
            "2026-10-29T21:34:45Z",
        );
        let o = Outstanding::from_incomplete(&inc).unwrap();
        assert!(o.held);
        assert_eq!(o.wip_branch, "coderhelm-wip/coderhelm/fix-1");
        assert_eq!(o.base_sha, "abc");
        assert_eq!(o.requests, "1. Fix the tie");
        assert_eq!(
            o.pickup_line(),
            "Picking up the held changes from 2026-10-29 21:34 UTC: re-running the checks with \
             the current setup, then landing them."
        );
        assert!(o.note().contains("held the changes from landing"));
        assert!(inc.status_line().contains("held, not landed"));
        // A record without a WIP branch has nothing to land.
        let swept = Incomplete {
            kind: Some(Kind::ReviewFix),
            ..Default::default()
        };
        assert!(Outstanding::from_incomplete(&swept).is_none());
    }

    #[test]
    fn a_held_flag_survives_the_record() {
        let now = chrono::Utc::now();
        let r = Round::start(Kind::Feedback, 0, "abc", "x", false, now);
        let inc = Incomplete::held(&r, "held", "2026-10-29T21:34:45Z");
        let back = Incomplete::from_attr(&inc.to_attr()).unwrap();
        assert!(back.held);
        let old = Incomplete::from_attr(&Incomplete::of(&r, "late", "t").to_attr()).unwrap();
        assert!(!old.held);
    }

    #[test]
    fn a_wip_branch_ahead_of_the_pr_is_outstanding_work() {
        let compare = serde_json::json!({
            "ahead_by": 3,
            "behind_by": 0,
            "merge_base_commit": {"sha": "3cb5799"},
            "files": [{"filename": "dags/utils/schedule_common.py"}],
            "commits": [
                {"commit": {"committer": {"date": "2026-10-29T21:31:20Z"}}},
                {"commit": {"committer": {"date": "2026-10-29T21:34:40Z"}}}
            ]
        });
        let o = Outstanding::from_wip_compare("coderhelm-wip/coderhelm/fix-1", &compare).unwrap();
        assert!(o.held);
        assert_eq!(o.base_sha, "3cb5799");
        assert_eq!(o.when().as_deref(), Some("2026-10-29 21:34 UTC"));
        // Nothing the pull request lacks: nothing outstanding.
        let even =
            serde_json::json!({"ahead_by": 0, "files": [], "merge_base_commit": {"sha": "x"}});
        assert!(Outstanding::from_wip_compare("w", &even).is_none());
    }

    #[test]
    fn the_resumed_round_is_asked_the_held_requests_and_the_new_comment() {
        let o = Outstanding {
            held: true,
            requests: "1. Break ties by id\n2. Warn once per run".into(),
            ..Default::default()
        };
        let b = o.round_body("@coderhelm");
        assert!(b.starts_with("## Outstanding work to land first\n"));
        assert!(b.contains("## What that round was asked\n1. Break ties by id"));
        assert!(b.contains("don't redo or revert them"));
        assert!(b.ends_with("## The new comment\n@coderhelm\n"));
    }

    #[test]
    fn the_held_rounds_requests_come_from_the_last_review_before_it_stopped() {
        let reviews = serde_json::json!([
            {"user": {"login": "owner", "type": "User"}, "body": "first pass", "submitted_at": "2026-10-29T18:00:00Z"},
            {"user": {"login": "coderhelm[bot]", "type": "Bot"}, "body": "verdict", "submitted_at": "2026-10-29T21:16:00Z"},
            {"user": {"login": "lead", "type": "User"}, "body": "1. a\n2. b\n3. c\n4. d\n5. e", "submitted_at": "2026-10-29T21:26:30Z"},
            {"user": {"login": "lead", "type": "User"}, "body": "", "submitted_at": "2026-10-29T21:27:00Z"},
            {"user": {"login": "owner", "type": "User"}, "body": "later", "submitted_at": "2026-10-30T09:00:00Z"}
        ]);
        assert_eq!(
            requests_from_reviews(&reviews, "2026-10-29T21:34:45Z"),
            "1. a\n2. b\n3. c\n4. d\n5. e"
        );
        assert_eq!(requests_from_reviews(&reviews, ""), "later");
        assert_eq!(requests_from_reviews(&serde_json::json!([]), ""), "");
    }

    #[test]
    fn an_unfinished_round_says_it_will_finish_it() {
        let o = Outstanding {
            since: "not a time".into(),
            ..Default::default()
        };
        assert_eq!(
            o.pickup_line(),
            "Picking up the unfinished fix round: finishing it, re-running the checks, then \
             landing it."
        );
    }
}
