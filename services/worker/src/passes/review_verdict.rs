//! The one place that decides what CoderHelm posts as its formal review on a
//! PR, and the only code that posts or dismisses one.
//!
//! Every path goes through [`decide`]: a review that finished, one that
//! didn't, a reply whose conclusion may disagree with the standing verdict,
//! and the stuck-review sweep (which runs a review). The invariants it keeps:
//!
//! - A review that didn't finish never posts a formal verdict.
//! - A reply that concludes nothing blocks never stands next to CoderHelm's
//!   own "changes requested": the formal review is re-run in the same job.
//! - A finished review's formal verdict is APPROVE or REQUEST_CHANGES, never
//!   a bare COMMENT ([`binary`]). Only GitHub's rule against verdicts on the
//!   bot's own PR makes it a COMMENT there; answers to a person's question
//!   are comments, not formal reviews.
//! - A finished review that holds nothing never leaves an older "changes
//!   requested" of CoderHelm's standing: its APPROVE replaces it.
//!
//! A test fails if any other module calls the GitHub review endpoints.

use crate::clients::github::{GitHubClient, InlineComment};
use crate::models::ReviewMessage;
use crate::passes::review_agent::VerdictChange;
use tracing::{info, warn};

/// CoderHelm's formal review standing on a PR (its newest review that set a
/// GitHub review state).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandingReview {
    /// "APPROVE" | "REQUEST_CHANGES"
    pub verdict: String,
    /// A review that didn't finish (an old error review).
    pub failed: bool,
    pub head_sha: String,
}

impl StandingReview {
    /// The standing review holds the PR (a real or an error request for
    /// changes).
    pub fn blocks(&self) -> bool {
        self.failed || self.verdict == "REQUEST_CHANGES"
    }
}

/// What happened that may change the formal review.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Input {
    /// A review that finished. `verdict` comes from
    /// [`super::review_agent::decide_verdict`].
    Review {
        verdict: &'static str,
        /// Something changed since the last review (verdict, findings, fixed
        /// threads, labels, a person's new commit).
        has_news: bool,
        /// A person asked for it (a reply, a re-request, the sweep, a
        /// reconciliation).
        explicit: bool,
        /// Another review of this same commit was posted while this one ran.
        already_answered: bool,
    },
    /// A review that didn't finish (model API, unreadable answer, GitHub).
    Failed,
    /// A reply CoderHelm is about to post, and what it concludes.
    Reply { change: VerdictChange },
}

/// What CoderHelm's PR looks like for the decision.
#[derive(Debug, Clone, Default)]
pub struct Context {
    pub standing: Option<StandingReview>,
    /// GitHub forbids APPROVE / REQUEST_CHANGES on the bot's own PR.
    pub self_authored: bool,
}

/// What to do about the formal review.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Post a formal review with `event`; first dismiss CoderHelm's own
    /// standing "changes requested" when `dismiss_stale`.
    Post {
        event: &'static str,
        dismiss_stale: bool,
    },
    /// Run the full formal review of the head now, in this job.
    Review,
    /// Post nothing formal.
    Nothing,
}

/// The formal event for a verdict: `APPROVE`, or `REQUEST_CHANGES` for
/// anything else. A formal review never posts a bare `COMMENT` as its
/// verdict (only answers to a person's question are comments, and those are
/// not formal reviews).
pub fn binary(verdict: &str) -> &'static str {
    if verdict == "APPROVE" {
        "APPROVE"
    } else {
        "REQUEST_CHANGES"
    }
}

/// The decision. Pure: every rule about what CoderHelm posts formally is here.
pub fn decide(input: Input, ctx: &Context) -> Decision {
    let standing_blocks = ctx.standing.as_ref().is_some_and(StandingReview::blocks);
    match input {
        // No verdict from a review that didn't finish; the previous one stays.
        Input::Failed => Decision::Nothing,
        Input::Reply { change } => {
            if ctx.self_authored {
                return Decision::Nothing;
            }
            let Some(st) = ctx.standing.as_ref() else {
                return Decision::Nothing;
            };
            let disagrees = match change {
                VerdictChange::Clear => st.blocks(),
                VerdictChange::Block => st.verdict == "APPROVE" && !st.failed,
                // An error review stands on the PR: replace it.
                VerdictChange::None => st.failed,
            };
            if disagrees {
                Decision::Review
            } else {
                Decision::Nothing
            }
        }
        Input::Review {
            verdict,
            has_news,
            explicit,
            already_answered,
        } => {
            // A formal verdict is APPROVE or REQUEST_CHANGES, never a bare
            // comment: anything that isn't an approval holds the merge.
            let verdict = binary(verdict);
            // A verdict that no longer blocks must replace a standing block,
            // news or not.
            let clears_a_block = standing_blocks && verdict == "APPROVE";
            if already_answered || !(explicit || has_news || clears_a_block) {
                return Decision::Nothing;
            }
            if ctx.self_authored {
                // GitHub refuses either verdict on the bot's own PR; the body
                // still carries it.
                return Decision::Post {
                    event: "COMMENT",
                    dismiss_stale: false,
                };
            }
            // APPROVE replaces a standing request for changes on GitHub by
            // itself.
            Decision::Post {
                event: verdict,
                dismiss_stale: false,
            }
        }
    }
}

/// Posts the formal review `decision` asked for: one batched review with
/// inline comments, falling back to body-only when GitHub rejects an anchor.
/// The only caller of the GitHub review endpoints.
pub async fn post(
    github: &GitHubClient,
    msg: &ReviewMessage,
    head: &str,
    event: &'static str,
    dismiss_stale: bool,
    body: &str,
    inline: &[InlineComment],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let posted = github
        .create_pr_review_inline(
            &msg.repo_owner,
            &msg.repo_name,
            msg.pr_number,
            head,
            event,
            body,
            inline,
        )
        .await;
    if let Err(e) = posted {
        warn!(pr = msg.pr_number, error = %e, "Inline review post failed — retrying body-only");
        let body_only = super::review_agent::fold_inline_into_body(body, inline);
        github
            .create_pr_review(
                &msg.repo_owner,
                &msg.repo_name,
                msg.pr_number,
                event,
                &body_only,
            )
            .await?;
    }
    if dismiss_stale {
        dismiss_stale_change_requests(github, msg, head).await;
    }
    Ok(())
}

/// Dismisses CoderHelm's own "changes requested" reviews on the PR, after a
/// finished review of `head` found nothing that holds the merge. Best-effort:
/// a repo can restrict who dismisses reviews, so a failure is logged.
async fn dismiss_stale_change_requests(github: &GitHubClient, msg: &ReviewMessage, head: &str) {
    let reviews = match github
        .list_pr_reviews(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            warn!(pr = msg.pr_number, error = %e, "Could not list reviews to dismiss a stale request for changes");
            return;
        }
    };
    let short = &head[..head.len().min(7)];
    for id in own_change_requests(&reviews) {
        let note =
            format!("Superseded by CoderHelm's review of `{short}`: nothing left holds the merge.");
        match github
            .dismiss_review(&msg.repo_owner, &msg.repo_name, msg.pr_number, id, &note)
            .await
        {
            Ok(()) => {
                info!(
                    pr = msg.pr_number,
                    review_id = id,
                    "Dismissed a stale request for changes"
                )
            }
            Err(e) => {
                warn!(pr = msg.pr_number, review_id = id, error = %e, "Could not dismiss a stale request for changes")
            }
        }
    }
}

/// Ids of CoderHelm's own reviews still in the "changes requested" state.
pub(crate) fn own_change_requests(reviews: &serde_json::Value) -> Vec<u64> {
    reviews
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or(&[])
        .iter()
        .filter(|r| r["state"].as_str() == Some("CHANGES_REQUESTED"))
        .filter(|r| {
            r["user"]["type"].as_str() == Some("Bot")
                && r["user"]["login"]
                    .as_str()
                    .is_some_and(|l| l.contains("coderhelm"))
        })
        .filter_map(|r| r["id"].as_u64())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn standing(verdict: &str, failed: bool) -> Option<StandingReview> {
        Some(StandingReview {
            verdict: verdict.into(),
            failed,
            head_sha: "h1".into(),
        })
    }

    fn review(verdict: &'static str, has_news: bool, explicit: bool) -> Input {
        Input::Review {
            verdict,
            has_news,
            explicit,
            already_answered: false,
        }
    }

    /// Invariant (a): a review that didn't finish never posts a verdict,
    /// whatever stands on the PR.
    #[test]
    fn a_failed_review_never_posts() {
        for st in [
            None,
            standing("APPROVE", false),
            standing("REQUEST_CHANGES", false),
            standing("REQUEST_CHANGES", true),
        ] {
            for self_authored in [false, true] {
                let ctx = Context {
                    standing: st.clone(),
                    self_authored,
                };
                assert_eq!(decide(Input::Failed, &ctx), Decision::Nothing);
            }
        }
    }

    /// Invariant (b): a reply that finds nothing blocking while CoderHelm's
    /// own request for changes (real or an error) stands re-runs the formal
    /// review; a blocker over an approval does too.
    #[test]
    fn a_reply_that_disagrees_with_the_standing_verdict_reruns_the_review() {
        let clear = Input::Reply {
            change: VerdictChange::Clear,
        };
        let ctx = |s| Context {
            standing: s,
            self_authored: false,
        };
        assert_eq!(
            decide(clear, &ctx(standing("REQUEST_CHANGES", true))),
            Decision::Review
        );
        assert_eq!(
            decide(clear, &ctx(standing("REQUEST_CHANGES", false))),
            Decision::Review
        );
        assert_eq!(
            decide(clear, &ctx(standing("APPROVE", false))),
            Decision::Nothing
        );
        assert_eq!(decide(clear, &ctx(None)), Decision::Nothing);
        let block = Input::Reply {
            change: VerdictChange::Block,
        };
        assert_eq!(
            decide(block, &ctx(standing("APPROVE", false))),
            Decision::Review
        );
        assert_eq!(
            decide(block, &ctx(standing("REQUEST_CHANGES", false))),
            Decision::Nothing
        );
        let none = Input::Reply {
            change: VerdictChange::None,
        };
        assert_eq!(
            decide(none, &ctx(standing("REQUEST_CHANGES", true))),
            Decision::Review
        );
        assert_eq!(
            decide(none, &ctx(standing("REQUEST_CHANGES", false))),
            Decision::Nothing
        );
        // Its own PR: GitHub keeps no review state for the bot there.
        let own = Context {
            standing: standing("REQUEST_CHANGES", true),
            self_authored: true,
        };
        assert_eq!(decide(clear, &own), Decision::Nothing);
    }

    #[test]
    fn a_finished_review_that_clears_a_block_always_posts() {
        let blocked = Context {
            standing: standing("REQUEST_CHANGES", true),
            self_authored: false,
        };
        // No news, not asked for: still posted, because a block stands.
        assert_eq!(
            decide(review("APPROVE", false, false), &blocked),
            Decision::Post {
                event: "APPROVE",
                dismiss_stale: false
            }
        );
        // Anything but an approval doesn't clear it.
        assert_eq!(
            decide(review("COMMENT", false, false), &blocked),
            Decision::Nothing
        );
        // Same verdict, nothing new, nothing asked: quiet.
        let approved = Context {
            standing: standing("APPROVE", false),
            self_authored: false,
        };
        assert_eq!(
            decide(review("APPROVE", false, false), &approved),
            Decision::Nothing
        );
        assert_eq!(
            decide(review("REQUEST_CHANGES", true, false), &approved),
            Decision::Post {
                event: "REQUEST_CHANGES",
                dismiss_stale: false
            }
        );
        // The first of two reviews of one commit speaks for both.
        let dup = Input::Review {
            verdict: "APPROVE",
            has_news: true,
            explicit: true,
            already_answered: true,
        };
        assert_eq!(decide(dup, &blocked), Decision::Nothing);
        // Its own PR: always a COMMENT.
        let own = Context {
            standing: None,
            self_authored: true,
        };
        assert_eq!(
            decide(review("REQUEST_CHANGES", true, false), &own),
            Decision::Post {
                event: "COMMENT",
                dismiss_stale: false
            }
        );
    }

    /// A formal review is APPROVE or REQUEST_CHANGES on a person's PR,
    /// never a bare COMMENT, whatever verdict string reaches it.
    #[test]
    fn a_formal_review_is_never_a_bare_comment() {
        for standing_review in [
            None,
            standing("APPROVE", false),
            standing("REQUEST_CHANGES", false),
            standing("REQUEST_CHANGES", true),
        ] {
            let ctx = Context {
                standing: standing_review,
                self_authored: false,
            };
            for verdict in ["APPROVE", "REQUEST_CHANGES", "COMMENT", ""] {
                for (news, asked) in [(true, false), (false, true), (true, true), (false, false)] {
                    if let Decision::Post { event, .. } = decide(review(verdict, news, asked), &ctx)
                    {
                        assert!(
                            event == "APPROVE" || event == "REQUEST_CHANGES",
                            "{verdict} posted as {event}"
                        );
                        assert_eq!(event == "APPROVE", verdict == "APPROVE", "{verdict}");
                    }
                }
            }
        }
        assert_eq!(binary("COMMENT"), "REQUEST_CHANGES");
        assert_eq!(binary("APPROVE"), "APPROVE");
    }

    #[test]
    fn only_coderhelms_own_change_requests_are_dismissed() {
        let reviews = serde_json::json!([
            {"id": 1, "state": "CHANGES_REQUESTED", "user": {"login": "coderhelm[bot]", "type": "Bot"}},
            {"id": 2, "state": "APPROVED", "user": {"login": "coderhelm[bot]", "type": "Bot"}},
            {"id": 3, "state": "CHANGES_REQUESTED", "user": {"login": "dev", "type": "User"}},
            {"id": 4, "state": "CHANGES_REQUESTED", "user": {"login": "other[bot]", "type": "Bot"}},
            {"id": 5, "state": "DISMISSED", "user": {"login": "coderhelm[bot]", "type": "Bot"}}
        ]);
        assert_eq!(own_change_requests(&reviews), vec![1]);
    }

    /// Nothing but this module posts or dismisses a formal review: every
    /// other source file in the worker is scanned for the GitHub review
    /// calls.
    #[test]
    fn no_other_module_posts_a_formal_review() {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut offenders = vec![];
        let mut stack = vec![src.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                let rel = path
                    .strip_prefix(&src)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                // The client defines the calls; this module is their one user.
                if rel == "clients/github.rs" || rel == "passes/review_verdict.rs" {
                    continue;
                }
                let text = std::fs::read_to_string(&path).unwrap();
                for call in [
                    ".create_pr_review(",
                    ".create_pr_review_inline(",
                    ".dismiss_review(",
                ] {
                    if text.contains(call) {
                        offenders.push(format!("{rel}: {call}"));
                    }
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "formal reviews posted outside review_verdict: {offenders:?}"
        );
    }
}
