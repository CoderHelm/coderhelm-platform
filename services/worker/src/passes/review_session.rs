//! The PR's "CoderHelm review" check run as a review job sees it: the gateway
//! queues it when it schedules a push review (`common::review_check`), the
//! job takes it over in progress when it starts, its Details link the
//! review's session page on this install's dashboard (`common::review_session`
//! keeps the record), and it is completed when the job ends, whatever the
//! ending; a job that hands its review on hands the check on with it.
//!
//! A check run needs the app's checks:write permission, which an
//! installation may not have granted. GitHub then refuses (403) and the
//! review goes on exactly as without it: the refusal is logged once per
//! installation at info, and nothing else changes.

use crate::clients::github::GitHubClient;
use common::review_check::{self as rc, CheckCalls, Close};

pub(crate) use common::review_check::Target as CheckTarget;

/// The session page of job `job` on the dashboard at `app`.
pub(crate) fn session_url(app: &str, job: &str) -> String {
    format!("{app}/reviewer/session?id={job}")
}

#[async_trait::async_trait]
impl CheckCalls for GitHubClient {
    async fn create(
        &self,
        owner: &str,
        repo: &str,
        body: &serde_json::Value,
    ) -> Result<u64, String> {
        self.create_check_run(owner, repo, body)
            .await
            .map_err(|e| e.to_string())
    }
    async fn update(
        &self,
        owner: &str,
        repo: &str,
        id: u64,
        body: &serde_json::Value,
    ) -> Result<(), String> {
        self.update_check_run(owner, repo, id, body)
            .await
            .map_err(|e| e.to_string())
    }
    async fn list(&self, owner: &str, repo: &str, head: &str) -> Result<serde_json::Value, String> {
        self.list_review_checks(owner, repo, head)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Pure: the one-line title of a check in progress: what the review reads.
pub(crate) fn reading_title(files: &[String], head: &str) -> String {
    let short = rc::short(head);
    match files {
        [] => format!("Reading the change at {short}"),
        [one] => format!("Reading 1 changed file at {short}: {one}"),
        [first, rest @ ..] => format!(
            "Reading {} changed files at {short}: {first} and {} more",
            rest.len() + 1,
            rest.len()
        ),
    }
}

/// Why a job ended without a new review, for its check.
#[derive(Debug, Clone, Copy)]
pub(crate) enum NotReviewed<'a> {
    /// A newer commit was pushed (before or during the review).
    Superseded { newer: &'a str },
    /// CoderHelm's own push while its run is still fixing the PR.
    OwnPushDuringRun,
    /// A description edit this commit was already reviewed with.
    DescriptionSeen { head: &'a str },
    /// The commit already has CoderHelm's verdict.
    AlreadyReviewed { head: &'a str, verdict: &'a str },
    /// Another review of the same commit holds it.
    HeldByAnotherReview { head: &'a str },
    /// The team has no model provider set up.
    NoModelProvider,
    /// Nothing of the PR's own change moved since CoderHelm approved.
    UnchangedSinceApproval { approved: &'a str },
    /// Nothing of the PR's own change moved since CoderHelm's last review.
    UnchangedSinceReview { reviewed: &'a str, verdict: &'a str },
    /// A hunks-only re-review settled its findings; the full review
    /// follows (the check goes to it when it can).
    FullReviewFollows { head: &'a str },
    /// A person's comment was answered and the commit has no verdict to show
    /// again (with one, the check goes back to it, see `close_restoring`).
    Answered { head: &'a str },
}

/// Pure: the conclusion and title of a check whose job ended without a new
/// review. A verdict that stands keeps its conclusion (an approval passes);
/// every other ending is neutral, never a failure.
pub(crate) fn not_reviewed(why: NotReviewed<'_>) -> (&'static str, String) {
    use common::review_check::{standing_conclusion, verdict_words};
    match why {
        NotReviewed::Superseded { newer } => (
            "neutral",
            format!("Replaced by the review of {}", rc::short(newer)),
        ),
        NotReviewed::OwnPushDuringRun => (
            "neutral",
            "CoderHelm's own push during its run: reviewed when the run finishes".into(),
        ),
        NotReviewed::DescriptionSeen { head } => (
            "neutral",
            format!(
                "Already reviewed with this description at {}",
                rc::short(head)
            ),
        ),
        NotReviewed::AlreadyReviewed { head, verdict } => (
            standing_conclusion(verdict),
            format!(
                "Already reviewed {}: {} stands",
                rc::short(head),
                verdict_words(verdict)
            ),
        ),
        NotReviewed::HeldByAnotherReview { head } => (
            "neutral",
            format!(
                "Held by another review of {}: its verdict is the one that counts",
                rc::short(head)
            ),
        ),
        NotReviewed::NoModelProvider => (
            "neutral",
            "Review didn't finish: no model provider is set up".into(),
        ),
        NotReviewed::UnchangedSinceApproval { approved } => (
            "success",
            format!(
                "No change to review since the approval at {}; the approval stands",
                rc::short(approved)
            ),
        ),
        NotReviewed::UnchangedSinceReview { reviewed, verdict } => (
            standing_conclusion(verdict),
            format!(
                "No change to review since the last review at {}; earlier verdict stands",
                rc::short(reviewed)
            ),
        ),
        NotReviewed::FullReviewFollows { head } => (
            "neutral",
            format!(
                "Changes since the last review settle its findings; a full review of {} follows",
                rc::short(head)
            ),
        ),
        NotReviewed::Answered { head } => (
            "neutral",
            format!(
                "Answered your reply on {}; no verdict on this commit yet",
                rc::short(head)
            ),
        ),
    }
}

/// How the review ended, for its check.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CheckEnd<'a> {
    /// A verdict. `review_url` is the review it posted (empty when nothing
    /// new was posted and the standing review was brought up to date).
    Reviewed {
        verdict: &'a str,
        findings: usize,
        review_url: &'a str,
    },
    /// Finished without a verdict on purpose: `reason` says why.
    Skipped { reason: &'a str },
    /// Finished without a new review on purpose, with its own conclusion
    /// and title (a standing verdict carried, a newer commit's review).
    Settled {
        conclusion: &'static str,
        title: &'a str,
        reason: &'a str,
    },
    /// Didn't finish; sent again (when the check couldn't be handed to the
    /// retry, it is completed with this).
    Retrying { reason: &'a str },
    /// Didn't finish, and every retry failed.
    Failed { reason: &'a str },
}

fn plural(n: usize, one: &str, many: &str) -> String {
    if n == 1 {
        format!("1 {one}")
    } else {
        format!("{n} {many}")
    }
}

/// Pure: the first line of an error, short enough for a title.
fn short_reason(reason: &str) -> String {
    let line = reason.lines().next().unwrap_or("").trim();
    let line = line.trim_end_matches('.');
    if line.chars().count() > 120 {
        format!("{}...", line.chars().take(117).collect::<String>())
    } else {
        line.to_string()
    }
}

/// Pure: the conclusion and output of a check. Findings make it `neutral`
/// (they are in the review, the check never blocks a merge by itself); a
/// clean review is `success`; a standing verdict keeps its conclusion; any
/// ending without a review is `neutral` (a service error says nothing about
/// the code, so it is never a red `failure`). `session_url` is where the
/// summary sends the reader for every step.
pub(crate) fn check_close(end: CheckEnd<'_>, pr_url: &str, session_url: &str) -> Close {
    let session = format!("[See the session]({session_url}) for every step.");
    let (conclusion, title, body) = match end {
        CheckEnd::Reviewed {
            verdict,
            findings,
            review_url,
        } => {
            let requested = verdict == "REQUEST_CHANGES";
            let title = match (requested, findings) {
                (true, 0) => "Changes requested".to_string(),
                (true, n) => format!("Changes requested: {}", plural(n, "finding", "findings")),
                (false, 0) => "Approved, no findings".to_string(),
                (false, n) => format!("Approved with {}", plural(n, "note", "notes")),
            };
            let link = if review_url.is_empty() {
                format!(
                    "Nothing new to post, so the earlier review on [the pull request]({pr_url}) \
                     stands, brought up to date for this commit."
                )
            } else {
                format!("[Read the review]({review_url}) on the pull request.")
            };
            let conclusion = if requested || findings > 0 {
                "neutral"
            } else {
                "success"
            };
            (conclusion, title, link)
        }
        CheckEnd::Skipped { reason } => ("neutral", "Not reviewed".to_string(), reason.to_string()),
        CheckEnd::Settled {
            conclusion,
            title,
            reason,
        } => (conclusion, title.to_string(), reason.to_string()),
        CheckEnd::Retrying { reason } => (
            "neutral",
            format!("Review didn't finish: {}", short_reason(reason)),
            format!(
                "The review hit a service error and runs again shortly. Error: {}",
                common::truncate_str(reason, 300)
            ),
        ),
        CheckEnd::Failed { reason } => (
            "neutral",
            format!("Review didn't finish: {}", short_reason(reason)),
            format!(
                "Every try of this review hit a service error, so there is no verdict for this \
                 commit; any earlier verdict stands. Error: {}",
                common::truncate_str(reason, 300)
            ),
        ),
    };
    Close {
        conclusion,
        title,
        summary: format!("{body}\n\n{session}"),
        details_url: None,
    }
}

/// Pure: how a check handed to a retry reads while the retry waits.
pub(crate) fn retry_wait(reason: &str, due: i64) -> (String, String) {
    let at = chrono::DateTime::from_timestamp(due, 0)
        .map(|t| t.format("%H:%M UTC").to_string())
        .unwrap_or_default();
    (
        format!(
            "Review didn't finish: {}; trying again at {at}",
            short_reason(reason)
        ),
        format!(
            "The review hit a service error and runs again at {at}, in this check. Error: {}",
            common::truncate_str(reason, 300)
        ),
    )
}

/// The check's title and summary while the review waits for GitHub's rate
/// limit to reset (`text`: `common::github_wait::waiting_text`).
pub(crate) fn limit_wait(text: &str) -> (String, String) {
    (
        format!("Review paused: {text}"),
        format!(
            "{text}. GitHub's hourly request limit for this installation is used up; nothing \
             failed on the review's side, and it runs in this check once the limit resets."
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::review_check::fake::FakeChecks;

    #[test]
    fn conclusions_follow_the_review() {
        let c = |end| check_close(end, "pr", "session").conclusion;
        let reviewed = |verdict, findings, review_url| CheckEnd::Reviewed {
            verdict,
            findings,
            review_url,
        };
        assert_eq!(c(reviewed("APPROVE", 0, "r")), "success");
        assert_eq!(c(reviewed("APPROVE", 2, "r")), "neutral");
        assert_eq!(c(reviewed("REQUEST_CHANGES", 3, "r")), "neutral");
        assert_eq!(c(reviewed("REQUEST_CHANGES", 0, "r")), "neutral");
        assert_eq!(c(CheckEnd::Skipped { reason: "x" }), "neutral");
        assert_eq!(c(CheckEnd::Retrying { reason: "x" }), "neutral");
        assert_eq!(c(CheckEnd::Failed { reason: "x" }), "neutral");
        assert_eq!(
            c(CheckEnd::Settled {
                conclusion: "success",
                title: "t",
                reason: "r"
            }),
            "success"
        );
    }

    #[test]
    fn an_unfinished_review_says_why_in_its_title() {
        let failed = check_close(
            CheckEnd::Failed {
                reason: "the review ran past its 780s budget\nmore detail",
            },
            "pr",
            "s",
        );
        assert_eq!(
            failed.title,
            "Review didn't finish: the review ran past its 780s budget"
        );
        let (title, summary) = retry_wait("model API 529 overloaded", 1_793_896_153);
        assert_eq!(
            title,
            "Review didn't finish: model API 529 overloaded; trying again at 16:29 UTC"
        );
        assert!(summary.contains("in this check"));
        assert!(short_reason(&"x".repeat(300)).ends_with("..."));
    }

    #[test]
    fn the_summary_links_the_review_and_the_session() {
        let done = check_close(
            CheckEnd::Reviewed {
                verdict: "REQUEST_CHANGES",
                findings: 1,
                review_url: "https://github.com/acme/web/pull/7#pullrequestreview-1",
            },
            "https://github.com/acme/web/pull/7",
            "https://app.example/reviewer/session?id=j",
        );
        assert_eq!(done.title, "Changes requested: 1 finding");
        assert!(done
            .summary
            .contains("[Read the review](https://github.com/acme/web/pull/7#pullrequestreview-1)"));
        assert!(done
            .summary
            .contains("(https://app.example/reviewer/session?id=j)"));
        // Nothing new posted: the PR's standing review is where to look.
        let quiet = check_close(
            CheckEnd::Reviewed {
                verdict: "APPROVE",
                findings: 0,
                review_url: "",
            },
            "https://github.com/acme/web/pull/7",
            "s",
        );
        assert_eq!(quiet.title, "Approved, no findings");
        assert!(quiet
            .summary
            .contains("(https://github.com/acme/web/pull/7)"));
        let notes = check_close(
            CheckEnd::Reviewed {
                verdict: "APPROVE",
                findings: 2,
                review_url: "r",
            },
            "p",
            "s",
        );
        assert_eq!(notes.title, "Approved with 2 notes");
    }

    #[test]
    fn the_title_says_what_it_reads() {
        let head = "9fceb02d0ae598e95dc970b74767f19372d61af8";
        assert_eq!(reading_title(&[], head), "Reading the change at 9fceb02");
        assert_eq!(
            reading_title(&["src/a.ts".into()], head),
            "Reading 1 changed file at 9fceb02: src/a.ts"
        );
        assert_eq!(
            reading_title(&["src/a.ts".into(), "src/b.ts".into(), "c".into()], head),
            "Reading 3 changed files at 9fceb02: src/a.ts and 2 more"
        );
        assert_eq!(
            rc::started_title(rc::Work::Review, head),
            "Reviewing 9fceb02"
        );
        assert_eq!(
            session_url("https://app.example", "j"),
            "https://app.example/reviewer/session?id=j"
        );
    }

    const HEAD: &str = "9fceb02d0ae598e95dc970b74767f19372d61af8";
    const NEWER: &str = "b75b65a0054151e44f8015042f1607e67c31dbdc";
    const EARLIER: &str = "56458a8bcd1d9b24463b378233880cfd05195b39";

    #[test]
    fn every_ending_without_a_review_says_so() {
        use NotReviewed::*;
        let cases: Vec<(NotReviewed, &str, &str)> = vec![
            (
                Superseded { newer: NEWER },
                "neutral",
                "Replaced by the review of b75b65a",
            ),
            (
                OwnPushDuringRun,
                "neutral",
                "CoderHelm's own push during its run: reviewed when the run finishes",
            ),
            (
                DescriptionSeen { head: HEAD },
                "neutral",
                "Already reviewed with this description at 9fceb02",
            ),
            (
                AlreadyReviewed {
                    head: HEAD,
                    verdict: "APPROVE",
                },
                "success",
                "Already reviewed 9fceb02: approved stands",
            ),
            (
                AlreadyReviewed {
                    head: HEAD,
                    verdict: "REQUEST_CHANGES",
                },
                "neutral",
                "Already reviewed 9fceb02: changes requested stands",
            ),
            (
                HeldByAnotherReview { head: HEAD },
                "neutral",
                "Held by another review of 9fceb02: its verdict is the one that counts",
            ),
            (
                NoModelProvider,
                "neutral",
                "Review didn't finish: no model provider is set up",
            ),
            (
                UnchangedSinceApproval { approved: EARLIER },
                "success",
                "No change to review since the approval at 56458a8; the approval stands",
            ),
            (
                UnchangedSinceReview {
                    reviewed: EARLIER,
                    verdict: "REQUEST_CHANGES",
                },
                "neutral",
                "No change to review since the last review at 56458a8; earlier verdict stands",
            ),
            (
                UnchangedSinceReview {
                    reviewed: EARLIER,
                    verdict: "APPROVE",
                },
                "success",
                "No change to review since the last review at 56458a8; earlier verdict stands",
            ),
            (
                FullReviewFollows { head: HEAD },
                "neutral",
                "Changes since the last review settle its findings; a full review of 9fceb02 \
                 follows",
            ),
        ];
        for (why, conclusion, title) in cases {
            let (c, t) = not_reviewed(why);
            assert_eq!((c, t.as_str()), (conclusion, title), "{why:?}");
            // Never a red check for an ending without a review.
            assert_ne!(c, "failure");
        }
    }

    fn target() -> CheckTarget<'static> {
        CheckTarget {
            installation: 1,
            owner: "acme",
            repo: "web",
            pr: 7,
            head: HEAD,
        }
    }

    fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }

    /// The gateway queues the check for the push job (`head#…`), as
    /// `routes::github_webhook` does.
    async fn queued(gh: &FakeChecks) -> u64 {
        let key = format!("head#{HEAD}");
        match rc::queue(gh, &target(), &key, now() + 180, "pr-page").await {
            rc::Queued::Created(id) => id,
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn the_job_reuses_the_queued_check_through_to_its_verdict() {
        let gh = FakeChecks::default();
        let id = queued(&gh).await;
        let key = format!("head#{HEAD}");
        // Job start: in progress at once, the same check.
        assert_eq!(
            rc::start(
                &gh,
                &target(),
                &key,
                &rc::started_title(rc::Work::Review, HEAD),
                "pr-page"
            )
            .await,
            Some(id)
        );
        assert_eq!(gh.run(id)["status"], "in_progress");
        assert_eq!(gh.run(id)["output"]["title"], "Reviewing 9fceb02");
        // Verdict mode: what it reads, linking the session.
        rc::progress(
            &gh,
            &target(),
            id,
            &key,
            &reading_title(&["src/a.ts".into()], HEAD),
            "session",
        )
        .await;
        assert_eq!(gh.run(id)["details_url"], "session");
        let close = check_close(
            CheckEnd::Reviewed {
                verdict: "APPROVE",
                findings: 0,
                review_url: "r",
            },
            "pr",
            "session",
        );
        rc::close(&gh, &target(), id, &close).await;
        assert_eq!(gh.run(id)["conclusion"], "success");
        assert_eq!(gh.count(), 1, "one check on the commit");
    }

    #[tokio::test]
    async fn each_skip_completes_the_queued_check() {
        for why in [
            NotReviewed::Superseded { newer: NEWER },
            NotReviewed::OwnPushDuringRun,
            NotReviewed::AlreadyReviewed {
                head: HEAD,
                verdict: "APPROVE",
            },
            NotReviewed::UnchangedSinceApproval { approved: EARLIER },
            NotReviewed::HeldByAnotherReview { head: HEAD },
        ] {
            let gh = FakeChecks::default();
            let id = queued(&gh).await;
            let key = format!("head#{HEAD}");
            rc::start(
                &gh,
                &target(),
                &key,
                &rc::started_title(rc::Work::Review, HEAD),
                "pr-page",
            )
            .await;
            let (conclusion, title) = not_reviewed(why);
            let close = check_close(
                CheckEnd::Settled {
                    conclusion,
                    title: &title,
                    reason: "why",
                },
                "pr",
                "pr-page",
            );
            rc::close(&gh, &target(), id, &close).await;
            let run = gh.run(id);
            assert_eq!(run["status"], "completed", "{why:?}");
            assert_eq!(run["conclusion"], conclusion, "{why:?}");
            assert_eq!(run["output"]["title"], title.as_str(), "{why:?}");
            assert_eq!(gh.count(), 1);
        }
    }

    #[tokio::test]
    async fn a_retry_and_a_full_review_carry_the_check_on() {
        let gh = FakeChecks::default();
        let id = queued(&gh).await;
        let key = format!("head#{HEAD}");
        rc::start(
            &gh,
            &target(),
            &key,
            &rc::started_title(rc::Work::Review, HEAD),
            "pr-page",
        )
        .await;
        // A service error: the check waits for the retry, queued.
        let retry_key = format!("retry#1#head#{HEAD}");
        let due = now() + 60;
        let (title, summary) = retry_wait("model API 529 overloaded", due);
        assert!(rc::hand_off(&gh, &target(), id, &retry_key, due, &title, &summary).await);
        assert_eq!(gh.run(id)["status"], "queued");
        assert!(gh.run(id)["output"]["title"]
            .as_str()
            .unwrap()
            .starts_with("Review didn't finish: model API 529 overloaded; trying again at "));
        // The retry takes it; its hunks-only review hands it to the full one.
        assert_eq!(
            rc::start(&gh, &target(), &retry_key, "Reviewing", "pr-page").await,
            Some(id)
        );
        let full_key = format!("full#{HEAD}");
        assert!(rc::hand_off(&gh, &target(), id, &full_key, now(), "Full", "s").await);
        assert_eq!(
            rc::start(&gh, &target(), &full_key, "Reviewing", "pr-page").await,
            Some(id)
        );
        // Every try failed: neutral, saying why.
        let close = check_close(
            CheckEnd::Failed {
                reason: "the review ran past its 780s budget",
            },
            "pr",
            "s",
        );
        rc::close(&gh, &target(), id, &close).await;
        assert_eq!(gh.run(id)["conclusion"], "neutral");
        assert_eq!(
            gh.run(id)["output"]["title"],
            "Review didn't finish: the review ran past its 780s budget"
        );
        assert_eq!(gh.count(), 1, "one check on the commit throughout");
    }

    #[tokio::test]
    async fn an_ask_and_a_push_on_one_commit_share_one_check() {
        let gh = FakeChecks::default();
        let id = queued(&gh).await;
        // An ask's job doesn't take the push's check at its start...
        assert_eq!(
            rc::start(&gh, &target(), "desc#x", "Reviewing", "pr-page").await,
            None
        );
        // ...but when it runs the formal review, it takes over the open
        // check instead of adding a second one.
        assert_eq!(
            rc::take_over_or_open(&gh, &target(), "desc#x", "Reading", "s").await,
            Some(id)
        );
        assert_eq!(gh.count(), 1);
    }

    #[tokio::test]
    async fn a_refused_check_degrades_silently() {
        let gh = FakeChecks::refusing();
        let key = format!("head#{HEAD}");
        assert_eq!(
            rc::queue(&gh, &target(), &key, now(), "u").await,
            rc::Queued::Nothing
        );
        assert_eq!(rc::start(&gh, &target(), &key, "R", "u").await, None);
        assert_eq!(
            rc::take_over_or_open(&gh, &target(), &key, "R", "u").await,
            None
        );
        // Only listings were tried, and nothing was created.
        assert!(gh.calls.lock().unwrap().iter().all(|c| c == "list"));
        assert_eq!(gh.count(), 0);
    }

    #[tokio::test]
    async fn no_head_no_check() {
        let gh = FakeChecks::default();
        let t = CheckTarget {
            installation: 1,
            owner: "acme",
            repo: "web",
            pr: 7,
            head: "",
        };
        assert_eq!(rc::take_over_or_open(&gh, &t, "k", "R", "u").await, None);
        assert_eq!(rc::start(&gh, &t, "k", "R", "u").await, None);
        assert!(gh.calls.lock().unwrap().is_empty());
    }
}
