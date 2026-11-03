//! The PR's "CoderHelm review" check run: opened in progress when a formal
//! review starts, its Details link the review's session page on this
//! install's dashboard (`common::review_session` keeps the record), and
//! completed when the job ends, whatever the ending.
//!
//! A check run needs the app's checks:write permission, which an
//! installation may not have granted. GitHub then refuses (403) and the
//! review goes on exactly as without it: the refusal is logged once per
//! installation at info, and nothing else changes.

use std::collections::HashSet;
use std::sync::Mutex;

use crate::clients::github::{is_not_permitted, GitHubClient};
use tracing::{info, warn};

type Error = Box<dyn std::error::Error + Send + Sync>;

/// The check's name on the PR.
pub(crate) const CHECK_NAME: &str = "CoderHelm review";

/// The session page of job `job` on the dashboard at `app`.
pub(crate) fn session_url(app: &str, job: &str) -> String {
    format!("{app}/reviewer/session?id={job}")
}

/// The two check-run calls, so the silent degrade can be tested without
/// GitHub.
#[async_trait::async_trait]
pub(crate) trait CheckApi: Sync {
    async fn create(&self, owner: &str, repo: &str, body: &serde_json::Value)
        -> Result<u64, Error>;
    async fn update(
        &self,
        owner: &str,
        repo: &str,
        id: u64,
        body: &serde_json::Value,
    ) -> Result<(), Error>;
}

#[async_trait::async_trait]
impl CheckApi for GitHubClient {
    async fn create(
        &self,
        owner: &str,
        repo: &str,
        body: &serde_json::Value,
    ) -> Result<u64, Error> {
        self.create_check_run(owner, repo, body).await
    }
    async fn update(
        &self,
        owner: &str,
        repo: &str,
        id: u64,
        body: &serde_json::Value,
    ) -> Result<(), Error> {
        self.update_check_run(owner, repo, id, body).await
    }
}

/// Installations that refused a check run in this process: their refusal
/// was logged once.
static REFUSED: Mutex<Option<HashSet<u64>>> = Mutex::new(None);

/// True the first time `installation` refuses in this process.
fn first_refusal(installation: u64) -> bool {
    let mut set = REFUSED.lock().unwrap_or_else(|p| p.into_inner());
    set.get_or_insert_with(HashSet::new).insert(installation)
}

/// Log a failed check-run call. A refusal for want of permission is the
/// expected state of an installation that hasn't granted checks:write: one
/// info line, then silence.
fn log_failure(installation: u64, pr: u64, what: &str, e: &Error) {
    if is_not_permitted(&**e) {
        if first_refusal(installation) {
            info!(
                installation,
                pr,
                "GitHub refused the CoderHelm review check (the app has no checks:write \
                 permission on this installation); reviews go on without it"
            );
        }
        return;
    }
    warn!(installation, pr, what, error = %e, "CoderHelm review check: GitHub call failed");
}

/// Pure: the one-line title of a check in progress: what the review reads.
pub(crate) fn reading_title(files: &[String], head: &str) -> String {
    let short = &head[..head.len().min(7)];
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

/// Where a review is: the check's place on the PR.
pub(crate) struct CheckTarget<'a> {
    pub installation: u64,
    pub owner: &'a str,
    pub repo: &'a str,
    pub pr: u64,
    pub head: &'a str,
}

/// Open the check in progress on `t.head`, its Details link `details_url`.
/// `None` when GitHub refused or failed: the review goes on without it.
pub(crate) async fn open_check<C: CheckApi + ?Sized>(
    api: &C,
    t: &CheckTarget<'_>,
    job: &str,
    title: &str,
    details_url: &str,
) -> Option<u64> {
    if t.head.is_empty() {
        return None;
    }
    let body = serde_json::json!({
        "name": CHECK_NAME,
        "head_sha": t.head,
        "status": "in_progress",
        "external_id": job,
        "details_url": details_url,
        "started_at": chrono::Utc::now().to_rfc3339(),
        "output": {
            "title": common::truncate_str(title, 250),
            "summary": format!(
                "CoderHelm is reviewing this pull request. [Follow the review]({details_url}) \
                 step by step."
            ),
        },
    });
    match api.create(t.owner, t.repo, &body).await {
        Ok(id) => Some(id),
        Err(e) => {
            log_failure(t.installation, t.pr, "open", &e);
            None
        }
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
    /// Didn't finish; sent again (the retry opens its own check).
    Retrying { reason: &'a str },
    /// Didn't finish, and every retry failed.
    Failed { reason: &'a str },
}

/// A completed check: its conclusion and output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckClose {
    pub conclusion: &'static str,
    pub title: String,
    pub summary: String,
}

fn plural(n: usize, one: &str, many: &str) -> String {
    if n == 1 {
        format!("1 {one}")
    } else {
        format!("{n} {many}")
    }
}

/// Pure: the conclusion and output of a check. Findings make it `neutral`
/// (they are in the review, the check never blocks a merge by itself); a
/// clean review is `success`; a review skipped on purpose is `skipped`; one
/// that didn't finish is `cancelled` (a service error says nothing about the
/// code, so it is never a red `failure`).
pub(crate) fn check_close(end: CheckEnd<'_>, pr_url: &str, session_url: &str) -> CheckClose {
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
        CheckEnd::Skipped { reason } => ("skipped", "Not reviewed".to_string(), reason.to_string()),
        CheckEnd::Retrying { reason } => (
            "cancelled",
            "Didn't finish, trying again".to_string(),
            format!(
                "The review hit a service error and runs again shortly; the next try has its \
                 own check. Error: {}",
                common::truncate_str(reason, 300)
            ),
        ),
        CheckEnd::Failed { reason } => (
            "cancelled",
            "Didn't finish".to_string(),
            format!(
                "Every try of this review hit a service error, so there is no verdict for this \
                 commit; any earlier verdict stands. Error: {}",
                common::truncate_str(reason, 300)
            ),
        ),
    };
    CheckClose {
        conclusion,
        title,
        summary: format!("{body}\n\n{session}"),
    }
}

/// Complete check `id`. Never fails the review: an error is logged.
pub(crate) async fn close_check<C: CheckApi + ?Sized>(
    api: &C,
    t: &CheckTarget<'_>,
    id: u64,
    close: &CheckClose,
) {
    let body = serde_json::json!({
        "status": "completed",
        "conclusion": close.conclusion,
        "completed_at": chrono::Utc::now().to_rfc3339(),
        "output": {
            "title": common::truncate_str(&close.title, 250),
            "summary": common::truncate_str(&close.summary, 60_000),
        },
    });
    if let Err(e) = api.update(t.owner, t.repo, id, &body).await {
        log_failure(t.installation, t.pr, "close", &e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    /// GitHub as the test wants it: refusing, failing, or working, with the
    /// calls it got.
    struct FakeGitHub {
        answer: fn() -> Result<u64, Error>,
        calls: StdMutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl CheckApi for FakeGitHub {
        async fn create(
            &self,
            _owner: &str,
            _repo: &str,
            body: &serde_json::Value,
        ) -> Result<u64, Error> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("create {}", body["status"]));
            (self.answer)()
        }
        async fn update(
            &self,
            _owner: &str,
            _repo: &str,
            id: u64,
            body: &serde_json::Value,
        ) -> Result<(), Error> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("update {id} {}", body["conclusion"]));
            (self.answer)().map(|_| ())
        }
    }

    /// The error GitHub's client returns for an installation without
    /// checks:write.
    fn forbidden() -> Result<u64, Error> {
        Err("HTTP status client error (403 Forbidden) for url \
             (https://api.github.com/repos/acme/web/check-runs): \
             {\"message\":\"Resource not accessible by integration\"}"
            .into())
    }

    fn target(installation: u64) -> CheckTarget<'static> {
        CheckTarget {
            installation,
            owner: "acme",
            repo: "web",
            pr: 7,
            head: "9fceb02d0ae598e95dc970b74767f19372d61af8",
        }
    }

    #[tokio::test]
    async fn a_refused_check_degrades_silently() {
        let gh = FakeGitHub {
            answer: forbidden,
            calls: StdMutex::new(vec![]),
        };
        let t = target(9_001);
        let id = open_check(
            &gh,
            &t,
            "job",
            "Reading",
            "https://app.example/reviewer/session?id=job",
        )
        .await;
        // No check, no error, and the review carries on.
        assert_eq!(id, None);
        // Closing a check GitHub refuses is logged and dropped too.
        let close = check_close(
            CheckEnd::Skipped { reason: "x" },
            "https://github.com/acme/web/pull/7",
            "s",
        );
        close_check(&gh, &t, 1, &close).await;
        assert_eq!(gh.calls.lock().unwrap().len(), 2);
        // The refusal is logged once per installation.
        assert!(!first_refusal(9_001));
        assert!(first_refusal(9_002));
        assert!(!first_refusal(9_002));
    }

    #[tokio::test]
    async fn an_open_check_carries_its_session_link() {
        let gh = FakeGitHub {
            answer: || Ok(42),
            calls: StdMutex::new(vec![]),
        };
        let t = target(1);
        let id = open_check(&gh, &t, "job", "Reading", "u").await;
        assert_eq!(id, Some(42));
        assert_eq!(gh.calls.lock().unwrap()[0], "create \"in_progress\"");
        // No head, no check (a check run needs the commit).
        let mut no_head = target(1);
        no_head.head = "";
        assert_eq!(open_check(&gh, &no_head, "job", "Reading", "u").await, None);
        assert_eq!(gh.calls.lock().unwrap().len(), 1);
    }

    #[test]
    fn only_a_refusal_reads_as_not_permitted() {
        let e = forbidden().unwrap_err();
        assert!(is_not_permitted(&*e));
        let other: Error = "HTTP status server error (502 Bad Gateway) for url (u)".into();
        assert!(!is_not_permitted(&*other));
    }

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
        assert_eq!(c(CheckEnd::Skipped { reason: "x" }), "skipped");
        assert_eq!(c(CheckEnd::Retrying { reason: "x" }), "cancelled");
        assert_eq!(c(CheckEnd::Failed { reason: "x" }), "cancelled");
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
            session_url("https://app.example", "j"),
            "https://app.example/reviewer/session?id=j"
        );
    }
}
