//! CoderHelm's earlier reviews on a pull request, out of the way once it
//! posts a new one.
//!
//! GitHub can't delete a submitted review. A pull request CoderHelm reviewed
//! three times showed three full review bodies, each with its inline
//! comments, and a reader had to work out which one was current. The
//! cleanest equivalent, decided here and carried out by the conversation
//! layer (`worker::passes::conversation::supersede_reviews`):
//!
//! - every earlier review of CoderHelm's is rewritten to [`OUTDATED_BODY`];
//! - its earlier inline comments are hidden as outdated: only CoderHelm's
//!   own, only in threads it opened, and only where no person's reply is
//!   left unanswered;
//! - its own threads whose finding the new review no longer raises are
//!   resolved, never one a person opened or replied to without an answer;
//! - its own earlier request for changes is dismissed with
//!   [`DISMISS_MESSAGE`].
//!
//! A person's review, comment or thread is never touched.

/// What an earlier review of CoderHelm's says once superseded.
pub const OUTDATED_BODY: &str = "Outdated: see CoderHelm's latest review";
/// The dismissal message on CoderHelm's own superseded request for changes.
pub const DISMISS_MESSAGE: &str = "Superseded by CoderHelm's latest review";

/// A submitted review on the pull request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Review {
    pub id: u64,
    /// Written by CoderHelm's app.
    pub own: bool,
    /// `APPROVED`, `CHANGES_REQUESTED`, `COMMENTED`, `DISMISSED`, `PENDING`.
    pub state: String,
    pub body: String,
}

/// One comment in a review thread, oldest first.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ThreadComment {
    /// GraphQL node id (what hiding takes).
    pub node_id: String,
    pub own: bool,
    /// The review it was posted with (0 for a reply outside a review).
    pub review_id: u64,
    pub minimized: bool,
}

/// A review thread on the pull request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Thread {
    /// GraphQL node id (what resolving takes).
    pub id: String,
    pub resolved: bool,
    pub path: String,
    /// The first comment's text (the finding).
    pub first_body: String,
    pub comments: Vec<ThreadComment>,
}

impl Thread {
    /// CoderHelm opened it.
    pub fn own(&self) -> bool {
        self.comments.first().is_some_and(|c| c.own)
    }

    /// A person wrote the last word: their reply waits for an answer.
    pub fn person_waiting(&self) -> bool {
        self.comments.last().is_some_and(|c| !c.own)
    }
}

/// What to do with the earlier reviews.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    /// Reviews whose body becomes [`OUTDATED_BODY`].
    pub edit: Vec<u64>,
    /// Requests for changes to dismiss.
    pub dismiss: Vec<u64>,
    /// Inline comments to hide as outdated (node ids).
    pub minimize: Vec<String>,
    /// Threads to resolve (node ids).
    pub resolve: Vec<String>,
}

/// A finding's key: its file and its first line, in lower case with the
/// punctuation left out, so the same finding raised again matches.
pub fn finding_key(path: &str, body: &str) -> String {
    let first = body
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty() && !l.starts_with("<!--"))
        .unwrap_or("");
    let words: Vec<String> = first
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect();
    format!("{}:{}", path.trim(), words.join(" "))
}

/// The plan once review `new_id` is posted, raising the findings `raised`
/// ([`finding_key`] of each of its inline comments).
pub fn plan(reviews: &[Review], threads: &[Thread], new_id: u64, raised: &[String]) -> Plan {
    let earlier: Vec<&Review> = reviews
        .iter()
        .filter(|r| r.own && r.id != new_id && r.state != "PENDING")
        .collect();
    let earlier_ids: Vec<u64> = earlier.iter().map(|r| r.id).collect();
    let mut out = Plan {
        edit: earlier
            .iter()
            .filter(|r| r.body.trim() != OUTDATED_BODY)
            .map(|r| r.id)
            .collect(),
        dismiss: earlier
            .iter()
            .filter(|r| r.state == "CHANGES_REQUESTED")
            .map(|r| r.id)
            .collect(),
        ..Default::default()
    };
    for t in threads {
        // A person waiting on an answer keeps the whole thread in sight; a
        // thread a person opened keeps CoderHelm's answers in it.
        if t.person_waiting() || !t.own() {
            continue;
        }
        for c in &t.comments {
            if c.own && !c.minimized && earlier_ids.contains(&c.review_id) {
                out.minimize.push(c.node_id.clone());
            }
        }
        if !t.resolved && !raised.contains(&finding_key(&t.path, &t.first_body)) {
            out.resolve.push(t.id.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn own_review(id: u64, state: &str) -> Review {
        Review {
            id,
            own: true,
            state: state.into(),
            body: format!("Review {id}: the full verdict and its findings."),
        }
    }

    fn c(node: &str, own: bool, review_id: u64) -> ThreadComment {
        ThreadComment {
            node_id: node.into(),
            own,
            review_id,
            minimized: false,
        }
    }

    fn thread(id: &str, path: &str, body: &str, comments: Vec<ThreadComment>) -> Thread {
        Thread {
            id: id.into(),
            resolved: false,
            path: path.into(),
            first_body: body.into(),
            comments,
        }
    }

    /// Three reviews by CoderHelm, then a fourth: the three collapse to one
    /// line each, their comments are hidden, the finding fixed since is
    /// resolved, and the old request for changes is dismissed.
    #[test]
    fn three_superseded_reviews_collapse_to_the_current_one() {
        let reviews = vec![
            own_review(1, "CHANGES_REQUESTED"),
            own_review(2, "COMMENTED"),
            own_review(3, "CHANGES_REQUESTED"),
            own_review(4, "APPROVED"),
        ];
        let threads = vec![
            thread(
                "T1",
                "src/a.ts",
                "Null check missing before `items.length`.",
                vec![c("C1", true, 1)],
            ),
            thread(
                "T2",
                "src/b.ts",
                "The retry loop never ends when the list is empty.",
                vec![c("C2", true, 3)],
            ),
            // The finding the new review raises again: hidden, not resolved.
            thread(
                "T3",
                "src/c.ts",
                "The timeout is in seconds, the API takes milliseconds.",
                vec![c("C3", true, 2)],
            ),
        ];
        let raised = vec![finding_key(
            "src/c.ts",
            "The timeout is in seconds; the API takes milliseconds!",
        )];
        let p = plan(&reviews, &threads, 4, &raised);
        assert_eq!(p.edit, vec![1, 2, 3]);
        assert_eq!(p.dismiss, vec![1, 3]);
        assert_eq!(p.minimize, vec!["C1", "C2", "C3"]);
        assert_eq!(p.resolve, vec!["T1", "T2"]);
        // Run again after it was done: nothing left to do.
        let done: Vec<Review> = reviews
            .iter()
            .map(|r| Review {
                body: if r.id == 4 {
                    r.body.clone()
                } else {
                    OUTDATED_BODY.into()
                },
                state: if r.state == "CHANGES_REQUESTED" {
                    "DISMISSED".into()
                } else {
                    r.state.clone()
                },
                ..r.clone()
            })
            .collect();
        let hidden: Vec<Thread> = threads
            .iter()
            .map(|t| Thread {
                resolved: t.id != "T3",
                comments: t
                    .comments
                    .iter()
                    .map(|x| ThreadComment {
                        minimized: true,
                        ..x.clone()
                    })
                    .collect(),
                ..t.clone()
            })
            .collect();
        assert_eq!(plan(&done, &hidden, 4, &raised), Plan::default());
    }

    /// A person's reply with no answer keeps the thread in sight: nothing
    /// in it is hidden and it is not resolved. Once CoderHelm answered, it
    /// may be.
    #[test]
    fn a_persons_reply_keeps_its_thread_visible() {
        let reviews = vec![own_review(1, "COMMENTED"), own_review(2, "COMMENTED")];
        let waiting = vec![thread(
            "T1",
            "src/a.ts",
            "Null check missing.",
            vec![c("C1", true, 1), c("P1", false, 0)],
        )];
        let p = plan(&reviews, &waiting, 2, &[]);
        assert!(p.minimize.is_empty() && p.resolve.is_empty(), "{p:?}");
        assert_eq!(p.edit, vec![1]);
        let answered = vec![thread(
            "T1",
            "src/a.ts",
            "Null check missing.",
            vec![c("C1", true, 1), c("P1", false, 0), c("C9", true, 0)],
        )];
        let p = plan(&reviews, &answered, 2, &[]);
        assert_eq!(p.minimize, vec!["C1"]);
        assert_eq!(p.resolve, vec!["T1"]);
    }

    /// A person's review, comment and thread are never touched.
    #[test]
    fn a_persons_review_is_never_touched() {
        let reviews = vec![
            Review {
                id: 7,
                own: false,
                state: "CHANGES_REQUESTED".into(),
                body: "Please rename this.".into(),
            },
            own_review(8, "COMMENTED"),
            own_review(9, "COMMENTED"),
        ];
        let threads = vec![thread(
            "T7",
            "src/a.ts",
            "Please rename this.",
            vec![c("P7", false, 7), c("C8", true, 8)],
        )];
        let p = plan(&reviews, &threads, 9, &[]);
        assert_eq!(p.edit, vec![8]);
        assert!(p.dismiss.is_empty(), "{p:?}");
        // Their thread stays as it is, CoderHelm's answer in it too.
        assert!(p.minimize.is_empty(), "{p:?}");
        assert!(p.resolve.is_empty(), "{p:?}");
    }
}
