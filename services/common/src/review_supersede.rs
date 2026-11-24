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
//! Between reviews [`tidy`] keeps the rest true: an approval of an older
//! commit, every body on a merged or closed pull request, a thread whose
//! checks went green or whose code left the diff, and [`fold`] keeps a new
//! review short (only the verdict and what holds the merge in sight).
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
    /// The commit GitHub posted it on.
    pub commit_id: String,
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
    /// GitHub marks the thread outdated: the lines it is on changed since.
    pub outdated: bool,
    /// The lines it was on are no longer in the pull request's diff (GitHub
    /// has no current line for it).
    pub line_gone: bool,
    /// The reviewer's answer to "does this finding still hold at the
    /// head?", when it was asked; `None` until it was.
    pub holds_at_head: Option<bool>,
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
    // A carried approval rests on the review it carries: that one, its
    // comments and its threads stay as they are.
    let carried = reviews
        .iter()
        .find(|r| r.id == new_id)
        .and_then(|r| carried_from(&r.body));
    let earlier: Vec<&Review> = reviews
        .iter()
        .filter(|r| r.own && r.id != new_id && r.state != "PENDING")
        .filter(|r| !rests_on(r, carried.as_deref()))
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
        let kept = t.comments.first().is_some_and(|c| {
            reviews
                .iter()
                .any(|r| r.id == c.review_id && rests_on(r, carried.as_deref()))
        });
        if !kept && !t.resolved && !raised.contains(&finding_key(&t.path, &t.first_body)) {
            out.resolve.push(t.id.clone());
        }
    }
    out
}

// ── Tidy: kept tidy between reviews ──────────────────────────────────────
//
// A new review is not the only moment CoderHelm's words stop being true. A
// push makes its approval a verdict on older code; checks that went green
// make its "the checks are red" thread wrong; a merge makes every review
// body history. [`tidy`] decides, from the pull request as it is now, what
// of CoderHelm's own is cut to one line, resolved or hidden. It runs on a
// push, on checks finishing, on a merge or a close, and on the periodic
// sweep (`worker::passes::review_tidy`), and posts nothing.

/// Marks a review body cut to one line by [`tidy`]; a body carrying it, or
/// [`OUTDATED_BODY`], is never cut again.
pub const COLLAPSED_MARK: &str = "<!-- coderhelm:review-collapsed -->";

/// The mark a review body brought up to date for a newer commit carries
/// (`worker::passes::review_verdict::UPDATED_MARK`, pinned equal by a test
/// there), followed by "_Updated for `<sha7>`".
pub const UPDATED_MARK: &str = "<!-- coderhelm:review-updated -->";

/// The label CoderHelm's reviewer puts on a finding about a failing check
/// (`worker::passes::review_rules::mandatory_label`, pinned by a test there).
pub const FAILING_CHECK_LABEL: &str = "Blocking (failing check)";

/// The mark a required-rule finding carries
/// (`worker::passes::review_rules::REQUIRED_MARK`, pinned by a test there):
/// a missing test is not fixed by its line moving, so such a thread is
/// never resolved because its lines left the diff.
pub const REQUIRED_MARK: &str = "<!-- coderhelm:required -->";

/// Where the pull request stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PrState {
    #[default]
    Open,
    Merged,
    Closed,
}

/// The pull request as it is now, as [`tidy`] reads it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrNow {
    pub owner: String,
    pub repo: String,
    pub number: u64,
    pub head: String,
    pub state: PrState,
    /// The head's checks: `Some(true)` all finished and none failed,
    /// `Some(false)` one failed, `None` still running or not read.
    pub checks_pass: Option<bool>,
    /// The files the pull request changes against its merge base, when read.
    pub changed_paths: Option<Vec<String>>,
}

/// Why a thread of CoderHelm's is resolved without a new review.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settled {
    /// A failing-check finding whose checks now pass on the head.
    ChecksPass,
    /// The file left the pull request's diff.
    FileGone,
    /// The lines it was on left the diff.
    LinesGone,
    /// The reviewer read the head and the finding no longer holds.
    FixedAtHead,
}

/// What [`tidy`] decided.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tidy {
    /// Review bodies cut to one line: (review id, the new body).
    pub collapse: Vec<(u64, String)>,
    /// Threads to resolve (node id, why).
    pub resolve: Vec<(String, Settled)>,
    /// Own comments to hide as outdated (node ids).
    pub minimize: Vec<String>,
}

impl Tidy {
    pub fn is_empty(&self) -> bool {
        self.collapse.is_empty() && self.resolve.is_empty() && self.minimize.is_empty()
    }
}

/// Already cut to one line (by [`tidy`] or by a later review).
pub fn collapsed(body: &str) -> bool {
    body.trim() == OUTDATED_BODY || body.contains(COLLAPSED_MARK)
}

/// The commit a review speaks for: the one it was brought up to date for
/// when its body says so, else the one it was posted on.
pub fn reviewed_sha(r: &Review) -> String {
    if let Some(at) = r.body.find(UPDATED_MARK) {
        let rest = &r.body[at + UPDATED_MARK.len()..];
        if let Some(start) = rest.find("_Updated for `") {
            let rest = &rest[start + "_Updated for `".len()..];
            if let Some(end) = rest.find('`') {
                let sha = rest[..end].trim();
                if sha.len() >= 7 && sha.chars().all(|c| c.is_ascii_hexdigit()) {
                    return sha.to_string();
                }
            }
        }
    }
    r.commit_id.clone()
}

/// Whether `sha` (full or short) is the head.
fn is_head(sha: &str, head: &str) -> bool {
    sha.len() >= 7 && head.len() >= 7 && (head.starts_with(sha) || sha.starts_with(head))
}

// ── Carried approvals ────────────────────────────────────────────────────
//
// A push that only brings in the base branch is not reviewed again: the
// approval of the earlier commit is posted again on the new head. That
// review's verdict rests on the earlier one, so the earlier one is never cut
// or hidden while the carried approval is CoderHelm's latest word, and the
// carried approval itself carries the earlier review's content.

/// The mark a carried approval starts with, followed by the commit whose
/// review it carries and ` -->`.
pub const CARRIES_MARK: &str = "<!-- coderhelm:carries ";

/// The mark before the carried review's own content in a carried approval,
/// followed by the commit that review was written for and ` -->`.
pub const CARRIED_REVIEW_MARK: &str = "<!-- coderhelm:carried-review ";

/// The commit a carried approval's verdict rests on, when `body` is one:
/// its [`CARRIES_MARK`], or the one-line wording carried approvals had
/// before they carried the review ("the same as the approved `abc1234`").
pub fn carried_from(body: &str) -> Option<String> {
    let sha_after = |marker: &str, end: char| -> Option<String> {
        let at = body.find(marker)?;
        let rest = &body[at + marker.len()..];
        let sha = rest[..rest.find(end)?].trim();
        (sha.len() >= 7 && sha.chars().all(|c| c.is_ascii_hexdigit())).then(|| sha.to_string())
    };
    sha_after(CARRIES_MARK, '-').or_else(|| sha_after("the same as the approved `", '`'))
}

/// The commit CoderHelm's latest review on the pull request carries its
/// verdict from, when that review is a carried approval: the review of that
/// commit is the one the standing verdict rests on.
pub fn live_carried_sha(reviews: &[Review]) -> Option<String> {
    reviews
        .iter()
        .filter(|r| r.own && r.state != "PENDING" && !r.body.trim().is_empty())
        .max_by_key(|r| r.id)
        .and_then(|r| carried_from(&r.body))
}

/// The review a carried approval rests on: CoderHelm's own, for the commit
/// `carried` names.
fn rests_on(r: &Review, carried: Option<&str>) -> bool {
    carried.is_some_and(|c| is_head(c, &reviewed_sha(r)))
}

/// The ids of the reviews the latest carried approval rests on.
pub fn kept_for_carry(reviews: &[Review]) -> Vec<u64> {
    let live = live_carried_sha(reviews);
    reviews
        .iter()
        .filter(|r| r.own && rests_on(r, live.as_deref()))
        .map(|r| r.id)
        .collect()
}

/// The verdict a review gave, in two words and an icon.
pub fn verdict_word(r: &Review) -> &'static str {
    let b = r.body.as_str();
    match r.state.as_str() {
        "APPROVED" if b.contains("Approved with notes") => "✅ Approved with notes",
        "APPROVED" => "✅ Approved",
        "CHANGES_REQUESTED" => "❌ Changes requested",
        _ if b.contains("✅ **Approved with notes**") => "✅ Approved with notes",
        _ if b.contains("✅ **Approved**") => "✅ Approved",
        _ if b.contains("❌ **Changes requested**") => "❌ Changes requested",
        _ => "💬 Reviewed",
    }
}

/// A review body cut to one line: the verdict, the commit it was of (a
/// link to it in the pull request) and why it no longer stands.
pub fn one_line(r: &Review, pr: &PrNow, why: &str) -> String {
    let sha = reviewed_sha(r);
    let short = &sha[..sha.len().min(7)];
    let at = if short.is_empty() {
        String::new()
    } else {
        format!(
            " [`{short}`](https://github.com/{}/{}/pull/{}/commits/{sha})",
            pr.owner, pr.repo, pr.number
        )
    };
    format!("{COLLAPSED_MARK}\n{}{at} ({why})", verdict_word(r))
}

/// What to tidy on the pull request now. Only CoderHelm's own reviews,
/// comments and threads are ever named; a thread a person opened, or one
/// where a person's reply waits for an answer, is left as it is.
///
/// - Reviews: on a merged or closed pull request every own review body is
///   cut to one line. On an open one a body that speaks for an older commit
///   than the head is cut, except a request for changes (a formal one, or a
///   comment review saying so on CoderHelm's own pull request): it still
///   holds the merge, so its reasons stay readable until the next review
///   replaces it.
/// - Threads (open pull request): resolved, with CoderHelm's comments in
///   them hidden, once the finding no longer holds ([`Settled`]).
/// - Threads (merged or closed): CoderHelm's comments in its own resolved
///   threads are hidden.
pub fn tidy(reviews: &[Review], threads: &[Thread], pr: &PrNow) -> Tidy {
    let mut out = Tidy::default();
    let done = pr.state != PrState::Open;
    // The review CoderHelm's latest carried approval rests on stays in full
    // while the pull request is open: it is what the verdict says was checked.
    let live = if done {
        None
    } else {
        live_carried_sha(reviews)
    };
    for r in reviews.iter().filter(|r| r.own && r.state != "PENDING") {
        if r.body.trim().is_empty() || collapsed(&r.body) || rests_on(r, live.as_deref()) {
            continue;
        }
        let why = match pr.state {
            PrState::Merged => "merged",
            PrState::Closed => "closed",
            PrState::Open => {
                let sha = reviewed_sha(r);
                if pr.head.is_empty()
                    || sha.is_empty()
                    || is_head(&sha, &pr.head)
                    || verdict_word(r) == "❌ Changes requested"
                {
                    continue;
                }
                "superseded by later commits"
            }
        };
        out.collapse.push((r.id, one_line(r, pr, why)));
    }
    for t in threads {
        if !t.own() || t.person_waiting() {
            continue;
        }
        let settled = if t.resolved || done {
            None
        } else {
            settled(t, pr)
        };
        if let Some(why) = settled {
            out.resolve.push((t.id.clone(), why));
        }
        if settled.is_some() || (done && t.resolved) {
            out.minimize.extend(
                t.comments
                    .iter()
                    .filter(|c| c.own && !c.minimized)
                    .map(|c| c.node_id.clone()),
            );
        }
    }
    out
}

/// Why an open thread of CoderHelm's no longer holds, if it doesn't.
pub fn settled(t: &Thread, pr: &PrNow) -> Option<Settled> {
    if t.holds_at_head == Some(false) {
        return Some(Settled::FixedAtHead);
    }
    if t.holds_at_head == Some(true) {
        return None;
    }
    if failing_check_finding(&t.first_body) && pr.checks_pass == Some(true) {
        return Some(Settled::ChecksPass);
    }
    if let Some(paths) = &pr.changed_paths {
        if !t.path.is_empty() && !paths.iter().any(|p| p == &t.path) {
            return Some(Settled::FileGone);
        }
    }
    if t.outdated && t.line_gone && !t.first_body.contains(REQUIRED_MARK) {
        return Some(Settled::LinesGone);
    }
    None
}

/// A finding about a failing check (its label, or the reviewer's words for
/// one).
pub fn failing_check_finding(body: &str) -> bool {
    let first = body.to_ascii_lowercase();
    first.contains(&FAILING_CHECK_LABEL.to_ascii_lowercase())
        || first.contains("are red on this head")
        || first.contains("is red on this head")
}

/// A head's checks in one answer, from GitHub's check runs for it:
/// `Some(true)` every one finished and none failed, `Some(false)` one
/// failed, `None` one is still running or there are none. CoderHelm's own
/// review check is left out: it is about the review, not the code.
pub fn checks_pass(check_runs: &serde_json::Value) -> Option<bool> {
    let runs: Vec<&serde_json::Value> = check_runs["check_runs"]
        .as_array()?
        .iter()
        .filter(|c| {
            !c["app"]["slug"]
                .as_str()
                .unwrap_or("")
                .to_ascii_lowercase()
                .contains("coderhelm")
        })
        .collect();
    if runs.is_empty() {
        return None;
    }
    if runs.iter().any(|c| {
        matches!(
            c["conclusion"].as_str(),
            Some("failure" | "timed_out" | "cancelled" | "action_required" | "startup_failure")
        )
    }) {
        return Some(false);
    }
    if runs
        .iter()
        .any(|c| c["status"].as_str() != Some("completed"))
    {
        return None;
    }
    Some(true)
}

// ── Short from the start ─────────────────────────────────────────────────

/// A review body with only `visible` in sight and `details` folded under
/// one collapsed `<details>` block (GitHub renders it in review bodies; the
/// blank lines around the inside keep its markdown rendering), then
/// `footer`. The summary names the sections folded inside.
pub fn fold(visible: &str, details: &str, footer: &str) -> String {
    let visible = visible.trim();
    let details = details.trim();
    let mut out = visible.to_string();
    if !details.is_empty() {
        out.push_str(&format!(
            "\n\n<details>\n<summary>{}</summary>\n\n{details}\n\n</details>",
            fold_summary(details)
        ));
    }
    out.push_str(footer);
    out
}

/// The `<summary>` of a folded block: the headings inside, else "Details".
pub fn fold_summary(details: &str) -> String {
    let mut names: Vec<String> = Vec::new();
    for l in details.lines() {
        let l = l.trim();
        if !l.starts_with('#') {
            continue;
        }
        let name = l.trim_start_matches('#').trim().trim_matches('*').trim();
        let name = name.split(':').next().unwrap_or("").trim();
        if !name.is_empty() && !names.iter().any(|n| n == name) {
            names.push(name.to_string());
        }
    }
    if names.is_empty() {
        return "Details".to_string();
    }
    let shown = names.len().min(6);
    let mut s = format!("Details: {}", names[..shown].join(" · "));
    if names.len() > shown {
        s.push_str(&format!(" · {} more", names.len() - shown));
    }
    s
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
            ..Default::default()
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
            ..Default::default()
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
                ..Default::default()
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

    fn pr(head: &str, state: PrState) -> PrNow {
        PrNow {
            owner: "acme".into(),
            repo: "web".into(),
            number: 7,
            head: head.into(),
            state,
            ..Default::default()
        }
    }

    fn posted(id: u64, state: &str, commit: &str, body: &str) -> Review {
        Review {
            id,
            own: true,
            state: state.into(),
            body: body.into(),
            commit_id: commit.into(),
        }
    }

    const HEAD: &str = "bbbbbbb2222222222222222222222222222222222";
    const OLD: &str = "aaaaaaa1111111111111111111111111111111111";

    /// An approval of an older commit is cut to one line naming the commit;
    /// one of the head stays; running again does nothing.
    #[test]
    fn an_approval_of_an_older_commit_is_one_line() {
        let r = vec![
            posted(1, "APPROVED", OLD, "✅ **Approved with notes**: ..."),
            posted(2, "APPROVED", HEAD, "✅ **Approved**: nothing."),
        ];
        let t = tidy(&r, &[], &pr(HEAD, PrState::Open));
        assert_eq!(t.collapse.len(), 1);
        let (id, body) = &t.collapse[0];
        assert_eq!(*id, 1);
        assert!(body.starts_with(COLLAPSED_MARK));
        assert!(body.ends_with(
            "✅ Approved with notes [`aaaaaaa`](https://github.com/acme/web/pull/7/commits/aaaaaaa1111111111111111111111111111111111) (superseded by later commits)"
        ), "{body}");
        let again = vec![posted(1, "APPROVED", OLD, body), r[1].clone()];
        assert!(tidy(&again, &[], &pr(HEAD, PrState::Open)).is_empty());
    }

    /// A body brought up to date for the head (a silent re-review) speaks
    /// for the head, though GitHub keeps it on the older commit.
    #[test]
    fn a_body_brought_up_to_date_speaks_for_the_head() {
        let body = format!("{UPDATED_MARK}\n_Updated for `bbbbbbb`: what this review said about `aaaaaaa` was replaced by the review of the current commit below._\n\n✅ **Approved**");
        let r = vec![posted(1, "APPROVED", OLD, &body)];
        assert_eq!(reviewed_sha(&r[0]), "bbbbbbb");
        assert!(tidy(&r, &[], &pr(HEAD, PrState::Open)).collapse.is_empty());
    }

    /// A request for changes on an older commit still holds the merge: its
    /// reasons stay readable until a review replaces it. On a merge every
    /// body is cut, that one too.
    #[test]
    fn a_standing_request_for_changes_stays_until_the_merge() {
        let r = vec![posted(
            1,
            "CHANGES_REQUESTED",
            OLD,
            "❌ **Changes requested**: ...",
        )];
        assert!(tidy(&r, &[], &pr(HEAD, PrState::Open)).collapse.is_empty());
        let t = tidy(&r, &[], &pr(HEAD, PrState::Merged));
        assert!(t.collapse[0].1.ends_with("(merged)"), "{t:?}");
        assert!(t.collapse[0].1.contains("❌ Changes requested"));
        let t = tidy(&r, &[], &pr(HEAD, PrState::Closed));
        assert!(t.collapse[0].1.ends_with("(closed)"), "{t:?}");
    }

    /// A person's review is never cut; a review with no body (a reply) or
    /// one already cut by a later review is left.
    #[test]
    fn only_own_bodies_are_cut() {
        let mut person = posted(1, "APPROVED", OLD, "LGTM");
        person.own = false;
        let r = vec![
            person,
            posted(2, "COMMENTED", OLD, ""),
            posted(3, "COMMENTED", OLD, OUTDATED_BODY),
        ];
        assert!(tidy(&r, &[], &pr(HEAD, PrState::Merged))
            .collapse
            .is_empty());
    }

    fn own_thread(id: &str, path: &str, body: &str) -> Thread {
        Thread {
            id: id.into(),
            path: path.into(),
            first_body: body.into(),
            comments: vec![c(&format!("{id}c"), true, 1)],
            ..Default::default()
        }
    }

    /// A failing-check finding resolves once the head's checks pass, and
    /// its comment is hidden; while they run or fail it stays.
    #[test]
    fn a_failing_check_finding_settles_when_the_checks_pass() {
        let t = vec![own_thread(
            "T1",
            "e2e/slips.spec.ts",
            "🔴 Blocking (failing check): Lint & Type Check and Deploy Preview are red on this head.",
        )];
        let mut now = pr(HEAD, PrState::Open);
        assert!(tidy(&[], &t, &now).is_empty());
        now.checks_pass = Some(false);
        assert!(tidy(&[], &t, &now).is_empty());
        now.checks_pass = Some(true);
        let d = tidy(&[], &t, &now);
        assert_eq!(d.resolve, vec![("T1".to_string(), Settled::ChecksPass)]);
        assert_eq!(d.minimize, vec!["T1c"]);
    }

    /// Code gone from the diff: the file, or the lines (not for a required
    /// rule, which its lines moving does not fix). The reviewer's answer
    /// wins both ways.
    #[test]
    fn a_finding_whose_code_left_the_diff_settles() {
        let mut now = pr(HEAD, PrState::Open);
        now.changed_paths = Some(vec!["src/a.ts".into()]);
        let gone = own_thread("T1", "src/b.ts", "Null check missing.");
        assert_eq!(settled(&gone, &now), Some(Settled::FileGone));
        let mut lines = own_thread("T2", "src/a.ts", "Null check missing.");
        assert_eq!(settled(&lines, &now), None);
        lines.outdated = true;
        assert_eq!(settled(&lines, &now), None);
        lines.line_gone = true;
        assert_eq!(settled(&lines, &now), Some(Settled::LinesGone));
        let mut required = lines.clone();
        required.first_body = format!("{REQUIRED_MARK}\nNo test covers the new branch.");
        assert_eq!(settled(&required, &now), None);
        required.holds_at_head = Some(false);
        assert_eq!(settled(&required, &now), Some(Settled::FixedAtHead));
        lines.holds_at_head = Some(true);
        assert_eq!(settled(&lines, &now), None);
    }

    /// A thread a person opened, or one where a person's reply waits, is
    /// never resolved or hidden, merged or not.
    #[test]
    fn a_persons_thread_or_reply_is_never_tidied() {
        let mut now = pr(HEAD, PrState::Open);
        now.checks_pass = Some(true);
        let body = "🔴 Blocking (failing check): CI is red on this head.";
        let mut waiting = own_thread("T1", "a.ts", body);
        waiting.comments.push(c("P1", false, 0));
        let mut theirs = own_thread("T2", "a.ts", body);
        theirs.comments.insert(0, c("P2", false, 9));
        theirs.resolved = true;
        let threads = vec![waiting, theirs];
        assert!(tidy(&[], &threads, &now).is_empty());
        now.state = PrState::Merged;
        assert!(tidy(&[], &threads, &now).is_empty());
    }

    /// On a merge CoderHelm's resolved threads are hidden; its open ones
    /// stay as they are.
    #[test]
    fn a_merge_hides_own_resolved_threads() {
        let mut done = own_thread("T1", "a.ts", "Null check missing.");
        done.resolved = true;
        let open = own_thread("T2", "a.ts", "Retry never ends.");
        let d = tidy(&[], &[done, open], &pr(HEAD, PrState::Merged));
        assert!(d.resolve.is_empty());
        assert_eq!(d.minimize, vec!["T1c"]);
    }

    /// Checks: CoderHelm's own review check is not the code's; a run in
    /// progress is not a pass; a failure is a failure.
    #[test]
    fn the_heads_checks_in_one_answer() {
        let runs = |v: serde_json::Value| serde_json::json!({ "check_runs": v });
        let ok = serde_json::json!({"status":"completed","conclusion":"success","app":{"slug":"github-actions"}});
        let own = serde_json::json!({"status":"in_progress","conclusion":null,"app":{"slug":"coderhelm"}});
        let running = serde_json::json!({"status":"in_progress","conclusion":null,"app":{"slug":"github-actions"}});
        let red = serde_json::json!({"status":"completed","conclusion":"failure","app":{"slug":"github-actions"}});
        assert_eq!(checks_pass(&runs(serde_json::json!([ok, own]))), Some(true));
        assert_eq!(checks_pass(&runs(serde_json::json!([ok, running]))), None);
        assert_eq!(
            checks_pass(&runs(serde_json::json!([running, red]))),
            Some(false)
        );
        assert_eq!(checks_pass(&runs(serde_json::json!([own]))), None);
    }

    /// Folded: the verdict in sight, the rest in one collapsed block named
    /// by its sections, the footer last.
    #[test]
    fn a_review_folds_its_details() {
        let body = fold(
            "✅ **Approved**: nothing in the change needs fixing.",
            "## PR Overview\n| File | What |\n\n#### Risk: low\n\n#### Verification\n- ok",
            "\n\n---\nfooter",
        );
        assert!(body.starts_with("✅ **Approved**"));
        assert!(body.contains(
            "<details>\n<summary>Details: PR Overview · Risk · Verification</summary>\n\n## PR Overview"
        ), "{body}");
        assert!(body.ends_with("</details>\n\n---\nfooter"));
        assert_eq!(fold("v", "  ", ""), "v");
        // The review's style pass (`comment_style::tidy`) keeps the block.
        let place = crate::conversation::Place::github("acme", "web", 7);
        let kept = crate::comment_style::tidy(&body, &place);
        for line in [
            "<details>",
            "</details>",
            "<summary>Details: PR Overview · Risk · Verification</summary>",
        ] {
            assert!(kept.lines().any(|l| l == line), "{line} lost from {kept}");
        }
        assert_eq!(fold_summary("plain text"), "Details");
    }
}
