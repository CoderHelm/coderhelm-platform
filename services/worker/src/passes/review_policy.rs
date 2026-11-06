//! When a review may hold the merge, and when it may change its mind.
//!
//! Pure rules, applied by `review_pr` after the model and the critic and
//! replayed by the offline evaluation (`review_policy_eval`):
//!
//! 1. One review per head commit: an automatic trigger (push, label, ready)
//!    for a commit CoderHelm already reviewed returns the existing verdict
//!    ([`start`]); posting takes a conditional claim per (PR, head, trigger
//!    kind) so two jobs can't both post ([`verdict_claim_sk`]).
//! 2. Verdict stability: a repeat review on an identical fingerprint (see
//!    `review_fingerprint`: everything the verdict rests on, never the commit
//!    alone) doesn't flip on model noise, and a person's re-review ask there
//!    gets the standing verdict without a model review ([`answers_from_standing`]).
//!    When the fingerprint differs the verdict follows what holds the merge
//!    now, and says what changed ([`stabilize`]).
//! 3. Blocking policy: which findings hold the merge is
//!    [`super::review_severity`]'s decision (blocking by default). On top of
//!    it, follow-ups the PR already tracks, problems the PR didn't cause and
//!    failures the base branch has too are notes ([`apply_blocking_policy`]).
//! 4. Blockers up front: after the first reviewed head, a new blocker may
//!    only sit on lines changed since the previous reviewed head (or be one
//!    a person's reply made relevant); any other is a note, and from round 3
//!    on it is also a lesson for the reviewer ([`hold_late_blockers`]).

use super::review_agent::{Evidence, Finding};
use super::review_fingerprint::{in_words, Changed};
use super::review_sweep::ReviewRecord;
use super::review_verdict::{HoldKind, Holding};
use std::collections::{HashMap, HashSet};

/// Who asked for a review.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerKind {
    /// A webhook (push, label, ready for review), a schedule, or a run's own
    /// review of its PR.
    Automatic,
    /// A person asked (a reply, the re-request button, the dashboard, an
    /// edit of the description that answers the review), the
    /// stuck-review sweep, or a reply whose conclusion the formal review must
    /// follow.
    Explicit,
}

/// The kind of a review trigger.
pub fn trigger_kind(trigger: &str) -> TriggerKind {
    match trigger {
        "reply" | "rerequest" | "sweep" | "reconcile" | common::review_description::TRIGGER => {
            TriggerKind::Explicit
        }
        _ => TriggerKind::Automatic,
    }
}

/// A person's re-review ask takes the per-commit claim the gateway takes for
/// push reviews, so a push review and a re-review ask of the same commit
/// never both run.
pub fn takes_head_claim(trigger: &str) -> bool {
    matches!(
        trigger,
        "reply" | "rerequest" | common::review_description::TRIGGER
    )
}

/// The per-commit review claim (`common::claim`), shared by the gateway's
/// push reviews and a person's re-review ask.
pub fn head_claim_sk(owner: &str, name: &str, pr: u64, head: &str) -> String {
    common::claim::review_head_sk(owner, name, pr, head)
}

/// The claim a review takes right before it posts its formal verdict:
/// automatic triggers share one per head; each person's ask has its own
/// (its dedup key is per action), so a redelivered or concurrent copy of the
/// same trigger never posts twice.
pub fn verdict_claim_sk(
    owner: &str,
    name: &str,
    pr: u64,
    head: &str,
    trigger: &str,
    dedup_key: &str,
) -> String {
    let kind = match trigger_kind(trigger) {
        TriggerKind::Automatic => "auto".to_string(),
        TriggerKind::Explicit if dedup_key.is_empty() => format!("ask#{trigger}"),
        TriggerKind::Explicit => format!("ask#{dedup_key}"),
    };
    format!("REVIEWVERDICT#{owner}/{name}#{pr:06}#{head}#{kind}")
}

/// How long a person's ask waits for a running review of the same commit
/// before it runs, and how many times it waits.
pub const ASK_WAIT_SECS: u64 = 180;
pub const ASK_MAX_WAITS: u32 = 3;

/// A person's re-review ask that found another review of the same commit
/// still running: the same ask again, a little later, under its own dedup
/// key, so it answers the person once that review has finished. `None` once
/// it has waited [`ASK_MAX_WAITS`] times: it runs now instead.
pub fn deferred_ask(msg: &crate::models::ReviewMessage) -> Option<crate::models::ReviewMessage> {
    let (base, waited) = match msg.dedup_key.rsplit_once("#waited") {
        Some((b, n)) => (b.to_string(), n.parse::<u32>().unwrap_or(ASK_MAX_WAITS)),
        None => (msg.dedup_key.clone(), 0),
    };
    if waited >= ASK_MAX_WAITS {
        return None;
    }
    let mut next = msg.clone();
    next.dedup_key = format!("{base}#waited{}", waited + 1);
    next.wait_secs = 0;
    Some(next)
}

/// CoderHelm's finished review of `head`, if it has one (newest first).
pub fn review_of_head<'a>(records: &'a [ReviewRecord], head: &str) -> Option<&'a ReviewRecord> {
    records.iter().find(|r| {
        !head.is_empty()
            && r.head_sha == head
            && r.is_review()
            && !r.failed()
            && matches!(r.verdict.as_str(), "APPROVE" | "REQUEST_CHANGES")
    })
}

/// What a review job does before any model work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Start {
    Review,
    /// The commit was already reviewed and nobody asked again: its verdict
    /// stands.
    ReturnExisting {
        verdict: String,
    },
    /// The commit was reviewed, but its description has changed since (a
    /// draft's edit is not reviewed until it is marked ready, say): reviewed
    /// as a description edit.
    DescriptionChanged,
}

/// Rule 1: an automatic trigger for an already-reviewed commit returns the
/// existing verdict, unless its description moved on since. A person's ask
/// goes on (and may be answered from the verdict, see
/// [`answers_from_standing`]); a run's own review of its PR always runs, the
/// run needs its verdict. An older record with no fingerprint keeps returning
/// its verdict here: a burst of events for one commit must not start paid
/// reviews, and a person's ask reviews it afresh.
pub fn start(trigger: &str, head: &str, description: &str, records: &[ReviewRecord]) -> Start {
    if trigger_kind(trigger) == TriggerKind::Explicit || trigger == super::review_pr::TRIGGER_RUN {
        return Start::Review;
    }
    match review_of_head(records, head) {
        Some(r) => match &r.fingerprint {
            Some(f) if f.description != description => Start::DescriptionChanged,
            _ => Start::ReturnExisting {
                verdict: r.verdict.clone(),
            },
        },
        None => Start::Review,
    }
}

/// A person asked for another look and nothing the verdict rests on changed:
/// no model review, the standing verdict is the answer ([`standing_answer`]).
/// Only for a person's ask: the sweep, a run's own review, a description edit
/// and a reconciliation after a settled finding have their own reasons to run.
pub fn answers_from_standing(trigger: &str, changed: &[Changed], reviewed: bool) -> bool {
    matches!(trigger, "reply" | "rerequest") && reviewed && changed.is_empty()
}

/// The answer to a re-review ask with nothing changed. Fixed words from
/// structured facts; the threads stay as they are.
pub fn standing_answer(head: &str, verdict: &str, holding: &[String]) -> String {
    let short = &head[..head.len().min(7)];
    let verdict = super::review_verdict::binary(verdict);
    let mut s = format!(
        "**Unchanged since my last review of `{short}`: {}.** The code, the description, the \
         review threads, the CI results and what people wrote are the same, so I didn't review \
         it again.",
        human(verdict)
    );
    if !holding.is_empty() {
        s.push_str("\n\n**Still holding the merge:**");
        for h in holding {
            s.push_str(&format!("\n- {h}"));
        }
        s.push_str("\n\nPush a fix, edit the description, or answer a finding in its thread.");
    }
    s
}

/// The line after an answer whose reading disagrees with the verdict when
/// nothing the verdict rests on changed: a reading of a comment can't clear or
/// add a block, and the answer must not stand as if it had.
pub fn verdict_stands_note(head: &str, verdict: &str) -> String {
    let short = &head[..head.len().min(7)];
    format!(
        "My formal verdict on `{short}` stays: {}. A comment doesn't change it; a push, an edit \
         of the description, or an answer in a finding's thread does.",
        human(super::review_verdict::binary(verdict))
    )
}

/// Which review round this head is, and the head reviewed before it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Round {
    /// 1 for the first reviewed head of the PR.
    pub number: usize,
    pub previous_head: Option<String>,
}

/// The round of `head`: one more than the distinct earlier heads CoderHelm
/// finished a review of. `records` is newest first.
pub fn round(records: &[ReviewRecord], head: &str) -> Round {
    let mut heads: Vec<&str> = Vec::new();
    for r in records {
        if r.head_sha.is_empty() || r.head_sha == head || !r.is_review() || r.failed() {
            continue;
        }
        if !heads.contains(&r.head_sha.as_str()) {
            heads.push(&r.head_sha);
        }
    }
    Round {
        number: heads.len() + 1,
        previous_head: heads.first().map(|h| h.to_string()),
    }
}

// ── Rule 3: what may block ──

/// Severities that may hold the merge.
pub fn blocking_severity(severity: &str) -> bool {
    matches!(
        severity.trim().to_ascii_lowercase().as_str(),
        "blocking" | "high" | "critical"
    )
}

/// Makes a finding a note: it is still posted, it never holds the merge.
/// `why` is appended to its body.
pub fn demote(f: &mut Finding, why: &str) {
    f.note_only = Some(why.to_string());
    if blocking_severity(&f.severity) {
        f.severity = "medium".to_string();
    }
    f.weaken_to(Evidence::UnverifiedConcern);
    if f.evidence() == Evidence::Question {
        f.blocks = Some(false);
    }
    if !why.is_empty() && !f.body.contains(why) {
        f.body = format!("{}\n\n_{why}_", f.body.trim_end());
    }
}

/// Why a finding that would hold the merge is a note under rule 3, if it is.
fn note_reason(f: &Finding, base_failing: &[String]) -> Option<&'static str> {
    if f.tracked == Some(true) {
        return Some(
            "A note, not a block: the PR already tracks this as a follow-up, or it is outside this repository.",
        );
    }
    if f.caused_by_diff == Some(false) {
        return Some("A note, not a block: this isn't caused by the PR's change.");
    }
    let text = format!("{} {}", f.title, f.body).to_ascii_lowercase();
    if base_failing
        .iter()
        .map(|n| n.trim().to_ascii_lowercase())
        .any(|n| n.len() >= 3 && text.contains(&n))
    {
        return Some(
            "A note, not a block: this check fails on the base branch too, so the PR didn't cause it.",
        );
    }
    None
}

/// Rule 3. A finding that would hold the merge ([`Finding::holds_merge`])
/// but that the PR already tracks, didn't cause, or that the base branch
/// fails too becomes a note. Returns how many were demoted.
pub fn apply_blocking_policy(findings: &mut [Finding], base_failing: &[String]) -> usize {
    let mut n = 0;
    for f in findings.iter_mut() {
        if !f.holds_merge() {
            continue;
        }
        if let Some(why) = note_reason(f, base_failing) {
            demote(f, why);
            n += 1;
        }
    }
    n
}

/// Names of the checks that failed on the base branch's commit (a GitHub
/// check-runs response).
pub fn failing_checks(check_runs: &serde_json::Value) -> Vec<String> {
    check_runs["check_runs"]
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or(&[])
        .iter()
        .filter(|r| {
            matches!(
                r["conclusion"].as_str(),
                Some("failure" | "timed_out" | "startup_failure")
            )
        })
        .filter_map(|r| r["name"].as_str().map(String::from))
        .collect()
}

// ── Rule 4: blockers up front ──

/// Lines within this distance of a changed line count as changed (a fix
/// often moves the code next to it).
const NEAR_LINES: u64 = 3;

fn near(lines: &HashSet<u64>, from: u64, to: u64) -> bool {
    lines
        .iter()
        .any(|&l| l + NEAR_LINES >= from && l <= to + NEAR_LINES)
}

/// Marks a file whose patch GitHub left out (too large): every line of it
/// counts as changed.
pub const WHOLE_FILE: u64 = u64::MAX;

/// The right-side lines a compare response changed, by file: added lines,
/// and the line where something was deleted. Context lines don't count.
pub fn changed_lines(compare: &serde_json::Value) -> HashMap<String, HashSet<u64>> {
    let mut map: HashMap<String, HashSet<u64>> = HashMap::new();
    for f in compare["files"]
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or(&[])
    {
        let Some(path) = f["filename"].as_str() else {
            continue;
        };
        let set = map.entry(path.to_string()).or_default();
        let Some(patch) = f["patch"].as_str() else {
            set.insert(WHOLE_FILE);
            continue;
        };
        let mut new_ln = 0u64;
        for line in patch.lines() {
            if let Some(hdr) = line.strip_prefix("@@") {
                new_ln = hdr
                    .split('+')
                    .nth(1)
                    .and_then(|p| p.trim().split([',', ' ']).next())
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(0);
                continue;
            }
            match line.chars().next() {
                Some('+') => {
                    set.insert(new_ln);
                    new_ln += 1;
                }
                Some('-') => {
                    set.insert(new_ln);
                }
                Some('\\') => {}
                _ => new_ln += 1,
            }
        }
    }
    map
}

/// Is the finding on code changed since the previous reviewed head?
fn on_changed_lines(f: &Finding, changed: &HashMap<String, HashSet<u64>>) -> bool {
    let Some(lines) = changed.get(&f.file) else {
        return false;
    };
    if lines.contains(&WHOLE_FILE) {
        return true;
    }
    if f.line == 0 {
        return !lines.is_empty();
    }
    near(lines, f.line, f.end_line.unwrap_or(f.line).max(f.line))
}

/// What rule 4 knows about this review.
pub struct UpFront<'a> {
    pub round: &'a Round,
    /// Right-side lines changed since the previous reviewed head, by file.
    /// `None` when unknown (no previous head, or the diff couldn't be read):
    /// nothing is held then.
    pub changed_since_previous: Option<&'a HashMap<String, HashSet<u64>>>,
    /// Where CoderHelm's earlier threads sit: raising one again is not new.
    pub earlier_threads: &'a [(String, u64)],
    /// A person wrote something since the previous review.
    pub person_spoke: bool,
}

/// What rule 4 held back.
#[derive(Debug, Default)]
pub struct Late {
    /// New blockers on code already reviewed, now notes, out of `findings`.
    pub held: Vec<Finding>,
    /// From round 3: the reviewer should have raised them earlier, and
    /// learns so.
    pub self_lesson: bool,
}

pub const LATE_NOTE: &str =
    "On code unchanged since my previous review, so a note, not a block: it should have been raised then.";

/// Rule 4. Every blocker belongs in the first review of the code it is
/// about. After the first reviewed head, a new blocker on lines unchanged
/// since the previous reviewed head is held back as a note, unless it repeats
/// an earlier thread or a person's reply since made it relevant.
pub fn hold_late_blockers(findings: &mut Vec<Finding>, ctx: &UpFront<'_>) -> Late {
    let Some(changed) = ctx.changed_since_previous else {
        return Late::default();
    };
    if ctx.round.number < 2 {
        return Late::default();
    }
    let earlier = |f: &Finding| {
        ctx.earlier_threads
            .iter()
            .any(|(p, l)| *p == f.file && l.abs_diff(f.line) <= NEAR_LINES)
    };
    let (mut held, keep): (Vec<Finding>, Vec<Finding>) =
        std::mem::take(findings).into_iter().partition(|f| {
            f.holds_merge()
                && !on_changed_lines(f, changed)
                && !earlier(f)
                && !(ctx.person_spoke && f.prompted_by_reply == Some(true))
        });
    *findings = keep;
    for f in held.iter_mut() {
        demote(f, LATE_NOTE);
    }
    Late {
        self_lesson: !held.is_empty() && ctx.round.number >= 3,
        held,
    }
}

/// The lesson a review that raised a blocker late keeps for the repository.
pub const RAISE_EARLY_LESSON: &str =
    "Raise every blocking finding in the first review of the code \
     it is about. A defect first noticed in a later round, on lines that haven't changed since \
     they were reviewed, is a note and doesn't hold the merge: read the whole change before the \
     first verdict.";

/// The fixed id of [`RAISE_EARLY_LESSON`]: seen again, never stored twice.
pub const RAISE_EARLY_LESSON_ID: &str = "raiseblockersearly";

/// What the reviewer is told about this review's place in the PR's history:
/// its own earlier verdict on this same commit, and from round 2 on that new
/// blockers belong only on lines changed since the previous round.
pub fn prompt_section(same_head_before: Option<&ReviewRecord>, round: &Round) -> String {
    let mut s = String::new();
    if let Some(prev) = same_head_before {
        s.push_str(&format!(
            "\n## Your previous review of this same commit\nYou already reviewed this exact \
             commit: {}. The code hasn't changed since. Keep that verdict unless something a \
             person wrote since then changes the facts; if your verdict differs, say what changed \
             it in one sentence in `change_of_mind`.\n",
            human(super::review_verdict::binary(&prev.verdict))
        ));
    }
    if round.number >= 2 {
        let prev = round
            .previous_head
            .as_deref()
            .map(|h| format!(" (`{}`)", &h[..h.len().min(7)]))
            .unwrap_or_default();
        s.push_str(&format!(
            "\n## Review round {}\nEvery blocking finding belongs in the first review of the \
             code it is about. Block only on lines changed since your previous review{prev}, or on \
             something a person's reply since made relevant (set `prompted_by_reply`: true). \
             Anything else you notice now on code you already reviewed is a note: it won't hold \
             the merge.\n",
            round.number
        ));
    }
    s
}

// ── Rule 2: verdict stability ──

/// A person's comment that only asks for another look carries no new facts.
pub fn is_bare_command(text: &str) -> bool {
    const FILLER: &[&str] = &[
        "rereview", "re", "review", "please", "pls", "plz", "again", "another", "look", "take",
        "a", "run", "rerun", "can", "could", "you", "thanks", "thank", "ptal", "now", "one",
        "more", "time", "kindly", "hey", "hi", "the", "this", "pr", "it", "latest", "commit",
    ];
    let lower = text.to_ascii_lowercase();
    let words: Vec<String> = lower
        .split_whitespace()
        .filter(|w| !w.starts_with('@'))
        .map(|w| w.chars().filter(|c| c.is_ascii_alphanumeric()).collect())
        .filter(|w: &String| !w.is_empty())
        .collect();
    words.len() <= 8 && words.iter().all(|w| FILLER.contains(&w.as_str()))
}

pub(crate) fn is_person(user: &serde_json::Value) -> bool {
    user["type"].as_str() != Some("Bot")
        && user["login"]
            .as_str()
            .is_some_and(|l| !l.is_empty() && !l.contains("coderhelm"))
}

/// The first person who wrote something with substance after `since` (an
/// RFC 3339 time), in PR conversation comments, inline review comments or
/// review bodies. A bare "re-review" doesn't count.
pub fn person_spoke_since(comments: &[serde_json::Value], since: &str) -> Option<String> {
    let since = chrono::DateTime::parse_from_rfc3339(since).ok()?;
    comments.iter().find_map(|c| {
        let at = c["created_at"]
            .as_str()
            .or_else(|| c["submitted_at"].as_str())
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())?;
        let body = c["body"].as_str().unwrap_or("").trim();
        (at > since && is_person(&c["user"]) && !body.is_empty() && !is_bare_command(body))
            .then(|| c["user"]["login"].as_str().unwrap_or("").to_string())
    })
}

/// Everything holding the merge, by name, on every route: findings that hold
/// it, earlier threads still open (path, line, body), unconfirmed facts that
/// block, a failed verification, findings CoderHelm fixes on its own PR, and
/// items a held earlier verdict keeps. What `Verdict` is made from.
pub fn named_holding(
    findings: &[Finding],
    open_threads: &[(String, u64, String)],
    unconfirmed: &[super::review_agent::UnconfirmedFact],
    verify_failed: bool,
    own_open: &[String],
    kept: &[String],
) -> Vec<Holding> {
    let mut out: Vec<Holding> = Vec::new();
    for f in findings.iter().filter(|f| f.holds_merge()) {
        let kind = if f.evidence() == Evidence::Question {
            HoldKind::Question
        } else {
            HoldKind::Defect
        };
        let name = if f.line > 0 {
            format!("`{}:{}` {}", f.file, f.line, f.title)
        } else if f.file.is_empty() {
            f.title.clone()
        } else {
            format!("`{}` {}", f.file, f.title)
        };
        out.push(Holding::new(kind, name));
    }
    for (path, line, body) in open_threads {
        out.push(Holding::new(
            HoldKind::EarlierThread,
            super::own_review::thread_line(path, *line, body),
        ));
    }
    // Unconfirmed facts are FYI notes on the approval, never a hold.
    let _ = unconfirmed;
    if verify_failed {
        out.push(Holding::new(
            HoldKind::Verification,
            "The verification run failed",
        ));
    }
    for line in own_open {
        let listed = out.iter().any(|h| h.name == *line)
            || findings
                .iter()
                .any(|f| f.holds_merge() && line.contains(&f.title));
        if !listed {
            out.push(Holding::new(HoldKind::OwnFinding, line.clone()));
        }
    }
    for name in kept {
        if !out.iter().any(|h| h.name == *name) {
            out.push(Holding::new(HoldKind::KeptFromEarlier, name.clone()));
        }
    }
    out
}

/// The verdict after rule 2, and what the review says about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stable {
    pub verdict: &'static str,
    /// A line for the review body: why the verdict changed on this commit,
    /// or why it didn't. Fixed words from structured facts; a model's reason
    /// only ever as its own sentence in its own paragraph.
    pub note: Option<String>,
    /// The verdict this one replaced on the same commit.
    pub flipped_from: Option<String>,
    /// The new verdict differed and was refused (an identical fingerprint).
    pub held: bool,
    /// What holds the merge from the earlier review when its block was held.
    pub kept: Vec<String>,
}

/// Rule 2. `previous` is CoderHelm's finished review of this same head, if
/// any; `changed` is what differs from its fingerprint (empty: identical).
/// `explicit`: a person asked, so a verdict that didn't change says so.
///
/// Only an identical fingerprint holds an earlier verdict, and only a block
/// that still names what holds it can be held: nothing named left means no
/// block. With anything changed the verdict is what holds the merge now.
pub fn stabilize(
    previous: Option<&ReviewRecord>,
    verdict: &'static str,
    findings: &mut [Finding],
    changed: &[Changed],
    change_of_mind: &str,
    explicit: bool,
) -> Stable {
    let unchanged = Stable {
        verdict,
        note: None,
        flipped_from: None,
        held: false,
        kept: Vec::new(),
    };
    let Some(prev) = previous else {
        return unchanged;
    };
    let before = super::review_verdict::binary(&prev.verdict);
    let short = &prev.head_sha[..prev.head_sha.len().min(7)];
    if before == verdict {
        if !explicit {
            return unchanged;
        }
        let what = if changed.is_empty() {
            "The code and description are the same.".to_string()
        } else {
            format!(
                "Since then {} changed; the verdict didn't.",
                in_words(changed)
            )
        };
        return Stable {
            note: Some(format!(
                "**Unchanged since my last review of `{short}`: {}.** {what}",
                human(verdict)
            )),
            ..unchanged
        };
    }
    let flip = |what: String| Stable {
        verdict,
        note: Some(match common::prose::one_whole_sentence(change_of_mind) {
            Some(reason) => format!(
                "**Changed my verdict on `{short}`** from {} to {}. {what}\n\n{reason}",
                human(before),
                human(verdict)
            ),
            None => format!(
                "**Changed my verdict on `{short}`** from {} to {}. {what}",
                human(before),
                human(verdict)
            ),
        }),
        flipped_from: Some(before.to_string()),
        held: false,
        kept: Vec::new(),
    };
    if !changed.is_empty() {
        return flip(format!("What changed: {}.", in_words(changed)));
    }
    if before == "APPROVE" {
        for f in findings.iter_mut().filter(|f| f.holds_merge()) {
            demote(
                f,
                "Raised on a second look at a commit I already approved, with nothing new since: a note, not a block.",
            );
        }
        return Stable {
            verdict: before,
            note: Some(format!(
                "Same commit as my last review (`{short}`) and nothing it rests on changed, so \
                 my earlier verdict (approve) stands. Anything new below is a note."
            )),
            flipped_from: None,
            held: true,
            kept: Vec::new(),
        };
    }
    // A held block names what holds it, or it isn't a block.
    if prev.holding.is_empty() {
        return flip("Nothing named holds the merge.".to_string());
    }
    Stable {
        verdict: before,
        note: Some(format!(
            "Same commit as my last review (`{short}`) and nothing it rests on changed, so my \
             earlier verdict (request changes) stands. Reply with what changed, or push a fix."
        )),
        flipped_from: None,
        held: true,
        kept: prev.holding.clone(),
    }
}

fn human(verdict: &str) -> &'static str {
    if verdict == "APPROVE" {
        "approve"
    } else {
        "request changes"
    }
}

#[cfg(test)]
mod tests {
    use super::super::review_fingerprint::{self as fp, Fingerprint};
    use super::super::review_verdict::Verdict;
    use super::*;

    fn rec(head: &str, verdict: &str, at: &str) -> ReviewRecord {
        ReviewRecord {
            verdict: verdict.into(),
            posted_as: verdict.into(),
            head_sha: head.into(),
            created_at: at.into(),
            body: "review".into(),
            fingerprint: None,
            holding: Vec::new(),
        }
    }

    fn defect(file: &str, line: u64, severity: &str, category: &str) -> Finding {
        Finding {
            file: file.into(),
            line,
            severity: severity.into(),
            category: category.into(),
            title: "t".into(),
            body: "b".into(),
            evidence: Some("verified-defect".into()),
            ..Default::default()
        }
    }

    #[test]
    fn an_automatic_trigger_on_a_reviewed_head_returns_the_verdict() {
        let records = vec![rec("h1", "APPROVE", "2026-10-01T00:00:00Z")];
        assert_eq!(
            start("synchronize", "h1", "", &records),
            Start::ReturnExisting {
                verdict: "APPROVE".into()
            }
        );
        assert_eq!(start("labeled", "h2", "", &records), Start::Review);
        // A person asked, or a run needs the verdict: it runs.
        assert_eq!(start("reply", "h1", "", &records), Start::Review);
        assert_eq!(start("rerequest", "h1", "", &records), Start::Review);
        // A description edit that answers the review is a person's ask: the
        // same commit is reviewed again with the new description.
        assert_eq!(start("description", "h1", "", &records), Start::Review);
        assert_eq!(start("run", "h1", "", &records), Start::Review);
        // A review that didn't finish isn't a verdict.
        let mut failed = rec("h1", "REQUEST_CHANGES", "2026-10-01T00:00:00Z");
        failed.body = format!("{} (x)", super::super::review_sweep::LEGACY_FAILED_PREFIX);
        assert_eq!(start("synchronize", "h1", "", &[failed]), Start::Review);
    }

    #[test]
    fn verdict_claims_are_per_head_and_trigger_kind() {
        let auto = verdict_claim_sk("o", "r", 7, "h1", "synchronize", "head#h1");
        assert_eq!(auto, "REVIEWVERDICT#o/r#000007#h1#auto");
        assert_eq!(
            auto,
            verdict_claim_sk("o", "r", 7, "h1", "labeled", "head#h1")
        );
        let ask = verdict_claim_sk("o", "r", 7, "h1", "reply", "reply#9");
        assert_ne!(ask, auto);
        assert_ne!(
            ask,
            verdict_claim_sk("o", "r", 7, "h1", "reply", "reply#10")
        );
        assert_ne!(auto, verdict_claim_sk("o", "r", 7, "h2", "synchronize", ""));
    }

    #[test]
    fn the_head_claim_is_the_gateways_key() {
        assert_eq!(
            head_claim_sk("o", "r", 7, "abc"),
            "REVIEWCLAIM#o/r#000007#abc"
        );
        assert!(takes_head_claim("reply") && takes_head_claim("rerequest"));
        assert!(takes_head_claim("description"));
        assert!(!takes_head_claim("synchronize") && !takes_head_claim("reconcile"));
    }

    /// A person's re-review ask always gets a visible answer: an unchanged
    /// verdict says it is unchanged, from structured facts.
    #[test]
    fn an_explicit_ask_with_the_same_verdict_says_it_is_unchanged() {
        let prev = rec("h1aaaaaaa", "APPROVE", "2026-10-01T00:00:00Z");
        let s = stabilize(Some(&prev), "APPROVE", &mut [], &[], "", true);
        assert_eq!(s.verdict, "APPROVE");
        assert_eq!(
            s.note.unwrap(),
            "**Unchanged since my last review of `h1aaaaa`: approve.** The code and description are the same."
        );
        let s = stabilize(
            Some(&prev),
            "APPROVE",
            &mut [],
            &[Changed::People],
            "",
            true,
        );
        assert_eq!(
            s.note.unwrap(),
            "**Unchanged since my last review of `h1aaaaa`: approve.** Since then what people wrote changed; the verdict didn't."
        );
    }

    #[test]
    fn an_ask_that_meets_a_running_review_waits_for_it_then_runs() {
        let msg = crate::models::ReviewMessage {
            trigger: "reply".into(),
            dedup_key: "reply#42".into(),
            ..Default::default()
        };
        let once = deferred_ask(&msg).unwrap();
        assert_eq!(once.dedup_key, "reply#42#waited1");
        assert_eq!(once.trigger, "reply");
        let mut last = once;
        for _ in 1..ASK_MAX_WAITS {
            last = deferred_ask(&last).unwrap();
        }
        assert_eq!(last.dedup_key, format!("reply#42#waited{ASK_MAX_WAITS}"));
        assert!(deferred_ask(&last).is_none(), "after the last wait it runs");
    }

    #[test]
    fn rounds_count_distinct_reviewed_heads() {
        let records = vec![
            rec("h3", "REQUEST_CHANGES", "3"),
            rec("h2", "REQUEST_CHANGES", "2"),
            rec("h2", "REQUEST_CHANGES", "2b"),
            rec("h1", "APPROVE", "1"),
        ];
        assert_eq!(
            round(&records, "h4"),
            Round {
                number: 4,
                previous_head: Some("h3".into())
            }
        );
        assert_eq!(round(&records, "h3").number, 3);
        assert_eq!(round(&[], "h1").number, 1);
    }

    #[test]
    fn a_defect_holds_the_merge_whatever_its_rated_severity() {
        for (sev, holds) in [
            ("blocking", true),
            ("high", true),
            ("critical", true),
            ("medium", true),
            ("low", true),
            ("nit", false),
            ("", true),
        ] {
            assert_eq!(defect("a.rs", 1, sev, "bug").holds_merge(), holds, "{sev}");
        }
    }

    #[test]
    fn nits_hardening_tracked_and_base_failures_are_notes() {
        let mut fs = vec![
            defect("a.rs", 1, "high", "style"),
            defect("a.rs", 2, "blocking", "logging"),
            defect("a.rs", 3, "high", "hardening"),
            Finding {
                tracked: Some(true),
                ..defect("a.rs", 4, "high", "bug")
            },
            Finding {
                caused_by_diff: Some(false),
                ..defect("a.rs", 5, "high", "bug")
            },
            Finding {
                title: "integration-tests job fails".into(),
                ..defect("a.rs", 6, "high", "bug")
            },
            defect("a.rs", 7, "high", "bug"),
        ];
        // Style, logging and hardening are notes by the classifier already;
        // the policy makes the tracked, uncaused and base-failing ones notes.
        let n = apply_blocking_policy(&mut fs, &["integration-tests".to_string()]);
        assert_eq!(n, 3);
        let holding: Vec<u64> = fs
            .iter()
            .filter(|f| f.holds_merge())
            .map(|f| f.line)
            .collect();
        assert_eq!(holding, vec![7]);
        assert!(fs[5].body.contains("fails on the base branch too"));
    }

    #[test]
    fn failing_checks_reads_check_runs() {
        let v = serde_json::json!({"check_runs": [
            {"name": "lint", "conclusion": "failure"},
            {"name": "build", "conclusion": "success"},
            {"name": "e2e", "conclusion": "timed_out"},
            {"name": "deploy", "status": "in_progress", "conclusion": null}
        ]});
        assert_eq!(failing_checks(&v), vec!["lint", "e2e"]);
    }

    #[test]
    fn late_blockers_on_unchanged_lines_are_held_as_notes() {
        let changed: HashMap<String, HashSet<u64>> =
            [("a.rs".to_string(), HashSet::from([40, 41]))].into();
        let r3 = Round {
            number: 3,
            previous_head: Some("h2".into()),
        };
        let mut fs = vec![
            defect("a.rs", 42, "high", "bug"),  // near a changed line: stays
            defect("a.rs", 200, "high", "bug"), // unchanged: held
            defect("b.rs", 5, "high", "bug"),   // unchanged file: held
            defect("b.rs", 9, "high", "bug"),   // an earlier thread: stays
            defect("b.rs", 80, "nit", "bug"),   // a note anyway: stays
            Finding {
                prompted_by_reply: Some(true),
                ..defect("b.rs", 120, "high", "bug")
            }, // a person's reply made it relevant: stays
        ];
        let threads = vec![("b.rs".to_string(), 10)];
        let late = hold_late_blockers(
            &mut fs,
            &UpFront {
                round: &r3,
                changed_since_previous: Some(&changed),
                earlier_threads: &threads,
                person_spoke: true,
            },
        );
        assert_eq!(
            late.held.iter().map(|f| f.line).collect::<Vec<_>>(),
            vec![200, 5]
        );
        assert!(late.held.iter().all(|f| !f.holds_merge()));
        assert!(late.self_lesson);
        assert_eq!(fs.len(), 4);

        // Round 2: held, but no lesson. First round or unknown diff: nothing.
        let r2 = Round {
            number: 2,
            previous_head: Some("h1".into()),
        };
        let mut fs = vec![defect("b.rs", 5, "high", "bug")];
        let late = hold_late_blockers(
            &mut fs,
            &UpFront {
                round: &r2,
                changed_since_previous: Some(&changed),
                earlier_threads: &[],
                person_spoke: false,
            },
        );
        assert_eq!(late.held.len(), 1);
        assert!(!late.self_lesson);
        let mut fs = vec![defect("b.rs", 5, "high", "bug")];
        let late = hold_late_blockers(
            &mut fs,
            &UpFront {
                round: &r2,
                changed_since_previous: None,
                earlier_threads: &[],
                person_spoke: false,
            },
        );
        assert!(late.held.is_empty() && fs.len() == 1);
        // Without a person's reply, "prompted by a reply" doesn't excuse it.
        let mut fs = vec![Finding {
            prompted_by_reply: Some(true),
            ..defect("b.rs", 5, "high", "bug")
        }];
        let late = hold_late_blockers(
            &mut fs,
            &UpFront {
                round: &r2,
                changed_since_previous: Some(&changed),
                earlier_threads: &[],
                person_spoke: false,
            },
        );
        assert_eq!(late.held.len(), 1);
    }

    #[test]
    fn changed_lines_are_added_and_deleted_lines_only() {
        let compare = serde_json::json!({"files": [
            {"filename": "a.rs", "patch": "@@ -10,4 +10,4 @@ fn x()\n ctx\n-old\n+new\n ctx2\n+added"},
            {"filename": "big.json"}
        ]});
        let c = changed_lines(&compare);
        assert_eq!(c["a.rs"], HashSet::from([11, 13]));
        assert!(c["big.json"].contains(&WHOLE_FILE));
        let f = defect("big.json", 9000, "high", "bug");
        assert!(on_changed_lines(&f, &c));
    }

    #[test]
    fn the_prompt_names_the_earlier_verdict_and_the_round() {
        assert_eq!(
            prompt_section(
                None,
                &Round {
                    number: 1,
                    previous_head: None
                }
            ),
            ""
        );
        let prev = rec("h1", "APPROVE", "t");
        let s = prompt_section(
            Some(&prev),
            &Round {
                number: 3,
                previous_head: Some("abcdef123".into()),
            },
        );
        assert!(s.contains("this exact commit: approve"));
        assert!(s.contains("change_of_mind"));
        assert!(s.contains("Review round 3"));
        assert!(s.contains("`abcdef1`"));
    }

    #[test]
    fn bare_rereview_asks_carry_no_new_facts() {
        for t in [
            "@coderhelm re-review",
            "@coderhelm please review again",
            "@coderhelm ptal",
            "@coderhelm rerun the review please",
        ] {
            assert!(is_bare_command(t), "{t}");
        }
        for t in [
            "@coderhelm re-review, the API change is deployed",
            "the null case can't happen: the caller filters it",
        ] {
            assert!(!is_bare_command(t), "{t}");
        }
    }

    #[test]
    fn person_spoke_since_ignores_bots_old_comments_and_bare_asks() {
        let c = serde_json::json!([
            {"user": {"login": "dev", "type": "User"}, "body": "old point", "created_at": "2026-10-01T00:00:00Z"},
            {"user": {"login": "coderhelm[bot]", "type": "Bot"}, "body": "x", "created_at": "2026-10-03T00:00:00Z"},
            {"user": {"login": "dev", "type": "User"}, "body": "@coderhelm re-review", "created_at": "2026-10-03T00:00:00Z"}
        ]);
        let c = c.as_array().unwrap();
        assert_eq!(person_spoke_since(c, "2026-10-02T00:00:00Z"), None);
        let mut more = c.clone();
        more.push(serde_json::json!({"user": {"login": "lead", "type": "User"}, "body": "the flag is on in prod now", "submitted_at": "2026-10-04T00:00:00Z"}));
        assert_eq!(
            person_spoke_since(&more, "2026-10-02T00:00:00Z").as_deref(),
            Some("lead")
        );
    }

    #[test]
    fn a_verdict_never_flips_on_an_identical_fingerprint() {
        let prev = rec("h1aaaaaaa", "APPROVE", "2026-10-01T00:00:00Z");
        let mut fs = vec![defect("a.rs", 1, "high", "bug")];
        let s = stabilize(Some(&prev), "REQUEST_CHANGES", &mut fs, &[], "", true);
        assert_eq!(s.verdict, "APPROVE");
        assert!(s.held);
        assert!(!fs[0].holds_merge(), "the new blocker became a note");
        assert!(s.note.unwrap().contains("earlier verdict (approve) stands"));
        // A block held this way still names what holds it.
        let mut prev_rc = rec("h1aaaaaaa", "REQUEST_CHANGES", "2026-10-01T00:00:00Z");
        prev_rc.holding = vec!["`a.rs:3` Null deref".to_string()];
        let s = stabilize(Some(&prev_rc), "APPROVE", &mut [], &[], "noise.", false);
        assert_eq!(s.verdict, "REQUEST_CHANGES");
        assert!(s.held);
        assert_eq!(s.kept, prev_rc.holding);
        assert!(!s.note.unwrap().contains("noise"));
    }

    /// The live failure: a block with nothing named holding it can't be held.
    #[test]
    fn a_block_with_nothing_named_clears_even_on_an_identical_fingerprint() {
        let prev = rec("a1b2c3daaa", "REQUEST_CHANGES", "2026-10-01T00:00:00Z");
        let s = stabilize(Some(&prev), "APPROVE", &mut [], &[], "", true);
        assert_eq!(s.verdict, "APPROVE");
        assert!(s.note.unwrap().contains("Nothing named holds the merge"));
    }

    #[test]
    fn with_anything_changed_the_verdict_follows_what_holds_the_merge() {
        let mut prev = rec("h1aaaaaaa", "REQUEST_CHANGES", "2026-10-01T00:00:00Z");
        prev.holding = vec!["x".to_string()];
        for changed in [
            vec![Changed::Description],
            vec![Changed::Threads],
            vec![Changed::Results],
            vec![Changed::People],
            vec![Changed::NoRecord],
        ] {
            let s = stabilize(Some(&prev), "APPROVE", &mut [], &changed, "", false);
            assert_eq!(s.verdict, "APPROVE", "{changed:?}");
            assert_eq!(s.flipped_from.as_deref(), Some("REQUEST_CHANGES"));
        }
        // No earlier review of this commit: nothing to compare.
        let s = stabilize(None, "REQUEST_CHANGES", &mut [], &[], "", true);
        assert_eq!(s.note, None);
    }

    /// Whatever a model writes, it is never inside a template sentence: a
    /// finished sentence of it stands in its own paragraph, or it isn't shown.
    #[test]
    fn model_text_is_never_spliced_into_a_sentence() {
        let mut prev = rec("h1aaaaaaa", "APPROVE", "2026-10-01T00:00:00Z");
        prev.holding = vec![];
        let model = "The reply concludes nothing blocks merge.";
        for (verdict, explicit) in [("REQUEST_CHANGES", true), ("APPROVE", true)] {
            let note = stabilize(
                Some(&prev),
                verdict,
                &mut [],
                &[Changed::People],
                model,
                explicit,
            )
            .note
            .unwrap();
            assert!(!note.contains("because"), "{note}");
            assert!(!note.contains("doesn't change it"), "{note}");
            if note.contains(model) {
                assert!(note.ends_with(&format!("\n\n{model}")), "{note}");
            }
        }
        // An unfinished thought isn't shown at all.
        let note = stabilize(
            Some(&prev),
            "REQUEST_CHANGES",
            &mut [],
            &[Changed::People],
            "because the reply",
            true,
        )
        .note
        .unwrap();
        assert!(!note.contains("because the reply"), "{note}");
    }

    /// The source shape behind the garbled note: no format string in the code
    /// that writes posted review text puts a value in the middle of words when
    /// that value is a model's free text.
    #[test]
    fn no_template_embeds_a_models_reason() {
        let spliced = [
            "{because}",
            "{} doesn't change it",
            concat!("info.trim", "()"),
            concat!(": {", "why}"),
        ];
        for (name, src) in [
            ("review_policy.rs", include_str!("review_policy.rs")),
            ("review_verdict.rs", include_str!("review_verdict.rs")),
        ] {
            let code = src.split("#[cfg(test)]").next().unwrap();
            for shape in spliced {
                assert!(!code.contains(shape), "{name} still has {shape}");
            }
        }
        // The model's reason for a changed mind reaches posted text one way:
        // as a whole sentence on its own.
        let policy = include_str!("review_policy.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        assert!(policy.contains(concat!("one_whole_sentence(change", "_of_mind)")));
        // A reply's reason is prompt context for the review it starts, never
        // what a posted note says changed: stabilize gets structured facts.
        let pr = include_str!("review_pr.rs");
        assert!(pr.contains(concat!(
            "&changed_since,\n        &output.change",
            "_of_mind"
        )));
        assert!(!pr.contains(concat!("a reply on this PR changed", " the facts")));
    }

    /// run_once needs GitHub and the database to run, so its order is read
    /// from its source: the answer comes before any review work, and the
    /// posted verdict comes from what holds the merge.
    #[test]
    fn run_once_answers_before_reviewing_and_posts_only_the_named_verdict() {
        let src = include_str!("review_pr.rs");
        let at = |needle: &str| src.find(needle).unwrap_or_else(|| panic!("{needle}"));
        let answer = at(concat!("review_policy::answers_from", "_standing("));
        assert!(answer < at(concat!(".open_check(state, &github", ", &msg, &head_sha")));
        assert!(answer < at(concat!("review_lessons::settle_before", "_review(")));
        assert!(at(concat!("live_job.follow", "_head(")) < answer);
        let named = at(concat!("review_policy::named", "_holding("));
        assert!(named < at(concat!("&final", "_verdict,\n        ")));
        assert!(src.contains(concat!("let verdict = final", "_verdict.event();")));
        assert!(named < at(concat!("note_review", "_state(\n        state")));
    }

    #[test]
    fn a_reask_on_an_identical_fingerprint_is_answered_from_the_verdict() {
        assert!(answers_from_standing("reply", &[], true));
        assert!(answers_from_standing("rerequest", &[], true));
        // Anything changed, or no review of this commit: reviewed.
        assert!(!answers_from_standing("reply", &[Changed::People], true));
        assert!(!answers_from_standing(
            "rerequest",
            &[Changed::NoRecord],
            true
        ));
        assert!(!answers_from_standing("reply", &[], false));
        // The routes with their own reason to run.
        for t in ["description", "reconcile", "sweep", "run", "synchronize"] {
            assert!(!answers_from_standing(t, &[], true), "{t}");
        }
        let text = standing_answer(
            "a1b2c3daaa",
            "REQUEST_CHANGES",
            &["`a.rs:3` Null deref".to_string()],
        );
        assert!(
            text.starts_with("**Unchanged since my last review of `a1b2c3d`: request changes.**")
        );
        assert!(text.contains("- `a.rs:3` Null deref"));
        assert!(!standing_answer("a1b2c3daaa", "APPROVE", &[]).contains("Still holding"));
    }

    #[test]
    fn an_automatic_trigger_reviews_again_only_for_a_changed_description() {
        let mut r = rec("h1", "REQUEST_CHANGES", "2026-10-01T00:00:00Z");
        r.fingerprint = Some(super::super::review_fingerprint::Fingerprint {
            head: "h1".into(),
            description: "d1".into(),
            ..Default::default()
        });
        let records = vec![r];
        assert_eq!(
            start("ready_for_review", "h1", "d1", &records),
            Start::ReturnExisting {
                verdict: "REQUEST_CHANGES".into()
            }
        );
        assert_eq!(
            start("ready_for_review", "h1", "d2", &records),
            Start::DescriptionChanged
        );
    }

    // ── Replays of what a person saw, through the same functions run_once uses ──

    /// A review record of `head`, its fingerprint as it was, naming `holding`.
    fn stored(verdict: &str, holding: &[&str], fp: &Fingerprint) -> ReviewRecord {
        let mut r = rec(&fp.head, verdict, "2026-11-04T00:00:00Z");
        r.fingerprint = Some(fp.clone());
        r.holding = holding.iter().map(|h| h.to_string()).collect();
        r
    }

    fn state_of(open: bool) -> Fingerprint {
        Fingerprint {
            head: "a1b2c3d0000".into(),
            description: fp::description_part("Applies with the deploy note below."),
            threads: Some(fp::threads_part([
                ("T_1".to_string(), !open),
                ("T_2".to_string(), !open),
            ])),
            results: Some(fp::results_part(
                &serde_json::json!({"check_runs": [{"name": "plan", "conclusion": "success"}]}),
                &[],
                "",
            )),
            people: Some(fp::people_part(&[])),
        }
    }

    /// What run_once does from the model's verdict to the posted heading.
    fn review(
        prev: &ReviewRecord,
        now: &Fingerprint,
        findings: &mut [Finding],
        open: &[(String, u64, String)],
    ) -> (Verdict, Stable, String) {
        let proposed = super::super::review_agent::decide_verdict(findings, open.len(), &[], false);
        let changed = fp::changes(prev.fingerprint.as_ref(), now);
        let stable = stabilize(Some(prev), proposed, findings, &changed, "", false);
        let verdict =
            Verdict::from_holding(named_holding(findings, open, &[], false, &[], &stable.kept));
        let heading = super::super::review_agent::verdict_heading(&verdict, 0);
        (verdict, stable, heading)
    }

    /// The live PR: description edited, both threads settled, no new findings,
    /// same commit. It posted "something holds the merge" and stayed stuck.
    #[test]
    fn replay_description_edited_and_both_threads_settled_clears() {
        let before = state_of(true);
        let prev = stored(
            "REQUEST_CHANGES",
            &["`deploy.tf:3` State key is shared", "`deploy.tf:9` No lock"],
            &before,
        );
        let mut now = state_of(false);
        now.description = fp::description_part("Applies on merge; the workflow shows no step.");
        let (verdict, stable, heading) = review(&prev, &now, &mut [], &[]);
        assert_eq!(verdict.event(), "APPROVE");
        assert_eq!(stable.flipped_from.as_deref(), Some("REQUEST_CHANGES"));
        let note = stable.note.unwrap();
        assert!(note.contains("What changed: the description and the review threads."));
        assert!(heading.starts_with("✅ **Approved**"));
        assert!(!heading.contains("Changes requested"));
        // An APPROVE replaces the standing request for changes on GitHub, and
        // it always posts when it clears a block.
        let d = super::super::review_verdict::decide(
            super::super::review_verdict::Input::Review {
                verdict: verdict.event(),
                has_news: false,
                explicit: false,
                already_answered: false,
            },
            &super::super::review_verdict::Context {
                standing: Some(super::super::review_verdict::StandingReview {
                    verdict: "REQUEST_CHANGES".into(),
                    failed: false,
                    head_sha: before.head.clone(),
                }),
                self_authored: false,
            },
        );
        assert_eq!(
            d,
            super::super::review_verdict::Decision::Post {
                event: "APPROVE",
                dismiss_stale: false
            }
        );
    }

    #[test]
    fn replay_a_settled_thread_with_another_holding_finding_still_blocks_and_names_it() {
        let before = state_of(true);
        let prev = stored("REQUEST_CHANGES", &["old"], &before);
        let mut now = state_of(true);
        now.threads = Some(fp::threads_part([
            ("T_1".to_string(), true),
            ("T_2".to_string(), false),
        ]));
        let open = vec![(
            "deploy.tf".to_string(),
            9,
            "**No lock on the state** (bug)".to_string(),
        )];
        let (verdict, _, heading) = review(&prev, &now, &mut [], &open);
        assert_eq!(verdict.event(), "REQUEST_CHANGES");
        assert!(heading.contains("1 earlier requested change still open"));
        assert!(heading.contains("No lock on the state"), "{heading}");
        assert!(heading.contains("deploy.tf:9"), "{heading}");
    }

    #[test]
    fn replay_an_identical_fingerprint_keeps_a_real_block_named_against_model_noise() {
        let state = state_of(true);
        let prev = stored(
            "REQUEST_CHANGES",
            &["`deploy.tf:3` State key is shared"],
            &state,
        );
        // The model now finds nothing: noise. The block stands, named.
        let (verdict, stable, heading) = review(&prev, &state, &mut [], &[]);
        assert_eq!(verdict.event(), "REQUEST_CHANGES");
        assert!(stable.held);
        assert!(
            heading.contains("`deploy.tf:3` State key is shared"),
            "{heading}"
        );
    }

    /// An older record has no fingerprint: changed, reviewed afresh, and it
    /// clears when nothing holds.
    #[test]
    fn replay_an_old_record_without_a_fingerprint_counts_as_changed() {
        let mut prev = rec("a1b2c3d0000", "REQUEST_CHANGES", "2026-11-04T00:00:00Z");
        prev.fingerprint = None;
        let now = state_of(false);
        let changed = fp::changes(prev.fingerprint.as_ref(), &now);
        assert_eq!(changed, vec![Changed::NoRecord]);
        assert!(!answers_from_standing("reply", &changed, true));
        let (verdict, _, _) = review(&prev, &now, &mut [], &[]);
        assert_eq!(verdict.event(), "APPROVE");
    }

    /// A re-review ask on an identical fingerprint: no model review, the same
    /// verdict in the reply, the threads as they are.
    #[test]
    fn replay_a_rereview_ask_with_nothing_changed_stays_as_it_was() {
        let state = state_of(true);
        let prev = stored(
            "REQUEST_CHANGES",
            &["`deploy.tf:3` State key is shared"],
            &state,
        );
        let changed = fp::changes(prev.fingerprint.as_ref(), &state);
        assert!(answers_from_standing("rerequest", &changed, true));
        let text = standing_answer(&prev.head_sha, &prev.verdict, &prev.holding);
        assert!(text.contains("`a1b2c3d`: request changes"));
        assert!(text.contains("`deploy.tf:3` State key is shared"));
        // A bare ask is no new words either.
        let mut with_ask = state.clone();
        with_ask.people = Some(fp::people_part(&[serde_json::json!({
            "user": {"login": "dev", "type": "User"},
            "body": "@coderhelm re-review",
        })]));
        assert!(fp::changes(prev.fingerprint.as_ref(), &with_ask).is_empty());
    }

    /// The reported comment: same head, same description, a person's comment
    /// naming a blocker, a reading that says the opposite. One answer, no
    /// formal review, nothing garbled, and the check keeps its verdict.
    #[tokio::test]
    async fn replay_a_comment_naming_a_blocker_gets_one_answer_and_the_verdict_stands() {
        use super::super::review_agent::VerdictChange;
        use super::super::review_verdict::{decide, Context, Decision, Input, StandingReview};
        use common::review_check as rc;
        let state = state_of(true);
        let prev = stored(
            "REQUEST_CHANGES",
            &["`deploy.tf:3` State key is shared"],
            &state,
        );
        let mut now = state.clone();
        now.people = Some(fp::people_part(&[serde_json::json!({
            "user": {"login": "dev", "type": "User"},
            "body": "The deploy note in the description is the blocker.",
        })]));
        let changed = fp::changes(prev.fingerprint.as_ref(), &now);
        assert_eq!(changed, vec![Changed::People]);
        let standing = StandingReview {
            verdict: "REQUEST_CHANGES".into(),
            failed: false,
            head_sha: state.head.clone(),
        };
        let ctx = Context {
            standing: Some(standing.clone()),
            self_authored: false,
        };
        // The model read "nothing blocks": a reading can't clear a block.
        let d = decide(
            Input::Reply {
                change: VerdictChange::Clear,
                more_than_words: fp::more_than_words(&changed),
            },
            &ctx,
        );
        assert_eq!(d, Decision::Nothing);
        assert!(super::super::review_verdict::reply_disagrees(
            VerdictChange::Clear,
            &standing
        ));
        let note = verdict_stands_note(&state.head, &standing.verdict);
        assert_eq!(
            note,
            "My formal verdict on `a1b2c3d` stays: request changes. A comment doesn't change it; a push, an edit of the description, or an answer in a finding's thread does."
        );
        // Same reading over an approval can't add a block either.
        let approved = Context {
            standing: Some(StandingReview {
                verdict: "APPROVE".into(),
                ..standing.clone()
            }),
            self_authored: false,
        };
        assert_eq!(
            decide(
                Input::Reply {
                    change: VerdictChange::Block,
                    more_than_words: false
                },
                &approved
            ),
            Decision::Nothing
        );
        // The check: finished verdict, reply shown, back to the verdict.
        let gh = rc::fake::FakeChecks::default();
        let t = rc::Target {
            installation: 1,
            owner: "o",
            repo: "r",
            pr: 7,
            head: &state.head,
        };
        let v = rc::take_over_or_open(&gh, &t, "head#x", "Reading", "u")
            .await
            .unwrap();
        rc::close(
            &gh,
            &t,
            v,
            &rc::Close {
                conclusion: "neutral",
                title: "Changes requested: 1 finding".into(),
                summary: "s".into(),
                details_url: None,
            },
        )
        .await;
        rc::queue_work(&gh, &t, rc::Work::Reply, "reply#9", 0, "u").await;
        assert_eq!(
            gh.shown(&state.head)["output"]["title"],
            "Reading your reply on a1b2c3d"
        );
        let id = rc::start(&gh, &t, "reply#9", "Reading your reply", "session")
            .await
            .unwrap();
        rc::close_restoring(
            &gh,
            &t,
            id,
            &rc::Close {
                conclusion: "neutral",
                title: "Answered".into(),
                summary: "Answered.".into(),
                details_url: Some("session".into()),
            },
        )
        .await;
        let shown = gh.shown(&state.head);
        assert_eq!(shown["output"]["title"], "Changes requested: 1 finding");
        assert_eq!(shown["conclusion"], "neutral");
        assert_eq!(shown["details_url"], "session");
    }

    /// What still starts a review: a person's words that settle a thread change
    /// the threads; an edited description; a new push.
    #[test]
    fn replay_the_cases_that_still_review() {
        use super::super::review_agent::VerdictChange;
        use super::super::review_verdict::{decide, Context, Decision, Input, StandingReview};
        let state = state_of(true);
        let ctx = Context {
            standing: Some(StandingReview {
                verdict: "REQUEST_CHANGES".into(),
                failed: false,
                head_sha: state.head.clone(),
            }),
            self_authored: false,
        };
        let starts = |now: &Fingerprint| {
            decide(
                Input::Reply {
                    change: VerdictChange::Clear,
                    more_than_words: fp::more_than_words(&fp::changes(Some(&state), now)),
                },
                &ctx,
            )
        };
        let mut thread = state.clone();
        thread.threads = Some(fp::threads_part([
            ("T_1".to_string(), true),
            ("T_2".to_string(), false),
        ]));
        assert_eq!(starts(&thread), Decision::Review);
        let mut edited = state.clone();
        edited.description = fp::description_part("A new description.");
        assert_eq!(starts(&edited), Decision::Review);
        let mut push = state.clone();
        push.head = "abc".into();
        assert_eq!(starts(&push), Decision::Review);
        assert!(!answers_from_standing("description", &[], true));
        assert_eq!(
            start("synchronize", "another-head", "d", &[]),
            Start::Review
        );
        // An error review on the PR isn't a verdict: a reply replaces it.
        let failed = Context {
            standing: Some(StandingReview {
                verdict: "REQUEST_CHANGES".into(),
                failed: true,
                head_sha: state.head.clone(),
            }),
            self_authored: false,
        };
        assert_eq!(
            decide(
                Input::Reply {
                    change: VerdictChange::None,
                    more_than_words: false
                },
                &failed
            ),
            Decision::Review
        );
    }

    /// Every route to a verdict: nothing named is an approval, anything named
    /// is changes requested and the heading names it.
    #[test]
    fn every_route_through_the_chokepoint_names_what_holds_or_approves() {
        use super::super::review_agent::{verdict_heading, UnconfirmedFact};
        let finding = [defect("a.rs", 3, "high", "bug")];
        let thread = [("b.rs".to_string(), 9, "**Open thing** (bug)".to_string())];
        let fact = [UnconfirmedFact {
            fact: "The vendor returns cents".into(),
            blocks: true,
            why: String::new(),
        }];
        let note_fact = [UnconfirmedFact {
            fact: "Staging has no data".into(),
            blocks: false,
            why: String::new(),
        }];
        let own = ["Own item (`c.rs:1`)".to_string()];
        let kept = ["Kept item".to_string()];
        for f in [&[][..], &finding[..]] {
            for t in [&[][..], &thread[..]] {
                for u in [&[][..], &fact[..], &note_fact[..]] {
                    for v in [false, true] {
                        for o in [&[][..], &own[..]] {
                            for k in [&[][..], &kept[..]] {
                                let holding = named_holding(f, t, u, v, o, k);
                                let verdict = Verdict::from_holding(holding.clone());
                                let heading = verdict_heading(&verdict, 0);
                                if holding.is_empty() {
                                    assert_eq!(verdict.event(), "APPROVE");
                                    assert!(heading.starts_with("✅"));
                                } else {
                                    assert_eq!(verdict.event(), "REQUEST_CHANGES");
                                    for h in &holding {
                                        assert!(heading.contains(&h.name), "{heading}");
                                    }
                                }
                                // An unconfirmed fact never holds: it is an
                                // FYI note on the approval.
                                let _ = u;
                                let empty = !f.is_empty()
                                    || !t.is_empty()
                                    || v
                                    || !o.is_empty()
                                    || !k.is_empty();
                                assert_eq!(!holding.is_empty(), empty);
                            }
                        }
                    }
                }
            }
        }
    }
}
