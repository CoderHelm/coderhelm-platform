//! When a review may hold the merge, and when it may change its mind.
//!
//! Pure rules, applied by `review_pr` after the model and the critic and
//! replayed by the offline evaluation (`review_policy_eval`):
//!
//! 1. One review per head commit: an automatic trigger (push, label, ready)
//!    for a commit CoderHelm already reviewed returns the existing verdict
//!    ([`start`]); posting takes a conditional claim per (PR, head, trigger
//!    kind) so two jobs can't both post ([`verdict_claim_sk`]).
//! 2. Verdict stability: on an unchanged head the verdict never flips
//!    between approve and request-changes without new information (a
//!    person's comment, a reply the formal review must follow); a flip says
//!    why ([`stabilize`]).
//! 3. Blocking policy: only a high-severity defect caused by the diff holds
//!    the merge. Low and medium findings, nits, hardening, follow-ups the PR
//!    already tracks and failures the base branch has too are notes
//!    ([`apply_blocking_policy`]).
//! 4. Blockers up front: after the first reviewed head, a new blocker may
//!    only sit on lines changed since the previous reviewed head (or be one
//!    a person's reply made relevant); any other is a note, and from round 3
//!    on it is also a lesson for the reviewer ([`hold_late_blockers`]).

use super::review_agent::{Evidence, Finding};
use super::review_sweep::ReviewRecord;
use std::collections::{HashMap, HashSet};

/// Who asked for a review.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerKind {
    /// A webhook (push, label, ready for review), a schedule, or a run's own
    /// review of its PR.
    Automatic,
    /// A person asked (a reply, the re-request button, the dashboard), the
    /// stuck-review sweep, or a reply whose conclusion the formal review must
    /// follow.
    Explicit,
}

/// The kind of a review trigger.
pub fn trigger_kind(trigger: &str) -> TriggerKind {
    match trigger {
        "reply" | "rerequest" | "sweep" | "reconcile" => TriggerKind::Explicit,
        _ => TriggerKind::Automatic,
    }
}

/// A person's re-review ask takes the per-commit claim the gateway takes for
/// push reviews, so a push review and a re-review ask of the same commit
/// never both run.
pub fn takes_head_claim(trigger: &str) -> bool {
    matches!(trigger, "reply" | "rerequest")
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
}

/// Rule 1: an automatic trigger for an already-reviewed commit returns the
/// existing verdict. A person's ask runs (and must explain a change of
/// mind, see [`stabilize`]); a run's own review of its PR always runs, the
/// run needs its verdict.
pub fn start(trigger: &str, head: &str, records: &[ReviewRecord]) -> Start {
    if trigger_kind(trigger) == TriggerKind::Explicit || trigger == super::review_pr::TRIGGER_RUN {
        return Start::Review;
    }
    match review_of_head(records, head) {
        Some(r) => Start::ReturnExisting {
            verdict: r.verdict.clone(),
        },
        None => Start::Review,
    }
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

/// Categories that are notes whatever their severity: style and wording
/// nits, log levels, hardening that isn't a defect, follow-up work.
const NOTE_CATEGORIES: &[&str] = &[
    "style",
    "nit",
    "naming",
    "formatting",
    "logging",
    "log-level",
    "docs",
    "documentation",
    "hardening",
    "follow-up",
    "followup",
    "maintainability",
];

fn note_category(category: &str) -> bool {
    let c = category
        .trim()
        .to_ascii_lowercase()
        .replace(['_', ' '], "-");
    NOTE_CATEGORIES.contains(&c.as_str())
}

/// Makes a finding a note: it is still posted, it never holds the merge.
/// `why` is appended to its body.
pub fn demote(f: &mut Finding, why: &str) {
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
    if note_category(&f.category) {
        return Some(
            "A note, not a block: style, logging and hardening points never hold the merge.",
        );
    }
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

/// Rule 3. Only a high-severity defect caused by the diff (or a repo policy
/// such as required tests, which sets `blocking`) holds the merge; everything
/// else that would hold it becomes a note. Low and medium findings already
/// don't hold it ([`Finding::holds_merge`]). Returns how many were demoted.
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

fn is_person(user: &serde_json::Value) -> bool {
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

/// The verdict after rule 2, and what the review says about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stable {
    pub verdict: &'static str,
    /// A line for the review body: why the verdict changed on this commit,
    /// or why it didn't.
    pub note: Option<String>,
    /// The verdict this one replaced on the same commit.
    pub flipped_from: Option<String>,
    /// The new verdict differed and was refused (no new information).
    pub held: bool,
}

/// Rule 2. `previous` is CoderHelm's finished review of this same head, if
/// any. `new_info` names what is new since it (a person's comment, a reply
/// the formal review must follow); without it the earlier verdict stands and
/// new blockers become notes. With it, the change is explained with
/// `change_of_mind` (the reviewer's own sentence) or `new_info`.
pub fn stabilize(
    previous: Option<&ReviewRecord>,
    verdict: &'static str,
    findings: &mut [Finding],
    new_info: Option<&str>,
    change_of_mind: &str,
) -> Stable {
    let unchanged = Stable {
        verdict,
        note: None,
        flipped_from: None,
        held: false,
    };
    let Some(prev) = previous else {
        return unchanged;
    };
    let before = super::review_verdict::binary(&prev.verdict);
    if before == verdict {
        return unchanged;
    }
    let short = &prev.head_sha[..prev.head_sha.len().min(7)];
    if let Some(info) = new_info {
        let why = if change_of_mind.trim().is_empty() {
            info.trim().to_string()
        } else {
            change_of_mind.trim().to_string()
        };
        return Stable {
            verdict,
            note: Some(format!(
                "**Changed my verdict on `{short}`** from {} to {}: {why}",
                human(before),
                human(verdict)
            )),
            flipped_from: Some(before.to_string()),
            held: false,
        };
    }
    if before == "APPROVE" {
        for f in findings.iter_mut().filter(|f| f.holds_merge()) {
            demote(
                f,
                "Raised on a second look at a commit I already approved, with nothing new since: a note, not a block.",
            );
        }
    }
    Stable {
        verdict: before,
        note: Some(format!(
            "Same commit as my last review (`{short}`) and nothing new since, so my earlier verdict \
             ({}) stands.{}",
            human(before),
            if before == "APPROVE" {
                " Anything new below is a note."
            } else {
                " Reply with what changed, or push a fix."
            }
        )),
        flipped_from: None,
        held: true,
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
    use super::*;

    fn rec(head: &str, verdict: &str, at: &str) -> ReviewRecord {
        ReviewRecord {
            verdict: verdict.into(),
            posted_as: verdict.into(),
            head_sha: head.into(),
            created_at: at.into(),
            body: "review".into(),
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
            start("synchronize", "h1", &records),
            Start::ReturnExisting {
                verdict: "APPROVE".into()
            }
        );
        assert_eq!(start("labeled", "h2", &records), Start::Review);
        // A person asked, or a run needs the verdict: it runs.
        assert_eq!(start("reply", "h1", &records), Start::Review);
        assert_eq!(start("rerequest", "h1", &records), Start::Review);
        assert_eq!(start("run", "h1", &records), Start::Review);
        // A review that didn't finish isn't a verdict.
        let mut failed = rec("h1", "REQUEST_CHANGES", "2026-10-01T00:00:00Z");
        failed.body = format!("{} (x)", super::super::review_sweep::LEGACY_FAILED_PREFIX);
        assert_eq!(start("synchronize", "h1", &[failed]), Start::Review);
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
        assert!(!takes_head_claim("synchronize") && !takes_head_claim("reconcile"));
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
    fn only_high_severity_defects_hold_the_merge() {
        for (sev, holds) in [
            ("blocking", true),
            ("high", true),
            ("critical", true),
            ("medium", false),
            ("low", false),
            ("nit", false),
            ("", false),
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
                ..defect("a.rs", 6, "high", "tests")
            },
            defect("a.rs", 7, "high", "bug"),
        ];
        let n = apply_blocking_policy(&mut fs, &["integration-tests".to_string()]);
        assert_eq!(n, 6);
        let holding: Vec<u64> = fs
            .iter()
            .filter(|f| f.holds_merge())
            .map(|f| f.line)
            .collect();
        assert_eq!(holding, vec![7]);
        assert!(fs[0].body.contains("never hold the merge"));
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
            defect("a.rs", 42, "high", "bug"),   // near a changed line: stays
            defect("a.rs", 200, "high", "bug"),  // unchanged: held
            defect("b.rs", 5, "high", "bug"),    // unchanged file: held
            defect("b.rs", 9, "high", "bug"),    // an earlier thread: stays
            defect("b.rs", 80, "medium", "bug"), // a note anyway: stays
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
    fn a_verdict_never_flips_on_an_unchanged_head_without_news() {
        let prev = rec("h1aaaaaaa", "APPROVE", "2026-10-01T00:00:00Z");
        let mut fs = vec![defect("a.rs", 1, "high", "bug")];
        let s = stabilize(Some(&prev), "REQUEST_CHANGES", &mut fs, None, "");
        assert_eq!(s.verdict, "APPROVE");
        assert!(s.held && s.flipped_from.is_none());
        assert!(!fs[0].holds_merge());
        assert!(s.note.unwrap().contains("stands"));

        let prev_rc = rec("h1aaaaaaa", "REQUEST_CHANGES", "2026-10-01T00:00:00Z");
        let s = stabilize(Some(&prev_rc), "APPROVE", &mut [], None, "");
        assert_eq!(s.verdict, "REQUEST_CHANGES");
        assert!(s.held);

        // New information: the flip happens and says why.
        let mut fs = vec![defect("a.rs", 1, "high", "bug")];
        let s = stabilize(
            Some(&prev),
            "REQUEST_CHANGES",
            &mut fs,
            Some("dev's reply"),
            "dev confirmed the field is null for refunds, which this code dereferences",
        );
        assert_eq!(s.verdict, "REQUEST_CHANGES");
        assert_eq!(s.flipped_from.as_deref(), Some("APPROVE"));
        assert!(s.note.unwrap().contains("null for refunds"));
        assert!(fs[0].holds_merge());

        // Same verdict, or no earlier review of this head: nothing to say.
        let s = stabilize(Some(&prev), "APPROVE", &mut [], None, "");
        assert_eq!(s.note, None);
        let s = stabilize(None, "REQUEST_CHANGES", &mut [], None, "");
        assert_eq!((s.verdict, s.held), ("REQUEST_CHANGES", false));
    }
}
