//! What may hold the merge, decided from facts the review already has.
//!
//! A pull request's fix was right by its third commit: CI and the preview's
//! end-to-end run were green and a person's product answers were built.
//! CoderHelm's review still requested changes for six more rounds on two
//! generic findings, "A parallel structure next to an existing one" and "A
//! test out of proportion to the change". The parallel structure never
//! named the function it duplicated, across three rewrites of the file. The
//! test-size finding moved from one test file to the next as each was
//! trimmed, and landed on a test whose cases the review itself had asked to
//! restore. CoderHelm's own fix rounds deferred both as making no
//! progress, and the review kept blocking on them. A person had answered
//! that the path pages should be covered; the work was reverted as out of
//! scope and a later review called it missing.
//!
//! [`judge`] runs on every review, after the findings are graded and before
//! the verdict is made. Each rule makes a finding a note with its reason,
//! never drops it silently:
//!
//! 1. **A blocking finding is actionable** ([`unactionable`]): it names its
//!    file and line, and a smallest-change finding names the existing code
//!    it duplicates or what its size is out of proportion to.
//! 2. **A finding doesn't move to another file** ([`moved`]): a generic
//!    finding raised before on another file was about that file's code; the
//!    same words on the next file are a new claim the review hasn't shown.
//! 3. **No opposite requests** ([`opposite`]): a finding that asks to take
//!    out what an earlier finding on the same file asked to add (or the
//!    other way round) doesn't hold; the review names the request it would
//!    undo.
//! 4. **A finding CoderHelm's own rounds deferred holds no more**
//!    ([`deferred`]), unless it is on code changed since the previous
//!    review: new code is new evidence.
//! 5. **A person's answer decides scope** ([`against_people`]): a finding
//!    that calls work out of scope a person asked for is a note; one that
//!    calls missing what a person asked for says so.
//! 6. **Approve when the pull request does what the ticket asks**
//!    ([`criteria_met`]): once each criterion is proven both ways and CI is
//!    green, what is left that isn't a shown defect is a follow-up note.

use super::review_agent::{Evidence, Finding};
use super::review_policy::demote;

/// An earlier finding of CoderHelm's on this pull request: a review thread
/// (resolved or not) or an item of its own fix rounds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Earlier {
    pub path: String,
    pub title: String,
    pub body: String,
}

impl Earlier {
    /// From a review thread's first comment.
    pub fn of_thread(path: &str, body: &str) -> Self {
        let title = body
            .lines()
            .map(|l| {
                l.trim()
                    .trim_matches(|c| c == '*' || c == '#' || c == '_')
                    .trim()
            })
            .find(|l| !l.is_empty())
            .unwrap_or("")
            .to_string();
        // A severity label leads some titles ("Should fix: ...").
        let title = title
            .split_once(": ")
            .filter(|(head, _)| head.split_whitespace().count() <= 2)
            .map_or(title.clone(), |(_, t)| t.trim().to_string());
        Self {
            path: path.to_string(),
            title,
            body: body.to_string(),
        }
    }

    /// From a fix round's item, `title (`path:line`)`.
    pub fn of_item(item: &str) -> Self {
        let k = super::review_one_verdict::Key::of_line(item);
        let title = item
            .rsplit_once("(`")
            .map_or(item, |(t, _)| t)
            .trim()
            .to_string();
        Self {
            path: k.path,
            title: title.clone(),
            body: title,
        }
    }
}

/// What [`judge`] knows besides the findings.
#[derive(Debug, Default)]
pub struct Facts<'a> {
    pub earlier: &'a [Earlier],
    /// Items CoderHelm's own fix rounds deferred (no progress after every
    /// approach, or a person-gate reason).
    pub deferred: &'a [String],
    /// Right-side lines changed since the previous reviewed head, by file.
    pub changed_since_previous:
        Option<&'a std::collections::HashMap<String, std::collections::HashSet<u64>>>,
    /// What people wrote on the pull request and on the ticket.
    pub people: &'a [String],
    /// Each ticket criterion is proven ([`criteria_met`]).
    pub criteria_proven: bool,
    /// No check fails on the head.
    pub ci_green: bool,
}

/// A finding [`judge`] made a note, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Made {
    pub title: String,
    pub file: String,
    pub why: String,
}

/// The rules, in order, over the findings that hold the merge.
pub fn judge(findings: &mut [Finding], facts: &Facts<'_>) -> Vec<Made> {
    let mut made = Vec::new();
    for f in findings.iter_mut() {
        against_people(f, facts.people);
        // Every finding is judged, not only those that hold: on CoderHelm's
        // own pull request a note is still work that holds it
        // (`own_review::required`), unless a policy made it a note, which
        // is what each rule here does.
        if f.note_only.is_some() && !f.holds_merge() {
            continue;
        }
        let actionable_matters = f.holds_merge() || super::review_rules::check_started(f).is_some();
        let why = actionable_matters
            .then(|| unactionable(f))
            .flatten()
            .or_else(|| moved(f, facts.earlier))
            .or_else(|| opposite(f, facts.earlier))
            .or_else(|| deferred(f, facts.deferred, facts.changed_since_previous))
            .or_else(|| out_of_scope_asked(f, facts.people))
            .or_else(|| follow_up(f, facts.criteria_proven, facts.ci_green));
        if let Some(why) = why {
            demote(f, &why);
            made.push(Made {
                title: f.title.clone(),
                file: f.file.clone(),
                why,
            });
        }
    }
    made
}

/// Rules that a by-rule finding (a rule the repository requires, a failing
/// check, an unproven critical path) answers to as well: only the
/// smallest-change rule's, which are generic.
fn by_repo_rule(f: &Finding) -> bool {
    super::review_rules::mandatory(f).is_some()
        && f.required.as_deref() != Some(super::review_rules::SMALLEST_RULE)
}

/// Rule 1: why a blocking finding isn't actionable, if it isn't.
pub fn unactionable(f: &Finding) -> Option<String> {
    if by_repo_rule(f) {
        return None;
    }
    if f.file.trim().is_empty() {
        return Some(
            "A note, not a block: it names no file. A blocking finding names its file and line."
                .into(),
        );
    }
    if f.line == 0 {
        return Some(format!(
            "A note, not a block: it names no line in `{}`. A blocking finding names its file and line.",
            f.file
        ));
    }
    let text = format!("{} {}", f.title, f.body);
    if f.title == PARALLEL && !text.contains("the existing `") {
        return Some(
            "A note, not a block: it doesn't name the existing code this duplicates.".into(),
        );
    }
    if f.title == TEST_SIZE && !text.contains(" against ") && !text.contains("remove") {
        return Some(
            "A note, not a block: it names neither the cases to remove nor what the tests are out of proportion to."
                .into(),
        );
    }
    None
}

const PARALLEL: &str = "A parallel structure next to an existing one";
const TEST_SIZE: &str = "A test out of proportion to the change";

/// Rule 2: a generic finding raised before on another file.
pub fn moved(f: &Finding, earlier: &[Earlier]) -> Option<String> {
    super::review_rules::check_started(f)?;
    let before = earlier
        .iter()
        .find(|e| e.title.eq_ignore_ascii_case(f.title.trim()) && e.path != f.file)?;
    Some(format!(
        "A note, not a block: this was raised before on `{}`, and that code has changed since; the same words on `{}` are a new claim this review hasn't shown.",
        before.path, f.file
    ))
}

/// Which way a finding asks the code to go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Way {
    More,
    Less,
}

fn way(text: &str) -> Option<Way> {
    let t = text.to_lowercase();
    const LESS: &[&str] = &[
        "out of proportion",
        "too large",
        "too big",
        "too many",
        "trim",
        "cut them",
        "remove the",
        "drop the",
        "fewer",
    ];
    const MORE: &[&str] = &[
        "restore",
        "add a test",
        "add a case",
        "add the case",
        "missing test",
        "no test",
        "doesn't test",
        "does not test",
        "not covered",
        "keep the case",
        "put back",
    ];
    if LESS.iter().any(|w| t.contains(w)) {
        Some(Way::Less)
    } else if MORE.iter().any(|w| t.contains(w)) {
        Some(Way::More)
    } else {
        None
    }
}

/// Rule 3: a finding asking the opposite of an earlier one on its file.
pub fn opposite(f: &Finding, earlier: &[Earlier]) -> Option<String> {
    let now = way(&format!("{} {}", f.title, f.body))?;
    let before = earlier.iter().find(|e| {
        e.path == f.file
            && !e.title.eq_ignore_ascii_case(f.title.trim())
            && way(&format!("{} {}", e.title, e.body)).is_some_and(|w| w != now)
    })?;
    Some(format!(
        "A note, not a block: it would undo my earlier request on `{}` (\"{}\"), which stands.",
        f.file, before.title
    ))
}

/// Rule 4: a finding CoderHelm's own rounds deferred, on code that hasn't
/// changed since the previous review.
pub fn deferred(
    f: &Finding,
    items: &[String],
    changed: Option<&std::collections::HashMap<String, std::collections::HashSet<u64>>>,
) -> Option<String> {
    let line = super::own_review::thread_line(&f.file, f.line, &f.title);
    let hit = items
        .iter()
        .find(|i| super::own_review_ledger::same_finding(i, &line))?;
    let new_code = changed.is_some_and(|c| {
        c.get(&f.file).is_some_and(|lines| {
            f.line > 0
                && lines
                    .iter()
                    .any(|l| l.abs_diff(f.line) <= 3 || *l == super::review_policy::WHOLE_FILE)
        })
    });
    if new_code {
        return None;
    }
    let what = hit.rsplit_once(" (`").map_or(hit.as_str(), |(t, _)| t);
    Some(format!(
        "A note, not a block: CoderHelm's own fix rounds deferred \"{what}\" after making no progress, and nothing it applies to changed since. It stays a follow-up."
    ))
}

/// Significant words of a person's text or a finding.
fn words(text: &str) -> Vec<String> {
    const STOP: &[&str] = &[
        "about", "after", "again", "should", "would", "could", "there", "these", "those", "which",
        "while", "where", "their", "being", "other", "change", "changes", "pull", "request",
        "review", "finding", "please", "thanks",
    ];
    let mut out: Vec<String> = text
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() >= 5 && !STOP.contains(w))
        .map(str::to_string)
        .collect();
    out.sort();
    out.dedup();
    out
}

/// The person's words that speak to a finding: at least three significant
/// words shared.
fn answer_for<'a>(f: &Finding, people: &'a [String]) -> Option<&'a String> {
    let fw = words(&format!("{} {}", f.title, f.body));
    people.iter().find(|p| {
        let pw = words(p);
        fw.iter().filter(|w| pw.contains(w)).count() >= 3
    })
}

fn says_out_of_scope(f: &Finding) -> bool {
    let t = format!("{} {}", f.title, f.body).to_lowercase();
    let generic_check = super::review_rules::check_started(f).is_some();
    (f.category.eq_ignore_ascii_case("scope") && !generic_check)
        || [
            "out of scope",
            "outside the ticket",
            "not asked",
            "unrelated",
            "beyond the ticket",
            "isn't part of the ticket",
        ]
        .iter()
        .any(|p| t.contains(p))
}

fn says_missing(f: &Finding) -> bool {
    let t = format!("{} {}", f.title, f.body).to_lowercase();
    f.defeats_goal == Some(true)
        || [
            "missing",
            "not covered",
            "no longer covers",
            "was removed",
            "doesn't cover",
        ]
        .iter()
        .any(|p| t.contains(p))
}

/// The quote a finding carries when a person's answer bears on it.
fn quote(answer: &str) -> String {
    let one = answer.split_whitespace().collect::<Vec<_>>().join(" ");
    common::truncate_str(&one, 240).to_string()
}

/// Rule 5, for a finding that calls missing what a person asked for: it
/// says so (whether it holds or not), so nobody reads it as new.
pub fn against_people(f: &mut Finding, people: &[String]) {
    if says_out_of_scope(f) || !says_missing(f) {
        return;
    }
    let Some(a) = answer_for(f, people) else {
        return;
    };
    let note = format!(
        "A person asked for this on the pull request or the ticket (\"{}\"); the change doesn't do it, so this is that answer not being built, not a new request.",
        quote(a)
    );
    if !f.body.contains(&note) {
        f.body = format!("{}\n\n_{note}_", f.body.trim_end());
    }
}

/// Rule 5, for a finding that calls out of scope what a person asked for.
pub fn out_of_scope_asked(f: &Finding, people: &[String]) -> Option<String> {
    if !says_out_of_scope(f) {
        return None;
    }
    let a = answer_for(f, people)?;
    Some(format!(
        "A note, not a block: a person asked for this (\"{}\"), so it is in scope.",
        quote(a)
    ))
}

/// Rule 6: with the ticket's criteria proven and CI green, what holds that
/// isn't a shown defect is a follow-up.
pub fn follow_up(f: &Finding, criteria_proven: bool, ci_green: bool) -> Option<String> {
    if !(criteria_proven && ci_green) || by_repo_rule(f) {
        return None;
    }
    let advice = super::review_rules::check_started(f).is_some()
        && f.required.as_deref() != Some(super::review_rules::SMALLEST_RULE)
        || super::review_severity::pure_style(f)
        || f.evidence() != Evidence::VerifiedDefect;
    advice.then(|| {
        "Follow-up, not a block: the pull request does what the ticket asks (each criterion proven end to end, CI green).".to_string()
    })
}

/// Whether the ticket's criteria are proven on `head`, from the run's
/// two-way proofs (`e2e_proof` on the run, as `common::e2e_proof` keeps
/// them: each criterion's test fails on staging and passes on the preview).
/// `None` when the run keeps none. Read from the JSON so this works before
/// and after that module lands.
pub fn criteria_met(proofs_json: &str, head: &str) -> Option<bool> {
    let v: serde_json::Value = serde_json::from_str(proofs_json).ok()?;
    let on_head: Vec<&serde_json::Value> = v
        .as_array()?
        .iter()
        .filter(|p| {
            let h = p["record"]["head"].as_str().unwrap_or("");
            !h.is_empty() && !head.is_empty() && (h.starts_with(head) || head.starts_with(h))
        })
        .collect();
    if on_head.is_empty() {
        return None;
    }
    let leg = |p: &serde_json::Value, side: &str| {
        let l = &p["record"][side];
        (
            l["ran"].as_bool().unwrap_or(false),
            l["passed"].as_u64().unwrap_or(0),
            l["failed"].as_u64().unwrap_or(0),
        )
    };
    Some(on_head.iter().all(|p| {
        let (b_ran, _, b_failed) = leg(p, "base");
        let (c_ran, c_passed, c_failed) = leg(p, "change");
        !p["test"].as_str().unwrap_or("").is_empty()
            && b_ran
            && b_failed > 0
            && c_ran
            && c_failed == 0
            && c_passed > 0
    }))
}

/// The items CoderHelm's own rounds deferred, from its ledger.
pub fn deferred_items(ledger: &super::own_review_ledger::Ledger) -> Vec<String> {
    use super::own_review_ledger::Settled;
    ledger
        .findings
        .iter()
        .filter(|t| {
            matches!(
                t.settled,
                Some(Settled::Noted { .. }) | Some(Settled::Deferred { .. })
            )
        })
        .map(|t| t.item.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn f(v: serde_json::Value) -> Finding {
        let mut base = json!({"file": "a.ts", "line": 3, "title": "t", "body": "b",
            "severity": "blocking", "evidence": "verified-defect", "category": "bug"});
        for (k, val) in v.as_object().unwrap() {
            base[k] = val.clone();
        }
        serde_json::from_value(base).unwrap()
    }

    #[test]
    fn a_blocking_finding_names_its_file_line_and_what_it_conflicts_with() {
        assert!(unactionable(&f(json!({}))).is_none());
        assert!(unactionable(&f(json!({"line": 0}))).is_some());
        assert!(unactionable(&f(json!({"file": ""}))).is_some());
        let par = f(json!({"title": PARALLEL, "body": "Extend it."}));
        assert!(unactionable(&par).is_some());
        let named =
            f(json!({"title": PARALLEL, "body": "`a.ts` adds `x` next to the existing `y`."}));
        assert!(unactionable(&named).is_none());
    }

    #[test]
    fn opposite_requests_and_moved_findings_are_notes() {
        let earlier = vec![
            Earlier::of_thread(
                "t/b.test.tsx",
                "**Should fix: Restore the selected-chip case**\n\nPut back the case.",
            ),
            Earlier::of_item("A test out of proportion to the change (`t/a.test.ts`)"),
        ];
        let size = f(
            json!({"file": "t/b.test.tsx", "line": 4, "title": TEST_SIZE,
            "body": "The tests are 300 added lines against 40 lines of changed code for this change. Cut them."}),
        );
        assert!(moved(&size, &earlier).is_some());
        assert!(opposite(&size, &earlier).is_some());
    }

    #[test]
    fn criteria_are_met_only_when_every_proof_on_the_head_holds_both_ways() {
        let ok = json!([{"criterion": "c", "test": "e2e/a.spec.ts › one", "record": {"head": "abc123", "base": {"ran": true, "passed": 0, "failed": 1}, "change": {"ran": true, "passed": 1, "failed": 0}}}]).to_string();
        assert_eq!(criteria_met(&ok, "abc123"), Some(true));
        assert_eq!(criteria_met(&ok, "fff"), None);
        let both_green = json!([{"criterion": "c", "test": "e2e/a.spec.ts › one", "record": {"head": "abc123", "base": {"ran": true, "passed": 1, "failed": 0}, "change": {"ran": true, "passed": 1, "failed": 0}}}]).to_string();
        assert_eq!(criteria_met(&both_green, "abc123"), Some(false));
    }
}
