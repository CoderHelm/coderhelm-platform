//! One verdict per finding.
//!
//! A review of a pull request listed a finding under "Holding the merge"
//! while the same review said it no longer applied. Another listed two
//! findings it had kept as notes ("on code unchanged since my previous
//! review... they should have been raised then") and, on CoderHelm's own
//! pull request, listed the same two under "Holding the merge" as work to
//! finish before ready. Each finding had two verdicts in one review.
//!
//! [`settle`] gives every finding one state before the body is written:
//! blocking, a note, settled (its thread resolved, or this review found it
//! fixed or answered), or no longer applying (the code it was about is no
//! longer in the change). "Holding the merge" is built only from what is
//! still blocking at the head with its thread open:
//!
//! - an earlier thread that is resolved, or that this review settled, never
//!   holds, whatever kind of hold named it (an open thread, the earlier
//!   review's kept items, CoderHelm's own open work);
//! - a finding held back as a note because it came late (on lines unchanged
//!   since the previous review) never holds, on any pull request: on
//!   CoderHelm's own it is still work for this round's fix, listed apart
//!   ([`late_lines`]);
//! - an earlier thread or a kept item this review now calls a note is a
//!   note, unless a required rule holds it (only a fix settles those);
//! - a sentence of the review's own summary saying a holding finding no
//!   longer applies, or is fixed or settled, settles an earlier thread or a
//!   kept item; any other such sentence is dropped, since the hold rests on
//!   the head.
//!
//! [`check`] reads the final body and names every finding listed under
//! "Holding the merge" that the same body also calls a note, settled or no
//! longer applying. [`enforce`] runs it on every review before it is
//! posted: the contradicting text is removed and the miss logged. The
//! tests assemble bodies the way the review does and fail the build on any
//! contradiction.

use super::review_agent::Finding;
use super::review_verdict::{HoldKind, Holding};

/// The one state a finding has in a review.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Blocking,
    Note,
    /// Its thread is resolved, or this review found it fixed or answered.
    Settled,
    /// The code it was about is no longer in the change.
    NoLongerApplies,
}

impl State {
    pub fn words(self) -> &'static str {
        match self {
            State::Blocking => "blocking",
            State::Note => "a note",
            State::Settled => "settled",
            State::NoLongerApplies => "no longer applies",
        }
    }
}

/// Where a finding is and what it says: enough to tell one finding from
/// another however a list words it (`` `a.ts:3` Title ``, `` Title (`a.ts:3`) ``).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Key {
    pub path: String,
    pub line: u64,
    pub words: Vec<String>,
}

const STOP: &[&str] = &[
    "this", "that", "with", "from", "when", "then", "than", "into", "still", "does", "doesn",
    "have", "been", "were", "will", "only", "also", "they", "them", "their", "there", "which",
    "what", "should", "would", "could", "earlier", "review", "finding", "note", "block", "blocks",
    "merge", "applies", "apply", "longer", "settled", "fixed", "resolved",
];

fn words(text: &str) -> Vec<String> {
    let mut out: Vec<String> = text
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() >= 4 && !STOP.contains(w))
        .map(str::to_string)
        .collect();
    out.sort();
    out.dedup();
    out
}

/// A backticked `path` or `path:line` in `text`, with where it sits.
fn located(text: &str) -> Option<(usize, usize, String, u64)> {
    let mut from = 0;
    while let Some(a) = text[from..].find('`') {
        let a = from + a;
        let b = a + 1 + text[a + 1..].find('`')?;
        let inner = &text[a + 1..b];
        let looks_like_path = !inner.is_empty()
            && !inner.contains(char::is_whitespace)
            && (inner.contains('/') || inner.contains('.'));
        if looks_like_path {
            let (path, line) = match inner.rsplit_once(':') {
                Some((p, l)) if l.chars().all(|c| c.is_ascii_digit()) && !l.is_empty() => {
                    (p.to_string(), l.parse().unwrap_or(0))
                }
                _ => (inner.to_string(), 0),
            };
            return Some((a, b + 1, path, line));
        }
        from = b + 1;
    }
    None
}

impl Key {
    pub fn new(path: &str, line: u64, title: &str) -> Self {
        Self {
            path: path.trim().to_string(),
            line,
            words: words(title),
        }
    }

    pub fn of_finding(f: &Finding) -> Self {
        Self::new(&f.file, f.line, &f.title)
    }

    /// A finding line as the review and the description write one.
    pub fn of_line(text: &str) -> Self {
        let text = text.trim().trim_start_matches(['-', '*', ' ']);
        match located(text) {
            Some((a, b, path, line)) => {
                let mut rest = format!("{}{}", &text[..a], &text[b..]);
                rest = rest.replace("()", " ");
                // What follows the title in parentheses is why it holds or
                // how to test it, never the finding.
                let title = rest
                    .find(" (")
                    .map_or(rest.as_str(), |i| &rest[..i])
                    .to_string();
                Self::new(&path, line, &title)
            }
            None => Self::new("", 0, text),
        }
    }

    /// The same finding: the same file, and most of the smaller title's
    /// words. A later review words it differently and its line moves.
    pub fn same(&self, other: &Key) -> bool {
        if !self.path.is_empty() && !other.path.is_empty() && self.path != other.path {
            return false;
        }
        let (small, big) = if self.words.len() <= other.words.len() {
            (&self.words, &other.words)
        } else {
            (&other.words, &self.words)
        };
        if small.is_empty() {
            return !self.path.is_empty()
                && self.path == other.path
                && self.line > 0
                && self.line == other.line;
        }
        let shared = small.iter().filter(|w| big.contains(w)).count();
        if self.path.is_empty() || other.path.is_empty() {
            // Prose that names no file: most of the title, and two words
            // at least, so a common word never matches alone.
            shared * 3 >= small.len() * 2 && shared >= small.len().min(2)
        } else {
            shared * 2 >= small.len()
        }
    }
}

/// One finding's state, for the log and the tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub name: String,
    pub state: State,
    pub why: String,
}

/// What [`settle`] reads.
#[derive(Debug, Default)]
pub struct Inputs<'a> {
    /// Everything named as holding the merge (`review_policy::named_holding`).
    pub holding: Vec<Holding>,
    /// Earlier threads that are closed, with their state: resolved on
    /// GitHub or settled by this review ([`State::Settled`]), or about code
    /// no longer in the change ([`State::NoLongerApplies`]).
    pub closed: &'a [(Key, State)],
    /// Findings held back as notes because they came late.
    pub late: &'a [Key],
    /// This review's other notes.
    pub notes: &'a [Key],
    /// The review's own summary.
    pub prose: &'a str,
}

/// What [`settle`] decided.
#[derive(Debug, Default)]
pub struct Settled {
    /// What still holds the merge: the only list "Holding the merge" is
    /// written from.
    pub holding: Vec<Holding>,
    /// Every finding that was named as holding, with its one state.
    pub entries: Vec<Entry>,
    /// The summary with no sentence that contradicts a hold.
    pub prose: String,
}

/// Phrases that say a finding doesn't hold.
pub const NOT_HOLDING: &[&str] = &[
    "no longer applies",
    "no longer apply",
    "no longer holds",
    "no longer relevant",
    "no longer an issue",
    "no longer a problem",
    "doesn't apply anymore",
    "does not apply anymore",
    "is settled",
    "was settled",
    "is resolved",
    "was resolved",
    "already fixed",
    "is fixed",
    "was fixed",
    "has been fixed",
    "are fixed",
    "were fixed",
    "don't change the verdict",
    "doesn't change the verdict",
    "not a block",
    "doesn't hold the merge",
    "does not hold the merge",
    "don't hold the merge",
];

fn says_not_holding(text: &str) -> Option<State> {
    let t = text.to_lowercase();
    let hit = NOT_HOLDING.iter().find(|p| t.contains(*p))?;
    Some(if hit.starts_with("no longer") || hit.contains("anymore") {
        State::NoLongerApplies
    } else if hit.contains("verdict") || hit.contains("block") || hit.contains("hold") {
        State::Note
    } else {
        State::Settled
    })
}

/// The sentences of a paragraph, each with its end punctuation.
fn sentences(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let chars: Vec<char> = text.chars().collect();
    for (i, c) in chars.iter().enumerate() {
        cur.push(*c);
        let end =
            matches!(c, '.' | '!' | '?') && chars.get(i + 1).is_none_or(|n| n.is_whitespace());
        if end || *c == '\n' {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// One state for every finding named as holding the merge.
pub fn settle(inputs: Inputs<'_>) -> Settled {
    let mut out = Settled::default();
    let mut prose_drops: Vec<String> = Vec::new();
    let said: Vec<(String, State)> = sentences(inputs.prose)
        .into_iter()
        .filter_map(|s| says_not_holding(&s).map(|st| (s, st)))
        .collect();
    for h in inputs.holding {
        let key = Key::of_line(&h.name);
        let named = |kind: State, why: &str| Entry {
            name: h.name.clone(),
            state: kind,
            why: why.to_string(),
        };
        // A failed verification run is no finding: nothing settles it.
        if h.kind == HoldKind::Verification {
            out.entries
                .push(named(State::Blocking, "the verification run failed"));
            out.holding.push(h);
            continue;
        }
        if out
            .holding
            .iter()
            .any(|kept| Key::of_line(&kept.name).same(&key))
        {
            // Named twice: listed once.
            continue;
        }
        if let Some((_, st)) = inputs.closed.iter().find(|(k, _)| k.same(&key)) {
            out.entries.push(named(
                *st,
                "its thread is resolved or this review settled it",
            ));
            continue;
        }
        if inputs.late.iter().any(|k| k.same(&key)) {
            out.entries.push(named(
                State::Note,
                "raised late, on code unchanged since the previous review",
            ));
            continue;
        }
        let earlier = matches!(h.kind, HoldKind::EarlierThread | HoldKind::KeptFromEarlier);
        if earlier && inputs.notes.iter().any(|k| k.same(&key)) {
            out.entries.push(named(
                State::Note,
                "this review calls it a note at the head",
            ));
            continue;
        }
        let prose = said
            .iter()
            .find(|(s, _)| Key::of_line(s).same(&key) || mentions(s, &key));
        if let Some((sentence, st)) = prose {
            if earlier {
                out.entries
                    .push(named(*st, "this review's summary settles it"));
                continue;
            }
            // The hold rests on the head (a finding of this review, a rule,
            // CoderHelm's own open work): the sentence goes.
            prose_drops.push(sentence.clone());
        }
        out.entries
            .push(named(State::Blocking, "still holds at the head"));
        out.holding.push(h);
    }
    out.prose = sentences(inputs.prose)
        .into_iter()
        .filter(|s| !prose_drops.contains(s))
        .collect::<String>()
        .trim()
        .to_string();
    out
}

/// A sentence that names the finding's file (with or without its line) and
/// shares most of its words.
fn mentions(sentence: &str, key: &Key) -> bool {
    if key.path.is_empty() || !sentence.contains(&key.path) {
        return false;
    }
    let w = words(sentence);
    if key.words.is_empty() {
        return true;
    }
    let shared = key.words.iter().filter(|k| w.contains(k)).count();
    shared * 2 >= key.words.len()
}

/// CoderHelm's own pull request: the findings it fixes in this round, as
/// the description lists them. Late ones are work too, sent in the same
/// round, and marked so nobody reads them as holding the pull request.
pub fn late_lines(late: &[Finding]) -> Vec<String> {
    late.iter()
        .map(|f| {
            let at = if f.line > 0 {
                format!("`{}:{}`", f.file, f.line)
            } else {
                format!("`{}`", f.file)
            };
            let title = f.title.split_whitespace().collect::<Vec<_>>().join(" ");
            format!("{title} ({at}) ({LATE_MARK})")
        })
        .collect()
}

/// How a late finding is marked in CoderHelm's own work list.
pub const LATE_MARK: &str = "missed by an earlier review: a note, fixed in this round";

/// The review's section for findings held back because they came late,
/// each group saying why: the previous review missed them, a check that
/// started after it found them, or a smallest-change check gave advice
/// after the first review.
pub fn late_markdown(late: &[Finding]) -> String {
    use super::review_policy::{ADVISORY_LATE_NOTE, NEW_CHECK_NOTE};
    if late.is_empty() {
        return String::new();
    }
    let line = |f: &Finding| {
        if f.line > 0 {
            format!("- `{}:{}` {}\n", f.file, f.line, f.title)
        } else {
            format!("- `{}` {}\n", f.file, f.title)
        }
    };
    fn note(f: &Finding) -> &str {
        f.note_only.as_deref().unwrap_or("")
    }
    let mut s = String::from(LATE_HEAD);
    for (pick, why) in [
        (
            None,
            "\nOn code that hasn't changed since my previous review, which missed them. They are \
             notes, not part of the verdict:\n",
        ),
        (
            Some(NEW_CHECK_NOTE),
            "\nOn code that hasn't changed since my previous review, found by a check that \
             started running after it. They are notes, not part of the verdict:\n",
        ),
        (
            Some(ADVISORY_LATE_NOTE),
            "\nAdvice from the smallest-change checks after the first review. They are notes, \
             not part of the verdict:\n",
        ),
    ] {
        let group: Vec<&Finding> = late
            .iter()
            .filter(|f| match pick {
                Some(n) => note(f) == n,
                None => note(f) != NEW_CHECK_NOTE && note(f) != ADVISORY_LATE_NOTE,
            })
            .collect();
        if group.is_empty() {
            continue;
        }
        s.push_str(why);
        for f in group {
            s.push_str(&line(f));
        }
    }
    s
}

/// The heading of [`late_markdown`]'s section.
pub const LATE_HEAD: &str = "\n\n#### Also worth a look\n";

/// The heading the verdict lists what holds the merge under
/// (`review_agent::verdict_heading`).
pub const HOLDING_HEAD: &str = "**Holding the merge:**";

/// A finding the body lists as holding the merge and also calls something
/// else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contradiction {
    /// The line under "Holding the merge".
    pub holding: String,
    /// The text that says otherwise.
    pub said: String,
    pub state: State,
}

/// The items under "Holding the merge", and the span of that list.
fn holding_list(body: &str) -> (Vec<String>, Option<(usize, usize)>) {
    let Some(at) = body.find(HOLDING_HEAD) else {
        return (Vec::new(), None);
    };
    let start = at + HOLDING_HEAD.len();
    let mut end = start;
    let mut items = Vec::new();
    for line in body[start..].split_inclusive('\n') {
        let t = line.trim();
        if t.is_empty() && items.is_empty() {
            end += line.len();
            continue;
        }
        match t.strip_prefix("- ") {
            Some(item) => {
                items.push(item.trim().to_string());
                end += line.len();
            }
            None => break,
        }
    }
    (items, Some((start, end)))
}

/// Every finding the body lists under "Holding the merge" that the same
/// body also calls a note, settled or no longer applying.
pub fn check(body: &str) -> Vec<Contradiction> {
    let (items, span) = holding_list(body);
    let Some((start, end)) = span else {
        return Vec::new();
    };
    let rest = format!("{}\n{}", &body[..start], &body[end..]);
    let mut out = Vec::new();
    let mut in_late = false;
    for line in rest.lines() {
        let t = line.trim();
        if t.starts_with("####") || (t.starts_with("**") && t.ends_with("**")) {
            in_late = t == LATE_HEAD.trim();
        }
        // The verdict's own one-line reason repeats what holds.
        if t.starts_with("_Why changes requested") || t.is_empty() {
            continue;
        }
        let state = if in_late && t.starts_with("- ") {
            Some(State::Note)
        } else {
            says_not_holding(t)
        };
        let Some(state) = state else {
            continue;
        };
        for part in sentences(t) {
            if !in_late && says_not_holding(&part).is_none() {
                continue;
            }
            let said = Key::of_line(&part);
            for item in &items {
                let key = Key::of_line(item);
                if (key.same(&said) || mentions(&part, &key))
                    && !out.iter().any(|c: &Contradiction| c.holding == *item)
                {
                    out.push(Contradiction {
                        holding: item.clone(),
                        said: part.trim().to_string(),
                        state,
                    });
                }
            }
        }
    }
    out
}

/// The body with no finding given two verdicts: every sentence or note
/// line that contradicts a hold is removed, so the posted verdict and its
/// words agree. The contradictions found are returned for the log; a
/// review assembled by [`settle`] has none.
pub fn enforce(body: &str) -> (String, Vec<Contradiction>) {
    let found = check(body);
    if found.is_empty() {
        return (body.to_string(), found);
    }
    let (_, span) = holding_list(body);
    let Some((start, end)) = span else {
        return (body.to_string(), found);
    };
    let mut rest_before = body[..start].to_string();
    let mut rest_after = body[end..].to_string();
    for c in &found {
        for part in [&mut rest_before, &mut rest_after] {
            if let Some(i) = part.find(&c.said) {
                part.replace_range(i..i + c.said.len(), "");
            }
        }
    }
    let fixed = format!("{rest_before}{}{rest_after}", &body[start..end]);
    // An emptied bullet goes too.
    let fixed = fixed
        .lines()
        .filter(|l| l.trim() != "-")
        .collect::<Vec<_>>()
        .join("\n");
    (fixed, found)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::review_verdict::{HoldKind as K, Holding, Verdict};
    use serde_json::json;

    fn finding(file: &str, line: u64, title: &str) -> Finding {
        serde_json::from_value(json!({"file": file, "line": line, "title": title, "body": "b"}))
            .unwrap()
    }

    #[test]
    fn keys_read_every_way_a_finding_is_written() {
        let a = Key::of_line("`src/a.ts:12` Booth dropdown lists Guest-Booth twice");
        let b = Key::of_line("Booth dropdown lists Guest-Booth twice (`src/a.ts:14`)");
        let c = Key::of_line(
            "- `src/a.ts:12` Booth dropdown lists Guest-Booth twice (required: tests with behavior changes)",
        );
        assert_eq!(a.path, "src/a.ts");
        assert_eq!(a.line, 12);
        assert!(a.same(&b) && a.same(&c) && b.same(&c));
        let other = Key::of_line("`src/b.ts:12` Booth dropdown lists Guest-Booth twice");
        assert!(!a.same(&other));
        let prose = Key::of_line("The booth dropdown listing Guest-Booth twice no longer applies.");
        assert!(prose.same(&a));
        let unrelated = Key::of_line("The slot list no longer applies.");
        assert!(!unrelated.same(&a));
    }

    #[test]
    fn a_resolved_or_settled_thread_never_holds() {
        let closed = [(
            Key::of_line("Booth dropdown lists Guest-Booth twice (`src/a.ts:12`)"),
            State::Settled,
        )];
        let s = settle(Inputs {
            holding: vec![
                Holding::new(
                    K::EarlierThread,
                    "Booth dropdown lists Guest-Booth twice (`src/a.ts:12`)",
                ),
                Holding::new(
                    K::OwnFinding,
                    "Booth dropdown can list Guest-Booth twice (`src/a.ts:15`)",
                ),
                Holding::new(K::Defect, "`src/b.ts:3` Null deref"),
            ],
            closed: &closed,
            ..Default::default()
        });
        assert_eq!(s.holding.len(), 1);
        assert_eq!(s.holding[0].name, "`src/b.ts:3` Null deref");
        assert_eq!(
            s.entries
                .iter()
                .filter(|e| e.state == State::Settled)
                .count(),
            2
        );
    }

    #[test]
    fn a_late_finding_is_a_note_and_never_holds() {
        let late = [Key::of_finding(&finding(
            "apps/web/src/utils/filterMaps.ts",
            0,
            "A parallel structure next to an existing one",
        ))];
        let s = settle(Inputs {
            holding: vec![Holding::new(
                K::OwnFinding,
                "A parallel structure next to an existing one (`apps/web/src/utils/filterMaps.ts`)",
            )],
            late: &late,
            ..Default::default()
        });
        assert!(s.holding.is_empty());
        assert_eq!(s.entries[0].state, State::Note);
    }

    #[test]
    fn the_summary_settles_an_earlier_thread_and_never_contradicts_a_head_hold() {
        let prose = "The earlier comment on `src/a.ts` about the duplicate booth option no \
                     longer applies. The `src/b.ts` null deref was fixed. Tests pass.";
        let s = settle(Inputs {
            holding: vec![
                Holding::new(
                    K::EarlierThread,
                    "Duplicate booth option in the dropdown (`src/a.ts:12`)",
                ),
                Holding::new(K::Defect, "`src/b.ts:3` Null deref"),
            ],
            prose,
            ..Default::default()
        });
        assert_eq!(s.holding.len(), 1);
        assert_eq!(s.holding[0].name, "`src/b.ts:3` Null deref");
        assert_eq!(s.entries[0].state, State::NoLongerApplies);
        assert!(!s.prose.contains("was fixed"), "{}", s.prose);
        assert!(s.prose.contains("Tests pass."));
    }

    #[test]
    fn a_required_thread_holds_over_a_note_and_verification_always_holds() {
        let notes = [Key::of_line("`src/a.ts:12` Missing test for the dedupe")];
        let s = settle(Inputs {
            holding: vec![
                Holding::new(K::Required, "Missing test for the dedupe (`src/a.ts:12`)"),
                Holding::new(K::Verification, "The verification run failed"),
            ],
            notes: &notes,
            prose: "The verification run failed was fixed.",
            ..Default::default()
        });
        assert_eq!(s.holding.len(), 2);
    }

    #[test]
    fn check_names_a_finding_with_two_verdicts_and_enforce_removes_the_other_words() {
        let v = Verdict::from_holding(vec![
            Holding::new(
                K::OwnFinding,
                "A test out of proportion to the change (`a.test.ts`)",
            ),
            Holding::new(K::Defect, "`src/b.ts:3` Null deref"),
        ]);
        let mut body = crate::passes::review_agent::verdict_heading(&v, 1);
        body.push_str("\n\n_Why changes requested: A test out of proportion to the change; `src/b.ts:3` Null deref._");
        body.push_str("\n\nThe `src/b.ts` null deref no longer applies: the guard is back.");
        body.push_str(&late_markdown(&[{
            let mut f = finding("a.test.ts", 0, "A test out of proportion to the change");
            crate::passes::review_policy::demote(&mut f, crate::passes::review_policy::LATE_NOTE);
            f
        }]));
        let found = check(&body);
        assert_eq!(found.len(), 2, "{found:?}");
        let (fixed, _) = enforce(&body);
        assert!(check(&fixed).is_empty(), "{fixed}");
        assert!(fixed.contains(HOLDING_HEAD));
        assert!(fixed.contains("- `src/b.ts:3` Null deref"));
    }

    #[test]
    fn a_body_with_one_verdict_per_finding_passes() {
        let v = Verdict::from_holding(vec![Holding::new(K::Defect, "`src/b.ts:3` Null deref")]);
        let mut body = crate::passes::review_agent::verdict_heading(&v, 1);
        body.push_str("\n\nThe earlier dropdown comment is fixed. Checked the guard.");
        body.push_str(&late_markdown(&[{
            let mut f = finding("a.test.ts", 0, "A test out of proportion to the change");
            crate::passes::review_policy::demote(&mut f, crate::passes::review_policy::LATE_NOTE);
            f
        }]));
        assert!(check(&body).is_empty(), "{body}");
        assert_eq!(enforce(&body).0, body);
    }

    #[test]
    fn late_lines_are_marked_as_notes_for_the_round() {
        let l = late_lines(&[finding(
            "a.ts",
            4,
            "A parallel structure next to an existing one",
        )]);
        assert_eq!(
            l,
            vec![format!(
                "A parallel structure next to an existing one (`a.ts:4`) ({LATE_MARK})"
            )]
        );
    }

    /// Every check the review runs with no model says when it started, so
    /// a finding it raises on code an earlier review read is told apart
    /// from a miss of that review.
    #[test]
    fn every_check_without_a_model_says_when_it_started() {
        use crate::passes::minimal_change::{Kind, Widening};
        for kind in [
            Kind::Parallel,
            Kind::Replacing,
            Kind::LogLevel,
            Kind::Reporting,
            Kind::TestSize,
            Kind::InventedPayload,
        ] {
            let w = Widening {
                kind,
                path: "a.ts".into(),
                what: "x".into(),
            };
            let f = &crate::passes::review_rules::wider_findings(&[w])[0];
            assert!(
                crate::passes::review_rules::check_started(f).is_some(),
                "{kind:?} has no start in CHECKS_STARTED"
            );
        }
        let flip = crate::passes::minimal_change::VendorFlip {
            path: "e2e/a.spec.ts".into(),
            removed: "a".into(),
            added: "b".into(),
            criterion: "c".into(),
        };
        let f = &crate::passes::review_rules::vendor_flip_findings(&[flip])[0];
        assert!(crate::passes::review_rules::check_started(f).is_some());
        for (_, since) in crate::passes::review_rules::CHECKS_STARTED {
            assert!(
                chrono::DateTime::parse_from_rfc3339(since).is_ok(),
                "{since}"
            );
        }
    }
}
