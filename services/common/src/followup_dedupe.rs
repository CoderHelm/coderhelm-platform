//! Duplicate follow-ups close themselves.
//!
//! The follow-ups of one source ticket (its whole family: the follow-ups
//! filed from it, and any filed from those) can cover the same topic twice,
//! in other words. CoderHelm reads them together and decides ([`decide`]):
//!
//! - two tickets are the same topic when the follow-up topic matcher says so
//!   (`followup_tickets::near`, the title words and the key files they name)
//!   or the small model check reads them as the same work ([`SYSTEM`],
//!   [`prompt`], [`read_answer`]);
//! - in each group of the same topic, one ticket is kept ([`rank`]): one
//!   whose run finished its work, else one with code and a pull request,
//!   else the earliest; every other run in the group is closed as a
//!   duplicate of it ([`close_comment`]);
//! - two tickets that take opposite approaches to the same thing (fix the
//!   components, delete the components) are never closed: each gets a note
//!   naming the other, for a person to choose ([`flag_comment`]).
//!
//! The decision is pure and tested with a fake model; the gateway reads the
//! records and runs the closes (`routes::followup_dedupe`).

use crate::followup_tickets::{self as ft, Draft, Existing, Near};
use crate::log_repo::live::Judge;
use std::collections::BTreeMap;

/// Pairs of tickets, by their index in the candidates.
pub type Pairs = Vec<(usize, usize)>;

/// One follow-up ticket and its latest run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Candidate {
    pub key: String,
    pub title: String,
    pub body: String,
    /// The latest run's id (sortable by time), empty when no run worked it.
    pub run_id: String,
    /// The latest run's status, empty when none.
    pub status: String,
    /// The run opened a pull request.
    pub has_pr: bool,
}

impl Candidate {
    fn finished(&self) -> bool {
        matches!(self.status.as_str(), "completed" | "merged")
    }
    /// Its run may be closed now (`run_status::close_check`).
    pub fn closable(&self) -> bool {
        !self.run_id.is_empty()
            && matches!(
                crate::run_status::close_check(&self.status),
                crate::run_status::CloseCheck::Close
            )
    }
}

/// A run to close as a duplicate of the kept ticket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Close {
    pub dup: String,
    pub run_id: String,
    pub keep: String,
    pub topic: String,
}

/// Two tickets flagged on both, neither closed: they take opposite
/// approaches to one topic (`opposed`), or the matcher and the small check
/// disagree on whether they are the same work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Opposed {
    pub a: String,
    pub b: String,
    pub topic: String,
    pub opposed: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    pub close: Vec<Close>,
    pub flag: Vec<Opposed>,
    /// Duplicates with no run to close (none yet, or one already over):
    /// a person's start of one is refused with the ticket that covers it.
    pub covered: Vec<Close>,
}

impl Plan {
    /// The ticket that covers `key`, when `key` is a duplicate.
    pub fn covering(&self, key: &str) -> Option<&Close> {
        self.close
            .iter()
            .chain(self.covered.iter())
            .find(|c| c.dup.eq_ignore_ascii_case(key.trim()))
    }
}

/// The order a group's tickets are kept in: finished work first, then code
/// with a pull request, then the earliest run (a ticket no run worked
/// last), then the lower key.
pub fn rank(c: &Candidate) -> (u8, u8, u8, String, u64) {
    let number = c
        .key
        .rsplit('-')
        .next()
        .and_then(|n| n.parse().ok())
        .unwrap_or(u64::MAX);
    (
        u8::from(!c.finished()),
        u8::from(!c.has_pr),
        u8::from(c.run_id.is_empty()),
        c.run_id.clone(),
        number,
    )
}

/// The words that make a ticket a removal of what it names.
const REMOVAL: [&str; 6] = [
    "remove",
    "delete",
    "drop",
    "deprecate",
    "retire",
    "strip out",
];

/// Whether a title asks to remove what it names (its first words).
pub fn removes(title: &str) -> bool {
    let t = title.trim().to_ascii_lowercase();
    REMOVAL.iter().any(|w| t.starts_with(w))
}

fn existing(c: &Candidate) -> Existing {
    Existing {
        key: c.key.clone(),
        url: String::new(),
        summary: c.title.clone(),
        marks: ft::marks(&format!("{}\n{}", c.title, c.body))
            .into_iter()
            .collect(),
    }
}

fn draft(c: &Candidate) -> Draft {
    Draft {
        summary: c.title.clone(),
        description: c.body.clone(),
    }
}

/// The pairs the topic matcher reads as the same topic, and the ones it
/// reads as maybe (for the small check).
pub fn matcher_pairs(cands: &[Candidate]) -> (Pairs, Pairs) {
    let mut same = Vec::new();
    let mut maybe = Vec::new();
    for i in 0..cands.len() {
        for j in (i + 1)..cands.len() {
            let e = [existing(&cands[i])];
            match ft::near(&draft(&cands[j]), &e) {
                Near::Same(_) => same.push((i, j)),
                Near::Maybe(_) => maybe.push((i, j)),
                Near::New => {}
            }
        }
    }
    (same, maybe)
}

/// The small check's instructions.
pub const SYSTEM: &str = "You compare follow-up tickets filed from one source ticket. \
Two tickets are the SAME when doing one makes the other unnecessary: the same fix to the same \
code, in other words. Two are OPPOSED when they are about the same code but take opposite \
approaches (one fixes or keeps it, the other deletes it). Tickets on different code or different \
fixes are neither. Answer with one line per pair that is SAME or OPPOSED, as `SAME KEY-1 KEY-2` \
or `OPPOSED KEY-1 KEY-2`, and nothing else. When no pair is either, answer `NONE`.";

/// The small check's question: every ticket, and the pairs the matcher
/// already finds close.
pub fn prompt(cands: &[Candidate], near: &[(usize, usize)]) -> String {
    let mut s = String::from("Tickets:\n");
    for c in cands {
        let body: String = c.body.split_whitespace().collect::<Vec<_>>().join(" ");
        let body: String = body.chars().take(400).collect();
        s.push_str(&format!("- {}: {}\n  {}\n", c.key, c.title.trim(), body));
    }
    if !near.is_empty() {
        s.push_str("\nPairs that look close by their words (check them, and any other pair):\n");
        for (i, j) in near {
            s.push_str(&format!("- {} and {}\n", cands[*i].key, cands[*j].key));
        }
    }
    s
}

/// The small check's answer: the SAME pairs and the OPPOSED pairs, by
/// index, only between tickets in `cands`.
pub fn read_answer(text: Option<&str>, cands: &[Candidate]) -> (Pairs, Pairs) {
    let idx = |k: &str| {
        let k = k.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-');
        cands.iter().position(|c| c.key.eq_ignore_ascii_case(k))
    };
    let mut same = Vec::new();
    let mut opposed = Vec::new();
    for line in text.unwrap_or("").lines() {
        let w: Vec<&str> = line.split_whitespace().collect();
        if w.len() < 3 {
            continue;
        }
        let kind = w[0].trim_matches(|c: char| !c.is_ascii_alphabetic());
        let (Some(a), Some(b)) = (idx(w[1]), idx(w[2])) else {
            continue;
        };
        if a == b {
            continue;
        }
        let pair = (a.min(b), a.max(b));
        if kind.eq_ignore_ascii_case("SAME") {
            same.push(pair);
        } else if kind.eq_ignore_ascii_case("OPPOSED") {
            opposed.push(pair);
        }
    }
    (same, opposed)
}

/// The plan from the pairs: groups of the same topic, the ticket each
/// keeps, the runs it closes, and the opposed pairs it flags.
pub fn plan(cands: &[Candidate], same: &[(usize, usize)], opposed: &[(usize, usize)]) -> Plan {
    let n = cands.len();
    let opp: Vec<(usize, usize)> = opposed.to_vec();
    let mut parent: Vec<usize> = (0..n).collect();
    fn root(p: &mut [usize], mut i: usize) -> usize {
        while p[i] != i {
            p[i] = p[p[i]];
            i = p[i];
        }
        i
    }
    for &(a, b) in same {
        if opp.contains(&(a.min(b), a.max(b))) {
            continue;
        }
        let (ra, rb) = (root(&mut parent, a), root(&mut parent, b));
        if ra != rb {
            parent[ra] = rb;
        }
    }
    let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for i in 0..n {
        let r = root(&mut parent, i);
        groups.entry(r).or_default().push(i);
    }
    let in_opposition = |i: usize| opp.iter().any(|&(a, b)| a == i || b == i);
    let mut out = Plan::default();
    for members in groups.values() {
        if members.len() < 2 {
            continue;
        }
        let mut m = members.clone();
        m.sort_by_key(|&i| rank(&cands[i]));
        let keep = &cands[m[0]];
        for &i in &m[1..] {
            let c = &cands[i];
            if in_opposition(i) {
                continue;
            }
            let one = Close {
                dup: c.key.clone(),
                run_id: c.run_id.clone(),
                keep: keep.key.clone(),
                topic: topic_of(keep),
            };
            if c.closable() {
                out.close.push(one);
            } else {
                out.covered.push(one);
            }
        }
    }
    for &(a, b) in &opp {
        let (x, y) = if rank(&cands[a]) <= rank(&cands[b]) {
            (a, b)
        } else {
            (b, a)
        };
        out.flag.push(Opposed {
            a: cands[x].key.clone(),
            b: cands[y].key.clone(),
            topic: topic_of(&cands[x]),
            opposed: true,
        });
    }
    out.close.sort_by(|a, b| a.dup.cmp(&b.dup));
    out.covered.sort_by(|a, b| a.dup.cmp(&b.dup));
    out.flag.sort_by(|a, b| (&a.a, &a.b).cmp(&(&b.a, &b.b)));
    out.flag.dedup();
    out
}

/// The kept ticket's topic, as the comment names it.
fn topic_of(c: &Candidate) -> String {
    let t = c.title.trim().trim_end_matches('.');
    let t: String = t.chars().take(100).collect();
    t
}

/// The decision when both readers are in: a pair closes only when the
/// matcher and the small check both read it as the same topic. A pair one
/// of them reads as the same and the other doesn't is flagged; a pair
/// either reads as opposed, or a removal against a fix of what either reads
/// as the same, is flagged as opposed. Nothing else is touched.
pub fn agreed(
    cands: &[Candidate],
    matcher_same: &[(usize, usize)],
    model_same: &[(usize, usize)],
    model_opposed: &[(usize, usize)],
) -> Plan {
    let norm = |&(a, b): &(usize, usize)| (a.min(b), a.max(b));
    let m: Vec<(usize, usize)> = matcher_same.iter().map(norm).collect();
    let k: Vec<(usize, usize)> = model_same.iter().map(norm).collect();
    let mut opposed: Vec<(usize, usize)> = model_opposed.iter().map(norm).collect();
    for p in m.iter().chain(k.iter()) {
        if removes(&cands[p.0].title) != removes(&cands[p.1].title) && !opposed.contains(p) {
            opposed.push(*p);
        }
    }
    let both: Vec<(usize, usize)> = m
        .iter()
        .filter(|p| k.contains(p) && !opposed.contains(p))
        .copied()
        .collect();
    let mut out = plan(cands, &both, &opposed);
    let mut unsure: Vec<(usize, usize)> = m
        .iter()
        .chain(k.iter())
        .filter(|p| !both.contains(p) && !opposed.contains(p))
        .copied()
        .collect();
    unsure.sort();
    unsure.dedup();
    for (a, b) in unsure {
        let (x, y) = if rank(&cands[a]) <= rank(&cands[b]) {
            (a, b)
        } else {
            (b, a)
        };
        out.flag.push(Opposed {
            a: cands[x].key.clone(),
            b: cands[y].key.clone(),
            topic: topic_of(&cands[x]),
            opposed: false,
        });
    }
    out.flag
        .sort_by(|a, b| (&a.a, &a.b, a.opposed).cmp(&(&b.a, &b.b, b.opposed)));
    out.flag.dedup_by(|a, b| a.a == b.a && a.b == b.b);
    out
}

/// The decision for one family: the matcher and the small check, closing
/// only where they agree ([`agreed`]). Without a model, or without its
/// answer, nothing is decided.
pub async fn decide(cands: &[Candidate], judge: Option<&dyn Judge>) -> Plan {
    let Some(j) = judge.filter(|_| cands.len() > 1) else {
        return Plan::default();
    };
    let (near_same, near_maybe) = matcher_pairs(cands);
    let mut close: Vec<(usize, usize)> = near_same.clone();
    close.extend(near_maybe.iter().copied());
    let Ok(text) = j.judge(SYSTEM, &prompt(cands, &close)).await else {
        return Plan::default();
    };
    let (model_same, model_opposed) = read_answer(Some(&text), cands);
    agreed(cands, &near_same, &model_same, &model_opposed)
}

/// What a closed duplicate's pull request and ticket are told.
pub fn close_comment(keep: &str, topic: &str) -> String {
    format!(
        "Duplicate of {} ({}). Closing; the work continues there. Closed by CoderHelm.",
        keep.trim(),
        topic.trim()
    )
}

/// What each of two flagged tickets is told ([`Opposed`]).
pub fn flagged_comment(f: &Opposed, other: &str) -> String {
    if f.opposed {
        flag_comment(other, &f.topic)
    } else {
        format!(
            "This ticket and {} may be the same work ({}), but CoderHelm's two checks disagree, so it closed neither. \
             A person on the team decides whether one of them goes.",
            other.trim(),
            f.topic.trim()
        )
    }
}

/// What each of two opposed tickets is told.
pub fn flag_comment(other: &str, topic: &str) -> String {
    format!(
        "This ticket and {} cover the same thing ({}) in opposite ways, so CoderHelm closed neither. \
         A person on the team picks which one goes ahead and closes the other.",
        other.trim(),
        topic.trim()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(key: &str, title: &str, status: &str, run: &str, pr: bool) -> Candidate {
        Candidate {
            key: key.into(),
            title: title.into(),
            body: String::new(),
            run_id: run.into(),
            status: status.into(),
            has_pr: pr,
        }
    }

    #[test]
    fn the_kept_ticket_is_finished_then_coded_then_earliest() {
        let a = c("ABC-2", "x", "running", "01A", true);
        let b = c("ABC-3", "x", "completed", "01B", true);
        let d = c("ABC-4", "x", "running", "01C", false);
        let none = c("ABC-1", "x", "", "", false);
        let mut v = [&a, &b, &d, &none];
        v.sort_by_key(|x| rank(x));
        let keys: Vec<&str> = v.iter().map(|x| x.key.as_str()).collect();
        assert_eq!(keys, vec!["ABC-3", "ABC-2", "ABC-4", "ABC-1"]);
    }

    #[test]
    fn the_answer_is_read_only_for_known_tickets() {
        let v = vec![
            c("ABC-1", "a", "", "", false),
            c("ABC-2", "b", "", "", false),
        ];
        let (s, o) = read_answer(
            Some("SAME ABC-2 ABC-1\nOPPOSED ABC-1 ABC-9\nnoise\nOPPOSED `ABC-1` ABC-2"),
            &v,
        );
        assert_eq!(s, vec![(0, 1)]);
        assert_eq!(o, vec![(0, 1)]);
        assert_eq!(read_answer(None, &v), (vec![], vec![]));
    }

    #[test]
    fn opposed_tickets_are_flagged_never_closed() {
        let v = vec![
            c(
                "ABC-6",
                "Fix alt on the unused banner components",
                "completed",
                "01A",
                true,
            ),
            c(
                "ABC-8",
                "Remove the unused banner components",
                "awaiting_ci",
                "01B",
                true,
            ),
        ];
        let p = plan(&v, &[(0, 1)], &[(0, 1)]);
        assert!(p.close.is_empty());
        assert_eq!(p.flag.len(), 1);
        assert_eq!(
            (p.flag[0].a.as_str(), p.flag[0].b.as_str()),
            ("ABC-6", "ABC-8")
        );
    }

    #[test]
    fn a_run_already_over_is_never_closed() {
        let v = vec![
            c("ABC-1", "x", "completed", "01A", true),
            c("ABC-2", "x", "merged", "01B", true),
            c("ABC-3", "x", "", "", false),
        ];
        let p = plan(&v, &[(0, 1), (0, 2)], &[]);
        assert!(p.close.is_empty(), "{p:?}");
        // Still known as covered: a person's start of ABC-3 is refused.
        assert_eq!(p.covering("ABC-3").map(|c| c.keep.as_str()), Some("ABC-1"));
    }

    #[test]
    fn only_pairs_both_readers_call_the_same_close() {
        let v = vec![
            c("ABC-1", "Fix the alt text", "completed", "01A", true),
            c("ABC-2", "Fix the alt text again", "running", "01B", true),
            c(
                "ABC-3",
                "Fix the alt text once more",
                "running",
                "01C",
                true,
            ),
            c("ABC-4", "Delete the alt helper", "running", "01D", true),
        ];
        // Both: 1-2. Matcher only: 1-3. Check only: 2-3. 1-4 fix vs delete.
        let p = agreed(&v, &[(0, 1), (0, 2), (0, 3)], &[(1, 0), (1, 2)], &[]);
        let closes: Vec<(&str, &str)> = p
            .close
            .iter()
            .map(|c| (c.dup.as_str(), c.keep.as_str()))
            .collect();
        assert_eq!(closes, vec![("ABC-2", "ABC-1")]);
        let flags: Vec<(&str, &str, bool)> = p
            .flag
            .iter()
            .map(|f| (f.a.as_str(), f.b.as_str(), f.opposed))
            .collect();
        assert_eq!(
            flags,
            vec![
                ("ABC-1", "ABC-3", false),
                ("ABC-1", "ABC-4", true),
                ("ABC-2", "ABC-3", false),
            ]
        );
        assert!(flagged_comment(&p.flag[0], "ABC-3").contains("disagree"));
        assert!(flagged_comment(&p.flag[1], "ABC-4").contains("opposite"));
    }

    #[test]
    fn the_comments_say_what_happened() {
        assert_eq!(
            close_comment("ABC-1", "Fix alt"),
            "Duplicate of ABC-1 (Fix alt). Closing; the work continues there. Closed by CoderHelm."
        );
        assert!(flag_comment("ABC-2", "Banner").contains("ABC-2"));
        assert!(!flag_comment("ABC-2", "Banner").contains('\u{2192}'));
    }
}
