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
//! - in each group of the same topic, one ticket is kept ([`keeper`]). The
//!   keeper is decided once and recorded ([`Kept`], `FOLLOWUP_KEEP#`), and
//!   every later sweep keeps the recorded one while it is still in the
//!   running. A ticket that is out ([`Candidate::out`]: closed, cancelled,
//!   covered, its pull request closed) is never kept; when none is
//!   recorded, or the recorded one went out, the keeper is picked from the
//!   members still in ([`rank`]: finished work, then code with a pull
//!   request, then the earliest). Every other run in the group is closed as
//!   a duplicate of it ([`close_comment`]), except one a person holds
//!   ([`person_holds`]) and never in a way that leaves the topic, or the
//!   family, with no open pull request;
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
    /// Its pull request is still open: neither merged nor closed.
    pub pr_open: bool,
    /// Its pull request merged.
    pub pr_merged: bool,
}

impl Candidate {
    fn finished(&self) -> bool {
        matches!(self.status.as_str(), "completed" | "merged")
    }
    /// Out of the running for its topic, so never its keeper: its run was
    /// closed or cancelled (by a person or by an earlier sweep), ended
    /// covered by another pull request, was started again or archived, or
    /// its pull request was closed without merging.
    pub fn out(&self) -> bool {
        use crate::run_status as rs;
        matches!(
            self.status.as_str(),
            rs::CLOSED | rs::CANCELLED | rs::COVERED_BY_PR | rs::RETRIED | rs::ARCHIVED
        ) || (self.has_pr && !self.pr_open && !self.merged())
    }
    fn merged(&self) -> bool {
        self.pr_merged || self.status == crate::run_status::MERGED
    }
    /// It carries its topic's work: its pull request is open or merged.
    pub fn holds_work(&self) -> bool {
        self.pr_open || self.merged()
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

/// One keeper decision, recorded once per family and keeper
/// ([`keep_record_sk`]) and honoured by every later sweep: `keep` is kept
/// for the topic its group (`members`) covers while it is in the running.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Kept {
    pub keep: String,
    pub members: Vec<String>,
    pub topic: String,
    /// When it was decided (RFC 3339); the earliest decision wins when two
    /// recorded keepers meet in one group.
    pub at: String,
}

/// The settings record of `keep` as the keeper decided in `root`'s family.
pub fn keep_record_sk(root: &str, keep: &str) -> String {
    format!(
        "{}{}",
        keep_record_prefix(root),
        keep.trim().to_ascii_uppercase()
    )
}

/// The start of every keeper record of `root`'s family.
pub fn keep_record_prefix(root: &str) -> String {
    format!("FOLLOWUP_KEEP#{}#", root.trim().to_ascii_uppercase())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    /// Keeper decisions this plan makes that no record holds yet: written
    /// (once, conditionally) before any close.
    pub record: Vec<Kept>,
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

/// The keeper of one group (`members`, indexes into `cands`): the member a
/// keeper record names, when one still in the running does (the earliest
/// decision first), else the best ranked member still in the running.
/// `None` when every member is out. The flag says whether a record already
/// holds the decision.
pub fn keeper(cands: &[Candidate], members: &[usize], kept: &[Kept]) -> Option<(usize, bool)> {
    let live: Vec<usize> = members
        .iter()
        .copied()
        .filter(|&i| !cands[i].out())
        .collect();
    let mut recorded: Vec<(&Kept, usize)> = kept
        .iter()
        .filter_map(|k| {
            live.iter()
                .copied()
                .find(|&i| cands[i].key.eq_ignore_ascii_case(k.keep.trim()))
                .map(|i| (k, i))
        })
        .collect();
    recorded.sort_by(|a, b| (&a.0.at, &a.0.keep).cmp(&(&b.0.at, &b.0.keep)));
    if let Some(&(_, i)) = recorded.first() {
        return Some((i, true));
    }
    live.into_iter()
        .min_by_key(|&i| rank(&cands[i]))
        .map(|i| (i, false))
}

/// Whether a person holds a pull request, so no sweep closes it: a commit
/// of theirs is on it (not CoderHelm's or another app's, not a merge of
/// another branch), or a review of theirs asked for changes. `commits` and
/// `reviews` are GitHub's lists for the pull request.
pub fn person_holds(commits: &[serde_json::Value], reviews: &[serde_json::Value]) -> bool {
    let committed = commits.iter().any(|c| {
        !crate::person_lines::is_bot_commit(c)
            && c["parents"].as_array().map_or(0, |p| p.len()) <= 1
    });
    let asked = reviews.iter().any(|r| {
        let login = r["user"]["login"].as_str().unwrap_or("");
        r["state"].as_str() == Some("CHANGES_REQUESTED")
            && r["user"]["type"].as_str() != Some("Bot")
            && !login.ends_with("[bot]")
    });
    committed || asked
}

/// The note on a duplicate's pull request when it is closed: which ticket
/// and pull request carry the work now, and that its branch stays.
pub fn pr_close_note(keep: &str, topic: &str, keeper_pr: &str, branch: &str) -> String {
    let carried = if keeper_pr.trim().is_empty() {
        format!("{} carries this work", keep.trim())
    } else {
        format!("{} carries this work in {}", keep.trim(), keeper_pr.trim())
    };
    let kept = if branch.trim().is_empty() {
        "Its branch is kept.".to_string()
    } else {
        format!("Its branch `{}` is kept.", branch.trim())
    };
    format!(
        "Closed by CoderHelm as a duplicate follow-up ({}): {carried}. {kept}",
        topic.trim()
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
/// keeps ([`keeper`], honouring `kept`), the runs it closes, and the
/// opposed pairs it flags.
///
/// A duplicate is closed only in favour of a keeper it is paired with
/// itself. A close never takes the last open pull request: a duplicate
/// with an open pull request closes only when the keeper's work is open or
/// merged, and never when it is the family's last open one.
pub fn plan(
    cands: &[Candidate],
    same: &[(usize, usize)],
    opposed: &[(usize, usize)],
    kept: &[Kept],
) -> Plan {
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
    // A duplicate is one of its keeper's own pairs: a chain through a third
    // ticket (one closed already, often) never makes two topics one.
    let paired = |i: usize, k: usize| {
        same.iter()
            .any(|&(a, b)| (a.min(b), a.max(b)) == (i.min(k), i.max(k)))
    };
    let mut out = Plan::default();
    // The family's open pull requests: a close never takes the last one.
    let mut open_prs = cands.iter().filter(|c| c.pr_open).count();
    for members in groups.values() {
        if members.len() < 2 {
            continue;
        }
        let Some((k, recorded)) = keeper(cands, members, kept) else {
            continue;
        };
        let keep = &cands[k];
        if !recorded {
            let mut keys: Vec<String> = members.iter().map(|&i| cands[i].key.clone()).collect();
            keys.sort();
            out.record.push(Kept {
                keep: keep.key.clone(),
                members: keys,
                topic: topic_of(keep),
                at: String::new(),
            });
        }
        let mut m: Vec<usize> = members.iter().copied().filter(|&i| i != k).collect();
        m.sort_by_key(|&i| rank(&cands[i]));
        for i in m {
            let c = &cands[i];
            if in_opposition(i) || !paired(i, k) {
                continue;
            }
            let one = Close {
                dup: c.key.clone(),
                run_id: c.run_id.clone(),
                keep: keep.key.clone(),
                topic: topic_of(keep),
            };
            if !c.closable() || c.out() {
                out.covered.push(one);
                continue;
            }
            if c.pr_open {
                if !keep.holds_work() || open_prs <= 1 {
                    // Its pull request would be the topic's (or the
                    // family's) last open one: it stays.
                    continue;
                }
                open_prs -= 1;
            }
            out.close.push(one);
        }
    }
    for &(a, b) in &opp {
        if cands[a].out() || cands[b].out() {
            // One of them was closed already: nothing is left to choose.
            continue;
        }
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

/// The decision when both readers are in, on the same topic key the
/// follow-up filing uses (`followup_tickets::near`, then the small check
/// for a maybe): a pair closes only when the small check reads it as the
/// same work and the matcher finds the two near (the same topic, or maybe
/// the same). That is what filing would never have filed twice, so a
/// family filed before that rule is reconciled on it too. A pair the
/// matcher reads as the same and the small check doesn't, or the small
/// check reads as the same and the matcher finds unrelated, is flagged; a
/// pair either reads as opposed, or a removal against a fix of what either
/// reads as the same, is flagged as opposed. Nothing else is touched.
pub fn agreed(
    cands: &[Candidate],
    matcher_same: &[(usize, usize)],
    matcher_maybe: &[(usize, usize)],
    model_same: &[(usize, usize)],
    model_opposed: &[(usize, usize)],
    kept: &[Kept],
) -> Plan {
    let norm = |&(a, b): &(usize, usize)| (a.min(b), a.max(b));
    let m: Vec<(usize, usize)> = matcher_same.iter().map(norm).collect();
    let near: Vec<(usize, usize)> = m
        .iter()
        .copied()
        .chain(matcher_maybe.iter().map(norm))
        .collect();
    let k: Vec<(usize, usize)> = model_same.iter().map(norm).collect();
    let mut opposed: Vec<(usize, usize)> = model_opposed.iter().map(norm).collect();
    for p in m.iter().chain(k.iter()) {
        if removes(&cands[p.0].title) != removes(&cands[p.1].title) && !opposed.contains(p) {
            opposed.push(*p);
        }
    }
    let both: Vec<(usize, usize)> = k
        .iter()
        .filter(|p| near.contains(p) && !opposed.contains(p))
        .copied()
        .collect();
    let mut out = plan(cands, &both, &opposed, kept);
    let mut unsure: Vec<(usize, usize)> = m
        .iter()
        .chain(k.iter())
        .filter(|p| !both.contains(p) && !opposed.contains(p))
        .filter(|p| !cands[p.0].out() && !cands[p.1].out())
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
/// only where both lean the same way ([`agreed`]), keeping the keepers `kept` records.
/// Without a model, or without its answer, nothing is decided.
pub async fn decide(cands: &[Candidate], judge: Option<&dyn Judge>, kept: &[Kept]) -> Plan {
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
    agreed(
        cands,
        &near_same,
        &near_maybe,
        &model_same,
        &model_opposed,
        kept,
    )
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
            pr_open: pr && status != "merged",
            pr_merged: pr && status == "merged",
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
        let p = plan(&v, &[(0, 1)], &[(0, 1)], &[]);
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
        let p = plan(&v, &[(0, 1), (0, 2)], &[], &[]);
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
        let p = agreed(
            &v,
            &[(0, 1), (0, 2), (0, 3)],
            &[],
            &[(1, 0), (1, 2)],
            &[],
            &[],
        );
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

    fn closes(p: &Plan) -> Vec<(&str, &str)> {
        p.close
            .iter()
            .map(|c| (c.dup.as_str(), c.keep.as_str()))
            .collect()
    }

    /// What a sweep's plan leaves behind: its new records stamped, its
    /// closes applied (the run closed by CoderHelm, its pull request closed).
    fn apply(v: &mut [Candidate], p: &Plan, kept: &mut Vec<Kept>, at: &str) {
        for r in &p.record {
            kept.push(Kept {
                at: at.into(),
                ..r.clone()
            });
        }
        for c in &p.close {
            let d = v.iter_mut().find(|x| x.key == c.dup).unwrap();
            d.status = "closed".into();
            d.pr_open = false;
        }
    }

    #[test]
    fn the_keeper_is_recorded_once_and_a_later_sweep_never_flips_it() {
        // Three tickets on one topic, each with an open pull request.
        let mut v = vec![
            c("ABC-14", "x", "running", "01A", true),
            c("ABC-19", "x", "completed", "01C", true),
            c("ABC-23", "x", "running", "01B", true),
        ];
        let same = [(0, 1), (0, 2), (1, 2)];
        let mut kept = Vec::new();
        // Sweep A: ABC-19 finished, so it is kept; the other two close.
        let a = plan(&v, &same, &[], &kept);
        assert_eq!(closes(&a), vec![("ABC-14", "ABC-19"), ("ABC-23", "ABC-19")]);
        assert_eq!(a.record.len(), 1);
        assert_eq!(a.record[0].keep, "ABC-19");
        apply(&mut v, &a, &mut kept, "2026-01-01T10:00:00Z");
        // Between sweeps the keeper's run goes back to work on a review:
        // it no longer counts as finished. Ranked afresh, the earliest run
        // (the closed ABC-14) would come first.
        v[1].status = "running".into();
        let b = plan(&v, &same, &[], &kept);
        assert!(b.close.is_empty(), "the keeper is never closed: {b:?}");
        assert!(b.record.is_empty(), "the decision is not made again");
        assert_eq!(
            b.covering("ABC-14").map(|c| c.keep.as_str()),
            Some("ABC-19")
        );
        assert_eq!(
            b.covering("ABC-23").map(|c| c.keep.as_str()),
            Some("ABC-19")
        );
        // Even with no record at all, a closed member is never the keeper.
        let fresh = plan(&v, &same, &[], &[]);
        assert!(fresh.close.is_empty(), "{fresh:?}");
        assert_eq!(fresh.record[0].keep, "ABC-19");
    }

    #[test]
    fn a_chain_through_a_closed_ticket_never_joins_two_topics() {
        // ABC-2 and ABC-4 are each the same as the closed ABC-3, not as each
        // other: neither closes the other.
        let mut v = vec![
            c("ABC-2", "x", "completed", "01A", true),
            c("ABC-3", "x", "closed", "01B", true),
            c("ABC-4", "x", "completed", "01C", true),
        ];
        v[1].pr_open = false;
        let p = plan(&v, &[(0, 1), (1, 2)], &[], &[]);
        assert!(p.close.is_empty(), "{p:?}");
    }

    #[test]
    fn a_maybe_the_small_check_reads_as_the_same_closes_as_filing_would() {
        // In other words: the matcher finds them only maybe the same.
        let v = vec![
            c(
                "ABC-2",
                "Stop filenames as alt in fragments",
                "completed",
                "01A",
                true,
            ),
            c(
                "ABC-3",
                "Image alt falls back to the asset filename",
                "completed",
                "01B",
                true,
            ),
            c(
                "ABC-4",
                "Coach image alt from the coach name",
                "completed",
                "01C",
                true,
            ),
        ];
        // 2-3: maybe, and the same work. 2-4: maybe, not the same.
        let p = agreed(&v, &[], &[(0, 1), (0, 2)], &[(0, 1)], &[], &[]);
        assert_eq!(closes(&p), vec![("ABC-3", "ABC-2")]);
        assert!(p.flag.is_empty(), "{:?}", p.flag);
        // The small check alone, on two the matcher finds unrelated, closes
        // nothing: it is flagged.
        let p = agreed(&v, &[], &[], &[(0, 1)], &[], &[]);
        assert!(p.close.is_empty());
        assert_eq!(p.flag.len(), 1);
        assert!(!p.flag[0].opposed);
    }

    #[test]
    fn a_recorded_keeper_wins_over_a_better_ranked_member() {
        let v = vec![
            c("ABC-2", "x", "running", "01B", true),
            c("ABC-3", "x", "completed", "01A", true),
        ];
        let kept = vec![Kept {
            keep: "ABC-2".into(),
            members: vec!["ABC-2".into(), "ABC-3".into()],
            topic: "x".into(),
            at: "2026-01-01T10:00:00Z".into(),
        }];
        let p = plan(&v, &[(0, 1)], &[], &kept);
        assert_eq!(closes(&p), vec![("ABC-3", "ABC-2")]);
        assert!(p.record.is_empty());
    }

    #[test]
    fn a_keeper_a_person_closed_is_picked_again_from_the_open_members() {
        let mut v = vec![
            c("ABC-2", "x", "closed", "01A", true),
            c("ABC-3", "x", "running", "01C", true),
            c("ABC-4", "x", "running", "01B", true),
        ];
        v[0].pr_open = false;
        let kept = vec![Kept {
            keep: "ABC-2".into(),
            members: vec!["ABC-2".into(), "ABC-3".into(), "ABC-4".into()],
            topic: "x".into(),
            at: "2026-01-01T10:00:00Z".into(),
        }];
        let p = plan(&v, &[(0, 1), (0, 2), (1, 2)], &[], &kept);
        assert_eq!(p.record.len(), 1);
        assert_eq!(p.record[0].keep, "ABC-4", "the earliest open run");
        assert_eq!(closes(&p), vec![("ABC-3", "ABC-4")]);
        assert_eq!(p.covering("ABC-2").map(|c| c.keep.as_str()), Some("ABC-4"));
    }

    #[test]
    fn a_group_with_every_member_out_keeps_and_closes_nothing() {
        let mut v = vec![
            c("ABC-2", "x", "closed", "01A", true),
            c("ABC-3", "x", "cancelled", "01B", true),
        ];
        for x in &mut v {
            x.pr_open = false;
        }
        let p = plan(&v, &[(0, 1)], &[], &[]);
        assert_eq!(p, Plan::default());
    }

    #[test]
    fn the_last_open_pull_request_is_never_closed() {
        // The keeper has no pull request yet: the duplicate's open one stays.
        let v = vec![
            c("ABC-2", "x", "running", "01A", false),
            c("ABC-3", "x", "running", "01B", true),
        ];
        let kept = vec![Kept {
            keep: "ABC-2".into(),
            at: "2026-01-01T10:00:00Z".into(),
            ..Default::default()
        }];
        let p = plan(&v, &[(0, 1)], &[], &kept);
        assert!(p.close.is_empty(), "{p:?}");
        assert!(p.covering("ABC-3").is_none(), "its start is not refused");
        // The keeper merged: the duplicate's pull request is the family's
        // last open one, so it stays too.
        let v = vec![
            c("ABC-2", "x", "merged", "01A", true),
            c("ABC-3", "x", "running", "01B", true),
        ];
        assert!(plan(&v, &[(0, 1)], &[], &[]).close.is_empty());
        // A duplicate with no pull request still closes: no work is lost.
        let v = vec![
            c("ABC-2", "x", "completed", "01A", true),
            c("ABC-3", "x", "running", "01B", false),
        ];
        assert_eq!(
            closes(&plan(&v, &[(0, 1)], &[], &[])),
            vec![("ABC-3", "ABC-2")]
        );
    }

    #[test]
    fn a_pull_request_closed_without_merging_is_out() {
        let mut x = c("ABC-2", "x", "completed", "01A", true);
        assert!(!x.out() && x.holds_work());
        x.pr_open = false;
        assert!(x.out() && !x.holds_work());
        x.pr_merged = true;
        assert!(!x.out() && x.holds_work());
        for s in [
            "closed",
            "cancelled",
            "covered_by_pr",
            "retried",
            "archived",
        ] {
            assert!(c("ABC-2", "x", s, "01A", false).out(), "{s}");
        }
    }

    #[test]
    fn a_person_holds_a_pull_request_with_a_commit_or_a_change_request() {
        use serde_json::json;
        let bot = json!({"author": {"login": "coderhelm[bot]", "type": "Bot"}, "parents": [{}]});
        let merge = json!({"author": {"login": "dana", "type": "User"}, "parents": [{}, {}]});
        let person = json!({"author": {"login": "dana", "type": "User"}, "parents": [{}]});
        let approve = json!({"user": {"login": "dana", "type": "User"}, "state": "APPROVED"});
        let ask = json!({"user": {"login": "dana", "type": "User"}, "state": "CHANGES_REQUESTED"});
        let app_ask = json!({"user": {"login": "coderhelm[bot]", "type": "Bot"}, "state": "CHANGES_REQUESTED"});
        assert!(!person_holds(&[bot.clone(), merge], &[approve, app_ask]));
        assert!(person_holds(&[bot.clone(), person], &[]));
        assert!(person_holds(&[bot], &[ask]));
    }

    #[test]
    fn the_pull_request_note_names_the_keeper_and_keeps_the_branch() {
        let n = pr_close_note("ABC-1", "Fix alt", "harbor/web#12", "coderhelm/abc-3");
        assert_eq!(
            n,
            "Closed by CoderHelm as a duplicate follow-up (Fix alt): ABC-1 carries this work in harbor/web#12. \
             Its branch `coderhelm/abc-3` is kept."
        );
        assert!(!n.contains('\u{2192}'));
    }

    #[test]
    fn keeper_records_are_per_family_and_keeper() {
        assert_eq!(
            keep_record_sk(" abc-1 ", "abc-4"),
            "FOLLOWUP_KEEP#ABC-1#ABC-4"
        );
        assert!(keep_record_sk("ABC-1", "ABC-4").starts_with(&keep_record_prefix("ABC-1")));
        assert!(!keep_record_prefix("ABC-1").starts_with("FOLLOWUP#"));
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
