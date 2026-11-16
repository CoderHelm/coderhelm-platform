//! The fix loop on CoderHelm's own review findings, tracked finding by
//! finding.
//!
//! A run's own review asked for changes on its pull request. Two fix rounds
//! ran; the second one, at the cap of two, "made no progress", and the loop
//! deferred what was open and finished. The rounds had not failed on the
//! code: their edits to the files the fix needed were refused (outside the
//! round's files), their checks were skipped for lack of time, and an edit
//! failed on its own tool input. A blocked round was counted like a round
//! where the model could not solve the finding, the count was per round
//! rather than per finding, and the cap ended the loop before the ladder of
//! approaches was ever climbed. The finding left behind was a one-line
//! config omission, named with its file and line.
//!
//! Here every finding has its own [`Track`]:
//!
//! - a round that a tool or a guard blocked ([`Blocker`]) is not a stall:
//!   the next round on that finding removes the block (no file boundary for
//!   a refused edit, a fresh invocation with its full time when checks were
//!   skipped for time, the tool's exact input shape), up to [`MAX_BLOCKED`];
//! - a round where the model tried and the finding is still open is a
//!   stall, and the next one climbs the ladder for that finding alone: one
//!   finding only, a fresh context with small edits, a re-split, the
//!   strongest model, up to [`MAX_STALLED`];
//! - a finding the ticket, its comments or the plan rule out is settled by
//!   the ticket ([`Settled::ByTicket`]) when the round quotes the words and
//!   they are found there: it is dropped, never deferred;
//! - a finding is deferred only with a reason from the person gate
//!   (`common::person_gate`: a credential, a permission only a person grants,
//!   an irreversible choice nothing settles, work someone else holds, an
//!   approval the team requires, an action outside every system CoderHelm
//!   reaches), claimed by two honest rounds or by the round that exhausted
//!   every approach;
//! - a finding that survives every approach without such a reason is
//!   [`Plan::Unfixed`]: CoderHelm's own limit, never a person's to-do. The
//!   pull request is marked ready with it listed as a deferred note.
//!
//! A round cut short by its step's time ([`Blocker::NoTime`]) is neither a
//! stall nor a block: the check it started is owed, and the next round
//! starts in a fresh step with its full time and runs that check first
//! ([`Track::cut_short`], up to [`MAX_CUT_SHORT`]). A security run once
//! answered a proof-only finding with every check passing on the head but
//! one runtime check the sandbox ran out of time for, and the finding was
//! handed to a person as "still open".

use serde::{Deserialize, Serialize};

use super::supervisor::Rung;

/// Run-record field holding the ledger, as JSON.
pub const FIELD: &str = "own_review_ledger";

/// Rounds on one finding where the model tried and it stayed open: the
/// first attempt and four changes of approach (one finding only, a fresh
/// context, a re-split, the strongest model).
pub const MAX_STALLED: u32 = 5;
/// Rounds on one finding that a tool or a guard blocked, each followed by a
/// round that removes the block.
pub const MAX_BLOCKED: u32 = 3;
/// Rounds on one finding cut short by their step's time, each followed by a
/// round in a fresh step with its full time. Only a safety bound: a check
/// cut short is resumed, and a round that keeps being cut short points at
/// something else, which the pull request then lists as a note.
pub const MAX_CUT_SHORT: u32 = 6;
/// The most fix rounds a run takes on its own findings in all: every
/// finding's bound, for the record's claim.
pub const MAX_ROUNDS: u32 = 24;
/// Times the completion guard sends a finished run back to its checks
/// because its own review still asks for changes on the head.
pub const MAX_CONTINUES: u32 = 2;

/// The person gate's reason for `code` (`common::person_gate::Reason`):
/// the only reasons a finding is deferred for, in the words a person reads.
fn person_label(code: &str) -> Option<&'static str> {
    common::person_gate::REASONS
        .iter()
        .find(|r| r.code() == code.trim())
        .map(|r| r.label())
}

/// What blocked a round, read from the round itself: not the model failing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Blocker {
    /// An edit was refused as outside the round's files.
    Scope,
    /// An edit was refused for another reason.
    Refused,
    /// A check was cut short, or the round stopped, by its step's time:
    /// never a block, the check is owed to a fresh step.
    NoTime,
    /// A tool call failed on its own input (an `edits` sent as a string).
    ToolInput,
}

impl Blocker {
    pub fn words(self) -> &'static str {
        match self {
            Blocker::Scope => "an edit the fix needed was refused as outside the round's files",
            Blocker::Refused => "an edit the fix needed was refused",
            Blocker::NoTime => {
                "a check it started was cut short by its step's time, before it finished"
            }
            Blocker::ToolInput => "an edit failed on the tool's input shape",
        }
    }
}

/// The blockers a round met: its refused edits (`write_guard`) and the
/// markers its tool results carry in the log.
pub fn blockers_of(
    refused: &[super::write_guard::Refusal],
    log: &[serde_json::Value],
) -> Vec<Blocker> {
    let mut out = Vec::new();
    let mut add = |b: Blocker| {
        if !out.contains(&b) {
            out.push(b);
        }
    };
    for r in refused {
        if r.why.contains("outside the round") {
            add(Blocker::Scope);
        } else {
            add(Blocker::Refused);
        }
    }
    for v in log {
        let text = v.to_string();
        if text.contains("outside the round's files") {
            add(Blocker::Scope);
        }
        if text.contains("not run: not enough time left")
            || text.contains("not run yet: owed to the next step")
        {
            add(Blocker::NoTime);
        }
        if text.contains("edit_file not applied") {
            add(Blocker::ToolInput);
        }
    }
    out
}

/// How a finding was settled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Settled {
    /// The next review no longer raised it.
    Fixed,
    /// The ticket, a comment on it or the plan rules it out, in these words.
    ByTicket { quote: String },
    /// A person-gate reason: only a person can do it.
    Deferred { reason: String, what: String },
    /// CoderHelm's own rounds left it open after every approach they have
    /// (`why`): listed on the pull request as CoderHelm's note, and the pull
    /// request goes ready for review. Never a person's to-do, and never
    /// approved over: CoderHelm's verdict on the head stays as it is, so
    /// nothing merges on it.
    Noted { why: String },
}

/// One finding of the run's own review and every round taken on it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Track {
    /// The finding as the review last listed it.
    pub item: String,
    /// Rounds taken on it.
    #[serde(default)]
    pub attempts: u32,
    /// Honest rounds after which it stayed open.
    #[serde(default)]
    pub stalled: u32,
    /// Rounds a tool or a guard blocked.
    #[serde(default)]
    pub blocked: u32,
    /// Rounds cut short by their step's time ([`Blocker::NoTime`]): neither
    /// a stall nor a block, the next round resumes in a fresh step.
    #[serde(default)]
    pub cut_short: u32,
    /// What blocked the last round on it (empty when nothing did).
    #[serde(default)]
    pub last_blocked: Vec<Blocker>,
    /// Rounds that said only a person can do it, with the gate's reason.
    #[serde(default)]
    pub person_claims: u32,
    #[serde(default)]
    pub person_reason: String,
    #[serde(default)]
    pub person_what: String,
    #[serde(default)]
    pub settled: Option<Settled>,
}

/// The round in flight: which findings it took and what it met.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastRound {
    pub taken: Vec<String>,
    #[serde(default)]
    pub blocked: Vec<Blocker>,
    /// The round's own account (its closing notes).
    #[serde(default)]
    pub account: String,
    /// The round ended: what it met is recorded.
    #[serde(default)]
    pub ended: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ledger {
    #[serde(default)]
    pub findings: Vec<Track>,
    #[serde(default)]
    pub last: Option<LastRound>,
    /// Times the completion guard sent the run back ([`MAX_CONTINUES`]).
    #[serde(default)]
    pub continues: u32,
}

/// What the loop does next.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    /// Nothing the review raised is open.
    Ready,
    /// Another round, on `focus`.
    Round {
        focus: Vec<String>,
        approach: Approach,
        /// No file boundary: the last round's edit was refused.
        unscoped: bool,
        /// Start in a fresh invocation with its full time.
        fresh: bool,
    },
    /// Everything left is settled with a reason: the deferred ones (item,
    /// the gate's words and what the person does) and those the ticket
    /// rules out (item, the ticket's words). The review judges the head
    /// again with them settled.
    Settle {
        deferred: Vec<(String, String)>,
        by_ticket: Vec<(String, String)>,
        /// CoderHelm's notes: item, and what its rounds left ([`Settled::Noted`]).
        noted: Vec<(String, String)>,
    },
    /// Findings that survived every approach without a person-gate reason:
    /// the run is not finished and says so.
    Unfixed { why: String },
}

/// The approach a round takes.
#[derive(Debug, Clone, PartialEq)]
pub enum Approach {
    /// Every open finding, as before: nothing stalled.
    All,
    /// The last round was blocked: these blocks are removed.
    Unblock(Vec<Blocker>),
    /// The finding stalled: the ladder's next approach for it alone.
    Ladder(Rung),
}

/// The words of a finding line without where it is: `Title (`path:line`)`.
fn split_line(line: &str) -> (String, String) {
    match line.rsplit_once("(`") {
        Some((title, at)) => {
            let at = at.trim_end().trim_end_matches(')').trim_end_matches('`');
            let path = at.split(':').next().unwrap_or(at).trim().to_string();
            (title.trim().to_string(), path)
        }
        None => (line.trim().to_string(), String::new()),
    }
}

fn words(text: &str) -> Vec<String> {
    let mut out: Vec<String> = text
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric() && c != '-' && c != '_' && c != '.')
        .map(|w| w.trim_matches('.').to_string())
        .filter(|w| w.len() >= 4)
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Whether two finding lines are the same finding: the same file and most
/// of the significant words. A later review words a finding differently and
/// its line moves with the code; it is still the same finding.
pub fn same_finding(a: &str, b: &str) -> bool {
    let (ta, pa) = split_line(a);
    let (tb, pb) = split_line(b);
    if pa != pb {
        return false;
    }
    let (wa, wb) = (words(&ta), words(&tb));
    let small = wa.len().min(wb.len());
    if small == 0 {
        return ta.eq_ignore_ascii_case(&tb);
    }
    let shared = wa.iter().filter(|w| wb.contains(w)).count();
    shared * 2 >= small
}

fn norm(text: &str) -> String {
    text.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// A round's structured lines, after `marker`: `<finding> :: <rest>`.
fn marked<'a>(account: &'a str, marker: &str) -> Vec<(&'a str, Vec<&'a str>)> {
    account
        .lines()
        .filter_map(|l| {
            let l = l.trim().trim_start_matches(['-', '*', ' ']);
            let rest = l.strip_prefix(marker)?.trim();
            let mut parts = rest.split("::").map(str::trim);
            let finding = parts.next()?;
            Some((finding, parts.collect()))
        })
        .collect()
}

pub const TICKET_CONFLICT: &str = "TICKET CONFLICT:";
pub const NEEDS_A_PERSON: &str = "NEEDS A PERSON:";

/// What a fix round is told about the ticket and about what it can't do:
/// a finding the ticket rules out is never coded around, and only a person
/// gate reason makes a finding a person's.
pub fn round_rules(ticket: &str) -> String {
    let codes: Vec<&str> = common::person_gate::REASONS
        .iter()
        .map(|r| r.code())
        .collect();
    let ticket = ticket.trim();
    let ticket = if ticket.is_empty() {
        String::new()
    } else {
        format!(
            "\n\n## The ticket, its comments and the plan\nThe review must not ask for what \
             these rule out; the newest comment wins over the ticket's first words.\n\n{}",
            common::truncate_str(ticket, 12_000)
        )
    };
    format!(
        "\n\n## When a finding can't be fixed in the code\nFix every finding in the code when \
         you can; a tool error or a refused edit is a reason to try the edit another way, \
         never a reason to stop.\n- A finding that asks for something the ticket, a comment on \
         it or the plan rules out: don't change the code for it. Write one line `{TICKET_CONFLICT} \
         <the finding as listed> :: <the exact words from the ticket, a comment or the plan>`.\n- \
         A finding only a person can settle: write one line `{NEEDS_A_PERSON} <the finding as \
         listed> :: <one of: {}> :: <exactly what the person does>`. Nothing else makes a \
         finding a person's.{ticket}",
        codes.join(", ")
    )
}

impl Ledger {
    pub fn of_json(json: &str) -> Ledger {
        serde_json::from_str(json).unwrap_or_default()
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    fn find_mut(&mut self, item: &str) -> Option<&mut Track> {
        self.findings
            .iter_mut()
            .find(|t| same_finding(&t.item, item))
    }

    /// The round about to run takes `focus`.
    pub fn start_round(&mut self, focus: &[String]) {
        self.last = Some(LastRound {
            taken: focus.to_vec(),
            ..Default::default()
        });
    }

    /// The round ended: what it met and its own account.
    pub fn end_round(&mut self, blocked: Vec<Blocker>, account: &str) {
        let last = self.last.get_or_insert_with(LastRound::default);
        last.blocked = blocked;
        last.account = common::truncate_str(account, 6_000).to_string();
        last.ended = true;
    }

    /// A review listed `open` after the last round: what it fixed, what
    /// blocked it, what the ticket rules out, and what only a person can do.
    /// `ticket` is the ticket, its comments and the plan, to check quotes in.
    pub fn observe(&mut self, open: &[String], ticket: &str) {
        let last = self.last.take();
        for t in self.findings.iter_mut().filter(|t| t.settled.is_none()) {
            if !open.iter().any(|o| same_finding(&t.item, o)) {
                t.settled = Some(Settled::Fixed);
            }
        }
        let ticket_n = norm(ticket);
        for item in open {
            if self.find_mut(item).is_none() {
                self.findings.push(Track {
                    item: item.clone(),
                    ..Default::default()
                });
            }
            let t = self.find_mut(item).expect("tracked above");
            t.item = item.clone();
            if matches!(t.settled, Some(Settled::Fixed)) {
                // Raised again after it was fixed: open again.
                t.settled = None;
            }
            let Some(l) = last.as_ref().filter(|l| l.ended) else {
                continue;
            };
            if !l.taken.iter().any(|x| same_finding(x, item)) {
                continue;
            }
            t.attempts += 1;
            let says =
                |finding: &str| same_finding(finding, item) || norm(item).contains(&norm(finding));
            let quote = marked(&l.account, TICKET_CONFLICT)
                .into_iter()
                .filter(|(f, _)| says(f))
                .filter_map(|(_, rest)| {
                    rest.first()
                        .map(|q| q.trim_matches(['"', '`', '\'']).to_string())
                })
                .find(|q| {
                    let q = norm(q);
                    q.len() >= 12 && ticket_n.contains(&q)
                });
            if let Some(quote) = quote {
                t.settled = Some(Settled::ByTicket { quote });
                continue;
            }
            if let Some((code, what)) = marked(&l.account, NEEDS_A_PERSON)
                .into_iter()
                .filter(|(f, _)| says(f))
                .find_map(|(_, rest)| {
                    let code = rest.first()?.to_string();
                    person_label(&code)?;
                    Some((code, rest.get(1).map(|w| w.to_string()).unwrap_or_default()))
                })
            {
                t.person_claims += 1;
                t.person_reason = code;
                t.person_what = what;
            }
            match round_ending(&l.blocked) {
                RoundEnding::Honest => {
                    t.stalled += 1;
                    t.last_blocked.clear();
                }
                RoundEnding::CutShort => {
                    t.cut_short += 1;
                    t.last_blocked = vec![Blocker::NoTime];
                }
                RoundEnding::Blocked => {
                    t.blocked += 1;
                    t.last_blocked = l.blocked.clone();
                }
            }
            // Two honest rounds that said the same person-gate reason.
            if t.person_claims >= 2 && t.last_blocked.is_empty() {
                t.settled = Some(Settled::Deferred {
                    reason: t.person_reason.clone(),
                    what: t.person_what.clone(),
                });
            }
        }
    }

    fn exhausted(t: &Track) -> bool {
        t.stalled >= MAX_STALLED || t.blocked >= MAX_BLOCKED || t.cut_short >= MAX_CUT_SHORT
    }

    /// A round ended and no review has read it yet: its account (proof it
    /// gave, a check it ran) is what the next review of the head judges.
    /// A round that pushed nothing leaves the head where it was, so only a
    /// review of that same head can read it.
    pub fn round_unread(&self) -> bool {
        self.last.as_ref().is_some_and(|l| l.ended)
    }

    /// What follows for the findings `open` now, by each one's own record.
    pub fn decide(&self, open: &[String], spent_usd: f64, cap_usd: f64) -> Plan {
        let tracks: Vec<&Track> = open
            .iter()
            .filter_map(|o| self.findings.iter().find(|t| same_finding(&t.item, o)))
            .collect();
        let live: Vec<&Track> = tracks
            .iter()
            .copied()
            .filter(|t| t.settled.is_none())
            .collect();
        if open.is_empty() {
            return Plan::Ready;
        }
        // Every finding left with its reason: settled, or exhausted with a
        // person-gate reason claimed.
        let settle = || {
            let mut deferred = Vec::new();
            let mut by_ticket = Vec::new();
            let mut noted = Vec::new();
            for t in tracks.iter().copied() {
                match &t.settled {
                    Some(Settled::Noted { why }) => noted.push((t.item.clone(), why.clone())),
                    Some(Settled::ByTicket { quote }) => {
                        by_ticket.push((t.item.clone(), quote.clone()))
                    }
                    Some(Settled::Deferred { reason, what }) => {
                        deferred.push((t.item.clone(), deferred_words(reason, what)))
                    }
                    None if !t.person_reason.is_empty() => deferred.push((
                        t.item.clone(),
                        deferred_words(&t.person_reason, &t.person_what),
                    )),
                    _ => {}
                }
            }
            Plan::Settle {
                deferred,
                by_ticket,
                noted,
            }
        };
        if live.is_empty() {
            return settle();
        }
        if super::supervisor::near_spend_cap(spent_usd, cap_usd) {
            return Plan::Unfixed {
                why: format!(
                    "the run reached its spend ceiling with {} still open: {}",
                    count(live.len()),
                    names(&live)
                ),
            };
        }
        let Some(target) = live.iter().copied().find(|t| !Self::exhausted(t)) else {
            // Every approach was taken on every finding left.
            let (gated, not): (Vec<&Track>, Vec<&Track>) = live
                .iter()
                .copied()
                .partition(|t| !t.person_reason.is_empty());
            if !not.is_empty() {
                return Plan::Unfixed {
                    why: format!(
                        "CoderHelm's review still asks for changes after every approach on {} \
                         ({}): {}",
                        count(not.len()),
                        not.iter()
                            .map(|t| format!(
                                "{} rounds, {} blocked by tools or guards, {} cut short by time",
                                t.attempts, t.blocked, t.cut_short
                            ))
                            .collect::<Vec<_>>()
                            .join("; "),
                        names(&not)
                    ),
                };
            }
            debug_assert!(!gated.is_empty());
            return settle();
        };
        let items = |ts: &[&Track]| ts.iter().map(|t| t.item.clone()).collect::<Vec<_>>();
        if !target.last_blocked.is_empty() {
            let b = target.last_blocked.clone();
            return Plan::Round {
                focus: items(&[target]),
                unscoped: b
                    .iter()
                    .any(|x| matches!(x, Blocker::Scope | Blocker::Refused)),
                fresh: b.contains(&Blocker::NoTime),
                approach: Approach::Unblock(b),
            };
        }
        if live.iter().all(|t| t.stalled == 0) {
            let fresh: Vec<&Track> = live
                .iter()
                .copied()
                .filter(|t| !Self::exhausted(t))
                .collect();
            return Plan::Round {
                focus: items(&fresh),
                approach: Approach::All,
                unscoped: false,
                fresh: false,
            };
        }
        let rung = match target.stalled {
            0 => Rung::Continue,
            1 => Rung::Narrow,
            2 => Rung::SwitchTools,
            3 => Rung::Resplit,
            _ => Rung::Escalate,
        };
        Plan::Round {
            focus: items(&[target]),
            approach: Approach::Ladder(rung),
            unscoped: false,
            fresh: false,
        }
    }

    /// Records `items` as CoderHelm's notes ([`Settled::Noted`]): its own
    /// rounds left them open for `why`. The pull request lists them and goes
    /// ready; the review no longer holds the draft on them.
    pub fn note(&mut self, items: &[String], why: &str) {
        for item in items {
            if self.find_mut(item).is_none() {
                self.findings.push(Track {
                    item: item.clone(),
                    ..Default::default()
                });
            }
            let t = self.find_mut(item).expect("tracked above");
            if !matches!(
                t.settled,
                Some(Settled::ByTicket { .. } | Settled::Deferred { .. })
            ) {
                t.settled = Some(Settled::Noted {
                    why: why.to_string(),
                });
            }
        }
    }

    /// Whether every finding still open on `open` is settled with a reason
    /// (a person-gate reason, or the ticket), so the review's verdict on the
    /// head must not hold on it.
    pub fn settled_item(&self, line: &str) -> Option<&Settled> {
        self.findings
            .iter()
            .find(|t| same_finding(&t.item, line))
            .and_then(|t| match &t.settled {
                Some(Settled::Fixed) | None => None,
                Some(s) => Some(s),
            })
    }
}

/// How a round ended, from what it met.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoundEnding {
    /// Nothing blocked it: the model tried.
    Honest,
    /// Only its step's time cut it short: the check is owed, not blocked.
    CutShort,
    /// A tool or a guard blocked it.
    Blocked,
}

fn round_ending(blocked: &[Blocker]) -> RoundEnding {
    if blocked.is_empty() {
        RoundEnding::Honest
    } else if blocked.iter().all(|b| *b == Blocker::NoTime) {
        RoundEnding::CutShort
    } else {
        RoundEnding::Blocked
    }
}

/// The deferred note for findings CoderHelm's own rounds left open (`why`):
/// listed on the pull request, which is marked ready. Never a person's
/// to-do: it says what CoderHelm did and what is left.
pub fn note_lines(items: &[String], why: &str) -> Vec<String> {
    items
        .iter()
        .map(|i| format!("{i}: CoderHelm's note, left open after its own rounds ({why})"))
        .collect()
}

fn deferred_words(code: &str, what: &str) -> String {
    let label = person_label(code).unwrap_or("only a person can do it");
    if what.trim().is_empty() {
        label.to_string()
    } else {
        format!("{label}: {}", what.trim())
    }
}

fn count(n: usize) -> String {
    if n == 1 {
        "1 finding".to_string()
    } else {
        format!("{n} findings")
    }
}

fn names(ts: &[&Track]) -> String {
    ts.iter()
        .map(|t| t.item.as_str())
        .collect::<Vec<_>>()
        .join("; ")
}

/// What a round on `approach` is told, after the findings.
pub fn instruction(approach: &Approach, focus: &[String]) -> String {
    let what = format!("CoderHelm's own review findings ({})", focus.join("; "));
    match approach {
        Approach::All => String::new(),
        Approach::Ladder(r) => super::supervisor::round_instruction(*r, &what),
        Approach::Unblock(b) if b.iter().all(|x| *x == Blocker::NoTime) => format!(
            "\n\n## This round's approach\nThe last round on {what} was cut short by its step's \
             time before a check it started finished. That check is owed: this round starts in a \
             fresh step with its full time. Run the check the last round's account names as not \
             confirmed first, as its own `bash` call, then finish what is left. Write a result \
             only from output you have here."
        ),
        Approach::Unblock(b) => format!(
            "\n\n## This round's approach\nThe last round on {what} didn't fail on the code: {}. \
             This round removes that: it has no file boundary (edit the file the fix needs, and \
             say why in one line), it starts with its full time, and `edit_file` takes `edits` as \
             a JSON array of {{\"old_text\", \"new_text\"}} objects, never a string. Make the \
             change, save it, and run the checks that cover it.",
            b.iter().map(|x| x.words()).collect::<Vec<_>>().join("; ")
        ),
    }
}

/// The deferred lines for the pull request: each item with its person-gate
/// reason, and each item the ticket rules out with its words.
pub fn settled_lines(
    deferred: &[(String, String)],
    by_ticket: &[(String, String)],
    noted: &[(String, String)],
) -> Vec<String> {
    deferred
        .iter()
        .map(|(i, why)| format!("{i}: needs a person ({why})"))
        .chain(
            by_ticket
                .iter()
                .map(|(i, q)| format!("{i}: not done, the ticket rules it out (\"{q}\")")),
        )
        .chain(
            noted
                .iter()
                .flat_map(|(i, why)| note_lines(std::slice::from_ref(i), why)),
        )
        .collect()
}

/// What the completion guard decides for a run about to be marked
/// completed with its pull request, from its own review's latest verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Completion {
    /// Its review approves the head (or has nothing on it to hold).
    Complete,
    /// Its review still asks for changes on the head: the run goes back to
    /// its checks and its review, not to "completed".
    Continue(String),
    /// It asked for changes on the head after every approach CoderHelm
    /// has: CoderHelm's own limit, never a person's to-do. What is open is
    /// listed on the pull request as a deferred note with this, and the
    /// pull request is marked ready.
    Defer(String),
    /// A deferred finding has no reason only a person can give: never
    /// "completed" on it.
    Hold(String),
}

/// The completion guard: a run never reaches "completed" while its own
/// review's latest verdict on the pull request's head is "changes
/// requested", nor with a deferred finding that has no person-gate reason.
pub fn completion(verdict: Option<(&str, &str)>, head: &str, ledger: &Ledger) -> Completion {
    let unreasoned: Vec<&Track> = ledger
        .findings
        .iter()
        .filter(|t| matches!(&t.settled, Some(Settled::Deferred { reason, .. }) if person_label(reason).is_none()))
        .collect();
    if !unreasoned.is_empty() {
        return Completion::Hold(format!(
            "deferred without a reason only a person can give: {}",
            names(&unreasoned)
        ));
    }
    let changes = matches!(verdict, Some((v, sha)) if v == "REQUEST_CHANGES" && (sha == head || head.is_empty()));
    if !changes {
        return Completion::Complete;
    }
    // What the review still asks for is CoderHelm's own notes, listed on the
    // ready pull request: nothing more to do, and nothing approved over.
    let noted = ledger
        .findings
        .iter()
        .any(|t| matches!(t.settled, Some(Settled::Noted { .. })));
    if noted && ledger.findings.iter().all(|t| t.settled.is_some()) {
        return Completion::Complete;
    }
    let short = head.get(..7).unwrap_or(head);
    if ledger.continues >= MAX_CONTINUES {
        return Completion::Defer(format!(
            "CoderHelm's review still asks for changes on `{short}` after the run went back to \
             its checks {} times",
            ledger.continues
        ));
    }
    if let Some(t) = ledger
        .findings
        .iter()
        .find(|t| t.settled.is_none() && Ledger::exhausted(t) && t.person_reason.is_empty())
    {
        return Completion::Defer(format!(
            "CoderHelm's review still asks for changes on `{short}` and every approach on this \
             finding was taken: {}",
            t.item
        ));
    }
    Completion::Continue(format!(
        "CoderHelm's review asks for changes on `{short}`: the run goes back to its checks and \
         its review, and fixes what is open before it finishes"
    ))
}

// ── The record ──────────────────────────────────────────────────────────────

use crate::WorkerState;
use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;

pub fn of_item(item: &HashMap<String, AttributeValue>) -> Ledger {
    item.get(FIELD)
        .and_then(|v| v.as_s().ok())
        .map(|j| Ledger::of_json(j))
        .unwrap_or_default()
}

pub async fn load(state: &WorkerState, team_id: &str, run_id: &str) -> Ledger {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(team_id))
        .key("run_id", super::attr_s(run_id))
        .projection_expression("#l")
        .expression_attribute_names("#l", FIELD)
        .consistent_read(true)
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .map(|i| of_item(&i))
        .unwrap_or_default()
}

pub async fn save(state: &WorkerState, team_id: &str, run_id: &str, ledger: &Ledger) {
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", super::attr_s(team_id))
        .key("run_id", super::attr_s(run_id))
        .update_expression("SET #l = :l")
        .expression_attribute_names("#l", FIELD)
        .expression_attribute_values(":l", super::attr_s(&ledger.to_json()))
        .send()
        .await
    {
        tracing::warn!(run_id, error = %e, "Could not record the own review's findings ledger");
    }
}

/// A review-fix round ended: what blocked it and its account, on the
/// ledger, for the next review to read.
pub async fn record_round_end(
    state: &WorkerState,
    team_id: &str,
    run_id: &str,
    blocked: Vec<Blocker>,
    account: &str,
) {
    let mut l = load(state, team_id, run_id).await;
    l.end_round(blocked, account);
    save(state, team_id, run_id, &l).await;
}

/// The ticket, the comments people wrote on it and the plan's cause, from
/// the run's record: what the review and the fix rounds hold findings to.
pub async fn ticket_text(
    state: &WorkerState,
    team_id: &str,
    item: &HashMap<String, AttributeValue>,
) -> String {
    use common::ticket_comments::Store;
    let s = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let mut out = format!("### Ticket: {}\n{}", s("title"), s("ticket_body"));
    let ticket_id = s("ticket_id");
    if !ticket_id.is_empty() {
        let store = common::ticket_comments::Dynamo {
            client: &state.dynamo,
            table: &state.config.settings_table_name,
        };
        if let Ok(comments) = store.of_ticket(team_id, &ticket_id).await {
            for c in comments.iter().filter(|c| !c.text.trim().is_empty()) {
                out.push_str(&format!(
                    "\n\n### Comment on the ticket by {}\n{}",
                    if c.author.is_empty() {
                        "a person"
                    } else {
                        &c.author
                    },
                    c.text.trim()
                ));
            }
        }
    }
    let plan = s("fix_decision");
    if let Some(cause) = serde_json::from_str::<serde_json::Value>(&plan)
        .ok()
        .and_then(|v| v["cause"].as_str().map(str::to_string))
    {
        out.push_str(&format!("\n\n### The plan\n{cause}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shape of the finding the run deferred: a one-line config
    /// omission, with its file and line. The second review words it
    /// differently and its line moved.
    const NAV: &str = "Site navbar and footer still render on the portal page; signup-portal isn't in settings.blankLayout (`apps/web/src/lib/routing/renderers.tsx:1632`)";
    const NAV_LATER: &str = "The site navbar and footer still render on the portal pages because `signup-portal` isn't in settings.blankLayout (`apps/web/src/lib/routing/renderers.tsx:1636`)";
    const LINT: &str = "e2e lint fails on the two new files (`e2e/tests/signup.spec.ts:121`)";

    fn ended(l: &mut Ledger, focus: &[&str], blocked: Vec<Blocker>, account: &str) {
        l.start_round(&focus.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        l.end_round(blocked, account);
    }

    #[test]
    fn the_same_finding_is_known_across_reviews_that_word_it_differently() {
        assert!(same_finding(NAV, NAV_LATER));
        assert!(!same_finding(NAV, LINT));
        assert!(!same_finding(
            NAV,
            "Site navbar and footer still render (`apps/web/src/other.tsx:3`)"
        ));
    }

    #[test]
    fn a_blocked_round_is_not_a_stall_and_the_next_one_removes_the_block() {
        let mut l = Ledger::default();
        let open = vec![NAV.to_string(), LINT.to_string()];
        l.observe(&open, "");
        assert!(matches!(
            l.decide(&open, 0.0, 200.0),
            Plan::Round {
                approach: Approach::All,
                ..
            }
        ));
        // The round's edits were refused and its checks skipped for time.
        ended(
            &mut l,
            &[NAV, LINT],
            vec![Blocker::Scope, Blocker::NoTime],
            "",
        );
        let open = vec![NAV_LATER.to_string()];
        l.observe(&open, "");
        let t = l
            .findings
            .iter()
            .find(|t| same_finding(&t.item, NAV))
            .unwrap();
        assert_eq!((t.stalled, t.blocked), (0, 1));
        assert_eq!(
            l.findings
                .iter()
                .find(|t| same_finding(&t.item, LINT))
                .unwrap()
                .settled,
            Some(Settled::Fixed)
        );
        match l.decide(&open, 0.0, 200.0) {
            Plan::Round {
                focus,
                approach: Approach::Unblock(b),
                unscoped,
                fresh,
            } => {
                assert_eq!(focus, vec![NAV_LATER.to_string()]);
                assert!(unscoped && fresh, "{b:?}");
                let said = instruction(&Approach::Unblock(b), &focus);
                assert!(said.contains("no file boundary") && said.contains("never a string"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_stalled_finding_climbs_its_own_ladder_and_is_never_deferred_without_a_gate_reason() {
        let mut l = Ledger::default();
        let open = vec![NAV.to_string()];
        l.observe(&open, "");
        let mut rungs = Vec::new();
        for _ in 0..MAX_STALLED {
            match l.decide(&open, 0.0, 200.0) {
                Plan::Round {
                    approach, focus, ..
                } => {
                    rungs.push(approach);
                    ended(&mut l, &[focus[0].as_str()], vec![], "Tried.");
                }
                other => panic!("{other:?}"),
            }
            l.observe(&open, "");
        }
        assert_eq!(
            rungs,
            vec![
                Approach::All,
                Approach::Ladder(Rung::Narrow),
                Approach::Ladder(Rung::SwitchTools),
                Approach::Ladder(Rung::Resplit),
                Approach::Ladder(Rung::Escalate),
            ]
        );
        match l.decide(&open, 0.0, 200.0) {
            Plan::Unfixed { why } => assert!(why.contains("every approach"), "{why}"),
            other => panic!("{other:?}"),
        }
        // CoderHelm's own limit: listed as its note, never held for a person.
        assert!(matches!(
            completion(Some(("REQUEST_CHANGES", "abcdef1234")), "abcdef1234", &l),
            Completion::Defer(_)
        ));
    }

    #[test]
    fn a_located_one_line_omission_is_fixed_by_the_loop_not_deferred() {
        // Round 1: blocked by the scope guard; round 2: unscoped, it lands
        // and the next review no longer raises it.
        let mut l = Ledger::default();
        let open = vec![NAV.to_string()];
        l.observe(&open, "");
        ended(&mut l, &[NAV], vec![Blocker::Scope], "");
        l.observe(&[NAV_LATER.to_string()], "");
        let Plan::Round {
            unscoped: true,
            focus,
            ..
        } = l.decide(&[NAV_LATER.to_string()], 0.0, 200.0)
        else {
            panic!("an unscoped round on the finding");
        };
        ended(
            &mut l,
            &[focus[0].as_str()],
            vec![],
            "Added it to the list.",
        );
        l.observe(&[], "");
        assert_eq!(l.decide(&[], 0.0, 200.0), Plan::Ready);
        assert_eq!(l.findings[0].settled, Some(Settled::Fixed));
        assert_eq!(
            completion(Some(("APPROVE", "abcdef1234")), "abcdef1234", &l),
            Completion::Complete
        );
    }

    #[test]
    fn a_finding_the_ticket_rules_out_is_settled_by_the_ticket_not_deferred() {
        let ticket = "### Ticket: Sign-up pages\nEmbed the portal.\n\n### Comment on the ticket by a person\nKeep your site's header and footer. You do not need to remove the footer.";
        let mut l = Ledger::default();
        let open = vec![NAV.to_string()];
        l.observe(&open, ticket);
        ended(
            &mut l,
            &[NAV],
            vec![],
            &format!("{TICKET_CONFLICT} {NAV} :: \"Keep your site's header and footer\""),
        );
        l.observe(&open, ticket);
        match l.decide(&open, 0.0, 200.0) {
            Plan::Settle {
                deferred,
                by_ticket,
                noted,
            } => {
                assert!(deferred.is_empty());
                assert_eq!(by_ticket.len(), 1);
                let lines = settled_lines(&deferred, &by_ticket, &noted);
                assert!(lines[0].contains("the ticket rules it out"), "{lines:?}");
            }
            other => panic!("{other:?}"),
        }
        assert!(l.settled_item(NAV_LATER).is_some());
        // A quote the ticket doesn't hold settles nothing: it is a stall.
        let mut m = Ledger::default();
        m.observe(&open, ticket);
        ended(
            &mut m,
            &[NAV],
            vec![],
            &format!("{TICKET_CONFLICT} {NAV} :: \"remove the navbar everywhere\""),
        );
        m.observe(&open, ticket);
        assert!(m.settled_item(NAV).is_none());
        assert_eq!(m.findings[0].stalled, 1);
    }

    #[test]
    fn only_a_person_gate_reason_defers_and_it_must_hold_twice() {
        let mut l = Ledger::default();
        let open = vec![NAV.to_string()];
        l.observe(&open, "");
        let claim =
            format!("{NEEDS_A_PERSON} {NAV} :: grant :: give the app write access to the CMS");
        ended(&mut l, &[NAV], vec![], &claim);
        l.observe(&open, "");
        assert!(l.settled_item(NAV).is_none(), "one claim is not enough");
        ended(&mut l, &[NAV], vec![], &claim);
        l.observe(&open, "");
        match l.decide(&open, 0.0, 200.0) {
            Plan::Settle { deferred, .. } => {
                assert!(deferred[0]
                    .1
                    .contains("a permission only a person can grant"));
            }
            other => panic!("{other:?}"),
        }
        // A made-up reason is no reason.
        let mut m = Ledger::default();
        m.observe(&open, "");
        for _ in 0..2 {
            ended(
                &mut m,
                &[NAV],
                vec![],
                &format!("{NEEDS_A_PERSON} {NAV} :: too hard :: x"),
            );
            m.observe(&open, "");
        }
        assert!(m.settled_item(NAV).is_none());
    }

    #[test]
    fn the_completion_guard_never_completes_on_changes_requested() {
        let l = Ledger::default();
        assert_eq!(completion(None, "h", &l), Completion::Complete);
        assert_eq!(
            completion(Some(("APPROVE", "h")), "h", &l),
            Completion::Complete
        );
        // A verdict of an older head is not this head's.
        assert_eq!(
            completion(Some(("REQUEST_CHANGES", "old")), "h", &l),
            Completion::Complete
        );
        assert!(matches!(
            completion(Some(("REQUEST_CHANGES", "h")), "h", &l),
            Completion::Continue(_)
        ));
        let spent = Ledger {
            continues: MAX_CONTINUES,
            ..Default::default()
        };
        assert!(matches!(
            completion(Some(("REQUEST_CHANGES", "h")), "h", &spent),
            Completion::Defer(_)
        ));
        let bad = Ledger {
            findings: vec![Track {
                item: NAV.into(),
                settled: Some(Settled::Deferred {
                    reason: "no progress".into(),
                    what: String::new(),
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(matches!(
            completion(Some(("APPROVE", "h")), "h", &bad),
            Completion::Hold(_)
        ));
    }

    #[test]
    fn blockers_are_read_from_the_refusals_and_the_log() {
        let refused = vec![super::super::write_guard::Refusal {
            path: "a.tsx".into(),
            why: "refused: outside the round's files".into(),
        }];
        let log = vec![
            serde_json::json!({"content": "not run: not enough time left"}),
            serde_json::json!({"content": "Error: edit_file not applied: `edits` was a string"}),
        ];
        assert_eq!(
            blockers_of(&refused, &log),
            vec![Blocker::Scope, Blocker::NoTime, Blocker::ToolInput]
        );
        assert!(blockers_of(&[], &[]).is_empty());
    }

    #[test]
    fn the_round_is_told_the_ticket_and_the_only_reasons() {
        let r = round_rules("### Ticket: x\nKeep the footer.");
        assert!(r.contains("Keep the footer."));
        for reason in common::person_gate::REASONS {
            assert!(r.contains(reason.code()));
        }
        assert!(r.contains(TICKET_CONFLICT) && r.contains(NEEDS_A_PERSON));
    }
}
