//! Scope is decided once, written down, and held.
//!
//! A pull request for one duplicated filter chip planned the fix and wrote
//! "Out: the path pages, an open product question" in its proposal. Its
//! task list then said "switch any other caller", a later round built the
//! path pages, the scope check took the code out again (tests are exempt
//! from that check, so the path-page tests stayed), an automatic revert put
//! the tests back, and a person finally deleted them by hand. Its
//! description kept saying the path pages were out of scope while the diff
//! changed them, and asked the team the question the plan had already
//! answered with a default.
//!
//! The rules here:
//!
//! - **One decision, at plan time.** The plan writes `## Out of scope` in
//!   design.md, one `- <item>: <why>` line each ([`Ledger::from_plan`]). An
//!   open product question is never left open: it becomes an entry decided
//!   with its default ([`default_of`]). [`plan_gaps`] sends a plan back
//!   that has no such section, leaves a question open, gives an item no
//!   reason, has a task that names an out-of-scope item, or has an
//!   open-ended task ("switch any other caller") that could reach one.
//! - **Every round is held to it.** [`breaches`] reads the diff, tests
//!   included: a changed path an entry names, or an added line carrying one
//!   of its marks (a backticked name, a path, a route), is a breach the
//!   round's check sends back.
//! - **Scope changes only with a reason someone gave.** A round that must
//!   build an out-of-scope item writes `Scope change: <item>: <why>`; it is
//!   accepted only when the why cites an acceptance criterion or a person's
//!   comment ([`Ledger::apply_changes`]), and the entry then records who
//!   moved it in and why.
//! - **The description says what the ledger says.** Its out-of-scope line
//!   is rendered from the entries the diff keeps out; an entry the diff
//!   breaches is said as not verified, never as out of scope
//!   ([`contradictions`] drops a written sentence that claims otherwise).
//!   Question sections in a description become defaults
//!   ([`questions_to_defaults`]), sections about earlier attempts go
//!   ([`drop_history`]), and the written notes fit a length scaled to the
//!   diff ([`budget_words`], [`fit`]).
//!
//! Everything here is pure text: the worker loads and saves the ledger on
//! the run (`FIELD`).

use serde::{Deserialize, Serialize};

/// The run attribute that holds the ledger as JSON.
pub const FIELD: &str = "scope_ledger";
/// The plan's section.
pub const OUT_HEADING: &str = "## Out of scope";
/// The coder's line that moves an item into scope.
pub const CHANGE_MARK: &str = "scope change:";
/// Who decided an entry.
pub const BY_PLAN: &str = "plan";
pub const BY_DEFAULT: &str = "default";

/// One decision about scope.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// What is left out, in a few words.
    pub item: String,
    /// Why it is left out (for a question decided by default, the default).
    #[serde(default)]
    pub why: String,
    /// What in a diff shows the item: backticked names, paths, routes.
    #[serde(default)]
    pub marks: Vec<String>,
    /// [`BY_PLAN`] or [`BY_DEFAULT`].
    #[serde(default)]
    pub by: String,
    /// Set when the item was moved into scope later: the criterion or the
    /// comment that asked for it.
    #[serde(default)]
    pub moved_in: String,
}

impl Entry {
    /// Still out of scope.
    pub fn is_out(&self) -> bool {
        self.moved_in.trim().is_empty()
    }

    /// The entry as one line: `item (why)`.
    pub fn line(&self) -> String {
        let item = self.item.trim().trim_end_matches('.');
        let why = self.why.trim().trim_end_matches('.');
        if why.is_empty() {
            item.to_string()
        } else {
            format!("{item} ({why})")
        }
    }
}

/// The run's scope decisions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ledger {
    #[serde(default)]
    pub entries: Vec<Entry>,
    /// What is in scope: the ticket's acceptance criteria as the run read
    /// them before any code (`stop_judge::acceptance`, the same list the
    /// planner's criteria-to-test mapping reads). An item a criterion asks
    /// for is never out of scope.
    #[serde(default)]
    pub criteria: Vec<String>,
}

/// An out-of-scope item the diff touches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Breach {
    pub item: String,
    pub why: String,
    pub path: String,
    /// The mark found (a path, or a name on an added line).
    pub mark: String,
    /// The diff takes out an item a person or a criterion moved into
    /// scope (`why` says who asked), instead of bringing back one left out.
    #[serde(default)]
    pub reverted: bool,
}

impl Breach {
    /// The instruction for the round that fixes it.
    pub fn instruction(&self) -> String {
        if self.reverted {
            return format!(
                "`{}` takes out \"{}\" (`{}`), which is in scope: {}. Put it back; what a person asked for is never reverted as out of scope.",
                self.path,
                self.item.trim_end_matches('.'),
                self.mark,
                self.why.trim_end_matches('.'),
            );
        }
        format!(
            "`{}` brings back \"{}\" (`{}`), which the plan decided is out of scope: {}. Take it out of the code and its tests, or, only when an acceptance criterion or a person's comment asks for it, add `Scope change: {}: <the criterion or comment, quoted>` to your Evidence.",
            self.path,
            self.item.trim_end_matches('.'),
            self.mark,
            self.why.trim_end_matches('.'),
            self.item.trim_end_matches('.'),
        )
    }
}

fn lower(s: &str) -> String {
    s.to_lowercase()
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The text of a heading line (`## X`, `**X**`, `**X:**`), lowercased; `None`
/// for any other line.
fn heading(line: &str) -> Option<String> {
    let t = line.trim();
    if t.starts_with('#') {
        let h = t.trim_start_matches('#').trim().trim_matches('*').trim();
        return Some(lower(h.trim_end_matches(':')));
    }
    if let Some(inner) = t.strip_prefix("**") {
        if let Some(end) = inner.find("**") {
            let rest = inner[end + 2..].trim();
            if rest.is_empty() {
                return Some(lower(inner[..end].trim().trim_end_matches(':')));
            }
        }
    }
    None
}

/// Whether a heading names one of `names` (each a lowercase prefix).
fn named(h: &str, names: &[&str]) -> bool {
    names.iter().any(|n| h.starts_with(n))
}

/// The lines under every heading that starts with one of `names`, up to the
/// next heading.
fn sections<'a>(text: &'a str, names: &[&str]) -> Vec<&'a str> {
    let mut out = Vec::new();
    let mut inside = false;
    for l in text.lines() {
        if let Some(h) = heading(l) {
            inside = named(&h, names);
            continue;
        }
        if inside {
            out.push(l);
        }
    }
    out
}

/// `text` without the sections whose heading starts with one of `names`
/// (heading included), and the lines taken out.
fn without_sections(
    text: &str,
    names: &[&str],
    keep: &dyn Fn(&str) -> bool,
) -> (String, Vec<String>) {
    let mut kept: Vec<&str> = Vec::new();
    let mut gone: Vec<String> = Vec::new();
    let mut inside = false;
    // A dropped section's heading, kept only if a line under it stays.
    let mut held: Option<&str> = None;
    for l in text.lines() {
        if let Some(h) = heading(l) {
            inside = named(&h, names);
            held = None;
            if inside {
                held = Some(l);
            } else {
                kept.push(l);
            }
            continue;
        }
        if inside && !keep(l) {
            if !l.trim().is_empty() {
                gone.push(l.trim().to_string());
            }
        } else {
            if let Some(h) = held.take() {
                kept.push(h);
            }
            kept.push(l);
        }
    }
    let mut out = kept.join("\n");
    while out.contains("\n\n\n") {
        out = out.replace("\n\n\n", "\n\n");
    }
    (out.trim().to_string(), gone)
}

/// A bullet's text, without its marker.
fn bullet(line: &str) -> Option<String> {
    let t = line.trim_start();
    let rest = t
        .strip_prefix("- ")
        .or_else(|| t.strip_prefix("* "))
        .or_else(|| t.strip_prefix("+ "))?;
    let rest = rest
        .trim_start_matches("[ ] ")
        .trim_start_matches("[x] ")
        .trim();
    (!rest.is_empty()).then(|| rest.to_string())
}

const NONE_WORDS: &[&str] = &["none", "nothing", "n/a", "no open"];

fn is_none(text: &str) -> bool {
    let l = lower(text.trim().trim_start_matches(['-', '*']).trim());
    NONE_WORDS.iter().any(|n| {
        l == *n
            || l.starts_with(&format!("{n}."))
            || l.starts_with(&format!("{n}:"))
            || l.starts_with(&format!("{n} "))
    })
}

/// Words too common to tell anything apart.
const STOP_MARKS: &[&str] = &[
    "true", "false", "null", "none", "this", "that", "the", "todo", "test", "tests", "main",
    "index", "string", "number", "id", "ids",
];

fn clean_mark(raw: &str) -> String {
    raw.trim()
        .trim_matches(|c: char| ",.;:()[]{}\"'".contains(c))
        .trim_end_matches("()")
        .to_string()
}

fn is_identifier(s: &str) -> bool {
    s.chars().count() >= 4
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        && s.chars().next().is_some_and(|c| !c.is_ascii_digit())
        && (s.contains('_')
            || s.chars().skip(1).any(|c| c.is_ascii_uppercase())
            || s.chars().any(|c| c.is_ascii_digit()))
}

fn is_path_like(s: &str) -> bool {
    !s.contains("://")
        && !s.contains(' ')
        && ((s.contains('/') && s.chars().count() >= 4)
            || s.rsplit_once('.').is_some_and(|(a, ext)| {
                !a.is_empty()
                    && (1..=5).contains(&ext.len())
                    && ext.chars().all(|c| c.is_ascii_alphanumeric())
                    && ext.chars().any(|c| c.is_ascii_alphabetic())
            }))
}

/// What in a diff shows an item: its backticked names (identifiers, paths,
/// routes) and the bare paths and routes in its text.
pub fn marks_of(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |m: String| {
        if m.chars().count() >= 4
            && !STOP_MARKS.contains(&lower(&m).as_str())
            && !out.iter().any(|o| o.eq_ignore_ascii_case(&m))
        {
            out.push(m);
        }
    };
    for (i, part) in text.split('`').enumerate() {
        if i % 2 == 1 {
            let m = clean_mark(part);
            if is_identifier(&m) || is_path_like(&m) {
                push(m);
            }
        } else {
            for w in part.split_whitespace() {
                let m = clean_mark(w);
                let bare_path = (m.starts_with('/') && m.chars().count() >= 4)
                    || m.matches('/').count() >= 2
                    || (m.contains('/')
                        && m.rsplit_once('.')
                            .is_some_and(|(_, e)| (1..=5).contains(&e.len())));
                if bare_path && is_path_like(&m) {
                    push(m);
                }
            }
        }
    }
    out
}

/// A plan or description line `item: why` (or `item, because why`, or
/// `item (why)`) as an entry.
pub fn parse_line(line: &str, by: &str) -> Option<Entry> {
    let t = one_line(line.trim().trim_start_matches(['-', '*', '+']).trim());
    if t.is_empty() || is_none(&t) {
        return None;
    }
    let marks = marks_of(&t);
    // Split at the first ": " outside backticks, else " because ", else a
    // trailing parenthesis.
    let mut depth = false;
    let mut at = None;
    for (i, c) in t.char_indices() {
        if c == '`' {
            depth = !depth;
        } else if !depth && (c == ':' || c == '\u{2014}') && t[i + c.len_utf8()..].starts_with(' ')
        {
            at = Some((i, c.len_utf8()));
            break;
        }
    }
    let (item, why) = if let Some((i, n)) = at {
        (t[..i].trim().to_string(), t[i + n..].trim().to_string())
    } else if let Some((a, b)) = t.split_once(" because ") {
        (a.trim().to_string(), b.trim().to_string())
    } else if let Some((a, b)) = t.split_once(", which ") {
        (a.trim().to_string(), format!("it {}", b.trim()))
    } else if t.ends_with(')') {
        match t.rfind(" (") {
            Some(i) => (
                t[..i].trim().to_string(),
                t[i + 2..t.len() - 1].trim().to_string(),
            ),
            None => (t.clone(), String::new()),
        }
    } else {
        (t.clone(), String::new())
    };
    // Bold the line's markers left behind ("**Item**: why" splits into
    // "Item**" and "why") is no part of either.
    let item = item.trim_matches('*').trim().to_string();
    let why = why.trim_matches('*').trim().to_string();
    if item.is_empty() {
        return None;
    }
    Some(Entry {
        item: item.trim_end_matches(['.', ',']).to_string(),
        why: why.trim_end_matches('.').to_string(),
        marks,
        by: by.to_string(),
        moved_in: String::new(),
    })
}

/// Words that open a default in a question's line.
const DEFAULT_OPENERS: &[&str] = &[
    "meanwhile:",
    "meanwhile,",
    "meanwhile",
    "for now:",
    "for now,",
    "for now",
    "default:",
    "until someone decides,",
    "until then,",
    "until decided,",
];

/// An open product question, decided with its default: the entry says what
/// stays as it is and why (the default), never asks.
pub fn default_of(question: &str) -> Entry {
    let t = one_line(question.trim().trim_start_matches(['-', '*']).trim());
    let t = t
        .trim_start_matches("Open product question:")
        .trim()
        .to_string();
    let l = lower(&t);
    let mut ask = t.clone();
    let mut default = String::new();
    for o in DEFAULT_OPENERS {
        if let Some(i) = l.find(o) {
            ask = t[..i].trim().to_string();
            default = t[i + o.len()..].trim().to_string();
            break;
        }
    }
    if default.is_empty() {
        if let Some(i) = t.find('?') {
            let rest = t[i + 1..].trim();
            ask = t[..i].to_string();
            default = rest.to_string();
        }
    }
    let ask = ask.trim().trim_end_matches(['?', '.', ' ']).to_string();
    let item = {
        let a = ask.as_str();
        let mut s = a;
        for p in [
            "Should we ",
            "Should ",
            "Do we ",
            "Does ",
            "Do ",
            "Which ",
            "Whether ",
            "Is ",
            "Are ",
            "Can ",
        ] {
            if let Some(r) = a.strip_prefix(p) {
                s = r;
                break;
            }
        }
        let mut c = s.chars();
        match c.next() {
            Some(f) => f.to_uppercase().chain(c).collect::<String>(),
            None => String::new(),
        }
    };
    let default = default.trim().trim_end_matches('.').to_string();
    let why = if default.is_empty() {
        "decided with the default: today's behaviour stays".to_string()
    } else {
        let mut c = default.chars();
        let d = match c.next() {
            Some(f) => f.to_lowercase().chain(c).collect::<String>(),
            None => String::new(),
        };
        format!("decided with the default: {d}")
    };
    Entry {
        marks: marks_of(&t),
        item,
        why,
        by: BY_DEFAULT.to_string(),
        moved_in: String::new(),
    }
}

/// Headings that hold open product questions. "Questions only a person can
/// answer" is not one: `person_gate` already decides those, and only what
/// truly needs a person reaches "Needed from you".
const QUESTION_HEADINGS: &[&str] = &[
    "open product question",
    "open question",
    "product question",
    "questions for the team",
    "questions for reviewers",
    "unresolved question",
];

/// Headings that hold the out-of-scope decisions.
const OUT_HEADINGS: &[&str] = &[
    "out of scope",
    "not in scope",
    "left out",
    "non-goals",
    "non goals",
];

/// Openers of a line that records what a lookup showed: the evidence for
/// the entry above it, never an item of its own. A plan once wrote
/// "Looked up: the page returned 200 and `X` was not in it" under an item,
/// and the check read it as an item left out, named `X`, and told the
/// planner five times over that its tasks name an out-of-scope item.
const EVIDENCE_OPENERS: &[&str] = &[
    "looked up",
    "lookup:",
    "no lookup settles",
    "no lookup",
    "evidence:",
    "checked:",
    "measured:",
    "source:",
    "seen:",
    "shows:",
    "showed:",
];

fn is_evidence(text: &str) -> bool {
    let l = lower(text.trim().trim_matches('*').trim());
    EVIDENCE_OPENERS.iter().any(|o| l.starts_with(o))
}

/// The items an out-of-scope section lists: its top-level bullets. A
/// bullet nested under one (deeper than the section's first bullet) is
/// that item's evidence or detail, and a line that records a lookup is
/// evidence wherever it stands; neither is an item.
fn out_bullets(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut top: Option<usize> = None;
    for l in sections(text, OUT_HEADINGS) {
        let Some(b) = bullet(l) else {
            continue;
        };
        let indent = l.len() - l.trim_start().len();
        let top = *top.get_or_insert(indent);
        if indent > top || is_evidence(&b) {
            continue;
        }
        out.push(b);
    }
    out
}

/// Adds `e` unless an entry already says it: the same item, one item
/// opening the other ("`/x` pages" and "`/x` pages also cover y"), or a
/// path or route both name; marks are merged.
fn push_entry(out: &mut Vec<Entry>, e: Entry) {
    if e.item.trim().is_empty() {
        return;
    }
    let (a, short) = (lower(&e.item), 8);
    let routes = |x: &Entry| -> Vec<String> {
        x.marks
            .iter()
            .filter(|m| m.contains('/'))
            .map(|m| lower(m))
            .collect()
    };
    let mine = routes(&e);
    if let Some(x) = out.iter_mut().find(|x| {
        let b = lower(&x.item);
        a == b
            || (b.len() >= short && a.starts_with(&b))
            || (a.len() >= short && b.starts_with(&a))
            || routes(x).iter().any(|r| mine.contains(r))
    }) {
        for m in e.marks {
            if !x.marks.iter().any(|y| y.eq_ignore_ascii_case(&m)) {
                x.marks.push(m);
            }
        }
        return;
    }
    out.push(e);
}

/// The `Out:` bullets of a proposal's `## Scope` section, nested or inline.
fn scope_outs(proposal: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut in_out = false;
    let mut out_indent = 0usize;
    for l in sections(proposal, &["scope"]) {
        let indent = l.len() - l.trim_start().len();
        let Some(b) = bullet(l) else {
            continue;
        };
        let bl = lower(&b);
        if let Some(rest) = bl.strip_prefix("out:").map(|_| b[4..].trim().to_string()) {
            in_out = true;
            out_indent = indent;
            if !rest.is_empty() {
                out.push(rest);
            }
            continue;
        }
        if bl.starts_with("in:") {
            in_out = false;
            continue;
        }
        if in_out && indent > out_indent {
            out.push(b);
        } else {
            in_out = false;
        }
    }
    out
}

impl Ledger {
    pub fn from_json(s: &str) -> Ledger {
        serde_json::from_str(s).unwrap_or_default()
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The entries still out of scope.
    pub fn outs(&self) -> impl Iterator<Item = &Entry> {
        self.entries.iter().filter(|e| e.is_out())
    }

    /// The decisions a plan wrote: design.md's `## Out of scope` (and the
    /// proposal's), the proposal's `## Scope` `Out:` bullets, and every open
    /// product question decided with its default. A mark the tasks name is
    /// in scope, so it is no mark of an out-of-scope item.
    pub fn from_plan(proposal: &str, design: &str, tasks: &str) -> Ledger {
        let mut entries: Vec<Entry> = Vec::new();
        for text in [design, proposal] {
            for b in out_bullets(text) {
                if let Some(e) = parse_line(&b, BY_PLAN) {
                    push_entry(&mut entries, e);
                }
            }
        }
        for b in scope_outs(proposal) {
            if let Some(e) = parse_line(&b, BY_PLAN) {
                push_entry(&mut entries, e);
            }
        }
        for text in [design, proposal] {
            for l in sections(text, QUESTION_HEADINGS) {
                if let Some(b) = bullet(l) {
                    if !is_none(&b) {
                        push_entry(&mut entries, default_of(&b));
                    }
                }
            }
        }
        let task_text = lower(tasks);
        for e in &mut entries {
            e.marks.retain(|m| !task_text.contains(&lower(m)));
        }
        Ledger {
            entries,
            criteria: Vec::new(),
        }
    }

    /// [`Ledger::from_plan`] decided against the ticket's acceptance
    /// `criteria`: they are what is in scope, and an entry a criterion asks
    /// for is moved in with that criterion as its reason.
    pub fn from_ticket_and_plan(
        criteria: &[String],
        proposal: &str,
        design: &str,
        tasks: &str,
    ) -> Ledger {
        let mut l = Ledger::from_plan(proposal, design, tasks);
        for e in &mut l.entries {
            if let Some(c) = asked_by(e, criteria) {
                e.moved_in = format!("the acceptance criterion \"{}\" asks for it", one_line(c));
            }
        }
        l.criteria = criteria.to_vec();
        l
    }

    /// What the ticket itself names is in scope, whatever the plan wrote
    /// (`ticket`: its title and body). A name or path the ticket says is no
    /// mark of an out-of-scope item (a task may name it, a diff may add it),
    /// and an item the ticket says in full is moved in with that reason.
    /// Returns a line for each item moved in.
    pub fn named_by_ticket(&mut self, ticket: &str) -> Vec<String> {
        let t = lower(ticket);
        if t.trim().is_empty() {
            return Vec::new();
        }
        let mut moved = Vec::new();
        for e in self.entries.iter_mut().filter(|e| e.is_out()) {
            e.marks.retain(|m| !t.contains(&lower(m)));
            let item = lower(e.item.trim().trim_end_matches('.'));
            if item.chars().count() >= 12 && t.contains(&item) {
                e.moved_in = "the ticket names it".to_string();
                moved.push(format!("\"{}\" moved into scope: {}", e.item, e.moved_in));
            }
        }
        moved
    }

    /// Moves an item into scope, recording who asked and why. False when no
    /// entry matches.
    pub fn move_in(&mut self, item: &str, why: &str) -> bool {
        let l = lower(item.trim().trim_matches('`'));
        if l.is_empty() {
            return false;
        }
        let Some(e) = self.entries.iter_mut().find(|e| {
            e.is_out()
                && (lower(&e.item).contains(&l)
                    || l.contains(&lower(&e.item))
                    || e.marks.iter().any(|m| l.contains(&lower(m))))
        }) else {
            return false;
        };
        e.moved_in = one_line(why);
        true
    }

    /// Reads the coder's `Scope change: <item>: <why>` lines and moves each
    /// item in whose why cites `sources` (the ticket's acceptance criteria
    /// and the people's comments on the pull request or the ticket).
    /// Returns the lines accepted and the lines refused, each with why.
    pub fn apply_changes(&mut self, notes: &str, sources: &[String]) -> (Vec<String>, Vec<String>) {
        let mut accepted = Vec::new();
        let mut refused = Vec::new();
        for (item, why) in changes(notes) {
            if !cites(&why, sources) {
                refused.push(format!(
                    "\"{item}\" stays out of scope: \"{why}\" quotes no acceptance criterion and no person's comment."
                ));
                continue;
            }
            if self.move_in(&item, &why) {
                accepted.push(format!("\"{item}\" moved into scope: {why}"));
            }
        }
        (accepted, refused)
    }

    /// What people said decides scope as they say it: each out-of-scope
    /// entry that a person's answer or comment asks for (`said`: who, and
    /// what they wrote; matched as [`asked_by`] matches a criterion) moves
    /// in, recording who asked and their words. No `Scope change:` line is
    /// needed. A reviewer once answered on a pull request that the path
    /// pages should cover both rows; the run replied "Understood" and
    /// reverted the path-page work as out of scope. A comment that keeps an
    /// item out ("leave the path pages for a later ticket") moves nothing.
    /// Returns a line for each item moved in. Run at seed and on each
    /// round's comments.
    pub fn take_people(&mut self, said: &[(String, String)]) -> Vec<String> {
        let mut moved = Vec::new();
        for e in self.entries.iter_mut().filter(|e| e.is_out()) {
            let Some((who, text)) = said.iter().find(|(_, t)| person_asks_for(e, t)) else {
                continue;
            };
            let words: String = one_line(text).chars().take(160).collect();
            let who = if who.trim().is_empty() {
                "a person"
            } else {
                who.trim()
            };
            e.moved_in = format!("{who} asked for it: \"{words}\"");
            moved.push(format!("\"{}\" moved into scope: {}", e.item, e.moved_in));
        }
        moved
    }

    /// The out-of-scope entry a piece of planned work names (one of its
    /// marks, or its item said in full): that work is not built again.
    pub fn covers(&self, text: &str) -> Option<&Entry> {
        let l = lower(text);
        self.outs().find(|e| {
            e.marks.iter().any(|m| l.contains(&lower(m)))
                || (e.item.chars().count() >= 12 && l.contains(&lower(&e.item)))
        })
    }

    /// The out-of-scope lines for a description, without the entries the
    /// diff breaches.
    pub fn lines_kept(&self, breached: &[Breach]) -> Vec<String> {
        self.outs()
            .filter(|e| !breached.iter().any(|b| b.item == e.item))
            .map(Entry::line)
            .collect()
    }
}

/// The coder's `Scope change: <item>: <why>` lines.
pub fn changes(notes: &str) -> Vec<(String, String)> {
    notes
        .lines()
        .filter_map(|l| {
            let t = l.trim().trim_start_matches(['-', '*']).trim();
            let at = lower(t).find(CHANGE_MARK)?;
            let rest = t[at + CHANGE_MARK.len()..].trim();
            let (item, why) = rest.split_once(": ")?;
            let item = item.trim().trim_matches('`').trim();
            let why = why.trim();
            (!item.is_empty() && !why.is_empty()).then(|| (item.to_string(), why.to_string()))
        })
        .collect()
}

/// Words that tell no item apart from another: a criterion and an item
/// that share only these share nothing. "No layout shift when the page
/// loads or when items toggle" once matched an item about one script per
/// page, on "when" (counted twice) and "page".
const COMMON_WORDS: &[&str] = &[
    "when", "with", "that", "this", "from", "have", "will", "into", "each", "only", "then", "than",
    "there", "their", "what", "which", "while", "also", "does", "should", "must", "they", "them",
    "been", "being", "were", "more", "most", "some", "such", "other", "after", "before", "about",
    "over", "under", "same", "both", "every", "where", "there", "here", "page", "pages", "user",
    "users", "make", "made", "work", "works", "show", "shows",
];

/// The distinct words of `text` that tell an item apart.
fn significant(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for w in lower(text)
        .split(|c: char| !c.is_alphanumeric() && c != '-' && c != '_')
        .filter(|w| w.chars().count() >= 4 && !COMMON_WORDS.contains(w))
    {
        if !out.iter().any(|o| o == w) {
            out.push(w.to_string());
        }
    }
    out
}

/// Words of a comment that keeps an item out rather than asking for it.
const KEEPS_OUT: &[&str] = &[
    "out of scope",
    "leave out",
    "leave it out",
    "leave them out",
    "leave the",
    "not needed",
    "no need",
    "don't",
    "do not",
    "skip",
    "separate ticket",
    "later ticket",
    "another ticket",
    "not in this",
];

/// Whether a person's words ask for an out-of-scope entry: they cite it the
/// way a criterion would, and don't keep it out.
pub fn person_asks_for(e: &Entry, text: &str) -> bool {
    let l = lower(text);
    !KEEPS_OUT.iter().any(|w| l.contains(w)) && asked_by(e, &[text.to_string()]).is_some()
}

/// The criterion that asks for an entry's item, when one does.
pub fn asked_by<'a>(e: &Entry, criteria: &'a [String]) -> Option<&'a String> {
    let said = format!("{} {}", e.item, e.marks.join(" "));
    criteria
        .iter()
        .find(|c| cites(&said, std::slice::from_ref(*c)))
}

/// Whether `why` cites one of `sources`: it shares at least three of a
/// source's significant words (all of them for a shorter source).
pub fn cites(why: &str, sources: &[String]) -> bool {
    let said = significant(why);
    sources.iter().any(|s| {
        let words = significant(s);
        if words.is_empty() {
            return false;
        }
        let shared = words.iter().filter(|w| said.contains(w)).count();
        shared >= words.len().min(3)
    })
}

/// Lines added in a patch.
fn added(patch: &str) -> impl Iterator<Item = &str> {
    patch
        .lines()
        .filter(|l| l.starts_with('+') && !l.starts_with("+++"))
        .map(|l| &l[1..])
}

fn path_hit(path: &str, mark: &str) -> bool {
    let m = mark.trim_start_matches("./");
    is_path_like(m)
        && m.contains('.')
        && !m.starts_with('/')
        && (path == m || path.ends_with(&format!("/{m}")))
}

/// The out-of-scope items `files` (path, status, patch) bring in: a changed
/// path an entry names, or an added line, in code or tests, carrying one of
/// its marks. One breach per item and file.
pub fn breaches(ledger: &Ledger, files: &[(String, String, String)]) -> Vec<Breach> {
    let mut out: Vec<Breach> = Vec::new();
    for e in ledger.outs() {
        for (path, _, patch) in files {
            let found = e
                .marks
                .iter()
                .find(|m| path_hit(path, m))
                .cloned()
                .or_else(|| {
                    e.marks
                        .iter()
                        .filter(|m| !path_hit(path, m))
                        .find(|m| added(patch).any(|l| l.contains(m.as_str())))
                        .cloned()
                });
            if let Some(mark) = found {
                out.push(Breach {
                    item: e.item.clone(),
                    why: e.why.clone(),
                    path: path.clone(),
                    mark,
                    reverted: false,
                });
            }
        }
    }
    // An item moved into scope that the diff takes out: a mark on a
    // removed line the file's added lines no longer carry.
    for e in ledger.entries.iter().filter(|e| !e.is_out()) {
        for (path, _, patch) in files {
            let mark = e.marks.iter().find(|m| {
                removed(patch).any(|l| l.contains(m.as_str()))
                    && !added(patch).any(|l| l.contains(m.as_str()))
            });
            if let Some(mark) = mark {
                out.push(Breach {
                    item: e.item.clone(),
                    why: e.moved_in.clone(),
                    path: path.clone(),
                    mark: mark.clone(),
                    reverted: true,
                });
            }
        }
    }
    out
}

/// Lines removed in a patch.
fn removed(patch: &str) -> impl Iterator<Item = &str> {
    patch
        .lines()
        .filter(|l| l.starts_with('-') && !l.starts_with("---"))
        .map(|l| &l[1..])
}

/// Phrases of a task that reach past the files it names.
const OPEN_ENDED: &[&str] = &[
    "any other caller",
    "any other callers",
    "all other callers",
    "any remaining caller",
    "the other callers",
    "switch any",
    "update any other",
    "and any others",
    "wherever else",
    "everywhere else",
    "the rest of the callers",
];

/// What a draft plan gets wrong about scope, one line each for the
/// send-back. Empty for a plan that stops (no change, a question that
/// blocks it).
pub fn plan_gaps(proposal: &str, design: &str, tasks: &str, criteria: &[String]) -> Vec<String> {
    let t = tasks.trim_start();
    if t.is_empty() || t.starts_with("NO_CHANGES_NEEDED") || t.starts_with("CLARIFICATION_NEEDED") {
        return Vec::new();
    }
    let mut out = Vec::new();
    if !has_out_section(design) {
        out.push(format!(
            "design.md lacks `{OUT_HEADING}`. Decide scope once: one `- <item>: <why>` line for each thing the change leaves out (with the names or paths that would show it), or `None`."
        ));
    }
    out.extend(decision_gaps(proposal, design, tasks, criteria));
    unique(out)
}

/// Whether design.md has its out-of-scope section.
pub fn has_out_section(design: &str) -> bool {
    design
        .lines()
        .any(|l| heading(l).is_some_and(|h| named(&h, OUT_HEADINGS)))
}

/// [`plan_gaps`] without the missing section (`plan_shape` says that with
/// the other sections it asks for).
pub fn decision_gaps(
    proposal: &str,
    design: &str,
    tasks: &str,
    criteria: &[String],
) -> Vec<String> {
    decision_gaps_for(proposal, design, tasks, criteria, "")
}

/// [`decision_gaps`] read against the ticket's own words (`ticket`: its
/// title and body): what the ticket names is in scope, so a task naming it
/// is never a task naming an out-of-scope item ([`Ledger::named_by_ticket`]).
pub fn decision_gaps_for(
    proposal: &str,
    design: &str,
    tasks: &str,
    criteria: &[String],
    ticket: &str,
) -> Vec<String> {
    let t = tasks.trim_start();
    if t.is_empty() || t.starts_with("NO_CHANGES_NEEDED") || t.starts_with("CLARIFICATION_NEEDED") {
        return Vec::new();
    }
    let mut out = Vec::new();
    let asks: Vec<String> = [design, proposal]
        .iter()
        .flat_map(|x| sections(x, QUESTION_HEADINGS))
        .filter_map(bullet)
        .filter(|b| !is_none(b))
        .collect();
    if !asks.is_empty() {
        out.push(format!(
            "The plan leaves {} product question(s) open. Decide each with its default (today's behaviour, or the smallest choice the ticket allows) and list it under `{OUT_HEADING}` as `- <item>: decided with the default: <default>`; never ask the team in the pull request. First: {}",
            asks.len(),
            one_line(&asks[0])
        ));
    }
    let mut ledger = Ledger::from_plan(proposal, design, "");
    ledger.named_by_ticket(ticket);
    for e in ledger.outs() {
        if let Some(c) = asked_by(e, criteria) {
            out.push(format!(
                "`{OUT_HEADING}` leaves out \"{}\", but the ticket's acceptance criterion asks for it: \"{}\". Scope comes from the criteria: plan it.",
                e.item,
                one_line(c)
            ));
        }
    }
    for e in ledger.outs().filter(|e| e.why.trim().is_empty()) {
        out.push(format!(
            "`{OUT_HEADING}` gives no reason for \"{}\". Write `- <item>: <why>`.",
            e.item
        ));
    }
    for line in tasks.lines().filter(|l| bullet(l).is_some()) {
        let l = lower(line);
        for e in ledger.outs() {
            if let Some(m) = e.marks.iter().find(|m| l.contains(&lower(m))) {
                out.push(format!(
                    "A task names `{m}`, which `{OUT_HEADING}` leaves out (\"{}\"). Take it out of the task, or move the item into scope and say why.",
                    e.item
                ));
            }
        }
        if let Some(p) = OPEN_ENDED.iter().find(|p| l.contains(*p)) {
            out.push(format!(
                "A task reaches past its files (\"{p}\"). Search now and name each file it changes as its own task, or list what it leaves out under `{OUT_HEADING}`: {}",
                one_line(line.trim().trim_start_matches(['-', '*']).trim())
                    .chars()
                    .take(160)
                    .collect::<String>()
            ));
        }
    }
    unique(out)
}

/// `lines` in their order, each said once: a check that finds the same
/// thing on five task lines says it once.
pub fn unique(lines: Vec<String>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for l in lines {
        if !out.iter().any(|o| o.trim() == l.trim()) {
            out.push(l);
        }
    }
    out
}

/// A description's question sections and `Open product question:` lines,
/// taken out and decided with their defaults.
pub fn questions_to_defaults(text: &str) -> (String, Vec<Entry>) {
    let (kept, gone) = without_sections(text, QUESTION_HEADINGS, &|_| false);
    let mut entries: Vec<Entry> = Vec::new();
    for g in &gone {
        let b = bullet(g).unwrap_or_else(|| g.clone());
        if b.contains('?') || DEFAULT_OPENERS.iter().any(|o| lower(&b).contains(o)) {
            push_entry(&mut entries, default_of(&b));
        }
    }
    let mut lines: Vec<&str> = Vec::new();
    for l in kept.lines() {
        let t = l.trim().trim_start_matches(['-', '*']).trim();
        if lower(t).starts_with("open product question:") || lower(t).starts_with("open question:")
        {
            push_entry(
                &mut entries,
                default_of(t.split_once(':').map(|x| x.1).unwrap_or(t)),
            );
            continue;
        }
        lines.push(l);
    }
    (lines.join("\n").trim().to_string(), entries)
}

/// Headings about how the pull request got here, not what its head is.
const HISTORY_HEADINGS: &[&str] = &[
    "earlier attempt",
    "previous attempt",
    "prior attempt",
    "attempts",
    "history",
    "timeline",
    "what changed since",
    "changes since",
    "since the last push",
    "update ",
    "updates",
    "deferred",
];

/// A description without its sections about earlier attempts and pushes:
/// it describes the head only. The lines `ci_lines` manages (a revert's
/// "Deferred" line, the "CI is failing" section) stay; `ci_lines` decides
/// whether they are still true.
pub fn drop_history(text: &str) -> (String, Vec<String>) {
    without_sections(text, HISTORY_HEADINGS, &|l| {
        l.contains(crate::ci_lines::REVERT_MARK)
            || l.contains("<!-- coderhelm:ci-failing")
            || l.contains("<!-- /coderhelm:ci-failing")
    })
}

/// The out-of-scope items a description writes: its `Out of scope`
/// sections and `Out of scope: a, and b` sentences.
pub fn written_outs(text: &str) -> Vec<Entry> {
    let mut out: Vec<Entry> = Vec::new();
    for l in sections(text, OUT_HEADINGS) {
        if let Some(e) = bullet(l).and_then(|b| parse_line(&b, BY_PLAN)) {
            push_entry(&mut out, e);
        }
    }
    for l in text.lines() {
        let low = lower(l);
        if let Some(i) = low.find("out of scope:") {
            let rest = &l[i + "out of scope:".len()..];
            let rest = rest.split(". ").next().unwrap_or(rest);
            for part in rest.split(" and ").flat_map(|p| p.split("; ")) {
                let part = part.trim().trim_start_matches(',').trim();
                if let Some(e) = parse_line(part, BY_PLAN) {
                    push_entry(&mut out, e);
                }
            }
        }
    }
    out
}

/// A statement the diff refutes, taken out of a description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refuted {
    pub said: String,
    pub why: String,
}

const KEPT_OUT_WORDS: &[&str] = &[
    "out of scope",
    "not in scope",
    "left out",
    "untouched",
    "not touched",
    "unchanged",
    "still resolve",
    "still filter",
    "keep current behaviour",
    "keep current behavior",
    "keep their current",
    "not changed",
];

const PUT_BACK_WORDS: &[&str] = &[
    "reverted",
    "restored",
    "put back",
    "taken out of this pull request",
    "back to the base",
    "back to base",
    "dropped from this pull request",
];

fn sentences(line: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let b = line.as_bytes();
    for i in 0..b.len() {
        if b[i] == b'.' && (i + 1 == b.len() || b[i + 1] == b' ') {
            out.push(&line[start..=i]);
            start = (i + 1).min(line.len());
        }
    }
    if start < line.len() {
        out.push(&line[start..]);
    }
    out
}

/// A written description held against the diff: a sentence that says an
/// item is out of scope (or untouched) while the diff brings it in, or that
/// a file was reverted or put back while the diff still changes it, is
/// taken out. `breached` are the ledger's items the diff brings in;
/// `changed` the diff's paths. The lines `ci_lines` manages are left to it.
pub fn contradictions(
    text: &str,
    breached: &[Breach],
    changed: &[String],
) -> (String, Vec<Refuted>) {
    let mut refuted = Vec::new();
    let mut lines: Vec<String> = Vec::new();
    for l in text.lines() {
        if l.contains(crate::ci_lines::REVERT_MARK)
            || l.trim_start().starts_with('<')
            || l.trim_start().starts_with('|')
        {
            lines.push(l.to_string());
            continue;
        }
        let mut kept = String::new();
        for s in sentences(l) {
            let low = lower(s);
            let out_claim = KEPT_OUT_WORDS.iter().any(|w| low.contains(w));
            let hit = out_claim
                .then(|| {
                    breached.iter().find(|b| {
                        low.contains(&lower(&b.mark))
                            || (b.item.len() >= 8 && low.contains(&lower(&b.item)))
                    })
                })
                .flatten();
            if let Some(b) = hit {
                refuted.push(Refuted {
                    said: one_line(s),
                    why: format!("the diff changes `{}` (`{}`)", b.path, b.mark),
                });
                continue;
            }
            if PUT_BACK_WORDS.iter().any(|w| low.contains(w)) {
                let named: Vec<&String> = changed
                    .iter()
                    .filter(|p| {
                        let file = p.rsplit('/').next().unwrap_or(p);
                        low.contains(&lower(p))
                            || (file.contains('.') && low.contains(&lower(file)))
                    })
                    .collect();
                if let Some(p) = named.first() {
                    refuted.push(Refuted {
                        said: one_line(s),
                        why: format!("the diff still changes `{p}`"),
                    });
                    continue;
                }
            }
            kept.push_str(s);
        }
        let k = kept.trim_end();
        if l.trim().is_empty() || !k.trim().is_empty() {
            lines.push(if kept.is_empty() {
                l.to_string()
            } else {
                k.to_string()
            });
        }
    }
    let mut out = lines.join("\n");
    while out.contains("\n\n\n") {
        out = out.replace("\n\n\n", "\n\n");
    }
    (out.trim().to_string(), refuted)
}

/// Words the written notes may hold for a change of `code_lines` changed
/// lines (tests and generated files left out): never more than the change
/// itself warrants, with a floor for a one-line fix and a ceiling for a
/// large one (the run page holds the rest).
pub fn budget_words(code_lines: usize) -> usize {
    (80 + 3 * code_lines).clamp(150, 600)
}

pub fn words(text: &str) -> usize {
    text.split_whitespace().count()
}

/// The sentence that says where the rest is.
pub const REST: &str = "The rest of the notes are on the run page.";

/// `text` within `budget` words: whole paragraphs from the top (a heading
/// stays with the paragraph after it), then [`REST`]. CoderHelm's own blocks
/// (`<!-- ... -->` lines) are kept whole. Returns the text and whether
/// anything was cut.
pub fn fit(text: &str, budget: usize) -> (String, bool) {
    // A text already fitted is not cut again.
    let base = text
        .trim_end()
        .strip_suffix(REST)
        .map(str::trim_end)
        .unwrap_or(text);
    if words(base) <= budget {
        return (text.to_string(), false);
    }
    let text = base;
    let paras: Vec<&str> = text.split("\n\n").collect();
    // A paragraph inside one of CoderHelm's marked blocks
    // (`<!-- coderhelm:x -->` to `<!-- /coderhelm:x -->`) is its own.
    let mut owned = vec![false; paras.len()];
    let mut depth = 0i32;
    // A marker opens a block only when its closing marker is in the text
    // (a revert line's mark is a single comment).
    let opens_block = |p: &str| -> i32 {
        p.match_indices("<!-- coderhelm:")
            .filter(|(i, _)| {
                let name: String = p[i + "<!-- coderhelm:".len()..]
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                    .collect();
                !name.is_empty() && text.contains(&format!("<!-- /coderhelm:{name}"))
            })
            .count() as i32
    };
    for (k, p) in paras.iter().enumerate() {
        let opens = opens_block(p);
        let closes = p.matches("<!-- /coderhelm:").count() as i32;
        owned[k] = depth > 0
            || opens > 0
            || closes > 0
            || p.trim_start().starts_with("<!--")
            || p.contains(crate::ci_lines::REVERT_MARK);
        depth = (depth + opens - closes).max(0);
    }
    let mut out: Vec<&str> = Vec::new();
    let mut used = 0usize;
    let mut cut = false;
    let mut i = 0;
    while i < paras.len() {
        let p = paras[i];
        let own = owned[i];
        // A heading alone travels with what follows it.
        let joined_next = !own
            && heading(p.trim()).is_some()
            && p.trim().lines().count() == 1
            && i + 1 < paras.len()
            && !owned[i + 1];
        let n = words(p) + if joined_next { words(paras[i + 1]) } else { 0 };
        if own || (!cut && used + n <= budget) {
            out.push(p);
            if joined_next && !own {
                out.push(paras[i + 1]);
                i += 1;
            }
            if !own {
                used += n;
            }
        } else {
            cut = true;
            if joined_next {
                i += 1;
            }
        }
        i += 1;
    }
    let mut s = out.join("\n\n").trim().to_string();
    if cut {
        if !s.is_empty() {
            s.push_str("\n\n");
        }
        s.push_str(REST);
    }
    (s, cut)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROPOSAL: &str = "## Problem\nTwo chips.\n\n## Scope\n- In: shared wing chips and `?wings=` resolution.\n- Out:\n  - Merging the two vendor rows.\n  - `/tours/<wing>` path pages, which resolve one wing through `findNormalizedWing` and `selectedWing`. Both are open product questions.\n\n## Open product questions\n- Which spelling should the single chip use? Meanwhile it uses whichever row comes first.\n- Should `/tours/<wing>` path pages also cover every row sharing the slug? Meanwhile they keep current behaviour (one id).\n";
    const TASKS: &str = "- [ ] apps/web/src/utils/filterMaps.ts: [modify] Add slugToIds; switch any other slugToId caller in apps/web/src to slugToIds\n- [ ] apps/web/src/components/blocks/WingsFilter.tsx: [modify] dedupe by slug\n";

    #[test]
    fn marks_are_names_paths_and_routes() {
        let m = marks_of("`/tours/<wing>` pages via `findNormalizedWing` and `selectedWing` in apps/web/src/services/tourService.ts; `x`, `the` and https://a.b/c are not");
        assert!(m.contains(&"/tours/<wing>".to_string()), "{m:?}");
        assert!(m.contains(&"findNormalizedWing".to_string()));
        assert!(m.contains(&"selectedWing".to_string()));
        assert!(m.contains(&"apps/web/src/services/tourService.ts".to_string()));
        assert!(!m.iter().any(|x| x.contains("https")), "{m:?}");
    }

    #[test]
    fn a_plan_line_splits_into_item_and_why() {
        let e = parse_line(
            "- Merging the two vendor rows: a data change in another repository",
            BY_PLAN,
        )
        .unwrap();
        assert_eq!(e.item, "Merging the two vendor rows");
        assert_eq!(e.why, "a data change in another repository");
        let e = parse_line(
            "`/tours/<wing>` pages, which resolve one wing through `selectedWing`",
            BY_PLAN,
        )
        .unwrap();
        assert_eq!(e.item, "`/tours/<wing>` pages");
        assert!(e.why.starts_with("it resolve"));
        assert!(parse_line("None", BY_PLAN).is_none());
        assert!(parse_line("- none: the change stays inside this file", BY_PLAN).is_none());
    }

    #[test]
    fn a_question_becomes_its_default() {
        let e = default_of("Should `/tours/<wing>` path pages also cover every row sharing the slug? Meanwhile they keep current behaviour (one id).");
        assert!(
            e.item.starts_with("`/tours/<wing>` path pages also cover"),
            "{}",
            e.item
        );
        assert_eq!(
            e.why,
            "decided with the default: they keep current behaviour (one id)"
        );
        assert_eq!(e.by, BY_DEFAULT);
        assert!(!e.item.contains('?'));
        let bare = default_of("Should members see the block?");
        assert_eq!(
            bare.why,
            "decided with the default: today's behaviour stays"
        );
    }

    #[test]
    fn the_plan_decides_once_and_questions_become_defaults() {
        let l = Ledger::from_plan(PROPOSAL, "## Reuse\n- `slugToIds`\n", TASKS);
        let items: Vec<&str> = l.entries.iter().map(|e| e.item.as_str()).collect();
        assert!(items.contains(&"Merging the two vendor rows"), "{items:?}");
        assert!(
            items
                .iter()
                .any(|i| i.starts_with("`/tours/<wing>` path pages")),
            "{items:?}"
        );
        assert!(
            items
                .iter()
                .any(|i| i.starts_with("Spelling should the single chip use")),
            "{items:?}"
        );
        // The tasks name slugToIds: it is no mark of anything left out.
        assert!(l
            .entries
            .iter()
            .all(|e| !e.marks.iter().any(|m| m == "slugToIds")));
        let path = l
            .entries
            .iter()
            .find(|e| e.item.starts_with("`/tours"))
            .unwrap();
        assert!(
            path.marks.contains(&"selectedWing".to_string()),
            "{:?}",
            path.marks
        );
        let back = Ledger::from_json(&l.to_json());
        assert_eq!(back, l);
    }

    #[test]
    fn plan_gaps_send_back_questions_contradictions_and_open_ended_tasks() {
        let g = plan_gaps(PROPOSAL, "## Reuse\n- x\n", TASKS, &[]);
        assert!(
            g.iter().any(|x| x.contains("lacks `## Out of scope`")),
            "{g:?}"
        );
        assert!(
            g.iter().any(|x| x.contains("product question(s) open")),
            "{g:?}"
        );
        assert!(
            g.iter().any(|x| x.contains("reaches past its files")),
            "{g:?}"
        );
        let good_design = "## Out of scope\n- Merging the two vendor rows: a data fix in another repository\n- `/tours/<wing>` path pages (`selectedWing`): decided with the default: one id, as today\n";
        let good_tasks = "- [ ] apps/web/src/utils/filterMaps.ts: [modify] Add slugToIds\n";
        assert!(plan_gaps("## Problem\nx\n", good_design, good_tasks, &[]).is_empty());
        let contradicting = "- [ ] apps/web/src/components/sanity-blocks/tours-block.tsx: [modify] use every id for `selectedWing`\n";
        let g = plan_gaps("", good_design, contradicting, &[]);
        assert!(
            g.iter().any(|x| x.contains("A task names `selectedWing`")),
            "{g:?}"
        );
        let no_reason = "## Out of scope\n- Merging the two vendor rows\n";
        assert!(plan_gaps("", no_reason, good_tasks, &[])
            .iter()
            .any(|x| x.contains("gives no reason")));
        assert!(plan_gaps("", "", "NO_CHANGES_NEEDED: done", &[]).is_empty());
    }

    fn file(path: &str, added: &[&str]) -> (String, String, String) {
        let patch = added
            .iter()
            .map(|l| format!("+{l}"))
            .collect::<Vec<_>>()
            .join("\n");
        (path.to_string(), "modified".to_string(), patch)
    }

    #[test]
    fn scope_comes_from_the_criteria() {
        let criteria =
            vec!["Only one guest-wing filter shows on the seasonal slots page".to_string()];
        let design = "## Out of scope\n- The duplicate guest-wing filter on seasonal slots: another ticket\n- `/tours/<wing>` path pages (`selectedWing`): decided with the default: one id, as today\n";
        let g = plan_gaps("", design, "- [ ] a.ts: x\n", &criteria);
        assert!(
            g.iter()
                .any(|x| x.contains("acceptance criterion asks for it")),
            "{g:?}"
        );
        assert!(!g.iter().any(|x| x.contains("path pages")), "{g:?}");
        let l = Ledger::from_ticket_and_plan(&criteria, "", design, "");
        assert_eq!(l.criteria, criteria);
        assert_eq!(l.outs().count(), 1);
        assert!(l.outs().next().unwrap().item.contains("/tours/<wing>"));
    }

    #[test]
    fn a_breach_is_found_in_code_and_in_tests() {
        let design = "## Out of scope\n- `/tours/<wing>` path pages (`selectedWing`, `findNormalizedWing`): decided with the default: one id, as today\n";
        let l = Ledger::from_plan("", design, TASKS);
        let in_scope = [file(
            "apps/web/src/utils/filterMaps.ts",
            &["export const slugToIds = () => [];"],
        )];
        assert!(breaches(&l, &in_scope).is_empty());
        let test_only = [file(
            "apps/web/src/test/components/tours-block-dedup.test.tsx",
            &[
                "  it(\"sends every wing id sharing a /tours/<wing> path slug\", () => {",
                "    expect(args.selectedWing?.id).toBe(\"s-b\");",
            ],
        )];
        let b = breaches(&l, &test_only);
        assert_eq!(b.len(), 1, "{b:?}");
        assert!(b[0].instruction().contains("Scope change:"));
        assert!(l
            .covers("Planned: `selectedWing` sends every id on /tours/<wing> pages")
            .is_some());
        assert!(l
            .covers("Planned: one chip per slug in `WingsFilter`")
            .is_none());
        let mut moved = l.clone();
        assert!(moved.move_in(
            "/tours/<wing> path pages",
            "criterion: path pages list every tour"
        ));
        assert!(breaches(&moved, &test_only).is_empty());
    }

    #[test]
    fn a_scope_change_needs_a_criterion_or_a_comment() {
        let design = "## Out of scope\n- `/tours/<wing>` path pages (`selectedWing`): decided with the default: one id, as today\n";
        let mut l = Ledger::from_plan("", design, "");
        let sources = vec![
            "The tours page for one wing lists the tours of every row sharing the wing's slug"
                .to_string(),
        ];
        let (ok, no) = l.apply_changes(
            "Scope change: `/tours/<wing>` path pages: the reviewer might want it",
            &sources,
        );
        assert!(ok.is_empty() && no.len() == 1, "{ok:?} {no:?}");
        assert!(l.entries[0].is_out());
        let (ok, _) = l.apply_changes(
            "- Scope change: /tours/<wing> path pages: the ticket's criterion says the tours page lists every row sharing the wing's slug",
            &sources,
        );
        assert_eq!(ok.len(), 1);
        assert!(!l.entries[0].is_out());
        assert!(l.entries[0].moved_in.contains("criterion"));
    }

    #[test]
    fn questions_in_a_description_become_defaults() {
        let body = "Changes here.\n\n## Open product questions\nThe ticket doesn't settle these. Please answer on the pull request:\n- Which spelling should the chip use? Meanwhile it uses the first row.\n\n## Risk\nLow.\n- Open product question: Does the filter remember the library? For now it does not.";
        let (text, e) = questions_to_defaults(body);
        assert!(!text.to_lowercase().contains("question"), "{text}");
        assert!(text.contains("## Risk"));
        assert_eq!(e.len(), 2, "{e:?}");
        assert_eq!(e[0].why, "decided with the default: it uses the first row");
    }

    #[test]
    fn history_goes_and_the_ci_lines_stay() {
        let body = "Summary.\n\n## Earlier attempts\n- Tried a map first, reverted it.\n\n## Deferred\n- Reverted 1 file to the green head <!-- coderhelm:revert to=abc paths=a.ts -->\n- Waiting on the vendor\n\n## Risk\nLow.";
        let (text, gone) = drop_history(body);
        assert!(!text.contains("Tried a map"));
        assert!(!text.contains("Waiting on the vendor"));
        assert!(text.contains("coderhelm:revert"));
        assert!(text.contains("## Risk"));
        assert_eq!(gone.len(), 2);
    }

    #[test]
    fn a_written_claim_the_diff_refutes_goes() {
        let b = vec![Breach {
            item: "`/tours/<wing>` path pages".into(),
            why: "one id".into(),
            path: "apps/web/src/services/tourService.ts".into(),
            mark: "/tours/<wing>".into(),
            reverted: false,
        }];
        let changed = vec![
            "apps/web/src/services/tourService.ts".to_string(),
            "apps/web/src/test/components/tours-block-dedup.test.tsx".to_string(),
        ];
        let text = "Redirect helpers are untouched. Out of scope: merging the rows, and `/tours/<wing>` pages, which still resolve one wing.\nRestored `tours-block-dedup.test.tsx` to the base.\nAdds `slugToIds`.";
        let (out, r) = contradictions(text, &b, &changed);
        assert_eq!(r.len(), 2, "{r:?}");
        assert!(out.contains("Redirect helpers are untouched."));
        assert!(out.contains("Adds `slugToIds`."));
        assert!(!out.contains("still resolve"));
        assert!(!out.contains("Restored"));
        // Nothing breached and nothing put back: the text stands.
        let (same, none) = contradictions(text, &[], &["x.ts".to_string()]);
        assert!(none.is_empty());
        assert_eq!(same, text);
    }

    #[test]
    fn the_notes_fit_a_budget_scaled_to_the_change() {
        assert_eq!(budget_words(0), 150);
        assert_eq!(budget_words(100), 380);
        assert_eq!(budget_words(10_000), 600);
        let para = "word ".repeat(100);
        let text = format!("## Changes\n\n{para}\n\n<!-- coderhelm:x -->\n\nkept inside\n\n<!-- /coderhelm:x -->\n\n{para}\n\n{para}");
        let (out, cut) = fit(&text, 150);
        assert!(cut);
        assert!(out.ends_with(REST));
        assert!(out.contains("<!-- coderhelm:x -->\n\nkept inside\n\n<!-- /coderhelm:x -->"));
        assert!(words(&out) <= 150 + words(REST) + 10, "{}", words(&out));
        let (same, cut) = fit("short", 150);
        assert!(!cut);
        assert_eq!(same, "short");
        // Fitting twice changes nothing more.
        assert_eq!(fit(&out, 150).0, out);
    }

    #[test]
    fn written_outs_are_read_from_sections_and_sentences() {
        let w = written_outs("Out of scope: merging the two rows, and `/tours/<wing>` pages, which still resolve one wing.\n\n### Out of scope\n- The mobile app: another repository\n");
        assert!(w.iter().any(|e| e.item.contains("/tours/<wing>")), "{w:?}");
        assert!(w.iter().any(|e| e.item == "The mobile app"), "{w:?}");
    }
}
