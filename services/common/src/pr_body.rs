//! The one layout of a pull request description, and the checks it must
//! pass before it is posted.
//!
//! Reviewers read CoderHelm's descriptions as a wall: a lead of bold labels
//! whose numbered "Needed from you" list swallowed the lines after it (one
//! `<li>` holding the review question and the files to read), seven
//! collapsed sections with names like "More", workflows that never run on a
//! pull request listed as "did not run", the head sha in prose, and no
//! place that said which feedback was wanted. The layout here is fixed:
//!
//! 1. the Summary, a paragraph of 60 words or fewer with the ticket linked;
//! 2. `### Needed from you`, a task list of at most three items a person
//!    must do beyond reviewing, each with a stable hidden id so a tick
//!    survives a rewrite ([`ticked_ids`]); omitted when nothing is needed;
//! 3. `### Review focus`: the feedback wanted, the file to read first, where
//!    to see a UI change, and the order to read a large change in;
//! 4. one-line Risk and Rollback paragraphs;
//! 5. `### Verification`: the short sha once, then a table of at most eight
//!    rows, failed first, and a Before and After table for a UI change;
//! 6. at most three collapsed blocks with fixed names, then a one-line
//!    footer.
//!
//! Everything here is pure text. What goes in comes from the recorded facts
//! (`pr_facts::view`); [`defects`] is the spec's checklist, run on every
//! body in the tests and the quality eval.

use serde::{Deserialize, Serialize};

/// GitHub refuses a body over 65,536 characters; CoderHelm stays well under.
pub const BODY_MAX: usize = 60_000;
/// Words a reader sees without opening anything, for an ordinary change.
pub const WORD_BUDGET: usize = 200;
/// Words in sight past which the body fails.
pub const WORD_HARD: usize = 300;
pub const SUMMARY_WORDS: usize = 60;
pub const ITEM_WORDS: usize = 30;
pub const FOCUS_WORDS: usize = 25;
pub const MAX_ITEMS: usize = 3;
pub const MAX_ROWS: usize = 8;
pub const MAX_FOLDS: usize = 3;
pub const MAX_DEP_ROWS: usize = 10;
/// Changed lines (generated files left out) past which the read order is
/// given.
pub const LARGE_CHANGE: usize = 400;
/// A command longer than this, or one with `&&` or `|`, goes in a block.
pub const INLINE_COMMAND: usize = 40;

/// The three collapsed blocks, in their order.
pub const NOTES: &str = "Implementation notes";
pub const EVIDENCE: &str = "Full test evidence";
pub const BUILT: &str = "How CoderHelm built this";

/// CoderHelm's generated blocks below the visible part. Each is rebuilt on
/// every write; `pr_layout` puts each in its fold.
pub const NOTES_TOP_START: &str = "<!-- coderhelm:notes-top -->";
pub const NOTES_TOP_END: &str = "<!-- /coderhelm:notes-top -->";
pub const CHECKS_START: &str = "<!-- coderhelm:all-checks -->";
pub const CHECKS_END: &str = "<!-- /coderhelm:all-checks -->";
pub const BUILT_START: &str = "<!-- coderhelm:built -->";
pub const BUILT_END: &str = "<!-- /coderhelm:built -->";
pub const FOOTER_START: &str = "<!-- coderhelm:footer -->";
pub const FOOTER_END: &str = "<!-- /coderhelm:footer -->";
/// The first fold's name when it is not [`NOTES`] (a dependency update's
/// "All 48 updates"), kept in the visible part's hidden header.
pub const LABEL_START: &str = "<!-- coderhelm:fold-label ";
/// The end of the summary CoderHelm wrote, before the ticket link and the
/// sentence about work outside the diff.
pub const SUMMARY_END: &str = "<!-- ch:summary-end -->";

/// Every generated block, start and end.
pub const GENERATED: [(&str, &str); 4] = [
    (NOTES_TOP_START, NOTES_TOP_END),
    (CHECKS_START, CHECKS_END),
    (BUILT_START, BUILT_END),
    (FOOTER_START, FOOTER_END),
];

// ── What a person must do ───────────────────────────────────────────────

/// When an item is done, relative to the merge.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Merge {
    #[default]
    Before,
    After,
}

impl Merge {
    pub fn words(self) -> &'static str {
        match self {
            Merge::Before => "before merge",
            Merge::After => "after merge",
        }
    }
}

/// One item of "Needed from you". What it is comes from the gate that
/// decides what reaches a person (`person_gate`), as the gate's own
/// [`crate::person_gate::HumanItem`]; this module only places it against
/// the merge and renders it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Needed {
    /// Stable across rewrites (`deploy-staging`); made from the action when
    /// empty.
    pub id: String,
    pub merge: Merge,
    pub item: crate::person_gate::HumanItem,
}

impl Needed {
    /// The id the item's tick is kept under.
    pub fn key(&self) -> String {
        if self.id.trim().is_empty() {
            slug(self.item.action())
        } else {
            slug(&self.id)
        }
    }
}

// ── Review focus ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadFirst {
    pub path: String,
    /// The file at the head (empty when the repository's address is not
    /// known).
    pub url: String,
    pub why: String,
    /// What can be skimmed or skipped.
    pub skim: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Focus {
    /// What the reviewer decides: the merge decision only, as a question
    /// that names the kind of feedback.
    pub question: String,
    /// CoderHelm's own conclusion and the evidence it rests on, with the
    /// sign-offs the merge decision carries (`pr_facts::conclusion`): the
    /// reviewer is never asked to do CoderHelm's analysis.
    pub conclusion: String,
    pub read_first: Option<ReadFirst>,
    /// Where to see a UI change (a preview link and the exact place).
    pub see_it: String,
    /// "Large change: N lines; suggested order: ..." when the diff is large.
    pub large: String,
}

/// One row of a dependency update's visible table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DepRow {
    pub package: String,
    pub change: String,
    pub why: String,
}

// ── Risk ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    #[default]
    Low,
    Medium,
    High,
}

impl Level {
    pub fn word(self) -> &'static str {
        match self {
            Level::Low => "Low",
            Level::Medium => "Medium",
            Level::High => "High",
        }
    }

    pub fn of(word: &str) -> Level {
        match word.trim().to_ascii_lowercase().as_str() {
            "high" => Level::High,
            "medium" => Level::Medium,
            _ => Level::Low,
        }
    }
}

// ── Verification ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Outcome {
    Failed,
    /// Still running: shown as running, never as a result, and replaced
    /// when it ends (the stamp marks the description for a rebuild).
    Running,
    NotRun,
    Passed,
}

/// How a row still running reads.
pub const RUNNING: &str = "Running";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub check: String,
    pub outcome: Outcome,
    /// What was checked or why it failed or did not run.
    pub detail: String,
}

impl Row {
    pub fn result(&self) -> String {
        let d = one_line(&self.detail);
        let d = d.trim_end_matches('.');
        match (self.outcome, d.is_empty()) {
            (Outcome::Passed, true) => "Passed".into(),
            (Outcome::Passed, false) => format!("Passed; {d}"),
            (Outcome::Failed, true) => "Failed".into(),
            (Outcome::Failed, false) => format!("Failed: {d}"),
            (Outcome::Running, true) => format!("{RUNNING}; updated here when it ends"),
            (Outcome::Running, false) => format!("{RUNNING}; {d}"),
            (Outcome::NotRun, true) => "Not run: nothing reported it".into(),
            (Outcome::NotRun, false) => format!("Not run: {d}"),
        }
    }
}

/// The screenshots of a UI change, or why there are none.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Screens {
    Images {
        before: String,
        before_alt: String,
        after: String,
        after_alt: String,
    },
    /// One line: why there is no screenshot.
    Missing(String),
    /// Each changed page captured on the preview: its path, then the
    /// desktop and phone images (empty when that width didn't load).
    Pages(Vec<(String, String, String)>),
}

// ── The body ───────────────────────────────────────────────────────────

/// A ticket as the description links it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ticket {
    /// `KEY-123` or `#12`.
    pub key: String,
    pub url: String,
}

impl Ticket {
    pub fn link(&self) -> String {
        if self.url.starts_with("https://") && !self.key.starts_with('#') {
            format!("[{}]({})", self.key, self.url)
        } else {
            self.key.clone()
        }
    }
}

/// Everything one description shows.
#[derive(Debug, Clone, Default)]
pub struct Body {
    pub ticket: Option<Ticket>,
    /// The head's short sha.
    pub head: String,
    /// The head the results are of, when the description could not be
    /// brought up to the current head.
    pub results_of: String,
    /// One sentence, only when merging without an action breaks something.
    pub alert: String,
    /// What the pull request waits on, in one line (`waiting_line`): the
    /// first thing a reader sees.
    pub waiting: String,
    pub summary: String,
    /// Work CoderHelm did outside the diff, one sentence.
    pub outside: String,
    pub needed: Vec<Needed>,
    /// Ids a person already ticked.
    pub ticked: Vec<String>,
    pub focus: Focus,
    pub dep_rows: Vec<DepRow>,
    pub level: Level,
    pub risk: String,
    pub rollback: String,
    pub rows: Vec<Row>,
    /// `Some` for a UI change.
    pub screens: Option<Screens>,
    /// The first fold's name ([`NOTES`] when empty).
    pub notes_label: String,
    /// Generated text at the top of the first fold.
    pub notes_top: String,
    /// The full list of checks, commands and outputs.
    pub evidence: String,
    /// The plan, steps and where the run is.
    pub built: String,
    pub run_url: String,
}

/// A rendered body, split where `pr_layout` takes its parts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Rendered {
    /// Sections 1 to 5, without the lead markers.
    pub visible: String,
    pub notes_top: String,
    pub evidence: String,
    pub built: String,
    pub footer: String,
}

pub fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn upper_first(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().chain(c).collect(),
        None => String::new(),
    }
}

fn lower_first(s: &str) -> String {
    // A name keeps its case: `CoderHelm`, `GitHub`, `API`.
    let first_word = s.split_whitespace().next().unwrap_or("");
    let name = first_word.chars().skip(1).any(|c| c.is_uppercase());
    let mut c = s.chars();
    match c.next() {
        Some(f) if !name => f.to_lowercase().chain(c).collect(),
        _ => s.to_string(),
    }
}

/// `s` as a sentence part: one line, no closing stop.
fn part(s: &str) -> String {
    one_line(s)
        .trim_end_matches(['.', ';', ',', ':'])
        .trim()
        .to_string()
}

/// A stable id from words: `Deploy the staging Studio` is
/// `deploy-the-staging-studio`.
pub fn slug(text: &str) -> String {
    let mut out = String::new();
    for c in text.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('-') && !out.is_empty() {
            out.push('-');
        }
        if out.len() >= 40 {
            break;
        }
    }
    out.trim_matches('-').to_string()
}

/// Text without link targets, inline HTML comments and markup, for counting.
fn readable(text: &str) -> String {
    let mut t = text.to_string();
    // `](url)` goes; the link text stays.
    while let Some(a) = t.find("](") {
        let Some(b) = t[a..].find(')') else {
            break;
        };
        t.replace_range(a + 1..a + b + 1, "");
    }
    while let (Some(a), Some(b)) = (t.find("<!--"), t.find("-->")) {
        if b < a {
            break;
        }
        t.replace_range(a..b + 3, " ");
    }
    t
}

/// The words in `text` a reader reads: link targets, comments and
/// punctuation-only tokens left out.
pub fn words(text: &str) -> usize {
    readable(text)
        .split_whitespace()
        .filter(|w| w.chars().any(|c| c.is_alphanumeric()))
        .count()
}

/// `text` in at most `max` words: whole sentences while they fit, else the
/// first `max` words closed with a stop. Never an ellipsis.
pub fn fit_words(text: &str, max: usize) -> String {
    let t = one_line(text);
    if words(&t) <= max {
        return t;
    }
    let mut out = String::new();
    for s in t.split_inclusive(". ") {
        if words(&out) + words(s) > max {
            break;
        }
        out.push_str(s);
    }
    let out = out.trim().to_string();
    if !out.is_empty() {
        return out;
    }
    let cut: Vec<&str> = t.split_whitespace().take(max).collect();
    format!(
        "{}.",
        cut.join(" ").trim_end_matches(['.', ',', ';', ':', '('])
    )
}

/// A table cell: one line, pipes escaped, line breaks as `<br>`.
pub fn cell(s: &str) -> String {
    s.trim()
        .replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace("\r\n", "<br>")
        .replace('\n', "<br>")
}

fn is_login(owner: &str) -> bool {
    let o = owner.trim();
    o.starts_with('@')
        && o.len() > 1
        && o[1..]
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '/')
}

/// The owner as the item shows it: a real `@login`, or a role in bold with
/// no article.
pub fn owner_token(owner: &str) -> String {
    let o = one_line(owner);
    let o = o
        .trim_matches(|c: char| c == '(' || c == ')' || c == '*')
        .trim();
    if is_login(o) {
        return format!("**{o}**");
    }
    let lower = o.to_lowercase();
    let bare = ["a ", "an ", "the "]
        .iter()
        .find_map(|a| lower.starts_with(a).then(|| &o[a.len()..]))
        .unwrap_or(o)
        .trim();
    let bare = if bare.is_empty() {
        "Repository owner"
    } else {
        bare
    };
    format!("**{}**", upper_first(bare))
}

fn is_short_command(cmd: &str) -> bool {
    let c = cmd.trim();
    c.chars().count() <= INLINE_COMMAND && !c.contains("&&") && !c.contains('|')
}

/// The command a gate item's action names: the whole action, or the first
/// one in backticks.
fn command_of(action: &str) -> Option<String> {
    let whole = action.trim().trim_matches('`').trim();
    if crate::pr_steps::is_command(whole) {
        return Some(whole.to_string());
    }
    let mut ticks = action.split('`');
    ticks.next();
    while let Some(inside) = ticks.next() {
        if crate::pr_steps::is_command(inside.trim()) {
            return Some(inside.trim().to_string());
        }
        ticks.next();
    }
    None
}

/// The gate's reason in a few words, for an item with no room for more.
fn short_reason(r: crate::person_gate::Reason) -> &'static str {
    use crate::person_gate::Reason;
    match r {
        Reason::Credential => "a credential only a person holds",
        Reason::Grant => "a permission only a person grants",
        Reason::Irreversible => "hard to reverse, no preference shown",
        Reason::HeldByAnother => "someone else holds the work",
        Reason::Approval => "team policy needs a person's approval",
        Reason::OutsideReach => "outside every system CoderHelm reaches",
    }
}

/// `s` without its parenthesised asides.
fn without_parentheticals(s: &str) -> String {
    let mut out = String::new();
    let mut depth = 0usize;
    for c in s.chars() {
        match c {
            '(' => depth += 1,
            ')' if depth > 0 => depth -= 1,
            _ if depth == 0 => out.push(c),
            _ => {}
        }
    }
    one_line(&out)
}

/// One task item, and the fenced command under it when it is long. The
/// gate's item says what is needed, from whom and why CoderHelm can't; a
/// yes/no carries CoderHelm's recommendation, already built.
pub fn item_markdown(n: &Needed, ticked: bool) -> String {
    let i = &n.item;
    let id = n.key();
    let mut action = part(i.action());
    for tail in [
        " after merge",
        " before merge",
        " after merging",
        " before merging",
    ] {
        if action.to_lowercase().ends_with(tail) {
            action.truncate(action.len() - tail.len());
        }
    }
    let mut action = action.trim().to_string();
    // A long command goes in a block under the item; a short one stays.
    let cmd = command_of(&action).filter(|c| !is_short_command(c));
    if let Some(c) = &cmd {
        if action.trim_matches('`').trim() == c {
            action = "Run the command below".to_string();
        } else {
            action = action.replace(&format!("`{c}`"), "the command below");
        }
    }
    let owner = owner_token(i.who());
    let lead = format!("{owner}, {}: ", n.merge.words());
    // Thirty words at most: a parenthetical in the action goes first.
    if words(&lead) + words(&action) + 3 + words(i.why_not()).max(4) > ITEM_WORDS {
        action = without_parentheticals(&action);
    }
    let mut text = upper_first(&action);
    let rec = part(i.recommendation());
    if i.yes_no() && !rec.is_empty() {
        if !text.ends_with('?') {
            text.push('?');
        }
        let rec = lower_first(&rec);
        if rec.contains("as built") {
            text.push_str(&format!(" If nobody answers: {rec}"));
        } else {
            text.push_str(&format!(" If nobody answers: {rec}, as built"));
        }
    }
    let mut text = text.trim_end_matches('.').to_string();
    if !text.ends_with('?') {
        text.push('.');
    }
    // Thirty words at most: the reason gives way next, to the gate's
    // reason in a few words when the writer's does not fit.
    let room = ITEM_WORDS.saturating_sub(words(&lead) + words(&text) + 3);
    let why = part(i.why_not());
    let why = if why.is_empty() || words(&why) > room {
        short_reason(i.reason()).to_string()
    } else {
        why
    };
    let why = fit_words(&lower_first(&why), room.max(4));
    let why = why.trim_end_matches('.');
    let mark = if ticked { "x" } else { " " };
    let mut out = format!("- [{mark}] <!-- ch:{id} --> {lead}{text} Why a person: {why}.");
    if let Some(c) = cmd {
        out.push_str(&format!("\n\n  ```sh\n  {c}\n  ```"));
    }
    out
}

/// The ids a person ticked in an earlier body.
pub fn ticked_ids(body: &str) -> Vec<String> {
    body.lines()
        .filter_map(|l| {
            let t = l.trim_start();
            let rest = t
                .strip_prefix("- [x] ")
                .or_else(|| t.strip_prefix("- [X] "))?;
            let rest = rest.trim_start().strip_prefix("<!-- ch:")?;
            Some(rest[..rest.find(" -->")?].trim().to_string())
        })
        .collect()
}

/// The summary paragraph: CoderHelm's own sentences, the ticket linked in
/// them, and the work done outside the diff, in 60 words or fewer.
pub fn summary_paragraph(summary: &str, ticket: Option<&Ticket>, outside: &str) -> String {
    let link = ticket
        .map(|t| format!(" ({})", t.link()))
        .unwrap_or_default();
    let outside = one_line(outside);
    let budget = SUMMARY_WORDS.saturating_sub(words(&link));
    let mut core = fit_words(summary, budget);
    if core.is_empty() {
        core = "Changes the code as the ticket asks.".to_string();
    }
    let core = if core.ends_with(['.', '!', '?']) {
        core
    } else {
        format!("{core}.")
    };
    // The ticket inside the first sentence's stop.
    let with_link = match core.find(". ") {
        Some(i) if !link.is_empty() => format!("{}{link}{}", &core[..i], &core[i..]),
        _ if !link.is_empty() => format!("{}{link}.", core.trim_end_matches('.')),
        _ => core,
    };
    let mut out = format!("{with_link}{SUMMARY_END}");
    if !outside.is_empty() && words(&with_link) + words(&outside) <= SUMMARY_WORDS {
        out.push(' ');
        out.push_str(&outside);
    }
    out
}

/// The summary CoderHelm wrote, read back from a rendered paragraph: the
/// ticket link and the sentence about outside work taken off.
pub fn summary_core(paragraph: &str, ticket_key: &str) -> String {
    let p = paragraph.split(SUMMARY_END).next().unwrap_or(paragraph);
    let mut p = one_line(p);
    if !ticket_key.is_empty() {
        // ` ([KEY](url))` or ` (KEY)`, wherever it stands.
        if let Some(a) = p.find(&format!(" ([{ticket_key}](")) {
            if let Some(b) = p[a..].find("))") {
                p.replace_range(a..a + b + 2, "");
            }
        }
        p = p.replace(&format!(" ({ticket_key})"), "");
    }
    p
}

fn focus_markdown(f: &Focus) -> Vec<String> {
    let mut bullets: Vec<String> = Vec::new();
    let q = fit_words(&one_line(&f.question), FOCUS_WORDS);
    let c = part(&f.conclusion);
    // The conclusion shares the first bullet when both fit the words a
    // focus bullet may have; else it is a bullet of its own, never cut
    // while it fits one.
    let said = format!("CoderHelm's conclusion: {}.", lower_first(&c));
    let mut own: Option<String> = None;
    let q = if c.is_empty() {
        q
    } else if words(&q) + words(&said) <= FOCUS_WORDS + 12 {
        format!("{q} {said}").trim().to_string()
    } else {
        own = Some(fit_words(&said, FOCUS_WORDS + 12));
        q
    };
    if !q.is_empty() {
        bullets.push(q);
    }
    let fixed = bullets.len() + usize::from(own.is_some());
    bullets.extend(own);
    if let Some(r) = &f.read_first {
        let path = format!("`{}`", r.path.replace('`', "'"));
        let file = if r.url.starts_with("https://") {
            format!("[{path}]({})", r.url)
        } else {
            path
        };
        let why = part(&r.why);
        let mut t = format!("Read first: {file}");
        if !why.is_empty() {
            t.push_str(&format!(" ({})", lower_first(&why)));
        }
        let skim = part(&r.skim);
        if !skim.is_empty() {
            t.push_str(&format!("; {skim}"));
        }
        t.push('.');
        bullets.push(t);
    }
    if !f.see_it.trim().is_empty() {
        bullets.push(format!("See it: {}.", part(&f.see_it)));
    }
    let large = part(&f.large);
    if !large.is_empty() {
        if bullets.len() < 3 {
            bullets.push(format!("{large}."));
        } else if let Some(last) = bullets.get_mut(fixed.max(1)) {
            last.push_str(&format!(" {large}."));
        }
    }
    if bullets.is_empty() {
        bullets.push("Correctness: is this right to merge as it is?".to_string());
    }
    bullets.into_iter().take(3).collect()
}

/// The rows as the table shows them: failed first, then not run, then
/// passed, at most [`MAX_ROWS`]; the rest summed up in one row.
pub fn table_rows(rows: &[Row]) -> Vec<Row> {
    let mut sorted: Vec<Row> = rows.to_vec();
    sorted.sort_by_key(|r| r.outcome);
    if sorted.len() <= MAX_ROWS {
        return sorted;
    }
    let rest = sorted.split_off(MAX_ROWS - 1);
    let failed = rest.iter().filter(|r| r.outcome == Outcome::Failed).count();
    let not_run = rest.iter().filter(|r| r.outcome == Outcome::NotRun).count();
    let running = rest
        .iter()
        .filter(|r| r.outcome == Outcome::Running)
        .count();
    let (outcome, detail) = if failed > 0 {
        (
            Outcome::Failed,
            format!("{failed} of them; all are listed in {EVIDENCE}"),
        )
    } else if running > 0 {
        (
            Outcome::Running,
            format!("{running} of them; updated here when they end"),
        )
    } else if not_run > 0 {
        (
            Outcome::NotRun,
            format!("{not_run} of them did not report; all are listed in {EVIDENCE}"),
        )
    } else {
        (Outcome::Passed, format!("all listed in {EVIDENCE}"))
    };
    sorted.push(Row {
        check: format!("{} more checks", rest.len()),
        outcome,
        detail,
    });
    sorted
}

fn table(head: &[&str], rows: &[Vec<String>]) -> String {
    let mut s = format!(
        "| {} |\n|{}|\n",
        head.join(" | "),
        vec!["---"; head.len()].join("|")
    );
    for r in rows {
        s.push_str(&format!(
            "| {} |\n",
            r.iter().map(|c| cell(c)).collect::<Vec<_>>().join(" | ")
        ));
    }
    s.trim_end().to_string()
}

fn screens_markdown(s: &Screens) -> String {
    match s {
        Screens::Images {
            before,
            before_alt,
            after,
            after_alt,
        } => {
            let img = |src: &str, alt: &str| {
                format!(
                    "<img src=\"{}\" width=\"380\" alt=\"{}\">",
                    src.replace('"', "%22"),
                    one_line(alt).replace('"', "'")
                )
            };
            format!(
                "| Before | After |\n|---|---|\n| {} | {} |",
                img(before, before_alt),
                img(after, after_alt)
            )
        }
        Screens::Missing(why) => format!("No screenshot: {}.", part(why)),
        Screens::Pages(pages) => {
            let img = |src: &str, alt: String| {
                if src.starts_with("https://") {
                    format!(
                        "<img src=\"{}\" width=\"{}\" alt=\"{}\">",
                        src.replace('"', "%22"),
                        if alt.contains("phone") { 180 } else { 380 },
                        alt.replace('"', "'")
                    )
                } else {
                    "didn't load".to_string()
                }
            };
            let mut out = String::from("| Page | Desktop | Phone |\n|---|---|---|\n");
            for (page, desktop, phone) in pages {
                let p = one_line(page).replace('|', "/").replace('`', "'");
                out.push_str(&format!(
                    "| `{p}` | {} | {} |\n",
                    img(desktop, format!("{p} at desktop width")),
                    img(phone, format!("{p} at phone width"))
                ));
            }
            out.trim_end().to_string()
        }
    }
}

/// The visible part's blocks, each rendered on its own (empty when the
/// body has none): what [`render`] lays out in its fixed order and
/// [`render_templated`] lays into a repository's template.
#[derive(Debug, Clone, Default)]
struct Blocks {
    header: String,
    alert: String,
    summary: String,
    /// With its `### Needed from you` heading.
    needed: String,
    /// With its `### Review focus` heading.
    focus: String,
    deps: String,
    risk: String,
    rollback: String,
    /// With its `### Verification` heading.
    verification: String,
    screens: String,
}

impl Blocks {
    /// The blocks in the fixed order.
    fn in_order(&self) -> Vec<&str> {
        [
            &self.alert,
            &self.summary,
            &self.needed,
            &self.focus,
            &self.deps,
            &self.risk,
            &self.rollback,
            &self.verification,
            &self.screens,
        ]
        .into_iter()
        .map(String::as_str)
        .filter(|b| !b.is_empty())
        .collect()
    }
}

/// A block without its `### ` heading line.
fn under_heading(block: &str) -> String {
    match block.split_once('\n') {
        Some((first, rest)) if first.starts_with("### ") => rest.trim().to_string(),
        _ => block.trim().to_string(),
    }
}

fn blocks(b: &Body) -> Blocks {
    let mut out = Blocks::default();
    let mut v: Vec<String> = Vec::new();
    let ticket = b.ticket.as_ref().filter(|t| !t.key.trim().is_empty());
    v.push(format!(
        "<!-- coderhelm:body v1 ticket={} head={} -->",
        ticket.map(|t| t.key.as_str()).unwrap_or("none"),
        if b.head.is_empty() { "none" } else { &b.head }
    ));
    let label = one_line(&b.notes_label);
    if !label.is_empty() && label != NOTES {
        v.push(format!("{LABEL_START}{label} -->"));
    }
    out.header = v.join("\n");
    // An alert when merging without an action breaks something, or when
    // the risk is High and nobody is asked for anything.
    let alert = if !b.alert.trim().is_empty() {
        part(&b.alert)
    } else if b.level == Level::High && b.needed.is_empty() {
        format!(
            "High risk: {}. Read it with care before merging",
            lower_first(&part(&b.risk))
        )
    } else {
        String::new()
    };
    let waiting = one_line(&b.waiting);
    out.alert = match (waiting.is_empty(), alert.is_empty()) {
        (true, true) => String::new(),
        (true, false) => format!("> [!IMPORTANT]\n> {alert}."),
        (false, true) => format!("> [!NOTE]\n> {waiting}"),
        (false, false) => format!("> [!IMPORTANT]\n> {waiting} {alert}."),
    };
    out.summary = summary_paragraph(&b.summary, ticket, &b.outside);
    if !b.needed.is_empty() {
        let mut s = String::from("### Needed from you\n\n");
        let items: Vec<String> = b
            .needed
            .iter()
            .take(MAX_ITEMS)
            .map(|i| item_markdown(i, b.ticked.contains(&i.key())))
            .collect();
        let loose = items.iter().any(|i| i.contains('\n'));
        s.push_str(&items.join(if loose { "\n\n" } else { "\n" }));
        out.needed = s;
    }
    let focus = focus_markdown(&b.focus);
    out.focus = format!(
        "### Review focus\n\n{}",
        focus
            .iter()
            .map(|f| format!("- {f}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    if !b.dep_rows.is_empty() {
        out.deps = table(
            &["Package", "Change", "Why it matters"],
            &b.dep_rows
                .iter()
                .take(MAX_DEP_ROWS)
                .map(|r| {
                    vec![
                        format!("`{}`", r.package.replace('`', "'")),
                        one_line(&r.change),
                        part(&r.why),
                    ]
                })
                .collect::<Vec<_>>(),
        );
    }
    let risk = part(&b.risk);
    let risk = if risk.chars().count() < 10 {
        "nothing recorded narrows what it touches beyond the files in the diff".to_string()
    } else {
        risk
    };
    out.risk = format!("**Risk:** {}. {}.", b.level.word(), upper_first(&risk));
    let rollback = part(&b.rollback);
    let rollback = if rollback.is_empty() {
        "Revert this pull request; nothing outside the code changes with it".to_string()
    } else {
        upper_first(&rollback)
    };
    out.rollback = format!("**Rollback:** {rollback}.");
    let lead_in = if !b.results_of.is_empty() && b.results_of != b.head {
        format!(
            "Results below are for {}; the head is now {}.",
            b.results_of, b.head
        )
    } else if b.head.is_empty() {
        "On the head of this PR:".to_string()
    } else {
        format!("On {} (head of this PR):", b.head)
    };
    let rows = table_rows(&b.rows);
    let mut verification = format!("### Verification\n\n{lead_in}\n\n");
    verification.push_str(&table(
        &["Check", "Result"],
        &rows
            .iter()
            .map(|r| vec![one_line(&r.check), r.result()])
            .collect::<Vec<_>>(),
    ));
    out.verification = verification;
    if let Some(s) = &b.screens {
        out.screens = screens_markdown(s);
    }
    out
}

/// The body's parts, rendered.
pub fn render(b: &Body) -> Rendered {
    let bl = blocks(b);
    let mut visible = bl.header.clone();
    visible.push('\n');
    visible.push_str(&bl.in_order().join("\n\n"));
    finish(b, visible)
}

/// The body laid into a repository's template (`pr_template`): the
/// template's headings, in its order, each filled with what it asks for;
/// what a person must do and the review focus after the first section.
/// `written` is the text the writer wrote (its sections are read by
/// heading). Returns the rendering and the written sections' titles it put
/// in sight, so they are not said again below. With a template of no
/// headings it is [`render`].
pub fn render_templated(b: &Body, template: &str, written: &str) -> (Rendered, Vec<String>) {
    use crate::pr_template::{self as tpl, Slot};
    let sections = tpl::sections(template);
    if sections.is_empty() {
        return (render(b), Vec::new());
    }
    let bl = blocks(b);
    let own = tpl::written_sections(written);
    let mut used: Vec<String> = Vec::new();
    let mut by_title = |title: &str, also: &[&str]| -> Option<String> {
        let found = own
            .iter()
            .find(|(t, x)| t.eq_ignore_ascii_case(title) && !x.trim().is_empty())
            .or_else(|| {
                own.iter().find(|(t, x)| {
                    let l = t.to_lowercase();
                    !x.trim().is_empty() && also.iter().any(|a| l.contains(a))
                })
            })?;
        used.push(found.0.clone());
        Some(found.1.clone())
    };
    let ticket = b.ticket.as_ref().filter(|t| !t.key.trim().is_empty());
    let slots: Vec<Slot> = sections.iter().map(tpl::slot).collect();
    let has = |s: Slot| slots.contains(&s);
    let mut out: Vec<String> = Vec::new();
    if !bl.alert.is_empty() {
        out.push(bl.alert.clone());
    }
    // The summary opens the body; a template without a section for it gets
    // it above its first heading.
    if !has(Slot::Summary) {
        out.push(bl.summary.clone());
    }
    let review_block = |out: &mut Vec<String>| {
        if !bl.needed.is_empty() {
            out.push(bl.needed.clone());
        }
        if !has(Slot::Review) {
            out.push(bl.focus.clone());
        }
        if !bl.deps.is_empty() {
            out.push(bl.deps.clone());
        }
    };
    if !has(Slot::Summary) {
        review_block(&mut out);
    }
    let mut placed_review = !has(Slot::Summary);
    for (s, slot) in sections.iter().zip(slots.iter().copied()) {
        let fill = match slot {
            Slot::Summary => {
                // The writer's own words for it are what the summary was
                // read from: not said again below.
                let _ = by_title(&s.title, &[]);
                bl.summary.clone()
            }
            Slot::Testing => under_heading(&bl.verification),
            Slot::Risk => format!("{}\n\n{}", bl.risk, bl.rollback),
            Slot::Screens => {
                if bl.screens.is_empty() {
                    "No screenshot: nothing in this change shows on a page.".to_string()
                } else {
                    bl.screens.clone()
                }
            }
            Slot::Ticket => match ticket {
                Some(t) => format!("Ticket: {}", t.link()),
                None => by_title(&s.title, &[])
                    .unwrap_or_else(|| "No ticket names this change.".to_string()),
            },
            Slot::Review => under_heading(&bl.focus),
            Slot::Checklist => {
                by_title(&s.title, &["checklist"]).unwrap_or_else(|| tpl::boxes(s).join("\n"))
            }
            Slot::Changes => by_title(&s.title, &["change"])
                .unwrap_or_else(|| "Listed under Implementation notes below.".to_string()),
            Slot::Other => by_title(&s.title, &[]).unwrap_or_else(|| {
                let own: Vec<String> = s
                    .lines
                    .iter()
                    .filter(|l| !l.trim().is_empty())
                    .cloned()
                    .collect();
                if own.is_empty() {
                    "Nothing for this change.".to_string()
                } else {
                    own.join("\n")
                }
            }),
        };
        out.push(format!("{}\n\n{}", s.heading(), fill.trim()));
        if !placed_review {
            review_block(&mut out);
            placed_review = true;
        }
    }
    if !has(Slot::Risk) {
        out.push(bl.risk.clone());
        out.push(bl.rollback.clone());
    }
    if !has(Slot::Testing) {
        out.push(bl.verification.clone());
    }
    if !has(Slot::Screens) && !bl.screens.is_empty() {
        out.push(bl.screens.clone());
    }
    let mut visible = format!("{}\n{}\n", bl.header, tpl::MARK);
    visible.push_str(
        &out.into_iter()
            .filter(|x| !x.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n\n"),
    );
    used.sort();
    used.dedup();
    (finish(b, visible), used)
}

/// The generated parts below the visible one.
fn finish(b: &Body, visible: String) -> Rendered {
    let ticket = b.ticket.as_ref().filter(|t| !t.key.trim().is_empty());
    let footer = {
        let from = ticket
            .map(|t| format!(" from {}", t.link()))
            .unwrap_or_default();
        let run = if b.run_url.starts_with("https://") {
            format!(" · [run log]({})", b.run_url)
        } else {
            String::new()
        };
        format!("Opened by CoderHelm{from}{run}")
    };
    let built = {
        let mut s = String::new();
        if b.run_url.starts_with("https://") {
            s.push_str(&format!(
                "Plan, steps and logs: [the run page]({}).",
                b.run_url
            ));
        }
        let extra = b.built.trim();
        if !extra.is_empty() {
            if !s.is_empty() {
                s.push_str("\n\n");
            }
            s.push_str(extra);
        }
        s
    };
    Rendered {
        visible,
        notes_top: b.notes_top.trim().to_string(),
        evidence: b.evidence.trim().to_string(),
        built,
        footer,
    }
}

/// `inner` between `start` and `end` lines, or nothing when it is empty.
pub fn block(start: &str, inner: &str, end: &str) -> String {
    if inner.trim().is_empty() {
        String::new()
    } else {
        format!("{start}\n{}\n{end}", inner.trim())
    }
}

// ── The checklist ──────────────────────────────────────────────────────

/// What a description must never show in sight.
const NEVER_IN_SIGHT: &[&str] = &[
    "did not run",
    "(a person",
    "(the reviewer",
    "recorded checks passed",
    "recorded check passed",
    "To let it:",
    "workflow_dispatch",
    "Nothing needed from you",
    "Open items",
    "the full list is on the run page",
    "Source ticket:",
    "**Check:**",
    "**Look at first:**",
    "**Review:**",
];

const ARROWS: &[char] = &[
    '\u{2192}', '\u{21d2}', '\u{279c}', '\u{2190}', '\u{2194}', '\u{27f6}', '\u{21e8}',
];

/// The part a reader sees without opening anything: outside every
/// `<details>`, without comments.
pub fn in_sight(body: &str) -> String {
    let mut depth = 0usize;
    let mut out: Vec<String> = Vec::new();
    for l in body.lines() {
        let t = l.trim();
        if t.starts_with("<details") {
            depth += 1;
            continue;
        }
        if t.starts_with("</details") {
            depth = depth.saturating_sub(1);
            continue;
        }
        if depth > 0 || (t.starts_with("<!--") && t.ends_with("-->") && !t.contains("--> ")) {
            continue;
        }
        out.push(l.to_string());
    }
    out.join("\n")
}

/// Words in sight, outside tables, code, alerts' markers and the footer.
pub fn visible_words(body: &str) -> usize {
    let seen = in_sight(body);
    let mut n = 0;
    let mut code = false;
    for l in seen.lines() {
        let t = l.trim();
        if t.starts_with("```") {
            code = !code;
            continue;
        }
        if code || t.starts_with('|') || t.starts_with("Opened by CoderHelm") {
            continue;
        }
        n += words(t.trim_start_matches("> [!IMPORTANT]"));
    }
    n
}

fn is_hex(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// A full 40-character sha in `text`.
pub fn full_sha(text: &str) -> Option<String> {
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .find(|w| w.len() == 40 && is_hex(w) && w.chars().any(|c| c.is_ascii_digit()))
        .map(str::to_string)
}

/// The spec's checklist, on the markdown source: one line per defect, empty
/// when the body passes. `head` is the head's short sha.
pub fn defects(body: &str, head: &str) -> Vec<String> {
    let mut d: Vec<String> = Vec::new();
    let seen = in_sight(body);
    let lines: Vec<&str> = seen.lines().filter(|l| !l.trim().is_empty()).collect();
    // The first block is the Summary, or one alert and then the Summary.
    match lines.first().map(|l| l.trim()) {
        Some(l) if l.starts_with("> [!") => {
            let after = lines.iter().find(|l| !l.trim().starts_with('>'));
            if after.is_none_or(|l| is_block_start(l.trim())) {
                d.push("an alert is not followed by the Summary".into());
            }
        }
        Some(l) if is_block_start(l) => {
            d.push(format!("the body does not open with the Summary: {l}"))
        }
        None => d.push("nothing is in sight".into()),
        _ => {}
    }
    if seen.matches("> [!").count() > 1 {
        d.push("more than one alert".into());
    }
    // Headings: only these three, in this order; a body laid into the
    // repository's template has the template's headings.
    let templated = body.contains(crate::pr_template::MARK);
    let allowed = ["Needed from you", "Review focus", "Verification"];
    let mut last = 0usize;
    for l in lines.iter().filter(|_| !templated) {
        let t = l.trim();
        if t.starts_with('#') {
            let h = t.trim_start_matches('#').trim();
            match allowed.iter().position(|a| *a == h) {
                Some(i) if i >= last && t.starts_with("### ") => last = i,
                _ => d.push(format!("a heading out of place: {t}")),
            }
        }
    }
    for h in ["Review focus", "Verification"]
        .iter()
        .filter(|_| !templated)
    {
        if !seen.contains(&format!("### {h}\n")) {
            d.push(format!("no {h} section"));
        }
    }
    // The Summary.
    if let Some(p) = lines
        .iter()
        .find(|l| !is_block_start(l.trim()) && !l.trim().starts_with('>'))
    {
        let n = words(p);
        if n > SUMMARY_WORDS {
            d.push(format!("the Summary has {n} words"));
        }
    }
    // Needed from you.
    let section = |name: &str| -> Vec<&str> {
        let mut inside = false;
        let mut out = Vec::new();
        for l in seen.lines() {
            let t = l.trim();
            if t.starts_with('#') || t.starts_with("**Risk:**") {
                inside = t == format!("### {name}");
                continue;
            }
            if inside {
                out.push(l);
            }
        }
        out
    };
    let needed = section("Needed from you");
    let items: Vec<&&str> = needed.iter().filter(|l| l.starts_with("- [")).collect();
    if seen.contains("### Needed from you") && !(1..=MAX_ITEMS).contains(&items.len()) {
        d.push(format!("Needed from you has {} items", items.len()));
    }
    for l in needed.iter().filter(|l| {
        let t = l.trim();
        !t.is_empty() && !t.starts_with("- [") && !l.starts_with("  ")
    }) {
        d.push(format!("Needed from you holds more than task items: {l}"));
    }
    for i in &items {
        let has_owner = i.contains("--> **");
        let when = i.contains(", before merge: ") || i.contains(", after merge: ");
        if !has_owner || !when || !i.contains("Why a person: ") {
            d.push(format!("an item lacks owner, merge timing or why: {i}"));
        }
        if !i.contains("<!-- ch:") {
            d.push(format!("an item has no hidden id: {i}"));
        }
        let n = words(i);
        if n > ITEM_WORDS {
            d.push(format!("an item has {n} words: {i}"));
        }
        let mut ticks = i.split('`');
        ticks.next();
        while let Some(code) = ticks.next() {
            if !is_short_command(code) {
                d.push(format!("a long command inline: {code}"));
            }
            ticks.next();
        }
    }
    // Review focus.
    let focus: Vec<&str> = section("Review focus")
        .into_iter()
        .filter(|l| l.starts_with("- "))
        .collect();
    if !(1..=3).contains(&focus.len()) {
        d.push(format!("Review focus has {} bullets", focus.len()));
    }
    if let Some(first) = focus.first() {
        let kinds = [
            "approach",
            "naming",
            "copy",
            "security",
            "performance",
            "correctness",
        ];
        let lower = first.to_lowercase();
        if !first.trim_end().ends_with('?') && !kinds.iter().any(|k| lower.contains(k)) {
            d.push(format!("the first focus bullet names no feedback: {first}"));
        }
    }
    for f in &focus {
        let n = words(f);
        if n > FOCUS_WORDS + 12 {
            d.push(format!("a focus bullet has {n} words"));
        }
    }
    // Risk and Rollback.
    let risk = lines.iter().find(|l| l.starts_with("**Risk:** "));
    match risk {
        Some(r) => {
            let rest = &r["**Risk:** ".len()..];
            let ok = ["Low. ", "Medium. ", "High. "]
                .iter()
                .any(|p| rest.starts_with(p) && rest.len() >= p.len() + 10);
            if !ok {
                d.push(format!("the risk has no level and reason: {r}"));
            }
        }
        None => d.push("no Risk line".into()),
    }
    if !lines
        .iter()
        .any(|l| l.starts_with("**Rollback:** ") && l.len() > "**Rollback:** ".len() + 3)
    {
        d.push("no Rollback line".into());
    }
    if risk.is_some_and(|r| r.starts_with("**Risk:** High"))
        && !seen.contains("### Needed from you")
        && !seen.contains("> [!")
    {
        d.push("High risk with no action and no alert".into());
    }
    // Verification.
    let ver = section("Verification");
    let lead_in = ver.iter().find(|l| !l.trim().is_empty());
    match lead_in {
        Some(l) if l.starts_with("On ") || l.starts_with("Results below are for ") => {
            if !head.is_empty() && !l.contains(head) {
                d.push(format!("the lead-in does not name the head {head}: {l}"));
            }
        }
        _ => d.push("Verification has no lead-in with the head".into()),
    }
    if !head.is_empty() {
        let n = readable(&seen).matches(head).count();
        if n != 1 {
            d.push(format!("the short sha is in sight {n} times"));
        }
    }
    let rows: Vec<&str> = ver
        .iter()
        .copied()
        .filter(|l| l.starts_with('|') && !l.starts_with("|---") && !l.starts_with("| Check |"))
        .take_while(|l| !l.starts_with("| Before |"))
        .collect();
    if rows.len() > MAX_ROWS {
        d.push(format!("Verification has {} rows", rows.len()));
    }
    let mut seen_pass = false;
    for r in &rows {
        let result = r
            .trim()
            .trim_end_matches('|')
            .rsplit(" | ")
            .next()
            .unwrap_or("")
            .trim();
        if result.starts_with("Passed") {
            seen_pass = true;
        } else if result.starts_with("Failed") {
            if seen_pass {
                d.push(format!("a failed row after a passed one: {r}"));
            }
        } else if !result.starts_with("Not run: ") && !result.starts_with(RUNNING) {
            d.push(format!(
                "a result that is not Passed, Failed, Running or Not run: {r}"
            ));
        }
    }
    if let Some(sha) = full_sha(body) {
        d.push(format!("a full sha in the body: {sha}"));
    }
    // UI images.
    for l in seen.lines().filter(|l| l.contains("<img")) {
        if !l.contains("alt=\"") || !l.contains("width=\"") {
            d.push(format!("an image without alt or width: {l}"));
        }
        if l.contains("X-Amz-Expires") || l.contains("Signature=") {
            d.push(format!("an expiring image link: {l}"));
        }
    }
    // Collapsed blocks.
    let folds = body.matches("<details>").count();
    if folds > MAX_FOLDS {
        d.push(format!("{folds} collapsed blocks"));
    }
    let all: Vec<&str> = body.lines().collect();
    for (i, l) in all.iter().enumerate() {
        let t = l.trim();
        if t.starts_with("<details>") {
            let s = all.get(i + 1).map(|x| x.trim()).unwrap_or("");
            let label = s
                .strip_prefix("<summary>")
                .and_then(|x| x.split("</summary>").next())
                .unwrap_or("");
            if label.is_empty() {
                d.push("a collapsed block with no plain summary line".into());
            }
            if label.contains("**") || label.contains('`') || label.contains('<') {
                d.push(format!("markup in a summary: {label}"));
            }
            if words(label) > 4 {
                d.push(format!("a summary label over 4 words: {label}"));
            }
            if all.get(i + 2).is_some_and(|x| !x.trim().is_empty()) {
                d.push(format!("no blank line after the summary: {label}"));
            }
        }
    }
    // Footer.
    if !lines.iter().any(|l| l.starts_with("Opened by CoderHelm")) {
        d.push("no footer".into());
    }
    // What never shows.
    for n in NEVER_IN_SIGHT {
        if seen.contains(n) {
            d.push(format!("in sight: {n}"));
        }
    }
    if seen.contains('\u{2026}') || body.contains('\u{2026}') {
        d.push("an ellipsis".into());
    }
    if body.chars().any(|c| ARROWS.contains(&c)) {
        d.push("an arrow glyph".into());
    }
    for w in readable(&seen).split(|c: char| c.is_whitespace() || "()[]`,;".contains(c)) {
        if is_uuid(w) {
            d.push(format!("a raw id in sight: {w}"));
        }
    }
    let n = visible_words(body);
    if n > WORD_HARD {
        d.push(format!("{n} words in sight"));
    }
    if body.chars().count() >= BODY_MAX {
        d.push(format!("{} characters", body.chars().count()));
    }
    d
}

fn is_block_start(t: &str) -> bool {
    t.starts_with('#')
        || t.starts_with("- ")
        || t.starts_with('|')
        || t.starts_with("**")
        || t.starts_with('<')
        || t.starts_with("```")
        || t.chars().next().is_some_and(|c| c.is_ascii_digit()) && t.contains(". ")
}

fn is_uuid(w: &str) -> bool {
    let parts: Vec<&str> = w.split('-').collect();
    parts.len() == 5
        && [8, 4, 4, 4, 12]
            .iter()
            .zip(&parts)
            .all(|(n, p)| p.len() == *n && is_hex(p))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What the gate makes of a step a person was to do.
    fn gated(text: &str, how: &str, owner: &str, why_not: &str) -> Needed {
        use crate::person_gate::{decide, Ask, Kind, Outcome as G};
        let a = Ask {
            kind: Kind::Step,
            text: text.into(),
            how: how.into(),
            owner: owner.into(),
            why_not: why_not.into(),
            ..Default::default()
        };
        match decide(&a, &Default::default()) {
            G::Human(item) => Needed {
                id: "deploy-staging".into(),
                merge: Merge::After,
                item,
            },
            o => panic!("not a person's: {o:?}"),
        }
    }

    fn deploy() -> Needed {
        gated(
            "Deploy Studio to staging so editors see the warning",
            "cd apps/studio && npm run deploy:staging",
            "@studio-owner",
            "the repo has no deploy workflow CoderHelm can start",
        )
    }

    #[test]
    fn an_item_is_one_line_with_its_owner_timing_and_reason() {
        let out = item_markdown(&deploy(), false);
        assert_eq!(
            out,
            "- [ ] <!-- ch:deploy-staging --> **@studio-owner**, after merge: Give CoderHelm the deploy credentials for this environment or run the command below once. Why a person: the repo has no deploy workflow CoderHelm can start.\n\n  ```sh\n  cd apps/studio && npm run deploy:staging\n  ```"
        );
        // A role has no article and no parenthetical; a short command stays
        // in the line.
        let role = gated(
            "Deploy Studio to staging so editors see the warning",
            "npm run seed",
            "a repository admin",
            "the repo has no deploy workflow CoderHelm can start",
        );
        let out = item_markdown(&role, true);
        assert!(out.starts_with("- [x] <!-- ch:deploy-staging --> **Repository admin**, after merge: Give CoderHelm the deploy credentials"), "{out}");
        assert!(out.contains("`npm run seed`"), "{out}");
        assert!(!out.contains("```"), "{out}");
    }

    #[test]
    fn a_yes_no_carries_the_recommendation_already_built() {
        use crate::person_gate::{decide, Ask, Kind, Outcome as G};
        let a = Ask {
            kind: Kind::Question,
            text: "Should the migration email customers whose bookings move?".into(),
            options: vec!["email them once".into(), "send nothing".into()],
            ..Default::default()
        };
        let G::Human(item) = decide(&a, &Default::default()) else {
            panic!("not a person's");
        };
        let out = item_markdown(
            &Needed {
                id: String::new(),
                merge: Merge::Before,
                item,
            },
            false,
        );
        assert!(out.contains("before merge: Yes or no: Should the migration email customers whose bookings move? If nobody answers: email them once, as built."), "{out}");
        assert!(words(&out) <= ITEM_WORDS, "{} {out}", words(&out));
    }

    #[test]
    fn a_long_reason_gives_way_to_the_thirty_word_limit() {
        let i = gated(
            "Deploy Studio to staging so editors see the warning",
            "cd apps/studio && npm run deploy:staging",
            "@studio-owner",
            "the repository has no deploy workflow that CoderHelm can start, and its app is not allowed to dispatch one, and nobody has granted it, and the team prefers a person to run deploys",
        );
        let out = item_markdown(&i, false);
        let first = out.lines().next().unwrap();
        assert!(words(first) <= ITEM_WORDS, "{} {first}", words(first));
        assert!(!first.contains('\u{2026}'));
    }

    #[test]
    fn ticks_carry_over_by_id() {
        let prior = "### Needed from you\n\n- [x] <!-- ch:deploy-staging --> **@a**, after merge: Deploy.\n- [ ] <!-- ch:other --> **@b**, before merge: Other.";
        assert_eq!(ticked_ids(prior), vec!["deploy-staging".to_string()]);
        let b = Body {
            needed: vec![deploy()],
            ticked: ticked_ids(prior),
            ..Default::default()
        };
        assert!(render(&b)
            .visible
            .contains("- [x] <!-- ch:deploy-staging -->"));
    }

    #[test]
    fn the_summary_links_the_ticket_and_stays_within_sixty_words() {
        let t = Ticket {
            key: "KEY-123".into(),
            url: "https://tracker.example.com/browse/KEY-123".into(),
        };
        let p = summary_paragraph(
            "Adds a Studio warning when a subtitle is too long. Editors can still publish.",
            Some(&t),
            "Posted the 23 over-limit documents on KEY-123.",
        );
        assert_eq!(p, "Adds a Studio warning when a subtitle is too long ([KEY-123](https://tracker.example.com/browse/KEY-123)). Editors can still publish.<!-- ch:summary-end --> Posted the 23 over-limit documents on KEY-123.");
        assert_eq!(
            summary_core(&p, "KEY-123"),
            "Adds a Studio warning when a subtitle is too long. Editors can still publish."
        );
        let long = "word ".repeat(90);
        let p = summary_paragraph(&long, Some(&t), "");
        assert!(words(&p) <= SUMMARY_WORDS, "{}", words(&p));
        assert!(!p.contains('\u{2026}'));
    }

    #[test]
    fn rows_put_failures_first_and_never_pass_eight() {
        let rows: Vec<Row> = (0..12)
            .map(|i| Row {
                check: format!("job {i} (CI)"),
                outcome: if i == 11 {
                    Outcome::Failed
                } else {
                    Outcome::Passed
                },
                detail: String::new(),
            })
            .collect();
        let out = table_rows(&rows);
        assert_eq!(out.len(), MAX_ROWS);
        assert_eq!(out[0].check, "job 11 (CI)");
        assert_eq!(out[7].check, "5 more checks");
        assert_eq!(out[7].result(), format!("Passed; all listed in {EVIDENCE}"));
    }

    #[test]
    fn cells_escape_pipes() {
        assert_eq!(cell("a | b\nc"), "a \\| b<br>c");
        assert_eq!(cell("`x|y`"), "`x\\|y`");
    }

    #[test]
    fn the_checklist_catches_the_old_shapes() {
        let bad = "**Check:** `cd apps/studio && npm run deploy:staging`; 6 recorded checks passed on d78bb24.\n**Risk:** Low.\n**Needed from you:**\n1. Deploy it (a person on the team)\n   **Review:** decide whether this is right.\n   **Look at first:**\n- `Deploy to Production` did not run on `d78bb24`.\n\n<details><summary>More</summary>\n*em*\n</details>\n";
        let found = defects(bad, "d78bb24");
        for want in [
            "does not open with the Summary",
            "no Review focus section",
            "no Rollback line",
            "in sight: did not run",
            "in sight: (a person",
            "in sight: **Check:**",
            "no blank line after the summary",
            "no footer",
        ] {
            assert!(found.iter().any(|f| f.contains(want)), "{want}: {found:#?}");
        }
        assert!(defects("a \u{2192} b", "")
            .iter()
            .any(|f| f.contains("arrow")));
        assert!(defects("x\u{2026}", "")
            .iter()
            .any(|f| f.contains("ellipsis")));
        let sha = "0123456789abcdef0123456789abcdef01234567";
        assert!(defects(sha, "").iter().any(|f| f.contains("full sha")));
    }

    #[test]
    fn a_rendered_body_passes_its_own_checklist() {
        let b = Body {
            ticket: Some(Ticket {
                key: "KEY-123".into(),
                url: "https://tracker.example.com/browse/KEY-123".into(),
            }),
            head: "d78bb24".into(),
            summary: "Adds a warning.".into(),
            needed: vec![deploy()],
            focus: Focus {
                question: "Approach: is a warning what KEY-123 asks for?".into(),
                read_first: Some(ReadFirst {
                    path: "src/a.ts".into(),
                    url: "https://github.com/acme/site/blob/d78bb24/src/a.ts".into(),
                    why: "the rule".into(),
                    skim: "the other file only registers it".into(),
                }),
                ..Default::default()
            },
            level: Level::Low,
            risk: "warning-level validation only".into(),
            rollback: "revert this PR".into(),
            rows: vec![Row {
                check: "Unit tests (CI)".into(),
                outcome: Outcome::Passed,
                detail: "3 new cases".into(),
            }],
            run_url: "https://app.example.com/runs/1".into(),
            ..Default::default()
        };
        let r = render(&b);
        let body = format!(
            "{}\n\n<details>\n<summary>{BUILT}</summary>\n\n{}\n\n</details>\n\n{}",
            r.visible, r.built, r.footer
        );
        assert_eq!(defects(&body, "d78bb24"), Vec::<String>::new(), "{body}");
    }
}
