//! Whether a review finding holds the merge, and how a note is labeled.
//!
//! Changes are requested for a named correctness, security or data-loss
//! problem with its location and evidence, and for what blocks by rule
//! whatever the model called it ([`super::review_rules::mandatory`]: a rule
//! the repository requires, broken; a check failing on this commit; a
//! critical path with no test or proof). Every other finding is posted on an
//! approval as a labeled note ([`Label`]), in three tiers:
//!
//! - **Should fix**: a written team rule not marked required, or a problem
//!   outside the three blocking kinds (a missing test where tests aren't
//!   required, whatever `optional` the model gave it);
//! - **Suggestion**: an optional improvement, hardening or a refactor;
//! - **Nit** and **FYI** (suggestion tier): a style, naming or layout
//!   preference; a question, a concern about an edge case resting on state
//!   nobody could check, an unconfirmed fact, or a problem without a location
//!   or evidence.
//!
//! A possible break of the change's main path that nobody settled is none
//! of these: it blocks as "needs a check" naming the check
//! ([`super::review_rules::main_path_unchecked`]); [`ground`] reads one from
//! a concern that says it hits every request and couldn't confirm it.
//!
//! [`classify`] decides from the finding's structured fields (basis,
//! category, location, stated-goal miss); the model's own severity is only
//! an input. [`ground`] fills those fields from what the finding shows (a
//! code path it traces, a documented exit or status code the code gets
//! backwards, a team rule it quotes), and [`second_pass`] has an independent
//! judge check every note against the same rubric. A finding that blocks is
//! still put to the stop judge before "changes requested" is posted
//! (`passes::stop_judge`).
//!
//! The misses behind the evidence rules: a provisioning script that threw on
//! an installer's documented "success, reboot required" code, a write that
//! hid rows from two incremental cursors (traced line by line), and a
//! sort-key tie a data query confirmed: each a correctness or data-loss
//! problem shown by evidence, so each blocks.

use super::review_agent::{Basis, Evidence, Finding};
use crate::agent::provider::{self, ModelProvider};
use crate::models::TokenUsage;
use crate::WorkerState;
use serde::Deserialize;
use std::collections::HashMap;

/// Whether a finding holds the merge, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Class {
    Blocks(&'static str),
    Note(&'static str),
}

impl Class {
    pub fn blocks(&self) -> bool {
        matches!(self, Class::Blocks(_))
    }

    pub fn reason(&self) -> &'static str {
        match self {
            Class::Blocks(r) | Class::Note(r) => r,
        }
    }
}

/// The independent severity check's answer for one finding.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeverityCheck {
    /// The judge said it holds the merge.
    pub blocking: bool,
    /// The judge's one-line reason.
    pub reason: String,
}

/// Categories that are a preference the reviewer infers: wording, naming,
/// layout, log levels, conventions nobody wrote down, checks by hand.
const PREFERENCE_CATEGORIES: &[&str] = &[
    "style",
    "nit",
    "naming",
    "formatting",
    "logging",
    "log-level",
    "docs",
    "documentation",
    "convention",
    "manual-check",
];

/// Categories that are an optional improvement unless the finding says the
/// code as written is wrong (`optional`: false).
const SUGGESTION_CATEGORIES: &[&str] = &[
    "hardening",
    "maintainability",
    "follow-up",
    "followup",
    "refactor",
    "suggestion",
    "scope",
    "observability",
];

/// Phrases a finding uses to say the current code is correct and the point
/// is optional. (Hedges about likelihood or impact are not among them: they
/// don't make a defect optional.)
const OPTIONAL_PHRASES: &[&str] = &[
    "not a correctness issue",
    "not a bug",
    "still catches",
    "already catches",
    "if you'd like",
    "if you would like",
    "if that's intended",
    "if that is intended",
    "nice to have",
    "nice-to-have",
    "no harm",
    "can't break anything",
    "cannot break anything",
    "works as is",
    "correct as is",
    "purely cosmetic",
];

/// The finding says the current code is correct and the point is optional.
fn says_optional(f: &Finding) -> bool {
    let text = format!("{} {}", f.title, f.body).to_ascii_lowercase();
    OPTIONAL_PHRASES.iter().any(|p| text.contains(p))
}

/// Phrases that cite a CI result or a data query.
const EVIDENCE_PHRASES: &[&str] = &[
    "check is red",
    "ci is red",
    "failed check",
    "failed tests run",
    "failed test run",
    "check run failed",
    "fails on this head",
    "query found",
    "query returned",
    "query shows",
    "a query on",
];

/// Whether the finding traces a concrete mechanism through cited evidence:
/// a basis it names, a file and line, a code span, a CI result, a data query
/// or documentation. The audit of misgraded notes found this, not hedge
/// words, is what separates a defect from an opinion.
pub fn traces_mechanism(f: &Finding) -> bool {
    if matches!(
        f.basis(),
        Some(Basis::Code | Basis::Tests | Basis::Ci | Basis::Data)
    ) || (f.basis() == Some(Basis::Documented) && f.cites_source())
    {
        return true;
    }
    let lower = f.body.to_ascii_lowercase();
    cites_code(&f.body)
        || f.body.matches('`').count() >= 2
        || EVIDENCE_PHRASES.iter().any(|p| lower.contains(p))
}

fn category_in(category: &str, list: &[&str]) -> bool {
    let c = category
        .trim()
        .to_ascii_lowercase()
        .replace(['_', ' '], "-");
    list.contains(&c.as_str())
}

/// Why a note is a preference the reviewer inferred.
pub const PREFERENCE: &str = "a preference the reviewer inferred";
/// Why a note is an optional improvement.
pub const OPTIONAL: &str = "an optional suggestion; the current code is correct";
/// Why a question is a note.
pub const QUESTION: &str = "a question; reviews ask in notes and never hold the merge on one";
/// Why a team-rule finding with no defect shown is a note.
pub const TEAM_RULE: &str = "a written team rule the change doesn't follow, with no defect shown";
/// Why a finding outside the three blocking kinds is a note.
pub const NOT_BLOCKING_KIND: &str = "not a correctness, security or data-loss problem";
/// Why a finding with no file is a note.
pub const NO_LOCATION: &str = "no location in the change";
/// Why an unshown concern is a note.
pub const UNSHOWN: &str = "a concern the review couldn't show with evidence";

/// How a note is labeled on the approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Label {
    ShouldFix,
    Nit,
    Suggestion,
    Fyi,
}

impl Label {
    pub fn as_str(self) -> &'static str {
        match self {
            Label::ShouldFix => "Should fix",
            Label::Nit => "Nit",
            Label::Suggestion => "Suggestion",
            Label::Fyi => "FYI",
        }
    }
}

/// The three tiers a finding falls in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Blocking,
    ShouldFix,
    Suggestion,
}

impl Tier {
    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Blocking => "blocking",
            Tier::ShouldFix => "should-fix",
            Tier::Suggestion => "suggestion",
        }
    }
}

/// The tier of a finding, from its class.
pub fn tier(f: &Finding) -> Tier {
    match label(f) {
        None => Tier::Blocking,
        Some(Label::ShouldFix) => Tier::ShouldFix,
        Some(_) => Tier::Suggestion,
    }
}

/// The label a note carries (`None`: it blocks).
pub fn label(f: &Finding) -> Option<Label> {
    match classify(f) {
        Class::Blocks(_) => None,
        Class::Note(r) if r == PREFERENCE => Some(Label::Nit),
        Class::Note(r) if r == TEAM_RULE || r == NOT_BLOCKING_KIND => Some(Label::ShouldFix),
        Class::Note(r) if r == OPTIONAL => Some(Label::Suggestion),
        Class::Note(_) => Some(Label::Fyi),
    }
}

/// Strictly optional: a preference the reviewer inferred or an optional
/// improvement where the code is correct. On CoderHelm's own pull requests
/// everything else is fixed before the PR is marked ready
/// ([`super::own_review`]).
pub fn optional(f: &Finding) -> bool {
    matches!(classify(f), Class::Note(r) if r == PREFERENCE || r == OPTIONAL)
}

/// Categories that name a data-loss problem.
const DATA_CATEGORIES: &[&str] = &[
    "data",
    "data-loss",
    "dataloss",
    "data-integrity",
    "integrity",
    "durability",
    "persistence",
    "migration",
];

/// Categories of findings about tests.
const TEST_CATEGORIES: &[&str] = &["tests", "test", "test-coverage", "coverage"];

/// Phrases of a finding that asks for a test rather than showing one fails.
const MISSING_TEST_PHRASES: &[&str] = &[
    "no test",
    "missing test",
    "add a test",
    "needs a test",
    "need a test",
    "not tested",
    "untested",
    "without tests",
    "without a test",
    "lacks test",
    "lacks a test",
    "no coverage",
    "isn't covered",
    "is not covered",
    "add tests",
    "add coverage",
];

/// A tests finding that asks for a test (a Suggestion), not one that shows
/// a test fails or proves nothing (a correctness problem).
fn asks_for_a_test(f: &Finding) -> bool {
    let text = format!("{} {}", f.title, f.body).to_ascii_lowercase();
    MISSING_TEST_PHRASES.iter().any(|p| text.contains(p))
}

/// The blocking kind a finding is, if it is one: security by its category,
/// data loss by its category, and correctness for any other defect: a PR
/// that misses its own stated goal, a test that fails or proves nothing, or
/// a hardening or maintainability finding that traces a concrete defect. A
/// preference, a question, a request for a test, and an improvement with no
/// defect shown are no blocking kind.
pub fn blocking_kind(f: &Finding) -> Option<common::stop_judge::BlockCategory> {
    use common::stop_judge::BlockCategory as K;
    if category_in(&f.category, &["security"]) {
        return Some(K::Security);
    }
    if category_in(&f.category, DATA_CATEGORIES) {
        return Some(K::DataLoss);
    }
    if f.defeats_goal == Some(true) {
        return Some(K::Correctness);
    }
    if category_in(&f.category, PREFERENCE_CATEGORIES) || category_in(&f.category, &["question"]) {
        return None;
    }
    if category_in(&f.category, TEST_CATEGORIES) && asks_for_a_test(f) {
        return None;
    }
    // A category alone (hardening, maintainability, observability) never
    // makes a finding optional when it traces a concrete defect.
    if category_in(&f.category, SUGGESTION_CATEGORIES)
        && !(traces_mechanism(f) && f.optional != Some(true))
    {
        return None;
    }
    Some(K::Correctness)
}

/// Whether the finding shows a security or data-loss defect: the model's
/// `optional` can't make one of those a suggestion.
fn shown_harm(f: &Finding) -> bool {
    matches!(
        blocking_kind(f),
        Some(
            common::stop_judge::BlockCategory::Security
                | common::stop_judge::BlockCategory::DataLoss
        )
    ) && f.evidence() == Evidence::VerifiedDefect
        && traces_mechanism(f)
}

/// The rubric. Pure over the finding's fields.
pub fn classify(f: &Finding) -> Class {
    // What blocks by rule is decided first: no policy note, question form or
    // `optional` flag lifts it.
    if let Some(why) = super::review_rules::mandatory(f) {
        return Class::Blocks(why);
    }
    if f.note_only.is_some() {
        return Class::Note("a policy made it a note");
    }
    if f.evidence() == Evidence::Question {
        return Class::Note(QUESTION);
    }
    let team_rule = f.team_rule.as_deref().is_some_and(|r| !r.trim().is_empty());
    if team_rule && blocking_kind(f).is_none() {
        return Class::Note(TEAM_RULE);
    }
    if f.severity.trim().eq_ignore_ascii_case("nit")
        || category_in(&f.category, PREFERENCE_CATEGORIES)
    {
        return Class::Note(PREFERENCE);
    }
    // A missing test where tests aren't required is a should-fix note: the
    // model's `optional` doesn't make new untested code a suggestion.
    if category_in(&f.category, TEST_CATEGORIES) && asks_for_a_test(f) && !says_optional(f) {
        return if f.severity_check.as_ref().is_some_and(|c| c.blocking) {
            located_and_shown(f, "the severity check found it a blocking problem")
        } else {
            Class::Note(NOT_BLOCKING_KIND)
        };
    }
    if (f.optional == Some(true) || says_optional(f)) && !shown_harm(f) {
        return Class::Note(OPTIONAL);
    }
    let judged_blocking = f.severity_check.as_ref().is_some_and(|c| c.blocking);
    let Some(kind) = blocking_kind(f) else {
        if judged_blocking {
            return located_and_shown(f, "the severity check found it a blocking problem");
        }
        return if category_in(&f.category, SUGGESTION_CATEGORIES) {
            Class::Note(OPTIONAL)
        } else {
            Class::Note(NOT_BLOCKING_KIND)
        };
    };
    if judged_blocking {
        return located_and_shown(f, "the severity check found it a blocking problem");
    }
    if f.evidence() == Evidence::UnverifiedConcern
        && (f.basis() == Some(Basis::External) || !traces_mechanism(f))
    {
        return Class::Note(UNSHOWN);
    }
    if !traces_mechanism(f) && f.evidence() != Evidence::VerifiedDefect {
        return Class::Note(UNSHOWN);
    }
    located_and_shown(
        f,
        match kind {
            common::stop_judge::BlockCategory::Security => "a security problem shown by evidence",
            common::stop_judge::BlockCategory::DataLoss => "a data-loss problem shown by evidence",
            common::stop_judge::BlockCategory::Correctness => {
                if f.defeats_goal == Some(true) {
                    "the PR doesn't achieve its stated goal"
                } else {
                    "a correctness problem shown by evidence"
                }
            }
        },
    )
}

/// Blocks only with a location in the change.
fn located_and_shown(f: &Finding, why: &'static str) -> Class {
    if f.file.trim().is_empty() {
        return Class::Note(NO_LOCATION);
    }
    Class::Blocks(why)
}

// ── Grounding: fields from what the finding shows ──

/// An exit or status code whose meaning the tool's own documentation fixes.
struct DocumentedCode {
    /// Words that name the tool or protocol (lower case).
    tool: &'static [&'static str],
    codes: &'static [u32],
    /// What the documentation says, in a few words.
    meaning: &'static str,
    source: &'static str,
}

/// Codes that mean success although they aren't 0 or 200. Code that treats
/// one as a failure fails on a success path.
const DOCUMENTED_CODES: &[DocumentedCode] = &[
    DocumentedCode {
        tool: &["msiexec", "windows installer", ".msi"],
        codes: &[3010, 1641],
        meaning: "msiexec exit codes 3010 (ERROR_SUCCESS_REBOOT_REQUIRED) and 1641 \
                  (ERROR_SUCCESS_REBOOT_INITIATED) mean the install succeeded",
        source: "Windows Installer error codes, \
                 https://learn.microsoft.com/en-us/windows/win32/msi/error-codes",
    },
    DocumentedCode {
        tool: &["robocopy"],
        codes: &[1, 2, 3, 4, 5, 6, 7],
        meaning: "robocopy exit codes below 8 mean the copy succeeded",
        source: "Return codes used by the Robocopy utility, \
                 https://learn.microsoft.com/en-us/troubleshoot/windows-server/backup-and-storage/return-codes-used-robocopy-utility",
    },
    DocumentedCode {
        tool: &["http", "status code", "status"],
        codes: &[201, 202, 204, 206, 304],
        meaning: "HTTP 2xx statuses are successes, and 304 Not Modified is a successful \
                  conditional request",
        source: "RFC 9110 (HTTP Semantics), section 15, \
                 https://www.rfc-editor.org/rfc/rfc9110#section-15",
    },
];

/// Words saying the code treats the outcome as a failure.
const FAILURE_WORDS: &[&str] = &["fail", "throw", "error", "abort", "non-zero", "nonzero"];

/// Paths and words that mark code which runs once in production with
/// nothing to retry or roll it back.
const ONE_SHOT_PATHS: &[&str] = &[
    "user_data",
    "userdata",
    "user-data",
    "cloud-init",
    "cloudinit",
    "startup",
    "bootstrap",
    "provision",
    "migration",
    "migrate",
    "deploy",
    "release",
];
const ONE_SHOT_WORDS: &[&str] = &[
    "user_data",
    "user data",
    "startup script",
    "provisioning",
    "first boot",
    "migration",
    "deploy step",
    "deploy check",
    "release step",
    "runs once",
    "run once",
    "one-shot",
    "one shot",
    "no retry",
    "without retry",
    "never retried",
];

/// Whether the finding is on code that runs once in production.
pub fn one_shot(f: &Finding) -> bool {
    if f.one_shot.is_some() {
        return f.one_shot == Some(true);
    }
    let path = f.file.to_ascii_lowercase();
    let text = format!("{} {}", f.title, f.body).to_ascii_lowercase();
    ONE_SHOT_PATHS.iter().any(|p| path.contains(p))
        || ONE_SHOT_WORDS.iter().any(|w| text.contains(w))
}

/// Whether `n` appears in `text` as a number of its own.
fn has_number(text: &str, n: u32) -> bool {
    let needle = n.to_string();
    let b = text.as_bytes();
    text.match_indices(&needle).any(|(i, m)| {
        let before = i == 0 || !b[i - 1].is_ascii_digit();
        let end = i + m.len();
        let after = end == b.len() || !b[end].is_ascii_digit();
        before && after
    })
}

/// The documented code a finding says the change gets backwards, if any:
/// it names the tool, one of its success codes, and a failure.
fn documented_contradiction(f: &Finding) -> Option<&'static DocumentedCode> {
    let text = format!(
        "{} {} {}",
        f.title,
        f.body,
        f.suggestion.as_deref().unwrap_or("")
    )
    .to_ascii_lowercase();
    if !FAILURE_WORDS.iter().any(|w| text.contains(w)) {
        return None;
    }
    DOCUMENTED_CODES.iter().find(|d| {
        d.tool.iter().any(|t| text.contains(t)) && d.codes.iter().any(|&c| has_number(&text, c))
    })
}

/// Whether `word` is a code reference: `path/file.ext:12` (or `#L12`).
fn code_ref(word: &str) -> bool {
    let w = word.trim_matches(|c: char| "`'\"()[]{},;".contains(c));
    let (path, line) = match w.split_once(":").or_else(|| w.split_once("#L")) {
        Some(p) => p,
        None => return false,
    };
    let ext_ok = path.rsplit_once('.').is_some_and(|(stem, ext)| {
        !stem.is_empty()
            && (1..=6).contains(&ext.len())
            && ext.chars().all(|c| c.is_ascii_alphanumeric())
    });
    ext_ok && line.chars().next().is_some_and(|c| c.is_ascii_digit())
}

/// Whether `word` names a file: `dir/name.ext` or `name.ext`.
fn file_ref(word: &str) -> bool {
    let w = word.trim_matches(|c: char| "`'\"()[]{},;:".contains(c));
    w.rsplit_once('.').is_some_and(|(stem, ext)| {
        !stem.is_empty()
            && (1..=5).contains(&ext.len())
            && ext
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
            && ext.chars().any(|c| c.is_ascii_lowercase())
            && stem
                .chars()
                .all(|c| c.is_alphanumeric() || "/_-.".contains(c))
    })
}

/// Whether `text` points at a line: "line 12", "line ~52", "lines 3-9",
/// "L357".
fn line_ref(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    let words: Vec<&str> = lower.split_whitespace().collect();
    words.windows(2).any(|w| {
        (w[0].trim_start_matches('(') == "line" || w[0].trim_start_matches('(') == "lines")
            && w[1]
                .trim_start_matches('~')
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_digit())
    }) || text.split_whitespace().any(|w| {
        let w = w.trim_matches(|c: char| "`'\"()[]{},;".contains(c));
        w.strip_prefix('L')
            .is_some_and(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit() || c == '-'))
    })
}

/// Whether the text traces the code: a file with a line (`path:12`, or a
/// file named next to "line 12").
pub fn cites_code(text: &str) -> bool {
    text.split_whitespace().any(code_ref)
        || (text.split_whitespace().any(file_ref) && line_ref(text))
}

/// Phrases that cite a team's or repository's written rule.
const RULE_PHRASES: &[&str] = &[
    "team rules",
    "team's rules",
    "team's written rules",
    "team instructions",
    "team's instructions",
    "repository's rules",
    "repository's own rules",
    "repo's rules",
    "repo's own rules",
    "repository instructions",
    "written rule",
    "rules.md",
    "contributing.md",
    "agents.md",
    "claude.md",
];

/// The written rule a finding cites in its text, if any.
fn cited_rule(f: &Finding) -> Option<&'static str> {
    let text = format!("{} {}", f.title, f.body).to_ascii_lowercase();
    RULE_PHRASES.iter().copied().find(|p| text.contains(p))
}

/// Lower case letters and digits, one space between words.
fn normalized(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether `rule` is quoted from the team's written rules.
pub fn quotes_team_rule(rule: &str, team_rules: &str) -> bool {
    let r = normalized(rule);
    r.len() >= 12 && normalized(team_rules).contains(&r)
}

/// What [`ground`] changed.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Grounded {
    /// Documented exit or status codes the code gets backwards on a path
    /// that runs once in production.
    pub documented: usize,
    /// Concerns that trace the code path, made verified.
    pub traced: usize,
    /// Cited team rules that aren't in the team's written rules (dropped).
    pub unquoted_rules: usize,
    /// Findings whose text cites the team's or repository's written rules.
    pub cited_rules: usize,
    /// Unsettled concerns whose text says they hit every request (or call,
    /// search, run) of the change: a main-path risk.
    pub main_path: usize,
}

/// Fills the classifier's fields from what each finding shows. Runs after
/// the critic and before the checks that may make a finding a note.
///
/// - A finding naming a documented success code the code treats as a
///   failure, on a path that runs once in production, is verified by that
///   documentation and is a defect, never a hardening note.
/// - A concern that traces the code path (a file and line) is verified by
///   the code: "not verified" is only for unknown external state.
/// - A cited team rule must be quoted from the team's written rules; one
///   that isn't is dropped (the finding is then judged without it).
pub fn ground(findings: &mut [Finding], team_rules: &str) -> Grounded {
    let mut g = Grounded::default();
    for f in findings.iter_mut() {
        if f.note_only.is_some() || f.evidence() == Evidence::Question {
            continue;
        }
        if let Some(rule) = f.team_rule.clone() {
            if !quotes_team_rule(&rule, team_rules) {
                f.team_rule = None;
                g.unquoted_rules += 1;
            }
        }
        // A finding whose text cites the team's or the repository's written
        // rules (a rules file it read, the team instructions) breaks a rule.
        if f.team_rule.is_none() && !says_optional(f) {
            if let Some(cited) = cited_rule(f) {
                f.team_rule = Some(format!("cited in the finding ({cited})"));
                g.cited_rules += 1;
            }
        }
        if one_shot(f) {
            if let Some(doc) = documented_contradiction(f) {
                let was_held = f.holds_merge();
                f.verify_by_documentation(doc.source);
                f.one_shot = Some(true);
                f.optional = Some(false);
                if category_in(&f.category, PREFERENCE_CATEGORIES)
                    || category_in(&f.category, SUGGESTION_CATEGORIES)
                {
                    f.category = "bug".to_string();
                }
                if !was_held {
                    let why = format!(
                        "This blocks: {}, so the code fails on a success path. It runs once in \
                         production with no automatic retry, so a person would have to finish \
                         the work by hand; how rarely the code is returned doesn't lower that. \
                         Source: {}.",
                        doc.meaning,
                        f.source.as_deref().unwrap_or(doc.source).trim()
                    );
                    f.body = format!("{}\n\n_{why}_", f.body.trim_end());
                    g.documented += 1;
                }
                continue;
            }
        }
        if f.evidence() == Evidence::UnverifiedConcern
            && f.basis() != Some(Basis::External)
            && cites_code(&f.body)
        {
            f.verify_by_code();
            g.traced += 1;
        }
        if f.main_path.as_deref().is_none_or(|m| m.trim().is_empty()) {
            if let Some(path) = unsettled_main_path(f) {
                f.main_path = Some(path);
                g.main_path += 1;
            }
        }
    }
    g
}

/// Categories whose concern isn't a possible break even when it is on
/// every call: speed, upkeep, scope, what is logged or measured.
const NOT_A_BREAK_CATEGORIES: &[&str] = &[
    "question",
    "perf",
    "performance",
    "observability",
    "maintainability",
    "refactor",
    "scope",
    "follow-up",
    "followup",
    "manual-check",
];

/// Words that start a clause saying the code runs on every call.
const EVERY_WORDS: &[&str] = &["every", "each", "all"];

/// What the change runs on every call of: the noun after "every".
const MAIN_PATH_NOUNS: &[&str] = &[
    "request",
    "call",
    "search",
    "query",
    "write",
    "read",
    "run",
    "upload",
    "download",
    "message",
    "event",
    "login",
    "sync",
    "job",
    "build",
    "deploy",
    "invocation",
    "page",
];

/// Phrases of a finding that says it couldn't settle the point.
const UNSETTLED_PHRASES: &[&str] = &[
    "couldn't confirm",
    "could not confirm",
    "can't confirm",
    "cannot confirm",
    "couldn't verify",
    "could not verify",
    "can't verify",
    "cannot verify",
    "couldn't check",
    "could not check",
    "couldn't tell",
    "could not tell",
    "not verified",
    "unverified",
    "unclear whether",
    "not sure whether",
];

/// The main path an unsettled concern says it hits ("every search with a
/// body"), from its own words: it says the code runs on every request (or
/// call, search, write, run) and that it couldn't settle whether that
/// breaks. A concern about one edge case, or one that settled the point,
/// has none.
pub fn unsettled_main_path(f: &Finding) -> Option<String> {
    if f.evidence() != Evidence::UnverifiedConcern
        || says_optional(f)
        || category_in(&f.category, PREFERENCE_CATEGORIES)
        || category_in(&f.category, TEST_CATEGORIES)
        || category_in(&f.category, NOT_A_BREAK_CATEGORIES)
        || f.file.trim().is_empty()
    {
        return None;
    }
    let text = format!("{}. {}", f.title, f.body);
    let lower = text.to_ascii_lowercase();
    if !UNSETTLED_PHRASES.iter().any(|p| lower.contains(p)) {
        return None;
    }
    let words: Vec<&str> = lower.split_whitespace().collect();
    words.windows(2).enumerate().find_map(|(i, w)| {
        let noun = w[1].trim_matches(|c: char| !c.is_ascii_alphanumeric());
        let noun = noun
            .strip_suffix("es")
            .filter(|n| n.ends_with("ch"))
            .unwrap_or(noun);
        let noun = noun.strip_suffix('s').unwrap_or(noun);
        if !EVERY_WORDS.contains(&w[0]) || !MAIN_PATH_NOUNS.contains(&noun) {
            return None;
        }
        // The clause, up to its first punctuation.
        let clause: Vec<&str> = words[i..]
            .iter()
            .take(8)
            .scan(false, |ended, w| {
                if *ended {
                    return None;
                }
                *ended = w.ends_with([':', '.', ',', ';', ')']);
                Some(w.trim_end_matches([':', '.', ',', ';', ')']))
            })
            .collect();
        Some(clause.join(" "))
    })
}

/// How an unsettled main-path risk's posted body names its check.
pub const NEEDS_CHECK_LEAD: &str = "This needs a check before merge:";

/// Sets each finding's severity to match its class: a finding that holds the
/// merge reads as blocking ("high" at least), a note is "medium" at most.
/// The model's severity is kept in `model_severity` when it changes.
///
/// A finding that holds the merge loses the sentences that call it optional
/// ("a suggestion only", "just a heads-up"), so its words match the verdict.
pub fn finalize(findings: &mut [Finding]) {
    for f in findings.iter_mut() {
        if f.holds_merge() {
            let stripped = super::review_rules::strip_hedges(&f.body);
            if !stripped.is_empty() {
                f.body = stripped;
            }
        }
        // An unsettled main-path risk names the check that settles it, so
        // "not verified" is never its last word.
        if super::review_rules::mandatory(f) == Some(super::review_rules::MAIN_PATH_WHY)
            && f.evidence() != Evidence::VerifiedDefect
            && !f.body.contains(NEEDS_CHECK_LEAD)
        {
            f.body = format!(
                "{}\n\n_{NEEDS_CHECK_LEAD} {}. This is on {}, and nothing in the review or the PR's \
                 own verification exercises it yet. If the check passes, say so here and this \
                 becomes a note; if it fails, it is a defect to fix._",
                f.body.trim_end(),
                super::review_rules::settling_check(f),
                f.main_path.as_deref().unwrap_or("the change's main path").trim()
            );
        }
        let high = matches!(
            f.severity.trim().to_ascii_lowercase().as_str(),
            "blocking" | "high" | "critical"
        );
        let to = match (f.holds_merge(), high) {
            (true, false) => "high",
            (false, true) => "medium",
            _ => continue,
        };
        if f.model_severity.is_none() {
            f.model_severity = Some(f.severity.clone());
        }
        f.severity = to.to_string();
    }
}

// ── The second pass: an independent check of every note ──

/// The rubric the severity check applies; the same as [`classify`].
pub const JUDGE_SYSTEM: &str = "You check the severity of code review findings that were posted \
     as non-blocking notes. You see each finding's text, the diff hunk it is on, the pull \
     request's stated goal and the team's written rules; you don't see the reviewer's reasoning. \
     A finding BLOCKS the merge only when it is a correctness, security or data-loss problem \
     that the finding shows with evidence (the code path, a test or CI result, a data query, or \
     the documentation of the tool, API or platform involved: exit codes, status codes, vendor \
     docs) at a location in the change. A finding also BLOCKS when it breaks a rule the team's \
     written rules mark required (MUST, MUST NOT, NEVER, REQUIRED or ALWAYS in capitals), or \
     when it is on a critical path (a header, hash or signature another system validates, \
     auth, payments, data writes, a public contract, a deploy step) and nothing in the PR \
     proves the change holds there (no test, no check). A finding also BLOCKS when it is a \
     possible break of the change's main path (code that every request, search, write or run \
     the change exists for goes through) that the finding couldn't settle and that neither the \
     PR's tests nor its described verification exercise: unchecked state on the main path is a \
     check to run before merge, not a note. Correctness includes a PR that doesn't achieve its \
     stated goal; data loss includes lost or stale writes, cursors or watermarks that skip \
     rows, duplicates and ordering that flips between runs. Likelihood and estimated impact \
     never make a shown defect non-blocking. Everything else is a note: style and naming, \
     optional improvements, a written team rule not followed with no defect shown, questions, \
     a missing test where the team doesn't require tests, and concerns about an edge case or \
     rare input resting on state nobody could check. Output ONLY a fenced ```json block: \
     {\"checks\":[{\"index\":<int>,\"blocking\":<bool>,\"reason\":\"one short sentence\"}]} \
     with one entry per finding.";

/// The lines of `diff` (unified) in the hunk of `file` that holds right-side
/// `line`, at most 60. Empty when the diff doesn't show it.
pub fn hunk_for(diff: &str, file: &str, line: u64) -> String {
    let mut in_file = false;
    let mut hunk: Vec<&str> = Vec::new();
    let mut start = 0u64;
    let mut len = 0u64;
    let mut found = String::new();
    let mut flush = |hunk: &mut Vec<&str>, start: u64, len: u64| {
        if found.is_empty() && !hunk.is_empty() && line >= start && line < start + len.max(1) {
            found = hunk.iter().take(60).cloned().collect::<Vec<_>>().join("\n");
        }
        hunk.clear();
    };
    for l in diff.lines() {
        if let Some(rest) = l.strip_prefix("diff --git ") {
            flush(&mut hunk, start, len);
            in_file = rest.ends_with(&format!(" b/{file}"));
            continue;
        }
        if !in_file {
            continue;
        }
        if l.starts_with("@@") {
            flush(&mut hunk, start, len);
            // @@ -a,b +c,d @@
            let right = l
                .split_whitespace()
                .find(|p| p.starts_with('+'))
                .unwrap_or("+0");
            let mut parts = right[1..].split(',');
            start = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
            len = parts.next().and_then(|v| v.parse().ok()).unwrap_or(1);
            hunk.push(l);
        } else if !hunk.is_empty() {
            hunk.push(l);
        }
    }
    flush(&mut hunk, start, len);
    found
}

/// The judge's message: the stated goal, the team's rules, and each note
/// (by its index in `findings`) with its hunk. Only the finding's own text
/// goes in, never the reviewer's reasoning about severity.
pub fn judge_prompt(
    findings: &[Finding],
    indexes: &[usize],
    diff: &str,
    pr_title: &str,
    pr_body: &str,
    team_rules: &str,
) -> String {
    let mut s = format!(
        "## The PR's stated goal\n{}\n\n{}\n",
        pr_title.trim(),
        common::truncate_str(pr_body.trim(), 2_000)
    );
    if !team_rules.trim().is_empty() {
        s.push_str(&format!(
            "\n## The team's written rules\n{}\n",
            common::truncate_str(team_rules.trim(), 4_000)
        ));
    }
    s.push_str("\n## Findings posted as non-blocking\n");
    for &i in indexes {
        let f = &findings[i];
        let hunk = hunk_for(diff, &f.file, f.line);
        s.push_str(&format!(
            "\n### {i}: {}:{} [{}]\n{}\n\n{}\n",
            f.file,
            f.line,
            f.category,
            f.title.trim(),
            common::truncate_str(f.body.trim(), 1_500)
        ));
        if !hunk.is_empty() {
            s.push_str(&format!("```diff\n{hunk}\n```\n"));
        }
    }
    s
}

#[derive(Deserialize)]
struct JudgeAnswer {
    #[serde(default)]
    checks: Vec<JudgeCheck>,
}

#[derive(Deserialize)]
struct JudgeCheck {
    index: usize,
    #[serde(default)]
    blocking: bool,
    #[serde(default)]
    reason: String,
}

/// The judge's answers by finding index. None when they can't be read.
pub fn parse_judge(text: &str) -> Option<HashMap<usize, SeverityCheck>> {
    let block = match text.rfind("```json") {
        Some(s) => {
            let after = &text[s + 7..];
            after.find("```").map(|e| &after[..e])?
        }
        None => text,
    };
    let a: JudgeAnswer = serde_json::from_str(block.trim()).ok()?;
    Some(
        a.checks
            .into_iter()
            .map(|c| {
                (
                    c.index,
                    SeverityCheck {
                        blocking: c.blocking,
                        reason: c.reason.trim().to_string(),
                    },
                )
            })
            .collect(),
    )
}

/// The notes the second pass checks: every finding that doesn't hold the
/// merge, except those a policy made notes (the PR tracks it, didn't cause
/// it, a late blocker).
pub fn second_pass_candidates(findings: &[Finding]) -> Vec<usize> {
    findings
        .iter()
        .enumerate()
        .filter(|(_, f)| f.note_only.is_none() && !f.holds_merge())
        .map(|(i, _)| i)
        .collect()
}

/// Records the judge's answers on the findings. A finding the judge calls
/// blocking holds the merge, and its body says why. Returns how many it
/// raised.
pub fn apply_judge(
    findings: &mut [Finding],
    indexes: &[usize],
    answers: &HashMap<usize, SeverityCheck>,
) -> usize {
    let mut raised = 0;
    for &i in indexes {
        let (Some(f), Some(check)) = (findings.get_mut(i), answers.get(&i)) else {
            continue;
        };
        f.severity_check = Some(check.clone());
        if check.blocking {
            if f.evidence() == Evidence::Question {
                f.blocks = Some(true);
            }
            if !check.reason.is_empty() && !f.body.contains(&check.reason) {
                f.body = format!(
                    "{}\n\n_This blocks: {}_",
                    f.body.trim_end(),
                    check.reason.trim_end_matches('.')
                );
            }
            raised += 1;
        }
    }
    raised
}

/// What the second pass did, for the logs.
#[derive(Debug, Default, Clone, Copy)]
pub struct SecondPass {
    pub checked: usize,
    pub raised: usize,
}

/// The second pass: one batched judge call over every note. Fail-open: when
/// the judge can't be reached or read, the notes stay as they are and are
/// not marked checked.
#[allow(clippy::too_many_arguments)]
pub async fn second_pass(
    state: &WorkerState,
    provider: &ModelProvider,
    findings: &mut [Finding],
    diff: &str,
    pr_title: &str,
    pr_body: &str,
    team_rules: &str,
    usage: &mut TokenUsage,
) -> SecondPass {
    let indexes = second_pass_candidates(findings);
    if indexes.is_empty() {
        return SecondPass::default();
    }
    let prompt = judge_prompt(findings, &indexes, diff, pr_title, pr_body, team_rules);
    let reply = provider::converse_simple(
        state,
        provider,
        provider.judgment_model_id(),
        JUDGE_SYSTEM,
        &prompt,
        usage,
    )
    .await;
    let Some(answers) = reply.ok().and_then(|t| parse_judge(&t)) else {
        return SecondPass::default();
    };
    let raised = apply_judge(findings, &indexes, &answers);
    SecondPass {
        checked: indexes.iter().filter(|i| answers.contains_key(i)).count(),
        raised,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn finding(v: serde_json::Value) -> Finding {
        let mut base = json!({"file": "src/a.rs", "line": 3, "title": "t", "body": "b"});
        for (k, val) in v.as_object().unwrap() {
            base[k] = val.clone();
        }
        serde_json::from_value(base).unwrap()
    }

    #[test]
    fn blocking_is_the_default_and_severity_is_only_an_input() {
        // A verified bug blocks whatever severity the model gave it.
        for sev in ["blocking", "high", "medium", "low"] {
            let f =
                finding(json!({"severity": sev, "category": "bug", "evidence": "verified-defect"}));
            assert!(f.holds_merge(), "{sev}");
        }
        // A data query verifies like the code does.
        let f = finding(
            json!({"severity": "medium", "category": "correctness", "basis": "data", "evidence": "unverified-concern"}),
        );
        assert!(f.holds_merge());
    }

    #[test]
    fn the_three_kinds_of_note() {
        let pref = finding(
            json!({"severity": "high", "category": "naming", "evidence": "verified-defect"}),
        );
        assert_eq!(classify(&pref), Class::Note(PREFERENCE));
        let nit = finding(json!({"severity": "nit", "category": "bug"}));
        assert!(!nit.holds_merge());
        let optional = finding(json!({"severity": "high", "category": "perf", "optional": true}));
        assert!(!optional.holds_merge());
        let hardening = finding(
            json!({"severity": "high", "category": "hardening", "evidence": "verified-defect"}),
        );
        assert!(!hardening.holds_merge());
        let external =
            finding(json!({"severity": "high", "category": "correctness", "basis": "external"}));
        assert!(!external.holds_merge());
        // The same external concern on a one-shot path is an FYI note too:
        // nothing shows the problem (a documented code would, see
        // `ground_reads_documented_codes_on_one_shot_paths`).
        let on_deploy = finding(
            json!({"file": "deploy/run.sh", "severity": "medium", "category": "correctness", "basis": "external"}),
        );
        assert!(!on_deploy.holds_merge());
        assert_eq!(label(&on_deploy), Some(Label::Fyi));
        assert_eq!(label(&pref), Some(Label::Nit));
        assert_eq!(label(&hardening), Some(Label::Suggestion));
        assert_eq!(label(&optional), Some(Label::Suggestion));
    }

    #[test]
    fn goals_and_shown_security_block_a_team_rule_alone_is_should_fix() {
        let rule = finding(
            json!({"severity": "low", "category": "convention", "team_rule": "Log a WARNING with the record id"}),
        );
        assert!(!rule.holds_merge());
        assert_eq!(classify(&rule), Class::Note(TEAM_RULE));
        assert_eq!(label(&rule), Some(Label::ShouldFix));
        assert_eq!(tier(&rule), Tier::ShouldFix);
        // A shown security defect the model called optional still blocks.
        let leak = finding(
            json!({"severity": "low", "category": "security", "basis": "code", "optional": true,
                   "body": "`src/a.rs:3` inlines the secret into the public bundle"}),
        );
        assert!(leak.holds_merge());
        // A team rule broken by a shown correctness defect blocks as that.
        let rule_bug = finding(
            json!({"severity": "low", "category": "correctness", "evidence": "verified-defect", "team_rule": "Never drop rows"}),
        );
        assert!(rule_bug.holds_merge());
        let goal =
            finding(json!({"severity": "medium", "category": "suggestion", "defeats_goal": true}));
        assert!(goal.holds_merge());
        // Security blocks when the finding shows it; an unshown concern is FYI.
        let sec = finding(
            json!({"severity": "low", "category": "security", "evidence": "unverified-concern"}),
        );
        assert!(!sec.holds_merge());
        assert_eq!(label(&sec), Some(Label::Fyi));
        let shown = finding(
            json!({"severity": "low", "category": "security", "basis": "code",
                   "body": "`src/a.rs:3` builds the SQL from `req.query`"}),
        );
        assert!(shown.holds_merge());
        assert_eq!(
            classify(&shown),
            Class::Blocks("a security problem shown by evidence")
        );
        // No location: a note.
        let nowhere = finding(json!({"file": "", "category": "correctness", "basis": "code"}));
        assert_eq!(classify(&nowhere), Class::Note(NO_LOCATION));
        // A policy note stays a note.
        let mut tracked = finding(json!({"severity": "high", "category": "bug"}));
        super::super::review_policy::demote(&mut tracked, "tracked");
        assert!(!tracked.holds_merge());
    }

    #[test]
    fn ground_reads_documented_codes_on_one_shot_paths() {
        let mut fs = vec![
            finding(
                json!({"file": "modules/host/main.tf", "severity": "medium", "category": "hardening", "evidence": "unverified-concern",
                "title": "msiexec 3010 would be logged as a failure",
                "body": "The startup script throws on any non-zero code; a fresh install usually returns 0."}),
            ),
            // Same code, not a one-shot path: left as rated.
            finding(
                json!({"file": "tools/dev/setup.ps1", "severity": "medium", "category": "hardening", "evidence": "unverified-concern",
                "title": "msiexec 3010 printed as an error", "body": "A developer can run it again."}),
            ),
            // A one-shot path, no documented code: left as rated.
            finding(
                json!({"file": "deploy/smoke.sh", "severity": "medium", "category": "hardening", "evidence": "unverified-concern",
                "title": "Might time out", "body": "A slow endpoint might fail the check."}),
            ),
        ];
        let g = ground(&mut fs, "");
        assert_eq!(g.documented, 1);
        assert!(fs[0].holds_merge());
        assert_eq!(fs[0].basis(), Some(Basis::Documented));
        assert!(fs[0]
            .source
            .as_deref()
            .unwrap()
            .contains("learn.microsoft.com"));
        assert!(!fs[1].holds_merge());
        assert!(!fs[2].holds_merge());
        // A 304 on a deploy check.
        let mut http = vec![finding(
            json!({"file": "scripts/deploy/check.sh", "severity": "low", "category": "hardening",
            "title": "304 fails the deploy", "body": "The check exits 1 unless the HTTP status is 200."}),
        )];
        assert_eq!(ground(&mut http, "").documented, 1);
        assert!(http[0].body.contains("RFC 9110"));
    }

    #[test]
    fn ground_verifies_a_traced_code_path_and_checks_quoted_rules() {
        let mut fs = vec![
            finding(
                json!({"severity": "medium", "category": "correctness", "evidence": "unverified-concern",
                "body": "The write in `src/sync/write.rs:88` keeps updated_at, and `src/sync/cursor.rs:40` reads MAX(updated_at)."}),
            ),
            finding(
                json!({"severity": "medium", "category": "correctness", "evidence": "unverified-concern",
                "body": "Another worker might write at the same time."}),
            ),
            finding(
                json!({"severity": "medium", "category": "convention", "team_rule": "Always use tabs for indentation in every file"}),
            ),
        ];
        let g = ground(
            &mut fs,
            "Log a WARNING with the record id for every skipped record.",
        );
        assert_eq!(g.traced, 1);
        assert_eq!(g.unquoted_rules, 1);
        assert!(fs[0].holds_merge());
        assert!(!fs[1].holds_merge());
        assert!(fs[2].team_rule.is_none());
        assert!(!fs[2].holds_merge());
        assert!(cites_code("see web/app.ts#L12"));
        assert!(!cites_code("at 10:30 the job ran"));
    }

    #[test]
    fn finalize_makes_the_severity_read_like_the_class() {
        let mut fs = vec![
            finding(json!({"severity": "medium", "category": "bug"})),
            finding(json!({"severity": "high", "category": "style"})),
            finding(json!({"severity": "blocking", "category": "bug"})),
        ];
        finalize(&mut fs);
        assert_eq!(fs[0].severity, "high");
        assert_eq!(fs[0].model_severity.as_deref(), Some("medium"));
        assert_eq!(fs[1].severity, "medium");
        assert_eq!(fs[2].severity, "blocking");
        assert!(fs[2].model_severity.is_none());
    }

    #[test]
    fn the_second_pass_sees_notes_only_and_can_raise_them() {
        let mut fs = vec![
            finding(json!({"severity": "high", "category": "bug"})),
            finding(
                json!({"severity": "medium", "category": "hardening", "title": "Ties flip",
                "body": "Rows with the same sort key still flip between full and incremental runs."}),
            ),
            finding(json!({"severity": "low", "category": "style"})),
        ];
        let mut tracked = finding(json!({"severity": "high", "category": "bug"}));
        super::super::review_policy::demote(&mut tracked, "tracked");
        fs.push(tracked);
        let idx = second_pass_candidates(&fs);
        assert_eq!(idx, vec![1, 2]);
        let diff = "diff --git a/src/a.rs b/src/a.rs\n--- a/src/a.rs\n+++ b/src/a.rs\n@@ -1,2 +1,4 @@\n fn a() {}\n+fn b() {}\n+fn c() {}\n fn d() {}\n";
        let prompt = judge_prompt(
            &fs,
            &idx,
            diff,
            "Stop rows flipping between runs",
            "Removes the flip.",
            "",
        );
        assert!(prompt.contains("Stop rows flipping between runs"));
        assert!(prompt.contains("+fn b() {}"));
        assert!(!prompt.contains("### 0:"));
        let answers = parse_judge(
            "```json\n{\"checks\":[{\"index\":1,\"blocking\":true,\"reason\":\"the PR's goal is to stop this flip\"},{\"index\":2,\"blocking\":false,\"reason\":\"a preference\"}]}\n```",
        )
        .unwrap();
        assert_eq!(apply_judge(&mut fs, &idx, &answers), 1);
        assert!(fs[1].holds_merge());
        assert!(fs[1]
            .body
            .contains("This blocks: the PR's goal is to stop this flip"));
        assert!(!fs[2].holds_merge());
        assert_eq!(
            fs[2].severity_check.as_ref().map(|c| c.blocking),
            Some(false)
        );
        assert!(parse_judge("not json").is_none());
    }

    #[test]
    fn the_judge_is_told_the_rubric() {
        for part in [
            "BLOCKS the merge only when it is a correctness, security or data-loss",
            "at a location in the change",
            "style and naming",
            "optional improvements",
            "doesn't achieve its",
            "written team rule not followed",
            "Likelihood and estimated impact",
        ] {
            assert!(JUDGE_SYSTEM.contains(part), "{part}");
        }
    }

    #[test]
    fn a_blocking_finding_reads_as_blocking_in_the_posted_comment() {
        let mut fs = vec![
            finding(
                json!({"severity": "medium", "category": "correctness", "evidence": "verified-defect", "title": "Stale cursor"}),
            ),
            finding(
                json!({"line": 4, "severity": "high", "category": "naming", "title": "Rename"}),
            ),
        ];
        finalize(&mut fs);
        let changed = HashMap::from([(
            "src/a.rs".to_string(),
            std::collections::HashSet::from([3u64, 4]),
        )]);
        let posted = super::super::review_agent::to_postable(&fs, &changed, &HashMap::new());
        let blocker = &posted.inline[0].body;
        assert!(
            blocker.starts_with("🔴 **Blocking (correctness): Stale cursor**"),
            "{blocker}"
        );
        assert!(!blocker.contains(super::super::review_agent::NON_BLOCKING_MARK));
        let note = &posted.inline[1].body;
        assert!(note.starts_with("💬 **Nit: Rename**"), "{note}");
        assert!(note.contains(super::super::review_agent::NON_BLOCKING_MARK));
    }

    /// The two notes of an approved review, as the reviewer emitted them: a
    /// new request path through a tunnel sends every search body chunked,
    /// "couldn't confirm" the server accepts it (the PR's live check hit a
    /// no-body endpoint), and the new request function has no test.
    fn tunnel_review() -> Vec<Finding> {
        vec![
            finding(json!({
                "file": "src/commands/search.js", "line": 101, "severity": "low",
                "category": "hardening", "evidence": "unverified-concern", "optional": true,
                "title": "Tunnelled requests with a body are sent without Content-Length",
                "body": "`request` passes `init.body` as a JSON string. On the `fetch` path, Node \
                         sets `Content-Length` for a string body. On the new `https.request` path \
                         no length is set, so Node sends `Transfer-Encoding: chunked` for every \
                         search with a body: `search query`, `find-class` and the trace hop. I \
                         couldn't confirm whether the private domain accepts chunked search \
                         requests; the description's live check names `_cluster/health`, which \
                         has no body. Setting the header keeps both paths sending the same thing.",
                "suggestion": "headers: { ...headers, Host: credentials.host, \"Content-Length\": Buffer.byteLength(body) },"
            })),
            finding(json!({
                "file": "src/commands/search.js", "line": 92, "severity": "low",
                "category": "tests", "evidence": "verified-defect", "optional": true,
                "title": "requestViaTunnel has no test",
                "body": "The new tests cover `isPrivateEndpoint` and `proxyConfig`, but nothing \
                         exercises `requestViaTunnel` or the `credentials.tunnelPort` branch in \
                         `request`. That code carries the TLS servername/Host handling and the \
                         response shape that `searchIndex` relies on. Suggested test: start a \
                         local `https.createServer` and assert the Host header, path and JSON."
            })),
        ]
    }

    #[test]
    fn an_unsettled_main_path_risk_blocks_as_needing_a_check() {
        let changed = vec!["src/commands/search.js".to_string()];
        for require_tests in [false, true] {
            let mut fs = tunnel_review();
            let g = ground(&mut fs, "");
            assert_eq!(g.main_path, 1);
            assert_eq!(fs[0].main_path.as_deref(), Some("every search with a body"));
            super::super::review_rules::apply(
                &mut fs,
                &super::super::review_rules::Policy::new(require_tests, ""),
                &changed,
            );
            finalize(&mut fs);
            // Case 1: blocks whatever "optional" and "hardening" said, and
            // names the check that settles it.
            assert!(fs[0].holds_merge(), "require_tests={require_tests}");
            assert_eq!(
                super::super::review_rules::mandatory(&fs[0]),
                Some(super::super::review_rules::MAIN_PATH_WHY)
            );
            assert_eq!(
                super::super::review_rules::mandatory_label(&fs[0]).as_deref(),
                Some("Blocking (needs a check)")
            );
            assert!(fs[0].body.contains(NEEDS_CHECK_LEAD), "{}", fs[0].body);
            assert!(fs[0].body.contains("every search with a body"));
            // Case 2: blocks with "Require tests" on, a should-fix note off.
            assert_eq!(fs[1].holds_merge(), require_tests);
            if !require_tests {
                assert_eq!(label(&fs[1]), Some(Label::ShouldFix));
            }
        }
        // The reviewer's own words for the check, when it gives them.
        let mut named = tunnel_review();
        named[0].main_path = Some("every search through the tunnel".into());
        named[0].check =
            Some("run `cli search query` once through the tunnel and paste the result".into());
        finalize(&mut named);
        assert!(named[0].holds_merge());
        assert!(named[0]
            .body
            .contains("run `cli search query` once through the tunnel and paste the result"));
        // A verified defect on the main path blocks as one.
        let mut shown = tunnel_review();
        shown[0].main_path = Some("every search with a body".into());
        shown[0].evidence = Some("verified-defect".into());
        shown[0].basis = Some("documented".into());
        shown[0].source = Some("the server's docs: Content-Length is required".into());
        assert_eq!(
            super::super::review_rules::mandatory_label(&shown[0]).as_deref(),
            Some("Blocking (main path)")
        );
    }

    #[test]
    fn a_settled_or_speculative_concern_stays_a_note() {
        // Settled harmless by a check: a note.
        let mut settled = tunnel_review();
        settled[0].main_path = Some("every search with a body".into());
        settled[0].proof = Some("a local server received the body with Content-Length set".into());
        assert!(!settled[0].holds_merge());
        // A run the PR reports settles it too.
        settled[0].proof =
            Some("checked by hand: `search query` through the tunnel returned hits".into());
        assert!(!settled[0].holds_merge());
        // Speculation about an edge case: no "every request", a note.
        let mut edge = vec![finding(json!({
            "file": "src/commands/search.js", "line": 60, "severity": "low",
            "category": "hardening", "evidence": "unverified-concern", "basis": "external",
            "title": "A slow bastion might time out the first connection",
            "body": "If the bastion is slow to start, the first connection might time out. I \
                     couldn't confirm how long it takes; a retry would be nice to have."
        }))];
        assert_eq!(ground(&mut edge, "").main_path, 0);
        assert!(!edge[0].holds_merge());
        // "Every request" with the point settled in its own words: a note.
        let mut fine = vec![finding(json!({
            "file": "src/commands/search.js", "line": 70, "severity": "low",
            "category": "hardening", "evidence": "unverified-concern",
            "title": "Every request reopens the agent",
            "body": "Every request builds a new agent; not a bug, just slower."
        }))];
        assert_eq!(ground(&mut fine, "").main_path, 0);
        assert!(!fine[0].holds_merge());
        // Speed on every request isn't a possible break.
        let mut slow = vec![finding(json!({
            "file": "src/commands/search.js", "line": 75, "severity": "medium",
            "category": "perf", "evidence": "unverified-concern",
            "title": "A new TLS handshake for every request",
            "body": "Every request opens a new connection; I couldn't confirm how slow that is."
        }))];
        assert_eq!(ground(&mut slow, "").main_path, 0);
        assert!(!slow[0].holds_merge());
        // A main path on a tests finding is judged by the tests rule only.
        let mut t = tunnel_review();
        t[1].main_path = Some("every search".into());
        assert!(!t[1].holds_merge());
    }

    #[test]
    fn hunk_for_finds_the_right_side_line() {
        let diff = "diff --git a/x.rs b/x.rs\n@@ -1,1 +1,2 @@\n a\n+b\n@@ -10,1 +20,2 @@\n y\n+z\ndiff --git a/y.rs b/y.rs\n@@ -1 +1 @@\n-q\n+r\n";
        assert!(hunk_for(diff, "x.rs", 21).contains("+z"));
        assert!(!hunk_for(diff, "x.rs", 21).contains("+b"));
        assert!(hunk_for(diff, "y.rs", 1).contains("+r"));
        assert!(hunk_for(diff, "x.rs", 99).is_empty());
    }
}
