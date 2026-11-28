//! The one gate every run status write goes through.
//!
//! A run may end three ways, and only these:
//!
//! - [`RunEnd::Done`]: with what it delivered (its pull request, a merge,
//!   the reason no change was needed, the pull request that already does
//!   the work, or the report it answered with);
//! - [`RunEnd::Waiting`]: on a named person or permission, with what they
//!   do and the evidence for why only they can;
//! - [`RunEnd::Stopped`]: with a typed reason ([`Stop`]) and what was
//!   tried ([`Tried`]).
//!
//! [`gate`] turns an ending into the write: the status, the status-index key
//! (`status_run_id`) and the ending itself, kept on the run as
//! [`FIELD_ENDING`]. An ending that lacks its evidence (a done run without
//! its pull request, a wait without who or why) is not written as claimed:
//! it is written as [`Stop::Unproven`], naming what was missing, and logged
//! as an error.
//!
//! A status that doesn't end the run (queued, running, waiting for CI) goes
//! through [`working`], typed so it can't name an ending, and a claim undone
//! goes back through [`restored`]. The status-index key is built here and
//! nowhere else: a test below reads every service's sources and refuses a
//! status write that doesn't come through this module.
//!
//! Red CI ended runs as "an error it doesn't know how to repair, tried again
//! once" with no retry made: the pass knew why it stopped, wrote the reason
//! as a sentence, and the error classifier read the sentence back. A reason
//! is a value now, from the place that knew it, to the record.

use std::collections::HashMap;

use aws_sdk_dynamodb::operation::put_item::builders::PutItemFluentBuilder;
use aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder;
use aws_sdk_dynamodb::types::AttributeValue;

use crate::run_status as st;

/// Run attribute holding the ending (a map: `kind`, `reason`, `status`,
/// `evidence`, `tried`, `retries`).
pub const FIELD_ENDING: &str = "run_ending";

/// How a run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunEnd {
    Done(Done),
    Waiting(Waiting),
    Stopped(Stopped),
}

/// A finished run and what it delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Done {
    /// Its pull request, handed over (`completed`).
    Pr { number: u64, url: String },
    /// Its pull request merged (`merged`).
    Merged { number: u64, url: String },
    /// The code already does what was asked (`no_change`).
    NoChange { why: String },
    /// An open pull request already does the work (`covered_by_pr`).
    CoveredBy { url: String },
    /// A run whose work is an answer, not a change (an alert triage, an
    /// investigation, a scheduled skill): where the answer is (`completed`).
    Answered { at: String },
}

/// Who or what a waiting run waits on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitOn {
    /// A person: who, and the one thing they do (`needs_input`).
    Person { who: String, action: String },
    /// A permission or limit only a person can change: which, and what to do
    /// (`needs_input`).
    Permission { what: String, action: String },
    /// A change outside the code (`needs_outside_change`).
    OutsideChange { what: String },
}

/// A run waiting on someone, with the evidence that only they can act.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Waiting {
    pub on: WaitOn,
    pub evidence: String,
}

/// What a stopped run tried before it stopped. Every field is a count or a
/// step that happened: "tried again" is said only from `retries`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tried {
    /// Retries recorded for the error that stopped it (`run_recovery`'s
    /// ledger).
    pub retries: u32,
    /// Fix rounds run.
    pub rounds: u32,
    /// Fixes pushed.
    pub pushed: u32,
    /// The steps taken, each in a few words.
    pub steps: Vec<String>,
}

impl Tried {
    /// What was tried, in one line. Never claims a retry the ledger doesn't
    /// hold.
    pub fn line(&self) -> String {
        let mut parts = Vec::new();
        if self.rounds > 0 {
            parts.push(format!(
                "{} fix round{} ({} pushed)",
                self.rounds,
                if self.rounds == 1 { "" } else { "s" },
                self.pushed
            ));
        }
        if self.retries > 0 {
            parts.push(format!(
                "tried again {} time{}",
                self.retries,
                if self.retries == 1 { "" } else { "s" }
            ));
        }
        parts.extend(
            self.steps
                .iter()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
        );
        if parts.is_empty() {
            "nothing before it stopped".to_string()
        } else {
            parts.join("; ")
        }
    }
}

/// A run's checks red at its ending: which, on which head, and which of them
/// the base branch has red too, read at the ending.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RedCi {
    /// The failing checks on the head.
    pub checks: Vec<String>,
    /// The head they fail on (a commit, else the branch).
    pub head: String,
    /// The base branch.
    pub base: String,
    /// Those of `checks` that fail on the base branch's latest commit too.
    pub base_red: Vec<String>,
    /// The failing assertion or error, as CI printed it.
    pub excerpt: String,
}

impl RedCi {
    /// The failing checks that are this change's: not red on the base.
    pub fn own(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter(|c| !self.base_red.contains(c))
            .cloned()
            .collect()
    }

    /// Every failing check fails on the base branch too: red before the
    /// change.
    pub fn all_on_base(&self) -> bool {
        !self.checks.is_empty() && self.own().is_empty()
    }

    /// The reason in plain words: the checks, the head, the base.
    pub fn reason(&self) -> String {
        let names = if self.checks.is_empty() {
            "the repository's CI".to_string()
        } else {
            self.checks.join(", ")
        };
        let head = short_head(&self.head);
        let mut r = format!("CI is failing on {head}: {names}");
        let excerpt = self.excerpt.trim();
        if !excerpt.is_empty() {
            r.push_str(&format!(": {}", crate::truncate_str(excerpt, 240)));
        }
        let base = if self.base.trim().is_empty() {
            "the base branch"
        } else {
            self.base.trim()
        };
        if self.base_red.is_empty() {
            r.push_str(&format!(". {base} is green on these checks"));
        } else {
            r.push_str(&format!(
                ". {base} is red too on {}",
                self.base_red.join(", ")
            ));
        }
        r
    }
}

fn short_head(head: &str) -> String {
    let h = head.trim();
    if h.len() == 40 && h.chars().all(|c| c.is_ascii_hexdigit()) {
        format!("`{}`", &h[..7])
    } else if h.is_empty() {
        "the head".to_string()
    } else {
        format!("`{h}`")
    }
}

/// Why a run stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stop {
    /// Its own checks stay red after its fix rounds (`failed`).
    RedCi(RedCi),
    /// A pass raised an error CoderHelm gets past by itself, read by
    /// `run_recovery::classify` (`failed`, resumed by the self-heal sweep).
    Error { kind: String, plain: String },
    /// Its own review still asks for changes after every approach
    /// (`failed`).
    ReviewOpen { items: String },
    /// The run guard stopped it (its ceiling, a loop, a refused scope) with
    /// nothing to ship and nothing only a person has missing (`failed`).
    Guard { why: String },
    /// A person cancelled it (`cancelled`).
    CancelledBy { who: String },
    /// A person, or CoderHelm for a person's reason, closed it (`closed`).
    ClosedBy { who: String, why: String },
    /// Another run takes over (`retried`).
    Superseded { by: String },
    /// An ending was asked for without its evidence: what was missing
    /// (`failed`). Only [`gate`] writes it.
    Unproven { wanted: String, missing: String },
}

impl Stop {
    fn code(&self) -> String {
        match self {
            Stop::RedCi(_) => "red_ci".into(),
            Stop::Error { kind, .. } => format!("error:{kind}"),
            Stop::ReviewOpen { .. } => "review_open".into(),
            Stop::Guard { .. } => "guard".into(),
            Stop::CancelledBy { .. } => "cancelled".into(),
            Stop::ClosedBy { .. } => "closed".into(),
            Stop::Superseded { .. } => "superseded".into(),
            Stop::Unproven { .. } => "ending_unproven".into(),
        }
    }

    /// A person chose this stop: nothing CoderHelm tried is owed.
    fn by_person(&self) -> bool {
        matches!(
            self,
            Stop::CancelledBy { .. } | Stop::ClosedBy { .. } | Stop::Superseded { .. }
        )
    }
}

/// A stopped run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stopped {
    pub reason: Stop,
    pub tried: Tried,
}

/// A status that doesn't end the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Working {
    Pending,
    Queued,
    Running,
    AwaitingCi,
    AwaitingSetup,
    AwaitingChange,
}

impl Working {
    pub fn status(self) -> &'static str {
        match self {
            Working::Pending => st::PENDING,
            Working::Queued => st::QUEUED,
            Working::Running => st::RUNNING,
            Working::AwaitingCi => st::AWAITING_CI,
            Working::AwaitingSetup => st::AWAITING_SETUP,
            Working::AwaitingChange => st::AWAITING_CHANGE,
        }
    }

    /// The working status named `status`, if it is one.
    pub fn of(status: &str) -> Option<Working> {
        [
            Working::Pending,
            Working::Queued,
            Working::Running,
            Working::AwaitingCi,
            Working::AwaitingSetup,
            Working::AwaitingChange,
        ]
        .into_iter()
        .find(|w| w.status() == status)
    }
}

/// Whether `status` ends a run (it isn't working, waiting for CI, or
/// hidden).
pub fn is_ending(status: &str) -> bool {
    Working::of(status).is_none() && status != st::ARCHIVED
}

/// One status write, from [`gate`], [`working`] or [`restored`].
#[derive(Debug, Clone, PartialEq)]
pub struct Write {
    status: &'static str,
    sri: String,
    ending: Option<AttributeValue>,
}

impl Write {
    pub fn status(&self) -> &'static str {
        self.status
    }
    pub fn status_attr(&self) -> AttributeValue {
        AttributeValue::S(self.status.to_string())
    }
    /// The status-index key, `<status>#<run_id>`.
    pub fn sri(&self) -> &str {
        &self.sri
    }
    pub fn sri_attr(&self) -> AttributeValue {
        AttributeValue::S(self.sri.clone())
    }
    /// The ending to keep on the run ([`FIELD_ENDING`]); `None` for a
    /// working status or a restored one.
    pub fn ending(&self) -> Option<&AttributeValue> {
        self.ending.as_ref()
    }

    /// Adds this write to an update built without a client: `status_value`
    /// (the status placeholder the expression names) and `:sri` are set,
    /// and the ending goes in the SET clause as `#run_ending = :run_ending`.
    pub fn add_to(
        &self,
        status_value: &str,
        expression: &mut String,
        names: &mut HashMap<String, String>,
        values: &mut HashMap<String, AttributeValue>,
    ) {
        values.insert(status_value.to_string(), self.status_attr());
        values.insert(":sri".to_string(), self.sri_attr());
        if let Some(e) = &self.ending {
            *expression = with_ending(expression);
            names.insert("#run_ending".into(), FIELD_ENDING.into());
            values.insert(":run_ending".into(), e.clone());
        }
    }
}

/// `expression` with the ending set in its SET clause.
fn with_ending(expression: &str) -> String {
    let t = expression.trim_start();
    if t.contains("#run_ending = :run_ending") {
        return expression.to_string();
    }
    match t.strip_prefix("SET ") {
        Some(rest) => format!("SET #run_ending = :run_ending, {rest}"),
        None => format!("SET #run_ending = :run_ending {t}"),
    }
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn blank(v: &str) -> bool {
    v.trim().is_empty()
}

/// What a claimed ending lacks, if anything.
fn missing(end: &RunEnd) -> Option<(&'static str, &'static str)> {
    match end {
        RunEnd::Done(d) => match d {
            Done::Pr { number, url } | Done::Merged { number, url }
                if *number == 0 || blank(url) =>
            {
                Some(("done", "its pull request"))
            }
            Done::NoChange { why } if blank(why) => Some(("done", "why no change was needed")),
            Done::CoveredBy { url } if blank(url) => {
                Some(("done", "the pull request that covers it"))
            }
            Done::Answered { at } if blank(at) => Some(("done", "where its answer is")),
            _ => None,
        },
        RunEnd::Waiting(w) => {
            if blank(&w.evidence) {
                return Some(("waiting", "the evidence that only they can act"));
            }
            match &w.on {
                WaitOn::Person { who, action } | WaitOn::Permission { what: who, action }
                    if blank(who) || blank(action) =>
                {
                    Some(("waiting", "who it waits on and what they do"))
                }
                WaitOn::OutsideChange { what } if blank(what) => {
                    Some(("waiting", "the change it waits on"))
                }
                _ => None,
            }
        }
        RunEnd::Stopped(x) => match &x.reason {
            Stop::RedCi(r) if r.checks.is_empty() || blank(&r.head) => {
                Some(("stopped", "which checks fail and on which head"))
            }
            Stop::Error { plain, .. } if blank(plain) => Some(("stopped", "the error")),
            Stop::ReviewOpen { items } if blank(items) => {
                Some(("stopped", "what its review still asks for"))
            }
            Stop::Guard { why } if blank(why) => Some(("stopped", "why the guard stopped it")),
            Stop::CancelledBy { who } | Stop::ClosedBy { who, .. } if blank(who) => {
                Some(("stopped", "who stopped it"))
            }
            Stop::Superseded { by } if blank(by) => Some(("stopped", "the run that takes over")),
            _ => None,
        },
    }
}

fn status_of(end: &RunEnd) -> &'static str {
    match end {
        RunEnd::Done(Done::Pr { .. } | Done::Answered { .. }) => st::COMPLETED,
        RunEnd::Done(Done::Merged { .. }) => st::MERGED,
        RunEnd::Done(Done::NoChange { .. }) => st::NO_CHANGE,
        RunEnd::Done(Done::CoveredBy { .. }) => st::COVERED_BY_PR,
        RunEnd::Waiting(Waiting {
            on: WaitOn::OutsideChange { .. },
            ..
        }) => st::NEEDS_OUTSIDE_CHANGE,
        RunEnd::Waiting(_) => st::NEEDS_INPUT,
        RunEnd::Stopped(x) => match x.reason {
            Stop::CancelledBy { .. } => st::CANCELLED,
            Stop::ClosedBy { .. } => st::CLOSED,
            Stop::Superseded { .. } => st::RETRIED,
            Stop::RedCi(_)
            | Stop::Error { .. }
            | Stop::ReviewOpen { .. }
            | Stop::Guard { .. }
            | Stop::Unproven { .. } => st::FAILED,
        },
    }
}

/// The ending's kind, reason code and evidence.
fn described(end: &RunEnd) -> (&'static str, String, String) {
    match end {
        RunEnd::Done(d) => {
            let (code, evidence) = match d {
                Done::Pr { number, url } => ("pr", format!("pull request #{number} {url}")),
                Done::Merged { number, url } => ("merged", format!("pull request #{number} {url}")),
                Done::NoChange { why } => ("no_change", why.clone()),
                Done::CoveredBy { url } => ("covered_by_pr", url.clone()),
                Done::Answered { at } => ("answered", at.clone()),
            };
            ("done", code.to_string(), evidence)
        }
        RunEnd::Waiting(w) => {
            let (code, who) = match &w.on {
                WaitOn::Person { who, action } => ("person", format!("{who}: {action}")),
                WaitOn::Permission { what, action } => ("permission", format!("{what}: {action}")),
                WaitOn::OutsideChange { what } => ("outside_change", what.clone()),
            };
            (
                "waiting",
                code.to_string(),
                format!("{who}. {}", w.evidence.trim()),
            )
        }
        RunEnd::Stopped(x) => {
            let evidence = match &x.reason {
                Stop::RedCi(r) => r.reason(),
                Stop::Error { plain, .. } => plain.clone(),
                Stop::ReviewOpen { items } => items.clone(),
                Stop::Guard { why } => why.clone(),
                Stop::CancelledBy { who } => format!("cancelled by {who}"),
                Stop::ClosedBy { who, why } => format!("closed by {who}: {why}"),
                Stop::Superseded { by } => format!("run {by} takes over"),
                Stop::Unproven { wanted, missing } => {
                    format!("ended {wanted} without {missing}")
                }
            };
            ("stopped", x.reason.code(), evidence)
        }
    }
}

/// The write for an ending. See the module comment.
pub fn gate(run_id: &str, end: &RunEnd) -> Write {
    let refused;
    let end = match missing(end) {
        None => end,
        Some((wanted, what)) => {
            tracing::error!(
                run_id,
                ending = ?end,
                missing = what,
                "A run ending lacks its evidence: written as unproven"
            );
            refused = RunEnd::Stopped(Stopped {
                reason: Stop::Unproven {
                    wanted: wanted.into(),
                    missing: what.into(),
                },
                tried: Tried::default(),
            });
            &refused
        }
    };
    let status = status_of(end);
    let (kind, reason, evidence) = described(end);
    let mut m = HashMap::from([
        ("kind".to_string(), s(kind)),
        ("reason".to_string(), s(&reason)),
        ("status".to_string(), s(status)),
        (
            "evidence".to_string(),
            s(crate::truncate_str(evidence.trim(), 1_500)),
        ),
    ]);
    if let RunEnd::Stopped(x) = end {
        if !x.reason.by_person() {
            m.insert(
                "tried".into(),
                s(crate::truncate_str(&x.tried.line(), 1_000)),
            );
            m.insert(
                "retries".into(),
                AttributeValue::N(x.tried.retries.to_string()),
            );
        }
    }
    Write {
        status,
        sri: format!("{status}#{run_id}"),
        ending: Some(AttributeValue::M(m)),
    }
}

/// The write for a status that doesn't end the run.
pub fn working(w: Working, run_id: &str) -> Write {
    Write {
        status: w.status(),
        sri: format!("{}#{run_id}", w.status()),
        ending: None,
    }
}

/// The write that puts a run back in `status`, a status read from its own
/// record, when a claim on it is undone (a step that took it and couldn't
/// go on). The ending the record holds stays. `None` for a status that
/// isn't a run status.
pub fn restored(status: &str, run_id: &str) -> Option<Write> {
    let status = *st::ALL.iter().find(|x| **x == status)?;
    Some(Write {
        status,
        sri: format!("{status}#{run_id}"),
        ending: None,
    })
}

/// The status-index key for putting a run back in `status` ([`restored`]);
/// a status the list doesn't name is put back as the record had it.
pub fn restored_sri(status: &str, run_id: &str) -> AttributeValue {
    match restored(status, run_id) {
        Some(w) => w.sri_attr(),
        None => {
            tracing::warn!(
                run_id,
                status,
                "Putting back a status that isn't a run status"
            );
            s(&format!("{status}#{run_id}"))
        }
    }
}

/// A run's delivered ending on its pull request: merged when `status` says
/// so, else handed over ([`Done::Pr`]).
pub fn delivered(status: &str, number: u64, url: &str) -> RunEnd {
    if status == st::MERGED {
        RunEnd::Done(Done::Merged {
            number,
            url: url.to_string(),
        })
    } else {
        RunEnd::Done(Done::Pr {
            number,
            url: url.to_string(),
        })
    }
}

/// A pull request's address on GitHub.
pub fn pr_url(owner: &str, repo: &str, number: u64) -> String {
    format!("https://github.com/{owner}/{repo}/pull/{number}")
}

/// Another run takes over from this one (`retried`).
pub fn superseded(by: &str) -> RunEnd {
    RunEnd::Stopped(Stopped {
        reason: Stop::Superseded { by: by.to_string() },
        tried: Tried::default(),
    })
}

/// A run whose work is an answer, finished: where the answer is.
pub fn answered(at: &str) -> RunEnd {
    RunEnd::Done(Done::Answered { at: at.to_string() })
}

/// The write for a job run's row (a scheduled skill, a plugin) in `status`
/// (`queued`, `running`, `completed`, `failed`): finished with where its
/// answer is, stopped with its error, or still working.
pub fn for_job(status: &str, run_id: &str, answered_at: &str, error: &str) -> Write {
    if let Some(w) = Working::of(status) {
        return working(w, run_id);
    }
    if status == st::COMPLETED {
        return gate(run_id, &answered(answered_at));
    }
    gate(
        run_id,
        &RunEnd::Stopped(Stopped {
            reason: Stop::Error {
                kind: "job".into(),
                plain: if error.trim().is_empty() {
                    format!("the job ended {status}")
                } else {
                    error.to_string()
                },
            },
            tried: Tried::default(),
        }),
    )
}

/// Sets a [`Write`] on an update.
pub trait EndWrite {
    /// Sets `status_value` (the status placeholder the expression names) and
    /// `:sri`, and adds the ending to the SET clause. Call it after
    /// `update_expression`.
    fn run_status(self, status_value: &str, w: &Write) -> Self;
}

impl EndWrite for UpdateItemFluentBuilder {
    fn run_status(self, status_value: &str, w: &Write) -> Self {
        let b = self
            .expression_attribute_values(status_value, w.status_attr())
            .expression_attribute_values(":sri", w.sri_attr());
        match &w.ending {
            Some(e) => {
                let expr = b.get_update_expression().clone().unwrap_or_default();
                debug_assert!(
                    !expr.is_empty(),
                    "run_status is called after update_expression"
                );
                b.update_expression(with_ending(&expr))
                    .expression_attribute_names("#run_ending", FIELD_ENDING)
                    .expression_attribute_values(":run_ending", e.clone())
            }
            None => b,
        }
    }
}

/// Sets a [`Write`] on a new run record.
pub trait EndPut {
    /// Sets `status`, `status_run_id` and the ending.
    fn run_status(self, w: &Write) -> Self;
}

impl EndPut for PutItemFluentBuilder {
    fn run_status(self, w: &Write) -> Self {
        let b = self
            .item("status", w.status_attr())
            .item("status_run_id", w.sri_attr());
        match &w.ending {
            Some(e) => b.item(FIELD_ENDING, e.clone()),
            None => b,
        }
    }
}

/// The ending's fields for a record built as a map.
pub fn item_fields(w: &Write) -> Vec<(&'static str, AttributeValue)> {
    let mut v = vec![("status", w.status_attr()), ("status_run_id", w.sri_attr())];
    if let Some(e) = &w.ending {
        v.push((FIELD_ENDING, e.clone()));
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(w: &Write, k: &str) -> String {
        match w.ending() {
            Some(AttributeValue::M(m)) => match m.get(k) {
                Some(AttributeValue::S(v)) => v.clone(),
                Some(AttributeValue::N(v)) => v.clone(),
                _ => String::new(),
            },
            _ => String::new(),
        }
    }

    fn red(base_red: &[&str]) -> RedCi {
        RedCi {
            checks: vec!["Deploy Staging".into(), "Lint".into()],
            head: "0123456789abcdef0123456789abcdef01234567".into(),
            base: "main".into(),
            base_red: base_red.iter().map(|s| s.to_string()).collect(),
            excerpt: "expect(received).toEqual(expected)".into(),
        }
    }

    #[test]
    fn each_ending_writes_its_status_and_its_evidence() {
        let cases = [
            (
                RunEnd::Done(Done::Pr {
                    number: 12,
                    url: "https://github.com/harbor/web/pull/12".into(),
                }),
                st::COMPLETED,
                "pr",
            ),
            (
                RunEnd::Done(Done::NoChange {
                    why: "the handler already trims the input".into(),
                }),
                st::NO_CHANGE,
                "no_change",
            ),
            (
                RunEnd::Waiting(Waiting {
                    on: WaitOn::Permission {
                        what: "the team's usage limit".into(),
                        action: "Raise the limit in Settings".into(),
                    },
                    evidence: "Monthly cache read tokens limit reached".into(),
                }),
                st::NEEDS_INPUT,
                "permission",
            ),
            (
                RunEnd::Stopped(Stopped {
                    reason: Stop::RedCi(red(&[])),
                    tried: Tried {
                        rounds: 2,
                        ..Default::default()
                    },
                }),
                st::FAILED,
                "red_ci",
            ),
            (
                RunEnd::Stopped(Stopped {
                    reason: Stop::CancelledBy {
                        who: "a teammate".into(),
                    },
                    tried: Tried::default(),
                }),
                st::CANCELLED,
                "cancelled",
            ),
        ];
        for (end, status, reason) in cases {
            let w = gate("R1", &end);
            assert_eq!(w.status(), status, "{end:?}");
            assert_eq!(w.sri(), format!("{status}#R1"));
            assert_eq!(field(&w, "reason"), reason);
            assert_eq!(field(&w, "status"), status);
            assert!(!field(&w, "evidence").is_empty());
        }
    }

    /// An ending that lacks its evidence is never written as claimed.
    #[test]
    fn an_ending_without_its_evidence_is_written_unproven() {
        for end in [
            RunEnd::Done(Done::Pr {
                number: 0,
                url: String::new(),
            }),
            RunEnd::Done(Done::Answered { at: " ".into() }),
            RunEnd::Waiting(Waiting {
                on: WaitOn::Person {
                    who: "the reviewer".into(),
                    action: "Review pull request #3".into(),
                },
                evidence: String::new(),
            }),
            RunEnd::Waiting(Waiting {
                on: WaitOn::Person {
                    who: String::new(),
                    action: "Look".into(),
                },
                evidence: "x".into(),
            }),
            RunEnd::Stopped(Stopped {
                reason: Stop::RedCi(RedCi::default()),
                tried: Tried::default(),
            }),
            RunEnd::Stopped(Stopped {
                reason: Stop::Error {
                    kind: "transient".into(),
                    plain: String::new(),
                },
                tried: Tried::default(),
            }),
        ] {
            let w = gate("R1", &end);
            assert_eq!(w.status(), st::FAILED, "{end:?}");
            assert_eq!(field(&w, "reason"), "ending_unproven", "{end:?}");
        }
    }

    /// "Tried again" comes only from a recorded retry.
    #[test]
    fn tried_again_is_said_only_for_a_recorded_retry() {
        let none = Tried {
            rounds: 2,
            pushed: 0,
            ..Default::default()
        };
        assert!(!none.line().contains("tried again"), "{}", none.line());
        let once = Tried {
            retries: 1,
            ..Default::default()
        };
        assert!(once.line().contains("tried again 1 time"));
        assert_eq!(Tried::default().line(), "nothing before it stopped");
    }

    #[test]
    fn a_red_ci_reason_names_the_checks_the_head_and_the_base() {
        let r = red(&["Deploy Staging"]);
        let why = r.reason();
        assert!(why.contains("`0123456`"), "{why}");
        assert!(why.contains("Deploy Staging, Lint"), "{why}");
        assert!(why.contains("main is red too on Deploy Staging"), "{why}");
        assert_eq!(r.own(), vec!["Lint".to_string()]);
        assert!(!r.all_on_base());
        assert!(red(&["Deploy Staging", "Lint"]).all_on_base());
        assert!(red(&[]).reason().contains("main is green on these checks"));
    }

    #[test]
    fn the_ending_goes_in_the_set_clause() {
        let w = gate(
            "R1",
            &RunEnd::Done(Done::CoveredBy {
                url: "https://github.com/harbor/web/pull/9".into(),
            }),
        );
        let mut expr = "SET #s = :s, status_run_id = :sri REMOVE x".to_string();
        let (mut names, mut values) = (HashMap::new(), HashMap::new());
        w.add_to(":s", &mut expr, &mut names, &mut values);
        assert_eq!(
            expr,
            "SET #run_ending = :run_ending, #s = :s, status_run_id = :sri REMOVE x"
        );
        assert_eq!(names["#run_ending"], FIELD_ENDING);
        assert_eq!(values[":sri"], s("covered_by_pr#R1"));
        // Twice is once.
        assert_eq!(with_ending(&expr), expr);
        // A working status carries no ending.
        let w = working(Working::Running, "R1");
        assert_eq!(w.sri(), "running#R1");
        assert!(w.ending().is_none());
        assert!(restored("not a status", "R1").is_none());
        assert!(!is_ending(st::AWAITING_CI) && is_ending(st::COMPLETED));
    }

    fn sources() -> Vec<(std::path::PathBuf, String)> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .canonicalize()
            .unwrap();
        let mut out = Vec::new();
        let mut stack = vec![
            root.join("common/src"),
            root.join("worker/src"),
            root.join("gateway/src"),
        ];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().is_some_and(|x| x == "rs")
                    && !p.ends_with("common/src/run_ending.rs")
                {
                    out.push((p.clone(), std::fs::read_to_string(&p).unwrap_or_default()));
                }
            }
        }
        out
    }

    /// Where the block that opens just before `body` ends: the index in
    /// `body` after its closing brace. Braces in strings, chars and
    /// comments don't count.
    fn block_end(body: &str) -> usize {
        let b = body.as_bytes();
        let (mut i, mut depth) = (0usize, 1usize);
        while i < b.len() {
            match b[i] {
                b'/' if b.get(i + 1) == Some(&b'/') => {
                    while i < b.len() && b[i] != b'\n' {
                        i += 1;
                    }
                }
                b'/' if b.get(i + 1) == Some(&b'*') => {
                    i += 2;
                    while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                        i += 1;
                    }
                    i += 1;
                }
                b'r' if matches!(b.get(i + 1), Some(b'#') | Some(b'"'))
                    && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_')) =>
                {
                    let mut j = i + 1;
                    let mut hashes = 0;
                    while b.get(j) == Some(&b'#') {
                        hashes += 1;
                        j += 1;
                    }
                    if b.get(j) == Some(&b'"') {
                        j += 1;
                        loop {
                            if j >= b.len() {
                                break;
                            }
                            if b[j] == b'"'
                                && b[j + 1..]
                                    .iter()
                                    .take(hashes)
                                    .filter(|c| **c == b'#')
                                    .count()
                                    == hashes
                            {
                                j += 1 + hashes;
                                break;
                            }
                            j += 1;
                        }
                        i = j;
                        continue;
                    }
                }
                b'"' => {
                    i += 1;
                    while i < b.len() && b[i] != b'"' {
                        if b[i] == b'\\' {
                            i += 1;
                        }
                        i += 1;
                    }
                }
                b'\'' => {
                    // A char literal ('{', '\''), not a lifetime.
                    if b.get(i + 1) == Some(&b'\\') {
                        i += 2;
                        while i < b.len() && b[i] != b'\'' {
                            i += 1;
                        }
                    } else if b.get(i + 2) == Some(&b'\'') {
                        i += 2;
                    }
                }
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return i + 1;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        b.len()
    }

    /// The file without its test modules (`#[cfg(test)] mod x { .. }`,
    /// wherever they are: a file may hold several between its code).
    fn non_test(text: &str) -> String {
        let mut out = String::new();
        let mut rest = text;
        while let Some(i) = rest.find("#[cfg(test)]") {
            let after = &rest[i + "#[cfg(test)]".len()..];
            let t = after.trim_start();
            let opens = t.starts_with("mod ")
                && t.find('{')
                    .is_some_and(|b| t.find(';').is_none_or(|semi| b < semi));
            if !opens {
                out.push_str(&rest[..i + "#[cfg(test)]".len()]);
                rest = after;
                continue;
            }
            out.push_str(&rest[..i]);
            let body = &t[t.find('{').unwrap() + 1..];
            rest = &body[block_end(body)..];
        }
        out.push_str(rest);
        out
    }

    #[test]
    fn test_modules_are_cut_whatever_their_strings_hold() {
        let src = "fn a() {}\n#[cfg(test)]\nmod t {\n    const X: &str = \"{\\\"m\\\":1\";\n    // }\n    fn f() { let c = '{'; let r = r#\"}\"#; }\n}\nfn b() { \":sri\" }\n";
        let kept = non_test(src);
        assert!(kept.contains("fn a()") && kept.contains("fn b()"), "{kept}");
        assert!(!kept.contains("const X"), "{kept}");
    }

    /// Every call site that sets a run's status goes through this gate. A
    /// status write binds the status-index key (`:sri` in an update,
    /// `status_run_id` in a new record): each one, in every service, must
    /// take its value from this module, so an ending without a typed reason
    /// can't be written.
    #[test]
    fn every_status_write_goes_through_the_gate() {
        let mut bad = Vec::new();
        let mut seen = 0;
        for (path, text) in sources() {
            let code = non_test(&text);
            let code = code.as_str();
            let lines: Vec<&str> = code.lines().collect();
            for (i, line) in lines.iter().enumerate() {
                let t = line.trim_start();
                if t.starts_with("//") {
                    continue;
                }
                let binds = t.contains("\":sri\"") || t.contains("\"status_run_id\"");
                if !binds {
                    continue;
                }
                seen += 1;
                let window = lines[i..(i + 3).min(lines.len())].join(" ");
                if !window.contains("run_ending::") && !window.contains(".run_status(") {
                    bad.push(format!("{}:{}: {}", path.display(), i + 1, t));
                }
            }
            // A status-index key built by hand.
            let squashed: String = code.split_whitespace().collect();
            for status in st::ALL {
                for needle in [
                    format!("format!(\"{status}#{{run_id}}\")"),
                    format!("format!(\"{status}#{{}}\",run_id)"),
                    format!("format!(\"{status}#{{}}\",msg.run_id)"),
                    format!("format!(\"{status}#{{}}\",r.run_id)"),
                ] {
                    if squashed.contains(&needle) {
                        bad.push(format!("{}: builds `{status}#` by hand", path.display()));
                    }
                }
            }
            for needle in ["format!(\"{status}#{run_id}\")", "format!(\"{}#{run_id}\","] {
                if squashed.contains(needle) {
                    bad.push(format!("{}: builds a status key by hand", path.display()));
                }
            }
        }
        assert!(seen > 0, "the walk found no status writes at all");
        assert!(
            bad.is_empty(),
            "set a run's status with common::run_ending (gate, working, restored):\n{}",
            bad.join("\n")
        );
    }

    /// The stuck-run sweep is Python and can't call the gate: each status
    /// write it makes keeps an ending in the gate's shape all the same.
    #[test]
    fn the_python_sweep_writes_an_ending_with_every_status() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap();
        let py = std::fs::read_to_string(root.join("lambda/stuck-run-cleanup/handler.py")).unwrap();
        assert!(py.contains(&format!("ENDING_FIELD = \"{FIELD_ENDING}\"")));
        let writes: Vec<&str> = py.split("status_run_id = :sri").skip(1).collect();
        assert!(!writes.is_empty());
        for w in writes {
            assert!(
                w.trim_start().starts_with(", run_ending = :run_ending"),
                "a sweep status write without its ending: {}",
                &w[..w.len().min(80)]
            );
        }
    }

    /// `.run_status(` is this module's: no other type may take the name and
    /// so slip a write past the walk above.
    #[test]
    fn only_the_gate_defines_run_status() {
        let bad: Vec<String> = sources()
            .into_iter()
            .filter(|(_, t)| t.contains("fn run_status("))
            .map(|(p, _)| p.display().to_string())
            .collect();
        assert!(bad.is_empty(), "{bad:?}");
    }
}
