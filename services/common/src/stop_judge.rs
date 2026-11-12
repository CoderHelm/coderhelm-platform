//! The stop judge: the one place a costly decision is made.
//!
//! A costly decision is one that spends a person's attention or holds the
//! work up: asking a person, stopping a run or making it wait, blocking a
//! pull request (or keeping "changes requested"), starting another fix cycle
//! beyond the first, and handing a run off. Each used to be decided by the
//! narrow step that hit it, and each picked the costly outcome when unsure.
//! Now every one is put to this judge, with the whole picture: the ticket and
//! its acceptance criteria, related tickets and their pull requests, the plan,
//! the diff, CI and plan results, what the proof checks and rules found (as
//! evidence, not as verdicts), the review threads, the run's budget, and the
//! decision the step proposes with its reason.
//!
//! The judge answers one of three ways ([`Verdict`]):
//!
//! - **Proceed**: the work moves on, on a written assumption that becomes an
//!   open item in the pull request description. For a proposed extra cycle it
//!   means "deliver what exists"; for a proposed block it means "approve, with
//!   notes".
//! - **Ask**: only for a fact a person alone has (a product choice, access or
//!   credentials, or readings of the ticket that lead to different code). The
//!   question carries what was tried, 2 to 4 options, a recommended default
//!   and a deadline after which reversible work goes ahead with the default.
//! - **Hold**: only with a named blocker that cites checked evidence from the
//!   picture. For a proposed extra cycle it means "this blocker is worth
//!   another cycle"; for a proposed block it means "request changes", which
//!   further needs a correctness, security or data-loss problem with a
//!   location.
//!
//! One call on the strongest configured model at high effort, structured
//! output ([`judge`]). An answer that breaks the rules above is asked again
//! once, with the schema and what was wrong; a second bad answer falls back
//! deterministically ([`fallback`]): proceed for reversible work, never to
//! asking a person.
//!
//! The only way to get a [`Judged`] is from [`judge`], and the code that asks
//! a person, sets `needs_input`, requests changes or starts another loop takes
//! one, so none of it can run without the judge. A source test in the worker
//! (`passes::stop_judge`) fails when a path does it any other way.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The costly decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// Ask a person a question and wait for the answer.
    Ask,
    /// Stop the run, or make it wait on something.
    Stop,
    /// Post "changes requested" on a pull request, or keep it standing.
    Block,
    /// Start another fix cycle beyond the first.
    ExtraCycle,
    /// Hand the run to a person.
    Handoff,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Ask => "ask",
            Kind::Stop => "stop",
            Kind::Block => "block",
            Kind::ExtraCycle => "extra_cycle",
            Kind::Handoff => "handoff",
        }
    }

    /// Analytics groups decisions as stops (a run waited or a person was
    /// asked) or blocks (a pull request was held).
    pub fn group(self) -> Group {
        match self {
            Kind::Block => Group::Block,
            _ => Group::Stop,
        }
    }

    /// The judge may ask a person for this decision. A review never asks:
    /// what it wants to know is a note on the approval.
    fn may_ask(self) -> bool {
        !matches!(self, Kind::Block)
    }
}

/// How analytics groups a decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Group {
    Stop,
    Block,
}

/// The facts only a person has. Anything else, the run finds out or assumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersonOnly {
    /// A product choice: what the thing should do, when the ticket doesn't say
    /// and the options differ for the people using it.
    ProductChoice,
    /// Access or credentials the run doesn't have.
    Access,
    /// Readings of the ticket that lead to different code.
    DivergentReadings,
}

impl PersonOnly {
    fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "product_choice" => Some(Self::ProductChoice),
            "access" | "access_or_credentials" | "credentials" => Some(Self::Access),
            "divergent_readings" => Some(Self::DivergentReadings),
            _ => None,
        }
    }
}

/// What a request for changes may rest on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockCategory {
    Correctness,
    Security,
    DataLoss,
}

impl BlockCategory {
    pub fn parse(s: &str) -> Option<Self> {
        match s
            .trim()
            .to_ascii_lowercase()
            .replace(['-', ' '], "_")
            .as_str()
        {
            "correctness" | "bug" | "defect" => Some(Self::Correctness),
            "security" => Some(Self::Security),
            "data_loss" | "data" | "dataloss" => Some(Self::DataLoss),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Correctness => "correctness",
            Self::Security => "security",
            Self::DataLoss => "data_loss",
        }
    }
}

// ── The picture ────────────────────────────────────────────────────────────

/// A ticket related to this one (a sibling, the parent, a duplicate) and
/// the pull request that does its work, if any.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Related {
    pub ticket: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pr: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pr_state: String,
}

/// One piece of evidence: what a check, a rule, CI or a person's thread
/// found. Each has an id the judge cites.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Evidence {
    /// `E1`, `E2`, ... (assigned by [`Picture::with_ids`] when empty).
    #[serde(default)]
    pub id: String,
    /// Where it came from: `proof`, `rules`, `ci`, `tests`, `review`, ...
    pub source: String,
    /// What it says, in plain words.
    pub finding: String,
    /// `path:line`, a check name or a lookup, when it has one.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub location: String,
    /// Confirmed by running something or reading the code, not inferred.
    #[serde(default)]
    pub checked: bool,
}

/// One review or comment thread and where it stands.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Thread {
    /// `T1`, `T2`, ... (assigned by [`Picture::with_ids`] when empty).
    #[serde(default)]
    pub id: String,
    pub summary: String,
    /// Still open (not resolved, not answered).
    #[serde(default)]
    pub open: bool,
    /// The last word in it is a person's.
    #[serde(default)]
    pub person_last: bool,
}

/// What the run has spent and may spend.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Budget {
    #[serde(default)]
    pub spend_usd: f64,
    #[serde(default)]
    pub spend_cap_usd: f64,
    #[serde(default)]
    pub continuations: u32,
    #[serde(default)]
    pub continuation_cap: u32,
    /// Fix cycles of the step asking, so far.
    #[serde(default)]
    pub cycles: u32,
    #[serde(default)]
    pub cycle_cap: u32,
}

impl Budget {
    /// No room for another cycle of the step asking.
    pub fn cycles_spent(&self) -> bool {
        self.cycle_cap > 0 && self.cycles >= self.cycle_cap
    }

    fn line(&self) -> String {
        format!(
            "spent ${:.2} of ${:.2}; continuations {} of {}; fix cycles of this step {} of {}",
            self.spend_usd,
            self.spend_cap_usd,
            self.continuations,
            self.continuation_cap,
            self.cycles,
            self.cycle_cap
        )
    }
}

/// The question a step would ask, if the judge lets it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProposedQuestion {
    pub question: String,
    #[serde(default)]
    pub tried: Vec<String>,
    #[serde(default)]
    pub options: Vec<String>,
}

/// Everything the judge sees.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Picture {
    /// The decision proposed.
    pub kind: Option<Kind>,
    /// The step that proposes it (`plan`, `fix_location`, `run_guard`,
    /// `review`, `own_review`, ...).
    pub step: String,
    /// Why the step proposes it.
    pub reason: String,
    /// Undoing what proceeding does costs nothing more than a commit or a
    /// comment (a branch, a draft, a review). Nearly everything is.
    pub reversible: bool,
    pub ticket_title: String,
    pub ticket_body: String,
    #[serde(default)]
    pub acceptance: Vec<String>,
    #[serde(default)]
    pub related: Vec<Related>,
    #[serde(default)]
    pub plan: String,
    #[serde(default)]
    pub diff_summary: String,
    /// CI and plan results, one line each.
    #[serde(default)]
    pub results: Vec<String>,
    #[serde(default)]
    pub evidence: Vec<Evidence>,
    #[serde(default)]
    pub threads: Vec<Thread>,
    #[serde(default)]
    pub budget: Budget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub question: Option<ProposedQuestion>,
    /// Set when going on is impossible, not a judgment: the repository isn't
    /// connected, a person ended the run, the run's hard ceiling is spent.
    /// The decision is still recorded here, without a model call.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub cannot_proceed: String,
    /// Earlier decisions of this kind on this team that people's acts
    /// showed wrong (a question answered with its default, a block dismissed
    /// or merged over), one line each: what the judge learns from the outcome
    /// log (`decision_log::overturned_lines`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub overturned: Vec<String>,
    /// What the repository's memories say that bears on this decision (how
    /// the team works, past fixes, corrections), one line each, chosen by
    /// relevance to it. Context for the judge, never evidence on their own.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub memories: Vec<String>,
}

/// Longest text of one field in the prompt and the stored record.
const MAX_FIELD: usize = 6_000;
const MAX_ITEMS: usize = 40;
/// Most overturned decisions shown to the judge.
pub const MAX_OVERTURNED: usize = 5;
/// Most repository memories shown to the judge.
pub const MAX_MEMORIES: usize = 5;

impl Picture {
    pub fn new(kind: Kind, step: &str, reason: &str) -> Self {
        Self {
            kind: Some(kind),
            step: step.to_string(),
            reason: reason.to_string(),
            reversible: true,
            ..Default::default()
        }
    }

    /// Ids on every evidence item and thread that has none, and the lists
    /// bounded.
    pub fn with_ids(mut self) -> Self {
        self.evidence.truncate(MAX_ITEMS);
        self.threads.truncate(MAX_ITEMS);
        self.related.truncate(MAX_ITEMS);
        self.results.truncate(MAX_ITEMS);
        self.overturned.truncate(MAX_OVERTURNED);
        self.memories.truncate(MAX_MEMORIES);
        for (i, e) in self.evidence.iter_mut().enumerate() {
            if e.id.trim().is_empty() {
                e.id = format!("E{}", i + 1);
            }
        }
        for (i, t) in self.threads.iter_mut().enumerate() {
            if t.id.trim().is_empty() {
                t.id = format!("T{}", i + 1);
            }
        }
        self
    }

    /// The decision proposed (a stop when unset).
    pub fn kind(&self) -> Kind {
        self.kind.unwrap_or(Kind::Stop)
    }

    /// Evidence or a thread by the id the judge cites.
    fn cited(&self, id: &str) -> Option<Cited<'_>> {
        let id = id.trim().trim_matches(|c| c == '[' || c == ']');
        if let Some(e) = self.evidence.iter().find(|e| e.id.eq_ignore_ascii_case(id)) {
            return Some(Cited::Evidence(e));
        }
        self.threads
            .iter()
            .find(|t| t.id.eq_ignore_ascii_case(id))
            .map(Cited::Thread)
    }
}

enum Cited<'a> {
    Evidence(&'a Evidence),
    Thread(&'a Thread),
}

impl Cited<'_> {
    /// Evidence counts when it was checked; a thread when a person's
    /// words in it are still open.
    fn checked(&self) -> bool {
        match self {
            Cited::Evidence(e) => e.checked,
            Cited::Thread(t) => t.open && t.person_last,
        }
    }
}

fn clip(s: &str) -> String {
    crate::head_tail_str(s.trim(), MAX_FIELD)
}

// ── The answer ─────────────────────────────────────────────────────────────

/// A question the judge lets the run ask.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Question {
    pub question: String,
    pub person_only: PersonOnly,
    /// What the run already tried or read.
    pub tried: Vec<String>,
    /// 2 to 4 options.
    pub options: Vec<String>,
    /// One of `options`.
    pub default: String,
    /// After this many hours reversible work goes ahead with the default.
    pub deadline_hours: u32,
}

/// A named blocker, on checked evidence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hold {
    pub blocker: String,
    /// Ids from the picture (`E1`, `T2`), each checked.
    pub evidence: Vec<String>,
    /// For a block: what kind of problem, and where.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<BlockCategory>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub location: String,
}

/// The judge's answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum Verdict {
    Proceed { assumption: String },
    Ask(Question),
    Hold(Hold),
}

impl Verdict {
    pub fn as_str(&self) -> &'static str {
        match self {
            Verdict::Proceed { .. } => "proceed",
            Verdict::Ask(_) => "ask",
            Verdict::Hold(_) => "hold",
        }
    }
}

/// Where the answer came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// The model's first answer.
    Model,
    /// The model's second answer, after the first broke a rule.
    ModelReasked,
    /// A rule settled it before any model call (no budget for another cycle).
    Rule(String),
    /// Two bad answers (or no answer): the deterministic fallback.
    Fallback(String),
}

/// A judged decision. Only [`judge`] makes one.
#[derive(Debug, Clone, PartialEq)]
pub struct Judged {
    kind: Kind,
    step: String,
    verdict: Verdict,
    source: Source,
    /// The model's answers, as they came (for the record and replay).
    replies: Vec<Value>,
    _sealed: (),
}

impl Judged {
    pub fn kind(&self) -> Kind {
        self.kind
    }
    pub fn step(&self) -> &str {
        &self.step
    }
    pub fn verdict(&self) -> &Verdict {
        &self.verdict
    }
    pub fn source(&self) -> &Source {
        &self.source
    }
    pub fn replies(&self) -> &[Value] {
        &self.replies
    }

    /// The work moves on.
    pub fn proceeds(&self) -> bool {
        matches!(self.verdict, Verdict::Proceed { .. })
    }

    pub fn question(&self) -> Option<&Question> {
        match &self.verdict {
            Verdict::Ask(q) => Some(q),
            _ => None,
        }
    }

    pub fn hold(&self) -> Option<&Hold> {
        match &self.verdict {
            Verdict::Hold(h) => Some(h),
            _ => None,
        }
    }

    /// The assumption a proceed rests on.
    pub fn assumption(&self) -> Option<&str> {
        match &self.verdict {
            Verdict::Proceed { assumption } => Some(assumption),
            _ => None,
        }
    }

    /// The line the pull request description lists under Open items for a
    /// proceed: the assumption, so a person sees what was decided for them.
    pub fn open_item(&self) -> Option<String> {
        self.assumption()
            .map(|a| format!("Assumed ({}): {}", self.step_words(), a.trim()))
    }

    fn step_words(&self) -> String {
        self.step.replace('_', " ")
    }

    /// A question as a person reads it: what was tried, the options with the
    /// default marked, and when the default is taken.
    pub fn question_markdown(&self) -> Option<String> {
        let q = self.question()?;
        let mut s = format!("**{}**\n\n", q.question.trim());
        if !q.tried.is_empty() {
            s.push_str("What I checked first:\n");
            for t in &q.tried {
                s.push_str(&format!("- {}\n", t.trim()));
            }
            s.push('\n');
        }
        s.push_str("Options:\n");
        for (i, o) in q.options.iter().enumerate() {
            let mark = if o == &q.default {
                " (recommended)"
            } else {
                ""
            };
            s.push_str(&format!("{}. {}{}\n", i + 1, o.trim(), mark));
        }
        s.push_str(&format!(
            "\nIf there is no answer within {} hours, I go ahead with the recommended option.",
            q.deadline_hours
        ));
        Some(s)
    }

    /// The question as the run's question list stores it.
    pub fn run_question(&self) -> Option<crate::run_questions::Question> {
        let q = self.question()?;
        Some(crate::run_questions::Question {
            id: "q1".into(),
            question: q.question.clone(),
            why: format!(
                "Default if unanswered in {} hours: {}",
                q.deadline_hours, q.default
            ),
            suggestions: q.options.clone(),
            checked: q.tried.clone(),
            ..Default::default()
        })
    }

    /// What the run page and the notification say for a hold.
    pub fn hold_words(&self) -> Option<String> {
        let h = self.hold()?;
        let mut s = h.blocker.trim().to_string();
        if !h.location.is_empty() {
            s.push_str(&format!(" ({})", h.location.trim()));
        }
        Some(s)
    }
}

// ── The model call ─────────────────────────────────────────────────────────

/// What the judge asks a model. The worker's implementation calls the
/// strongest configured model at high effort; the replay suite plays back
/// recorded answers.
#[allow(async_fn_in_trait)]
pub trait JudgeModel {
    /// One structured answer, or why there is none.
    async fn answer(&self, system: &str, prompt: &str, schema: &Value) -> Result<Value, String>;
}

/// The effort every judgment step runs at: high, not max.
pub const JUDGMENT_EFFORT: &str = "high";

/// The longest a person is given before reversible work takes the default.
pub const MAX_DEADLINE_HOURS: u32 = 168;
/// The deadline when the model gives none.
pub const DEFAULT_DEADLINE_HOURS: u32 = 24;

pub const SYSTEM: &str = "You are CoderHelm's stop judge. A step of an automated coding run \
proposes a costly decision: asking a person, stopping or waiting, blocking a pull request, \
starting another fix cycle, or handing the run off. You see the whole run and decide.\n\n\
Default to proceed. Decide the most reasonable assumption, proceed with it, and state it in one \
sentence: it is listed as an open item on the pull request, where a person can correct it. \
Uncertainty alone is never a reason to stop. A parse failure, a missing field, a rule that \
matched, or a check that reports something are evidence to weigh, not verdicts.\n\n\
Ask only for a fact only a person has: a product choice the ticket leaves open, access or \
credentials the run lacks, or readings of the ticket that lead to different code. A question \
must say what was tried, give 2 to 4 options and recommend one; reversible work goes ahead \
with the recommendation after the deadline.\n\n\
Hold only with a named blocker that cites checked evidence ids from the picture (E1, T2). For a \
proposed block (request changes) a hold also needs a correctness, security or data-loss problem \
and its location; style, naming, suggestions and unconfirmed concerns are notes on an approval. \
For a proposed extra fix cycle, hold means the named blocker is worth one more cycle; proceed \
means deliver what exists with open items. For a run that reached its budget, proceed means \
deliver the draft with open items; hold means a person must act on the named blocker.";

/// The structured answer's schema.
pub fn schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["verdict", "assumption", "question", "person_only", "tried", "options",
                     "default", "deadline_hours", "blocker", "evidence", "category", "location"],
        "properties": {
            "verdict": { "type": "string", "enum": ["proceed", "ask", "hold"] },
            "assumption": { "type": "string" },
            "question": { "type": "string" },
            "person_only": { "type": "string",
                             "enum": ["", "product_choice", "access", "divergent_readings"] },
            "tried": { "type": "array", "items": { "type": "string" } },
            "options": { "type": "array", "items": { "type": "string" } },
            "default": { "type": "string" },
            "deadline_hours": { "type": "integer" },
            "blocker": { "type": "string" },
            "evidence": { "type": "array", "items": { "type": "string" } },
            "category": { "type": "string",
                          "enum": ["", "correctness", "security", "data_loss", "other"] },
            "location": { "type": "string" }
        }
    })
}

/// The picture, as the judge reads it.
pub fn prompt(p: &Picture) -> String {
    let mut s = String::new();
    let kind = p.kind();
    s.push_str(&format!(
        "## Proposed decision\n{} by the {} step: {}\nReversible: {}\n\n",
        kind.as_str(),
        p.step,
        clip(&p.reason),
        if p.reversible { "yes" } else { "no" }
    ));
    if let Some(q) = &p.question {
        s.push_str(&format!(
            "## The question it would ask\n{}\n",
            clip(&q.question)
        ));
        for t in &q.tried {
            s.push_str(&format!("- tried: {}\n", clip(t)));
        }
        for o in &q.options {
            s.push_str(&format!("- option: {}\n", clip(o)));
        }
        s.push('\n');
    }
    s.push_str(&format!(
        "## Ticket\n{}\n\n{}\n\n",
        clip(&p.ticket_title),
        clip(&p.ticket_body)
    ));
    if !p.acceptance.is_empty() {
        s.push_str("## Acceptance criteria\n");
        for a in &p.acceptance {
            s.push_str(&format!("- {}\n", clip(a)));
        }
        s.push('\n');
    }
    if !p.related.is_empty() {
        s.push_str("## Related tickets\n");
        for r in &p.related {
            let pr = if r.pr.is_empty() {
                String::new()
            } else {
                format!(" (PR {} {})", r.pr, r.pr_state)
            };
            s.push_str(&format!("- {}: {}{}\n", r.ticket, clip(&r.summary), pr));
        }
        s.push('\n');
    }
    for (head, body) in [("Plan", &p.plan), ("Diff summary", &p.diff_summary)] {
        if !body.trim().is_empty() {
            s.push_str(&format!("## {head}\n{}\n\n", clip(body)));
        }
    }
    if !p.results.is_empty() {
        s.push_str("## CI and plan results\n");
        for r in &p.results {
            s.push_str(&format!("- {}\n", clip(r)));
        }
        s.push('\n');
    }
    if !p.evidence.is_empty() {
        s.push_str("## Evidence (cite by id)\n");
        for e in &p.evidence {
            s.push_str(&format!(
                "- [{}] {} ({}{}): {}\n",
                e.id,
                e.source,
                if e.checked { "checked" } else { "not checked" },
                if e.location.is_empty() {
                    String::new()
                } else {
                    format!(", {}", e.location)
                },
                clip(&e.finding)
            ));
        }
        s.push('\n');
    }
    if !p.threads.is_empty() {
        s.push_str("## Threads (cite by id)\n");
        for t in &p.threads {
            s.push_str(&format!(
                "- [{}] {}{}: {}\n",
                t.id,
                if t.open { "open" } else { "settled" },
                if t.person_last {
                    ", a person spoke last"
                } else {
                    ""
                },
                clip(&t.summary)
            ));
        }
        s.push('\n');
    }
    if !p.overturned.is_empty() {
        s.push_str(
            "## Earlier decisions like this one that people overturned\nWhat people did after \
             them shows they weren't needed. Don't repeat one unless this case differs in a way \
             you can name.\n",
        );
        for o in &p.overturned {
            s.push_str(&format!("- {}\n", clip(o)));
        }
        s.push('\n');
    }
    if !p.memories.is_empty() {
        s.push_str(
            "## What this repository's memories say\nLearned on earlier work here or written by \
             the team. Context, not evidence: a memory never makes a stop or a question needed \
             on its own, and the code and results above win where they disagree.\n",
        );
        for m in &p.memories {
            s.push_str(&format!("- {}\n", clip(m)));
        }
        s.push('\n');
    }
    s.push_str(&format!("## Budget\n{}\n", p.budget.line()));
    s
}

/// Reads and checks an answer against the rules. `Err` says what was wrong,
/// in words the re-ask passes on.
pub fn validate(p: &Picture, v: &Value) -> Result<Verdict, String> {
    let s = |k: &str| v[k].as_str().unwrap_or("").trim().to_string();
    let list = |k: &str| -> Vec<String> {
        v[k].as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str())
                    .map(|x| x.trim().to_string())
                    .filter(|x| !x.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    };
    let kind = p.kind();
    match s("verdict").as_str() {
        "proceed" => {
            let assumption = s("assumption");
            if assumption.is_empty() {
                return Err("a proceed must state its assumption in `assumption`".into());
            }
            Ok(Verdict::Proceed { assumption })
        }
        "ask" => {
            if !kind.may_ask() {
                return Err(
                    "a review never asks a person: proceed (approve with notes) or hold".into(),
                );
            }
            let question = s("question");
            if question.is_empty() {
                return Err("an ask needs `question`".into());
            }
            let Some(person_only) = PersonOnly::parse(&s("person_only")) else {
                return Err(
                    "an ask must name what only a person has in `person_only`: product_choice, \
                     access or divergent_readings; anything else, proceed on an assumption"
                        .into(),
                );
            };
            let tried = list("tried");
            if tried.is_empty() {
                return Err("an ask must list what was tried in `tried`".into());
            }
            let options = list("options");
            if !(2..=4).contains(&options.len()) {
                return Err(format!(
                    "an ask needs 2 to 4 `options`, not {}",
                    options.len()
                ));
            }
            let default = s("default");
            let Some(default) = options
                .iter()
                .find(|o| o.eq_ignore_ascii_case(&default))
                .cloned()
            else {
                return Err("`default` must be one of `options`, word for word".into());
            };
            let hours = v["deadline_hours"].as_u64().unwrap_or(0) as u32;
            let deadline_hours = if hours == 0 {
                DEFAULT_DEADLINE_HOURS
            } else {
                hours.min(MAX_DEADLINE_HOURS)
            };
            Ok(Verdict::Ask(Question {
                question,
                person_only,
                tried,
                options,
                default,
                deadline_hours,
            }))
        }
        "hold" => {
            let blocker = s("blocker");
            if blocker.is_empty() {
                return Err("a hold must name its blocker in `blocker`".into());
            }
            let evidence = list("evidence");
            if evidence.is_empty() {
                return Err("a hold must cite evidence ids from the picture in `evidence`".into());
            }
            for id in &evidence {
                match p.cited(id) {
                    None => return Err(format!("`{id}` is not an evidence or thread id")),
                    Some(c) if !c.checked() => {
                        return Err(format!(
                            "`{id}` was not checked; a hold rests only on checked evidence or a \
                             person's open thread"
                        ))
                    }
                    Some(_) => {}
                }
            }
            let category = BlockCategory::parse(&s("category"));
            let location = s("location");
            if kind == Kind::Block {
                if category.is_none() {
                    return Err(
                        "changes are requested only for a correctness, security or data-loss \
                         problem; anything else is a note on an approval (proceed)"
                            .into(),
                    );
                }
                if location.is_empty() {
                    return Err("a request for changes must give the problem's location".into());
                }
            }
            Ok(Verdict::Hold(Hold {
                blocker,
                evidence,
                category,
                location,
            }))
        }
        other => Err(format!(
            "`verdict` must be proceed, ask or hold, not `{other}`"
        )),
    }
}

/// The answer when the model gave none that keeps the rules. Never an ask:
/// reversible work proceeds; work that can't be undone holds on the step's
/// own reason.
pub fn fallback(p: &Picture, why: &str) -> Verdict {
    if p.reversible {
        Verdict::Proceed {
            assumption: format!(
                "the stop judge had no usable answer ({}), so the work went ahead as planned \
                 instead of stopping on: {}",
                crate::truncate_str(why.trim(), 200),
                crate::truncate_str(p.reason.trim(), 300)
            ),
        }
    } else {
        Verdict::Hold(Hold {
            blocker: format!(
                "{} (the step can't be undone and the stop judge had no usable answer)",
                crate::truncate_str(p.reason.trim(), 300)
            ),
            evidence: Vec::new(),
            category: None,
            location: String::new(),
        })
    }
}

/// A rule that settles the decision before any model call, if one does:
/// going on is impossible (no access the run could get, a hard ceiling), or
/// another cycle with no cycles left (the run's hard limits are not the
/// judge's to lift).
fn settled_by_rule(p: &Picture) -> Option<(Verdict, String)> {
    if !p.cannot_proceed.trim().is_empty() {
        return Some((
            Verdict::Hold(Hold {
                blocker: p.cannot_proceed.trim().to_string(),
                evidence: Vec::new(),
                category: None,
                location: String::new(),
            }),
            "cannot proceed".into(),
        ));
    }
    if p.kind() == Kind::ExtraCycle && p.budget.cycles_spent() {
        return Some((
            Verdict::Proceed {
                assumption: format!(
                    "the {} step used its {} fix cycles; the work is delivered with what is \
                     still open listed",
                    p.step.replace('_', " "),
                    p.budget.cycle_cap
                ),
            },
            "cycle budget".into(),
        ));
    }
    None
}

/// The decision. One model call; a rule-breaking answer is asked again once
/// with what was wrong; then the deterministic fallback.
pub async fn judge<M: JudgeModel>(model: &M, picture: Picture) -> Judged {
    let picture = picture.with_ids();
    let kind = picture.kind();
    let step = picture.step.clone();
    let make = |verdict, source, replies| Judged {
        kind,
        step: step.clone(),
        verdict,
        source,
        replies,
        _sealed: (),
    };
    if let Some((verdict, rule)) = settled_by_rule(&picture) {
        return make(verdict, Source::Rule(rule), Vec::new());
    }
    let schema = schema();
    let first_prompt = prompt(&picture);
    let mut replies = Vec::new();
    let first = model.answer(SYSTEM, &first_prompt, &schema).await;
    let problem = match first {
        Ok(v) => {
            let checked = validate(&picture, &v);
            replies.push(v);
            match checked {
                Ok(verdict) => return make(verdict, Source::Model, replies),
                Err(e) => e,
            }
        }
        Err(e) => format!("no answer: {e}"),
    };
    let again = format!(
        "{first_prompt}\n## Your earlier answer broke a rule\n{problem}\n\nAnswer again with \
         one JSON object matching this schema exactly:\n{schema}"
    );
    match model.answer(SYSTEM, &again, &schema).await {
        Ok(v) => {
            let checked = validate(&picture, &v);
            replies.push(v);
            match checked {
                Ok(verdict) => make(verdict, Source::ModelReasked, replies),
                Err(e) => make(
                    fallback(&picture, &e),
                    Source::Fallback(format!("{problem}; then {e}")),
                    replies,
                ),
            }
        }
        Err(e) => make(
            fallback(&picture, &e),
            Source::Fallback(format!("{problem}; then no answer: {e}")),
            replies,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Plays back answers in order; `Err` once they run out.
    struct Recorded(Mutex<Vec<Result<Value, String>>>);

    impl Recorded {
        fn new(v: Vec<Result<Value, String>>) -> Self {
            Self(Mutex::new(v.into_iter().rev().collect()))
        }
    }

    impl JudgeModel for Recorded {
        async fn answer(&self, _: &str, _: &str, _: &Value) -> Result<Value, String> {
            self.0
                .lock()
                .unwrap()
                .pop()
                .unwrap_or_else(|| Err("no more".into()))
        }
    }

    fn block(blocker: &str, evidence: &[&str], category: &str, location: &str) -> Value {
        json!({"verdict":"hold","assumption":"","question":"","person_only":"","tried":[],
               "options":[],"default":"","deadline_hours":0,"blocker":blocker,
               "evidence":evidence,"category":category,"location":location})
    }

    #[test]
    fn overturned_decisions_reach_the_prompt_bounded() {
        let mut p = picture(Kind::Block);
        assert!(!prompt(&p).contains("people overturned"));
        p.overturned = (0..9).map(|i| format!("hold number {i}")).collect();
        let p = p.with_ids();
        assert_eq!(p.overturned.len(), MAX_OVERTURNED);
        let text = prompt(&p);
        assert!(text.contains("## Earlier decisions like this one that people overturned"));
        assert!(text.contains("- hold number 0"));
        assert!(!text.contains("hold number 5"));
        // Older stored pictures without the field still read.
        let old: Picture = serde_json::from_value(json!({"kind":"block","step":"review",
            "reason":"r","reversible":true,"ticket_title":"","ticket_body":""}))
        .unwrap();
        assert!(old.overturned.is_empty());
    }

    #[test]
    fn repository_memories_reach_the_prompt_bounded_as_context() {
        let mut p = picture(Kind::Block);
        assert!(!prompt(&p).contains("memories say"));
        p.memories = (0..8)
            .map(|i| format!("memory number {i}: run the tests from services/"))
            .collect();
        let p = p.with_ids();
        assert_eq!(p.memories.len(), MAX_MEMORIES);
        let text = prompt(&p);
        assert!(text.contains("## What this repository's memories say"));
        assert!(text.contains("Context, not evidence"));
        assert!(text.contains("- memory number 0:"));
        assert!(!text.contains("memory number 5"));
        let old: Picture = serde_json::from_value(json!({"kind":"block","step":"review",
            "reason":"r","reversible":true,"ticket_title":"","ticket_body":""}))
        .unwrap();
        assert!(old.memories.is_empty());
    }

    fn proceed(a: &str) -> Value {
        json!({"verdict":"proceed","assumption":a,"question":"","person_only":"","tried":[],
               "options":[],"default":"","deadline_hours":0,"blocker":"","evidence":[],
               "category":"","location":""})
    }

    fn picture(kind: Kind) -> Picture {
        let mut p = Picture::new(kind, "test", "a step wants to stop");
        p.evidence = vec![
            Evidence {
                source: "ci".into(),
                finding: "unit tests fail".into(),
                location: "src/a.rs:10".into(),
                checked: true,
                ..Default::default()
            },
            Evidence {
                source: "review".into(),
                finding: "might be slow".into(),
                checked: false,
                ..Default::default()
            },
        ];
        p
    }

    fn run<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn a_good_answer_is_taken_as_it_is() {
        let m = Recorded::new(vec![Ok(proceed("the button goes on the settings page"))]);
        let j = run(judge(&m, picture(Kind::Ask)));
        assert!(j.proceeds());
        assert_eq!(j.source(), &Source::Model);
        assert!(j.open_item().unwrap().contains("settings page"));
    }

    #[test]
    fn a_hold_needs_checked_evidence_and_a_block_needs_category_and_location() {
        let p = picture(Kind::Block).with_ids();
        assert!(validate(
            &p,
            &block("tests fail", &["E1"], "correctness", "src/a.rs:10")
        )
        .is_ok());
        // Unchecked evidence.
        assert!(validate(&p, &block("slow", &["E2"], "correctness", "src/a.rs:1")).is_err());
        // Unknown id.
        assert!(validate(&p, &block("x", &["E9"], "correctness", "a")).is_err());
        // A style point is never a block.
        assert!(validate(&p, &block("naming", &["E1"], "other", "src/a.rs:10")).is_err());
        // No location.
        assert!(validate(&p, &block("tests fail", &["E1"], "correctness", "")).is_err());
        // A stop needs no category.
        let p = picture(Kind::Stop).with_ids();
        assert!(validate(&p, &block("tests fail", &["E1"], "", "")).is_ok());
    }

    #[test]
    fn an_ask_needs_a_person_only_fact_options_a_default_and_what_was_tried() {
        let p = picture(Kind::Ask).with_ids();
        let ask = |person: &str, opts: Vec<&str>, default: &str, tried: Vec<&str>| {
            json!({"verdict":"ask","assumption":"","question":"Which page?","person_only":person,
                   "tried":tried,"options":opts,"default":default,"deadline_hours":12,
                   "blocker":"","evidence":[],"category":"","location":""})
        };
        let ok = validate(
            &p,
            &ask("product_choice", vec!["a", "b"], "a", vec!["read it"]),
        );
        assert!(matches!(ok, Ok(Verdict::Ask(ref q)) if q.deadline_hours == 12));
        assert!(validate(&p, &ask("", vec!["a", "b"], "a", vec!["r"])).is_err());
        assert!(validate(&p, &ask("access", vec!["a"], "a", vec!["r"])).is_err());
        assert!(validate(
            &p,
            &ask("access", vec!["a", "b", "c", "d", "e"], "a", vec!["r"])
        )
        .is_err());
        assert!(validate(&p, &ask("access", vec!["a", "b"], "c", vec!["r"])).is_err());
        assert!(validate(&p, &ask("access", vec!["a", "b"], "a", vec![])).is_err());
        // A review never asks.
        let p = picture(Kind::Block).with_ids();
        assert!(validate(&p, &ask("access", vec!["a", "b"], "a", vec!["r"])).is_err());
    }

    #[test]
    fn a_bad_answer_is_asked_again_once_then_falls_back_to_proceed_never_to_asking() {
        let bad = json!({"verdict":"maybe"});
        let m = Recorded::new(vec![Ok(bad.clone()), Ok(proceed("ok"))]);
        let j = run(judge(&m, picture(Kind::Ask)));
        assert_eq!(j.source(), &Source::ModelReasked);
        assert_eq!(j.replies().len(), 2);

        let m = Recorded::new(vec![Ok(bad.clone()), Ok(bad)]);
        let j = run(judge(&m, picture(Kind::Ask)));
        assert!(j.proceeds());
        assert!(matches!(j.source(), Source::Fallback(_)));
        assert!(j.question().is_none());

        let m = Recorded::new(vec![Err("timeout".into()), Err("timeout".into())]);
        let j = run(judge(&m, picture(Kind::Handoff)));
        assert!(j.proceeds());
    }

    #[test]
    fn irreversible_work_without_an_answer_holds_on_its_own_reason() {
        let mut p = picture(Kind::Stop);
        p.reversible = false;
        let m = Recorded::new(vec![]);
        let j = run(judge(&m, p));
        assert!(j.hold().is_some());
        assert!(j.question().is_none());
    }

    #[test]
    fn no_cycle_is_started_past_the_budget_and_no_model_is_called() {
        let mut p = picture(Kind::ExtraCycle);
        p.budget.cycles = 2;
        p.budget.cycle_cap = 2;
        let m = Recorded::new(vec![Ok(block("tests fail", &["E1"], "", ""))]);
        let j = run(judge(&m, p));
        assert!(j.proceeds());
        assert_eq!(j.source(), &Source::Rule("cycle budget".into()));
        assert!(j.replies().is_empty());
    }

    #[test]
    fn what_cannot_go_on_holds_without_a_model_call() {
        let mut p = picture(Kind::Stop);
        p.cannot_proceed = "the repository is not connected".into();
        let m = Recorded::new(vec![Ok(proceed("go"))]);
        let j = run(judge(&m, p));
        assert_eq!(j.hold_words().unwrap(), "the repository is not connected");
        assert_eq!(j.source(), &Source::Rule("cannot proceed".into()));
    }

    #[test]
    fn the_question_reads_with_options_default_and_deadline() {
        let p = picture(Kind::Ask);
        let m = Recorded::new(vec![Ok(json!({"verdict":"ask","assumption":"",
            "question":"Monthly or yearly totals?","person_only":"product_choice",
            "tried":["read the ticket","read the report code"],"options":["Monthly","Yearly"],
            "default":"monthly","deadline_hours":0,"blocker":"","evidence":[],"category":"",
            "location":""}))]);
        let j = run(judge(&m, p));
        let md = j.question_markdown().unwrap();
        assert!(md.contains("1. Monthly (recommended)"));
        assert!(md.contains("24 hours"));
        assert!(md.contains("read the report code"));
        let rq = j.run_question().unwrap();
        assert_eq!(rq.suggestions, vec!["Monthly", "Yearly"]);
    }

    #[test]
    fn the_prompt_carries_the_whole_picture() {
        let mut p = picture(Kind::Stop);
        p.ticket_title = "Add export".into();
        p.acceptance = vec!["CSV download".into()];
        p.related = vec![Related {
            ticket: "SIB-2".into(),
            summary: "the sibling".into(),
            pr: "#9".into(),
            pr_state: "open".into(),
        }];
        p.threads = vec![Thread {
            summary: "why this?".into(),
            open: true,
            person_last: true,
            ..Default::default()
        }];
        p.budget.cycle_cap = 2;
        let text = prompt(&p.with_ids());
        for want in [
            "Add export",
            "CSV download",
            "SIB-2",
            "[E1]",
            "[T1]",
            "of 2",
        ] {
            assert!(text.contains(want), "{want} missing:\n{text}");
        }
    }
}
