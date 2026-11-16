//! The one gate for everything CoderHelm would hand to a person.
//!
//! A dependency update's pull request asked its reader to run a registry
//! lookup CoderHelm could run itself, listed the same decision nine times
//! in nine wordings as "a product decision only a person can settle" when
//! the ticket itself settled it, told the reader to delete CoderHelm's own
//! lines, cut sentences off with an ellipsis, and asked the reviewer to do
//! CoderHelm's analysis of the major version jumps. The owner: "it is a
//! senior developer ... it has more access than I do; it should be able to
//! do it itself".
//!
//! [`PRINCIPLE`] is the standard, said once here and given to every model
//! that writes something for a person. [`decide`] holds every item to it,
//! in this order:
//!
//! 1. **Do it** ([`Outcome::Do`]): anything CoderHelm can do or verify with
//!    its own access (the sandbox, the package registry and the network,
//!    GitHub through the installation, data sources, connected apps,
//!    knowledge, the code graph), and anything already owned by CoderHelm,
//!    is done in the run and its result recorded.
//! 2. **Settled** ([`Outcome::Settled`]): what the ticket, the repository
//!    (history, conventions, earlier pull requests and why they closed), the
//!    team's settings or an earlier answer already settle. One line in the
//!    collapsed evidence, with the source.
//! 3. **Decided** ([`Outcome::Decided`]): a choice nothing settles is still
//!    made, as a careful engineer would make it: the codebase's pattern, the
//!    safest reversible option, the ticket's intent. One line ("Chose X over
//!    Y because Z; comment to switch"), never a question.
//! 4. **Review** ([`Outcome::Review`]): a final visual or behavioural
//!    sign-off belongs to the reviewer's merge decision, folded into the one
//!    review line, never a separate chore.
//! 5. **A person** ([`Outcome::Human`]): only for a listed [`Reason`]. A
//!    choice reaches a person only when it is hard to reverse or reaches
//!    people outside the pull request, the options differ for the business,
//!    and nothing shows a preference; even then it is one yes/no question
//!    with the work done on CoderHelm's recommendation.
//!
//! [`gate`] applies [`decide`] to a list and keeps one entry per decision
//! ([`key`]): the same decision recorded on several attempts, rounds or
//! passes, in other words, is one entry in its latest wording. A model can
//! only move an item out of the person's list or merge two of them
//! (`Gated::demote`, `Gated::merge`), never add one.
//!
//! Every place CoderHelm hands work to people (the run page, the pull
//! request's description, ticket comments, chat answers, outside steps)
//! goes through this module; a guard test in the worker fails when a new
//! path writes a person-facing item without it. Rendering is the caller's:
//! the gate returns structure ([`HumanItem`], [`Decision`]) so the layout
//! can change without touching it.

use crate::handback::HumanOnly;

/// The standard every pass holds to, for prompts and for people.
pub const PRINCIPLE: &str = "CoderHelm owns the work end to end, like a senior engineer with more access than the person who asked. \
Anything its own access reaches it does or verifies itself and records the result: the sandbox (builds, tests, scripts), the package registries and the network, GitHub through the installation, data sources, connected apps, knowledge and the code graph. \
What the ticket, the repository (its history, conventions, config, earlier pull requests and why they closed), the team's settings or an earlier answer already settle is settled: cite the source, never ask. \
When nothing settles a choice it still decides, as a careful engineer would: the codebase's pattern, the safest reversible option, the ticket's intent; it does the work that way and notes the decision in one line with the alternative and why. Reversible engineering choices (file location, naming, which pull request, a version within range, the test approach, splitting the work, page copy with a sensible default) are never asked. \
It hands a person only what its access and the facts can't settle: a credential, a permission only a person can grant, an approval the team requires, work someone else holds, an action outside every system it reaches, or a choice that is hard to reverse or reaches people outside the pull request (data loss, a public API or contract, pricing, legal or security posture, production traffic, messages to customers) where the options differ for the business and nothing shows a preference. Even then it does the work on its recommendation and asks one yes/no question naming the options and its recommendation. \
It never asks a person to edit or delete its own text, never asks a reviewer to do its analysis, and states its own conclusion with the evidence.";

/// The only reasons an item reaches a person.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Reason {
    /// A secret, token or sign-in only a person can provide.
    Credential,
    /// A permission CoderHelm lacks and only a person can grant (an org
    /// owner, a repository admin).
    Grant,
    /// A choice that is hard to reverse or reaches people outside the pull
    /// request, where the options differ for the business and nothing
    /// shows a preference. Always a yes/no on CoderHelm's recommendation.
    Irreversible,
    /// Someone else holds the work.
    HeldByAnother,
    /// The team's policy waits for a person's approval.
    Approval,
    /// An action outside every system CoderHelm reaches.
    OutsideReach,
}

/// Every reason on the list.
pub const REASONS: &[Reason] = &[
    Reason::Credential,
    Reason::Grant,
    Reason::Irreversible,
    Reason::HeldByAnother,
    Reason::Approval,
    Reason::OutsideReach,
];

impl Reason {
    pub fn code(self) -> &'static str {
        match self {
            Reason::Credential => "credential",
            Reason::Grant => "grant",
            Reason::Irreversible => "irreversible_choice",
            Reason::HeldByAnother => "held_by_another",
            Reason::Approval => "approval",
            Reason::OutsideReach => "outside_reach",
        }
    }

    /// Why CoderHelm can't, as a person reads it.
    pub fn label(self) -> &'static str {
        match self {
            Reason::Credential => "it needs a credential only a person can provide",
            Reason::Grant => "it needs a permission only a person can grant",
            Reason::Irreversible => {
                "the choice is hard to reverse and nothing in the ticket, repository or settings shows a preference"
            }
            Reason::HeldByAnother => "someone else is working on it",
            Reason::Approval => "the team's policy needs a person's approval",
            Reason::OutsideReach => "it is outside every system CoderHelm reaches",
        }
    }
}

/// Where an item came from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Kind {
    /// A decision CoderHelm (or its judge) proceeded on.
    Assumption,
    /// An open item on a pull request or a run.
    #[default]
    OpenItem,
    /// A step outside the diff (a deploy, a content change, a look).
    Step,
    /// A question to put to a person.
    Question,
    /// A step CoderHelm tried that did not work: `why_not` is the cause
    /// from that attempt. It reaches a person only when the cause itself is
    /// a listed reason (a permission, a credential, an approval); a tool
    /// failure, a bad request or a missing workflow is CoderHelm's to work
    /// around, never a person's chore.
    Attempt,
}

impl Kind {
    fn code(self) -> &'static str {
        match self {
            Kind::Assumption => "assumption",
            Kind::OpenItem => "open",
            Kind::Step => "step",
            Kind::Question => "question",
            Kind::Attempt => "attempt",
        }
    }
}

/// One thing headed to a person, as its writer put it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ask {
    pub kind: Kind,
    /// What the decision is about, when the writer named it (`fix
    /// location`, a step's name): the key that ties rewordings together.
    pub topic: String,
    /// The item in the writer's words.
    pub text: String,
    /// The command or step that settles it, when one is named.
    pub how: String,
    /// The owner the writer named (`CoderHelm`, `a content editor`).
    pub owner: String,
    /// Why the writer says CoderHelm can't.
    pub why_not: String,
    /// The options, when it is a choice; the first is the default.
    pub options: Vec<String>,
}

impl Ask {
    pub fn new(kind: Kind, text: &str) -> Ask {
        Ask {
            kind,
            text: one_line(text),
            ..Default::default()
        }
    }

    fn all(&self) -> String {
        format!(
            "{} {} {} {} {}",
            self.topic,
            self.text,
            self.how,
            self.why_not,
            self.options.join(" ")
        )
        .to_lowercase()
    }
}

/// What the gate can see about the run: what settles things and what
/// CoderHelm can reach. Empty means "nothing known", never "nothing there".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Context {
    /// The ticket's title and text.
    pub ticket: String,
    /// The repository's history as lines (`#12 closed unmerged: ...`).
    pub history: Vec<String>,
    /// Team settings and conventions that bear on choices, as lines.
    pub settings: Vec<String>,
    /// Earlier answers from people, word for word.
    pub answers: Vec<String>,
    /// CoderHelm holds the credentials a deploy command needs (set for the
    /// sandbox or a connected app).
    pub can_deploy: bool,
}

/// What settles an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Ticket,
    History,
    Convention,
    Settings,
    Answer,
}

impl Source {
    pub fn words(self) -> &'static str {
        match self {
            Source::Ticket => "the ticket",
            Source::History => "the repository's history",
            Source::Convention => "the repository's conventions",
            Source::Settings => "the team's settings",
            Source::Answer => "an earlier answer",
        }
    }
}

/// What CoderHelm does itself in place of a person.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Work {
    /// Reads the package registry (integrity, maintainers) for these
    /// packages, or every moved line when none is named.
    Registry(Vec<String>),
    /// Runs this command in its sandbox.
    Command(String),
    /// Its own step, as written (a query to run, a list to post).
    Own(String),
}

impl Work {
    pub fn words(&self) -> String {
        match self {
            Work::Registry(p) if p.is_empty() => {
                "read the package registry's integrity and maintainer records".to_string()
            }
            Work::Registry(p) => format!(
                "read the package registry's integrity and maintainer records for {}",
                p.join(", ")
            ),
            Work::Command(c) => format!("ran `{c}` in its sandbox"),
            Work::Own(w) => w.clone(),
        }
    }
}

/// A choice CoderHelm made where nothing settled it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub chose: String,
    pub over: String,
    pub because: String,
    /// The reviewer should know before merging (it changes what merges).
    pub matters_for_merge: bool,
}

impl Decision {
    /// One line: what, over what, why, how to switch.
    pub fn line(&self) -> String {
        let over = if self.over.trim().is_empty() {
            String::new()
        } else {
            format!(" over {}", trim_end(&self.over))
        };
        format!(
            "Chose {}{over} because {}; comment to switch.",
            trim_end(&self.chose),
            trim_end(&self.because)
        )
    }
}

/// One item only a person can do. Its fields are private: it exists only
/// for a [`Reason`] on the list, with one action, who can do it and why
/// CoderHelm can't.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HumanItem {
    reason: Reason,
    action: String,
    who: String,
    why_not: String,
    options: Vec<String>,
    recommendation: String,
}

impl HumanItem {
    pub fn reason(&self) -> Reason {
        self.reason
    }
    pub fn action(&self) -> &str {
        &self.action
    }
    pub fn who(&self) -> &str {
        &self.who
    }
    pub fn why_not(&self) -> &str {
        &self.why_not
    }
    pub fn options(&self) -> &[String] {
        &self.options
    }
    pub fn recommendation(&self) -> &str {
        &self.recommendation
    }

    /// Whether it is a yes/no on CoderHelm's recommendation.
    pub fn yes_no(&self) -> bool {
        self.reason == Reason::Irreversible
    }

    /// The item in one line: the one action, why CoderHelm can't, the
    /// options and the recommendation, and who can do it. Never cut.
    pub fn line(&self) -> String {
        let mut t = trim_end(&self.action).to_string();
        t.push('.');
        if !self.options.is_empty() {
            t.push_str(&format!(" Options: {}.", self.options.join("; or ")));
        }
        if !self.recommendation.is_empty() {
            t.push_str(&format!(
                " CoderHelm recommends {}{}.",
                trim_end(&self.recommendation),
                if self.yes_no() {
                    " and has done the work that way"
                } else {
                    ""
                }
            ));
        }
        t.push_str(&format!(" CoderHelm can't: {}.", trim_end(&self.why_not)));
        format!("{t} ({})", self.who)
    }
}

/// What the gate makes of one item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Do(Work),
    Settled { source: Source, note: String },
    Decided(Decision),
    Review(String),
    Human(HumanItem),
}

impl Outcome {
    pub fn class(&self) -> Class {
        match self {
            Outcome::Do(_) => Class::Do,
            Outcome::Settled { .. } => Class::Settled,
            Outcome::Decided(_) => Class::Decided,
            Outcome::Review(_) => Class::Review,
            Outcome::Human(_) => Class::Human,
        }
    }
}

/// An outcome's class, for counts and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Class {
    Do,
    Settled,
    Decided,
    Review,
    Human,
}

// ── Reading the words ───────────────────────────────────────────────────

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn trim_end(s: &str) -> &str {
    s.trim().trim_end_matches(['.', ';', ','])
}

fn any(t: &str, marks: &[&str]) -> bool {
    marks.iter().any(|m| t.contains(m))
}

/// A registry lookup: maintainers, integrity, checksums, `npm view`.
fn names_registry_lookup(t: &str) -> bool {
    any(
        t,
        &[
            "npm view",
            "npm info",
            "npm show",
            "yarn info",
            "pnpm view",
            "pip show",
            "pip download",
            "cargo info",
            "dist.integrity",
            "integrity hash",
            "lockfile hash",
            "maintainers",
            "checksum",
            "provenance",
        ],
    ) || (t.contains("registry")
        && any(
            t,
            &["integrity", "maintainer", "publisher", "hash", "checksum"],
        ))
}

/// Package names a registry lookup names: `name@version` words.
fn packages_in(text: &str) -> Vec<String> {
    let mut out: Vec<String> = text
        .split(|c: char| c.is_whitespace() || c == '`' || c == ',' || c == '(' || c == ')')
        .map(|w| w.trim_end_matches(['.', ':', ';']))
        .filter(|w| {
            let at = w.get(1..).and_then(|r| r.find('@')).map(|i| i + 1);
            at.is_some_and(|i| {
                w[i + 1..]
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_digit())
                    && !w.contains("://")
            })
        })
        .map(String::from)
        .collect();
    out.sort();
    out.dedup();
    out
}

/// A visual or behavioural sign-off: how something looks or reads.
fn names_look(t: &str) -> bool {
    t.trim_start().starts_with("look at")
        || any(
            t,
            &[
                "how a page looks",
                "how the page looks",
                "looks right",
                "visual check",
                "check visually",
                "eyeball",
                "judgement only a person",
                "judgment only a person",
            ],
        )
}

fn names_deploy(t: &str) -> bool {
    any(t, &["deploy", "release to", "publish to production"])
}

/// Whether the item asks a person to test: run a flow or a suite, read an
/// end-to-end result, check a preview in a browser. Testing is CoderHelm's
/// own work, whatever stopped it (a missing permission is an install item,
/// said once in setup checks, never a tester's chore on a pull request).
fn asks_testing(a: &Ask) -> bool {
    let t = one_line(&a.text).to_lowercase();
    let t = t
        .trim_start_matches("after merging: ")
        .trim_start_matches("before merging: ")
        .trim_start_matches(['-', '*', ' ']);
    let verb = t.split_whitespace().next().unwrap_or("");
    let testing_verb = matches!(
        verb,
        "run"
            | "rerun"
            | "re-run"
            | "read"
            | "check"
            | "confirm"
            | "verify"
            | "test"
            | "try"
            | "open"
            | "watch"
    );
    testing_verb
        && any(
            t,
            &[
                "e2e",
                "end-to-end",
                "end to end",
                "chromium",
                "webkit",
                "firefox",
                "safari",
                "playwright",
                "cypress",
                "browser",
                "smoke test",
                "test result",
                "the tests",
                "the test suite",
                "the spec",
                "the flow",
                "preview",
            ],
        )
}

/// Whether the item's action is adding labels to a pull request.
fn adds_labels(a: &Ask) -> bool {
    let t = one_line(&a.text).to_lowercase();
    let t = t
        .trim_start_matches("after merging: ")
        .trim_start_matches("before merging: ");
    (t.starts_with("add pr label") || t.starts_with("add labels") || t.starts_with("add the label"))
        && !t.contains("deploy")
}

/// Whether the item's action is a deploy: its words start with one, or the
/// command it names deploys. A step that only names a workflow called
/// "Deploy Preview" (to read its result) is not a deploy.
fn asks_deploy(a: &Ask) -> bool {
    let text = one_line(&a.text).to_lowercase();
    let mut t = text.as_str();
    for lead in ["after merging: ", "before merging: ", "after merge: "] {
        t = t.strip_prefix(lead).unwrap_or(t);
    }
    let verb = ["deploy", "release", "publish", "redeploy", "ship"]
        .iter()
        .any(|v| t.starts_with(v));
    verb || command_in(a).is_some_and(|c| names_deploy(&c.to_lowercase()))
}

/// A command a person was asked to run, when the item names one.
fn command_in(a: &Ask) -> Option<String> {
    let cand = |s: &str| -> Option<String> {
        let s = s.trim().trim_matches('`').trim();
        crate::pr_steps::is_command(s).then(|| s.to_string())
    };
    if let Some(c) = cand(&a.how) {
        return Some(c);
    }
    let mut ticks = a.text.split('`');
    ticks.next();
    while let Some(inside) = ticks.next() {
        if let Some(c) = cand(inside) {
            return Some(c);
        }
        ticks.next();
    }
    let lower = a.text.to_lowercase();
    for lead in ["run ", "execute "] {
        if let Some(at) = lower.find(lead) {
            let rest = &a.text[at + lead.len()..];
            let cmd: String = rest
                .split(" and ")
                .next()
                .unwrap_or("")
                .trim()
                .trim_end_matches('.')
                .to_string();
            if crate::pr_steps::is_command(&cmd) {
                return Some(cmd);
            }
        }
    }
    None
}

/// Choices that are hard to reverse or reach people outside the pull
/// request.
fn names_irreversible(t: &str) -> bool {
    any(
        t,
        &[
            "data loss",
            "lose data",
            "delete data",
            "deletes data",
            "drop table",
            "drop the table",
            "drop column",
            "irreversible",
            "can't be undone",
            "cannot be undone",
            "public api",
            "breaking change to the api",
            "api contract",
            "contract with",
            "pricing",
            "price change",
            "billing",
            "refund",
            "legal",
            "licen",
            "compliance",
            "security posture",
            "rotate the",
            "key rotation",
            "signing key",
            "production traffic",
            "email customers",
            "emails to customers",
            "notify customers",
            "message customers",
            "sent to customers",
            "send to customers",
            "customers receive",
        ],
    )
}

/// Words that say the writer cited what settles the choice.
fn cited_source(t: &str, ctx: &Context) -> Option<Source> {
    if any(
        t,
        &[
            "the ticket is",
            "the ticket asks",
            "the ticket says",
            "the ticket states",
            "ticket explicitly",
            "explicit, self-contained",
            "as the ticket",
            "per the ticket",
            "the issue asks",
            "the issue says",
            "acceptance criteria",
        ],
    ) {
        return Some(Source::Ticket);
    }
    if any(
        t,
        &[
            "closed unmerged",
            "closed without merging",
            "was closed",
            "earlier pull request",
            "previous pull request",
            "git history",
            "as before",
            "last time",
        ],
    ) {
        return Some(Source::History);
    }
    if any(
        t,
        &[
            "the repository's convention",
            "the repo's convention",
            "existing pattern",
            "the codebase's pattern",
            "the same way as",
            "follows the pattern",
            "like the other",
            "contributing",
            "agents.md",
            "claude.md",
        ],
    ) {
        return Some(Source::Convention);
    }
    if any(
        t,
        &[
            "team's settings",
            "team setting",
            "the team's rule",
            "team instruction",
        ],
    ) {
        return Some(Source::Settings);
    }
    let words = content_words(t);
    if words.len() >= 3 {
        for a in &ctx.answers {
            let aw = content_words(&a.to_lowercase());
            let shared = words.iter().filter(|w| aw.contains(w)).count();
            if shared * 2 >= words.len().min(aw.len()).max(1) && shared >= 2 {
                return Some(Source::Answer);
            }
        }
    }
    None
}

/// The listed reason the words name, read against the handback list.
fn reason_in(t: &str) -> Option<Reason> {
    // A line the gate already let through says its reason in these words.
    if let Some(r) = REASONS
        .iter()
        .find(|r| t.contains(&r.label().to_lowercase()))
    {
        return Some(*r);
    }
    if any(t, &["approv"]) && any(t, &["policy", "agent permission"]) {
        return Some(Reason::Approval);
    }
    if any(
        t,
        &[
            "set the",
            "add the",
            "provide the",
            "paste the",
            "store the",
        ],
    ) && any(t, &["secret", "token", "password", "api key", "credential"])
    {
        return Some(Reason::Credential);
    }
    if any(
        t,
        &[
            "org owner",
            "organization owner",
            "repository admin",
            "only an admin",
            "grant the app",
            "grant coderhelm",
            "the app lacks",
            "the app wasn't granted",
            "not allowed to",
            "permission",
            "resource not accessible by integration",
            "turn the repository on",
            "not turned on",
            "app isn't installed",
            "no connection can",
            "connection exists",
            "write connection",
            "write token",
            "no access to",
        ],
    ) {
        return Some(Reason::Grant);
    }
    // Content on production is changed by the people who own it, under
    // the team's rules: CoderHelm prepares it and never writes it there.
    if any(
        t,
        &[
            "never writes production",
            "production content",
            "content on production",
            "on production",
        ],
    ) {
        return Some(Reason::Approval);
    }
    if any(
        t,
        &[
            "outside every system",
            "no system coderhelm reaches",
            "physical device",
            "real device",
            "payment method",
            "paid checkout",
            "paid registration",
            "real payment",
            "vendor admin",
            "portal admin",
            "third-party admin",
            "in person",
            "phone call",
            "call the vendor",
            "sign a contract",
        ],
    ) {
        return Some(Reason::OutsideReach);
    }
    match crate::handback::human_only_in(t) {
        Some(HumanOnly::Credential) => Some(Reason::Credential),
        Some(HumanOnly::MissingAccess) => Some(Reason::Grant),
        Some(HumanOnly::HeldByAnother) => Some(Reason::HeldByAnother),
        Some(HumanOnly::Approval) => Some(Reason::Approval),
        _ => None,
    }
}

fn who_for(reason: Reason, owner: &str) -> String {
    let o = one_line(owner);
    if !o.is_empty() && !o.eq_ignore_ascii_case("coderhelm") && !o.eq_ignore_ascii_case("a person")
    {
        return o;
    }
    match reason {
        Reason::Credential => "a person who holds the credential",
        Reason::Grant => "an organization owner or repository admin",
        Reason::Irreversible => "the ticket's owner",
        Reason::HeldByAnother => "whoever holds the work",
        Reason::Approval => "a team admin",
        Reason::OutsideReach => "a person on the team",
    }
    .to_string()
}

fn owned_by_coderhelm(a: &Ask, t: &str) -> bool {
    a.owner.trim().eq_ignore_ascii_case("coderhelm")
        || any(
            t,
            &[
                "owned by coderhelm",
                "owner: coderhelm",
                "coderhelm does this",
                "coderhelm runs",
                "coderhelm queries",
                "coderhelm posts",
            ],
        )
}

/// What CoderHelm's recommendation is for a choice: the first option, or
/// what the writer says it built on.
fn recommendation_of(a: &Ask) -> String {
    if let Some(o) = a.options.first() {
        return one_line(o);
    }
    let t = &a.text;
    for mark in ["(Default: ", "Default: ", "default: ", "defaulting to "] {
        if let Some(at) = t.find(mark) {
            let rest = &t[at + mark.len()..];
            return rest
                .split([')', '\n'])
                .next()
                .unwrap_or("")
                .trim()
                .trim_end_matches('.')
                .to_string();
        }
    }
    String::new()
}

/// The words of a sentence worth comparing.
fn content_words(text: &str) -> Vec<String> {
    const COMMON: &[&str] = &[
        "that",
        "this",
        "with",
        "from",
        "because",
        "there",
        "their",
        "which",
        "never",
        "nothing",
        "have",
        "been",
        "were",
        "what",
        "when",
        "into",
        "them",
        "they",
        "does",
        "should",
        "would",
        "could",
        "coderhelm",
        "pull",
        "request",
        "instead",
        "asking",
        "assumed",
        "settle",
        "correct",
        "comment",
        "otherwise",
        "delete",
        "line",
        "person",
        "only",
    ];
    let mut out: Vec<String> = text
        .split(|c: char| !c.is_alphanumeric())
        .map(str::to_lowercase)
        .filter(|w| w.len() > 3 && !COMMON.contains(&w.as_str()))
        .collect();
    out.sort();
    out.dedup();
    out
}

// ── The gate ────────────────────────────────────────────────────────────

/// What becomes of one item. Deterministic; the model can only lower it
/// further ([`Gated::demote`]).
pub fn decide(a: &Ask, ctx: &Context) -> Outcome {
    let t = a.all();
    // A step CoderHelm tried: only a listed reason in the attempt's own
    // cause reaches a person. Anything else (a tool that failed, a request
    // the service refused as malformed, no workflow to start) is CoderHelm's
    // to retry or do another way.
    // Labels on a pull request go through the issues API: no workflow
    // permission is involved, whatever a run said about one.
    if adds_labels(a) {
        return Outcome::Do(Work::Own(one_line(&a.text)));
    }
    // Testing is never a person's item.
    if asks_testing(a) {
        return Outcome::Do(Work::Own(one_line(&a.text)));
    }
    if a.kind == Kind::Attempt {
        let cause = one_line(&a.why_not).to_lowercase();
        // Refused a workflow start, with a label that starts the same run:
        // CoderHelm adds the label (no workflow permission needed).
        let refused_start = cause.contains("start workflows")
            || cause.contains("workflow_dispatch")
            || cause.contains("resource not accessible");
        if refused_start && t.contains("label") && !t.contains("deploy") {
            return Outcome::Do(Work::Own(format!("{} (by its label)", one_line(&a.text))));
        }
        // A deploy it tried and could not start: the one missing thing (the
        // permission, or the deploy credentials) is said once, as for any
        // deploy; a deploy CoderHelm holds credentials for, it runs.
        if names_deploy(&t) && asks_deploy(a) {
            return deploy_item(a, &t, ctx);
        }
        return match reason_in(&cause) {
            Some(reason @ (Reason::Grant | Reason::Credential | Reason::Approval)) => {
                Outcome::Human(HumanItem {
                    reason,
                    action: first_nonempty(&a.how, &a.text),
                    who: who_for(reason, &a.owner),
                    why_not: first_nonempty(&a.why_not, reason.label()),
                    options: Vec::new(),
                    recommendation: String::new(),
                })
            }
            _ => Outcome::Do(Work::Own(one_line(&a.text))),
        };
    }
    // 1. CoderHelm's own work, or work its access reaches.
    if owned_by_coderhelm(a, &t) {
        return Outcome::Do(Work::Own(one_line(&a.text)));
    }
    if names_registry_lookup(&t) {
        return Outcome::Do(Work::Registry(packages_in(&format!(
            "{} {}",
            a.text, a.how
        ))));
    }
    if names_look(&t) {
        return Outcome::Review(one_line(&a.text));
    }
    // A deploy: CoderHelm runs it when it holds the credentials; else the
    // one missing thing is said once.
    if a.kind == Kind::Step && names_deploy(&t) && asks_deploy(a) {
        return deploy_item(a, &t, ctx);
    }
    // 2. A listed reason the item itself names.
    if let Some(reason) = reason_in(&t) {
        return Outcome::Human(HumanItem {
            reason,
            action: first_nonempty(&a.how, &a.text),
            who: who_for(reason, &a.owner),
            why_not: first_nonempty(&a.why_not, reason.label()),
            options: Vec::new(),
            recommendation: String::new(),
        });
    }
    // 3. A choice: settled by a source, else decided, else (hard to
    // reverse, nothing settles it) one yes/no on the recommendation.
    let choice = matches!(a.kind, Kind::Assumption | Kind::Question)
        || !a.options.is_empty()
        || any(
            &t,
            &[
                "product decision",
                "product choice",
                "business decision",
                "decide",
                "which behaviour",
                "which behavior",
                "no owner is named",
            ],
        );
    if choice {
        if let Some(source) = cited_source(&t, ctx) {
            return Outcome::Settled {
                source,
                note: format!(
                    "{} (settled by {}).",
                    trim_end(&first_nonempty(&a.topic_line(), &a.text)),
                    source.words()
                ),
            };
        }
        if names_irreversible(&t) {
            let rec = first_nonempty(&recommendation_of(a), "going ahead as built");
            let mut options = a.options.clone();
            if options.is_empty() {
                options = vec![rec.clone(), "leaving it out".to_string()];
            }
            return Outcome::Human(HumanItem {
                reason: Reason::Irreversible,
                action: format!("Yes or no: {}", trim_end(&strip_question_marks(&a.text))),
                who: who_for(Reason::Irreversible, &a.owner),
                why_not: Reason::Irreversible.label().to_string(),
                options,
                recommendation: rec,
            });
        }
        let chose = first_nonempty(&recommendation_of(a), &a.text);
        return Outcome::Decided(Decision {
            chose: if chose == a.text {
                format!("to proceed as built ({})", trim_end(&a.text))
            } else {
                chose
            },
            over: a.options.get(1).cloned().unwrap_or_default(),
            because:
                "it is reversible and nothing in the ticket, repository or settings asks otherwise"
                    .to_string(),
            matters_for_merge: false,
        });
    }
    // 4. Anything else left open is CoderHelm's own work.
    Outcome::Do(Work::Own(one_line(&first_nonempty(&a.how, &a.text))))
}

/// A deploy: CoderHelm runs it when it holds the credentials; else the one
/// missing thing is said once.
fn deploy_item(a: &Ask, t: &str, ctx: &Context) -> Outcome {
    // A deploy CoderHelm can start (a workflow it dispatches or one a label
    // starts, or credentials it holds) is its own: never a credential item
    // for a person while it has not tried. An attempt whose own cause is a
    // listed reason still reaches a person.
    let tried_and_refused =
        a.kind == Kind::Attempt && reason_in(&one_line(&a.why_not).to_lowercase()).is_some();
    if ctx.can_deploy && !tried_and_refused {
        return Outcome::Do(match command_in(a) {
            Some(c) => Work::Command(c),
            None => Work::Own(one_line(&a.text)),
        });
    }
    let reason = reason_in(t).unwrap_or(Reason::Credential);
    let missing = match reason {
        Reason::Grant => "grant CoderHelm's GitHub App permission to start the deploy workflow",
        Reason::Credential => {
            "give CoderHelm the deploy credentials for this environment (Settings, Agents, sandbox secrets)"
        }
        Reason::Approval => "approve the deploy where the team's policy asks (the Agent permissions page)",
        _ => "",
    };
    let action = match (missing, command_in(a)) {
        ("", _) => first_nonempty(&a.text, &a.how),
        (m, Some(c)) if reason != Reason::Approval => {
            format!("{} or run `{c}` once", sentence_start(m))
        }
        (m, _) => sentence_start(m),
    };
    Outcome::Human(HumanItem {
        reason,
        action,
        who: who_for(reason, &a.owner),
        why_not: first_nonempty(&a.why_not, reason.label()),
        options: Vec::new(),
        recommendation: String::new(),
    })
}

impl Ask {
    fn topic_line(&self) -> String {
        if self.topic.trim().is_empty() {
            self.text.clone()
        } else {
            format!("{}: {}", one_line(&self.topic), self.text)
        }
    }
}

fn first_nonempty(a: &str, b: &str) -> String {
    let a = one_line(a);
    if a.is_empty() {
        one_line(b)
    } else {
        a
    }
}

fn sentence_start(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().chain(c).collect(),
        None => String::new(),
    }
}

fn strip_question_marks(s: &str) -> String {
    s.replace('?', "")
}

/// An item for a reason its writer already holds (a hand-back reason read
/// by code): the same item [`decide`] would make for that reason. `None`
/// for a reason that is a choice CoderHelm makes itself.
pub fn for_reason(why: HumanOnly, a: &Ask) -> Option<HumanItem> {
    let reason = match why {
        HumanOnly::Credential => Reason::Credential,
        HumanOnly::MissingAccess => Reason::Grant,
        HumanOnly::HeldByAnother => Reason::HeldByAnother,
        HumanOnly::Approval => Reason::Approval,
        _ => return None,
    };
    Some(HumanItem {
        reason,
        action: first_nonempty(&a.how, &a.text),
        who: who_for(reason, &a.owner),
        why_not: first_nonempty(&a.why_not, reason.label()),
        options: Vec::new(),
        recommendation: String::new(),
    })
}

/// The stable key of an item's decision: its kind and topic, or its
/// content words when it has no topic.
pub fn key(a: &Ask) -> String {
    let topic = one_line(&a.topic).to_lowercase();
    if !topic.is_empty() {
        return format!("{}:{topic}", a.kind.code());
    }
    let mut w = content_words(&a.text);
    w.truncate(12);
    format!("{}:{}", a.kind.code(), w.join(" "))
}

/// Whether two items say the same decision in other words: the same key,
/// or most of their content words shared.
pub fn same_decision(a: &Ask, b: &Ask) -> bool {
    if key(a) == key(b) {
        return true;
    }
    if !a.topic.trim().is_empty() && !b.topic.trim().is_empty() {
        return false;
    }
    let (x, y) = (content_words(&a.text), content_words(&b.text));
    if x.len() < 3 || y.len() < 3 {
        return false;
    }
    let shared = x.iter().filter(|w| y.contains(w)).count();
    shared * 10 >= x.len().min(y.len()) * 6
}

/// A list of items after the gate.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Gated {
    /// Each item once, its latest wording, with what became of it.
    pub items: Vec<(Ask, Outcome)>,
}

impl Gated {
    pub fn of(&self, class: Class) -> impl Iterator<Item = &(Ask, Outcome)> {
        self.items.iter().filter(move |(_, o)| o.class() == class)
    }

    /// What only a person can do.
    pub fn human(&self) -> Vec<&HumanItem> {
        self.items
            .iter()
            .filter_map(|(_, o)| match o {
                Outcome::Human(h) => Some(h),
                _ => None,
            })
            .collect()
    }

    /// What CoderHelm does itself.
    pub fn work(&self) -> Vec<&Work> {
        self.items
            .iter()
            .filter_map(|(_, o)| match o {
                Outcome::Do(w) => Some(w),
                _ => None,
            })
            .collect()
    }

    /// One line each for the collapsed evidence: what was settled and by
    /// what, and each decision made.
    pub fn evidence_lines(&self) -> Vec<String> {
        self.items
            .iter()
            .filter_map(|(_, o)| match o {
                Outcome::Settled { note, .. } => Some(note.clone()),
                Outcome::Decided(d) => Some(d.line()),
                _ => None,
            })
            .collect()
    }

    /// What the review line carries: the sign-offs folded into the merge
    /// decision, and decisions that matter for it.
    pub fn review_notes(&self) -> Vec<String> {
        self.items
            .iter()
            .filter_map(|(_, o)| match o {
                Outcome::Review(r) => Some(r.clone()),
                Outcome::Decided(d) if d.matters_for_merge => Some(d.line()),
                _ => None,
            })
            .collect()
    }

    /// How many items went each way, for the run's analytics.
    pub fn counts(&self) -> Counts {
        let n = |c: Class| self.of(c).count() as u32;
        Counts {
            done: n(Class::Do),
            settled: n(Class::Settled),
            decided: n(Class::Decided),
            review: n(Class::Review),
            human: n(Class::Human),
        }
    }

    /// The model found item `i` not a person's after all: it becomes the
    /// given outcome, which may not be a person's.
    pub fn demote(&mut self, i: usize, to: Outcome) {
        if matches!(to, Outcome::Human(_)) {
            return;
        }
        if let Some(item) = self.items.get_mut(i) {
            item.1 = to;
        }
    }

    /// The model found items `i` and `j` the same decision: one entry, in
    /// the later one's wording.
    pub fn merge(&mut self, i: usize, j: usize) {
        if i == j || i >= self.items.len() || j >= self.items.len() {
            return;
        }
        // The earlier one goes; the later wording stays.
        self.items.remove(i.min(j));
    }
}

/// Items by class, for a run's record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Counts {
    pub done: u32,
    pub settled: u32,
    pub decided: u32,
    pub review: u32,
    pub human: u32,
}

impl Counts {
    pub fn add(&mut self, o: &Counts) {
        self.done += o.done;
        self.settled += o.settled;
        self.decided += o.decided;
        self.review += o.review;
        self.human += o.human;
    }
}

/// Every item through [`decide`], one entry per decision in its latest
/// wording (items come in the order they were written).
pub fn gate(asks: Vec<Ask>, ctx: &Context) -> Gated {
    let mut kept: Vec<Ask> = Vec::new();
    for a in asks {
        if a.text.trim().is_empty() {
            continue;
        }
        match kept.iter().position(|k| same_decision(k, &a)) {
            Some(i) => kept[i] = a,
            None => kept.push(a),
        }
    }
    Gated {
        items: kept
            .into_iter()
            .map(|a| {
                let o = decide(&a, ctx);
                (a, o)
            })
            .collect(),
    }
}

/// The one-line gate for text already written for a person (a rendered
/// line, a ticket comment's item): what it is, read without context.
pub fn line_outcome(kind: Kind, text: &str) -> Outcome {
    decide(&Ask::new(kind, text), &Context::default())
}

/// Phrases CoderHelm never writes to a person: edits to its own text, its
/// analysis handed to a reviewer.
pub const NEVER_ASKED: &[&str] = &[
    "delete this line",
    "otherwise delete",
    "edit this description",
    "remove this line",
    "feedback wanted",
    "does the evidence beside",
];

/// Whether a person-facing text keeps the rules: none of [`NEVER_ASKED`],
/// and nothing cut off with an ellipsis.
pub fn keeps_the_rules(text: &str) -> Result<(), String> {
    let lower = text.to_lowercase();
    if let Some(p) = NEVER_ASKED.iter().find(|p| lower.contains(*p)) {
        return Err(format!("asks a person \"{p}\""));
    }
    if text.contains('…') {
        return Err("cuts text off with an ellipsis".to_string());
    }
    Ok(())
}

// ── The sink check: the last word before text reaches a person ──────────

/// Words that mark a line as an item addressed to a person, wherever it
/// sits in the text.
pub const PERSON_MARKS: &[&str] = &[
    "CoderHelm could not:",
    "CoderHelm cannot:",
    "CoderHelm can't:",
    "(a person on the team)",
];

/// Whether a line opens the block of what a person has to do.
fn opens_needed(l: &str) -> bool {
    let t = l
        .trim()
        .trim_start_matches('#')
        .trim()
        .trim_matches('*')
        .trim();
    t.to_lowercase().starts_with("needed from you")
}

/// The text of a list item (`- `, `* `, `- [ ] `, `1. `), when the line is
/// one.
fn list_item(l: &str) -> Option<&str> {
    let t = l.trim_start();
    for lead in ["- [ ] ", "- [x] ", "- ", "* "] {
        if let Some(r) = t.strip_prefix(lead) {
            return Some(r.trim());
        }
    }
    let digits = t.chars().take_while(|c| c.is_ascii_digit()).count();
    if digits > 0 && t[digits..].starts_with(". ") {
        return Some(t[digits + 2..].trim());
    }
    None
}

/// Every item a text addresses to a person: the list under "Needed from
/// you", and any line that says CoderHelm could not or cannot. Returns the
/// line's index and the item's text.
pub fn person_lines(text: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let mut inside = false;
    for (i, l) in text.lines().enumerate() {
        if opens_needed(l) {
            inside = true;
            // "**Needed from you:** Nothing." and an item on the same line.
            let rest = l.split_once(':').map(|(_, r)| r.trim()).unwrap_or("");
            let rest = rest.trim_start_matches("**").trim();
            if !rest.is_empty() && rest != crate::pr_steps::NOTHING_NEEDED {
                out.push((i, rest.to_string()));
            }
            continue;
        }
        let t = l.trim();
        if inside && (t.starts_with('#') || (t.starts_with("**") && list_item(l).is_none())) {
            inside = false;
        }
        let item = list_item(l);
        let marked = PERSON_MARKS.iter().any(|m| l.contains(m));
        match item {
            Some(it) if inside || marked => out.push((i, it.to_string())),
            None if marked => out.push((i, t.to_string())),
            _ => {}
        }
    }
    out
}

/// One line a person was to read, through the gate: a line saying
/// CoderHelm could not is read as an attempt, with its cause.
pub fn sink_outcome(item: &str) -> Outcome {
    // "Only if X: ..." is not a person's until X is found: CoderHelm checks
    // X first, and asks only then, without the condition.
    let lower = item.trim().to_lowercase();
    if lower.starts_with("only if ") {
        return Outcome::Do(Work::Own(format!("check first: {}", one_line(item))));
    }
    // A line the gate let through carries its reason's words.
    let split = item
        .split_once("CoderHelm could not:")
        .or_else(|| item.split_once("CoderHelm could not do it:"));
    if let Some((what, why)) = split {
        let a = Ask {
            kind: Kind::Attempt,
            text: one_line(what),
            why_not: one_line(why),
            ..Default::default()
        };
        return decide(&a, &Context::default());
    }
    line_outcome(Kind::Step, item)
}

/// What the sink check found in a text about to reach a person.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Sunk {
    /// The text with every item a person was not to get taken out (an empty
    /// "Needed from you" list says nothing is needed).
    pub text: String,
    /// The items taken out: CoderHelm's own work, sent back to the run.
    pub sent_back: Vec<String>,
    /// The items left for a person: each one the gate let through.
    pub human: Vec<String>,
}

impl Sunk {
    /// Whether anything had to be taken out: a defect upstream.
    pub fn defect(&self) -> bool {
        !self.sent_back.is_empty()
    }
}

/// The final check every person-facing text passes before it is posted:
/// each item addressed to a person goes through the gate again, and one a
/// person was not to get (CoderHelm can do it, something settles it, it
/// belongs to the review) is taken out and returned as work. Whatever wrote
/// the text, nothing doable reaches a person.
pub fn sink(text: &str) -> Sunk {
    let items = person_lines(text);
    if items.is_empty() {
        return Sunk {
            text: text.to_string(),
            ..Default::default()
        };
    }
    let mut drop: Vec<usize> = Vec::new();
    let mut sunk = Sunk::default();
    for (i, it) in &items {
        match sink_outcome(it) {
            Outcome::Human(_) => sunk.human.push(it.clone()),
            _ => {
                drop.push(*i);
                sunk.sent_back.push(it.clone());
            }
        }
    }
    let lines: Vec<&str> = text.lines().collect();
    let mut kept: Vec<String> = Vec::new();
    for (i, l) in lines.iter().enumerate() {
        if drop.contains(&i) {
            continue;
        }
        kept.push((*l).to_string());
    }
    // A "Needed from you" heading left with no item says nothing is needed.
    let mut out: Vec<String> = Vec::new();
    for (i, l) in kept.iter().enumerate() {
        if opens_needed(l) && !l.contains(crate::pr_steps::NOTHING_NEEDED) {
            let next_item = kept[i + 1..]
                .iter()
                .find(|n| !n.trim().is_empty())
                .is_some_and(|n| list_item(n).is_some());
            let same_line = l
                .split_once(':')
                .is_some_and(|(_, r)| !r.trim().trim_start_matches("**").trim().is_empty());
            if !next_item && !same_line {
                let head = l.trim_end().trim_end_matches(':').trim_end_matches("**");
                let head = head.trim_end_matches(':');
                if l.trim_start().starts_with('#') {
                    out.push(l.clone());
                    out.push(String::new());
                    out.push(crate::pr_steps::NOTHING_NEEDED.to_string());
                } else {
                    out.push(format!("{head}:** {}", crate::pr_steps::NOTHING_NEEDED));
                }
                continue;
            }
        }
        out.push(l.clone());
    }
    let mut t = out.join("\n");
    if text.ends_with('\n') {
        t.push('\n');
    }
    sunk.text = t;
    sunk
}

/// Whether a run may wait on a person: only with at least one item the
/// gate let through and nothing CoderHelm can still do itself.
pub fn may_wait_on_person(human: usize, doable: usize) -> bool {
    human > 0 && doable == 0
}

/// The prompt section that puts [`PRINCIPLE`] in front of a model that
/// writes for people.
pub fn prompt_section() -> String {
    format!("## Owning the work\n{PRINCIPLE}\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reading an end-to-end result or running a flow in browsers is
    /// CoderHelm's work, even when a missing permission stopped it.
    #[test]
    fn testing_is_never_a_persons_item() {
        let text = "**Needed from you:**\n\
1. Read the Deploy Preview e2e result. CoderHelm could not: CoderHelm's GitHub App is not allowed to start workflows in this repository\n\
2. Run the sign-up flow inside the frame in a fresh browser context in Chromium and WebKit. CoderHelm could not: CoderHelm's GitHub App is not allowed to start workflows in this repository\n";
        let sunk = sink(text);
        assert_eq!(sunk.sent_back.len(), 2, "{sunk:?}");
        assert!(sunk.human.is_empty(), "{sunk:?}");
        assert!(!sunk.text.contains("e2e"), "{}", sunk.text);
        // A real person item stays.
        let keep = "**Needed from you:**\n1. One paid registration on real devices. CoderHelm cannot: No real devices or payment method. (QA)\n";
        assert_eq!(sink(keep).human.len(), 1, "{:?}", sink(keep));
    }

    fn assumption(topic: &str, text: &str) -> Ask {
        Ask {
            kind: Kind::Assumption,
            topic: topic.into(),
            text: text.into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_registry_lookup_is_coderhelms() {
        let a = Ask::new(
            Kind::OpenItem,
            "Run npm view left-pad@4.18.0 maintainers dist.integrity and compare the result to the lockfile hash.",
        );
        match decide(&a, &Context::default()) {
            Outcome::Do(Work::Registry(p)) => assert_eq!(p, vec!["left-pad@4.18.0".to_string()]),
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn nine_wordings_of_a_ticket_settled_decision_are_one_settled_note() {
        let words = [
            "The ticket is an explicit, self-contained security-update request, so CoderHelm opens a new pull request applying the planned lockfile bumps.",
            "The ticket asks for every bump in one pull request; the earlier one was closed unmerged, so a new one is opened.",
            "A new pull request is opened because the ticket is an explicit request and the earlier one was closed unmerged.",
            "The ticket is an explicit request for the security updates; CoderHelm opens a new pull request for them.",
            "The earlier pull request was closed unmerged, so a fresh one carries the bumps the ticket asks for.",
            "Opening a new pull request: the ticket is an explicit, self-contained request.",
            "The ticket asks for all bumps together; a new pull request is opened since the earlier one closed unmerged.",
            "The ticket is explicit; CoderHelm applies the planned lockfile bumps on a new pull request.",
            "As the ticket asks, every bump goes in one new pull request.",
        ];
        let asks = words
            .iter()
            .map(|w| assumption("fix location", w))
            .collect();
        let g = gate(asks, &Context::default());
        assert_eq!(g.items.len(), 1, "{g:?}");
        assert_eq!(g.items[0].0.text, words[8], "latest wording kept");
        assert_eq!(g.items[0].1.class(), Class::Settled);
        assert!(g.human().is_empty());
        assert_eq!(g.evidence_lines().len(), 1);
    }

    #[test]
    fn a_reversible_choice_with_no_guidance_is_decided_and_noted() {
        let a = Ask {
            kind: Kind::Question,
            text: "Should the helper live in utils/ or next to the component?".into(),
            options: vec!["next to the component".into(), "utils/".into()],
            ..Default::default()
        };
        match decide(&a, &Context::default()) {
            Outcome::Decided(d) => {
                assert_eq!(
                    d.line(),
                    "Chose next to the component over utils/ because it is reversible and nothing in the ticket, repository or settings asks otherwise; comment to switch."
                );
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn page_copy_with_a_default_is_decided_never_asked() {
        let q = |w: &str| Ask {
            kind: Kind::Question,
            text: w.into(),
            ..Default::default()
        };
        let g = gate(
            vec![
                q("What H1 and intro copy should each of the four venue pages use? (Default: '{Venue} Summer Camp Registration' plus one sentence.)"),
                q("Which heading and intro sentence should the four venue pages show? (Default: '{Venue} Summer Camp Registration' plus one sentence.)"),
            ],
            &Context::default(),
        );
        assert_eq!(g.items.len(), 1, "the same question twice is one");
        match &g.items[0].1 {
            Outcome::Decided(d) => {
                assert!(d.chose.contains("Summer Camp Registration"), "{d:?}");
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn an_irreversible_customer_facing_choice_is_one_yes_no_on_the_recommendation() {
        let a = Ask {
            kind: Kind::Question,
            text: "Should the migration email customers whose bookings move to the new venue?"
                .into(),
            options: vec!["email them once".into(), "send nothing".into()],
            ..Default::default()
        };
        match decide(&a, &Context::default()) {
            Outcome::Human(h) => {
                assert!(h.yes_no());
                assert_eq!(h.recommendation(), "email them once");
                let l = h.line();
                assert!(
                    l.starts_with("Yes or no: Should the migration email customers"),
                    "{l}"
                );
                assert!(l.contains("has done the work that way"), "{l}");
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn a_permission_only_an_owner_grants_reaches_a_person_with_one_action() {
        let a = Ask {
            kind: Kind::Step,
            text: "Start the release workflow".into(),
            how: "Grant the app \"Actions: write\" on the repository".into(),
            why_not: "CoderHelm's GitHub App is not allowed to start workflows in this repository"
                .into(),
            owner: "an organization owner".into(),
            ..Default::default()
        };
        match decide(&a, &Context::default()) {
            Outcome::Human(h) => {
                assert_eq!(h.reason(), Reason::Grant);
                assert_eq!(h.who(), "an organization owner");
                assert!(keeps_the_rules(&h.line()).is_ok());
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn a_deploy_coderhelm_holds_credentials_for_is_run_else_one_missing_thing() {
        let a = Ask {
            kind: Kind::Step,
            text: "Deploy the staging Studio".into(),
            how: "cd apps/studio && npm run deploy:staging".into(),
            why_not: "the repository has no deploy workflow CoderHelm can start".into(),
            ..Default::default()
        };
        let ctx = Context {
            can_deploy: true,
            ..Default::default()
        };
        assert!(matches!(decide(&a, &ctx), Outcome::Do(Work::Command(_))));
        match decide(&a, &Context::default()) {
            Outcome::Human(h) => {
                let l = h.line();
                assert_eq!(l.matches("npm run deploy:staging").count(), 1, "{l}");
                assert_eq!(h.reason(), Reason::Credential);
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn a_look_is_folded_into_the_review_and_owned_work_is_done() {
        let look = Ask {
            kind: Kind::Step,
            text: "Look at Studio: the landing document, hero Sub Title field".into(),
            why_not: "how a page looks is a judgement only a person can make".into(),
            ..Default::default()
        };
        assert_eq!(decide(&look, &Context::default()).class(), Class::Review);
        let own = Ask::new(
            Kind::OpenItem,
            "Missing from the end-to-end checklist: no step lists the 23 documents over the limit. Settle: Add a checklist step owned by CoderHelm to query and post all 23 over-limit documents. Only a person can settle this: a product decision.",
        );
        assert_eq!(decide(&own, &Context::default()).class(), Class::Do);
    }

    #[test]
    fn nothing_written_for_a_person_edits_coderhelms_text_or_is_cut() {
        assert!(keeps_the_rules("Correct it in a comment; otherwise delete this line.").is_err());
        assert!(keeps_the_rules("The ticket is an explicit, self-contained…").is_err());
        assert!(keeps_the_rules("Feedback wanted: does the evidence beside each jump").is_err());
        assert!(keeps_the_rules("Grant the app Actions: write.").is_ok());
    }

    #[test]
    fn the_model_can_only_lower_or_merge() {
        let mut g = gate(
            vec![Ask {
                kind: Kind::Step,
                text: "Add the PAYMENTS_KEY secret".into(),
                why_not: "it needs the API key for the payments sandbox".into(),
                ..Default::default()
            }],
            &Context::default(),
        );
        assert_eq!(g.human().len(), 1);
        let h = match &g.items[0].1 {
            Outcome::Human(h) => h.clone(),
            _ => unreachable!(),
        };
        g.demote(0, Outcome::Human(h));
        assert_eq!(g.human().len(), 1);
        g.demote(0, Outcome::Do(Work::Own("x".into())));
        assert!(g.human().is_empty());
    }
}
