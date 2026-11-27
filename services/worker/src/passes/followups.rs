//! The worker's side of follow-up tickets (`common::followup_tickets`) and
//! of "never ask to connect what is connected" (`common::connected_access`).
//!
//! - [`planner_caps`]: the capabilities the planner reads, with the team's
//!   ticket tracker in them, so a step on Jira is CoderHelm's.
//! - [`reaches`]: what the run reaches, for the stop judge's picture.
//! - [`prepare`]: once the plan is written, reads the drafts the ticket's
//!   deliverable asks for and turns the plan's filing steps (and any
//!   "connect Jira" step) into CoderHelm's. Nothing is filed yet: a plan a
//!   later check rejects files nothing.
//! - [`file`]: once the plan is final (it passed every check, and the run
//!   ends with it or implements it), files the drafts (in the source
//!   ticket's Jira project, else as GitHub issues, else in the comment),
//!   once per topic: the follow-ups already filed for the source (CoderHelm's
//!   records, its comment on the source, the tracker's label search) are
//!   read first, and a draft one of them covers is not filed again (its
//!   added scope goes on that ticket). One comment lists the keys.
//! - [`delivers_findings`], [`nothing_left`] and [`finish_delivered`]: a
//!   no-code run whose deliverable is its findings (an audit, a question),
//!   or whose only open work was the filing, completes instead of waiting
//!   on a person or planning again.

use aws_sdk_dynamodb::types::AttributeValue;
use common::connected_access::{self, Reach};
use common::followup_tickets::{self as ft, Draft, Filed, Linked, Tracker};
use common::run_ending::EndWrite;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, warn};

use super::fix_location::{FixDecision, FixLocation, TicketIntent};
use super::{attr_n, attr_s, RunUsageValues};
use crate::agent::provider::ModelProvider;
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TicketSource, TokenUsage};
use crate::WorkerState;
use common::followup_tickets::{Existing, Near};

/// The settings record of one filed draft.
pub fn record_sk(source: &str, draft_id: &str) -> String {
    format!("FOLLOWUP#{}#{draft_id}", source.trim().to_ascii_uppercase())
}

/// How long one call to the Jira app may take (the hybrid proxy gives the
/// site 25 seconds).
const CALL: Duration = Duration::from_secs(30);

/// The team's Jira app, as far as follow-ups care.
#[derive(Clone, Default)]
pub(crate) struct Jira {
    pub(crate) connected: bool,
    pub(crate) create_url: String,
    pub(crate) secret: String,
    pub(crate) site: String,
    /// The team's Jira trigger label (`coderhelm` unless it set another): a
    /// follow-up never carries it.
    pub(crate) trigger_label: String,
}

pub(crate) async fn jira(state: &WorkerState, team_id: &str) -> Jira {
    let item: Option<HashMap<String, AttributeValue>> = state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s("JIRA#config"))
        .send()
        .await
        .ok()
        .and_then(|o| o.item);
    let text = |k: &str| {
        item.as_ref()
            .and_then(|i| i.get(k))
            .and_then(|v| v.as_s().ok())
            .map(|s| s.trim().to_string())
            .unwrap_or_default()
    };
    let conn = common::jira_connection::Connection::from_config(item.as_ref(), false);
    Jira {
        connected: conn.connected(),
        create_url: Some(text("create_ticket_url"))
            .filter(|u| u.starts_with("https://"))
            .unwrap_or_default(),
        secret: text("forge_secret"),
        site: conn.site.clone(),
        trigger_label: Some(text("trigger_label"))
            .filter(|l| !l.is_empty())
            .unwrap_or_else(|| "coderhelm".to_string()),
    }
}

/// The source of the ticket this run works, when the ticket is itself a
/// follow-up CoderHelm filed: what the gateway said (its labels or
/// CoderHelm's record), else CoderHelm's record of the key, else the
/// sentence every follow-up's description ends with. `None` for any other
/// ticket.
pub async fn followup_source(state: &WorkerState, msg: &TicketMessage) -> Option<String> {
    if let Some(src) = &msg.followup_of {
        return Some(src.clone());
    }
    if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.trim().is_empty() {
        let item = state
            .dynamo
            .get_item()
            .table_name(&state.config.settings_table_name)
            .key("pk", attr_s(&msg.team_id))
            .key("sk", attr_s(&ft::of_record_sk(&msg.ticket_id)))
            .send()
            .await
            .ok()
            .and_then(|o| o.item);
        if let Some(item) = item {
            return Some(
                item.get("source")
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default(),
            );
        }
    }
    ft::source_of_body(&msg.body)
}

/// Takes one of today's follow-up slots for `source`
/// ([`ft::DAILY_CAP_PER_SOURCE`]), atomically across runs. False when the
/// day's slots are taken, or the count can't be read (nothing is filed
/// then: a cascade is worse than a late ticket).
async fn take_daily_slot(state: &WorkerState, team_id: &str, source: &str) -> bool {
    let day = chrono::Utc::now().format("%Y-%m-%d").to_string();
    state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&ft::day_record_sk(source, &day)))
        .update_expression("ADD filed :one")
        .condition_expression("attribute_not_exists(filed) OR filed < :cap")
        .expression_attribute_values(":one", attr_n(1))
        .expression_attribute_values(":cap", attr_n(ft::DAILY_CAP_PER_SOURCE as i64))
        .send()
        .await
        .map_err(|e| {
            warn!(source, error = %e, "Follow-up daily count not taken: nothing filed");
        })
        .is_ok()
}

/// What the run reaches through the team's connections: Jira when the app
/// is connected.
pub async fn reaches(state: &WorkerState, team_id: &str) -> Vec<Reach> {
    let j = jira(state, team_id).await;
    if j.connected {
        vec![connected_access::jira(&j.site, !j.create_url.is_empty())]
    } else {
        Vec::new()
    }
}

/// The capabilities the planner reads: the standard catalogue and the
/// team's ticket tracker.
pub async fn planner_caps(
    state: &WorkerState,
    msg: &TicketMessage,
    github: &GitHubClient,
) -> Vec<common::host_steps::Capability> {
    let mut caps = super::skill_run::standard_caps();
    let j = jira(state, &msg.team_id).await;
    let tracker = if j.connected {
        common::host_steps::tracker(Some((&j.site, !j.create_url.is_empty())), false)
    } else {
        let issues = github.permission_state("issues").await.token_has != Some(false);
        common::host_steps::tracker(None, issues)
    };
    caps.push(tracker);
    caps.sort_by(|a, b| a.id.cmp(&b.id));
    caps
}

/// The source ticket's id as follow-ups name it: the Jira key, or `#N` for
/// a GitHub issue.
fn source_of(msg: &TicketMessage) -> String {
    if matches!(msg.source, TicketSource::Github) && msg.issue_number > 0 {
        format!("#{}", msg.issue_number)
    } else {
        msg.ticket_id.trim().to_string()
    }
}

/// What [`settle`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settled {
    pub drafts: Vec<Draft>,
    pub filed: Vec<Filed>,
    pub tracker: Tracker,
    /// The comment posted on the source ticket (also the run's record).
    pub comment: String,
    /// One line for the plan's step and the run page.
    pub line: String,
}

impl Settled {
    pub fn all_filed(&self) -> bool {
        !self.filed.is_empty() && self.filed.iter().all(|f| f.key().is_some())
    }
}

/// Whether the plan asks for the drafts to be filed: its own
/// `followup_tickets`, or a step (outside, end to end, the next step) that
/// files follow-up tickets.
pub fn asks_to_file(d: &FixDecision) -> bool {
    !d.followup_tickets.is_empty()
        || d.outside_change.as_ref().is_some_and(|c| {
            ft::is_filing_step("", &c.change)
                || c.steps
                    .iter()
                    .any(|s| ft::is_filing_step(&s.tool, &format!("{} {}", s.step, s.how)))
        })
        || d.end_to_end.iter().any(|e| ft::is_filing_step("", &e.step))
        || d.next_step
            .as_ref()
            .is_some_and(|n| ft::is_filing_step("", &n.action))
}

/// The plan's person steps, questions and next step that ask a person to
/// connect a system the run reaches become CoderHelm's (or go). Returns how
/// many it changed.
pub fn drop_connect_asks(d: &mut FixDecision, reaches: &[Reach]) -> usize {
    if reaches.is_empty() {
        return 0;
    }
    let mut n = 0;
    if let Some(c) = d.outside_change.as_mut() {
        for s in c.steps.iter_mut().filter(|s| s.by.trim() == "person") {
            let words = format!("{} {} {}", s.why_person, s.grant, s.how);
            if let Some(r) = connected_access::asks_to_connect(&words, reaches) {
                s.by = "coderhelm".into();
                s.owner = "CoderHelm".into();
                s.reason.clear();
                s.why_person.clear();
                s.grant.clear();
                if s.result.trim().is_empty() {
                    s.result =
                        format!("{} is connected; CoderHelm does this through it.", r.system);
                }
                n += 1;
            }
        }
    }
    if let Some(next) = d.next_step.as_ref() {
        if connected_access::asks_to_connect(
            &format!("{} {}", next.action, next.why_person),
            reaches,
        )
        .is_some()
        {
            d.next_step = None;
            n += 1;
        }
    }
    let before = d.questions.len();
    d.questions
        .retain(|q| connected_access::asks_to_connect(q, reaches).is_none());
    n + (before - d.questions.len())
}

/// The plan after the drafts were filed: its filing steps are CoderHelm's,
/// done when every draft was filed, with `line` as their result; a next
/// step or question about filing goes; an outside change that was only the
/// filing is done.
pub fn mark_filed(d: &mut FixDecision, line: &str, all_filed: bool) {
    let mut drop_change = false;
    if let Some(c) = d.outside_change.as_mut() {
        for s in c.steps.iter_mut() {
            if ft::is_filing_step(&s.tool, &format!("{} {}", s.step, s.how)) {
                s.by = "coderhelm".into();
                s.owner = "CoderHelm".into();
                s.tool = ft::TOOL.into();
                s.reason.clear();
                s.why_person.clear();
                s.grant.clear();
                s.done = all_filed;
                s.result = line.to_string();
            }
        }
        let open_for_person = c.steps.iter().any(|s| s.by.trim() == "person" && !s.done);
        drop_change = all_filed && ft::is_filing_step("", &c.change) && !open_for_person;
    }
    if drop_change {
        d.outside_change = None;
    }
    for e in d.end_to_end.iter_mut() {
        if ft::is_filing_step("", &e.step) {
            e.by = "coderhelm".into();
            e.owner = "CoderHelm".into();
        }
    }
    if d.next_step
        .as_ref()
        .is_some_and(|n| ft::is_filing_step("", &n.action))
    {
        d.next_step = None;
    }
    d.questions.retain(|q| !ft::is_filing_step("", q));
}

/// Whether a plan that codes nothing leaves nothing for a person once its
/// follow-ups are filed: no unanswered question of its own, no next step
/// for a person, no person step left open, and not a bug whose fix's place
/// is still unknown.
pub fn nothing_left(
    d: &FixDecision,
    tasks: &str,
    answers: &[common::run_questions::Answer],
) -> bool {
    let codes_nothing = tasks
        .trim_start()
        .starts_with(super::fix_location::NO_CODE_CHANGE);
    let asks = !common::run_questions::unanswered(
        common::run_questions::from_lines(&d.questions),
        answers,
    )
    .is_empty();
    let next = d.next_step.as_ref().is_some_and(|n| n.is_usable());
    let person = d
        .outside_change
        .as_ref()
        .is_some_and(|c| c.steps.iter().any(|s| s.by.trim() == "person" && !s.done));
    let unknown_bug = d.intent == TicketIntent::Bug && d.fix_location == FixLocation::Unknown;
    codes_nothing && !asks && !next && !person && !unknown_bug
}

/// Whether the plan's deliverable is its findings, so the run ends
/// delivered with them on the ticket (and its follow-ups filed): a plan
/// that codes nothing, asks nothing of its own, and either answers a
/// question (an audit, a review, a scoping: what it found is the work, and
/// a step it leaves for a person is a recommendation, not a wait) or files
/// follow-ups and leaves nothing for a person ([`nothing_left`]). Such a
/// plan is never "a change outside the code that leaves nothing for a
/// person": it is done.
pub fn delivers_findings(
    d: &FixDecision,
    tasks: &str,
    answers: &[common::run_questions::Answer],
    files_followups: bool,
) -> bool {
    let codes_nothing = tasks
        .trim_start()
        .starts_with(super::fix_location::NO_CODE_CHANGE);
    if !codes_nothing {
        return false;
    }
    let asks = !common::run_questions::unanswered(
        common::run_questions::from_lines(&d.questions),
        answers,
    )
    .is_empty();
    let found = !d.cause.trim().is_empty()
        || !tasks
            .trim_start()
            .trim_start_matches(super::fix_location::NO_CODE_CHANGE)
            .trim()
            .is_empty();
    let answers_a_question = d.intent == TicketIntent::Question && found;
    !asks && (answers_a_question || (files_followups && nothing_left(d, tasks, answers)))
}

/// Whether a cached plan was its ticket's deliverable, so a new run of the
/// unchanged ticket takes it up instead of planning again: it codes
/// nothing, carries its decision, and drafts follow-up tickets (its own
/// list, or proposal.md's) for findings it states.
pub fn reusable_findings(p: &super::plan::PlanResult) -> bool {
    let Some(d) = p.fix.as_ref() else {
        return false;
    };
    p.tasks
        .trim_start()
        .starts_with(super::fix_location::NO_CODE_CHANGE)
        && !p.proposal.trim().is_empty()
        && (d.intent == TicketIntent::Question || asks_to_file(d))
        && !ft::drafts(&d.followup_tickets, &p.proposal).is_empty()
}

/// The plan's follow-ups, ready to file once the plan is final ([`file`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Due {
    pub drafts: Vec<Draft>,
}

/// The filing steps' result while the plan still has checks to pass.
pub const DUE_LINE: &str =
    "CoderHelm files these follow-up tickets once the plan has passed its checks";

/// The pure part of [`prepare`]: the plan's "connect" asks for what the run
/// reaches go, and when the plan asks for follow-ups, its drafts are read
/// and its filing steps become CoderHelm's. Returns how many asks were
/// dropped, and the drafts when there are any to file.
pub fn prepare_plan(d: &mut FixDecision, reach: &[Reach], proposal: &str) -> (usize, Option<Due>) {
    let dropped = drop_connect_asks(d, reach);
    if !asks_to_file(d) {
        return (dropped, None);
    }
    let drafts = ft::drafts(&d.followup_tickets, proposal);
    if drafts.is_empty() {
        return (dropped, None);
    }
    mark_filed(d, DUE_LINE, true);
    (dropped, Some(Due { drafts }))
}

/// Reads the plan's follow-ups and makes its filing steps CoderHelm's.
/// Nothing is filed or posted here: the plan may still be rejected by a
/// later check, and a rejected plan files nothing. `None` when the plan
/// files nothing; the plan's "connect Jira" asks are dropped either way.
pub async fn prepare(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    d: &mut FixDecision,
    proposal: &str,
) -> Option<Due> {
    let reach = reaches(state, &msg.team_id).await;
    let (dropped, due) = prepare_plan(d, &reach, proposal);
    if dropped > 0 {
        info!(
            run_id,
            dropped,
            "The plan asked a person to connect a system CoderHelm reaches: CoderHelm's now"
        );
    }
    due
}

/// The small model check on drafts near a filed follow-up.
pub struct Judge<'a> {
    pub provider: &'a ModelProvider,
    pub usage: &'a mut TokenUsage,
}

/// Files the plan's follow-ups, once per topic, and settles the plan's
/// steps. Called only on the plan the run ends with or implements, so an
/// intermediate plan never files. Before filing, the follow-ups already
/// filed for the source are read ([`filed_before`]); a draft whose topic one
/// covers ([`ft::near`], and the small check for the near ones) is not
/// filed again, and the scope it adds is said on that ticket.
pub async fn file(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &TicketMessage,
    run_id: &str,
    d: &mut FixDecision,
    due: Due,
    judge: Option<Judge<'_>>,
) -> Settled {
    let drafts = due.drafts;
    let source = source_of(msg);
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let j = jira(state, &msg.team_id).await;
    let jira_source = matches!(msg.source, TicketSource::Jira) && !j.create_url.is_empty();
    let issues = github.permission_state("issues").await.token_has;
    let mut tracker = ft::tracker(&source, jira_source, &j.site, &repo, issues);
    if matches!(tracker, Tracker::Github { .. }) && msg.repo_owner.is_empty() {
        tracker = Tracker::Comment("the run has no repository to open issues in".into());
    }
    // Depth one: a run on a follow-up files none of its own, so follow-ups
    // never cascade. Its drafts go in its comment.
    if let Some(of) = followup_source(state, msg).await {
        info!(run_id, of = %of, "The ticket is a follow-up: no follow-ups filed from it");
        tracker = Tracker::Comment(ft::depth_refusal(&of));
    }
    // The team's agent permissions decide, as for every write CoderHelm
    // makes outside the pull request.
    if !matches!(tracker, Tracker::Comment(_)) {
        let args = json!({
            "source": source,
            "tracker": match &tracker {
                Tracker::Jira { project, .. } => format!("jira:{project}"),
                Tracker::Github { repo } => format!("github:{repo}"),
                Tracker::Comment(_) => String::new(),
            },
            "tickets": drafts.len(),
            "drafts": common::content_hash(
                &drafts.iter().map(|d| ft::normal_summary(&d.summary)).collect::<Vec<_>>().join("\n")
            ),
        });
        let ctx =
            common::agent_policy::GateCtx::new(&msg.team_id, common::agent_policy::Agent::Coder)
                .repo(&repo)
                .run(run_id);
        let outcome = crate::agent::policy::gate(state, ctx, None)
            .check(common::agent_policy::Request {
                tool: ft::TOOL,
                args: &args,
                why: "file the follow-up tickets this ticket asks for",
            })
            .await;
        match outcome {
            common::agent_policy::Outcome::Run { .. } => {}
            common::agent_policy::Outcome::Refused { message, .. } => {
                tracker = Tracker::Comment(format!(
                    "the team's agent permissions don't let CoderHelm file tickets ({})",
                    one_line(&message)
                ));
            }
            common::agent_policy::Outcome::Pending { .. } => {
                tracker = Tracker::Comment(
                    "the approval the team's agent permissions ask for filing tickets wasn't given in time"
                        .into(),
                );
            }
            common::agent_policy::Outcome::Stop(s) => {
                tracker = Tracker::Comment(one_line(&s.note()));
            }
        }
    }
    let run_url = format!(
        "{}/runs/detail?id={run_id}",
        common::public_urls::app_url(&state.config.stage)
    );
    // What is already filed for the source, and where each draft stands.
    let mut places: Vec<Place> = Vec::new();
    if !matches!(tracker, Tracker::Comment(_)) {
        let existing = filed_before(state, github, msg, &tracker, &j, &source).await;
        let mut asks: Vec<(usize, Vec<Existing>)> = Vec::new();
        for (i, draft) in drafts.iter().enumerate() {
            let sk = record_sk(&source, &ft::draft_id(&source, draft));
            if let Some(k) = kept(state, &msg.team_id, &sk).await {
                places.push(Place::Kept(k));
                continue;
            }
            match ft::near(draft, &existing) {
                Near::Same(e) => places.push(Place::Covered(e.clone())),
                Near::Maybe(c) => {
                    asks.push((i, c.into_iter().cloned().collect()));
                    places.push(Place::New);
                }
                Near::New => places.push(Place::New),
            }
        }
        if !asks.is_empty() {
            let answers = same_topic(state, &drafts, &asks, judge).await;
            for ((i, cands), covered_by) in asks.iter().zip(answers) {
                if let Some(key) = covered_by {
                    if let Some(e) = cands.iter().find(|e| e.key == key) {
                        places[*i] = Place::Covered(e.clone());
                    }
                }
            }
        }
    }
    let mut filed: Vec<Filed> = Vec::new();
    let mut stop: Option<String> = None;
    for (i, draft) in drafts.iter().enumerate() {
        if let Some(why) = &stop {
            filed.push(Filed::NotFiled(why.clone()));
            continue;
        }
        let sk = record_sk(&source, &ft::draft_id(&source, draft));
        let one = match (&tracker, places.get(i)) {
            (Tracker::Comment(_), _) => break,
            (_, Some(Place::Kept(k))) => k.filed(),
            (t, Some(Place::Covered(e))) => {
                cover(state, github, msg, run_id, t, &source, &sk, draft, e).await
            }
            (t, _) => {
                file_one(
                    state, github, msg, run_id, t, &j, &source, &repo, &run_url, draft,
                )
                .await
            }
        };
        // A refusal that would refuse every draft the same way stops there.
        if let Filed::NotFiled(why) = &one {
            if why.contains("lacks the Jira project permission")
                || why.contains("didn't accept CoderHelm's shared secret")
                || why == &ft::daily_cap_refusal(&source)
            {
                stop = Some(why.clone());
            }
        }
        filed.push(one);
    }
    let comment = ft::comment(&drafts, &filed, &tracker);
    let line = settled_line(&drafts, &filed, &tracker);
    post_comment(state, github, msg, &comment).await;
    super::add_progress_note(state, &msg.team_id, run_id, &line).await;
    let new = filed.iter().filter(|f| f.is_new()).count();
    let settled = Settled {
        drafts,
        filed,
        tracker,
        comment,
        line,
    };
    mark_filed(d, &settled.line, settled.all_filed());
    info!(run_id, new, "Follow-up tickets settled");
    settled
}

/// The one line for the plan's step and the run page: what was filed new,
/// what earlier tickets cover, and what wasn't filed and why.
pub fn settled_line(drafts: &[Draft], filed: &[Filed], tracker: &Tracker) -> String {
    let new: Vec<&str> = filed
        .iter()
        .filter(|f| f.is_new())
        .filter_map(Filed::key)
        .collect();
    let earlier: Vec<&str> = filed
        .iter()
        .filter(|f| f.key().is_some() && !f.is_new())
        .filter_map(Filed::key)
        .collect();
    if new.is_empty() && earlier.is_empty() {
        return format!(
            "Not filed: {}. The drafts are on the ticket.",
            match tracker {
                Tracker::Comment(why) => why.clone(),
                _ => filed
                    .iter()
                    .find_map(|f| match f {
                        Filed::NotFiled(w) => Some(w.clone()),
                        _ => None,
                    })
                    .unwrap_or_default(),
            }
        );
    }
    let mut earlier_keys: Vec<&str> = Vec::new();
    for k in earlier {
        if !earlier_keys.contains(&k) {
            earlier_keys.push(k);
        }
    }
    let mut parts = Vec::new();
    if !new.is_empty() {
        parts.push(format!(
            "Filed {} of {} follow-up tickets: {}",
            new.len(),
            drafts.len(),
            new.join(", ")
        ));
    } else {
        parts.push(format!(
            "No new follow-up tickets: the {} drafts are covered",
            drafts.len()
        ));
    }
    if !earlier_keys.is_empty() {
        parts.push(format!(
            "already filed from this ticket: {}",
            earlier_keys.join(", ")
        ));
    }
    parts.join("; ")
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Where one draft stands before filing.
#[derive(Debug, Clone)]
enum Place {
    /// An earlier run kept a record for this very draft.
    Kept(KeptRecord),
    /// A follow-up already filed covers its topic.
    Covered(Existing),
    /// Filed now (the app still finds a ticket with the same summary).
    New,
}

/// What an earlier run kept for one draft.
#[derive(Debug, Clone)]
struct KeptRecord {
    key: String,
    url: String,
    /// The draft was not filed: `key` covered it.
    covers: bool,
}

impl KeptRecord {
    fn filed(&self) -> Filed {
        if self.covers {
            Filed::Covered {
                key: self.key.clone(),
                url: self.url.clone(),
                title: String::new(),
            }
        } else {
            Filed::Ticket {
                key: self.key.clone(),
                url: self.url.clone(),
                existing: true,
                linked: Linked::Yes,
            }
        }
    }
}

/// What an earlier run kept for this draft, if anything.
async fn kept(state: &WorkerState, team_id: &str, sk: &str) -> Option<KeptRecord> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(sk))
        .send()
        .await
        .ok()?
        .item?;
    let s = |k: &str| item.get(k).and_then(|v| v.as_s().ok()).cloned();
    Some(KeptRecord {
        key: s("key").filter(|k| !k.is_empty())?,
        url: s("url").unwrap_or_default(),
        covers: s("covers").is_some_and(|c| c == "1"),
    })
}

/// The settings record of one draft: the ticket it was filed as (or the
/// one that covers it), with the draft's summary and key files, so the
/// next run reads what this one filed by topic.
pub struct Record<'a> {
    pub sk: &'a str,
    pub key: &'a str,
    pub url: &'a str,
    pub draft: &'a Draft,
    pub covers: bool,
}

/// The attribute holding a record's key files and components.
const MARKS_ATTR: &str = "marks";

async fn keep(state: &WorkerState, msg: &TicketMessage, run_id: &str, r: Record<'_>) {
    let marks = ft::marks(&format!("{}\n{}", r.draft.summary, r.draft.description))
        .into_iter()
        .collect::<Vec<_>>()
        .join(",");
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&msg.team_id))
        .item("sk", attr_s(r.sk))
        .item("key", attr_s(r.key))
        .item("url", attr_s(r.url))
        .item("ticket_id", attr_s(&msg.ticket_id))
        .item("summary", attr_s(r.draft.summary.trim()))
        .item(MARKS_ATTR, attr_s(&marks))
        .item("covers", attr_s(if r.covers { "1" } else { "0" }))
        .item("run_id", attr_s(run_id))
        .item("filed_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
    {
        warn!(key = r.key, error = %e, "Could not keep a filed follow-up (a retry finds it in the tracker)");
    }
}

/// A record as an already filed follow-up. A record of a covered draft
/// names the ticket that covers it, not that ticket's summary.
pub fn existing_of_record(item: &HashMap<String, AttributeValue>) -> Option<Existing> {
    let s = |k: &str| {
        item.get(k)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    };
    let key = s("key");
    if key.trim().is_empty() {
        return None;
    }
    let covers = s("covers") == "1";
    Some(Existing {
        key,
        url: s("url"),
        summary: if covers { String::new() } else { s("summary") },
        marks: if covers {
            Vec::new()
        } else {
            s(MARKS_ATTR)
                .split(',')
                .map(str::trim)
                .filter(|m| !m.is_empty())
                .map(str::to_string)
                .collect()
        },
    })
}

/// The follow-ups already filed for `source`: CoderHelm's records of them,
/// the comment it posted on the source (for records kept before they held
/// summaries), and the tracker's label search (the Jira app's list, or the
/// repository's labelled issues).
async fn filed_before(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &TicketMessage,
    tracker: &Tracker,
    j: &Jira,
    source: &str,
) -> Vec<Existing> {
    let prefix = format!("FOLLOWUP#{}#", source.trim().to_ascii_uppercase());
    let mut records: Vec<Existing> = Vec::new();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    loop {
        let page = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :s)")
            .expression_attribute_values(":p", attr_s(&msg.team_id))
            .expression_attribute_values(":s", attr_s(&prefix))
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let Ok(page) = page else { break };
        records.extend(page.items().iter().filter_map(existing_of_record));
        match page.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    // What CoderHelm said on the source: its follow-ups comment.
    let said = match msg.source {
        TicketSource::Jira => said_followups(state, &msg.team_id, &format!("JIRA#{source}")).await,
        _ => Vec::new(),
    };
    let found = match tracker {
        Tracker::Jira { project, .. } => jira_list(state, j, project, source).await,
        Tracker::Github { repo } => {
            let (owner, name) = repo.split_once('/').unwrap_or((repo.as_str(), ""));
            let label = ft::source_label(source);
            github
                .issues_with_label(owner, name, &label)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|(n, t, url)| Existing {
                    key: format!("#{n}"),
                    url,
                    summary: t,
                    marks: Vec::new(),
                })
                .collect()
        }
        Tracker::Comment(_) => Vec::new(),
    };
    // A record whose ticket nothing else names (one kept before records
    // held summaries) still counts as filed, with no topic to compare.
    ft::merge_existing(ft::merge_existing(records, said), found)
}

/// The follow-up tickets CoderHelm's own comment on a ticket lists (its
/// `followups` slot, kept with what CoderHelm said there).
async fn said_followups(state: &WorkerState, team_id: &str, place: &str) -> Vec<Existing> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(team_id))
        .key(
            "sk",
            attr_s(&format!("{}{place}", common::conversation::SK_PREFIX)),
        )
        .send()
        .await
        .ok()
        .and_then(|o| o.item);
    let doc = item
        .as_ref()
        .and_then(|i| i.get("doc"))
        .and_then(|v| v.as_s().ok())
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .unwrap_or_default();
    said_followups_of(&doc)
}

/// Pure: the follow-ups listed in a kept conversation (`doc`, the list of
/// what CoderHelm said): every `followups` part and its history.
pub fn said_followups_of(doc: &Value) -> Vec<Existing> {
    let mut out: Vec<Existing> = Vec::new();
    for said in doc.as_array().into_iter().flatten() {
        let meta = &said["meta"];
        let mut texts: Vec<&str> = Vec::new();
        if meta["slot"].as_str() == Some("followups") {
            texts.extend(said["body"].as_str());
        }
        for p in meta["parts"].as_array().into_iter().flatten() {
            if p["slot"].as_str() == Some("followups") {
                texts.extend(p["body"].as_str());
            }
        }
        for t in texts {
            out = ft::merge_existing(out, ft::titles_from_comment(t));
        }
    }
    out
}

/// The follow-ups the Jira app finds filed for `source` in `project` (its
/// list path). An app older than that path refuses the empty summary with
/// a 400 and files nothing; the list is then empty.
async fn jira_list(state: &WorkerState, j: &Jira, project: &str, source: &str) -> Vec<Existing> {
    if j.create_url.is_empty() {
        return Vec::new();
    }
    let body = json!({
        "projectKey": project,
        "summary": "",
        "forge_secret": j.secret,
        "followup": { "source": source, "list": true },
    });
    let Ok(r) = state
        .http
        .post(&j.create_url)
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .timeout(CALL)
        .send()
        .await
    else {
        return Vec::new();
    };
    if !r.status().is_success() {
        return Vec::new();
    }
    let v: Value = r.json().await.unwrap_or_default();
    jira_listed(&v, &j.site)
}

/// Pure: the app's list answer (`{followup: 1, issues: [{key, summary,
/// description}]}`) as filed follow-ups.
pub fn jira_listed(v: &Value, site: &str) -> Vec<Existing> {
    if v["followup"].as_u64() != Some(1) {
        return Vec::new();
    }
    v["issues"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|i| {
            let key = i["key"]
                .as_str()
                .filter(|k| common::git_conventions::is_ticket_key(k))?;
            Some(Existing {
                key: key.to_string(),
                url: if site.is_empty() {
                    String::new()
                } else {
                    format!("{}/browse/{key}", site.trim_end_matches('/'))
                },
                summary: i["summary"].as_str().unwrap_or("").to_string(),
                marks: ft::marks(i["description"].as_str().unwrap_or(""))
                    .into_iter()
                    .collect(),
            })
        })
        .collect()
}

/// The small check on the drafts near a filed follow-up: for each, the key
/// of the one that covers it, or `None`. Without a model (or when its
/// answer doesn't read), the closest candidate covers it
/// ([`ft::read_same_topic`]).
async fn same_topic(
    state: &WorkerState,
    drafts: &[Draft],
    asks: &[(usize, Vec<Existing>)],
    judge: Option<Judge<'_>>,
) -> Vec<Option<String>> {
    let pairs: Vec<(&Draft, Vec<&Existing>)> = asks
        .iter()
        .filter_map(|(i, c)| Some((drafts.get(*i)?, c.iter().collect())))
        .collect();
    let answer = match judge {
        Some(Judge { provider, usage }) => crate::agent::provider::converse_simple(
            state,
            provider,
            provider.primary_model_id(),
            ft::SAME_TOPIC_SYSTEM,
            &ft::same_topic_prompt(&pairs),
            usage,
        )
        .await
        .ok(),
        None => None,
    };
    ft::read_same_topic(answer.as_deref(), &pairs)
}

/// A draft an already filed follow-up covers: not filed. When the draft
/// names files or components that ticket doesn't, that scope is said on
/// the ticket instead. Kept, so a retry reads it as covered.
#[allow(clippy::too_many_arguments)]
async fn cover(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &TicketMessage,
    run_id: &str,
    tracker: &Tracker,
    source: &str,
    sk: &str,
    draft: &Draft,
    e: &Existing,
) -> Filed {
    let added = ft::added_scope(draft, e);
    if !added.is_empty() {
        let text = ft::scope_comment(draft, &added, source);
        let say = common::conversation::Say::status(
            &format!("followup-scope-{}", ft::draft_id(source, draft)),
            text,
        );
        let sent = match tracker {
            Tracker::Jira { .. } => super::conversation::jira(
                state,
                &msg.team_id,
                &e.key,
                say,
                "info",
                "More scope from a later reading",
            )
            .await
            .map(|_| ()),
            Tracker::Github { repo } => match (
                repo.split_once('/'),
                e.key.trim_start_matches('#').parse::<u64>(),
            ) {
                (Some((owner, name)), Ok(n)) => {
                    super::conversation::github(github, owner, name, n, say)
                        .await
                        .map(|_| ())
                }
                _ => Ok(()),
            },
            Tracker::Comment(_) => Ok(()),
        };
        if let Err(err) = sent {
            warn!(key = %e.key, error = %err, "Could not say the added scope on the follow-up");
        }
    }
    keep(
        state,
        msg,
        run_id,
        Record {
            sk,
            key: &e.key,
            url: &e.url,
            draft,
            covers: true,
        },
    )
    .await;
    Filed::Covered {
        key: e.key.clone(),
        url: e.url.clone(),
        title: e.summary.clone(),
    }
}

/// Files one draft no follow-up covers.
#[allow(clippy::too_many_arguments)]
async fn file_one(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &TicketMessage,
    run_id: &str,
    tracker: &Tracker,
    j: &Jira,
    source: &str,
    repo: &str,
    run_url: &str,
    draft: &Draft,
) -> Filed {
    let sk = record_sk(source, &ft::draft_id(source, draft));
    let description = ft::description(draft, source, &msg.title, run_url);
    // Never the team's trigger label: filing a follow-up starts no run.
    let trigger = match tracker {
        Tracker::Jira { .. } => j.trigger_label.as_str(),
        _ => "coderhelm",
    };
    let labels = ft::labels(source, trigger);
    // At most DAILY_CAP_PER_SOURCE a day from one source, across runs.
    if !matches!(tracker, Tracker::Comment(_))
        && !take_daily_slot(state, &msg.team_id, source).await
    {
        return Filed::NotFiled(ft::daily_cap_refusal(source));
    }
    let filed = match tracker {
        Tracker::Jira { project, site } => {
            let body = json!({
                "projectKey": project,
                "summary": draft.summary,
                "description": description,
                "labels": labels,
                "forge_secret": j.secret,
                "followup": { "source": source },
            });
            let sent = state
                .http
                .post(&j.create_url)
                .header("Content-Type", "application/json")
                .body(body.to_string())
                .timeout(CALL)
                .send()
                .await;
            match sent {
                Err(_) => Filed::NotFiled(format!(
                    "the Coderhelm app on {} didn't answer",
                    if site.is_empty() {
                        "the Jira site"
                    } else {
                        site
                    }
                )),
                Ok(r) => {
                    let status = r.status().as_u16();
                    let v: Value = r.json().await.unwrap_or_default();
                    jira_filed(status, &v, site, project)
                }
            }
        }
        Tracker::Github { repo } => {
            let (owner, name) = repo.split_once('/').unwrap_or((repo.as_str(), ""));
            let label = ft::source_label(source);
            let found = github
                .issues_with_label(owner, name, &label)
                .await
                .ok()
                .and_then(|list| {
                    list.into_iter().find(|(_, t, _)| {
                        ft::normal_summary(t) == ft::normal_summary(&draft.summary)
                    })
                });
            match found {
                Some((n, _, url)) => Filed::Ticket {
                    key: format!("#{n}"),
                    url,
                    existing: true,
                    linked: Linked::Yes,
                },
                None => {
                    let gh_labels: Vec<String> =
                        labels.iter().filter(|l| l.len() <= 50).cloned().collect();
                    match github
                        .create_labelled_issue(
                            owner,
                            name,
                            &draft.summary,
                            &description,
                            &gh_labels,
                        )
                        .await
                    {
                        Ok((n, url)) if n > 0 => Filed::Ticket {
                            key: format!("#{n}"),
                            url,
                            existing: false,
                            linked: Linked::Yes,
                        },
                        Ok(_) => Filed::NotFiled("GitHub returned no issue number".into()),
                        Err(e) => Filed::NotFiled(format!(
                            "GitHub refused the issue in {repo}: {}",
                            common::truncate_str(&one_line(&e.to_string()), 300)
                        )),
                    }
                }
            }
        }
        Tracker::Comment(why) => Filed::NotFiled(why.clone()),
    };
    if let Filed::Ticket { key, url, .. } = &filed {
        keep(
            state,
            msg,
            run_id,
            Record {
                sk: &sk,
                key,
                url,
                draft,
                covers: false,
            },
        )
        .await;
        keep_followup_of(state, msg, run_id, key, source, repo, draft, &description).await;
    }
    filed
}

/// CoderHelm's record of a filed follow-up by its own key: the gateway
/// starts no run on it from its label or assignee, a run on it files no
/// follow-ups, and a person's start from CoderHelm reads the ticket from it.
#[allow(clippy::too_many_arguments)]
async fn keep_followup_of(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    key: &str,
    source: &str,
    repo: &str,
    draft: &Draft,
    description: &str,
) {
    if ft::project_of(key).is_none() {
        // A GitHub issue (`#N`): its labels and text mark it.
        return;
    }
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&msg.team_id))
        .item("sk", attr_s(&ft::of_record_sk(key)))
        .item("key", attr_s(key.trim()))
        .item("source", attr_s(source.trim()))
        .item("repo", attr_s(repo.trim_matches('/')))
        .item("title", attr_s(draft.summary.trim()))
        .item("body", attr_s(description))
        .item("run_id", attr_s(run_id))
        .item("installation_id", attr_n(msg.installation_id as i64))
        .item("filed_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
    {
        warn!(key, error = %e, "Could not keep the follow-up's own record (its labels and text still mark it)");
    }
}

/// What the Jira app's answer to one follow-up means. An app older than the
/// follow-up trigger answers without `followup`: the ticket was filed, with
/// no link to the source.
pub fn jira_filed(status: u16, v: &Value, site: &str, project: &str) -> Filed {
    if !(200..300).contains(&status) {
        return Filed::NotFiled(ft::refusal(status, v, site, project));
    }
    let Some(key) = v["key"]
        .as_str()
        .filter(|k| common::git_conventions::is_ticket_key(k))
    else {
        return Filed::NotFiled("the Jira app returned no ticket key".into());
    };
    let url = if site.is_empty() {
        String::new()
    } else {
        format!("{}/browse/{key}", site.trim_end_matches('/'))
    };
    let linked = if v["followup"].as_u64() != Some(1) {
        Linked::No(
            "the site's Coderhelm app is older than the version that links follow-ups".into(),
        )
    } else if v["linked"]["ok"].as_bool() == Some(true) {
        Linked::Yes
    } else {
        let l = &v["linked"];
        let why = if l["error"].as_str() == Some("missing_permission") {
            "the app lacks the Jira project permission Link Issues".to_string()
        } else {
            l["error"]
                .as_str()
                .map(one_line)
                .unwrap_or_else(|| "the app didn't say why".into())
        };
        Linked::No(why)
    };
    Filed::Ticket {
        key: key.to_string(),
        url,
        existing: v["existing"].as_bool().unwrap_or(false),
        linked,
    }
}

/// The one comment on the source ticket (its own slot, edited in place).
async fn post_comment(state: &WorkerState, github: &GitHubClient, msg: &TicketMessage, text: &str) {
    let say = common::conversation::Say::status("followups", text);
    let sent = match msg.source {
        TicketSource::Jira if !msg.ticket_id.is_empty() => super::conversation::jira(
            state,
            &msg.team_id,
            &msg.ticket_id,
            say,
            "info",
            "Follow-up tickets",
        )
        .await
        .map(|_| ()),
        TicketSource::Github if msg.issue_number > 0 => super::conversation::github(
            github,
            &msg.repo_owner,
            &msg.repo_name,
            msg.issue_number,
            say,
        )
        .await
        .map(|_| ()),
        _ => return,
    };
    if let Err(e) = sent {
        warn!(ticket = %msg.ticket_id, error = %e, "Could not post the follow-up tickets on the ticket");
    }
}

/// The delivered run's report: what was found, the follow-ups, what a
/// person may do next (a recommendation: the run waits for none of it),
/// and for a question, the audit itself. Nothing in it asks.
pub fn delivered_report(
    decision: &FixDecision,
    proposal: &str,
    settled: Option<&Settled>,
) -> String {
    // The person's next step is said as a recommendation below, never as
    // "the one step I need from you".
    let mut shown = decision.clone();
    shown.next_step = None;
    let mut report = super::fix_location::unknown_report(&shown, false);
    if let Some(i) = report.rfind("I didn't change any code") {
        report.truncate(i);
    }
    let mut out = report.trim_end().to_string();
    if let Some(s) = settled {
        out.push_str(&format!("\n\n**Follow-up tickets**\n\n{}", s.comment));
    }
    let mut left: Vec<String> = Vec::new();
    if let Some(n) = decision.next_step.as_ref().filter(|n| n.is_usable()) {
        left.push(one_line(&n.action));
    }
    if let Some(c) = decision.outside_change.as_ref() {
        for s in c
            .steps
            .iter()
            .filter(|s| s.by.trim() == "person" && !s.done)
        {
            left.push(one_line(&s.step));
        }
    }
    for e in decision
        .end_to_end
        .iter()
        .filter(|e| e.by.trim() == "person")
    {
        let l = one_line(&e.step);
        if !l.is_empty() && !left.contains(&l) {
            left.push(l);
        }
    }
    if !left.is_empty() {
        out.push_str("\n\n**For a person, when they choose (this ticket is done without it)**\n");
        for l in &left {
            out.push_str(&format!("- {l}\n"));
        }
    }
    if decision.intent == TicketIntent::Question && !proposal.trim().is_empty() {
        out.push_str(&format!(
            "\n\n**The audit (proposal.md)**\n\n{}",
            proposal.trim()
        ));
    }
    out
}

/// Ends a no-code run whose work is done: the findings and the follow-ups
/// on the ticket, the run `completed` with its decision and report. Nothing
/// is asked.
#[allow(clippy::too_many_arguments)]
pub async fn finish_delivered(
    state: &WorkerState,
    msg: &TicketMessage,
    run_id: &str,
    decision: &FixDecision,
    proposal: &str,
    settled: Option<&Settled>,
    usage: &TokenUsage,
    start: &std::time::Instant,
    mcp_ids: &[String],
) {
    let title = super::fix_location::ask_title(decision);
    let report = delivered_report(decision, proposal, settled);
    if matches!(msg.source, TicketSource::Jira) && !msg.ticket_id.is_empty() {
        let body = super::fix_location::jira_text(&report);
        if let Err(e) = super::conversation::jira(
            state,
            &msg.team_id,
            &msg.ticket_id,
            common::conversation::Say::status("run", &body),
            "info",
            title,
        )
        .await
        {
            warn!(run_id, error = %e, "Could not post the findings on the ticket");
        }
    }
    let delta = usage.unrecorded();
    let status = common::run_status::COMPLETED;
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", attr_s(&msg.team_id))
        .key("run_id", attr_s(run_id))
        .update_expression(
            "SET #status = :s, tokens_in = if_not_exists(tokens_in, :uzero) + :ti, tokens_out = if_not_exists(tokens_out, :uzero) + :to, \
             cache_read_tokens = if_not_exists(cache_read_tokens, :uzero) + :crt, cache_write_tokens = if_not_exists(cache_write_tokens, :uzero) + :cwt, \
             cost_usd = if_not_exists(cost_usd, :uzero) + :c, duration_s = :d, updated_at = :t, \
             current_pass = :cp, status_run_id = :sri, mcp_servers = :mcp, \
             fix_location = :fl, fix_decision = :fd, outside_change_report = :rep \
             REMOVE waiting_on",
        )
        .condition_expression(common::run_status::not_ended("#status"))
        .expression_attribute_names("#status", "status")
        // Finished with its findings as the answer (`common::run_ending`).
        .run_status(
            ":s",
            &common::run_ending::gate(
                run_id,
                &common::run_ending::answered(&format!(
                    "the findings report on the run and the ticket: {}",
                    common::truncate_str(report.trim(), 300)
                )),
            ),
        )
        .usage_values(&delta)
        .expression_attribute_values(":d", attr_n(start.elapsed().as_secs()))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":cp", attr_s("done"))
        .expression_attribute_values(
            ":mcp",
            AttributeValue::L(mcp_ids.iter().map(|s| attr_s(s)).collect()),
        )
        .expression_attribute_values(":fl", attr_s(decision.effective_location().as_str()))
        .expression_attribute_values(
            ":fd",
            attr_s(&serde_json::to_string(decision).unwrap_or_default()),
        )
        .expression_attribute_values(":rep", attr_s(&common::person_gate::sink(&format!("**{title}**\n\n{report}")).text));
    let res = common::run_status::EndedValues::ended_values(res)
        .send()
        .await;
    match res {
        Ok(_) => usage.mark_recorded(&delta),
        Err(e) => warn!(run_id, error = %e, "Could not record the finished run"),
    }
    super::webhooks::emit_run(state, &msg.team_id, run_id, status).await;
    let month = chrono::Utc::now().format("%Y-%m").to_string();
    for period in [month.as_str(), "ALL_TIME"] {
        let _ = state
            .dynamo
            .update_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", attr_s(&msg.team_id))
            .key("period", attr_s(period))
            .update_expression("ADD completed :one")
            .expression_attribute_values(":one", attr_n(1))
            .send()
            .await;
    }
    info!(
        run_id,
        "Delivered without code: findings and follow-ups on the ticket"
    );
}

#[cfg(test)]
mod tests {
    use super::super::fix_location::{NextStep, OutsideChange};
    use super::super::outside_work::NoteStep;
    use super::*;

    /// The plan as the incident's run wrote it, cut to what matters.
    pub fn audit_plan() -> FixDecision {
        FixDecision {
            fix_location: FixLocation::Unknown,
            intent: TicketIntent::Question,
            cause: "The named components are unused; the real gaps are elsewhere.".into(),
            outside_change: Some(OutsideChange {
                system: "Jira".into(),
                change: "File the 8 additive follow-up tickets listed in proposal.md".into(),
                steps: vec![
                    NoteStep {
                        step: "Post the audit table on the ticket".into(),
                        by: "coderhelm".into(),
                        tool: "ticket comment".into(),
                        ..Default::default()
                    },
                    NoteStep {
                        step: "File the 8 follow-up tickets".into(),
                        by: "person".into(),
                        owner: "the ticket's reporter".into(),
                        why_person: "CoderHelm has no Jira connection (Connected apps: not set up), so it cannot create tickets".into(),
                        grant: "Connect Jira in Settings, Integrations".into(),
                        reason: "outside_reach".into(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            end_to_end: vec![super::super::fix_location::EndToEndStep {
                step: "Follow-up tickets filed".into(),
                by: "person".into(),
                owner: "ticket reporter".into(),
                ..Default::default()
            }],
            next_step: Some(NextStep {
                action: "File the 8 follow-up tickets from proposal.md in the Jira ABC project, or connect Jira so CoderHelm can".into(),
                why_person: "no Jira connection".into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn the_filing_is_coderhelm_s_and_nothing_is_left_for_a_person() {
        let mut d = audit_plan();
        assert!(asks_to_file(&d));
        let tasks = "NO_CODE_CHANGE: the audit is in proposal.md";
        assert!(!nothing_left(&d, tasks, &[]), "before: a person files them");
        let reach = [connected_access::jira(
            "https://example.atlassian.net",
            true,
        )];
        assert!(drop_connect_asks(&mut d, &reach) >= 2);
        mark_filed(&mut d, "Filed 8 of 8 follow-up tickets: ABC-1, ABC-2", true);
        assert!(
            d.outside_change.is_none(),
            "the outside change was the filing"
        );
        assert!(d.next_step.is_none());
        assert_eq!(d.end_to_end[0].by, "coderhelm");
        assert!(nothing_left(&d, tasks, &[]));
        assert!(
            !nothing_left(&d, "- [ ] a.ts: [modify] x", &[]),
            "a plan with code ships code"
        );
    }

    #[test]
    fn a_partial_filing_keeps_the_step_open_for_coderhelm_not_a_person() {
        let mut d = audit_plan();
        mark_filed(&mut d, "Filed 3 of 8", false);
        let c = d.outside_change.as_ref().unwrap();
        let s = &c.steps[1];
        assert_eq!(
            (s.by.as_str(), s.done, s.tool.as_str()),
            ("coderhelm", false, ft::TOOL)
        );
        assert!(s.why_person.is_empty() && s.grant.is_empty());
    }

    #[test]
    fn preparing_a_plan_files_nothing_and_hands_the_filing_to_coderhelm() {
        let mut d = audit_plan();
        let reach = [connected_access::jira(
            "https://example.atlassian.net",
            true,
        )];
        let proposal = "## Follow-up tickets to file\n1. Add alt validation to `imageWithAlt`.\n2. Fall back to the coach name.\n";
        let (dropped, due) = prepare_plan(&mut d, &reach, proposal);
        assert!(dropped >= 2);
        assert_eq!(due.map(|x| x.drafts.len()), Some(2));
        assert!(
            d.outside_change.is_none(),
            "the outside change was the filing"
        );
        assert!(nothing_left(&d, "NO_CODE_CHANGE: audit", &[]));
        // A plan that asks for no filing has nothing due.
        let mut plain = FixDecision::default();
        assert_eq!(prepare_plan(&mut plain, &reach, proposal).1, None);
    }

    /// The incident's second plan: a question answered, no outside change,
    /// a person's next step that is only a recommendation.
    fn findings_plan() -> FixDecision {
        FixDecision {
            fix_location: FixLocation::Unknown,
            intent: TicketIntent::Question,
            cause: "The named components are unused; the real gaps are the optional alt on the shared schema.".into(),
            end_to_end: vec![super::super::fix_location::EndToEndStep {
                step: "Fill alt text on the 3 production records".into(),
                by: "person".into(),
                owner: "Content editor".into(),
                ..Default::default()
            }],
            next_step: Some(NextStep {
                action: "A content editor sets the alt text on the 3 records that have none".into(),
                why_person: "no write connection to the content store".into(),
                outcomes: vec![
                    super::super::fix_location::Outcome {
                        if_: "set".into(),
                        means: "the warning flags nothing".into(),
                    },
                    super::super::fix_location::Outcome {
                        if_: "left".into(),
                        means: "the warning shows on 3".into(),
                    },
                ],
            }),
            ..Default::default()
        }
    }

    #[test]
    fn findings_are_a_deliverable_and_a_person_s_step_is_a_recommendation() {
        let d = findings_plan();
        let tasks = "NO_CODE_CHANGE: the decision for each component is in design.md.";
        assert!(
            !nothing_left(&d, tasks, &[]),
            "the old check saw a person's step"
        );
        assert!(delivers_findings(&d, tasks, &[], true));
        assert!(delivers_findings(&d, tasks, &[], false));
        // A plan that codes is never a findings deliverable.
        assert!(!delivers_findings(&d, "- [ ] a.ts: [modify] x", &[], true));
        // An unknown bug with a person's step is not done.
        let bug = FixDecision {
            intent: TicketIntent::Bug,
            ..findings_plan()
        };
        assert!(!delivers_findings(&bug, tasks, &[], false));
        // A plan with its own open question asks it.
        let asks = FixDecision {
            questions: vec!["Which dataset is the source of truth?".into()],
            ..findings_plan()
        };
        assert!(!delivers_findings(&asks, tasks, &[], true));
        let r = delivered_report(&d, "# Audit\n\nThe table.", None);
        assert!(
            r.contains("For a person, when they choose (this ticket is done without it)"),
            "{r}"
        );
        assert!(r.contains("A content editor sets the alt text"), "{r}");
        assert!(r.contains("**The audit (proposal.md)**"), "{r}");
        for never in [
            "The one step I need from you",
            "Reply here",
            "I didn't change any code",
        ] {
            assert!(!r.contains(never), "{never}: {r}");
        }
    }

    #[test]
    fn a_findings_plan_is_taken_up_again_not_planned_again() {
        let p = super::super::plan::PlanResult {
            proposal: "## Follow-up tickets to file\n1. Add alt validation.\n".into(),
            tasks: "NO_CODE_CHANGE: the audit is in proposal.md".into(),
            fix: Some(findings_plan()),
            ..Default::default()
        };
        assert!(reusable_findings(&p));
        let coded = super::super::plan::PlanResult {
            tasks: "- [ ] a.ts: [modify] x".into(),
            ..p.clone()
        };
        assert!(!reusable_findings(&coded));
        let no_drafts = super::super::plan::PlanResult {
            proposal: "# Audit\nNothing to file.".into(),
            ..p.clone()
        };
        assert!(!reusable_findings(&no_drafts));
        let undecided = super::super::plan::PlanResult { fix: None, ..p };
        assert!(!reusable_findings(&undecided));
    }

    #[test]
    fn what_was_filed_before_is_read_from_records_comments_and_the_app() {
        let mut item = HashMap::new();
        item.insert("key".to_string(), attr_s("ABC-93"));
        item.insert("summary".to_string(), attr_s("Require alt on imageWithAlt"));
        item.insert(MARKS_ATTR.to_string(), attr_s("imagewithalt,mediaasset"));
        let e = existing_of_record(&item).unwrap();
        assert_eq!(e.marks, vec!["imagewithalt", "mediaasset"]);
        item.insert("covers".to_string(), attr_s("1"));
        let covered = existing_of_record(&item).unwrap();
        assert!(
            covered.summary.is_empty(),
            "a covered draft's words are not the ticket's"
        );
        let doc = json!([{
            "id": "1",
            "body": "x",
            "meta": {"slot": "run", "parts": [
                {"slot": "followups", "body": "Filed 1 follow-up ticket from this ticket's findings:\nABC-98: Remove unused prototypes (https://x/browse/ABC-98)"}
            ]}
        }]);
        let said = said_followups_of(&doc);
        assert_eq!(said.len(), 1);
        assert_eq!(said[0].summary, "Remove unused prototypes");
        let listed = jira_listed(
            &json!({"followup": 1, "issues": [{"key": "ABC-96", "summary": "Fix alt on the unused components", "description": "In `Hero.tsx` and ContactCard."}, {"key": "nope"}]}),
            "https://x.atlassian.net",
        );
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].url, "https://x.atlassian.net/browse/ABC-96");
        assert!(listed[0].marks.contains(&"contactcard".to_string()));
        // An app older than the list path answers something else: no list.
        assert!(jira_listed(&json!({"error": "projectKey and summary required"}), "").is_empty());
    }

    #[test]
    fn the_line_says_what_is_new_and_what_earlier_tickets_cover() {
        let drafts = vec![
            Draft {
                summary: "a".into(),
                ..Default::default()
            },
            Draft {
                summary: "b".into(),
                ..Default::default()
            },
        ];
        let t = Tracker::Jira {
            project: "ABC".into(),
            site: String::new(),
        };
        let filed = vec![
            Filed::Ticket {
                key: "ABC-98".into(),
                url: String::new(),
                existing: false,
                linked: Linked::Yes,
            },
            Filed::Covered {
                key: "ABC-96".into(),
                url: String::new(),
                title: String::new(),
            },
        ];
        assert_eq!(
            settled_line(&drafts, &filed, &t),
            "Filed 1 of 2 follow-up tickets: ABC-98; already filed from this ticket: ABC-96"
        );
        let none_new = vec![filed[1].clone(), filed[1].clone()];
        assert_eq!(
            settled_line(&drafts, &none_new, &t),
            "No new follow-up tickets: the 2 drafts are covered; already filed from this ticket: ABC-96"
        );
    }

    #[test]
    fn the_app_s_answers_read_as_filed_refused_or_unlinked() {
        let ok = json!({"key": "ABC-9", "existing": false, "linked": {"ok": true, "type": "Relates"}, "followup": 1});
        assert_eq!(
            jira_filed(201, &ok, "https://x.atlassian.net", "ABC"),
            Filed::Ticket {
                key: "ABC-9".into(),
                url: "https://x.atlassian.net/browse/ABC-9".into(),
                existing: false,
                linked: Linked::Yes
            }
        );
        let old = json!({"key": "ABC-9", "id": "1"});
        let Filed::Ticket {
            linked: Linked::No(why),
            ..
        } = jira_filed(201, &old, "", "ABC")
        else {
            panic!("an older app still filed it");
        };
        assert!(why.contains("older"), "{why}");
        let refused = json!({"error": "missing_permission", "missing": ["CREATE_ISSUES"], "words": "Create Issues"});
        let Filed::NotFiled(why) = jira_filed(403, &refused, "", "ABC") else {
            panic!("refused");
        };
        assert!(why.contains("Create Issues in ABC"), "{why}");
        assert!(!why.to_lowercase().contains("connect jira"), "{why}");
    }
}
