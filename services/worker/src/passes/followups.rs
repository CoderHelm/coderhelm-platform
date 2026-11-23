//! The worker's side of follow-up tickets (`common::followup_tickets`) and
//! of "never ask to connect what is connected" (`common::connected_access`).
//!
//! - [`planner_caps`]: the capabilities the planner reads, with the team's
//!   ticket tracker in them, so a step on Jira is CoderHelm's.
//! - [`reaches`]: what the run reaches, for the stop judge's picture.
//! - [`settle`]: once the plan is written, files the drafts the ticket's
//!   deliverable asks for (in the source ticket's Jira project, else as
//!   GitHub issues, else in the comment), keeps what was filed so a retry
//!   files nothing twice, posts one comment listing the new keys, and turns
//!   the plan's filing steps (and any "connect Jira" step) into CoderHelm's.
//! - [`nothing_left`] and [`finish_delivered`]: a no-code run whose only
//!   open work was the filing completes, instead of waiting on a person.

use aws_sdk_dynamodb::types::AttributeValue;
use common::connected_access::{self, Reach};
use common::followup_tickets::{self as ft, Draft, Filed, Linked, Tracker};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, warn};

use super::fix_location::{FixDecision, FixLocation, TicketIntent};
use super::{attr_n, attr_s, RunUsageValues};
use crate::clients::github::GitHubClient;
use crate::models::{TicketMessage, TicketSource, TokenUsage};
use crate::WorkerState;

/// The settings record of one filed draft.
pub fn record_sk(source: &str, draft_id: &str) -> String {
    format!("FOLLOWUP#{}#{draft_id}", source.trim().to_ascii_uppercase())
}

/// How long one call to the Jira app may take (the hybrid proxy gives the
/// site 25 seconds).
const CALL: Duration = Duration::from_secs(30);

/// The team's Jira app, as far as follow-ups care.
#[derive(Clone, Default)]
struct Jira {
    connected: bool,
    create_url: String,
    secret: String,
    site: String,
}

async fn jira(state: &WorkerState, team_id: &str) -> Jira {
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
    }
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

/// Files the plan's follow-ups and settles the plan's steps. `None` when
/// the plan files nothing (no drafts, or no step asks for them); the plan's
/// "connect Jira" asks are dropped either way.
pub async fn settle(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &TicketMessage,
    run_id: &str,
    d: &mut FixDecision,
    proposal: &str,
) -> Option<Settled> {
    let reach = reaches(state, &msg.team_id).await;
    let dropped = drop_connect_asks(d, &reach);
    if dropped > 0 {
        info!(
            run_id,
            dropped,
            "The plan asked a person to connect a system CoderHelm reaches: CoderHelm's now"
        );
    }
    if !asks_to_file(d) {
        return None;
    }
    let drafts = ft::drafts(&d.followup_tickets, proposal);
    if drafts.is_empty() {
        return None;
    }
    let source = source_of(msg);
    let repo = format!("{}/{}", msg.repo_owner, msg.repo_name);
    let j = jira(state, &msg.team_id).await;
    let jira_source = matches!(msg.source, TicketSource::Jira) && !j.create_url.is_empty();
    let issues = github.permission_state("issues").await.token_has;
    let mut tracker = ft::tracker(&source, jira_source, &j.site, &repo, issues);
    if matches!(tracker, Tracker::Github { .. }) && msg.repo_owner.is_empty() {
        tracker = Tracker::Comment("the run has no repository to open issues in".into());
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
    let mut filed: Vec<Filed> = Vec::new();
    let mut stop: Option<String> = None;
    for draft in &drafts {
        if let Some(why) = &stop {
            filed.push(Filed::NotFiled(why.clone()));
            continue;
        }
        let one = match &tracker {
            Tracker::Comment(_) => break,
            t => file_one(state, github, msg, t, &j, &source, &repo, &run_url, draft).await,
        };
        // A refusal that would refuse every draft the same way stops there.
        if let Filed::NotFiled(why) = &one {
            if why.contains("lacks the Jira project permission")
                || why.contains("didn't accept CoderHelm's shared secret")
            {
                stop = Some(why.clone());
            }
        }
        filed.push(one);
    }
    let comment = ft::comment(&drafts, &filed, &tracker);
    let keys: Vec<String> = filed
        .iter()
        .filter_map(Filed::key)
        .map(str::to_string)
        .collect();
    let line = if keys.is_empty() {
        format!(
            "Not filed: {}. The drafts are on the ticket.",
            match &tracker {
                Tracker::Comment(why) => why.clone(),
                _ => filed
                    .iter()
                    .find_map(|f| match f {
                        Filed::NotFiled(w) => Some(w.clone()),
                        Filed::Ticket { .. } => None,
                    })
                    .unwrap_or_default(),
            }
        )
    } else {
        format!(
            "Filed {} of {} follow-up tickets: {}",
            keys.len(),
            drafts.len(),
            keys.join(", ")
        )
    };
    post_comment(state, github, msg, &comment).await;
    super::add_progress_note(state, &msg.team_id, run_id, &line).await;
    let settled = Settled {
        drafts,
        filed,
        tracker,
        comment,
        line,
    };
    mark_filed(d, &settled.line, settled.all_filed());
    info!(run_id, filed = keys.len(), "Follow-up tickets settled");
    Some(settled)
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// What an earlier run filed for this draft, if anything.
async fn kept(state: &WorkerState, team_id: &str, sk: &str) -> Option<(String, String)> {
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
    Some((
        s("key").filter(|k| !k.is_empty())?,
        s("url").unwrap_or_default(),
    ))
}

async fn keep(state: &WorkerState, msg: &TicketMessage, sk: &str, key: &str, url: &str) {
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&msg.team_id))
        .item("sk", attr_s(sk))
        .item("key", attr_s(key))
        .item("url", attr_s(url))
        .item("ticket_id", attr_s(&msg.ticket_id))
        .item("filed_at", attr_s(&chrono::Utc::now().to_rfc3339()))
        .send()
        .await
    {
        warn!(key, error = %e, "Could not keep a filed follow-up (a retry finds it in the tracker)");
    }
}

/// Files one draft, once.
#[allow(clippy::too_many_arguments)]
async fn file_one(
    state: &WorkerState,
    github: &GitHubClient,
    msg: &TicketMessage,
    tracker: &Tracker,
    j: &Jira,
    source: &str,
    repo: &str,
    run_url: &str,
    draft: &Draft,
) -> Filed {
    let sk = record_sk(source, &ft::draft_id(source, draft));
    if let Some((key, url)) = kept(state, &msg.team_id, &sk).await {
        return Filed::Ticket {
            key,
            url,
            existing: true,
            linked: Linked::Yes,
        };
    }
    let description = ft::description(draft, source, &msg.title, run_url);
    let labels = ft::labels(repo, source);
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
            let label = labels.last().cloned().unwrap_or_default();
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
        keep(state, msg, &sk, key, url).await;
    }
    filed
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
    settled: &Settled,
    usage: &TokenUsage,
    start: &std::time::Instant,
    mcp_ids: &[String],
) {
    let title = super::fix_location::ask_title(decision);
    let mut report = super::fix_location::unknown_report(decision, false);
    // Not a question: what was found, and the follow-ups.
    if let Some(i) = report.rfind("I didn't change any code") {
        report.truncate(i);
    }
    let report = format!(
        "{}\n\n**Follow-up tickets**\n\n{}{}",
        report.trim_end(),
        settled.comment,
        if decision.intent == TicketIntent::Question && !proposal.trim().is_empty() {
            format!("\n\n**The audit (proposal.md)**\n\n{}", proposal.trim())
        } else {
            String::new()
        }
    );
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
        .expression_attribute_values(":s", attr_s(status))
        .usage_values(&delta)
        .expression_attribute_values(":d", attr_n(start.elapsed().as_secs()))
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":cp", attr_s("done"))
        .expression_attribute_values(":sri", attr_s(&format!("{status}#{run_id}")))
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
