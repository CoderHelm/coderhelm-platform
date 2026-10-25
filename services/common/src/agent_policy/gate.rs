//! The gate every agent loop calls before every call it makes.
//!
//! A loop builds one [`Gate`] for its run or chat message and asks
//! [`Gate::check`] before each tool call, MCP tool, data source query,
//! sandbox command or network request. The gate loads the team's policy,
//! asks the registry for the call's risk, gets the engine's decision,
//! records it, and for an ask raises an approval card and (in a run) waits
//! for the answer.
//!
//! What the loop does with the [`Outcome`]:
//!
//! - `Run`: make the call.
//! - `Refused`: don't; give the model the message instead of a result.
//! - `Pending`: (chats) don't; show the card, give the model the message.
//! - `Stop`: end the loop now. Paused work is queued and resumes; an
//!   approval nobody answered in time ends the run cleanly with a note.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::Mutex;

use super::approvals::{self, Approval, Status};
use super::audit::{self, ActorKind, Event, PolicyFields};
use super::engine;
use super::model::*;
use super::registry::{self, ToolRisk};
use super::store::PolicyStore;

/// Who and what a loop is working for.
#[derive(Debug, Clone, Default)]
pub struct GateCtx {
    pub team: String,
    pub agent: Option<Agent>,
    pub repo: Option<String>,
    /// The run, when the loop is part of one.
    pub run_id: String,
    /// The chat thread, when the loop answers a chat.
    pub thread_id: String,
    /// The person the agent works for, when one is known.
    pub person: String,
    /// What the agent is doing, shown on approval cards as the "why" when
    /// the call itself gives none.
    pub task: String,
}

impl GateCtx {
    pub fn new(team: &str, agent: Agent) -> Self {
        Self {
            team: team.to_string(),
            agent: Some(agent),
            ..Default::default()
        }
    }
    pub fn repo(mut self, repo: &str) -> Self {
        let r = repo.trim().trim_matches('/').to_ascii_lowercase();
        self.repo = (!r.is_empty() && r.contains('/')).then_some(r);
        self
    }
    pub fn run(mut self, run_id: &str) -> Self {
        self.run_id = run_id.to_string();
        self
    }
    pub fn thread(mut self, thread_id: &str) -> Self {
        self.thread_id = thread_id.to_string();
        self
    }
    pub fn person(mut self, email: &str) -> Self {
        self.person = email.trim().to_ascii_lowercase();
        self
    }
    pub fn task(mut self, task: &str) -> Self {
        self.task = crate::truncate_str(task.trim(), 300).to_string();
        self
    }
    fn agent(&self) -> Agent {
        self.agent.unwrap_or(Agent::Coder)
    }
    fn scope_key(&self) -> String {
        if !self.run_id.is_empty() {
            format!("run:{}", self.run_id)
        } else {
            format!("thread:{}", self.thread_id)
        }
    }
}

/// How an ask is answered.
#[derive(Debug, Clone, Copy)]
pub enum Wait {
    /// Wait here for a person (runs). The card expires after the team's
    /// timeout or `max`, whichever is first.
    Block { max: Duration, poll: Duration },
    /// Return at once with the card (chats show it and continue when it's
    /// answered).
    Return,
}

/// Tells people a card is waiting (Slack, Teams, email). Implemented by
/// the services; a failure never stops the gate.
#[async_trait::async_trait]
pub trait Notifier: Send + Sync {
    async fn approval_needed(&self, team: &str, a: &Approval);
}

/// Why a loop must stop.
#[derive(Debug, Clone, PartialEq)]
pub enum Stop {
    Paused { reason: String },
    TimedOut { approval: Box<Approval> },
}

impl Stop {
    /// The note the run ends with.
    pub fn note(&self) -> String {
        match self {
            Stop::Paused { reason } => {
                format!(
                "Agents were paused{}. This work is queued and picks up again when the pause ends.",
                if reason.is_empty() { String::new() } else { format!(" ({reason})") }
            )
            }
            Stop::TimedOut { approval } => format!(
                "Stopped: nobody answered the request to {} in time. Nothing was done. Approve it \
                 (or add a rule) on the Agent permissions page, then retry the run.",
                lower_first(&approval.headline())
            ),
        }
    }
}

fn lower_first(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_lowercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

/// The error a loop returns when it stops for a [`Stop`]: its message is
/// the note. Callers find it with `downcast_ref` to end the run cleanly
/// instead of marking it failed.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentStopped(pub Stop);

impl std::fmt::Display for AgentStopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.note())
    }
}

impl std::error::Error for AgentStopped {}

/// What the loop does with one call.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Run {
        verdict: Verdict,
    },
    Refused {
        verdict: Verdict,
        message: String,
    },
    Pending {
        approval: Box<Approval>,
        message: String,
    },
    Stop(Stop),
}

impl Outcome {
    pub fn runs(&self) -> bool {
        matches!(self, Outcome::Run { .. })
    }
}

/// One call to check.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    pub tool: &'a str,
    pub args: &'a Value,
    /// The agent's own reason, when it gave one.
    pub why: &'a str,
}

pub struct Gate {
    store: Arc<dyn PolicyStore>,
    registry: &'static dyn ToolRisk,
    notifier: Option<Arc<dyn Notifier>>,
    pub ctx: GateCtx,
    wait: Wait,
    /// The names offered to the model. A call to anything else is unknown.
    offered: Option<HashSet<String>>,
    cache: Mutex<Option<(std::time::Instant, TeamPolicy)>>,
}

/// How long a loaded policy is reused before it is read again, so a pause
/// or a new rule reaches a running loop within seconds.
const POLICY_TTL: Duration = Duration::from_secs(10);

impl Gate {
    pub fn new(store: Arc<dyn PolicyStore>, ctx: GateCtx) -> Self {
        Self {
            store,
            registry: registry::default_registry(),
            notifier: None,
            ctx,
            wait: Wait::Return,
            offered: None,
            cache: Mutex::new(None),
        }
    }
    pub fn with_notifier(mut self, n: Arc<dyn Notifier>) -> Self {
        self.notifier = Some(n);
        self
    }
    pub fn with_wait(mut self, w: Wait) -> Self {
        self.wait = w;
        self
    }
    pub fn with_registry(mut self, r: &'static dyn ToolRisk) -> Self {
        self.registry = r;
        self
    }
    /// The tools offered to the model this turn.
    pub fn offering<I: IntoIterator<Item = S>, S: Into<String>>(mut self, names: I) -> Self {
        self.offered = Some(names.into_iter().map(Into::into).collect());
        self
    }
    pub fn set_offered<I: IntoIterator<Item = S>, S: Into<String>>(&mut self, names: I) {
        self.offered = Some(names.into_iter().map(Into::into).collect());
    }
    pub fn store(&self) -> &Arc<dyn PolicyStore> {
        &self.store
    }

    /// The team's policy, read at most every few seconds. A read failure
    /// falls back to the defaults (which never allow a change on their own).
    pub async fn policy(&self) -> TeamPolicy {
        let mut c = self.cache.lock().await;
        if let Some((at, p)) = c.as_ref() {
            if at.elapsed() < POLICY_TTL {
                return p.clone();
            }
        }
        let p = match self.store.load(&self.ctx.team).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(team = %self.ctx.team, error = %e, "Couldn't read agent permissions; using the safe defaults");
                TeamPolicy::default()
            }
        };
        *c = Some((std::time::Instant::now(), p.clone()));
        p
    }

    /// The call as the engine sees it.
    pub fn call_for(&self, tool: &str) -> Call {
        let (kind, server) = registry::kind_of(tool);
        Call {
            agent: self.ctx.agent(),
            kind,
            tool: tool.to_string(),
            server,
            repo: self.ctx.repo.clone(),
            command: None,
        }
    }

    fn risk_for(&self, call: &Call) -> Option<Risk> {
        if matches!(call.kind, CallKind::Command | CallKind::Egress) {
            return Some(Risk::Write);
        }
        if let Some(o) = &self.offered {
            if !o.contains(&call.tool) {
                return None;
            }
        }
        self.registry.risk(&call.tool)
    }

    /// Checks one tool call.
    pub async fn check(&self, req: Request<'_>) -> Outcome {
        let call = self.call_for(req.tool);
        self.check_call(call, req.args, req.why).await
    }

    /// Checks one sandbox command.
    pub async fn check_command(&self, command: &str, why: &str) -> Outcome {
        let call = Call::command(self.ctx.agent(), command).in_repo(self.ctx.repo.as_deref());
        let args = serde_json::json!({ "command": command });
        self.check_call(call, &args, why).await
    }

    /// Checks one network request from the sandbox (or an agent's fetch).
    pub async fn check_egress(&self, host: &str) -> Outcome {
        let call = Call::egress(self.ctx.agent(), host).in_repo(self.ctx.repo.as_deref());
        let args = serde_json::json!({ "host": host });
        self.check_call(call, &args, "").await
    }

    pub async fn check_call(&self, call: Call, args: &Value, why: &str) -> Outcome {
        let policy = self.policy().await;
        let now = super::now();
        let risk = self.risk_for(&call);
        let verdict = engine::decide(&policy, &call, risk, &now);
        match verdict.decision {
            Decision::Allow => {
                if !matches!(verdict.decided_by, DecidedBy::Default { risk: Risk::Read }) {
                    self.record(&call, &verdict, audit::action::TOOL_CALLED, "ok", "")
                        .await;
                }
                Outcome::Run { verdict }
            }
            Decision::Deny if verdict.paused() => {
                let reason = match &verdict.decided_by {
                    DecidedBy::Paused { reason } => reason.clone(),
                    _ => String::new(),
                };
                self.record(&call, &verdict, audit::action::TOOL_REFUSED, "paused", "")
                    .await;
                Outcome::Stop(Stop::Paused { reason })
            }
            Decision::Deny => {
                self.record(&call, &verdict, audit::action::TOOL_REFUSED, "refused", "")
                    .await;
                let message = format!(
                    "Not allowed: {} Don't try this again. Carry on without it, and say what you \
                     couldn't do.",
                    verdict.reason
                );
                Outcome::Refused { verdict, message }
            }
            Decision::Ask => self.ask(&policy, call, args, why, verdict, &now).await,
        }
    }

    async fn ask(
        &self,
        policy: &TeamPolicy,
        call: Call,
        args: &Value,
        why: &str,
        verdict: Verdict,
        now: &str,
    ) -> Outcome {
        let team = &self.ctx.team;
        let hash = approvals::args_hash(&call.tool, call.command.as_deref(), args);
        let scope_key = self.ctx.scope_key();

        // Approved once already (a chat that asked, then tried again).
        if let Ok(Some(approval_id)) = self
            .store
            .take_grant(team, &approvals::grant_key(&scope_key, &hash))
            .await
        {
            let v = Verdict {
                decision: Decision::Allow,
                risk: verdict.risk,
                decided_by: DecidedBy::Grant {
                    approval_id: approval_id.clone(),
                },
                reason: "A person approved this exact call once.".into(),
                capped: false,
            };
            self.record(&call, &v, audit::action::TOOL_CALLED, "ok", &approval_id)
                .await;
            return Outcome::Run { verdict: v };
        }

        // The same call already waiting in this run or chat: reuse its card.
        let existing = self
            .store
            .list_approvals(team, true)
            .await
            .unwrap_or_default()
            .into_iter()
            .find(|a| a.scope_key() == scope_key && a.args_hash == hash && !a.expired_at(now));

        let timeout = match self.wait {
            Wait::Block { max, .. } => policy.ask_timeout().min(max.as_secs().max(1)),
            Wait::Return => policy.ask_timeout(),
        };
        let approval = match existing {
            Some(a) => a,
            None => {
                let a = Approval {
                    id: format!("ap_{}", ulid::Ulid::new().to_string().to_ascii_lowercase()),
                    status: Status::Pending,
                    agent: call.agent,
                    kind: call.kind,
                    tool: call.tool.clone(),
                    server: call.server.clone(),
                    repo: call.repo.clone(),
                    command: call.command.clone(),
                    run_id: self.ctx.run_id.clone(),
                    thread_id: self.ctx.thread_id.clone(),
                    requested_for: self.ctx.person.clone(),
                    args: approvals::redact(args),
                    args_hash: hash.clone(),
                    risk: verdict.risk,
                    why: if why.trim().is_empty() {
                        self.ctx.task.clone()
                    } else {
                        crate::truncate_str(why.trim(), 500).to_string()
                    },
                    asked_because: verdict.decided_by.clone(),
                    reason: verdict.reason.clone(),
                    created_at: now.to_string(),
                    expires_at: plus_secs(now, timeout),
                    decided_by: String::new(),
                    decided_via: None,
                    decided_at: None,
                    choice: None,
                    note: String::new(),
                    rule_id: String::new(),
                };
                if let Err(e) = self.store.put_approval(team, &a).await {
                    // No card means nobody can answer: refuse rather than wait.
                    tracing::error!(team = %team, error = %e, "Couldn't save an approval card");
                    return Outcome::Refused {
                        message: format!(
                            "This needs a person's approval and the request couldn't be saved. \
                             Don't try it again. ({})",
                            verdict.reason
                        ),
                        verdict,
                    };
                }
                self.record(
                    &call,
                    &verdict,
                    audit::action::TOOL_QUEUED,
                    "waiting",
                    &a.id,
                )
                .await;
                if let Some(n) = &self.notifier {
                    n.approval_needed(team, &a).await;
                }
                a
            }
        };

        match self.wait {
            Wait::Return => Outcome::Pending {
                message: format!(
                    "This needs a person's approval first ({}). An approval card is showing. \
                     Stop here and tell them in one short sentence what you'd like to do and \
                     why; you can try again once they approve.",
                    verdict.reason
                ),
                approval: Box::new(approval),
            },
            Wait::Block { poll, .. } => self.wait_for(approval, verdict, poll).await,
        }
    }

    async fn wait_for(&self, approval: Approval, verdict: Verdict, poll: Duration) -> Outcome {
        let team = &self.ctx.team;
        loop {
            let now = super::now();
            let current = self
                .store
                .get_approval(team, &approval.id)
                .await
                .ok()
                .flatten()
                .unwrap_or_else(|| approval.clone());
            match current.status {
                Status::Approved => {
                    // "Once" left a grant for a chat to retry with; a run
                    // that waited uses it here.
                    let _ = self
                        .store
                        .take_grant(
                            team,
                            &approvals::grant_key(&current.scope_key(), &current.args_hash),
                        )
                        .await;
                    return Outcome::Run {
                        verdict: Verdict {
                            decision: Decision::Allow,
                            risk: verdict.risk,
                            decided_by: DecidedBy::Grant {
                                approval_id: current.id.clone(),
                            },
                            reason: format!("Approved by {}.", current.decided_by),
                            capped: false,
                        },
                    };
                }
                Status::Denied => {
                    return Outcome::Refused {
                        message: format!(
                            "{} declined this{}. Don't try it again; carry on without it and say \
                             what you couldn't do.",
                            if current.decided_by.is_empty() {
                                "A person"
                            } else {
                                &current.decided_by
                            },
                            if current.note.is_empty() {
                                String::new()
                            } else {
                                format!(": \"{}\"", current.note)
                            }
                        ),
                        verdict: Verdict {
                            decision: Decision::Deny,
                            ..verdict
                        },
                    };
                }
                Status::Expired => {
                    return Outcome::Stop(Stop::TimedOut {
                        approval: Box::new(current),
                    })
                }
                Status::Pending if current.expired_at(&now) => {
                    let mut expired = current.clone();
                    expired.status = Status::Expired;
                    expired.decided_at = Some(now.clone());
                    if self
                        .store
                        .update_approval(team, &expired, Status::Pending)
                        .await
                        .unwrap_or(false)
                    {
                        self.store
                            .audit(
                                team,
                                Event::new(
                                    "system",
                                    ActorKind::System,
                                    audit::action::EXPIRED,
                                    "expired",
                                )
                                .approval(&expired.id)
                                .run(&expired.run_id)
                                .tool(&expired.tool, expired.risk.map_or("", |r| r.as_str())),
                            )
                            .await;
                        return Outcome::Stop(Stop::TimedOut {
                            approval: Box::new(expired),
                        });
                    }
                    // Someone answered at the last moment: read it again.
                    continue;
                }
                Status::Pending => {}
            }
            // Pause while waiting also ends the wait (and queues the work).
            if let Some(p) = self
                .store
                .load(team)
                .await
                .ok()
                .and_then(|p| p.paused(&now).cloned())
            {
                return Outcome::Stop(Stop::Paused { reason: p.reason });
            }
            tokio::time::sleep(poll).await;
        }
    }

    async fn record(
        &self,
        call: &Call,
        v: &Verdict,
        action: &str,
        result: &str,
        approval_id: &str,
    ) {
        let actor = format!("agent:{}", call.agent.key());
        let mut e = Event::new(&actor, ActorKind::Agent, action, result)
            .run(&self.ctx.run_id)
            .tool(
                &match &call.command {
                    Some(c) if call.kind == CallKind::Command => format!("run_command: {c}"),
                    Some(h) if call.kind == CallKind::Egress => format!("connect: {h}"),
                    _ => call.tool.clone(),
                },
                v.risk.map_or("unknown", |r| r.as_str()),
            )
            .detail(v.reason.clone())
            .policy(PolicyFields {
                agent: call.agent.key().into(),
                repo: call.repo.clone().unwrap_or_default(),
                decision: v.decision.as_str().into(),
                rule: rule_ref(&v.decided_by),
                thread_id: self.ctx.thread_id.clone(),
            });
        if !approval_id.is_empty() {
            e = e.approval(approval_id);
        }
        self.store.audit(&self.ctx.team, e).await;
    }
}

/// A short reference to what decided, for the log: a rule id or a word.
pub fn rule_ref(d: &DecidedBy) -> String {
    match d {
        DecidedBy::Rule { id, .. } => id.clone(),
        DecidedBy::Default { risk } => format!("default:{}", risk.as_str()),
        DecidedBy::AgentOff { scope } => format!("agent_off:{}", scope.key()),
        DecidedBy::Paused { .. } => "paused".into(),
        DecidedBy::Unknown => "unknown_tool".into(),
        DecidedBy::CommandAllowlist { .. } => "command_allowlist".into(),
        DecidedBy::EgressAllowlist => "egress_allowlist".into(),
        DecidedBy::Grant { approval_id } => format!("approved_once:{approval_id}"),
    }
}

fn plus_secs(now: &str, secs: u64) -> String {
    chrono::DateTime::parse_from_rfc3339(now)
        .map(|t| t.with_timezone(&chrono::Utc))
        .unwrap_or_else(|_| chrono::Utc::now())
        .checked_add_signed(chrono::Duration::seconds(secs as i64))
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}
