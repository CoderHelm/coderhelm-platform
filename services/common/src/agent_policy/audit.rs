//! Every decision, approval and rule change, written as an audit event.
//!
//! The events have exactly the shape of the plugins work's `AUDIT#` stream
//! (`common::plugins::audit::Event`: id, at, actor, actor_kind, action,
//! run_id, approval_id, tool, risk, result, detail), and its action names
//! where one exists (`tool.called`, `tool.refused`,
//! `tool.queued_for_approval`). Until that stream is merged they go to the
//! agent activity log (`AGENT_LOG#<ulid>` under the team, kept 90 days);
//! afterwards [`AuditSink`] writes to `AUDIT#` and the log stops growing.

use serde::{Deserialize, Serialize};

/// Who acted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ActorKind {
    #[default]
    Person,
    Agent,
    Schedule,
    System,
}

pub mod action {
    pub const TOOL_CALLED: &str = "tool.called";
    pub const TOOL_REFUSED: &str = "tool.refused";
    pub const TOOL_QUEUED: &str = "tool.queued_for_approval";
    pub const APPROVED: &str = "approval.approved";
    pub const DENIED: &str = "approval.denied";
    pub const EXPIRED: &str = "approval.expired";
    pub const RULE_ADDED: &str = "rule.added";
    pub const RULE_REMOVED: &str = "rule.removed";
    pub const POLICY_SAVED: &str = "policy.saved";
    pub const PAUSED: &str = "agents.paused";
    pub const RESUMED: &str = "agents.resumed";
    pub const WORK_QUEUED: &str = "work.queued";
    pub const WORK_RESUMED: &str = "work.resumed";
    pub const SIGN_IN_ASKED: &str = "sign_in.asked";
    pub const SIGN_IN_DECLINED: &str = "sign_in.declined";
}

/// One event, in the `AUDIT#` stream's shape.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    pub at: String,
    /// An email, `agent:<key>` for an agent, `system`.
    pub actor: String,
    #[serde(default)]
    pub actor_kind: ActorKind,
    pub action: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub approval_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tool: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub risk: String,
    /// `ok`, `refused`, `waiting`, or a short outcome.
    pub result: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub detail: String,
    /// Fields the plugins stream doesn't have yet: the agent, repository,
    /// the decision and the rule that made it. Kept under one key so the
    /// event still reads as an `AUDIT#` event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyFields>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PolicyFields {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub agent: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub repo: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub decision: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub rule: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub thread_id: String,
}

impl Event {
    pub fn new(actor: &str, kind: ActorKind, action: &str, result: &str) -> Self {
        Self {
            id: super::next_id(),
            at: super::now(),
            actor: actor.to_string(),
            actor_kind: kind,
            action: action.to_string(),
            result: result.to_string(),
            ..Default::default()
        }
    }
    pub fn run(mut self, id: &str) -> Self {
        self.run_id = id.to_string();
        self
    }
    pub fn approval(mut self, id: &str) -> Self {
        self.approval_id = id.to_string();
        self
    }
    pub fn tool(mut self, name: &str, risk: &str) -> Self {
        self.tool = name.to_string();
        self.risk = risk.to_string();
        self
    }
    pub fn detail(mut self, d: impl Into<String>) -> Self {
        self.detail = crate::truncate_str(&d.into(), 600).to_string();
        self
    }
    pub fn policy(mut self, p: PolicyFields) -> Self {
        self.policy = Some(p);
        self
    }
}

/// Where events go. A failure is logged and never stops the call: losing an
/// audit line must not undo a decision.
#[async_trait::async_trait]
pub trait AuditSink: Send + Sync {
    async fn record(&self, team: &str, e: Event);
}
