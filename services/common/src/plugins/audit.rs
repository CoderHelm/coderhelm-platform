//! One audit stream: `AUDIT#<ulid>` under the team, newest last.
//!
//! Every plugin step writes here: who or what triggered a run, the plugin
//! version, each tool and its risk, every draft, approval, stop, schedule
//! and publish, and every install, update, rollback and setting change. The
//! activity logs that predate it (skills access, tool asks, MCP calls, run
//! traces, role changes) fold into it over time; new work writes only here.
//!
//! An event is never edited or deleted by CoderHelm.

use serde::{Deserialize, Serialize};

use super::kv::{get_as, put_as, Expect, Kv};
use super::new_id;

pub const PREFIX: &str = "AUDIT#";
/// Events one page of the audit page shows.
pub const PAGE: usize = 50;

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

/// The actions, by name (the audit page filters on them).
pub mod action {
    pub const FLAG_SET: &str = "flag.set";
    pub const SETTINGS_SAVED: &str = "settings.saved";
    pub const SOURCE_ADDED: &str = "source.added";
    pub const SOURCE_REMOVED: &str = "source.removed";
    pub const SOURCE_SYNCED: &str = "source.synced";
    pub const INSTALL_REQUESTED: &str = "plugin.install_requested";
    pub const INSTALLED: &str = "plugin.installed";
    pub const INSTALL_DECLINED: &str = "plugin.install_declined";
    pub const UPDATED: &str = "plugin.updated";
    pub const UPDATE_WAITING: &str = "plugin.update_waiting";
    pub const UPDATE_APPROVED: &str = "plugin.update_approved";
    pub const UPDATE_DECLINED: &str = "plugin.update_declined";
    pub const UPDATE_AVAILABLE: &str = "plugin.update_available";
    pub const ROLLED_BACK: &str = "plugin.rolled_back";
    pub const OPTIONS_CHANGED: &str = "plugin.options_changed";
    pub const PLUGIN_SETTINGS: &str = "plugin.settings_changed";
    pub const ENABLED: &str = "plugin.enabled";
    pub const DISABLED: &str = "plugin.disabled";
    pub const UNINSTALLED: &str = "plugin.uninstalled";
    pub const RUN_REQUESTED: &str = "run.requested";
    pub const RUN_STARTED: &str = "run.started";
    pub const RUN_REFUSED: &str = "run.refused";
    pub const RUN_FINISHED: &str = "run.finished";
    pub const RUN_FAILED: &str = "run.failed";
    pub const TOOL_CALLED: &str = "tool.called";
    pub const TOOL_REFUSED: &str = "tool.refused";
    pub const TOOL_QUEUED: &str = "tool.queued_for_approval";
    pub const DRAFT_SAVED: &str = "draft.saved";
    pub const DRAFT_EDITED: &str = "draft.edited";
    pub const DRAFT_SUBMITTED: &str = "draft.submitted";
    pub const APPROVED: &str = "draft.approved";
    pub const APPROVAL_REFUSED: &str = "draft.approval_refused";
    pub const REJECTED: &str = "draft.rejected";
    pub const STOPPED: &str = "draft.stopped";
    pub const SCHEDULED: &str = "publish.scheduled";
    pub const CANCELLED: &str = "publish.cancelled";
    pub const PUBLISHED: &str = "publish.done";
    pub const PUBLISH_FAILED: &str = "publish.failed";
    pub const PUBLISH_REFUSED: &str = "publish.refused";
    pub const RESULTS: &str = "results.recorded";
    pub const CHAT_BRIEF: &str = "chat.brief";
    pub const CHAT_REFUSED: &str = "chat.refused";
}

/// One event.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    pub at: String,
    /// An email, `plugin:<id>` for an agent, `schedule`, `system`.
    pub actor: String,
    #[serde(default)]
    pub actor_kind: ActorKind,
    pub action: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub plugin_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub plugin_version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub artifact_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub approval_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tool: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub risk: String,
    /// `ok`, `refused`, `failed`, `waiting`, or a short outcome.
    pub result: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub detail: String,
}

impl Event {
    pub fn new(actor: &str, kind: ActorKind, action: &str, result: &str) -> Self {
        Self {
            id: new_id(),
            at: super::now_rfc3339(),
            actor: actor.to_string(),
            actor_kind: kind,
            action: action.to_string(),
            result: result.to_string(),
            ..Default::default()
        }
    }
    pub fn plugin(mut self, id: &str, version: &str) -> Self {
        self.plugin_id = id.to_string();
        self.plugin_version = version.to_string();
        self
    }
    pub fn run(mut self, id: &str) -> Self {
        self.run_id = id.to_string();
        self
    }
    pub fn artifact(mut self, id: &str) -> Self {
        self.artifact_id = id.to_string();
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
}

/// Appends an event. A failure is logged and never stops the step: the step
/// itself is the record of truth, and losing an audit line must not undo
/// an approval. (Every step that matters also lands on its own record.)
pub async fn record(kv: &dyn Kv, team: &str, e: Event) {
    let sk = format!("{PREFIX}{}", e.id);
    if let Err(err) = put_as(kv, team, &sk, &e, Expect::Absent).await {
        tracing::error!(team, action = %e.action, error = %err, "Couldn't write an audit event");
    }
}

/// What the audit page asks for.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    pub plugin_id: Option<String>,
    pub action: Option<String>,
    pub run_id: Option<String>,
    pub artifact_id: Option<String>,
}

impl Filter {
    fn matches(&self, e: &Event) -> bool {
        self.plugin_id
            .as_deref()
            .map(|p| e.plugin_id == p)
            .unwrap_or(true)
            && self
                .action
                .as_deref()
                .map(|a| e.action == a || e.action.starts_with(&format!("{a}.")))
                .unwrap_or(true)
            && self
                .run_id
                .as_deref()
                .map(|r| e.run_id == r)
                .unwrap_or(true)
            && self
                .artifact_id
                .as_deref()
                .map(|a| e.artifact_id == a)
                .unwrap_or(true)
    }
}

/// One page of events, newest first, and the cursor for the next page.
pub async fn page(
    kv: &dyn Kv,
    team: &str,
    filter: &Filter,
    cursor: Option<&str>,
    want: usize,
) -> Result<(Vec<Event>, Option<String>), String> {
    let mut out = Vec::new();
    let mut after = cursor.map(|c| format!("{PREFIX}{c}"));
    // Read in chunks until the page is full or the stream ends; a filter
    // can skip many events, so the scan is bounded.
    for _ in 0..20 {
        let chunk = kv
            .query(team, PREFIX, true, Some(200), after.as_deref())
            .await?;
        if chunk.is_empty() {
            return Ok((out, None));
        }
        let last = chunk.last().map(|r| r.sk.clone());
        for r in chunk {
            let sk = r.sk.clone();
            if let Ok(e) = serde_json::from_value::<Event>(r.data) {
                if filter.matches(&e) {
                    out.push(e);
                    if out.len() == want {
                        let next = sk.strip_prefix(PREFIX).map(str::to_string);
                        return Ok((out, next));
                    }
                }
            }
        }
        after = last;
    }
    let next = after.and_then(|a| a.strip_prefix(PREFIX).map(str::to_string));
    Ok((out, next))
}

/// Every event for one run or one artifact (the run page and draft page).
pub async fn for_filter(kv: &dyn Kv, team: &str, filter: &Filter) -> Vec<Event> {
    let mut all = page(kv, team, filter, None, 500)
        .await
        .map(|(v, _)| v)
        .unwrap_or_default();
    all.reverse();
    all
}

/// One event by id (tests).
pub async fn get(kv: &dyn Kv, team: &str, id: &str) -> Option<Event> {
    get_as::<Event>(kv, team, &format!("{PREFIX}{id}"))
        .await
        .ok()
        .flatten()
        .map(|(e, _)| e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::kv::Memory;

    #[tokio::test]
    async fn events_page_newest_first_and_filter() {
        let kv = Memory::new();
        for i in 0..7 {
            let e = Event::new("a@x.io", ActorKind::Person, action::TOOL_CALLED, "ok")
                .plugin(if i % 2 == 0 { "p1" } else { "p2" }, "1.0.0")
                .detail(format!("n{i}"));
            record(&kv, "T", e).await;
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        let (p, next) = page(&kv, "T", &Filter::default(), None, 3).await.unwrap();
        assert_eq!(p.len(), 3);
        assert_eq!(p[0].detail, "n6");
        let (p2, _) = page(&kv, "T", &Filter::default(), next.as_deref(), 3)
            .await
            .unwrap();
        assert_eq!(p2[0].detail, "n3");
        let only = Filter {
            plugin_id: Some("p2".into()),
            ..Default::default()
        };
        let (p, _) = page(&kv, "T", &only, None, 50).await.unwrap();
        assert_eq!(p.len(), 3);
        let by_action = Filter {
            action: Some("tool".into()),
            ..Default::default()
        };
        assert_eq!(
            page(&kv, "T", &by_action, None, 50).await.unwrap().0.len(),
            7
        );
        // Another team's stream is empty.
        assert!(page(&kv, "U", &Filter::default(), None, 50)
            .await
            .unwrap()
            .0
            .is_empty());
    }
}
