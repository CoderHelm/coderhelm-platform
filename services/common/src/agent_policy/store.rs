//! Where agent permissions are kept: the settings table, under the team's
//! own key, so nothing here is reachable from another team.
//!
//! - `AGENT_POLICY`: the team's policy (rules, switches, pause, sandbox),
//!   one record with a version; a save that read an older version is refused.
//! - `AGENT_APPROVAL#<id>`: one approval card. Kept 30 days.
//! - `AGENT_GRANT#<scope>#<hash>`: a one-time "approve once", used once.
//! - `AGENT_SIGNIN#<thread>#<server>`: a sign-in prompt raised or declined
//!   in one thread. Kept 30 days.
//! - `AGENT_QUEUE#<ulid>`: work that arrived while agents were paused.
//! - `AGENT_LOG#<ulid>`: the decision log (see [`super::audit`]). Kept 90
//!   days.

use std::collections::HashMap;
use std::sync::Mutex;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::approvals::{Approval, Status};
use super::audit::{AuditSink, Event};
use super::model::TeamPolicy;
use super::signin::ThreadState;

pub const POLICY_SK: &str = "AGENT_POLICY";
pub const APPROVAL_PREFIX: &str = "AGENT_APPROVAL#";
pub const GRANT_PREFIX: &str = "AGENT_GRANT#";
pub const SIGNIN_PREFIX: &str = "AGENT_SIGNIN#";
pub const QUEUE_PREFIX: &str = "AGENT_QUEUE#";
pub const LOG_PREFIX: &str = "AGENT_LOG#";
/// One partition listing every paused team (sk = team id), so the
/// scheduled sweep finds pauses whose until-time passed without a scan.
pub const PAUSED_PK: &str = "AGENT_PAUSED_TEAMS";
pub const APPROVAL_TTL_SECS: i64 = 30 * 86_400;
pub const LOG_TTL_SECS: i64 = 90 * 86_400;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// Someone saved a newer version first.
    Conflict,
    Other(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Conflict => f.write_str(
                "Someone else changed agent permissions while you were editing. Reload and try again.",
            ),
            StoreError::Other(e) => f.write_str(e),
        }
    }
}

/// Work that arrived while agents were paused: the message as it would have
/// been sent, replayed in order when the pause ends.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueuedWork {
    pub id: String,
    pub queued_at: String,
    /// What it is, in plain words ("Run for ACME-12").
    pub label: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    /// The worker message, exactly.
    pub message: Value,
}

#[async_trait::async_trait]
pub trait PolicyStore: Send + Sync {
    /// The team's policy; [`TeamPolicy::starting`] when none is saved.
    async fn load(&self, team: &str) -> Result<TeamPolicy, String>;
    /// Saves `policy` (whose version is already bumped) if the stored one is
    /// still `expect`.
    async fn save(&self, team: &str, policy: &TeamPolicy, expect: u64) -> Result<(), StoreError>;

    async fn put_approval(&self, team: &str, a: &Approval) -> Result<(), String>;
    async fn get_approval(&self, team: &str, id: &str) -> Result<Option<Approval>, String>;
    /// Writes `a` only if the stored card is still `expect`. False if not.
    async fn update_approval(
        &self,
        team: &str,
        a: &Approval,
        expect: Status,
    ) -> Result<bool, String>;
    /// Newest first.
    async fn list_approvals(&self, team: &str, pending_only: bool)
        -> Result<Vec<Approval>, String>;

    async fn put_grant(
        &self,
        team: &str,
        key: &str,
        approval_id: &str,
        expires_at: &str,
    ) -> Result<(), String>;
    /// Uses a grant: true once, false after (and when there is none).
    async fn take_grant(&self, team: &str, key: &str) -> Result<Option<String>, String>;

    async fn get_sign_in(
        &self,
        team: &str,
        thread: &str,
        server: &str,
    ) -> Result<Option<ThreadState>, String>;
    /// With `only_if_new`, writes only when nothing is there and says whether
    /// it did.
    async fn put_sign_in(
        &self,
        team: &str,
        thread: &str,
        server: &str,
        state: ThreadState,
        only_if_new: bool,
    ) -> Result<bool, String>;
    async fn clear_sign_in(&self, team: &str, thread: &str, server: &str) -> Result<(), String>;

    async fn queue_work(&self, team: &str, w: &QueuedWork) -> Result<(), String>;
    /// Oldest first.
    async fn list_queued(&self, team: &str) -> Result<Vec<QueuedWork>, String>;
    async fn remove_queued(&self, team: &str, id: &str) -> Result<(), String>;

    /// Notes that a team is paused (or has kept work), for the sweep.
    async fn mark_paused(&self, team: &str) -> Result<(), String>;
    async fn unmark_paused(&self, team: &str) -> Result<(), String>;
    async fn paused_teams(&self) -> Result<Vec<String>, String>;

    /// Appends an audit event; a failure is logged, never returned.
    async fn audit(&self, team: &str, e: Event);
    /// The newest audit events, newest first.
    async fn recent_audit(&self, team: &str, limit: usize) -> Result<Vec<Event>, String>;
}

#[async_trait::async_trait]
impl<T: PolicyStore + ?Sized> AuditSink for T {
    async fn record(&self, team: &str, e: Event) {
        self.audit(team, e).await
    }
}

// ─── In memory (tests, and the gateway's and worker's tests) ──────────────

/// Everything in memory, keyed by team like the real table.
#[derive(Default)]
pub struct MemStore {
    policies: Mutex<HashMap<String, TeamPolicy>>,
    approvals: Mutex<HashMap<(String, String), Approval>>,
    grants: Mutex<HashMap<(String, String), String>>,
    sign_ins: Mutex<HashMap<(String, String, String), ThreadState>>,
    queue: Mutex<Vec<(String, QueuedWork)>>,
    paused: Mutex<std::collections::BTreeSet<String>>,
    pub events: Mutex<Vec<(String, Event)>>,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }
    /// Events recorded for a team, oldest first.
    pub fn events_for(&self, team: &str) -> Vec<Event> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, _)| t == team)
            .map(|(_, e)| e.clone())
            .collect()
    }
}

#[async_trait::async_trait]
impl PolicyStore for MemStore {
    async fn load(&self, team: &str) -> Result<TeamPolicy, String> {
        Ok(self
            .policies
            .lock()
            .unwrap()
            .get(team)
            .cloned()
            .unwrap_or_else(TeamPolicy::starting))
    }
    async fn save(&self, team: &str, policy: &TeamPolicy, expect: u64) -> Result<(), StoreError> {
        let mut m = self.policies.lock().unwrap();
        let current = m.get(team).map_or(0, |p| p.version);
        if current != expect {
            return Err(StoreError::Conflict);
        }
        m.insert(team.to_string(), policy.clone());
        Ok(())
    }
    async fn put_approval(&self, team: &str, a: &Approval) -> Result<(), String> {
        self.approvals
            .lock()
            .unwrap()
            .insert((team.into(), a.id.clone()), a.clone());
        Ok(())
    }
    async fn get_approval(&self, team: &str, id: &str) -> Result<Option<Approval>, String> {
        Ok(self
            .approvals
            .lock()
            .unwrap()
            .get(&(team.into(), id.into()))
            .cloned())
    }
    async fn update_approval(
        &self,
        team: &str,
        a: &Approval,
        expect: Status,
    ) -> Result<bool, String> {
        let mut m = self.approvals.lock().unwrap();
        let key = (team.to_string(), a.id.clone());
        match m.get(&key) {
            Some(cur) if cur.status == expect => {
                m.insert(key, a.clone());
                Ok(true)
            }
            _ => Ok(false),
        }
    }
    async fn list_approvals(
        &self,
        team: &str,
        pending_only: bool,
    ) -> Result<Vec<Approval>, String> {
        let mut v: Vec<Approval> = self
            .approvals
            .lock()
            .unwrap()
            .iter()
            .filter(|((t, _), a)| t == team && (!pending_only || a.status == Status::Pending))
            .map(|(_, a)| a.clone())
            .collect();
        v.sort_by(|a, b| b.created_at.cmp(&a.created_at).then(b.id.cmp(&a.id)));
        Ok(v)
    }
    async fn put_grant(
        &self,
        team: &str,
        key: &str,
        approval_id: &str,
        _expires_at: &str,
    ) -> Result<(), String> {
        self.grants
            .lock()
            .unwrap()
            .insert((team.into(), key.into()), approval_id.into());
        Ok(())
    }
    async fn take_grant(&self, team: &str, key: &str) -> Result<Option<String>, String> {
        Ok(self
            .grants
            .lock()
            .unwrap()
            .remove(&(team.into(), key.into())))
    }
    async fn get_sign_in(
        &self,
        team: &str,
        thread: &str,
        server: &str,
    ) -> Result<Option<ThreadState>, String> {
        Ok(self
            .sign_ins
            .lock()
            .unwrap()
            .get(&(team.into(), thread.into(), server.into()))
            .copied())
    }
    async fn put_sign_in(
        &self,
        team: &str,
        thread: &str,
        server: &str,
        state: ThreadState,
        only_if_new: bool,
    ) -> Result<bool, String> {
        let mut m = self.sign_ins.lock().unwrap();
        let key = (team.to_string(), thread.to_string(), server.to_string());
        if only_if_new && m.contains_key(&key) {
            return Ok(false);
        }
        m.insert(key, state);
        Ok(true)
    }
    async fn clear_sign_in(&self, team: &str, thread: &str, server: &str) -> Result<(), String> {
        self.sign_ins
            .lock()
            .unwrap()
            .remove(&(team.into(), thread.into(), server.into()));
        Ok(())
    }
    async fn queue_work(&self, team: &str, w: &QueuedWork) -> Result<(), String> {
        self.queue.lock().unwrap().push((team.into(), w.clone()));
        Ok(())
    }
    async fn list_queued(&self, team: &str) -> Result<Vec<QueuedWork>, String> {
        let mut v: Vec<QueuedWork> = self
            .queue
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, _)| t == team)
            .map(|(_, w)| w.clone())
            .collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(v)
    }
    async fn remove_queued(&self, team: &str, id: &str) -> Result<(), String> {
        self.queue
            .lock()
            .unwrap()
            .retain(|(t, w)| !(t == team && w.id == id));
        Ok(())
    }
    async fn mark_paused(&self, team: &str) -> Result<(), String> {
        self.paused.lock().unwrap().insert(team.into());
        Ok(())
    }
    async fn unmark_paused(&self, team: &str) -> Result<(), String> {
        self.paused.lock().unwrap().remove(team);
        Ok(())
    }
    async fn paused_teams(&self) -> Result<Vec<String>, String> {
        Ok(self.paused.lock().unwrap().iter().cloned().collect())
    }
    async fn audit(&self, team: &str, e: Event) {
        self.events.lock().unwrap().push((team.into(), e));
    }
    async fn recent_audit(&self, team: &str, limit: usize) -> Result<Vec<Event>, String> {
        let mut v = self.events_for(team);
        v.reverse();
        v.truncate(limit);
        Ok(v)
    }
}

// ─── DynamoDB ─────────────────────────────────────────────────────────────

/// The settings table.
#[derive(Clone)]
pub struct DynamoStore {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub table: String,
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}
fn n(v: impl ToString) -> AttributeValue {
    AttributeValue::N(v.to_string())
}
fn ttl_in(secs: i64) -> AttributeValue {
    n(chrono::Utc::now().timestamp() + secs)
}
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn data_of<T: serde::de::DeserializeOwned>(item: &HashMap<String, AttributeValue>) -> Option<T> {
    let d = item.get("data")?.as_s().ok()?;
    serde_json::from_str(d).ok()
}
fn is_condition_failed<E: std::fmt::Debug>(e: &aws_sdk_dynamodb::error::SdkError<E>) -> bool {
    format!("{e:?}").contains("ConditionalCheckFailed")
}

impl DynamoStore {
    pub fn new(dynamo: aws_sdk_dynamodb::Client, table: impl Into<String>) -> Self {
        Self {
            dynamo,
            table: table.into(),
        }
    }

    async fn query_prefix(
        &self,
        team: &str,
        prefix: &str,
        newest_first: bool,
        limit: Option<i32>,
    ) -> Result<Vec<HashMap<String, AttributeValue>>, String> {
        let mut out = Vec::new();
        let mut start: Option<HashMap<String, AttributeValue>> = None;
        loop {
            let mut q = self
                .dynamo
                .query()
                .table_name(&self.table)
                .expression_attribute_values(":pk", s(team))
                .scan_index_forward(!newest_first)
                .set_exclusive_start_key(start.take());
            q = if prefix.is_empty() {
                q.key_condition_expression("pk = :pk")
            } else {
                q.key_condition_expression("pk = :pk AND begins_with(sk, :p)")
                    .expression_attribute_values(":p", s(prefix))
            };
            if let Some(l) = limit {
                q = q.limit(l);
            }
            let r = q.send().await.map_err(err)?;
            out.extend(r.items().iter().cloned());
            match r.last_evaluated_key() {
                Some(k) if limit.is_none_or(|l| (out.len() as i32) < l) => start = Some(k.clone()),
                _ => break,
            }
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
impl PolicyStore for DynamoStore {
    async fn load(&self, team: &str) -> Result<TeamPolicy, String> {
        let r = self
            .dynamo
            .get_item()
            .table_name(&self.table)
            .key("pk", s(team))
            .key("sk", s(POLICY_SK))
            .consistent_read(true)
            .send()
            .await
            .map_err(err)?;
        Ok(r.item()
            .and_then(data_of)
            .unwrap_or_else(TeamPolicy::starting))
    }

    async fn save(&self, team: &str, policy: &TeamPolicy, expect: u64) -> Result<(), StoreError> {
        let data = serde_json::to_string(policy).map_err(|e| StoreError::Other(e.to_string()))?;
        let mut put = self
            .dynamo
            .put_item()
            .table_name(&self.table)
            .item("pk", s(team))
            .item("sk", s(POLICY_SK))
            .item("data", s(&data))
            .item("v", n(policy.version));
        put = if expect == 0 {
            put.condition_expression("attribute_not_exists(v) OR v = :e")
                .expression_attribute_values(":e", n(0))
        } else {
            put.condition_expression("v = :e")
                .expression_attribute_values(":e", n(expect))
        };
        match put.send().await {
            Ok(_) => Ok(()),
            Err(e) if is_condition_failed(&e) => Err(StoreError::Conflict),
            Err(e) => Err(StoreError::Other(e.to_string())),
        }
    }

    async fn put_approval(&self, team: &str, a: &Approval) -> Result<(), String> {
        self.dynamo
            .put_item()
            .table_name(&self.table)
            .item("pk", s(team))
            .item("sk", s(&format!("{APPROVAL_PREFIX}{}", a.id)))
            .item("data", s(&serde_json::to_string(a).map_err(err)?))
            .item("status", s(a.status.as_str()))
            .item("ttl", ttl_in(APPROVAL_TTL_SECS))
            .condition_expression("attribute_not_exists(sk)")
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn get_approval(&self, team: &str, id: &str) -> Result<Option<Approval>, String> {
        let r = self
            .dynamo
            .get_item()
            .table_name(&self.table)
            .key("pk", s(team))
            .key("sk", s(&format!("{APPROVAL_PREFIX}{id}")))
            .consistent_read(true)
            .send()
            .await
            .map_err(err)?;
        Ok(r.item().and_then(data_of))
    }

    async fn update_approval(
        &self,
        team: &str,
        a: &Approval,
        expect: Status,
    ) -> Result<bool, String> {
        let r = self
            .dynamo
            .put_item()
            .table_name(&self.table)
            .item("pk", s(team))
            .item("sk", s(&format!("{APPROVAL_PREFIX}{}", a.id)))
            .item("data", s(&serde_json::to_string(a).map_err(err)?))
            .item("status", s(a.status.as_str()))
            .item("ttl", ttl_in(APPROVAL_TTL_SECS))
            .condition_expression("#st = :e")
            .expression_attribute_names("#st", "status")
            .expression_attribute_values(":e", s(expect.as_str()))
            .send()
            .await;
        match r {
            Ok(_) => Ok(true),
            Err(e) if is_condition_failed(&e) => Ok(false),
            Err(e) => Err(e.to_string()),
        }
    }

    async fn list_approvals(
        &self,
        team: &str,
        pending_only: bool,
    ) -> Result<Vec<Approval>, String> {
        let items = self.query_prefix(team, APPROVAL_PREFIX, true, None).await?;
        let mut v: Vec<Approval> = items
            .iter()
            .filter_map(data_of::<Approval>)
            .filter(|a| !pending_only || a.status == Status::Pending)
            .collect();
        v.sort_by(|a, b| b.created_at.cmp(&a.created_at).then(b.id.cmp(&a.id)));
        Ok(v)
    }

    async fn put_grant(
        &self,
        team: &str,
        key: &str,
        approval_id: &str,
        _expires_at: &str,
    ) -> Result<(), String> {
        self.dynamo
            .put_item()
            .table_name(&self.table)
            .item("pk", s(team))
            .item("sk", s(&format!("{GRANT_PREFIX}{key}")))
            .item("approval_id", s(approval_id))
            .item("ttl", ttl_in(2 * 86_400))
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn take_grant(&self, team: &str, key: &str) -> Result<Option<String>, String> {
        // Deleting with ReturnValues ALL_OLD uses it exactly once even when
        // two calls race.
        let r = self
            .dynamo
            .delete_item()
            .table_name(&self.table)
            .key("pk", s(team))
            .key("sk", s(&format!("{GRANT_PREFIX}{key}")))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
            .send()
            .await
            .map_err(err)?;
        Ok(r.attributes()
            .and_then(|a| a.get("approval_id"))
            .and_then(|v| v.as_s().ok())
            .cloned())
    }

    async fn get_sign_in(
        &self,
        team: &str,
        thread: &str,
        server: &str,
    ) -> Result<Option<ThreadState>, String> {
        let r = self
            .dynamo
            .get_item()
            .table_name(&self.table)
            .key("pk", s(team))
            .key("sk", s(&format!("{SIGNIN_PREFIX}{thread}#{server}")))
            .consistent_read(true)
            .send()
            .await
            .map_err(err)?;
        Ok(r.item()
            .and_then(|i| i.get("state"))
            .and_then(|v| v.as_s().ok())
            .and_then(|st| match st.as_str() {
                "asked" => Some(ThreadState::Asked),
                "declined" => Some(ThreadState::Declined),
                _ => None,
            }))
    }

    async fn put_sign_in(
        &self,
        team: &str,
        thread: &str,
        server: &str,
        state: ThreadState,
        only_if_new: bool,
    ) -> Result<bool, String> {
        let mut put = self
            .dynamo
            .put_item()
            .table_name(&self.table)
            .item("pk", s(team))
            .item("sk", s(&format!("{SIGNIN_PREFIX}{thread}#{server}")))
            .item(
                "state",
                s(match state {
                    ThreadState::Asked => "asked",
                    ThreadState::Declined => "declined",
                }),
            )
            .item("ttl", ttl_in(APPROVAL_TTL_SECS));
        if only_if_new {
            put = put.condition_expression("attribute_not_exists(sk)");
        }
        match put.send().await {
            Ok(_) => Ok(true),
            Err(e) if is_condition_failed(&e) => Ok(false),
            Err(e) => Err(e.to_string()),
        }
    }

    async fn clear_sign_in(&self, team: &str, thread: &str, server: &str) -> Result<(), String> {
        self.dynamo
            .delete_item()
            .table_name(&self.table)
            .key("pk", s(team))
            .key("sk", s(&format!("{SIGNIN_PREFIX}{thread}#{server}")))
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn queue_work(&self, team: &str, w: &QueuedWork) -> Result<(), String> {
        self.dynamo
            .put_item()
            .table_name(&self.table)
            .item("pk", s(team))
            .item("sk", s(&format!("{QUEUE_PREFIX}{}", w.id)))
            .item("data", s(&serde_json::to_string(w).map_err(err)?))
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn list_queued(&self, team: &str) -> Result<Vec<QueuedWork>, String> {
        let items = self.query_prefix(team, QUEUE_PREFIX, false, None).await?;
        Ok(items.iter().filter_map(data_of).collect())
    }

    async fn remove_queued(&self, team: &str, id: &str) -> Result<(), String> {
        self.dynamo
            .delete_item()
            .table_name(&self.table)
            .key("pk", s(team))
            .key("sk", s(&format!("{QUEUE_PREFIX}{id}")))
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn mark_paused(&self, team: &str) -> Result<(), String> {
        self.dynamo
            .put_item()
            .table_name(&self.table)
            .item("pk", s(PAUSED_PK))
            .item("sk", s(team))
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn unmark_paused(&self, team: &str) -> Result<(), String> {
        self.dynamo
            .delete_item()
            .table_name(&self.table)
            .key("pk", s(PAUSED_PK))
            .key("sk", s(team))
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn paused_teams(&self) -> Result<Vec<String>, String> {
        let items = self.query_prefix(PAUSED_PK, "", false, None).await?;
        Ok(items
            .iter()
            .filter_map(|i| i.get("sk").and_then(|v| v.as_s().ok()).cloned())
            .collect())
    }

    async fn audit(&self, team: &str, e: Event) {
        let data = match serde_json::to_string(&e) {
            Ok(d) => d,
            Err(_) => return,
        };
        let r = self
            .dynamo
            .put_item()
            .table_name(&self.table)
            .item("pk", s(team))
            .item("sk", s(&format!("{LOG_PREFIX}{}", e.id)))
            .item("data", s(&data))
            .item("ttl", ttl_in(LOG_TTL_SECS))
            .send()
            .await;
        if let Err(err) = r {
            tracing::error!(team, action = %e.action, error = %err, "Couldn't write an agent decision to the log");
        }
    }

    async fn recent_audit(&self, team: &str, limit: usize) -> Result<Vec<Event>, String> {
        let items = self
            .query_prefix(team, LOG_PREFIX, true, Some(limit.clamp(1, 200) as i32))
            .await?;
        Ok(items.iter().filter_map(data_of).take(limit).collect())
    }
}
