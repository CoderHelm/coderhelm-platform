//! Schedules and the worker queue, behind traits so the release check can
//! run the whole flow in memory.
//!
//! - A one-off schedule publishes an approved draft at the time the approver
//!   picked; cancelling deletes it, so nothing fires after a cancel.
//! - A recurring schedule starts a plugin's `schedule` trigger.
//! - A results check reads a published post's numbers a few days later.
//!
//! In production these are EventBridge Scheduler schedules in one group
//! that send their message to the tickets queue ([`EventBridge`]); a
//! one-off schedule deletes itself after it fires.

/// The SDK, for the services that build an [`EventBridge`].
pub use aws_sdk_scheduler;

use std::collections::BTreeMap;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use serde_json::Value;

/// Where messages for the worker go.
#[async_trait::async_trait]
pub trait Queue: Send + Sync {
    async fn send(&self, body: &Value) -> Result<(), String>;
}

/// Creates and deletes schedules.
#[async_trait::async_trait]
pub trait Scheduler: Send + Sync {
    /// Sends `body` once at `when`. Replaces a schedule of the same name.
    async fn at(&self, name: &str, when: DateTime<Utc>, body: &Value) -> Result<(), String>;
    /// Sends `body` on an EventBridge `cron(...)` expression (UTC).
    async fn cron(&self, name: &str, expr: &str, body: &Value) -> Result<(), String>;
    /// Deletes a schedule. `Ok(false)` when there was none (already fired or
    /// deleted).
    async fn delete(&self, name: &str) -> Result<bool, String>;
}

/// A schedule name EventBridge accepts (letters, digits, `-`, `_`, `.`, at
/// most 64), unique per team.
pub fn name(kind: &str, team: &str, id: &str) -> String {
    let t = &crate::content_hash(team)[..10];
    let id: String = id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    let n = format!("chp-{kind}-{t}-{id}");
    n.chars().take(64).collect()
}

// ── In memory (tests) ───────────────────────────────────────────────────────

#[derive(Default)]
pub struct MemoryQueue {
    pub sent: Mutex<Vec<Value>>,
}

#[async_trait::async_trait]
impl Queue for MemoryQueue {
    async fn send(&self, body: &Value) -> Result<(), String> {
        self.sent.lock().unwrap().push(body.clone());
        Ok(())
    }
}

impl MemoryQueue {
    /// Takes every message sent so far.
    pub fn take(&self) -> Vec<Value> {
        std::mem::take(&mut *self.sent.lock().unwrap())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Planned {
    At(DateTime<Utc>, Value),
    Cron(String, Value),
}

#[derive(Default)]
pub struct MemoryScheduler {
    pub items: Mutex<BTreeMap<String, Planned>>,
}

impl MemoryScheduler {
    /// Fires (and removes) every one-off schedule due by `now`, as
    /// EventBridge would.
    pub fn due(&self, now: DateTime<Utc>) -> Vec<Value> {
        let mut items = self.items.lock().unwrap();
        let due: Vec<String> = items
            .iter()
            .filter(|(_, p)| matches!(p, Planned::At(t, _) if *t <= now))
            .map(|(k, _)| k.clone())
            .collect();
        due.into_iter()
            .filter_map(|k| match items.remove(&k) {
                Some(Planned::At(_, v)) => Some(v),
                _ => None,
            })
            .collect()
    }

    pub fn has(&self, name: &str) -> bool {
        self.items.lock().unwrap().contains_key(name)
    }

    pub fn names(&self) -> Vec<String> {
        self.items.lock().unwrap().keys().cloned().collect()
    }
}

#[async_trait::async_trait]
impl Scheduler for MemoryScheduler {
    async fn at(&self, name: &str, when: DateTime<Utc>, body: &Value) -> Result<(), String> {
        self.items
            .lock()
            .unwrap()
            .insert(name.to_string(), Planned::At(when, body.clone()));
        Ok(())
    }
    async fn cron(&self, name: &str, expr: &str, body: &Value) -> Result<(), String> {
        self.items.lock().unwrap().insert(
            name.to_string(),
            Planned::Cron(expr.to_string(), body.clone()),
        );
        Ok(())
    }
    async fn delete(&self, name: &str) -> Result<bool, String> {
        Ok(self.items.lock().unwrap().remove(name).is_some())
    }
}

// ── AWS ─────────────────────────────────────────────────────────────────────

/// The tickets queue.
pub struct Sqs {
    pub client: aws_sdk_sqs::Client,
    pub url: String,
}

#[async_trait::async_trait]
impl Queue for Sqs {
    async fn send(&self, body: &Value) -> Result<(), String> {
        if self.url.is_empty() {
            return Err("no queue configured".into());
        }
        self.client
            .send_message()
            .queue_url(&self.url)
            .message_body(body.to_string())
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("{}", aws_sdk_sqs::error::DisplayErrorContext(&e)))
    }
}

/// EventBridge Scheduler: schedules in `group` that send to the queue at
/// `queue_arn` with `role_arn` (a role only allowed to send there).
pub struct EventBridge {
    pub client: aws_sdk_scheduler::Client,
    pub group: String,
    pub role_arn: String,
    pub queue_arn: String,
}

impl EventBridge {
    fn ready(&self) -> Result<(), String> {
        if self.group.is_empty() || self.role_arn.is_empty() || self.queue_arn.is_empty() {
            Err("plugin schedules aren't configured (PLUGIN_SCHEDULE_GROUP, PLUGIN_SCHEDULER_ROLE_ARN, TICKET_QUEUE_ARN)".into())
        } else {
            Ok(())
        }
    }

    fn target(&self, body: &Value) -> Result<aws_sdk_scheduler::types::Target, String> {
        aws_sdk_scheduler::types::Target::builder()
            .arn(&self.queue_arn)
            .role_arn(&self.role_arn)
            .input(body.to_string())
            .build()
            .map_err(|e| e.to_string())
    }

    async fn create(
        &self,
        name: &str,
        expr: String,
        body: &Value,
        one_off: bool,
    ) -> Result<(), String> {
        use aws_sdk_scheduler::types::{
            ActionAfterCompletion, FlexibleTimeWindow, FlexibleTimeWindowMode,
        };
        self.ready()?;
        let _ = self.delete(name).await;
        let mut req = self
            .client
            .create_schedule()
            .name(name)
            .group_name(&self.group)
            .schedule_expression(expr)
            .schedule_expression_timezone("UTC")
            .flexible_time_window(
                FlexibleTimeWindow::builder()
                    .mode(FlexibleTimeWindowMode::Off)
                    .build()
                    .map_err(|e| e.to_string())?,
            )
            .target(self.target(body)?);
        if one_off {
            req = req.action_after_completion(ActionAfterCompletion::Delete);
        }
        req.send()
            .await
            .map(|_| ())
            .map_err(|e| format!("{}", aws_sdk_scheduler::error::DisplayErrorContext(&e)))
    }
}

#[async_trait::async_trait]
impl Scheduler for EventBridge {
    async fn at(&self, name: &str, when: DateTime<Utc>, body: &Value) -> Result<(), String> {
        let expr = format!("at({})", when.format("%Y-%m-%dT%H:%M:%S"));
        self.create(name, expr, body, true).await
    }
    async fn cron(&self, name: &str, expr: &str, body: &Value) -> Result<(), String> {
        self.create(name, expr.to_string(), body, false).await
    }
    async fn delete(&self, name: &str) -> Result<bool, String> {
        use aws_sdk_scheduler::operation::delete_schedule::DeleteScheduleError;
        self.ready()?;
        match self
            .client
            .delete_schedule()
            .name(name)
            .group_name(&self.group)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => match e.as_service_error() {
                Some(DeleteScheduleError::ResourceNotFoundException(_)) => Ok(false),
                _ => Err(format!(
                    "{}",
                    aws_sdk_scheduler::error::DisplayErrorContext(&e)
                )),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn names_are_short_unique_per_team_and_safe() {
        let a = name("pub", "TEAM#a", "01abc/def");
        let b = name("pub", "TEAM#b", "01abc/def");
        assert_ne!(a, b);
        assert!(a.len() <= 64);
        assert!(a.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'));
        assert!(name("cron", "T", &"x".repeat(200)).len() <= 64);
    }

    #[tokio::test]
    async fn one_off_schedules_fire_once_and_a_cancel_stops_them() {
        let s = MemoryScheduler::default();
        let t0 = Utc::now();
        s.at("a", t0 + chrono::Duration::hours(1), &json!({"n": 1}))
            .await
            .unwrap();
        s.at("b", t0 + chrono::Duration::hours(2), &json!({"n": 2}))
            .await
            .unwrap();
        assert!(s.due(t0).is_empty());
        assert!(s.delete("b").await.unwrap());
        assert!(!s.delete("b").await.unwrap());
        let fired = s.due(t0 + chrono::Duration::hours(3));
        assert_eq!(fired, vec![json!({"n": 1})]);
        assert!(s.due(t0 + chrono::Duration::hours(4)).is_empty());
    }
}
