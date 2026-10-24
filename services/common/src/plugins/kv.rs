//! Where plugin records live: one JSON document per key, under the team.
//!
//! Every record is `(pk, sk) -> data` with a revision number. `pk` is the
//! team id for everything a team owns, so a lookup can only ever reach the
//! team that asked; the two index partitions ([`SOURCE_INDEX`],
//! [`SCHEDULE_INDEX`]) hold only `team|id` pointers for the periodic jobs.
//!
//! Writes that decide something (an approval, a stop, claiming a publish)
//! are conditional on the revision they read ([`Expect::Rev`]), so two people
//! acting at once can't both win.
//!
//! The settings table holds them in production ([`Dynamo`]); [`Memory`] is
//! the same contract for tests.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

/// The partition listing every team's plugin sources: `team|source_id`.
pub const SOURCE_INDEX: &str = "PKGSRC_INDEX";
/// The partition listing every team with installed plugins: `team`.
pub const TEAM_INDEX: &str = "PKG_TEAMS";
/// Slack workspaces and Teams webhooks mapped to a team for inbound briefs.
pub const CHAT_INDEX: &str = "PKG_CHAT_INDEX";
/// Recurring schedules created for plugins: `team|plugin|n`.
pub const SCHEDULE_INDEX: &str = "PKG_SCHEDULES";

/// One stored record.
#[derive(Debug, Clone, PartialEq)]
pub struct Rec {
    pub sk: String,
    pub data: Value,
    pub rev: u64,
}

/// What a write expects of the stored record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    /// Write whatever is there.
    Any,
    /// Only when there is no record yet.
    Absent,
    /// Only when the record is still at this revision.
    Rev(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvError {
    /// The record changed (or appeared) since it was read.
    Conflict,
    Other(String),
}

impl std::fmt::Display for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvError::Conflict => write!(f, "the record changed meanwhile"),
            KvError::Other(e) => write!(f, "{e}"),
        }
    }
}

#[async_trait::async_trait]
pub trait Kv: Send + Sync {
    async fn get(&self, pk: &str, sk: &str) -> Result<Option<Rec>, String>;
    /// Writes `data`; returns the new revision.
    async fn put(&self, pk: &str, sk: &str, data: &Value, expect: Expect) -> Result<u64, KvError>;
    /// Records whose key starts with `prefix`, in key order (newest first
    /// when `reverse`, for keys that end in a time-ordered id), at most
    /// `limit`, starting after the key `after`.
    async fn query(
        &self,
        pk: &str,
        prefix: &str,
        reverse: bool,
        limit: Option<usize>,
        after: Option<&str>,
    ) -> Result<Vec<Rec>, String>;
    async fn delete(&self, pk: &str, sk: &str) -> Result<(), String>;
}

/// Reads a record as `T`.
pub async fn get_as<T: DeserializeOwned>(
    kv: &dyn Kv,
    pk: &str,
    sk: &str,
) -> Result<Option<(T, u64)>, String> {
    match kv.get(pk, sk).await? {
        Some(r) => serde_json::from_value(r.data)
            .map(|t| Some((t, r.rev)))
            .map_err(|e| format!("{sk}: {e}")),
        None => Ok(None),
    }
}

/// Writes `t` as a record.
pub async fn put_as<T: Serialize>(
    kv: &dyn Kv,
    pk: &str,
    sk: &str,
    t: &T,
    expect: Expect,
) -> Result<u64, KvError> {
    let v = serde_json::to_value(t).map_err(|e| KvError::Other(e.to_string()))?;
    kv.put(pk, sk, &v, expect).await
}

/// Every record under `prefix` as `T` (records that don't read are skipped).
pub async fn query_as<T: DeserializeOwned>(
    kv: &dyn Kv,
    pk: &str,
    prefix: &str,
) -> Result<Vec<T>, String> {
    Ok(kv
        .query(pk, prefix, false, None, None)
        .await?
        .into_iter()
        .filter_map(|r| serde_json::from_value(r.data).ok())
        .collect())
}

// ── In memory (tests) ───────────────────────────────────────────────────────

/// The same contract in memory.
#[derive(Default)]
pub struct Memory {
    data: Mutex<HashMap<String, BTreeMap<String, (Value, u64)>>>,
}

impl Memory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every key under a partition (tests).
    pub fn keys(&self, pk: &str) -> Vec<String> {
        self.data
            .lock()
            .unwrap()
            .get(pk)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl Kv for Memory {
    async fn get(&self, pk: &str, sk: &str) -> Result<Option<Rec>, String> {
        Ok(self
            .data
            .lock()
            .unwrap()
            .get(pk)
            .and_then(|m| m.get(sk))
            .map(|(v, rev)| Rec {
                sk: sk.to_string(),
                data: v.clone(),
                rev: *rev,
            }))
    }

    async fn put(&self, pk: &str, sk: &str, data: &Value, expect: Expect) -> Result<u64, KvError> {
        let mut all = self.data.lock().unwrap();
        let part = all.entry(pk.to_string()).or_default();
        let cur = part.get(sk).map(|(_, r)| *r);
        match (expect, cur) {
            (Expect::Absent, Some(_)) => return Err(KvError::Conflict),
            (Expect::Rev(want), Some(have)) if want != have => return Err(KvError::Conflict),
            (Expect::Rev(_), None) => return Err(KvError::Conflict),
            _ => {}
        }
        let rev = cur.unwrap_or(0) + 1;
        part.insert(sk.to_string(), (data.clone(), rev));
        Ok(rev)
    }

    async fn query(
        &self,
        pk: &str,
        prefix: &str,
        reverse: bool,
        limit: Option<usize>,
        after: Option<&str>,
    ) -> Result<Vec<Rec>, String> {
        let all = self.data.lock().unwrap();
        let Some(part) = all.get(pk) else {
            return Ok(Vec::new());
        };
        let mut v: Vec<Rec> = part
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, (d, r))| Rec {
                sk: k.clone(),
                data: d.clone(),
                rev: *r,
            })
            .collect();
        if reverse {
            v.reverse();
        }
        if let Some(a) = after {
            v.retain(|r| {
                if reverse {
                    r.sk.as_str() < a
                } else {
                    r.sk.as_str() > a
                }
            });
        }
        if let Some(n) = limit {
            v.truncate(n);
        }
        Ok(v)
    }

    async fn delete(&self, pk: &str, sk: &str) -> Result<(), String> {
        if let Some(p) = self.data.lock().unwrap().get_mut(pk) {
            p.remove(sk);
        }
        Ok(())
    }
}

// ── DynamoDB ────────────────────────────────────────────────────────────────

/// The settings table: `pk`, `sk`, the JSON in `data`, the revision in `rev`.
pub struct Dynamo {
    pub client: aws_sdk_dynamodb::Client,
    pub table: String,
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn rec_of(i: &HashMap<String, AttributeValue>) -> Option<Rec> {
    let sk = i.get("sk")?.as_s().ok()?.clone();
    let data = serde_json::from_str(i.get("data")?.as_s().ok()?).ok()?;
    let rev = i
        .get("rev")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    Some(Rec { sk, data, rev })
}

#[async_trait::async_trait]
impl Kv for Dynamo {
    async fn get(&self, pk: &str, sk: &str) -> Result<Option<Rec>, String> {
        let out = self
            .client
            .get_item()
            .table_name(&self.table)
            .key("pk", s(pk))
            .key("sk", s(sk))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e)))?;
        Ok(out.item().and_then(rec_of))
    }

    async fn put(&self, pk: &str, sk: &str, data: &Value, expect: Expect) -> Result<u64, KvError> {
        use aws_sdk_dynamodb::operation::put_item::PutItemError;
        let text = serde_json::to_string(data).map_err(|e| KvError::Other(e.to_string()))?;
        let next = match expect {
            Expect::Rev(r) => r + 1,
            _ => 1,
        };
        let mut req = self
            .client
            .put_item()
            .table_name(&self.table)
            .item("pk", s(pk))
            .item("sk", s(sk))
            .item("data", s(&text))
            .item("rev", AttributeValue::N(next.to_string()));
        match expect {
            Expect::Any => {}
            Expect::Absent => req = req.condition_expression("attribute_not_exists(sk)"),
            Expect::Rev(r) => {
                req = req
                    .condition_expression("rev = :r")
                    .expression_attribute_values(":r", AttributeValue::N(r.to_string()))
            }
        }
        match req.send().await {
            Ok(_) => Ok(next),
            Err(e) => {
                if matches!(
                    e.as_service_error(),
                    Some(PutItemError::ConditionalCheckFailedException(_))
                ) {
                    Err(KvError::Conflict)
                } else {
                    Err(KvError::Other(format!(
                        "{}",
                        aws_sdk_dynamodb::error::DisplayErrorContext(&e)
                    )))
                }
            }
        }
    }

    async fn query(
        &self,
        pk: &str,
        prefix: &str,
        reverse: bool,
        limit: Option<usize>,
        after: Option<&str>,
    ) -> Result<Vec<Rec>, String> {
        let mut out = Vec::new();
        let mut start =
            after.map(|a| HashMap::from([("pk".to_string(), s(pk)), ("sk".to_string(), s(a))]));
        loop {
            let mut q = self
                .client
                .query()
                .table_name(&self.table)
                .key_condition_expression("pk = :p AND begins_with(sk, :s)")
                .expression_attribute_values(":p", s(pk))
                .expression_attribute_values(":s", s(prefix))
                .scan_index_forward(!reverse)
                .set_exclusive_start_key(start.take());
            if let Some(n) = limit {
                q = q.limit((n - out.len().min(n)).max(1) as i32);
            }
            let r = q
                .send()
                .await
                .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e)))?;
            out.extend(r.items().iter().filter_map(rec_of));
            if limit.map(|n| out.len() >= n).unwrap_or(false) {
                out.truncate(limit.unwrap_or(usize::MAX));
                return Ok(out);
            }
            match r.last_evaluated_key() {
                Some(k) if !k.is_empty() => start = Some(k.clone()),
                _ => return Ok(out),
            }
        }
    }

    async fn delete(&self, pk: &str, sk: &str) -> Result<(), String> {
        self.client
            .delete_item()
            .table_name(&self.table)
            .key("pk", s(pk))
            .key("sk", s(sk))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn conditional_writes_and_queries() {
        let m = Memory::new();
        assert_eq!(m.put("t", "A#1", &json!(1), Expect::Absent).await, Ok(1));
        assert_eq!(
            m.put("t", "A#1", &json!(2), Expect::Absent).await,
            Err(KvError::Conflict)
        );
        assert_eq!(m.put("t", "A#1", &json!(2), Expect::Rev(1)).await, Ok(2));
        assert_eq!(
            m.put("t", "A#1", &json!(3), Expect::Rev(1)).await,
            Err(KvError::Conflict)
        );
        assert_eq!(
            m.put("t", "A#2", &json!(3), Expect::Rev(1)).await,
            Err(KvError::Conflict)
        );
        m.put("t", "A#2", &json!(4), Expect::Any).await.unwrap();
        m.put("t", "B#1", &json!(5), Expect::Any).await.unwrap();
        let a = m.query("t", "A#", false, None, None).await.unwrap();
        assert_eq!(
            a.iter().map(|r| r.sk.as_str()).collect::<Vec<_>>(),
            ["A#1", "A#2"]
        );
        let a = m.query("t", "A#", true, Some(1), None).await.unwrap();
        assert_eq!(a[0].sk, "A#2");
        let a = m.query("t", "A#", true, None, Some("A#2")).await.unwrap();
        assert_eq!(a[0].sk, "A#1");
        // Another partition sees nothing.
        assert!(m
            .query("u", "A#", false, None, None)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(m.get("u", "A#1").await.unwrap(), None);
        m.delete("t", "A#1").await.unwrap();
        assert_eq!(m.get("t", "A#1").await.unwrap(), None);
    }
}
