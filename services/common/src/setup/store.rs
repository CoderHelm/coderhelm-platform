//! Where the settings Setup reads and writes live, behind one trait so the
//! registry's reads and writes can be tested against memory with the same
//! code that runs against DynamoDB.
//!
//! Every method works on one team's partition of the settings table (and
//! the team's E2E settings in the E2E table); a store can never reach
//! another team.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use aws_sdk_dynamodb::types::AttributeValue;

use crate::e2e::model::Settings as E2eSettings;

pub type Item = HashMap<String, AttributeValue>;

#[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// The team every read and write is for.
    fn team(&self) -> &str;
    async fn get(&self, sk: &str) -> Result<Option<Item>, String>;
    /// Writes a whole item. Its `pk` must be the team.
    async fn put(&self, item: Item) -> Result<(), String>;
    async fn delete(&self, sk: &str) -> Result<(), String>;
    /// Adds the team to a settings index (`crate::settings_index`), as the
    /// settings routes do after a save.
    async fn index(&self, name: &str) -> Result<(), String>;
    /// The team's saved E2E settings, `None` when never saved.
    async fn e2e_settings(&self) -> Result<Option<E2eSettings>, String>;
    async fn put_e2e_settings(&self, s: &E2eSettings) -> Result<(), String>;
    /// Removes the saved E2E settings (undo of a row Setup created).
    async fn delete_e2e_settings(&self) -> Result<(), String>;
}

pub fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

/// A settings item that keeps one JSON document in `config`, as the stale
/// pull request and security fix settings are saved.
pub fn config_item(team_id: &str, sk: &str, config_json: &str, by: &str, now: &str) -> Item {
    HashMap::from([
        ("pk".to_string(), s(team_id)),
        ("sk".to_string(), s(sk)),
        ("config".to_string(), s(config_json)),
        ("updated_at".to_string(), s(now)),
        ("updated_by".to_string(), s(by)),
    ])
}

fn check_pk(team: &str, item: &Item) -> Result<(), String> {
    match item.get("pk").and_then(|v| v.as_s().ok()) {
        Some(pk) if pk == team => Ok(()),
        _ => Err("an item for another team".to_string()),
    }
}

// ── DynamoDB ────────────────────────────────────────────────────────────

pub struct DynamoStore {
    pub client: aws_sdk_dynamodb::Client,
    pub settings_table: String,
    /// Empty when the install has no E2E table.
    pub e2e_table: String,
    pub team: String,
}

#[async_trait::async_trait]
impl Store for DynamoStore {
    fn team(&self) -> &str {
        &self.team
    }

    async fn get(&self, sk: &str) -> Result<Option<Item>, String> {
        self.client
            .get_item()
            .table_name(&self.settings_table)
            .key("pk", s(&self.team))
            .key("sk", s(sk))
            .consistent_read(true)
            .send()
            .await
            .map(|o| o.item().cloned())
            .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e)))
    }

    async fn put(&self, item: Item) -> Result<(), String> {
        check_pk(&self.team, &item)?;
        self.client
            .put_item()
            .table_name(&self.settings_table)
            .set_item(Some(item))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e)))
    }

    async fn delete(&self, sk: &str) -> Result<(), String> {
        self.client
            .delete_item()
            .table_name(&self.settings_table)
            .key("pk", s(&self.team))
            .key("sk", s(sk))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e)))
    }

    async fn index(&self, name: &str) -> Result<(), String> {
        crate::settings_index::add(&self.client, &self.settings_table, name, &self.team).await
    }

    async fn e2e_settings(&self) -> Result<Option<E2eSettings>, String> {
        let st = crate::e2e::store::Store::new(&self.client, &self.e2e_table);
        if !st.enabled() {
            return Ok(None);
        }
        st.saved_settings(&self.team).await
    }

    async fn put_e2e_settings(&self, v: &E2eSettings) -> Result<(), String> {
        let st = crate::e2e::store::Store::new(&self.client, &self.e2e_table);
        if !st.enabled() {
            return Err("Tests aren't set up on this install.".to_string());
        }
        st.put_settings(&self.team, v).await
    }

    async fn delete_e2e_settings(&self) -> Result<(), String> {
        let st = crate::e2e::store::Store::new(&self.client, &self.e2e_table);
        if !st.enabled() {
            return Ok(());
        }
        st.delete_settings(&self.team).await
    }
}

// ── In memory (tests) ───────────────────────────────────────────────────

/// The same contract in memory.
#[derive(Default)]
pub struct MemoryStore {
    pub team: String,
    items: Mutex<BTreeMap<String, Item>>,
    e2e: Mutex<Option<E2eSettings>>,
    indexes: Mutex<Vec<String>>,
    /// Writes to sort keys starting with this fail (partial failure tests).
    pub fail_writes_to: Mutex<Option<String>>,
}

impl MemoryStore {
    pub fn new(team: &str) -> Self {
        Self {
            team: team.to_string(),
            ..Default::default()
        }
    }

    pub fn indexes(&self) -> Vec<String> {
        self.indexes.lock().unwrap().clone()
    }

    pub fn fail_writes(&self, prefix: Option<&str>) {
        *self.fail_writes_to.lock().unwrap() = prefix.map(str::to_string);
    }

    fn fails(&self, sk: &str) -> bool {
        self.fail_writes_to
            .lock()
            .unwrap()
            .as_deref()
            .is_some_and(|p| sk.starts_with(p))
    }
}

#[async_trait::async_trait]
impl Store for MemoryStore {
    fn team(&self) -> &str {
        &self.team
    }

    async fn get(&self, sk: &str) -> Result<Option<Item>, String> {
        Ok(self.items.lock().unwrap().get(sk).cloned())
    }

    async fn put(&self, item: Item) -> Result<(), String> {
        check_pk(&self.team, &item)?;
        let sk = item
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .ok_or("an item without a sort key")?;
        if self.fails(&sk) {
            return Err(format!("write to {sk} failed"));
        }
        self.items.lock().unwrap().insert(sk, item);
        Ok(())
    }

    async fn delete(&self, sk: &str) -> Result<(), String> {
        if self.fails(sk) {
            return Err(format!("delete of {sk} failed"));
        }
        self.items.lock().unwrap().remove(sk);
        Ok(())
    }

    async fn index(&self, name: &str) -> Result<(), String> {
        let mut i = self.indexes.lock().unwrap();
        if !i.iter().any(|n| n == name) {
            i.push(name.to_string());
        }
        Ok(())
    }

    async fn e2e_settings(&self) -> Result<Option<E2eSettings>, String> {
        Ok(self.e2e.lock().unwrap().clone())
    }

    async fn put_e2e_settings(&self, v: &E2eSettings) -> Result<(), String> {
        if self.fails(E2E_ROW) {
            return Err("write to the E2E settings failed".to_string());
        }
        *self.e2e.lock().unwrap() = Some(v.clone());
        Ok(())
    }

    async fn delete_e2e_settings(&self) -> Result<(), String> {
        *self.e2e.lock().unwrap() = None;
        Ok(())
    }
}

/// The name Setup gives the team's E2E settings row (it lives in the E2E
/// table, not the settings table).
pub const E2E_ROW: &str = "E2E#SETTINGS";

/// Reads each row once (a page that reads every setting of many repos).
/// Writes go straight through and drop what was cached.
pub struct Cached<'a> {
    inner: &'a dyn Store,
    items: Mutex<HashMap<String, Option<Item>>>,
    e2e: Mutex<Option<Option<E2eSettings>>>,
}

impl<'a> Cached<'a> {
    pub fn new(inner: &'a dyn Store) -> Self {
        Self {
            inner,
            items: Mutex::new(HashMap::new()),
            e2e: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl Store for Cached<'_> {
    fn team(&self) -> &str {
        self.inner.team()
    }

    async fn get(&self, sk: &str) -> Result<Option<Item>, String> {
        if let Some(hit) = self.items.lock().unwrap().get(sk) {
            return Ok(hit.clone());
        }
        let v = self.inner.get(sk).await?;
        self.items.lock().unwrap().insert(sk.to_string(), v.clone());
        Ok(v)
    }

    async fn put(&self, item: Item) -> Result<(), String> {
        self.items.lock().unwrap().clear();
        self.inner.put(item).await
    }

    async fn delete(&self, sk: &str) -> Result<(), String> {
        self.items.lock().unwrap().clear();
        self.inner.delete(sk).await
    }

    async fn index(&self, name: &str) -> Result<(), String> {
        self.inner.index(name).await
    }

    async fn e2e_settings(&self) -> Result<Option<E2eSettings>, String> {
        if let Some(hit) = self.e2e.lock().unwrap().clone() {
            return Ok(hit);
        }
        let v = self.inner.e2e_settings().await?;
        *self.e2e.lock().unwrap() = Some(v.clone());
        Ok(v)
    }

    async fn put_e2e_settings(&self, s: &E2eSettings) -> Result<(), String> {
        *self.e2e.lock().unwrap() = None;
        self.inner.put_e2e_settings(s).await
    }

    async fn delete_e2e_settings(&self) -> Result<(), String> {
        *self.e2e.lock().unwrap() = None;
        self.inner.delete_e2e_settings().await
    }
}
