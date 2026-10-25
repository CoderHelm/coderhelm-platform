//! Where sources, synced items and their indexes are kept: the settings
//! table and the data bucket, behind a trait so the sync engine is tested
//! in memory.

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;

use super::super::search::{decode_index, encode_index, Index};
use super::super::{self as kb, FileMeta};
use super::{registry_sk, source_prefix, source_record, source_sk, Source, REGISTRY_PK};

#[async_trait::async_trait]
pub trait SyncStore: Send + Sync {
    async fn source(&self, team: &str, kb_id: &str, id: &str) -> Result<Option<Source>, String>;
    /// Saves a source as is (its hold included).
    async fn put_source(&self, team: &str, s: &Source) -> Result<(), String>;
    /// Takes the source's hold until `until` when nobody holds it at `now`
    /// (a crashed sync's hold has run out). False when another sync has it.
    async fn take_lease(
        &self,
        team: &str,
        s: &Source,
        now: i64,
        until: i64,
    ) -> Result<bool, String>;
    /// The source's synced items.
    async fn items(
        &self,
        team: &str,
        kb_id: &str,
        source_id: &str,
    ) -> Result<Vec<FileMeta>, String>;
    async fn put_item(&self, team: &str, f: &FileMeta) -> Result<(), String>;
    /// The item's record and every stored version of it.
    async fn delete_item(&self, team: &str, f: &FileMeta) -> Result<(), String>;
    async fn read_index(&self, team: &str, key: &str) -> Result<Option<Arc<Index>>, String>;
    async fn write_index(&self, team: &str, key: &str, index: &Index) -> Result<(), String>;
    async fn delete_object(&self, team: &str, key: &str);
    /// Records when the source is next due, for the sweep.
    async fn put_registry(&self, team: &str, s: &Source) -> Result<(), String>;
}

/// The settings table and the data bucket.
pub struct AwsSyncStore {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub s3: aws_sdk_s3::Client,
    pub table: String,
    pub bucket: String,
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn n(v: i64) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

/// A source's record with its hold as its own attribute, so a write can be
/// made conditional on it.
pub fn record_with_lease(team: &str, src: &Source) -> HashMap<String, AttributeValue> {
    let mut r = source_record(team, src);
    r.insert("lease".into(), n(src.lease_until));
    r
}

/// Every source of one base.
pub async fn base_sources(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team: &str,
    kb_id: &str,
) -> Result<Vec<Source>, String> {
    let mut out = Vec::new();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    loop {
        let r = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", s(team))
            .expression_attribute_values(":p", s(&source_prefix(kb_id)))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| format!("sources: {e}"))?;
        out.extend(r.items().iter().filter_map(kb::read_data::<Source>));
        match r.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => return Ok(out),
        }
    }
}

/// One registered source as the sweep and webhooks see it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RegistryRow {
    pub sk: String,
    pub next: i64,
    pub paused: bool,
    pub kind: String,
    pub account: String,
}

/// The sweep's view: every registered source with when it is due and
/// whether it is paused.
pub async fn registry(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
) -> Result<Vec<RegistryRow>, String> {
    let mut out = Vec::new();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    loop {
        let r = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", s(REGISTRY_PK))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| format!("registry: {e}"))?;
        for item in r.items() {
            let sk = item
                .get("sk")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default();
            let next = item
                .get("next")
                .and_then(|v| v.as_n().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let paused = item
                .get("paused")
                .and_then(|v| v.as_bool().ok())
                .copied()
                .unwrap_or(false);
            let text = |k: &str| {
                item.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            out.push(RegistryRow {
                sk,
                next,
                paused,
                kind: text("kind"),
                account: text("account"),
            });
        }
        match r.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => return Ok(out),
        }
    }
}

/// Removes a source from the registry.
pub async fn delete_registry(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team: &str,
    kb_id: &str,
    id: &str,
) -> Result<(), String> {
    dynamo
        .delete_item()
        .table_name(table)
        .key("pk", s(REGISTRY_PK))
        .key("sk", s(&registry_sk(team, kb_id, id)))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("registry delete: {e}"))
}

#[async_trait::async_trait]
impl SyncStore for AwsSyncStore {
    async fn source(&self, team: &str, kb_id: &str, id: &str) -> Result<Option<Source>, String> {
        if !kb::valid_id(kb_id) || !kb::valid_id(id) {
            return Ok(None);
        }
        let r = self
            .dynamo
            .get_item()
            .table_name(&self.table)
            .key("pk", s(team))
            .key("sk", s(&source_sk(kb_id, id)))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| format!("source: {e}"))?;
        Ok(r.item().and_then(kb::read_data::<Source>))
    }

    async fn put_source(&self, team: &str, src: &Source) -> Result<(), String> {
        self.dynamo
            .put_item()
            .table_name(&self.table)
            .set_item(Some(record_with_lease(team, src)))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("source save: {e}"))
    }

    async fn take_lease(
        &self,
        team: &str,
        src: &Source,
        now: i64,
        until: i64,
    ) -> Result<bool, String> {
        let mut held = src.clone();
        held.lease_until = until;
        let r = self
            .dynamo
            .put_item()
            .table_name(&self.table)
            .set_item(Some(record_with_lease(team, &held)))
            .condition_expression(
                "attribute_exists(pk) AND (attribute_not_exists(lease) OR lease < :now)",
            )
            .expression_attribute_values(":now", n(now))
            .send()
            .await;
        match r {
            Ok(_) => Ok(true),
            Err(e) => {
                let text = format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e));
                if text.contains("ConditionalCheckFailed") {
                    Ok(false)
                } else {
                    Err(format!("source hold: {text}"))
                }
            }
        }
    }

    async fn items(
        &self,
        team: &str,
        kb_id: &str,
        source_id: &str,
    ) -> Result<Vec<FileMeta>, String> {
        let all: Vec<FileMeta> = super::super::search::Store::files(
            &kb::search::AwsStore {
                dynamo: self.dynamo.clone(),
                s3: self.s3.clone(),
                table: self.table.clone(),
                bucket: self.bucket.clone(),
            },
            team,
            kb_id,
        )
        .await?;
        Ok(all
            .into_iter()
            .filter(|f| f.source_id == source_id)
            .collect())
    }

    async fn put_item(&self, team: &str, f: &FileMeta) -> Result<(), String> {
        self.dynamo
            .put_item()
            .table_name(&self.table)
            .set_item(Some(kb::file_record(team, f)))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("item save: {e}"))
    }

    async fn delete_item(&self, team: &str, f: &FileMeta) -> Result<(), String> {
        let folder = kb::file_folder(team, &f.kb_id, &f.id);
        if kb::owns_key(team, &folder) {
            if let Ok(list) = self
                .s3
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&folder)
                .send()
                .await
            {
                for o in list.contents() {
                    if let Some(k) = o.key() {
                        let _ = self
                            .s3
                            .delete_object()
                            .bucket(&self.bucket)
                            .key(k)
                            .send()
                            .await;
                    }
                }
            }
        }
        self.dynamo
            .delete_item()
            .table_name(&self.table)
            .key("pk", s(team))
            .key("sk", s(&kb::file_sk(&f.kb_id, &f.id)))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("item delete: {e}"))
    }

    async fn read_index(&self, team: &str, key: &str) -> Result<Option<Arc<Index>>, String> {
        if !kb::owns_key(team, key) {
            return Err("that index isn't this team's".into());
        }
        let obj = match self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(o) => o,
            Err(_) => return Ok(None),
        };
        let bytes = obj
            .body
            .collect()
            .await
            .map_err(|e| format!("index read: {e}"))?
            .into_bytes();
        Ok(decode_index(&bytes).ok().map(Arc::new))
    }

    async fn write_index(&self, team: &str, key: &str, index: &Index) -> Result<(), String> {
        if !kb::owns_key(team, key) {
            return Err("that index isn't this team's".into());
        }
        self.s3
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type("application/json")
            .body(encode_index(index).into())
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("index save: {e}"))
    }

    async fn delete_object(&self, team: &str, key: &str) {
        if kb::owns_key(team, key) {
            let _ = self
                .s3
                .delete_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await;
        }
    }

    async fn put_registry(&self, team: &str, src: &Source) -> Result<(), String> {
        self.dynamo
            .put_item()
            .table_name(&self.table)
            .item("pk", s(REGISTRY_PK))
            .item("sk", s(&registry_sk(team, &src.kb_id, &src.id)))
            .item("next", n(src.next_sync_at))
            .item("paused", AttributeValue::Bool(src.paused))
            .item("kind", s(src.kind_key()))
            .item("account", s(&src.account))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("registry save: {e}"))
    }
}

/// An in-memory store for tests here and in the services.
pub mod memory {
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    pub struct MemSyncStore {
        pub sources: Mutex<HashMap<String, Source>>,
        pub items: Mutex<HashMap<String, FileMeta>>,
        pub objects: Mutex<HashMap<String, Arc<Index>>>,
        pub registry: Mutex<HashMap<String, (i64, bool)>>,
        pub source_writes: Mutex<Vec<Source>>,
    }

    #[async_trait::async_trait]
    impl SyncStore for MemSyncStore {
        async fn source(&self, _team: &str, _kb: &str, id: &str) -> Result<Option<Source>, String> {
            Ok(self.sources.lock().unwrap().get(id).cloned())
        }
        async fn put_source(&self, _team: &str, s: &Source) -> Result<(), String> {
            self.source_writes.lock().unwrap().push(s.clone());
            self.sources.lock().unwrap().insert(s.id.clone(), s.clone());
            Ok(())
        }
        async fn take_lease(
            &self,
            _team: &str,
            s: &Source,
            now: i64,
            until: i64,
        ) -> Result<bool, String> {
            let mut all = self.sources.lock().unwrap();
            let cur = all.get(&s.id).map(|c| c.lease_until).unwrap_or(0);
            if cur >= now {
                return Ok(false);
            }
            let mut held = s.clone();
            held.lease_until = until;
            all.insert(s.id.clone(), held);
            Ok(true)
        }
        async fn items(
            &self,
            _team: &str,
            kb_id: &str,
            source_id: &str,
        ) -> Result<Vec<FileMeta>, String> {
            let mut v: Vec<FileMeta> = self
                .items
                .lock()
                .unwrap()
                .values()
                .filter(|f| f.kb_id == kb_id && f.source_id == source_id)
                .cloned()
                .collect();
            v.sort_by(|a, b| a.external_id.cmp(&b.external_id));
            Ok(v)
        }
        async fn put_item(&self, _team: &str, f: &FileMeta) -> Result<(), String> {
            self.items.lock().unwrap().insert(f.id.clone(), f.clone());
            Ok(())
        }
        async fn delete_item(&self, team: &str, f: &FileMeta) -> Result<(), String> {
            self.items.lock().unwrap().remove(&f.id);
            let folder = kb::file_folder(team, &f.kb_id, &f.id);
            self.objects
                .lock()
                .unwrap()
                .retain(|k, _| !k.starts_with(&folder));
            Ok(())
        }
        async fn read_index(&self, _team: &str, key: &str) -> Result<Option<Arc<Index>>, String> {
            Ok(self.objects.lock().unwrap().get(key).cloned())
        }
        async fn write_index(&self, team: &str, key: &str, index: &Index) -> Result<(), String> {
            assert!(kb::owns_key(team, key));
            self.objects
                .lock()
                .unwrap()
                .insert(key.to_string(), Arc::new(index.clone()));
            Ok(())
        }
        async fn delete_object(&self, _team: &str, key: &str) {
            self.objects.lock().unwrap().remove(key);
        }
        async fn put_registry(&self, _team: &str, s: &Source) -> Result<(), String> {
            self.registry
                .lock()
                .unwrap()
                .insert(s.id.clone(), (s.next_sync_at, s.paused));
            Ok(())
        }
    }
}
