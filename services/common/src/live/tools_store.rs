//! Where a team's tools, their versions, approvals, ask-me-first requests and
//! run counters are kept. All in the settings table, under the team.
//!
//! - `TOOL#{name}`: the tool, its switch, the approved and latest versions.
//! - `TOOLV#{name}#{hash}`: one exact version, who wrote it, from where, and
//!   the example run its approver saw.
//! - `TOOLASK#{id}`: one ask-me-first call waiting for (or given) a yes.
//! - `TOOLRATE#{name}#{hour}`: runs this hour.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::{json, Value};

use super::tools::{content_hash, ToolDef};

type Item = HashMap<String, AttributeValue>;

const TOOL: &str = "TOOL#";
const VERSION: &str = "TOOLV#";
const ASK: &str = "TOOLASK#";
const RATE: &str = "TOOLRATE#";
pub const MAX_TOOLS: usize = 50;

fn s(i: &Item, k: &str) -> String {
    i.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}
fn b(i: &Item, k: &str) -> bool {
    i.get(k)
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false)
}
fn n(i: &Item, k: &str) -> i64 {
    i.get(k)
        .and_then(|v| v.as_n().ok())
        .and_then(|x| x.parse().ok())
        .unwrap_or(0)
}
fn av(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}
fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// A tool and where it stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolRecord {
    pub name: String,
    pub enabled: bool,
    /// The version agents run. Empty until a person approves one.
    pub approved: String,
    pub approved_by: String,
    pub approved_at: String,
    /// The newest version written.
    pub latest: String,
    pub created_by: String,
    pub created_at: String,
}

impl ToolRecord {
    fn from_item(i: &Item) -> Option<ToolRecord> {
        Some(ToolRecord {
            name: s(i, "sk").strip_prefix(TOOL)?.to_string(),
            enabled: b(i, "enabled"),
            approved: s(i, "approved"),
            approved_by: s(i, "approved_by"),
            approved_at: s(i, "approved_at"),
            latest: s(i, "latest"),
            created_by: s(i, "created_by"),
            created_at: s(i, "created_at"),
        })
    }
    pub fn json(&self) -> Value {
        json!({
            "name": self.name, "enabled": self.enabled, "approved": self.approved,
            "approved_by": self.approved_by, "approved_at": self.approved_at, "latest": self.latest,
            "created_by": self.created_by, "created_at": self.created_at,
            "awaiting_approval": self.latest != self.approved,
        })
    }
}

/// One exact version of a tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionRecord {
    pub hash: String,
    pub def: ToolDef,
    pub author: String,
    /// `dashboard`, or `repo owner/name@sha path`.
    pub origin: String,
    pub created_at: String,
    /// What the example run returned, for the approver.
    pub example: String,
    pub example_ok: bool,
}

impl VersionRecord {
    fn from_item(i: &Item) -> Option<VersionRecord> {
        let def: ToolDef = serde_json::from_str(&s(i, "def")).ok()?;
        let hash = s(i, "hash");
        // A stored version whose content no longer matches its hash is never used.
        if content_hash(&def) != hash {
            return None;
        }
        Some(VersionRecord {
            hash,
            def,
            author: s(i, "author"),
            origin: s(i, "origin"),
            created_at: s(i, "created_at"),
            example: s(i, "example"),
            example_ok: b(i, "example_ok"),
        })
    }
    pub fn json(&self) -> Value {
        json!({
            "hash": self.hash, "def": self.def, "author": self.author, "origin": self.origin,
            "created_at": self.created_at, "example": self.example, "example_ok": self.example_ok,
        })
    }
}

/// The tools agents on `repo` may use: switched on, with an approved
/// version whose stored content still matches its hash, scoped to the repo.
/// A tool with no approved version, or only an edited (not yet re-approved)
/// latest version, never runs anything but what was approved.
pub fn pick_usable(
    tools: &[ToolRecord],
    versions: &HashMap<(String, String), VersionRecord>,
    repo: &str,
) -> Vec<(ToolRecord, ToolDef)> {
    tools
        .iter()
        .filter(|t| t.enabled && !t.approved.is_empty())
        .filter_map(|t| {
            let v = versions.get(&(t.name.clone(), t.approved.clone()))?;
            (v.hash == t.approved && content_hash(&v.def) == v.hash && v.def.usable_for(repo))
                .then(|| (t.clone(), v.def.clone()))
        })
        .collect()
}

/// An ask-me-first call to record.
#[derive(Debug, Clone, Copy)]
pub struct AskRequest<'a> {
    pub name: &'a str,
    pub hash: &'a str,
    pub inputs: &'a Value,
    pub actor: &'a str,
    pub repo: &'a str,
    pub pr: u64,
    pub why: &'a str,
}

pub struct ToolStore<'a> {
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub table: &'a str,
    pub team: &'a str,
}

impl ToolStore<'_> {
    async fn get(&self, sk: &str) -> Result<Option<Item>, String> {
        self.dynamo
            .get_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(sk))
            .consistent_read(true)
            .send()
            .await
            .map(|o| o.item().cloned())
            .map_err(|e| format!("tool store get: {e}"))
    }

    async fn query(&self, prefix: &str) -> Result<Vec<Item>, String> {
        let mut out = Vec::new();
        let mut start = None;
        loop {
            let r = self
                .dynamo
                .query()
                .table_name(self.table)
                .key_condition_expression("pk = :p AND begins_with(sk, :s)")
                .expression_attribute_values(":p", av(self.team))
                .expression_attribute_values(":s", av(prefix))
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(|e| format!("tool store query: {e}"))?;
            out.extend(r.items().iter().cloned());
            start = r.last_evaluated_key().cloned();
            if start.is_none() {
                return Ok(out);
            }
        }
    }

    pub async fn list(&self) -> Result<Vec<ToolRecord>, String> {
        let mut v: Vec<ToolRecord> = self
            .query(TOOL)
            .await?
            .iter()
            .filter_map(ToolRecord::from_item)
            .collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(v)
    }

    pub async fn tool(&self, name: &str) -> Result<Option<ToolRecord>, String> {
        Ok(self
            .get(&format!("{TOOL}{name}"))
            .await?
            .as_ref()
            .and_then(ToolRecord::from_item))
    }

    pub async fn version(&self, name: &str, hash: &str) -> Result<Option<VersionRecord>, String> {
        Ok(self
            .get(&format!("{VERSION}{name}#{hash}"))
            .await?
            .as_ref()
            .and_then(VersionRecord::from_item))
    }

    pub async fn versions(&self, name: &str) -> Result<Vec<VersionRecord>, String> {
        let mut v: Vec<VersionRecord> = self
            .query(&format!("{VERSION}{name}#"))
            .await?
            .iter()
            .filter_map(VersionRecord::from_item)
            .collect();
        v.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(v)
    }

    /// Saves a version (idempotent by hash) and points the tool's `latest` at
    /// it. Never changes which version is approved.
    pub async fn save_version(
        &self,
        def: &ToolDef,
        author: &str,
        origin: &str,
        example: &str,
        example_ok: bool,
    ) -> Result<(ToolRecord, VersionRecord), String> {
        let hash = content_hash(def);
        let at = now();
        let existing = self.version(&def.name, &hash).await?;
        let version = match existing {
            Some(v) => v,
            None => {
                self.dynamo
                    .put_item()
                    .table_name(self.table)
                    .item("pk", av(self.team))
                    .item("sk", av(&format!("{VERSION}{}#{hash}", def.name)))
                    .item("record_type", av("tool_version"))
                    .item("hash", av(&hash))
                    .item(
                        "def",
                        av(&serde_json::to_string(def).map_err(|e| e.to_string())?),
                    )
                    .item("author", av(author))
                    .item("origin", av(origin))
                    .item("created_at", av(&at))
                    .item("example", av(crate::truncate_str(example, 8_000)))
                    .item("example_ok", AttributeValue::Bool(example_ok))
                    .send()
                    .await
                    .map_err(|e| format!("save tool version: {e}"))?;
                VersionRecord {
                    hash: hash.clone(),
                    def: def.clone(),
                    author: author.into(),
                    origin: origin.into(),
                    created_at: at.clone(),
                    example: example.into(),
                    example_ok,
                }
            }
        };
        self.dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{TOOL}{}", def.name)))
            .update_expression(
                "SET latest = :h, record_type = :rt, created_by = if_not_exists(created_by, :by), \
                 created_at = if_not_exists(created_at, :at), enabled = if_not_exists(enabled, :off), \
                 approved = if_not_exists(approved, :empty)",
            )
            .expression_attribute_values(":h", av(&hash))
            .expression_attribute_values(":rt", av("tool"))
            .expression_attribute_values(":by", av(author))
            .expression_attribute_values(":at", av(&at))
            .expression_attribute_values(":off", AttributeValue::Bool(false))
            .expression_attribute_values(":empty", av(""))
            .send()
            .await
            .map_err(|e| format!("save tool: {e}"))?;
        let record = self.tool(&def.name).await?.ok_or("tool vanished")?;
        Ok((record, version))
    }

    /// Approves one exact version. The caller has already checked who may.
    pub async fn approve(&self, name: &str, hash: &str, by: &str) -> Result<(), String> {
        self.version(name, hash)
            .await?
            .ok_or("That version doesn't exist.")?;
        self.dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{TOOL}{name}")))
            .update_expression("SET approved = :h, approved_by = :by, approved_at = :at")
            .condition_expression("attribute_exists(pk)")
            .expression_attribute_values(":h", av(hash))
            .expression_attribute_values(":by", av(by))
            .expression_attribute_values(":at", av(&now()))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("approve tool: {e}"))
    }

    pub async fn set_enabled(&self, name: &str, enabled: bool) -> Result<(), String> {
        self.dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{TOOL}{name}")))
            .update_expression("SET enabled = :e")
            .condition_expression("attribute_exists(pk)")
            .expression_attribute_values(":e", AttributeValue::Bool(enabled))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("switch tool: {e}"))
    }

    pub async fn delete(&self, name: &str) -> Result<(), String> {
        for v in self.versions(name).await? {
            let _ = self
                .dynamo
                .delete_item()
                .table_name(self.table)
                .key("pk", av(self.team))
                .key("sk", av(&format!("{VERSION}{name}#{}", v.hash)))
                .send()
                .await;
        }
        self.dynamo
            .delete_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{TOOL}{name}")))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("delete tool: {e}"))
    }

    /// The tools agents on `repo` may use. See [`pick_usable`].
    pub async fn usable(&self, repo: &str) -> Result<Vec<(ToolRecord, ToolDef)>, String> {
        let tools = self.list().await?;
        let mut versions = HashMap::new();
        for t in tools.iter().filter(|t| t.enabled && !t.approved.is_empty()) {
            if let Some(v) = self.version(&t.name, &t.approved).await? {
                versions.insert((t.name.clone(), t.approved.clone()), v);
            }
        }
        Ok(pick_usable(&tools, &versions, repo))
    }

    /// Counts one run this hour and says whether it fits the limit.
    pub async fn take_run(&self, name: &str, limit: u32) -> Result<bool, String> {
        let hour = chrono::Utc::now().format("%Y%m%d%H").to_string();
        let ttl = chrono::Utc::now().timestamp() + 7200;
        let r = self
            .dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{RATE}{name}#{hour}")))
            .update_expression("ADD runs :one SET #t = :ttl")
            .condition_expression("attribute_not_exists(runs) OR runs < :limit")
            .expression_attribute_names("#t", "ttl")
            .expression_attribute_values(":one", AttributeValue::N("1".into()))
            .expression_attribute_values(":ttl", AttributeValue::N(ttl.to_string()))
            .expression_attribute_values(":limit", AttributeValue::N(limit.to_string()))
            .send()
            .await;
        match r {
            Ok(_) => Ok(true),
            Err(e)
                if e.as_service_error()
                    .is_some_and(|x| x.is_conditional_check_failed_exception()) =>
            {
                Ok(false)
            }
            Err(e) => Err(format!("count tool run: {e}")),
        }
    }

    /// Records an ask-me-first call waiting for a person. Returns its id.
    pub async fn create_ask(&self, a: &AskRequest<'_>) -> Result<String, String> {
        let AskRequest {
            name,
            hash,
            inputs,
            actor,
            repo,
            pr,
            why,
        } = *a;
        let id = ulid::Ulid::new().to_string();
        let now_ts = chrono::Utc::now().timestamp();
        self.dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", av(self.team))
            .item("sk", av(&format!("{ASK}{id}")))
            .item("record_type", av("tool_ask"))
            .item("tool", av(name))
            .item("hash", av(hash))
            .item("inputs", av(&inputs.to_string()))
            .item("actor", av(actor))
            .item("repo", av(repo))
            .item("pr", AttributeValue::N(pr.to_string()))
            .item("why", av(crate::truncate_str(why, 500)))
            .item("status", av("pending"))
            .item("created_ts", AttributeValue::N(now_ts.to_string()))
            .item("created_at", av(&now()))
            .item("ttl", AttributeValue::N((now_ts + 7 * 86400).to_string()))
            .send()
            .await
            .map_err(|e| format!("create ask: {e}"))?;
        Ok(id)
    }

    /// An ask's status and when it was made.
    pub async fn ask_status(&self, id: &str) -> Result<Option<(String, i64)>, String> {
        Ok(self
            .get(&format!("{ASK}{id}"))
            .await?
            .map(|i| (s(&i, "status"), n(&i, "created_ts"))))
    }

    /// A person's yes or no. Only a pending, unexpired ask can be decided.
    pub async fn decide(&self, id: &str, approve: bool, by: &str) -> Result<(), String> {
        let expired_before =
            chrono::Utc::now().timestamp() - super::tools::ASK_WAIT.as_secs() as i64;
        self.dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", av(self.team))
            .key("sk", av(&format!("{ASK}{id}")))
            .update_expression("SET #s = :s, decided_by = :by, decided_at = :at")
            .condition_expression("#s = :pending AND created_ts > :exp")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":s", av(if approve { "approved" } else { "denied" }))
            .expression_attribute_values(":pending", av("pending"))
            .expression_attribute_values(":exp", AttributeValue::N(expired_before.to_string()))
            .expression_attribute_values(":by", av(by))
            .expression_attribute_values(":at", av(&now()))
            .send()
            .await
            .map(|_| ())
            .map_err(|_| "That request was already decided or has expired.".to_string())
    }

    /// Asks from the last day, newest first.
    pub async fn asks(&self) -> Result<Vec<Value>, String> {
        let cutoff = chrono::Utc::now().timestamp() - 86400;
        let wait = super::tools::ASK_WAIT.as_secs() as i64;
        let now_ts = chrono::Utc::now().timestamp();
        let mut v: Vec<(i64, Value)> = self
            .query(ASK)
            .await?
            .iter()
            .filter(|i| n(i, "created_ts") > cutoff)
            .map(|i| {
                let created = n(i, "created_ts");
                let mut status = s(i, "status");
                if status == "pending" && now_ts - created >= wait {
                    status = "expired".into();
                }
                (created, json!({
                    "id": s(i, "sk").trim_start_matches(ASK), "tool": s(i, "tool"), "hash": s(i, "hash"),
                    "inputs": serde_json::from_str::<Value>(&s(i, "inputs")).unwrap_or(Value::Null),
                    "actor": s(i, "actor"), "repo": s(i, "repo"), "pr": n(i, "pr"), "why": s(i, "why"),
                    "status": status, "created_at": s(i, "created_at"), "decided_by": s(i, "decided_by"),
                    "expires_in_s": (wait - (now_ts - created)).max(0),
                }))
            })
            .collect();
        v.sort_by_key(|x| std::cmp::Reverse(x.0));
        Ok(v.into_iter().map(|x| x.1).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::tools::parse_def;

    fn def(read: &str) -> ToolDef {
        parse_def(&format!(
            "name: find_order\nuse_when: x\nsource: db\nkind: sql\nread: {read}\ninputs:\n  n: {{ type: integer, min: 1, max: 9, example: 1 }}\n"
        ))
        .unwrap()
    }
    fn rec(enabled: bool, approved: &str, latest: &str) -> ToolRecord {
        ToolRecord {
            name: "find_order".into(),
            enabled,
            approved: approved.into(),
            approved_by: "b@example.com".into(),
            approved_at: String::new(),
            latest: latest.into(),
            created_by: "a@example.com".into(),
            created_at: String::new(),
        }
    }
    fn ver(d: &ToolDef) -> VersionRecord {
        VersionRecord {
            hash: content_hash(d),
            def: d.clone(),
            author: "a@example.com".into(),
            origin: "dashboard".into(),
            created_at: String::new(),
            example: String::new(),
            example_ok: true,
        }
    }

    #[test]
    fn only_approved_switched_on_versions_are_listed() {
        let v1 = def("SELECT id FROM orders WHERE id = :n");
        let v2 = def("SELECT id, total FROM orders WHERE id = :n");
        let (h1, h2) = (content_hash(&v1), content_hash(&v2));
        let versions = HashMap::from([
            (("find_order".to_string(), h1.clone()), ver(&v1)),
            (("find_order".to_string(), h2.clone()), ver(&v2)),
        ]);
        // never approved
        assert!(pick_usable(&[rec(true, "", &h1)], &versions, "acme/web").is_empty());
        // switched off
        assert!(pick_usable(&[rec(false, &h1, &h1)], &versions, "acme/web").is_empty());
        // edited after approval: the approved version runs, not the edit
        let got = pick_usable(&[rec(true, &h1, &h2)], &versions, "acme/web");
        assert_eq!(got[0].1, v1);
        // approved version missing → nothing
        assert!(
            pick_usable(&[rec(true, "deadbeefdeadbeef", &h1)], &versions, "acme/web").is_empty()
        );
    }

    #[test]
    fn a_tampered_stored_version_is_never_used() {
        let v = def("SELECT id FROM orders WHERE id = :n");
        let mut tampered = ver(&v);
        tampered.def.read = "SELECT id, secret FROM orders WHERE id = :n".into();
        let versions = HashMap::from([(("find_order".to_string(), content_hash(&v)), tampered)]);
        assert!(pick_usable(
            &[rec(true, &content_hash(&v), &content_hash(&v))],
            &versions,
            "acme/web"
        )
        .is_empty());
        // and from_item drops a mismatched hash
        let mut item: Item = HashMap::new();
        item.insert("hash".into(), av(&content_hash(&v)));
        item.insert(
            "def".into(),
            av(&serde_json::to_string(&def("SELECT 2 WHERE 1 = :n")).unwrap()),
        );
        assert!(VersionRecord::from_item(&item).is_none());
    }

    #[test]
    fn repo_scope_applies() {
        let mut v = def("SELECT id FROM orders WHERE id = :n");
        v.repos = vec!["acme/web".into()];
        let h = content_hash(&v);
        let versions = HashMap::from([(("find_order".to_string(), h.clone()), ver(&v))]);
        assert_eq!(
            pick_usable(&[rec(true, &h, &h)], &versions, "acme/web").len(),
            1
        );
        assert!(pick_usable(&[rec(true, &h, &h)], &versions, "acme/api").is_empty());
    }
}
