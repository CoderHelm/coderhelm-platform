//! Where connected sources and the log of every lookup are kept.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::{json, Value};

use super::api::AuthStyle;
use super::aws::{new_external_id, AwsTarget};
use super::redact::DEFAULT_HIDDEN;

const SOURCE_PREFIX: &str = "DATASOURCE#";
const LOG_PREFIX: &str = "DSLOG#";
const EXTERNAL_ID_SK: &str = "DSAWS#external-id";
/// Lookups stay in the activity log this long.
const LOG_DAYS: i64 = 180;
pub const MAX_SOURCES: usize = 20;

type Item = HashMap<String, AttributeValue>;

/// What a source is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A GraphQL API, read with a key.
    Api,
    /// Log groups in the team's AWS account.
    Logs,
    /// A PostgreSQL database in the team's AWS account.
    Database,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Api => "api",
            Kind::Logs => "logs",
            Kind::Database => "database",
        }
    }
    pub fn parse(s: &str) -> Kind {
        match s {
            "logs" => Kind::Logs,
            "database" => Kind::Database,
            _ => Kind::Api,
        }
    }
}

/// A connected source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub id: String,
    pub kind: Kind,
    pub name: String,
    pub url: String,
    pub auth: AuthStyle,
    /// The key, sealed for this team. Never returned to a browser.
    pub key_sealed: String,
    pub key_hint: String,
    /// The reviewer may use it. Off until a person turns it on.
    pub enabled: bool,
    pub setup_complete: bool,
    pub status: String,
    pub status_message: String,
    pub has_mutations: bool,
    pub kinds: Option<usize>,
    pub schema_available: bool,
    pub samples: Vec<(String, String)>,
    pub hidden_fields: Vec<String>,
    /// `owner/name` repos whose reviews may use it. Empty means all.
    pub repos: Vec<String>,
    pub last_checked_at: String,
    pub created_at: String,
    pub created_by: String,
    /// Where in the team's AWS account a logs or database source reads.
    pub aws: Option<AwsTarget>,
}

fn s(item: &Item, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}
fn b(item: &Item, k: &str) -> bool {
    item.get(k)
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false)
}
fn list(item: &Item, k: &str) -> Vec<String> {
    item.get(k)
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|x| x.as_s().ok().cloned()).collect())
        .unwrap_or_default()
}
fn av_s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}
fn av_list(v: &[String]) -> AttributeValue {
    AttributeValue::L(v.iter().map(|x| av_s(x)).collect())
}

pub fn default_hidden_fields() -> Vec<String> {
    DEFAULT_HIDDEN.iter().map(|x| x.to_string()).collect()
}

impl Source {
    pub fn schema_s3_key(team_id: &str, id: &str) -> String {
        format!("config/data-sources/{team_id}/{id}/schema.txt")
    }

    pub fn to_item(&self, team_id: &str) -> Item {
        let samples = serde_json::to_string(&self.samples).unwrap_or_else(|_| "[]".to_string());
        let mut item: Item = HashMap::from([
            ("pk".to_string(), av_s(team_id)),
            (
                "sk".to_string(),
                av_s(&format!("{SOURCE_PREFIX}{}", self.id)),
            ),
            ("record_type".to_string(), av_s("data_source")),
            ("kind".to_string(), av_s(self.kind.as_str())),
            ("name".to_string(), av_s(&self.name)),
            ("url".to_string(), av_s(&self.url)),
            ("auth".to_string(), av_s(self.auth.as_str())),
            ("key_sealed".to_string(), av_s(&self.key_sealed)),
            ("key_hint".to_string(), av_s(&self.key_hint)),
            ("enabled".to_string(), AttributeValue::Bool(self.enabled)),
            (
                "setup_complete".to_string(),
                AttributeValue::Bool(self.setup_complete),
            ),
            ("status".to_string(), av_s(&self.status)),
            ("status_message".to_string(), av_s(&self.status_message)),
            (
                "has_mutations".to_string(),
                AttributeValue::Bool(self.has_mutations),
            ),
            (
                "schema_available".to_string(),
                AttributeValue::Bool(self.schema_available),
            ),
            ("samples".to_string(), av_s(&samples)),
            ("hidden_fields".to_string(), av_list(&self.hidden_fields)),
            ("repos".to_string(), av_list(&self.repos)),
            ("last_checked_at".to_string(), av_s(&self.last_checked_at)),
            ("created_at".to_string(), av_s(&self.created_at)),
            ("created_by".to_string(), av_s(&self.created_by)),
        ]);
        if let Some(k) = self.kinds {
            item.insert("kinds".to_string(), AttributeValue::N(k.to_string()));
        }
        if let Some(a) = &self.aws {
            item.insert(
                "aws".to_string(),
                av_s(&serde_json::to_string(a).unwrap_or_default()),
            );
        }
        item
    }

    pub fn from_item(item: &Item) -> Option<Source> {
        let id = s(item, "sk").strip_prefix(SOURCE_PREFIX)?.to_string();
        Some(Source {
            id,
            kind: Kind::parse(&s(item, "kind")),
            name: s(item, "name"),
            url: s(item, "url"),
            auth: AuthStyle::parse(&s(item, "auth")),
            key_sealed: s(item, "key_sealed"),
            key_hint: s(item, "key_hint"),
            enabled: b(item, "enabled"),
            setup_complete: b(item, "setup_complete"),
            status: s(item, "status"),
            status_message: s(item, "status_message"),
            has_mutations: b(item, "has_mutations"),
            kinds: item
                .get("kinds")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok()),
            schema_available: b(item, "schema_available"),
            samples: serde_json::from_str(&s(item, "samples")).unwrap_or_default(),
            hidden_fields: list(item, "hidden_fields"),
            repos: list(item, "repos"),
            last_checked_at: s(item, "last_checked_at"),
            created_at: s(item, "created_at"),
            created_by: s(item, "created_by"),
            aws: serde_json::from_str(&s(item, "aws")).ok(),
        })
    }

    /// What a browser may see. The sealed key is never part of it.
    pub fn public_json(&self) -> Value {
        let aws = self.aws.as_ref().map(|a| {
            json!({
                "account": a.account(),
                "region": a.region,
                "role_arn": a.role_arn,
                "log_groups": a.log_groups,
                "cluster": a.cluster_arn.rsplit(':').next().unwrap_or(""),
                "database": a.database,
            })
        });
        json!({
            "id": self.id,
            "kind": self.kind.as_str(),
            "aws": aws,
            "name": self.name,
            "url": self.url,
            "status": self.status,
            "status_message": self.status_message,
            "enabled": self.enabled,
            "setup_complete": self.setup_complete,
            "key_hint": self.key_hint,
            "has_mutations": self.has_mutations,
            "kinds": self.kinds,
            "schema_available": self.schema_available,
            "hidden_fields": self.hidden_fields,
            "repos": self.repos,
            "last_checked_at": self.last_checked_at,
            "created_at": self.created_at,
            "created_by": self.created_by,
        })
    }

    /// Whether a review of `repo` (`owner/name`) may use this source.
    pub fn usable_for(&self, repo: &str) -> bool {
        self.enabled
            && self.status == "ready"
            && (self.repos.is_empty() || self.repos.iter().any(|r| r.eq_ignore_ascii_case(repo)))
    }
}

pub async fn list_sources(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
) -> Result<Vec<Source>, String> {
    let out = dynamo
        .query()
        .table_name(table)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", av_s(team_id))
        .expression_attribute_values(":p", av_s(SOURCE_PREFIX))
        .send()
        .await
        .map_err(|e| format!("list data sources: {e}"))?;
    Ok(out.items().iter().filter_map(Source::from_item).collect())
}

pub async fn get_source(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    id: &str,
) -> Result<Option<Source>, String> {
    let out = dynamo
        .get_item()
        .table_name(table)
        .key("pk", av_s(team_id))
        .key("sk", av_s(&format!("{SOURCE_PREFIX}{id}")))
        .send()
        .await
        .map_err(|e| format!("get data source: {e}"))?;
    Ok(out.item().and_then(Source::from_item))
}

pub async fn put_source(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    source: &Source,
) -> Result<(), String> {
    dynamo
        .put_item()
        .table_name(table)
        .set_item(Some(source.to_item(team_id)))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("save data source: {e}"))
}

pub async fn delete_source(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    id: &str,
) -> Result<(), String> {
    dynamo
        .delete_item()
        .table_name(table)
        .key("pk", av_s(team_id))
        .key("sk", av_s(&format!("{SOURCE_PREFIX}{id}")))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("delete data source: {e}"))
}

/// The team's private ID for AWS roles, made on first use and never changed.
/// Only Coderhelm knows which team an ID belongs to, so a role that requires it
/// can only be used on that team's behalf.
pub async fn external_id(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
) -> Result<String, String> {
    let get = || async {
        dynamo
            .get_item()
            .table_name(table)
            .key("pk", av_s(team_id))
            .key("sk", av_s(EXTERNAL_ID_SK))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| format!("get external id: {e}"))
            .map(|o| {
                o.item()
                    .map(|i| s(i, "external_id"))
                    .filter(|x| !x.is_empty())
            })
    };
    if let Some(id) = get().await? {
        return Ok(id);
    }
    // Two first visits at once must end with one ID: only the first write lands.
    let _ = dynamo
        .put_item()
        .table_name(table)
        .item("pk", av_s(team_id))
        .item("sk", av_s(EXTERNAL_ID_SK))
        .item("record_type", av_s("data_source_external_id"))
        .item("external_id", av_s(&new_external_id()))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await;
    get()
        .await?
        .ok_or_else(|| "external id was not saved".to_string())
}

/// One lookup, as recorded. Written for every attempt, including refused ones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Activity {
    pub source_id: String,
    pub source_name: String,
    /// A person's email, or `reviewer`.
    pub actor: String,
    pub repo: String,
    pub pr: u64,
    /// The lookup text, shortened.
    pub query: String,
    /// `ok` | `refused` | `failed`
    pub outcome: &'static str,
    pub detail: String,
    pub ms: u128,
}

impl Activity {
    fn summary(&self) -> String {
        let q: String = self.query.split_whitespace().collect::<Vec<_>>().join(" ");
        crate::truncate_str(&q, 160).to_string()
    }
}

/// Records a lookup. A failure to record is returned so callers can decide;
/// the reviewer withholds the result of a lookup it could not record.
pub async fn log_activity(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    a: &Activity,
) -> Result<(), String> {
    let now = chrono::Utc::now();
    let at = now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let ttl = (now + chrono::Duration::days(LOG_DAYS)).timestamp();
    let mut req = dynamo
        .put_item()
        .table_name(table)
        .item("pk", av_s(team_id))
        .item(
            "sk",
            av_s(&format!("{LOG_PREFIX}{at}#{}", ulid::Ulid::new())),
        )
        .item("record_type", av_s("data_source_lookup"))
        .item("at", av_s(&at))
        .item("source_id", av_s(&a.source_id))
        .item("source_name", av_s(&a.source_name))
        .item("actor", av_s(&a.actor))
        .item("query", av_s(crate::truncate_str(&a.query, 2000)))
        .item("summary", av_s(&a.summary()))
        .item("outcome", av_s(a.outcome))
        .item("detail", av_s(crate::truncate_str(&a.detail, 500)))
        .item("ms", AttributeValue::N(a.ms.to_string()))
        .item("ttl", AttributeValue::N(ttl.to_string()));
    if !a.repo.is_empty() {
        req = req
            .item("repo", av_s(&a.repo))
            .item("pr", AttributeValue::N(a.pr.to_string()));
    }
    req.send()
        .await
        .map(|_| ())
        .map_err(|e| format!("log lookup: {e}"))
}

/// The most recent lookups, newest first. `before` is the `cursor` of the
/// oldest one already shown, to page further back.
pub async fn list_activity(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    limit: i32,
    before: Option<&str>,
) -> Result<Vec<Value>, String> {
    let mut q = dynamo
        .query()
        .table_name(table)
        .expression_attribute_values(":pk", av_s(team_id))
        .scan_index_forward(false);
    q = q
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":p", av_s(LOG_PREFIX));
    if let Some(b) = before.filter(|b| b.starts_with(LOG_PREFIX)) {
        // Start just after the oldest lookup already shown.
        q = q
            .exclusive_start_key("pk", av_s(team_id))
            .exclusive_start_key("sk", av_s(b));
    }
    let out = q
        .limit(limit.clamp(1, 200))
        .send()
        .await
        .map_err(|e| format!("list lookups: {e}"))?;
    Ok(out
        .items()
        .iter()
        .map(|i| {
            let outcome = s(i, "outcome");
            let mut ev = json!({
                "at": s(i, "at"),
                "source_id": s(i, "source_id"),
                "source_name": s(i, "source_name"),
                "actor": s(i, "actor"),
                "summary": s(i, "summary"),
                "query": s(i, "query"),
                "cursor": s(i, "sk"),
                "ok": outcome == "ok",
                "outcome": outcome,
                "detail": s(i, "detail"),
                "ms": i.get("ms").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0),
            });
            let repo = s(i, "repo");
            if !repo.is_empty() {
                ev["repo"] = json!(repo);
                ev["pr"] = json!(i.get("pr").and_then(|v| v.as_n().ok()).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0));
            }
            ev
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> Source {
        Source {
            id: "01ABC".into(),
            kind: Kind::Api,
            name: "api.example.com".into(),
            url: "https://api.example.com/graphql".into(),
            auth: AuthStyle::Bearer,
            key_sealed: "c2VhbGVk".into(),
            key_hint: "9x2k".into(),
            enabled: true,
            setup_complete: true,
            status: "ready".into(),
            status_message: String::new(),
            has_mutations: true,
            kinds: Some(14),
            schema_available: true,
            samples: vec![("Look up orders".into(), "{ orders { id } }".into())],
            hidden_fields: default_hidden_fields(),
            repos: vec!["acme/web".into()],
            last_checked_at: "2026-01-01T00:00:00Z".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
            created_by: "a@example.com".into(),
            aws: None,
        }
    }

    #[test]
    fn item_round_trip() {
        let src = source();
        let item = src.to_item("TEAM#1");
        assert_eq!(s(&item, "sk"), "DATASOURCE#01ABC");
        assert_eq!(Source::from_item(&item).unwrap(), src);
    }

    #[test]
    fn an_aws_source_round_trips() {
        let mut src = source();
        src.kind = Kind::Database;
        src.key_sealed = String::new();
        src.aws = Some(AwsTarget {
            role_arn: "arn:aws:iam::123456789012:role/CoderhelmReadOnly".into(),
            region: "us-east-1".into(),
            cluster_arn: "arn:aws:rds:us-east-1:123456789012:cluster:app-db".into(),
            secret_arn:
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:coderhelm-readonly-app-db-x"
                    .into(),
            database: "app".into(),
            ..Default::default()
        });
        let item = src.to_item("TEAM#1");
        assert_eq!(s(&item, "kind"), "database");
        assert_eq!(Source::from_item(&item).unwrap(), src);
        let v = src.public_json();
        assert_eq!(v["kind"], "database");
        assert_eq!(v["aws"]["cluster"], "app-db");
        assert_eq!(v["aws"]["account"], "123456789012");
        // An item saved before kinds existed reads as an API.
        let mut old = source().to_item("TEAM#1");
        old.remove("kind");
        assert_eq!(Source::from_item(&old).unwrap().kind, Kind::Api);
    }

    #[test]
    fn the_sealed_key_never_reaches_a_browser() {
        let v = source().public_json();
        let text = v.to_string();
        assert!(!text.contains("c2VhbGVk"));
        assert!(v.get("key_sealed").is_none());
        assert_eq!(v["key_hint"], "9x2k");
    }

    #[test]
    fn usable_only_when_on_ready_and_in_scope() {
        let mut src = source();
        assert!(src.usable_for("acme/web"));
        assert!(src.usable_for("Acme/Web"));
        assert!(!src.usable_for("acme/api"));
        src.repos.clear();
        assert!(src.usable_for("acme/api"));
        src.enabled = false;
        assert!(!src.usable_for("acme/api"));
        src.enabled = true;
        src.status = "error".into();
        assert!(!src.usable_for("acme/api"));
    }
}
