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
    /// A REST API, read with GET requests under the paths the team allows.
    Rest,
    /// An OpenSearch (or Elasticsearch) cluster, searched read only.
    Search,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Api => "api",
            Kind::Logs => "logs",
            Kind::Database => "database",
            Kind::Rest => "rest",
            Kind::Search => "search",
        }
    }
    pub fn parse(s: &str) -> Kind {
        match s {
            "logs" => Kind::Logs,
            "database" => Kind::Database,
            "rest" => Kind::Rest,
            "search" => Kind::Search,
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
    /// REST: the header the key is sent in (e.g. `X-Api-Key`).
    pub header_name: String,
    /// REST: path prefixes agents may GET. Nothing outside them is read.
    pub paths: Vec<String>,
    /// Search: the cluster user the key (its password) belongs to.
    pub user: String,
    /// Search: index names or patterns agents may read. Empty: every index
    /// the user sees.
    pub indexes: Vec<String>,
    /// What agents should know when using it (what the system is, what its
    /// fields mean, where else its data lives). Given to them with it.
    pub instructions: String,
}

/// Characters of a source's guide for agents.
pub const MAX_INSTRUCTIONS: usize = 4000;

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
            ("header_name".to_string(), av_s(&self.header_name)),
            ("paths".to_string(), av_list(&self.paths)),
            ("user".to_string(), av_s(&self.user)),
            ("indexes".to_string(), av_list(&self.indexes)),
            ("instructions".to_string(), av_s(&self.instructions)),
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
            header_name: s(item, "header_name"),
            paths: list(item, "paths"),
            user: s(item, "user"),
            indexes: list(item, "indexes"),
            instructions: s(item, "instructions"),
        })
    }

    /// A REST or Search source as the client that reads it expects it (the
    /// address, key header, allowed paths or indexes, and hidden fields).
    pub fn reader(&self) -> super::connectors::Connector {
        use super::connectors::{ConnKind, Connector};
        let kind = if self.kind == Kind::Search {
            ConnKind::OpenSearch
        } else {
            ConnKind::Rest
        };
        Connector {
            enabled: self.enabled,
            status: self.status.clone(),
            site: self.url.clone(),
            header_name: self.header_name.clone(),
            paths: self.paths.clone(),
            email: self.user.clone(),
            indexes: self.indexes.clone(),
            hidden_fields: self.hidden_fields.clone(),
            ..Connector::new(kind)
        }
    }

    /// A data source holding what a REST API or OpenSearch connection made in
    /// Settings → Integrations held, under `id`. The sealed key moves as it
    /// is: both are sealed for the team with the same purpose.
    pub fn from_connection(c: &super::connectors::Connector, id: &str) -> Option<Source> {
        use super::connectors::ConnKind;
        let kind = match c.kind() {
            ConnKind::Rest => Kind::Rest,
            ConnKind::OpenSearch => Kind::Search,
            _ => return None,
        };
        let name = if !c.name.is_empty() {
            c.name.clone()
        } else if kind == Kind::Search {
            "OpenSearch".to_string()
        } else {
            "REST API".to_string()
        };
        let mut hidden = default_hidden_fields();
        for h in &c.hidden_fields {
            if !hidden.iter().any(|x| x.eq_ignore_ascii_case(h)) {
                hidden.push(h.clone());
            }
        }
        Some(Source {
            id: id.to_string(),
            kind,
            name,
            url: c.site.clone(),
            auth: AuthStyle::parse(""),
            key_sealed: c.key_sealed.clone(),
            key_hint: c.key_hint.clone(),
            enabled: c.enabled,
            setup_complete: true,
            status: c.status.clone(),
            status_message: c.status_message.clone(),
            has_mutations: false,
            kinds: None,
            schema_available: false,
            samples: Vec::new(),
            hidden_fields: hidden,
            repos: c.repos.clone(),
            last_checked_at: c.updated_at.clone(),
            created_at: c.updated_at.clone(),
            created_by: c.updated_by.clone(),
            aws: None,
            header_name: c.header_name.clone(),
            paths: c.paths.clone(),
            user: c.email.clone(),
            indexes: c.indexes.clone(),
            instructions: c.instructions.clone(),
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
                "cluster": if a.engine == "redshift" { a.cluster_id.as_str() } else { a.cluster_arn.rsplit(':').next().unwrap_or("") },
                "database": a.database,
                "engine": if a.engine.is_empty() { "postgres" } else { a.engine.as_str() },
                "databases": a.databases,
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
            "header_name": self.header_name,
            "paths": self.paths,
            "user": self.user,
            "indexes": self.indexes,
            "instructions": self.instructions,
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

/// The data source id a moved connection gets: fixed, so moving it twice
/// (a retry, two processes at once) never makes two.
pub fn moved_id(c: &super::connectors::Connector) -> String {
    let name = if c.name.is_empty() {
        "default"
    } else {
        c.name.as_str()
    };
    format!("moved-{}-{name}", c.kind().as_str())
}

/// Moves the REST API and OpenSearch connections a team made in Settings →
/// Integrations into its data sources, where every other system agents read
/// lives, then removes them there. Safe to run any number of times: a source
/// is written only if it isn't there yet, and the connection is removed
/// after. Returns how many moved.
pub async fn adopt_connections(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
) -> Result<usize, String> {
    use super::connectors::{self, ConnKind};
    let conns = connectors::list(dynamo, table, team_id).await?;
    let movable: Vec<_> = conns
        .iter()
        .filter(|c| matches!(c.kind(), ConnKind::Rest | ConnKind::OpenSearch))
        .collect();
    if movable.is_empty() {
        return Ok(0);
    }
    let taken: Vec<String> = list_sources(dynamo, table, team_id)
        .await?
        .into_iter()
        .map(|s| s.name.to_lowercase())
        .collect();
    let mut moved = 0;
    for c in movable {
        let Some(mut source) = Source::from_connection(c, &moved_id(c)) else {
            continue;
        };
        if taken.contains(&source.name.to_lowercase()) {
            source.name = format!("{} ({})", source.name, source.kind.as_str());
        }
        let put = dynamo
            .put_item()
            .table_name(table)
            .set_item(Some(source.to_item(team_id)))
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await;
        if let Err(e) = put {
            let exists = e
                .as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception());
            if !exists {
                return Err(format!("move connection {}: {e}", c.id()));
            }
        }
        connectors::delete(dynamo, table, team_id, c.kind(), &c.name).await?;
        moved += 1;
    }
    Ok(moved)
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

/// Hidden-field suggestions for one source, kept beside it so an admin can
/// accept or dismiss each. Dismissed entries are never suggested again.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HiddenSuggestions {
    #[serde(default)]
    pub suggestions: Vec<super::hidden_suggest::Suggestion>,
    #[serde(default)]
    pub dismissed: Vec<String>,
    /// The table and column names they were worked out from.
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub at: String,
}

fn suggestions_sk(source_id: &str) -> String {
    format!("DATA_SOURCE_HIDDEN#{source_id}")
}

pub async fn get_hidden_suggestions(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    source_id: &str,
) -> Result<HiddenSuggestions, String> {
    let out = dynamo
        .get_item()
        .table_name(table)
        .key("pk", av_s(team_id))
        .key("sk", av_s(&suggestions_sk(source_id)))
        .send()
        .await
        .map_err(|e| format!("get hidden suggestions: {e}"))?;
    Ok(out
        .item()
        .map(|i| serde_json::from_str(&s(i, "data")).unwrap_or_default())
        .unwrap_or_default())
}

pub async fn put_hidden_suggestions(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    source_id: &str,
    h: &HiddenSuggestions,
) -> Result<(), String> {
    dynamo
        .put_item()
        .table_name(table)
        .item("pk", av_s(team_id))
        .item("sk", av_s(&suggestions_sk(source_id)))
        .item("record_type", av_s("data_source_hidden_suggestions"))
        .item(
            "data",
            av_s(&serde_json::to_string(h).unwrap_or_else(|_| "{}".into())),
        )
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("put hidden suggestions: {e}"))
}

pub async fn delete_hidden_suggestions(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    source_id: &str,
) -> Result<(), String> {
    dynamo
        .delete_item()
        .table_name(table)
        .key("pk", av_s(team_id))
        .key("sk", av_s(&suggestions_sk(source_id)))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("delete hidden suggestions: {e}"))
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
    /// `ok` | `refused` | `failed` | `unknown_name` (the lookup named a table,
    /// column, type or field the source doesn't have, so it wasn't sent)
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

/// The run and the work a lookup was made during, for a link from the log.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunRef {
    pub run_id: String,
    /// e.g. "owner/name#12", or "owner/name · TICKET-1" before a PR exists.
    pub origin: String,
}

/// Records a lookup. A failure to record is returned so callers can decide;
/// the reviewer withholds the result of a lookup it could not record.
pub async fn log_activity(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    a: &Activity,
) -> Result<(), String> {
    log_activity_in(dynamo, table, team_id, a, &RunRef::default()).await
}

/// [`log_activity`], also keeping the run and work it was made during.
pub async fn log_activity_in(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    a: &Activity,
    run: &RunRef,
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
    for (k, v) in [("run_id", &run.run_id), ("origin", &run.origin)] {
        if !v.is_empty() {
            req = req.item(k, av_s(crate::truncate_str(v, 300)));
        }
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
            for k in ["run_id", "origin"] {
                let v = s(i, k);
                if !v.is_empty() {
                    ev[k] = json!(v);
                }
            }
            ev
        })
        .collect())
}

/// One lookup, as the work it was made during sees it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Lookup {
    pub source_id: String,
    pub source_name: String,
    pub actor: String,
    pub repo: String,
    pub pr: u64,
    pub run_id: String,
    pub origin: String,
    pub outcome: String,
}

/// Lookups between `since` and `until` (RFC 3339), oldest first, at most
/// `cap` looked at.
pub async fn lookups_between(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    since: &str,
    until: &str,
    cap: usize,
) -> Result<Vec<Lookup>, String> {
    let mut out = Vec::new();
    let mut start = None;
    loop {
        let r = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND sk BETWEEN :lo AND :hi")
            .expression_attribute_values(":pk", av_s(team_id))
            .expression_attribute_values(":lo", av_s(&format!("{LOG_PREFIX}{since}")))
            .expression_attribute_values(":hi", av_s(&format!("{LOG_PREFIX}{until}~")))
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| format!("lookups: {e}"))?;
        for i in r.items() {
            out.push(Lookup {
                source_id: s(i, "source_id"),
                source_name: s(i, "source_name"),
                actor: s(i, "actor"),
                repo: s(i, "repo"),
                pr: i
                    .get("pr")
                    .and_then(|v| v.as_n().ok())
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(0),
                run_id: s(i, "run_id"),
                origin: s(i, "origin"),
                outcome: s(i, "outcome"),
            });
        }
        start = r.last_evaluated_key().cloned();
        if start.is_none() || out.len() >= cap {
            return Ok(out);
        }
    }
}

/// Lookups since `since` (RFC 3339) by outcome: `(ok, refused, failed)`.
pub async fn count_activity(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    since: &str,
) -> Result<(i64, i64, i64), String> {
    let (mut ok, mut refused, mut failed) = (0, 0, 0);
    let mut start = None;
    loop {
        let out = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND sk BETWEEN :lo AND :hi")
            .expression_attribute_values(":pk", av_s(team_id))
            .expression_attribute_values(":lo", av_s(&format!("{LOG_PREFIX}{since}")))
            .expression_attribute_values(":hi", av_s(&format!("{LOG_PREFIX}~")))
            .projection_expression("outcome")
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| format!("count lookups: {e}"))?;
        for i in out.items() {
            match s(i, "outcome").as_str() {
                "ok" => ok += 1,
                // Nothing was sent either way.
                "refused" | "unknown_name" => refused += 1,
                _ => failed += 1,
            }
        }
        start = out.last_evaluated_key().cloned();
        if start.is_none() {
            return Ok((ok, refused, failed));
        }
    }
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
            header_name: String::new(),
            paths: Vec::new(),
            user: String::new(),
            indexes: Vec::new(),
            instructions: String::new(),
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

    #[test]
    fn a_connection_becomes_a_data_source_with_its_key_and_guide() {
        use super::super::connectors::{ConnKind, Connector};
        let c = Connector {
            name: "billing".into(),
            enabled: true,
            status: "ready".into(),
            site: "https://api.example.com".into(),
            header_name: "X-Api-Key".into(),
            paths: vec!["/v1/invoices".into()],
            key_sealed: "sealed".into(),
            key_hint: "AB12".into(),
            hidden_fields: vec!["notes".into()],
            instructions: "amount is in cents.".into(),
            ..Connector::new(ConnKind::Rest)
        };
        assert_eq!(moved_id(&c), "moved-rest-billing");
        let s = Source::from_connection(&c, &moved_id(&c)).unwrap();
        assert_eq!(s.kind, Kind::Rest);
        assert_eq!(
            (s.name.as_str(), s.url.as_str()),
            ("billing", "https://api.example.com")
        );
        assert_eq!(
            (s.key_sealed.as_str(), s.key_hint.as_str()),
            ("sealed", "AB12")
        );
        assert!(s.enabled && s.setup_complete && s.status == "ready");
        assert_eq!(s.paths, vec!["/v1/invoices".to_string()]);
        assert!(s.hidden_fields.contains(&"notes".to_string()) && s.hidden_fields.len() > 1);
        assert_eq!(s.instructions, "amount is in cents.");
        // It round-trips through the table, and reads back as the client expects.
        let back = Source::from_item(&s.to_item("TEAM#1")).unwrap();
        assert_eq!(back, s);
        let r = back.reader();
        assert_eq!(r.kind(), ConnKind::Rest);
        assert_eq!(
            (r.site.as_str(), r.header_name.as_str()),
            ("https://api.example.com", "X-Api-Key")
        );

        let os = Connector {
            email: "reader".into(),
            indexes: vec!["orders-*".into()],
            ..Connector::new(ConnKind::OpenSearch)
        };
        assert_eq!(moved_id(&os), "moved-opensearch-default");
        let s = Source::from_connection(&os, "x").unwrap();
        assert_eq!(
            (s.kind, s.name.as_str(), s.user.as_str()),
            (Kind::Search, "OpenSearch", "reader")
        );
        assert_eq!(s.reader().kind(), ConnKind::OpenSearch);
        assert!(Source::from_connection(&Connector::new(ConnKind::NewRelic), "x").is_none());
    }

    /// Against a real team's saved connections, without writing anything.
    /// Set LIVE_TEAM and LIVE_TABLE, then run
    /// `cargo test -- --ignored moved_connections`. Each REST API and
    /// OpenSearch connection is turned into the data source the move would
    /// save, its sealed key is opened, and one real read is made through it.
    #[tokio::test]
    #[ignore = "needs a real AWS account"]
    async fn moved_connections_read_like_before() {
        use super::super::connectors::{self, ConnKind};
        use super::super::http::Https;
        use super::super::opensearch::OpenSearch;
        use super::super::rest::Rest;
        let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} not set"));
        let (team, table) = (env("LIVE_TEAM"), env("LIVE_TABLE"));
        let conf = aws_config::load_from_env().await;
        let dynamo = aws_sdk_dynamodb::Client::new(&conf);
        let kms = aws_sdk_kms::Client::new(&conf);
        let conns = connectors::list(&dynamo, &table, &team).await.unwrap();
        let movable: Vec<_> = conns
            .iter()
            .filter(|c| matches!(c.kind(), ConnKind::Rest | ConnKind::OpenSearch))
            .collect();
        assert!(
            !movable.is_empty(),
            "no REST or OpenSearch connections to move"
        );
        for c in movable {
            let s = Source::from_connection(c, &moved_id(c)).unwrap();
            assert_eq!(Source::from_item(&s.to_item(&team)).unwrap(), s);
            let key = super::super::secret::open(&kms, &team, &s.key_sealed)
                .await
                .unwrap();
            let r = s.reader();
            let read = match s.kind {
                Kind::Rest => Rest {
                    conn: &r,
                    key: &key,
                    http: &Https,
                }
                .get(&s.paths[0], None)
                .await
                .map(|a| format!("{} -> {} characters", a.asked, a.text.len())),
                _ => OpenSearch {
                    conn: &r,
                    password: &key,
                    http: &Https,
                }
                .indexes()
                .await
                .map(|a| format!("{} -> {}", a.asked, a.text.lines().count())),
            };
            println!(
                "{} ({}) as data source {:?}: {:?}",
                c.id(),
                s.kind.as_str(),
                s.name,
                read.as_ref().map_err(|e| &e.message)
            );
            assert!(read.is_ok(), "{} didn't read after the move", c.id());
        }
    }
}
