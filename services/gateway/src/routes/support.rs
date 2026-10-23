//! Support tickets: anyone on a team can ask Coderhelm for help from inside
//! the dashboard, follow the thread, and get an email when support answers.
//! Platform admins work every team's tickets from one queue.
//!
//! Storage is the settings table, the same place other per-team items live:
//!
//! - `pk = <team_id>`, `sk = SUPPORT#<ticket id>`: the ticket.
//! - `pk = <team_id>`, `sk = SUPPORT_MSG#<ticket id>#<time>#<id>`: one message
//!   or timeline event in its thread, oldest first.
//! - `pk = SUPPORT_QUEUE`, `sk = <ticket id>`: a copy of the ticket for the
//!   admin queue, so the queue is one query instead of a table scan. Written
//!   in the same transaction as the team's copy.
//! - `pk = <team_id>`, `sk = SUPPORT_RATE#<bucket>`: a counter that limits how
//!   often a team can open tickets and reply. Expires through `ttl`.
//!
//! Isolation: every customer read and write is keyed by the caller's own
//! `team_id` from the session, so another team's ticket id finds nothing and
//! answers 404. Only a platform admin (the same check `/api/admin/teams`
//! uses) reaches the queue and other teams' tickets, and only through the
//! `/api/admin/support` routes.
//!
//! What a ticket records about where it was filed: the dashboard path (no
//! query string), the team and the filer's role, and the items attached to
//! it (runs, reviews, releases, plans, alerts, data sources; at most
//! [`MAX_ATTACHMENTS`]). An attachment is its id, kind, a readable name and
//! a status, read from the team's own record when it is attached: an item
//! that is not the caller's team's is refused. Never a diff, file contents
//! or anything read from the team's repos. Likely secrets typed into the
//! subject or message are replaced before anything is stored or emailed.

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem};
use aws_sdk_sesv2::types::{Body, Content, Destination, EmailContent, Message as SesMessage};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Extension, Json,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tracing::{error, info, warn};

use crate::models::Claims;
use crate::AppState;

pub const QUEUE_PK: &str = "SUPPORT_QUEUE";
const META_PREFIX: &str = "SUPPORT#";
const MSG_PREFIX: &str = "SUPPORT_MSG#";
const RATE_PREFIX: &str = "SUPPORT_RATE#";

const APP: &str = "https://app.coderhelm.com";
const DEFAULT_INBOX: &str = "support@coderhelm.com";

pub const MAX_SUBJECT: usize = 150;
pub const MAX_BODY: usize = 8000;
const MAX_PAGE: usize = 300;
const MAX_ITEM_ID: usize = 200;
const MAX_LABEL: usize = 200;
/// Items one ticket can carry, over the ticket and all its replies.
pub const MAX_ATTACHMENTS: usize = 10;
/// What tickets can carry.
pub const ITEM_KINDS: [&str; 6] = ["run", "review", "release", "plan", "alert", "data_source"];

/// New tickets a team may open per hour, and one person within that.
pub const TEAM_TICKETS_PER_HOUR: u32 = 10;
pub const USER_TICKETS_PER_HOUR: u32 = 5;
/// Customer replies a team may send per hour (support replies are not limited).
pub const TEAM_REPLIES_PER_HOUR: u32 = 60;

// ── Model ────────────────────────────────────────────────────────────────

/// Whose move it is. `Open`: support's. `Waiting`: the customer's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Open,
    Waiting,
    Resolved,
}

impl Status {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "open" => Some(Self::Open),
            "waiting" => Some(Self::Waiting),
            "resolved" => Some(Self::Resolved),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Waiting => "waiting",
            Self::Resolved => "resolved",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelatedItem {
    /// run | review | release | plan | alert | data_source
    pub kind: String,
    pub id: String,
    /// Readable name: a run's title, "owner/repo #42", "owner/repo v1.2.0".
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub repo: String,
    /// The item's status when it was attached: a run's status, a review's
    /// verdict, an alert's outcome.
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TicketContext {
    /// The dashboard path the ticket was filed from, without a query string.
    #[serde(default)]
    pub page: String,
    /// The first attachment, kept for tickets filed before `items` and for
    /// readers that know only one item.
    #[serde(default)]
    pub item: Option<RelatedItem>,
    /// Everything attached to the ticket, with the ticket or in a reply.
    #[serde(default)]
    pub items: Vec<RelatedItem>,
    /// The browser, for "it looks broken" reports.
    #[serde(default)]
    pub user_agent: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ticket {
    pub id: String,
    pub team_id: String,
    #[serde(default)]
    pub team_name: String,
    pub subject: String,
    pub status: Status,
    /// normal | high | urgent
    pub priority: String,
    pub opened_by_id: String,
    pub opened_by_email: String,
    #[serde(default)]
    pub opened_by_name: String,
    /// The filer's role on the team when they filed.
    pub role: String,
    pub context: TicketContext,
    pub created_at: String,
    pub updated_at: String,
    /// customer | support
    pub last_actor: String,
    pub message_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    /// message | event
    pub kind: String,
    /// customer | support
    pub author: String,
    #[serde(default)]
    pub author_email: String,
    #[serde(default)]
    pub author_name: String,
    /// The text, or for an event its action: "resolved", "reopened",
    /// "status:waiting", "priority:urgent".
    pub body: String,
    pub created_at: String,
    /// Items attached with this message.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<RelatedItem>,
}

/// Who is asking, from the session.
#[derive(Debug, Clone)]
pub struct Caller {
    pub user_id: String,
    pub team_id: String,
    pub email: String,
    pub name: String,
    pub role: String,
    pub platform_admin: bool,
    /// A read-only support-view session (see `support_access`).
    pub in_support_view: bool,
}

impl Caller {
    pub(crate) fn from_claims(c: &Claims) -> Self {
        Caller {
            user_id: c.sub.clone(),
            team_id: c.team_id.clone(),
            email: c.email.clone(),
            name: c.github_login.clone().unwrap_or_default(),
            role: c.role.clone(),
            platform_admin: super::api::is_platform_admin(c),
            in_support_view: c.support_view.is_some(),
        }
    }
}

/// The key condition for items under one key, with or without an `sk` prefix.
fn key_condition(prefix: &str) -> &'static str {
    if prefix.is_empty() {
        "pk = :pk"
    } else {
        "pk = :pk AND begins_with(sk, :p)"
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Fail {
    BadRequest(String),
    NotFound,
    Forbidden,
    /// Refused, with the reason in words.
    Denied(String),
    TooMany(String),
    Internal(String),
}

impl Fail {
    pub(crate) fn reply(self) -> (StatusCode, Json<Value>) {
        let (code, msg) = match self {
            Fail::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            Fail::NotFound => (StatusCode::NOT_FOUND, "Ticket not found.".to_string()),
            Fail::Forbidden => (
                StatusCode::FORBIDDEN,
                "Only Coderhelm platform admins can do this.".to_string(),
            ),
            Fail::Denied(m) => (StatusCode::FORBIDDEN, m),
            Fail::TooMany(m) => (StatusCode::TOO_MANY_REQUESTS, m),
            Fail::Internal(e) => {
                error!(error = %e, "support tickets");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Something went wrong on our side. Try again.".to_string(),
                )
            }
        };
        (code, Json(json!({ "error": msg })))
    }
}

pub(crate) type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

// ── Cleaning what people send ────────────────────────────────────────────

static SECRET_PATTERNS: LazyLock<Vec<(Regex, &'static str)>> = LazyLock::new(|| {
    [
        // Private key blocks, whole.
        (
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----",
            "[hidden private key]",
        ),
        // key=value and "key": "value" pairs whose key names a secret.
        (
            r#"(?i)((?:password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|client[_-]?secret|authorization|cookie)["']?\s*[:=]\s*["']?)(?:bearer\s+|basic\s+)?[^\s"',;&}]+"#,
            "$1[hidden]",
        ),
        (r"(?i)\b(bearer|basic)\s+[a-z0-9._~+/=-]{8,}", "$1 [hidden]"),
        // JSON web tokens.
        (
            r"\beyJ[a-zA-Z0-9_-]{8,}\.[a-zA-Z0-9_-]{8,}\.[a-zA-Z0-9_-]{8,}",
            "[hidden]",
        ),
        // AWS access key ids.
        (r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b", "[hidden]"),
        // GitHub tokens.
        (
            r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})",
            "[hidden]",
        ),
        // Slack tokens.
        (r"\bxox[abposr]-[A-Za-z0-9-]{10,}", "[hidden]"),
        // Model provider keys.
        (r"\bsk-(?:ant-)?[A-Za-z0-9_-]{20,}", "[hidden]"),
    ]
    .into_iter()
    .map(|(p, r)| (Regex::new(p).expect("valid pattern"), r))
    .collect()
});

/// Replace likely secrets in free text. Email addresses are kept: people
/// name teammates in tickets.
pub fn scrub(text: &str) -> String {
    let mut out = text.to_string();
    for (re, with) in SECRET_PATTERNS.iter() {
        if re.is_match(&out) {
            out = re.replace_all(&out, *with).into_owned();
        }
    }
    out
}

/// A dashboard path, or empty. The query string and fragment are dropped
/// (they can hold search words); anything that is not a plain in-app path
/// is refused.
pub fn clean_page(page: &str) -> String {
    let path = page.split(['?', '#']).next().unwrap_or("").trim();
    if !path.starts_with('/') || path.starts_with("//") || path.chars().count() > MAX_PAGE {
        return String::new();
    }
    if path
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '-' | '_' | '.'))
    {
        path.to_string()
    } else {
        String::new()
    }
}

fn clean_item_id(id: &str) -> Option<String> {
    let id = id.trim();
    let ok = !id.is_empty()
        && id.chars().count() <= MAX_ITEM_ID
        && id.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':' | '#' | '/' | '@' | '+')
        });
    ok.then(|| id.to_string())
}

pub(crate) fn clip(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

pub(crate) fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[derive(Debug, Deserialize, Default)]
pub struct NewTicket {
    #[serde(default)]
    pub subject: String,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub priority: Option<String>,
    #[serde(default)]
    pub page: Option<String>,
    /// The item from the page the ticket was filed on (older dashboards).
    #[serde(default)]
    pub item: Option<ItemRef>,
    /// Items the filer attached.
    #[serde(default)]
    pub items: Vec<ItemRef>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ItemRef {
    pub kind: String,
    pub id: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub repo: Option<String>,
}

impl ItemRef {
    /// The item as the customer described it, cleaned. `None` when it is not
    /// a kind tickets can be about or its id is not a plain id.
    pub fn cleaned(&self) -> Option<RelatedItem> {
        let kind = ITEM_KINDS
            .iter()
            .find(|k| **k == self.kind.as_str())?
            .to_string();
        let id = clean_item_id(&self.id)?;
        let text =
            |v: &Option<String>| clip(&scrub(&one_line(v.as_deref().unwrap_or(""))), MAX_LABEL);
        Some(RelatedItem {
            kind,
            id,
            label: text(&self.label),
            repo: text(&self.repo),
            status: String::new(),
        })
    }
}

fn priority(p: Option<&str>) -> &'static str {
    match p {
        Some("urgent") => "urgent",
        Some("high") => "high",
        _ => "normal",
    }
}

fn check_body(body: &str, what: &str) -> Result<String, Fail> {
    let body = body.trim();
    if body.is_empty() {
        return Err(Fail::BadRequest(format!("{what} cannot be empty.")));
    }
    if body.chars().count() > MAX_BODY {
        return Err(Fail::BadRequest(format!(
            "{what} is too long (at most {MAX_BODY} characters)."
        )));
    }
    Ok(scrub(body))
}

// ── Attachments ──────────────────────────────────────────────────────────

/// Finds an item in a team's own records. A trait so the team checks can be
/// tested without DynamoDB.
pub trait ItemLookup {
    /// The item as `team_id`'s records have it (its readable name, repo and
    /// status), or `None` when the team has no such item. Reads only under
    /// `team_id`.
    async fn find(&self, team_id: &str, item: &RelatedItem) -> Result<Option<RelatedItem>, String>;
}

/// Everything attached to a ticket, including the one item older tickets
/// kept in `context.item`.
pub fn attachments(t: &Ticket) -> Vec<RelatedItem> {
    if t.context.items.is_empty() {
        t.context.item.iter().cloned().collect()
    } else {
        t.context.items.clone()
    }
}

fn kind_words(kind: &str) -> &'static str {
    match kind {
        "run" => "run",
        "review" => "review",
        "release" => "release",
        "plan" => "plan",
        "alert" => "alert",
        _ => "data source",
    }
}

/// The items a person wants to attach, checked: each a kind tickets carry
/// with a plain id, each found in the caller's team's own records (its name
/// and status come from there, not from the request), none attached twice,
/// and no more than [`MAX_ATTACHMENTS`] on the ticket with `already`.
pub async fn resolve_items<L: ItemLookup>(
    lookup: &L,
    team_id: &str,
    refs: &[ItemRef],
    already: &[RelatedItem],
) -> Result<Vec<RelatedItem>, Fail> {
    let mut out: Vec<RelatedItem> = Vec::new();
    for r in refs {
        let Some(cleaned) = r.cleaned() else {
            return Err(Fail::BadRequest(
                "Attach runs, reviews, releases, plans, alerts or data sources.".into(),
            ));
        };
        let seen = |v: &[RelatedItem]| {
            v.iter()
                .any(|i| i.kind == cleaned.kind && i.id == cleaned.id)
        };
        if seen(already) || seen(&out) {
            continue;
        }
        if already.len() + out.len() >= MAX_ATTACHMENTS {
            return Err(Fail::BadRequest(format!(
                "A ticket can carry at most {MAX_ATTACHMENTS} items."
            )));
        }
        let found = lookup
            .find(team_id, &cleaned)
            .await
            .map_err(Fail::Internal)?;
        let Some(mut found) = found else {
            return Err(Fail::BadRequest(format!(
                "That {} is not one of your team's.",
                kind_words(&cleaned.kind)
            )));
        };
        // What is kept is the team's own record of it, cleaned again.
        found.kind = cleaned.kind.clone();
        found.id = cleaned.id.clone();
        let text = |v: &str, max: usize| clip(&scrub(&one_line(v)), max);
        found.label = text(
            if found.label.is_empty() {
                &cleaned.label
            } else {
                &found.label
            },
            MAX_LABEL,
        );
        found.repo = text(&found.repo, MAX_LABEL);
        found.status = text(&found.status, 40);
        out.push(found);
    }
    Ok(out)
}

// ── Storage ──────────────────────────────────────────────────────────────

/// Where tickets are kept. A trait so the access rules can be tested without
/// DynamoDB.
pub trait Store {
    /// Write the ticket to its team and to the admin queue, together.
    async fn save(&self, t: &Ticket) -> Result<(), String>;
    /// A team's ticket. Looks only under `team_id`.
    async fn load(&self, team_id: &str, id: &str) -> Result<Option<Ticket>, String>;
    async fn list_team(&self, team_id: &str) -> Result<Vec<Ticket>, String>;
    /// Every team's tickets (the admin queue).
    async fn list_queue(&self) -> Result<Vec<Ticket>, String>;
    /// A ticket's queue copy, which names its team.
    async fn queue_entry(&self, id: &str) -> Result<Option<Ticket>, String>;
    async fn append(&self, team_id: &str, ticket_id: &str, m: &Message) -> Result<(), String>;
    async fn thread(&self, team_id: &str, ticket_id: &str) -> Result<Vec<Message>, String>;
    /// Count one use of `bucket` for the team; false once `limit` is reached.
    async fn take(&self, team_id: &str, bucket: &str, limit: u32, ttl: i64)
        -> Result<bool, String>;
}

pub struct DynamoStore<'a> {
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub table: &'a str,
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn data_of<T: for<'de> Deserialize<'de>>(item: &HashMap<String, AttributeValue>) -> Option<T> {
    item.get("data")
        .and_then(|v| v.as_s().ok())
        .and_then(|d| serde_json::from_str(d).ok())
}

impl DynamoStore<'_> {
    fn put(&self, pk: &str, sk: &str, t: &Ticket, data: &str) -> Result<Put, String> {
        Put::builder()
            .table_name(self.table)
            .item("pk", s(pk))
            .item("sk", s(sk))
            .item("team_id", s(&t.team_id))
            .item("status", s(t.status.as_str()))
            .item("updated_at", s(&t.updated_at))
            .item("data", s(data))
            .build()
            .map_err(|e| e.to_string())
    }

    async fn query_all(
        &self,
        pk: &str,
        prefix: &str,
    ) -> Result<Vec<HashMap<String, AttributeValue>>, String> {
        let mut out = Vec::new();
        let mut start = None;
        loop {
            // DynamoDB refuses `begins_with` with an empty string: no prefix
            // means every item under the key.
            let mut q = self
                .dynamo
                .query()
                .table_name(self.table)
                .key_condition_expression(key_condition(prefix))
                .expression_attribute_values(":pk", s(pk));
            if !prefix.is_empty() {
                q = q.expression_attribute_values(":p", s(prefix));
            }
            let page = q
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e)))?;
            out.extend(page.items().iter().cloned());
            start = page.last_evaluated_key().cloned();
            if start.is_none() || out.len() >= 5000 {
                break;
            }
        }
        Ok(out)
    }
}

impl Store for DynamoStore<'_> {
    async fn save(&self, t: &Ticket) -> Result<(), String> {
        let data = serde_json::to_string(t).map_err(|e| e.to_string())?;
        let team = self.put(&t.team_id, &format!("{META_PREFIX}{}", t.id), t, &data)?;
        let queue = self.put(QUEUE_PK, &t.id, t, &data)?;
        self.dynamo
            .transact_write_items()
            .transact_items(TransactWriteItem::builder().put(team).build())
            .transact_items(TransactWriteItem::builder().put(queue).build())
            .send()
            .await
            .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e)))?;
        Ok(())
    }

    async fn load(&self, team_id: &str, id: &str) -> Result<Option<Ticket>, String> {
        let r = self
            .dynamo
            .get_item()
            .table_name(self.table)
            .key("pk", s(team_id))
            .key("sk", s(&format!("{META_PREFIX}{id}")))
            .send()
            .await
            .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e)))?;
        Ok(r.item().and_then(data_of))
    }

    async fn list_team(&self, team_id: &str) -> Result<Vec<Ticket>, String> {
        let items = self.query_all(team_id, META_PREFIX).await?;
        Ok(items.iter().filter_map(data_of).collect())
    }

    async fn list_queue(&self) -> Result<Vec<Ticket>, String> {
        let items = self.query_all(QUEUE_PK, "").await?;
        Ok(items.iter().filter_map(data_of).collect())
    }

    async fn queue_entry(&self, id: &str) -> Result<Option<Ticket>, String> {
        let r = self
            .dynamo
            .get_item()
            .table_name(self.table)
            .key("pk", s(QUEUE_PK))
            .key("sk", s(id))
            .send()
            .await
            .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e)))?;
        Ok(r.item().and_then(data_of))
    }

    async fn append(&self, team_id: &str, ticket_id: &str, m: &Message) -> Result<(), String> {
        let data = serde_json::to_string(m).map_err(|e| e.to_string())?;
        self.dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", s(team_id))
            .item(
                "sk",
                s(&format!(
                    "{MSG_PREFIX}{ticket_id}#{}#{}",
                    m.created_at, m.id
                )),
            )
            .item("data", s(&data))
            .send()
            .await
            .map_err(|e| format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e)))?;
        Ok(())
    }

    async fn thread(&self, team_id: &str, ticket_id: &str) -> Result<Vec<Message>, String> {
        let items = self
            .query_all(team_id, &format!("{MSG_PREFIX}{ticket_id}#"))
            .await?;
        Ok(items.iter().filter_map(data_of).collect())
    }

    async fn take(
        &self,
        team_id: &str,
        bucket: &str,
        limit: u32,
        ttl: i64,
    ) -> Result<bool, String> {
        let r = self
            .dynamo
            .update_item()
            .table_name(self.table)
            .key("pk", s(team_id))
            .key("sk", s(&format!("{RATE_PREFIX}{bucket}")))
            .update_expression("ADD n :one SET #ttl = if_not_exists(#ttl, :ttl)")
            .condition_expression("attribute_not_exists(n) OR n < :limit")
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_values(":one", AttributeValue::N("1".into()))
            .expression_attribute_values(":limit", AttributeValue::N(limit.to_string()))
            .expression_attribute_values(":ttl", AttributeValue::N(ttl.to_string()))
            .send()
            .await;
        match r {
            Ok(_) => Ok(true),
            Err(e)
                if e.as_service_error()
                    .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
            {
                Ok(false)
            }
            Err(e) => Err(format!(
                "{}",
                aws_sdk_dynamodb::error::DisplayErrorContext(&e)
            )),
        }
    }
}

// ── The rules ────────────────────────────────────────────────────────────

/// Ids that sort in the order they were made, even within one millisecond,
/// so a reply and the event it causes keep their order in the thread.
static IDS: LazyLock<std::sync::Mutex<ulid::Generator>> =
    LazyLock::new(|| std::sync::Mutex::new(ulid::Generator::new()));

fn new_id() -> String {
    let mut g = IDS.lock().unwrap_or_else(|e| e.into_inner());
    g.generate()
        .unwrap_or_else(|_| ulid::Ulid::new())
        .to_string()
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn hour_bucket(now: chrono::DateTime<chrono::Utc>) -> (String, i64) {
    let hour = now.format("%Y%m%d%H").to_string();
    // Keep the counter a day past its hour, then let it expire.
    (hour, now.timestamp() + 2 * 3600 + 86_400)
}

/// Newest activity first.
fn by_recent(mut v: Vec<Ticket>) -> Vec<Ticket> {
    v.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
    v
}

/// Open a ticket for the caller's team, within the hourly limits, with the
/// items attached that the team's records confirm.
pub async fn create<S: Store, L: ItemLookup>(
    store: &S,
    lookup: &L,
    caller: &Caller,
    req: &NewTicket,
    team_name: &str,
    user_agent: &str,
) -> Result<(Ticket, Message), Fail> {
    let subject = one_line(req.subject.trim());
    if subject.is_empty() {
        return Err(Fail::BadRequest("Add a short subject.".into()));
    }
    if subject.chars().count() > MAX_SUBJECT {
        return Err(Fail::BadRequest(format!(
            "The subject is too long (at most {MAX_SUBJECT} characters)."
        )));
    }
    let body = check_body(&req.body, "The description")?;
    let refs: Vec<ItemRef> = req.item.iter().chain(req.items.iter()).cloned().collect();
    let items = resolve_items(lookup, &caller.team_id, &refs, &[]).await?;

    // The person's allowance first, so one busy person does not use up the
    // team's.
    let (hour, ttl) = hour_bucket(chrono::Utc::now());
    let user_ok = store
        .take(
            &caller.team_id,
            &format!("create:{hour}:{}", caller.user_id),
            USER_TICKETS_PER_HOUR,
            ttl,
        )
        .await
        .map_err(Fail::Internal)?;
    if !user_ok {
        return Err(Fail::TooMany(
            "You have opened a lot of tickets in the last hour. Add to an open ticket, or try again later.".into(),
        ));
    }

    let team_ok = store
        .take(
            &caller.team_id,
            &format!("create:{hour}"),
            TEAM_TICKETS_PER_HOUR,
            ttl,
        )
        .await
        .map_err(Fail::Internal)?;
    if !team_ok {
        return Err(Fail::TooMany(
            "Your team has opened a lot of tickets in the last hour. Add to an open ticket, or try again later.".into(),
        ));
    }
    let now = now_iso();
    let ticket = Ticket {
        id: new_id(),
        team_id: caller.team_id.clone(),
        team_name: team_name.to_string(),
        subject: scrub(&subject),
        status: Status::Open,
        priority: priority(req.priority.as_deref()).to_string(),
        opened_by_id: caller.user_id.clone(),
        opened_by_email: caller.email.clone(),
        opened_by_name: caller.name.clone(),
        role: caller.role.clone(),
        context: TicketContext {
            page: clean_page(req.page.as_deref().unwrap_or("")),
            item: items.first().cloned(),
            items: items.clone(),
            user_agent: clip(&one_line(user_agent), 300),
        },
        created_at: now.clone(),
        updated_at: now.clone(),
        last_actor: "customer".into(),
        message_count: 1,
    };
    let first = Message {
        id: new_id(),
        kind: "message".into(),
        author: "customer".into(),
        author_email: caller.email.clone(),
        author_name: caller.name.clone(),
        body,
        created_at: now,
        attachments: items,
    };
    store.save(&ticket).await.map_err(Fail::Internal)?;
    store
        .append(&ticket.team_id, &ticket.id, &first)
        .await
        .map_err(Fail::Internal)?;
    Ok((ticket, first))
}

/// The caller's team's tickets.
pub async fn list_for_team<S: Store>(store: &S, caller: &Caller) -> Result<Vec<Ticket>, Fail> {
    let v = store
        .list_team(&caller.team_id)
        .await
        .map_err(Fail::Internal)?;
    Ok(by_recent(v))
}

/// One of the caller's team's tickets with its thread. Another team's ticket
/// is not found, whoever asks.
pub async fn open_for_team<S: Store>(
    store: &S,
    caller: &Caller,
    id: &str,
) -> Result<(Ticket, Vec<Message>), Fail> {
    let ticket = store
        .load(&caller.team_id, id)
        .await
        .map_err(Fail::Internal)?
        .ok_or(Fail::NotFound)?;
    // The key already names the team; this guards a copy that was written wrong.
    if ticket.team_id != caller.team_id {
        return Err(Fail::NotFound);
    }
    let thread = store
        .thread(&caller.team_id, id)
        .await
        .map_err(Fail::Internal)?;
    Ok((ticket, sorted(thread)))
}

fn sorted(mut v: Vec<Message>) -> Vec<Message> {
    v.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
    v
}

fn event(author: &str, caller: &Caller, action: &str, at: &str) -> Message {
    Message {
        id: new_id(),
        kind: "event".into(),
        author: author.into(),
        author_email: caller.email.clone(),
        author_name: caller.name.clone(),
        body: action.into(),
        created_at: at.into(),
        attachments: Vec::new(),
    }
}

/// A customer adds to their team's ticket, with or without more items. It
/// becomes support's turn again, which reopens a resolved ticket.
pub async fn reply_as_customer<S: Store, L: ItemLookup>(
    store: &S,
    lookup: &L,
    caller: &Caller,
    id: &str,
    body: &str,
    refs: &[ItemRef],
) -> Result<(Ticket, Message), Fail> {
    // Attaching items alone is a reply too.
    let body = if body.trim().is_empty() && !refs.is_empty() {
        String::new()
    } else {
        check_body(body, "The reply")?
    };
    let (mut ticket, _) = open_for_team(store, caller, id).await?;
    let mut all = attachments(&ticket);
    let added = resolve_items(lookup, &caller.team_id, refs, &all).await?;
    if body.is_empty() && added.is_empty() {
        return Err(Fail::BadRequest(
            "Those items are already on this ticket.".into(),
        ));
    }
    let (hour, ttl) = hour_bucket(chrono::Utc::now());
    if !store
        .take(
            &caller.team_id,
            &format!("reply:{hour}"),
            TEAM_REPLIES_PER_HOUR,
            ttl,
        )
        .await
        .map_err(Fail::Internal)?
    {
        return Err(Fail::TooMany(
            "Your team has sent a lot of replies in the last hour. Try again later.".into(),
        ));
    }
    let now = now_iso();
    if ticket.status == Status::Resolved {
        store
            .append(
                &ticket.team_id,
                &ticket.id,
                &event("customer", caller, "reopened", &now),
            )
            .await
            .map_err(Fail::Internal)?;
    }
    let m = Message {
        id: new_id(),
        kind: "message".into(),
        author: "customer".into(),
        author_email: caller.email.clone(),
        author_name: caller.name.clone(),
        body,
        created_at: now.clone(),
        attachments: added.clone(),
    };
    store
        .append(&ticket.team_id, &ticket.id, &m)
        .await
        .map_err(Fail::Internal)?;
    all.extend(added);
    ticket.context.item = all.first().cloned();
    ticket.context.items = all;
    ticket.status = Status::Open;
    ticket.last_actor = "customer".into();
    ticket.updated_at = now;
    ticket.message_count += 1;
    store.save(&ticket).await.map_err(Fail::Internal)?;
    Ok((ticket, m))
}

/// Adds a timeline event to one of `team_id`'s tickets (support access
/// granted or ended, support viewed the workspace) and moves the ticket up
/// the queue. Its status stays as it is. A ticket of another team is not
/// found.
pub async fn note_on_ticket<S: Store>(
    store: &S,
    team_id: &str,
    ticket_id: &str,
    author: &str,
    caller: &Caller,
    action: &str,
) -> Result<(), Fail> {
    let mut ticket = store
        .load(team_id, ticket_id)
        .await
        .map_err(Fail::Internal)?
        .ok_or(Fail::NotFound)?;
    if ticket.team_id != team_id {
        return Err(Fail::NotFound);
    }
    let now = now_iso();
    store
        .append(team_id, ticket_id, &event(author, caller, action, &now))
        .await
        .map_err(Fail::Internal)?;
    ticket.updated_at = now;
    store.save(&ticket).await.map_err(Fail::Internal)
}

fn require_admin(caller: &Caller) -> Result<(), Fail> {
    if caller.platform_admin {
        Ok(())
    } else {
        Err(Fail::Forbidden)
    }
}

/// Every team's tickets, for platform admins.
pub async fn admin_list<S: Store>(store: &S, caller: &Caller) -> Result<Vec<Ticket>, Fail> {
    require_admin(caller)?;
    let v = store.list_queue().await.map_err(Fail::Internal)?;
    Ok(by_recent(v))
}

/// Any team's ticket and thread, for platform admins.
pub async fn admin_open<S: Store>(
    store: &S,
    caller: &Caller,
    id: &str,
) -> Result<(Ticket, Vec<Message>), Fail> {
    require_admin(caller)?;
    let entry = store
        .queue_entry(id)
        .await
        .map_err(Fail::Internal)?
        .ok_or(Fail::NotFound)?;
    let ticket = store
        .load(&entry.team_id, id)
        .await
        .map_err(Fail::Internal)?
        .unwrap_or(entry);
    let thread = store
        .thread(&ticket.team_id, id)
        .await
        .map_err(Fail::Internal)?;
    Ok((ticket, sorted(thread)))
}

/// Support answers. By default it is then the customer's turn; `status`
/// can resolve in the same step.
pub async fn admin_reply<S: Store>(
    store: &S,
    caller: &Caller,
    id: &str,
    body: &str,
    status: Option<Status>,
) -> Result<(Ticket, Message), Fail> {
    require_admin(caller)?;
    let body = check_body(body, "The reply")?;
    let (mut ticket, _) = admin_open(store, caller, id).await?;
    let now = now_iso();
    let m = Message {
        id: new_id(),
        kind: "message".into(),
        author: "support".into(),
        author_email: caller.email.clone(),
        author_name: caller.name.clone(),
        body,
        created_at: now.clone(),
        attachments: Vec::new(),
    };
    store
        .append(&ticket.team_id, &ticket.id, &m)
        .await
        .map_err(Fail::Internal)?;
    let next = status.unwrap_or(Status::Waiting);
    if next == Status::Resolved {
        store
            .append(
                &ticket.team_id,
                &ticket.id,
                &event("support", caller, "resolved", &now),
            )
            .await
            .map_err(Fail::Internal)?;
    }
    ticket.status = next;
    ticket.last_actor = "support".into();
    ticket.updated_at = now;
    ticket.message_count += 1;
    store.save(&ticket).await.map_err(Fail::Internal)?;
    Ok((ticket, m))
}

/// Support changes a ticket's status or priority. Returns the ticket and
/// whether it has just been resolved.
pub async fn admin_set<S: Store>(
    store: &S,
    caller: &Caller,
    id: &str,
    status: Option<Status>,
    new_priority: Option<&str>,
) -> Result<(Ticket, bool), Fail> {
    require_admin(caller)?;
    if status.is_none() && new_priority.is_none() {
        return Err(Fail::BadRequest("Nothing to change.".into()));
    }
    let (mut ticket, _) = admin_open(store, caller, id).await?;
    let now = now_iso();
    let mut changed = false;
    let mut resolved_now = false;
    if let Some(next) = status {
        if next != ticket.status {
            let action = match (ticket.status, next) {
                (_, Status::Resolved) => "resolved".to_string(),
                (Status::Resolved, _) => "reopened".to_string(),
                (_, s) => format!("status:{}", s.as_str()),
            };
            resolved_now = next == Status::Resolved;
            store
                .append(
                    &ticket.team_id,
                    &ticket.id,
                    &event("support", caller, &action, &now),
                )
                .await
                .map_err(Fail::Internal)?;
            ticket.status = next;
            changed = true;
        }
    }
    if let Some(p) = new_priority {
        let p = priority(Some(p));
        if p != ticket.priority {
            store
                .append(
                    &ticket.team_id,
                    &ticket.id,
                    &event("support", caller, &format!("priority:{p}"), &now),
                )
                .await
                .map_err(Fail::Internal)?;
            ticket.priority = p.to_string();
            changed = true;
        }
    }
    if changed {
        ticket.updated_at = now;
        store.save(&ticket).await.map_err(Fail::Internal)?;
    }
    Ok((ticket, resolved_now))
}

// ── What each side sees ──────────────────────────────────────────────────

/// A thread as the customer sees it: support's people are "Coderhelm support",
/// not their own addresses.
pub fn customer_view(thread: Vec<Message>) -> Vec<Message> {
    thread
        .into_iter()
        .map(|mut m| {
            if m.author == "support" {
                m.author_email = String::new();
                m.author_name = "Coderhelm support".into();
            }
            m
        })
        .collect()
}

/// A ticket as the customer sees it: the browser string is for support only.
pub fn customer_ticket(mut t: Ticket) -> Ticket {
    t.context.user_agent = String::new();
    t
}

// ── Email ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mail {
    pub to: String,
    pub subject: String,
    pub text: String,
    pub html: String,
}

fn esc(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

fn customer_link(id: &str) -> String {
    format!("{APP}/support/ticket/?id={id}")
}

fn admin_link(id: &str) -> String {
    format!("{APP}/admin/tickets/?id={id}")
}

fn item_words(item: &RelatedItem) -> String {
    let what = match item.kind.as_str() {
        "run" => "Run",
        "review" => "Review",
        "release" => "Release",
        "plan" => "Plan",
        "alert" => "Alert",
        _ => "Data source",
    };
    let name = if item.label.is_empty() {
        item.id.clone()
    } else {
        item.label.clone()
    };
    let mut out = format!("{what}: {name}");
    if !item.repo.is_empty() && !name.contains(&item.repo) {
        out.push_str(&format!(" ({})", item.repo));
    }
    if !item.status.is_empty() {
        out.push_str(&format!(" · {}", item.status.replace('_', " ")));
    }
    out
}

fn html_page(
    heading: &str,
    lines: &[(String, String)],
    quote: &str,
    button: (&str, &str),
) -> String {
    let rows: String = lines
        .iter()
        .map(|(k, v)| {
            format!(
                "<tr><td style=\"padding:2px 12px 2px 0;color:#71717a\">{}</td><td style=\"padding:2px 0;color:#e4e4e7\">{}</td></tr>",
                esc(k),
                esc(v)
            )
        })
        .collect();
    format!(
        "<!doctype html><html><body style=\"margin:0;background:#09090b;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif\">\
<div style=\"max-width:560px;margin:0 auto;padding:24px 16px;color:#e4e4e7\">\
<p style=\"font-size:18px;font-weight:600;margin:0 0 12px\">{}</p>\
<table style=\"font-size:13px;margin:0 0 16px\">{rows}</table>\
<div style=\"white-space:pre-wrap;font-size:14px;line-height:20px;border-left:3px solid #3f3f46;padding:4px 0 4px 12px;margin:0 0 20px\">{}</div>\
<a href=\"{}\" style=\"display:inline-block;background:#ffffff;color:#09090b;text-decoration:none;font-size:14px;font-weight:600;padding:8px 16px;border-radius:8px\">{}</a>\
<p style=\"font-size:12px;color:#71717a;margin:24px 0 0\">Coderhelm support</p>\
</div></body></html>",
        esc(heading),
        esc(quote),
        esc(button.1),
        esc(button.0),
    )
}

fn context_lines(t: &Ticket) -> Vec<(String, String)> {
    let who = if t.opened_by_name.is_empty() {
        t.opened_by_email.clone()
    } else {
        format!("{} ({})", t.opened_by_name, t.opened_by_email)
    };
    let mut lines = vec![
        ("From".to_string(), who),
        (
            "Team".to_string(),
            if t.team_name.is_empty() {
                t.team_id.clone()
            } else {
                format!("{} ({})", t.team_name, t.team_id)
            },
        ),
        ("Role".to_string(), t.role.clone()),
        ("Priority".to_string(), t.priority.clone()),
    ];
    if !t.context.page.is_empty() {
        lines.push(("Page".to_string(), t.context.page.clone()));
    }
    for item in attachments(t) {
        lines.push(("Attached".to_string(), item_words(&item)));
    }
    lines
}

/// To the support inbox: a new ticket.
pub fn mail_new_ticket(inbox: &str, t: &Ticket, body: &str) -> Mail {
    let team = if t.team_name.is_empty() {
        &t.team_id
    } else {
        &t.team_name
    };
    let subject = format!("[Support] {team}: {}", t.subject);
    let lines = context_lines(t);
    let link = admin_link(&t.id);
    let mut text = format!("New ticket: {}\n\n", t.subject);
    for (k, v) in &lines {
        text.push_str(&format!("{k}: {v}\n"));
    }
    text.push_str(&format!("\n{body}\n\nAnswer it: {link}\n"));
    Mail {
        to: inbox.to_string(),
        subject,
        html: html_page(
            &format!("New ticket: {}", t.subject),
            &lines,
            body,
            ("Open the ticket", &link),
        ),
        text,
    }
}

/// To the support inbox: the customer added to a ticket.
pub fn mail_customer_reply(inbox: &str, t: &Ticket, m: &Message) -> Mail {
    let team = if t.team_name.is_empty() {
        &t.team_id
    } else {
        &t.team_name
    };
    let link = admin_link(&t.id);
    let who = if m.author_name.is_empty() {
        m.author_email.clone()
    } else {
        m.author_name.clone()
    };
    let mut lines = vec![
        ("From".to_string(), who),
        ("Team".to_string(), team.to_string()),
    ];
    for item in &m.attachments {
        lines.push(("Attached".to_string(), item_words(item)));
    }
    let mut text = format!("{}\n\n", t.subject);
    for (k, v) in lines.iter().skip(2) {
        text.push_str(&format!("{k}: {v}\n"));
    }
    text.push_str(&format!("{}\n\nAnswer it: {link}\n", m.body));
    Mail {
        to: inbox.to_string(),
        subject: format!("[Support] Reply from {team}: {}", t.subject),
        text,
        html: html_page(
            &format!("New reply: {}", t.subject),
            &lines,
            &m.body,
            ("Open the ticket", &link),
        ),
    }
}

/// To the person who filed: support answered.
pub fn mail_support_reply(t: &Ticket, m: &Message) -> Option<Mail> {
    if t.opened_by_email.is_empty() {
        return None;
    }
    let link = customer_link(&t.id);
    let resolved = t.status == Status::Resolved;
    let heading = if resolved {
        format!("Resolved: {}", t.subject)
    } else {
        format!("Coderhelm support replied: {}", t.subject)
    };
    let note = if resolved {
        "We marked this ticket resolved. Reply in Coderhelm if it is not."
    } else {
        "Reply in Coderhelm to keep the conversation in one place."
    };
    Some(Mail {
        to: t.opened_by_email.clone(),
        subject: format!("Re: {}", t.subject),
        text: format!("{heading}\n\n{}\n\n{note}\n{link}\n", m.body),
        html: html_page(
            &heading,
            &[("".to_string(), note.to_string())],
            &m.body,
            ("See the ticket", &link),
        ),
    })
}

/// To the person who filed: support resolved the ticket without a reply.
pub fn mail_resolved(t: &Ticket) -> Option<Mail> {
    if t.opened_by_email.is_empty() {
        return None;
    }
    let link = customer_link(&t.id);
    let note =
        "We marked this ticket resolved. Reply in Coderhelm if it is not, and it opens again.";
    Some(Mail {
        to: t.opened_by_email.clone(),
        subject: format!("Resolved: {}", t.subject),
        text: format!("{}\n\n{note}\n{link}\n", t.subject),
        html: html_page(
            &format!("Resolved: {}", t.subject),
            &[],
            note,
            ("See the ticket", &link),
        ),
    })
}

fn inbox() -> String {
    std::env::var("SUPPORT_INBOX")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_INBOX.to_string())
}

/// Send through SES from the same address as the weekly summary. A failed
/// email is logged, never a failed request: the ticket is already saved.
async fn send(state: &AppState, mail: Mail) {
    let content = |v: &str| Content::builder().data(v).charset("UTF-8").build();
    let built = (|| -> Result<SesMessage, aws_sdk_sesv2::error::BuildError> {
        Ok(SesMessage::builder()
            .subject(content(&mail.subject)?)
            .body(
                Body::builder()
                    .html(content(&mail.html)?)
                    .text(content(&mail.text)?)
                    .build(),
            )
            .build())
    })();
    let message = match built {
        Ok(m) => m,
        Err(e) => {
            error!(error = %e, "support email could not be built");
            return;
        }
    };
    if let Err(e) = state
        .ses
        .send_email()
        .from_email_address(&state.config.ses_from_address)
        .destination(Destination::builder().to_addresses(&mail.to).build())
        .content(EmailContent::builder().simple(message).build())
        .send()
        .await
    {
        warn!(error = %aws_sdk_sesv2::error::DisplayErrorContext(&e), "support email not sent");
    }
}

// ── Lookups the handlers need ────────────────────────────────────────────

pub(crate) async fn team_name(state: &AppState, team_id: &str) -> String {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.table_name)
        .key("pk", s(team_id))
        .key("sk", s("META"))
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned())
        .and_then(|i| i.get("github_org").and_then(|v| v.as_s().ok()).cloned())
        .unwrap_or_default()
}

/// Items read from the team's own records: every read is keyed on the
/// caller's team, so another team's id finds nothing.
pub struct DynamoLookup<'a> {
    pub state: &'a AppState,
}

fn dynamo_err<E>(e: aws_sdk_dynamodb::error::SdkError<E>) -> String
where
    E: std::error::Error + aws_sdk_dynamodb::error::ProvideErrorMetadata + 'static,
{
    format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e))
}

/// `owner/name` and the PR number from `REVIEW#{owner}/{name}#{pr:06}[#…]`.
fn review_repo_pr(sk: &str) -> Option<(String, u64)> {
    let rest = sk.strip_prefix("REVIEW#")?;
    let mut parts = rest.splitn(3, '#');
    let repo = parts.next()?;
    let pr = parts.next()?.parse().ok()?;
    (!repo.is_empty()).then(|| (repo.to_string(), pr))
}

impl ItemLookup for DynamoLookup<'_> {
    async fn find(&self, team_id: &str, item: &RelatedItem) -> Result<Option<RelatedItem>, String> {
        let cfg = &self.state.config;
        let db = &self.state.dynamo;
        let id = item.id.as_str();
        let get = |row: &HashMap<String, AttributeValue>, k: &str| {
            row.get(k)
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default()
        };
        let found = |label: String, repo: String, status: String| {
            Some(RelatedItem {
                kind: item.kind.clone(),
                id: item.id.clone(),
                label,
                repo,
                status,
            })
        };
        let settings = |sk: String| {
            db.get_item()
                .table_name(&cfg.settings_table_name)
                .key("pk", s(team_id))
                .key("sk", s(&sk))
                .send()
        };
        match item.kind.as_str() {
            "run" => {
                let r = db
                    .get_item()
                    .table_name(&cfg.runs_table_name)
                    .key("team_id", s(team_id))
                    .key("run_id", s(id))
                    .send()
                    .await
                    .map_err(dynamo_err)?;
                Ok(r.item()
                    .and_then(|row| found(get(row, "title"), get(row, "repo"), get(row, "status"))))
            }
            "review" => {
                let Some((repo, pr)) = review_repo_pr(id) else {
                    return Ok(None);
                };
                let r = settings(id.to_string()).await.map_err(dynamo_err)?;
                Ok(r.item().and_then(|row| {
                    found(format!("{repo} #{pr}"), repo.clone(), get(row, "verdict"))
                }))
            }
            "release" => {
                let Some((repo, tag)) = id.rsplit_once('@') else {
                    return Ok(None);
                };
                if repo.is_empty() || tag.is_empty() {
                    return Ok(None);
                }
                let r = settings(format!("RELEASE#{repo}#{tag}"))
                    .await
                    .map_err(dynamo_err)?;
                Ok(r.item()
                    .and_then(|_| found(format!("{repo} {tag}"), repo.to_string(), String::new())))
            }
            "plan" => {
                if id.is_empty() || id.len() > 30 || !id.chars().all(|c| c.is_ascii_alphanumeric())
                {
                    return Ok(None);
                }
                let r = db
                    .get_item()
                    .table_name(&cfg.plans_table_name)
                    .key("pk", s(team_id))
                    .key("sk", s(&format!("PLAN#{id}")))
                    .send()
                    .await
                    .map_err(dynamo_err)?;
                Ok(r.item()
                    .and_then(|row| found(get(row, "title"), get(row, "repo"), get(row, "status"))))
            }
            "alert" => {
                if ulid::Ulid::from_string(id).is_err() {
                    return Ok(None);
                }
                let r = settings(format!("ALERT_EVENT#{id}"))
                    .await
                    .map_err(dynamo_err)?;
                Ok(r.item().and_then(|row| {
                    found(get(row, "title"), get(row, "repo"), get(row, "outcome"))
                }))
            }
            "data_source" => {
                let source =
                    common::live::store::get_source(db, &cfg.settings_table_name, team_id, id)
                        .await?;
                Ok(source
                    .and_then(|src| found(src.name.clone(), String::new(), src.status.clone())))
            }
            _ => Ok(None),
        }
    }
}

pub(crate) fn store(state: &AppState) -> DynamoStore<'_> {
    DynamoStore {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
    }
}

fn detail(t: Ticket, thread: Vec<Message>) -> Json<Value> {
    Json(json!({ "ticket": t, "messages": thread }))
}

// ── Handlers: customers ──────────────────────────────────────────────────

/// GET /api/support/tickets — the team's tickets, newest activity first.
pub async fn list(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let caller = Caller::from_claims(&claims);
    let v = list_for_team(&store(&state), &caller)
        .await
        .map_err(Fail::reply)?;
    let v: Vec<Ticket> = v.into_iter().map(customer_ticket).collect();
    Ok(Json(json!({ "tickets": v })))
}

/// POST /api/support/tickets — open a ticket.
pub async fn create_handler(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    headers: HeaderMap,
    Json(req): Json<NewTicket>,
) -> Reply {
    let caller = Caller::from_claims(&claims);
    let name = team_name(&state, &caller.team_id).await;
    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let lookup = DynamoLookup { state: &state };
    let (ticket, first) = create(&store(&state), &lookup, &caller, &req, &name, ua)
        .await
        .map_err(Fail::reply)?;
    info!(team = %caller.team_id, ticket = %ticket.id, "support ticket opened");
    send(&state, mail_new_ticket(&inbox(), &ticket, &first.body)).await;
    Ok(detail(customer_ticket(ticket), customer_view(vec![first])))
}

/// GET /api/support/tickets/:id — one of the team's tickets and its thread.
pub async fn get(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    let caller = Caller::from_claims(&claims);
    let (t, thread) = open_for_team(&store(&state), &caller, &id)
        .await
        .map_err(Fail::reply)?;
    Ok(detail(customer_ticket(t), customer_view(thread)))
}

#[derive(Deserialize)]
pub struct ReplyBody {
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub priority: Option<String>,
    /// Items to attach with the reply (customers only).
    #[serde(default)]
    pub items: Vec<ItemRef>,
}

/// POST /api/support/tickets/:id/reply — the customer adds to the thread.
pub async fn reply(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(req): Json<ReplyBody>,
) -> Reply {
    let caller = Caller::from_claims(&claims);
    let st = store(&state);
    let lookup = DynamoLookup { state: &state };
    let (t, m) = reply_as_customer(&st, &lookup, &caller, &id, &req.body, &req.items)
        .await
        .map_err(Fail::reply)?;
    send(&state, mail_customer_reply(&inbox(), &t, &m)).await;
    let (t, thread) = open_for_team(&st, &caller, &id)
        .await
        .map_err(Fail::reply)?;
    Ok(detail(customer_ticket(t), customer_view(thread)))
}

/// GET /api/support/badge — how many of the team's tickets wait on the team,
/// and for platform admins how many wait on support.
pub async fn badge(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let caller = Caller::from_claims(&claims);
    let st = store(&state);
    let mine = list_for_team(&st, &caller).await.map_err(Fail::reply)?;
    let waiting = mine.iter().filter(|t| t.status == Status::Waiting).count();
    let queue = if caller.platform_admin {
        let all = admin_list(&st, &caller).await.map_err(Fail::reply)?;
        Some(all.iter().filter(|t| t.status == Status::Open).count())
    } else {
        None
    };
    Ok(Json(json!({ "waiting": waiting, "queue_open": queue })))
}

// ── Handlers: platform admins ────────────────────────────────────────────

/// GET /api/admin/support/tickets — every team's tickets.
pub async fn admin_list_handler(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let caller = Caller::from_claims(&claims);
    if !caller.platform_admin {
        warn!(email = %claims.email, "Support queue refused: not a platform admin");
    }
    let v = admin_list(&store(&state), &caller)
        .await
        .map_err(Fail::reply)?;
    // Which teams have support access on, so the queue can offer a view.
    let open: Vec<String> = v
        .iter()
        .filter(|t| t.status != Status::Resolved)
        .map(|t| t.team_id.clone())
        .collect();
    let access = super::support_access::active_until(&state, &open).await;
    Ok(Json(json!({ "tickets": v, "support_access": access })))
}

/// GET /api/admin/support/tickets/:id — any ticket and its full thread.
pub async fn admin_get(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    let caller = Caller::from_claims(&claims);
    let (t, thread) = admin_open(&store(&state), &caller, &id)
        .await
        .map_err(Fail::reply)?;
    admin_detail(&state, t, thread).await
}

/// A ticket for support, with when its team's support access ends (null
/// when it is off).
async fn admin_detail(state: &AppState, t: Ticket, thread: Vec<Message>) -> Reply {
    let access = super::support_access::active_until(state, std::slice::from_ref(&t.team_id)).await;
    let until = access.get(&t.team_id).cloned();
    Ok(Json(
        json!({ "ticket": t, "messages": thread, "support_access": { "until": until } }),
    ))
}

/// POST /api/admin/support/tickets/:id/reply — support answers; `status`
/// "resolved" resolves in the same step. Emails the person who filed.
pub async fn admin_reply_handler(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(req): Json<ReplyBody>,
) -> Reply {
    let caller = Caller::from_claims(&claims);
    let status = req.status.as_deref().and_then(Status::parse);
    let st = store(&state);
    let (t, m) = admin_reply(&st, &caller, &id, &req.body, status)
        .await
        .map_err(Fail::reply)?;
    if let Some(mail) = mail_support_reply(&t, &m) {
        send(&state, mail).await;
    }
    let (t, thread) = admin_open(&st, &caller, &id).await.map_err(Fail::reply)?;
    admin_detail(&state, t, thread).await
}

/// POST /api/admin/support/tickets/:id/status — set status and/or priority.
/// Emails the person who filed when the ticket is resolved.
pub async fn admin_status(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(req): Json<ReplyBody>,
) -> Reply {
    let caller = Caller::from_claims(&claims);
    let status = match req.status.as_deref() {
        None => None,
        Some(v) => Some(Status::parse(v).ok_or_else(|| {
            Fail::BadRequest("Status must be open, waiting or resolved.".into()).reply()
        })?),
    };
    let st = store(&state);
    let (t, resolved_now) = admin_set(&st, &caller, &id, status, req.priority.as_deref())
        .await
        .map_err(Fail::reply)?;
    if resolved_now {
        if let Some(mail) = mail_resolved(&t) {
            send(&state, mail).await;
        }
    }
    let (t, thread) = admin_open(&st, &caller, &id).await.map_err(Fail::reply)?;
    admin_detail(&state, t, thread).await
}

#[cfg(test)]
mod tests {
    #[test]
    fn an_empty_prefix_never_reaches_begins_with() {
        // DynamoDB refuses begins_with(sk, ""): the admin queue reads every
        // item under its key with no prefix.
        assert_eq!(super::key_condition(""), "pk = :pk");
        assert_eq!(
            super::key_condition("TICKET#"),
            "pk = :pk AND begins_with(sk, :p)"
        );
    }

    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// The settings table in memory: (pk, sk) → value.
    #[derive(Default)]
    struct Mem {
        tickets: Mutex<BTreeMap<(String, String), Ticket>>,
        msgs: Mutex<BTreeMap<(String, String), Message>>,
        counters: Mutex<HashMap<(String, String), u32>>,
    }

    impl Store for Mem {
        async fn save(&self, t: &Ticket) -> Result<(), String> {
            let mut m = self.tickets.lock().unwrap();
            m.insert(
                (t.team_id.clone(), format!("{META_PREFIX}{}", t.id)),
                t.clone(),
            );
            m.insert((QUEUE_PK.into(), t.id.clone()), t.clone());
            Ok(())
        }
        async fn load(&self, team_id: &str, id: &str) -> Result<Option<Ticket>, String> {
            Ok(self
                .tickets
                .lock()
                .unwrap()
                .get(&(team_id.to_string(), format!("{META_PREFIX}{id}")))
                .cloned())
        }
        async fn list_team(&self, team_id: &str) -> Result<Vec<Ticket>, String> {
            Ok(self
                .tickets
                .lock()
                .unwrap()
                .iter()
                .filter(|((pk, sk), _)| pk == team_id && sk.starts_with(META_PREFIX))
                .map(|(_, t)| t.clone())
                .collect())
        }
        async fn list_queue(&self) -> Result<Vec<Ticket>, String> {
            Ok(self
                .tickets
                .lock()
                .unwrap()
                .iter()
                .filter(|((pk, _), _)| pk == QUEUE_PK)
                .map(|(_, t)| t.clone())
                .collect())
        }
        async fn queue_entry(&self, id: &str) -> Result<Option<Ticket>, String> {
            Ok(self
                .tickets
                .lock()
                .unwrap()
                .get(&(QUEUE_PK.to_string(), id.to_string()))
                .cloned())
        }
        async fn append(&self, team_id: &str, ticket_id: &str, m: &Message) -> Result<(), String> {
            self.msgs.lock().unwrap().insert(
                (
                    team_id.to_string(),
                    format!("{MSG_PREFIX}{ticket_id}#{}#{}", m.created_at, m.id),
                ),
                m.clone(),
            );
            Ok(())
        }
        async fn thread(&self, team_id: &str, ticket_id: &str) -> Result<Vec<Message>, String> {
            let prefix = format!("{MSG_PREFIX}{ticket_id}#");
            Ok(self
                .msgs
                .lock()
                .unwrap()
                .iter()
                .filter(|((pk, sk), _)| pk == team_id && sk.starts_with(&prefix))
                .map(|(_, m)| m.clone())
                .collect())
        }
        async fn take(
            &self,
            team_id: &str,
            bucket: &str,
            limit: u32,
            _ttl: i64,
        ) -> Result<bool, String> {
            let mut c = self.counters.lock().unwrap();
            let n = c
                .entry((team_id.to_string(), bucket.to_string()))
                .or_insert(0);
            if *n >= limit {
                return Ok(false);
            }
            *n += 1;
            Ok(true)
        }
    }

    fn person(team: &str, user: &str) -> Caller {
        Caller {
            user_id: format!("USER#{user}"),
            team_id: format!("TEAM#{team}"),
            email: format!("{user}@example.com"),
            name: user.to_string(),
            role: "member".into(),
            platform_admin: false,
            in_support_view: false,
        }
    }

    /// Each team's items, by (team, kind, id).
    #[derive(Default)]
    struct Items(Mutex<HashMap<(String, String, String), RelatedItem>>);

    impl Items {
        fn add(&self, team: &str, kind: &str, id: &str, label: &str, status: &str) {
            self.0.lock().unwrap().insert(
                (format!("TEAM#{team}"), kind.into(), id.into()),
                RelatedItem {
                    kind: kind.into(),
                    id: id.into(),
                    label: label.into(),
                    repo: "acme/web".into(),
                    status: status.into(),
                },
            );
        }
    }

    impl ItemLookup for Items {
        async fn find(
            &self,
            team_id: &str,
            item: &RelatedItem,
        ) -> Result<Option<RelatedItem>, String> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .get(&(team_id.to_string(), item.kind.clone(), item.id.clone()))
                .cloned())
        }
    }

    fn none() -> Items {
        Items::default()
    }

    fn admin() -> Caller {
        Caller {
            platform_admin: true,
            ..person("staff", "helper")
        }
    }

    fn ask(subject: &str) -> NewTicket {
        NewTicket {
            subject: subject.into(),
            body: "The run stopped at the test step.".into(),
            priority: None,
            page: Some("/runs/detail/?id=01RUN".into()),
            item: None,
            items: Vec::new(),
        }
    }

    async fn file(store: &Mem, who: &Caller, subject: &str) -> Ticket {
        create(
            store,
            &none(),
            who,
            &ask(subject),
            "acme-web",
            "Test browser",
        )
        .await
        .unwrap()
        .0
    }

    #[tokio::test]
    async fn another_team_can_never_read_list_or_reply_to_a_ticket() {
        let store = Mem::default();
        let alice = person("alpha", "alice");
        let mallory = person("beta", "mallory");
        let t = file(&store, &alice, "Run stuck").await;

        // Not in the other team's list.
        assert!(list_for_team(&store, &mallory).await.unwrap().is_empty());
        // Not readable by id, even knowing the id.
        assert_eq!(
            open_for_team(&store, &mallory, &t.id).await.unwrap_err(),
            Fail::NotFound
        );
        // No reply lands on it.
        assert_eq!(
            reply_as_customer(&store, &none(), &mallory, &t.id, "hello", &[])
                .await
                .unwrap_err(),
            Fail::NotFound
        );
        assert_eq!(store.thread(&alice.team_id, &t.id).await.unwrap().len(), 1);
        // Nor do the admin routes answer to a team owner of the other team.
        let owner = Caller {
            role: "owner".into(),
            ..mallory.clone()
        };
        assert_eq!(
            admin_list(&store, &owner).await.unwrap_err(),
            Fail::Forbidden
        );
        assert_eq!(
            admin_open(&store, &owner, &t.id).await.unwrap_err(),
            Fail::Forbidden
        );
        assert_eq!(
            admin_reply(&store, &owner, &t.id, "x", None)
                .await
                .unwrap_err(),
            Fail::Forbidden
        );
        assert_eq!(
            admin_set(&store, &owner, &t.id, Some(Status::Resolved), None)
                .await
                .unwrap_err(),
            Fail::Forbidden
        );

        // The filer's team still sees it, with every teammate.
        let bob = person("alpha", "bob");
        assert_eq!(list_for_team(&store, &bob).await.unwrap().len(), 1);
        assert!(open_for_team(&store, &bob, &t.id).await.is_ok());
    }

    #[tokio::test]
    async fn a_platform_admin_sees_every_team_and_a_customer_route_still_does_not() {
        let store = Mem::default();
        let a = file(&store, &person("alpha", "alice"), "One").await;
        let b = file(&store, &person("beta", "bea"), "Two").await;
        let staff = admin();
        let all = admin_list(&store, &staff).await.unwrap();
        assert_eq!(all.len(), 2);
        assert!(admin_open(&store, &staff, &a.id).await.is_ok());
        assert!(admin_open(&store, &staff, &b.id).await.is_ok());
        // The customer routes stay keyed to the admin's own team.
        assert_eq!(
            open_for_team(&store, &staff, &a.id).await.unwrap_err(),
            Fail::NotFound
        );
    }

    #[tokio::test]
    async fn turns_flip_between_support_and_the_customer() {
        let store = Mem::default();
        let alice = person("alpha", "alice");
        let staff = admin();
        let t = file(&store, &alice, "Help").await;
        assert_eq!(t.status, Status::Open);

        let (t, _) = admin_reply(&store, &staff, &t.id, "Try again now.", None)
            .await
            .unwrap();
        assert_eq!(t.status, Status::Waiting);
        assert_eq!(t.last_actor, "support");

        let (t, _) = reply_as_customer(&store, &none(), &alice, &t.id, "Still broken.", &[])
            .await
            .unwrap();
        assert_eq!(t.status, Status::Open);

        let (t, _) = admin_reply(&store, &staff, &t.id, "Fixed.", Some(Status::Resolved))
            .await
            .unwrap();
        assert_eq!(t.status, Status::Resolved);

        // A reply to a resolved ticket reopens it, with an event in the thread.
        let (t, _) = reply_as_customer(&store, &none(), &alice, &t.id, "It came back.", &[])
            .await
            .unwrap();
        assert_eq!(t.status, Status::Open);
        let (_, thread) = open_for_team(&store, &alice, &t.id).await.unwrap();
        let events: Vec<_> = thread
            .iter()
            .filter(|m| m.kind == "event")
            .map(|m| m.body.as_str())
            .collect();
        assert_eq!(events, ["resolved", "reopened"]);
        assert_eq!(t.message_count, 5);

        // The queue copy follows the team's copy.
        let q = store.queue_entry(&t.id).await.unwrap().unwrap();
        assert_eq!(q, t);
    }

    #[tokio::test]
    async fn admin_status_records_what_changed() {
        let store = Mem::default();
        let staff = admin();
        let t = file(&store, &person("alpha", "alice"), "Slow").await;
        let (t2, resolved) = admin_set(
            &store,
            &staff,
            &t.id,
            Some(Status::Resolved),
            Some("urgent"),
        )
        .await
        .unwrap();
        assert!(resolved);
        assert_eq!(t2.priority, "urgent");
        let (_, again) = admin_set(&store, &staff, &t.id, Some(Status::Resolved), None)
            .await
            .unwrap();
        assert!(!again, "resolving twice is not news");
        let (t3, _) = admin_set(&store, &staff, &t.id, Some(Status::Waiting), None)
            .await
            .unwrap();
        assert_eq!(t3.status, Status::Waiting);
        let (_, thread) = admin_open(&store, &staff, &t.id).await.unwrap();
        let events: Vec<_> = thread
            .iter()
            .filter(|m| m.kind == "event")
            .map(|m| m.body.clone())
            .collect();
        assert_eq!(events, ["resolved", "priority:urgent", "reopened"]);
        assert_eq!(
            admin_set(&store, &staff, &t.id, None, None)
                .await
                .unwrap_err(),
            Fail::BadRequest("Nothing to change.".into())
        );
    }

    #[tokio::test]
    async fn opening_tickets_is_rate_limited_per_person_and_per_team() {
        let store = Mem::default();
        let alice = person("alpha", "alice");
        for i in 0..USER_TICKETS_PER_HOUR {
            file(&store, &alice, &format!("Ticket {i}")).await;
        }
        let err = create(&store, &none(), &alice, &ask("One more"), "", "")
            .await
            .unwrap_err();
        assert!(matches!(err, Fail::TooMany(_)));

        // Teammates share the team's hourly allowance.
        let mut n = USER_TICKETS_PER_HOUR;
        let mut k = 0;
        while n < TEAM_TICKETS_PER_HOUR {
            let mate = person("alpha", &format!("mate{k}"));
            file(&store, &mate, "Another").await;
            n += 1;
            k += 1;
        }
        let fresh = person("alpha", "newcomer");
        assert!(matches!(
            create(&store, &none(), &fresh, &ask("Late"), "", "")
                .await
                .unwrap_err(),
            Fail::TooMany(_)
        ));
        // Another team is not affected.
        assert!(create(
            &store,
            &none(),
            &person("beta", "bea"),
            &ask("Mine"),
            "",
            ""
        )
        .await
        .is_ok());
    }

    #[tokio::test]
    async fn what_a_ticket_keeps_is_cleaned() {
        let store = Mem::default();
        let alice = person("alpha", "alice");
        let req = NewTicket {
            subject: "  Deploy   fails  ".into(),
            body: "Our key is AKIAABCDEFGHIJKLMNOP and token=abc123secret, ping bob@example.com"
                .into(),
            priority: Some("urgent".into()),
            page: Some("/releases/?repo=acme/web&q=v1#top".into()),
            item: None,
            items: Vec::new(),
        };
        let (t, m) = create(&store, &none(), &alice, &req, "acme", "Browser/1.0")
            .await
            .unwrap();
        assert_eq!(t.subject, "Deploy fails");
        assert_eq!(t.context.page, "/releases/");
        assert_eq!(t.priority, "urgent");
        assert_eq!(t.role, "member");
        assert!(!m.body.contains("AKIA"));
        assert!(!m.body.contains("abc123secret"));
        assert!(m.body.contains("bob@example.com"), "emails are kept");

        assert!(create(
            &store,
            &none(),
            &alice,
            &NewTicket {
                body: "x".into(),
                ..Default::default()
            },
            "",
            ""
        )
        .await
        .is_err());
        let long = NewTicket {
            subject: "s".repeat(MAX_SUBJECT + 1),
            body: "x".into(),
            ..Default::default()
        };
        assert!(matches!(
            create(&store, &none(), &alice, &long, "", "")
                .await
                .unwrap_err(),
            Fail::BadRequest(_)
        ));
    }

    #[test]
    fn pages_and_items_are_plain_ids_or_nothing() {
        assert_eq!(clean_page("/runs/detail/?id=1"), "/runs/detail/");
        assert_eq!(clean_page("https://evil.example/x"), "");
        assert_eq!(clean_page("//evil.example"), "");
        assert_eq!(clean_page("/a b"), "");
        assert_eq!(clean_page(""), "");

        let ok = ItemRef {
            kind: "review".into(),
            id: "REVIEW#acme/web#000042#x".into(),
            label: Some("acme/web #42".into()),
            repo: None,
        };
        assert_eq!(ok.cleaned().unwrap().id, "REVIEW#acme/web#000042#x");
        let bad_kind = ItemRef {
            kind: "file".into(),
            ..ok.clone()
        };
        assert!(bad_kind.cleaned().is_none());
        let bad_id = ItemRef {
            id: "x<script>".into(),
            ..ok.clone()
        };
        assert!(bad_id.cleaned().is_none());
        let secret_label = ItemRef {
            label: Some("token=hunter2".into()),
            ..ok
        };
        assert_eq!(secret_label.cleaned().unwrap().label, "token=[hidden]");
    }

    #[test]
    fn scrub_hides_common_secrets() {
        let text = "ghp_abcdefghijklmnopqrstuvwxyz0123 xoxb-1234567890-abcdef sk-ant-abcdefghijklmnopqrstuv \
                    -----BEGIN RSA PRIVATE KEY-----\nMIIabc\n-----END RSA PRIVATE KEY----- Authorization: Bearer abcdefghijk";
        let out = scrub(text);
        for gone in ["ghp_", "xoxb-", "sk-ant-", "MIIabc", "abcdefghijk"] {
            assert!(!out.contains(gone), "{gone} survived: {out}");
        }
    }

    #[test]
    fn customers_do_not_see_support_addresses_or_the_browser_string() {
        let thread = vec![Message {
            id: "1".into(),
            kind: "message".into(),
            author: "support".into(),
            author_email: "helper@example.com".into(),
            author_name: "helper".into(),
            body: "Hi".into(),
            created_at: "t".into(),
            attachments: Vec::new(),
        }];
        let v = customer_view(thread);
        assert_eq!(v[0].author_email, "");
        assert_eq!(v[0].author_name, "Coderhelm support");
    }

    #[tokio::test]
    async fn emails_go_both_ways_and_escape_what_people_typed() {
        let store = Mem::default();
        let alice = person("alpha", "alice");
        let mut req = ask("<b>Broken</b>");
        req.item = Some(ItemRef {
            kind: "run".into(),
            id: "01RUN".into(),
            label: Some("Fix login".into()),
            repo: Some("acme/web".into()),
        });
        let items = Items::default();
        items.add("alpha", "run", "01RUN", "Fix login", "failed");
        let (t, first) = create(&store, &items, &alice, &req, "acme", "")
            .await
            .unwrap();

        let to_support = mail_new_ticket("help@example.com", &t, &first.body);
        assert_eq!(to_support.to, "help@example.com");
        assert!(to_support.subject.contains("acme"));
        assert!(to_support.html.contains("&lt;b&gt;Broken&lt;/b&gt;"));
        assert!(!to_support.html.contains("<b>Broken"));
        assert!(to_support
            .text
            .contains("Attached: Run: Fix login (acme/web) · failed"));
        assert!(to_support
            .text
            .contains(&format!("/admin/tickets/?id={}", t.id)));

        let (t, m) = admin_reply(&store, &admin(), &t.id, "Fixed it", None)
            .await
            .unwrap();
        let to_customer = mail_support_reply(&t, &m).unwrap();
        assert_eq!(to_customer.to, "alice@example.com");
        assert!(to_customer
            .text
            .contains(&format!("/support/ticket/?id={}", t.id)));
        assert!(
            !to_customer.text.contains("helper@example.com"),
            "support staff stay anonymous"
        );

        let (t, m) = reply_as_customer(&store, &none(), &alice, &t.id, "Thanks", &[])
            .await
            .unwrap();
        assert_eq!(
            mail_customer_reply("help@example.com", &t, &m).to,
            "help@example.com"
        );
        assert_eq!(mail_resolved(&t).unwrap().to, "alice@example.com");
    }

    #[tokio::test]
    async fn attachments_must_be_the_callers_teams_and_keep_only_their_names() {
        let store = Mem::default();
        let items = Items::default();
        items.add("alpha", "run", "01RUN", "Fix login", "failed");
        items.add(
            "alpha",
            "alert",
            "01J0000000000000000000000A",
            "CPU high",
            "opened",
        );
        items.add("beta", "plan", "PLANB", "Their plan", "draft");
        let alice = person("alpha", "alice");
        let mut req = ask("Run broke");
        req.items = vec![
            ItemRef {
                kind: "run".into(),
                id: "01RUN".into(),
                // What the browser says is not what is kept.
                label: Some("made up".into()),
                repo: None,
            },
            ItemRef {
                kind: "alert".into(),
                id: "01J0000000000000000000000A".into(),
                label: None,
                repo: None,
            },
            // The same run twice is kept once.
            ItemRef {
                kind: "run".into(),
                id: "01RUN".into(),
                label: None,
                repo: None,
            },
        ];
        let (t, first) = create(&store, &items, &alice, &req, "acme", "")
            .await
            .unwrap();
        assert_eq!(t.context.items.len(), 2);
        assert_eq!(t.context.items[0].label, "Fix login");
        assert_eq!(t.context.items[0].status, "failed");
        assert_eq!(t.context.item.as_ref().unwrap().id, "01RUN");
        assert_eq!(first.attachments.len(), 2);

        // Another team's plan is refused, and nothing is saved or counted.
        let mut theirs = ask("Their plan");
        theirs.items = vec![ItemRef {
            kind: "plan".into(),
            id: "PLANB".into(),
            label: Some("Their plan".into()),
            repo: None,
        }];
        let before = store.list_team(&alice.team_id).await.unwrap().len();
        let err = create(&store, &items, &alice, &theirs, "acme", "")
            .await
            .unwrap_err();
        assert_eq!(
            err,
            Fail::BadRequest("That plan is not one of your team's.".into())
        );
        assert_eq!(store.list_team(&alice.team_id).await.unwrap().len(), before);

        // A reply can attach more of the team's own items, not another team's.
        items.add("alpha", "data_source", "src1", "Orders API", "ready");
        let (t2, m) = reply_as_customer(
            &store,
            &items,
            &alice,
            &t.id,
            "",
            &[ItemRef {
                kind: "data_source".into(),
                id: "src1".into(),
                label: None,
                repo: None,
            }],
        )
        .await
        .unwrap();
        assert_eq!(m.body, "");
        assert_eq!(m.attachments[0].label, "Orders API");
        assert_eq!(attachments(&t2).len(), 3);
        let err = reply_as_customer(
            &store,
            &items,
            &alice,
            &t.id,
            "and this",
            &[ItemRef {
                kind: "plan".into(),
                id: "PLANB".into(),
                label: None,
                repo: None,
            }],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Fail::BadRequest(_)));
        // Kinds tickets do not carry are refused.
        let err = reply_as_customer(
            &store,
            &items,
            &alice,
            &t.id,
            "x",
            &[ItemRef {
                kind: "secret".into(),
                id: "k".into(),
                label: None,
                repo: None,
            }],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Fail::BadRequest(_)));
    }

    #[tokio::test]
    async fn a_ticket_carries_at_most_ten_items() {
        let store = Mem::default();
        let items = Items::default();
        for i in 0..12 {
            items.add("alpha", "run", &format!("R{i}"), "run", "done");
        }
        let alice = person("alpha", "alice");
        let refs = |r: std::ops::Range<usize>| -> Vec<ItemRef> {
            r.map(|i| ItemRef {
                kind: "run".into(),
                id: format!("R{i}"),
                label: None,
                repo: None,
            })
            .collect()
        };
        let mut req = ask("Many");
        req.items = refs(0..11);
        assert!(matches!(
            create(&store, &items, &alice, &req, "", "")
                .await
                .unwrap_err(),
            Fail::BadRequest(_)
        ));
        req.items = refs(0..9);
        let (t, _) = create(&store, &items, &alice, &req, "", "").await.unwrap();
        let (t, _) = reply_as_customer(&store, &items, &alice, &t.id, "one more", &refs(9..10))
            .await
            .unwrap();
        assert_eq!(attachments(&t).len(), MAX_ATTACHMENTS);
        let err = reply_as_customer(&store, &items, &alice, &t.id, "too many", &refs(10..11))
            .await
            .unwrap_err();
        assert!(matches!(err, Fail::BadRequest(_)));
    }

    #[tokio::test]
    async fn ticket_notes_land_only_on_the_teams_own_ticket() {
        let store = Mem::default();
        let alice = person("alpha", "alice");
        let t = file(&store, &alice, "Look please").await;
        note_on_ticket(
            &store,
            &alice.team_id,
            &t.id,
            "support",
            &admin(),
            "support_viewed",
        )
        .await
        .unwrap();
        let (_, thread) = open_for_team(&store, &alice, &t.id).await.unwrap();
        assert_eq!(thread.last().unwrap().body, "support_viewed");
        assert_eq!(
            note_on_ticket(&store, "TEAM#beta", &t.id, "support", &admin(), "x")
                .await
                .unwrap_err(),
            Fail::NotFound
        );
    }
}
