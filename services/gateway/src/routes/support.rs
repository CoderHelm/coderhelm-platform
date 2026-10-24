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
//!   often a team can open tickets, reply and add people. Expires through `ttl`.
//!
//! People on a ticket: the person who filed it, plus up to
//! [`MAX_PARTICIPANTS`] others kept on the ticket record (and so on its queue
//! copy): members of the same team, or outside addresses copied by email.
//! They can be named when the ticket is opened or added later; both go
//! through the same team check and land as `added:` events in the thread.
//! Everyone on it is emailed when support replies or the status changes.
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
/// People on one ticket besides the person who filed it.
pub const MAX_PARTICIPANTS: usize = 20;
/// People a team may add to its tickets per hour, over all its tickets.
pub const TEAM_PEOPLE_ADDS_PER_HOUR: u32 = 30;

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
    /// Who else is on the ticket and gets its emails, besides the filer.
    #[serde(default)]
    pub participants: Vec<Participant>,
    /// The latest message, one line, for the lists.
    #[serde(default)]
    pub last_snippet: String,
    /// Who wrote the latest message.
    #[serde(default)]
    pub last_author_name: String,
}

/// Someone on a ticket besides the person who filed it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Participant {
    /// The member's user id (`USER#…`); empty for an outside address.
    #[serde(default)]
    pub user_id: String,
    #[serde(default)]
    pub email: String,
    /// Their name or GitHub login, when they are a member.
    #[serde(default)]
    pub name: String,
    /// Copied by email only: not on the team, cannot sign in.
    #[serde(default)]
    pub outside: bool,
    /// Who added them (a name or address), and when.
    #[serde(default)]
    pub added_by: String,
    #[serde(default)]
    pub added_at: String,
}

impl Participant {
    /// What the thread and emails call them.
    pub fn label(&self) -> &str {
        if self.name.is_empty() {
            &self.email
        } else {
            &self.name
        }
    }

    fn is(&self, other: &Participant) -> bool {
        if !self.user_id.is_empty() && !other.user_id.is_empty() {
            return same_user(&self.user_id, &other.user_id);
        }
        !self.email.is_empty() && self.email.eq_ignore_ascii_case(&other.email)
    }
}

/// A user id with or without its `USER#` prefix.
fn bare_user(id: &str) -> &str {
    id.strip_prefix("USER#").unwrap_or(id)
}

fn same_user(a: &str, b: &str) -> bool {
    !bare_user(a).is_empty() && bare_user(a) == bare_user(b)
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

/// An email address, trimmed and lowercased, or `None` when it is not one:
/// one `@`, something before it, a dotted domain, no spaces or brackets.
pub fn clean_email(raw: &str) -> Option<String> {
    let email = raw.trim().to_lowercase();
    let (local, domain) = email.split_once('@')?;
    let ok = !local.is_empty()
        && local.len() <= 64
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && !domain.contains("..")
        && !domain.contains('@')
        && email.len() <= 254
        && email.chars().all(|c| {
            c.is_ascii_graphic()
                && !matches!(
                    c,
                    '<' | '>' | '(' | ')' | ',' | ';' | ':' | '"' | '[' | ']' | '\\'
                )
        });
    ok.then_some(email)
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
    /// People to put on the ticket as it opens: members by id, or addresses.
    /// Checked the same way as adding them later ([`add_participant`]).
    #[serde(default)]
    pub people: Vec<PersonRef>,
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

// ── People ───────────────────────────────────────────────────────────────

/// Someone on a team, as the team's member list has them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Member {
    /// `USER#…`
    pub user_id: String,
    pub email: String,
    /// Their name, else their GitHub login.
    pub name: String,
    pub avatar_url: String,
}

/// A team's active members (not pending invites). A trait so the team
/// checks can be tested without DynamoDB.
pub trait Members {
    async fn members(&self, team_id: &str) -> Result<Vec<Member>, String>;
}

/// Who to add or remove: a member by id, or an address.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct PersonRef {
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
}

impl PersonRef {
    fn user_id(&self) -> Option<&str> {
        self.user_id
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
    }
    fn email(&self) -> Option<&str> {
        self.email
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
    }
}

/// Which routes are asking: the team's own (keyed by the caller's team), or
/// a platform admin's support routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Team,
    Support,
}

impl Side {
    fn author(self) -> &'static str {
        match self {
            Side::Team => "customer",
            Side::Support => "support",
        }
    }
}

/// The filer and the team's admins and owners manage who is on a ticket;
/// platform admins do through the support routes. Anyone else on the team
/// can only add or remove themselves.
pub fn can_manage_people(caller: &Caller, side: Side, t: &Ticket) -> bool {
    match side {
        Side::Support => caller.platform_admin,
        Side::Team => {
            same_user(&caller.user_id, &t.opened_by_id)
                || crate::permissions::role_level(&caller.role) >= 3
        }
    }
}

/// The person a request names, checked against the ticket's team: a member
/// id must be one of that team's members (never another team's), and an
/// address must be valid. An address that belongs to a member is that
/// member; any other address is copied as an outsider.
async fn person_for<M: Members>(
    members: &M,
    team_id: &str,
    who: &PersonRef,
) -> Result<Participant, Fail> {
    let list = members.members(team_id).await.map_err(Fail::Internal)?;
    let member = |m: &Member| Participant {
        user_id: m.user_id.clone(),
        email: m.email.to_lowercase(),
        name: clip(&one_line(&m.name), 120),
        outside: false,
        added_by: String::new(),
        added_at: String::new(),
    };
    if let Some(id) = who.user_id() {
        return list
            .iter()
            .find(|m| same_user(&m.user_id, id))
            .map(member)
            .ok_or_else(|| Fail::BadRequest("That person is not on this team.".into()));
    }
    let Some(raw) = who.email() else {
        return Err(Fail::BadRequest(
            "Choose someone on the team, or enter an email address.".into(),
        ));
    };
    let email =
        clean_email(raw).ok_or_else(|| Fail::BadRequest("Enter a valid email address.".into()))?;
    if let Some(m) = list.iter().find(|m| m.email.eq_ignore_ascii_case(&email)) {
        return Ok(member(m));
    }
    Ok(Participant {
        user_id: String::new(),
        email,
        name: String::new(),
        outside: true,
        added_by: String::new(),
        added_at: String::new(),
    })
}

/// Loads a ticket the way `side` may: the caller's team's own, or any
/// ticket for a platform admin.
async fn ticket_for<S: Store>(
    store: &S,
    caller: &Caller,
    side: Side,
    id: &str,
) -> Result<Ticket, Fail> {
    match side {
        Side::Team => open_for_team(store, caller, id).await.map(|(t, _)| t),
        Side::Support => admin_open(store, caller, id).await.map(|(t, _)| t),
    }
}

/// The people a new ticket names, checked like [`add_participant`] checks
/// one: members of the caller's own team by id (never another team's), or
/// valid addresses, an address that belongs to a member being that member.
/// The filer is on every ticket already, so naming them is skipped, and so
/// is naming someone twice. Nothing is written; any refusal names the
/// person and refuses the whole ticket.
pub async fn people_for_new<M: Members>(
    members: &M,
    caller: &Caller,
    refs: &[PersonRef],
) -> Result<Vec<Participant>, Fail> {
    if refs.is_empty() {
        return Ok(Vec::new());
    }
    if refs.len() > MAX_PARTICIPANTS * 2 {
        return Err(Fail::BadRequest(format!(
            "A ticket can have at most {MAX_PARTICIPANTS} people besides the person who filed it."
        )));
    }
    let mut out: Vec<Participant> = Vec::new();
    for who in refs {
        let p = person_for(members, &caller.team_id, who).await?;
        let filer = (!p.user_id.is_empty() && same_user(&p.user_id, &caller.user_id))
            || (!caller.email.is_empty() && p.email.eq_ignore_ascii_case(&caller.email));
        if filer || out.iter().any(|q| q.is(&p)) {
            continue;
        }
        out.push(p);
    }
    if out.len() > MAX_PARTICIPANTS {
        return Err(Fail::BadRequest(format!(
            "A ticket can have at most {MAX_PARTICIPANTS} people besides the person who filed it."
        )));
    }
    Ok(out)
}

fn caller_label(caller: &Caller) -> String {
    if caller.name.is_empty() {
        caller.email.clone()
    } else {
        caller.name.clone()
    }
}

/// Adds someone to a ticket so they get its emails. A member may add
/// themselves ("Follow"); adding anyone else takes [`can_manage_people`].
/// At most [`MAX_PARTICIPANTS`], and additions are rate limited per team.
pub async fn add_participant<S: Store, M: Members>(
    store: &S,
    members: &M,
    caller: &Caller,
    side: Side,
    id: &str,
    who: &PersonRef,
) -> Result<Ticket, Fail> {
    let mut t = ticket_for(store, caller, side, id).await?;
    let mut p = person_for(members, &t.team_id, who).await?;
    let me = side == Side::Team && !p.outside && same_user(&p.user_id, &caller.user_id);
    if !me && !can_manage_people(caller, side, &t) {
        return Err(Fail::Denied(
            "Only the person who filed this ticket or a team admin can add people. You can follow it yourself.".into(),
        ));
    }
    let filer = (!p.user_id.is_empty() && same_user(&p.user_id, &t.opened_by_id))
        || (!t.opened_by_email.is_empty() && p.email.eq_ignore_ascii_case(&t.opened_by_email));
    if filer {
        return Err(Fail::BadRequest(format!(
            "{} filed this ticket and already gets its emails.",
            p.label()
        )));
    }
    if t.participants.iter().any(|q| q.is(&p)) {
        return Err(Fail::BadRequest(format!(
            "{} is already on this ticket.",
            p.label()
        )));
    }
    if t.participants.len() >= MAX_PARTICIPANTS {
        return Err(Fail::BadRequest(format!(
            "A ticket can have at most {MAX_PARTICIPANTS} people besides the person who filed it."
        )));
    }
    let (hour, ttl) = hour_bucket(chrono::Utc::now());
    if !store
        .take(
            &t.team_id,
            &format!("people:{hour}"),
            TEAM_PEOPLE_ADDS_PER_HOUR,
            ttl,
        )
        .await
        .map_err(Fail::Internal)?
    {
        return Err(Fail::TooMany(
            "A lot of people were added to your team's tickets in the last hour. Try again later."
                .into(),
        ));
    }
    let now = now_iso();
    p.added_by = caller_label(caller);
    p.added_at = now.clone();
    let action = if me {
        "followed".to_string()
    } else {
        format!("added:{}", p.label())
    };
    store
        .append(
            &t.team_id,
            &t.id,
            &event(side.author(), caller, &action, &now),
        )
        .await
        .map_err(Fail::Internal)?;
    t.participants.push(p);
    t.updated_at = now;
    store.save(&t).await.map_err(Fail::Internal)?;
    Ok(t)
}

/// Takes someone off a ticket. Anyone may take themselves off ("Unfollow");
/// taking off anyone else takes [`can_manage_people`].
pub async fn remove_participant<S: Store>(
    store: &S,
    caller: &Caller,
    side: Side,
    id: &str,
    who: &PersonRef,
) -> Result<Ticket, Fail> {
    let mut t = ticket_for(store, caller, side, id).await?;
    let email = who.email().map(str::to_lowercase);
    let at = t
        .participants
        .iter()
        .position(|q| match (who.user_id(), &email) {
            (Some(uid), _) => same_user(&q.user_id, uid),
            (None, Some(e)) => q.email.eq_ignore_ascii_case(e),
            _ => false,
        })
        .ok_or_else(|| Fail::BadRequest("They are not on this ticket.".into()))?;
    let p = t.participants[at].clone();
    let me = side == Side::Team && !p.outside && same_user(&p.user_id, &caller.user_id);
    if !me && !can_manage_people(caller, side, &t) {
        return Err(Fail::Denied(
            "Only the person who filed this ticket or a team admin can remove people.".into(),
        ));
    }
    let now = now_iso();
    let action = if me {
        "left".to_string()
    } else {
        format!("removed:{}", p.label())
    };
    store
        .append(
            &t.team_id,
            &t.id,
            &event(side.author(), caller, &action, &now),
        )
        .await
        .map_err(Fail::Internal)?;
    t.participants.remove(at);
    t.updated_at = now;
    store.save(&t).await.map_err(Fail::Internal)?;
    Ok(t)
}

/// The filer or a team admin marks their ticket resolved.
pub async fn resolve_as_customer<S: Store>(
    store: &S,
    caller: &Caller,
    id: &str,
) -> Result<Ticket, Fail> {
    let (mut t, _) = open_for_team(store, caller, id).await?;
    if !can_manage_people(caller, Side::Team, &t) {
        return Err(Fail::Denied(
            "Only the person who filed this ticket or a team admin can mark it resolved.".into(),
        ));
    }
    if t.status == Status::Resolved {
        return Err(Fail::BadRequest("This ticket is already resolved.".into()));
    }
    let now = now_iso();
    store
        .append(
            &t.team_id,
            &t.id,
            &event("customer", caller, "resolved", &now),
        )
        .await
        .map_err(Fail::Internal)?;
    t.status = Status::Resolved;
    t.updated_at = now;
    store.save(&t).await.map_err(Fail::Internal)?;
    Ok(t)
}

/// Names a thread lacks, filled in from the team's member list by address,
/// so a message never shows a bare address for someone with a name. The
/// filer's and members' names on the ticket are refreshed the same way.
pub fn fill_names(t: &mut Ticket, thread: &mut [Message], members: &[Member]) {
    let name_of = |email: &str| {
        members
            .iter()
            .find(|m| !email.is_empty() && m.email.eq_ignore_ascii_case(email))
            .map(|m| m.name.clone())
            .filter(|n| !n.is_empty())
    };
    for m in thread.iter_mut() {
        if m.author == "customer" && m.author_name.is_empty() {
            if let Some(n) = name_of(&m.author_email) {
                m.author_name = n;
            }
        }
    }
    if t.opened_by_name.is_empty() {
        if let Some(n) = name_of(&t.opened_by_email) {
            t.opened_by_name = n;
        }
    }
    for p in t.participants.iter_mut().filter(|p| !p.outside) {
        if let Some(n) = members
            .iter()
            .find(|m| same_user(&m.user_id, &p.user_id))
            .map(|m| m.name.clone())
            .filter(|n| !n.is_empty())
        {
            p.name = n;
        }
    }
}

/// Avatars of the people on a ticket and in its thread, by lowercased address.
pub fn avatars(t: &Ticket, thread: &[Message], members: &[Member]) -> HashMap<String, String> {
    let mut wanted: Vec<String> = thread
        .iter()
        .map(|m| m.author_email.to_lowercase())
        .chain(std::iter::once(t.opened_by_email.to_lowercase()))
        .chain(t.participants.iter().map(|p| p.email.to_lowercase()))
        .filter(|e| !e.is_empty())
        .collect();
    wanted.sort();
    wanted.dedup();
    members
        .iter()
        .filter(|m| !m.avatar_url.is_empty() && wanted.contains(&m.email.to_lowercase()))
        .map(|m| (m.email.to_lowercase(), m.avatar_url.clone()))
        .collect()
}

/// The latest message, kept on the ticket for the lists.
fn note_latest(t: &mut Ticket, m: &Message) {
    t.last_snippet = clip(&one_line(&m.body), 160);
    t.last_author_name = if m.author_name.is_empty() {
        m.author_email.clone()
    } else {
        m.author_name.clone()
    };
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
/// items attached that the team's records confirm and the people it names
/// ([`people_for_new`]) already on it. Each person put on it counts toward
/// the team's hourly additions and is an `added:` event in the thread, as
/// if they were added just after it opened.
pub async fn create<S: Store, L: ItemLookup, M: Members>(
    store: &S,
    lookup: &L,
    members: &M,
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
    let mut people = people_for_new(members, caller, &req.people).await?;

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
    for _ in &people {
        if !store
            .take(
                &caller.team_id,
                &format!("people:{hour}"),
                TEAM_PEOPLE_ADDS_PER_HOUR,
                ttl,
            )
            .await
            .map_err(Fail::Internal)?
        {
            return Err(Fail::TooMany(
                "A lot of people were added to your team's tickets in the last hour. Send the ticket without them, or try again later."
                    .into(),
            ));
        }
    }
    let now = now_iso();
    let by = caller_label(caller);
    for p in people.iter_mut() {
        p.added_by = by.clone();
        p.added_at = now.clone();
    }
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
        participants: people,
        last_snippet: String::new(),
        last_author_name: String::new(),
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
    let mut ticket = ticket;
    note_latest(&mut ticket, &first);
    store.save(&ticket).await.map_err(Fail::Internal)?;
    store
        .append(&ticket.team_id, &ticket.id, &first)
        .await
        .map_err(Fail::Internal)?;
    // After the first message, so the thread reads: asked, then added.
    for p in &ticket.participants {
        store
            .append(
                &ticket.team_id,
                &ticket.id,
                &event(
                    "customer",
                    caller,
                    &format!("added:{}", p.label()),
                    &first.created_at,
                ),
            )
            .await
            .map_err(Fail::Internal)?;
    }
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
    note_latest(&mut ticket, &m);
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
    note_latest(&mut ticket, &m);
    store.save(&ticket).await.map_err(Fail::Internal)?;
    Ok((ticket, m))
}

/// Support changes a ticket's status or priority. Returns the ticket and,
/// when its status changed, the event recorded ("resolved", "reopened",
/// "status:waiting"), for the emails.
pub async fn admin_set<S: Store>(
    store: &S,
    caller: &Caller,
    id: &str,
    status: Option<Status>,
    new_priority: Option<&str>,
) -> Result<(Ticket, Option<String>), Fail> {
    require_admin(caller)?;
    if status.is_none() && new_priority.is_none() {
        return Err(Fail::BadRequest("Nothing to change.".into()));
    }
    let (mut ticket, _) = admin_open(store, caller, id).await?;
    let now = now_iso();
    let mut changed = false;
    let mut status_change = None;
    if let Some(next) = status {
        if next != ticket.status {
            let action = match (ticket.status, next) {
                (_, Status::Resolved) => "resolved".to_string(),
                (Status::Resolved, _) => "reopened".to_string(),
                (_, s) => format!("status:{}", s.as_str()),
            };
            status_change = Some(action.clone());
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
    Ok((ticket, status_change))
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
    if t.last_actor == "support" && !t.last_snippet.is_empty() {
        t.last_author_name = "Coderhelm support".into();
    }
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
    button: Option<(&str, &str)>,
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
{}\
<p style=\"font-size:12px;color:#71717a;margin:24px 0 0\">Coderhelm support</p>\
</div></body></html>",
        esc(heading),
        esc(quote),
        button.map(|(label, href)| format!(
            "<a href=\"{}\" style=\"display:inline-block;background:#ffffff;color:#09090b;text-decoration:none;font-size:14px;font-weight:600;padding:8px 16px;border-radius:8px\">{}</a>",
            esc(href),
            esc(label)
        )).unwrap_or_default(),
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
    if !t.participants.is_empty() {
        lines.push(("People".to_string(), people_words(t)));
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
            Some(("Open the ticket", &link)),
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
    if !t.participants.is_empty() {
        lines.push(("People".to_string(), people_words(t)));
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
            Some(("Open the ticket", &link)),
        ),
    }
}

/// Someone a ticket's emails go to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recipient {
    pub email: String,
    /// On the team (can sign in and answer), or an outside address.
    pub member: bool,
}

/// Everyone a ticket's emails go to: the filer, then each person on it, once
/// each, and only them.
pub fn recipients(t: &Ticket) -> Vec<Recipient> {
    let mut out: Vec<Recipient> = Vec::new();
    let people = std::iter::once((t.opened_by_email.as_str(), true)).chain(
        t.participants
            .iter()
            .map(|p| (p.email.as_str(), !p.outside)),
    );
    for (email, member) in people {
        let email = email.trim().to_lowercase();
        if email.is_empty() || out.iter().any(|r| r.email == email) {
            continue;
        }
        out.push(Recipient { email, member });
    }
    out
}

/// "Alice (filed), Bob, carol@example.org (copied)"
pub fn people_words(t: &Ticket) -> String {
    let filer = if t.opened_by_name.is_empty() {
        t.opened_by_email.clone()
    } else {
        t.opened_by_name.clone()
    };
    std::iter::once(format!("{filer} (filed)"))
        .chain(t.participants.iter().map(|p| {
            if p.outside {
                format!("{} (copied)", p.email)
            } else {
                p.label().to_string()
            }
        }))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The line under each email: members answer in Coderhelm; an outside
/// address cannot sign in, so it is told how to get an answer to support.
/// Replies by email are not read (there is no inbound mail), and each email
/// says so.
fn footer(r: &Recipient) -> &'static str {
    if r.member {
        "Reply by visiting the ticket in Coderhelm. Replies to this email are not read."
    } else {
        "You are copied on this ticket. This address cannot sign in to Coderhelm and replies to this email are not read; ask someone on the team to answer in Coderhelm."
    }
}

fn mail_to_people(t: &Ticket, subject: &str, heading: &str, quote: &str) -> Vec<Mail> {
    let link = customer_link(&t.id);
    let people = people_words(t);
    recipients(t)
        .into_iter()
        .map(|r| {
            let note = footer(&r);
            let mut text =
                format!("{heading}\n\n{quote}\n\nPeople on this ticket: {people}\n\n{note}\n");
            if r.member {
                text.push_str(&format!("{link}\n"));
            }
            Mail {
                to: r.email.clone(),
                subject: subject.to_string(),
                html: html_page(
                    heading,
                    &[
                        ("People".to_string(), people.clone()),
                        ("".to_string(), note.to_string()),
                    ],
                    quote,
                    r.member.then_some(("See the ticket", link.as_str())),
                ),
                text,
            }
        })
        .collect()
}

/// To everyone on the ticket: support answered.
pub fn mail_support_reply(t: &Ticket, m: &Message) -> Vec<Mail> {
    let heading = if t.status == Status::Resolved {
        format!("Resolved: {}", t.subject)
    } else {
        format!("Coderhelm support replied: {}", t.subject)
    };
    mail_to_people(t, &format!("Re: {}", t.subject), &heading, &m.body)
}

/// To everyone on the ticket: its status changed. `action` is the event
/// ("resolved", "reopened", "status:waiting", "status:open"), `by` who did it.
pub fn mail_status(t: &Ticket, action: &str, by: &str) -> Vec<Mail> {
    let (heading, note) = match action {
        "resolved" => (
            format!("Resolved: {}", t.subject),
            format!("{by} marked this ticket resolved. Reply in Coderhelm if it is not, and it opens again."),
        ),
        "reopened" => (
            format!("Reopened: {}", t.subject),
            format!("{by} opened this ticket again."),
        ),
        "status:waiting" => (
            format!("Waiting on your team: {}", t.subject),
            format!("{by} is waiting for an answer from your team."),
        ),
        _ => (
            format!("With Coderhelm support: {}", t.subject),
            format!("{by} marked this ticket open; it is support's turn."),
        ),
    };
    mail_to_people(t, &heading, &heading, &note)
}

/// To the support inbox: the team marked its ticket resolved.
pub fn mail_team_resolved(inbox: &str, t: &Ticket, by: &str) -> Mail {
    let team = if t.team_name.is_empty() {
        &t.team_id
    } else {
        &t.team_name
    };
    let link = admin_link(&t.id);
    let note = format!("{by} marked this ticket resolved.");
    Mail {
        to: inbox.to_string(),
        subject: format!("[Support] Resolved by {team}: {}", t.subject),
        text: format!("{}\n\n{note}\n\n{link}\n", t.subject),
        html: html_page(
            &format!("Resolved by the team: {}", t.subject),
            &[("Team".to_string(), team.to_string())],
            &note,
            Some(("Open the ticket", &link)),
        ),
    }
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

/// A team's members from the users table (pending invites left out).
pub struct DynamoMembers<'a> {
    pub state: &'a AppState,
}

impl Members for DynamoMembers<'_> {
    async fn members(&self, team_id: &str) -> Result<Vec<Member>, String> {
        let mut out = Vec::new();
        let mut start = None;
        loop {
            let page = self
                .state
                .dynamo
                .query()
                .table_name(&self.state.config.users_table_name)
                .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
                .expression_attribute_values(":pk", s(team_id))
                .expression_attribute_values(":p", s("USER#"))
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(dynamo_err)?;
            for row in page.items() {
                let get = |k: &str| {
                    row.get(k)
                        .and_then(|v| v.as_s().ok())
                        .cloned()
                        .unwrap_or_default()
                };
                if get("status") == "invited" {
                    continue;
                }
                let name = get("name");
                out.push(Member {
                    user_id: get("sk"),
                    email: get("email").to_lowercase(),
                    name: if name.is_empty() {
                        get("github_login")
                    } else {
                        name
                    },
                    avatar_url: get("avatar_url"),
                });
            }
            start = page.last_evaluated_key().cloned();
            if start.is_none() || out.len() >= 2000 {
                break;
            }
        }
        Ok(out)
    }
}

/// The team's members, or none when they cannot be read (names then fall
/// back to addresses; nothing else depends on them).
async fn team_members(state: &AppState, team_id: &str) -> Vec<Member> {
    DynamoMembers { state }
        .members(team_id)
        .await
        .unwrap_or_else(|e| {
            warn!(error = %e, "support: team members not read");
            Vec::new()
        })
}

/// The caller, named from the team's member list when the session carries
/// no GitHub login.
fn named(mut caller: Caller, members: &[Member]) -> Caller {
    if let Some(m) = members
        .iter()
        .find(|m| same_user(&m.user_id, &caller.user_id))
    {
        if !m.name.is_empty() {
            caller.name = clip(&one_line(&m.name), 120);
        }
    }
    caller
}

/// A ticket for the team: support's people anonymous, names filled in.
async fn team_detail(state: &AppState, t: Ticket, thread: Vec<Message>) -> Reply {
    let members = team_members(state, &t.team_id).await;
    let mut t = customer_ticket(t);
    let mut thread = customer_view(thread);
    fill_names(&mut t, &mut thread, &members);
    let avatars = avatars(&t, &thread, &members);
    Ok(Json(
        json!({ "ticket": t, "messages": thread, "avatars": avatars }),
    ))
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
    let members = team_members(&state, &claims.team_id).await;
    let caller = named(Caller::from_claims(&claims), &members);
    let name = team_name(&state, &caller.team_id).await;
    let ua = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let lookup = DynamoLookup { state: &state };
    let roster = DynamoMembers { state: &state };
    let st = store(&state);
    let (ticket, first) = create(&st, &lookup, &roster, &caller, &req, &name, ua)
        .await
        .map_err(Fail::reply)?;
    info!(team = %caller.team_id, ticket = %ticket.id, people = ticket.participants.len(), "support ticket opened");
    send(&state, mail_new_ticket(&inbox(), &ticket, &first.body)).await;
    let (ticket, thread) = open_for_team(&st, &caller, &ticket.id)
        .await
        .map_err(Fail::reply)?;
    team_detail(&state, ticket, thread).await
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
    team_detail(&state, t, thread).await
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
    let members = team_members(&state, &claims.team_id).await;
    let caller = named(Caller::from_claims(&claims), &members);
    let st = store(&state);
    let lookup = DynamoLookup { state: &state };
    let (t, m) = reply_as_customer(&st, &lookup, &caller, &id, &req.body, &req.items)
        .await
        .map_err(Fail::reply)?;
    send(&state, mail_customer_reply(&inbox(), &t, &m)).await;
    let (t, thread) = open_for_team(&st, &caller, &id)
        .await
        .map_err(Fail::reply)?;
    team_detail(&state, t, thread).await
}

/// POST /api/support/tickets/:id/participants — add a member (`user_id`) or
/// an outside address (`email`). Adding yourself is following.
pub async fn add_people(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(who): Json<PersonRef>,
) -> Reply {
    people_change(&state, &claims, &id, &who, Side::Team, true).await
}

/// POST /api/support/tickets/:id/participants/remove — take someone off.
/// Taking yourself off is unfollowing.
pub async fn remove_people(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(who): Json<PersonRef>,
) -> Reply {
    people_change(&state, &claims, &id, &who, Side::Team, false).await
}

/// POST /api/admin/support/tickets/:id/participants — support adds someone.
pub async fn admin_add_people(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(who): Json<PersonRef>,
) -> Reply {
    people_change(&state, &claims, &id, &who, Side::Support, true).await
}

/// POST /api/admin/support/tickets/:id/participants/remove
pub async fn admin_remove_people(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(who): Json<PersonRef>,
) -> Reply {
    people_change(&state, &claims, &id, &who, Side::Support, false).await
}

async fn people_change(
    state: &AppState,
    claims: &Claims,
    id: &str,
    who: &PersonRef,
    side: Side,
    add: bool,
) -> Reply {
    let st = store(state);
    let members = DynamoMembers { state };
    let mut caller = Caller::from_claims(claims);
    if side == Side::Team {
        caller = named(caller, &team_members(state, &claims.team_id).await);
    }
    if add {
        add_participant(&st, &members, &caller, side, id, who).await
    } else {
        remove_participant(&st, &caller, side, id, who).await
    }
    .map_err(Fail::reply)?;
    match side {
        Side::Team => {
            let (t, thread) = open_for_team(&st, &caller, id).await.map_err(Fail::reply)?;
            team_detail(state, t, thread).await
        }
        Side::Support => {
            let (t, thread) = admin_open(&st, &caller, id).await.map_err(Fail::reply)?;
            admin_detail(state, t, thread).await
        }
    }
}

/// POST /api/support/tickets/:id/resolve — the filer or a team admin marks
/// it resolved. Support and everyone else on the ticket are emailed.
pub async fn resolve(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    let members = team_members(&state, &claims.team_id).await;
    let caller = named(Caller::from_claims(&claims), &members);
    let st = store(&state);
    let t = resolve_as_customer(&st, &caller, &id)
        .await
        .map_err(Fail::reply)?;
    let by = caller_label(&caller);
    send(&state, mail_team_resolved(&inbox(), &t, &by)).await;
    for mail in mail_status(&t, "resolved", &by) {
        if !mail.to.eq_ignore_ascii_case(&caller.email) {
            send(&state, mail).await;
        }
    }
    let (t, thread) = open_for_team(&st, &caller, &id)
        .await
        .map_err(Fail::reply)?;
    team_detail(&state, t, thread).await
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
    let members = team_members(state, &t.team_id).await;
    let (mut t, mut thread) = (t, thread);
    fill_names(&mut t, &mut thread, &members);
    let avatars = avatars(&t, &thread, &members);
    Ok(Json(json!({
        "ticket": t,
        "messages": thread,
        "avatars": avatars,
        "support_access": { "until": until },
    })))
}

/// POST /api/admin/support/tickets/:id/reply — support answers; `status`
/// "resolved" resolves in the same step. Emails everyone on the ticket.
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
    for mail in mail_support_reply(&t, &m) {
        send(&state, mail).await;
    }
    let (t, thread) = admin_open(&st, &caller, &id).await.map_err(Fail::reply)?;
    admin_detail(&state, t, thread).await
}

/// POST /api/admin/support/tickets/:id/status — set status and/or priority.
/// Emails everyone on the ticket when its status changes.
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
    let (t, changed) = admin_set(&st, &caller, &id, status, req.priority.as_deref())
        .await
        .map_err(Fail::reply)?;
    if let Some(action) = changed {
        for mail in mail_status(&t, &action, "Coderhelm support") {
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
            people: Vec::new(),
        }
    }

    async fn file(store: &Mem, who: &Caller, subject: &str) -> Ticket {
        create(
            store,
            &none(),
            &roster(),
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
        assert_eq!(resolved.as_deref(), Some("resolved"));
        assert_eq!(t2.priority, "urgent");
        let (_, again) = admin_set(&store, &staff, &t.id, Some(Status::Resolved), None)
            .await
            .unwrap();
        assert!(again.is_none(), "resolving twice is not news");
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
        let err = create(&store, &none(), &roster(), &alice, &ask("One more"), "", "")
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
            create(&store, &none(), &roster(), &fresh, &ask("Late"), "", "")
                .await
                .unwrap_err(),
            Fail::TooMany(_)
        ));
        // Another team is not affected.
        assert!(create(
            &store,
            &none(),
            &roster(),
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
            people: Vec::new(),
        };
        let (t, m) = create(
            &store,
            &none(),
            &roster(),
            &alice,
            &req,
            "acme",
            "Browser/1.0",
        )
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
            &roster(),
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
            create(&store, &none(), &roster(), &alice, &long, "", "")
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
        let (t, first) = create(&store, &items, &roster(), &alice, &req, "acme", "")
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
        let mails = mail_support_reply(&t, &m);
        assert_eq!(mails.len(), 1);
        let to_customer = &mails[0];
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
        let resolved = mail_status(&t, "resolved", "Coderhelm support");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].to, "alice@example.com");
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
        let (t, first) = create(&store, &items, &roster(), &alice, &req, "acme", "")
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
        let err = create(&store, &items, &roster(), &alice, &theirs, "acme", "")
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
            create(&store, &items, &roster(), &alice, &req, "", "")
                .await
                .unwrap_err(),
            Fail::BadRequest(_)
        ));
        req.items = refs(0..9);
        let (t, _) = create(&store, &items, &roster(), &alice, &req, "", "")
            .await
            .unwrap();
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

    // ── People on a ticket ───────────────────────────────────────────────

    /// Each team's members, by team.
    #[derive(Default)]
    struct Roster(Mutex<HashMap<String, Vec<Member>>>);

    impl Roster {
        fn with(people: &[(&str, &str)]) -> Roster {
            let r = Roster::default();
            for (team, user) in people {
                r.0.lock()
                    .unwrap()
                    .entry(format!("TEAM#{team}"))
                    .or_default()
                    .push(Member {
                        user_id: format!("USER#{user}"),
                        email: format!("{user}@example.com"),
                        name: format!("{user} name"),
                        avatar_url: format!("https://avatars.example.com/{user}"),
                    });
            }
            r
        }
    }

    impl Members for Roster {
        async fn members(&self, team_id: &str) -> Result<Vec<Member>, String> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .get(team_id)
                .cloned()
                .unwrap_or_default())
        }
    }

    fn uid(id: &str) -> PersonRef {
        PersonRef {
            user_id: Some(format!("USER#{id}")),
            email: None,
        }
    }

    fn addr(email: &str) -> PersonRef {
        PersonRef {
            user_id: None,
            email: Some(email.into()),
        }
    }

    fn roster() -> Roster {
        Roster::with(&[
            ("alpha", "alice"),
            ("alpha", "bob"),
            ("alpha", "carol"),
            ("alpha", "dana"),
            ("beta", "mallory"),
        ])
    }

    fn events(thread: &[Message]) -> Vec<String> {
        thread
            .iter()
            .filter(|m| m.kind == "event")
            .map(|m| m.body.clone())
            .collect()
    }

    #[tokio::test]
    async fn the_filer_and_team_admins_add_and_remove_people_members_only_themselves() {
        let store = Mem::default();
        let team = roster();
        let alice = person("alpha", "alice");
        let bob = person("alpha", "bob");
        let t = file(&store, &alice, "Deploys hang").await;

        // The filer adds a teammate.
        let t2 = add_participant(&store, &team, &alice, Side::Team, &t.id, &uid("carol"))
            .await
            .unwrap();
        assert_eq!(t2.participants.len(), 1);
        assert_eq!(t2.participants[0].name, "carol name");
        assert_eq!(t2.participants[0].added_by, "alice");
        assert!(!t2.participants[0].outside);

        // A member who did not file cannot add or remove someone else.
        assert!(matches!(
            add_participant(&store, &team, &bob, Side::Team, &t.id, &uid("dana")).await,
            Err(Fail::Denied(_))
        ));
        assert!(matches!(
            remove_participant(&store, &bob, Side::Team, &t.id, &uid("carol")).await,
            Err(Fail::Denied(_))
        ));

        // A team admin can.
        let admin_bob = Caller {
            role: "admin".into(),
            ..bob.clone()
        };
        add_participant(&store, &team, &admin_bob, Side::Team, &t.id, &uid("dana"))
            .await
            .unwrap();
        let t3 = remove_participant(&store, &admin_bob, Side::Team, &t.id, &uid("carol"))
            .await
            .unwrap();
        assert_eq!(
            t3.participants
                .iter()
                .map(|p| p.email.as_str())
                .collect::<Vec<_>>(),
            ["dana@example.com"]
        );

        // So can a platform admin, through the support routes only.
        let staff = admin();
        add_participant(&store, &team, &staff, Side::Support, &t.id, &uid("carol"))
            .await
            .unwrap();
        assert_eq!(
            add_participant(&store, &team, &staff, Side::Team, &t.id, &uid("bob"))
                .await
                .unwrap_err(),
            Fail::NotFound,
            "the team routes stay keyed to the admin's own team"
        );
        // A plain member calling the support routes is refused.
        assert_eq!(
            add_participant(&store, &team, &bob, Side::Support, &t.id, &uid("bob"))
                .await
                .unwrap_err(),
            Fail::Forbidden
        );

        let (_, thread) = open_for_team(&store, &alice, &t.id).await.unwrap();
        assert_eq!(
            events(&thread),
            [
                "added:carol name",
                "added:dana name",
                "removed:carol name",
                "added:carol name"
            ]
        );
        let support_event = thread.iter().rev().find(|m| m.kind == "event").unwrap();
        assert_eq!(support_event.author, "support");
    }

    #[tokio::test]
    async fn members_follow_and_unfollow_themselves() {
        let store = Mem::default();
        let team = roster();
        let alice = person("alpha", "alice");
        let bob = person("alpha", "bob");
        let t = file(&store, &alice, "Slow reviews").await;

        let t2 = add_participant(&store, &team, &bob, Side::Team, &t.id, &uid("bob"))
            .await
            .unwrap();
        assert_eq!(t2.participants[0].user_id, "USER#bob");
        // Following twice is refused, not doubled.
        assert!(matches!(
            add_participant(&store, &team, &bob, Side::Team, &t.id, &uid("bob")).await,
            Err(Fail::BadRequest(_))
        ));
        // The filer already gets every email.
        assert!(matches!(
            add_participant(&store, &team, &alice, Side::Team, &t.id, &uid("alice")).await,
            Err(Fail::BadRequest(_))
        ));
        let t3 = remove_participant(&store, &bob, Side::Team, &t.id, &uid("bob"))
            .await
            .unwrap();
        assert!(t3.participants.is_empty());
        let (_, thread) = open_for_team(&store, &alice, &t.id).await.unwrap();
        assert_eq!(events(&thread), ["followed", "left"]);
        // The queue copy carries the same people.
        assert_eq!(store.queue_entry(&t.id).await.unwrap().unwrap(), t3);
    }

    #[tokio::test]
    async fn nobody_from_another_team_can_be_added_except_as_an_outside_address() {
        let store = Mem::default();
        let team = roster();
        let alice = person("alpha", "alice");
        let t = file(&store, &alice, "Billing").await;

        // Another team's member, by id: refused.
        assert_eq!(
            add_participant(&store, &team, &alice, Side::Team, &t.id, &uid("mallory"))
                .await
                .unwrap_err(),
            Fail::BadRequest("That person is not on this team.".into())
        );
        // Another team cannot add people to this team's ticket at all.
        let mallory = Caller {
            role: "owner".into(),
            ..person("beta", "mallory")
        };
        assert_eq!(
            add_participant(&store, &team, &mallory, Side::Team, &t.id, &uid("mallory"))
                .await
                .unwrap_err(),
            Fail::NotFound
        );
        // An address is copied as an outsider (it gives no access).
        let t2 = add_participant(
            &store,
            &team,
            &alice,
            Side::Team,
            &t.id,
            &addr(" Mallory@Example.com "),
        )
        .await
        .unwrap();
        assert!(t2.participants[0].outside);
        assert_eq!(t2.participants[0].email, "mallory@example.com");
        assert!(t2.participants[0].user_id.is_empty());
        // A teammate's address is that teammate, not an outsider.
        let t3 = add_participant(
            &store,
            &team,
            &alice,
            Side::Team,
            &t.id,
            &addr("bob@example.com"),
        )
        .await
        .unwrap();
        assert!(!t3.participants[1].outside);
        assert_eq!(t3.participants[1].user_id, "USER#bob");
    }

    #[test]
    fn email_addresses_are_checked() {
        for ok in ["a@example.com", " Pat.Lee+x@mail.example.org ", "x_y@a.io"] {
            assert!(clean_email(ok).is_some(), "{ok}");
        }
        assert_eq!(
            clean_email(" Pat@Example.COM ").as_deref(),
            Some("pat@example.com")
        );
        for bad in [
            "",
            "pat",
            "pat@",
            "@example.com",
            "pat@example",
            "pat@.example.com",
            "pat@example.com.",
            "pat@exa..mple.com",
            "pat@@example.com",
            "pat lee@example.com",
            "pat@example.com, other@example.com",
            "Pat <pat@example.com>",
            "pat@example.com\nBcc: x@example.com",
        ] {
            assert!(clean_email(bad).is_none(), "{bad:?}");
        }
    }

    #[tokio::test]
    async fn a_bad_address_is_refused_before_anything_is_written() {
        let store = Mem::default();
        let alice = person("alpha", "alice");
        let t = file(&store, &alice, "Login").await;
        for bad in ["nope", "a@b", "x@example.com, y@example.com", ""] {
            assert_eq!(
                add_participant(&store, &roster(), &alice, Side::Team, &t.id, &addr(bad))
                    .await
                    .unwrap_err(),
                if bad.is_empty() {
                    Fail::BadRequest(
                        "Choose someone on the team, or enter an email address.".into(),
                    )
                } else {
                    Fail::BadRequest("Enter a valid email address.".into())
                }
            );
        }
        let (t2, thread) = open_for_team(&store, &alice, &t.id).await.unwrap();
        assert!(t2.participants.is_empty());
        assert!(events(&thread).is_empty());
    }

    #[tokio::test]
    async fn a_ticket_has_at_most_twenty_people_and_additions_are_rate_limited() {
        let store = Mem::default();
        let alice = person("alpha", "alice");
        let t = file(&store, &alice, "Many people").await;
        for i in 0..MAX_PARTICIPANTS {
            add_participant(
                &store,
                &none_roster(),
                &alice,
                Side::Team,
                &t.id,
                &addr(&format!("guest{i}@example.org")),
            )
            .await
            .unwrap();
        }
        assert_eq!(
            add_participant(
                &store,
                &none_roster(),
                &alice,
                Side::Team,
                &t.id,
                &addr("one-more@example.org")
            )
            .await
            .unwrap_err(),
            Fail::BadRequest(format!(
                "A ticket can have at most {MAX_PARTICIPANTS} people besides the person who filed it."
            ))
        );

        // The hourly limit is per team, over all its tickets.
        let other = file(&store, &alice, "Another").await;
        let mut refused = false;
        for i in 0..TEAM_PEOPLE_ADDS_PER_HOUR {
            let r = add_participant(
                &store,
                &none_roster(),
                &alice,
                Side::Team,
                &other.id,
                &addr(&format!("more{i}@example.org")),
            )
            .await;
            match r {
                Ok(_) => {}
                Err(Fail::TooMany(_)) => {
                    refused = true;
                    break;
                }
                Err(Fail::BadRequest(_)) => continue,
                Err(e) => panic!("{e:?}"),
            }
        }
        assert!(refused, "the team's hourly allowance runs out");
    }

    fn none_roster() -> Roster {
        Roster::default()
    }

    #[tokio::test]
    async fn emails_go_to_everyone_on_the_ticket_and_only_them() {
        let store = Mem::default();
        let team = roster();
        let alice = person("alpha", "alice");
        let staff = admin();
        let t = file(&store, &alice, "Flaky CI").await;
        let other = file(&store, &person("alpha", "dana"), "Unrelated").await;
        add_participant(&store, &team, &alice, Side::Team, &t.id, &uid("bob"))
            .await
            .unwrap();
        add_participant(
            &store,
            &team,
            &alice,
            Side::Team,
            &t.id,
            &addr("guest@example.org"),
        )
        .await
        .unwrap();
        // Someone on another ticket only.
        add_participant(
            &store,
            &team,
            &person("alpha", "dana"),
            Side::Team,
            &other.id,
            &uid("carol"),
        )
        .await
        .unwrap();

        let (t, m) = admin_reply(&store, &staff, &t.id, "Fixed in the runner.", None)
            .await
            .unwrap();
        let mails = mail_support_reply(&t, &m);
        let to: Vec<&str> = mails.iter().map(|m| m.to.as_str()).collect();
        assert_eq!(
            to,
            ["alice@example.com", "bob@example.com", "guest@example.org"]
        );
        for mail in &mails {
            assert!(mail.text.contains(
                "People on this ticket: alice (filed), bob name, guest@example.org (copied)"
            ));
            assert!(
                !mail.text.contains("helper@example.com"),
                "support staff stay anonymous"
            );
        }
        // Members get the link and are told to answer there; the outsider
        // gets no link and is told the address cannot sign in.
        let member = &mails[1];
        assert!(member
            .text
            .contains(&format!("/support/ticket/?id={}", t.id)));
        assert!(member
            .text
            .contains("Reply by visiting the ticket in Coderhelm"));
        let outside = &mails[2];
        assert!(!outside.text.contains("/support/ticket/"));
        assert!(!outside.html.contains("/support/ticket/"));
        assert!(outside.text.contains("cannot sign in"));
        assert!(!outside.text.contains("Reply by visiting"));

        // Status changes reach the same people.
        let (t, action) = admin_set(&store, &staff, &t.id, Some(Status::Resolved), None)
            .await
            .unwrap();
        let mails = mail_status(&t, action.as_deref().unwrap(), "Coderhelm support");
        assert_eq!(mails.len(), 3);
        assert!(mails.iter().all(|m| m.subject.starts_with("Resolved: ")));

        // A teammate's reply goes to support's inbox, naming who is on it.
        let (t, m) = reply_as_customer(&store, &none(), &alice, &t.id, "Back again", &[])
            .await
            .unwrap();
        let to_support = mail_customer_reply("help@example.com", &t, &m);
        assert_eq!(to_support.to, "help@example.com");
        assert!(to_support.html.contains("guest@example.org (copied)"));

        // A person on the ticket twice (by id and address) is emailed once.
        let mut twice = t.clone();
        twice.participants.push(Participant {
            email: "BOB@example.com".into(),
            ..twice.participants[0].clone()
        });
        assert_eq!(recipients(&twice).len(), 3);
    }

    #[tokio::test]
    async fn the_filer_or_a_team_admin_marks_a_ticket_resolved() {
        let store = Mem::default();
        let alice = person("alpha", "alice");
        let bob = person("alpha", "bob");
        let t = file(&store, &alice, "Done?").await;
        assert!(matches!(
            resolve_as_customer(&store, &bob, &t.id).await,
            Err(Fail::Denied(_))
        ));
        let t2 = resolve_as_customer(&store, &alice, &t.id).await.unwrap();
        assert_eq!(t2.status, Status::Resolved);
        assert!(matches!(
            resolve_as_customer(&store, &alice, &t.id).await,
            Err(Fail::BadRequest(_))
        ));
        assert_eq!(
            resolve_as_customer(&store, &person("beta", "mallory"), &t.id)
                .await
                .unwrap_err(),
            Fail::NotFound
        );
        let (_, thread) = open_for_team(&store, &alice, &t.id).await.unwrap();
        assert_eq!(events(&thread), ["resolved"]);
    }

    #[tokio::test]
    async fn names_fill_in_for_bare_addresses_and_the_list_keeps_the_latest_line() {
        let store = Mem::default();
        let nameless = Caller {
            name: String::new(),
            ..person("alpha", "alice")
        };
        let t = file(&store, &nameless, "Names").await;
        let (t, _) =
            reply_as_customer(&store, &none(), &nameless, &t.id, "Second  line\nhere", &[])
                .await
                .unwrap();
        assert_eq!(t.last_snippet, "Second line here");
        let (mut t, mut thread) = open_for_team(&store, &nameless, &t.id).await.unwrap();
        assert!(thread.iter().all(|m| m.author_name.is_empty()));
        let members = roster().members("TEAM#alpha").await.unwrap();
        fill_names(&mut t, &mut thread, &members);
        assert!(thread.iter().all(|m| m.author_name == "alice name"));
        assert_eq!(t.opened_by_name, "alice name");
        let av = avatars(&t, &thread, &members);
        assert_eq!(av.len(), 1, "only the people on this ticket");
        assert_eq!(av["alice@example.com"], "https://avatars.example.com/alice");

        // Support's latest line reads as Coderhelm support to the customer.
        let (t, _) = admin_reply(&store, &admin(), &t.id, "On it", None)
            .await
            .unwrap();
        assert_eq!(customer_ticket(t).last_author_name, "Coderhelm support");
    }

    // ── People named as a ticket opens ───────────────────────────────────

    fn ask_with(subject: &str, people: Vec<PersonRef>) -> NewTicket {
        NewTicket {
            people,
            ..ask(subject)
        }
    }

    #[tokio::test]
    async fn people_named_on_a_new_ticket_are_on_it_from_the_start() {
        let store = Mem::default();
        let alice = person("alpha", "alice");
        let req = ask_with(
            "Deploys hang",
            vec![
                uid("bob"),
                // A teammate's address is that teammate.
                addr(" Carol@Example.com "),
                // Anyone else's address is copied by email only.
                addr("guest@example.org"),
                // The filer is on every ticket already, and twice is once.
                uid("alice"),
                addr("alice@example.com"),
                PersonRef {
                    user_id: Some("bob".into()),
                    email: None,
                },
                addr("GUEST@example.org"),
            ],
        );
        let (t, first) = create(&store, &none(), &roster(), &alice, &req, "acme", "")
            .await
            .unwrap();
        let who: Vec<(&str, &str, bool)> = t
            .participants
            .iter()
            .map(|p| (p.user_id.as_str(), p.email.as_str(), p.outside))
            .collect();
        assert_eq!(
            who,
            [
                ("USER#bob", "bob@example.com", false),
                ("USER#carol", "carol@example.com", false),
                ("", "guest@example.org", true),
            ]
        );
        assert!(t
            .participants
            .iter()
            .all(|p| p.added_by == "alice" && p.added_at == t.created_at));
        assert_eq!(t.participants[0].name, "bob name");

        // The thread reads: the question, then who was added, as events.
        let (saved, thread) = open_for_team(&store, &alice, &t.id).await.unwrap();
        assert_eq!(saved, t);
        assert_eq!(thread[0], first);
        assert_eq!(
            events(&thread),
            [
                "added:bob name",
                "added:carol name",
                "added:guest@example.org"
            ]
        );
        assert!(thread[1..]
            .iter()
            .all(|m| m.author == "customer" && m.author_email == "alice@example.com"));
        // The queue copy carries the same people.
        assert_eq!(store.queue_entry(&t.id).await.unwrap().unwrap(), t);

        // They get the same emails as people added later, and support sees
        // who is on it from the first email.
        let to: Vec<String> = recipients(&t).into_iter().map(|r| r.email).collect();
        assert_eq!(
            to,
            [
                "alice@example.com",
                "bob@example.com",
                "carol@example.com",
                "guest@example.org"
            ]
        );
        let new = mail_new_ticket("inbox@example.com", &t, &first.body);
        assert!(new
            .text
            .contains("People: alice (filed), bob name, carol name, guest@example.org (copied)"));

        // And they can be taken off the same way.
        let t2 = remove_participant(&store, &alice, Side::Team, &t.id, &uid("bob"))
            .await
            .unwrap();
        assert_eq!(t2.participants.len(), 2);
    }

    #[tokio::test]
    async fn a_new_ticket_never_names_another_teams_member_and_writes_nothing_when_refused() {
        let store = Mem::default();
        let alice = person("alpha", "alice");

        // Another team's member, by id: refused, and the ticket is not opened.
        assert_eq!(
            create(
                &store,
                &none(),
                &roster(),
                &alice,
                &ask_with("Billing", vec![uid("bob"), uid("mallory")]),
                "",
                ""
            )
            .await
            .unwrap_err(),
            Fail::BadRequest("That person is not on this team.".into())
        );
        // A bad address, or an entry with neither: the same.
        for (bad, why) in [
            (addr("nope@"), "Enter a valid email address."),
            (
                addr("x@example.com, y@example.com"),
                "Enter a valid email address.",
            ),
            (
                PersonRef::default(),
                "Choose someone on the team, or enter an email address.",
            ),
        ] {
            assert_eq!(
                create(
                    &store,
                    &none(),
                    &roster(),
                    &alice,
                    &ask_with("Billing", vec![bad]),
                    "",
                    ""
                )
                .await
                .unwrap_err(),
                Fail::BadRequest(why.into())
            );
        }
        assert!(store.list_team("TEAM#alpha").await.unwrap().is_empty());
        assert!(store.list_queue().await.unwrap().is_empty());
        // Refusals use none of the hourly allowance: five tickets still open.
        for i in 0..USER_TICKETS_PER_HOUR {
            create(
                &store,
                &none(),
                &roster(),
                &alice,
                &ask(&format!("Ticket {i}")),
                "",
                "",
            )
            .await
            .unwrap();
        }

        // Another team filing names only its own members: alpha's people by
        // id are refused, and an alpha address is only an outside copy.
        let mallory = Caller {
            role: "owner".into(),
            ..person("beta", "mallory")
        };
        assert_eq!(
            create(
                &store,
                &none(),
                &roster(),
                &mallory,
                &ask_with("Theirs", vec![uid("alice")]),
                "",
                ""
            )
            .await
            .unwrap_err(),
            Fail::BadRequest("That person is not on this team.".into())
        );
        let (t, _) = create(
            &store,
            &none(),
            &roster(),
            &mallory,
            &ask_with("Theirs", vec![addr("alice@example.com")]),
            "",
            "",
        )
        .await
        .unwrap();
        assert!(t.participants[0].outside, "no access to beta's ticket");
        assert!(t.participants[0].user_id.is_empty());
        // Alpha's people still cannot read it.
        assert_eq!(
            open_for_team(&store, &alice, &t.id).await.unwrap_err(),
            Fail::NotFound
        );
    }

    #[tokio::test]
    async fn a_new_ticket_has_at_most_twenty_people_and_they_count_toward_the_hourly_additions() {
        let store = Mem::default();
        let alice = person("alpha", "alice");
        let guests = |n: usize, tag: &str| -> Vec<PersonRef> {
            (0..n)
                .map(|i| addr(&format!("{tag}{i}@example.org")))
                .collect()
        };
        assert_eq!(
            create(
                &store,
                &none(),
                &roster(),
                &alice,
                &ask_with("Too many", guests(MAX_PARTICIPANTS + 1, "g")),
                "",
                ""
            )
            .await
            .unwrap_err(),
            Fail::BadRequest(format!(
                "A ticket can have at most {MAX_PARTICIPANTS} people besides the person who filed it."
            ))
        );
        let (t, _) = create(
            &store,
            &none(),
            &roster(),
            &alice,
            &ask_with("Twenty", guests(MAX_PARTICIPANTS, "g")),
            "",
            "",
        )
        .await
        .unwrap();
        assert_eq!(t.participants.len(), MAX_PARTICIPANTS);
        // The ticket is full, as if they had been added one by one.
        assert!(matches!(
            add_participant(&store, &roster(), &alice, Side::Team, &t.id, &uid("bob")).await,
            Err(Fail::BadRequest(_))
        ));

        // Twenty of the team's hourly thirty are used: another twenty are refused.
        let bob = person("alpha", "bob");
        assert!(matches!(
            create(
                &store,
                &none(),
                &roster(),
                &bob,
                &ask_with("Twenty more", guests(MAX_PARTICIPANTS, "h")),
                "",
                ""
            )
            .await,
            Err(Fail::TooMany(_))
        ));
        assert_eq!(store.list_team("TEAM#alpha").await.unwrap().len(), 1);
    }
}
