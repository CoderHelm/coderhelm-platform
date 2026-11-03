//! Support tickets from self-hosted installs reach CoderHelm's support
//! queue, and support's answers come back to the install.
//!
//! What leaves an install, and nothing else: the subject, each message the
//! person writes, their name and email (so support can answer), the
//! dashboard page the ticket was filed from, the install's id and version,
//! and for each item attached its kind, name, status and, for a run, its
//! error text and step. Never code, diffs, files or anything read from a
//! repository. Likely secrets are already replaced before a ticket is saved
//! (`scrub` in the gateway's support routes) and again here.
//!
//! **Install side.** Each ticket and each message goes to an outbox
//! (`pk = SUPPORT_OUTBOX`) in the request that saves it and is sent to
//! CoderHelm signed with the install's registration secret
//! (`hybrid::sign`). An install without a registration registers first:
//! the same registration hybrid uses, which turns no hybrid part on.
//! Anything not sent stays in the outbox and the ticket says "Sending to
//! CoderHelm". The worker's `support_sync` (every few minutes) sends the
//! outbox again and asks CoderHelm for support's answers since the last
//! time ([`UPDATES_PATH`], outbound only: an install needn't accept calls),
//! adds them to the ticket and emails the people on it from the install.
//!
//! **Hosted side.** An install's tickets live under the team
//! [`install_team`] in the support queue, so staff work them like any
//! other. Their answers aren't emailed from the hosted side: the install
//! fetches them and emails its own people.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub const TICKETS_PATH: &str = "/hybrid/support/tickets";
pub const MESSAGES_PATH: &str = "/hybrid/support/messages";
pub const UPDATES_PATH: &str = "/hybrid/support/updates";

pub const OUTBOX_PK: &str = "SUPPORT_OUTBOX";
const SYNC_PK: &str = "SUPPORT_SYNC";
const SYNC_SK: &str = "CURSOR";
const QUEUE_PK: &str = "SUPPORT_QUEUE";
const META_PREFIX: &str = "SUPPORT#";
const MSG_PREFIX: &str = "SUPPORT_MSG#";

pub const MAX_SUBJECT: usize = 150;
pub const MAX_BODY: usize = 8000;
pub const MAX_NAME: usize = 120;
pub const MAX_EMAIL: usize = 254;
pub const MAX_PAGE: usize = 300;
pub const MAX_ITEMS: usize = 10;
pub const MAX_LABEL: usize = 200;
/// A run's error text, as attached.
pub const MAX_ERROR: usize = 1500;
/// Answers handed back in one poll.
pub const MAX_UPDATES: usize = 200;

/// The team an install's tickets live under on the hosted side.
pub fn install_team(install_id: &str) -> String {
    format!("SUPPORT_INSTALL#{install_id}")
}

/// The install a hosted team key names, if it is an install's.
pub fn install_of_team(team_id: &str) -> Option<&str> {
    team_id
        .strip_prefix("SUPPORT_INSTALL#")
        .filter(|i| !i.is_empty())
}

/// One item attached to a ticket, as it leaves the install.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ItemOut {
    pub kind: String,
    pub id: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub status: String,
    /// A run's error text, when it failed.
    #[serde(default)]
    pub error: String,
    /// A run's step when it stopped.
    #[serde(default)]
    pub step: String,
}

/// A new ticket, from an install.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TicketOut {
    pub ticket_id: String,
    pub message_id: String,
    pub subject: String,
    pub body: String,
    pub name: String,
    pub email: String,
    #[serde(default)]
    pub page: String,
    #[serde(default)]
    pub priority: String,
    #[serde(default)]
    pub items: Vec<ItemOut>,
    #[serde(default)]
    pub version: String,
    pub created_at: String,
}

/// A person's reply on a ticket, from an install.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MessageOut {
    pub ticket_id: String,
    pub message_id: String,
    pub body: String,
    pub name: String,
    pub email: String,
    #[serde(default)]
    pub items: Vec<ItemOut>,
    pub created_at: String,
}

/// Something support did on an install's ticket: an answer (`message`) or a
/// status change (`event`, body "resolved", "reopened", "status:waiting").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Update {
    pub ticket_id: String,
    pub message_id: String,
    pub kind: String,
    pub body: String,
    pub created_at: String,
    /// The ticket's status after this (open, waiting, resolved).
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Updates {
    #[serde(default)]
    pub updates: Vec<Update>,
    /// The cursor for the next poll.
    #[serde(default)]
    pub now: String,
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub fn clip(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

fn plain_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn email_ok(s: &str) -> bool {
    let s = s.trim();
    s.len() <= MAX_EMAIL
        && s.split_once('@').is_some_and(|(a, d)| {
            !a.is_empty() && d.contains('.') && !s.contains(char::is_whitespace)
        })
}

fn items_ok(items: &[ItemOut]) -> Result<(), &'static str> {
    if items.len() > MAX_ITEMS {
        return Err("Too many attachments.");
    }
    for i in items {
        if i.kind.len() > 30 || i.id.len() > 200 || i.label.chars().count() > MAX_LABEL {
            return Err("An attachment is too long.");
        }
        if i.error.chars().count() > MAX_ERROR || i.status.len() > 60 || i.step.len() > 120 {
            return Err("An attachment's details are too long.");
        }
    }
    Ok(())
}

impl ItemOut {
    /// The item as it may leave: names and statuses one line, the error
    /// text cut to [`MAX_ERROR`] with likely secrets replaced.
    pub fn clean(mut self, scrub: impl Fn(&str) -> String) -> Self {
        self.kind = clip(&one_line(&self.kind), 30);
        self.id = clip(&self.id, 200);
        self.label = clip(&one_line(&scrub(&self.label)), MAX_LABEL);
        self.status = clip(&one_line(&self.status), 60);
        self.step = clip(&one_line(&self.step), 120);
        self.error = clip(scrub(&self.error).trim(), MAX_ERROR);
        self
    }
}

impl TicketOut {
    /// Checked on the hosted side before anything is stored.
    pub fn check(&self) -> Result<(), &'static str> {
        if !plain_id(&self.ticket_id) || !plain_id(&self.message_id) {
            return Err("Send the ticket's ids.");
        }
        let subject = self.subject.trim();
        if subject.is_empty() || subject.chars().count() > MAX_SUBJECT {
            return Err("The subject is missing or too long.");
        }
        if self.body.trim().is_empty() || self.body.chars().count() > MAX_BODY {
            return Err("The message is missing or too long.");
        }
        if self.name.chars().count() > MAX_NAME || !email_ok(&self.email) {
            return Err("Send the person's name and email.");
        }
        if self.page.chars().count() > MAX_PAGE || self.version.len() > 40 {
            return Err("The page or version is too long.");
        }
        items_ok(&self.items)
    }
}

impl MessageOut {
    pub fn check(&self) -> Result<(), &'static str> {
        if !plain_id(&self.ticket_id) || !plain_id(&self.message_id) {
            return Err("Send the ticket's ids.");
        }
        if self.body.chars().count() > MAX_BODY
            || (self.body.trim().is_empty() && self.items.is_empty())
        {
            return Err("The message is missing or too long.");
        }
        if self.name.chars().count() > MAX_NAME || !email_ok(&self.email) {
            return Err("Send the person's name and email.");
        }
        items_ok(&self.items)
    }
}

/// The text support reads about the attachments: one line per item, a
/// run's error under it.
pub fn items_text(items: &[ItemOut]) -> String {
    let mut out = String::new();
    for i in items {
        let mut line = format!(
            "- {}: {}",
            i.kind,
            if i.label.is_empty() { &i.id } else { &i.label }
        );
        if !i.status.is_empty() {
            line.push_str(&format!(" ({})", i.status));
        }
        if !i.step.is_empty() {
            line.push_str(&format!(", step: {}", i.step));
        }
        out.push_str(&line);
        out.push('\n');
        if !i.error.is_empty() {
            out.push_str(&format!("  Error: {}\n", i.error));
        }
    }
    out
}

// ── The install's outbox ─────────────────────────────────────────────────

/// One ticket or message waiting to reach CoderHelm.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutboxEntry {
    /// `ticket` or `message`.
    pub kind: String,
    pub team_id: String,
    pub ticket_id: String,
    pub message_id: String,
    pub payload: Value,
    pub queued_at: String,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default)]
    pub last_error: String,
}

impl OutboxEntry {
    pub fn ticket(team_id: &str, t: &TicketOut) -> Self {
        Self {
            kind: "ticket".into(),
            team_id: team_id.into(),
            ticket_id: t.ticket_id.clone(),
            message_id: t.message_id.clone(),
            payload: serde_json::to_value(t).unwrap_or(Value::Null),
            queued_at: t.created_at.clone(),
            attempts: 0,
            last_error: String::new(),
        }
    }
    pub fn message(team_id: &str, m: &MessageOut) -> Self {
        Self {
            kind: "message".into(),
            team_id: team_id.into(),
            ticket_id: m.ticket_id.clone(),
            message_id: m.message_id.clone(),
            payload: serde_json::to_value(m).unwrap_or(Value::Null),
            queued_at: m.created_at.clone(),
            attempts: 0,
            last_error: String::new(),
        }
    }
    /// Its key in the outbox: oldest first, so a ticket goes before its replies.
    pub fn sk(&self) -> String {
        format!("{}#{}", self.queued_at, self.message_id)
    }
    fn path(&self) -> &'static str {
        if self.kind == "ticket" {
            TICKETS_PATH
        } else {
            MESSAGES_PATH
        }
    }
}

/// Where a ticket stands with CoderHelm, kept on the install's ticket
/// (`coderhelm` in its data).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Delivery {
    /// `sending` or `sent`.
    pub state: String,
    #[serde(default)]
    pub error: String,
    #[serde(default)]
    pub sent_at: String,
}

/// The install's support records.
#[allow(async_fn_in_trait)]
pub trait Local {
    async fn outbox(&self) -> Result<Vec<OutboxEntry>, String>;
    async fn remove(&self, e: &OutboxEntry) -> Result<(), String>;
    async fn failed(&self, e: &OutboxEntry, error: &str) -> Result<(), String>;
    async fn set_delivery(
        &self,
        team_id: &str,
        ticket_id: &str,
        d: &Delivery,
    ) -> Result<(), String>;
    async fn cursor(&self) -> Result<String, String>;
    async fn set_cursor(&self, cursor: &str) -> Result<(), String>;
    /// The team a ticket belongs to on this install.
    async fn ticket_team(&self, ticket_id: &str) -> Result<Option<String>, String>;
    /// Add support's answer or status change to the ticket. The ticket's data
    /// after it, or None when this update was added before (nothing to email).
    async fn apply(&self, team_id: &str, u: &Update) -> Result<Option<Value>, String>;
}

/// CoderHelm's support intake, signed as the install.
#[allow(async_fn_in_trait)]
pub trait Hosted {
    async fn post(&self, path: &str, body: &Value) -> Result<Value, String>;
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct FlushReport {
    pub sent: usize,
    pub failed: usize,
    pub last_error: String,
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Send what's waiting, oldest first. A ticket whose own send failed keeps
/// its replies waiting too, so CoderHelm never sees a reply before its ticket.
pub async fn flush<L: Local, H: Hosted>(local: &L, hosted: &H) -> Result<FlushReport, String> {
    let mut waiting = local.outbox().await?;
    waiting.sort_by_key(|e| e.sk());
    let mut report = FlushReport::default();
    let mut blocked: Vec<String> = Vec::new();
    let mut tickets_ok: HashMap<String, String> = HashMap::new();
    for e in &waiting {
        if blocked.contains(&e.ticket_id) {
            continue;
        }
        match hosted.post(e.path(), &e.payload).await {
            Ok(_) => {
                local.remove(e).await?;
                report.sent += 1;
                tickets_ok.insert(e.ticket_id.clone(), e.team_id.clone());
            }
            Err(err) => {
                local.failed(e, &err).await?;
                report.failed += 1;
                report.last_error = err.clone();
                blocked.push(e.ticket_id.clone());
                let _ = local
                    .set_delivery(
                        &e.team_id,
                        &e.ticket_id,
                        &Delivery {
                            state: "sending".into(),
                            error: err,
                            sent_at: String::new(),
                        },
                    )
                    .await;
            }
        }
    }
    let at = now_iso();
    for (ticket, team) in tickets_ok {
        if blocked.contains(&ticket) {
            continue;
        }
        local
            .set_delivery(
                &team,
                &ticket,
                &Delivery {
                    state: "sent".into(),
                    error: String::new(),
                    sent_at: at.clone(),
                },
            )
            .await?;
    }
    Ok(report)
}

/// Ask CoderHelm for support's answers since the last poll and add them to
/// the tickets. Each newly added one comes back (with its ticket) for the
/// emails; one added before doesn't.
pub async fn poll<L: Local, H: Hosted>(
    local: &L,
    hosted: &H,
) -> Result<Vec<(Value, Update)>, String> {
    let since = local.cursor().await?;
    let v = hosted
        .post(UPDATES_PATH, &json!({ "since": since }))
        .await?;
    let got: Updates = serde_json::from_value(v).map_err(|e| format!("CoderHelm's answer: {e}"))?;
    let mut out = Vec::new();
    for u in got.updates.iter().take(MAX_UPDATES) {
        let Some(team) = local.ticket_team(&u.ticket_id).await? else {
            continue;
        };
        if let Some(ticket) = local.apply(&team, u).await? {
            out.push((ticket, u.clone()));
        }
    }
    if !got.now.is_empty() && got.now > since {
        local.set_cursor(&got.now).await?;
    }
    Ok(out)
}

// ── The emails an install sends for support's answers ───────────────────

fn esc(v: &str) -> String {
    v.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Everyone on an install's ticket (the filer, then each person added), once each.
pub fn recipients(ticket: &Value) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let filer = ticket["opened_by_email"].as_str().unwrap_or("");
    let people = ticket["participants"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for e in std::iter::once(filer.to_string()).chain(
        people
            .iter()
            .map(|p| p["email"].as_str().unwrap_or("").to_string()),
    ) {
        let e = e.trim().to_lowercase();
        if !e.is_empty() && !out.contains(&e) {
            out.push(e);
        }
    }
    out
}

/// One email per person on the ticket: support answered, or changed its status.
pub fn mails_for(ticket: &Value, u: &Update, app_url: &str) -> Vec<crate::mail::Mail> {
    let subject = ticket["subject"].as_str().unwrap_or("Your support ticket");
    let id = ticket["id"].as_str().unwrap_or(&u.ticket_id);
    let link = format!("{}/support/ticket/?id={id}", app_url.trim_end_matches('/'));
    let (heading, quote) = if u.kind == "message" {
        let h = if u.status == "resolved" {
            format!("Resolved: {subject}")
        } else {
            format!("CoderHelm support replied: {subject}")
        };
        (h, u.body.clone())
    } else {
        match u.body.as_str() {
            "resolved" => (
                format!("Resolved: {subject}"),
                "CoderHelm support marked this ticket resolved. Reply in CoderHelm if it isn't, and it opens again.".to_string(),
            ),
            "reopened" => (format!("Reopened: {subject}"), "CoderHelm support opened this ticket again.".to_string()),
            "status:waiting" => (
                format!("Waiting on your team: {subject}"),
                "CoderHelm support is waiting for an answer from your team.".to_string(),
            ),
            _ => (format!("With CoderHelm support: {subject}"), "It's CoderHelm support's turn.".to_string()),
        }
    };
    let note = "Reply by visiting the ticket in CoderHelm. Replies to this email are not read.";
    let text = format!("{heading}\n\n{quote}\n\n{note}\n{link}\n");
    let html = format!(
        "<div style=\"font-family:sans-serif;max-width:560px\"><h2 style=\"font-size:18px\">{}</h2>\
         <div style=\"white-space:pre-wrap;border-left:3px solid #ddd;padding-left:12px\">{}</div>\
         <p style=\"color:#666\">{}</p><p><a href=\"{}\">See the ticket</a></p></div>",
        esc(&heading),
        esc(&quote),
        esc(note),
        esc(&link)
    );
    recipients(ticket)
        .into_iter()
        .map(|to| crate::mail::Mail {
            kind: "support".into(),
            to,
            template: "support_update",
            params: json!({
                "ticket": crate::mail::templates::fit(subject, 200),
                "note": crate::mail::templates::fit(&format!("{heading}\n\n{quote}"), 2000),
                "link": format!("{}/support/", app_url.trim_end_matches('/')),
            })
            .as_object()
            .cloned()
            .unwrap_or_default(),
            ses: crate::mail::SesBody::Simple {
                subject: format!("Re: {subject}"),
                text: text.clone(),
                html: Some(html.clone()),
                headers: vec![],
            },
        })
        .collect()
}

// ── DynamoDB ─────────────────────────────────────────────────────────────

/// The install's support records on its settings table.
pub struct DynamoLocal<'a> {
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub table: &'a str,
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn derr<E: std::error::Error + 'static>(e: aws_sdk_dynamodb::error::SdkError<E>) -> String {
    format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e))
}

impl DynamoLocal<'_> {
    pub async fn enqueue(&self, e: &OutboxEntry) -> Result<(), String> {
        let data = serde_json::to_string(e).map_err(|x| x.to_string())?;
        self.dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", s(OUTBOX_PK))
            .item("sk", s(&e.sk()))
            .item("data", s(&data))
            .send()
            .await
            .map_err(derr)?;
        Ok(())
    }

    async fn ticket_data(&self, team_id: &str, ticket_id: &str) -> Result<Option<Value>, String> {
        let r = self
            .dynamo
            .get_item()
            .table_name(self.table)
            .key("pk", s(team_id))
            .key("sk", s(&format!("{META_PREFIX}{ticket_id}")))
            .consistent_read(true)
            .send()
            .await
            .map_err(derr)?;
        Ok(r.item()
            .and_then(|i| i.get("data"))
            .and_then(|v| v.as_s().ok())
            .and_then(|d| serde_json::from_str(d).ok()))
    }

    /// The ticket on its team and in the install's own queue copy, like the
    /// gateway's support store saves it.
    async fn save_ticket(&self, t: &Value) -> Result<(), String> {
        let team = t["team_id"].as_str().unwrap_or_default();
        let id = t["id"].as_str().unwrap_or_default();
        let data = t.to_string();
        for (pk, sk) in [
            (team.to_string(), format!("{META_PREFIX}{id}")),
            (QUEUE_PK.to_string(), id.to_string()),
        ] {
            self.dynamo
                .put_item()
                .table_name(self.table)
                .item("pk", s(&pk))
                .item("sk", s(&sk))
                .item("team_id", s(team))
                .item("status", s(t["status"].as_str().unwrap_or("open")))
                .item("updated_at", s(t["updated_at"].as_str().unwrap_or("")))
                .item("data", s(&data))
                .send()
                .await
                .map_err(derr)?;
        }
        Ok(())
    }
}

/// A ticket's data after support's update: status, latest message and count.
pub fn ticket_after(mut t: Value, u: &Update) -> Value {
    if !u.status.is_empty() {
        t["status"] = json!(u.status);
    }
    t["updated_at"] = json!(u.created_at);
    if u.kind == "message" {
        t["last_actor"] = json!("support");
        let n = t["message_count"].as_u64().unwrap_or(0) + 1;
        t["message_count"] = json!(n);
        t["last_snippet"] = json!(clip(&one_line(&u.body), 160));
        t["last_author_name"] = json!("CoderHelm support");
    }
    t
}

/// The message support's update adds to the install's thread.
pub fn message_of(u: &Update) -> Value {
    json!({
        "id": u.message_id,
        "kind": u.kind,
        "author": "support",
        "author_email": "",
        "author_name": "CoderHelm support",
        "body": u.body,
        "created_at": u.created_at,
    })
}

impl Local for DynamoLocal<'_> {
    async fn outbox(&self) -> Result<Vec<OutboxEntry>, String> {
        let r = self
            .dynamo
            .query()
            .table_name(self.table)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", s(OUTBOX_PK))
            .limit(100)
            .send()
            .await
            .map_err(derr)?;
        Ok(r.items()
            .iter()
            .filter_map(|i| i.get("data").and_then(|v| v.as_s().ok()))
            .filter_map(|d| serde_json::from_str(d).ok())
            .collect())
    }

    async fn remove(&self, e: &OutboxEntry) -> Result<(), String> {
        self.dynamo
            .delete_item()
            .table_name(self.table)
            .key("pk", s(OUTBOX_PK))
            .key("sk", s(&e.sk()))
            .send()
            .await
            .map_err(derr)?;
        Ok(())
    }

    async fn failed(&self, e: &OutboxEntry, error: &str) -> Result<(), String> {
        let mut e = e.clone();
        e.attempts += 1;
        e.last_error = clip(error, 300);
        self.enqueue(&e).await
    }

    async fn set_delivery(
        &self,
        team_id: &str,
        ticket_id: &str,
        d: &Delivery,
    ) -> Result<(), String> {
        let Some(mut t) = self.ticket_data(team_id, ticket_id).await? else {
            return Ok(());
        };
        t["coderhelm"] = serde_json::to_value(d).unwrap_or(Value::Null);
        self.save_ticket(&t).await
    }

    async fn cursor(&self) -> Result<String, String> {
        let r = self
            .dynamo
            .get_item()
            .table_name(self.table)
            .key("pk", s(SYNC_PK))
            .key("sk", s(SYNC_SK))
            .send()
            .await
            .map_err(derr)?;
        Ok(r.item()
            .and_then(|i| i.get("since"))
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default())
    }

    async fn set_cursor(&self, cursor: &str) -> Result<(), String> {
        self.dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", s(SYNC_PK))
            .item("sk", s(SYNC_SK))
            .item("since", s(cursor))
            .send()
            .await
            .map_err(derr)?;
        Ok(())
    }

    async fn ticket_team(&self, ticket_id: &str) -> Result<Option<String>, String> {
        let r = self
            .dynamo
            .get_item()
            .table_name(self.table)
            .key("pk", s(QUEUE_PK))
            .key("sk", s(ticket_id))
            .send()
            .await
            .map_err(derr)?;
        Ok(r.item()
            .and_then(|i| i.get("team_id"))
            .and_then(|v| v.as_s().ok())
            .cloned())
    }

    async fn apply(&self, team_id: &str, u: &Update) -> Result<Option<Value>, String> {
        let Some(t) = self.ticket_data(team_id, &u.ticket_id).await? else {
            return Ok(None);
        };
        let sk = format!(
            "{MSG_PREFIX}{}#{}#{}",
            u.ticket_id, u.created_at, u.message_id
        );
        // Once only: a second poll of the same answer adds nothing and emails no one.
        let put = self
            .dynamo
            .put_item()
            .table_name(self.table)
            .item("pk", s(team_id))
            .item("sk", s(&sk))
            .item("data", s(&message_of(u).to_string()))
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await;
        match put {
            Ok(_) => {}
            Err(e)
                if e.as_service_error()
                    .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
            {
                return Ok(None)
            }
            Err(e) => return Err(derr(e)),
        }
        let after = ticket_after(t, u);
        self.save_ticket(&after).await?;
        Ok(Some(after))
    }
}

/// CoderHelm's intake through the install's registration (`hybrid::client`).
pub struct HostedClient<'a> {
    pub http: &'a reqwest::Client,
    pub creds: crate::hybrid::client::Credentials,
}

impl Hosted for HostedClient<'_> {
    async fn post(&self, path: &str, body: &Value) -> Result<Value, String> {
        crate::hybrid::client::call(self.http, &self.creds, "POST", path, Some(body))
            .await
            .map_err(|e| e.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Mem {
        outbox: Mutex<Vec<OutboxEntry>>,
        delivery: Mutex<HashMap<String, Delivery>>,
        cursor: Mutex<String>,
        tickets: Mutex<HashMap<String, Value>>,
        applied: Mutex<Vec<String>>,
    }

    impl Local for Mem {
        async fn outbox(&self) -> Result<Vec<OutboxEntry>, String> {
            Ok(self.outbox.lock().unwrap().clone())
        }
        async fn remove(&self, e: &OutboxEntry) -> Result<(), String> {
            self.outbox.lock().unwrap().retain(|x| x.sk() != e.sk());
            Ok(())
        }
        async fn failed(&self, e: &OutboxEntry, error: &str) -> Result<(), String> {
            for x in self.outbox.lock().unwrap().iter_mut() {
                if x.sk() == e.sk() {
                    x.attempts += 1;
                    x.last_error = error.to_string();
                }
            }
            Ok(())
        }
        async fn set_delivery(
            &self,
            _team: &str,
            ticket: &str,
            d: &Delivery,
        ) -> Result<(), String> {
            self.delivery
                .lock()
                .unwrap()
                .insert(ticket.into(), d.clone());
            Ok(())
        }
        async fn cursor(&self) -> Result<String, String> {
            Ok(self.cursor.lock().unwrap().clone())
        }
        async fn set_cursor(&self, c: &str) -> Result<(), String> {
            *self.cursor.lock().unwrap() = c.into();
            Ok(())
        }
        async fn ticket_team(&self, id: &str) -> Result<Option<String>, String> {
            Ok(self
                .tickets
                .lock()
                .unwrap()
                .get(id)
                .map(|_| "TEAM#acme".to_string()))
        }
        async fn apply(&self, _team: &str, u: &Update) -> Result<Option<Value>, String> {
            let key = format!("{}#{}", u.ticket_id, u.message_id);
            if self.applied.lock().unwrap().contains(&key) {
                return Ok(None);
            }
            self.applied.lock().unwrap().push(key);
            let mut g = self.tickets.lock().unwrap();
            let t = g.get(&u.ticket_id).cloned().unwrap();
            let after = ticket_after(t, u);
            g.insert(u.ticket_id.clone(), after.clone());
            Ok(Some(after))
        }
    }

    /// CoderHelm, up or down; records what it got.
    #[derive(Default)]
    struct Fake {
        down: Mutex<bool>,
        got: Mutex<Vec<(String, Value)>>,
        updates: Mutex<Vec<Update>>,
    }

    impl Hosted for Fake {
        async fn post(&self, path: &str, body: &Value) -> Result<Value, String> {
            if *self.down.lock().unwrap() {
                return Err("CoderHelm's servers didn't answer".into());
            }
            self.got
                .lock()
                .unwrap()
                .push((path.to_string(), body.clone()));
            if path == UPDATES_PATH {
                return Ok(
                    json!({ "updates": *self.updates.lock().unwrap(), "now": "2026-11-03T10:00:00.000Z" }),
                );
            }
            Ok(json!({ "ok": true }))
        }
    }

    fn ticket() -> TicketOut {
        TicketOut {
            ticket_id: "01T".into(),
            message_id: "01M".into(),
            subject: "Run stopped".into(),
            body: "It stopped before the PR.".into(),
            name: "Ann".into(),
            email: "ann@acme.dev".into(),
            page: "/runs/detail".into(),
            priority: "normal".into(),
            items: vec![],
            version: "v0.8.20".into(),
            created_at: "2026-11-03T09:00:00.000Z".into(),
        }
    }

    fn reply() -> MessageOut {
        MessageOut {
            ticket_id: "01T".into(),
            message_id: "01R".into(),
            body: "Any news?".into(),
            name: "Ann".into(),
            email: "ann@acme.dev".into(),
            items: vec![],
            created_at: "2026-11-03T09:05:00.000Z".into(),
        }
    }

    #[tokio::test]
    async fn offline_then_delivered_in_order() {
        let local = Mem::default();
        local
            .outbox
            .lock()
            .unwrap()
            .push(OutboxEntry::message("TEAM#acme", &reply()));
        local
            .outbox
            .lock()
            .unwrap()
            .push(OutboxEntry::ticket("TEAM#acme", &ticket()));
        let hosted = Fake::default();
        *hosted.down.lock().unwrap() = true;

        let r = flush(&local, &hosted).await.unwrap();
        // The ticket failed, so its reply waited: one attempt, nothing sent.
        assert_eq!((r.sent, r.failed), (0, 1));
        assert_eq!(local.outbox.lock().unwrap().len(), 2);
        assert_eq!(local.delivery.lock().unwrap()["01T"].state, "sending");
        assert!(!local.delivery.lock().unwrap()["01T"].error.is_empty());

        *hosted.down.lock().unwrap() = false;
        let r = flush(&local, &hosted).await.unwrap();
        assert_eq!((r.sent, r.failed), (2, 0));
        assert!(local.outbox.lock().unwrap().is_empty());
        assert_eq!(local.delivery.lock().unwrap()["01T"].state, "sent");
        let got = hosted.got.lock().unwrap();
        assert_eq!(got[0].0, TICKETS_PATH);
        assert_eq!(got[1].0, MESSAGES_PATH);
    }

    #[tokio::test]
    async fn an_answer_is_added_and_emailed_once() {
        let local = Mem::default();
        local.tickets.lock().unwrap().insert(
            "01T".into(),
            json!({ "id": "01T", "subject": "Run stopped", "status": "open", "message_count": 1,
                    "opened_by_email": "ann@acme.dev", "participants": [{ "email": "bo@acme.dev" }] }),
        );
        let hosted = Fake::default();
        hosted.updates.lock().unwrap().push(Update {
            ticket_id: "01T".into(),
            message_id: "01S".into(),
            kind: "message".into(),
            body: "Fixed in v0.8.21.".into(),
            created_at: "2026-11-03T09:30:00.000Z".into(),
            status: "waiting".into(),
        });
        // An answer for a ticket this install doesn't have is skipped.
        hosted.updates.lock().unwrap().push(Update {
            ticket_id: "OTHER".into(),
            message_id: "x".into(),
            ..Default::default()
        });

        let first = poll(&local, &hosted).await.unwrap();
        assert_eq!(first.len(), 1);
        let (t, u) = &first[0];
        assert_eq!(t["status"], "waiting");
        assert_eq!(t["message_count"], 2);
        assert_eq!(t["last_author_name"], "CoderHelm support");
        assert_eq!(*local.cursor.lock().unwrap(), "2026-11-03T10:00:00.000Z");
        let mails = mails_for(t, u, "https://coderhelm.acme.dev");
        assert_eq!(
            mails.iter().map(|m| m.to.as_str()).collect::<Vec<_>>(),
            ["ann@acme.dev", "bo@acme.dev"]
        );
        match &mails[0].ses {
            crate::mail::SesBody::Simple { text, .. } => {
                assert!(text.contains("https://coderhelm.acme.dev/support/ticket/?id=01T"));
                assert!(!text.contains("app.coderhelm.com"));
            }
            _ => panic!("simple email"),
        }

        // The same answer again: nothing new, no second email.
        let again = poll(&local, &hosted).await.unwrap();
        assert!(again.is_empty());
    }

    #[test]
    fn the_hosted_side_takes_only_ticket_fields_in_bounds() {
        assert!(ticket().check().is_ok());
        let mut t = ticket();
        t.subject = "x".repeat(MAX_SUBJECT + 1);
        assert!(t.check().is_err());
        let mut t = ticket();
        t.email = "not an email".into();
        assert!(t.check().is_err());
        let mut t = ticket();
        t.ticket_id = "../x".into();
        assert!(t.check().is_err());
        let mut t = ticket();
        t.items = vec![ItemOut {
            kind: "run".into(),
            id: "r".into(),
            error: "e".repeat(MAX_ERROR + 1),
            ..Default::default()
        }];
        assert!(t.check().is_err());
        assert!(reply().check().is_ok());
    }

    #[test]
    fn an_attached_run_leaves_with_its_error_cut_and_scrubbed() {
        let i = ItemOut {
            kind: "run".into(),
            id: "r1".into(),
            label: "Fix  titles".into(),
            error: format!("token=abc {}", "y".repeat(3000)),
            ..Default::default()
        }
        .clean(|s| s.replace("token=abc", "token=[hidden]"));
        assert_eq!(i.label, "Fix titles");
        assert!(i.error.starts_with("token=[hidden]"));
        assert_eq!(i.error.chars().count(), MAX_ERROR);
        assert!(items_text(&[i]).contains("Error: token=[hidden]"));
    }

    #[test]
    fn install_teams_are_recognized() {
        assert_eq!(install_of_team(&install_team("ins_abc")), Some("ins_abc"));
        assert_eq!(install_of_team("TEAM#acme"), None);
        assert_eq!(install_of_team("SUPPORT_INSTALL#"), None);
    }
}
