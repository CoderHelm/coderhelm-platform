//! Drafts, their versions, and the approval gate bound to one version.
//!
//! A plugin run saves what it makes as drafts (`ARTIFACT#<id>`): a blog
//! post, LinkedIn variants, an email, an image, or a consequential action
//! it asked for. Every edit is a new version (`ARTVER#<id>#<n>`). Nothing a
//! draft says reaches anyone until a person approves that exact version:
//!
//! - the approval (`APPROVAL#<id>`, kept on the draft too) names the version
//!   and its content hash; editing the draft makes a new version, which
//!   needs its own approval, and publishing checks the hash again;
//! - the approver can never be the person (or agent) who asked;
//! - only the plugin's approvers (or the team's admins when it names none)
//!   approve, and a draft can need more than one approval;
//! - a stop is sticky: a stopped draft can't be edited, approved or
//!   published again, and its schedule is deleted;
//! - an approval can publish now or at a time (a one-off schedule that a
//!   cancel deletes).
//!
//! Every step is written to the audit stream.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::audit::{self, action, ActorKind, Event};
use super::install::Approvers;
use super::kv::{get_as, put_as, Expect, Kv, KvError};
use super::schedule::{self, Queue, Scheduler};
use super::{code, new_id, store_err, Refusal};

pub const PREFIX: &str = "ARTIFACT#";
pub const VERSION_PREFIX: &str = "ARTVER#";
pub const APPROVAL_PREFIX: &str = "APPROVAL#";
/// Largest draft text kept.
pub const MAX_CONTENT: usize = 100_000;
/// The latest a publish can be scheduled.
pub const MAX_SCHEDULE_DAYS: i64 = 90;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    #[default]
    Draft,
    InReview,
    Approved,
    Scheduled,
    Publishing,
    Published,
    Rejected,
    Stopped,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalStatus {
    #[default]
    Pending,
    Approved,
    Rejected,
    Stopped,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decision {
    pub by: String,
    pub at: String,
    /// `dashboard`, `email` or `chat`.
    pub via: String,
}

/// The gate for one version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Approval {
    pub id: String,
    pub artifact_id: String,
    pub version: u32,
    pub content_hash: String,
    pub status: ApprovalStatus,
    pub requested_by: String,
    pub requested_at: String,
    #[serde(default = "one")]
    pub min: u32,
    #[serde(default)]
    pub approvals: Vec<Decision>,
    #[serde(default)]
    pub decided_by: String,
    #[serde(default)]
    pub decided_at: String,
    #[serde(default)]
    pub via: String,
    #[serde(default)]
    pub publish_at: Option<String>,
    #[serde(default)]
    pub reason: String,
}

fn one() -> u32 {
    1
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Artifact {
    pub id: String,
    pub plugin_id: String,
    #[serde(default)]
    pub plugin_name: String,
    #[serde(default)]
    pub plugin_version: String,
    #[serde(default)]
    pub plugin_hash: String,
    #[serde(default)]
    pub run_id: String,
    /// A draft kind (`blog`, `social`, ...) or `action`.
    pub kind: String,
    pub title: String,
    pub status: Status,
    /// The current version.
    pub version: u32,
    /// Who (or which agent) asked for it: the run's requester.
    pub requested_by: String,
    /// The consequential tool that sends it, when it is published.
    #[serde(default)]
    pub target: Option<String>,
    /// For an action: the arguments the agent asked to call it with.
    #[serde(default)]
    pub action_args: Option<Value>,
    /// The channel it is for (`linkedin`, `blog`, ...).
    #[serde(default)]
    pub channel: String,
    #[serde(default)]
    pub approval: Option<Approval>,
    #[serde(default)]
    pub stopped: bool,
    #[serde(default)]
    pub stop_reason: String,
    #[serde(default)]
    pub publish_at: Option<String>,
    #[serde(default)]
    pub schedule_name: Option<String>,
    #[serde(default)]
    pub published_at: Option<String>,
    #[serde(default)]
    pub result_url: Option<String>,
    #[serde(default)]
    pub error: String,
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub version: u32,
    pub title: String,
    pub content: String,
    pub content_hash: String,
    pub author: String,
    pub created_at: String,
}

/// Who is acting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Actor {
    pub email: String,
    pub is_admin: bool,
}

impl Actor {
    pub fn new(email: &str, is_admin: bool) -> Self {
        Self {
            email: email.trim().to_ascii_lowercase(),
            is_admin,
        }
    }
}

fn sk(id: &str) -> String {
    format!("{PREFIX}{id}")
}
fn vsk(id: &str, v: u32) -> String {
    format!("{VERSION_PREFIX}{id}#{v:05}")
}

fn conflict(e: KvError) -> Refusal {
    match e {
        KvError::Conflict => Refusal::new(
            code::CONFLICT,
            "Someone acted on this draft meanwhile. Reload it.",
        ),
        other => store_err(other),
    }
}

pub async fn get(kv: &dyn Kv, team: &str, id: &str) -> Result<(Artifact, u64), Refusal> {
    get_as::<Artifact>(kv, team, &sk(id))
        .await
        .map_err(store_err)?
        .ok_or_else(|| Refusal::new(code::NOT_FOUND, "That draft doesn't exist."))
}

pub async fn versions(kv: &dyn Kv, team: &str, id: &str) -> Result<Vec<Version>, Refusal> {
    super::kv::query_as::<Version>(kv, team, &format!("{VERSION_PREFIX}{id}#"))
        .await
        .map_err(store_err)
}

pub async fn version(kv: &dyn Kv, team: &str, id: &str, v: u32) -> Result<Version, Refusal> {
    get_as::<Version>(kv, team, &vsk(id, v))
        .await
        .map_err(store_err)?
        .map(|(v, _)| v)
        .ok_or_else(|| Refusal::new(code::NOT_FOUND, "That version doesn't exist."))
}

/// Drafts, newest first, filtered by status and plugin.
pub async fn list(
    kv: &dyn Kv,
    team: &str,
    status: Option<Status>,
    plugin: Option<&str>,
    run: Option<&str>,
) -> Result<Vec<Artifact>, Refusal> {
    let mut v: Vec<Artifact> = super::kv::query_as::<Artifact>(kv, team, PREFIX)
        .await
        .map_err(store_err)?
        .into_iter()
        .filter(|a| status.map(|s| a.status == s).unwrap_or(true))
        .filter(|a| plugin.map(|p| a.plugin_id == p).unwrap_or(true))
        .filter(|a| run.map(|r| a.run_id == r).unwrap_or(true))
        .collect();
    v.sort_by(|a, b| b.id.cmp(&a.id));
    Ok(v)
}

/// Where a draft came from.
#[derive(Debug, Clone, Default)]
pub struct Origin {
    pub plugin_id: String,
    pub plugin_name: String,
    pub plugin_version: String,
    pub plugin_hash: String,
    pub run_id: String,
    pub requested_by: String,
    /// How many approvals its drafts need.
    pub min: u32,
}

/// What to save.
#[derive(Debug, Clone, Default)]
pub struct NewDraft {
    pub kind: String,
    pub title: String,
    pub content: String,
    pub channel: String,
    pub target: Option<String>,
    pub action_args: Option<Value>,
}

/// Saves a new draft (version 1) and puts it in review with an approval
/// for that version. The agent that wrote it is its author; the run's
/// requester is who asked.
pub async fn save(kv: &dyn Kv, team: &str, o: &Origin, d: NewDraft) -> Result<Artifact, Refusal> {
    if d.content.len() > MAX_CONTENT {
        return Err(Refusal::new(code::INVALID, "That draft is too long."));
    }
    if d.title.trim().is_empty() {
        return Err(Refusal::new(code::INVALID, "A draft needs a title."));
    }
    let id = new_id();
    let now = super::now_rfc3339();
    let author = format!("plugin:{}", o.plugin_id);
    let ver = Version {
        version: 1,
        title: crate::truncate_str(d.title.trim(), 200).to_string(),
        content: d.content.clone(),
        content_hash: crate::content_hash(&d.content),
        author: author.clone(),
        created_at: now.clone(),
    };
    put_as(kv, team, &vsk(&id, 1), &ver, Expect::Absent)
        .await
        .map_err(store_err)?;
    let mut a = Artifact {
        id: id.clone(),
        plugin_id: o.plugin_id.clone(),
        plugin_name: o.plugin_name.clone(),
        plugin_version: o.plugin_version.clone(),
        plugin_hash: o.plugin_hash.clone(),
        run_id: o.run_id.clone(),
        kind: d.kind.clone(),
        title: ver.title.clone(),
        status: Status::Draft,
        version: 1,
        requested_by: o.requested_by.clone(),
        target: d.target,
        action_args: d.action_args,
        channel: d.channel,
        created_at: now.clone(),
        updated_at: now,
        ..Default::default()
    };
    open_review(&mut a, &ver, o.min.max(1));
    put_as(kv, team, &sk(&id), &a, Expect::Absent)
        .await
        .map_err(store_err)?;
    write_approval(kv, team, &a).await;
    audit::record(
        kv,
        team,
        Event::new(&author, ActorKind::Agent, action::DRAFT_SAVED, "ok")
            .plugin(&a.plugin_id, &a.plugin_version)
            .run(&a.run_id)
            .artifact(&a.id)
            .approval(a.approval.as_ref().map(|x| x.id.as_str()).unwrap_or(""))
            .detail(format!(
                "{} {}: {}",
                a.kind,
                if a.channel.is_empty() { "" } else { &a.channel },
                a.title
            )),
    )
    .await;
    Ok(a)
}

fn open_review(a: &mut Artifact, v: &Version, min: u32) {
    a.status = Status::InReview;
    a.approval = Some(Approval {
        id: new_id(),
        artifact_id: a.id.clone(),
        version: v.version,
        content_hash: v.content_hash.clone(),
        status: ApprovalStatus::Pending,
        requested_by: a.requested_by.clone(),
        requested_at: super::now_rfc3339(),
        min,
        ..Default::default()
    });
}

/// The `APPROVAL#` record (a signed link finds its draft through it).
async fn write_approval(kv: &dyn Kv, team: &str, a: &Artifact) {
    if let Some(ap) = &a.approval {
        let _ = put_as(
            kv,
            team,
            &format!("{APPROVAL_PREFIX}{}", ap.id),
            ap,
            Expect::Any,
        )
        .await;
    }
}

/// The draft an approval belongs to.
pub async fn by_approval(
    kv: &dyn Kv,
    team: &str,
    approval_id: &str,
) -> Result<(Artifact, u64), Refusal> {
    let ap = get_as::<Approval>(kv, team, &format!("{APPROVAL_PREFIX}{approval_id}"))
        .await
        .map_err(store_err)?
        .map(|(a, _)| a)
        .ok_or_else(|| Refusal::new(code::NOT_FOUND, "That approval doesn't exist."))?;
    get(kv, team, &ap.artifact_id).await
}

fn same(a: &str, b: &str) -> bool {
    a.trim().eq_ignore_ascii_case(b.trim())
}

/// Why `actor` can't approve this draft's version `version` now, or Ok.
pub fn may_approve(
    a: &Artifact,
    approvers: &Approvers,
    actor: &Actor,
    version: u32,
) -> Result<(), Refusal> {
    if a.stopped || a.status == Status::Stopped {
        return Err(Refusal::new(
            code::STOPPED,
            "This draft was stopped. A stop is permanent.",
        ));
    }
    if a.status != Status::InReview {
        return Err(Refusal::new(
            code::WRONG_STATE,
            "This draft isn't waiting for approval.",
        ));
    }
    let ap = a
        .approval
        .as_ref()
        .ok_or_else(|| Refusal::new(code::WRONG_STATE, "This draft isn't waiting for approval."))?;
    if version != a.version || ap.version != a.version {
        return Err(Refusal::new(
            code::STALE_VERSION,
            format!(
                "The draft changed since. Approve version {} instead.",
                a.version
            ),
        ));
    }
    if same(&actor.email, &a.requested_by) {
        return Err(Refusal::new(
            code::SAME_PERSON,
            "You asked for this draft, so someone else has to approve it.",
        ));
    }
    let in_group = if approvers.members.is_empty() {
        actor.is_admin
    } else {
        approvers.members.iter().any(|m| same(m, &actor.email))
    };
    if !in_group {
        return Err(Refusal::new(
            code::NOT_IN_GROUP,
            "You aren't one of this plugin's approvers.",
        ));
    }
    if ap.approvals.iter().any(|d| same(&d.by, &actor.email)) {
        return Err(Refusal::new(
            code::CONFLICT,
            "You already approved this version.",
        ));
    }
    Ok(())
}

/// Requester, approvers and admins may stop or cancel.
fn may_stop(a: &Artifact, approvers: &Approvers, actor: &Actor) -> bool {
    actor.is_admin
        || same(&actor.email, &a.requested_by)
        || approvers.members.iter().any(|m| same(m, &actor.email))
}

/// The publish message for an approved version.
pub fn publish_message(team: &str, a: &Artifact) -> Value {
    json!({
        "type": "plugin_publish",
        "team_id": team,
        "artifact_id": a.id,
        "version": a.version,
        "approval_id": a.approval.as_ref().map(|x| x.id.clone()).unwrap_or_default(),
    })
}

fn ev(actor: &Actor, act: &str, result: &str, a: &Artifact) -> Event {
    Event::new(&actor.email, ActorKind::Person, act, result)
        .plugin(&a.plugin_id, &a.plugin_version)
        .run(&a.run_id)
        .artifact(&a.id)
        .approval(a.approval.as_ref().map(|x| x.id.as_str()).unwrap_or(""))
}

/// Approves `version`. When the draft has had all the approvals it needs:
/// with a target it publishes now, or at `publish_at` through a one-off
/// schedule; without one it is simply approved.
#[allow(clippy::too_many_arguments)]
pub async fn approve(
    kv: &dyn Kv,
    sched: &dyn Scheduler,
    queue: &dyn Queue,
    team: &str,
    id: &str,
    version: u32,
    publish_at: Option<DateTime<Utc>>,
    approvers: &Approvers,
    actor: &Actor,
    via: &str,
    now: DateTime<Utc>,
) -> Result<Artifact, Refusal> {
    let (mut a, rev) = get(kv, team, id).await?;
    if let Err(r) = may_approve(&a, approvers, actor, version) {
        audit::record(
            kv,
            team,
            ev(actor, action::APPROVAL_REFUSED, "refused", &a)
                .detail(format!("{}: {}", r.code, r.error)),
        )
        .await;
        return Err(r);
    }
    if let Some(t) = publish_at {
        if t <= now {
            return Err(Refusal::new(
                code::INVALID,
                "Pick a time in the future, or approve to publish now.",
            ));
        }
        if t > now + chrono::Duration::days(MAX_SCHEDULE_DAYS) {
            return Err(Refusal::new(
                code::INVALID,
                format!("A publish can be scheduled up to {MAX_SCHEDULE_DAYS} days ahead."),
            ));
        }
        if a.target.is_none() {
            return Err(Refusal::new(
                code::INVALID,
                "This draft isn't published by the plugin, so it has no publish time.",
            ));
        }
    }
    // The version's text must still be what the approval names.
    let v = version_of(kv, team, &a).await?;
    let ap = a.approval.as_mut().expect("checked");
    if v.content_hash != ap.content_hash {
        return Err(Refusal::new(
            code::STALE_VERSION,
            "The draft's text changed. Reload it.",
        ));
    }
    ap.approvals.push(Decision {
        by: actor.email.clone(),
        at: now.to_rfc3339(),
        via: via.to_string(),
    });
    let done = ap.approvals.len() as u32 >= ap.min.max(approvers.min).max(1);
    if done {
        ap.status = ApprovalStatus::Approved;
        ap.decided_by = actor.email.clone();
        ap.decided_at = now.to_rfc3339();
        ap.via = via.to_string();
        ap.publish_at = publish_at.map(|t| t.to_rfc3339());
        a.status = Status::Approved;
    }
    a.updated_at = now.to_rfc3339();
    // Decide first (the revision makes two approvers acting at once safe),
    // then schedule.
    let rev = put_as(kv, team, &sk(id), &a, Expect::Rev(rev))
        .await
        .map_err(conflict)?;
    write_approval(kv, team, &a).await;
    audit::record(
        kv,
        team,
        ev(
            actor,
            action::APPROVED,
            if done { "ok" } else { "waiting" },
            &a,
        )
        .detail(format!(
            "version {} via {via}{}",
            a.version,
            if done { "" } else { ", needs more approvals" }
        )),
    )
    .await;
    if !done || a.target.is_none() {
        return Ok(a);
    }
    let msg = publish_message(team, &a);
    match publish_at {
        Some(t) => {
            let name = schedule::name("pub", team, id);
            sched.at(&name, t, &msg).await.map_err(|e| {
                Refusal::new(code::STORE, format!("Couldn't schedule the publish: {e}"))
            })?;
            a.status = Status::Scheduled;
            a.publish_at = Some(t.to_rfc3339());
            a.schedule_name = Some(name);
            put_as(kv, team, &sk(id), &a, Expect::Rev(rev))
                .await
                .map_err(conflict)?;
            audit::record(
                kv,
                team,
                ev(actor, action::SCHEDULED, "ok", &a)
                    .detail(format!("publishes at {}", t.to_rfc3339())),
            )
            .await;
        }
        None => {
            queue.send(&msg).await.map_err(|e| {
                Refusal::new(code::STORE, format!("Couldn't start the publish: {e}"))
            })?;
        }
    }
    Ok(a)
}

async fn version_of(kv: &dyn Kv, team: &str, a: &Artifact) -> Result<Version, Refusal> {
    version(kv, team, &a.id, a.version).await
}

/// Rejects `version`.
pub async fn reject(
    kv: &dyn Kv,
    team: &str,
    id: &str,
    version: u32,
    reason: &str,
    approvers: &Approvers,
    actor: &Actor,
    via: &str,
) -> Result<Artifact, Refusal> {
    let (mut a, rev) = get(kv, team, id).await?;
    if let Err(r) = may_approve(&a, approvers, actor, version) {
        // Rejecting your own draft is allowed (it never goes out).
        if !(r.code == code::SAME_PERSON || (r.code == code::CONFLICT)) {
            return Err(r);
        }
    }
    let now = super::now_rfc3339();
    if let Some(ap) = a.approval.as_mut() {
        ap.status = ApprovalStatus::Rejected;
        ap.decided_by = actor.email.clone();
        ap.decided_at = now.clone();
        ap.via = via.to_string();
        ap.reason = crate::truncate_str(reason, 500).to_string();
    }
    a.status = Status::Rejected;
    a.updated_at = now;
    put_as(kv, team, &sk(id), &a, Expect::Rev(rev))
        .await
        .map_err(conflict)?;
    write_approval(kv, team, &a).await;
    audit::record(
        kv,
        team,
        ev(actor, action::REJECTED, "ok", &a).detail(reason.to_string()),
    )
    .await;
    Ok(a)
}

/// Stops the draft for good: nothing about it can be approved, edited or
/// published again, and a scheduled publish is deleted.
pub async fn stop(
    kv: &dyn Kv,
    sched: &dyn Scheduler,
    team: &str,
    id: &str,
    reason: &str,
    approvers: &Approvers,
    actor: &Actor,
) -> Result<Artifact, Refusal> {
    let (mut a, rev) = get(kv, team, id).await?;
    if !may_stop(&a, approvers, actor) {
        return Err(Refusal::new(
            code::NOT_IN_GROUP,
            "Only the requester, the plugin's approvers or an admin can stop a draft.",
        ));
    }
    if a.status == Status::Published {
        return Err(Refusal::new(
            code::WRONG_STATE,
            "This draft is already published.",
        ));
    }
    if a.stopped {
        return Ok(a);
    }
    if let Some(name) = a.schedule_name.take() {
        let _ = sched.delete(&name).await;
    }
    a.stopped = true;
    a.stop_reason = crate::truncate_str(reason, 500).to_string();
    a.status = Status::Stopped;
    if let Some(ap) = a.approval.as_mut() {
        ap.status = ApprovalStatus::Stopped;
    }
    a.updated_at = super::now_rfc3339();
    put_as(kv, team, &sk(id), &a, Expect::Rev(rev))
        .await
        .map_err(conflict)?;
    write_approval(kv, team, &a).await;
    audit::record(
        kv,
        team,
        ev(actor, action::STOPPED, "ok", &a).detail(reason.to_string()),
    )
    .await;
    Ok(a)
}

/// Cancels a scheduled publish: the schedule is deleted before its time, so
/// nothing is sent. The draft can be edited and sent for approval again.
pub async fn cancel(
    kv: &dyn Kv,
    sched: &dyn Scheduler,
    team: &str,
    id: &str,
    approvers: &Approvers,
    actor: &Actor,
) -> Result<Artifact, Refusal> {
    let (mut a, rev) = get(kv, team, id).await?;
    if !may_stop(&a, approvers, actor) {
        return Err(Refusal::new(
            code::NOT_IN_GROUP,
            "Only the requester, the plugin's approvers or an admin can cancel.",
        ));
    }
    if a.status != Status::Scheduled {
        return Err(Refusal::new(
            code::WRONG_STATE,
            "Only a scheduled publish can be cancelled.",
        ));
    }
    if let Some(name) = a.schedule_name.take() {
        sched
            .delete(&name)
            .await
            .map_err(|e| Refusal::new(code::STORE, format!("Couldn't cancel the schedule: {e}")))?;
    }
    a.status = Status::Cancelled;
    a.publish_at = None;
    if let Some(ap) = a.approval.as_mut() {
        ap.status = ApprovalStatus::Cancelled;
    }
    a.updated_at = super::now_rfc3339();
    put_as(kv, team, &sk(id), &a, Expect::Rev(rev))
        .await
        .map_err(conflict)?;
    write_approval(kv, team, &a).await;
    audit::record(kv, team, ev(actor, action::CANCELLED, "ok", &a)).await;
    Ok(a)
}

/// Saves an edit as a new version and puts it back in review. An approval
/// for an older version (and its schedule) no longer counts.
#[allow(clippy::too_many_arguments)]
pub async fn edit(
    kv: &dyn Kv,
    sched: &dyn Scheduler,
    team: &str,
    id: &str,
    content: &str,
    title: Option<&str>,
    min: u32,
    actor: &Actor,
) -> Result<Artifact, Refusal> {
    let (mut a, rev) = get(kv, team, id).await?;
    if a.stopped {
        return Err(Refusal::new(
            code::STOPPED,
            "This draft was stopped. A stop is permanent.",
        ));
    }
    if matches!(a.status, Status::Published | Status::Publishing) {
        return Err(Refusal::new(
            code::WRONG_STATE,
            "This draft is already published.",
        ));
    }
    if content.len() > MAX_CONTENT {
        return Err(Refusal::new(code::INVALID, "That draft is too long."));
    }
    if let Some(name) = a.schedule_name.take() {
        let _ = sched.delete(&name).await;
    }
    a.publish_at = None;
    let next = a.version + 1;
    let ver = Version {
        version: next,
        title: title
            .map(|t| crate::truncate_str(t.trim(), 200).to_string())
            .unwrap_or_else(|| a.title.clone()),
        content: content.to_string(),
        content_hash: crate::content_hash(content),
        author: actor.email.clone(),
        created_at: super::now_rfc3339(),
    };
    put_as(kv, team, &vsk(id, next), &ver, Expect::Absent)
        .await
        .map_err(conflict)?;
    a.version = next;
    a.title = ver.title.clone();
    open_review(&mut a, &ver, min.max(1));
    a.updated_at = super::now_rfc3339();
    put_as(kv, team, &sk(id), &a, Expect::Rev(rev))
        .await
        .map_err(conflict)?;
    write_approval(kv, team, &a).await;
    audit::record(
        kv,
        team,
        ev(actor, action::DRAFT_EDITED, "ok", &a).detail(format!("version {next}")),
    )
    .await;
    Ok(a)
}

/// Sends a draft (or a rejected or cancelled one) for approval again.
pub async fn submit(
    kv: &dyn Kv,
    team: &str,
    id: &str,
    min: u32,
    actor: &Actor,
) -> Result<Artifact, Refusal> {
    let (mut a, rev) = get(kv, team, id).await?;
    if a.stopped {
        return Err(Refusal::new(
            code::STOPPED,
            "This draft was stopped. A stop is permanent.",
        ));
    }
    if !matches!(
        a.status,
        Status::Draft | Status::Rejected | Status::Cancelled | Status::Failed
    ) {
        return Err(Refusal::new(
            code::WRONG_STATE,
            "This draft is already in review or done.",
        ));
    }
    let v = version_of(kv, team, &a).await?;
    open_review(&mut a, &v, min.max(1));
    a.error.clear();
    a.updated_at = super::now_rfc3339();
    put_as(kv, team, &sk(id), &a, Expect::Rev(rev))
        .await
        .map_err(conflict)?;
    write_approval(kv, team, &a).await;
    audit::record(kv, team, ev(actor, action::DRAFT_SUBMITTED, "ok", &a)).await;
    Ok(a)
}

/// Saves the draft (used by publish).
pub async fn put(kv: &dyn Kv, team: &str, a: &Artifact, rev: u64) -> Result<u64, Refusal> {
    put_as(kv, team, &sk(&a.id), a, Expect::Rev(rev))
        .await
        .map_err(conflict)
}

/// The draft as the dashboard shows it, with whether `actor` may approve.
pub fn view(
    a: &Artifact,
    versions: &[Version],
    approvers: &Approvers,
    actor: Option<&Actor>,
) -> Value {
    let why = actor.map(|act| {
        may_approve(a, approvers, act, a.version)
            .err()
            .map(|r| r.code)
    });
    json!({
        "id": a.id, "plugin_id": a.plugin_id, "plugin_name": a.plugin_name, "plugin_version": a.plugin_version,
        "run_id": a.run_id, "kind": a.kind, "title": a.title, "status": a.status, "version": a.version,
        "requested_by": a.requested_by, "target": a.target, "channel": a.channel,
        "publish_at": a.publish_at, "published_at": a.published_at, "result_url": a.result_url,
        "error": a.error, "stopped": a.stopped, "stop_reason": a.stop_reason,
        "approval": a.approval.as_ref().map(|ap| json!({
            "id": ap.id, "version": ap.version, "content_hash": ap.content_hash, "status": ap.status,
            "requested_by": ap.requested_by, "decided_by": ap.decided_by, "decided_at": ap.decided_at,
            "via": ap.via, "publish_at": ap.publish_at, "min": ap.min, "approvals": ap.approvals,
        })),
        "versions": versions.iter().map(|v| json!({
            "version": v.version, "title": v.title, "content": v.content, "content_hash": v.content_hash,
            "author": v.author, "created_at": v.created_at,
        })).collect::<Vec<_>>(),
        "can_approve": matches!(why, Some(None)),
        "why_not": why.flatten(),
        "created_at": a.created_at, "updated_at": a.updated_at,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::plugins::kv::Memory;
    use crate::plugins::schedule::{MemoryQueue, MemoryScheduler};

    pub fn origin(requested_by: &str) -> Origin {
        Origin {
            plugin_id: "marketing".into(),
            plugin_name: "marketing".into(),
            plugin_version: "1.2.0".into(),
            plugin_hash: "h".into(),
            run_id: "run1".into(),
            requested_by: requested_by.into(),
            min: 1,
        }
    }

    pub fn post(target: bool) -> NewDraft {
        NewDraft {
            kind: "social".into(),
            title: "Launch: 2.0".into(),
            content: "We shipped 2.0.".into(),
            channel: "linkedin".into(),
            target: target.then(|| "publish_post".to_string()),
            action_args: None,
        }
    }

    fn editors() -> Approvers {
        Approvers {
            members: vec!["editor@a.io".into(), "second@a.io".into()],
            min: 1,
        }
    }

    #[tokio::test]
    async fn the_approver_cannot_be_the_requester_and_must_be_in_the_group() {
        let (kv, s, q) = (
            Memory::new(),
            MemoryScheduler::default(),
            MemoryQueue::default(),
        );
        let a = save(&kv, "T", &origin("editor@a.io"), post(true))
            .await
            .unwrap();
        assert_eq!(a.status, Status::InReview);
        let now = Utc::now();
        let r = approve(
            &kv,
            &s,
            &q,
            "T",
            &a.id,
            1,
            None,
            &editors(),
            &Actor::new("Editor@A.io", true),
            "dashboard",
            now,
        )
        .await
        .unwrap_err();
        assert_eq!(r.code, code::SAME_PERSON);
        let r = approve(
            &kv,
            &s,
            &q,
            "T",
            &a.id,
            1,
            None,
            &editors(),
            &Actor::new("outsider@a.io", false),
            "dashboard",
            now,
        )
        .await
        .unwrap_err();
        assert_eq!(r.code, code::NOT_IN_GROUP);
        // Admins approve only when the plugin names no approvers.
        let r = approve(
            &kv,
            &s,
            &q,
            "T",
            &a.id,
            1,
            None,
            &editors(),
            &Actor::new("admin@a.io", true),
            "dashboard",
            now,
        )
        .await
        .unwrap_err();
        assert_eq!(r.code, code::NOT_IN_GROUP);
        let ok = approve(
            &kv,
            &s,
            &q,
            "T",
            &a.id,
            1,
            None,
            &editors(),
            &Actor::new("second@a.io", false),
            "dashboard",
            now,
        )
        .await
        .unwrap();
        assert_eq!(ok.status, Status::Approved);
        assert_eq!(q.take().len(), 1, "publishes now");
        let refused = audit::for_filter(
            &kv,
            "T",
            &audit::Filter {
                action: Some(action::APPROVAL_REFUSED.into()),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(refused.len(), 3);
    }

    #[tokio::test]
    async fn an_approval_is_bound_to_the_exact_version() {
        let (kv, s, q) = (
            Memory::new(),
            MemoryScheduler::default(),
            MemoryQueue::default(),
        );
        let a = save(&kv, "T", &origin("plugin:marketing"), post(true))
            .await
            .unwrap();
        let ed = Actor::new("editor@a.io", false);
        let a2 = edit(&kv, &s, "T", &a.id, "We shipped 2.0 today.", None, 1, &ed)
            .await
            .unwrap();
        assert_eq!(a2.version, 2);
        assert_eq!(a2.approval.as_ref().unwrap().version, 2);
        let r = approve(
            &kv,
            &s,
            &q,
            "T",
            &a.id,
            1,
            None,
            &editors(),
            &Actor::new("second@a.io", false),
            "email",
            Utc::now(),
        )
        .await
        .unwrap_err();
        assert_eq!(r.code, code::STALE_VERSION);
        assert!(approve(
            &kv,
            &s,
            &q,
            "T",
            &a.id,
            2,
            None,
            &editors(),
            &Actor::new("second@a.io", false),
            "email",
            Utc::now()
        )
        .await
        .is_ok());
        // Editing after approval voids it: back in review for version 3.
        let a3 = edit(&kv, &s, "T", &a.id, "Changed again.", None, 1, &ed)
            .await
            .unwrap();
        assert_eq!((a3.status, a3.version), (Status::InReview, 3));
    }

    #[tokio::test]
    async fn a_stop_is_sticky() {
        let (kv, s, q) = (
            Memory::new(),
            MemoryScheduler::default(),
            MemoryQueue::default(),
        );
        let a = save(&kv, "T", &origin("plugin:marketing"), post(true))
            .await
            .unwrap();
        let now = Utc::now();
        approve(
            &kv,
            &s,
            &q,
            "T",
            &a.id,
            1,
            Some(now + chrono::Duration::hours(2)),
            &editors(),
            &Actor::new("editor@a.io", false),
            "dashboard",
            now,
        )
        .await
        .unwrap();
        assert_eq!(s.names().len(), 1);
        let st = stop(
            &kv,
            &s,
            "T",
            &a.id,
            "off brand",
            &editors(),
            &Actor::new("second@a.io", false),
        )
        .await
        .unwrap();
        assert!(st.stopped);
        assert!(s.names().is_empty(), "the schedule is gone");
        let ed = Actor::new("editor@a.io", false);
        assert_eq!(
            edit(&kv, &s, "T", &a.id, "x", None, 1, &ed)
                .await
                .unwrap_err()
                .code,
            code::STOPPED
        );
        assert_eq!(
            submit(&kv, "T", &a.id, 1, &ed).await.unwrap_err().code,
            code::STOPPED
        );
        assert_eq!(
            approve(
                &kv,
                &s,
                &q,
                "T",
                &a.id,
                1,
                None,
                &editors(),
                &ed,
                "dashboard",
                now
            )
            .await
            .unwrap_err()
            .code,
            code::STOPPED
        );
    }

    #[tokio::test]
    async fn approve_for_a_time_then_cancel_before_it() {
        let (kv, s, q) = (
            Memory::new(),
            MemoryScheduler::default(),
            MemoryQueue::default(),
        );
        let a = save(&kv, "T", &origin("plugin:marketing"), post(true))
            .await
            .unwrap();
        let now = Utc::now();
        let at = now + chrono::Duration::days(1);
        let sch = approve(
            &kv,
            &s,
            &q,
            "T",
            &a.id,
            1,
            Some(at),
            &editors(),
            &Actor::new("editor@a.io", false),
            "dashboard",
            now,
        )
        .await
        .unwrap();
        assert_eq!(sch.status, Status::Scheduled);
        assert!(q.take().is_empty(), "nothing is sent before the time");
        let c = cancel(
            &kv,
            &s,
            "T",
            &a.id,
            &editors(),
            &Actor::new("editor@a.io", false),
        )
        .await
        .unwrap();
        assert_eq!(c.status, Status::Cancelled);
        assert!(
            s.due(at + chrono::Duration::minutes(1)).is_empty(),
            "the cancelled schedule never fires"
        );
        // A time in the past or too far ahead is refused.
        let b = save(&kv, "T", &origin("plugin:marketing"), post(true))
            .await
            .unwrap();
        let past = approve(
            &kv,
            &s,
            &q,
            "T",
            &b.id,
            1,
            Some(now - chrono::Duration::minutes(1)),
            &editors(),
            &Actor::new("editor@a.io", false),
            "dashboard",
            now,
        )
        .await;
        assert_eq!(past.unwrap_err().code, code::INVALID);
    }

    #[tokio::test]
    async fn two_approvals_when_the_plugin_needs_two() {
        let (kv, s, q) = (
            Memory::new(),
            MemoryScheduler::default(),
            MemoryQueue::default(),
        );
        let mut o = origin("plugin:marketing");
        o.min = 2;
        let a = save(&kv, "T", &o, post(true)).await.unwrap();
        let g = Approvers {
            members: vec!["editor@a.io".into(), "second@a.io".into()],
            min: 2,
        };
        let one = approve(
            &kv,
            &s,
            &q,
            "T",
            &a.id,
            1,
            None,
            &g,
            &Actor::new("editor@a.io", false),
            "dashboard",
            Utc::now(),
        )
        .await
        .unwrap();
        assert_eq!(one.status, Status::InReview);
        assert_eq!(
            approve(
                &kv,
                &s,
                &q,
                "T",
                &a.id,
                1,
                None,
                &g,
                &Actor::new("editor@a.io", false),
                "dashboard",
                Utc::now()
            )
            .await
            .unwrap_err()
            .code,
            code::CONFLICT
        );
        let two = approve(
            &kv,
            &s,
            &q,
            "T",
            &a.id,
            1,
            None,
            &g,
            &Actor::new("second@a.io", false),
            "dashboard",
            Utc::now(),
        )
        .await
        .unwrap();
        assert_eq!(two.status, Status::Approved);
    }

    #[tokio::test]
    async fn drafts_are_the_teams_own() {
        let kv = Memory::new();
        let a = save(&kv, "TEAM#a", &origin("x@a.io"), post(false))
            .await
            .unwrap();
        assert_eq!(
            get(&kv, "TEAM#b", &a.id).await.unwrap_err().code,
            code::NOT_FOUND
        );
        assert!(list(&kv, "TEAM#b", None, None, None)
            .await
            .unwrap()
            .is_empty());
        let ap = a.approval.unwrap().id;
        assert!(by_approval(&kv, "TEAM#b", &ap).await.is_err());
        assert!(by_approval(&kv, "TEAM#a", &ap).await.is_ok());
    }
}
