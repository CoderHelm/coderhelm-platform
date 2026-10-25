//! Running a consequential tool, only after its approval.
//!
//! A publish message names a draft, a version and an approval. Before
//! anything is sent, everything is checked again against the stored draft:
//! the plugins flag is on, the draft isn't stopped, the approval is
//! approved, it names this version, the draft is still at this version, and
//! the version's text still hashes to what was approved. The draft is
//! claimed (`publishing`) by revision, so a duplicate delivery sends
//! nothing twice. Then the plugin's tool runs with the approved text, and
//! the outcome is recorded on the draft and in the audit stream.
//!
//! When the plugin reads results back ([`Manifest::results`]), a results
//! check is scheduled for a few days later; what comes back is kept as a
//! lesson the plugin's next runs read.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::audit::{self, action, ActorKind, Event};
use super::connect::{Connections, HttpSend};
use super::drafts::{self, ApprovalStatus, Artifact, Status};
use super::install;
use super::kv::{put_as, query_as, Expect, Kv};
use super::schedule::{self, Scheduler};
use super::{code, flag, new_id, Refusal};
use crate::mcp_credentials::Cipher;
use crate::messages::{PluginPublishMessage, PluginResultsMessage};

pub const LESSON_PREFIX: &str = "PKGLESSON#";
/// Lessons a run reads.
pub const LESSONS_READ: usize = 8;

pub struct Deps<'a> {
    pub kv: &'a dyn Kv,
    pub sched: &'a dyn Scheduler,
    pub http: &'a dyn HttpSend,
    pub conns: &'a dyn Connections,
    pub cipher: &'a dyn Cipher,
}

/// What a publish ended as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Published(Option<String>),
    Refused(String),
    Failed(String),
}

fn refuse_ev(a: &Artifact, why: &str) -> Event {
    Event::new(
        "system",
        ActorKind::System,
        action::PUBLISH_REFUSED,
        "refused",
    )
    .plugin(&a.plugin_id, &a.plugin_version)
    .run(&a.run_id)
    .artifact(&a.id)
    .approval(a.approval.as_ref().map(|x| x.id.as_str()).unwrap_or(""))
    .detail(why.to_string())
}

/// Why this message may not publish this draft, or Ok.
pub fn check(a: &Artifact, msg: &PluginPublishMessage, current_hash: &str) -> Result<(), String> {
    if a.stopped || a.status == Status::Stopped {
        return Err("the draft was stopped".into());
    }
    if !matches!(a.status, Status::Approved | Status::Scheduled) {
        return Err(format!("the draft is {:?}, not approved", a.status).to_lowercase());
    }
    let ap = a.approval.as_ref().ok_or("no approval")?;
    if ap.status != ApprovalStatus::Approved {
        return Err("the approval isn't approved".into());
    }
    if ap.id != msg.approval_id {
        return Err("another approval than the one approved".into());
    }
    if ap.version != msg.version || a.version != msg.version {
        return Err(format!(
            "version {} was approved, the draft is at version {}",
            ap.version, a.version
        ));
    }
    if ap.content_hash != current_hash {
        return Err("the text changed after it was approved".into());
    }
    Ok(())
}

/// Publishes one approved draft version.
pub async fn run(
    d: &Deps<'_>,
    msg: &PluginPublishMessage,
    now: DateTime<Utc>,
) -> Result<Outcome, Refusal> {
    let team = msg.team_id.as_str();
    let (mut a, rev) = drafts::get(d.kv, team, &msg.artifact_id).await?;
    if !flag::enabled(d.kv, team).await {
        audit::record(d.kv, team, refuse_ev(&a, "plugins are off for this team")).await;
        return Ok(Outcome::Refused("flag off".into()));
    }
    let v = drafts::version(d.kv, team, &a.id, msg.version).await?;
    if let Err(why) = check(&a, msg, &v.content_hash) {
        audit::record(d.kv, team, refuse_ev(&a, &why)).await;
        return Ok(Outcome::Refused(why));
    }
    // Claim it: a second delivery finds it publishing and stops here.
    a.status = Status::Publishing;
    a.schedule_name = None;
    let rev = match drafts::put(d.kv, team, &a, rev).await {
        Ok(r) => r,
        Err(r) if r.code == code::CONFLICT => {
            return Ok(Outcome::Refused("already being published".into()))
        }
        Err(r) => return Err(r),
    };
    let target = a.target.clone().unwrap_or_default();
    let result = send(d, team, &a, &v, &target).await;
    let (outcome, act, res, detail) = match &result {
        Ok(url) => {
            a.status = Status::Published;
            a.published_at = Some(now.to_rfc3339());
            a.result_url = url.clone();
            a.error.clear();
            (
                Outcome::Published(url.clone()),
                action::PUBLISHED,
                "ok",
                url.clone().unwrap_or_default(),
            )
        }
        Err(e) => {
            a.status = Status::Failed;
            a.error = crate::truncate_str(e, 500).to_string();
            (
                Outcome::Failed(e.clone()),
                action::PUBLISH_FAILED,
                "failed",
                e.clone(),
            )
        }
    };
    put_as(
        d.kv,
        team,
        &format!("{}{}", drafts::PREFIX, a.id),
        &a,
        Expect::Rev(rev),
    )
    .await
    .map_err(super::store_err)?;
    let risk = crate::tool_registry::Risk::Consequential.as_str();
    audit::record(
        d.kv,
        team,
        Event::new("system", ActorKind::System, act, res)
            .plugin(&a.plugin_id, &a.plugin_version)
            .run(&a.run_id)
            .artifact(&a.id)
            .approval(a.approval.as_ref().map(|x| x.id.as_str()).unwrap_or(""))
            .tool(&target, risk)
            .detail(format!("version {}: {detail}", a.version)),
    )
    .await;
    if result.is_ok() {
        schedule_results(d, team, &a, now).await;
    }
    Ok(outcome)
}

/// Runs the draft's tool with the approved text.
async fn send(
    d: &Deps<'_>,
    team: &str,
    a: &Artifact,
    v: &drafts::Version,
    target: &str,
) -> Result<Option<String>, String> {
    let (p, _) = install::get(d.kv, team, &a.plugin_id)
        .await
        .map_err(|e| e.error)?;
    if !p.active() {
        return Err("the plugin is turned off".into());
    }
    let snap = install::current_snapshot(d.kv, team, &p)
        .await
        .map_err(|e| e.error)?;
    let m = &snap.manifest;
    if let Some((server, tool)) = target.split_once("__") {
        // An action on a connection's tool, asked for in a run.
        if !m.grant().allows(target) {
            return Err(format!("{target} isn't in the plugin's manifest any more"));
        }
        let args = a.action_args.clone().unwrap_or_else(|| json!({}));
        return d.conns.call(team, server, tool, &args).await.map(|_| None);
    }
    let spec = m
        .named_tool(target)
        .ok_or_else(|| format!("{target} isn't in the plugin's manifest any more"))?;
    if m.tool_risk(spec, target) != crate::tool_registry::Risk::Consequential {
        return Err(format!("{target} isn't a consequential tool"));
    }
    let conn = spec.connection.as_deref().unwrap_or("");
    let credential = install::open_setting(d.cipher, team, &p, conn)
        .await
        .ok_or_else(|| format!("The {conn} connection isn't set in the plugin's settings."))?;
    let fields = std::collections::BTreeMap::from([
        ("title", v.title.clone()),
        ("content", v.content.clone()),
        ("kind", a.kind.clone()),
        ("channel", a.channel.clone()),
    ]);
    let mut body = match &spec.body {
        // The plugin's template, with the approved text and the team's
        // non-sensitive settings filled in as text.
        Some(t) => super::manifest::render_body(t, &fields, &p.settings),
        None => json!({
            "title": v.title,
            "content": v.content,
            "kind": a.kind,
            "channel": a.channel,
        }),
    };
    if let (Some(Value::Object(extra)), Some(obj)) = (a.action_args.clone(), body.as_object_mut()) {
        if spec.body.is_none() {
            for (k, val) in extra {
                obj.entry(k).or_insert(val);
            }
        }
    }
    let url = spec.url.as_deref().unwrap_or("");
    let method = spec.method.as_deref().unwrap_or("POST");
    let reply = d
        .http
        .send(team, &credential, method, url, &spec.headers, &body)
        .await?;
    if !(200..300).contains(&reply.status) {
        return Err(format!(
            "{} answered {}: {}",
            url::Url::parse(url)
                .ok()
                .and_then(|u| u.host_str().map(str::to_string))
                .unwrap_or_default(),
            reply.status,
            crate::truncate_str(&reply.body, 300)
        ));
    }
    Ok(reply.url)
}

async fn schedule_results(d: &Deps<'_>, team: &str, a: &Artifact, now: DateTime<Utc>) {
    let Ok((p, _)) = install::get(d.kv, team, &a.plugin_id).await else {
        return;
    };
    let Ok(snap) = install::current_snapshot(d.kv, team, &p).await else {
        return;
    };
    let Some(r) = &snap.manifest.results else {
        return;
    };
    let when = now + chrono::Duration::days(r.after_days as i64);
    let body = json!({ "type": "plugin_results", "team_id": team, "artifact_id": a.id });
    if let Err(e) = d
        .sched
        .at(&schedule::name("res", team, &a.id), when, &body)
        .await
    {
        tracing::warn!(team, artifact = %a.id, error = %e, "Couldn't schedule a results check");
    }
}

/// What a published draft did, kept for the plugin's next runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Lesson {
    pub id: String,
    pub plugin_id: String,
    pub artifact_id: String,
    pub kind: String,
    pub channel: String,
    pub title: String,
    pub result_url: String,
    /// What the analytics tool said, clipped.
    pub numbers: String,
    pub at: String,
}

/// Reads a published draft's results through the plugin's read-only
/// results tool and keeps them as a lesson.
pub async fn results(d: &Deps<'_>, msg: &PluginResultsMessage) -> Result<Option<Lesson>, Refusal> {
    let team = msg.team_id.as_str();
    flag::require(d.kv, team).await?;
    let (a, _) = drafts::get(d.kv, team, &msg.artifact_id).await?;
    if a.status != Status::Published {
        return Ok(None);
    }
    let (p, _) = install::get(d.kv, team, &a.plugin_id).await?;
    let snap = install::current_snapshot(d.kv, team, &p).await?;
    let Some(r) = &snap.manifest.results else {
        return Ok(None);
    };
    let server = r.via.strip_prefix("mcp:").unwrap_or(&r.via);
    // The results tool is read only by the manifest's own check; checked
    // again here before it is called.
    if crate::tool_registry::classify_external(&r.tool) != crate::tool_registry::Risk::Read {
        return Err(Refusal::new(
            code::NOT_GRANTED,
            "The results tool isn't read only.",
        ));
    }
    let args = json!({ "url": a.result_url, "title": a.title, "published_at": a.published_at });
    let text = d
        .conns
        .call(team, server, &r.tool, &args)
        .await
        .map_err(|e| Refusal::new(code::STORE, e))?;
    let lesson = Lesson {
        id: new_id(),
        plugin_id: a.plugin_id.clone(),
        artifact_id: a.id.clone(),
        kind: a.kind.clone(),
        channel: a.channel.clone(),
        title: a.title.clone(),
        result_url: a.result_url.clone().unwrap_or_default(),
        numbers: crate::truncate_str(&crate::live::redact::redact_text(&text).0, 2000).to_string(),
        at: super::now_rfc3339(),
    };
    put_as(
        d.kv,
        team,
        &format!("{LESSON_PREFIX}{}#{}", a.plugin_id, lesson.id),
        &lesson,
        Expect::Absent,
    )
    .await
    .map_err(super::store_err)?;
    audit::record(
        d.kv,
        team,
        Event::new("schedule", ActorKind::Schedule, action::RESULTS, "ok")
            .plugin(&a.plugin_id, &a.plugin_version)
            .artifact(&a.id)
            .tool(&format!("{server}__{}", r.tool), "read")
            .detail(lesson.numbers.clone()),
    )
    .await;
    Ok(Some(lesson))
}

/// The plugin's latest lessons, newest first.
pub async fn lessons(kv: &dyn Kv, team: &str, plugin: &str) -> Vec<Lesson> {
    let mut v = query_as::<Lesson>(kv, team, &format!("{LESSON_PREFIX}{plugin}#"))
        .await
        .unwrap_or_default();
    v.reverse();
    v.truncate(LESSONS_READ);
    v
}
