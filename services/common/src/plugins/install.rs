//! Installing, updating, pinning and rolling back a plugin.
//!
//! - **Install** is admin only and pins the plugin to a snapshot by its
//!   content hash. When the team has two or more admins, another admin
//!   approves it (the rule team tools already use,
//!   [`crate::live::tools::can_approve`]).
//! - **Updates** come from the plugin's source on every push (webhook) or
//!   poll. Wording-only changes (skills, agent text, descriptions) apply at
//!   once, pinned to the new commit, and are recorded in the audit log. A
//!   version that adds or widens a tool, scope, trigger, connection or
//!   output ([`manifest::widens`]) stays waiting while the plugin keeps
//!   running the current version, until an admin approves it.
//! - **Options**: auto-update on or off, track a branch or a tag, or pin to
//!   a commit (a pinned plugin never updates).
//! - **Rollback** to the previous version is one step, and turns
//!   auto-update off so the next poll doesn't undo it.
//! - Installing grants no access by itself: connections are set separately
//!   in the plugin's settings.
//!
//! Records: `PKG#<id>` under the team.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::audit::{self, action, ActorKind, Event};
use super::kv::{self, get_as, put_as, query_as, Expect, Kv};
use super::manifest::{self, GrantDiff, Manifest};
use super::package::{self, RepoRead, Snapshot};
use super::schedule::Scheduler;
use super::source::{self, Track, TrackKind};
use super::{code, store_err, Refusal};
use crate::mcp_credentials::Cipher;

pub const PREFIX: &str = "PKG#";
/// Versions kept in a plugin's history.
const HISTORY: usize = 10;

/// One pinned version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VersionRef {
    pub hash: String,
    pub sha: String,
    pub version: String,
    #[serde(default)]
    pub approved_by: Vec<String>,
    #[serde(default)]
    pub at: String,
}

impl VersionRef {
    fn of(s: &Snapshot) -> Self {
        Self {
            hash: s.hash.clone(),
            sha: s.sha.clone(),
            version: s.manifest.version.clone(),
            approved_by: Vec::new(),
            at: super::now_rfc3339(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    #[default]
    PendingInstall,
    Active,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingKind {
    Install,
    Update,
}

/// A version waiting for an admin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pending {
    pub kind: PendingKind,
    pub version: VersionRef,
    pub requested_by: String,
    pub requested_at: String,
    /// What it adds to the grant, one plain line each.
    #[serde(default)]
    pub widened: Vec<String>,
    #[serde(default)]
    pub diff: GrantDiff,
}

/// Who may approve a plugin's drafts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Approvers {
    /// Emails. Empty: the team's admins.
    #[serde(default)]
    pub members: Vec<String>,
    #[serde(default = "one")]
    pub min: u32,
}

fn one() -> u32 {
    1
}

impl Default for Approvers {
    fn default() -> Self {
        Self {
            members: Vec::new(),
            min: 1,
        }
    }
}

fn yes() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Installed {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub source_id: String,
    /// The plugin's folder in its source.
    #[serde(default)]
    pub path: String,
    pub status: Status,
    #[serde(default)]
    pub current: Option<VersionRef>,
    #[serde(default)]
    pub previous: Option<VersionRef>,
    #[serde(default)]
    pub history: Vec<VersionRef>,
    #[serde(default)]
    pub pending: Option<Pending>,
    /// A newer version seen while auto-update is off.
    #[serde(default)]
    pub available: Option<VersionRef>,
    #[serde(default = "yes")]
    pub auto_update: bool,
    #[serde(default)]
    pub track: Track,
    /// Values for the manifest's settings (not sensitive).
    #[serde(default)]
    pub settings: BTreeMap<String, String>,
    /// Sensitive values, sealed for this team and plugin. Never returned.
    #[serde(default)]
    pub sealed: BTreeMap<String, String>,
    #[serde(default)]
    pub monthly_tokens: u64,
    #[serde(default)]
    pub approvers: Approvers,
    /// Where review cards go: "", "teams", "slack" or "both".
    #[serde(default)]
    pub notify: String,
    pub installed_by: String,
    pub installed_at: String,
    #[serde(default)]
    pub updated_at: String,
}

impl Installed {
    /// The pinned hash, when there is one.
    pub fn hash(&self) -> Option<&str> {
        self.current.as_ref().map(|c| c.hash.as_str())
    }

    pub fn active(&self) -> bool {
        self.status == Status::Active && self.current.is_some()
    }

    fn push_history(&mut self, v: VersionRef) {
        self.history.retain(|h| h.hash != v.hash);
        self.history.insert(0, v);
        self.history.truncate(HISTORY);
    }

    /// The plugin as the dashboard shows it (contract `Installed`). Sealed
    /// values are never included: a sensitive setting shows only `set`.
    pub fn view(&self, snap: Option<&Snapshot>, used_tokens: u64) -> serde_json::Value {
        use serde_json::json;
        let m = snap.map(|s| &s.manifest);
        let tools: Vec<_> = m
            .map(|m| {
                let g = m.grant();
                m.tools
                    .iter()
                    .map(|t| {
                        if let Some(server) = t.server() {
                            let risk = t
                                .allow
                                .iter()
                                .filter_map(|a| g.risk(&format!("{server}__{}", a.replace('*', "x"))))
                                .max()
                                .map(|r| r.as_str())
                                .unwrap_or("read");
                            json!({ "name": t.reference, "kind": "mcp", "ref": t.reference, "allow": t.allow, "risk": risk })
                        } else {
                            let n = t.name.clone().unwrap_or_default();
                            json!({ "name": n, "kind": t.kind, "risk": m.tool_risk(t, &n).as_str(), "description": t.description })
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        let settings: Vec<_> = m
            .map(|m| {
                m.settings
                    .iter()
                    .map(|s| {
                        let set = self.settings.contains_key(&s.key) || self.sealed.contains_key(&s.key);
                        json!({
                            "key": s.key, "type": s.kind, "provider": s.provider, "scopes": s.scopes,
                            "sensitive": s.sensitive, "description": s.description, "set": set,
                            "value": if s.sensitive { None } else { self.settings.get(&s.key) },
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let items = |kind: &str| -> Vec<serde_json::Value> {
            snap.map(|s| {
                s.items
                    .iter()
                    .filter(|i| (kind == "agent") == (i.kind == "agent"))
                    .map(
                        |i| json!({ "path": i.path, "name": i.name, "description": i.description }),
                    )
                    .collect()
            })
            .unwrap_or_default()
        };
        json!({
            "id": self.id, "name": self.name, "description": self.description,
            "source_id": self.source_id, "path": self.path, "status": self.status,
            "version": self.current.as_ref().map(|c| c.version.clone()).unwrap_or_default(),
            "hash": self.current.as_ref().map(|c| c.hash.clone()).unwrap_or_default(),
            "sha": self.current.as_ref().map(|c| c.sha.clone()).unwrap_or_default(),
            "previous": self.previous, "pending": self.pending, "available": self.available,
            "auto_update": self.auto_update, "track": self.track,
            "tools": tools,
            "triggers": m.map(|m| serde_json::to_value(&m.triggers).unwrap_or_default()).unwrap_or_default(),
            "outputs": m.map(|m| serde_json::to_value(&m.outputs).unwrap_or_default()).unwrap_or_default(),
            "settings": settings, "skills": items("skill"), "agents": items("agent"),
            "monthly_tokens": self.monthly_tokens, "used_tokens": used_tokens,
            "approvers": self.approvers, "notify": { "channel": self.notify },
            "installed_by": self.installed_by, "installed_at": self.installed_at, "updated_at": self.updated_at,
        })
    }
}

fn sk(id: &str) -> String {
    format!("{PREFIX}{id}")
}

pub async fn list(kv: &dyn Kv, team: &str) -> Result<Vec<Installed>, Refusal> {
    query_as::<Installed>(kv, team, PREFIX)
        .await
        .map_err(store_err)
}

pub async fn get(kv: &dyn Kv, team: &str, id: &str) -> Result<(Installed, u64), Refusal> {
    get_as::<Installed>(kv, team, &sk(id))
        .await
        .map_err(store_err)?
        .ok_or_else(|| Refusal::new(code::NOT_FOUND, "That plugin isn't installed."))
}

async fn save(kv: &dyn Kv, team: &str, p: &mut Installed, rev: Option<u64>) -> Result<(), Refusal> {
    p.updated_at = super::now_rfc3339();
    let expect = match rev {
        Some(r) => Expect::Rev(r),
        None => Expect::Absent,
    };
    put_as(kv, team, &sk(&p.id), p, expect)
        .await
        .map_err(|e| match e {
            kv::KvError::Conflict => {
                Refusal::new(code::CONFLICT, "The plugin changed meanwhile. Try again.")
            }
            other => store_err(other),
        })?;
    Ok(())
}

/// The snapshot a plugin is pinned to.
pub async fn current_snapshot(kv: &dyn Kv, team: &str, p: &Installed) -> Result<Snapshot, Refusal> {
    let hash = p.hash().ok_or_else(|| {
        Refusal::new(code::WRONG_STATE, "The plugin has no approved version yet.")
    })?;
    package::snapshot(kv, team, hash)
        .await
        .map_err(store_err)?
        .ok_or_else(|| Refusal::new(code::NOT_FOUND, "The plugin's version is no longer kept."))
}

fn ev(by: &str, kind: ActorKind, act: &str, result: &str, p: &Installed, version: &str) -> Event {
    Event::new(by, kind, act, result).plugin(&p.id, version)
}

/// Installs the plugin a source holds at `path`. Pinned to that snapshot;
/// waiting for a second admin when the team has more than one.
pub async fn install(
    kv: &dyn Kv,
    team: &str,
    source_id: &str,
    path: &str,
    by: &str,
    admins: usize,
) -> Result<Installed, Refusal> {
    let (src, _) = source::get(kv, team, source_id).await?;
    let found = src.plugins.iter().find(|f| f.path == path).ok_or_else(|| {
        Refusal::new(
            code::NOT_FOUND,
            "That source has no plugin there. Sync it first.",
        )
    })?;
    if found.hash.is_empty() {
        return Err(Refusal::new(
            code::INVALID,
            format!(
                "That plugin's manifest has problems: {}",
                found.problems.join(" ")
            ),
        ));
    }
    let snap = package::snapshot(kv, team, &found.hash)
        .await
        .map_err(store_err)?
        .ok_or_else(|| Refusal::new(code::NOT_FOUND, "Sync the source again first."))?;
    let id = snap.manifest.id();
    if get_as::<Installed>(kv, team, &sk(&id))
        .await
        .map_err(store_err)?
        .is_some()
    {
        return Err(Refusal::new(
            code::CONFLICT,
            "That plugin is already installed.",
        ));
    }
    let version = VersionRef::of(&snap);
    let min = snap
        .manifest
        .outputs
        .iter()
        .filter_map(|o| o.approval.as_ref().map(|a| a.min.max(1)))
        .max()
        .unwrap_or(1);
    let mut p = Installed {
        id: id.clone(),
        name: snap.manifest.name.clone(),
        description: snap.manifest.description.clone(),
        source_id: source_id.to_string(),
        path: path.to_string(),
        status: Status::PendingInstall,
        track: src.track.clone(),
        auto_update: true,
        monthly_tokens: snap
            .manifest
            .budget
            .as_ref()
            .map(|b| b.monthly_tokens)
            .unwrap_or(0),
        approvers: Approvers {
            members: Vec::new(),
            min,
        },
        installed_by: by.to_string(),
        installed_at: super::now_rfc3339(),
        ..Default::default()
    };
    let needs_second = admins > 1;
    if needs_second {
        p.pending = Some(Pending {
            kind: PendingKind::Install,
            version: version.clone(),
            requested_by: by.to_string(),
            requested_at: super::now_rfc3339(),
            widened: snap.manifest.grant_lines().into_iter().collect(),
            diff: GrantDiff {
                added: snap.manifest.grant_lines().into_iter().collect(),
                removed: Vec::new(),
            },
        });
    } else {
        let mut v = version.clone();
        v.approved_by = vec![by.to_string()];
        p.current = Some(v.clone());
        p.push_history(v);
        p.status = Status::Active;
    }
    save(kv, team, &mut p, None).await?;
    kv.put(kv::TEAM_INDEX, team, &serde_json::json!({}), Expect::Any)
        .await
        .map_err(store_err)?;
    let act = if needs_second {
        action::INSTALL_REQUESTED
    } else {
        action::INSTALLED
    };
    let result = if needs_second { "waiting" } else { "ok" };
    audit::record(
        kv,
        team,
        ev(by, ActorKind::Person, act, result, &p, &version.version).detail(format!(
            "{} pinned to {}",
            p.id,
            &version.hash[..12.min(version.hash.len())]
        )),
    )
    .await;
    Ok(p)
}

/// An admin approves the waiting install or update `hash`. The person who
/// asked for it can't, when the team has another admin.
pub async fn approve(
    kv: &dyn Kv,
    team: &str,
    id: &str,
    hash: &str,
    by: &str,
    admins: usize,
) -> Result<Installed, Refusal> {
    let (mut p, rev) = get(kv, team, id).await?;
    let pending = p
        .pending
        .clone()
        .ok_or_else(|| Refusal::new(code::WRONG_STATE, "Nothing is waiting for approval."))?;
    if pending.version.hash != hash {
        return Err(Refusal::new(
            code::STALE_VERSION,
            "A newer version is waiting now. Review that one.",
        ));
    }
    crate::live::tools::can_approve(&pending.requested_by, by, admins)
        .map_err(|e| Refusal::new(code::NEEDS_SECOND_ADMIN, e))?;
    let mut v = pending.version.clone();
    v.approved_by = vec![pending.requested_by.clone(), by.to_string()];
    v.approved_by.dedup();
    if let Some(cur) = p.current.take() {
        p.previous = Some(cur);
    }
    p.current = Some(v.clone());
    p.push_history(v.clone());
    p.pending = None;
    p.available = None;
    if p.status == Status::PendingInstall {
        p.status = Status::Active;
    }
    save(kv, team, &mut p, Some(rev)).await?;
    let act = match pending.kind {
        PendingKind::Install => action::INSTALLED,
        PendingKind::Update => action::UPDATE_APPROVED,
    };
    audit::record(
        kv,
        team,
        ev(by, ActorKind::Person, act, "ok", &p, &v.version)
            .detail(format!("approved {}", pending.widened.join("; "))),
    )
    .await;
    Ok(p)
}

/// An admin declines the waiting install (the plugin is removed) or update
/// (the plugin stays on its current version).
pub async fn decline(
    kv: &dyn Kv,
    team: &str,
    id: &str,
    hash: &str,
    by: &str,
) -> Result<Option<Installed>, Refusal> {
    let (mut p, rev) = get(kv, team, id).await?;
    let pending = p
        .pending
        .clone()
        .ok_or_else(|| Refusal::new(code::WRONG_STATE, "Nothing is waiting for approval."))?;
    if pending.version.hash != hash {
        return Err(Refusal::new(
            code::STALE_VERSION,
            "A newer version is waiting now.",
        ));
    }
    if pending.kind == PendingKind::Install {
        kv.delete(team, &sk(id)).await.map_err(store_err)?;
        audit::record(
            kv,
            team,
            ev(
                by,
                ActorKind::Person,
                action::INSTALL_DECLINED,
                "ok",
                &p,
                &pending.version.version,
            ),
        )
        .await;
        return Ok(None);
    }
    p.pending = None;
    save(kv, team, &mut p, Some(rev)).await?;
    audit::record(
        kv,
        team,
        ev(
            by,
            ActorKind::Person,
            action::UPDATE_DECLINED,
            "ok",
            &p,
            &pending.version.version,
        ),
    )
    .await;
    Ok(Some(p))
}

/// What happened to a version a sync offered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Offer {
    /// It is the version already pinned.
    Same,
    /// Pinned to a commit: updates are off.
    Pinned,
    /// Auto-update is off: kept as available.
    Available,
    /// Wording only: applied, pinned to the new commit.
    Applied,
    /// Widens the grant: waiting for an admin.
    Waiting(Vec<String>),
}

/// Offers a new snapshot to an installed plugin (a sync found it).
/// `manual`: an admin asked for it (applies even with auto-update off).
pub async fn offer(
    kv: &dyn Kv,
    team: &str,
    id: &str,
    snap: &Snapshot,
    by: &str,
    manual: bool,
) -> Result<Offer, Refusal> {
    let (mut p, rev) = get(kv, team, id).await?;
    if p.status == Status::PendingInstall {
        return Ok(Offer::Same);
    }
    if p.hash() == Some(snap.hash.as_str()) {
        return Ok(Offer::Same);
    }
    if p.track.pinned()
        && snap.sha != p.track.value
        && !p.track.value.is_empty()
        && !snap.sha.starts_with(&p.track.value)
    {
        return Ok(Offer::Pinned);
    }
    if !p.auto_update && !manual {
        let v = VersionRef::of(snap);
        if p.available
            .as_ref()
            .map(|a| a.hash != v.hash)
            .unwrap_or(true)
        {
            p.available = Some(v.clone());
            save(kv, team, &mut p, Some(rev)).await?;
            audit::record(
                kv,
                team,
                ev(
                    "system",
                    ActorKind::System,
                    action::UPDATE_AVAILABLE,
                    "ok",
                    &p,
                    &v.version,
                ),
            )
            .await;
        }
        return Ok(Offer::Available);
    }
    let current = current_snapshot(kv, team, &p).await?;
    let widened = manifest::widens(&current.manifest, &snap.manifest);
    let v = VersionRef::of(snap);
    if widened.is_empty() {
        let mut applied = v.clone();
        applied.approved_by = vec!["wording only".to_string()];
        p.previous = p.current.take();
        p.current = Some(applied.clone());
        p.push_history(applied);
        p.available = None;
        // A waiting widened version is replaced by a newer wording-only one
        // only if it is the same lineage; keep it waiting otherwise.
        save(kv, team, &mut p, Some(rev)).await?;
        audit::record(
            kv,
            team,
            ev(by, actor_kind(by), action::UPDATED, "ok", &p, &v.version).detail(format!(
                "wording only, pinned to {}",
                crate::truncate_str(&v.sha, 12)
            )),
        )
        .await;
        return Ok(Offer::Applied);
    }
    if p.pending
        .as_ref()
        .map(|x| x.version.hash == v.hash)
        .unwrap_or(false)
    {
        return Ok(Offer::Waiting(widened));
    }
    p.pending = Some(Pending {
        kind: PendingKind::Update,
        version: v.clone(),
        requested_by: by.to_string(),
        requested_at: super::now_rfc3339(),
        widened: widened.clone(),
        diff: manifest::diff(&current.manifest, &snap.manifest),
    });
    save(kv, team, &mut p, Some(rev)).await?;
    audit::record(
        kv,
        team,
        ev(
            by,
            actor_kind(by),
            action::UPDATE_WAITING,
            "waiting",
            &p,
            &v.version,
        )
        .detail(widened.join("; ")),
    )
    .await;
    Ok(Offer::Waiting(widened))
}

fn actor_kind(by: &str) -> ActorKind {
    if by.contains('@') {
        ActorKind::Person
    } else {
        ActorKind::System
    }
}

/// Reads the plugin's source at the plugin's own track and offers what is
/// there. The reader is the source's (public, or the team's installation).
pub async fn sync_plugin(
    kv: &dyn Kv,
    reader: &dyn RepoRead,
    team: &str,
    team_installation: Option<u64>,
    id: &str,
    by: &str,
    manual: bool,
) -> Result<Offer, Refusal> {
    let (p, _) = get(kv, team, id).await?;
    let (src, _) = source::get(kv, team, &p.source_id).await?;
    source::check_reader(&src, team_installation)?;
    let git_ref = if p.track.pinned() {
        p.track.value.clone()
    } else {
        p.track.git_ref()
    };
    let sha = reader
        .resolve(&git_ref)
        .await
        .map_err(|e| Refusal::new(code::INVALID, format!("Couldn't read {}: {e}", src.repo)))?;
    if p.current.as_ref().map(|c| c.sha == sha).unwrap_or(false) && !manual {
        return Ok(Offer::Same);
    }
    let pkg = package::read(reader, &sha, &p.path)
        .await
        .map_err(|e| Refusal::new(code::INVALID, e.join(" ")))?;
    if pkg.manifest.id() != p.id {
        return Err(Refusal::new(
            code::INVALID,
            "The plugin at that path now has another name. Install it as a new plugin.",
        ));
    }
    let snap = package::store(kv, team, &p.source_id, &p.path, &sha, &pkg)
        .await
        .map_err(store_err)?;
    offer(kv, team, id, &snap, by, manual).await
}

/// Rolls back to the previous version and turns auto-update off.
pub async fn rollback(kv: &dyn Kv, team: &str, id: &str, by: &str) -> Result<Installed, Refusal> {
    let (mut p, rev) = get(kv, team, id).await?;
    let prev = p.previous.clone().ok_or_else(|| {
        Refusal::new(
            code::WRONG_STATE,
            "There is no previous version to go back to.",
        )
    })?;
    let cur = p.current.take();
    p.current = Some(prev.clone());
    p.previous = cur;
    p.pending = None;
    p.auto_update = false;
    save(kv, team, &mut p, Some(rev)).await?;
    audit::record(
        kv,
        team,
        ev(
            by,
            ActorKind::Person,
            action::ROLLED_BACK,
            "ok",
            &p,
            &prev.version,
        )
        .detail("auto-update turned off; turn it on again to follow the source"),
    )
    .await;
    Ok(p)
}

/// Auto-update and what the plugin tracks. Returns the plugin and whether
/// a sync should run now (the track moved).
pub async fn set_options(
    kv: &dyn Kv,
    team: &str,
    id: &str,
    auto_update: Option<bool>,
    track: Option<Track>,
    by: &str,
) -> Result<(Installed, bool), Refusal> {
    let (mut p, rev) = get(kv, team, id).await?;
    let mut changed = Vec::new();
    let mut resync = false;
    if let Some(a) = auto_update {
        if a != p.auto_update {
            p.auto_update = a;
            changed.push(format!("auto-update {}", if a { "on" } else { "off" }));
            resync |= a;
        }
    }
    if let Some(t) = track {
        t.check()?;
        if t != p.track {
            changed.push(format!(
                "tracks {} {}",
                match t.kind {
                    TrackKind::Branch => "branch",
                    TrackKind::Tag => "tag",
                    TrackKind::Sha => "commit",
                },
                if t.value.is_empty() {
                    "(default)"
                } else {
                    &t.value
                }
            ));
            p.track = t;
            resync = true;
        }
    }
    if changed.is_empty() {
        return Ok((p, false));
    }
    save(kv, team, &mut p, Some(rev)).await?;
    let version = p
        .current
        .as_ref()
        .map(|c| c.version.clone())
        .unwrap_or_default();
    audit::record(
        kv,
        team,
        ev(
            by,
            ActorKind::Person,
            action::OPTIONS_CHANGED,
            "ok",
            &p,
            &version,
        )
        .detail(changed.join(", ")),
    )
    .await;
    Ok((p, resync))
}

/// The context sensitive plugin settings are sealed with.
pub fn seal_context(team: &str, plugin: &str) -> std::collections::HashMap<String, String> {
    std::collections::HashMap::from([
        ("team".to_string(), team.to_string()),
        ("plugin".to_string(), plugin.to_string()),
        ("purpose".to_string(), "plugin-settings".to_string()),
    ])
}

/// Saves setting values. Keys must be the manifest's; sensitive values are
/// sealed for this team and plugin and never stored or returned in clear.
/// An empty value clears a setting.
pub async fn set_settings(
    kv: &dyn Kv,
    cipher: &dyn Cipher,
    team: &str,
    id: &str,
    values: &BTreeMap<String, String>,
    by: &str,
) -> Result<Installed, Refusal> {
    use base64::Engine;
    let (mut p, rev) = get(kv, team, id).await?;
    let snap = current_snapshot(kv, team, &p).await?;
    let mut changed = Vec::new();
    for (k, v) in values {
        let s = snap.manifest.setting(k).ok_or_else(|| {
            Refusal::new(
                code::INVALID,
                format!("{k} isn't one of this plugin's settings."),
            )
        })?;
        if v.len() > 4000 {
            return Err(Refusal::new(code::INVALID, format!("{k} is too long.")));
        }
        changed.push(k.clone());
        if v.is_empty() {
            p.settings.remove(k);
            p.sealed.remove(k);
            continue;
        }
        if s.sensitive || s.kind == "secret" {
            let blob = cipher
                .encrypt(seal_context(team, id), v.as_bytes())
                .await
                .map_err(|e| Refusal::new(code::STORE, format!("Couldn't seal {k}: {e}")))?;
            p.sealed.insert(
                k.clone(),
                base64::engine::general_purpose::STANDARD.encode(blob),
            );
            p.settings.remove(k);
        } else {
            p.settings.insert(k.clone(), v.clone());
            p.sealed.remove(k);
        }
    }
    save(kv, team, &mut p, Some(rev)).await?;
    let version = p
        .current
        .as_ref()
        .map(|c| c.version.clone())
        .unwrap_or_default();
    audit::record(
        kv,
        team,
        ev(
            by,
            ActorKind::Person,
            action::PLUGIN_SETTINGS,
            "ok",
            &p,
            &version,
        )
        .detail(format!("changed {}", changed.join(", "))),
    )
    .await;
    Ok(p)
}

/// Opens one sealed setting for a call (never returned by the API).
pub async fn open_setting(
    cipher: &dyn Cipher,
    team: &str,
    p: &Installed,
    key: &str,
) -> Option<String> {
    use base64::Engine;
    if let Some(v) = p.settings.get(key) {
        return Some(v.clone());
    }
    let blob = base64::engine::general_purpose::STANDARD
        .decode(p.sealed.get(key)?)
        .ok()?;
    let plain = cipher
        .decrypt(seal_context(team, &p.id), &blob)
        .await
        .ok()?;
    String::from_utf8(plain).ok()
}

/// The plugin's monthly token limit (0: none).
pub async fn set_limit(
    kv: &dyn Kv,
    team: &str,
    id: &str,
    monthly_tokens: u64,
    by: &str,
) -> Result<Installed, Refusal> {
    let (mut p, rev) = get(kv, team, id).await?;
    p.monthly_tokens = monthly_tokens;
    save(kv, team, &mut p, Some(rev)).await?;
    let version = p
        .current
        .as_ref()
        .map(|c| c.version.clone())
        .unwrap_or_default();
    audit::record(
        kv,
        team,
        ev(
            by,
            ActorKind::Person,
            action::PLUGIN_SETTINGS,
            "ok",
            &p,
            &version,
        )
        .detail(format!("monthly limit {monthly_tokens}")),
    )
    .await;
    Ok(p)
}

/// Who approves the plugin's drafts (emails), and how many approvals each
/// draft needs.
pub async fn set_approvers(
    kv: &dyn Kv,
    team: &str,
    id: &str,
    members: Vec<String>,
    min: u32,
    notify: Option<String>,
    by: &str,
) -> Result<Installed, Refusal> {
    let (mut p, rev) = get(kv, team, id).await?;
    let mut members: Vec<String> = members
        .into_iter()
        .map(|m| m.trim().to_ascii_lowercase())
        .filter(|m| !m.is_empty())
        .collect();
    members.sort();
    members.dedup();
    if members.iter().any(|m| !m.contains('@') || m.len() > 200) || members.len() > 50 {
        return Err(Refusal::new(
            code::INVALID,
            "Approvers are up to 50 email addresses.",
        ));
    }
    if min == 0 || min > 5 {
        return Err(Refusal::new(
            code::INVALID,
            "A draft needs 1 to 5 approvals.",
        ));
    }
    if !members.is_empty() && (members.len() as u32) < min {
        return Err(Refusal::new(
            code::INVALID,
            "There are fewer approvers than the approvals a draft needs.",
        ));
    }
    if let Some(n) = &notify {
        if !matches!(n.as_str(), "" | "teams" | "slack" | "both") {
            return Err(Refusal::new(
                code::INVALID,
                "Notify is teams, slack, both or nothing.",
            ));
        }
        p.notify = n.clone();
    }
    p.approvers = Approvers { members, min };
    save(kv, team, &mut p, Some(rev)).await?;
    let version = p
        .current
        .as_ref()
        .map(|c| c.version.clone())
        .unwrap_or_default();
    audit::record(
        kv,
        team,
        ev(
            by,
            ActorKind::Person,
            action::PLUGIN_SETTINGS,
            "ok",
            &p,
            &version,
        )
        .detail(format!(
            "approvers {} (min {})",
            if p.approvers.members.is_empty() {
                "admins".into()
            } else {
                p.approvers.members.join(", ")
            },
            min
        )),
    )
    .await;
    Ok(p)
}

/// Turns the plugin on or off. Off: no triggers fire and no run starts.
pub async fn set_enabled(
    kv: &dyn Kv,
    team: &str,
    id: &str,
    on: bool,
    by: &str,
) -> Result<Installed, Refusal> {
    let (mut p, rev) = get(kv, team, id).await?;
    match (on, p.status) {
        (true, Status::Disabled) => p.status = Status::Active,
        (false, Status::Active) => p.status = Status::Disabled,
        (_, Status::PendingInstall) => {
            return Err(Refusal::new(
                code::WRONG_STATE,
                "The install is still waiting for an admin.",
            ))
        }
        _ => return Ok(p),
    }
    save(kv, team, &mut p, Some(rev)).await?;
    let version = p
        .current
        .as_ref()
        .map(|c| c.version.clone())
        .unwrap_or_default();
    audit::record(
        kv,
        team,
        ev(
            by,
            ActorKind::Person,
            if on {
                action::ENABLED
            } else {
                action::DISABLED
            },
            "ok",
            &p,
            &version,
        ),
    )
    .await;
    Ok(p)
}

/// Removes the plugin (its drafts and audit trail stay).
pub async fn uninstall(
    kv: &dyn Kv,
    sched: &dyn Scheduler,
    team: &str,
    id: &str,
    by: &str,
) -> Result<(), Refusal> {
    let (p, _) = get(kv, team, id).await?;
    sync_crons(kv, sched, team, &p, None).await;
    kv.delete(team, &sk(id)).await.map_err(store_err)?;
    let version = p
        .current
        .as_ref()
        .map(|c| c.version.clone())
        .unwrap_or_default();
    audit::record(
        kv,
        team,
        ev(
            by,
            ActorKind::Person,
            action::UNINSTALLED,
            "ok",
            &p,
            &version,
        ),
    )
    .await;
    Ok(())
}

/// Makes the plugin's recurring schedules match its pinned manifest: one
/// per `schedule` trigger while the plugin is active, none otherwise. Pass
/// `None` to remove them all.
pub async fn sync_crons(
    kv: &dyn Kv,
    sched: &dyn Scheduler,
    team: &str,
    p: &Installed,
    m: Option<&Manifest>,
) -> Vec<String> {
    let mut wanted: Vec<(String, String, serde_json::Value)> = Vec::new();
    if let (Some(m), true) = (m, p.active()) {
        for (i, t) in m.triggers.iter().enumerate() {
            if t.on != "schedule" {
                continue;
            }
            let Some(expr) = t.cron.as_deref().and_then(manifest::eventbridge_cron) else {
                continue;
            };
            let name = super::schedule::name("cron", team, &format!("{}-{i}", p.id));
            let body = serde_json::json!({ "type": "plugin_trigger", "team_id": team, "plugin_id": p.id, "trigger": i });
            wanted.push((name, expr, body));
        }
    }
    let prefix = format!("{team}|{}|", p.id);
    let existing: Vec<String> = kv
        .query(kv::SCHEDULE_INDEX, &prefix, false, None, None)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter_map(|r| r.data["name"].as_str().map(str::to_string))
        .collect();
    for old in &existing {
        if !wanted.iter().any(|(n, _, _)| n == old) {
            let _ = sched.delete(old).await;
            let _ = kv
                .delete(kv::SCHEDULE_INDEX, &format!("{prefix}{old}"))
                .await;
        }
    }
    let mut made = Vec::new();
    for (name, expr, body) in wanted {
        match sched.cron(&name, &expr, &body).await {
            Ok(()) => {
                let _ = kv
                    .put(
                        kv::SCHEDULE_INDEX,
                        &format!("{prefix}{name}"),
                        &serde_json::json!({ "name": name, "expr": expr }),
                        Expect::Any,
                    )
                    .await;
                made.push(name);
            }
            Err(e) => {
                tracing::warn!(team, plugin = %p.id, error = %e, "Couldn't create a plugin schedule")
            }
        }
    }
    made
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::plugins::kv::Memory;
    use crate::plugins::manifest::tests::MARKETING;
    use crate::plugins::package::tests::{marketing_files, FakeRepo};
    use crate::plugins::schedule::MemoryScheduler;
    use crate::plugins::source::tests::FakeAccess;

    pub const TEAM: &str = "TEAM#a";

    /// A team with the marketing plugin's source added and synced.
    pub async fn with_source(kv: &Memory) -> (FakeRepo, String) {
        let access = FakeAccess {
            public: vec!["acme/plugins".into()],
            ..Default::default()
        };
        let src = source::add(
            kv,
            &access,
            TEAM,
            None,
            "https://github.com/acme/plugins",
            None,
            "admin@a.io",
        )
        .await
        .unwrap();
        let repo = FakeRepo::default();
        push(&repo, "a1a1a1a1", MARKETING);
        source::sync(kv, &repo, TEAM, None, &src.id, false)
            .await
            .unwrap();
        (repo, src.id)
    }

    pub fn push(repo: &FakeRepo, sha: &str, manifest: &str) {
        let files = marketing_files(manifest);
        let refs: Vec<(&str, &str)> = files
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect();
        repo.commit(sha, "", &refs);
    }

    /// The manifest with one more allowed Sanity tool.
    pub fn widened() -> String {
        MARKETING.replace(
            "allow: [get_*, create_draft]",
            "allow: [get_*, create_draft, update_document]",
        )
    }

    #[tokio::test]
    async fn install_needs_a_second_admin_when_there_is_one() {
        let kv = Memory::new();
        let (_, src) = with_source(&kv).await;
        let p = install(&kv, TEAM, &src, "plugins/marketing", "admin@a.io", 2)
            .await
            .unwrap();
        assert_eq!(p.status, Status::PendingInstall);
        assert!(p.current.is_none());
        let hash = p.pending.as_ref().unwrap().version.hash.clone();
        // The admin who asked can't approve it.
        let r = approve(&kv, TEAM, "marketing", &hash, "ADMIN@a.io", 2)
            .await
            .unwrap_err();
        assert_eq!(r.code, code::NEEDS_SECOND_ADMIN);
        let p = approve(&kv, TEAM, "marketing", &hash, "second@a.io", 2)
            .await
            .unwrap();
        assert_eq!(p.status, Status::Active);
        assert_eq!(p.hash(), Some(hash.as_str()));
        assert_eq!(p.monthly_tokens, 2_000_000);
    }

    #[tokio::test]
    async fn a_lone_admin_installs_at_once_pinned_by_hash() {
        let kv = Memory::new();
        let (_, src) = with_source(&kv).await;
        let p = install(&kv, TEAM, &src, "plugins/marketing", "admin@a.io", 1)
            .await
            .unwrap();
        assert!(p.active());
        assert_eq!(p.hash().unwrap().len(), 64);
        assert_eq!(
            install(&kv, TEAM, &src, "plugins/marketing", "admin@a.io", 1)
                .await
                .unwrap_err()
                .code,
            code::CONFLICT
        );
    }

    #[tokio::test]
    async fn wording_applies_at_once_and_widening_waits() {
        let kv = Memory::new();
        let (repo, src) = with_source(&kv).await;
        install(&kv, TEAM, &src, "plugins/marketing", "admin@a.io", 1)
            .await
            .unwrap();
        let first = get(&kv, TEAM, "marketing")
            .await
            .unwrap()
            .0
            .current
            .unwrap();
        // A wording change (description, version) applies, pinned to sha2.
        push(
            &repo,
            "b2b2b2b2",
            &MARKETING
                .replace("Drafts launch posts from releases", "Drafts better posts")
                .replace("1.2.0", "1.2.1"),
        );
        let o = sync_plugin(&kv, &repo, TEAM, None, "marketing", "system", false)
            .await
            .unwrap();
        assert_eq!(o, Offer::Applied);
        let p = get(&kv, TEAM, "marketing").await.unwrap().0;
        assert_eq!(p.current.as_ref().unwrap().sha, "b2b2b2b2");
        assert_eq!(p.previous.as_ref().unwrap().hash, first.hash);
        // A new tool waits; the plugin keeps running 1.2.1.
        push(&repo, "c3c3c3c3", &widened());
        match sync_plugin(&kv, &repo, TEAM, None, "marketing", "system", false)
            .await
            .unwrap()
        {
            Offer::Waiting(w) => assert!(w.iter().any(|l| l.contains("update_document")), "{w:?}"),
            other => panic!("{other:?}"),
        }
        let p = get(&kv, TEAM, "marketing").await.unwrap().0;
        assert_eq!(p.current.as_ref().unwrap().sha, "b2b2b2b2");
        let pending = p.pending.clone().unwrap();
        assert_eq!(pending.kind, PendingKind::Update);
        assert!(pending
            .diff
            .added
            .iter()
            .any(|l| l.contains("update_document")));
        // The audit log has both.
        let events = audit::for_filter(
            &kv,
            TEAM,
            &audit::Filter {
                plugin_id: Some("marketing".into()),
                ..Default::default()
            },
        )
        .await;
        let acts: Vec<&str> = events.iter().map(|e| e.action.as_str()).collect();
        assert!(
            acts.contains(&action::UPDATED) && acts.contains(&action::UPDATE_WAITING),
            "{acts:?}"
        );
        // An admin approves it.
        let p = approve(
            &kv,
            TEAM,
            "marketing",
            &pending.version.hash,
            "admin@a.io",
            1,
        )
        .await
        .unwrap();
        assert_eq!(p.current.as_ref().unwrap().sha, "c3c3c3c3");
    }

    #[tokio::test]
    async fn pinning_stops_updates_and_auto_update_off_keeps_them_available() {
        let kv = Memory::new();
        let (repo, src) = with_source(&kv).await;
        install(&kv, TEAM, &src, "plugins/marketing", "admin@a.io", 1)
            .await
            .unwrap();
        set_options(
            &kv,
            TEAM,
            "marketing",
            None,
            Some(Track {
                kind: TrackKind::Sha,
                value: "a1a1a1a1".into(),
            }),
            "admin@a.io",
        )
        .await
        .unwrap();
        push(&repo, "b2b2b2b2", &MARKETING.replace("1.2.0", "1.3.0"));
        assert_eq!(
            sync_plugin(&kv, &repo, TEAM, None, "marketing", "system", false)
                .await
                .unwrap(),
            Offer::Same
        );
        assert_eq!(
            get(&kv, TEAM, "marketing")
                .await
                .unwrap()
                .0
                .current
                .unwrap()
                .sha,
            "a1a1a1a1"
        );
        // Following the branch again with auto-update off: seen, not applied.
        set_options(
            &kv,
            TEAM,
            "marketing",
            Some(false),
            Some(Track::default()),
            "admin@a.io",
        )
        .await
        .unwrap();
        assert_eq!(
            sync_plugin(&kv, &repo, TEAM, None, "marketing", "system", false)
                .await
                .unwrap(),
            Offer::Available
        );
        let p = get(&kv, TEAM, "marketing").await.unwrap().0;
        assert_eq!(p.current.as_ref().unwrap().sha, "a1a1a1a1");
        assert_eq!(p.available.as_ref().unwrap().sha, "b2b2b2b2");
        // An admin's sync applies it.
        assert_eq!(
            sync_plugin(&kv, &repo, TEAM, None, "marketing", "admin@a.io", true)
                .await
                .unwrap(),
            Offer::Applied
        );
    }

    #[tokio::test]
    async fn rollback_goes_to_the_previous_version_in_one_step() {
        let kv = Memory::new();
        let (repo, src) = with_source(&kv).await;
        install(&kv, TEAM, &src, "plugins/marketing", "admin@a.io", 1)
            .await
            .unwrap();
        push(&repo, "b2b2b2b2", &MARKETING.replace("1.2.0", "1.2.1"));
        sync_plugin(&kv, &repo, TEAM, None, "marketing", "system", false)
            .await
            .unwrap();
        let p = rollback(&kv, TEAM, "marketing", "admin@a.io")
            .await
            .unwrap();
        assert_eq!(p.current.as_ref().unwrap().sha, "a1a1a1a1");
        assert_eq!(p.previous.as_ref().unwrap().sha, "b2b2b2b2");
        assert!(!p.auto_update, "a rollback isn't undone by the next poll");
        assert_eq!(
            sync_plugin(&kv, &repo, TEAM, None, "marketing", "system", false)
                .await
                .unwrap(),
            Offer::Available
        );
    }

    #[tokio::test]
    async fn sensitive_settings_are_sealed_and_never_shown() {
        let kv = Memory::new();
        let (_, src) = with_source(&kv).await;
        install(&kv, TEAM, &src, "plugins/marketing", "admin@a.io", 1)
            .await
            .unwrap();
        let cipher = crate::plugins::connect::tests::XorCipher;
        let vals = BTreeMap::from([
            ("linkedin".to_string(), "conn-123".to_string()),
            (
                "brand_voice_doc".to_string(),
                "https://docs.example.com/voice".to_string(),
            ),
        ]);
        let p = set_settings(&kv, &cipher, TEAM, "marketing", &vals, "admin@a.io")
            .await
            .unwrap();
        assert!(!p.settings.contains_key("linkedin"));
        assert!(p.sealed.contains_key("linkedin"));
        let snap = current_snapshot(&kv, TEAM, &p).await.unwrap();
        let v = p.view(Some(&snap), 0).to_string();
        assert!(!v.contains("conn-123"));
        assert_eq!(
            open_setting(&cipher, TEAM, &p, "linkedin").await.as_deref(),
            Some("conn-123")
        );
        // Sealed for this team: another team's context won't open it.
        assert_eq!(open_setting(&cipher, "TEAM#b", &p, "linkedin").await, None);
        let bad = BTreeMap::from([("nope".to_string(), "x".to_string())]);
        assert_eq!(
            set_settings(&kv, &cipher, TEAM, "marketing", &bad, "a@a.io")
                .await
                .unwrap_err()
                .code,
            code::INVALID
        );
    }

    #[tokio::test]
    async fn schedule_triggers_become_recurring_schedules() {
        let kv = Memory::new();
        let sched = MemoryScheduler::default();
        let (_, src) = with_source(&kv).await;
        let p = install(&kv, TEAM, &src, "plugins/marketing", "admin@a.io", 1)
            .await
            .unwrap();
        let snap = current_snapshot(&kv, TEAM, &p).await.unwrap();
        let made = sync_crons(&kv, &sched, TEAM, &p, Some(&snap.manifest)).await;
        assert_eq!(made.len(), 1);
        assert!(sched.has(&made[0]));
        uninstall(&kv, &sched, TEAM, "marketing", "admin@a.io")
            .await
            .unwrap();
        assert!(sched.names().is_empty());
    }
}
