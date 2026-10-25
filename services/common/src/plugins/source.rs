//! Where plugins come from: git sources a team adds (its private
//! marketplace).
//!
//! A source is a GitHub repository holding one plugin at its top or many
//! under `plugins/<name>/`. It is either:
//!
//! - **public**: read by URL with no credentials at all; or
//! - **installation**: private, read through the team's own GitHub App
//!   installation, and only when that installation can read it. The record
//!   keeps the installation it was added with, which is always the adding
//!   team's own, and every read goes through exactly that installation, so a
//!   private repository is never readable by another team.
//!
//! Each source tracks a branch (the default branch when empty) or a tag, or
//! is pinned to a commit. A push to a source on the team's installation is
//! synced by webhook; public sources (and, as a backstop, installation ones)
//! are polled every 15 minutes. Records: `PKGSRC#<id>` under the team, and a
//! `team|id` pointer in [`kv::SOURCE_INDEX`](super::kv::SOURCE_INDEX) for the
//! poll.

use serde::{Deserialize, Serialize};

use super::audit::{self, action, ActorKind, Event};
use super::kv::{self, get_as, put_as, query_as, Expect, Kv};
use super::package::{self, RepoRead, Snapshot};
use super::{code, store_err, Refusal};

pub const PREFIX: &str = "PKGSRC#";
/// Sources one team may add.
pub const MAX_SOURCES: usize = 10;
/// Installation sources are also polled when their last read is this old,
/// in case a webhook was missed.
pub const INSTALLATION_POLL_SECS: i64 = 6 * 3600;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Access {
    #[default]
    Public,
    Installation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TrackKind {
    #[default]
    Branch,
    Tag,
    Sha,
}

/// What a source or a plugin follows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Track {
    pub kind: TrackKind,
    /// The branch (empty: the default branch), the tag, or the commit.
    #[serde(default)]
    pub value: String,
}

impl Track {
    /// What to resolve: a branch or tag name, a sha, or "" for the default
    /// branch.
    pub fn git_ref(&self) -> String {
        match self.kind {
            TrackKind::Tag if !self.value.is_empty() => format!("tags/{}", self.value),
            _ => self.value.clone(),
        }
    }

    pub fn pinned(&self) -> bool {
        self.kind == TrackKind::Sha
    }

    pub fn check(&self) -> Result<(), Refusal> {
        let ok_name = |v: &str| {
            v.len() <= 200
                && !v.contains("..")
                && !v.starts_with('/')
                && v.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/'))
        };
        let ok = match self.kind {
            TrackKind::Branch => ok_name(&self.value),
            TrackKind::Tag => !self.value.is_empty() && ok_name(&self.value),
            TrackKind::Sha => {
                (7..=40).contains(&self.value.len())
                    && self.value.chars().all(|c| c.is_ascii_hexdigit())
            }
        };
        if ok {
            Ok(())
        } else {
            Err(Refusal::new(
                code::INVALID,
                "That branch, tag or commit isn't valid.",
            ))
        }
    }
}

/// A plugin a source holds, as last read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Found {
    pub path: String,
    pub name: String,
    pub version: String,
    pub description: String,
    /// The snapshot it was read into; empty when it didn't read.
    #[serde(default)]
    pub hash: String,
    #[serde(default)]
    pub problems: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Source {
    pub id: String,
    pub url: String,
    /// `owner/name`.
    pub repo: String,
    pub access: Access,
    /// The team's own installation (installation sources only).
    #[serde(default)]
    pub installation_id: u64,
    #[serde(default)]
    pub track: Track,
    /// The commit last read.
    #[serde(default)]
    pub sha: String,
    #[serde(default)]
    pub checked_at: String,
    #[serde(default)]
    pub error: String,
    pub added_by: String,
    pub added_at: String,
    #[serde(default)]
    pub plugins: Vec<Found>,
}

/// Decides how a repository can be read, for adding a source.
#[async_trait::async_trait]
pub trait AccessCheck: Send + Sync {
    /// True when the repository is public (readable with no credentials).
    async fn is_public(&self, repo: &str) -> Result<bool, String>;
    /// True when this installation can read the repository.
    async fn installation_reads(&self, installation_id: u64, repo: &str) -> Result<bool, String>;
}

/// A source address: a GitHub URL (`https://github.com/o/r`, with `.git`, or
/// `/tree/<ref>/<folder>`) or `owner/name`. Returns the repository, and the
/// ref and folder when the URL names them.
pub fn parse_url(url: &str) -> Option<(String, Option<String>, String)> {
    let u = url.trim().trim_end_matches('/');
    let rest = if let Some(r) = u.strip_prefix("https://github.com/") {
        r
    } else if u.contains("://") || u.starts_with("git@") {
        return None;
    } else {
        u
    };
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() < 2 {
        return None;
    }
    let repo = format!("{}/{}", parts[0], parts[1].trim_end_matches(".git"));
    if !crate::skills::valid_repo(&repo) {
        return None;
    }
    match parts.get(2) {
        None => Some((repo, None, String::new())),
        Some(&"tree") if parts.len() >= 4 => {
            let folder = parts[4..].join("/");
            if !package::valid_root(&folder) {
                return None;
            }
            Some((repo, Some(parts[3].to_string()), folder))
        }
        _ => None,
    }
}

/// A stable id for a source: the repository as a slug and a short hash.
pub fn source_id(repo: &str) -> String {
    let slug: String = repo
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let slug = slug.trim_matches('-').chars().take(40).collect::<String>();
    format!("{slug}-{}", &crate::content_hash(repo)[..6])
}

pub async fn list(kv: &dyn Kv, team: &str) -> Result<Vec<Source>, Refusal> {
    query_as::<Source>(kv, team, PREFIX)
        .await
        .map_err(store_err)
}

pub async fn get(kv: &dyn Kv, team: &str, id: &str) -> Result<(Source, u64), Refusal> {
    get_as::<Source>(kv, team, &format!("{PREFIX}{id}"))
        .await
        .map_err(store_err)?
        .ok_or_else(|| Refusal::new(code::NOT_FOUND, "That source isn't connected."))
}

/// Adds a source. Public repositories are read by URL; a private one only
/// through `team_installation`, the team's own installation, and only when
/// it can read the repository.
pub async fn add(
    kv: &dyn Kv,
    access: &dyn AccessCheck,
    team: &str,
    team_installation: Option<u64>,
    url: &str,
    track: Option<Track>,
    by: &str,
) -> Result<Source, Refusal> {
    let (repo, url_ref, _folder) = parse_url(url).ok_or_else(|| {
        Refusal::new(
            code::INVALID,
            "That isn't a GitHub repository address (https://github.com/owner/name).",
        )
    })?;
    let track = track.unwrap_or(Track {
        kind: TrackKind::Branch,
        value: url_ref.unwrap_or_default(),
    });
    track.check()?;
    let existing = list(kv, team).await?;
    if existing.len() >= MAX_SOURCES {
        return Err(Refusal::new(
            code::INVALID,
            format!("A team can add up to {MAX_SOURCES} sources."),
        ));
    }
    let id = source_id(&repo);
    if existing.iter().any(|s| s.id == id) {
        return Err(Refusal::new(
            code::CONFLICT,
            "That repository is already a source.",
        ));
    }
    let (kind, installation_id) = if access.is_public(&repo).await.unwrap_or(false) {
        (Access::Public, 0)
    } else {
        match team_installation {
            Some(i) if i > 0 && access.installation_reads(i, &repo).await.unwrap_or(false) => {
                (Access::Installation, i)
            }
            _ => {
                return Err(Refusal::new(
                    code::FORBIDDEN,
                    "CoderHelm can't read that repository. Make it public, or install the team's GitHub App on it.",
                ))
            }
        }
    };
    let src = Source {
        id: id.clone(),
        url: url.trim().to_string(),
        repo,
        access: kind,
        installation_id,
        track,
        added_by: by.to_string(),
        added_at: super::now_rfc3339(),
        ..Default::default()
    };
    put_as(kv, team, &format!("{PREFIX}{id}"), &src, Expect::Absent)
        .await
        .map_err(store_err)?;
    kv.put(
        kv::SOURCE_INDEX,
        &format!("{team}|{id}"),
        &serde_json::json!({}),
        Expect::Any,
    )
    .await
    .map_err(store_err)?;
    audit::record(
        kv,
        team,
        Event::new(by, ActorKind::Person, action::SOURCE_ADDED, "ok")
            .detail(format!("{} ({:?})", src.repo, src.access).to_lowercase()),
    )
    .await;
    Ok(src)
}

/// Removes a source. Its installed plugins have to be uninstalled first.
pub async fn remove(kv: &dyn Kv, team: &str, id: &str, by: &str) -> Result<(), Refusal> {
    let (src, _) = get(kv, team, id).await?;
    let installed = super::install::list(kv, team).await?;
    if installed.iter().any(|p| p.source_id == id) {
        return Err(Refusal::new(
            code::WRONG_STATE,
            "Plugins from this source are installed. Uninstall them first.",
        ));
    }
    kv.delete(team, &format!("{PREFIX}{id}"))
        .await
        .map_err(store_err)?;
    kv.delete(kv::SOURCE_INDEX, &format!("{team}|{id}"))
        .await
        .map_err(store_err)?;
    audit::record(
        kv,
        team,
        Event::new(by, ActorKind::Person, action::SOURCE_REMOVED, "ok").detail(src.repo),
    )
    .await;
    Ok(())
}

/// Refuses a read through an installation that isn't the team's own (the
/// team relinked GitHub, or the record was copied): a private repository is
/// only ever read for the team whose installation reads it.
pub fn check_reader(src: &Source, team_installation: Option<u64>) -> Result<(), Refusal> {
    match src.access {
        Access::Public => Ok(()),
        Access::Installation if team_installation == Some(src.installation_id) && src.installation_id > 0 => Ok(()),
        Access::Installation => Err(Refusal::new(
            code::FORBIDDEN,
            "This source was added through another GitHub installation. Remove it and add it again.",
        )),
    }
}

/// Reads a source at its track: every plugin it holds is kept as a
/// snapshot, and the source's list updated. Nothing is read again when the
/// commit hasn't moved (unless `force`). Returns the source.
pub async fn sync(
    kv: &dyn Kv,
    reader: &dyn RepoRead,
    team: &str,
    team_installation: Option<u64>,
    id: &str,
    force: bool,
) -> Result<Source, Refusal> {
    let (mut src, rev) = get(kv, team, id).await?;
    check_reader(&src, team_installation)?;
    let now = super::now_rfc3339();
    let sha = match reader.resolve(&src.track.git_ref()).await {
        Ok(s) => s,
        Err(e) => {
            src.error = format!("Couldn't read {}: {e}", src.repo);
            src.checked_at = now;
            let _ = put_as(kv, team, &format!("{PREFIX}{id}"), &src, Expect::Rev(rev)).await;
            return Ok(src);
        }
    };
    if sha == src.sha && src.error.is_empty() && !force {
        src.checked_at = now;
        let _ = put_as(kv, team, &format!("{PREFIX}{id}"), &src, Expect::Rev(rev)).await;
        return Ok(src);
    }
    let tree = match reader.tree(&sha).await {
        Ok(t) => t,
        Err(e) => {
            src.error = format!("Couldn't list the files: {e}");
            src.checked_at = now;
            let _ = put_as(kv, team, &format!("{PREFIX}{id}"), &src, Expect::Rev(rev)).await;
            return Ok(src);
        }
    };
    let mut found = Vec::new();
    for root in package::plugin_roots(&tree) {
        match package::read(reader, &sha, &root).await {
            Ok(pkg) => {
                let snap = package::store(kv, team, id, &root, &sha, &pkg)
                    .await
                    .map_err(store_err)?;
                found.push(found_of(&snap));
            }
            Err(problems) => found.push(Found {
                path: root.clone(),
                name: root.rsplit('/').next().unwrap_or("").to_string(),
                problems,
                ..Default::default()
            }),
        }
    }
    src.plugins = found;
    src.sha = sha;
    src.error = if src.plugins.is_empty() {
        "No plugin.yaml at the top or under plugins/<name>/.".into()
    } else {
        String::new()
    };
    src.checked_at = now;
    put_as(kv, team, &format!("{PREFIX}{id}"), &src, Expect::Any)
        .await
        .map_err(store_err)?;
    audit::record(
        kv,
        team,
        Event::new("system", ActorKind::System, action::SOURCE_SYNCED, "ok").detail(format!(
            "{} at {}: {} plugins",
            src.repo,
            crate::truncate_str(&src.sha, 12),
            src.plugins.len()
        )),
    )
    .await;
    Ok(src)
}

fn found_of(s: &Snapshot) -> Found {
    Found {
        path: s.root.clone(),
        name: s.manifest.id(),
        version: s.manifest.version.clone(),
        description: s.manifest.description.clone(),
        hash: s.hash.clone(),
        problems: Vec::new(),
    }
}

/// Every team source as `(team, source id)`, for the 15 minute poll.
pub async fn all(kv: &dyn Kv) -> Result<Vec<(String, String)>, String> {
    Ok(kv
        .query(kv::SOURCE_INDEX, "", false, None, None)
        .await?
        .into_iter()
        .filter_map(|r| {
            let (t, s) = r.sk.split_once('|')?;
            Some((t.to_string(), s.to_string()))
        })
        .collect())
}

/// Whether the poll reads this source now: public sources every time,
/// installation sources (synced by webhook) when their last read is old.
pub fn due_for_poll(src: &Source, now: chrono::DateTime<chrono::Utc>) -> bool {
    match src.access {
        Access::Public => true,
        Access::Installation => chrono::DateTime::parse_from_rfc3339(&src.checked_at)
            .map(|t| (now - t.with_timezone(&chrono::Utc)).num_seconds() >= INSTALLATION_POLL_SECS)
            .unwrap_or(true),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::plugins::kv::Memory;
    use crate::plugins::package::tests::{marketing_files, FakeRepo};
    use std::collections::HashMap;

    /// Public repositories, and which installation reads which private one.
    #[derive(Default)]
    pub struct FakeAccess {
        pub public: Vec<String>,
        pub installs: HashMap<u64, Vec<String>>,
    }

    #[async_trait::async_trait]
    impl AccessCheck for FakeAccess {
        async fn is_public(&self, repo: &str) -> Result<bool, String> {
            Ok(self.public.iter().any(|r| r == repo))
        }
        async fn installation_reads(&self, i: u64, repo: &str) -> Result<bool, String> {
            Ok(self
                .installs
                .get(&i)
                .map(|l| l.iter().any(|r| r == repo))
                .unwrap_or(false))
        }
    }

    #[test]
    fn urls() {
        assert_eq!(
            parse_url("https://github.com/acme/plugins").unwrap().0,
            "acme/plugins"
        );
        assert_eq!(
            parse_url("https://github.com/acme/plugins.git").unwrap().0,
            "acme/plugins"
        );
        let (r, rf, f) =
            parse_url("https://github.com/acme/plugins/tree/v2/plugins/marketing").unwrap();
        assert_eq!(
            (r.as_str(), rf.as_deref(), f.as_str()),
            ("acme/plugins", Some("v2"), "plugins/marketing")
        );
        assert_eq!(parse_url("acme/plugins").unwrap().0, "acme/plugins");
        assert!(parse_url("https://gitlab.com/acme/plugins").is_none());
        assert!(parse_url("git@github.com:acme/plugins.git").is_none());
        assert!(parse_url("https://github.com/acme").is_none());
        assert!(parse_url("https://github.com/acme/p/tree/main/../x").is_none());
    }

    #[test]
    fn tracks() {
        assert!(Track {
            kind: TrackKind::Branch,
            value: String::new()
        }
        .check()
        .is_ok());
        assert!(Track {
            kind: TrackKind::Tag,
            value: "v1.2.0".into()
        }
        .check()
        .is_ok());
        assert!(Track {
            kind: TrackKind::Tag,
            value: String::new()
        }
        .check()
        .is_err());
        assert!(Track {
            kind: TrackKind::Sha,
            value: "abc1234".into()
        }
        .check()
        .is_ok());
        assert!(Track {
            kind: TrackKind::Sha,
            value: "main".into()
        }
        .check()
        .is_err());
        assert!(Track {
            kind: TrackKind::Branch,
            value: "../x".into()
        }
        .check()
        .is_err());
        assert_eq!(
            Track {
                kind: TrackKind::Tag,
                value: "v1".into()
            }
            .git_ref(),
            "tags/v1"
        );
    }

    pub fn marketplace() -> FakeRepo {
        let repo = FakeRepo::default();
        let files = marketing_files(crate::plugins::manifest::tests::MARKETING);
        let refs: Vec<(&str, &str)> = files
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect();
        repo.commit("a1a1a1a1", "", &refs);
        repo
    }

    #[tokio::test]
    async fn a_public_url_syncs_with_no_credentials() {
        let kv = Memory::new();
        let access = FakeAccess {
            public: vec!["acme/plugins".into()],
            ..Default::default()
        };
        let src = add(
            &kv,
            &access,
            "TEAM#a",
            None,
            "https://github.com/acme/plugins",
            None,
            "admin@a.io",
        )
        .await
        .unwrap();
        assert_eq!(src.access, Access::Public);
        assert_eq!(src.installation_id, 0);
        let repo = marketplace();
        let synced = sync(&kv, &repo, "TEAM#a", None, &src.id, false)
            .await
            .unwrap();
        assert_eq!(synced.plugins.len(), 1);
        assert_eq!(synced.plugins[0].name, "marketing");
        assert!(!synced.plugins[0].hash.is_empty());
        assert_eq!(
            all(&kv).await.unwrap(),
            vec![("TEAM#a".to_string(), src.id.clone())]
        );
    }

    #[tokio::test]
    async fn a_private_repo_is_read_through_the_teams_installation_only() {
        let kv = Memory::new();
        let access = FakeAccess {
            installs: HashMap::from([(11, vec!["acme/private".into()])]),
            ..Default::default()
        };
        // Team A's installation (11) reads it.
        let src = add(
            &kv,
            &access,
            "TEAM#a",
            Some(11),
            "acme/private",
            None,
            "a@a.io",
        )
        .await
        .unwrap();
        assert_eq!(
            (src.access, src.installation_id),
            (Access::Installation, 11)
        );
        // Team B, with its own installation (22), can't add it.
        let refused = add(
            &kv,
            &access,
            "TEAM#b",
            Some(22),
            "acme/private",
            None,
            "b@b.io",
        )
        .await
        .unwrap_err();
        assert_eq!(refused.code, code::FORBIDDEN);
        // Nor without one.
        assert!(
            add(&kv, &access, "TEAM#b", None, "acme/private", None, "b@b.io")
                .await
                .is_err()
        );
        // Team B can't reach team A's record, and team A's record never
        // reads through another installation.
        assert_eq!(
            get(&kv, "TEAM#b", &src.id).await.unwrap_err().code,
            code::NOT_FOUND
        );
        let repo = marketplace();
        assert_eq!(
            sync(&kv, &repo, "TEAM#a", Some(22), &src.id, false)
                .await
                .unwrap_err()
                .code,
            code::FORBIDDEN
        );
        assert!(sync(&kv, &repo, "TEAM#a", Some(11), &src.id, false)
            .await
            .is_ok());
        assert!(sync(&kv, &repo, "TEAM#b", Some(22), &src.id, false)
            .await
            .is_err());
    }

    #[test]
    fn the_poll_reads_public_sources_always_and_installation_ones_as_a_backstop() {
        let now = chrono::Utc::now();
        let mut s = Source {
            access: Access::Public,
            ..Default::default()
        };
        assert!(due_for_poll(&s, now));
        s.access = Access::Installation;
        s.checked_at = (now - chrono::Duration::minutes(20)).to_rfc3339();
        assert!(!due_for_poll(&s, now));
        s.checked_at = (now - chrono::Duration::hours(7)).to_rfc3339();
        assert!(due_for_poll(&s, now));
    }
}
