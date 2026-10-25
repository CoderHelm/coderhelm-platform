//! Reading a plugin folder from a repository, and keeping what was read.
//!
//! A plugin folder is the manifest plus the Claude Code layout CoderHelm
//! already reads for skills: `skills/<name>/SKILL.md` (and text files next
//! to it), `agents/<name>.md`, `commands/<name>.md`, relative to the folder.
//! The skill-repository rules apply as they are (`crate::skills`): text
//! only, size limits, nothing run.
//!
//! What's read is kept as a snapshot (`PKGSNAP#<hash>`) whose text lives in
//! content-addressed blobs (`PKGBLOB#<sha256>`), all under the team. A
//! plugin is pinned to a snapshot by its hash: the hash covers the manifest
//! and every item and file, so any change is a new hash.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::kv::{get_as, put_as, Expect, Kv};
use super::manifest::{self, Manifest};
use crate::skills::{self, Kind};
use crate::tool_registry::glob_match;

pub const SNAP: &str = "PKGSNAP#";
pub const BLOB: &str = "PKGBLOB#";

/// Reads a repository at a commit. The worker implements it twice: through
/// the team's GitHub App installation (private repositories), and without
/// any credentials (public repositories).
#[async_trait::async_trait]
pub trait RepoRead: Send + Sync {
    /// The commit a branch or tag points at now. A full sha is returned as is.
    async fn resolve(&self, git_ref: &str) -> Result<String, String>;
    /// Every file at `sha`, with its size.
    async fn tree(&self, sha: &str) -> Result<Vec<(String, u64)>, String>;
    /// A file's text at `sha`.
    async fn read(&self, sha: &str, path: &str) -> Result<String, String>;
}

/// A plugin folder as read.
#[derive(Debug, Clone)]
pub struct Package {
    pub manifest: Manifest,
    pub manifest_text: String,
    /// Paths relative to the plugin folder.
    pub items: Vec<skills::Parsed>,
    pub files: Vec<(String, String)>,
    pub skipped: Vec<String>,
}

impl Package {
    /// The content hash a version is pinned by.
    pub fn hash(&self) -> String {
        let mut parts: Vec<String> = vec![
            "manifest".into(),
            serde_json::to_string(&self.manifest).unwrap_or_default(),
        ];
        let mut items: Vec<_> = self
            .items
            .iter()
            .map(|i| (i.path.clone(), i.hash.clone()))
            .collect();
        items.sort();
        for (p, h) in items {
            parts.push(format!("item {p}"));
            parts.push(h);
        }
        let mut files: Vec<_> = self
            .files
            .iter()
            .map(|(p, t)| (p.clone(), crate::content_hash(t)))
            .collect();
        files.sort();
        for (p, h) in files {
            parts.push(format!("file {p}"));
            parts.push(h);
        }
        let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
        crate::sha256_hex(&refs)
    }
}

/// `root` joined with `rel` (root "" is the repository's top).
pub fn join(root: &str, rel: &str) -> String {
    if root.is_empty() {
        rel.to_string()
    } else {
        format!("{}/{rel}", root.trim_end_matches('/'))
    }
}

/// A plugin folder path: relative, no `..`, or "" for the repository's top.
pub fn valid_root(root: &str) -> bool {
    root.is_empty()
        || (root.len() <= 200
            && !root.starts_with('/')
            && root
                .split('/')
                .all(|p| !p.is_empty() && p != "." && p != ".." && !p.contains('\\')))
}

/// The folders in a tree that hold a manifest: the top, and any
/// `plugins/<name>/` (a marketplace repository).
pub fn plugin_roots(tree: &[(String, u64)]) -> Vec<String> {
    let mut roots = Vec::new();
    for (p, _) in tree {
        for m in manifest::MANIFEST_FILES {
            if p == m {
                roots.push(String::new());
            } else if let Some(dir) = p.strip_suffix(&format!("/{m}")) {
                let parts: Vec<&str> = dir.split('/').collect();
                if parts.len() == 2 && parts[0] == "plugins" {
                    roots.push(dir.to_string());
                }
            }
        }
    }
    roots.sort();
    roots.dedup();
    roots
}

/// Reads the plugin folder `root` at `sha`.
pub async fn read(reader: &dyn RepoRead, sha: &str, root: &str) -> Result<Package, Vec<String>> {
    if !valid_root(root) {
        return Err(vec![format!("{root} isn't a folder in the repository.")]);
    }
    let tree = reader.tree(sha).await.map_err(|e| vec![e])?;
    let prefix = if root.is_empty() {
        String::new()
    } else {
        format!("{root}/")
    };
    let rel: Vec<(String, u64)> = tree
        .iter()
        .filter_map(|(p, s)| p.strip_prefix(&prefix).map(|r| (r.to_string(), *s)))
        .collect();
    let Some(mpath) = manifest::MANIFEST_FILES
        .iter()
        .find(|m| rel.iter().any(|(p, _)| p == *m))
    else {
        return Err(vec![format!(
            "No plugin.yaml in {}.",
            if root.is_empty() {
                "the repository's top folder"
            } else {
                root
            }
        )]);
    };
    let text = reader
        .read(sha, &join(root, mpath))
        .await
        .map_err(|e| vec![format!("Couldn't read the manifest: {e}")])?;
    let manifest = manifest::parse(&text)?;
    let globs: Vec<String> = manifest
        .skills
        .iter()
        .chain(&manifest.agents)
        .chain(&manifest.commands)
        .cloned()
        .collect();
    let sel = skills::select(&rel);
    let mut skipped = sel.skipped.clone();
    let mut items = Vec::new();
    for path in &sel.items {
        if !globs.iter().any(|g| glob_match(g, path)) {
            continue;
        }
        match reader.read(sha, &join(root, path)).await {
            Ok(t) if t.len() <= skills::MAX_ITEM_BYTES => {
                if let Some(p) = skills::parse_item(path, &t) {
                    items.push(p);
                }
            }
            Ok(_) => skipped.push(format!("{path}: too large")),
            Err(_) => skipped.push(format!("{path}: couldn't be read as text")),
        }
    }
    let mut files = Vec::new();
    for (skill, list) in &sel.files {
        if !items.iter().any(|i| &i.path == skill) {
            continue;
        }
        for path in list {
            match reader.read(sha, &join(root, path)).await {
                Ok(t) if t.len() <= skills::MAX_FILE_BYTES => files.push((path.clone(), t)),
                _ => skipped.push(format!("{path}: couldn't be read")),
            }
        }
    }
    let (items, files, over) = skills::within_budget(items, files);
    skipped.extend(over);
    Ok(Package {
        manifest,
        manifest_text: text,
        items,
        files,
        skipped,
    })
}

/// One item in a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemRef {
    pub path: String,
    pub kind: String,
    pub name: String,
    pub description: String,
    /// The blob holding its text.
    pub blob: String,
    #[serde(default)]
    pub files: Vec<String>,
}

/// One version of a plugin as read: what it is pinned by.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub hash: String,
    pub sha: String,
    pub source_id: String,
    pub root: String,
    pub manifest: Manifest,
    pub items: Vec<ItemRef>,
    /// Text files next to skills: path -> blob.
    #[serde(default)]
    pub files: BTreeMap<String, String>,
    #[serde(default)]
    pub skipped: Vec<String>,
    pub read_at: String,
}

impl Snapshot {
    pub fn agents(&self) -> impl Iterator<Item = &ItemRef> {
        self.items.iter().filter(|i| i.kind == Kind::Agent.as_str())
    }
    pub fn skills(&self) -> impl Iterator<Item = &ItemRef> {
        self.items.iter().filter(|i| i.kind != Kind::Agent.as_str())
    }
    /// The agent a trigger names, or the first agent.
    pub fn agent(&self, name: Option<&str>) -> Option<&ItemRef> {
        match name {
            Some(n) => self
                .agents()
                .find(|a| a.name == n || a.path.ends_with(&format!("/{n}.md"))),
            None => self.agents().next(),
        }
    }
}

/// Keeps a package: its text as blobs, and the snapshot. Idempotent: the
/// same package gives the same records.
pub async fn store(
    kv: &dyn Kv,
    team: &str,
    source_id: &str,
    root: &str,
    sha: &str,
    pkg: &Package,
) -> Result<Snapshot, String> {
    let hash = pkg.hash();
    if let Some((s, _)) = get_as::<Snapshot>(kv, team, &format!("{SNAP}{hash}")).await? {
        return Ok(s);
    }
    let mut items = Vec::new();
    for it in &pkg.items {
        let blob = put_blob(kv, team, &it.body).await?;
        items.push(ItemRef {
            path: it.path.clone(),
            kind: it.kind.as_str().to_string(),
            name: it.name.clone(),
            description: it.description.clone(),
            blob,
            files: it.files.clone(),
        });
    }
    let mut files = BTreeMap::new();
    for (p, t) in &pkg.files {
        files.insert(p.clone(), put_blob(kv, team, t).await?);
    }
    let snap = Snapshot {
        hash: hash.clone(),
        sha: sha.to_string(),
        source_id: source_id.to_string(),
        root: root.to_string(),
        manifest: pkg.manifest.clone(),
        items,
        files,
        skipped: pkg.skipped.clone(),
        read_at: super::now_rfc3339(),
    };
    put_as(kv, team, &format!("{SNAP}{hash}"), &snap, Expect::Any)
        .await
        .map_err(|e| e.to_string())?;
    Ok(snap)
}

async fn put_blob(kv: &dyn Kv, team: &str, text: &str) -> Result<String, String> {
    let h = crate::content_hash(text);
    let sk = format!("{BLOB}{h}");
    if kv.get(team, &sk).await?.is_none() {
        kv.put(team, &sk, &serde_json::json!({ "body": text }), Expect::Any)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(h)
}

/// A kept snapshot by hash.
pub async fn snapshot(kv: &dyn Kv, team: &str, hash: &str) -> Result<Option<Snapshot>, String> {
    Ok(get_as::<Snapshot>(kv, team, &format!("{SNAP}{hash}"))
        .await?
        .map(|(s, _)| s))
}

/// A blob's text.
pub async fn blob(kv: &dyn Kv, team: &str, hash: &str) -> Result<String, String> {
    let r = kv
        .get(team, &format!("{BLOB}{hash}"))
        .await?
        .ok_or_else(|| "That text is no longer kept.".to_string())?;
    Ok(r.data["body"].as_str().unwrap_or("").to_string())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::plugins::kv::Memory;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A repository in memory: commits by sha, refs by name.
    #[derive(Default)]
    pub struct FakeRepo {
        pub commits: Mutex<HashMap<String, BTreeMap<String, String>>>,
        pub refs: Mutex<HashMap<String, String>>,
    }

    impl FakeRepo {
        pub fn commit(&self, sha: &str, git_ref: &str, files: &[(&str, &str)]) {
            self.commits.lock().unwrap().insert(
                sha.into(),
                files
                    .iter()
                    .map(|(p, t)| (p.to_string(), t.to_string()))
                    .collect(),
            );
            self.refs.lock().unwrap().insert(git_ref.into(), sha.into());
        }
    }

    #[async_trait::async_trait]
    impl RepoRead for FakeRepo {
        async fn resolve(&self, git_ref: &str) -> Result<String, String> {
            if self.commits.lock().unwrap().contains_key(git_ref) {
                return Ok(git_ref.to_string());
            }
            self.refs
                .lock()
                .unwrap()
                .get(git_ref)
                .cloned()
                .ok_or_else(|| format!("no ref {git_ref}"))
        }
        async fn tree(&self, sha: &str) -> Result<Vec<(String, u64)>, String> {
            Ok(self
                .commits
                .lock()
                .unwrap()
                .get(sha)
                .ok_or("no commit")?
                .iter()
                .map(|(p, t)| (p.clone(), t.len() as u64))
                .collect())
        }
        async fn read(&self, sha: &str, path: &str) -> Result<String, String> {
            self.commits
                .lock()
                .unwrap()
                .get(sha)
                .and_then(|c| c.get(path).cloned())
                .ok_or_else(|| format!("no {path}"))
        }
    }

    pub const AGENT: &str = "---\nname: launch-writer\ndescription: Writes launch posts from release notes\n---\nRead the release notes and write posts in the brand voice.";
    pub const SKILL: &str =
        "---\nname: brand-voice\ndescription: How we sound\n---\nShort sentences. No hype.";

    pub fn marketing_files(manifest: &str) -> Vec<(String, String)> {
        vec![
            ("plugins/marketing/plugin.yaml".into(), manifest.into()),
            (
                "plugins/marketing/agents/launch-writer.md".into(),
                AGENT.into(),
            ),
            (
                "plugins/marketing/skills/brand-voice/SKILL.md".into(),
                SKILL.into(),
            ),
            (
                "plugins/marketing/skills/brand-voice/examples.md".into(),
                "Example post.".into(),
            ),
            ("README.md".into(), "A marketplace".into()),
        ]
    }

    #[tokio::test]
    async fn reads_a_plugin_folder_and_pins_it_by_hash() {
        let repo = FakeRepo::default();
        let files = marketing_files(crate::plugins::manifest::tests::MARKETING);
        let refs: Vec<(&str, &str)> = files
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect();
        repo.commit("a1a1a1a1", "main", &refs);
        let tree = repo.tree("a1a1a1a1").await.unwrap();
        assert_eq!(plugin_roots(&tree), vec!["plugins/marketing".to_string()]);
        let pkg = read(&repo, "a1a1a1a1", "plugins/marketing").await.unwrap();
        assert_eq!(pkg.items.len(), 2);
        assert_eq!(pkg.files.len(), 1);
        let h = pkg.hash();
        // The same content is the same hash; one changed byte isn't.
        assert_eq!(
            read(&repo, "a1a1a1a1", "plugins/marketing")
                .await
                .unwrap()
                .hash(),
            h
        );
        let mut changed = refs.clone();
        changed[2].1 = "---\nname: brand-voice\n---\nLonger sentences.";
        repo.commit("b2b2b2b2", "main", &changed);
        assert_ne!(
            read(&repo, "b2b2b2b2", "plugins/marketing")
                .await
                .unwrap()
                .hash(),
            h
        );

        let kv = Memory::new();
        let snap = store(&kv, "T", "src1", "plugins/marketing", "a1a1a1a1", &pkg)
            .await
            .unwrap();
        assert_eq!(snap.hash, h);
        assert_eq!(snap.agent(None).unwrap().name, "launch-writer");
        assert_eq!(
            blob(&kv, "T", &snap.agent(None).unwrap().blob)
                .await
                .unwrap(),
            AGENT
        );
        // Kept under the team only.
        assert!(snapshot(&kv, "U", &h).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_folder_without_a_manifest_or_outside_the_repo_is_refused() {
        let repo = FakeRepo::default();
        repo.commit("s", "main", &[("agents/a.md", "x")]);
        assert!(read(&repo, "s", "").await.unwrap_err()[0].contains("No plugin.yaml"));
        assert!(read(&repo, "s", "../x").await.is_err());
        assert!(!valid_root("/abs"));
        assert!(valid_root("plugins/a"));
    }
}
