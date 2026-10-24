//! Knowledge bases: documents a team uploads (PDF, Word, Markdown, text,
//! spreadsheets, HTML), read into passages, embedded with Titan and searched
//! by the brain chat and by the agents a base is attached to.
//!
//! Storage, all scoped by team:
//!
//! - a base and each of its files is one record on the settings table
//!   (`pk` = team id, `sk` = [`base_sk`] / [`file_sk`]), the record's JSON in
//!   `data`;
//! - the uploaded file and its search index (passages plus their vectors)
//!   are objects in the team's folder of the data bucket (KMS encrypted):
//!   [`source_key`] and [`index_key`], one folder per file version.
//!
//! One index object per file version means a file's ingest never contends
//! with another's, a delete is a prefix delete, and a new version replaces
//! the old one only once it is ready. Search loads the indexes of the bases
//! asked for (cached per version) and ranks passages in memory, which is
//! comfortably fast at the limits below.

pub mod answer;
pub mod chunk;
pub mod extract;
pub mod search;

use serde::{Deserialize, Serialize};

/// The largest file a knowledge base takes.
pub const MAX_FILE_BYTES: u64 = 25 * 1024 * 1024;
/// The largest file attached to one brain question.
pub const MAX_ATTACHMENT_BYTES: u64 = 10 * 1024 * 1024;
/// Files per knowledge base.
pub const MAX_FILES_PER_BASE: usize = 500;
/// Knowledge bases per team.
pub const MAX_BASES: usize = 50;
/// Passages kept from one file; the rest of a very long file is dropped
/// (and the file says so).
pub const MAX_CHUNKS_PER_FILE: usize = 2_000;
/// A file still "processing" after this long stopped: it shows as failed
/// so it can be uploaded again.
pub const STALE_PROCESSING_SECS: i64 = 20 * 60;

/// The agents a base can be attached to, with their names on the page.
pub const AGENTS: &[(&str, &str)] = &[
    ("planner", "Planner"),
    ("coder", "Coder"),
    ("reviewer", "Reviewer"),
    ("plan_chat", "Plan chat"),
];

/// Who may read a base in the brain chat and on its page (lowest role).
pub const READ_ROLES: &[&str] = &["viewer", "member", "admin"];

/// Numeric role level, the same scale as the gateway's
/// `permissions::role_level` (a gateway test keeps them equal).
pub fn role_level(role: &str) -> u8 {
    match role {
        "owner" => 4,
        "admin" => 3,
        "billing" => 2,
        "member" => 1,
        _ => 0,
    }
}

/// A knowledge base.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Base {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// Lowest role that may read it (one of [`READ_ROLES`]).
    #[serde(default = "default_read_role")]
    pub read_role: String,
    /// Every agent of the team searches it.
    #[serde(default)]
    pub attach_team: bool,
    /// Agents (keys of [`AGENTS`]) that search it, when not the whole team.
    #[serde(default)]
    pub agents: Vec<String>,
    /// A source of truth: preferred over other sources when they disagree.
    #[serde(default)]
    pub authoritative: bool,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub created_by: String,
    #[serde(default)]
    pub updated_at: String,
}

fn default_read_role() -> String {
    "member".into()
}

impl Base {
    /// May someone with `role` read this base?
    pub fn readable_by(&self, role: &str) -> bool {
        role_level(role) >= role_level(&self.read_role)
    }

    /// Does `agent` (a key of [`AGENTS`]) search this base?
    pub fn attached_to(&self, agent: &str) -> bool {
        self.attach_team || self.agents.iter().any(|a| a == agent)
    }
}

/// Where a file is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FileStatus {
    /// Waiting for the browser's upload.
    #[default]
    Uploading,
    /// Being read, split and embedded.
    Processing,
    Ready,
    Failed,
}

/// One file of a base (its current version).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FileMeta {
    pub id: String,
    pub kb_id: String,
    pub name: String,
    /// The file's kind ([`extract::Kind::key`]).
    pub kind: String,
    pub size: u64,
    #[serde(default)]
    pub status: FileStatus,
    /// Why it failed, in plain words.
    #[serde(default)]
    pub error: String,
    /// Bumped on every re-upload; the objects live under it.
    #[serde(default = "one")]
    pub version: u32,
    /// The version that is searchable now (0: none yet). A re-upload keeps
    /// the last ready version searchable until the new one is ready.
    #[serde(default)]
    pub ready_version: u32,
    /// The document's own date (its last-modified stamp when the file
    /// carries one), else the upload date: what citations say it is "as of".
    #[serde(default)]
    pub as_of: String,
    #[serde(default)]
    pub uploaded_at: String,
    #[serde(default)]
    pub uploaded_by: String,
    /// When processing started (for [`STALE_PROCESSING_SECS`]).
    #[serde(default)]
    pub processing_since: String,
    #[serde(default)]
    pub chunks: u32,
    /// Passages dropped past [`MAX_CHUNKS_PER_FILE`].
    #[serde(default)]
    pub truncated: bool,
}

fn one() -> u32 {
    1
}

impl FileMeta {
    /// The status shown: processing that stopped long ago reads as failed.
    pub fn shown_status(&self, now: chrono::DateTime<chrono::Utc>) -> (FileStatus, String) {
        if self.status == FileStatus::Processing {
            let started = chrono::DateTime::parse_from_rfc3339(&self.processing_since)
                .map(|t| t.with_timezone(&chrono::Utc))
                .ok();
            if started.is_some_and(|t| (now - t).num_seconds() > STALE_PROCESSING_SECS) {
                return (
                    FileStatus::Failed,
                    "Processing stopped before it finished. Upload the file again.".into(),
                );
            }
        }
        (self.status, self.error.clone())
    }

    /// The index searched now, if any version is ready.
    pub fn searchable_index(&self, team_id: &str) -> Option<String> {
        (self.ready_version > 0)
            .then(|| index_key(team_id, &self.kb_id, &self.id, self.ready_version))
    }
}

/// A plain id (letters, digits, `-`, `_`; at most 64): anything else is
/// refused before it reaches a key.
pub fn valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// A new id (ULID, lower case).
pub fn new_id() -> String {
    ulid::Ulid::new().to_string().to_lowercase()
}

/// Settings-table sort key of a base.
pub fn base_sk(kb_id: &str) -> String {
    format!("KB#{kb_id}")
}

/// Prefix of every base's sort key.
pub const BASE_PREFIX: &str = "KB#";

/// Settings-table sort key of a file.
pub fn file_sk(kb_id: &str, file_id: &str) -> String {
    format!("KBFILE#{kb_id}#{file_id}")
}

/// Prefix of the sort keys of a base's files.
pub fn file_prefix(kb_id: &str) -> String {
    format!("KBFILE#{kb_id}#")
}

/// The team's knowledge folder in the bucket.
pub fn team_prefix(team_id: &str) -> String {
    format!("{team_id}/knowledge/")
}

/// The folder of one file (every version).
pub fn file_folder(team_id: &str, kb_id: &str, file_id: &str) -> String {
    format!("{team_id}/knowledge/{kb_id}/{file_id}/")
}

/// The uploaded file of one version.
pub fn source_key(team_id: &str, kb_id: &str, file_id: &str, version: u32) -> String {
    format!("{team_id}/knowledge/{kb_id}/{file_id}/v{version}/source")
}

/// The search index of one version.
pub fn index_key(team_id: &str, kb_id: &str, file_id: &str, version: u32) -> String {
    format!("{team_id}/knowledge/{kb_id}/{file_id}/v{version}/index.json")
}

/// A file attached to one brain question, kept with the asker's folder.
pub fn attachment_key(team_id: &str, user_id: &str, attachment_id: &str) -> String {
    format!("{team_id}/brain/{user_id}/{attachment_id}/source")
}

/// Is `key` inside `team_id`'s own folders? Every read of an index or an
/// attachment checks this, so a crafted id never reaches another team's data.
pub fn owns_key(team_id: &str, key: &str) -> bool {
    !team_id.is_empty()
        && !team_id.contains('/')
        && !team_id.contains("..")
        && !key.contains("..")
        && (key.starts_with(&team_prefix(team_id)) || key.starts_with(&format!("{team_id}/brain/")))
}

/// A file name made safe to show and store: no folders, no control
/// characters, at most 200 bytes.
pub fn clean_file_name(raw: &str) -> String {
    let base = raw.rsplit(['/', '\\']).next().unwrap_or("");
    let cleaned: String = base.chars().filter(|c| !c.is_control()).collect();
    crate::truncate_str(cleaned.trim(), 200).to_string()
}

// ── Records ────────────────────────────────────────────────────────────────

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

/// The settings-table record of a base.
pub fn base_record(team_id: &str, b: &Base) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".into(), AttributeValue::S(team_id.to_string())),
        ("sk".into(), AttributeValue::S(base_sk(&b.id))),
        (
            "data".into(),
            AttributeValue::S(serde_json::to_string(b).unwrap_or_default()),
        ),
    ])
}

/// The settings-table record of a file. `version` is kept as its own
/// attribute so a write can require the version it was started for (a
/// re-upload meanwhile wins).
pub fn file_record(team_id: &str, f: &FileMeta) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".into(), AttributeValue::S(team_id.to_string())),
        ("sk".into(), AttributeValue::S(file_sk(&f.kb_id, &f.id))),
        ("version".into(), AttributeValue::N(f.version.to_string())),
        (
            "data".into(),
            AttributeValue::S(serde_json::to_string(f).unwrap_or_default()),
        ),
    ])
}

/// A record's `data`, read as JSON.
pub fn read_data<T: for<'de> Deserialize<'de>>(
    item: &HashMap<String, AttributeValue>,
) -> Option<T> {
    serde_json::from_str(item.get("data")?.as_s().ok()?).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_roles_follow_the_role_ladder() {
        let base = Base {
            read_role: "member".into(),
            ..Default::default()
        };
        assert!(!base.readable_by("viewer"));
        assert!(base.readable_by("member"));
        assert!(base.readable_by("billing"));
        assert!(base.readable_by("owner"));
        let admins = Base {
            read_role: "admin".into(),
            ..Default::default()
        };
        assert!(!admins.readable_by("member"));
        assert!(admins.readable_by("admin"));
        assert!(!admins.readable_by("someone-made-up"));
        let all = Base {
            read_role: "viewer".into(),
            ..Default::default()
        };
        assert!(all.readable_by("viewer"));
        // A record saved without a role defaults to members.
        let old: Base = serde_json::from_str(r#"{"id":"k","name":"n"}"#).unwrap();
        assert_eq!(old.read_role, "member");
    }

    #[test]
    fn attaching_reaches_the_named_agents_or_everyone() {
        let some = Base {
            agents: vec!["reviewer".into()],
            ..Default::default()
        };
        assert!(some.attached_to("reviewer"));
        assert!(!some.attached_to("coder"));
        let team = Base {
            attach_team: true,
            ..Default::default()
        };
        assert!(AGENTS.iter().all(|(a, _)| team.attached_to(a)));
    }

    #[test]
    fn keys_stay_inside_the_team() {
        let k = index_key("t1", "kb", "f", 2);
        assert_eq!(k, "t1/knowledge/kb/f/v2/index.json");
        assert!(owns_key("t1", &k));
        assert!(!owns_key("t2", &k));
        assert!(!owns_key("t1", "t1/knowledge/../t2/knowledge/x"));
        assert!(!owns_key("t1", "t10/knowledge/kb/f/v1/index.json"));
        assert!(owns_key("t1", &attachment_key("t1", "u", "a")));
        assert!(!owns_key("t1", "t1/memory.tar.gz"));
        assert!(!owns_key("", "/knowledge/x"));
        // Team ids carry a prefix with '#'.
        assert!(owns_key("TEAM#g_1", &index_key("TEAM#g_1", "kb", "f", 1)));
        assert!(!owns_key("TEAM#g_1", &index_key("TEAM#g_12", "kb", "f", 1)));
    }

    #[test]
    fn ids_and_names_are_cleaned() {
        assert!(valid_id("01jabc_x-1"));
        assert!(!valid_id("a/b"));
        assert!(!valid_id(""));
        assert!(!valid_id(&"x".repeat(65)));
        assert_eq!(clean_file_name("../../etc/passwd"), "passwd");
        assert_eq!(clean_file_name("C:\\docs\\Q3 plan.pdf"), "Q3 plan.pdf");
        assert_eq!(clean_file_name("a\u{0}b.txt"), "ab.txt");
    }

    #[test]
    fn processing_that_stopped_reads_as_failed() {
        let now = chrono::Utc::now();
        let mut f = FileMeta {
            status: FileStatus::Processing,
            processing_since: (now - chrono::Duration::minutes(5)).to_rfc3339(),
            ..Default::default()
        };
        assert_eq!(f.shown_status(now).0, FileStatus::Processing);
        f.processing_since = (now - chrono::Duration::hours(2)).to_rfc3339();
        let (s, why) = f.shown_status(now);
        assert_eq!(s, FileStatus::Failed);
        assert!(why.contains("Upload the file again"));
    }

    #[test]
    fn records_round_trip_and_carry_the_version() {
        let f = FileMeta {
            id: "f".into(),
            kb_id: "kb".into(),
            name: "a.pdf".into(),
            version: 3,
            ..Default::default()
        };
        let r = file_record("t", &f);
        assert_eq!(r["sk"].as_s().unwrap(), "KBFILE#kb#f");
        assert_eq!(r["version"].as_n().unwrap(), "3");
        assert_eq!(read_data::<FileMeta>(&r).unwrap(), f);
        let b = Base {
            id: "kb".into(),
            name: "Docs".into(),
            ..Default::default()
        };
        assert_eq!(read_data::<Base>(&base_record("t", &b)).unwrap(), b);
    }

    #[test]
    fn only_a_ready_version_is_searched() {
        let mut f = FileMeta {
            id: "f".into(),
            kb_id: "kb".into(),
            version: 2,
            ..Default::default()
        };
        assert_eq!(f.searchable_index("t"), None);
        f.ready_version = 1;
        assert_eq!(
            f.searchable_index("t").as_deref(),
            Some("t/knowledge/kb/f/v1/index.json")
        );
    }
}
