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
//! asked for (cached per version) and ranks passages in memory; a big
//! synced source also keeps a search pack ([`pack`]) so only its likely
//! items' indexes are read.
//!
//! A base can instead be **connected in place** ([`Backend::Remote`]): an
//! Amazon Bedrock knowledge base in the team's own AWS account ([`remote`]),
//! or a search tool the team already runs behind an MCP server
//! ([`remote_mcp`]), searched live where it is. Nothing of it is copied: it
//! has no files, no passages and no vectors here, and search merges the list
//! the provider ranked with the local ones.
//!
//! Who reads a base: its lowest read role, and optionally only named people
//! or roles ([`Base::readers`], checked with [`Base::open_to`]). A base read
//! through each person's own sign-in ([`Remote::per_person`]) answers every
//! search with that person's access in the tool itself.

pub mod answer;
pub mod ask;
pub mod bedrock_sync;
pub mod chunk;
pub mod extract;
pub mod pack;
pub mod remote;
pub mod remote_mcp;
pub mod search;
pub mod sources;
pub mod usage;

use serde::{Deserialize, Serialize};

/// The largest file a knowledge base takes.
pub const MAX_FILE_BYTES: u64 = 25 * 1024 * 1024;
/// The largest file attached to one brain question.
pub const MAX_ATTACHMENT_BYTES: u64 = 10 * 1024 * 1024;
/// Files per knowledge base.
pub const MAX_FILES_PER_BASE: usize = 500;
/// Named readers one base keeps ([`Base::readers`]).
pub const MAX_READERS: usize = 50;
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
    /// Where its passages are: here ([`Backend::Local`], every base made
    /// before connecting in place existed), or searched live elsewhere.
    #[serde(default, skip_serializing_if = "Backend::is_local")]
    pub backend: Backend,
    /// Only these people and roles read it (on top of [`Self::read_role`]):
    /// `user:<email>` or `role:<role key>`. Empty: everyone the read role
    /// allows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub readers: Vec<String>,
}

/// A person asking: who they are and which role they hold on the team.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Person {
    /// Lowercased email.
    pub email: String,
    /// Their role's key (`member`, `admin`, a custom role's id).
    pub role: String,
}

impl Person {
    pub fn new(email: &str, role: &str) -> Person {
        Person {
            email: email.trim().to_ascii_lowercase(),
            role: role.trim().to_string(),
        }
    }
}

/// A reader entry as stored (`user:<email>`, `role:<key>`), or why it isn't
/// one.
pub fn clean_reader(raw: &str) -> Result<String, String> {
    let raw = raw.trim();
    let bad = || {
        format!("\"{raw}\" isn't a person or a role. Use an email, or role: and the role's name.")
    };
    let (kind, value) = match raw.split_once(':') {
        Some((k, v)) if k.eq_ignore_ascii_case("user") || k.eq_ignore_ascii_case("role") => {
            (k.to_ascii_lowercase(), v.trim())
        }
        _ if raw.contains('@') => ("user".to_string(), raw),
        _ => return Err(bad()),
    };
    if value.is_empty()
        || value.len() > 200
        || value.chars().any(|c| c.is_control() || c.is_whitespace())
    {
        return Err(bad());
    }
    if kind == "user" {
        let (local, domain) = value.split_once('@').ok_or_else(bad)?;
        if local.is_empty() || !domain.contains('.') {
            return Err(bad());
        }
        return Ok(format!("user:{}", value.to_ascii_lowercase()));
    }
    Ok(format!("role:{value}"))
}

/// Reader entries checked, duplicates dropped, at most [`MAX_READERS`].
pub fn clean_readers(raw: &[String]) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    for r in raw.iter().filter(|r| !r.trim().is_empty()) {
        let r = clean_reader(r)?;
        if !out.contains(&r) {
            out.push(r);
        }
    }
    if out.len() > MAX_READERS {
        return Err(format!("Name up to {MAX_READERS} people and roles."));
    }
    Ok(out)
}

/// Where a base's passages live and are ranked.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Backend {
    /// Uploaded and synced files, embedded and ranked by Coderhelm.
    #[default]
    Local,
    /// Searched live where it lives; nothing is copied.
    Remote(Remote),
}

impl Backend {
    pub fn is_local(&self) -> bool {
        matches!(self, Backend::Local)
    }
}

/// A knowledge base connected in place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Remote {
    pub provider: Provider,
    /// The provider's address for it (a Bedrock knowledge base's ARN).
    pub target: String,
    /// The region it lives in, read from the ARN (never the install's).
    pub region: String,
    /// How Coderhelm reaches it.
    pub access: Access,
    /// Bedrock's hybrid search (vectors and keywords) is used: true unless
    /// the knowledge base's vector store refused it when it was connected.
    #[serde(default = "yes")]
    pub hybrid: bool,
    /// Searched with each person's own sign-in (a search tool whose MCP
    /// server signs every person in): a person only gets what the tool lets
    /// them see, and a search nobody asked for (an agent run without a
    /// person) skips it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub per_person: bool,
    /// The search tool's tool that searches (`search`), for an MCP base.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub search_tool: String,
    /// Its tool that reads one result in full (`fetch`), when it has one.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub fetch_tool: String,
    /// What the provider calls it, when connected (a search tool's server
    /// name), for the page.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
}

fn yes() -> bool {
    true
}

/// Who answers for a connected knowledge base.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    /// Amazon Bedrock Knowledge Bases (`bedrock-agent-runtime` Retrieve).
    BedrockKb,
    /// A search tool behind an MCP server on the team's Connected apps
    /// (Azure AI Search, Pinecone Assistant, Glean, Rovo, Dropbox Dash, an
    /// AgentCore gateway, the team's own): its `search` and `fetch` tools.
    Mcp,
}

impl Provider {
    pub fn label(self) -> &'static str {
        match self {
            Provider::BedrockKb => "Bedrock knowledge base",
            Provider::Mcp => "Search tool (MCP)",
        }
    }

    /// What a passage from it is cited as.
    pub fn source_kind(self) -> SourceKind {
        match self {
            Provider::BedrockKb => SourceKind::BedrockKb,
            Provider::Mcp => SourceKind::McpSearch,
        }
    }
}

/// The AWS identity a connected knowledge base is searched with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Access {
    /// A self-hosted install's own role, in the install's own account
    /// (`live::aws::install_read_role`): its permissions come with each
    /// version update.
    Install,
    /// The team's connected `CoderHelmLogReader` role in `account`, taken
    /// on with the team's private ID for a 15 minute session
    /// (`live::aws::assume`). The private ID is read from Coderhelm's own
    /// record of the account, never stored here.
    Account { account: String },
    /// A server on the team's Connected apps (`mcp_remote`): its shared
    /// connection, or each person's own sign-in ([`Remote::per_person`]).
    /// Tokens stay sealed with the server's record, never here.
    Connection { server_id: String },
}

/// Where a passage came from, as a [`search::Hit`] and a citation name it
/// (`source_kind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    /// A file someone uploaded.
    Upload,
    /// An item synced from a connected app.
    Synced(sources::Kind),
    /// A passage a connected Bedrock knowledge base returned, live.
    BedrockKb,
    /// A passage a connected search tool (MCP) returned, live.
    McpSearch,
}

impl SourceKind {
    /// The key a hit and a citation carry (empty for an upload).
    pub fn key(self) -> &'static str {
        match self {
            SourceKind::Upload => "",
            SourceKind::Synced(k) => k.key(),
            SourceKind::BedrockKb => "bedrock_kb",
            SourceKind::McpSearch => "mcp",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            SourceKind::Upload => "Uploaded file",
            SourceKind::Synced(k) => k.label(),
            SourceKind::BedrockKb => Provider::BedrockKb.label(),
            SourceKind::McpSearch => Provider::Mcp.label(),
        }
    }

    pub fn parse(key: &str) -> Option<SourceKind> {
        match key {
            "" => Some(SourceKind::Upload),
            "bedrock_kb" => Some(SourceKind::BedrockKb),
            "mcp" => Some(SourceKind::McpSearch),
            k => sources::Kind::parse(k).map(SourceKind::Synced),
        }
    }
}

fn default_read_role() -> String {
    "member".into()
}

impl Base {
    /// Connected in place: searched live where it lives, nothing copied.
    pub fn remote(&self) -> Option<&Remote> {
        match &self.backend {
            Backend::Remote(r) => Some(r),
            Backend::Local => None,
        }
    }

    /// May someone with `role` read this base?
    pub fn readable_by(&self, role: &str) -> bool {
        role_level(role) >= role_level(&self.read_role)
    }

    /// Is the base open to `who` by name? Always, unless it names its
    /// readers ([`Self::readers`]); then only a named person or someone
    /// holding a named role. On top of [`Self::readable_by`], never instead.
    pub fn open_to(&self, who: &Person) -> bool {
        self.readers.is_empty()
            || self.readers.iter().any(|r| match r.split_once(':') {
                Some(("user", e)) => !who.email.is_empty() && e.eq_ignore_ascii_case(&who.email),
                Some(("role", k)) => !who.role.is_empty() && k == who.role,
                _ => false,
            })
    }

    /// Read only by named people or roles.
    pub fn named_readers(&self) -> bool {
        !self.readers.is_empty()
    }

    /// Searched with each person's own sign-in.
    pub fn per_person(&self) -> bool {
        self.remote().is_some_and(|r| r.per_person)
    }

    /// May an agent run started by `who` (`None`: nobody, such as a
    /// scheduled run) search this attached base? A base for named readers
    /// only in runs one of them started; a base read with each person's own
    /// sign-in only when a person started the run. Every other attached base
    /// is searched by every run, which the page says when it is attached
    /// ([`Self::agent_access_note`]).
    pub fn searchable_in_run(&self, who: Option<&Person>) -> bool {
        match who {
            Some(p) => self.open_to(p),
            None => !self.named_readers() && !self.per_person(),
        }
    }

    /// What attaching it to agents means for who sees its results, for the
    /// page (empty when it isn't attached to any agent).
    pub fn agent_access_note(&self) -> &'static str {
        if !self.attach_team && self.agents.is_empty() {
            return "";
        }
        if self.per_person() {
            "Agent runs search it as the person who started them, with their own sign-in. Runs nobody started skip it."
        } else if self.named_readers() {
            "Agents search it only in runs started by the people and roles named here. Runs anyone else started skip it."
        } else {
            "Everyone who can start an agent run gets results from this knowledge base when the agent searches it, whatever their own access in the app the content comes from."
        }
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
    /// The connected source this item was synced from ([`sources`]); empty
    /// for an uploaded file.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source_id: String,
    /// The item's id in the source (a Notion page id, a Drive file id).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub external_id: String,
    /// The original page, which citations link to.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub url: String,
    /// Where it sits in the source ("Engineering / Runbooks").
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub path: String,
    /// The source's own version marker (an edit time, a revision, a blob
    /// sha): an item whose marker is unchanged is not fetched again.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub remote_version: String,
    /// SHA-256 of the text that was indexed: a fetched item whose text is
    /// unchanged is not embedded again.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content_hash: String,
    /// Why it was left out (`empty`, `unsupported`, `too_large`); empty
    /// when it wasn't. A left-out item also has status `Failed` and why.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub skipped: String,
    /// When its text was last indexed (RFC 3339).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub indexed_at: String,
}

impl FileMeta {
    /// Synced from a connected source (not uploaded).
    pub fn is_synced(&self) -> bool {
        !self.source_id.is_empty()
    }
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
    fn every_existing_base_is_local_and_a_connected_one_round_trips() {
        // A record saved before backends existed reads as local, and a local
        // base is saved exactly as before (no new field).
        let old: Base = serde_json::from_str(r#"{"id":"k","name":"n"}"#).unwrap();
        assert_eq!(old.backend, Backend::Local);
        assert!(old.remote().is_none());
        let saved = serde_json::to_value(&old).unwrap();
        assert!(saved.get("backend").is_none(), "{saved}");
        let b = Base {
            id: "kb".into(),
            name: "Runbooks".into(),
            backend: Backend::Remote(Remote {
                provider: Provider::BedrockKb,
                target: "arn:aws:bedrock:eu-west-1:111122223333:knowledge-base/ABCDE12345".into(),
                region: "eu-west-1".into(),
                access: Access::Account {
                    account: "111122223333".into(),
                },
                hybrid: true,
                per_person: false,
                search_tool: String::new(),
                fetch_tool: String::new(),
                name: String::new(),
            }),
            ..Default::default()
        };
        let v = serde_json::to_value(&b).unwrap();
        assert_eq!(v["backend"]["kind"], "remote");
        assert_eq!(v["backend"]["provider"], "bedrock_kb");
        assert_eq!(v["backend"]["access"]["kind"], "account");
        assert_eq!(read_data::<Base>(&base_record("t", &b)).unwrap(), b);
        // No private ID is ever kept on the base.
        assert!(!v.to_string().contains("external"));
        let install: Access = serde_json::from_str(r#"{"kind":"install"}"#).unwrap();
        assert_eq!(install, Access::Install);
    }

    #[test]
    fn source_kinds_name_where_a_passage_came_from() {
        assert_eq!(SourceKind::BedrockKb.key(), "bedrock_kb");
        assert_eq!(SourceKind::BedrockKb.label(), "Bedrock knowledge base");
        assert_eq!(SourceKind::parse("bedrock_kb"), Some(SourceKind::BedrockKb));
        assert_eq!(
            SourceKind::parse("notion"),
            Some(SourceKind::Synced(sources::Kind::Notion))
        );
        assert_eq!(SourceKind::parse(""), Some(SourceKind::Upload));
        assert_eq!(SourceKind::parse("nope"), None);
    }

    #[test]
    fn named_readers_open_a_base_to_those_people_and_roles_only() {
        assert_eq!(
            clean_reader("Ada@Example.com").unwrap(),
            "user:ada@example.com"
        );
        assert_eq!(clean_reader("role:custom:r1").unwrap(), "role:custom:r1");
        assert_eq!(clean_reader("USER:bo@x.io").unwrap(), "user:bo@x.io");
        for bad in [
            "ada",
            "user:",
            "user:no-at",
            "role:",
            "role:two words",
            "x:y",
        ] {
            assert!(clean_reader(bad).is_err(), "{bad}");
        }
        let many: Vec<String> = (0..=MAX_READERS).map(|i| format!("u{i}@x.io")).collect();
        assert!(clean_readers(&many).is_err());
        assert_eq!(
            clean_readers(&["a@x.io".into(), "A@x.io".into(), " ".into()]).unwrap(),
            ["user:a@x.io"]
        );

        let open = Base::default();
        let ada = Person::new("Ada@Example.com", "member");
        let bo = Person::new("bo@example.com", "member");
        let admin = Person::new("cy@example.com", "admin");
        assert!(open.open_to(&ada) && open.open_to(&Person::default()));
        let named = Base {
            readers: vec!["user:ada@example.com".into(), "role:admin".into()],
            ..Default::default()
        };
        assert!(named.open_to(&ada));
        assert!(!named.open_to(&bo));
        assert!(named.open_to(&admin));
        assert!(!named.open_to(&Person::default()));
        // Runs: a named base only for its readers, never for nobody.
        assert!(named.searchable_in_run(Some(&ada)));
        assert!(!named.searchable_in_run(Some(&bo)));
        assert!(!named.searchable_in_run(None));
        assert!(open.searchable_in_run(None));
        // An old record has no readers and saves none.
        let old: Base = serde_json::from_str(r#"{"id":"k","name":"n"}"#).unwrap();
        assert!(old.readers.is_empty());
        assert!(serde_json::to_value(&old).unwrap().get("readers").is_none());
        assert_eq!(SourceKind::parse("mcp"), Some(SourceKind::McpSearch));
        assert_eq!(Provider::Mcp.source_kind().label(), "Search tool (MCP)");
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
