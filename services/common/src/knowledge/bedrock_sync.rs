//! Copying a team's knowledge bases into an Amazon Bedrock knowledge base in
//! the team's own AWS account, so Bedrock agents and applications can
//! retrieve from them where they run.
//!
//! An admin picks which of the team's bases are copied and where to:
//!
//! - **An S3 folder** ([`Target::S3`], self-hosted installs): each document
//!   is written as `<prefix><base>/<file>.txt` with the metadata file Bedrock
//!   reads beside it (`<file>.txt.metadata.json`, at most 10 KB). A Bedrock
//!   knowledge base's S3 data source reads the folder and picks up added,
//!   changed and deleted files when it syncs: the export stack
//!   ([`EXPORT_TEMPLATE`]) makes the bucket, only the install's own roles
//!   write it, and its own schedule syncs the data source every 15 minutes,
//!   so the install's role and permission limit never write a knowledge base.
//! - **A custom data source** ([`Target::Custom`], hosted and self-hosted):
//!   documents are pushed straight into the knowledge base with
//!   `IngestKnowledgeBaseDocuments` and removed with
//!   `DeleteKnowledgeBaseDocuments` (no ingestion job). Hosted Coderhelm calls
//!   them with the team's own `CoderHelmLogReader` role, taken on with the
//!   team's External ID; the role's stack names the knowledge bases it may
//!   write to (`SyncKnowledgeBases`). A self-hosted install in the same
//!   account uses its own role.
//!
//! A copied knowledge base has no per-person access: everyone with Bedrock
//! access to it reads everything in it. That is why only the bases an admin
//! names are copied, and saving needs the admin to say they understand it
//! ([`Config::acknowledged_at`]).
//!
//! What was copied is recorded per document ([`Synced`]), so each sync
//! ([`reconcile`]) writes only what changed and deletes what is gone: a
//! deleted file, a deleted base, a base taken off the list, or syncing turned
//! off. Passages' text never reaches a log; errors say only what Bedrock or
//! S3 refused.
//!
//! Storage, in the settings table:
//! - `pk = <team>`, `sk = BEDROCK_SYNC`: the [`Config`] and its [`Status`];
//! - `pk = <team>`, `sk = BEDROCK_SYNC_DOC#<doc id>`: one copied document;
//! - `pk = BEDROCK_SYNC_REGISTRY`, `sk = <team>`: teams the sweep syncs.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::search::{IndexedChunk, Store};
use super::{Base, FileMeta};

/// The stack a self-hosted install's AWS admin deploys for the S3 folder:
/// the bucket only the install writes, and optionally the knowledge base's
/// S3 data source reading it.
pub const EXPORT_TEMPLATE: &str =
    include_str!("../../../../infra/cfn/coderhelm-bedrock-export.yaml");

pub const CONFIG_SK: &str = "BEDROCK_SYNC";
pub const DOC_PREFIX: &str = "BEDROCK_SYNC_DOC#";
pub const REGISTRY_PK: &str = "BEDROCK_SYNC_REGISTRY";

/// Documents per `IngestKnowledgeBaseDocuments` or `DeleteKnowledgeBaseDocuments`
/// call. The API reference allows 1 to 10 (the quotas page says 25); 10 is
/// safe under both.
pub const BATCH_DOCS: usize = 10;
/// Bytes of text per call: under the 6 MB request limit, with room for the
/// metadata and the JSON around it.
pub const BATCH_BYTES: usize = 5_000_000;
/// The longest document text copied; a longer one is cut there (and the
/// status says so).
pub const MAX_DOC_BYTES: usize = 4_500_000;
/// A metadata file Bedrock reads beside an S3 document is at most 10 KB.
pub const MAX_METADATA_BYTES: usize = 10 * 1024;
/// Errors the status keeps.
pub const MAX_ERRORS: usize = 10;

/// Asks the worker to sync one team's copy now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncMessage {
    pub team_id: String,
}

// ── Settings ─────────────────────────────────────────────────────────────────

/// Where the copies go.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Target {
    #[default]
    None,
    /// A folder in an S3 bucket in the install's own account, read by a
    /// Bedrock S3 data source. `knowledge_base_arn` and `data_source_id`,
    /// when set, name that data source, so saving checks it.
    S3 {
        bucket: String,
        prefix: String,
        region: String,
        #[serde(default)]
        knowledge_base_arn: String,
        #[serde(default)]
        data_source_id: String,
    },
    /// A custom data source of a knowledge base: documents pushed directly.
    Custom {
        knowledge_base_arn: String,
        data_source_id: String,
    },
}

impl Target {
    pub fn kind(&self) -> &'static str {
        match self {
            Target::None => "none",
            Target::S3 { .. } => "s3",
            Target::Custom { .. } => "custom",
        }
    }

    /// The knowledge base it feeds, when named.
    pub fn knowledge_base_arn(&self) -> Option<&str> {
        match self {
            Target::S3 {
                knowledge_base_arn, ..
            }
            | Target::Custom {
                knowledge_base_arn, ..
            } => Some(knowledge_base_arn.as_str()).filter(|s| !s.is_empty()),
            Target::None => None,
        }
    }
}

/// A team's sync to Bedrock.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub target: Target,
    /// The bases copied, by id. Only these, never every base.
    #[serde(default)]
    pub bases: Vec<String>,
    /// When an admin confirmed that everyone with Bedrock access to the
    /// knowledge base reads everything copied into it.
    #[serde(default)]
    pub acknowledged_at: String,
    #[serde(default)]
    pub acknowledged_by: String,
    #[serde(default)]
    pub updated_at: String,
    #[serde(default)]
    pub updated_by: String,
    /// The team's name, for each document's metadata.
    #[serde(default)]
    pub team_name: String,
    #[serde(default)]
    pub status: Status,
}

/// How the last sync went.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Status {
    /// `never`, `ok`, `partial` (some documents failed) or `failed`.
    #[serde(default = "never")]
    pub state: String,
    #[serde(default)]
    pub last_sync_at: String,
    #[serde(default)]
    pub last_success_at: String,
    /// Documents in Bedrock now, as far as Coderhelm copied them.
    #[serde(default)]
    pub documents: u32,
    #[serde(default)]
    pub added: u32,
    #[serde(default)]
    pub updated: u32,
    #[serde(default)]
    pub removed: u32,
    /// What went wrong, in plain words (never a passage's text).
    #[serde(default)]
    pub errors: Vec<String>,
    /// The last ingestion job started on an S3 data source.
    #[serde(default)]
    pub ingestion_job_id: String,
    /// Changes are in S3 but no ingestion job took them yet (one was
    /// already running): the next sync starts one.
    #[serde(default)]
    pub needs_ingestion: bool,
    /// A sync holds the team until this time (seconds), so two never run
    /// at once.
    #[serde(default)]
    pub lease_until: i64,
}

fn never() -> String {
    "never".into()
}

fn bucket_ok(b: &str) -> bool {
    (3..=63).contains(&b.len())
        && b.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
        && b.starts_with(|c: char| c.is_ascii_alphanumeric())
        && b.ends_with(|c: char| c.is_ascii_alphanumeric())
        && !b.contains("..")
}

/// A folder in a bucket: empty, or ending in `/`, without `..`, a leading
/// slash or unusual characters; at most 200 characters (the data source's
/// one inclusion prefix may be 300).
pub fn clean_prefix(raw: &str) -> Result<String, String> {
    let p = raw.trim().trim_start_matches('/');
    if p.is_empty() {
        return Ok(String::new());
    }
    let p = if p.ends_with('/') {
        p.to_string()
    } else {
        format!("{p}/")
    };
    let ok = p.len() <= 200
        && !p.contains("..")
        && !p.contains("//")
        && p.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/'));
    if ok {
        Ok(p)
    } else {
        Err(
            "Use a folder made of letters, digits, dashes, dots and slashes, such as coderhelm/."
                .into(),
        )
    }
}

fn data_source_ok(id: &str) -> bool {
    id.len() == 10
        && id
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
}

/// The target as an admin entered it, checked and cleaned, or what is wrong
/// in plain words. An S3 folder is only for a self-hosted install, whose
/// own role writes it.
pub fn check_target(t: &Target, self_hosted: bool) -> Result<Target, String> {
    let ds_fix =
        "Copy the data source's ID (10 letters and digits) from its page in the Bedrock console.";
    match t {
        Target::None => Err("Choose where to copy the knowledge bases.".into()),
        Target::S3 {
            bucket,
            prefix,
            region,
            knowledge_base_arn,
            data_source_id,
        } => {
            if !self_hosted {
                return Err("Copying to an S3 folder is for self-hosted installs. Use a custom data source instead.".into());
            }
            let bucket = bucket.trim().to_string();
            if !bucket_ok(&bucket) {
                return Err(
                    "Enter the bucket's name, such as coderhelm-prod-kb-export-111122223333."
                        .into(),
                );
            }
            let prefix = clean_prefix(prefix)?;
            let kb = knowledge_base_arn.trim();
            let ds = data_source_id.trim();
            let (kb, region) = if kb.is_empty() {
                if !ds.is_empty() {
                    return Err("Name the knowledge base the data source belongs to.".into());
                }
                (String::new(), region.trim().to_string())
            } else {
                let arn = super::remote::parse_kb_arn(kb)?;
                if !data_source_ok(ds) {
                    return Err(ds_fix.into());
                }
                (arn.arn(), arn.region)
            };
            Ok(Target::S3 {
                bucket,
                prefix,
                region,
                knowledge_base_arn: kb,
                data_source_id: ds.to_string(),
            })
        }
        Target::Custom {
            knowledge_base_arn,
            data_source_id,
        } => {
            let arn = super::remote::parse_kb_arn(knowledge_base_arn)?;
            // The install's own role never writes a knowledge base (its
            // permission limit doesn't allow it): in its own account it
            // copies into an S3 folder instead.
            if self_hosted && in_own_account(&arn.account) {
                return Err("A self-hosted install copies into a knowledge base in its own account through an S3 folder. Choose S3 folder, or a knowledge base in another connected account.".into());
            }
            let ds = data_source_id.trim();
            if !data_source_ok(ds) {
                return Err(ds_fix.into());
            }
            Ok(Target::Custom {
                knowledge_base_arn: arn.arn(),
                data_source_id: ds.to_string(),
            })
        }
    }
}

fn in_own_account(account: &str) -> bool {
    crate::live::aws::install_read_role()
        .as_deref()
        .and_then(|r| r.split(':').nth(4))
        == Some(account)
}

/// The bases that may be copied: the team's own (files here), not ones
/// connected in place (they already live elsewhere).
pub fn can_copy(b: &Base) -> bool {
    b.remote().is_none()
}

/// The access warning the admin confirms, word for word as the page shows it.
pub const ACCESS_WARNING: &str = "A Bedrock knowledge base has no per-person access. Everyone with Bedrock access to it, and every agent or application that retrieves from it, can read every document copied into it, whatever their access in Coderhelm or in the app the document came from.";

// ── Documents ────────────────────────────────────────────────────────────────

/// What a copied document says about itself (metadata Bedrock can filter on).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct DocMeta {
    pub title: String,
    pub link: String,
    /// The connector it came from (`notion`, `confluence`), or `upload`.
    pub source: String,
    pub team: String,
    pub base: String,
    pub base_id: String,
    pub file_id: String,
    pub updated_at: String,
}

impl DocMeta {
    /// Name and value of each attribute, in a steady order, each cut so the
    /// whole stays under [`MAX_METADATA_BYTES`].
    pub fn attributes(&self) -> Vec<(&'static str, String)> {
        let cut = |s: &str, n: usize| crate::truncate_str(s, n).to_string();
        vec![
            ("title", cut(&self.title, 300)),
            ("link", cut(&self.link, 2_000)),
            ("source", cut(&self.source, 40)),
            ("team", cut(&self.team, 200)),
            ("base", cut(&self.base, 200)),
            ("base_id", cut(&self.base_id, 64)),
            ("file_id", cut(&self.file_id, 64)),
            ("updated_at", cut(&self.updated_at, 40)),
        ]
    }
}

/// One document to copy.
#[derive(Debug, Clone, PartialEq)]
pub struct ExportDoc {
    pub id: String,
    pub kb_id: String,
    pub file_id: String,
    pub text: String,
    pub meta: DocMeta,
    /// The text was cut at [`MAX_DOC_BYTES`].
    pub truncated: bool,
}

/// A copied document's id in Bedrock (a custom document's id): the base and
/// the file, which are ids already.
pub fn doc_id(kb_id: &str, file_id: &str) -> String {
    format!("{kb_id}-{file_id}")
}

/// The document's object and its metadata file in an S3 folder.
pub fn s3_keys(prefix: &str, kb_id: &str, file_id: &str) -> (String, String) {
    let doc = format!("{prefix}{kb_id}/{file_id}.txt");
    let meta = format!("{doc}.metadata.json");
    (doc, meta)
}

/// The metadata file Bedrock reads beside an S3 document
/// (`{"metadataAttributes": {...}}`). Every value is a string, kept out of
/// the embedding (it is for filtering and citing).
pub fn s3_metadata(meta: &DocMeta) -> String {
    let mut attrs = serde_json::Map::new();
    for (k, v) in meta.attributes() {
        attrs.insert(
            k.to_string(),
            serde_json::json!({
                "value": { "type": "STRING", "stringValue": v },
                "includeForEmbedding": false,
            }),
        );
    }
    let mut out = serde_json::json!({ "metadataAttributes": attrs }).to_string();
    // Cut the link, then the title, should a very long one still not fit.
    if out.len() > MAX_METADATA_BYTES {
        let mut m = meta.clone();
        m.link = crate::truncate_str(&m.link, 500).to_string();
        m.title = crate::truncate_str(&m.title, 100).to_string();
        out = s3_metadata_unchecked(&m);
    }
    out
}

fn s3_metadata_unchecked(meta: &DocMeta) -> String {
    let attrs: serde_json::Map<String, serde_json::Value> = meta
        .attributes()
        .into_iter()
        .map(|(k, v)| {
            (
                k.to_string(),
                serde_json::json!({ "value": { "type": "STRING", "stringValue": v }, "includeForEmbedding": false }),
            )
        })
        .collect();
    serde_json::json!({ "metadataAttributes": attrs }).to_string()
}

/// The longest end of `a` that `b` starts with, up to `max` bytes.
fn overlap(a: &str, b: &str, max: usize) -> usize {
    let mut n = max.min(a.len()).min(b.len());
    while n > 0 {
        if a.is_char_boundary(a.len() - n) && b.is_char_boundary(n) && a.ends_with(&b[..n]) {
            return n;
        }
        n -= 1;
    }
    0
}

/// A document's text again from its passages: in order, a heading where
/// the place in the document changes (`## Page 3`), and the tail each
/// passage repeats from the one before ([`super::chunk::OVERLAP_CHARS`])
/// left out.
pub fn document_text(chunks: &[IndexedChunk]) -> String {
    let mut sorted: Vec<&IndexedChunk> = chunks.iter().collect();
    sorted.sort_by_key(|c| c.ord);
    let mut out = String::new();
    let mut prev_loc: Option<&str> = None;
    let mut prev_text = "";
    // Bytes: a passage repeats about this many characters, which may be
    // up to four bytes each.
    let max = super::chunk::OVERLAP_CHARS * 4 + 64;
    for c in sorted {
        let same = prev_loc == Some(c.loc.as_str());
        let text = c.text.trim_end();
        if !same {
            if !out.is_empty() {
                out.push_str("\n\n");
            }
            if !c.loc.trim().is_empty() {
                out.push_str("## ");
                out.push_str(c.loc.trim());
                out.push_str("\n\n");
            }
            out.push_str(text);
        } else {
            let n = overlap(prev_text, text, max);
            let rest = &text[n..];
            if n == 0 {
                out.push_str("\n\n");
            }
            out.push_str(rest);
        }
        prev_loc = Some(c.loc.as_str());
        prev_text = text;
    }
    out
}

/// What decides whether a copy is current: the version searched, the
/// text's hash and what the metadata says. Unchanged, the document isn't
/// read or written again.
pub fn fingerprint(team_name: &str, base: &Base, f: &FileMeta) -> String {
    let seed = format!(
        "{}|{}|{}|{}|{}|{}|{}|{}",
        f.ready_version, f.content_hash, f.as_of, f.url, f.name, base.name, team_name, f.kind
    );
    hex::encode(&Sha256::digest(seed.as_bytes())[..12])
}

/// Where a document links to: the original page for a synced item, the
/// base's page in Coderhelm for an uploaded file.
pub fn link(app_url: &str, base: &Base, f: &FileMeta) -> String {
    if !f.url.is_empty() {
        f.url.clone()
    } else {
        format!("{app_url}/knowledge/base?id={}", base.id)
    }
}

// ── Planning ─────────────────────────────────────────────────────────────────

/// One document as copied.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Synced {
    pub doc_id: String,
    pub kb_id: String,
    pub file_id: String,
    pub fingerprint: String,
    #[serde(default)]
    pub synced_at: String,
}

/// One document that should be in Bedrock.
#[derive(Debug, Clone, PartialEq)]
pub struct Wanted {
    pub doc_id: String,
    pub kb_id: String,
    pub file_id: String,
    pub fingerprint: String,
    pub index_key: String,
    pub meta: DocMeta,
}

/// What a sync does: documents to write (new or changed, by position in
/// `wanted`) and copies to delete.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Plan {
    pub add: Vec<usize>,
    pub update: Vec<usize>,
    pub delete: Vec<Synced>,
}

pub fn plan(wanted: &[Wanted], synced: &[Synced]) -> Plan {
    let had: HashMap<&str, &Synced> = synced.iter().map(|s| (s.doc_id.as_str(), s)).collect();
    let want: std::collections::HashSet<&str> = wanted.iter().map(|w| w.doc_id.as_str()).collect();
    let mut p = Plan::default();
    for (i, w) in wanted.iter().enumerate() {
        match had.get(w.doc_id.as_str()) {
            None => p.add.push(i),
            Some(s) if s.fingerprint != w.fingerprint => p.update.push(i),
            Some(_) => {}
        }
    }
    for s in synced {
        if !want.contains(s.doc_id.as_str()) {
            p.delete.push(s.clone());
        }
    }
    p
}

/// The documents that should be in Bedrock for `cfg`: every searchable
/// file of each chosen base the team still has. Nothing when syncing is
/// off, so turning it off removes every copy. Also the notes for chosen
/// bases that can't be copied.
pub async fn wanted(
    store: &dyn Store,
    team_id: &str,
    team_name: &str,
    app_url: &str,
    cfg: &Config,
) -> Result<(Vec<Wanted>, Vec<String>), String> {
    let mut out = Vec::new();
    let mut notes = Vec::new();
    if !cfg.enabled || cfg.target == Target::None {
        return Ok((out, notes));
    }
    for id in &cfg.bases {
        let Some(base) = store.base(team_id, id).await? else {
            continue;
        };
        if !can_copy(&base) {
            notes.push(format!(
                "{} is connected in place, so it isn't copied.",
                base.name
            ));
            continue;
        }
        for f in store.files(team_id, &base.id).await? {
            if f.kb_id != base.id {
                continue;
            }
            let Some(index_key) = f.searchable_index(team_id) else {
                continue;
            };
            out.push(Wanted {
                doc_id: doc_id(&base.id, &f.id),
                kb_id: base.id.clone(),
                file_id: f.id.clone(),
                fingerprint: fingerprint(team_name, &base, &f),
                index_key,
                meta: DocMeta {
                    title: f.name.clone(),
                    link: link(app_url, &base, &f),
                    source: if f.is_synced() {
                        f.kind.clone()
                    } else {
                        "upload".into()
                    },
                    team: team_name.to_string(),
                    base: base.name.clone(),
                    base_id: base.id.clone(),
                    file_id: f.id.clone(),
                    updated_at: if f.as_of.is_empty() {
                        f.uploaded_at.clone()
                    } else {
                        f.as_of.clone()
                    },
                },
            });
        }
    }
    out.sort_by(|a, b| a.doc_id.cmp(&b.doc_id));
    Ok((out, notes))
}

// ── Where copies go ──────────────────────────────────────────────────────────

/// Writes and deletes copies. Each call answers per document, in order.
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    async fn put(&self, docs: &[ExportDoc]) -> Vec<Result<(), String>>;
    async fn delete(&self, docs: &[Synced]) -> Vec<Result<(), String>>;
    /// After the writes: an S3 folder's ingestion job (its id), when
    /// `changed` and one is named. `Ok(None)` when nothing was started;
    /// `Err("busy")` when one was already running.
    async fn finish(&self, changed: bool) -> Result<Option<String>, String>;
}

/// The record of what was copied.
#[async_trait::async_trait]
pub trait SyncState: Send + Sync {
    async fn synced(&self, team_id: &str) -> Result<Vec<Synced>, String>;
    async fn mark(&self, team_id: &str, s: &Synced) -> Result<(), String>;
    async fn forget(&self, team_id: &str, doc_id: &str) -> Result<(), String>;
}

/// What one sync did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Outcome {
    pub added: u32,
    pub updated: u32,
    pub removed: u32,
    pub documents: u32,
    pub errors: Vec<String>,
    pub ingestion_job_id: String,
    pub needs_ingestion: bool,
}

impl Outcome {
    fn note(&mut self, e: String) {
        if self.errors.len() < MAX_ERRORS && !self.errors.contains(&e) {
            self.errors.push(e);
        }
    }

    /// The status after this sync, at `now`.
    pub fn status(&self, before: &Status, now: &str, failed: bool) -> Status {
        let state = if failed {
            "failed"
        } else if self.errors.is_empty() {
            "ok"
        } else {
            "partial"
        };
        Status {
            state: state.into(),
            last_sync_at: now.to_string(),
            last_success_at: if failed {
                before.last_success_at.clone()
            } else {
                now.to_string()
            },
            documents: self.documents,
            added: self.added,
            updated: self.updated,
            removed: self.removed,
            errors: self.errors.clone(),
            ingestion_job_id: if self.ingestion_job_id.is_empty() {
                before.ingestion_job_id.clone()
            } else {
                self.ingestion_job_id.clone()
            },
            needs_ingestion: self.needs_ingestion,
            lease_until: 0,
        }
    }
}

/// Reads one document's text from its index, cut at [`MAX_DOC_BYTES`].
async fn read_doc(store: &dyn Store, team_id: &str, w: &Wanted) -> Result<ExportDoc, String> {
    let index = store.index(team_id, &w.index_key).await?;
    let mut text = document_text(&index.chunks);
    let truncated = text.len() > MAX_DOC_BYTES;
    if truncated {
        text = crate::truncate_str(&text, MAX_DOC_BYTES).to_string();
    }
    Ok(ExportDoc {
        id: w.doc_id.clone(),
        kb_id: w.kb_id.clone(),
        file_id: w.file_id.clone(),
        text,
        meta: w.meta.clone(),
        truncated,
    })
}

/// Groups of documents for one call each: at most [`BATCH_DOCS`] and
/// [`BATCH_BYTES`] of text.
pub fn batches(docs: &[ExportDoc]) -> Vec<std::ops::Range<usize>> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut bytes = 0;
    for (i, d) in docs.iter().enumerate() {
        let n = d.text.len();
        if i > start && (i - start >= BATCH_DOCS || bytes + n > BATCH_BYTES) {
            out.push(start..i);
            start = i;
            bytes = 0;
        }
        bytes += n;
    }
    if start < docs.len() {
        out.push(start..docs.len());
    }
    out
}

/// One sync: copies what is new or changed, deletes what is gone, and
/// records each document as it lands. A document that fails stays as it
/// was recorded, so the next sync tries it again.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile(
    store: &dyn Store,
    state: &dyn SyncState,
    sink: &dyn Sink,
    team_id: &str,
    team_name: &str,
    app_url: &str,
    cfg: &Config,
    now: &str,
) -> Result<Outcome, String> {
    let (wanted, notes) = wanted(store, team_id, team_name, app_url, cfg).await?;
    let synced = state.synced(team_id).await?;
    let p = plan(&wanted, &synced);
    let mut out = Outcome::default();
    for n in notes {
        out.note(n);
    }
    let mut count = synced.len() as i64;
    let mut changed = false;

    // Deletes first: a base taken off the list leaves Bedrock before
    // anything else is written.
    for group in p.delete.chunks(BATCH_DOCS) {
        let results = sink.delete(group).await;
        for (s, r) in group.iter().zip(results) {
            match r {
                Ok(()) => {
                    state.forget(team_id, &s.doc_id).await?;
                    out.removed += 1;
                    count -= 1;
                    changed = true;
                }
                Err(e) => out.note(format!("A copy couldn't be removed: {e}")),
            }
        }
    }

    let writes: Vec<(usize, bool)> = p
        .add
        .iter()
        .map(|i| (*i, true))
        .chain(p.update.iter().map(|i| (*i, false)))
        .collect();
    // Read and write a few at a time, so a large base never sits in memory.
    for group in writes.chunks(BATCH_DOCS) {
        let mut docs = Vec::new();
        let mut which = Vec::new();
        for (i, new) in group {
            let w = &wanted[*i];
            match read_doc(store, team_id, w).await {
                Ok(d) => {
                    if d.truncated {
                        out.note(format!(
                            "{} is longer than Bedrock takes in one document; its start was copied.",
                            w.meta.title
                        ));
                    }
                    docs.push(d);
                    which.push((*i, *new));
                }
                Err(e) => {
                    tracing::warn!(doc = %w.doc_id, error = %e, "Bedrock sync: index unreadable");
                    out.note(format!(
                        "{} couldn't be read. It is tried again on the next sync.",
                        w.meta.title
                    ));
                }
            }
        }
        for range in batches(&docs) {
            let part = &docs[range.clone()];
            let results = sink.put(part).await;
            for ((i, new), r) in which[range].iter().zip(results) {
                let w = &wanted[*i];
                match r {
                    Ok(()) => {
                        state
                            .mark(
                                team_id,
                                &Synced {
                                    doc_id: w.doc_id.clone(),
                                    kb_id: w.kb_id.clone(),
                                    file_id: w.file_id.clone(),
                                    fingerprint: w.fingerprint.clone(),
                                    synced_at: now.to_string(),
                                },
                            )
                            .await?;
                        changed = true;
                        if *new {
                            out.added += 1;
                            count += 1;
                        } else {
                            out.updated += 1;
                        }
                    }
                    Err(e) => out.note(format!("{} wasn't copied: {e}", w.meta.title)),
                }
            }
        }
    }
    out.documents = count.max(0) as u32;
    match sink.finish(changed || cfg.status.needs_ingestion).await {
        Ok(Some(job)) => out.ingestion_job_id = job,
        Ok(None) => {}
        Err(e) if e == "busy" => {
            out.needs_ingestion = true;
            out.note("Bedrock was already syncing the data source; the next sync starts another so these changes go in.".into());
        }
        Err(e) => {
            out.needs_ingestion = true;
            out.note(format!("The data source's sync didn't start: {e}"));
        }
    }
    Ok(out)
}

// ── Records ──────────────────────────────────────────────────────────────────

use aws_sdk_dynamodb::types::AttributeValue;

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

/// The team's config, if it ever set one.
pub async fn load(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
) -> Result<Option<Config>, String> {
    let r = dynamo
        .get_item()
        .table_name(table)
        .key("pk", s(team_id))
        .key("sk", s(CONFIG_SK))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("bedrock sync read: {e}"))?;
    Ok(r.item().and_then(super::read_data::<Config>))
}

/// Saves the config (and the registry row the sweep reads).
pub async fn save(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    cfg: &Config,
) -> Result<(), String> {
    let data = serde_json::to_string(cfg).map_err(|e| e.to_string())?;
    dynamo
        .put_item()
        .table_name(table)
        .item("pk", s(team_id))
        .item("sk", s(CONFIG_SK))
        .item("data", s(&data))
        .send()
        .await
        .map_err(|e| format!("bedrock sync save: {e}"))?;
    dynamo
        .put_item()
        .table_name(table)
        .item("pk", s(REGISTRY_PK))
        .item("sk", s(team_id))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("bedrock sync registry: {e}"))
}

/// Takes the team's sync until `until` when no other sync holds it.
pub async fn take_lease(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    cfg: &Config,
    now: i64,
    until: i64,
) -> Result<bool, String> {
    let mut held = cfg.clone();
    held.status.lease_until = until;
    let data = serde_json::to_string(&held).map_err(|e| e.to_string())?;
    // The record is unchanged since it was read (its data the same), and
    // its lease ran out.
    let before = serde_json::to_string(cfg).map_err(|e| e.to_string())?;
    if cfg.status.lease_until > now {
        return Ok(false);
    }
    match dynamo
        .put_item()
        .table_name(table)
        .item("pk", s(team_id))
        .item("sk", s(CONFIG_SK))
        .item("data", s(&data))
        .condition_expression("#d = :before")
        .expression_attribute_names("#d", "data")
        .expression_attribute_values(":before", s(&before))
        .send()
        .await
    {
        Ok(_) => Ok(true),
        Err(e) if format!("{e:?}").contains("ConditionalCheckFailed") => Ok(false),
        Err(e) => Err(format!("bedrock sync lease: {e}")),
    }
}

/// Writes the status of a finished sync onto the latest config (an admin
/// may have saved meanwhile; their settings win, the status is the sync's).
pub async fn finish_status(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    status: Status,
) -> Result<(), String> {
    let mut cfg = load(dynamo, table, team_id).await?.unwrap_or_default();
    cfg.status = status;
    save(dynamo, table, team_id, &cfg).await
}

/// Every team with a sync on the registry.
pub async fn registry(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    loop {
        let r = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", s(REGISTRY_PK))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| format!("bedrock sync registry: {e}"))?;
        for item in r.items() {
            if let Some(t) = item.get("sk").and_then(|v| v.as_s().ok()) {
                out.push(t.clone());
            }
        }
        match r.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(out)
}

/// Takes a team off the registry (syncing is off and nothing is left in
/// Bedrock).
pub async fn unregister(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
) -> Result<(), String> {
    dynamo
        .delete_item()
        .table_name(table)
        .key("pk", s(REGISTRY_PK))
        .key("sk", s(team_id))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("bedrock sync registry: {e}"))
}

/// [`SyncState`] on the settings table.
pub struct DynamoState {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub table: String,
}

#[async_trait::async_trait]
impl SyncState for DynamoState {
    async fn synced(&self, team_id: &str) -> Result<Vec<Synced>, String> {
        let mut out = Vec::new();
        let mut start: Option<HashMap<String, AttributeValue>> = None;
        loop {
            let r = self
                .dynamo
                .query()
                .table_name(&self.table)
                .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
                .expression_attribute_values(":pk", s(team_id))
                .expression_attribute_values(":p", s(DOC_PREFIX))
                .set_exclusive_start_key(start.take())
                .consistent_read(true)
                .send()
                .await
                .map_err(|e| format!("bedrock sync docs: {e}"))?;
            for item in r.items() {
                if let Some(d) = super::read_data::<Synced>(item) {
                    out.push(d);
                }
            }
            match r.last_evaluated_key() {
                Some(k) if !k.is_empty() => start = Some(k.clone()),
                _ => break,
            }
        }
        Ok(out)
    }

    async fn mark(&self, team_id: &str, d: &Synced) -> Result<(), String> {
        let data = serde_json::to_string(d).map_err(|e| e.to_string())?;
        self.dynamo
            .put_item()
            .table_name(&self.table)
            .item("pk", s(team_id))
            .item("sk", s(&format!("{DOC_PREFIX}{}", d.doc_id)))
            .item("data", s(&data))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("bedrock sync doc save: {e}"))
    }

    async fn forget(&self, team_id: &str, doc_id: &str) -> Result<(), String> {
        self.dynamo
            .delete_item()
            .table_name(&self.table)
            .key("pk", s(team_id))
            .key("sk", s(&format!("{DOC_PREFIX}{doc_id}")))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("bedrock sync doc delete: {e}"))
    }
}

// ── AWS ──────────────────────────────────────────────────────────────────────

use crate::live::aws::Session;

fn creds(s: &Session) -> aws_sdk_bedrockagent::config::Credentials {
    aws_sdk_bedrockagent::config::Credentials::new(
        &s.access_key_id,
        &s.secret_access_key,
        (!s.session_token.is_empty()).then(|| s.session_token.clone()),
        None,
        "coderhelm-bedrock-sync",
    )
}

pub fn agent_client(s: &Session, region: &str) -> aws_sdk_bedrockagent::Client {
    use aws_sdk_bedrockagent::config::{BehaviorVersion, Region};
    aws_sdk_bedrockagent::Client::from_conf(
        aws_sdk_bedrockagent::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .credentials_provider(creds(s))
            .build(),
    )
}

pub fn s3_client(s: &Session, region: &str) -> aws_sdk_s3::Client {
    use aws_sdk_s3::config::{BehaviorVersion, Region};
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .credentials_provider(creds(s))
            .build(),
    )
}

/// An AWS error in plain words: what was refused and how to fix it. Never
/// the request (which holds the document).
pub fn plain_error(code: &str, message: &str) -> String {
    let detail = crate::live::error_detail(code, message);
    match code {
        "AccessDeniedException" | "AccessDenied" => {
            if crate::live::boundary_refusal(message) {
                format!(
                    "This install's permission limit doesn't allow it yet ({detail}). {}",
                    crate::live::PENDING_PERMISSION_FIX
                )
            } else {
                format!("The role Coderhelm uses isn't allowed to ({detail}). Update the role's stack with this knowledge base in SyncKnowledgeBases, or check the bucket policy.")
            }
        }
        "ResourceNotFoundException" | "NoSuchBucket" => {
            format!(
                "AWS has no such knowledge base, data source or bucket ({detail}). Check the IDs."
            )
        }
        "ValidationException" => {
            format!("Bedrock refused it ({detail}). A direct push needs a custom data source.")
        }
        "ThrottlingException" | "ServiceQuotaExceededException" | "SlowDown" => {
            "AWS is limiting requests right now; the next sync tries again.".into()
        }
        "" => format!("Coderhelm couldn't reach AWS ({detail})."),
        _ => format!("AWS refused it ({detail})."),
    }
}

fn sdk_error<E, R>(e: &aws_sdk_bedrockagent::error::SdkError<E, R>) -> (String, String)
where
    E: aws_sdk_bedrockagent::error::ProvideErrorMetadata + std::error::Error + 'static,
    R: std::fmt::Debug,
{
    match e.as_service_error() {
        Some(se) => (
            se.code().unwrap_or("Unknown").to_string(),
            se.message().unwrap_or("").to_string(),
        ),
        None => (
            String::new(),
            aws_sdk_bedrockagent::error::DisplayErrorContext(e).to_string(),
        ),
    }
}

/// A custom data source, written with `IngestKnowledgeBaseDocuments`.
pub struct CustomSink {
    pub client: aws_sdk_bedrockagent::Client,
    pub knowledge_base_id: String,
    pub data_source_id: String,
}

fn custom_document(
    d: &ExportDoc,
) -> Result<aws_sdk_bedrockagent::types::KnowledgeBaseDocument, String> {
    use aws_sdk_bedrockagent::types::*;
    let err = |e: aws_sdk_bedrockagent::error::BuildError| e.to_string();
    let mut meta = DocumentMetadata::builder().r#type(MetadataSourceType::InLineAttribute);
    for (k, v) in d.meta.attributes() {
        meta = meta.inline_attributes(
            MetadataAttribute::builder()
                .key(k)
                .value(
                    MetadataAttributeValue::builder()
                        .r#type(MetadataValueType::String)
                        .string_value(v)
                        .build()
                        .map_err(err)?,
                )
                .build()
                .map_err(err)?,
        );
    }
    Ok(KnowledgeBaseDocument::builder()
        .content(
            DocumentContent::builder()
                .data_source_type(ContentDataSourceType::Custom)
                .custom(
                    CustomContent::builder()
                        .custom_document_identifier(
                            CustomDocumentIdentifier::builder()
                                .id(&d.id)
                                .build()
                                .map_err(err)?,
                        )
                        .source_type(CustomSourceType::InLine)
                        .inline_content(
                            InlineContent::builder()
                                .r#type(InlineContentType::Text)
                                .text_content(
                                    TextContentDoc::builder()
                                        .data(&d.text)
                                        .build()
                                        .map_err(err)?,
                                )
                                .build()
                                .map_err(err)?,
                        )
                        .build()
                        .map_err(err)?,
                )
                .build()
                .map_err(err)?,
        )
        .metadata(meta.build().map_err(err)?)
        .build())
}

fn custom_identifier(id: &str) -> Result<aws_sdk_bedrockagent::types::DocumentIdentifier, String> {
    use aws_sdk_bedrockagent::types::*;
    DocumentIdentifier::builder()
        .data_source_type(ContentDataSourceType::Custom)
        .custom(
            CustomDocumentIdentifier::builder()
                .id(id)
                .build()
                .map_err(|e| e.to_string())?,
        )
        .build()
        .map_err(|e| e.to_string())
}

/// Per document, what Bedrock said of it: failed with its reason, or fine
/// (accepted and indexing in the background).
fn detail_results(
    ids: &[&str],
    details: &[aws_sdk_bedrockagent::types::KnowledgeBaseDocumentDetail],
) -> Vec<Result<(), String>> {
    use aws_sdk_bedrockagent::types::DocumentStatus;
    ids.iter()
        .map(|id| {
            let d = details.iter().find(|d| {
                d.identifier()
                    .and_then(|i| i.custom())
                    .is_some_and(|c| c.id() == *id)
            });
            match d {
                Some(d) if matches!(d.status(), DocumentStatus::Failed) => Err(format!(
                    "Bedrock failed it ({}).",
                    crate::truncate_str(d.status_reason().unwrap_or("no reason given"), 200)
                )),
                _ => Ok(()),
            }
        })
        .collect()
}

#[async_trait::async_trait]
impl Sink for CustomSink {
    async fn put(&self, docs: &[ExportDoc]) -> Vec<Result<(), String>> {
        let mut built = Vec::new();
        for d in docs {
            match custom_document(d) {
                Ok(b) => built.push(b),
                Err(e) => return docs.iter().map(|_| Err(e.clone())).collect(),
            }
        }
        let r = self
            .client
            .ingest_knowledge_base_documents()
            .knowledge_base_id(&self.knowledge_base_id)
            .data_source_id(&self.data_source_id)
            .set_documents(Some(built))
            .send()
            .await;
        // Five requests a second at most per account.
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        match r {
            Ok(out) => {
                let ids: Vec<&str> = docs.iter().map(|d| d.id.as_str()).collect();
                detail_results(&ids, out.document_details())
            }
            Err(e) => {
                let (code, msg) = sdk_error(&e);
                let m = plain_error(&code, &msg);
                docs.iter().map(|_| Err(m.clone())).collect()
            }
        }
    }

    async fn delete(&self, docs: &[Synced]) -> Vec<Result<(), String>> {
        let mut ids = Vec::new();
        for d in docs {
            match custom_identifier(&d.doc_id) {
                Ok(i) => ids.push(i),
                Err(e) => return docs.iter().map(|_| Err(e.clone())).collect(),
            }
        }
        let r = self
            .client
            .delete_knowledge_base_documents()
            .knowledge_base_id(&self.knowledge_base_id)
            .data_source_id(&self.data_source_id)
            .set_document_identifiers(Some(ids))
            .send()
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        match r {
            Ok(out) => {
                let ids: Vec<&str> = docs.iter().map(|d| d.doc_id.as_str()).collect();
                detail_results(&ids, out.document_details())
            }
            Err(e) => {
                let (code, msg) = sdk_error(&e);
                // Already gone is gone.
                if code == "ResourceNotFoundException" {
                    return docs.iter().map(|_| Ok(())).collect();
                }
                let m = plain_error(&code, &msg);
                docs.iter().map(|_| Err(m.clone())).collect()
            }
        }
    }

    async fn finish(&self, _changed: bool) -> Result<Option<String>, String> {
        Ok(None)
    }
}

/// An S3 folder, read by a Bedrock S3 data source (synced by the export
/// stack's schedule).
pub struct S3Sink {
    pub s3: aws_sdk_s3::Client,
    pub bucket: String,
    pub prefix: String,
}

#[async_trait::async_trait]
impl Sink for S3Sink {
    async fn put(&self, docs: &[ExportDoc]) -> Vec<Result<(), String>> {
        let mut out = Vec::new();
        for d in docs {
            let (doc_key, meta_key) = s3_keys(&self.prefix, &d.kb_id, &d.file_id);
            let r = async {
                self.s3
                    .put_object()
                    .bucket(&self.bucket)
                    .key(&doc_key)
                    .content_type("text/plain; charset=utf-8")
                    .body(d.text.clone().into_bytes().into())
                    .send()
                    .await
                    .map_err(|e| {
                        let (c, m) = sdk_error(&e);
                        plain_error(&c, &m)
                    })?;
                self.s3
                    .put_object()
                    .bucket(&self.bucket)
                    .key(&meta_key)
                    .content_type("application/json")
                    .body(s3_metadata(&d.meta).into_bytes().into())
                    .send()
                    .await
                    .map_err(|e| {
                        let (c, m) = sdk_error(&e);
                        plain_error(&c, &m)
                    })?;
                Ok::<(), String>(())
            }
            .await;
            out.push(r);
        }
        out
    }

    async fn delete(&self, docs: &[Synced]) -> Vec<Result<(), String>> {
        let mut out = Vec::new();
        for d in docs {
            let (doc_key, meta_key) = s3_keys(&self.prefix, &d.kb_id, &d.file_id);
            let mut r = Ok(());
            for key in [doc_key, meta_key] {
                if let Err(e) = self
                    .s3
                    .delete_object()
                    .bucket(&self.bucket)
                    .key(&key)
                    .send()
                    .await
                {
                    let (c, m) = sdk_error(&e);
                    r = Err(plain_error(&c, &m));
                }
            }
            out.push(r);
        }
        out
    }

    async fn finish(&self, _changed: bool) -> Result<Option<String>, String> {
        Ok(None)
    }
}

/// The AWS session a target is written with: a custom data source with the
/// team's connected `CoderHelmLogReader` role in the knowledge base's
/// account (taken on with the team's External ID, never a shared role); an
/// S3 folder with the install's own role. `Err` in plain words.
pub async fn session_for(
    sts: &aws_sdk_sts::Client,
    dynamo: &aws_sdk_dynamodb::Client,
    accounts_table: &str,
    team_id: &str,
    target: &Target,
) -> Result<Session, String> {
    let own = crate::live::aws::install_read_role();
    match target {
        Target::None => Err("Choose where to copy the knowledge bases.".into()),
        Target::S3 { region, .. } => {
            if own.is_none() {
                return Err("Copying to an S3 folder is for self-hosted installs.".into());
            }
            crate::live::aws::own_session(region).ok_or_else(|| {
                "This install's own AWS credentials aren't available here. Try again in a minute."
                    .into()
            })
        }
        Target::Custom {
            knowledge_base_arn, ..
        } => {
            let arn = super::remote::parse_kb_arn(knowledge_base_arn)?;
            let accounts = if accounts_table.is_empty() {
                Vec::new()
            } else {
                crate::live::aws_account::list(dynamo, accounts_table, team_id)
                    .await
                    .map_err(|_| {
                        "Coderhelm couldn't read your connected AWS accounts. Try again in a minute."
                            .to_string()
                    })?
            };
            // Never the install's own role: it doesn't write knowledge bases.
            let access = super::remote::access_for(&arn, None, &accounts)
                .map_err(|r| format!("{} {}", r.message, r.fix))?;
            let remote = super::remote::bedrock_remote(&arn, access, false);
            super::remote::session(
                sts,
                dynamo,
                accounts_table,
                team_id,
                &remote,
                "bedrock-sync",
            )
            .await
            .map_err(|r| format!("{} {}", r.message, r.fix))
        }
    }
}

/// The sink for `target`, written with `session`.
pub fn sink_for(session: &Session, target: &Target) -> Option<Box<dyn Sink>> {
    match target {
        Target::None => None,
        Target::Custom {
            knowledge_base_arn,
            data_source_id,
        } => {
            let region = super::remote::parse_kb_arn(knowledge_base_arn).ok()?.region;
            Some(Box::new(CustomSink {
                client: agent_client(session, &region),
                knowledge_base_id: kb_id_of(knowledge_base_arn),
                data_source_id: data_source_id.clone(),
            }))
        }
        Target::S3 {
            bucket,
            prefix,
            region,
            ..
        } => Some(Box::new(S3Sink {
            s3: s3_client(session, region),
            bucket: bucket.clone(),
            prefix: prefix.clone(),
        })),
    }
}

/// The knowledge base ARNs a team's sync writes to in `account`, for the
/// role's SyncKnowledgeBases.
pub fn sync_arns_in(cfg: Option<&Config>, account: &str) -> Vec<String> {
    cfg.and_then(|c| match &c.target {
        Target::Custom {
            knowledge_base_arn, ..
        } => super::remote::parse_kb_arn(knowledge_base_arn)
            .ok()
            .filter(|a| a.account == account)
            .map(|a| vec![a.arn()]),
        _ => None,
    })
    .unwrap_or_default()
}

/// The knowledge base id of an ARN (its last part).
pub fn kb_id_of(arn: &str) -> String {
    arn.rsplit('/').next().unwrap_or("").to_string()
}

/// Checks a target before it is saved: the knowledge base and data source
/// exist and the data source is the right kind (`CUSTOM` for a direct push,
/// `S3` reading the bucket otherwise). `Err` in plain words.
pub async fn check_data_source(
    client: &aws_sdk_bedrockagent::Client,
    target: &Target,
) -> Result<(), String> {
    let (kb, ds, want) = match target {
        Target::Custom {
            knowledge_base_arn,
            data_source_id,
        } => (knowledge_base_arn, data_source_id, "CUSTOM"),
        Target::S3 {
            knowledge_base_arn,
            data_source_id,
            ..
        } if !knowledge_base_arn.is_empty() => (knowledge_base_arn, data_source_id, "S3"),
        _ => return Ok(()),
    };
    let kb_id = kb_id_of(kb);
    client
        .get_knowledge_base()
        .knowledge_base_id(&kb_id)
        .send()
        .await
        .map_err(|e| {
            let (c, m) = sdk_error(&e);
            plain_error(&c, &m)
        })?;
    let out = client
        .get_data_source()
        .knowledge_base_id(&kb_id)
        .data_source_id(ds)
        .send()
        .await
        .map_err(|e| {
            let (c, m) = sdk_error(&e);
            plain_error(&c, &m)
        })?;
    let kind = out
        .data_source()
        .and_then(|d| d.data_source_configuration())
        .map(|c| c.r#type().as_str().to_string())
        .unwrap_or_default();
    if kind != want {
        return Err(if want == "CUSTOM" {
            format!("That data source reads from {kind}. A direct push needs a data source of type Custom: add one to the knowledge base in the Bedrock console and paste its ID.")
        } else {
            format!("That data source is of type {kind}. Name the S3 data source that reads this bucket.")
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::search::testing::MemoryStore;
    use super::*;
    use std::sync::Mutex;

    fn chunk(ord: u32, loc: &str, text: &str) -> IndexedChunk {
        IndexedChunk {
            ord,
            loc: loc.into(),
            text: text.into(),
            vector: vec![],
        }
    }

    #[test]
    fn document_text_drops_the_repeated_tail_and_heads_each_section() {
        let chunks = vec![
            chunk(1, "Page 1", "the cat sat on the mat. And then"),
            chunk(0, "", "Intro words."),
            chunk(2, "Page 1", "And then it slept."),
            chunk(3, "Page 2", "Other page."),
        ];
        let t = document_text(&chunks);
        assert_eq!(
            t,
            "Intro words.\n\n## Page 1\n\nthe cat sat on the mat. And then it slept.\n\n## Page 2\n\nOther page."
        );
        // Nothing repeated: the next passage starts a new paragraph.
        let t = document_text(&[chunk(0, "A", "one"), chunk(1, "A", "two")]);
        assert_eq!(t, "## A\n\none\n\ntwo");
    }

    #[test]
    fn the_s3_layout_and_metadata_file_are_what_bedrock_reads() {
        let (doc, meta) = s3_keys("coderhelm/", "kb1", "f1");
        assert_eq!(doc, "coderhelm/kb1/f1.txt");
        // The full name of the document plus .metadata.json, beside it.
        assert_eq!(meta, "coderhelm/kb1/f1.txt.metadata.json");
        let m = DocMeta {
            title: "Runbook".into(),
            link: "https://notion.so/x".into(),
            source: "notion".into(),
            team: "Acme".into(),
            base: "Ops".into(),
            base_id: "kb1".into(),
            file_id: "f1".into(),
            updated_at: "2026-11-01T00:00:00Z".into(),
        };
        let v: serde_json::Value = serde_json::from_str(&s3_metadata(&m)).unwrap();
        let a = &v["metadataAttributes"];
        for k in [
            "title",
            "link",
            "source",
            "team",
            "base",
            "base_id",
            "file_id",
            "updated_at",
        ] {
            assert_eq!(a[k]["value"]["type"], "STRING", "{k}");
            assert_eq!(a[k]["includeForEmbedding"], false, "{k}");
        }
        assert_eq!(a["link"]["value"]["stringValue"], "https://notion.so/x");
        assert_eq!(
            a["updated_at"]["value"]["stringValue"],
            "2026-11-01T00:00:00Z"
        );
        // Very long values still fit Bedrock's 10 KB.
        let long = DocMeta {
            title: "t".repeat(50_000),
            link: "l".repeat(50_000),
            ..m
        };
        assert!(s3_metadata(&long).len() <= MAX_METADATA_BYTES);
    }

    #[test]
    fn targets_are_checked_and_cleaned() {
        let kb = "arn:aws:bedrock:us-east-1:111122223333:knowledge-base/ABCDE12345";
        let s3 = Target::S3 {
            bucket: " coderhelm-prod-kb-export-111122223333 ".into(),
            prefix: "/coderhelm".into(),
            region: "us-west-2".into(),
            knowledge_base_arn: kb.into(),
            data_source_id: "DS12345678".into(),
        };
        match check_target(&s3, true).unwrap() {
            Target::S3 {
                bucket,
                prefix,
                region,
                ..
            } => {
                assert_eq!(bucket, "coderhelm-prod-kb-export-111122223333");
                assert_eq!(prefix, "coderhelm/");
                // The knowledge base's region, where the bucket must be.
                assert_eq!(region, "us-east-1");
            }
            t => panic!("{t:?}"),
        }
        assert!(check_target(&s3, false).is_err(), "S3 is self-hosted only");
        assert!(clean_prefix("a/../b").is_err());
        assert!(check_target(
            &Target::Custom {
                knowledge_base_arn: kb.into(),
                data_source_id: "bad".into()
            },
            false
        )
        .is_err());
        assert!(check_target(
            &Target::Custom {
                knowledge_base_arn: kb.into(),
                data_source_id: "DS12345678".into()
            },
            false
        )
        .is_ok());
        assert!(check_target(&Target::None, true).is_err());
    }

    #[test]
    fn batches_keep_under_ten_documents_and_the_payload_limit() {
        let d = |n: usize| ExportDoc {
            id: "x".into(),
            kb_id: "k".into(),
            file_id: "f".into(),
            text: "a".repeat(n),
            meta: DocMeta::default(),
            truncated: false,
        };
        let docs: Vec<ExportDoc> = (0..23).map(|_| d(10)).collect();
        let b = batches(&docs);
        assert_eq!(
            b.iter().map(|r| r.len()).collect::<Vec<_>>(),
            vec![10, 10, 3]
        );
        let big = vec![d(3_000_000), d(3_000_000), d(10)];
        let b = batches(&big);
        assert_eq!(b, vec![0..1, 1..3]);
    }

    // ── A sync against an in-memory team and Bedrock ──────────────────────

    #[derive(Default)]
    struct MemState(Mutex<Vec<Synced>>);

    #[async_trait::async_trait]
    impl SyncState for MemState {
        async fn synced(&self, _: &str) -> Result<Vec<Synced>, String> {
            Ok(self.0.lock().unwrap().clone())
        }
        async fn mark(&self, _: &str, s: &Synced) -> Result<(), String> {
            let mut v = self.0.lock().unwrap();
            v.retain(|x| x.doc_id != s.doc_id);
            v.push(s.clone());
            Ok(())
        }
        async fn forget(&self, _: &str, id: &str) -> Result<(), String> {
            self.0.lock().unwrap().retain(|x| x.doc_id != id);
            Ok(())
        }
    }

    /// Bedrock as a map of document id to (text, metadata), and the jobs
    /// started.
    #[derive(Default)]
    pub struct MemSink {
        pub docs: Mutex<HashMap<String, (String, DocMeta)>>,
        pub jobs: Mutex<u32>,
        pub fail: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl Sink for MemSink {
        async fn put(&self, docs: &[ExportDoc]) -> Vec<Result<(), String>> {
            docs.iter()
                .map(|d| {
                    if self.fail.lock().unwrap().contains(&d.id) {
                        return Err("refused".into());
                    }
                    self.docs
                        .lock()
                        .unwrap()
                        .insert(d.id.clone(), (d.text.clone(), d.meta.clone()));
                    Ok(())
                })
                .collect()
        }
        async fn delete(&self, docs: &[Synced]) -> Vec<Result<(), String>> {
            docs.iter()
                .map(|d| {
                    self.docs.lock().unwrap().remove(&d.doc_id);
                    Ok(())
                })
                .collect()
        }
        async fn finish(&self, changed: bool) -> Result<Option<String>, String> {
            if changed {
                *self.jobs.lock().unwrap() += 1;
                return Ok(Some(format!("job{}", self.jobs.lock().unwrap())));
            }
            Ok(None)
        }
    }

    fn base(id: &str, name: &str) -> Base {
        Base {
            id: id.into(),
            name: name.into(),
            read_role: "member".into(),
            ..Default::default()
        }
    }

    /// Adds (or replaces with a new version) a ready file of `kb` whose
    /// passages are `(loc, text)`.
    fn add(st: &MemoryStore, kb: &str, id: &str, name: &str, parts: &[(&str, &str)]) {
        let version = st
            .files
            .lock()
            .unwrap()
            .get("T")
            .and_then(|v| v.iter().find(|f| f.id == id).map(|f| f.ready_version))
            .unwrap_or(0)
            + 1;
        let chunks = parts
            .iter()
            .enumerate()
            .map(|(i, (loc, t))| chunk(i as u32, loc, t))
            .collect();
        let file = FileMeta {
            id: id.into(),
            kb_id: kb.into(),
            name: name.into(),
            kind: "markdown".into(),
            status: super::super::FileStatus::Ready,
            version,
            ready_version: version,
            content_hash: format!("h{version}"),
            ..Default::default()
        };
        let key = file.searchable_index("T").unwrap();
        let mut files = st.files.lock().unwrap();
        let list = files.entry("T".into()).or_default();
        list.retain(|f| f.id != id);
        list.push(file);
        st.indexes.lock().unwrap().insert(
            key,
            std::sync::Arc::new(super::super::search::Index {
                model: "m".into(),
                dims: 0,
                chunks,
            }),
        );
    }

    fn remove_file(st: &MemoryStore, id: &str) {
        st.files
            .lock()
            .unwrap()
            .get_mut("T")
            .unwrap()
            .retain(|f| f.id != id);
    }

    fn remove_base(st: &MemoryStore, kb: &str) {
        st.bases
            .lock()
            .unwrap()
            .get_mut("T")
            .unwrap()
            .retain(|b| b.id != kb);
        st.files
            .lock()
            .unwrap()
            .get_mut("T")
            .unwrap()
            .retain(|f| f.kb_id != kb);
    }

    fn team() -> (MemoryStore, Config) {
        let st = MemoryStore::default();
        st.add_base("T", &base("kba", "Ops"));
        st.add_base("T", &base("kbb", "Secret"));
        add(
            &st,
            "kba",
            "f1",
            "runbook.md",
            &[("Setup", "Restart the api with make restart.")],
        );
        add(
            &st,
            "kba",
            "f2",
            "oncall.md",
            &[("", "Page the on call person.")],
        );
        add(
            &st,
            "kbb",
            "f3",
            "salaries.csv",
            &[("", "Private numbers.")],
        );
        let cfg = Config {
            enabled: true,
            target: Target::Custom {
                knowledge_base_arn:
                    "arn:aws:bedrock:us-east-1:111122223333:knowledge-base/ABCDE12345".into(),
                data_source_id: "DS12345678".into(),
            },
            bases: vec!["kba".into()],
            ..Default::default()
        };
        (st, cfg)
    }

    async fn run(st: &MemoryStore, state: &MemState, sink: &MemSink, cfg: &Config) -> Outcome {
        reconcile(
            st,
            state,
            sink,
            "T",
            "Acme",
            "https://app.example.org",
            cfg,
            "2026-11-10T00:00:00Z",
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn only_the_chosen_bases_are_copied_with_their_metadata() {
        let (st, cfg) = team();
        let (state, sink) = (MemState::default(), MemSink::default());
        let o = run(&st, &state, &sink, &cfg).await;
        assert_eq!((o.added, o.updated, o.removed, o.documents), (2, 0, 0, 2));
        let docs = sink.docs.lock().unwrap();
        assert!(docs.contains_key("kba-f1") && docs.contains_key("kba-f2"));
        assert!(
            !docs.keys().any(|k| k.starts_with("kbb")),
            "an unchosen base is never copied"
        );
        let (text, meta) = &docs["kba-f1"];
        assert!(text.contains("## Setup") && text.contains("make restart"));
        assert_eq!(meta.base, "Ops");
        assert_eq!(meta.team, "Acme");
        assert_eq!(meta.source, "upload");
        assert_eq!(meta.link, "https://app.example.org/knowledge/base?id=kba");
    }

    #[tokio::test]
    async fn an_unchanged_sync_writes_nothing_and_changes_propagate() {
        let (st, cfg) = team();
        let (state, sink) = (MemState::default(), MemSink::default());
        run(&st, &state, &sink, &cfg).await;
        let o = run(&st, &state, &sink, &cfg).await;
        assert_eq!((o.added, o.updated, o.removed, o.documents), (0, 0, 0, 2));
        // A file re-uploaded (a new version): written again.
        add(
            &st,
            "kba",
            "f1",
            "runbook.md",
            &[("Setup", "Restart with make bounce.")],
        );
        let o = run(&st, &state, &sink, &cfg).await;
        assert_eq!((o.added, o.updated), (0, 1));
        assert!(sink.docs.lock().unwrap()["kba-f1"]
            .0
            .contains("make bounce"));
    }

    #[tokio::test]
    async fn deletes_propagate_for_files_bases_the_list_and_turning_it_off() {
        let (st, mut cfg) = team();
        let (state, sink) = (MemState::default(), MemSink::default());
        cfg.bases.push("kbb".into());
        run(&st, &state, &sink, &cfg).await;
        assert_eq!(sink.docs.lock().unwrap().len(), 3);
        // A deleted file.
        remove_file(&st, "f2");
        let o = run(&st, &state, &sink, &cfg).await;
        assert_eq!(o.removed, 1);
        assert!(!sink.docs.lock().unwrap().contains_key("kba-f2"));
        // A base taken off the list.
        cfg.bases.retain(|b| b != "kbb");
        let o = run(&st, &state, &sink, &cfg).await;
        assert_eq!((o.removed, o.documents), (1, 1));
        assert!(!sink.docs.lock().unwrap().contains_key("kbb-f3"));
        // A deleted base.
        remove_base(&st, "kba");
        let o = run(&st, &state, &sink, &cfg).await;
        assert_eq!((o.removed, o.documents), (1, 0));
        assert!(sink.docs.lock().unwrap().is_empty());
        assert!(state.0.lock().unwrap().is_empty());
        // Turning it off empties Bedrock too.
        let (st, cfg) = team();
        let (state, sink) = (MemState::default(), MemSink::default());
        run(&st, &state, &sink, &cfg).await;
        let off = Config {
            enabled: false,
            ..cfg
        };
        let o = run(&st, &state, &sink, &off).await;
        assert_eq!((o.removed, o.documents), (2, 0));
        assert!(sink.docs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_failed_document_is_tried_again_and_reported_without_its_text() {
        let (st, cfg) = team();
        let (state, sink) = (MemState::default(), MemSink::default());
        sink.fail.lock().unwrap().push("kba-f1".into());
        let o = run(&st, &state, &sink, &cfg).await;
        assert_eq!((o.added, o.documents), (1, 1));
        assert_eq!(o.errors.len(), 1);
        assert!(o.errors[0].starts_with("runbook.md wasn't copied"));
        assert!(
            !o.errors[0].contains("make restart"),
            "errors never carry text"
        );
        let s = o.status(&Status::default(), "now", false);
        assert_eq!(s.state, "partial");
        sink.fail.lock().unwrap().clear();
        let o = run(&st, &state, &sink, &cfg).await;
        assert_eq!((o.added, o.documents), (1, 2));
        assert!(o.errors.is_empty());
    }

    #[tokio::test]
    async fn an_ingestion_job_starts_only_after_a_change() {
        let (st, cfg) = team();
        let (state, sink) = (MemState::default(), MemSink::default());
        let o = run(&st, &state, &sink, &cfg).await;
        assert_eq!(o.ingestion_job_id, "job1");
        let o = run(&st, &state, &sink, &cfg).await;
        assert_eq!(o.ingestion_job_id, "");
        assert_eq!(*sink.jobs.lock().unwrap(), 1);
    }

    #[test]
    fn the_export_stack_keeps_the_bucket_private_and_writable_by_the_install_only() {
        let t = EXPORT_TEMPLATE;
        assert!(t.contains("DeletionPolicy: Retain"));
        assert!(t.contains("BlockPublicPolicy: true"));
        assert!(t.contains("aws:SecureTransport: \"false\""));
        assert!(t.contains("aws:PrincipalTag/coderhelm:role: [\"worker\", \"gateway\"]"));
        assert!(t.contains("role/${InstallPrefix}-*"));
        // The bucket's name starts with the install's prefix: the only
        // buckets the install's permission limit lets it write.
        assert!(t.contains("BucketName: !Sub \"${InstallPrefix}-kb-export-${AWS::AccountId}\""));
        assert!(t.contains("Type: AWS::Bedrock::DataSource"));
        assert!(t.contains("DataDeletionPolicy: DELETE"));
        // Its own schedule syncs the data source: the install never starts a job.
        assert!(t.contains("arn:aws:scheduler:::aws-sdk:bedrockagent:startIngestionJob"));
        assert!(t.contains("ScheduleExpression: rate(15 minutes)"));
        // Writes reach only the folder.
        assert!(t.contains("Resource: !Sub \"${ExportBucket.Arn}/${Prefix}*\""));
        assert!(!t.contains('\u{2192}'));
    }

    /// The template's resources and outputs, by logical id.
    fn export_template() -> serde_yaml::Mapping {
        // CloudFormation's short tags (!Ref, !Sub...) read as plain values.
        let v: serde_yaml::Value = serde_yaml::from_str(EXPORT_TEMPLATE).unwrap();
        v.as_mapping().unwrap().clone()
    }

    fn section<'a>(t: &'a serde_yaml::Mapping, name: &str) -> &'a serde_yaml::Mapping {
        t.get(name).and_then(|v| v.as_mapping()).unwrap()
    }

    #[test]
    fn the_export_stack_makes_a_knowledge_base_on_s3_vectors_by_default() {
        let t = export_template();
        let params = section(&t, "Parameters");
        let p = |k: &str| params.get(k).unwrap().as_mapping().unwrap();
        assert_eq!(
            p("CreateKnowledgeBase").get("Default").unwrap().as_str(),
            Some("yes")
        );
        assert_eq!(p("SetupToken").get("NoEcho").unwrap().as_bool(), Some(true));
        assert_eq!(p("SetupToken").get("Default").unwrap().as_str(), Some(""));
        let res = section(&t, "Resources");
        let kind = |k: &str| {
            res.get(k)
                .and_then(|r| r.get("Type"))
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| panic!("{k} missing"))
                .to_string()
        };
        assert_eq!(kind("VectorBucket"), "AWS::S3Vectors::VectorBucket");
        assert_eq!(kind("VectorIndex"), "AWS::S3Vectors::Index");
        assert_eq!(kind("KnowledgeBase"), "AWS::Bedrock::KnowledgeBase");
        assert_eq!(kind("KnowledgeBaseRole"), "AWS::IAM::Role");
        assert_eq!(kind("Reporter"), "AWS::Lambda::Function");
        for r in ["ReportStarted", "ReportBucket", "ReportReady"] {
            assert_eq!(kind(r), "Custom::CoderhelmReport");
        }
        // The new knowledge base and its parts only when asked for.
        for r in [
            "VectorBucket",
            "VectorIndex",
            "KnowledgeBase",
            "KnowledgeBaseRole",
        ] {
            assert_eq!(
                res.get(r).unwrap().get("Condition").unwrap().as_str(),
                Some("CreatesKnowledgeBase"),
                "{r}"
            );
        }
        let t = EXPORT_TEMPLATE;
        assert!(t.contains("Type: S3_VECTORS"));
        assert!(t.contains("foundation-model/amazon.titan-embed-text-v2:0"));
        assert!(t.contains("Dimension: 1024") && t.contains("Dimensions: 1024"));
        assert!(t.contains("- AMAZON_BEDROCK_TEXT") && t.contains("- AMAZON_BEDROCK_METADATA"));
        // Bedrock's role: only this account's knowledge bases take it on, and
        // it reaches the one index and model.
        assert!(t.contains("Service: bedrock.amazonaws.com"));
        assert!(t.contains("Resource: !GetAtt VectorIndex.IndexArn"));
        // The reporter reads only its own stack.
        assert!(t.contains("Resource: !Ref AWS::StackId"));
        let outs = section(&export_template(), "Outputs").clone();
        for o in [
            "BucketName",
            "Prefix",
            "DataSourceId",
            "KnowledgeBaseArn",
            "VectorBucketName",
        ] {
            assert!(outs.contains_key(o), "{o}");
        }
    }

    #[test]
    fn a_connected_base_is_never_copied() {
        let b = super::super::search::testing::remote_base("r", "Remote", "111122223333");
        assert!(!can_copy(&b));
        assert!(can_copy(&base("x", "x")));
    }
}
