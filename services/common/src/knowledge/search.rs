//! Searching knowledge bases: the stored index format, where bases and
//! files are read from, and how passages are ranked.
//!
//! Retrieval is hybrid. Two ranked lists are built over the same passages:
//!
//! - vector: cosine similarity between the question's Titan embedding and
//!   each passage's (metered, `common::embed`);
//! - keyword: BM25 over the passage text and its heading, so exact terms
//!   (error codes, names, ids) match even when the vectors miss them.
//!
//! Each list keeps only passages past its own floor ([`MIN_COSINE`];
//! [`MIN_TERM_SHARE`] of the question's terms). The lists are merged with
//! reciprocal rank fusion ([`RRF_K`]), nudged by recency and by a base
//! marked as a source of truth, and cut at [`MIN_FUSED`] so weak matches are
//! never cited.
//!
//! Every index records the embedding model and dimension it was built with.
//! An index whose model or dimension differs from the question's is never
//! scored against it: it is reported as stale so it can be re-embedded, and
//! an index whose own vectors disagree in dimension is refused outright.
//!
//! Team and base are part of every lookup's key (the base and its files are
//! read with `pk` = team and the base's sort key; index objects live under
//! the team's and base's folder), so nothing outside them is ever loaded.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use aws_sdk_dynamodb::types::AttributeValue;
use base64::Engine;
use serde::{Deserialize, Serialize};

use super::{Base, FileMeta, FileStatus};

// ── The stored index ────────────────────────────────────────────────────────

/// One passage with its vector.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexedChunk {
    pub ord: u32,
    /// Where it is, with its heading trail ("Setup > Install", "Page 3").
    pub loc: String,
    pub text: String,
    pub vector: Vec<f32>,
}

/// A file version's search index.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Index {
    /// The embedding model every vector came from.
    pub model: String,
    /// Every vector's length.
    pub dims: usize,
    pub chunks: Vec<IndexedChunk>,
}

impl Index {
    /// An index of `chunks` embedded by `model`. Refused when any vector's
    /// length differs from `dims`: one index never mixes dimensions.
    pub fn build(model: &str, dims: usize, chunks: Vec<IndexedChunk>) -> Result<Index, String> {
        if let Some(bad) = chunks.iter().find(|c| c.vector.len() != dims) {
            return Err(format!(
                "passage {} has {} dimensions, the index {dims}: mixed dimensions are refused",
                bad.ord,
                bad.vector.len()
            ));
        }
        Ok(Index {
            model: model.to_string(),
            dims,
            chunks,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct StoredIndex {
    format: u32,
    #[serde(default)]
    model: String,
    #[serde(default)]
    dims: usize,
    chunks: Vec<StoredChunk>,
}

#[derive(Serialize, Deserialize)]
struct StoredChunk {
    ord: u32,
    loc: String,
    text: String,
    /// The model and dimension of this passage's vector.
    #[serde(default)]
    model: String,
    #[serde(default)]
    dims: usize,
    /// The vector as little-endian f32 bytes, base64.
    v: String,
}

/// The index as stored (JSON, vectors packed, model and dimension on the
/// index and on each passage).
pub fn encode_index(index: &Index) -> Vec<u8> {
    let stored = StoredIndex {
        format: 2,
        model: index.model.clone(),
        dims: index.dims,
        chunks: index
            .chunks
            .iter()
            .map(|c| StoredChunk {
                ord: c.ord,
                loc: c.loc.clone(),
                text: c.text.clone(),
                model: index.model.clone(),
                dims: c.vector.len(),
                v: base64::engine::general_purpose::STANDARD.encode(
                    c.vector
                        .iter()
                        .flat_map(|f| f.to_le_bytes())
                        .collect::<Vec<u8>>(),
                ),
            })
            .collect(),
    };
    serde_json::to_vec(&stored).unwrap_or_default()
}

/// The index from its stored bytes. Refused when a passage's model or
/// dimension differs from the index's (never a mixed index).
pub fn decode_index(bytes: &[u8]) -> Result<Index, String> {
    let stored: StoredIndex =
        serde_json::from_slice(bytes).map_err(|e| format!("index unreadable: {e}"))?;
    let mut chunks = Vec::with_capacity(stored.chunks.len());
    for c in stored.chunks {
        if !c.model.is_empty() && c.model != stored.model {
            return Err(format!(
                "passage {} was embedded by {}, the index by {}: mixed models are refused",
                c.ord, c.model, stored.model
            ));
        }
        let raw = base64::engine::general_purpose::STANDARD
            .decode(c.v)
            .map_err(|_| format!("passage {} has an unreadable vector", c.ord))?;
        let vector: Vec<f32> = raw
            .chunks_exact(4)
            .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
            .collect();
        if c.dims != 0 && c.dims != vector.len() {
            return Err(format!(
                "passage {} says {} dimensions but has {}",
                c.ord,
                c.dims,
                vector.len()
            ));
        }
        chunks.push(IndexedChunk {
            ord: c.ord,
            loc: c.loc,
            text: c.text,
            vector,
        });
    }
    let dims = if stored.dims > 0 {
        stored.dims
    } else {
        chunks.first().map_or(0, |c| c.vector.len())
    };
    Index::build(&stored.model, dims, chunks)
}

// ── Where bases, files and indexes come from ────────────────────────────────

/// Reads a team's bases, files and indexes. Every method takes the team (and
/// the base), and answers only from records keyed by them.
#[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// Every base of the team (for lists and attachment checks).
    async fn bases(&self, team_id: &str) -> Result<Vec<Base>, String>;
    /// One base of the team, read by its key.
    async fn base(&self, team_id: &str, kb_id: &str) -> Result<Option<Base>, String>;
    /// The files of one base of the team, read by key prefix.
    async fn files(&self, team_id: &str, kb_id: &str) -> Result<Vec<FileMeta>, String>;
    /// The index at `key`, which must be inside the team's folder.
    async fn index(&self, team_id: &str, key: &str) -> Result<Arc<Index>, String>;
}

/// Turns text into a vector.
#[async_trait::async_trait]
pub trait Embedder: Send + Sync {
    /// The model id vectors come from.
    fn model(&self) -> &str;
    async fn embed(&self, text: &str) -> Result<Vec<f32>, String>;
}

/// Titan on Bedrock, every call metered ([`crate::embed::titan_embed`]).
pub struct Titan<'a> {
    pub client: &'a aws_sdk_bedrockruntime::Client,
    pub meter: &'a crate::usage::UsageMeter,
}

#[async_trait::async_trait]
impl Embedder for Titan<'_> {
    fn model(&self) -> &str {
        crate::embed::TITAN_V2
    }
    async fn embed(&self, text: &str) -> Result<Vec<f32>, String> {
        crate::embed::titan_embed(self.client, self.meter, text).await
    }
}

/// The settings table and the data bucket.
pub struct AwsStore {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub s3: aws_sdk_s3::Client,
    pub table: String,
    pub bucket: String,
}

/// Records under `pk` = team whose sort key starts with `prefix`, each
/// record's `data` read as JSON.
async fn query_data<T: for<'de> Deserialize<'de>>(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    prefix: &str,
) -> Result<Vec<T>, String> {
    let mut out = Vec::new();
    let mut start: Option<HashMap<String, AttributeValue>> = None;
    loop {
        let resp = dynamo
            .query()
            .table_name(table)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", AttributeValue::S(team_id.to_string()))
            .expression_attribute_values(":p", AttributeValue::S(prefix.to_string()))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| format!("query: {e}"))?;
        for item in resp.items() {
            if let Some(v) = super::read_data::<T>(item) {
                out.push(v);
            }
        }
        match resp.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(out)
}

/// Indexes kept in memory, by key (a key names one version, so a cached
/// index is never stale).
fn cache() -> &'static Mutex<HashMap<String, Arc<Index>>> {
    static C: OnceLock<Mutex<HashMap<String, Arc<Index>>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}
const CACHE_ENTRIES: usize = 64;

#[async_trait::async_trait]
impl Store for AwsStore {
    async fn bases(&self, team_id: &str) -> Result<Vec<Base>, String> {
        query_data(&self.dynamo, &self.table, team_id, super::BASE_PREFIX).await
    }

    async fn base(&self, team_id: &str, kb_id: &str) -> Result<Option<Base>, String> {
        if !super::valid_id(kb_id) {
            return Ok(None);
        }
        let r = self
            .dynamo
            .get_item()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(team_id.to_string()))
            .key("sk", AttributeValue::S(super::base_sk(kb_id)))
            .send()
            .await
            .map_err(|e| format!("base: {e}"))?;
        Ok(r.item().and_then(super::read_data::<Base>))
    }

    async fn files(&self, team_id: &str, kb_id: &str) -> Result<Vec<FileMeta>, String> {
        if !super::valid_id(kb_id) {
            return Ok(Vec::new());
        }
        query_data(
            &self.dynamo,
            &self.table,
            team_id,
            &super::file_prefix(kb_id),
        )
        .await
    }

    async fn index(&self, team_id: &str, key: &str) -> Result<Arc<Index>, String> {
        if !super::owns_key(team_id, key) {
            return Err("that index isn't this team's".into());
        }
        if let Some(hit) = cache().lock().ok().and_then(|c| c.get(key).cloned()) {
            return Ok(hit);
        }
        let obj = self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| format!("index read: {e}"))?;
        let bytes = obj
            .body
            .collect()
            .await
            .map_err(|e| format!("index read: {e}"))?
            .into_bytes();
        let index = Arc::new(decode_index(&bytes)?);
        if let Ok(mut c) = cache().lock() {
            if c.len() >= CACHE_ENTRIES {
                c.clear();
            }
            c.insert(key.to_string(), index.clone());
        }
        Ok(index)
    }
}

// ── Ranking ─────────────────────────────────────────────────────────────────

/// One passage found.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Hit {
    pub kb_id: String,
    pub kb_name: String,
    pub file_id: String,
    pub file_name: String,
    /// Where in the file ("Page 3").
    pub loc: String,
    /// The document's date.
    pub as_of: String,
    pub text: String,
    /// The fused score.
    pub score: f32,
    /// From a base marked as a source of truth.
    pub authoritative: bool,
    pub ord: u32,
}

/// A file whose index was built with another embedding model or dimension:
/// it must be re-embedded before it is searched again.
#[derive(Debug, Clone, PartialEq)]
pub struct Stale {
    pub kb_id: String,
    pub file_id: String,
    pub version: u32,
}

/// What a search found.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Found {
    pub hits: Vec<Hit>,
    pub stale: Vec<Stale>,
}

/// The vector list keeps passages at least this similar.
pub const MIN_COSINE: f32 = 0.25;
/// The keyword list keeps passages holding at least this share of the
/// question's terms.
pub const MIN_TERM_SHARE: f32 = 0.34;
/// Reciprocal rank fusion's constant.
pub const RRF_K: f32 = 60.0;
/// How deep each list is fused.
pub const LIST_DEPTH: usize = 50;
/// Fused scores under this are dropped: a passage must place within the
/// top 30 of at least one list (after that list's floor).
pub const MIN_FUSED: f32 = 1.0 / (RRF_K + 30.0);

pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let (mut dot, mut na, mut nb) = (0.0f32, 0.0f32, 0.0f32);
    for (x, y) in a.iter().zip(b) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na.sqrt() * nb.sqrt())
    }
}

const STOPWORDS: &[&str] = &[
    "the", "and", "for", "are", "was", "what", "when", "where", "which", "who", "how", "why",
    "does", "did", "our", "your", "you", "with", "that", "this", "from", "have", "has", "can",
    "will", "about", "into", "there", "their", "any", "all", "not", "but", "use", "used",
];

/// The tokens of a text: lower case, words and codes kept whole
/// (`err-4021`) and also split at `-` and `_` (`err`, `4021`).
pub fn tokens(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for w in text
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric() && c != '_' && c != '-')
        .map(|w| w.trim_matches(|c| c == '-' || c == '_'))
        .filter(|w| !w.is_empty())
    {
        out.push(w.to_string());
        if w.contains(['-', '_']) {
            out.extend(
                w.split(['-', '_'])
                    .filter(|p| !p.is_empty())
                    .map(str::to_string),
            );
        }
    }
    out
}

/// The question's search terms: tokens of 2+ characters (any with a digit
/// kept whatever its length), stopwords dropped, each once.
pub fn terms(text: &str) -> Vec<String> {
    let mut out: Vec<String> = tokens(text)
        .into_iter()
        .filter(|w| {
            (w.chars().count() >= 3 || w.chars().any(|c| c.is_ascii_digit()))
                && !STOPWORDS.contains(&w.as_str())
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// The share of `terms` found in `text` (0 to 1).
pub fn term_overlap(terms: &[String], text: &str) -> f32 {
    if terms.is_empty() {
        return 0.0;
    }
    let toks: std::collections::HashSet<String> = tokens(text).into_iter().collect();
    let found = terms.iter().filter(|t| toks.contains(t.as_str())).count();
    found as f32 / terms.len() as f32
}

/// How much a document's age keeps of its score: 1.0 today, 0.925 at a
/// year, never below 0.85.
pub fn recency(age_days: f32) -> f32 {
    0.85 + 0.15 * 0.5f32.powf(age_days.max(0.0) / 365.0)
}

/// One passage under consideration.
pub struct Candidate<'a> {
    pub text: &'a str,
    pub loc: &'a str,
    pub vector: &'a [f32],
    pub age_days: f32,
    pub authoritative: bool,
}

/// Positions (into `cands`) of the vector list: cosine at least
/// [`MIN_COSINE`], best first, at most [`LIST_DEPTH`].
pub fn vector_list(query: &[f32], cands: &[Candidate]) -> Vec<(usize, f32)> {
    let mut out: Vec<(usize, f32)> = cands
        .iter()
        .enumerate()
        .map(|(i, c)| (i, cosine(query, c.vector)))
        .filter(|(_, s)| *s >= MIN_COSINE)
        .collect();
    out.sort_by(|a, b| b.1.total_cmp(&a.1));
    out.truncate(LIST_DEPTH);
    out
}

/// Positions of the keyword list: BM25 (k1 1.2, b 0.75) over each passage's
/// heading and text, holding at least [`MIN_TERM_SHARE`] of the terms, best
/// first, at most [`LIST_DEPTH`].
pub fn keyword_list(query: &str, cands: &[Candidate]) -> Vec<(usize, f32)> {
    let q = terms(query);
    if q.is_empty() || cands.is_empty() {
        return Vec::new();
    }
    let docs: Vec<Vec<String>> = cands
        .iter()
        .map(|c| tokens(&format!("{}\n{}", c.loc, c.text)))
        .collect();
    let n = docs.len() as f32;
    let avg = docs.iter().map(|d| d.len()).sum::<usize>() as f32 / n.max(1.0);
    let mut df: HashMap<&str, usize> = HashMap::new();
    for d in &docs {
        let uniq: std::collections::HashSet<&str> = d.iter().map(String::as_str).collect();
        for t in &q {
            if uniq.contains(t.as_str()) {
                *df.entry(t.as_str()).or_default() += 1;
            }
        }
    }
    let (k1, b) = (1.2f32, 0.75f32);
    let mut out = Vec::new();
    for (i, d) in docs.iter().enumerate() {
        let mut tf: HashMap<&str, usize> = HashMap::new();
        for t in d {
            *tf.entry(t.as_str()).or_default() += 1;
        }
        let mut score = 0.0f32;
        let mut matched = 0usize;
        for t in &q {
            let Some(&f) = tf.get(t.as_str()) else {
                continue;
            };
            matched += 1;
            let dfn = *df.get(t.as_str()).unwrap_or(&0) as f32;
            let idf = ((n - dfn + 0.5) / (dfn + 0.5) + 1.0).ln();
            let f = f as f32;
            score +=
                idf * (f * (k1 + 1.0)) / (f + k1 * (1.0 - b + b * d.len() as f32 / avg.max(1.0)));
        }
        if score > 0.0 && matched as f32 / q.len() as f32 >= MIN_TERM_SHARE {
            out.push((i, score));
        }
    }
    out.sort_by(|a, b| b.1.total_cmp(&a.1));
    out.truncate(LIST_DEPTH);
    out
}

/// Reciprocal rank fusion of the two lists, nudged by recency and by a
/// source of truth, cut at [`MIN_FUSED`]. Returns `(position, score)`, best
/// first.
pub fn fuse(
    vector: &[(usize, f32)],
    keyword: &[(usize, f32)],
    cands: &[Candidate],
) -> Vec<(usize, f32)> {
    let mut fused: HashMap<usize, f32> = HashMap::new();
    for list in [vector, keyword] {
        for (rank, (i, _)) in list.iter().enumerate() {
            *fused.entry(*i).or_default() += 1.0 / (RRF_K + rank as f32 + 1.0);
        }
    }
    let mut out: Vec<(usize, f32)> = fused
        .into_iter()
        .filter(|(_, s)| *s >= MIN_FUSED)
        .map(|(i, s)| {
            let c = &cands[i];
            let nudged = s * recency(c.age_days) * if c.authoritative { 1.05 } else { 1.0 };
            (i, nudged)
        })
        .collect();
    out.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    out
}

fn age_days(as_of: &str, now: chrono::DateTime<chrono::Utc>) -> f32 {
    let day = as_of.get(..10).unwrap_or("");
    chrono::NaiveDate::parse_from_str(day, "%Y-%m-%d")
        .ok()
        .map(|d| (now.date_naive() - d).num_days() as f32)
        .unwrap_or(0.0)
}

/// The best passages for `query` in the team's bases `kb_ids`, at most `k`.
/// Each base is read by the team's key, so an id from another team (or a
/// made-up one) finds nothing.
pub async fn search(
    store: &dyn Store,
    embedder: &dyn Embedder,
    team_id: &str,
    kb_ids: &[String],
    query: &str,
    k: usize,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Found, String> {
    let query = query.trim();
    let mut found = Found::default();
    if query.is_empty() || kb_ids.is_empty() || k == 0 {
        return Ok(found);
    }
    let mut files: Vec<(Base, FileMeta, String)> = Vec::new();
    for id in kb_ids {
        let Some(base) = store.base(team_id, id).await? else {
            continue;
        };
        for f in store.files(team_id, &base.id).await? {
            if f.kb_id != base.id {
                continue;
            }
            // A failed re-upload keeps the last ready version searchable.
            if let Some(key) = f.searchable_index(team_id) {
                if f.status == FileStatus::Ready || f.ready_version > 0 {
                    files.push((base.clone(), f, key));
                }
            }
        }
    }
    if files.is_empty() {
        return Ok(found);
    }
    let qv = embedder.embed(crate::truncate_str(query, 8_000)).await?;
    let mut loaded: Vec<(Base, FileMeta, Arc<Index>)> = Vec::new();
    for (base, file, key) in files {
        match store.index(team_id, &key).await {
            Ok(i) if i.model != embedder.model() || i.dims != qv.len() => {
                tracing::warn!(kb = %base.id, file = %file.id, model = %i.model, dims = i.dims,
                    "Knowledge index from another embedding model; needs re-embedding");
                found.stale.push(Stale {
                    kb_id: base.id.clone(),
                    file_id: file.id.clone(),
                    version: file.ready_version,
                });
            }
            Ok(i) => loaded.push((base, file, i)),
            Err(e) => {
                tracing::warn!(kb = %base.id, file = %file.id, error = %e, "Knowledge index unreadable; skipped");
            }
        }
    }
    let mut owners: Vec<(&Base, &FileMeta, &IndexedChunk)> = Vec::new();
    let mut cands: Vec<Candidate> = Vec::new();
    for (base, file, index) in &loaded {
        let age = age_days(&file.as_of, now);
        for c in &index.chunks {
            owners.push((base, file, c));
            cands.push(Candidate {
                text: &c.text,
                loc: &c.loc,
                vector: &c.vector,
                age_days: age,
                authoritative: base.authoritative,
            });
        }
    }
    let ranked = fuse(
        &vector_list(&qv, &cands),
        &keyword_list(query, &cands),
        &cands,
    );
    // No more than three passages from one file, so one long document
    // doesn't crowd out the others.
    let mut per_file: HashMap<&str, usize> = HashMap::new();
    for (i, score) in ranked {
        let (base, file, c) = owners[i];
        let n = per_file.entry(file.id.as_str()).or_default();
        *n += 1;
        if *n > 3 {
            continue;
        }
        found.hits.push(Hit {
            kb_id: base.id.clone(),
            kb_name: base.name.clone(),
            file_id: file.id.clone(),
            file_name: file.name.clone(),
            loc: c.loc.clone(),
            as_of: file.as_of.clone(),
            text: c.text.clone(),
            score,
            authoritative: base.authoritative,
            ord: c.ord,
        });
        if found.hits.len() >= k {
            break;
        }
    }
    Ok(found)
}

/// An in-memory store and a word-hashing embedder, for tests here and in
/// the services.
pub mod testing {
    use super::*;

    /// Bases, files and indexes per team.
    #[derive(Default)]
    pub struct MemoryStore {
        pub bases: Mutex<HashMap<String, Vec<Base>>>,
        pub files: Mutex<HashMap<String, Vec<FileMeta>>>,
        pub indexes: Mutex<HashMap<String, Arc<Index>>>,
    }

    #[async_trait::async_trait]
    impl Store for MemoryStore {
        async fn bases(&self, team_id: &str) -> Result<Vec<Base>, String> {
            Ok(self
                .bases
                .lock()
                .unwrap()
                .get(team_id)
                .cloned()
                .unwrap_or_default())
        }
        async fn base(&self, team_id: &str, kb_id: &str) -> Result<Option<Base>, String> {
            Ok(self
                .bases
                .lock()
                .unwrap()
                .get(team_id)
                .and_then(|v| v.iter().find(|b| b.id == kb_id).cloned()))
        }
        async fn files(&self, team_id: &str, kb_id: &str) -> Result<Vec<FileMeta>, String> {
            Ok(self
                .files
                .lock()
                .unwrap()
                .get(team_id)
                .map(|v| v.iter().filter(|f| f.kb_id == kb_id).cloned().collect())
                .unwrap_or_default())
        }
        async fn index(&self, team_id: &str, key: &str) -> Result<Arc<Index>, String> {
            if !crate::knowledge::owns_key(team_id, key) {
                return Err("that index isn't this team's".into());
            }
            self.indexes
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| "no index".into())
        }
    }

    /// The test embedder's model id.
    pub const TEST_MODEL: &str = "test-words";

    /// Hashes words into a vector: texts sharing words point the same way.
    /// Like a real embedder, it doesn't see codes: tokens with digits are
    /// left out, so exact-term matching must come from the keyword list.
    pub struct WordEmbedder;

    pub fn embed_words(text: &str) -> Vec<f32> {
        let mut v = vec![0.0f32; 1024];
        // Whole codes (anything with a digit) and their parts are skipped.
        let plain: String = text
            .split_whitespace()
            .filter(|w| !w.chars().any(|c| c.is_ascii_digit()))
            .collect::<Vec<_>>()
            .join(" ");
        for w in terms(&plain) {
            let h = w
                .bytes()
                .fold(0u32, |h, b| h.wrapping_mul(31).wrapping_add(b as u32));
            v[(h % 1024) as usize] += 1.0;
        }
        v
    }

    #[async_trait::async_trait]
    impl Embedder for WordEmbedder {
        fn model(&self) -> &str {
            TEST_MODEL
        }
        async fn embed(&self, text: &str) -> Result<Vec<f32>, String> {
            Ok(embed_words(text))
        }
    }

    impl MemoryStore {
        /// Adds a ready file whose passages are `texts`.
        pub fn add_file(
            &self,
            team_id: &str,
            base: &Base,
            file_id: &str,
            name: &str,
            as_of: &str,
            texts: &[&str],
        ) {
            let chunks = texts
                .iter()
                .enumerate()
                .map(|(i, t)| IndexedChunk {
                    ord: i as u32,
                    loc: format!("Page {}", i + 1),
                    text: t.to_string(),
                    vector: embed_words(t),
                })
                .collect();
            self.add_index(
                team_id,
                base,
                file_id,
                name,
                as_of,
                Index::build(TEST_MODEL, 1024, chunks).unwrap(),
            );
        }

        /// Adds a ready file with a given index.
        pub fn add_index(
            &self,
            team_id: &str,
            base: &Base,
            file_id: &str,
            name: &str,
            as_of: &str,
            index: Index,
        ) {
            {
                let mut b = self.bases.lock().unwrap();
                let list = b.entry(team_id.to_string()).or_default();
                if !list.iter().any(|x| x.id == base.id) {
                    list.push(base.clone());
                }
            }
            let file = FileMeta {
                id: file_id.into(),
                kb_id: base.id.clone(),
                name: name.into(),
                kind: "text".into(),
                status: FileStatus::Ready,
                version: 1,
                ready_version: 1,
                as_of: as_of.into(),
                chunks: index.chunks.len() as u32,
                ..Default::default()
            };
            let key = file.searchable_index(team_id).unwrap();
            self.files
                .lock()
                .unwrap()
                .entry(team_id.to_string())
                .or_default()
                .push(file);
            self.indexes.lock().unwrap().insert(key, Arc::new(index));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;

    fn base(id: &str, name: &str) -> Base {
        Base {
            id: id.into(),
            name: name.into(),
            read_role: "member".into(),
            ..Default::default()
        }
    }

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-10-24T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    fn chunk(ord: u32, vector: Vec<f32>) -> IndexedChunk {
        IndexedChunk {
            ord,
            loc: "Page 1".into(),
            text: "x".into(),
            vector,
        }
    }

    #[test]
    fn the_index_round_trips_with_its_model_and_dimension() {
        let idx = Index::build(
            "amazon.titan-embed-text-v2:0",
            3,
            vec![IndexedChunk {
                ord: 3,
                loc: "Page 2".into(),
                text: "héllo".into(),
                vector: vec![0.25, -1.5, 3.0],
            }],
        )
        .unwrap();
        let back = decode_index(&encode_index(&idx)).unwrap();
        assert_eq!(back, idx);
        assert_eq!(
            (back.model.as_str(), back.dims),
            ("amazon.titan-embed-text-v2:0", 3)
        );
        let stored: Value = serde_json::from_slice(&encode_index(&idx)).unwrap();
        assert_eq!(stored["chunks"][0]["model"], "amazon.titan-embed-text-v2:0");
        assert_eq!(stored["chunks"][0]["dims"], 3);
        assert!(decode_index(b"nope").is_err());
    }

    use serde_json::Value;

    #[test]
    fn mixed_dimension_chunks_are_refused() {
        // Building one.
        let err =
            Index::build("m", 4, vec![chunk(0, vec![0.1; 4]), chunk(1, vec![0.1; 3])]).unwrap_err();
        assert!(err.contains("mixed dimensions"), "{err}");
        // Reading one that was written mixed.
        let mixed = serde_json::json!({
            "format": 2, "model": "m", "dims": 2,
            "chunks": [
                {"ord": 0, "loc": "", "text": "a", "model": "m", "dims": 2, "v": base64::engine::general_purpose::STANDARD.encode([0u8; 8])},
                {"ord": 1, "loc": "", "text": "b", "model": "m", "dims": 3, "v": base64::engine::general_purpose::STANDARD.encode([0u8; 12])}
            ]
        });
        let err = decode_index(&serde_json::to_vec(&mixed).unwrap()).unwrap_err();
        assert!(err.contains("mixed dimensions"), "{err}");
        // A passage from another model.
        let other = serde_json::json!({
            "format": 2, "model": "m", "dims": 2,
            "chunks": [{"ord": 0, "loc": "", "text": "a", "model": "old", "dims": 2, "v": base64::engine::general_purpose::STANDARD.encode([0u8; 8])}]
        });
        assert!(decode_index(&serde_json::to_vec(&other).unwrap())
            .unwrap_err()
            .contains("mixed models"));
    }

    #[tokio::test]
    async fn an_index_from_another_model_is_stale_never_scored() {
        let store = MemoryStore::default();
        let b = base("kb", "Docs");
        let old = Index::build(
            "old-model",
            1024,
            vec![IndexedChunk {
                ord: 0,
                loc: String::new(),
                text: "Parental leave is sixteen weeks.".into(),
                vector: embed_words("Parental leave is sixteen weeks."),
            }],
        )
        .unwrap();
        store.add_index("t", &b, "f_old", "Old.md", "2026-01-01", old);
        let small = Index::build(TEST_MODEL, 8, vec![chunk(0, vec![1.0; 8])]).unwrap();
        store.add_index("t", &b, "f_small", "Small.md", "2026-01-01", small);
        let found = search(
            &store,
            &WordEmbedder,
            "t",
            &["kb".into()],
            "parental leave weeks",
            5,
            now(),
        )
        .await
        .unwrap();
        assert!(found.hits.is_empty(), "{:?}", found.hits);
        let stale: Vec<&str> = found.stale.iter().map(|s| s.file_id.as_str()).collect();
        assert_eq!(stale, ["f_old", "f_small"]);
    }

    #[test]
    fn tokens_keep_codes_whole_and_split() {
        assert_eq!(
            tokens("ERR-4021 at db_main"),
            ["err-4021", "err", "4021", "at", "db_main", "db", "main"]
        );
        assert_eq!(
            terms("What is the SKU-42 refund window?"),
            ["42", "refund", "sku", "sku-42", "window"]
        );
        assert!(
            (term_overlap(&terms("refund window"), "The refund WINDOW is 30 days") - 1.0).abs()
                < 1e-6
        );
        assert!(recency(0.0) > recency(365.0) && recency(365.0) > recency(3650.0));
        assert!(recency(100_000.0) >= 0.85);
    }

    #[tokio::test]
    async fn an_exact_term_vector_search_misses_is_found() {
        let store = MemoryStore::default();
        let ops = base("kb_ops", "Runbooks");
        store.add_file(
            "t",
            &ops,
            "f1",
            "Errors.md",
            "2026-05-01",
            &[
                "ERR-4021 means the storage quota ran out; raise it in the console.",
                "Deploys run every weekday at noon from the main branch.",
                "Rotate the signing keys every ninety days.",
            ],
        );
        let q = "ERR-4021";
        // Pure vector search misses it: the embedder doesn't see codes.
        let qv = embed_words(q);
        let idx = store
            .indexes
            .lock()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .clone();
        let cands: Vec<Candidate> = idx
            .chunks
            .iter()
            .map(|c| Candidate {
                text: &c.text,
                loc: &c.loc,
                vector: &c.vector,
                age_days: 0.0,
                authoritative: false,
            })
            .collect();
        assert!(vector_list(&qv, &cands).is_empty());
        // Hybrid search finds it through the keyword list.
        let found = search(&store, &WordEmbedder, "t", &["kb_ops".into()], q, 5, now())
            .await
            .unwrap();
        assert_eq!(found.hits.len(), 1, "{:?}", found.hits);
        assert!(found.hits[0].text.starts_with("ERR-4021"));
    }

    #[test]
    fn the_score_floor_drops_weak_matches() {
        let strong = embed_words("parental leave sixteen weeks paid");
        let weak_text = "The office kitchen is cleaned on Fridays by facilities staff members.";
        let weak = embed_words(weak_text);
        let texts = ["Parental leave is sixteen weeks paid.", weak_text];
        let cands = [
            Candidate {
                text: texts[0],
                loc: "",
                vector: &strong,
                age_days: 0.0,
                authoritative: false,
            },
            Candidate {
                text: texts[1],
                loc: "",
                vector: &weak,
                age_days: 0.0,
                authoritative: false,
            },
        ];
        let q = "How many weeks of parental leave?";
        let qv = embed_words(q);
        // The weak passage shares nothing with the question: under both floors.
        assert!(cosine(&qv, &weak) < MIN_COSINE);
        let v = vector_list(&qv, &cands);
        let k = keyword_list(q, &cands);
        assert!(v.iter().all(|(i, _)| *i == 0) && k.iter().all(|(i, _)| *i == 0));
        let fused = fuse(&v, &k, &cands);
        assert_eq!(fused.iter().map(|(i, _)| *i).collect::<Vec<_>>(), [0]);
        // One word in common out of many is not enough for the keyword list.
        let thin = [Candidate {
            text: "weeks",
            loc: "",
            vector: &weak,
            age_days: 0.0,
            authoritative: false,
        }];
        assert!(keyword_list("parental leave policy weeks contractors", &thin).is_empty());
        // A passage that places only 31st in one list is under the floor.
        let deep: Vec<(usize, f32)> = (0..31).map(|i| (i, 1.0)).collect();
        let many: Vec<Candidate> = (0..31)
            .map(|_| Candidate {
                text: "",
                loc: "",
                vector: &weak,
                age_days: 0.0,
                authoritative: false,
            })
            .collect();
        let fused = fuse(&deep, &[], &many);
        assert_eq!(fused.len(), 30);
        assert!(fused.iter().all(|(i, _)| *i < 30));
    }

    #[test]
    fn fusion_rewards_both_lists_then_age_and_authority() {
        let v = vec![0.0f32; 2];
        let mk = |age: f32, auth: bool| Candidate {
            text: "",
            loc: "",
            vector: &v,
            age_days: age,
            authoritative: auth,
        };
        let cands = [
            mk(0.0, false),
            mk(0.0, false),
            mk(900.0, false),
            mk(900.0, true),
        ];
        // 0 is first in both lists; 1 only in one.
        let fused = fuse(&[(0, 0.9), (1, 0.8)], &[(0, 5.0)], &cands);
        assert_eq!(fused[0].0, 0);
        // Same ranks: the source of truth outranks the plain old one.
        let fused = fuse(&[(2, 0.9)], &[(3, 5.0)], &cands);
        assert_eq!(fused[0].0, 3);
        // Same ranks: the newer one outranks the older one.
        let fused = fuse(&[(2, 0.9)], &[(1, 5.0)], &cands);
        assert_eq!(fused[0].0, 1);
    }

    #[tokio::test]
    async fn search_finds_the_relevant_passage_with_its_citation() {
        let store = MemoryStore::default();
        let hr = base("kb_hr", "HR handbook");
        store.add_file(
            "team_a",
            &hr,
            "f1",
            "Leave policy.pdf",
            "2026-05-01",
            &[
                "Parental leave is sixteen weeks fully paid for every employee.",
                "Expense reports are due on the fifth business day of each month.",
            ],
        );
        let found = search(
            &store,
            &WordEmbedder,
            "team_a",
            &["kb_hr".into()],
            "How many weeks is parental leave?",
            5,
            now(),
        )
        .await
        .unwrap();
        let hits = found.hits;
        assert!(!hits.is_empty());
        assert_eq!(hits[0].file_name, "Leave policy.pdf");
        assert_eq!(hits[0].loc, "Page 1");
        assert_eq!(hits[0].as_of, "2026-05-01");
        assert!(hits[0].text.contains("sixteen weeks"));
        assert!(hits.iter().all(|h| !h.text.contains("Expense")), "{hits:?}");
    }

    #[tokio::test]
    async fn team_a_never_reads_team_b() {
        let store = MemoryStore::default();
        let secret = base("kb_b", "Team B secrets");
        store.add_file(
            "team_b",
            &secret,
            "fb",
            "Pricing.pdf",
            "2026-01-01",
            &["The enterprise discount floor is forty percent."],
        );
        let q = "enterprise discount floor percent";
        let b = search(
            &store,
            &WordEmbedder,
            "team_b",
            &["kb_b".into()],
            q,
            5,
            now(),
        )
        .await
        .unwrap();
        assert_eq!(b.hits.len(), 1);
        // Team A, naming team B's base id, finds nothing.
        let a = search(
            &store,
            &WordEmbedder,
            "team_a",
            &["kb_b".into()],
            q,
            5,
            now(),
        )
        .await
        .unwrap();
        assert!(a.hits.is_empty());
        // A base of team A's with the same id reads only team A's files.
        let mine = base("kb_b", "Team A notes");
        store.add_file(
            "team_a",
            &mine,
            "fa",
            "Notes.md",
            "2026-01-01",
            &["Lunch is at noon."],
        );
        let a = search(
            &store,
            &WordEmbedder,
            "team_a",
            &["kb_b".into()],
            q,
            5,
            now(),
        )
        .await
        .unwrap();
        assert!(a.hits.iter().all(|h| h.file_id == "fa"), "{:?}", a.hits);
        // And the store refuses another team's index key outright.
        let key = crate::knowledge::index_key("team_b", "kb_b", "fb", 1);
        assert!(store.index("team_a", &key).await.is_err());
    }

    #[tokio::test]
    async fn nothing_relevant_finds_nothing() {
        let store = MemoryStore::default();
        let hr = base("kb_hr", "HR handbook");
        store.add_file(
            "t",
            &hr,
            "f1",
            "Leave.pdf",
            "2026-05-01",
            &["Parental leave is sixteen weeks."],
        );
        let found = search(
            &store,
            &WordEmbedder,
            "t",
            &["kb_hr".into()],
            "Kubernetes ingress certificate rotation",
            5,
            now(),
        )
        .await
        .unwrap();
        assert!(found.hits.is_empty(), "{:?}", found.hits);
        assert!(
            search(&store, &WordEmbedder, "t", &[], "Parental leave", 5, now())
                .await
                .unwrap()
                .hits
                .is_empty()
        );
    }

    #[tokio::test]
    async fn files_not_ready_are_not_searched() {
        let store = MemoryStore::default();
        let b = base("kb", "Docs");
        store.add_file(
            "t",
            &b,
            "f1",
            "Draft.md",
            "2026-05-01",
            &["Parental leave is sixteen weeks."],
        );
        store.files.lock().unwrap().get_mut("t").unwrap()[0].ready_version = 0;
        let found = search(
            &store,
            &WordEmbedder,
            "t",
            &["kb".into()],
            "parental leave weeks",
            5,
            now(),
        )
        .await
        .unwrap();
        assert!(found.hits.is_empty());
    }

    #[tokio::test]
    async fn one_file_does_not_crowd_out_the_rest() {
        let store = MemoryStore::default();
        let b = base("kb", "Docs");
        let many: Vec<String> = (0..8)
            .map(|i| format!("Deploy freeze rules part {i}: deploy freeze on Fridays."))
            .collect();
        let refs: Vec<&str> = many.iter().map(String::as_str).collect();
        store.add_file("t", &b, "big", "Big.md", "2026-05-01", &refs);
        store.add_file(
            "t",
            &b,
            "small",
            "Small.md",
            "2026-05-01",
            &["Deploy freeze starts Friday noon."],
        );
        let found = search(
            &store,
            &WordEmbedder,
            "t",
            &["kb".into()],
            "deploy freeze friday",
            10,
            now(),
        )
        .await
        .unwrap();
        assert!(found.hits.iter().filter(|h| h.file_id == "big").count() <= 3);
        assert!(found.hits.iter().any(|h| h.file_id == "small"));
    }
}
