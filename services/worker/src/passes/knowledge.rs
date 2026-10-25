//! Knowledge bases in the worker: reading an uploaded file into a search
//! index, and the `search_knowledge` tool for the agents a base is attached
//! to (planner, coder, reviewer, plan chat).
//!
//! The gateway calls [`ingest`] asynchronously once the browser's upload is
//! in. Every Titan call is metered (`common::embed`). A file that fails
//! keeps its reason on its record, and a re-upload that fails leaves the
//! last ready version searchable.

use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};

use common::knowledge::chunk::chunk;
use common::knowledge::extract::{extract, Kind};
use common::knowledge::search::{encode_index, search, AwsStore, Hit, Index, IndexedChunk, Titan};
use common::knowledge::{self as kb, FileMeta, FileStatus};

use crate::agent::llm::ToolDefinition;
use crate::WorkerState;

/// Embedding calls in flight at once for one file.
const EMBED_CONCURRENCY: usize = 6;

/// One file version to read.
#[derive(Debug, Clone, Deserialize)]
pub struct IngestRequest {
    pub team_id: String,
    pub kb_id: String,
    pub file_id: String,
    pub version: u32,
}

fn store(state: &WorkerState) -> AwsStore {
    AwsStore {
        dynamo: state.dynamo.clone(),
        s3: state.s3.clone(),
        table: state.config.settings_table_name.clone(),
        bucket: state.config.bucket_name.clone(),
    }
}

fn meter(state: &WorkerState, team_id: &str) -> common::usage::UsageMeter {
    common::usage::UsageMeter::dynamo(
        state.dynamo.clone(),
        &state.config.analytics_table_name,
        team_id,
    )
}

async fn load_file(state: &WorkerState, req: &IngestRequest) -> Result<Option<FileMeta>, String> {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(req.team_id.clone()))
        .key(
            "sk",
            AttributeValue::S(kb::file_sk(&req.kb_id, &req.file_id)),
        )
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("file record: {e}"))?;
    Ok(item.item().and_then(kb::read_data::<FileMeta>))
}

/// Writes the file's record if it is still the version this run read (a
/// re-upload meanwhile wins). Returns false when it was superseded.
async fn save_file(state: &WorkerState, team_id: &str, f: &FileMeta) -> Result<bool, String> {
    let r = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(kb::file_record(team_id, f)))
        .condition_expression("attribute_exists(pk) AND version = :v")
        .expression_attribute_values(":v", AttributeValue::N(f.version.to_string()))
        .send()
        .await;
    match r {
        Ok(_) => Ok(true),
        Err(e) => {
            let text = format!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(&e));
            if text.contains("ConditionalCheckFailed") {
                Ok(false)
            } else {
                Err(format!("file record: {text}"))
            }
        }
    }
}

/// Reads, splits and embeds one uploaded file, then makes it searchable.
/// Never fails: problems are written on the file's record.
pub async fn ingest(state: &WorkerState, req: IngestRequest) -> Value {
    if !kb::valid_id(&req.kb_id) || !kb::valid_id(&req.file_id) || req.team_id.is_empty() {
        return json!({ "ok": false, "error": "bad request" });
    }
    let file = match load_file(state, &req).await {
        Ok(Some(f)) => f,
        Ok(None) => return json!({ "ok": false, "error": "no such file" }),
        Err(e) => {
            warn!(error = %e, "Knowledge ingest: file record unreadable");
            return json!({ "ok": false, "error": e });
        }
    };
    if file.version != req.version {
        info!(file = %req.file_id, "Knowledge ingest: a newer version was uploaded; skipped");
        return json!({ "ok": false, "error": "superseded" });
    }
    let result = read_and_index(state, &req, &file).await;
    let mut done = file.clone();
    match result {
        Ok((index, as_of, truncated)) => {
            done.status = FileStatus::Ready;
            done.error.clear();
            done.ready_version = req.version;
            done.chunks = index.chunks.len() as u32;
            done.truncated = truncated;
            if let Some(d) = as_of {
                done.as_of = d;
            }
        }
        Err(why) => {
            warn!(file = %req.file_id, error = %why, "Knowledge file failed");
            done.status = FileStatus::Failed;
            done.error = why;
        }
    }
    match save_file(state, &req.team_id, &done).await {
        Ok(true) => {
            if done.status == FileStatus::Ready {
                remove_old_versions(state, &req, done.ready_version).await;
            }
            info!(team_id = %req.team_id, file = %req.file_id, status = ?done.status, chunks = done.chunks, "Knowledge file processed");
            json!({ "ok": done.status == FileStatus::Ready, "chunks": done.chunks })
        }
        Ok(false) => json!({ "ok": false, "error": "superseded" }),
        Err(e) => {
            warn!(error = %e, "Knowledge ingest: result not saved");
            json!({ "ok": false, "error": e })
        }
    }
}

/// The file's index, its own date, and whether passages were dropped.
async fn read_and_index(
    state: &WorkerState,
    req: &IngestRequest,
    file: &FileMeta,
) -> Result<(Index, Option<String>, bool), String> {
    let kind = Kind::of(&file.name).ok_or("This kind of file isn't supported.")?;
    let key = kb::source_key(&req.team_id, &req.kb_id, &req.file_id, req.version);
    let obj = state
        .s3
        .get_object()
        .bucket(&state.config.bucket_name)
        .key(&key)
        .send()
        .await
        .map_err(|_| "The upload wasn't found. Upload the file again.".to_string())?;
    if obj.content_length().unwrap_or(0) as u64 > kb::MAX_FILE_BYTES {
        return Err("The file is larger than 25 MB.".into());
    }
    let bytes = obj
        .body
        .collect()
        .await
        .map_err(|_| "The upload couldn't be read. Upload the file again.".to_string())?
        .into_bytes()
        .to_vec();
    if bytes.len() as u64 > kb::MAX_FILE_BYTES {
        return Err("The file is larger than 25 MB.".into());
    }
    let extracted = tokio::task::spawn_blocking(move || extract(kind, &bytes))
        .await
        .map_err(|_| "The file couldn't be read.".to_string())??;
    let mut chunks = chunk(&extracted.sections);
    let truncated = chunks.len() > kb::MAX_CHUNKS_PER_FILE;
    chunks.truncate(kb::MAX_CHUNKS_PER_FILE);
    if chunks.is_empty() {
        return Err("The file has no text to read.".into());
    }
    let meter = meter(state, &req.team_id);
    let titan = Arc::new((state.bedrock.clone(), meter));
    let embedded: Vec<Result<IndexedChunk, String>> = futures::stream::iter(chunks)
        .map(|c| {
            let titan = titan.clone();
            async move {
                // The place goes in with the text, so "page 3 of the leave
                // policy" style questions match too.
                let input = if c.loc.is_empty() {
                    c.text.clone()
                } else {
                    format!("{}\n{}", c.loc, c.text)
                };
                let vector = common::embed::titan_embed(&titan.0, &titan.1, &input).await?;
                Ok(IndexedChunk {
                    ord: c.ord,
                    loc: c.loc,
                    text: c.text,
                    vector,
                })
            }
        })
        .buffered(EMBED_CONCURRENCY)
        .collect()
        .await;
    let mut vectors = Vec::with_capacity(embedded.len());
    for e in embedded {
        vectors.push(e.map_err(|e| {
            warn!(error = %e, "Knowledge embedding failed");
            "The text couldn't be indexed right now. Upload the file again in a few minutes."
                .to_string()
        })?);
    }
    // One model, one dimension: an index never mixes them.
    let index =
        Index::build(common::embed::TITAN_V2, common::embed::DIMENSIONS, vectors).map_err(|e| {
            warn!(error = %e, "Knowledge index refused");
            "The text couldn't be indexed right now. Upload the file again in a few minutes."
                .to_string()
        })?;
    state
        .s3
        .put_object()
        .bucket(&state.config.bucket_name)
        .key(kb::index_key(
            &req.team_id,
            &req.kb_id,
            &req.file_id,
            req.version,
        ))
        .content_type("application/json")
        .body(encode_index(&index).into())
        .send()
        .await
        .map_err(|e| {
            warn!(error = %e, "Knowledge index not saved");
            "The index couldn't be saved. Upload the file again.".to_string()
        })?;
    Ok((index, extracted.as_of, truncated))
}

/// Deletes every version of the file older than `keep`.
async fn remove_old_versions(state: &WorkerState, req: &IngestRequest, keep: u32) {
    let folder = kb::file_folder(&req.team_id, &req.kb_id, &req.file_id);
    let keep_prefix = format!("{folder}v{keep}/");
    let Ok(list) = state
        .s3
        .list_objects_v2()
        .bucket(&state.config.bucket_name)
        .prefix(&folder)
        .send()
        .await
    else {
        return;
    };
    for o in list.contents() {
        let Some(k) = o.key() else { continue };
        if !k.starts_with(&keep_prefix) && kb::owns_key(&req.team_id, k) {
            let _ = state
                .s3
                .delete_object()
                .bucket(&state.config.bucket_name)
                .key(k)
                .send()
                .await;
        }
    }
}

// ── The agents' tool ────────────────────────────────────────────────────────

/// The knowledge bases attached to one agent of a team.
pub struct KnowledgeTools {
    team_id: String,
    bases: Vec<kb::Base>,
}

/// The tool's name.
pub const TOOL: &str = "search_knowledge";

impl KnowledgeTools {
    /// The bases attached to `agent` (a key of `knowledge::AGENTS`), or None
    /// when there are none.
    pub async fn load(state: &WorkerState, team_id: &str, agent: &str) -> Option<Self> {
        let bases = match common::knowledge::search::Store::bases(&store(state), team_id).await {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "Knowledge bases unreadable; agent goes without");
                return None;
            }
        };
        Self::from_bases(team_id, bases, agent)
    }

    pub fn from_bases(team_id: &str, bases: Vec<kb::Base>, agent: &str) -> Option<Self> {
        let bases: Vec<kb::Base> = bases.into_iter().filter(|b| b.attached_to(agent)).collect();
        (!bases.is_empty()).then(|| Self {
            team_id: team_id.to_string(),
            bases,
        })
    }

    pub fn tools(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition {
            name: TOOL.into(),
            description: "Search the team's knowledge bases (documents the team uploaded: \
                          policies, specs, runbooks, product docs). Returns the best passages, \
                          each with its file, page or section and date. Cite the file and page \
                          when you rely on one."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "What to look for, in plain words" },
                    "limit": { "type": "integer", "description": "Passages to return (1-8, default 5)" }
                },
                "required": ["query"]
            }),
        }]
    }

    pub fn prompt_section(&self) -> String {
        let mut s = String::from(
            "\n\n## Team knowledge bases\nThe team's own documents, searched with search_knowledge. \
             Look things up there before assuming how the team does something; when sources \
             disagree, prefer one marked source of truth, then the newer one, and say so.\n",
        );
        for b in &self.bases {
            s.push_str(&format!(
                "- {}{}{}\n",
                b.name,
                if b.description.trim().is_empty() {
                    String::new()
                } else {
                    format!(": {}", common::truncate_str(b.description.trim(), 200))
                },
                if b.authoritative {
                    " (source of truth)"
                } else {
                    ""
                }
            ));
        }
        s
    }

    /// Runs the tool when `name` is it.
    pub async fn handle(&self, state: &WorkerState, name: &str, input: &Value) -> Option<String> {
        if name != TOOL {
            return None;
        }
        let query = input["query"].as_str().unwrap_or("").trim();
        if query.is_empty() {
            return Some("Give a query to search for.".into());
        }
        let limit = input["limit"].as_u64().unwrap_or(5).clamp(1, 8) as usize;
        let ids: Vec<String> = self.bases.iter().map(|b| b.id.clone()).collect();
        let meter = meter(state, &self.team_id);
        let titan = Titan {
            client: &state.bedrock,
            meter: &meter,
        };
        let hits = search(
            &store(state),
            &titan,
            &self.team_id,
            &ids,
            query,
            limit,
            chrono::Utc::now(),
        )
        .await;
        Some(match hits {
            Ok(found) => {
                if !found.stale.is_empty() {
                    warn!(
                        files = found.stale.len(),
                        "Knowledge files embedded by another model are skipped until re-embedded"
                    );
                }
                render_hits(&found.hits)
            }
            Err(e) => {
                warn!(error = %e, "Knowledge search failed");
                "The knowledge bases couldn't be searched right now. Continue without them.".into()
            }
        })
    }
}

/// Passages as the tool's answer.
pub fn render_hits(hits: &[Hit]) -> String {
    if hits.is_empty() {
        return "Nothing in the team's knowledge bases matches this.".into();
    }
    let mut s = String::new();
    for h in hits {
        s.push_str(&format!(
            "--- {} — {}{}{}{}\n{}\n",
            h.kb_name,
            h.file_name,
            if h.loc.is_empty() {
                String::new()
            } else {
                format!(", {}", h.loc)
            },
            if h.as_of.is_empty() {
                String::new()
            } else {
                format!(" (as of {})", h.as_of)
            },
            if h.authoritative {
                " [source of truth]"
            } else {
                ""
            },
            h.text.trim()
        ));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(id: &str, agents: &[&str], team: bool) -> kb::Base {
        kb::Base {
            id: id.into(),
            name: format!("Base {id}"),
            agents: agents.iter().map(|a| a.to_string()).collect(),
            attach_team: team,
            ..Default::default()
        }
    }

    #[test]
    fn only_attached_bases_reach_an_agent() {
        let bases = vec![
            base("a", &["reviewer"], false),
            base("b", &[], true),
            base("c", &["coder"], false),
        ];
        let r = KnowledgeTools::from_bases("t", bases.clone(), "reviewer").unwrap();
        let ids: Vec<&str> = r.bases.iter().map(|b| b.id.as_str()).collect();
        assert_eq!(ids, ["a", "b"]);
        assert!(
            KnowledgeTools::from_bases("t", vec![base("c", &["coder"], false)], "planner")
                .is_none()
        );
        let p = r.prompt_section();
        assert!(p.contains("Base a") && p.contains("search_knowledge"));
        assert_eq!(r.tools()[0].name, TOOL);
    }

    #[test]
    fn hits_render_with_their_citation() {
        assert!(render_hits(&[]).contains("Nothing"));
        let s = render_hits(&[Hit {
            kb_id: "k".into(),
            kb_name: "HR".into(),
            file_id: "f".into(),
            file_name: "Leave.pdf".into(),
            loc: "Page 4".into(),
            as_of: "2026-05-01".into(),
            text: "Sixteen weeks.".into(),
            score: 0.7,
            authoritative: true,
            ord: 0,
            ..Default::default()
        }]);
        assert!(s.contains("HR — Leave.pdf, Page 4 (as of 2026-05-01) [source of truth]"));
        assert!(s.contains("Sixteen weeks."));
    }

    #[test]
    fn an_ingest_request_reads() {
        let r: IngestRequest = serde_json::from_value(json!({
            "team_id": "TEAM#x", "kb_id": "kb", "file_id": "f", "version": 2
        }))
        .unwrap();
        assert_eq!(r.version, 2);
    }
}
