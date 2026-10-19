pub mod embedder;
pub mod lock;
pub mod s3_sync;

use crate::WorkerState;
use mentedb::MenteDb;
use mentedb_core::memory::{MemoryNode, MemoryType};
use mentedb_core::types::AgentId;
use std::path::PathBuf;
use tracing::{info, warn};

use self::embedder::embed_async;

/// Wraps a MenteDB instance with S3 sync and DynamoDB locking.
/// Provides a simple API for worker passes to store and recall memories.
/// Most pinned memories handed to an agent on one ticket.
const MAX_PINNED: usize = 10;

/// Fused-score relevance floor for recalled learnings; see `recall_context`.
const MIN_RELEVANCE: f32 = 0.02;

pub struct AgentMemory {
    db: MenteDb,
    bedrock: aws_sdk_bedrockruntime::Client,
    local_dir: PathBuf,
    team_id: String,
    repo_owner: String,
    repo_name: String,
    lock_token: Option<String>,
    dirty: bool,
    /// Ids of the memories the last recall handed to an agent, pinned ones
    /// included, so the run can count their use.
    pub last_recalled: Vec<String>,
}

/// Open a repo's memory directory with the engine settings the worker uses.
///
/// Engine calibration stays off. On open it embeds a built-in probe set
/// through the synchronous `EmbeddingProvider::embed`, which for Bedrock means
/// `block_on` inside the worker's async runtime: a panic, which aborts the
/// whole Lambda under the release profile's `panic = "abort"`. It also
/// costs about forty Titan calls on every run whose snapshot lacks the cached
/// result. Its only
/// effect would be to lower the write-time duplicate threshold from 0.97 to
/// the measured paraphrase band of the embedder, and that is the change we do
/// not want: the engine's write-time duplicate check has no value-update
/// guard, so a correction that swaps one word in a long learning (token
/// overlap >= 0.85) would be skipped as a duplicate and the stale fact kept.
/// With 0.97 only near-verbatim restatements are skipped, as before.
/// Supersession of value updates does not depend on calibration: the engine
/// default floor (0.3) was set from Titan and Cohere measurements.
fn open_db(
    dir: &std::path::Path,
    embedder: Box<dyn mentedb_embedding::EmbeddingProvider>,
) -> mentedb_core::error::MenteResult<MenteDb> {
    let config = mentedb::CognitiveConfig {
        calibration_enabled: false,
        ..Default::default()
    };
    MenteDb::open_with_embedder_and_config(dir, embedder, config)
}

/// Pinned memories (id, content), at most [`MAX_PINNED`], found through the
/// engine's tag index instead of loading every memory. The tag is checked
/// again on the loaded memory in case the index lags a write.
fn pinned_memories(db: &MenteDb) -> Vec<(String, String)> {
    // Oldest first, so which ten reach the agent does not change run to run.
    let mut pinned: Vec<(u64, String, String)> = db
        .memory_ids_with_tag(common::memory_meta::PINNED)
        .into_iter()
        .filter_map(|id| db.get_memory(id).ok())
        .filter(|n| n.tags.iter().any(|t| t == common::memory_meta::PINNED))
        .map(|n| (n.created_at, n.id.to_string(), n.content))
        .collect();
    pinned.sort();
    pinned
        .into_iter()
        .take(MAX_PINNED)
        .map(|(_, id, content)| (id, content))
        .collect()
}

/// Where a learning came from: the run and ticket it was learned on.
#[derive(Debug, Clone, Default)]
pub struct LearningOrigin {
    pub run_id: String,
    pub ticket_id: String,
    pub title: String,
}

impl AgentMemory {
    /// Open agent memory for a specific team+repo.
    /// Acquires a DynamoDB lock, downloads the snapshot from S3, and opens MenteDB.
    /// Returns None if memory is disabled or the lock is held by another invocation.
    pub async fn open(
        state: &WorkerState,
        team_id: &str,
        repo_owner: &str,
        repo_name: &str,
    ) -> Option<Self> {
        // Check feature flag
        if std::env::var("MEMORY_ENABLED").unwrap_or_default() != "true" {
            return None;
        }

        // Acquire lock (fencing token — see lock.rs)
        let lock_token = match lock::acquire_lock(
            &state.dynamo,
            &state.config.table_name,
            team_id,
            repo_owner,
            repo_name,
        )
        .await
        {
            Ok(token) => token,
            Err(e) => {
                warn!(error = %e, "Failed to acquire memory lock, running stateless");
                None
            }
        };

        let lock_token = lock_token?;

        // Download snapshot from S3
        let local_dir = match s3_sync::download_memory(
            &state.s3,
            &state.config.bucket_name,
            team_id,
            repo_owner,
            repo_name,
        )
        .await
        {
            Ok(dir) => dir,
            Err(e) => {
                warn!(error = %e, "Failed to download memory, running stateless");
                lock::release_lock(
                    &state.dynamo,
                    &state.config.table_name,
                    team_id,
                    repo_owner,
                    repo_name,
                    Some(&lock_token),
                )
                .await;
                return None;
            }
        };

        let bedrock_embedder = embedder::BedrockEmbedder::new(state.bedrock.clone());
        match open_db(&local_dir, Box::new(bedrock_embedder)) {
            Ok(db) => {
                info!(
                    memories = db.memory_count(),
                    "Opened agent memory for {repo_owner}/{repo_name}"
                );
                Some(Self {
                    db,
                    bedrock: state.bedrock.clone(),
                    local_dir,
                    team_id: team_id.to_string(),
                    repo_owner: repo_owner.to_string(),
                    repo_name: repo_name.to_string(),
                    lock_token: Some(lock_token),
                    dirty: false,
                    last_recalled: Vec::new(),
                })
            }
            Err(e) => {
                warn!(error = %e, "Failed to open MenteDB, running stateless");
                lock::release_lock(
                    &state.dynamo,
                    &state.config.table_name,
                    team_id,
                    repo_owner,
                    repo_name,
                    Some(&lock_token),
                )
                .await;
                None
            }
        }
    }

    /// Recall relevant memories for a given query text.
    /// Returns formatted context suitable for injection into prompts.
    pub async fn recall_context(&mut self, query: &str, k: usize) -> String {
        let embedding = match embed_async(&self.bedrock, query).await {
            Ok(emb) => emb,
            Err(e) => {
                warn!(error = %e, "Failed to embed recall query");
                return String::new();
            }
        };

        // Hybrid (vector + BM25 via RRF): tickets name exact code identifiers
        // (function names, crate names, file paths) that pure vector recall
        // ranks poorly — the keyword arm catches them. Same fused-score scale
        // as recall_similar, so the floor below still applies.
        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let results =
            match self
                .db
                .recall_hybrid_at(&embedding, Some(query), k, now_micros, None, None, None)
            {
                Ok(r) => r,
                Err(e) => {
                    warn!(error = %e, "Failed to recall memories");
                    return String::new();
                }
            };

        // Relevance floor: top-k on a small store returns SOMETHING no matter
        // how unrelated — injecting off-topic memories into every ticket
        // steers the agent wrong. Cosine similarity below the floor is noise.
        // mentedb's recall_similar returns RRF-fused rank scores
        // (max ≈ 0.08 = 1/60·0.7 + salience·0.05 + recency·0.02), NOT cosine
        // similarity — a cosine-scale floor filtered out every result. This
        // floor only drops the deep tail of the ranked list.
        let results: Vec<_> = results
            .into_iter()
            .filter(|(_, score)| *score >= MIN_RELEVANCE)
            .collect();
        self.last_recalled.clear();

        // Pinned memories reach the agent on every ticket: a person decided
        // they always apply.
        let pinned = pinned_memories(&self.db);
        if results.is_empty() && pinned.is_empty() {
            return String::new();
        }

        let mut context = String::from("## Agent Memory\n");
        if !pinned.is_empty() {
            context.push_str("Always follow these; your team pinned them for this repository:\n\n");
            for (id, content) in &pinned {
                context.push_str(&format!("- {content}\n"));
                self.last_recalled.push(id.clone());
            }
            context.push('\n');
        }
        if results.is_empty() {
            return context;
        }
        context.push_str("Relevant learnings from past runs on this repository:\n\n");
        for (id, score) in &results {
            if let Ok(node) = self.db.get_memory(*id) {
                let type_label = match node.memory_type {
                    MemoryType::AntiPattern => "⚠️ Anti-pattern",
                    MemoryType::Semantic => "📋 Finding",
                    MemoryType::Procedural => "🔧 Procedure",
                    MemoryType::Correction => "✏️ Correction",
                    MemoryType::Episodic => "📝 Note",
                    MemoryType::Reasoning => "💡 Reasoning",
                };
                if node.tags.iter().any(|t| t == common::memory_meta::PINNED) {
                    continue;
                }
                context.push_str(&format!(
                    "- **{type_label}** (relevance: {score:.2}): {}\n",
                    node.content
                ));
                self.last_recalled.push(id.to_string());
            }
        }
        context
    }

    /// Store a new learning from this run.
    pub async fn store_learning(
        &mut self,
        content: &str,
        memory_type: MemoryType,
        tags: Vec<String>,
    ) {
        // Generate embedding via async Bedrock call
        let embedding = match embed_async(&self.bedrock, content).await {
            Ok(emb) => emb,
            Err(e) => {
                warn!(error = %e, "Failed to embed memory content");
                return;
            }
        };

        let mut node = MemoryNode::new(AgentId::nil(), memory_type, content.to_string(), embedding);
        node.tags = tags;
        node.confidence = 0.9;

        match self.db.store_with_outcome(node) {
            Ok(mentedb::StoreOutcome::Stored) => self.dirty = true,
            Ok(mentedb::StoreOutcome::Duplicate { of }) => {
                info!(existing = %of, "Learning already remembered, not stored again");
            }
            Err(e) => warn!(error = %e, "Failed to store memory"),
        }
    }

    /// Store an anti-pattern (something that went wrong).
    pub async fn store_anti_pattern(&mut self, description: &str) {
        self.store_learning(
            description,
            MemoryType::AntiPattern,
            vec![
                "anti-pattern".to_string(),
                format!("repo:{}/{}", self.repo_owner, self.repo_name),
            ],
        )
        .await;
    }

    /// Extract and store learnings from a conversation log using a lightweight LLM call.
    /// Extracts code style preferences, patterns, anti-patterns, and corrections.
    /// Uses Haiku for speed/cost; called once at end of a successful run.
    pub async fn extract_learnings_from_conversation(
        &mut self,
        conversation_json: &[serde_json::Value],
        provider: &crate::agent::provider::ModelProvider,
        origin: &LearningOrigin,
    ) {
        let api_key = provider.api_key();
        let model_id = provider.primary_model_id();
        if conversation_json.is_empty() || api_key.is_empty() {
            return;
        }

        // Build a compact summary of the conversation (tool results + assistant text)
        let mut summary = String::new();
        let max_summary = 12_000; // keep extraction prompt small
        for turn in conversation_json {
            if summary.len() > max_summary {
                break;
            }
            let role = turn["role"].as_str().unwrap_or("");
            if role == "assistant" {
                // Extract text blocks from assistant
                if let Some(content) = turn["content"].as_array() {
                    for block in content {
                        if block["type"].as_str() == Some("text") {
                            if let Some(text) = block["text"].as_str() {
                                let truncated = common::truncate_str(text, 500);
                                summary.push_str(&format!("[Assistant]: {truncated}\n"));
                            }
                        }
                        if block["type"].as_str() == Some("tool_use") {
                            let name = block["name"].as_str().unwrap_or("?");
                            summary.push_str(&format!("[Tool call: {name}]\n"));
                        }
                    }
                }
            }
        }

        if summary.len() < 200 {
            return; // too little content to extract from
        }

        let extraction_prompt = format!(
            "Analyze this AI coding agent conversation and extract ONLY genuinely useful learnings about this repository. \
             Output a JSON array of objects with \"type\" and \"content\" fields.\n\n\
             Types:\n\
             - \"style\": Code style preferences (naming conventions, formatting, patterns used)\n\
             - \"architecture\": Architecture patterns (how modules are organized, key abstractions)\n\
             - \"procedure\": Build/test/deploy procedures discovered\n\
             - \"anti_pattern\": Things that went wrong or should be avoided\n\
             - \"correction\": Wrong assumptions that were corrected during the conversation\n\n\
             Rules:\n\
             - Only extract SPECIFIC, ACTIONABLE learnings (not vague observations)\n\
             - Each learning must be useful for future runs on this same repo\n\
             - Max 5 learnings. Quality over quantity. Return [] if nothing worth remembering.\n\
             - Content should be concise (1-2 sentences max)\n\n\
             Conversation:\n{summary}\n\n\
             JSON array:"
        );

        let client = reqwest::Client::new();
        let resp = match client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&{
                let mut body = serde_json::json!({
                    // Team-configured model — the hardcoded model id silently
                    // broke extraction for teams whose keys lack access to it.
                    "model": model_id,
                    "max_tokens": 4096,
                    "messages": [{"role": "user", "content": extraction_prompt}],
                });
                if common::is_always_thinking_model(model_id) {
                    body["output_config"] = serde_json::json!({"effort": "low"});
                }
                body
            })
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Memory extraction API call failed");
                return;
            }
        };

        let body: serde_json::Value = match resp.json().await {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "Failed to parse memory extraction response");
                return;
            }
        };
        // Count the call (after the run: the team's counters, not the run's).
        if let (Some(meter), Some(u)) = (&provider.meter, body.get("usage")) {
            let call = common::usage::CallUsage::from_api(u, common::usage::CacheTtl::FiveMinutes);
            meter
                .record_as(common::usage::feature::MEMORY, model_id, &call)
                .await;
        }

        // First TEXT block — thinking models lead with a `thinking` block.
        let text = body["content"]
            .as_array()
            .and_then(|arr| arr.iter().find(|b| b["type"] == "text"))
            .and_then(|b| b["text"].as_str())
            .unwrap_or("");

        // Parse the JSON array from the response (handle markdown fences)
        let clean = text
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();

        let learnings: Vec<serde_json::Value> = match serde_json::from_str(clean) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, raw = %clean, "Failed to parse extraction JSON");
                return;
            }
        };

        let repo_tag = format!("repo:{}/{}", self.repo_owner, self.repo_name);
        let mut stored = 0;
        for learning in learnings.iter().take(5) {
            let ltype = learning["type"].as_str().unwrap_or("semantic");
            let content = match learning["content"].as_str() {
                Some(c) if !c.is_empty() => c,
                _ => continue,
            };

            let (memory_type, tag) = match ltype {
                "style" => (MemoryType::Semantic, "code-style"),
                "architecture" => (MemoryType::Semantic, "architecture"),
                "procedure" => (MemoryType::Procedural, "procedure"),
                "anti_pattern" => (MemoryType::AntiPattern, "anti-pattern"),
                "correction" => (MemoryType::Correction, "correction"),
                _ => (MemoryType::Semantic, "learned"),
            };

            let mut tags = common::memory_meta::run_tags(
                tag,
                &origin.run_id,
                &origin.ticket_id,
                &origin.title,
            );
            tags.push(repo_tag.clone());
            self.store_learning(content, memory_type, tags).await;
            stored += 1;
        }

        if stored > 0 {
            info!(
                count = stored,
                "Extracted and stored learnings from conversation"
            );
        }
    }

    /// Flush, close, upload to S3, and release the lock.
    pub async fn close_and_upload(
        self,
        state: &WorkerState,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let memory_count = self.db.memory_count();

        // Flush/close/upload can each fail — capture the outcome instead of
        // early-returning, so the fenced lock release and /tmp cleanup below
        // run UNCONDITIONALLY. An early `?` here used to leave the lock to its
        // 15-minute TTL (repo stateless for following runs) and the local dir
        // behind on every flush or S3 hiccup.
        let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = async {
            if self.dirty {
                self.db
                    .flush()
                    .map_err(|e| format!("MenteDB flush error: {e}"))?;
            }
            self.db
                .close()
                .map_err(|e| format!("MenteDB close error: {e}"))?;

            // Only upload if we have stored something
            if self.dirty {
                s3_sync::upload_memory(
                    &state.s3,
                    &state.config.bucket_name,
                    &self.team_id,
                    &self.repo_owner,
                    &self.repo_name,
                    &self.local_dir,
                )
                .await?;
                info!(memories = memory_count, "Persisted agent memory to S3");
            }
            Ok(())
        }
        .await;

        // Release lock (fenced by our token) — always, success or not.
        if let Some(ref token) = self.lock_token {
            lock::release_lock(
                &state.dynamo,
                &state.config.table_name,
                &self.team_id,
                &self.repo_owner,
                &self.repo_name,
                Some(token),
            )
            .await;
        }

        // Clean up local files — always.
        s3_sync::cleanup_local(&self.local_dir);

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mentedb_core::error::MenteResult;
    use std::path::PathBuf;

    /// A snapshot written by the engine this worker ran before 0.40.0
    /// (git rev b4db444, release 0.24.4), in the exact tar.gz shape
    /// `s3_sync::upload_memory` puts in S3. It holds 24 worker-shaped learnings
    /// (`Fixture learning number {i}: ...`, embedding `fixture_vector(i)`, run
    /// and topic tags, every 7th pinned), then number 5 deleted and number 6
    /// edited and pinned through the dashboard's forget-then-store path.
    const OLD_SNAPSHOT: &[u8] = include_bytes!("testdata/snapshot-mentedb-0.24.4.tar.gz");

    /// The embedding the fixture gave learning `i`: four 0.5 entries.
    fn fixture_vector(i: usize) -> Vec<f32> {
        let mut v = vec![0.0f32; 1024];
        for k in 0..4 {
            v[(i * 37 + k * 251) % 1024] = 0.5;
        }
        v
    }

    /// The worker always embeds before it calls the engine, so the engine must
    /// never call the embedder itself: for Bedrock that would be `block_on`
    /// inside the async runtime.
    struct MustNotEmbed;

    impl mentedb_embedding::EmbeddingProvider for MustNotEmbed {
        fn embed(&self, _text: &str) -> MenteResult<Vec<f32>> {
            panic!("the engine called the embedder")
        }
        fn embed_batch(&self, _texts: &[&str]) -> MenteResult<Vec<Vec<f32>>> {
            panic!("the engine called the embedder")
        }
        fn dimensions(&self) -> usize {
            1024
        }
        fn model_name(&self) -> &str {
            "amazon.titan-embed-text-v2:0"
        }
    }

    fn unpack_old_snapshot() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("mentedb-compat-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        tar::Archive::new(flate2::read::GzDecoder::new(OLD_SNAPSHOT))
            .unpack(&dir)
            .unwrap();
        dir
    }

    fn find(db: &MenteDb, i: usize) -> Option<MemoryNode> {
        let prefix = format!("Fixture learning number {i}:");
        db.memory_ids()
            .into_iter()
            .filter_map(|id| db.get_memory(id).ok())
            .find(|n| n.content.starts_with(&prefix))
    }

    fn recall(
        db: &MenteDb,
        embedding: &[f32],
        text: &str,
    ) -> Vec<(mentedb_core::types::MemoryId, f32)> {
        db.recall_hybrid_at(embedding, Some(text), 5, now_micros(), None, None, None)
            .unwrap()
    }

    fn now_micros() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }

    fn check_old_learnings(db: &MenteDb) {
        // The old engine stored the edited learning (6) by deleting it and
        // storing it again under the same id, which left its vector out of
        // the index. The engine heals that on open, so it is its own best
        // match like every other learning.
        let edited = find(db, 6).expect("the edited learning survives");
        let hits = recall(db, &edited.embedding, &edited.content);
        assert_eq!(hits[0].0, edited.id, "the edited learning is recalled");
        assert!(
            hits[0].1 >= MIN_RELEVANCE,
            "the edited learning clears the relevance floor, scored {}",
            hits[0].1
        );

        for i in (0..24).filter(|i| *i != 5 && *i != 6) {
            let node = find(db, i).unwrap_or_else(|| panic!("learning {i} lost"));
            assert_eq!(node.embedding, fixture_vector(i), "learning {i} embedding");
            assert!(
                node.tags.iter().any(|t| t == "source:run"),
                "learning {i} tags"
            );
            let hits = recall(db, &fixture_vector(i), &node.content);
            assert_eq!(hits[0].0, node.id, "learning {i} is its own best match");
            assert!(
                hits[0].1 >= MIN_RELEVANCE,
                "learning {i} clears the relevance floor, scored {}",
                hits[0].1
            );
        }
    }

    #[test]
    fn a_snapshot_from_the_previous_engine_opens_and_recalls() {
        let dir = unpack_old_snapshot();
        let db = open_db(&dir, Box::new(MustNotEmbed)).unwrap();
        assert!(db.calibration().is_none(), "calibration stays off");

        assert_eq!(db.memory_count(), 23, "24 stored, one deleted");
        assert!(find(&db, 5).is_none(), "the deleted learning stays deleted");
        let edited = find(&db, 6).expect("the edited learning survives");
        assert!(edited.content.ends_with("(edited)"));
        check_old_learnings(&db);

        // Pinned ones come from the tag index: 0, 7, 14, 21 and the edited 6.
        let pinned = pinned_memories(&db);
        assert_eq!(pinned.len(), 5);
        assert!(pinned.iter().any(|(id, _)| *id == edited.id.to_string()));
        assert!(pinned.iter().all(|(_, c)| [
            "number 0:",
            "number 6:",
            "number 7:",
            "number 14:",
            "number 21:"
        ]
        .iter()
        .any(|n| c.contains(n))));

        // A run on the new engine adds a learning and uploads; the next run
        // opens that snapshot and finds old and new learnings alike.
        let mut node = MemoryNode::new(
            AgentId::nil(),
            MemoryType::Procedural,
            "Run the service tests from the services directory.".into(),
            fixture_vector(500),
        );
        node.tags = vec!["topic:procedure".into(), "source:run".into()];
        let new_id = node.id;
        assert!(matches!(
            db.store_with_outcome(node).unwrap(),
            mentedb::StoreOutcome::Stored
        ));
        db.flush().unwrap();
        db.close().unwrap();
        drop(db);

        let db = open_db(&dir, Box::new(MustNotEmbed)).unwrap();
        assert_eq!(db.memory_count(), 24);
        check_old_learnings(&db);
        let hits = recall(
            &db,
            &fixture_vector(500),
            "Run the service tests from the services directory.",
        );
        assert_eq!(hits[0].0, new_id);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_verbatim_repeat_is_not_stored_twice_but_a_one_word_change_is() {
        let dir = unpack_old_snapshot();
        let db = open_db(&dir, Box::new(MustNotEmbed)).unwrap();
        let existing = find(&db, 3).unwrap();

        let repeat = MemoryNode::new(
            AgentId::nil(),
            existing.memory_type,
            existing.content.clone(),
            existing.embedding.clone(),
        );
        assert!(matches!(
            db.store_with_outcome(repeat).unwrap(),
            mentedb::StoreOutcome::Duplicate { of } if of == existing.id
        ));

        // One word swapped in a long learning keeps token overlap above the
        // engine's 0.85 gate; at the 0.97 cosine threshold it is still stored,
        // so a correction is never dropped as a duplicate.
        let mut near = existing.embedding.clone();
        near[1] = 0.3;
        let correction = MemoryNode::new(
            AgentId::nil(),
            existing.memory_type,
            existing.content.replace("handlers", "routes"),
            near,
        );
        assert!(matches!(
            db.store_with_outcome(correction).unwrap(),
            mentedb::StoreOutcome::Stored
        ));
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
