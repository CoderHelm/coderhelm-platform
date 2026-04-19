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
pub struct AgentMemory {
    db: MenteDb,
    bedrock: aws_sdk_bedrockruntime::Client,
    local_dir: PathBuf,
    team_id: String,
    repo_owner: String,
    repo_name: String,
    has_lock: bool,
    dirty: bool,
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

        // Acquire lock
        let has_lock = match lock::acquire_lock(
            &state.dynamo,
            &state.config.table_name,
            team_id,
            repo_owner,
            repo_name,
        )
        .await
        {
            Ok(locked) => locked,
            Err(e) => {
                warn!(error = %e, "Failed to acquire memory lock, running stateless");
                false
            }
        };

        if !has_lock {
            return None;
        }

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
                )
                .await;
                return None;
            }
        };

        // Open MenteDB with Bedrock embedder
        let bedrock_embedder = embedder::BedrockEmbedder::new(state.bedrock.clone());
        match MenteDb::open_with_embedder(&local_dir, Box::new(bedrock_embedder)) {
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
                    has_lock,
                    dirty: false,
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

        let results = match self.db.recall_similar(&embedding, k) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Failed to recall memories");
                return String::new();
            }
        };

        if results.is_empty() {
            return String::new();
        }

        let mut context = String::from(
            "## Agent Memory\nRelevant learnings from past runs on this repository:\n\n",
        );
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
                context.push_str(&format!(
                    "- **{type_label}** (relevance: {score:.2}): {}\n",
                    node.content
                ));
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

        if let Err(e) = self.db.store(node) {
            warn!(error = %e, "Failed to store memory");
        } else {
            self.dirty = true;
        }
    }

    /// Store a review finding.
    pub async fn store_review_finding(&mut self, finding: &str) {
        self.store_learning(
            finding,
            MemoryType::Semantic,
            vec![
                "review".to_string(),
                format!("repo:{}/{}", self.repo_owner, self.repo_name),
            ],
        )
        .await;
    }

    /// Store a security finding.
    pub async fn store_security_finding(&mut self, finding: &str) {
        self.store_learning(
            finding,
            MemoryType::Semantic,
            vec![
                "security".to_string(),
                format!("repo:{}/{}", self.repo_owner, self.repo_name),
            ],
        )
        .await;
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

    /// Flush, close, upload to S3, and release the lock.
    pub async fn close_and_upload(
        mut self,
        state: &WorkerState,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let memory_count = self.db.memory_count();

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

        // Release lock
        if self.has_lock {
            lock::release_lock(
                &state.dynamo,
                &state.config.table_name,
                &self.team_id,
                &self.repo_owner,
                &self.repo_name,
            )
            .await;
        }

        // Clean up local files
        s3_sync::cleanup_local(&self.local_dir);

        Ok(())
    }
}
