use common::usage::UsageMeter;
use mentedb_core::error::{MenteError, MenteResult};
use mentedb_embedding::EmbeddingProvider;
use tracing::{debug, warn};

/// Embedding provider that uses Amazon Bedrock Titan Embeddings v2. Every
/// call is metered (`common::embed`, billed to CoderHelm).
pub struct BedrockEmbedder {
    client: aws_sdk_bedrockruntime::Client,
    meter: UsageMeter,
}

impl BedrockEmbedder {
    pub fn new(client: aws_sdk_bedrockruntime::Client, meter: UsageMeter) -> Self {
        Self { client, meter }
    }
}

impl EmbeddingProvider for BedrockEmbedder {
    fn embed(&self, text: &str) -> MenteResult<Vec<f32>> {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async { embed_async(&self.client, &self.meter, text).await })
    }

    fn embed_batch(&self, texts: &[&str]) -> MenteResult<Vec<Vec<f32>>> {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async {
            let mut results = Vec::with_capacity(texts.len());
            for text in texts {
                results.push(embed_async(&self.client, &self.meter, text).await?);
            }
            Ok(results)
        })
    }

    fn dimensions(&self) -> usize {
        common::embed::DIMENSIONS
    }

    fn model_name(&self) -> &str {
        common::embed::TITAN_V2
    }
}

/// Async embedding function for use from async contexts (avoids block_on panic).
pub async fn embed_async(
    client: &aws_sdk_bedrockruntime::Client,
    meter: &UsageMeter,
    text: &str,
) -> MenteResult<Vec<f32>> {
    let embedding = common::embed::titan_embed(client, meter, text)
        .await
        .map_err(MenteError::Storage)?;
    if embedding.len() != common::embed::DIMENSIONS {
        warn!(
            got = embedding.len(),
            "Unexpected embedding dimensions from Titan"
        );
    }
    debug!(dims = embedding.len(), "Generated Bedrock embedding");
    Ok(embedding)
}
