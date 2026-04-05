use mentedb_core::error::{MenteError, MenteResult};
use mentedb_embedding::EmbeddingProvider;
use tracing::{debug, warn};

/// Embedding provider that uses Amazon Bedrock Titan Embeddings v2.
pub struct BedrockEmbedder {
    client: aws_sdk_bedrockruntime::Client,
    model_id: String,
    dimensions: usize,
}

impl BedrockEmbedder {
    pub fn new(client: aws_sdk_bedrockruntime::Client) -> Self {
        Self {
            client,
            model_id: "amazon.titan-embed-text-v2:0".to_string(),
            dimensions: 1024,
        }
    }
}

impl EmbeddingProvider for BedrockEmbedder {
    fn embed(&self, text: &str) -> MenteResult<Vec<f32>> {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async { embed_with_bedrock(&self.client, &self.model_id, text).await })
    }

    fn embed_batch(&self, texts: &[&str]) -> MenteResult<Vec<Vec<f32>>> {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async {
            let mut results = Vec::with_capacity(texts.len());
            for text in texts {
                results.push(embed_with_bedrock(&self.client, &self.model_id, text).await?);
            }
            Ok(results)
        })
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn model_name(&self) -> &str {
        &self.model_id
    }
}

async fn embed_with_bedrock(
    client: &aws_sdk_bedrockruntime::Client,
    model_id: &str,
    text: &str,
) -> MenteResult<Vec<f32>> {
    let payload = serde_json::json!({
        "inputText": text,
        "dimensions": 1024,
        "normalize": true,
    });

    let response = client
        .invoke_model()
        .model_id(model_id)
        .body(aws_smithy_types::Blob::new(
            serde_json::to_vec(&payload).unwrap(),
        ))
        .content_type("application/json")
        .send()
        .await
        .map_err(|e| MenteError::Storage(format!("Bedrock embedding error: {e}")))?;

    let body: serde_json::Value =
        serde_json::from_slice(response.body().as_ref())
            .map_err(|e| MenteError::Serialization(format!("Failed to parse Bedrock response: {e}")))?;

    let embedding: Vec<f32> = body
        .get("embedding")
        .and_then(|v| v.as_array())
        .ok_or_else(|| MenteError::Storage("Missing embedding in Bedrock response".into()))?
        .iter()
        .filter_map(|v: &serde_json::Value| v.as_f64().map(|f| f as f32))
        .collect();

    if embedding.len() != 1024 {
        warn!(
            got = embedding.len(),
            "Unexpected embedding dimensions from Titan"
        );
    }

    debug!(dims = embedding.len(), "Generated Bedrock embedding");
    Ok(embedding)
}
