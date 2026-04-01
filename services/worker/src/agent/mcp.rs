use aws_sdk_lambda::Client as LambdaClient;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};

use super::llm::ToolDefinition;

/// Cached MCP tool schema loaded from S3.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct McpToolCache {
    pub server_id: String,
    pub tools: Vec<McpToolSchema>,
    pub cached_at: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpToolSchema {
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema", alias = "input_schema")]
    pub input_schema: Value,
}

/// Plugin info needed to invoke MCP servers.
#[derive(Debug, Clone)]
pub struct McpPlugin {
    pub server_id: String,
    pub npx_package: String,
    /// Credential key → env var name
    pub env_mapping: Vec<(String, String)>,
    /// Decrypted credential values (key → value)
    pub credentials: std::collections::HashMap<String, String>,
    /// Custom prompt from user (optional)
    pub custom_prompt: Option<String>,
}

/// Result of invoking an MCP tool via the proxy Lambda.
#[derive(Debug, Deserialize)]
pub struct McpToolResult {
    pub content: Vec<McpContentItem>,
    #[serde(rename = "isError", default)]
    pub is_error: bool,
}

#[derive(Debug, Deserialize)]
pub struct McpContentItem {
    #[serde(default)]
    #[allow(dead_code)]
    pub r#type: String,
    #[serde(default)]
    pub text: String,
}

/// Load cached MCP tool schemas from S3 for a given server.
pub async fn load_tool_cache(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    server_id: &str,
) -> Option<McpToolCache> {
    let key = format!("config/mcp-tools/{server_id}.json");
    let resp = s3.get_object().bucket(bucket).key(&key).send().await.ok()?;

    let bytes = resp.body.collect().await.ok()?.into_bytes();
    serde_json::from_slice(&bytes).ok()
}

/// Convert MCP tool schemas to Bedrock ToolDefinitions, prefixed with the server id
/// to avoid name collisions (e.g. `sentry__list_issues`).
pub fn to_tool_definitions(server_id: &str, tools: &[McpToolSchema]) -> Vec<ToolDefinition> {
    tools
        .iter()
        .map(|t| ToolDefinition {
            name: format!("{}__{}", server_id, t.name),
            description: t.description.clone(),
            input_schema: t.input_schema.clone(),
        })
        .collect()
}

/// Invoke the MCP proxy Lambda to list tools for a server.
/// This also caches the result in S3.
pub async fn list_tools(
    lambda: &LambdaClient,
    function_name: &str,
    plugin: &McpPlugin,
) -> Result<Vec<McpToolSchema>, Box<dyn std::error::Error + Send + Sync>> {
    let env_vars = build_env_vars(plugin);

    let payload = json!({
        "action": "list_tools",
        "server_id": plugin.server_id,
        "npx_package": plugin.npx_package,
        "env_vars": env_vars,
    });

    let resp = lambda
        .invoke()
        .function_name(function_name)
        .payload(aws_smithy_types::Blob::new(serde_json::to_vec(&payload)?))
        .send()
        .await?;

    let resp_payload = resp.payload().ok_or("No payload from MCP proxy")?.as_ref();

    let result: Value = serde_json::from_slice(resp_payload)?;

    if let Some(err) = result.get("error").and_then(|e| e.as_str()) {
        return Err(format!("MCP proxy error: {err}").into());
    }

    let tools: Vec<McpToolSchema> =
        serde_json::from_value(result.get("tools").cloned().unwrap_or(Value::Array(vec![])))?;

    info!(server_id = %plugin.server_id, tool_count = tools.len(), "Listed MCP tools");
    Ok(tools)
}

/// Invoke the MCP proxy Lambda to call a specific tool.
/// The `tool_name` should be the original (unprefixed) name.
pub async fn call_tool(
    lambda: &LambdaClient,
    function_name: &str,
    plugin: &McpPlugin,
    tool_name: &str,
    tool_input: &Value,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let env_vars = build_env_vars(plugin);

    let payload = json!({
        "action": "call_tool",
        "server_id": plugin.server_id,
        "npx_package": plugin.npx_package,
        "env_vars": env_vars,
        "tool_name": tool_name,
        "tool_input": tool_input,
    });

    let resp = lambda
        .invoke()
        .function_name(function_name)
        .payload(aws_smithy_types::Blob::new(serde_json::to_vec(&payload)?))
        .send()
        .await?;

    let resp_payload = resp.payload().ok_or("No payload from MCP proxy")?.as_ref();

    let result: Value = serde_json::from_slice(resp_payload)?;

    if let Some(err) = result.get("error").and_then(|e| e.as_str()) {
        return Err(format!("MCP tool error: {err}").into());
    }

    // Extract text content from the MCP result
    let mcp_result: McpToolResult = serde_json::from_value(
        result
            .get("result")
            .cloned()
            .unwrap_or(json!({"content": [], "isError": true})),
    )?;

    if mcp_result.is_error {
        let error_text = mcp_result
            .content
            .iter()
            .map(|c| c.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        return Err(format!("MCP tool returned error: {error_text}").into());
    }

    // Combine all text content blocks into a single JSON value
    let text = mcp_result
        .content
        .iter()
        .map(|c| c.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    Ok(json!(text))
}

/// Build env var map from plugin credentials + env_mapping.
fn build_env_vars(plugin: &McpPlugin) -> Value {
    let mut env = serde_json::Map::new();
    for (cred_key, env_var) in &plugin.env_mapping {
        if let Some(value) = plugin.credentials.get(cred_key) {
            env.insert(env_var.clone(), Value::String(value.clone()));
        }
    }
    Value::Object(env)
}

/// Load enabled MCP plugins with credentials for a tenant from DynamoDB.
/// Type alias for the MCP catalog: (server_id, description, tools)
pub type McpCatalog<'a> = &'a [(&'a str, &'a str, &'a [(&'a str, &'a str)])];

/// Returns plugins that are enabled AND have credentials stored.
pub async fn load_tenant_plugins(
    dynamo: &aws_sdk_dynamodb::Client,
    settings_table: &str,
    tenant_id: &str,
    catalog: McpCatalog<'_>,
) -> Vec<McpPlugin> {
    let result = dynamo
        .query()
        .table_name(settings_table)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(
            ":pk",
            aws_sdk_dynamodb::types::AttributeValue::S(tenant_id.to_string()),
        )
        .expression_attribute_values(
            ":prefix",
            aws_sdk_dynamodb::types::AttributeValue::S("PLUGIN#".to_string()),
        )
        .send()
        .await;

    let items = match result {
        Ok(output) => output.items().to_vec(),
        Err(e) => {
            warn!(error = %e, "Failed to load tenant plugins");
            return Vec::new();
        }
    };

    let mut plugins = Vec::new();
    for item in &items {
        let enabled = item
            .get("enabled")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false);
        let has_creds = item
            .get("has_credentials")
            .and_then(|v| v.as_bool().ok())
            .copied()
            .unwrap_or(false);

        if !enabled || !has_creds {
            continue;
        }

        let sk = match item.get("sk").and_then(|v| v.as_s().ok()) {
            Some(s) => s,
            None => continue,
        };
        let server_id = match sk.strip_prefix("PLUGIN#") {
            Some(id) => id,
            None => continue,
        };

        // Find this plugin in the catalog
        let catalog_entry = catalog.iter().find(|(id, _, _)| *id == server_id);
        let (_, npx_package, env_mapping) = match catalog_entry {
            Some(e) => e,
            None => {
                warn!(server_id, "Plugin not found in catalog, skipping");
                continue;
            }
        };

        // Parse stored credentials JSON
        let credentials: std::collections::HashMap<String, String> = item
            .get("credentials")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        let custom_prompt = item
            .get("custom_prompt")
            .and_then(|v| v.as_s().ok())
            .map(|s| s.to_string());

        plugins.push(McpPlugin {
            server_id: server_id.to_string(),
            npx_package: npx_package.to_string(),
            env_mapping: env_mapping
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            credentials,
            custom_prompt,
        });
    }

    info!(count = plugins.len(), "Loaded MCP plugins for tenant");
    plugins
}
