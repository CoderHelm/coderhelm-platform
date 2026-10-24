use aws_sdk_lambda::Client as LambdaClient;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};

use common::mcp_remote::runtime::Runtime;
use common::mcp_remote::store::{Owner, ServerRecord};
use common::mcp_remote::tools;

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
    pub team_id: String,
    /// The saved values, still sealed. Opened per call by [`build_env_vars`]
    /// and dropped when the call's payload is built; never logged.
    pub stored: common::mcp_credentials::Stored,
    /// Opens `stored` (decrypt only: the worker never seals).
    pub kms: aws_sdk_kms::Client,
    /// Custom prompt from user (optional)
    pub custom_prompt: Option<String>,
    /// Set for a remote server (Streamable HTTP, OAuth or a key): calls go
    /// to it directly instead of through the proxy Lambda.
    pub remote: Option<RemoteBinding>,
}

/// A remote MCP server, bound to the connection this run uses.
#[derive(Clone)]
pub struct RemoteBinding {
    pub rt: Runtime,
    pub server: ServerRecord,
    pub owner: Owner,
    /// Who is calling, for the audit trail (`planner, run 01J...`).
    pub actor: String,
    /// Only tools that read (the planner and the resolver).
    pub read_only: bool,
}

impl std::fmt::Debug for RemoteBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteBinding")
            .field("server", &self.server.id)
            .field("owner", &self.owner)
            .field("read_only", &self.read_only)
            .finish_non_exhaustive()
    }
}

/// What a run may use remote servers for.
pub struct RemoteScope<'a> {
    pub table: &'a str,
    pub key_id: &'a str,
    /// The person the run is for (their email), when known: their own
    /// connections to personal servers are used.
    pub user: Option<&'a str>,
    pub actor: String,
    pub read_only: bool,
}

/// The team's usable remote servers as plugins.
pub async fn load_remote_plugins(
    dynamo: &aws_sdk_dynamodb::Client,
    kms: &aws_sdk_kms::Client,
    team_id: &str,
    scope: &RemoteScope<'_>,
) -> Vec<McpPlugin> {
    let rt = Runtime::aws(
        dynamo.clone(),
        scope.table,
        kms.clone(),
        scope.key_id,
        team_id,
    );
    let mut out = Vec::new();
    for (server, owner) in rt.usable(scope.user).await {
        if tools::agent_tools(&server, scope.read_only).is_empty() {
            continue;
        }
        out.push(McpPlugin {
            server_id: server.id.clone(),
            npx_package: String::new(),
            env_mapping: Vec::new(),
            team_id: team_id.to_string(),
            stored: common::mcp_credentials::Stored::None,
            kms: kms.clone(),
            custom_prompt: Some(server.custom_prompt.clone()).filter(|p| !p.trim().is_empty()),
            remote: Some(RemoteBinding {
                rt: rt.clone(),
                server,
                owner,
                actor: scope.actor.clone(),
                read_only: scope.read_only,
            }),
        });
    }
    info!(count = out.len(), "Loaded remote MCP servers for team");
    out
}

/// The catalog servers run locally plus the remote ones; a remote
/// connection to a catalog server takes the place of its key.
pub fn merge_plugins(local: Vec<McpPlugin>, remote: Vec<McpPlugin>) -> Vec<McpPlugin> {
    let mut out: Vec<McpPlugin> = local
        .into_iter()
        .filter(|p| !remote.iter().any(|r| r.server_id == p.server_id))
        .collect();
    out.extend(remote);
    out
}

/// A remote server's tools, as schemas (unprefixed names).
fn remote_schemas(b: &RemoteBinding) -> Vec<McpToolSchema> {
    tools::agent_tools(&b.server, b.read_only)
        .into_iter()
        .filter_map(|(name, description, input_schema)| {
            let (_, tool) = name.split_once("__")?;
            Some(McpToolSchema {
                name: tool.to_string(),
                description,
                input_schema,
            })
        })
        .collect()
}

/// A plugin's tool schemas: a remote server's allowed tools, or a catalog
/// server's cached list (listing it through the proxy when not cached).
pub async fn schemas_for(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    lambda: &LambdaClient,
    function_name: &str,
    plugin: &McpPlugin,
) -> Option<Vec<McpToolSchema>> {
    if let Some(b) = &plugin.remote {
        return Some(remote_schemas(b));
    }
    if let Some(cache) = load_tool_cache(s3, bucket, &plugin.server_id).await {
        return Some(cache.tools);
    }
    if function_name.is_empty() {
        return None;
    }
    match list_tools(lambda, function_name, plugin).await {
        Ok(schemas) => Some(schemas),
        Err(e) => {
            warn!(server_id = %plugin.server_id, error = %e, "Failed to list MCP tools, skipping");
            None
        }
    }
}

/// Server ids whose tools were already filtered by what they do (remote
/// servers loaded for a read-only agent): the name-prefix rule doesn't apply.
pub fn hint_filtered(plugins: &[McpPlugin]) -> std::collections::HashSet<String> {
    plugins
        .iter()
        .filter(|p| p.remote.as_ref().is_some_and(|r| r.read_only))
        .map(|p| p.server_id.clone())
        .collect()
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

/// Convert MCP tool schemas to ToolDefinitions, prefixed with the server id
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
    if let Some(b) = &plugin.remote {
        return Ok(remote_schemas(b));
    }
    let env_vars = build_env_vars(plugin).await?;

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
    if let Some(b) = &plugin.remote {
        let out =
            b.rt.call_tool(&b.server, &b.owner, tool_name, tool_input, &b.actor)
                .await?;
        if out.is_error {
            return Err(format!("MCP tool returned error: {}", out.text).into());
        }
        return Ok(json!(out.text));
    }
    let env_vars = build_env_vars(plugin).await?;

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

/// Build env var map from plugin credentials + env_mapping. The values are
/// opened here, for this one call, and the opened map is dropped on return.
async fn build_env_vars(
    plugin: &McpPlugin,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let kms = common::mcp_credentials::Kms {
        client: &plugin.kms,
        key_id: "",
    };
    let credentials =
        common::mcp_credentials::load(&kms, &plugin.team_id, &plugin.server_id, &plugin.stored)
            .await
            .map_err(|e| format!("could not open {} credentials: {e}", plugin.server_id))?;
    let mut env = serde_json::Map::new();
    for (cred_key, env_var) in &plugin.env_mapping {
        if let Some(value) = credentials.get(cred_key) {
            env.insert(env_var.clone(), Value::String(value.clone()));
        }
    }
    Ok(Value::Object(env))
}

/// Load enabled MCP plugins with credentials for a team from DynamoDB.

/// Returns plugins that are enabled AND have credentials stored.
pub async fn load_team_plugins(
    dynamo: &aws_sdk_dynamodb::Client,
    kms: &aws_sdk_kms::Client,
    settings_table: &str,
    team_id: &str,
) -> Vec<McpPlugin> {
    let result = dynamo
        .query()
        .table_name(settings_table)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(
            ":pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
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
            warn!(error = %e, "Failed to load team plugins");
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
        let (npx_package, env_mapping) = match common::mcp_catalog::find(server_id) {
            // A remote-only server (no npx package) is reached through the
            // remote MCP path, never the local proxy.
            Some(e) if e.npx_package.is_empty() => continue,
            Some(e) => (e.npx_package, e.env_mapping),
            None => {
                warn!(server_id, "Plugin not found in catalog, skipping");
                continue;
            }
        };

        // Kept sealed until a call needs them. A record saved before sealing
        // still works; the gateway seals it (the worker cannot write here).
        let stored = common::mcp_credentials::stored(item);
        if matches!(stored, common::mcp_credentials::Stored::Plain(_)) {
            warn!(server_id, "Plugin credentials not sealed yet");
        }

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
            team_id: team_id.to_string(),
            stored,
            kms: kms.clone(),
            custom_prompt,
            remote: None,
        });
    }

    info!(count = plugins.len(), "Loaded MCP plugins for team");
    plugins
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local(id: &str) -> McpPlugin {
        let conf = aws_sdk_kms::Config::builder()
            .behavior_version(aws_sdk_kms::config::BehaviorVersion::latest())
            .region(aws_sdk_kms::config::Region::new("us-east-1"))
            .build();
        McpPlugin {
            server_id: id.into(),
            npx_package: "pkg".into(),
            env_mapping: vec![],
            team_id: "TEAM#a".into(),
            stored: common::mcp_credentials::Stored::None,
            kms: aws_sdk_kms::Client::from_conf(conf),
            custom_prompt: None,
            remote: None,
        }
    }

    #[test]
    fn a_remote_connection_replaces_the_same_servers_key() {
        let mut remote = local("linear");
        remote.npx_package.clear();
        let merged = merge_plugins(vec![local("linear"), local("sentry")], vec![remote]);
        let ids: Vec<_> = merged
            .iter()
            .map(|p| (p.server_id.as_str(), p.npx_package.as_str()))
            .collect();
        assert_eq!(ids, vec![("sentry", "pkg"), ("linear", "")]);
    }
}
