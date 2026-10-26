use axum::{extract::State, http::StatusCode, Extension, Json};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info};

use crate::models::Claims;
use crate::AppState;
use aws_sdk_dynamodb::types::AttributeValue;
use common::mcp_credentials::{self, Credentials, Kms};

fn attr_s(val: &str) -> AttributeValue {
    AttributeValue::S(val.to_string())
}

fn is_admin_or_owner(role: &str) -> bool {
    matches!(role, "admin" | "owner")
}

use common::mcp_catalog::CATALOG;

// ── Handlers ────────────────────────────────────────────────────────

/// Sync the plugin catalog JSON to S3 so other Lambdas can load it.
/// Only writes if the content has changed (compares MD5 with existing ETag).
pub async fn sync_catalog_to_s3(state: &AppState) {
    let catalog_json = match serde_json::to_vec(&json!({ "plugins": CATALOG })) {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to serialize plugin catalog: {e}");
            return;
        }
    };

    let bucket = &state.config.bucket_name;
    let key = "config/mcp-catalog.json";

    // Check if the existing object matches — skip write if unchanged
    let local_md5 = format!("{:x}", md5::compute(&catalog_json));
    if let Ok(head) = state.s3.head_object().bucket(bucket).key(key).send().await {
        if let Some(etag) = head.e_tag() {
            let etag_clean = etag.trim_matches('"');
            if etag_clean == local_md5 {
                info!("Plugin catalog unchanged, skipping S3 sync");
                return;
            }
        }
    }

    if let Err(e) = state
        .s3
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(catalog_json.into())
        .content_type("application/json")
        .send()
        .await
    {
        error!("Failed to sync plugin catalog to S3: {e}");
    } else {
        info!("Plugin catalog synced to s3://{bucket}/{key}");
    }
}

/// Return the MCP configs table name, falling back to settings table for migration compat.
pub(crate) fn mcp_table(state: &AppState) -> &str {
    let t = &state.config.mcp_configs_table_name;
    if t.is_empty() {
        &state.config.settings_table_name
    } else {
        t
    }
}

/// The catalog as the Integrations page shows it.
pub(crate) fn catalog_json() -> Vec<Value> {
    CATALOG
        .iter()
        .filter_map(|p| serde_json::to_value(p).ok())
        .collect()
}

fn cipher(state: &AppState) -> Kms<'_> {
    Kms {
        client: &state.kms,
        key_id: &state.config.secrets_key_id,
    }
}

/// The saved values for one team's MCP server, opened for this call only.
/// A record still holding plain values (saved before sealing) is sealed in
/// place on the way through. Never logs a value.
pub(crate) async fn load_credentials(
    state: &AppState,
    team_id: &str,
    plugin_id: &str,
) -> Result<Credentials, String> {
    let item = state
        .dynamo
        .get_item()
        .table_name(mcp_table(state))
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&format!("PLUGIN#{plugin_id}")))
        .send()
        .await
        .map_err(|e| format!("Failed to load plugin credentials: {e}"))?
        .item()
        .cloned()
        .unwrap_or_default();
    let stored = mcp_credentials::stored(&item);
    let kms = cipher(state);
    if matches!(stored, mcp_credentials::Stored::Plain(_)) {
        match mcp_credentials::migrate_item(&state.dynamo, &kms, mcp_table(state), &item).await {
            Ok(sealed) => {
                info!(team = %team_id, plugin = %plugin_id, sealed, "Sealed plain plugin credentials on read")
            }
            Err(e) => {
                error!(team = %team_id, plugin = %plugin_id, "Could not seal plain plugin credentials: {e}")
            }
        }
    }
    mcp_credentials::load(&kms, team_id, plugin_id, &stored).await
}

/// Why saved credentials weren't stored.
#[derive(Debug)]
pub(crate) enum StoreError {
    TooLong,
    Failed(String),
}

/// Turns a catalog plugin on for a team when it isn't yet (what the
/// Integrations page's Enable does), keeping a record that is already there.
pub(crate) async fn enable_if_missing(
    state: &AppState,
    team_id: &str,
    plugin_id: &str,
) -> Result<(), String> {
    let now = chrono::Utc::now().to_rfc3339();
    match state
        .dynamo
        .put_item()
        .table_name(mcp_table(state))
        .item("pk", attr_s(team_id))
        .item("sk", attr_s(&format!("PLUGIN#{plugin_id}")))
        .item("enabled", AttributeValue::Bool(true))
        .item("has_credentials", AttributeValue::Bool(false))
        .item("enabled_at", attr_s(&now))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(e)
            if e.as_service_error()
                .is_some_and(|s| s.is_conditional_check_failed_exception()) =>
        {
            Ok(())
        }
        Err(e) => Err(format!("enable plugin: {e}")),
    }
}

/// Seals `creds` for one team and plugin and stores them on its record (which
/// must exist). The one way credentials are saved: the Integrations page and
/// the Knowledge add flow both come here. Never logs a value.
pub(crate) async fn store_credentials(
    state: &AppState,
    team_id: &str,
    plugin_id: &str,
    creds: &Credentials,
) -> Result<(), StoreError> {
    let sealed = mcp_credentials::seal(&cipher(state), team_id, plugin_id, creds)
        .await
        .map_err(|e| {
            if e.contains("too long") {
                StoreError::TooLong
            } else {
                StoreError::Failed(e)
            }
        })?;
    let now = chrono::Utc::now().to_rfc3339();
    state
        .dynamo
        .update_item()
        .table_name(mcp_table(state))
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&format!("PLUGIN#{plugin_id}")))
        .update_expression(
            "SET #sealed = :creds, has_credentials = :hc, credentials_updated_at = :ts REMOVE #plain",
        )
        .expression_attribute_names("#sealed", mcp_credentials::SEALED_ATTR)
        .expression_attribute_names("#plain", mcp_credentials::PLAIN_ATTR)
        .expression_attribute_values(":creds", attr_s(&sealed))
        .expression_attribute_values(":hc", AttributeValue::Bool(true))
        .expression_attribute_values(":ts", attr_s(&now))
        .condition_expression("attribute_exists(pk)")
        .send()
        .await
        .map_err(|e| StoreError::Failed(format!("update credentials: {e}")))?;
    Ok(())
}

/// GET /api/plugins/catalog — return all available plugins.
pub async fn list_catalog() -> Json<Value> {
    Json(json!({ "plugins": CATALOG }))
}

/// What the dashboard sees of one plugin record. Built field by field, so a
/// saved credential value (sealed or plain) can never reach a response.
fn plugin_summary(item: &std::collections::HashMap<String, AttributeValue>) -> Option<Value> {
    let sk = item.get("sk")?.as_s().ok()?;
    let plugin_id = sk.strip_prefix("PLUGIN#")?;
    let enabled = item
        .get("enabled")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false);
    let has_credentials = item
        .get("has_credentials")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false);
    let enabled_at = item
        .get("enabled_at")
        .and_then(|v| v.as_s().ok())
        .map(|s| s.to_string());
    Some(json!({
        "plugin_id": plugin_id,
        "enabled": enabled,
        "has_credentials": has_credentials,
        "enabled_at": enabled_at,
        "custom_prompt": item.get("custom_prompt").and_then(|v| v.as_s().ok()).map(|s| s.to_string()),
    }))
}

/// GET /api/plugins — list plugins enabled for this team.
pub async fn list_enabled(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .query()
        .table_name(mcp_table(&state))
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.team_id))
        .expression_attribute_values(":prefix", attr_s("PLUGIN#"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to list plugins: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let plugins: Vec<Value> = result.items().iter().filter_map(plugin_summary).collect();

    Ok(Json(json!({ "plugins": plugins })))
}

/// POST /api/plugins/:id/enable — enable a plugin.
pub async fn enable_plugin(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plugin_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    if !is_admin_or_owner(&claims.role) {
        return Err(StatusCode::FORBIDDEN);
    }

    // Validate plugin exists in catalog, and runs locally with a key (a
    // remote-only entry connects through `mcp_servers` instead).
    match CATALOG.iter().find(|p| p.id == plugin_id) {
        None => return Err(StatusCode::NOT_FOUND),
        Some(p) if p.npx_package.is_empty() => return Err(StatusCode::BAD_REQUEST),
        Some(_) => {}
    }

    let now = chrono::Utc::now().to_rfc3339();

    state
        .dynamo
        .put_item()
        .table_name(mcp_table(&state))
        .item("pk", attr_s(&claims.team_id))
        .item("sk", attr_s(&format!("PLUGIN#{plugin_id}")))
        .item("enabled", AttributeValue::Bool(true))
        .item("has_credentials", AttributeValue::Bool(false))
        .item("enabled_at", attr_s(&now))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to enable plugin {plugin_id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(team = %claims.team_id, plugin = %plugin_id, "Plugin enabled");
    Ok(Json(json!({ "status": "enabled", "plugin_id": plugin_id })))
}

/// DELETE /api/plugins/:id — disable a plugin and remove credentials.
pub async fn disable_plugin(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plugin_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    if !is_admin_or_owner(&claims.role) {
        return Err(StatusCode::FORBIDDEN);
    }

    state
        .dynamo
        .delete_item()
        .table_name(mcp_table(&state))
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("PLUGIN#{plugin_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to disable plugin {plugin_id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(team = %claims.team_id, plugin = %plugin_id, "Plugin disabled");
    Ok(Json(
        json!({ "status": "disabled", "plugin_id": plugin_id }),
    ))
}

/// PUT /api/plugins/:id/credentials — save credentials for a plugin.
pub async fn update_credentials(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plugin_id): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    if !is_admin_or_owner(&claims.role) {
        return Err(StatusCode::FORBIDDEN);
    }

    // Validate plugin exists
    let plugin_def = CATALOG
        .iter()
        .find(|p| p.id == plugin_id)
        .ok_or(StatusCode::NOT_FOUND)?;

    // Validate all required fields are present
    let credentials = body
        .get("credentials")
        .and_then(|v| v.as_object())
        .ok_or(StatusCode::BAD_REQUEST)?;

    for field in plugin_def.credential_fields {
        let val = credentials
            .get(field.key)
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if val.is_empty() {
            return Ok(Json(
                json!({ "error": format!("Missing required field: {}", field.label) }),
            ));
        }
    }

    // Only the catalog's fields, as text; sealed for this team and server.
    let creds: Credentials = plugin_def
        .credential_fields
        .iter()
        .filter_map(|f| {
            credentials
                .get(f.key)
                .and_then(|v| v.as_str())
                .map(|v| (f.key.to_string(), v.to_string()))
        })
        .collect();
    match store_credentials(&state, &claims.team_id, &plugin_id, &creds).await {
        Ok(()) => {}
        Err(StoreError::TooLong) => {
            return Ok(Json(json!({ "error": "Credentials are too long" })));
        }
        Err(StoreError::Failed(e)) => {
            error!(plugin = %plugin_id, "Failed to save credentials: {e}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    info!(team = %claims.team_id, plugin = %plugin_id, "Plugin credentials updated");
    Ok(Json(json!({ "status": "saved" })))
}

/// PUT /api/plugins/:id/prompt — save a custom prompt for a plugin.
pub async fn update_prompt(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plugin_id): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    if !is_admin_or_owner(&claims.role) {
        return Err(StatusCode::FORBIDDEN);
    }

    if !CATALOG.iter().any(|p| p.id == plugin_id) {
        return Err(StatusCode::NOT_FOUND);
    }

    let custom_prompt = body
        .get("custom_prompt")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if custom_prompt.len() > 4000 {
        return Ok(Json(
            json!({ "error": "Prompt too long (max 4000 characters)" }),
        ));
    }

    state
        .dynamo
        .update_item()
        .table_name(mcp_table(&state))
        .key("pk", attr_s(&claims.team_id))
        .key("sk", attr_s(&format!("PLUGIN#{plugin_id}")))
        .update_expression("SET custom_prompt = :cp")
        .expression_attribute_values(":cp", attr_s(&custom_prompt))
        .condition_expression("attribute_exists(pk)")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update prompt for {plugin_id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(team = %claims.team_id, plugin = %plugin_id, "Plugin custom prompt updated");
    Ok(Json(json!({ "status": "saved" })))
}

/// POST /api/plugins/:id/test — test connection to an MCP server.
pub async fn test_connection(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    axum::extract::Path(plugin_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?; // admin+
    let plugin_def = CATALOG
        .iter()
        .find(|p| p.id == plugin_id && !p.npx_package.is_empty())
        .ok_or(StatusCode::NOT_FOUND)?;

    // Open the saved values for this call only.
    let creds = load_credentials(&state, &claims.team_id, &plugin_id)
        .await
        .map_err(|e| {
            error!(plugin = %plugin_id, "Failed to load plugin credentials for test: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Map credentials to env vars
    let mut env_vars = serde_json::Map::new();
    for (cred_key, env_var) in plugin_def.env_mapping {
        if let Some(val) = creds.get(*cred_key) {
            env_vars.insert(env_var.to_string(), json!(val));
        }
    }
    drop(creds);

    // Invoke MCP proxy Lambda with list_tools action
    let payload = json!({
        "action": "list_tools",
        "server_id": plugin_def.id,
        "npx_package": plugin_def.npx_package,
        "env_vars": env_vars,
    });

    let resp = state
        .lambda
        .invoke()
        .function_name(&state.config.mcp_proxy_function_name)
        .payload(aws_sdk_lambda::primitives::Blob::new(
            serde_json::to_vec(&payload).unwrap_or_default(),
        ))
        .send()
        .await
        .map_err(|e| {
            error!("Lambda invoke failed for plugin test {plugin_id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let response_payload = resp
        .payload()
        .map(|p| p.as_ref().to_vec())
        .unwrap_or_default();

    let response: serde_json::Value =
        serde_json::from_slice(&response_payload).unwrap_or(json!({}));

    if let Some(err) = response.get("error").and_then(|v| v.as_str()) {
        info!(team = %claims.team_id, plugin = %plugin_id, "Plugin test failed: {err}");
        record_test(&state, &claims.team_id, &plugin_id, false, err).await;
        return Ok(Json(json!({ "status": "error", "message": err })));
    }

    let tool_count = response
        .get("tools")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);

    info!(team = %claims.team_id, plugin = %plugin_id, tools = tool_count, "Plugin test succeeded");
    let note = format!(
        "{tool_count} tool{} available",
        if tool_count == 1 { "" } else { "s" }
    );
    record_test(&state, &claims.team_id, &plugin_id, true, &note).await;
    Ok(Json(json!({ "status": "ok", "tool_count": tool_count })))
}

/// Keeps the last test's result on the plugin's record, for the
/// Integrations page's "last check". Only while the plugin is switched on.
async fn record_test(state: &AppState, team_id: &str, plugin_id: &str, ok: bool, message: &str) {
    let _ = state
        .dynamo
        .update_item()
        .table_name(mcp_table(state))
        .key("pk", attr_s(team_id))
        .key("sk", attr_s(&format!("PLUGIN#{plugin_id}")))
        .update_expression("SET last_test_at = :t, last_test_ok = :ok, last_test_message = :m")
        .expression_attribute_values(":t", attr_s(&chrono::Utc::now().to_rfc3339()))
        .expression_attribute_values(":ok", AttributeValue::Bool(ok))
        .expression_attribute_values(":m", attr_s(common::truncate_str(message, 300)))
        .condition_expression("attribute_exists(pk)")
        .send()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plugin_summary_never_carries_credentials() {
        let item = std::collections::HashMap::from([
            ("pk".to_string(), attr_s("TEAM#a")),
            ("sk".to_string(), attr_s("PLUGIN#figma")),
            ("enabled".to_string(), AttributeValue::Bool(true)),
            ("has_credentials".to_string(), AttributeValue::Bool(true)),
            (
                "credentials".to_string(),
                attr_s(r#"{"api_token":"figd_plain_secret"}"#),
            ),
            (
                "credentials_sealed".to_string(),
                attr_s("c2VhbGVkX3NlY3JldA=="),
            ),
        ]);
        let out = plugin_summary(&item).unwrap();
        let text = out.to_string();
        assert!(!text.contains("figd_plain_secret"));
        assert!(!text.contains("c2VhbGVkX3NlY3JldA=="));
        assert!(!text.contains("credentials_sealed"));
        assert_eq!(out["plugin_id"], "figma");
        assert_eq!(out["has_credentials"], true);
    }
}
