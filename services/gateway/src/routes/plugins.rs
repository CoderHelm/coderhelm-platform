use axum::{extract::State, http::StatusCode, Extension, Json};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info};

use crate::models::Claims;
use crate::AppState;
use aws_sdk_dynamodb::types::AttributeValue;

fn attr_s(val: &str) -> AttributeValue {
    AttributeValue::S(val.to_string())
}

fn is_admin_or_owner(role: &str) -> bool {
    matches!(role, "admin" | "owner")
}

// ── Plugin catalog ──────────────────────────────────────────────────

#[derive(serde::Serialize)]
struct PluginDef {
    id: &'static str,
    name: &'static str,
    description: &'static str,
    category: &'static str,
    tier: u8,
    credential_fields: &'static [CredentialField],
    docs_url: &'static str,
    repo_url: &'static str,
}

#[derive(serde::Serialize)]
struct CredentialField {
    key: &'static str,
    label: &'static str,
    placeholder: &'static str,
    secret: bool,
}

const CATALOG: &[PluginDef] = &[
    // Tier 1 — High demand
    PluginDef {
        id: "figma",
        name: "Figma",
        description: "Extract design tokens, inspect components, and pull assets from Figma files.",
        category: "Design",
        tier: 1,
        credential_fields: &[CredentialField {
            key: "api_token",
            label: "Personal Access Token",
            placeholder: "figd_...",
            secret: true,
        }],
        docs_url: "https://www.figma.com/developers/api",
        repo_url: "https://github.com/nichochar/figma-mcp",
    },
    PluginDef {
        id: "sentry",
        name: "Sentry",
        description: "Query error events, list issues, and resolve incidents from Sentry.",
        category: "Monitoring",
        tier: 1,
        credential_fields: &[
            CredentialField {
                key: "auth_token",
                label: "Auth Token",
                placeholder: "sntrys_...",
                secret: true,
            },
            CredentialField {
                key: "org_slug",
                label: "Organization Slug",
                placeholder: "my-org",
                secret: false,
            },
        ],
        docs_url: "https://docs.sentry.io/api/",
        repo_url: "https://github.com/getsentry/sentry-mcp",
    },
    PluginDef {
        id: "linear",
        name: "Linear",
        description: "Create and update issues, query projects, and sync status with Linear.",
        category: "Project Management",
        tier: 1,
        credential_fields: &[CredentialField {
            key: "api_key",
            label: "API Key",
            placeholder: "lin_api_...",
            secret: true,
        }],
        docs_url: "https://developers.linear.app/docs",
        repo_url: "https://github.com/linear/linear-mcp-server",
    },
    PluginDef {
        id: "notion",
        name: "Notion",
        description: "Search pages, read databases, and sync documentation from Notion.",
        category: "Documentation",
        tier: 1,
        credential_fields: &[CredentialField {
            key: "api_key",
            label: "Integration Token",
            placeholder: "ntn_...",
            secret: true,
        }],
        docs_url: "https://developers.notion.com/",
        repo_url: "https://github.com/makenotion/notion-mcp-server",
    },
    PluginDef {
        id: "slack",
        name: "Slack",
        description: "Send messages, query channels, and post updates to Slack.",
        category: "Communication",
        tier: 1,
        credential_fields: &[CredentialField {
            key: "bot_token",
            label: "Bot Token",
            placeholder: "xoxb-...",
            secret: true,
        }],
        docs_url: "https://api.slack.com/docs",
        repo_url: "https://github.com/modelcontextprotocol/servers",
    },
    PluginDef {
        id: "datadog",
        name: "Datadog",
        description: "Query metrics, list monitors, and search logs from Datadog.",
        category: "Monitoring",
        tier: 1,
        credential_fields: &[
            CredentialField {
                key: "api_key",
                label: "API Key",
                placeholder: "dd-api-...",
                secret: true,
            },
            CredentialField {
                key: "app_key",
                label: "Application Key",
                placeholder: "dd-app-...",
                secret: true,
            },
            CredentialField {
                key: "site",
                label: "Datadog Site",
                placeholder: "datadoghq.com",
                secret: false,
            },
        ],
        docs_url: "https://docs.datadoghq.com/api/",
        repo_url: "https://github.com/winor30/datadog-mcp-server",
    },
    // Tier 2 — Strong use cases
    PluginDef {
        id: "supabase",
        name: "Supabase",
        description: "Manage tables, run queries, and inspect database schemas in Supabase.",
        category: "Database",
        tier: 2,
        credential_fields: &[
            CredentialField {
                key: "api_key",
                label: "Service Role Key",
                placeholder: "eyJ...",
                secret: true,
            },
            CredentialField {
                key: "project_url",
                label: "Project URL",
                placeholder: "https://abc.supabase.co",
                secret: false,
            },
        ],
        docs_url: "https://supabase.com/docs/guides/api",
        repo_url: "https://github.com/supabase-community/supabase-mcp",
    },
    PluginDef {
        id: "vercel",
        name: "Vercel",
        description: "List deployments, check build status, and manage environment variables.",
        category: "Deployment",
        tier: 2,
        credential_fields: &[CredentialField {
            key: "api_token",
            label: "API Token",
            placeholder: "vercel_...",
            secret: true,
        }],
        docs_url: "https://vercel.com/docs/rest-api",
        repo_url: "https://github.com/vercel/vercel-mcp",
    },
    PluginDef {
        id: "stripe",
        name: "Stripe",
        description: "List charges, subscriptions, and customers from your Stripe account.",
        category: "Payments",
        tier: 2,
        credential_fields: &[CredentialField {
            key: "api_key",
            label: "Restricted API Key",
            placeholder: "rk_live_...",
            secret: true,
        }],
        docs_url: "https://docs.stripe.com/api",
        repo_url: "https://github.com/stripe/agent-toolkit",
    },
    PluginDef {
        id: "cloudflare",
        name: "Cloudflare",
        description: "Manage DNS records, check analytics, and configure Workers.",
        category: "Deployment",
        tier: 2,
        credential_fields: &[
            CredentialField {
                key: "api_token",
                label: "API Token",
                placeholder: "cf_...",
                secret: true,
            },
            CredentialField {
                key: "account_id",
                label: "Account ID",
                placeholder: "abc123...",
                secret: false,
            },
        ],
        docs_url: "https://developers.cloudflare.com/api/",
        repo_url: "https://github.com/cloudflare/mcp-server-cloudflare",
    },
    PluginDef {
        id: "posthog",
        name: "PostHog",
        description: "Query events, feature flags, and user analytics from PostHog.",
        category: "Analytics",
        tier: 2,
        credential_fields: &[
            CredentialField {
                key: "api_key",
                label: "Personal API Key",
                placeholder: "phx_...",
                secret: true,
            },
            CredentialField {
                key: "host",
                label: "Host URL",
                placeholder: "https://app.posthog.com",
                secret: false,
            },
        ],
        docs_url: "https://posthog.com/docs/api",
        repo_url: "https://github.com/PostHog/posthog-mcp",
    },
    PluginDef {
        id: "confluence",
        name: "Confluence",
        description: "Search and read pages, spaces, and documentation from Confluence.",
        category: "Documentation",
        tier: 2,
        credential_fields: &[
            CredentialField {
                key: "api_token",
                label: "API Token",
                placeholder: "ATATT3...",
                secret: true,
            },
            CredentialField {
                key: "email",
                label: "Email",
                placeholder: "user@company.com",
                secret: false,
            },
            CredentialField {
                key: "base_url",
                label: "Base URL",
                placeholder: "https://company.atlassian.net",
                secret: false,
            },
        ],
        docs_url: "https://developer.atlassian.com/cloud/confluence/rest/v2/",
        repo_url: "https://github.com/modelcontextprotocol/servers",
    },
    PluginDef {
        id: "planetscale",
        name: "PlanetScale",
        description: "Run queries, inspect schemas, and manage branches in PlanetScale.",
        category: "Database",
        tier: 2,
        credential_fields: &[
            CredentialField {
                key: "username",
                label: "Username",
                placeholder: "ps_user_...",
                secret: false,
            },
            CredentialField {
                key: "password",
                label: "Password",
                placeholder: "pscale_pw_...",
                secret: true,
            },
            CredentialField {
                key: "database",
                label: "Database Name",
                placeholder: "my-database",
                secret: false,
            },
        ],
        docs_url: "https://planetscale.com/docs/concepts/planetscale-api-oauth-applications",
        repo_url: "https://github.com/planetscale/database-js",
    },
    PluginDef {
        id: "gitlab",
        name: "GitLab",
        description: "List merge requests, pipelines, and issues from GitLab.",
        category: "Source Control",
        tier: 2,
        credential_fields: &[
            CredentialField {
                key: "api_token",
                label: "Personal Access Token",
                placeholder: "glpat-...",
                secret: true,
            },
            CredentialField {
                key: "base_url",
                label: "GitLab URL",
                placeholder: "https://gitlab.com",
                secret: false,
            },
        ],
        docs_url: "https://docs.gitlab.com/ee/api/rest/",
        repo_url: "https://github.com/modelcontextprotocol/servers",
    },
    PluginDef {
        id: "neon",
        name: "Neon",
        description: "Manage Postgres databases, branches, and run queries on Neon.",
        category: "Database",
        tier: 2,
        credential_fields: &[CredentialField {
            key: "api_key",
            label: "API Key",
            placeholder: "neon_...",
            secret: true,
        }],
        docs_url: "https://neon.tech/docs/manage/api-keys",
        repo_url: "https://github.com/neondatabase/mcp-server-neon",
    },
    PluginDef {
        id: "turso",
        name: "Turso",
        description: "Manage LibSQL databases, run queries, and inspect schemas on Turso.",
        category: "Database",
        tier: 2,
        credential_fields: &[
            CredentialField {
                key: "api_token",
                label: "API Token",
                placeholder: "turso_...",
                secret: true,
            },
            CredentialField {
                key: "org_name",
                label: "Organization Name",
                placeholder: "my-org",
                secret: false,
            },
        ],
        docs_url: "https://docs.turso.tech/api-reference",
        repo_url: "https://github.com/tursodatabase/turso-mcp",
    },
    // Tier 3 — Nice to have
    PluginDef {
        id: "snyk",
        name: "Snyk",
        description: "Scan for vulnerabilities, list issues, and check dependency health.",
        category: "Security",
        tier: 3,
        credential_fields: &[CredentialField {
            key: "api_token",
            label: "API Token",
            placeholder: "snyk_...",
            secret: true,
        }],
        docs_url: "https://docs.snyk.io/snyk-api",
        repo_url: "https://github.com/snyk/snyk-mcp-server",
    },
    PluginDef {
        id: "launchdarkly",
        name: "LaunchDarkly",
        description: "Manage feature flags, list environments, and toggle flags.",
        category: "Feature Flags",
        tier: 3,
        credential_fields: &[CredentialField {
            key: "api_key",
            label: "Access Token",
            placeholder: "api-...",
            secret: true,
        }],
        docs_url: "https://apidocs.launchdarkly.com/",
        repo_url: "https://github.com/launchdarkly/mcp-server",
    },
    PluginDef {
        id: "mongodb",
        name: "MongoDB Atlas",
        description: "Query collections, inspect schemas, and manage clusters on MongoDB Atlas.",
        category: "Database",
        tier: 3,
        credential_fields: &[
            CredentialField {
                key: "public_key",
                label: "Public Key",
                placeholder: "abc123",
                secret: false,
            },
            CredentialField {
                key: "private_key",
                label: "Private Key",
                placeholder: "...",
                secret: true,
            },
            CredentialField {
                key: "group_id",
                label: "Project ID",
                placeholder: "64abc...",
                secret: false,
            },
        ],
        docs_url: "https://www.mongodb.com/docs/atlas/api/",
        repo_url: "https://github.com/mongodb/mongodb-mcp-server",
    },
    PluginDef {
        id: "grafana",
        name: "Grafana",
        description: "Query dashboards, list alerts, and explore data sources in Grafana.",
        category: "Monitoring",
        tier: 3,
        credential_fields: &[
            CredentialField {
                key: "api_key",
                label: "Service Account Token",
                placeholder: "glsa_...",
                secret: true,
            },
            CredentialField {
                key: "base_url",
                label: "Grafana URL",
                placeholder: "https://grafana.example.com",
                secret: false,
            },
        ],
        docs_url: "https://grafana.com/docs/grafana/latest/developers/http_api/",
        repo_url: "https://github.com/grafana/mcp-grafana",
    },
    PluginDef {
        id: "redis",
        name: "Redis",
        description: "Inspect keys, run commands, and monitor a Redis instance.",
        category: "Database",
        tier: 3,
        credential_fields: &[CredentialField {
            key: "connection_url",
            label: "Connection URL",
            placeholder: "redis://...",
            secret: true,
        }],
        docs_url: "https://redis.io/docs/",
        repo_url: "https://github.com/redis/mcp-redis",
    },
    PluginDef {
        id: "upstash",
        name: "Upstash",
        description: "Manage Redis and Kafka instances on Upstash.",
        category: "Database",
        tier: 3,
        credential_fields: &[
            CredentialField {
                key: "email",
                label: "Email",
                placeholder: "user@company.com",
                secret: false,
            },
            CredentialField {
                key: "api_key",
                label: "API Key",
                placeholder: "upstash_...",
                secret: true,
            },
        ],
        docs_url: "https://upstash.com/docs/devops/developer-api",
        repo_url: "https://github.com/upstash/mcp-server",
    },
    PluginDef {
        id: "contentful",
        name: "Contentful",
        description: "Read content models, entries, and assets from Contentful.",
        category: "CMS",
        tier: 3,
        credential_fields: &[
            CredentialField {
                key: "access_token",
                label: "Content Delivery Token",
                placeholder: "CFPAT-...",
                secret: true,
            },
            CredentialField {
                key: "space_id",
                label: "Space ID",
                placeholder: "abc123",
                secret: false,
            },
        ],
        docs_url: "https://www.contentful.com/developers/docs/references/content-delivery-api/",
        repo_url: "https://github.com/contentful/mcp-server",
    },
    PluginDef {
        id: "sanity",
        name: "Sanity",
        description: "Query documents, read schemas, and manage content in Sanity.",
        category: "CMS",
        tier: 3,
        credential_fields: &[
            CredentialField {
                key: "api_token",
                label: "API Token",
                placeholder: "sk...",
                secret: true,
            },
            CredentialField {
                key: "project_id",
                label: "Project ID",
                placeholder: "abc123",
                secret: false,
            },
        ],
        docs_url: "https://www.sanity.io/docs/http-api",
        repo_url: "https://github.com/sanity-io/sanity-mcp-server",
    },
    PluginDef {
        id: "airtable",
        name: "Airtable",
        description: "List bases, read records, and query tables in Airtable.",
        category: "Database",
        tier: 3,
        credential_fields: &[CredentialField {
            key: "api_key",
            label: "Personal Access Token",
            placeholder: "pat...",
            secret: true,
        }],
        docs_url: "https://airtable.com/developers/web/api",
        repo_url: "https://github.com/modelcontextprotocol/servers",
    },
];

// ── Handlers ────────────────────────────────────────────────────────

/// GET /api/plugins/catalog — return all available plugins.
pub async fn list_catalog() -> Json<Value> {
    Json(json!({ "plugins": CATALOG }))
}

/// GET /api/plugins — list plugins enabled for this tenant.
pub async fn list_enabled(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let result = state
        .dynamo
        .query()
        .table_name(&state.config.settings_table_name)
        .key_condition_expression("pk = :pk AND begins_with(sk, :prefix)")
        .expression_attribute_values(":pk", attr_s(&claims.tenant_id))
        .expression_attribute_values(":prefix", attr_s("PLUGIN#"))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to list plugins: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let plugins: Vec<Value> = result
        .items()
        .iter()
        .filter_map(|item| {
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
            }))
        })
        .collect();

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

    // Validate plugin exists in catalog
    if !CATALOG.iter().any(|p| p.id == plugin_id) {
        return Err(StatusCode::NOT_FOUND);
    }

    let now = chrono::Utc::now().to_rfc3339();

    state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", attr_s(&claims.tenant_id))
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

    info!(tenant = %claims.tenant_id, plugin = %plugin_id, "Plugin enabled");
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
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&format!("PLUGIN#{plugin_id}")))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to disable plugin {plugin_id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(tenant = %claims.tenant_id, plugin = %plugin_id, "Plugin disabled");
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

    // Store credentials as JSON string in the item
    let creds_json = serde_json::to_string(&credentials).map_err(|e| {
        error!("Failed to serialize credentials: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let now = chrono::Utc::now().to_rfc3339();

    state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", attr_s(&claims.tenant_id))
        .key("sk", attr_s(&format!("PLUGIN#{plugin_id}")))
        .update_expression(
            "SET credentials = :creds, has_credentials = :hc, credentials_updated_at = :ts",
        )
        .expression_attribute_values(":creds", attr_s(&creds_json))
        .expression_attribute_values(":hc", AttributeValue::Bool(true))
        .expression_attribute_values(":ts", attr_s(&now))
        .condition_expression("attribute_exists(pk)")
        .send()
        .await
        .map_err(|e| {
            error!("Failed to update credentials for {plugin_id}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    info!(tenant = %claims.tenant_id, plugin = %plugin_id, "Plugin credentials updated");
    Ok(Json(json!({ "status": "saved" })))
}
