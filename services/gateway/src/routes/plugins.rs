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
    icon: &'static str,
    credential_fields: &'static [CredentialField],
    docs_url: &'static str,
    repo_url: &'static str,
    default_prompt: &'static str,
    recommended_permissions: &'static str,
    /// npm package to spawn via `npx -y <package>` for MCP stdio transport.
    npx_package: &'static str,
    /// Maps credential_field.key → env var name the MCP server expects.
    env_mapping: &'static [(&'static str, &'static str)],
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
        icon: "figma",
        credential_fields: &[CredentialField {
            key: "api_token",
            label: "Personal Access Token",
            placeholder: "figd_...",
            secret: true,
        }],
        docs_url: "https://www.figma.com/developers/api",
        repo_url: "https://github.com/figma/mcp-server-guide",
        default_prompt: "Use the Figma MCP to extract design tokens, component details, and layout information from design files. Reference specific frames and layers by name. Do not modify or delete any Figma data.",
        recommended_permissions: "Read-only file access",
        npx_package: "figma-developer-mcp",
        env_mapping: &[("api_token", "FIGMA_API_KEY")],
    },
    PluginDef {
        id: "sentry",
        name: "Sentry",
        description: "Query error events, list issues, and resolve incidents from Sentry.",
        category: "Monitoring",
        tier: 1,
        icon: "sentry",
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
        default_prompt: "Use the Sentry MCP to investigate production errors. Query recent issues, inspect stack traces, and correlate errors with recent deployments. Do not resolve or delete issues unless explicitly asked.",
        recommended_permissions: "Read-only: event:read, issue:read, project:read",
        npx_package: "@sentry/mcp-server",
        env_mapping: &[("auth_token", "SENTRY_AUTH_TOKEN"), ("org_slug", "SENTRY_ORG")],
    },
    PluginDef {
        id: "linear",
        name: "Linear",
        description: "Create and update issues, query projects, and sync status with Linear.",
        category: "Project Management",
        tier: 1,
        icon: "linear",
        credential_fields: &[CredentialField {
            key: "api_key",
            label: "API Key",
            placeholder: "lin_api_...",
            secret: true,
        }],
        docs_url: "https://developers.linear.app/docs",
        repo_url: "https://linear.app/docs/mcp",
        default_prompt: "Use the Linear MCP to manage issues and projects. Create issues with proper labels and priorities, update status, and query project progress. Do not delete issues or modify team settings.",
        recommended_permissions: "Read + write issues only",
        npx_package: "@linear/mcp-server",
        env_mapping: &[("api_key", "LINEAR_API_KEY")],
    },
    PluginDef {
        id: "notion",
        name: "Notion",
        description: "Search pages, read databases, and sync documentation from Notion.",
        category: "Documentation",
        tier: 1,
        icon: "notion",
        credential_fields: &[CredentialField {
            key: "api_key",
            label: "Integration Token",
            placeholder: "ntn_...",
            secret: true,
        }],
        docs_url: "https://developers.notion.com/",
        repo_url: "https://github.com/makenotion/notion-mcp-server",
        default_prompt: "Use the Notion MCP to search documentation, read database entries, and reference wiki pages for project context. Do not create, modify, or delete pages unless explicitly asked.",
        recommended_permissions: "Read content only — no insert capabilities",
        npx_package: "@notionhq/notion-mcp-server",
        env_mapping: &[("api_key", "OPENAPI_MCP_HEADERS")],
    },

    // Tier 2 — Strong use cases

    PluginDef {
        id: "vercel",
        name: "Vercel",
        description: "List deployments, check build status, and manage environment variables.",
        category: "Deployment",
        tier: 2,
        icon: "vercel",
        credential_fields: &[CredentialField {
            key: "api_token",
            label: "API Token",
            placeholder: "vercel_...",
            secret: true,
        }],
        docs_url: "https://vercel.com/docs/rest-api",
        repo_url: "https://github.com/vercel/vercel-mcp",
        default_prompt: "Use the Vercel MCP to check deployment statuses, manage environment variables, and inspect build logs. Do not trigger deployments or modify production settings without confirmation.",
        recommended_permissions: "Read-only token — no deploy access",
        npx_package: "@vercel/mcp",
        env_mapping: &[("api_token", "VERCEL_API_TOKEN")],
    },
    PluginDef {
        id: "stripe",
        name: "Stripe",
        description: "List charges, subscriptions, and customers from your Stripe account.",
        category: "Payments",
        tier: 2,
        icon: "stripe",
        credential_fields: &[CredentialField {
            key: "api_key",
            label: "Restricted API Key",
            placeholder: "rk_live_...",
            secret: true,
        }],
        docs_url: "https://docs.stripe.com/api",
        repo_url: "https://github.com/stripe/agent-toolkit",
        default_prompt: "Use the Stripe MCP to query customers, list recent charges, and inspect subscription statuses. Never create charges, refunds, or modify billing data.",
        recommended_permissions: "Restricted key: read-only charges, customers, subscriptions",
        npx_package: "@stripe/mcp",
        env_mapping: &[("api_key", "STRIPE_SECRET_KEY")],
    },
    PluginDef {
        id: "cloudflare",
        name: "Cloudflare",
        description: "Manage DNS records, check analytics, and configure Workers.",
        category: "Deployment",
        tier: 2,
        icon: "cloudflare",
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
        default_prompt: "Use the Cloudflare MCP to inspect DNS records, Worker deployments, and zone analytics. Do not modify DNS records or deploy Workers without explicit confirmation.",
        recommended_permissions: "Read-only: Zone.Zone:Read, Zone.DNS:Read, Zone.Analytics:Read",
        npx_package: "@cloudflare/mcp-server-cloudflare",
        env_mapping: &[("api_token", "CLOUDFLARE_API_TOKEN"), ("account_id", "CLOUDFLARE_ACCOUNT_ID")],
    },
    PluginDef {
        id: "posthog",
        name: "PostHog",
        description: "Query events, feature flags, and user analytics from PostHog.",
        category: "Analytics",
        tier: 2,
        icon: "posthog",
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
        repo_url: "https://github.com/posthog/mcp",
        default_prompt: "Use the PostHog MCP to query user analytics events, check feature flag statuses, and analyze user funnels. Do not modify feature flags or delete data.",
        recommended_permissions: "Read-only personal API key",
        npx_package: "@nicholasoxford/posthog-mcp",
        env_mapping: &[("api_key", "POSTHOG_API_KEY"), ("host", "POSTHOG_HOST")],
    },

    PluginDef {
        id: "gitlab",
        name: "GitLab",
        description: "List merge requests, pipelines, and issues from GitLab.",
        category: "Source Control",
        tier: 2,
        icon: "gitlab",
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
        repo_url: "https://docs.gitlab.com/user/gitlab_duo/model_context_protocol/mcp_server/",
        default_prompt: "Use the GitLab MCP to list merge requests, check pipeline statuses, and query issues. Do not push code, merge MRs, or modify project settings.",
        recommended_permissions: "read_api scope only",
        npx_package: "@anthropic-ai/gitlab-mcp-server",
        env_mapping: &[("api_token", "GITLAB_TOKEN"), ("base_url", "GITLAB_URL")],
    },
    PluginDef {
        id: "neon",
        name: "Neon",
        description: "Manage Postgres databases, branches, and run queries on Neon.",
        category: "Database",
        tier: 2,
        icon: "neon",
        credential_fields: &[CredentialField {
            key: "api_key",
            label: "API Key",
            placeholder: "neon_...",
            secret: true,
        }],
        docs_url: "https://neon.tech/docs/manage/api-keys",
        repo_url: "https://github.com/neondatabase/mcp-server-neon",
        default_prompt: "Use the Neon MCP to inspect Postgres schemas and run read-only queries. Do not run DDL statements, drop tables, or modify data without explicit approval.",
        recommended_permissions: "Read-only database role — no DDL, no deletes",
        npx_package: "@neondatabase/mcp-server-neon",
        env_mapping: &[("api_key", "NEON_API_KEY")],
    },
    PluginDef {
        id: "turso",
        name: "Turso",
        description: "Manage LibSQL databases, run queries, and inspect schemas on Turso.",
        category: "Database",
        tier: 2,
        icon: "turso",
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
        default_prompt: "Use the Turso MCP to inspect LibSQL schemas and run read-only queries. Do not execute DDL or destructive operations without explicit approval.",
        recommended_permissions: "Read-only API token",
        npx_package: "@tursodatabase/turso-mcp",
        env_mapping: &[("api_token", "TURSO_API_TOKEN"), ("org_name", "TURSO_ORG")],
    },
    // Tier 3 — Nice to have
    PluginDef {
        id: "snyk",
        name: "Snyk",
        description: "Scan for vulnerabilities, list issues, and check dependency health.",
        category: "Security",
        tier: 3,
        icon: "snyk",
        credential_fields: &[CredentialField {
            key: "api_token",
            label: "API Token",
            placeholder: "snyk_...",
            secret: true,
        }],
        docs_url: "https://docs.snyk.io/snyk-api",
        repo_url: "https://github.com/snyk/snyk-ls",
        default_prompt: "Use the Snyk MCP to scan for vulnerabilities, check dependency health, and list security issues. Do not modify project settings or ignore policies.",
        recommended_permissions: "Read-only: view org, view project",
        npx_package: "snyk-mcp-server",
        env_mapping: &[("api_token", "SNYK_TOKEN")],
    },
    PluginDef {
        id: "launchdarkly",
        name: "LaunchDarkly",
        description: "Manage feature flags, list environments, and toggle flags.",
        category: "Feature Flags",
        tier: 3,
        icon: "launchdarkly",
        credential_fields: &[CredentialField {
            key: "api_key",
            label: "Access Token",
            placeholder: "api-...",
            secret: true,
        }],
        docs_url: "https://apidocs.launchdarkly.com/",
        repo_url: "https://github.com/launchdarkly/mcp-server",
        default_prompt: "Use the LaunchDarkly MCP to check feature flag statuses across environments. Do not toggle flags or modify targeting rules without explicit confirmation.",
        recommended_permissions: "Reader role — no write access",
        npx_package: "@launchdarkly/mcp-server",
        env_mapping: &[("api_key", "LAUNCHDARKLY_ACCESS_TOKEN")],
    },
    PluginDef {
        id: "mongodb",
        name: "MongoDB Atlas",
        description: "Query collections, inspect schemas, and manage clusters on MongoDB Atlas.",
        category: "Database",
        tier: 3,
        icon: "mongodb",
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
        repo_url: "https://github.com/mongodb-js/mongodb-mcp-server",
        default_prompt: "Use the MongoDB MCP to query collections and inspect document schemas. Do not drop collections, modify indexes, or delete data.",
        recommended_permissions: "Project Read Only role",
        npx_package: "mongodb-mcp-server",
        env_mapping: &[("public_key", "MDB_MCP_API_PUBLIC_KEY"), ("private_key", "MDB_MCP_API_PRIVATE_KEY"), ("group_id", "MDB_MCP_API_GROUP_ID")],
    },
    PluginDef {
        id: "grafana",
        name: "Grafana",
        description: "Query dashboards, list alerts, and explore data sources in Grafana.",
        category: "Monitoring",
        tier: 3,
        icon: "grafana",
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
        default_prompt: "Use the Grafana MCP to query dashboards, check alert statuses, and explore data sources. Do not modify dashboards or silence alerts.",
        recommended_permissions: "Viewer role service account",
        npx_package: "mcp-grafana",
        env_mapping: &[("api_key", "GRAFANA_API_KEY"), ("base_url", "GRAFANA_URL")],
    },
    PluginDef {
        id: "redis",
        name: "Redis",
        description: "Inspect keys, run commands, and monitor a Redis instance.",
        category: "Database",
        tier: 3,
        icon: "redis",
        credential_fields: &[CredentialField {
            key: "connection_url",
            label: "Connection URL",
            placeholder: "redis://...",
            secret: true,
        }],
        docs_url: "https://redis.io/docs/",
        repo_url: "https://github.com/redis/mcp-redis",
        default_prompt: "Use the Redis MCP to inspect keys and run read-only commands. Do not run FLUSHDB, DEL, or any destructive commands.",
        recommended_permissions: "Read-only user — no write commands",
        npx_package: "@redis/mcp-server",
        env_mapping: &[("connection_url", "REDIS_URL")],
    },
    PluginDef {
        id: "upstash",
        name: "Upstash",
        description: "Manage Redis and Kafka instances on Upstash.",
        category: "Database",
        tier: 3,
        icon: "upstash",
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
        default_prompt: "Use the Upstash MCP to inspect Redis databases and read data. Do not delete databases, flush data, or modify configurations.",
        recommended_permissions: "Read-only API key",
        npx_package: "@anthropic-ai/upstash-mcp-server",
        env_mapping: &[("email", "UPSTASH_EMAIL"), ("api_key", "UPSTASH_API_KEY")],
    },

    PluginDef {
        id: "sanity",
        name: "Sanity",
        description: "Query documents, read schemas, and manage content in Sanity.",
        category: "CMS",
        tier: 3,
        icon: "sanity",
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
        default_prompt: "Use the Sanity MCP to query documents and read content schemas. Do not create, publish, or delete documents without explicit approval.",
        recommended_permissions: "Viewer token — read-only access",
        npx_package: "sanity-mcp-server",
        env_mapping: &[("api_token", "SANITY_API_TOKEN"), ("project_id", "SANITY_PROJECT_ID")],
    },

];

// ── Handlers ────────────────────────────────────────────────────────

/// Sync the plugin catalog JSON to S3 so other Lambdas can load it.
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
fn mcp_table(state: &AppState) -> &str {
    let t = &state.config.mcp_configs_table_name;
    if t.is_empty() {
        &state.config.settings_table_name
    } else {
        t
    }
}

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
        .table_name(mcp_table(&state))
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
                "custom_prompt": item.get("custom_prompt").and_then(|v| v.as_s().ok()).map(|s| s.to_string()),
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
        .table_name(mcp_table(&state))
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
        .table_name(mcp_table(&state))
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
        .table_name(mcp_table(&state))
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
        .key("pk", attr_s(&claims.tenant_id))
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

    info!(tenant = %claims.tenant_id, plugin = %plugin_id, "Plugin custom prompt updated");
    Ok(Json(json!({ "status": "saved" })))
}
