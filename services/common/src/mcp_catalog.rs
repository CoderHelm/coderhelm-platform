//! The MCP servers a team can switch on from the Connections page: one
//! catalog for the gateway (the page, plan chat's tool calls, the copy synced
//! to S3 for the MCP proxy) and the worker (the agents' MCP tools).
//!
//! It used to be kept three times (the gateway's page, the gateway's plan
//! chat and the worker), and the copies drifted: a server one side knew the
//! other didn't, and an environment variable named two ways. Now there is
//! this one list.

#[derive(serde::Serialize, Debug)]
pub struct McpServerDef {
    pub id: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub category: &'static str,
    pub tier: u8,
    pub icon: &'static str,
    pub credential_fields: &'static [CredentialField],
    pub docs_url: &'static str,
    pub repo_url: &'static str,
    pub default_prompt: &'static str,
    pub recommended_permissions: &'static str,
    /// npm package to spawn via `npx -y <package>` for MCP stdio transport.
    pub npx_package: &'static str,
    /// Maps credential_field.key → env var name the MCP server expects.
    pub env_mapping: &'static [(&'static str, &'static str)],
    /// The vendor's own remote MCP server, signed in to with OAuth (see the
    /// gateway's `mcp_servers`). Empty when the vendor runs none.
    pub remote_url: &'static str,
    /// The vendor's remote server also takes a key as `Authorization:
    /// Bearer <key>`: a key saved for it connects to the remote server
    /// (the npm package path is for vendors without one).
    pub remote_key: bool,
    /// Why there is no key path, in plain words, when neither a package
    /// the proxy can run nor a remote server that takes a key exists.
    /// Empty when a key works.
    pub key_unavailable: &'static str,
}

#[derive(serde::Serialize, Debug)]
pub struct CredentialField {
    pub key: &'static str,
    pub label: &'static str,
    pub placeholder: &'static str,
    pub secret: bool,
}

pub const CATALOG: &[McpServerDef] = &[
    // Tier 1 — High demand
    McpServerDef {
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
        // No remote sign-in: Figma's MCP server registers only clients Figma
        // approved (dynamic registration answers 403) and takes no client
        // metadata documents, so a key is the way in.
        remote_url: "",
        remote_key: false,
        key_unavailable: "",
        env_mapping: &[("api_token", "FIGMA_API_KEY")],
    },
    McpServerDef {
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
        remote_url: "https://mcp.sentry.dev/mcp",
        remote_key: false,
        key_unavailable: "",
        env_mapping: &[("auth_token", "SENTRY_ACCESS_TOKEN"), ("org_slug", "SENTRY_ORG")],
    },
    McpServerDef {
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
        npx_package: "",
        remote_url: "https://mcp.linear.app/mcp",
        remote_key: true,
        key_unavailable: "",
        env_mapping: &[],
    },
    McpServerDef {
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
        remote_url: "https://mcp.notion.com/mcp",
        remote_key: false,
        key_unavailable: "",
        env_mapping: &[("api_key", "OPENAPI_MCP_HEADERS")],
    },

    // Tier 2 — Strong use cases

    McpServerDef {
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
        npx_package: "",
        remote_url: "https://mcp.vercel.com",
        remote_key: false,
        key_unavailable: "Vercel's MCP server takes no key: use Sign in with Vercel.",
        env_mapping: &[],
    },
    McpServerDef {
        id: "stripe",
        name: "Stripe",
        description: "List charges, subscriptions, and customers from your Stripe account.",
        category: "Payments",
        tier: 2,
        icon: "stripe",
        credential_fields: &[CredentialField {
            key: "api_key",
            label: "Agent API key",
            placeholder: "",
            secret: true,
        }],
        docs_url: "https://docs.stripe.com/mcp",
        repo_url: "https://github.com/stripe/agent-toolkit",
        default_prompt: "Use the Stripe MCP to query customers, list recent charges, and inspect subscription statuses. Never create charges, refunds, or modify billing data.",
        recommended_permissions: "Restricted key: read-only charges, customers, subscriptions",
        npx_package: "",
        remote_url: "https://mcp.stripe.com",
        remote_key: true,
        key_unavailable: "",
        env_mapping: &[],
    },
    // Cloudflare removed — their MCP server moved to remote OAuth-only (*.mcp.cloudflare.com)
    McpServerDef {
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
        npx_package: "",
        remote_url: "https://mcp.posthog.com/mcp",
        remote_key: false,
        key_unavailable: "PostHog's MCP server signs in with PostHog: use Sign in with PostHog.",
        env_mapping: &[],
    },

    McpServerDef {
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
        npx_package: "",
        remote_url: "https://gitlab.com/api/v4/mcp",
        remote_key: false,
        key_unavailable: "GitLab's MCP server takes no key: use Sign in with GitLab.",
        env_mapping: &[],
    },
    McpServerDef {
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
        npx_package: "",
        remote_url: "https://mcp.neon.tech/mcp",
        remote_key: true,
        key_unavailable: "",
        env_mapping: &[],
    },
    McpServerDef {
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
        npx_package: "",
        remote_url: "https://mcp.turso.ai/mcp",
        remote_key: false,
        key_unavailable: "Turso's MCP server takes no key: use Sign in with Turso.",
        env_mapping: &[],
    },
    // Tier 3 — Nice to have
    McpServerDef {
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
        npx_package: "",
        remote_url: "",
        remote_key: false,
        key_unavailable: "Snyk's MCP server runs through the Snyk command line, which Coderhelm can't run yet.",
        env_mapping: &[],
    },
    McpServerDef {
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
        npx_package: "",
        remote_url: "https://mcp.launchdarkly.com/mcp/launchdarkly",
        remote_key: false,
        key_unavailable: "LaunchDarkly's MCP server needs start options Coderhelm can't pass yet: use Sign in with LaunchDarkly.",
        env_mapping: &[],
    },
    McpServerDef {
        id: "mongodb",
        name: "MongoDB Atlas",
        description: "Query collections, inspect schemas, and manage clusters on MongoDB Atlas.",
        category: "Database",
        tier: 3,
        icon: "mongodb",
        credential_fields: &[
            CredentialField {
                key: "client_id",
                label: "Service account client ID",
                placeholder: "mdb_sa_id_...",
                secret: false,
            },
            CredentialField {
                key: "client_secret",
                label: "Service account client secret",
                placeholder: "mdb_sa_sk_...",
                secret: true,
            },
        ],
        docs_url: "https://www.mongodb.com/docs/atlas/configure-api-access/",
        repo_url: "https://github.com/mongodb-js/mongodb-mcp-server",
        default_prompt: "Use the MongoDB MCP to query collections and inspect document schemas. Do not drop collections, modify indexes, or delete data.",
        recommended_permissions: "Project Read Only role",
        npx_package: "mongodb-mcp-server",
        remote_url: "",
        remote_key: false,
        key_unavailable: "",
        env_mapping: &[("client_id", "MDB_MCP_API_CLIENT_ID"), ("client_secret", "MDB_MCP_API_CLIENT_SECRET")],
    },
    McpServerDef {
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
        npx_package: "",
        remote_url: "https://mcp.grafana.com/mcp",
        remote_key: false,
        key_unavailable: "Grafana's MCP server runs as its own program, which Coderhelm can't run yet. Use Sign in with Grafana.",
        env_mapping: &[],
    },
    McpServerDef {
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
        npx_package: "",
        remote_url: "",
        remote_key: false,
        key_unavailable: "Redis's MCP server runs with Python, which Coderhelm can't run yet.",
        env_mapping: &[],
    },
    McpServerDef {
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
        npx_package: "@upstash/mcp-server",
        remote_url: "https://mcp.upstash.com/mcp",
        remote_key: false,
        key_unavailable: "",
        env_mapping: &[("email", "UPSTASH_EMAIL"), ("api_key", "UPSTASH_API_KEY")],
    },

    McpServerDef {
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
        npx_package: "",
        remote_url: "https://mcp.sanity.io",
        remote_key: false,
        key_unavailable: "",
        env_mapping: &[("api_token", "SANITY_API_TOKEN"), ("project_id", "SANITY_PROJECT_ID")],
    },

    McpServerDef {
        id: "atlassian",
        name: "Atlassian",
        description: "Search and read Jira issues and Confluence pages through Atlassian's remote MCP server.",
        category: "Project Management",
        tier: 2,
        icon: "atlassian",
        credential_fields: &[],
        docs_url: "https://support.atlassian.com/atlassian-rovo-mcp-server/",
        repo_url: "https://support.atlassian.com/atlassian-rovo-mcp-server/",
        default_prompt: "Use the Atlassian MCP to read Jira issues and Confluence pages for context. Do not create, edit or transition issues or pages unless explicitly asked.",
        recommended_permissions: "Sign in with an account that can read the projects and spaces agents need",
        npx_package: "",
        env_mapping: &[],
        remote_url: "https://mcp.atlassian.com/v1/mcp",
        remote_key: false,
        key_unavailable: "",
    },
];

/// The catalog entry for a server id.
pub fn find(id: &str) -> Option<&'static McpServerDef> {
    CATALOG.iter().find(|p| p.id == id)
}

/// Where a person creates the key for a catalog server, in plain words, for
/// when its sign-in can't be used. Falls back to the server's API docs.
pub fn key_help(id: &str) -> Option<String> {
    let place = match id {
        "figma" => "Figma, Settings, Security, Personal access tokens (https://help.figma.com/hc/en-us/articles/8085703771159)",
        "vercel" => "Vercel, Account Settings, Tokens (https://vercel.com/account/settings/tokens)",
        "stripe" => "the Stripe Dashboard as an agent API key (https://docs.stripe.com/keys#agent-keys)",
        "sentry" => "Sentry, User Settings, Personal Tokens (https://sentry.io/settings/account/api/auth-tokens/)",
        "linear" => "Linear, Settings, Security & access, Personal API keys (https://linear.app/settings/account/security)",
        "notion" => "Notion, Settings, Connections, Develop or manage integrations (https://www.notion.so/profile/integrations)",
        "gitlab" => "GitLab, Preferences, Access tokens (https://gitlab.com/-/user_settings/personal_access_tokens)",
        "neon" => "the Neon Console, Account settings, API keys (https://console.neon.tech/app/settings/api-keys)",
        _ => {
            let p = find(id)?;
            return (!p.docs_url.is_empty()).then(|| {
                format!("Create one as {}'s API docs describe: {}", p.name, p.docs_url)
            });
        }
    };
    Some(format!(
        "Create one in {place}, then paste it under Use a key instead."
    ))
}

/// The catalog's server ids, in catalog order.
pub fn ids() -> impl Iterator<Item = &'static str> {
    CATALOG.iter().map(|p| p.id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_unique_and_every_server_maps_its_credentials() {
        let mut seen = std::collections::HashSet::new();
        for p in CATALOG {
            assert!(seen.insert(p.id), "{} listed twice", p.id);
            // A server is run locally (npx) or reached remotely, or says
            // plainly why it can't be used with a key.
            assert!(
                !p.npx_package.is_empty()
                    || !p.remote_url.is_empty()
                    || !p.key_unavailable.is_empty(),
                "{}",
                p.id
            );
            // A key goes to the remote server only when there is one.
            assert!(!p.remote_key || !p.remote_url.is_empty(), "{}", p.id);
            // Every entry has exactly one key story: a package, the remote
            // server taking the key, a reason there is none, or (Sanity) a
            // key kept for Knowledge.
            let key_paths = [
                !p.npx_package.is_empty(),
                p.remote_key,
                !p.key_unavailable.is_empty(),
            ]
            .iter()
            .filter(|b| **b)
            .count();
            assert!(key_paths <= 1, "{}: more than one key path", p.id);
            // A package's env mapping only exists with the package.
            assert!(
                !p.npx_package.is_empty() || p.env_mapping.is_empty() || p.id == "sanity",
                "{}",
                p.id
            );
            for (key, env) in p.env_mapping {
                assert!(
                    p.credential_fields.iter().any(|f| f.key == *key),
                    "{}: {key} maps to {env} but is not a credential field",
                    p.id
                );
            }
        }
        assert!(find("sanity").is_some());
        assert_eq!(find("nope").map(|p| p.id), None);
    }

    #[test]
    fn the_proxy_allowlist_is_exactly_the_catalogs_packages() {
        // Every package the catalog runs is allowed, and nothing else is: a
        // stale name there would be a package anyone could publish.
        let proxy = include_str!("../../../lambda/mcp-proxy/handler.mjs");
        let start = proxy.find("ALLOWED_PACKAGES = new Set([").unwrap();
        let end = start + proxy[start..].find("]);").unwrap();
        let allowed: std::collections::BTreeSet<&str> =
            proxy[start..end].split('"').skip(1).step_by(2).collect();
        let catalog: std::collections::BTreeSet<&str> = CATALOG
            .iter()
            .map(|p| p.npx_package)
            .filter(|p| !p.is_empty())
            .collect();
        assert_eq!(allowed, catalog);
    }

    #[test]
    fn the_proxy_lambda_knows_every_server() {
        let proxy = include_str!("../../../lambda/mcp-proxy/handler.mjs");
        for p in CATALOG.iter().filter(|p| !p.npx_package.is_empty()) {
            assert!(
                proxy.contains(p.npx_package),
                "lambda/mcp-proxy/handler.mjs doesn't allow {} ({})",
                p.npx_package,
                p.id
            );
        }
    }
}
