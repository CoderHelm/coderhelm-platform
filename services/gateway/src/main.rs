use axum::{
    http::header,
    middleware as axum_middleware,
    routing::{delete, get, post, put},
    Router,
};
use lambda_http::{run, Error};
use std::sync::Arc;
use tower_http::set_header::SetResponseHeaderLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod auth;
mod clients;
mod middleware;
mod models;
mod routes;

pub struct AppState {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub sqs: aws_sdk_sqs::Client,
    pub ses: aws_sdk_sesv2::Client,
    pub s3: aws_sdk_s3::Client,
    pub cognito: aws_sdk_cognitoidentityprovider::Client,
    pub lambda: aws_sdk_lambda::Client,
    pub http: reqwest::Client,
    pub secrets: models::Secrets,
    pub config: models::Config,
    pub cognito_client_secret: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Structured JSON logging for CloudWatch
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer().json().flatten_event(true))
        .init();

    // Initialize AWS SDK
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let dynamo = aws_sdk_dynamodb::Client::new(&aws_config);
    let sqs = aws_sdk_sqs::Client::new(&aws_config);
    let ses = aws_sdk_sesv2::Client::new(&aws_config);
    let s3 = aws_sdk_s3::Client::new(&aws_config);
    let cognito = aws_sdk_cognitoidentityprovider::Client::new(&aws_config);
    let lambda_client = aws_sdk_lambda::Client::new(&aws_config);
    let sm = aws_sdk_secretsmanager::Client::new(&aws_config);

    // Load secrets from Secrets Manager
    let secrets_name =
        std::env::var("SECRETS_NAME").unwrap_or_else(|_| "coderhelm/prod/secrets".to_string());
    let secrets = models::Secrets::load(&sm, &secrets_name).await?;

    let config = models::Config::from_env();

    // Only needed for the main gateway, not the streaming Lambda
    let cognito_client_secret = if std::env::var("STREAMING_MODE").is_ok() {
        String::new()
    } else {
        cognito
            .describe_user_pool_client()
            .user_pool_id(&config.cognito_user_pool_id)
            .client_id(&config.cognito_client_id)
            .send()
            .await
            .ok()
            .and_then(|r| r.user_pool_client)
            .and_then(|c| c.client_secret)
            .unwrap_or_default()
    };

    let state = Arc::new(AppState {
        dynamo,
        sqs,
        ses,
        s3,
        cognito,
        lambda: lambda_client,
        http: reqwest::Client::new(),
        secrets,
        config,
        cognito_client_secret,
    });

    // Sync MCP server catalog to S3 on cold start (skip for streaming Lambda)
    if std::env::var("STREAMING_MODE").is_err() {
        routes::plugins::sync_catalog_to_s3(&state).await;
    }

    // Build router
    // Protected API routes — require valid JWT with team scoping
    let api_routes = Router::new()
        .route("/me", get(routes::api::me))
        .route("/health", get(routes::api::health))
        .route("/teams", get(routes::api::list_teams))
        .route("/teams/switch", post(routes::api::switch_team))
        .route("/teams/rename", put(routes::api::rename_team))
        .route(
            "/allowlist",
            get(routes::api::list_allowlist)
                .post(routes::api::add_to_allowlist)
                .delete(routes::api::remove_from_allowlist),
        )
        .route("/runs", get(routes::api::list_runs))
        .route("/runs/:run_id", get(routes::api::get_run))
        .route("/runs/:run_id/traces", get(routes::api::get_run_traces))
        .route("/runs/:run_id/agent-log", get(routes::api::get_agent_log))
        .route("/runs/:run_id/openspec", get(routes::api::get_run_openspec))
        .route("/runs/:run_id/retry", post(routes::api::retry_run))
        .route("/runs/:run_id/re-review", post(routes::api::re_review_run))
        .route("/runs/:run_id/cancel", post(routes::api::cancel_run))
        .route("/repos", get(routes::api::list_repos))
        .route("/repos/sync", post(routes::api::sync_repos))
        .route(
            "/repos/:owner/:name",
            post(routes::api::update_repo).delete(routes::api::delete_repo),
        )
        .route(
            "/repos/:owner/:name/regenerate",
            post(routes::api::regenerate_repo),
        )
        .route(
            "/integrations/jira/check",
            get(routes::api::get_jira_integration_check)
                .post(routes::api::validate_jira_integration_payload)
                .delete(routes::api::delete_jira_integration),
        )
        .route(
            "/integrations/jira/secret",
            post(routes::api::generate_jira_secret).delete(routes::api::delete_jira_secret),
        )
        .route(
            "/integrations/jira/config",
            get(routes::api::get_jira_config).put(routes::api::update_jira_config),
        )
        .route(
            "/integrations/jira/projects",
            put(routes::api::update_jira_projects),
        )
        .route(
            "/integrations/jira/projects/fetch",
            get(routes::api::fetch_jira_projects),
        )
        .route(
            "/integrations/jira/events",
            get(routes::api::get_jira_events),
        )
        .route("/stats", get(routes::api::get_stats))
        .route("/stats/history", get(routes::api::get_stats_history))
        // GitHub App installation
        .route(
            "/github/link-installation",
            post(routes::github::link_installation),
        )
        .route(
            "/github/installation-status",
            get(routes::github::installation_status),
        )
        .route(
            "/notifications",
            get(routes::api::get_notification_prefs).put(routes::api::update_notification_prefs),
        )
        .route(
            "/instructions/global",
            get(routes::api::get_global_instructions).put(routes::api::update_global_instructions),
        )
        .route(
            "/instructions/repo/:owner/:name",
            get(routes::api::get_repo_instructions).put(routes::api::update_repo_instructions),
        )
        // Must-rules endpoints
        .route(
            "/rules/global",
            get(routes::api::get_global_rules).put(routes::api::update_global_rules),
        )
        .route(
            "/rules/repo/:owner/:name",
            get(routes::api::get_repo_rules).put(routes::api::update_repo_rules),
        )
        // Voice settings (global + per-repo)
        .route(
            "/voice/global",
            get(routes::api::get_global_voice).put(routes::api::update_global_voice),
        )
        .route(
            "/voice/repo/:owner/:name",
            get(routes::api::get_repo_voice).put(routes::api::update_repo_voice),
        )
        // Agents context (global + per-repo)
        .route(
            "/agents/global",
            get(routes::api::get_global_agents).put(routes::api::update_global_agents),
        )
        .route(
            "/agents/repo/:owner/:name",
            get(routes::api::get_repo_agents).put(routes::api::update_repo_agents),
        )
        // Budget settings
        .route(
            "/settings/budget",
            get(routes::api::get_budget).put(routes::api::update_budget),
        )
        // Workflow settings
        .route(
            "/settings/workflow",
            get(routes::api::get_workflow_settings).put(routes::api::update_workflow_settings),
        )
        // Model provider settings
        .route(
            "/settings/model-provider",
            get(routes::api::get_model_provider)
                .put(routes::api::update_model_provider)
                .delete(routes::api::delete_model_provider),
        )
        // Usage
        .route("/usage", get(routes::api::get_usage))
        // Banners
        .route("/banners", get(routes::banners::list_banners))
        // Account management
        .route("/account/reset", post(routes::api::reset_account))
        // User management
        .route("/users", get(routes::users::list_users))
        .route("/users/invite", post(routes::users::invite_user))
        .route("/users/:user_id/role", put(routes::users::update_role))
        .route("/users/:user_id/resend", post(routes::users::resend_invite))
        .route("/users/:user_id", delete(routes::users::remove_user))
        .route("/users/password", put(routes::users::change_password))
        .route("/users/mfa/setup", post(routes::users::mfa_setup))
        .route("/users/mfa/verify", post(routes::users::mfa_verify_setup))
        .route("/users/mfa", delete(routes::users::mfa_disable))
        // Plan endpoints
        .route(
            "/plans",
            get(routes::plans::list_plans).post(routes::plans::create_plan),
        )
        .route("/plans/chat", post(routes::plans::plan_chat))
        .route("/plans/chat/token", post(routes::plans::stream_token))
        // Template endpoints (must be before /plans/:plan_id to avoid wildcard capture)
        .route(
            "/plans/templates",
            get(routes::plans::list_templates).post(routes::plans::create_template),
        )
        .route(
            "/plans/templates/from-plan/:plan_id",
            post(routes::plans::create_template_from_plan),
        )
        .route(
            "/plans/templates/:template_id",
            get(routes::plans::get_template).delete(routes::plans::delete_template),
        )
        .route(
            "/plans/templates/:template_id/use",
            post(routes::plans::use_template),
        )
        .route(
            "/plans/:plan_id",
            get(routes::plans::get_plan)
                .put(routes::plans::update_plan)
                .delete(routes::plans::delete_plan),
        )
        .route("/plans/:plan_id/tasks", post(routes::plans::add_task))
        .route(
            "/plans/:plan_id/tasks/:task_id",
            put(routes::plans::update_task).delete(routes::plans::delete_task),
        )
        .route(
            "/plans/:plan_id/tasks/:task_id/approve",
            post(routes::plans::approve_task),
        )
        .route(
            "/plans/:plan_id/tasks/:task_id/reject",
            post(routes::plans::reject_task),
        )
        .route("/plans/:plan_id/execute", post(routes::plans::execute_plan))
        .route(
            "/plans/:plan_id/tasks/:task_id/force-run",
            post(routes::plans::force_run_task),
        )
        .route(
            "/plans/:plan_id/approve-and-execute",
            post(routes::plans::approve_all_and_execute),
        )
        .route(
            "/plans/:plan_id/openspec",
            get(routes::plans::get_plan_openspec),
        )
        .route(
            "/plans/:plan_id/openspec/generate",
            post(routes::plans::generate_plan_openspec),
        )
        .route(
            "/infrastructure",
            get(routes::infrastructure::get_infrastructure),
        )
        .route(
            "/infrastructure/refresh",
            post(routes::infrastructure::refresh_infrastructure),
        )
        .route(
            "/infrastructure/repo/:owner/:name",
            get(routes::infrastructure::get_repo_infrastructure),
        )
        .route(
            "/infrastructure/repo/:owner/:name/refresh",
            post(routes::infrastructure::refresh_repo_infrastructure),
        )
        // AWS Connections (Log Analyzer)
        .route(
            "/aws-connections",
            get(routes::log_analyzer::list_connections)
                .post(routes::log_analyzer::create_connection),
        )
        .route(
            "/aws-connections/cfn-url",
            get(routes::log_analyzer::get_cfn_url),
        )
        .route(
            "/aws-connections/:id",
            put(routes::log_analyzer::update_connection)
                .delete(routes::log_analyzer::delete_connection),
        )
        .route(
            "/aws-connections/:id/test",
            post(routes::log_analyzer::test_connection),
        )
        .route(
            "/aws-connections/:id/log-groups",
            get(routes::log_analyzer::discover_log_groups),
        )
        // Recommendations
        .route(
            "/recommendations",
            get(routes::log_analyzer::list_recommendations),
        )
        .route(
            "/recommendations/:id/plan",
            post(routes::log_analyzer::create_plan_from_recommendation),
        )
        .route(
            "/recommendations/:id/dismiss",
            post(routes::log_analyzer::dismiss_recommendation),
        )
        // MCP Servers
        .route("/plugins/catalog", get(routes::plugins::list_catalog))
        .route("/plugins", get(routes::plugins::list_enabled))
        .route("/plugins/:id/enable", post(routes::plugins::enable_plugin))
        .route("/plugins/:id", delete(routes::plugins::disable_plugin))
        .route(
            "/plugins/:id/credentials",
            put(routes::plugins::update_credentials),
        )
        .route("/plugins/:id/prompt", put(routes::plugins::update_prompt))
        .route("/plugins/:id/test", post(routes::plugins::test_connection))
        // Memory browser
        .route("/memories", get(routes::memory::list_memories))
        .route(
            "/memories/stats",
            get(routes::memory::memory_stats),
        )
        .route(
            "/memories/:memory_id",
            delete(routes::memory::delete_memory),
        )
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            middleware::auth::require_auth,
        ));

    // Webhook routes (public, verified by signature)
    let webhook_routes = Router::new()
        .route("/github", post(routes::github_webhook::handle))
        .route("/jira/:token", post(routes::jira_webhook::handle))
        .route("/jira", post(routes::jira_webhook::handle_forge));

    // Auth routes (public, rate-limited)
    let auth_routes = Router::new()
        .route("/signup", post(routes::auth::signup))
        .route("/waitlist", post(routes::auth::join_waitlist))
        .route("/login", post(routes::auth::login_email))
        .route("/verify-email", post(routes::auth::verify_email))
        .route("/forgot-password", post(routes::auth::forgot_password))
        .route("/confirm-reset", post(routes::auth::confirm_reset))
        .route("/mfa/verify", post(routes::auth::mfa_verify))
        .route("/google", get(routes::auth::google_login))
        .route("/google/callback", get(routes::auth::google_callback))
        .route("/github", get(routes::auth::github_login))
        .route("/github/callback", get(routes::auth::github_callback))
        .route("/logout", post(routes::auth::logout))
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            middleware::rate_limit::rate_limit_auth,
        ));

    let app = Router::new()
        // Root health/info
        .route(
            "/",
            get(|| async {
                axum::Json(serde_json::json!({ "service": "coderhelm", "status": "ok" }))
            }),
        )
        // Streaming endpoint uses Bearer JWT, not cookies — must be outside require_auth
        .route(
            "/api/plans/chat/stream",
            post(routes::plans::plan_chat_stream),
        )
        .nest("/webhooks", webhook_routes)
        .route(
            "/integrations/jira/forge-register",
            post(routes::api::forge_register_urls),
        )
        .nest("/auth", auth_routes)
        // Nest protected routes under /api
        .nest("/api", api_routes)
        .layer(SetResponseHeaderLayer::overriding(
            header::CACHE_CONTROL,
            header::HeaderValue::from_static("no-store"),
        ))
        .with_state(state);

    // In streaming mode the Lambda Web Adapter proxies requests to port 8080,
    // so we run a plain TCP server instead of the Lambda Runtime API handler.
    if std::env::var("STREAMING_MODE").is_ok() {
        let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
        tracing::info!("Streaming mode: listening on 0.0.0.0:8080");
        axum::serve(listener, app).await?;
        Ok(())
    } else {
        run(app).await
    }
}
