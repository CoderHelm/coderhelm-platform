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
    pub bedrock: aws_sdk_bedrockruntime::Client,
    pub cognito: aws_sdk_cognitoidentityprovider::Client,
    pub lambda: aws_sdk_lambda::Client,
    pub http: reqwest::Client,
    pub secrets: models::Secrets,
    pub config: models::Config,
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
    let bedrock = aws_sdk_bedrockruntime::Client::new(&aws_config);
    let cognito = aws_sdk_cognitoidentityprovider::Client::new(&aws_config);
    let lambda_client = aws_sdk_lambda::Client::new(&aws_config);
    let sm = aws_sdk_secretsmanager::Client::new(&aws_config);

    // Load secrets from Secrets Manager
    let secrets_name =
        std::env::var("SECRETS_NAME").unwrap_or_else(|_| "coderhelm/prod/secrets".to_string());
    let secrets = models::Secrets::load(&sm, &secrets_name).await?;

    let config = models::Config::from_env();

    let state = Arc::new(AppState {
        dynamo,
        sqs,
        ses,
        s3,
        bedrock,
        cognito,
        lambda: lambda_client,
        http: reqwest::Client::new(),
        secrets,
        config,
    });

    // Sync MCP server catalog to S3 on cold start
    routes::plugins::sync_catalog_to_s3(&state).await;

    // Build router
    // Protected API routes — require valid JWT with tenant scoping
    let api_routes = Router::new()
        .route("/me", get(routes::api::me))
        .route("/health", get(routes::api::health))
        .route("/tenants", get(routes::api::list_tenants))
        .route("/tenants/switch", post(routes::api::switch_tenant))
        .route("/tenants/rename", put(routes::api::rename_tenant))
        .route(
            "/allowlist",
            get(routes::api::list_allowlist)
                .post(routes::api::add_to_allowlist)
                .delete(routes::api::remove_from_allowlist),
        )
        .route("/runs", get(routes::api::list_runs))
        .route("/runs/:run_id", get(routes::api::get_run))
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
                .post(routes::api::validate_jira_integration_payload),
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
        // Billing endpoints
        .route("/billing", get(routes::billing::get_billing))
        .route(
            "/billing/portal",
            post(routes::billing::create_portal_session),
        )
        .route(
            "/billing/subscribe",
            post(routes::billing::create_subscription),
        )
        .route(
            "/billing/cancel",
            post(routes::billing::cancel_subscription),
        )
        .route(
            "/billing/reactivate",
            post(routes::billing::reactivate_subscription),
        )
        .route(
            "/billing/payment-method",
            post(routes::billing::create_setup_intent),
        )
        .route(
            "/billing/payment-methods",
            get(routes::billing::list_payment_methods),
        )
        .route(
            "/billing/payment-methods/:pm_id",
            delete(routes::billing::delete_payment_method),
        )
        .route(
            "/billing/payment-methods/:pm_id/default",
            put(routes::billing::set_default_payment_method),
        )
        .route(
            "/billing/customer",
            get(routes::billing::get_billing_customer),
        )
        .route("/billing/email", put(routes::billing::update_billing_email))
        .route("/billing/invoices", get(routes::billing::list_invoices))
        .route(
            "/billing/invoices/:invoice_id/pdf",
            get(routes::billing::download_invoice_pdf),
        )
        // Banners
        .route("/banners", get(routes::banners::list_banners))
        // Account management
        .route("/account/reset", post(routes::api::reset_account))
        // User management
        .route("/users", get(routes::users::list_users))
        .route("/users/invite", post(routes::users::invite_user))
        .route("/users/:user_id/role", put(routes::users::update_role))
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
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            middleware::auth::require_auth,
        ));

    let app = Router::new()
        // Root health/info
        .route(
            "/",
            get(|| async {
                axum::Json(serde_json::json!({ "service": "coderhelm", "status": "ok" }))
            }),
        )
        // Webhooks (public, verified by signature)
        .route("/webhooks/github", post(routes::github_webhook::handle))
        .route("/webhooks/jira/:token", post(routes::jira_webhook::handle))
        .route("/webhooks/jira", post(routes::jira_webhook::handle_forge))
        .route(
            "/integrations/jira/forge-register",
            post(routes::api::forge_register_urls),
        )
        .route("/webhooks/stripe", post(routes::stripe_webhook::handle))
        // Auth (public)
        .route("/auth/signup", post(routes::auth::signup))
        .route("/auth/waitlist", post(routes::auth::join_waitlist))
        .route("/auth/login", post(routes::auth::login_email))
        .route("/auth/verify-email", post(routes::auth::verify_email))
        .route("/auth/forgot-password", post(routes::auth::forgot_password))
        .route("/auth/confirm-reset", post(routes::auth::confirm_reset))
        .route("/auth/mfa/verify", post(routes::auth::mfa_verify))
        .route("/auth/google", get(routes::auth::google_login))
        .route("/auth/google/callback", get(routes::auth::google_callback))
        .route("/auth/github", get(routes::auth::github_login))
        .route("/auth/github/callback", get(routes::auth::github_callback))
        .route("/auth/logout", post(routes::auth::logout))
        // Nest protected routes under /api
        .nest("/api", api_routes)
        .layer(SetResponseHeaderLayer::overriding(
            header::CACHE_CONTROL,
            header::HeaderValue::from_static("no-store"),
        ))
        .with_state(state);

    run(app).await
}
