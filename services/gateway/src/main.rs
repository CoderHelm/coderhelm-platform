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
        http: reqwest::Client::new(),
        secrets,
        config,
    });

    // Build router
    // Protected API routes — require valid JWT with tenant scoping
    let api_routes = Router::new()
        .route("/me", get(routes::api::me))
        .route("/health", get(routes::api::health))
        .route("/runs", get(routes::api::list_runs))
        .route("/runs/:run_id", get(routes::api::get_run))
        .route("/runs/:run_id/openspec", get(routes::api::get_run_openspec))
        .route("/repos", get(routes::api::list_repos))
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
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            middleware::auth::require_auth,
        ));

    let app = Router::new()
        // Webhooks (public, verified by signature)
        .route("/webhooks/github", post(routes::github_webhook::handle))
        .route("/webhooks/jira", post(routes::jira_webhook::handle))
        .route("/webhooks/stripe", post(routes::stripe_webhook::handle))
        // Auth (public)
        .route("/auth/login", get(routes::auth::login))
        .route("/auth/callback", get(routes::auth::callback))
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
