use axum::{
    extract::State,
    http::header,
    middleware as axum_middleware,
    routing::{delete, get, post, put},
    Router,
};
use lambda_http::{run, Error};
use std::sync::Arc;
use tower_http::set_header::SetResponseHeaderLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod api_keys;
mod auth;
mod clients;
mod middleware;
mod model_call;
mod models;
mod openapi;
mod permissions;
mod routes;

pub struct AppState {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub sqs: aws_sdk_sqs::Client,
    pub ses: aws_sdk_sesv2::Client,
    pub s3: aws_sdk_s3::Client,
    pub cognito: aws_sdk_cognitoidentityprovider::Client,
    pub lambda: aws_sdk_lambda::Client,
    pub kms: aws_sdk_kms::Client,
    pub sts: aws_sdk_sts::Client,
    pub bedrock: aws_sdk_bedrockruntime::Client,
    /// Plugin schedules (publish at a time, recurring triggers).
    pub scheduler: common::plugins::schedule::aws_sdk_scheduler::Client,
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
    let kms = aws_sdk_kms::Client::new(&aws_config);
    let sts = aws_sdk_sts::Client::new(&aws_config);
    let bedrock = aws_sdk_bedrockruntime::Client::new(&aws_config);
    // Claude through Bedrock: set on every install, so a switch to Bedrock
    // in Settings needs no restart.
    common::model_provider::set_bedrock_client(bedrock.clone());
    let scheduler = common::plugins::schedule::aws_sdk_scheduler::Client::new(&aws_config);
    let sm = aws_sdk_secretsmanager::Client::new(&aws_config);
    // Where this deployment's dashboard and API are (config, or SSM).
    common::public_urls::init(common::public_urls::ssm::Client::new(&aws_config)).await;

    // Load secrets from Secrets Manager
    let secrets_name =
        std::env::var("SECRETS_NAME").unwrap_or_else(|_| "coderhelm/prod/secrets".to_string());
    let secrets = models::Secrets::load(&sm, &secrets_name).await?;

    // A hybrid install's registration with CoderHelm (none on other installs).
    common::hybrid::client::set_credentials(common::hybrid::client::Credentials::from_parts(
        secrets.hybrid_install_id.as_deref(),
        secrets.hybrid_install_secret.as_deref(),
        secrets.hybrid_relay_key.as_deref(),
    ));

    let config = models::Config::from_env();
    // A model call whose usage can't be written waits on the tickets queue.
    common::usage::set_fallback(Arc::new(common::usage::SqsFallback {
        sqs: sqs.clone(),
        queue_url: config.ticket_queue_url.clone(),
    }));

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
        kms,
        sts,
        bedrock,
        scheduler,
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
        .route("/dlq", get(routes::dlq::list_dlq_messages))
        .route("/dlq/redrive", post(routes::dlq::redrive_dlq_message))
        .route("/dlq/delete", post(routes::dlq::delete_dlq_message))
        .route("/dlq/purge", post(routes::dlq::purge_dlq))
        .route("/teams", get(routes::api::list_teams))
        .route("/teams/switch", post(routes::api::switch_team))
        .route("/teams/rename", put(routes::api::rename_team))
        .route(
            "/allowlist",
            get(routes::api::list_allowlist)
                .post(routes::api::add_to_allowlist)
                .delete(routes::api::remove_from_allowlist),
        )
        .route("/admin/teams", get(routes::admin::admin_teams))
        // The install's version and updates (self-hosted installs).
        .route("/system/version", get(routes::system::version))
        .route("/system/auto-update", put(routes::system::set_auto_update))
        .route("/system/update", post(routes::system::update_now))
        .route("/system/rollback", post(routes::system::rollback))
        .route("/system/test-email", post(routes::system::test_email))
        // The model check before an update, run again now ("Check again").
        .route("/system/update-check", post(routes::system::check_update))
        // How the install calls Claude: Bedrock or Anthropic API keys,
        // switched at any time once every model answers.
        .route(
            "/system/models",
            get(routes::system_models::get_models).put(routes::system_models::set_models),
        )
        .route(
            "/system/models/check",
            post(routes::system_models::check_models),
        )
        // Your own domain: status and records, set, back to the AWS address.
        .route(
            "/system/domain",
            get(routes::system::domain)
                .put(routes::system::set_domain)
                .delete(routes::system::remove_domain),
        )
        // Hybrid: CoderHelm's address, GitHub App relay, integrations and
        // delivery sharing (install admins; routes::hybrid_install).
        .route(
            "/system/hybrid",
            get(routes::hybrid_install::get_status).delete(routes::hybrid_install::unregister),
        )
        .route(
            "/system/hybrid/register",
            post(routes::hybrid_install::register),
        )
        .route(
            "/system/hybrid/names/:name",
            get(routes::hybrid_install::name_available),
        )
        .route(
            "/system/hybrid/name",
            put(routes::hybrid_install::claim_name).delete(routes::hybrid_install::release_name),
        )
        .route(
            "/system/hybrid/github/link",
            post(routes::hybrid_install::github_link),
        )
        .route(
            "/system/hybrid/github/:installation_id",
            put(routes::hybrid_install::github_repositories)
                .delete(routes::hybrid_install::github_unlink),
        )
        .route(
            "/system/hybrid/settings",
            put(routes::hybrid_install::put_settings),
        )
        // Deliveries: every event in and out, with what happened (admins).
        .route("/deliveries", get(routes::deliveries::list))
        .route("/deliveries/:id", get(routes::deliveries::get))
        .route(
            "/deliveries/:id/redeliver",
            post(routes::deliveries::redeliver),
        )
        // Hybrid installs, for CoderHelm's operators (hosted side).
        .route("/admin/hybrid", get(routes::hybrid::admin_list))
        .route(
            "/admin/hybrid/:install_id/revoke",
            post(routes::hybrid::admin_revoke),
        )
        // Setup checks and update readiness, each with its fix (install admins).
        .route("/system/checks", get(routes::setup_checks::checks))
        .route(
            "/system/preflight",
            get(routes::setup_checks::preflight_route),
        )
        .route(
            "/admin/teams/:team_id/flags",
            put(routes::admin::set_team_flags),
        )
        // The shared DLQ: every team's failed messages and the global jobs'
        .route("/admin/dlq", get(routes::admin::admin_dlq))
        // The daily check of recorded usage against Anthropic's report
        .route("/admin/usage-check", get(routes::admin::admin_usage_check))
        // Which sign-in apps (Notion, Atlassian, Google Drive) the install has
        .route(
            "/admin/sign-in-apps",
            get(routes::knowledge_tokens::sign_in_apps),
        )
        .route("/admin/dlq/delete", post(routes::admin::admin_dlq_delete))
        .route("/admin/beta-signups", get(routes::admin::beta_signups))
        .route(
            "/admin/beta-signups/invite",
            post(routes::admin::invite_beta_signup),
        )
        // Support tickets: the team's own, and the platform admin's queue
        .route(
            "/support/tickets",
            get(routes::support::list).post(routes::support::create_handler),
        )
        .route("/support/tickets/:id", get(routes::support::get))
        .route("/support/tickets/:id/reply", post(routes::support::reply))
        .route(
            "/support/tickets/:id/resolve",
            post(routes::support::resolve),
        )
        .route(
            "/support/tickets/:id/participants",
            post(routes::support::add_people),
        )
        .route(
            "/support/tickets/:id/participants/remove",
            post(routes::support::remove_people),
        )
        .route("/support/badge", get(routes::support::badge))
        .route(
            "/admin/support/tickets",
            get(routes::support::admin_list_handler),
        )
        .route(
            "/admin/support/tickets/:id",
            get(routes::support::admin_get),
        )
        .route(
            "/admin/support/tickets/:id/reply",
            post(routes::support::admin_reply_handler),
        )
        .route(
            "/admin/support/tickets/:id/status",
            post(routes::support::admin_status),
        )
        .route(
            "/admin/support/tickets/:id/participants",
            post(routes::support::admin_add_people),
        )
        .route(
            "/admin/support/tickets/:id/participants/remove",
            post(routes::support::admin_remove_people),
        )
        // Support access: the team lets support in for a window, and a
        // platform admin opens a read-only support view while it is on.
        .route(
            "/support/access",
            get(routes::support_access::get_access)
                .post(routes::support_access::grant_access)
                .delete(routes::support_access::revoke_access),
        )
        .route(
            "/support/view",
            get(routes::support_access::view_status).delete(routes::support_access::exit_view),
        )
        .route(
            "/admin/support/view",
            post(routes::support_access::start_view),
        )
        .route("/search", get(routes::search::search))
        .route("/runs", get(routes::api::list_runs))
        .route("/runs/:run_id", get(routes::api::get_run))
        .route("/runs/:run_id/traces", get(routes::api::get_run_traces))
        .route("/runs/:run_id/agent-log", get(routes::api::get_agent_log))
        .route("/runs/:run_id/openspec", get(routes::api::get_run_openspec))
        .route("/runs/:run_id/retry", post(routes::api::retry_run))
        .route("/runs/:run_id/reset", post(routes::api::reset_and_rerun))
        .route("/runs/:run_id/re-review", post(routes::api::re_review_run))
        .route("/runs/:run_id/cancel", post(routes::api::cancel_run))
        .route("/runs/:run_id/close", post(routes::run_close::close_run))
        .route("/runs/:run_id/reopen", post(routes::run_close::reopen_run))
        .route("/runs/close", post(routes::run_close::close_runs))
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
        .route("/repos/:owner/:name/role", post(routes::api::set_repo_role))
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
        .route("/analytics", get(routes::analytics::get_analytics))
        // Reviewer agent — per-repo config, reviews list/detail, ratings
        .route(
            "/reviewer/org-config",
            get(routes::reviewer::get_org_config).put(routes::reviewer::update_org_config),
        )
        .route(
            "/reviewer/config/:owner/:name",
            get(routes::reviewer::get_config).put(routes::reviewer::update_config),
        )
        .route(
            "/alert-routes",
            get(routes::alerts::list_routes)
                .put(routes::alerts::put_route)
                .delete(routes::alerts::delete_route),
        )
        .route(
            "/alert-routes/alarms",
            get(routes::alerts::list_topic_alarms),
        )
        .route(
            "/alert-routes/resolve-repo",
            post(routes::alerts::resolve_repo),
        )
        .route(
            "/alert-budget",
            get(routes::alerts::get_budget).put(routes::alerts::put_budget),
        )
        .route(
            "/alert-notify",
            get(routes::alerts::get_notify).put(routes::alerts::put_notify),
        )
        .route("/alert-notify/test", post(routes::alerts::test_notify))
        .route("/notify/test", post(routes::alerts::test_notify))
        .route("/alerts", get(routes::alerts::list_events))
        .route("/alerts/event", get(routes::alerts::get_event))
        .route("/reviewer/reviews", get(routes::reviewer::list_reviews))
        .route("/reviewer/review", get(routes::reviewer::get_review))
        .route("/reviewer/live", get(routes::reviewer::live_prs))
        .route("/reviewer/open-prs", get(routes::reviewer::open_prs))
        .route(
            "/reviewer/gate",
            get(routes::reviewer::get_gate).post(routes::reviewer::control_gate),
        )
        .route("/reviewer/review/rate", post(routes::reviewer::rate_review))
        .route("/reviewer/re-review", post(routes::reviewer::re_review))
        // What the reviewer learned from the team's pushback
        .route(
            "/reviewer/lessons",
            get(routes::review_lessons::list_lessons).delete(routes::review_lessons::delete_lesson),
        )
        // What CoderHelm learned from outcomes, and the admin controls
        .route("/learning", get(routes::learning::get_learning))
        .route("/learning/pin", post(routes::learning::pin_variant))
        .route("/learning/revert", post(routes::learning::revert_change))
        .route("/learning/restore", post(routes::learning::restore_change))
        .route("/learning/weights", put(routes::learning::put_weights))
        .route("/releases", get(routes::reviewer::list_releases))
        .route("/releases/send", post(routes::reviewer::send_release))
        .route("/releases/pending", get(routes::reviewer::pending_releases))
        .route("/releases/now", post(routes::reviewer::release_now))
        // Code graph — its own feature (serves the planner, the coder, CI
        // fixes, review replies and the reviewer)
        .route("/graph/repos", get(routes::graph::list_repos))
        .route("/graph/status", get(routes::graph::status))
        .route("/graph/symbol", get(routes::graph::symbol))
        .route("/graph/neighborhood", get(routes::graph::neighborhood))
        .route(
            "/graph/config/:owner/:name",
            put(routes::graph::update_config),
        )
        .route("/graph/rebuild/:owner/:name", post(routes::graph::rebuild))
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
        // Branch, commit and PR title templates
        .route(
            "/settings/git-conventions",
            get(routes::api::get_git_conventions).put(routes::api::update_git_conventions),
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
        .route("/users/permissions", get(routes::users::get_permissions))
        // Custom roles: anyone sees them; built-in admins and owners manage them
        .route(
            "/roles",
            get(routes::roles::list_roles).post(routes::roles::create_role),
        )
        .route("/roles/audit", get(routes::roles::list_audit))
        // Team API keys: admins make, list and revoke them (dashboard only)
        .route(
            "/api-keys",
            get(routes::api_keys::list).post(routes::api_keys::create),
        )
        .route("/api-keys/:key_id", delete(routes::api_keys::revoke))
        .route(
            "/roles/:role_id",
            put(routes::roles::update_role).delete(routes::roles::delete_role),
        )
        .route("/users/invite", post(routes::users::invite_user))
        .route("/users/:user_id/role", put(routes::users::update_role))
        .route("/users/:user_id/resend", post(routes::users::resend_invite))
        .route(
            "/users/:user_id/link-github",
            post(routes::users::link_github),
        )
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
        // Knowledge bases (admins manage; readers see what their role may read)
        .route(
            "/knowledge",
            get(routes::knowledge::list_bases).post(routes::knowledge::create_base),
        )
        .route(
            "/knowledge/:kb_id",
            get(routes::knowledge::get_base)
                .put(routes::knowledge::update_base)
                .delete(routes::knowledge::delete_base),
        )
        .route(
            "/knowledge/:kb_id/files",
            post(routes::knowledge::start_upload),
        )
        .route(
            "/knowledge/:kb_id/files/:file_id/uploaded",
            post(routes::knowledge::finish_upload),
        )
        .route(
            "/knowledge/:kb_id/files/:file_id",
            delete(routes::knowledge::delete_file),
        )
        // Knowledge sources (connected apps that stay in sync).
        .route(
            "/knowledge/connectors",
            get(routes::knowledge_sources::connectors),
        )
        .route(
            "/knowledge/connectors/:kind",
            delete(routes::knowledge_sources::disconnect),
        )
        .route(
            "/knowledge/connectors/:kind/connect",
            post(routes::knowledge_sources::connect),
        )
        .route(
            "/knowledge/connectors/:kind/signin",
            post(routes::knowledge_sources::signin),
        )
        .route(
            "/knowledge/connectors/:kind/token",
            post(routes::knowledge_tokens::save_token),
        )
        .route(
            "/knowledge/connectors/:kind/browse",
            get(routes::knowledge_sources::browse),
        )
        .route(
            "/knowledge/:kb_id/sources",
            post(routes::knowledge_sources::create_source),
        )
        .route(
            "/knowledge/:kb_id/sources/:source_id",
            put(routes::knowledge_sources::update_source)
                .delete(routes::knowledge_sources::delete_source),
        )
        .route(
            "/knowledge/:kb_id/sources/:source_id/sync",
            post(routes::knowledge_sources::sync_source),
        )
        .route(
            "/knowledge/:kb_id/sources/:source_id/signin",
            post(routes::knowledge_sources::source_signin),
        )
        // The company brain chat (asking streams from /api/brain/stream)
        .route("/brain/threads", get(routes::brain::list_threads))
        .route(
            "/brain/threads/:thread_id",
            get(routes::brain::get_thread)
                .put(routes::brain::update_thread)
                .delete(routes::brain::delete_thread),
        )
        .route(
            "/brain/threads/:thread_id/messages/:seq/feedback",
            put(routes::brain::rate_message),
        )
        .route("/brain/sources", get(routes::brain::sources))
        .route("/brain/starters", get(routes::brain::starters))
        .route("/brain/token", post(routes::brain::stream_token))
        .route("/brain/attachments", post(routes::brain::create_attachment))
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
        .route(
            "/aws-connections/:id/abilities",
            put(routes::integrations::set_aws_abilities),
        )
        .route(
            "/aws-connections/:id/abilities/check",
            post(routes::integrations::check_aws),
        )
        .route("/integrations", get(routes::integrations::list))
        // Recommendations
        .route(
            "/recommendations",
            get(routes::log_analyzer::list_recommendations),
        )
        .route(
            "/recommendations/notify",
            get(routes::log_analyzer::get_rec_notify).put(routes::log_analyzer::put_rec_notify),
        )
        .route(
            "/recommendations/:id/plan",
            post(routes::log_analyzer::create_plan_from_recommendation),
        )
        .route(
            "/recommendations/:id/dismiss",
            post(routes::log_analyzer::dismiss_recommendation),
        )
        // Logs: the log analyzer's findings and the fix runs started from them
        .route("/log-findings", get(routes::log_findings::list_findings))
        .route(
            "/log-findings/autofix",
            get(routes::log_findings::get_autofix).put(routes::log_findings::put_autofix),
        )
        .route(
            "/log-findings/:id/samples",
            get(routes::log_findings::get_samples),
        )
        .route(
            "/log-findings/:id/fix",
            post(routes::log_findings::create_fix),
        )
        .route(
            "/log-findings/:id/repo",
            get(routes::log_findings::get_repo),
        )
        .route(
            "/log-findings/:id/ignore",
            post(routes::log_findings::ignore_finding),
        )
        .route(
            "/log-findings/:id/acknowledge",
            post(routes::log_findings::acknowledge_finding),
        )
        .route(
            "/log-findings/:id/reopen",
            post(routes::log_findings::reopen_finding),
        )
        // MCP Servers
        // Plugin packages (the MCP servers keep /plugins, /plugins/catalog
        // and /plugins/:id/... for the Connections page).
        .route("/plugins/flag", get(routes::plugin_pkgs::get_flag))
        .route(
            "/plugins/settings",
            get(routes::plugin_pkgs::get_settings).put(routes::plugin_pkgs::put_settings),
        )
        .route(
            "/plugins/sources",
            get(routes::plugin_pkgs::list_sources).post(routes::plugin_pkgs::add_source),
        )
        .route(
            "/plugins/sources/:id/sync",
            post(routes::plugin_pkgs::sync_source),
        )
        .route(
            "/plugins/sources/:id",
            delete(routes::plugin_pkgs::remove_source),
        )
        .route("/plugins/available", get(routes::plugin_pkgs::available))
        .route(
            "/plugins/install",
            post(routes::plugin_pkgs::install_plugin),
        )
        .route(
            "/plugins/installed",
            get(routes::plugin_pkgs::list_installed),
        )
        .route(
            "/plugins/installed/:id",
            get(routes::plugin_pkgs::get_installed).delete(routes::plugin_pkgs::uninstall),
        )
        .route(
            "/plugins/installed/:id/approve",
            post(routes::plugin_pkgs::approve_pending),
        )
        .route(
            "/plugins/installed/:id/decline",
            post(routes::plugin_pkgs::decline_pending),
        )
        .route(
            "/plugins/installed/:id/options",
            put(routes::plugin_pkgs::set_options),
        )
        .route(
            "/plugins/installed/:id/rollback",
            post(routes::plugin_pkgs::rollback),
        )
        .route(
            "/plugins/installed/:id/sync",
            post(routes::plugin_pkgs::sync_plugin),
        )
        .route(
            "/plugins/installed/:id/settings",
            put(routes::plugin_pkgs::set_plugin_settings),
        )
        .route(
            "/plugins/installed/:id/limit",
            put(routes::plugin_pkgs::set_limit),
        )
        .route(
            "/plugins/installed/:id/approvers",
            put(routes::plugin_pkgs::set_approvers),
        )
        .route(
            "/plugins/installed/:id/enabled",
            put(routes::plugin_pkgs::set_enabled),
        )
        .route(
            "/plugins/installed/:id/runs",
            post(routes::plugin_pkgs::start_run),
        )
        .route("/plugins/runs", get(routes::plugin_pkgs::list_runs))
        .route("/plugins/runs/:run_id", get(routes::plugin_pkgs::get_run))
        .route("/plugins/drafts", get(routes::plugin_pkgs::list_drafts))
        .route(
            "/plugins/drafts/:id",
            get(routes::plugin_pkgs::get_draft).put(routes::plugin_pkgs::edit_draft),
        )
        .route(
            "/plugins/drafts/:id/submit",
            post(routes::plugin_pkgs::submit_draft),
        )
        .route(
            "/plugins/drafts/:id/approve",
            post(routes::plugin_pkgs::approve_draft),
        )
        .route(
            "/plugins/drafts/:id/reject",
            post(routes::plugin_pkgs::reject_draft),
        )
        .route(
            "/plugins/drafts/:id/stop",
            post(routes::plugin_pkgs::stop_draft),
        )
        .route(
            "/plugins/drafts/:id/cancel",
            post(routes::plugin_pkgs::cancel_draft),
        )
        .route(
            "/plugins/brain-sources",
            get(routes::plugin_pkgs::brain_sources),
        )
        .route("/audit", get(routes::plugin_pkgs::audit_log))
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
        // Remote MCP servers: by address, OAuth or a key, tools allowed one by one
        .route(
            "/mcp-servers",
            get(routes::mcp_servers::list).post(routes::mcp_servers::create),
        )
        .route(
            "/mcp-servers/detect",
            post(routes::mcp_servers::detect_auth),
        )
        .route(
            "/mcp-servers/oauth/callback",
            post(routes::mcp_servers::oauth_callback),
        )
        .route(
            "/mcp-servers/:id",
            get(routes::mcp_servers::get_one).delete(routes::mcp_servers::remove),
        )
        .route(
            "/mcp-servers/:id/connect",
            post(routes::mcp_servers::connect),
        )
        .route(
            "/mcp-servers/:id/connection",
            delete(routes::mcp_servers::disconnect),
        )
        .route(
            "/mcp-servers/:id/client",
            put(routes::mcp_servers::set_client),
        )
        .route("/mcp-servers/:id/key", put(routes::mcp_servers::set_key))
        .route(
            "/mcp-servers/:id/tools",
            put(routes::mcp_servers::set_tools),
        )
        .route(
            "/mcp-servers/:id/approve",
            post(routes::mcp_servers::approve),
        )
        .route("/mcp-servers/:id/check", post(routes::mcp_servers::check))
        .route(
            "/mcp-servers/:id/prompt",
            put(routes::mcp_servers::set_prompt),
        )
        .route(
            "/mcp-servers/:id/activity",
            get(routes::mcp_servers::activity),
        )
        // Data sources (read-only lookups for the reviewer)
        .route(
            "/tools",
            get(routes::tools::list).post(routes::tools::create),
        )
        .route("/tools/validate", post(routes::tools::check))
        .route("/tools/asks", get(routes::tools::asks))
        .route("/tools/asks/:id", post(routes::tools::decide))
        .route(
            "/tools/:name",
            get(routes::tools::get)
                .put(routes::tools::update)
                .delete(routes::tools::remove),
        )
        .route("/tools/:name/approve", post(routes::tools::approve))
        .route("/tools/:name/preview", post(routes::tools::preview))
        // Skills read from a connected repository (text only, never run)
        .route("/skills", get(routes::skills::list))
        .route("/skills/repos", post(routes::skills::connect))
        .route(
            "/skills/repos/:owner/:name",
            delete(routes::skills::disconnect),
        )
        .route(
            "/skills/repos/:owner/:name/sync",
            post(routes::skills::sync),
        )
        .route(
            "/skills/item",
            get(routes::skills::item).put(routes::skills::set_enabled),
        )
        .route(
            "/skills/asks",
            get(routes::skills::asks).post(routes::skills::try_agent),
        )
        .route("/skills/stats", get(routes::skills::stats))
        .route("/skills/recheck", post(routes::skills::recheck))
        .route("/skills/always", put(routes::skills::set_always_apply))
        .route("/skills/reviews", put(routes::skills::set_reviews))
        .route(
            "/skills/translated",
            put(routes::skills::set_use_translated),
        )
        .route("/skills/activity", get(routes::skills::activity))
        .route("/skills/used", get(routes::skills::used))
        .route("/activity/summary", get(routes::activity::summary))
        .route("/data-sources", get(routes::data_sources::list))
        .route("/connectors", get(routes::connectors::list))
        .route("/connectors/aws-check", get(routes::connectors::aws_check))
        .route(
            "/connectors/:kind",
            put(routes::connectors::save).delete(routes::connectors::remove),
        )
        .route("/connectors/:kind/test", post(routes::connectors::test))
        .route(
            "/data-sources/activity",
            get(routes::data_sources::activity),
        )
        .route("/data-sources/api/probe", post(routes::data_sources::probe))
        .route("/data-sources/api/check", post(routes::data_sources::check))
        .route("/data-sources/api", post(routes::data_sources::create))
        .route(
            "/data-sources/rest",
            post(routes::data_sources::create_rest),
        )
        .route(
            "/data-sources/search",
            post(routes::data_sources::create_search),
        )
        .route(
            "/data-sources/aws/setup",
            get(routes::data_sources::aws_setup),
        )
        .route(
            "/data-sources/aws/connect",
            post(routes::data_sources::aws_connect),
        )
        .route(
            "/data-sources/logs",
            post(routes::data_sources::create_logs),
        )
        .route(
            "/data-sources/database",
            post(routes::data_sources::create_database),
        )
        .route(
            "/data-sources/:id",
            put(routes::data_sources::update).delete(routes::data_sources::remove),
        )
        .route(
            "/data-sources/:id/key",
            post(routes::data_sources::replace_key),
        )
        .route("/data-sources/:id/test", post(routes::data_sources::test))
        .route(
            "/data-sources/:id/samples",
            get(routes::data_sources::samples),
        )
        .route(
            "/data-sources/:id/try",
            post(routes::data_sources::try_lookup),
        )
        .route(
            "/data-sources/:id/hidden-suggestions",
            get(routes::data_sources::hidden_suggestions)
                .post(routes::data_sources::decide_hidden_suggestions),
        )
        // E2E test intelligence
        .route(
            "/tests/settings",
            get(routes::tests::get_settings).put(routes::tests::put_settings),
        )
        .route("/tests/summary", get(routes::tests::summary))
        .route("/tests/runs", get(routes::tests::list_runs))
        .route("/tests/runs/:run_key", get(routes::tests::get_run))
        .route("/tests/failure-groups", get(routes::tests::failure_groups))
        .route("/tests/coverage", get(routes::tests::coverage))
        .route(
            "/tests/recommendations/:id/accept",
            post(routes::tests::accept_recommendation),
        )
        .route(
            "/tests/recommendations/:id/dismiss",
            post(routes::tests::dismiss_recommendation),
        )
        .route("/tests/setup", get(routes::tests_setup::status))
        .route("/tests/setup/preview", post(routes::tests_setup::preview))
        .route("/tests/setup/pr", post(routes::tests_setup::open_pr))
        .route("/tests", get(routes::tests::list_tests))
        .route("/tests/:id", get(routes::tests::get_test))
        .route("/tests/:id/quarantine", post(routes::tests::quarantine))
        .route("/tests/:id/pin", post(routes::tests::pin))
        .route("/tests/:id/fix", post(routes::tests::fix))
        // Memory browser
        .route(
            "/memories",
            get(routes::memory::list_memories).post(routes::memory::create_memory),
        )
        .route("/memories/agents", get(routes::memory::agent_profiles))
        // Agent permissions: rules, presets, switches, pause, sandbox lists,
        // the "test a call" box, approval cards and sign-in prompts.
        .route(
            "/agents/policy",
            get(routes::agent_policy::get_policy).put(routes::agent_policy::save_settings),
        )
        .route("/agents/policy/rules", post(routes::agent_policy::add_rule))
        .route(
            "/agents/policy/rules/:id",
            delete(routes::agent_policy::remove_rule),
        )
        .route(
            "/agents/policy/preset",
            post(routes::agent_policy::apply_preset),
        )
        .route(
            "/agents/policy/switches",
            put(routes::agent_policy::set_switch),
        )
        .route(
            "/agents/policy/commands",
            put(routes::agent_policy::set_commands),
        )
        .route(
            "/agents/policy/suggestions",
            post(routes::agent_policy::decide_suggestion),
        )
        .route("/agents/policy/test", post(routes::agent_policy::test_call))
        .route(
            "/agents/pause",
            post(routes::agent_policy::pause_all).delete(routes::agent_policy::resume_all),
        )
        .route(
            "/agents/approvals",
            get(routes::agent_policy::list_approvals),
        )
        .route(
            "/agents/approvals/:id",
            get(routes::agent_policy::get_approval),
        )
        .route(
            "/agents/approvals/:id/decide",
            post(routes::agent_policy::decide),
        )
        .route("/agents/activity", get(routes::agent_policy::activity))
        .route(
            "/agents/sign-in/decline",
            post(routes::agent_policy::decline_sign_in),
        )
        .route("/memories/preview", post(routes::memory::preview_recall))
        .route("/memories/stats", get(routes::memory::memory_stats))
        .route(
            "/memories/:memory_id",
            delete(routes::memory::delete_memory).put(routes::memory::update_memory),
        )
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            middleware::auth::require_auth,
        ));

    // Webhook routes (public, verified by signature)
    let webhook_routes = Router::new()
        .route("/github", post(routes::github_webhook::handle))
        .route("/jira/:token", post(routes::jira_webhook::handle))
        .route("/jira", post(routes::jira_webhook::handle_forge))
        .route("/alerts/sns", post(routes::alerts::handle_sns))
        // Knowledge sources: a provider says pages changed (signed).
        .route(
            "/knowledge/:token",
            post(routes::knowledge_sources::webhook),
        )
        // Plugin briefs from chat, verified with the team's own secrets
        .route("/slack/plugins", post(routes::plugin_pkgs::slack_command))
        .route(
            "/teams/plugins/:token",
            post(routes::plugin_pkgs::teams_message),
        )
        // CI calls signed with a GitHub Actions OIDC token
        .route("/ci/tests/results", post(routes::tests::ci_upload))
        .route("/ci/tests/quarantine", get(routes::tests::ci_quarantine))
        // Events CoderHelm's relay forwards to a hybrid install, and its
        // ownership proof (routes::hybrid_install).
        .route("/relay", post(routes::hybrid_install::intake))
        .route("/relay/proof", get(routes::hybrid_install::proof));

    // The hosted side of hybrid: what self-hosted installs call, signed with
    // their registration secret (404 everywhere else; routes::hybrid).
    let hybrid_routes = Router::new()
        .route(
            "/installs",
            post(routes::hybrid::register).delete(routes::hybrid::unregister),
        )
        .route("/installs/verify", post(routes::hybrid::verify))
        .route("/status", get(routes::hybrid::status))
        .route("/settings", put(routes::hybrid::settings))
        .route(
            "/names",
            put(routes::hybrid::claim_name).delete(routes::hybrid::delete_name),
        )
        .route("/names/records", put(routes::hybrid::put_records))
        .route("/names/:name", get(routes::hybrid::name_available))
        .route("/fallback", get(routes::hybrid::fallback))
        .route("/github/link-start", post(routes::hybrid::link_start))
        .route("/github/setup", get(routes::hybrid::github_setup))
        .route("/github/confirm", post(routes::hybrid::github_confirm))
        .route(
            "/github/links/:installation_id",
            put(routes::hybrid::set_link_repos).delete(routes::hybrid::delete_link),
        )
        .route("/github/token", post(routes::hybrid::github_token))
        .route("/deliveries", get(routes::hybrid::list_deliveries))
        .route("/deliveries/shared", post(routes::hybrid::share))
        .route("/deliveries/:id/redeliver", post(routes::hybrid::redeliver))
        .route(
            "/oauth/:kind/authorize",
            get(routes::hybrid::oauth_authorize),
        )
        .route("/oauth/callback", post(routes::hybrid::oauth_callback))
        .route("/oauth/token", post(routes::hybrid::oauth_token))
        .route("/oauth/revoke", post(routes::hybrid::oauth_revoke));

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
        // This deployment's GitHub App on GitHub (install, or configure).
        .route("/github/app", get(routes::github::app_install))
        // Hybrid linking: GitHub sign-in that proves an installation is yours.
        .route(
            "/github/callback/hybrid",
            get(routes::hybrid::github_signin_return),
        )
        .route("/logout", post(routes::auth::logout))
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            middleware::rate_limit::rate_limit_auth,
        ));

    // Unsubscribe links in emails: public, the signed token is the credential.
    let email_routes = Router::new()
        .route(
            "/unsubscribe",
            get(routes::email::unsubscribe_link).post(routes::email::unsubscribe_one_click),
        )
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            middleware::rate_limit::rate_limit_email,
        ));

    // Signed approval links from Slack, Teams and email: public, the signed
    // token is the credential. Opening one never answers anything.
    let approval_link_routes = Router::new()
        .route(
            "/link",
            get(routes::agent_policy::link).post(routes::agent_policy::link_answer),
        )
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            middleware::rate_limit::rate_limit_email,
        ));

    // Signed email approval links for plugin drafts: public, the signed
    // token (one approver, one version) is the credential.
    let plugin_link_routes = Router::new()
        .route(
            "/approve",
            get(routes::plugin_pkgs::email_approve_page).post(routes::plugin_pkgs::email_approve),
        )
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            middleware::rate_limit::rate_limit_email,
        ));

    let app = Router::new()
        // Root health/info
        .route(
            "/",
            get(|| async {
                axum::Json(serde_json::json!({ "service": "coderhelm", "status": "ok" }))
            }),
        )
        // The public API's OpenAPI document (src/openapi.rs)
        .route("/openapi.json", get(openapi::serve))
        // Streaming endpoint uses Bearer JWT, not cookies — must be outside require_auth
        .route(
            "/api/plans/chat/stream",
            post(routes::plans::plan_chat_stream),
        )
        // Opt-in pings from self-host installers: public, no session, a
        // fixed set of non-identifying fields (routes::telemetry).
        .route("/api/telemetry/ping", post(routes::telemetry::ping))
        // Where GitHub's app manifest flow lands on a self-hosted install
        // (routes::setup). 404 on hosted.
        .route(
            "/api/setup/github/callback",
            get(routes::setup::github_callback),
        )
        // A self-hosted install's setup status for the install guide: public,
        // ids, labels, statuses and codes only, and only until the first
        // person has an account (routes::setup_checks). 404 on hosted.
        .route("/api/setup/status", get(routes::setup_checks::status))
        // The install guide's hybrid steps: the one-time setup key, only
        // until the first person signs up (routes::hybrid_install).
        .route(
            "/api/setup/hybrid",
            get(routes::hybrid_install::setup_status),
        )
        .route(
            "/api/setup/hybrid/register",
            post(routes::hybrid_install::setup_register),
        )
        .route(
            "/api/setup/hybrid/name",
            post(routes::hybrid_install::setup_claim_name),
        )
        .route(
            "/api/setup/hybrid/github/link",
            post(routes::hybrid_install::setup_github_link),
        )
        .route(
            "/api/setup/hybrid/settings",
            post(routes::hybrid_install::setup_settings),
        )
        .route("/api/brain/stream", post(routes::brain::brain_stream))
        .nest("/webhooks", webhook_routes)
        .nest("/hybrid", hybrid_routes)
        .route(
            "/integrations/jira/forge-register",
            post(routes::api::forge_register_urls),
        )
        .nest("/auth", auth_routes)
        .nest("/email", email_routes)
        .nest("/agent-approvals", approval_link_routes)
        .nest("/plugins", plugin_link_routes)
        // Nest protected routes under /api
        .nest("/api", api_routes)
        .layer(SetResponseHeaderLayer::overriding(
            header::CACHE_CONTROL,
            header::HeaderValue::from_static("no-store"),
        ))
        // The public URL, if it couldn't be read at start (first deploy),
        // and the install's model provider (Settings, System, Models), read
        // again at most every 30 seconds.
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            |State(st): State<Arc<AppState>>,
             req: axum::extract::Request,
             next: axum::middleware::Next| async move {
                common::public_urls::ensure().await;
                common::model_provider::refresh(&st.dynamo, &st.config.settings_table_name).await;
                next.run(req).await
            },
        ))
        // Cross-origin answers for browsers (middleware::cors): outermost,
        // so preflights are answered before routing.
        .layer(axum_middleware::from_fn(middleware::cors::cors))
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
