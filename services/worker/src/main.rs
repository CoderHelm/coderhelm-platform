use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use std::sync::Arc;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod agent;
mod clients;
pub mod memory;
mod models;
mod passes;
mod team_installation;

pub struct WorkerState {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub s3: aws_sdk_s3::Client,
    pub ses: aws_sdk_sesv2::Client,
    pub bedrock: aws_sdk_bedrockruntime::Client, // Used for Titan embeddings only (not LLM)
    pub lambda: aws_sdk_lambda::Client,
    pub kms: aws_sdk_kms::Client,
    /// Takes on the read-only roles teams create for their logs and databases.
    pub sts: aws_sdk_sts::Client,
    pub sqs: aws_sdk_sqs::Client,
    pub codebuild: aws_sdk_codebuild::Client,
    pub logs: aws_sdk_cloudwatchlogs::Client,
    /// Plugin schedules (publish at a time, recurring triggers).
    pub scheduler: common::plugins::schedule::aws_sdk_scheduler::Client,
    pub http: reqwest::Client,
    pub config: models::Config,
    pub secrets: models::Secrets,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer().json().flatten_event(true))
        .init();

    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let dynamo = aws_sdk_dynamodb::Client::new(&aws_config);
    let s3 = aws_sdk_s3::Client::new(&aws_config);
    let ses = aws_sdk_sesv2::Client::new(&aws_config);
    let bedrock = aws_sdk_bedrockruntime::Client::new(&aws_config);
    // Claude through Bedrock: set on every install, so a switch to Bedrock
    // in Settings needs no restart.
    common::model_provider::set_bedrock_client(bedrock.clone());
    let lambda_client = aws_sdk_lambda::Client::new(&aws_config);
    let sqs = aws_sdk_sqs::Client::new(&aws_config);
    let codebuild = aws_sdk_codebuild::Client::new(&aws_config);
    let logs = aws_sdk_cloudwatchlogs::Client::new(&aws_config);
    let kms = aws_sdk_kms::Client::new(&aws_config);
    let sts = aws_sdk_sts::Client::new(&aws_config);
    let scheduler = common::plugins::schedule::aws_sdk_scheduler::Client::new(&aws_config);
    let sm = aws_sdk_secretsmanager::Client::new(&aws_config);
    // Where this deployment's dashboard and API are (config, or SSM).
    common::public_urls::init(common::public_urls::ssm::Client::new(&aws_config)).await;

    let config = models::Config::from_env();
    let secrets = models::Secrets::load(&sm, &config.secrets_name).await?;
    // A hybrid install's registration: GitHub tokens come from CoderHelm's broker.
    common::hybrid::client::set_credentials(common::hybrid::client::Credentials::from_parts(
        secrets.hybrid_install_id.as_deref(),
        secrets.hybrid_install_secret.as_deref(),
        secrets.hybrid_relay_key.as_deref(),
    ));
    // A model call whose usage can't be written waits on the tickets queue.
    common::usage::set_fallback(Arc::new(common::usage::SqsFallback {
        sqs: sqs.clone(),
        queue_url: config.ticket_queue_url.clone(),
    }));

    let state = Arc::new(WorkerState {
        dynamo,
        s3,
        ses,
        bedrock,
        lambda: lambda_client,
        kms,
        sts,
        sqs,
        codebuild,
        logs,
        scheduler,
        http: reqwest::Client::builder()
            .user_agent("coderhelm-worker/0.1")
            .build()?,
        config,
        secrets,
    });

    lambda_runtime::run(service_fn(move |event: LambdaEvent<serde_json::Value>| {
        let state = Arc::clone(&state);
        async move { handle_event(state, event).await }
    }))
    .await
}

/// An SQS batch (the queues), or a direct call from the gateway: the plan
/// chat's (and brain chat's) skills, agents and data source tools, answered
/// right away, or a knowledge base file to read.
async fn handle_event(
    state: Arc<WorkerState>,
    event: LambdaEvent<serde_json::Value>,
) -> Result<serde_json::Value, Error> {
    let (payload, context) = event.into_parts();
    common::public_urls::ensure().await;
    // The install's model provider (Settings, System, Models), read again
    // at most every 30 seconds.
    common::model_provider::refresh(&state.dynamo, &state.config.settings_table_name).await;
    // In-flight model calls are cancelled (and their usage recorded) this
    // long before the Lambda's own deadline, leaving time for the writes.
    common::model_stream::set_deadline_epoch_ms(Some(
        context.deadline as i64 - MODEL_DEADLINE_MARGIN_MS,
    ));
    let out = if let Some(req) = payload.get("plan_chat") {
        let req: passes::plan_chat::Request = serde_json::from_value(req.clone())?;
        // The brain chat uses the same tools; its calls count as the brain's.
        let feature = if req.feature == "brain" {
            common::usage::feature::BRAIN
        } else {
            common::usage::feature::CHAT
        };
        let call = passes::plan_chat::handle(&state, req);
        Ok(common::usage::with_feature(feature, call).await)
    } else if let Some(req) = payload.get("model_check") {
        // The updater's check before it applies a release: one test call
        // per model, through the install's provider.
        let req: passes::model_check::Request = serde_json::from_value(req.clone())?;
        Ok(passes::model_check::run(&state, req).await)
    } else if let Some(req) = payload.get("knowledge_ingest") {
        // A knowledge base file the browser finished uploading (called
        // asynchronously by the gateway).
        let req: passes::knowledge::IngestRequest = serde_json::from_value(req.clone())?;
        Ok(passes::knowledge::ingest(&state, req).await)
    } else if let Some(req) = payload.get("knowledge_sync") {
        // One knowledge source to sync now ("Sync now", a new source, a
        // provider's webhook; called asynchronously by the gateway).
        let req: common::knowledge::sources::SyncMessage = serde_json::from_value(req.clone())?;
        let call = passes::knowledge_sync::sync(&state, &req);
        Ok(common::usage::with_feature(common::usage::feature::EMBEDDING, call).await)
    } else {
        let sqs: SqsEvent = serde_json::from_value(payload)?;
        handle_sqs(state, LambdaEvent::new(sqs, context))
            .await
            .map(|_| serde_json::Value::Null)
    };
    // A call dropped mid-reply (an outer timeout) records itself in the
    // background: finish those writes before the Lambda is frozen.
    common::model_stream::settle_dropped_calls().await;
    out
}

/// How long before the Lambda deadline in-flight model calls are cancelled.
/// Ticket runs stop at 870s (30s before the 900s limit); a call still in
/// flight then is dropped and records itself, and this catches every other
/// path before the hard stop.
const MODEL_DEADLINE_MARGIN_MS: i64 = 20_000;

async fn handle_sqs(state: Arc<WorkerState>, event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    for record in event.payload.records {
        let body = record.body.unwrap_or_default();
        info!("Processing SQS message");

        // Parsed once: the installation check, the pause hold and the parse
        // into a message all read the same value.
        let raw: serde_json::Value = serde_json::from_str(&body)?;
        if !installation_is_teams_own(&state, &raw).await? {
            continue;
        }
        let message: models::WorkerMessage = serde_json::from_value(raw.clone())?;
        // A held usage record: added once by its call id. A failure fails
        // the message, so SQS delivers it again (and the DLQ keeps it).
        if let models::WorkerMessage::UsageRecord(rec) = &message {
            let store = common::usage::DynamoStore {
                dynamo: state.dynamo.clone(),
                table: state.config.analytics_table_name.clone(),
            };
            let added = common::usage::replay(&store, rec).await?;
            info!(call_id = %rec.call_id, team_id = %rec.team_id, ?added, "Recovered model usage");
            continue;
        }
        // "Pause all agents": agent work for a paused team is kept exactly as
        // it came and sent again when the pause ends. Nothing is dropped.
        let work = message
            .agent_work()
            .map(|(t, label, run)| (t.to_string(), label, run));
        if let Some((team, label, run_id)) = &work {
            let store = agent::policy::store(&state);
            if let Some(reason) =
                common::agent_policy::pause::hold_if_paused(&*store, team, label, run_id, &raw)
                    .await
            {
                info!(team_id = %team, %reason, "Agents paused: work kept for later");
                continue;
            }
        }
        // Every model call made while handling this message is counted under
        // the message's feature (spawned tasks fall back to "other").
        let feature = message.usage_feature();
        let (_, stopped) = agent::policy::watch_stops(common::usage::with_feature(
            feature,
            handle_message(Arc::clone(&state), message),
        ))
        .await;
        // A loop stopped because agents were paused mid-run: keep the work
        // (as the same run) to pick up again when the pause ends.
        if let (Some((common::agent_policy::Stop::Paused { .. }, run_id)), Some((team, label, _))) =
            (stopped, work)
        {
            let store = agent::policy::store(&state);
            let mut again = raw.clone();
            if !run_id.is_empty() && again.get("type").and_then(|t| t.as_str()) == Some("ticket") {
                again["continuation_run_id"] = serde_json::json!(run_id);
            }
            common::agent_policy::pause::keep(&*store, &team, &label, &run_id, &again).await;
        }
    }

    Ok(())
}

/// Sends kept work back to the tickets queue.
struct SqsResender<'a>(&'a WorkerState);

#[async_trait::async_trait]
impl common::agent_policy::pause::Resender for SqsResender<'_> {
    async fn send(&self, message: &serde_json::Value) -> Result<(), String> {
        self.0
            .sqs
            .send_message()
            .queue_url(&self.0.config.ticket_queue_url)
            .message_body(message.to_string())
            .send()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

/// A message that names a GitHub installation must name its team's stored
/// one; anything else is dropped with a warning (see team_installation). A
/// failed read of the team is returned as an error so SQS retries it.
async fn installation_is_teams_own(
    state: &WorkerState,
    raw: &serde_json::Value,
) -> Result<bool, Error> {
    use team_installation::{claims, decide, single_team, stored_installation, Verdict};
    let kind = raw.get("type").and_then(|t| t.as_str()).unwrap_or("");
    let (claimed, teams) = claims(raw);
    if claimed.is_empty() {
        return Ok(true);
    }
    let team = match single_team(&teams) {
        Ok(t) => t,
        Err(reason) => {
            warn!(kind, reason, "Refusing queued message");
            return Ok(false);
        }
    };
    let stored = match team {
        Some(t) => stored_installation(&state.dynamo, &state.config.teams_table_name, t).await?,
        None => None,
    };
    match decide(&claimed, team, stored) {
        Verdict::Allow => Ok(true),
        Verdict::Refuse(reason) => {
            warn!(
                kind,
                team_id = team.unwrap_or(""),
                claimed = ?claimed,
                stored = ?stored,
                reason,
                "Refusing queued message: installation does not belong to the team"
            );
            Ok(false)
        }
    }
}

async fn handle_message(state: Arc<WorkerState>, message: models::WorkerMessage) {
    match message {
        models::WorkerMessage::Ticket(msg) => {
            info!(team_id = %msg.team_id, ticket_id = %msg.ticket_id, "Starting ticket run");
            if let Err(e) = passes::orchestrate_ticket(&state, msg).await {
                error!(error = %common::run_fail::error_text(&*e), "Ticket run failed");
            }
        }
        models::WorkerMessage::Feedback(msg) => {
            info!(team_id = %msg.team_id, run_id = %msg.run_id, "Processing feedback");
            let team_id = msg.team_id.clone();
            let run_id = msg.run_id.clone();
            let retry_msg = msg.clone();
            if let Err(e) = passes::feedback::run(&state, msg).await {
                // A transient model failure is sent again later by the one
                // requeue policy; only a real failure marks the run failed.
                let err: &(dyn std::error::Error + 'static) = &*e;
                if common::model_retry::is_transient_model_error(err)
                    && passes::feedback::requeue_after_model_error(&state, &retry_msg).await
                {
                    return;
                }
                // The error with its causes: the SDK's own text is just
                // "service error".
                // A loop stopped (agents paused, an approval nobody
                // answered): end with its note, not as a failure.
                if let Some(stop) = agent::policy::claim_stop(&run_id) {
                    agent::policy::end_run_for_stop(&state, &team_id, &run_id, &stop).await;
                    return;
                }
                let reason = common::run_fail::error_text(err);
                error!(run_id, error = %reason, "Feedback run failed");
                // The one failed-status write. It never overwrites a run that
                // was cancelled (a closed PR) or merged meanwhile.
                if let Err(werr) = common::run_fail::fail_run(
                    &state.dynamo,
                    &state.config.runs_table_name,
                    &team_id,
                    &run_id,
                    &common::run_fail::RunFailure {
                        reason: &format!("Feedback pass failed: {reason}"),
                        pass: Some("feedback"),
                        ..Default::default()
                    },
                )
                .await
                {
                    error!(run_id, error = %werr, "Could not mark the run failed");
                }
            }
        }
        models::WorkerMessage::JiraComment(msg) => {
            info!(team_id = %msg.team_id, ticket_id = %msg.ticket_id, "Reading a ticket comment");
            if let Err(e) = passes::jira_comment::run(&state, msg).await {
                error!("Ticket comment failed: {e:?}");
            }
        }
        models::WorkerMessage::Lesson(msg) => {
            info!(team_id = %msg.team_id, run_id = %msg.run_id, trigger = %msg.trigger, "Lesson on where the fix belongs");
            if let Err(e) = passes::lessons::run(&state, msg).await {
                error!("Lesson failed: {e:?}");
            }
        }
        models::WorkerMessage::RewardOutcome(msg) => {
            info!(team_id = %msg.team_id, run_id = %msg.run_id, pr = msg.pr_number, "Scoring a closed pull request");
            if let Err(e) = passes::rewards::score_outcome(&state, msg).await {
                error!("Reward outcome failed: {e}");
            }
        }
        models::WorkerMessage::ReviewLesson(msg) => {
            info!(team_id = %msg.team_id, pr = msg.pr_number, trigger = %msg.trigger, "Judging pushback on a review");
            if let Err(e) = passes::review_lessons::run(&state, msg).await {
                error!("Review lesson failed: {e:?}");
            }
        }
        models::WorkerMessage::Onboard(msg) => {
            info!(team_id = %msg.team_id, repos = msg.repos.len(), "Processing onboard");
            let mut usage = models::TokenUsage::default();
            if let Err(e) = passes::onboard::run(&state, msg, &mut usage).await {
                error!("Onboard failed: {e}");
            }
        }
        models::WorkerMessage::MarkReady(msg) => {
            info!(team_id = %msg.team_id, pr_number = msg.pr_number, "Marking PR ready");
            if let Err(e) = mark_pr_ready(&state, msg).await {
                error!("Mark PR ready failed: {e}");
            }
        }
        models::WorkerMessage::PlanExecute(msg) => {
            info!(team_id = %msg.team_id, plan_id = %msg.plan_id, "Executing plan");
            if let Err(e) = passes::plan_execute::run(&state, msg).await {
                error!("Plan execute failed: {e}");
            }
        }
        models::WorkerMessage::PlanTaskContinue(msg) => {
            info!(team_id = %msg.team_id, plan_id = %msg.plan_id, tasks = msg.tasks.len(), "Continuing plan tasks");
            if let Err(e) = passes::plan_execute::continue_tasks(&state, msg).await {
                error!("Plan task continue failed: {e}");
            }
        }
        models::WorkerMessage::InfraAnalyze(msg) => {
            info!(team_id = %msg.team_id, "Analyzing infrastructure");
            let team_id = msg.team_id.clone();
            let mut usage = models::TokenUsage::default();
            if let Err(e) = passes::infra_analyze::run(&state, msg, &mut usage).await {
                error!("Infra analyze failed: {e}");
                // Store failed status so the dashboard can show the error
                let now = chrono::Utc::now().to_rfc3339();
                let _ = state
                    .dynamo
                    .put_item()
                    .table_name(&state.config.infra_table_name)
                    .item("pk", aws_sdk_dynamodb::types::AttributeValue::S(team_id))
                    .item(
                        "sk",
                        aws_sdk_dynamodb::types::AttributeValue::S("INFRA#analysis".to_string()),
                    )
                    .item(
                        "status",
                        aws_sdk_dynamodb::types::AttributeValue::S("failed".to_string()),
                    )
                    .item(
                        "has_infra",
                        aws_sdk_dynamodb::types::AttributeValue::Bool(false),
                    )
                    .item(
                        "error",
                        aws_sdk_dynamodb::types::AttributeValue::S(e.to_string()),
                    )
                    .item(
                        "updated_at",
                        aws_sdk_dynamodb::types::AttributeValue::S(now),
                    )
                    .send()
                    .await;
            }
        }
        models::WorkerMessage::Resume(msg) => {
            info!(team_id = %msg.team_id, run_id = %msg.run_id, "Resuming run from webhook event");
            if let Err(e) = passes::resume::run(&state, msg).await {
                error!("Resume failed: {e:?}");
            }
        }
        models::WorkerMessage::Review(msg) => {
            info!(team_id = %msg.team_id, repo = %format!("{}/{}", msg.repo_owner, msg.repo_name), pr = msg.pr_number, "Reviewing PR from label");
            if let Err(e) = passes::review_pr::run(&state, msg).await {
                error!("Review failed: {e:?}");
            }
        }
        models::WorkerMessage::ReviewSweep => {
            info!("Running stuck-review sweep");
            match passes::review_sweep::run(&state).await {
                Ok(report) => info!(
                    repos = report.repos,
                    open_prs = report.open_prs,
                    requeued = report.requeued.len(),
                    prs = ?report.requeued,
                    "Stuck-review sweep done"
                ),
                Err(e) => error!("Stuck-review sweep failed: {e:?}"),
            }
        }
        models::WorkerMessage::ReviewReminder => {
            info!("Running review-reminder sweep");
            if let Err(e) = passes::review_reminder::run(&state).await {
                error!("Review reminder sweep failed: {e:?}");
            }
            if let Err(e) = passes::pr_outcomes::backfill(&state).await {
                error!("Merged PR backfill failed: {e:?}");
            }
            if let Err(e) = passes::skills::sweep(&state).await {
                error!("Skill repository sweep failed: {e:?}");
            }
        }
        models::WorkerMessage::PluginRun(msg) => {
            info!(team_id = %msg.team_id, plugin = %msg.plugin_id, run_id = %msg.run_id, trigger = %msg.trigger, "Plugin run");
            if let Err(e) = passes::plugins::run_plugin(&state, msg).await {
                error!("Plugin run failed: {e:?}");
            }
        }
        models::WorkerMessage::PluginPublish(msg) => {
            info!(team_id = %msg.team_id, artifact = %msg.artifact_id, version = msg.version, "Plugin publish");
            if let Err(e) = passes::plugins::publish(&state, msg).await {
                error!("Plugin publish failed: {e:?}");
            }
        }
        models::WorkerMessage::PluginSync(msg) => {
            info!(team_id = %msg.team_id, source = %msg.source_id, plugin = %msg.plugin_id, reason = %msg.reason, "Plugin sync");
            if let Err(e) = passes::plugins::sync(&state, msg).await {
                error!("Plugin sync failed: {e:?}");
            }
        }
        models::WorkerMessage::PluginSweep => {
            info!("Plugin source poll");
            if let Err(e) = passes::plugins::sweep(&state).await {
                error!("Plugin source poll failed: {e:?}");
            }
        }
        models::WorkerMessage::PluginTrigger(msg) => {
            info!(team_id = %msg.team_id, plugin = %msg.plugin_id, trigger = msg.trigger, "Plugin schedule fired");
            if let Err(e) = passes::plugins::trigger(&state, msg).await {
                error!("Plugin trigger failed: {e:?}");
            }
        }
        models::WorkerMessage::PluginResults(msg) => {
            info!(team_id = %msg.team_id, artifact = %msg.artifact_id, "Plugin results");
            if let Err(e) = passes::plugins::results(&state, msg).await {
                error!("Plugin results failed: {e:?}");
            }
        }
        models::WorkerMessage::SkillSync(msg) => {
            info!(team_id = %msg.team_id, repo = %msg.repo, force = msg.force, "Skill repository sync");
            if let Err(e) = passes::skills::sync(&state, msg).await {
                error!("Skill sync failed: {e:?}");
            }
        }
        models::WorkerMessage::SkillAsk(msg) => {
            info!(team_id = %msg.team_id, repo = %msg.repo, agent = %msg.path, "Admin trying a team agent");
            if let Err(e) = passes::skills::try_ask(&state, msg).await {
                error!("Team agent try failed: {e:?}");
            }
        }
        models::WorkerMessage::HealthCheck(msg) => {
            info!(team_id = %msg.team_id, pr = msg.pr_number, attempt = msg.attempts, "Post-merge health check");
            if let Err(e) = passes::health_check::run(&state, msg).await {
                error!("Health check failed: {e:?}");
            }
        }
        models::WorkerMessage::WebhookEmit(msg) => {
            info!(team_id = %msg.event.team_id, event = %msg.event.event_type, id = %msg.event.id, "Webhook event");
            if let Err(e) = passes::webhooks::dispatch(&state, msg).await {
                error!("Webhook dispatch failed: {e:?}");
            }
        }
        models::WorkerMessage::WebhookSend(msg) => {
            info!(team_id = %msg.team_id, endpoint = %msg.endpoint_id, delivery = %msg.delivery_id, "Webhook retry");
            if let Err(e) = passes::webhooks::send(&state, msg).await {
                error!("Webhook send failed: {e:?}");
            }
        }
        models::WorkerMessage::RelayRetry(msg) => {
            info!(install = %msg.install_id, delivery = %msg.delivery_id, "Relay retry");
            if let Err(e) = passes::relay_retry::run(&state, msg).await {
                error!("Relay retry failed: {e:?}");
            }
        }
        models::WorkerMessage::AwaitMerge(msg) => {
            info!(team_id = %msg.team_id, pr = msg.pr_number, attempt = msg.attempts, "Armed auto-merge check");
            if let Err(e) = passes::await_merge::run(&state, msg).await {
                error!("Await-merge failed: {e:?}");
            }
        }
        models::WorkerMessage::ToolProposals(msg) => {
            info!(team_id = %msg.team_id, repo = %format!("{}/{}", msg.repo_owner, msg.repo_name), files = msg.paths.len(), "Tool proposals from repo");
            if let Err(e) = passes::tool_proposals::run(&state, msg).await {
                error!("Tool proposals failed: {e:?}");
            }
        }
        models::WorkerMessage::HiddenSuggest(msg) => {
            info!(team_id = %msg.team_id, source = %msg.source_id, "Hidden-field suggestions");
            if let Err(e) = passes::hidden_suggest::run(&state, msg).await {
                error!("Hidden-field suggestions failed: {e:?}");
            }
        }
        models::WorkerMessage::TagSweep(msg) => {
            info!(team_id = %msg.team_id, repo = %format!("{}/{}", msg.repo_owner, msg.repo_name), remaining_secs = msg.delay_remaining_secs, "Batched release-tag sweep");
            if let Err(e) = passes::review_actions::run_tag_sweep(&state, msg).await {
                error!("Tag sweep failed: {e:?}");
            }
        }
        models::WorkerMessage::ReleaseNotes(msg) => {
            info!(team_id = %msg.team_id, repo = %format!("{}/{}", msg.repo_owner, msg.repo_name), tag = %msg.tag, resend = %msg.resend, "Release notes");
            if let Err(e) = passes::release_notes::run(&state, msg).await {
                error!("Release notes failed: {e:?}");
            }
        }
        models::WorkerMessage::TestIngest(msg) => {
            info!(team_id = %msg.team_id, repo = %format!("{}/{}", msg.repo_owner, msg.repo_name), run_id = msg.run_id, "Reading E2E results");
            if let Err(e) = passes::e2e::ingest(&state, msg).await {
                error!("E2E ingest failed: {e:?}");
            }
        }
        models::WorkerMessage::TestAction(msg) => {
            info!(team_id = %msg.team_id, repo = %msg.repo, action = %msg.action, "E2E action");
            if let Err(e) = passes::e2e::act(&state, msg).await {
                error!("E2E action failed: {e:?}");
            }
        }
        models::WorkerMessage::WeeklySummary => {
            info!("Weekly summary emails");
            if let Err(e) = passes::weekly_summary::run(&state).await {
                error!("Weekly summary failed: {e:?}");
            }
        }
        models::WorkerMessage::WeeklySummaryTest(msg) => {
            info!(team_id = %msg.team_id, "Weekly summary test email");
            if let Err(e) = passes::weekly_summary::run_test(&state, msg).await {
                error!("Weekly summary test failed: {e:?}");
            }
        }
        models::WorkerMessage::GraphIndex(msg) => {
            info!(team_id = %msg.team_id, repo = %format!("{}/{}", msg.repo_owner, msg.repo_name), incremental = msg.changed_files.is_some(), "Code-graph index pass");
            if let Err(e) = passes::code_graph::run(&state, msg).await {
                error!("Code-graph index failed: {e:?}");
            }
        }
        models::WorkerMessage::KnowledgeSyncSweep => {
            let queued = passes::knowledge_sync::sweep(&state).await;
            info!(queued, "Knowledge sync sweep");
        }
        models::WorkerMessage::KnowledgeSync(msg) => {
            let out = passes::knowledge_sync::sync(&state, &msg).await;
            info!(team_id = %msg.team_id, source = %msg.source_id, result = %out, "Knowledge source synced");
        }
        models::WorkerMessage::DlqCleanup => {
            info!("DLQ cleanup");
            match passes::dlq_cleanup::run(&state).await {
                Ok(report) => info!(
                    read = report.read,
                    archived = report.archived,
                    kept = report.kept,
                    complete = report.complete,
                    "DLQ cleanup done"
                ),
                Err(e) => error!("DLQ cleanup failed: {e}"),
            }
        }
        models::WorkerMessage::LogAutofix => match passes::log_autofix::run(&state).await {
            Ok(r) => info!(
                teams = r.teams,
                started = r.started,
                skipped = r.skipped,
                "Automatic log-finding fixes swept"
            ),
            Err(e) => error!("Automatic log-finding sweep failed: {e}"),
        },
        models::WorkerMessage::UsageReconcile(msg) => {
            info!(day = %msg.day, "Usage reconciliation");
            if let Err(e) = passes::usage_reconcile::run(&state, msg).await {
                error!("Usage reconciliation failed: {e}");
            }
        }
        models::WorkerMessage::AgentResumeSweep => {
            let store = agent::policy::store(&state);
            let sent = common::agent_policy::pause::sweep(&*store, &SqsResender(&state)).await;
            info!(sent, "Paused agent work sweep done");
        }
        // Handled in `handle_sqs`, where a failure fails the message.
        models::WorkerMessage::UsageRecord(_) => {}
    }
}

async fn mark_pr_ready(
    state: &WorkerState,
    msg: models::MarkReadyMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let github = clients::github::GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        msg.installation_id,
        &state.http,
    )?;
    // Fetch the PR to get its GraphQL node_id (REST PATCH {"draft":false} doesn't work)
    let pr = github
        .get_pull_request(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await?;
    if pr["state"].as_str() != Some("open") || pr["draft"].as_bool() != Some(true) {
        info!(
            pr_number = msg.pr_number,
            "PR is closed or not a draft — nothing to mark ready"
        );
        return Ok(());
    }
    // A person put this PR back into draft: a CI pass must not undo that. A
    // timeline that can't be read leaves the draft alone (fail closed).
    let timeline = github
        .list_issue_timeline(&msg.repo_owner, &msg.repo_name, msg.pr_number)
        .await?;
    if let Some(who) = clients::github::human_converted_to_draft(&timeline) {
        info!(pr_number = msg.pr_number, converted_by = %who, "PR was put back into draft by a person — not marking ready");
        return Ok(());
    }
    let node_id = pr
        .get("node_id")
        .and_then(|v| v.as_str())
        .ok_or("PR missing node_id")?;
    github.mark_pr_ready(node_id).await?;
    info!(
        pr_number = msg.pr_number,
        repo = %format!("{}/{}", msg.repo_owner, msg.repo_name),
        "PR marked ready for review"
    );
    Ok(())
}
