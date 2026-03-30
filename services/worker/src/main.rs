use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod agent;
mod clients;
mod models;
mod passes;

pub struct WorkerState {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub s3: aws_sdk_s3::Client,
    pub ses: aws_sdk_sesv2::Client,
    pub bedrock: aws_sdk_bedrockruntime::Client,
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
    let sm = aws_sdk_secretsmanager::Client::new(&aws_config);

    let config = models::Config::from_env();
    let secrets = models::Secrets::load(&sm, &config.secrets_name).await?;

    let state = Arc::new(WorkerState {
        dynamo,
        s3,
        ses,
        bedrock,
        http: reqwest::Client::builder()
            .user_agent("coderhelm-worker/0.1")
            .build()?,
        config,
        secrets,
    });

    lambda_runtime::run(service_fn(move |event: LambdaEvent<SqsEvent>| {
        let state = Arc::clone(&state);
        async move { handle_sqs(state, event).await }
    }))
    .await
}

async fn handle_sqs(state: Arc<WorkerState>, event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    for record in event.payload.records {
        let body = record.body.unwrap_or_default();
        info!("Processing SQS message");

        let message: models::WorkerMessage = serde_json::from_str(&body)?;

        match message {
            models::WorkerMessage::Ticket(msg) => {
                info!(tenant_id = %msg.tenant_id, ticket_id = %msg.ticket_id, "Starting ticket run");
                if let Err(e) = passes::orchestrate_ticket(&state, msg).await {
                    error!("Ticket run failed: {e}");
                }
            }
            models::WorkerMessage::CiFix(msg) => {
                info!(tenant_id = %msg.tenant_id, run_id = %msg.run_id, "Starting CI fix");
                if let Err(e) = passes::ci_fix::run(&state, msg).await {
                    error!("CI fix failed: {e}");
                }
            }
            models::WorkerMessage::Feedback(msg) => {
                info!(tenant_id = %msg.tenant_id, run_id = %msg.run_id, "Processing feedback");
                let tenant_id = msg.tenant_id.clone();
                let run_id = msg.run_id.clone();
                if let Err(e) = passes::feedback::run(&state, msg).await {
                    error!("Feedback run failed: {e}");
                    let now = chrono::Utc::now().to_rfc3339();
                    let _ = state
                        .dynamo
                        .update_item()
                        .table_name(&state.config.runs_table_name)
                        .key(
                            "tenant_id",
                            aws_sdk_dynamodb::types::AttributeValue::S(tenant_id),
                        )
                        .key(
                            "run_id",
                            aws_sdk_dynamodb::types::AttributeValue::S(run_id.clone()),
                        )
                        .update_expression(
                            "SET #s = :s, #e = :e, status_run_id = :sri, updated_at = :t",
                        )
                        .expression_attribute_names("#s", "status")
                        .expression_attribute_names("#e", "error")
                        .expression_attribute_values(
                            ":s",
                            aws_sdk_dynamodb::types::AttributeValue::S("failed".to_string()),
                        )
                        .expression_attribute_values(
                            ":e",
                            aws_sdk_dynamodb::types::AttributeValue::S(format!("{e}")),
                        )
                        .expression_attribute_values(
                            ":sri",
                            aws_sdk_dynamodb::types::AttributeValue::S(format!("failed#{run_id}")),
                        )
                        .expression_attribute_values(
                            ":t",
                            aws_sdk_dynamodb::types::AttributeValue::S(now),
                        )
                        .send()
                        .await;
                }
            }
            models::WorkerMessage::Onboard(msg) => {
                info!(tenant_id = %msg.tenant_id, repos = msg.repos.len(), "Processing onboard");
                if let Err(e) = passes::onboard::run(&state, msg).await {
                    error!("Onboard failed: {e}");
                }
            }
            models::WorkerMessage::MarkReady(msg) => {
                info!(tenant_id = %msg.tenant_id, pr_number = msg.pr_number, "Marking PR ready");
                if let Err(e) = mark_pr_ready(&state, msg).await {
                    error!("Mark PR ready failed: {e}");
                }
            }
            models::WorkerMessage::PlanExecute(msg) => {
                info!(tenant_id = %msg.tenant_id, plan_id = %msg.plan_id, "Executing plan");
                if let Err(e) = passes::plan_execute::run(&state, msg).await {
                    error!("Plan execute failed: {e}");
                }
            }
            models::WorkerMessage::InfraAnalyze(msg) => {
                info!(tenant_id = %msg.tenant_id, "Analyzing infrastructure");
                let tenant_id = msg.tenant_id.clone();
                if let Err(e) = passes::infra_analyze::run(&state, msg).await {
                    error!("Infra analyze failed: {e}");
                    // Store failed status so the dashboard can show the error
                    let now = chrono::Utc::now().to_rfc3339();
                    let _ = state
                        .dynamo
                        .put_item()
                        .table_name(&state.config.table_name)
                        .item("pk", aws_sdk_dynamodb::types::AttributeValue::S(tenant_id))
                        .item(
                            "sk",
                            aws_sdk_dynamodb::types::AttributeValue::S(
                                "INFRA#analysis".to_string(),
                            ),
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
        }
    }

    Ok(())
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
