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
            .user_agent("d3ftly-worker/0.1")
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
                if let Err(e) = passes::feedback::run(&state, msg).await {
                    error!("Feedback run failed: {e}");
                }
            }
            models::WorkerMessage::Onboard(msg) => {
                info!(tenant_id = %msg.tenant_id, repos = msg.repos.len(), "Processing onboard");
                if let Err(e) = passes::onboard::run(&state, msg).await {
                    error!("Onboard failed: {e}");
                }
            }
        }
    }

    Ok(())
}
