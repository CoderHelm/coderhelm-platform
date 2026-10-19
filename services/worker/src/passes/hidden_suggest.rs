//! Works out which of a database source's columns likely hold a person's
//! details, from table and column names only, and keeps them as suggestions
//! for an admin to accept or dismiss (`common::live::hidden_suggest`).
//!
//! Runs when a database is connected, when its list of tables is read again
//! and has changed, or when an admin asks. One call on the light model,
//! counted as `data_source`. Nothing is hidden or un-hidden here.

use common::live::catalog::Catalog;
use common::live::hidden_suggest::{catalog_key, output_schema, suggestions, user_message, SYSTEM};
use common::live::store::{self, Kind, Source};
use tracing::{info, warn};

use crate::models::{HiddenSuggestMessage, TokenUsage, WorkerMessage};
use crate::WorkerState;

const MAX_TOKENS: i32 = 8_000;

/// Asks for suggestions in the background when `catalog` holds names the
/// last suggestions weren't worked out from.
pub async fn enqueue_if_changed(
    state: &WorkerState,
    team_id: &str,
    source_id: &str,
    catalog: &Catalog,
) {
    let table = &state.config.settings_table_name;
    let kept = store::get_hidden_suggestions(&state.dynamo, table, team_id, source_id)
        .await
        .unwrap_or_default();
    if kept.key == catalog_key(catalog) || state.config.ticket_queue_url.is_empty() {
        return;
    }
    let msg = WorkerMessage::HiddenSuggest(HiddenSuggestMessage {
        team_id: team_id.to_string(),
        source_id: source_id.to_string(),
        force: false,
    });
    let Ok(body) = serde_json::to_string(&msg) else {
        return;
    };
    if let Err(e) = state
        .sqs
        .send_message()
        .queue_url(&state.config.ticket_queue_url)
        .message_body(body)
        .send()
        .await
    {
        warn!(error = %e, "Could not queue hidden-field suggestions");
    }
}

pub async fn run(
    state: &WorkerState,
    msg: HiddenSuggestMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let table = &state.config.settings_table_name;
    let Some(source) =
        store::get_source(&state.dynamo, table, &msg.team_id, &msg.source_id).await?
    else {
        return Ok(());
    };
    if source.kind != Kind::Database {
        return Ok(());
    }
    let Some(text) = read_description(state, &msg.team_id, &source).await else {
        info!(source = %source.id, "No saved list of tables; no hidden-field suggestions");
        return Ok(());
    };
    let catalog = Catalog::parse(&text);
    if !catalog.has_tables() {
        return Ok(());
    }
    let mut kept =
        store::get_hidden_suggestions(&state.dynamo, table, &msg.team_id, &source.id).await?;
    let key = catalog_key(&catalog);
    if kept.key == key && !msg.force {
        return Ok(());
    }
    let provider =
        crate::agent::provider::ModelProvider::load_for_team(&state.dynamo, table, &msg.team_id)
            .await?;
    let mut usage = TokenUsage::default();
    let answer = crate::agent::anthropic::converse_json(
        &provider.client(),
        &state.config.light_model_id,
        SYSTEM,
        &user_message(&catalog),
        &output_schema(),
        // The API holds the answer to the schema.
        true,
        "low",
        MAX_TOKENS,
        &mut usage,
    )
    .await?;
    kept.suggestions = suggestions(&answer, &catalog, &source.hidden_fields, &kept.dismissed);
    kept.key = key;
    kept.at = chrono::Utc::now().to_rfc3339();
    store::put_hidden_suggestions(&state.dynamo, table, &msg.team_id, &source.id, &kept).await?;
    info!(
        team_id = %msg.team_id,
        source = %source.id,
        suggested = kept.suggestions.len(),
        input_tokens = usage.input_tokens,
        output_tokens = usage.output_tokens,
        "Hidden-field suggestions done"
    );
    Ok(())
}

async fn read_description(state: &WorkerState, team_id: &str, source: &Source) -> Option<String> {
    let out = state
        .s3
        .get_object()
        .bucket(&state.config.bucket_name)
        .key(Source::schema_s3_key(team_id, &source.id))
        .send()
        .await
        .ok()?;
    let bytes = out.body.collect().await.ok()?.into_bytes();
    String::from_utf8(bytes.to_vec()).ok()
}
