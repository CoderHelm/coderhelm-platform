//! Copying a team's chosen knowledge bases into its Bedrock knowledge base
//! (`common::knowledge::bedrock_sync` has the rules): one team's sync, and
//! the sweep that queues every team on the registry.
//!
//! The sweep runs with the knowledge sweep (every 15 minutes), so a file
//! added, changed or deleted reaches Bedrock within about that long; saving
//! the settings and "Sync now" queue a sync at once. A team whose sync is off
//! and has nothing left in Bedrock leaves the registry.

use serde_json::{json, Value};
use tracing::{info, warn};

use common::knowledge::bedrock_sync::{self as bs, Config, DynamoState};
use common::knowledge::search::AwsStore;

use crate::WorkerState;

/// How long one sync holds the team.
const LEASE_SECS: i64 = 15 * 60;
/// Teams one sweep queues at most.
const MAX_PER_SWEEP: usize = 500;

fn store(state: &WorkerState) -> AwsStore {
    AwsStore {
        dynamo: state.dynamo.clone(),
        s3: state.s3.clone(),
        table: state.config.settings_table_name.clone(),
        bucket: state.config.bucket_name.clone(),
    }
}

/// Syncs one team. Never fails: the outcome is on the team's status.
pub async fn sync(state: &WorkerState, team_id: &str) -> Value {
    let table = &state.config.settings_table_name;
    let cfg: Config = match bs::load(&state.dynamo, table, team_id).await {
        Ok(Some(c)) => c,
        Ok(None) => return json!({ "ok": false, "error": "no sync settings" }),
        Err(e) => return json!({ "ok": false, "error": e }),
    };
    let now = chrono::Utc::now();
    let secs = now.timestamp();
    match bs::take_lease(&state.dynamo, table, team_id, &cfg, secs, secs + LEASE_SECS).await {
        Ok(true) => {}
        Ok(false) => return json!({ "ok": true, "skipped": "another sync holds it" }),
        Err(e) => return json!({ "ok": false, "error": e }),
    }
    let at = now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let dstate = DynamoState {
        dynamo: state.dynamo.clone(),
        table: table.clone(),
    };
    let result = async {
        let session = bs::session_for(
            &state.sts,
            &state.dynamo,
            &state.config.aws_insights_table_name,
            team_id,
            &cfg.target,
        )
        .await?;
        let sink = bs::sink_for(&session, &cfg.target)
            .ok_or_else(|| "Choose where to copy the knowledge bases.".to_string())?;
        let team_name = if cfg.team_name.is_empty() {
            team_id.to_string()
        } else {
            cfg.team_name.clone()
        };
        bs::reconcile(
            &store(state),
            &dstate,
            sink.as_ref(),
            team_id,
            &team_name,
            common::public_urls::app_url_from_env(),
            &cfg,
            &at,
        )
        .await
    }
    .await;
    let (status, out) = match result {
        Ok(o) => {
            info!(
                team_id,
                added = o.added,
                updated = o.updated,
                removed = o.removed,
                documents = o.documents,
                errors = o.errors.len(),
                "Bedrock sync done"
            );
            let out = json!({ "ok": true, "added": o.added, "updated": o.updated, "removed": o.removed, "documents": o.documents });
            (o.status(&cfg.status, &at, false), out)
        }
        Err(e) => {
            // Only the plain-words reason: never a request or a document.
            warn!(team_id, error = %e, "Bedrock sync failed");
            let failed = bs::Outcome {
                documents: cfg.status.documents,
                errors: vec![e.clone()],
                needs_ingestion: cfg.status.needs_ingestion,
                ..Default::default()
            };
            (
                failed.status(&cfg.status, &at, true),
                json!({ "ok": false, "error": e }),
            )
        }
    };
    let off_and_empty = !cfg.enabled && status.documents == 0 && status.state != "failed";
    if let Err(e) = bs::finish_status(&state.dynamo, table, team_id, status).await {
        warn!(team_id, error = %e, "Bedrock sync status not saved");
    }
    if off_and_empty {
        if let Err(e) = bs::unregister(&state.dynamo, table, team_id).await {
            warn!(team_id, error = %e, "Bedrock sync registry not updated");
        }
    }
    out
}

/// Queues a sync for every team on the registry.
pub async fn sweep(state: &WorkerState) -> usize {
    let teams = match bs::registry(&state.dynamo, &state.config.settings_table_name).await {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "Bedrock sync registry unreadable");
            return 0;
        }
    };
    let mut queued = 0;
    for team_id in teams.into_iter().take(MAX_PER_SWEEP) {
        let body = json!({ "type": "bedrock_sync", "team_id": team_id });
        match state
            .sqs
            .send_message()
            .queue_url(&state.config.ticket_queue_url)
            .message_body(body.to_string())
            .send()
            .await
        {
            Ok(_) => queued += 1,
            Err(e) => warn!(error = %e, "Bedrock sync not queued"),
        }
    }
    queued
}
