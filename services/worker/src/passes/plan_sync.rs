//! Keeps a plan's task states following the work (`common::plan_sync`):
//! every run status the worker writes refreshes the plan the run belongs to.

use aws_sdk_dynamodb::types::AttributeValue;
use tracing::warn;

use crate::WorkerState;

/// The tables the plan state lives in.
pub fn store(state: &WorkerState) -> common::plan_sync::Dynamo {
    common::plan_sync::Dynamo {
        dynamo: state.dynamo.clone(),
        plans_table: state.config.plans_table_name.clone(),
        runs_table: state.config.runs_table_name.clone(),
        settings_table: state.config.settings_table_name.clone(),
    }
}

/// Re-derives a plan's state and its tasks'. Best effort: a failure is
/// logged and the next event or page view derives it again.
pub async fn refresh(state: &WorkerState, team_id: &str, plan_id: &str) {
    let now = chrono::Utc::now().to_rfc3339();
    if let Err(e) = common::plan_sync::refresh(&store(state), team_id, plan_id, &now).await {
        warn!(team_id, plan_id, error = %e, "Could not refresh the plan's state");
    }
}

/// A run's status changed: refresh its plan, when it has one.
pub async fn on_run(state: &WorkerState, team_id: &str, run_id: &str) {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.runs_table_name)
        .key("team_id", AttributeValue::S(team_id.into()))
        .key("run_id", AttributeValue::S(run_id.into()))
        .projection_expression("plan_id")
        .send()
        .await
        .ok()
        .and_then(|r| r.item);
    let Some((plan_id, _)) = item.as_ref().and_then(common::plan_sync::plan_of_run) else {
        return;
    };
    refresh(state, team_id, &plan_id).await;
}
