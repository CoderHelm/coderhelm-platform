//! Knowledge sources in the worker: syncing one source, and the scheduled
//! sweep that queues the ones that are due.
//!
//! The engine is `common::knowledge::sources::sync`; this opens the
//! source's connector with the team's connection (the GitHub App's token is
//! made here), embeds with Titan on the team's usage meter, and runs it.

use std::sync::Arc;

use serde_json::{json, Value};
use tracing::{info, warn};

use common::knowledge::search::Titan;
use common::knowledge::sources::store::{registry, AwsSyncStore, SyncStore};
use common::knowledge::sources::{connect, parse_registry_sk, sync, Kind, SyncMessage};

use crate::WorkerState;

/// Sources one sweep queues at most (the rest are due next time).
const MAX_PER_SWEEP: usize = 200;

fn store(state: &WorkerState) -> AwsSyncStore {
    AwsSyncStore {
        dynamo: state.dynamo.clone(),
        s3: state.s3.clone(),
        table: state.config.settings_table_name.clone(),
        bucket: state.config.bucket_name.clone(),
    }
}

fn mcp_table(state: &WorkerState) -> String {
    if state.config.mcp_configs_table_name.is_empty() {
        state.config.settings_table_name.clone()
    } else {
        state.config.mcp_configs_table_name.clone()
    }
}

/// Syncs one source. Never fails: the outcome is on the source's record.
pub async fn sync(state: &WorkerState, msg: &SyncMessage) -> Value {
    let st = store(state);
    let src = match st.source(&msg.team_id, &msg.kb_id, &msg.source_id).await {
        Ok(Some(s)) => s,
        Ok(None) => return json!({ "ok": false, "error": "no such source" }),
        Err(e) => return json!({ "ok": false, "error": e }),
    };
    let Some(kind) = src.kind else {
        return json!({ "ok": false, "error": "source has no kind" });
    };
    let github_token = if kind == Kind::Github {
        match super::plan_chat::github_for(state, &msg.team_id).await {
            Some(gh) => gh.installation_token().await.ok(),
            None => None,
        }
    } else {
        None
    };
    let env = connect::Env {
        dynamo: state.dynamo.clone(),
        kms: state.kms.clone(),
        secrets_key_id: state.config.secrets_key_id.clone(),
        settings_table: state.config.settings_table_name.clone(),
        mcp_table: mcp_table(state),
        team: msg.team_id.clone(),
        http: Arc::new(common::mcp_remote::http::GuardedHttps),
        github_token,
    };
    let now = || chrono::Utc::now().timestamp();
    let opened = match connect::open(&env, kind, &src.options).await {
        Ok(o) => o,
        Err(e) => {
            warn!(team_id = %msg.team_id, source = %msg.source_id, error = %e, "Knowledge source not opened");
            sync::fail(&st, &msg.team_id, &msg.kb_id, &msg.source_id, &e, now()).await;
            return json!({ "ok": false, "error": e.to_string() });
        }
    };
    let meter = common::usage::UsageMeter::dynamo(
        state.dynamo.clone(),
        &state.config.analytics_table_name,
        &msg.team_id,
    );
    let titan = Titan {
        client: &state.bedrock,
        meter: &meter,
    };
    let ctx = sync::Ctx {
        store: &st,
        connector: opened.connector.as_ref(),
        embedder: &titan,
        team: &msg.team_id,
        now: &now,
    };
    let out = sync::run(&ctx, &msg.kb_id, &msg.source_id).await;
    info!(
        team_id = %msg.team_id,
        source = %msg.source_id,
        ran = out.ran,
        listed = out.listed,
        fetched = out.fetched,
        same_text = out.same_text,
        indexed = out.indexed,
        embedded = out.embedded_chunks,
        reused = out.reused_chunks,
        removed = out.removed,
        failed = out.failed,
        "Knowledge source sync"
    );
    json!({
        "ok": out.error.is_none(),
        "ran": out.ran,
        "indexed": out.indexed,
        "removed": out.removed,
        "error": out.error.map(|e| e.to_string()),
    })
}

/// Queues a sync for every source that is due. Returns how many.
pub async fn sweep(state: &WorkerState) -> usize {
    let rows = match registry(&state.dynamo, &state.config.settings_table_name).await {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "Knowledge registry unreadable");
            return 0;
        }
    };
    let now = chrono::Utc::now().timestamp();
    let mut queued = 0;
    for row in rows {
        if row.paused || row.next > now || queued >= MAX_PER_SWEEP {
            continue;
        }
        let Some((team_id, kb_id, source_id)) = parse_registry_sk(&row.sk) else {
            continue;
        };
        let body = json!({
            "type": "knowledge_sync",
            "team_id": team_id,
            "kb_id": kb_id,
            "source_id": source_id,
        });
        let sent = state
            .sqs
            .send_message()
            .queue_url(&state.config.ticket_queue_url)
            .message_body(body.to_string())
            .send()
            .await;
        match sent {
            Ok(_) => queued += 1,
            Err(e) => warn!(error = %e, "Knowledge sync not queued"),
        }
    }
    queued
}
