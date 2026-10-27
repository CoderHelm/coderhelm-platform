//! Knowledge sources in the worker: syncing one source, and the scheduled
//! sweep that queues the ones that are due.
//!
//! The engine is `common::knowledge::sources::sync`; this opens the
//! source's connector with the team's connection (the GitHub App's token is
//! made here) or, for a source read through a member's sign-in, with that
//! member's connection, embeds with Titan on the team's usage meter, and
//! runs it.
//!
//! A sign-in source whose member left the team stops with one sentence and
//! "Sign in again" (their sign-in is never used after they leave). When a
//! sign-in source stops for want of a new sign-in, the team's owner and
//! admins are told once, in the team's chat channels and by email.

use std::sync::Arc;

use serde_json::{json, Value};
use tracing::{info, warn};

use common::knowledge::search::Titan;
use common::knowledge::sources::store::{registry, AwsSyncStore, SyncStore};
use common::knowledge::sources::{
    alert, connect, parse_registry_sk, signin_error, sync, Kind, Source, SyncMessage,
};
use common::mcp_remote::store::Owner;

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
    if let Some(si) = &src.signin {
        let personal = matches!(Owner::parse(&si.owner), Some(Owner::User(_)));
        if personal && member_on_team(state, &msg.team_id, &si.email).await == Some(false) {
            let e = signin_error(
                si,
                kind,
                format!("{} is no longer on the team.", si.who()),
                true,
            );
            info!(team_id = %msg.team_id, source = %msg.source_id, "Knowledge source's member left; asking for a new sign-in");
            sync::fail_with(&st, &msg.team_id, &msg.kb_id, &msg.source_id, e, now()).await;
            after_sync(state, &st, msg).await;
            return json!({ "ok": false, "error": "member left" });
        }
    }
    let opened = match connect::open_source(&env, &src).await {
        Ok(o) => o,
        Err(e) => {
            warn!(team_id = %msg.team_id, source = %msg.source_id, error = %e, "Knowledge source not opened");
            sync::fail(&st, &msg.team_id, &msg.kb_id, &msg.source_id, &e, now()).await;
            after_sync(state, &st, msg).await;
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
    if src.signin.is_some() {
        after_sync(state, &st, msg).await;
    }
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

/// Is `email` still a member of the team? `None` when the team's records
/// can't be read (the sync then goes ahead; the sign-in itself still has to
/// work).
async fn member_on_team(state: &WorkerState, team: &str, email: &str) -> Option<bool> {
    use aws_sdk_dynamodb::types::AttributeValue as Av;
    let mut start = None;
    for _ in 0..20 {
        let resp = state
            .dynamo
            .query()
            .table_name(&state.config.users_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", Av::S(team.to_string()))
            .expression_attribute_values(":p", Av::S("USER#".into()))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| warn!(error = %e, "Team members unreadable for a knowledge sign-in"))
            .ok()?;
        let found = resp.items().iter().any(|i| {
            let s = |k: &str| {
                i.get(k)
                    .and_then(|v| v.as_s().ok())
                    .map(String::as_str)
                    .unwrap_or("")
            };
            s("email").eq_ignore_ascii_case(email) && s("status") != "invited"
        });
        if found {
            return Some(true);
        }
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => return Some(false),
        }
    }
    None
}

/// After a sign-in source's sync: tells the team once when it needs a new
/// sign-in, and forgets that it told them once it works again.
async fn after_sync(state: &WorkerState, st: &AwsSyncStore, msg: &SyncMessage) {
    let Ok(Some(mut src)) = st.source(&msg.team_id, &msg.kb_id, &msg.source_id).await else {
        return;
    };
    if alert::should_alert(&src) {
        tell_admins(state, &msg.team_id, &src).await;
        if let Some(si) = src.signin.as_mut() {
            si.alerted_at = chrono::Utc::now().to_rfc3339();
        }
        let _ = st.put_source(&msg.team_id, &src).await;
        return;
    }
    let working = src.error.is_none();
    if let Some(si) = src
        .signin
        .as_mut()
        .filter(|si| working && !si.alerted_at.is_empty())
    {
        si.alerted_at.clear();
        let _ = st.put_source(&msg.team_id, &src).await;
    }
}

/// The team's chat channels and an email to each owner and admin.
async fn tell_admins(state: &WorkerState, team: &str, src: &Source) {
    use aws_sdk_dynamodb::types::AttributeValue as Av;
    let Some(e) = src.error.as_ref() else { return };
    let app = common::public_urls::app_url(&state.config.stage);
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", Av::S(team.to_string()))
        .key("sk", Av::S(common::alert_notify::TEAM_SETTINGS_SK.into()))
        .send()
        .await
        .ok()
        .and_then(|o| o.item().cloned())
        .unwrap_or_default();
    let cipher = common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: "",
    };
    let channels = common::notify::store::open_all(&cipher, team, &item).await;
    if !channels.is_empty() {
        common::notify::deliver(
            &common::notify::Http(&state.http),
            &channels,
            &alert::signin_message(app, src, e),
            "knowledge sign-in",
        )
        .await;
    }
    let (subject, body) = alert::signin_email(app, src, e);
    use common::mail::templates::fit;
    for to in admin_emails(state, team).await {
        let mail = common::mail::Mail {
            kind: "knowledge_signin".into(),
            to: to.clone(),
            template: "knowledge_signin",
            params: serde_json::json!({
                "source": fit(&src.name, 80),
                "problem": fit(&e.fix, 300),
                "link": alert::source_link(app, src),
            })
            .as_object()
            .cloned()
            .unwrap_or_default(),
            ses: common::mail::SesBody::Simple {
                subject: subject.clone(),
                text: body.clone(),
                html: None,
                headers: vec![],
            },
        };
        let sent = common::mail::send(&crate::clients::email::sender(state), &mail).await;
        common::deliveries::record(
            &state.dynamo,
            &state.config.settings_table_name,
            team,
            &common::mail::delivery("knowledge_signin", &to, &sent),
        )
        .await;
        if let Err(err) = sent {
            warn!(team, code = %err.code, "Knowledge sign-in email not sent");
        }
    }
}

/// The emails of the team's owners and admins.
async fn admin_emails(state: &WorkerState, team: &str) -> Vec<String> {
    use aws_sdk_dynamodb::types::AttributeValue as Av;
    let mut out = Vec::new();
    let mut start = None;
    for _ in 0..20 {
        let Ok(resp) = state
            .dynamo
            .query()
            .table_name(&state.config.users_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", Av::S(team.to_string()))
            .expression_attribute_values(":p", Av::S("USER#".into()))
            .set_exclusive_start_key(start.take())
            .send()
            .await
        else {
            break;
        };
        for i in resp.items() {
            let s = |k: &str| {
                i.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            let (email, role) = (s("email"), s("role"));
            if !email.is_empty() && s("status") != "invited" && (role == "owner" || role == "admin")
            {
                out.push(email);
            }
        }
        match resp.last_evaluated_key() {
            Some(k) => start = Some(k.clone()),
            None => break,
        }
    }
    out
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
