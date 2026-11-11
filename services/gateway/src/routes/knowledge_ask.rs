//! "Ask this knowledge base" on its page: a search that returns only the
//! ranked passages (one embedding, no model tokens), suggested questions,
//! and the hourly limit on asks from the page. A cited answer goes through
//! Chat (`POST /api/brain/stream` with the base mentioned and
//! `origin: "kb_page"`), which counts against the same limit.
//! The rules and the words live in `common::knowledge::ask`.

use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use common::knowledge::ask::{self, Suggested};
use common::knowledge::search::{search, Store, Titan};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn fail(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message })))
}

/// Counts one ask from a knowledge base page against the person's hourly
/// limit; refused (429) past [`ask::MAX_ASKS_PER_HOUR`]. A count that can't
/// be written lets the ask through: the limit guards cost, not access.
pub(crate) async fn count_ask(
    state: &AppState,
    team: &str,
    email: &str,
) -> Result<(), (StatusCode, Json<Value>)> {
    let now = chrono::Utc::now().timestamp();
    let sk = ask::ask_sk(email, ask::hour_of(now));
    let r = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team.to_string()))
        .key("sk", AttributeValue::S(sk))
        .update_expression("ADD asks :one SET #ttl = :ttl")
        .condition_expression("attribute_not_exists(asks) OR asks < :max")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":one", AttributeValue::N("1".into()))
        .expression_attribute_values(
            ":max",
            AttributeValue::N(ask::MAX_ASKS_PER_HOUR.to_string()),
        )
        .expression_attribute_values(":ttl", AttributeValue::N((now + 2 * 3600).to_string()))
        .send()
        .await;
    match r {
        Ok(_) => Ok(()),
        Err(e)
            if e.as_service_error()
                .is_some_and(|s| s.is_conditional_check_failed_exception()) =>
        {
            Err((
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": format!("You've asked {} questions here in the last hour. Ask again at the top of the hour, or keep going in Chat.", ask::MAX_ASKS_PER_HOUR),
                    "code": "rate_limited",
                })),
            ))
        }
        Err(e) => {
            warn!(error = %e, "Knowledge ask not counted");
            Ok(())
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SearchBody {
    pub query: String,
    #[serde(default)]
    pub k: Option<usize>,
}

/// POST /api/knowledge/:kb_id/search — the passages a search of this base
/// finds for a question, ranked, with a score against the best: what Chat
/// would read, without an answer. One embedding of the question; no model
/// tokens.
pub async fn search_base(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kb_id): Path<String>,
    Json(body): Json<SearchBody>,
) -> Reply {
    let base = super::knowledge::readable_base(&state, &claims, &kb_id).await?;
    let query = body.query.trim();
    if query.is_empty() || query.chars().count() > 2_000 {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Ask a question under 2,000 characters.",
        ));
    }
    count_ask(&state, &claims.team_id, &claims.email).await?;
    let k = body.k.unwrap_or(6).clamp(1, ask::MAX_PASSAGES);
    let meter = crate::model_call::chat_meter(&state, &claims.team_id);
    let titan = Titan {
        client: &state.bedrock,
        meter: &meter,
    };
    let st = super::knowledge::store(&state);
    let servers = super::knowledge::servers(&state, &claims.team_id);
    let actor = format!("knowledge-{}", claims.email);
    let found = search(
        &st,
        &titan,
        &super::knowledge::retriever(&state, &actor, &servers, Some(&claims.email)),
        &claims.team_id,
        std::slice::from_ref(&base.id),
        query,
        k,
        chrono::Utc::now(),
    )
    .await
    .map_err(|e| {
        warn!(error = %e, "Knowledge page search failed");
        fail(
            StatusCode::BAD_GATEWAY,
            "This knowledge base couldn't be searched right now. Try again.",
        )
    })?;
    super::knowledge::reembed(&state, &claims.team_id, &found.stale).await;
    info!(team_id = %claims.team_id, kb = %base.id, hits = found.hits.len(), "Knowledge page search");
    Ok(Json(json!({
        "passages": ask::passages(&found.hits, k),
        "reindexing": found.stale.len(),
        "unreached": found.unreached.iter().map(|u| u.reason.clone()).collect::<Vec<_>>(),
        "asks_per_hour": ask::MAX_ASKS_PER_HOUR,
    })))
}

async fn cached(state: &AppState, team: &str, kb_id: &str) -> Option<Suggested> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(team.to_string()))
        .key("sk", AttributeValue::S(ask::suggest_sk(kb_id)))
        .send()
        .await
        .ok()?
        .item()
        .and_then(common::knowledge::read_data::<Suggested>)
}

/// The questions a model writes from the titles, or `None` (no model, a
/// failed call, nothing usable).
async fn model_questions(
    state: &AppState,
    team: &str,
    base_name: &str,
    titles: &[String],
) -> Option<Vec<String>> {
    let (key, _, light) = super::brain::load_provider(state, team).await;
    let key = key.unwrap_or_default();
    if key.is_empty() && !common::model_provider::is_bedrock() {
        return None;
    }
    let body = json!({
        "model": light,
        "max_tokens": 300,
        "messages": [{ "role": "user", "content": ask::prompt(base_name, titles) }],
    });
    let meter = crate::model_call::chat_meter(state, team);
    let m = crate::model_call::brain_turn(&state.http, &key, &body, &meter, &light)
        .await
        .inspect_err(|e| warn!("Knowledge suggestions not written: {e}"))
        .ok()?;
    let text: String = m["content"]
        .as_array()
        .map(|c| {
            c.iter()
                .filter_map(|b| b["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    let q = ask::parse_questions(&text);
    (!q.is_empty()).then_some(q)
}

/// GET /api/knowledge/:kb_id/suggestions — questions to try. Kept per base
/// and built again only when its items changed by more than a tenth, or a
/// week went by; fixed sentences over the newest titles when no model can
/// write them.
pub async fn suggestions(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(kb_id): Path<String>,
) -> Reply {
    let base = super::knowledge::readable_base(&state, &claims, &kb_id).await?;
    let files = super::knowledge::store(&state)
        .files(&claims.team_id, &base.id)
        .await
        .unwrap_or_default();
    let sig = ask::signature(
        files
            .iter()
            .filter(|f| f.ready_version > 0)
            .map(|f| f.id.as_str()),
    );
    let now = chrono::Utc::now().timestamp();
    let old = cached(&state, &claims.team_id, &base.id).await;
    if !ask::needs_rebuild(old.as_ref(), &sig, now) {
        let s = old.unwrap_or_default();
        return Ok(Json(
            json!({ "questions": s.questions, "from": s.from, "built_at": s.built_at }),
        ));
    }
    if sig.is_empty() {
        return Ok(Json(
            json!({ "questions": [], "from": "titles", "built_at": now }),
        ));
    }
    let titles = ask::pick_titles(&files, ask::MAX_TITLES);
    let (questions, from) =
        match model_questions(&state, &claims.team_id, &base.name, &titles).await {
            Some(q) => (q, "model"),
            None => (ask::template_questions(&files), "titles"),
        };
    let s = Suggested {
        questions,
        built_at: now,
        from: from.into(),
        sig,
    };
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(ask::suggest_record(&claims.team_id, &base.id, &s)))
        .send()
        .await
    {
        warn!(error = %e, "Knowledge suggestions not kept");
    }
    info!(team_id = %claims.team_id, kb = %base.id, from, "Knowledge suggestions built");
    Ok(Json(
        json!({ "questions": s.questions, "from": s.from, "built_at": s.built_at }),
    ))
}
