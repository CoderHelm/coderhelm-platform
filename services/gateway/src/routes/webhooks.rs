//! Webhook endpoints: where CoderHelm sends a team's events (admins, and API
//! keys with the `webhooks` scope).
//!
//! - `GET /api/webhooks/event-types`: the event catalog.
//! - `GET|POST /api/webhooks/endpoints`, `GET|PUT|DELETE …/:endpoint_id`.
//! - `GET …/:endpoint_id/secret` reveals the signing secret;
//!   `POST …/secret/rotate` makes a new one (the old one keeps signing
//!   through an overlap).
//! - `POST …/:endpoint_id/test` sends a `webhook.test` event now.
//! - `GET …/:endpoint_id/deliveries`, `POST …/deliveries/:delivery_id/resend`.
//! - `POST …/:endpoint_id/replay` sends the kept events of a time range again.
//!
//! The rules (signing, retries, address checks, turning failing endpoints
//! off) are in `common::webhooks`; every attempt is a Deliveries record.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use common::deliveries::{self, Delivery, Filter, Status};
use common::webhooks::{self, catalog, endpoint, event, Mode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use crate::models::Claims;
use crate::routes::roles::{self, AuditEntry};
use crate::AppState;

type ApiError = (StatusCode, Json<Value>);
type Reply = Result<Json<Value>, ApiError>;

fn fail(code: StatusCode, message: &str) -> ApiError {
    (code, Json(json!({ "error": message })))
}

fn internal(context: &str, e: impl std::fmt::Display) -> ApiError {
    tracing::error!("{context}: {e}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong on our side. Try again.",
    )
}

fn admins_only(c: StatusCode) -> ApiError {
    fail(c, "Only admins can manage webhooks.")
}

fn kms(state: &AppState) -> common::mcp_credentials::Kms<'_> {
    common::mcp_credentials::Kms {
        client: &state.kms,
        key_id: &state.config.secrets_key_id,
    }
}

fn now_secs(now: chrono::DateTime<chrono::Utc>) -> String {
    now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

async fn load(state: &AppState, team: &str, id: &str) -> Result<endpoint::Stored, ApiError> {
    endpoint::get(&state.dynamo, &state.config.settings_table_name, team, id)
        .await
        .map_err(|e| internal("Failed to read a webhook endpoint", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such webhook endpoint."))
}

/// The address's name must resolve, and only to public addresses.
async fn check_public(url: &str) -> Result<(), ApiError> {
    let check = webhooks::deliver::check_address(&webhooks::deliver::SystemDns, url);
    match tokio::time::timeout(std::time::Duration::from_secs(5), check).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(message)) => Err(invalid(endpoint::Invalid {
            field: "url",
            message,
        })),
        Err(_) => Err(invalid(endpoint::Invalid {
            field: "url",
            message: "Looking up the address took too long. Try again.".into(),
        })),
    }
}

fn invalid(i: endpoint::Invalid) -> ApiError {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": i.message, "code": "bad_request", "field": i.field })),
    )
}

// ── Emitting ────────────────────────────────────────────────────────

/// Queues one event for the team's endpoints (`common::webhooks::emit_event`).
pub async fn emit(state: &AppState, team_id: &str, event_type: &str, data: Value) {
    webhooks::emit_event(
        &state.sqs,
        &state.config.webhook_queue_url,
        team_id,
        event_type,
        data,
    )
    .await;
}

/// Who did something, as events name them: a person's login or email, or
/// `api_key:<id>` for a request made with an API key.
pub fn actor(claims: &Claims) -> String {
    match &claims.api_key {
        Some(k) => format!("api_key:{}", k.id),
        None => claims.display_name(),
    }
}

/// `plan.approved`'s `data` (pure).
pub fn plan_approved_data(plan_id: &str, title: &str, tasks: usize, by: &str, app: &str) -> Value {
    json!({
        "plan_id": plan_id,
        "title": title,
        "task_count": tasks,
        "approved_by": by,
        "url": format!("{}/plans/detail/?id={}", app.trim_end_matches('/'), urlencoding::encode(plan_id)),
    })
}

/// The plan's title from a plan query's items (the `PLAN#<id>` item).
pub fn plan_title(
    items: &[std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>],
    plan_id: &str,
) -> String {
    let sk = format!("PLAN#{plan_id}");
    items
        .iter()
        .find(|i| i.get("sk").and_then(|v| v.as_s().ok()) == Some(&sk))
        .and_then(|i| i.get("title"))
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

/// Emits `plan.approved`.
pub async fn emit_plan_approved(
    state: &AppState,
    claims: &Claims,
    plan_id: &str,
    title: &str,
    tasks: usize,
) {
    let app = common::public_urls::app_url(&state.config.stage);
    emit(
        state,
        &claims.team_id,
        "plan.approved",
        plan_approved_data(plan_id, title, tasks, &actor(claims), app),
    )
    .await;
}

// ── Shapes (for the OpenAPI document) ───────────────────────────────

/// A webhook endpoint. Never carries its secret.
#[derive(Serialize, ToSchema)]
pub struct WebhookEndpoint {
    /// `ep_` and a ULID.
    #[schema(example = "ep_01jbendpt4a7b0c3d6e9f2g5h8")]
    pub id: String,
    /// Where events are POSTed. Always `https://`.
    #[schema(example = "https://hooks.example.com/coderhelm")]
    pub url: String,
    /// A note for people: what the endpoint is for.
    #[schema(example = "Deploy bot")]
    pub description: String,
    /// Event types it receives; `["*"]` is every event, including types
    /// added later.
    #[schema(example = json!(["run.completed", "run.failed"]))]
    pub events: Vec<String>,
    /// Off: nothing is sent (tests still are).
    #[schema(example = true)]
    pub enabled: bool,
    /// Why it's off: `manual` (turned off by a person) or `failing` (turned
    /// off by CoderHelm after 5 days of failed deliveries). Empty while on.
    #[schema(example = "")]
    pub disabled_reason: String,
    /// The last characters of the signing secret.
    #[schema(example = "whsec_…Q2c=")]
    pub secret_hint: String,
    /// While a rotated-out secret still signs too, when it stops.
    #[schema(nullable, example = "2026-10-27T09:00:00Z")]
    pub previous_secret_expires_at: Option<String>,
    /// The first failed delivery since the last success.
    #[schema(nullable, example = json!(null))]
    pub failing_since: Option<String>,
    /// The last delivered attempt.
    #[schema(nullable, example = "2026-10-26T14:31:48.120Z")]
    pub last_success_at: Option<String>,
    /// The last failed attempt.
    #[schema(nullable, example = "2026-10-25T08:02:11.004Z")]
    pub last_failure_at: Option<String>,
    /// When it was added.
    #[schema(example = "2026-10-20T09:00:00Z")]
    pub created_at: String,
    /// Who added it.
    #[schema(example = "lead@example.com")]
    pub created_by: String,
    /// When it last changed.
    #[schema(example = "2026-10-20T09:00:00Z")]
    pub updated_at: String,
}

fn view(e: &endpoint::Endpoint) -> Value {
    serde_json::to_value(e).unwrap_or_default()
}

/// A new endpoint.
#[derive(Deserialize, ToSchema)]
pub struct CreateEndpointBody {
    /// An `https://` address on the public internet.
    #[schema(example = "https://hooks.example.com/coderhelm")]
    pub url: String,
    /// A note for people: what the endpoint is for.
    #[serde(default)]
    #[schema(example = "Deploy bot")]
    pub description: Option<String>,
    /// Event types to receive, or `["*"]` for every event. Defaults to every
    /// event.
    #[serde(default)]
    #[schema(example = json!(["run.completed", "run.failed"]))]
    pub events: Option<Vec<String>>,
    /// Defaults to `true`.
    #[serde(default)]
    #[schema(example = true)]
    pub enabled: Option<bool>,
}

/// Changes to an endpoint. Only the fields sent change.
#[derive(Deserialize, ToSchema)]
pub struct UpdateEndpointBody {
    /// A new `https://` address.
    #[serde(default)]
    #[schema(example = "https://hooks.example.com/coderhelm/v2")]
    pub url: Option<String>,
    /// A new note.
    #[serde(default)]
    #[schema(example = "Deploy bot (staging)")]
    pub description: Option<String>,
    /// The new subscription list, or `["*"]`.
    #[serde(default)]
    #[schema(example = json!(["*"]))]
    pub events: Option<Vec<String>>,
    /// Turning an endpoint back on starts its failure count again.
    #[serde(default)]
    #[schema(example = true)]
    pub enabled: Option<bool>,
}

/// A new endpoint and its signing secret. The secret is shown here once;
/// reveal it later with `GET …/secret`.
#[derive(Serialize, ToSchema)]
pub struct CreatedEndpoint {
    pub endpoint: WebhookEndpoint,
    /// `whsec_` and 32 random bytes in base64. Verify signatures with it.
    #[schema(example = "whsec_C2FVsBQIhrscChlQIMV+b5sSYspob7oD7vS8nSUIQ2c=")]
    pub secret: String,
}

/// An endpoint's signing secret.
#[derive(Serialize, ToSchema)]
pub struct SecretView {
    /// The current secret.
    #[schema(example = "whsec_C2FVsBQIhrscChlQIMV+b5sSYspob7oD7vS8nSUIQ2c=")]
    pub secret: String,
    /// While the previous secret still signs too, when it stops.
    #[schema(nullable, example = json!(null))]
    pub previous_secret_expires_at: Option<String>,
}

/// How long the old secret keeps signing.
#[derive(Deserialize, ToSchema, Default)]
pub struct RotateBody {
    /// Hours the old secret keeps signing beside the new one: 0 to 168.
    /// Defaults to 24. With 0 the old secret stops at once.
    #[serde(default)]
    #[schema(example = 24)]
    pub overlap_hours: Option<i64>,
}

/// A rotated secret.
#[derive(Serialize, ToSchema)]
pub struct RotatedSecret {
    /// The new secret. Requests are signed with it from now on.
    #[schema(example = "whsec_k0q8vX2mZp5Rr3t1YbN6cH9dLwE4fJ7aS1uG0iT2oQs=")]
    pub secret: String,
    /// Until then, requests carry a second signature made with the old one.
    #[schema(nullable, example = "2026-10-27T09:00:00Z")]
    pub previous_secret_expires_at: Option<String>,
}

/// One delivery of an event to an endpoint.
#[derive(Serialize, ToSchema)]
pub struct WebhookDelivery {
    /// The Deliveries record's id.
    #[schema(example = "01JBDLVR5T8V1W4X7Y0Z3A6B9C")]
    pub id: String,
    /// The event's id: the same `webhook-id` on every attempt.
    #[schema(example = "evt_01JBEVNT8M1N4P7Q0R3S6T9V2W")]
    pub event_id: String,
    /// The event type.
    #[schema(example = "run.completed")]
    pub event: String,
    /// `pending` (being retried), `handled` (delivered) or `failed`.
    #[schema(example = "failed")]
    pub status: String,
    /// What happened, in a few words.
    #[schema(example = "Refused (401)")]
    pub outcome: String,
    /// A reason code when it failed.
    #[schema(example = "webhook_refused")]
    pub reason: String,
    /// The reason in plain words.
    #[schema(
        example = "Your webhook endpoint refused the event (a 4xx answer), so it isn't retried."
    )]
    pub reason_message: String,
    /// What to do about it.
    #[schema(
        example = "Check that the endpoint verifies signatures with the right secret and accepts JSON, then press Resend."
    )]
    pub reason_fix: String,
    /// Every attempt, oldest first (the last 20).
    pub attempts: Vec<DeliveryAttempt>,
    /// When the next retry is due, while one is.
    #[schema(nullable, example = json!(null))]
    pub next_retry_at: Option<String>,
    /// When the event was first sent to this endpoint.
    #[schema(example = "2026-10-26T14:31:47.780Z")]
    pub created_at: String,
    /// The body that was sent, with secret-looking values masked, cut at 4 KB.
    #[schema(example = "{\n  \"id\": \"evt_01JBEVNT8M1N4P7Q0R3S6T9V2W\" …")]
    pub payload_preview: String,
    /// The request's headers, the signature removed.
    #[schema(example = json!({ "webhook-id": "evt_01JBEVNT8M1N4P7Q0R3S6T9V2W", "webhook-timestamp": "1792420307", "webhook-signature": "[redacted]" }))]
    pub headers: std::collections::BTreeMap<String, String>,
}

/// One attempt.
#[derive(Serialize, ToSchema)]
pub struct DeliveryAttempt {
    /// When it was sent.
    #[schema(example = "2026-10-26T14:31:47.901Z")]
    pub at: String,
    /// The endpoint's HTTP status, when it answered.
    #[schema(nullable, example = 401)]
    pub code: Option<u16>,
    /// How long it took.
    #[schema(example = 112)]
    pub duration_ms: u64,
    /// What went wrong, when it did.
    #[schema(example = "Answered 401: bad signature")]
    pub error: String,
}

fn delivery_view(d: &Delivery) -> Value {
    let r = (!d.reason.is_empty()).then(|| deliveries::reason(&d.reason));
    json!({
        "id": d.id,
        "event_id": d.external_id,
        "event": d.event,
        "status": d.status,
        "outcome": d.outcome,
        "reason": d.reason,
        "reason_message": r.map(|r| r.message).unwrap_or(""),
        "reason_fix": r.map(|r| r.fix).unwrap_or(""),
        "attempts": d.attempts,
        "next_retry_at": d.next_retry_at,
        "created_at": d.created_at,
        "payload_preview": d.payload_preview,
        "headers": d.headers,
    })
}

/// Which kept events to send again.
#[derive(Deserialize, ToSchema)]
pub struct ReplayBody {
    /// RFC 3339. At most 14 days ago (events are kept that long).
    #[schema(example = "2026-10-25T00:00:00Z")]
    pub since: String,
    /// RFC 3339. Defaults to now.
    #[serde(default)]
    #[schema(example = "2026-10-26T00:00:00Z")]
    pub until: Option<String>,
    /// Only events whose deliveries to this endpoint all failed, or that it
    /// never got. Defaults to `false` (every event it subscribes to).
    #[serde(default)]
    #[schema(example = true)]
    pub only_failed: bool,
}

/// What a replay queued.
#[derive(Serialize, ToSchema)]
pub struct ReplayResult {
    /// Events queued to send again.
    #[schema(example = 12)]
    pub queued: usize,
    /// Kept events in the range (of every type).
    #[schema(example = 40)]
    pub found: usize,
    /// More matched than one replay sends (500); replay again from the last one.
    #[schema(example = false)]
    pub capped: bool,
}

/// The `limit` and `status` of a delivery list.
#[derive(Deserialize, IntoParams, Default)]
#[into_params(parameter_in = Query)]
pub struct DeliveryQuery {
    /// `pending`, `failed`, `ok` or `all` (default).
    #[serde(default)]
    pub status: Option<String>,
    /// 1 to 100, default 25.
    #[serde(default)]
    pub limit: Option<usize>,
}

// ── Handlers ────────────────────────────────────────────────────────

/// GET /api/webhooks/event-types
#[utoipa::path(
    get,
    path = "/api/webhooks/event-types",
    tag = "Webhooks",
    operation_id = "listWebhookEventTypes",
    summary = "List event types",
    description = "Every event type CoderHelm sends, with an example event. The `webhooks` section of this document has each one's schema.",
    responses(
        (status = 200, description = "The catalog.", body = inline(Object),
            example = json!({ "api_version": "2026-10-26", "event_types": [{ "type": "run.completed", "group": "Runs", "summary": "A run finished", "description": "…", "example": {} }] })),
    ),
    security(("api_key" = ["webhooks"])),
)]
pub async fn event_types(Extension(claims): Extension<Claims>) -> Reply {
    claims.require_role(3).map_err(admins_only)?;
    let list: Vec<Value> = catalog::EVENTS
        .iter()
        .map(|e| {
            json!({
                "type": e.name,
                "group": e.group,
                "summary": e.summary,
                "description": e.description,
                "subscribable": e.name != catalog::TEST_EVENT,
                "example": catalog::example_event(e.name),
            })
        })
        .collect();
    Ok(Json(
        json!({ "api_version": catalog::API_VERSION, "event_types": list }),
    ))
}

/// GET /api/webhooks/endpoints
#[utoipa::path(
    get,
    path = "/api/webhooks/endpoints",
    tag = "Webhooks",
    operation_id = "listWebhookEndpoints",
    summary = "List webhook endpoints",
    description = "The team's webhook endpoints, oldest first. Secrets are never listed.",
    responses(
        (status = 200, description = "The endpoints.", body = inline(Object),
            example = json!({ "endpoints": [{ "id": "ep_01jbendpt4a7b0c3d6e9f2g5h8", "url": "https://hooks.example.com/coderhelm", "description": "Deploy bot", "events": ["*"], "enabled": true, "disabled_reason": "", "secret_hint": "whsec_…Q2c=", "created_at": "2026-10-20T09:00:00Z", "created_by": "lead@example.com", "updated_at": "2026-10-20T09:00:00Z" }], "max": 20 })),
    ),
    security(("api_key" = ["webhooks"])),
)]
pub async fn list_endpoints(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    claims.require_role(3).map_err(admins_only)?;
    let all = endpoint::list(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
    )
    .await
    .map_err(|e| internal("Failed to list webhook endpoints", e))?;
    Ok(Json(json!({
        "endpoints": all.iter().map(|s| view(&s.endpoint)).collect::<Vec<_>>(),
        "max": endpoint::MAX_PER_TEAM,
        "disable_after_days": webhooks::deliver::DISABLE_AFTER_DAYS,
        "retention_days": deliveries::RETENTION_DAYS,
    })))
}

/// POST /api/webhooks/endpoints
#[utoipa::path(
    post,
    path = "/api/webhooks/endpoints",
    tag = "Webhooks",
    operation_id = "createWebhookEndpoint",
    summary = "Create a webhook endpoint",
    description = "Adds an endpoint and makes its signing secret, which this answer shows once. \
        The address must be `https://` and on the public internet: private, local and cloud \
        metadata addresses are refused here and again before every delivery. A team has at most \
        20 endpoints.",
    request_body = CreateEndpointBody,
    responses(
        (status = 201, description = "Created. Keep the secret.", body = CreatedEndpoint),
        (status = 400, description = "The address, events or description were refused; `field` names which.", body = crate::openapi::ErrorBody),
        (status = 409, description = "The team already has 20 endpoints.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["webhooks"])),
)]
pub async fn create_endpoint(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<CreateEndpointBody>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    claims.require_role(3).map_err(admins_only)?;
    let table = &state.config.settings_table_name;
    let now = chrono::Utc::now();
    let input = endpoint::Input {
        url: Some(body.url),
        description: body.description,
        events: body.events,
        enabled: body.enabled,
    };
    let mut e =
        endpoint::new_endpoint(&input, &claims.display_name(), &now_secs(now)).map_err(invalid)?;
    check_public(&e.url).await?;
    let existing = endpoint::list(&state.dynamo, table, &claims.team_id)
        .await
        .map_err(|e| internal("Failed to list webhook endpoints", e))?;
    if existing.len() >= endpoint::MAX_PER_TEAM {
        return Err(fail(
            StatusCode::CONFLICT,
            &format!(
                "A team can have {} webhook endpoints. Delete one first.",
                endpoint::MAX_PER_TEAM
            ),
        ));
    }
    let (secret, sealed) = endpoint::fresh_secret(&kms(&state), &claims.team_id, &e.id)
        .await
        .map_err(|err| internal("Failed to seal a webhook secret", err))?;
    e.secret_hint = webhooks::sign::hint(&secret);
    let st = endpoint::Stored {
        endpoint: e,
        secret_sealed: sealed,
        previous_secret_sealed: String::new(),
    };
    roles::record(
        &state,
        &claims,
        AuditEntry {
            action: "webhook_endpoint_created",
            role_id: &st.endpoint.id,
            role_name: &st.endpoint.url,
            detail: format!("Events: {}", st.endpoint.events.join(", ")),
        },
    )
    .await?;
    endpoint::put(&state.dynamo, table, &claims.team_id, &st)
        .await
        .map_err(|e| internal("Failed to save a webhook endpoint", e))?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "endpoint": view(&st.endpoint), "secret": secret })),
    ))
}

/// GET /api/webhooks/endpoints/:endpoint_id
#[utoipa::path(
    get,
    path = "/api/webhooks/endpoints/{endpoint_id}",
    tag = "Webhooks",
    operation_id = "getWebhookEndpoint",
    summary = "Get a webhook endpoint",
    description = "One endpoint, with its health: when it last delivered, and since when it has been failing, if it is.",
    params(("endpoint_id" = String, Path, description = "The endpoint's id.", example = "ep_01jbendpt4a7b0c3d6e9f2g5h8")),
    responses(
        (status = 200, description = "The endpoint.", body = WebhookEndpoint),
        (status = 404, description = "No such endpoint on this team.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["webhooks"])),
)]
pub async fn get_endpoint(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(endpoint_id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(admins_only)?;
    let st = load(&state, &claims.team_id, &endpoint_id).await?;
    Ok(Json(view(&st.endpoint)))
}

/// PUT /api/webhooks/endpoints/:endpoint_id
#[utoipa::path(
    put,
    path = "/api/webhooks/endpoints/{endpoint_id}",
    tag = "Webhooks",
    operation_id = "updateWebhookEndpoint",
    summary = "Update a webhook endpoint",
    description = "Changes the address, description, events, or turns the endpoint on or off. Fields left out keep their value. Turning an endpoint back on starts its failure count again; events missed while it was off are not sent by themselves (replay them).",
    params(("endpoint_id" = String, Path, description = "The endpoint's id.", example = "ep_01jbendpt4a7b0c3d6e9f2g5h8")),
    request_body = UpdateEndpointBody,
    responses(
        (status = 200, description = "The endpoint as saved.", body = WebhookEndpoint),
        (status = 400, description = "A field was refused; `field` names which.", body = crate::openapi::ErrorBody),
        (status = 404, description = "No such endpoint on this team.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["webhooks"])),
)]
pub async fn update_endpoint(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(endpoint_id): Path<String>,
    Json(body): Json<UpdateEndpointBody>,
) -> Reply {
    claims.require_role(3).map_err(admins_only)?;
    let mut st = load(&state, &claims.team_id, &endpoint_id).await?;
    let input = endpoint::Input {
        url: body.url,
        description: body.description,
        events: body.events,
        enabled: body.enabled,
    };
    let old_url = st.endpoint.url.clone();
    endpoint::apply_input(&mut st.endpoint, &input, &now_secs(chrono::Utc::now()))
        .map_err(invalid)?;
    if st.endpoint.url != old_url {
        check_public(&st.endpoint.url).await?;
    }
    endpoint::put(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
        &st,
    )
    .await
    .map_err(|e| internal("Failed to save a webhook endpoint", e))?;
    Ok(Json(view(&st.endpoint)))
}

/// DELETE /api/webhooks/endpoints/:endpoint_id
#[utoipa::path(
    delete,
    path = "/api/webhooks/endpoints/{endpoint_id}",
    tag = "Webhooks",
    operation_id = "deleteWebhookEndpoint",
    summary = "Delete a webhook endpoint",
    description = "Stops all deliveries to it, including retries already queued.",
    params(("endpoint_id" = String, Path, description = "The endpoint's id.", example = "ep_01jbendpt4a7b0c3d6e9f2g5h8")),
    responses(
        (status = 200, description = "Deleted.", body = inline(Object), example = json!({ "deleted": true })),
        (status = 404, description = "No such endpoint on this team.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["webhooks"])),
)]
pub async fn delete_endpoint(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(endpoint_id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(admins_only)?;
    let st = load(&state, &claims.team_id, &endpoint_id).await?;
    roles::record(
        &state,
        &claims,
        AuditEntry {
            action: "webhook_endpoint_deleted",
            role_id: &st.endpoint.id,
            role_name: &st.endpoint.url,
            detail: String::new(),
        },
    )
    .await?;
    endpoint::delete(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
        &endpoint_id,
    )
    .await
    .map_err(|e| internal("Failed to delete a webhook endpoint", e))?;
    Ok(Json(json!({ "deleted": true })))
}

/// GET /api/webhooks/endpoints/:endpoint_id/secret
#[utoipa::path(
    get,
    path = "/api/webhooks/endpoints/{endpoint_id}/secret",
    tag = "Webhooks",
    operation_id = "getWebhookSecret",
    summary = "Reveal the signing secret",
    description = "The endpoint's current signing secret. Each reveal is written to the team's audit log.",
    params(("endpoint_id" = String, Path, description = "The endpoint's id.", example = "ep_01jbendpt4a7b0c3d6e9f2g5h8")),
    responses(
        (status = 200, description = "The secret.", body = SecretView),
        (status = 404, description = "No such endpoint on this team.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["webhooks"])),
)]
pub async fn reveal_secret(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(endpoint_id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(admins_only)?;
    let st = load(&state, &claims.team_id, &endpoint_id).await?;
    let secret = endpoint::open(
        &kms(&state),
        &claims.team_id,
        &st.endpoint.id,
        &st.secret_sealed,
    )
    .await
    .map_err(|e| internal("Failed to open a webhook secret", e))?;
    roles::record(
        &state,
        &claims,
        AuditEntry {
            action: "webhook_secret_revealed",
            role_id: &st.endpoint.id,
            role_name: &st.endpoint.url,
            detail: String::new(),
        },
    )
    .await?;
    let live = endpoint::previous_live(&st, chrono::Utc::now());
    Ok(Json(json!({
        "secret": secret,
        "previous_secret_expires_at": if live { st.endpoint.previous_secret_expires_at.clone() } else { None },
    })))
}

/// POST /api/webhooks/endpoints/:endpoint_id/secret/rotate
#[utoipa::path(
    post,
    path = "/api/webhooks/endpoints/{endpoint_id}/secret/rotate",
    tag = "Webhooks",
    operation_id = "rotateWebhookSecret",
    summary = "Rotate the signing secret",
    description = "Makes a new secret. For `overlap_hours` (24 unless set) every request carries two \
        signatures, one per secret, so a receiver verifying with either keeps working while you \
        update it. Then only the new secret signs.",
    params(("endpoint_id" = String, Path, description = "The endpoint's id.", example = "ep_01jbendpt4a7b0c3d6e9f2g5h8")),
    request_body = RotateBody,
    responses(
        (status = 200, description = "The new secret, shown once here.", body = RotatedSecret),
        (status = 400, description = "`overlap_hours` is outside 0 to 168.", body = crate::openapi::ErrorBody),
        (status = 404, description = "No such endpoint on this team.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["webhooks"])),
)]
pub async fn rotate_secret(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(endpoint_id): Path<String>,
    body: Option<Json<RotateBody>>,
) -> Reply {
    claims.require_role(3).map_err(admins_only)?;
    let hours = body
        .and_then(|Json(b)| b.overlap_hours)
        .unwrap_or(endpoint::DEFAULT_OVERLAP_HOURS);
    if !(0..=endpoint::MAX_OVERLAP_HOURS).contains(&hours) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "The overlap is 0 to 168 hours.",
        ));
    }
    let mut st = load(&state, &claims.team_id, &endpoint_id).await?;
    let (secret, sealed) = endpoint::fresh_secret(&kms(&state), &claims.team_id, &st.endpoint.id)
        .await
        .map_err(|e| internal("Failed to seal a webhook secret", e))?;
    endpoint::rotate(&mut st, &secret, sealed, hours, chrono::Utc::now());
    roles::record(
        &state,
        &claims,
        AuditEntry {
            action: "webhook_secret_rotated",
            role_id: &st.endpoint.id,
            role_name: &st.endpoint.url,
            detail: format!("Old secret signs for {hours} more hours"),
        },
    )
    .await?;
    endpoint::put(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
        &st,
    )
    .await
    .map_err(|e| internal("Failed to save a webhook endpoint", e))?;
    Ok(Json(json!({
        "secret": secret,
        "previous_secret_expires_at": st.endpoint.previous_secret_expires_at,
    })))
}

fn ctx<'a>(state: &'a AppState, cipher: &'a common::mcp_credentials::Kms<'a>) -> webhooks::Ctx<'a> {
    webhooks::Ctx {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
        cipher,
        sqs: &state.sqs,
        queue_url: &state.config.webhook_queue_url,
        dns: &webhooks::deliver::SystemDns,
    }
}

/// POST /api/webhooks/endpoints/:endpoint_id/test
#[utoipa::path(
    post,
    path = "/api/webhooks/endpoints/{endpoint_id}/test",
    tag = "Webhooks",
    operation_id = "sendWebhookTest",
    summary = "Send a test event",
    description = "Sends one `webhook.test` event to the endpoint now, signed like every other event, \
        and answers with the attempt. A test is tried once (no retries) and is sent even when the \
        endpoint is off or doesn't subscribe to it.",
    params(("endpoint_id" = String, Path, description = "The endpoint's id.", example = "ep_01jbendpt4a7b0c3d6e9f2g5h8")),
    responses(
        (status = 200, description = "The delivery, delivered or not.", body = WebhookDelivery),
        (status = 404, description = "No such endpoint on this team.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["webhooks"])),
)]
pub async fn send_test(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(endpoint_id): Path<String>,
) -> Reply {
    claims.require_role(3).map_err(admins_only)?;
    let st = load(&state, &claims.team_id, &endpoint_id).await?;
    let e = event::Event::new(
        &claims.team_id,
        catalog::TEST_EVENT,
        json!({
            "message": "A test event from CoderHelm. Nothing happened.",
            "endpoint_id": st.endpoint.id,
        }),
    );
    let body = e.body();
    let cipher = kms(&state);
    let c = ctx(&state, &cipher);
    // Kept like any event, so its delivery can be sent again.
    if let Err(err) = event::keep(c.dynamo, c.table, &e, &body).await {
        tracing::warn!(error = %err, "Could not keep a test event");
    }
    let mut d = webhooks::deliver::new_record(&st.endpoint, &e.id, &e.event_type, body.as_bytes());
    webhooks::attempt(&c, &claims.team_id, &st, &e.id, &body, &mut d, Mode::Manual).await;
    Ok(Json(delivery_view(&d)))
}

/// GET /api/webhooks/endpoints/:endpoint_id/deliveries
#[utoipa::path(
    get,
    path = "/api/webhooks/endpoints/{endpoint_id}/deliveries",
    tag = "Webhooks",
    operation_id = "listWebhookDeliveries",
    summary = "List an endpoint's deliveries",
    description = "The endpoint's deliveries, newest first, each with every attempt. Kept 14 days.",
    params(
        ("endpoint_id" = String, Path, description = "The endpoint's id.", example = "ep_01jbendpt4a7b0c3d6e9f2g5h8"),
        DeliveryQuery,
    ),
    responses(
        (status = 200, description = "The deliveries.", body = inline(Object),
            example = json!({ "deliveries": [{ "id": "01JBDLVR5T8V1W4X7Y0Z3A6B9C", "event_id": "evt_01JBEVNT8M1N4P7Q0R3S6T9V2W", "event": "run.completed", "status": "handled", "outcome": "Delivered (200)", "reason": "", "reason_message": "", "reason_fix": "", "attempts": [{ "at": "2026-10-26T14:31:47.901Z", "code": 200, "duration_ms": 87, "error": "" }], "next_retry_at": null, "created_at": "2026-10-26T14:31:47.780Z" }] })),
        (status = 404, description = "No such endpoint on this team.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["webhooks"])),
)]
pub async fn list_endpoint_deliveries(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(endpoint_id): Path<String>,
    Query(q): Query<DeliveryQuery>,
) -> Reply {
    claims.require_role(3).map_err(admins_only)?;
    load(&state, &claims.team_id, &endpoint_id).await?;
    let f = Filter {
        source: Some(webhooks::deliver::SOURCE.into()),
        status: q.status,
        endpoint: Some(endpoint_id),
        ..Default::default()
    };
    let limit = q.limit.unwrap_or(25).clamp(1, 100);
    let items = deliveries::list(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
        &f,
        limit,
    )
    .await
    .map_err(|e| internal("Failed to list webhook deliveries", e))?;
    Ok(Json(json!({
        "deliveries": items.iter().map(delivery_view).collect::<Vec<_>>(),
    })))
}

/// Sends a kept delivery's event once more, now. Shared with the Deliveries
/// page's Redeliver.
pub async fn resend_record(
    state: &AppState,
    team_id: &str,
    mut d: Delivery,
) -> Result<Delivery, ApiError> {
    if d.source != webhooks::deliver::SOURCE || d.endpoint_id.is_empty() {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "This delivery isn't a webhook.",
        ));
    }
    let st = load(state, team_id, &d.endpoint_id).await?;
    let kept = event::get(
        &state.dynamo,
        &state.config.settings_table_name,
        team_id,
        &d.external_id,
    )
    .await
    .map_err(|e| internal("Failed to read a kept event", e))?
    .ok_or_else(|| {
        fail(
            StatusCode::GONE,
            "The event is no longer kept (events are kept 14 days).",
        )
    })?;
    let cipher = kms(state);
    let c = ctx(state, &cipher);
    webhooks::attempt(
        &c,
        team_id,
        &st,
        &kept.event.id,
        &kept.body,
        &mut d,
        Mode::Manual,
    )
    .await;
    Ok(d)
}

/// POST /api/webhooks/endpoints/:endpoint_id/deliveries/:delivery_id/resend
#[utoipa::path(
    post,
    path = "/api/webhooks/endpoints/{endpoint_id}/deliveries/{delivery_id}/resend",
    tag = "Webhooks",
    operation_id = "resendWebhookDelivery",
    summary = "Resend a delivery",
    description = "Sends the same event (the same body and `webhook-id`, signed now) to the endpoint \
        once more and adds the attempt to the delivery. Works on any delivery whose event is still \
        kept (14 days), delivered or not.",
    params(
        ("endpoint_id" = String, Path, description = "The endpoint's id.", example = "ep_01jbendpt4a7b0c3d6e9f2g5h8"),
        ("delivery_id" = String, Path, description = "The delivery's id.", example = "01JBDLVR5T8V1W4X7Y0Z3A6B9C"),
    ),
    responses(
        (status = 200, description = "The delivery with the new attempt.", body = WebhookDelivery),
        (status = 404, description = "No such endpoint or delivery.", body = crate::openapi::ErrorBody),
        (status = 410, description = "The event is no longer kept.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["webhooks"])),
)]
pub async fn resend_delivery(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path((endpoint_id, delivery_id)): Path<(String, String)>,
) -> Reply {
    claims.require_role(3).map_err(admins_only)?;
    let d = deliveries::get(
        &state.dynamo,
        &state.config.settings_table_name,
        &claims.team_id,
        &delivery_id,
    )
    .await
    .map_err(|e| internal("Failed to read a delivery", e))?
    .filter(|d| d.endpoint_id == endpoint_id)
    .ok_or_else(|| fail(StatusCode::NOT_FOUND, "No such delivery for this endpoint."))?;
    let d = resend_record(&state, &claims.team_id, d).await?;
    Ok(Json(delivery_view(&d)))
}

/// The most events one replay sends.
pub const MAX_REPLAY: usize = 500;

/// Events whose last delivery to the endpoint went fine, from its records.
fn delivered_ids(records: &[Delivery]) -> std::collections::HashSet<String> {
    records
        .iter()
        .filter(|d| d.status == Status::Handled || d.status == Status::Pending)
        .map(|d| d.external_id.clone())
        .collect()
}

/// POST /api/webhooks/endpoints/:endpoint_id/replay
#[utoipa::path(
    post,
    path = "/api/webhooks/endpoints/{endpoint_id}/replay",
    tag = "Webhooks",
    operation_id = "replayWebhookEvents",
    summary = "Replay events",
    description = "Sends the kept events made in a time range to this endpoint again, each as a new \
        delivery with retries, oldest first. Only event types the endpoint subscribes to now are \
        sent. Events carry their original `id`, so a receiver that already handled one can skip it. \
        At most 500 per call; events are kept 14 days.",
    params(("endpoint_id" = String, Path, description = "The endpoint's id.", example = "ep_01jbendpt4a7b0c3d6e9f2g5h8")),
    request_body = ReplayBody,
    responses(
        (status = 202, description = "Queued.", body = ReplayResult),
        (status = 400, description = "The range is not valid, or the endpoint is off.", body = crate::openapi::ErrorBody),
        (status = 404, description = "No such endpoint on this team.", body = crate::openapi::ErrorBody),
    ),
    security(("api_key" = ["webhooks"])),
)]
pub async fn replay(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(endpoint_id): Path<String>,
    Json(body): Json<ReplayBody>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    claims.require_role(3).map_err(admins_only)?;
    let st = load(&state, &claims.team_id, &endpoint_id).await?;
    if !st.endpoint.enabled {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Turn the endpoint on before replaying events to it.",
        ));
    }
    let now = chrono::Utc::now();
    let (since, until) = replay_range(&body, now).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let table = &state.config.settings_table_name;
    let kept = event::list_range(
        &state.dynamo,
        table,
        &claims.team_id,
        since,
        until,
        MAX_REPLAY * 4,
    )
    .await
    .map_err(|e| internal("Failed to list kept events", e))?;
    let found = kept.len();
    let mut picked = webhooks::replayable(&st.endpoint, &kept);
    if body.only_failed {
        let records = deliveries::list(
            &state.dynamo,
            table,
            &claims.team_id,
            &Filter {
                source: Some(webhooks::deliver::SOURCE.into()),
                endpoint: Some(endpoint_id.clone()),
                since: Some(now_secs(since)),
                ..Default::default()
            },
            1000,
        )
        .await
        .map_err(|e| internal("Failed to list webhook deliveries", e))?;
        let ok = delivered_ids(&records);
        picked.retain(|k| !ok.contains(&k.event.id));
    }
    let capped = picked.len() > MAX_REPLAY;
    let to_send: Vec<event::Kept> = picked.into_iter().take(MAX_REPLAY).cloned().collect();
    let cipher = kms(&state);
    let c = ctx(&state, &cipher);
    let queued = webhooks::replay(&c, &claims.team_id, &st.endpoint, &to_send)
        .await
        .map_err(|e| internal("Failed to queue a replay", e))?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "queued": queued, "found": found, "capped": capped })),
    ))
}

/// The range a replay covers, checked: `since` within the kept days and
/// before `until`.
pub fn replay_range(
    body: &ReplayBody,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>), String> {
    let parse = |s: &str| {
        chrono::DateTime::parse_from_rfc3339(s.trim())
            .map(|t| t.with_timezone(&chrono::Utc))
            .map_err(|_| format!("\"{s}\" isn't an RFC 3339 time."))
    };
    let since = parse(&body.since)?;
    let until = match body.until.as_deref() {
        Some(u) if !u.trim().is_empty() => parse(u)?,
        _ => now,
    };
    if since >= until {
        return Err("`since` must be before `until`.".into());
    }
    if since < now - chrono::Duration::days(deliveries::RETENTION_DAYS) {
        return Err(format!(
            "Events are kept {} days; start the range after that.",
            deliveries::RETENTION_DAYS
        ));
    }
    Ok((since, until.min(now)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(since: &str, until: Option<&str>) -> ReplayBody {
        ReplayBody {
            since: since.into(),
            until: until.map(Into::into),
            only_failed: false,
        }
    }

    #[test]
    fn plan_events_carry_the_documented_fields() {
        let d = plan_approved_data(
            "01JBPLAN",
            "Move exports",
            4,
            "lead@example.com",
            "https://app.example.com/",
        );
        let schema = catalog::data_schema(catalog::find("plan.approved").unwrap());
        let props = schema["properties"].as_object().unwrap();
        assert_eq!(
            props.keys().collect::<std::collections::BTreeSet<_>>(),
            d.as_object()
                .unwrap()
                .keys()
                .collect::<std::collections::BTreeSet<_>>()
        );
        assert_eq!(
            d["url"],
            "https://app.example.com/plans/detail/?id=01JBPLAN"
        );
        let items = vec![std::collections::HashMap::from([
            (
                "sk".to_string(),
                aws_sdk_dynamodb::types::AttributeValue::S("PLAN#p1".into()),
            ),
            (
                "title".to_string(),
                aws_sdk_dynamodb::types::AttributeValue::S("Move exports".into()),
            ),
        ])];
        assert_eq!(plan_title(&items, "p1"), "Move exports");
        assert_eq!(plan_title(&items, "p2"), "");
    }

    #[test]
    fn a_replay_range_stays_inside_the_kept_days() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-10-26T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let (s, u) = replay_range(&body("2026-10-25T00:00:00Z", None), now).unwrap();
        assert_eq!(
            (s.to_rfc3339(), u),
            ("2026-10-25T00:00:00+00:00".into(), now)
        );
        assert!(replay_range(&body("2026-10-01T00:00:00Z", None), now).is_err());
        assert!(replay_range(
            &body("2026-10-26T11:00:00Z", Some("2026-10-26T10:00:00Z")),
            now
        )
        .is_err());
        assert!(replay_range(&body("yesterday", None), now).is_err());
        let (_, u) = replay_range(
            &body("2026-10-25T00:00:00Z", Some("2027-01-01T00:00:00Z")),
            now,
        )
        .unwrap();
        assert_eq!(u, now, "never past now");
    }

    #[test]
    fn only_failed_skips_events_already_delivered_or_on_their_way() {
        let rec = |ev: &str, st: Status| {
            let mut d = Delivery::new(
                deliveries::Direction::Outbound,
                "webhook",
                "run.failed",
                "h",
            );
            d.external_id = ev.into();
            d.status = st;
            d
        };
        let ok = delivered_ids(&[
            rec("evt_a", Status::Handled),
            rec("evt_b", Status::Failed),
            rec("evt_c", Status::Pending),
        ]);
        assert!(ok.contains("evt_a") && ok.contains("evt_c") && !ok.contains("evt_b"));
    }
}
