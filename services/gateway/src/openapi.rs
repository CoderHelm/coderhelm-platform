//! The public API: which routes outside programs may call, and the OpenAPI
//! document that describes them.
//!
//! [`PUBLIC_ROUTES`] is the list. Nothing is public because of its path:
//! a route is public when it is written here. API keys reach only the routes
//! here marked [`RouteAuth::ApiKey`] (the auth middleware checks), and the
//! OpenAPI document is generated from the `#[utoipa::path]` annotations on
//! exactly these handlers and from the types they return.
//!
//! The document is committed at `docs/api/openapi.json` (the docs site
//! publishes it at https://docs.coderhelm.com/openapi.json). The tests below
//! fail when a public route has no entry in it, when it documents a route
//! that isn't public, or when the committed file differs from what the code
//! generates. To write it again after changing a public handler:
//!
//! ```sh
//! UPDATE_OPENAPI=1 cargo test -p gateway openapi
//! ```

use serde::Serialize;
use utoipa::openapi::header::HeaderBuilder;
use utoipa::openapi::security::{ApiKey, ApiKeyValue, HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::openapi::{ObjectBuilder, RefOr, ResponseBuilder, Type};
use utoipa::{Modify, OpenApi, ToSchema};

/// How a public route is called.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteAuth {
    /// A team API key: `Authorization: Bearer chk_...`.
    ApiKey,
    /// A GitHub Actions OIDC token for a connected repository.
    GithubOidc,
    /// A webhook, verified by its signature or by the secret in its URL.
    Webhook,
}

/// One public route. `path` is the full path as served, with `:name`
/// parameters as in `main.rs`; `handler` is `file::function` under `routes`.
#[derive(Debug, Clone, Copy)]
pub struct PublicRoute {
    pub method: &'static str,
    pub path: &'static str,
    pub handler: &'static str,
    pub auth: RouteAuth,
}

impl PublicRoute {
    /// The handler, when an API key may call it.
    pub fn key_handler(&self) -> Option<&'static str> {
        (self.auth == RouteAuth::ApiKey).then_some(self.handler)
    }

    /// The path as OpenAPI writes it: `{name}` for `:name`.
    pub fn openapi_path(&self) -> String {
        self.path
            .split('/')
            .map(|seg| match seg.strip_prefix(':') {
                Some(name) => format!("{{{name}}}"),
                None => seg.to_string(),
            })
            .collect::<Vec<_>>()
            .join("/")
    }
}

const fn key(method: &'static str, path: &'static str, handler: &'static str) -> PublicRoute {
    PublicRoute {
        method,
        path,
        handler,
        auth: RouteAuth::ApiKey,
    }
}

/// Every public route. Everything else the gateway serves is for the
/// dashboard (or for CoderHelm's own operators) and can change without
/// notice; it is not in the OpenAPI document and API keys can't call it.
pub const PUBLIC_ROUTES: &[PublicRoute] = &[
    key("GET", "/api/me", "api::me"),
    key("GET", "/api/runs", "api::list_runs"),
    key("GET", "/api/runs/:run_id", "api::get_run"),
    key("POST", "/api/runs/:run_id/retry", "api::retry_run"),
    key("POST", "/api/runs/:run_id/cancel", "api::cancel_run"),
    key("GET", "/api/plans", "plans::list_plans"),
    key("POST", "/api/plans", "plans::create_plan"),
    key("GET", "/api/plans/:plan_id", "plans::get_plan"),
    key(
        "POST",
        "/api/plans/:plan_id/approve-and-execute",
        "plans::approve_all_and_execute",
    ),
    key("GET", "/api/repos", "api::list_repos"),
    key("GET", "/api/reviewer/reviews", "reviewer::list_reviews"),
    key("GET", "/api/reviewer/review", "reviewer::get_review"),
    key("GET", "/api/usage", "api::get_usage"),
    key("GET", "/api/webhooks/event-types", "webhooks::event_types"),
    key("GET", "/api/webhooks/endpoints", "webhooks::list_endpoints"),
    key(
        "POST",
        "/api/webhooks/endpoints",
        "webhooks::create_endpoint",
    ),
    key(
        "GET",
        "/api/webhooks/endpoints/:endpoint_id",
        "webhooks::get_endpoint",
    ),
    key(
        "PUT",
        "/api/webhooks/endpoints/:endpoint_id",
        "webhooks::update_endpoint",
    ),
    key(
        "DELETE",
        "/api/webhooks/endpoints/:endpoint_id",
        "webhooks::delete_endpoint",
    ),
    key(
        "GET",
        "/api/webhooks/endpoints/:endpoint_id/secret",
        "webhooks::reveal_secret",
    ),
    key(
        "POST",
        "/api/webhooks/endpoints/:endpoint_id/secret/rotate",
        "webhooks::rotate_secret",
    ),
    key(
        "POST",
        "/api/webhooks/endpoints/:endpoint_id/test",
        "webhooks::send_test",
    ),
    key(
        "GET",
        "/api/webhooks/endpoints/:endpoint_id/deliveries",
        "webhooks::list_endpoint_deliveries",
    ),
    key(
        "POST",
        "/api/webhooks/endpoints/:endpoint_id/deliveries/:delivery_id/resend",
        "webhooks::resend_delivery",
    ),
    key(
        "POST",
        "/api/webhooks/endpoints/:endpoint_id/replay",
        "webhooks::replay",
    ),
    PublicRoute {
        method: "POST",
        path: "/webhooks/ci/tests/results",
        handler: "tests::ci_upload",
        auth: RouteAuth::GithubOidc,
    },
    PublicRoute {
        method: "GET",
        path: "/webhooks/ci/tests/quarantine",
        handler: "tests::ci_quarantine",
        auth: RouteAuth::GithubOidc,
    },
    PublicRoute {
        method: "POST",
        path: "/webhooks/github",
        handler: "github_webhook::handle",
        auth: RouteAuth::Webhook,
    },
    PublicRoute {
        method: "POST",
        path: "/webhooks/jira/:token",
        handler: "jira_webhook::handle",
        auth: RouteAuth::Webhook,
    },
    PublicRoute {
        method: "POST",
        path: "/webhooks/alerts/sns",
        handler: "alerts::handle_sns",
        auth: RouteAuth::Webhook,
    },
    PublicRoute {
        method: "POST",
        path: "/webhooks/knowledge/:token",
        handler: "knowledge_sources::webhook",
        auth: RouteAuth::Webhook,
    },
    PublicRoute {
        method: "POST",
        path: "/webhooks/relay",
        handler: "hybrid_install::intake",
        auth: RouteAuth::Webhook,
    },
];

/// May an API key call this routed handler with this method?
pub fn is_key_route(method: &str, handler: Option<&str>) -> bool {
    let Some(handler) = handler else {
        return false;
    };
    PUBLIC_ROUTES
        .iter()
        .any(|r| r.auth == RouteAuth::ApiKey && r.handler == handler && r.method == method)
}

// ── Shared types ────────────────────────────────────────────────────

/// The body of every error an API key request gets.
#[derive(Debug, Serialize, ToSchema)]
#[schema(example = json!({
    "error": "This API key doesn't have the \"runs\" scope (Retry, re-run, re-review, cancel, close or reopen runs).",
    "code": "missing_scope",
    "scope": "runs"
}))]
pub struct ErrorBody {
    /// A sentence to show a person.
    #[schema(
        example = "This API key doesn't have the \"runs\" scope (Retry, re-run, re-review, cancel, close or reopen runs)."
    )]
    pub error: String,
    /// What went wrong, for programs. One of: `bad_request`,
    /// `invalid_api_key`, `api_key_revoked`, `api_key_expired`,
    /// `api_key_owner_left`, `unauthorized`, `forbidden`, `missing_scope`,
    /// `not_available_to_api_keys`, `not_found`, `conflict`,
    /// `payload_too_large`, `rate_limited`, `token_limit_reached`,
    /// `internal_error`, `unavailable`. New codes may be added.
    #[schema(example = "missing_scope")]
    pub code: String,
    /// With `missing_scope`: the scope the key would need.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "runs")]
    pub scope: Option<String>,
}

/// The `code` an error status gets when its handler gave none.
pub fn default_code(status: axum::http::StatusCode) -> &'static str {
    match status.as_u16() {
        400 | 422 => "bad_request",
        401 => "unauthorized",
        403 => "forbidden",
        404 => "not_found",
        409 => "conflict",
        413 => "payload_too_large",
        // The only 429 a public handler answers is the team's monthly token
        // limit; the per-key rate limit answers before the handler runs.
        429 => "token_limit_reached",
        502..=504 => "unavailable",
        _ => "internal_error",
    }
}

// ── The document ────────────────────────────────────────────────────

const DESCRIPTION: &str =
    "The CoderHelm API lets your own programs start and follow CoderHelm's work: \
list and inspect runs, retry or cancel them, create and run plans, read the reviewer's reviews, \
check token usage, and manage the webhook endpoints CoderHelm sends your team's events to.\n\n\
Every request is authenticated with a team API key sent as a bearer token. Admins create keys in \
the dashboard under Settings, API keys. A key holds scopes, never more than the admin who made it, \
and has its own rate limit.\n\n\
CI jobs upload test reports with a GitHub Actions OIDC token instead of a key, and the webhooks \
are verified by their signatures.";

#[derive(OpenApi)]
#[openapi(
    info(
        title = "CoderHelm API",
        version = "1.0.0",
        description = DESCRIPTION,
        contact(name = "CoderHelm support", url = "https://docs.coderhelm.com/api"),
        license(name = "MIT", identifier = "MIT"),
    ),
    servers((url = "https://api.coderhelm.com", description = "Production")),
    paths(
        crate::routes::api::me,
        crate::routes::api::list_runs,
        crate::routes::api::get_run,
        crate::routes::api::retry_run,
        crate::routes::api::cancel_run,
        crate::routes::plans::list_plans,
        crate::routes::plans::create_plan,
        crate::routes::plans::get_plan,
        crate::routes::plans::approve_all_and_execute,
        crate::routes::api::list_repos,
        crate::routes::reviewer::list_reviews,
        crate::routes::reviewer::get_review,
        crate::routes::api::get_usage,
        crate::routes::webhooks::event_types,
        crate::routes::webhooks::list_endpoints,
        crate::routes::webhooks::create_endpoint,
        crate::routes::webhooks::get_endpoint,
        crate::routes::webhooks::update_endpoint,
        crate::routes::webhooks::delete_endpoint,
        crate::routes::webhooks::reveal_secret,
        crate::routes::webhooks::rotate_secret,
        crate::routes::webhooks::send_test,
        crate::routes::webhooks::list_endpoint_deliveries,
        crate::routes::webhooks::resend_delivery,
        crate::routes::webhooks::replay,
        crate::routes::tests::ci_upload,
        crate::routes::tests::ci_quarantine,
        crate::routes::github_webhook::handle,
        crate::routes::jira_webhook::handle,
        crate::routes::alerts::handle_sns,
        crate::routes::knowledge_sources::webhook,
        crate::routes::hybrid_install::intake,
    ),
    components(schemas(ErrorBody)),
    modifiers(&Common),
    tags(
        (name = "Account", description = "Who a key belongs to and what it may do."),
        (name = "Runs", description = "A run is one piece of work by CoderHelm's coder: a ticket or task turned into a pull request. Runs are listed newest first."),
        (name = "Plans", description = "A plan is a set of tasks, each of which becomes a run once the plan is executed."),
        (name = "Repositories", description = "The repositories connected to the team through the CoderHelm GitHub App."),
        (name = "Reviews", description = "Pull request reviews written by CoderHelm's reviewer."),
        (name = "Usage", description = "Tokens used this month and the team's limits."),
        (name = "CI", description = "Endpoints a CI job calls with its GitHub Actions OIDC token, so no CoderHelm secret is stored in CI."),
        (name = "Webhook endpoints", description = "Where CoderHelm sends your team's events (runs, pull requests, reviews, CI fixes, alerts, log findings, releases, plans). Each request is signed the Standard Webhooks way. The `webhooks` section of this document has every event's schema."),
        (name = "Webhooks", description = "Webhooks CoderHelm receives from GitHub, Jira, AWS SNS, Sanity and Notion. CoderHelm verifies each one before anything happens."),
    ),
)]
pub struct ApiDoc;

/// The parts every public operation shares: the security schemes, the error
/// responses and the rate-limit headers.
struct Common;

fn int_header(description: &str) -> utoipa::openapi::header::Header {
    HeaderBuilder::new()
        .schema(ObjectBuilder::new().schema_type(Type::Integer))
        .description(Some(description))
        .build()
}

fn error_response(description: &str) -> RefOr<utoipa::openapi::Response> {
    RefOr::T(
        ResponseBuilder::new()
            .description(description)
            .content(
                "application/json",
                utoipa::openapi::ContentBuilder::new()
                    .schema(Some(utoipa::openapi::Ref::from_schema_name("ErrorBody")))
                    .build(),
            )
            .build(),
    )
}

impl Modify for Common {
    fn modify(&self, doc: &mut utoipa::openapi::OpenApi) {
        let components = doc.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "api_key",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("chk_<id>_<secret>")
                    .description(Some(
                        "A team API key, made by an admin under Settings, API keys. Send it as \
                         `Authorization: Bearer chk_...`.",
                    ))
                    .build(),
            ),
        );
        components.add_security_scheme(
            "github_oidc",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .description(Some(
                        "A GitHub Actions OIDC token requested with the audience `coderhelm`, \
                         from a workflow run of a repository connected to your team.",
                    ))
                    .build(),
            ),
        );
        components.add_security_scheme(
            "github_signature",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                "X-Hub-Signature-256",
                "`sha256=` and the hex HMAC-SHA256 of the raw body, keyed with the GitHub App's webhook secret.",
            ))),
        );
        components.add_security_scheme(
            "relay_signature",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                "X-CoderHelm-Relay-Signature",
                "`t=<unix seconds>,d=<delivery id>,v1=<hex>`: the HMAC-SHA256 of `<t>.<d>.<raw body>`, keyed with the install's relay key.",
            ))),
        );
        components.add_security_scheme(
            "jira_signature",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                "X-Hub-Signature",
                "Only when the team generated a webhook secret: `sha256=` and the hex HMAC-SHA256 of the raw body, keyed with that secret.",
            ))),
        );

        components.add_security_scheme(
            "sanity_signature",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                "sanity-webhook-signature",
                "`t=<ms>,v1=` and the base64url HMAC-SHA256 of `<t>.<body>`, keyed with the token in the address.",
            ))),
        );
        components.add_security_scheme(
            "notion_signature",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                "X-Notion-Signature",
                "`sha256=` and the hex HMAC-SHA256 of the raw body, keyed with the subscription's verification token.",
            ))),
        );

        let rate_headers = [
            ("RateLimit-Limit", "Requests this key may make in a minute."),
            (
                "RateLimit-Remaining",
                "Requests left in the current minute.",
            ),
            (
                "RateLimit-Reset",
                "Seconds until the current minute ends and the count starts over.",
            ),
        ];
        let key_routes: Vec<(String, String)> = PUBLIC_ROUTES
            .iter()
            .filter(|r| r.auth == RouteAuth::ApiKey)
            .map(|r| (r.method.to_ascii_lowercase(), r.openapi_path()))
            .collect();
        for (path, item) in doc.paths.paths.iter_mut() {
            for (method, op) in [
                ("get", item.get.as_mut()),
                ("post", item.post.as_mut()),
                ("put", item.put.as_mut()),
                ("delete", item.delete.as_mut()),
            ] {
                let Some(op) = op else { continue };
                if !key_routes.iter().any(|(m, p)| m == method && p == path) {
                    continue;
                }
                let responses = &mut op.responses.responses;
                for (status, resp) in responses.iter_mut() {
                    if !status.starts_with('2') {
                        continue;
                    }
                    if let RefOr::T(resp) = resp {
                        for (name, description) in rate_headers {
                            resp.headers
                                .insert(name.to_string(), int_header(description));
                        }
                    }
                }
                responses.entry("401".into()).or_insert_with(|| {
                    error_response(
                        "The key is missing, malformed, revoked or expired, or the person who made it left the team.",
                    )
                });
                responses.entry("403".into()).or_insert_with(|| {
                    error_response("The key lacks the scope this endpoint needs (`missing_scope`).")
                });
                let mut limited = ResponseBuilder::new()
                    .description(
                        "Over the key's rate limit (`rate_limited`), or, for endpoints that start work, over the team's monthly token limit (`token_limit_reached`).",
                    )
                    .content(
                        "application/json",
                        utoipa::openapi::ContentBuilder::new()
                            .schema(Some(utoipa::openapi::Ref::from_schema_name("ErrorBody")))
                            .build(),
                    )
                    .build();
                limited.headers.insert(
                    "Retry-After".into(),
                    int_header("With `rate_limited`: seconds to wait before trying again."),
                );
                for (name, description) in rate_headers {
                    limited.headers.insert(name.into(), int_header(description));
                }
                responses.insert("429".into(), RefOr::T(limited));
                responses.entry("500".into()).or_insert_with(|| {
                    error_response("Something failed on CoderHelm's side. Safe to retry reads.")
                });
            }
        }
    }
}

/// GET /openapi.json: the document, public, so tools can read it from the
/// API itself as well as from the docs site.
pub async fn serve() -> impl axum::response::IntoResponse {
    use std::sync::OnceLock;
    static DOC: OnceLock<String> = OnceLock::new();
    (
        [
            (axum::http::header::CONTENT_TYPE, "application/json"),
            (axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        ],
        served_document(DOC.get_or_init(document_json).clone()),
    )
}

/// The document as this deployment serves it: a self-hosted install names
/// its own API as the server, not the hosted one.
fn served_document(doc: String) -> String {
    if !common::public_urls::self_hosted() {
        return doc;
    }
    let Ok(mut v) = serde_json::from_str::<serde_json::Value>(&doc) else {
        return doc;
    };
    v["servers"] = serde_json::json!([{
        "url": common::public_urls::api_url_from_env(),
        "description": "This install"
    }]);
    serde_json::to_string_pretty(&v)
        .map(|s| s + "\n")
        .unwrap_or(doc)
}

/// The document as committed: pretty JSON with a final newline.
pub fn document_json() -> String {
    let mut doc = serde_json::to_value(ApiDoc::openapi()).expect("the OpenAPI document serializes");
    if let Some(schemas) = doc
        .pointer_mut("/components/schemas")
        .and_then(|v| v.as_object_mut())
    {
        schemas.values_mut().for_each(json_schema_examples);
    }
    if let Some(paths) = doc.get_mut("paths") {
        inline_schemas(paths);
    }
    if let Some(obj) = doc.as_object_mut() {
        obj.insert("webhooks".into(), outbound_webhooks());
    }
    // Readers show a tag's `x-displayName` as written ("CI"), where they
    // would otherwise make a title out of its name ("C I").
    if let Some(tags) = doc.get_mut("tags").and_then(|v| v.as_array_mut()) {
        for tag in tags.iter_mut().filter_map(|t| t.as_object_mut()) {
            if let Some(name) = tag.get("name").cloned() {
                tag.insert("x-displayName".into(), name);
            }
        }
    }
    let mut s = serde_json::to_string_pretty(&doc).expect("the OpenAPI document serializes");
    s.push('\n');
    s
}

/// The events CoderHelm sends to a team's endpoints, as OpenAPI 3.1
/// `webhooks`: one operation per event type, made from the event catalog
/// (`common::webhooks::catalog`).
fn outbound_webhooks() -> serde_json::Value {
    use common::webhooks::catalog;
    use serde_json::json;
    let header = |name: &str, description: &str, example: &str| {
        json!({
            "name": name,
            "in": "header",
            "required": true,
            "description": description,
            "schema": { "type": "string" },
            "example": example,
        })
    };
    let params = json!([
        header("webhook-id", "The event id (`evt_…`). The same on every retry: use it to skip an event you already handled.", "evt_01JBEVNT8M1N4P7Q0R3S6T9V2W"),
        header("webhook-timestamp", "Unix seconds when this attempt was sent. Refuse a request more than 5 minutes off.", "1792420307"),
        header("webhook-signature", "Space-separated `v1,<base64>` signatures: HMAC-SHA256 of `<webhook-id>.<webhook-timestamp>.<raw body>` keyed with the endpoint's secret (the bytes after `whsec_`, base64-decoded). Two while a rotated secret still signs; accept the request when any matches.", "v1,K5oZfzN95Z9UVu1EsfQmfVNQhnkZ2pj9o9NDN/H/pI4="),
    ]);
    let mut out = serde_json::Map::new();
    for e in catalog::EVENTS {
        let example = catalog::example_event(e.name);
        out.insert(
            e.name.to_string(),
            json!({
                "post": {
                    "operationId": format!("event.{}", e.name),
                    "summary": e.summary,
                    "description": format!("{} Sent as `POST` to each endpoint subscribed to `{}` (or to every event).", e.description, e.name),
                    "tags": [format!("Events: {}", e.group)],
                    "parameters": params,
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": catalog::event_schema(e),
                                "examples": { "event": { "value": example } },
                            }
                        }
                    },
                    "responses": {
                        "2XX": { "description": "Received. Answer within 10 seconds; do the work afterwards." },
                        "410": { "description": "Gone: the delivery is final (not retried). Any 4xx other than 408 and 429 is final." },
                        "5XX": { "description": "Retried with backoff for about a day, as are timeouts, 408 and 429." },
                    },
                    "security": [],
                }
            }),
        );
    }
    serde_json::Value::Object(out)
}

/// OpenAPI 3.1 schemas are JSON Schema 2020-12, where an example is
/// `examples: [..]`; utoipa writes the older `example`. Tools that build
/// samples from the schema (the docs site among them) read `examples`.
fn json_schema_examples(schema: &mut serde_json::Value) {
    let Some(obj) = schema.as_object_mut() else {
        return;
    };
    if let Some(example) = obj.remove("example") {
        obj.entry("examples")
            .or_insert_with(|| serde_json::Value::Array(vec![example]));
    }
    for key in ["properties", "patternProperties", "$defs"] {
        if let Some(map) = obj.get_mut(key).and_then(|v| v.as_object_mut()) {
            map.values_mut().for_each(json_schema_examples);
        }
    }
    for key in ["allOf", "oneOf", "anyOf", "prefixItems"] {
        if let Some(list) = obj.get_mut(key).and_then(|v| v.as_array_mut()) {
            list.iter_mut().for_each(json_schema_examples);
        }
    }
    for key in ["items", "additionalProperties", "not", "contains"] {
        if let Some(inner) = obj.get_mut(key) {
            json_schema_examples(inner);
        }
    }
}

/// The schemas inside operations (parameters, bodies, responses), wherever a
/// `schema` key holds one.
fn inline_schemas(node: &mut serde_json::Value) {
    match node {
        serde_json::Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                if k == "schema" {
                    json_schema_examples(v);
                } else {
                    inline_schemas(v);
                }
            }
        }
        serde_json::Value::Array(list) => list.iter_mut().for_each(inline_schemas),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_self_hosted_install_serves_its_own_api_as_the_server() {
        use common::public_urls::{from_single, override_for_this_thread, Override};
        override_for_this_thread(Some(Override {
            urls: from_single("https://coderhelm.example.com"),
            site: None,
            self_hosted: true,
        }));
        let doc = super::served_document(super::document_json());
        override_for_this_thread(None);
        let v: serde_json::Value = serde_json::from_str(&doc).unwrap();
        assert_eq!(v["servers"][0]["url"], "https://coderhelm.example.com");
        assert!(!doc.contains("api.coderhelm.com"));
        // The hosted deployment serves the committed document unchanged.
        assert_eq!(
            super::served_document(super::document_json()),
            super::document_json()
        );
    }

    use super::*;
    use std::collections::BTreeSet;

    fn committed_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/api/openapi.json")
    }

    fn operations() -> BTreeSet<(String, String)> {
        let doc: serde_json::Value = serde_json::from_str(&document_json()).unwrap();
        let mut out = BTreeSet::new();
        for (path, item) in doc["paths"].as_object().unwrap() {
            for method in item.as_object().unwrap().keys() {
                out.insert((method.to_ascii_uppercase(), path.clone()));
            }
        }
        out
    }

    /// Outbound webhooks only go out: no unauthenticated or signature-only
    /// route takes anything on that path, so whoever controls an endpoint
    /// can't send events, answers or "deliveries" into CoderHelm.
    #[test]
    fn outbound_webhooks_have_no_inbound_path() {
        for r in PUBLIC_ROUTES
            .iter()
            .filter(|r| r.handler.starts_with("webhooks::"))
        {
            assert_eq!(r.auth, RouteAuth::ApiKey, "{} must need an API key", r.path);
            assert!(r.path.starts_with("/api/webhooks/"), "{}", r.path);
        }
        let main = include_str!("main.rs");
        let inbound = main
            .split(".nest(\"/webhooks\", webhook_routes)")
            .next()
            .unwrap();
        let webhook_router = inbound.rsplit("let webhook_routes").next().unwrap_or("");
        assert!(
            !webhook_router.contains("routes::webhooks::"),
            "an outbound webhook handler is on the unauthenticated /webhooks router"
        );
        // What an endpoint answers is only kept as a short note; nothing parses it.
        let deliver = include_str!("../../common/src/webhooks/deliver.rs");
        let code = deliver.split("#[cfg(test)]").next().unwrap();
        assert!(!code.contains(".json::<") && !code.contains("resp.json("));
    }

    #[test]
    fn openapi_documents_exactly_the_public_routes() {
        let public: BTreeSet<(String, String)> = PUBLIC_ROUTES
            .iter()
            .map(|r| (r.method.to_string(), r.openapi_path()))
            .collect();
        let documented = operations();
        for r in &public {
            assert!(
                documented.contains(r),
                "{} {} is public but has no #[utoipa::path] in the OpenAPI document. \
                 Annotate its handler and add it to ApiDoc's paths.",
                r.0,
                r.1
            );
        }
        for r in &documented {
            assert!(
                public.contains(r),
                "{} {} is in the OpenAPI document but not in openapi::PUBLIC_ROUTES.",
                r.0,
                r.1
            );
        }
    }

    #[test]
    fn openapi_every_public_route_is_served_by_its_handler() {
        let main = include_str!("main.rs");
        for r in PUBLIC_ROUTES {
            match r.path.strip_prefix("/api") {
                Some(inner) if r.auth == RouteAuth::ApiKey => assert_eq!(
                    crate::permissions::routed_handler(r.method, inner),
                    Some(r.handler),
                    "{} {} is not routed to {} in main.rs",
                    r.method,
                    r.path,
                    r.handler
                ),
                _ => {
                    let inner = r.path.strip_prefix("/webhooks").unwrap_or(r.path);
                    let call = format!("{}(routes::{})", r.method.to_ascii_lowercase(), r.handler);
                    let at = main
                        .find(&format!("\"{inner}\""))
                        .unwrap_or_else(|| panic!("{} is not routed in main.rs", r.path));
                    let line_end = main[at..].find(";\n").map_or(main.len(), |e| at + e);
                    assert!(
                        main[at..line_end].contains(&call),
                        "{} {} is not routed to {}",
                        r.method,
                        r.path,
                        r.handler
                    );
                }
            }
        }
        // Each handler's own capability is in the table (or it's open to
        // everyone on the team, like /api/me), so keys go through
        // resolve_access like any role.
        for r in PUBLIC_ROUTES.iter().filter(|r| r.auth == RouteAuth::ApiKey) {
            let cap = crate::permissions::handler_capability(r.handler);
            assert!(
                cap.is_some() || r.handler == "api::me",
                "{} has no capability in permissions::CAPABILITIES",
                r.handler
            );
        }
    }

    /// The scope each operation names is the capability its handler belongs
    /// to, the one `resolve_access` checks, so the docs say the right scope.
    #[test]
    fn openapi_scopes_are_the_handlers_capabilities() {
        let doc: serde_json::Value = serde_json::from_str(&document_json()).unwrap();
        for r in PUBLIC_ROUTES.iter().filter(|r| r.auth == RouteAuth::ApiKey) {
            let op = &doc["paths"][r.openapi_path()][r.method.to_ascii_lowercase()];
            let named: Vec<&str> = op["security"][0]["api_key"]
                .as_array()
                .unwrap_or_else(|| panic!("{} {} has no api_key security", r.method, r.path))
                .iter()
                .filter_map(|v| v.as_str())
                .collect();
            let want: Vec<&str> = crate::permissions::handler_capability(r.handler)
                .map(|c| vec![c.key])
                .unwrap_or_default();
            assert_eq!(named, want, "{} {} names the wrong scope", r.method, r.path);
            for scope in named {
                assert!(
                    crate::api_keys::key_scopes().iter().any(|c| c.key == scope),
                    "{scope} is not a key scope"
                );
            }
        }
    }

    #[test]
    fn openapi_operations_are_complete() {
        let doc: serde_json::Value = serde_json::from_str(&document_json()).unwrap();
        assert_eq!(doc["openapi"], "3.1.0");
        let mut ids = BTreeSet::new();
        for (path, item) in doc["paths"].as_object().unwrap() {
            for (method, op) in item.as_object().unwrap() {
                let at = format!("{method} {path}");
                let id = op["operationId"].as_str().unwrap_or("");
                assert!(!id.is_empty(), "{at}: no operationId");
                assert!(
                    ids.insert(id.to_string()),
                    "{at}: operationId {id} is used twice"
                );
                assert!(
                    op["summary"].as_str().is_some_and(|s| !s.is_empty()),
                    "{at}: no summary"
                );
                assert!(
                    op["description"].as_str().is_some_and(|s| s.len() > 20),
                    "{at}: no description"
                );
                assert_eq!(
                    op["tags"].as_array().map(Vec::len),
                    Some(1),
                    "{at}: one tag"
                );
                assert!(op["security"].is_array(), "{at}: no security");
                let responses = op["responses"].as_object().unwrap();
                assert!(
                    responses.keys().any(|k| k.starts_with('2')),
                    "{at}: no success response"
                );
                assert!(
                    responses.keys().any(|k| k.starts_with('4')),
                    "{at}: no error responses"
                );
            }
        }
        // Every schema field says what it is.
        for (name, schema) in doc["components"]["schemas"].as_object().unwrap() {
            if let Some(props) = schema["properties"].as_object() {
                for (field, prop) in props {
                    let described = prop["description"].as_str().is_some_and(|d| !d.is_empty())
                        || prop.get("$ref").is_some()
                        || prop["allOf"].is_array()
                        || prop["oneOf"].is_array();
                    assert!(described, "{name}.{field} has no description");
                }
            }
        }
        // Every field has an example, so sample responses and requests show
        // real values instead of "string" and 0. A field that only points at
        // another schema is covered by that schema's own fields.
        let is_ref = |p: &serde_json::Value| {
            let refs = |v: &serde_json::Value| v.get("$ref").is_some();
            refs(p)
                || ["allOf", "oneOf", "anyOf"]
                    .iter()
                    .any(|k| p[*k].as_array().is_some_and(|l| l.iter().any(refs)))
                || p.get("items").is_some_and(refs)
        };
        let mut missing = Vec::new();
        for (name, schema) in doc["components"]["schemas"].as_object().unwrap() {
            let parts =
                std::iter::once(schema).chain(schema["allOf"].as_array().into_iter().flatten());
            for part in parts {
                for (field, prop) in part["properties"].as_object().into_iter().flatten() {
                    if !is_ref(prop) && prop.get("examples").is_none() {
                        missing.push(format!("{name}.{field}"));
                    }
                }
            }
        }
        assert!(
            missing.is_empty(),
            "these public fields have no example (add #[schema(example = ...)]): {}",
            missing.join(", ")
        );
        // Schema examples are in the JSON Schema form.
        let run_id = &doc["components"]["schemas"]["RunSummary"]["properties"]["run_id"];
        assert!(run_id.get("example").is_none());
        assert_eq!(run_id["examples"][0], "01JB8X6F4M2C9QZ3T7V5W1K0HA");
        // A run in full always says which run it is (the dashboard shows and
        // copies it).
        let run = &doc["components"]["schemas"]["Run"];
        assert_eq!(
            run["properties"]["run_id"]["examples"][0],
            "01JB8X6F4M2C9QZ3T7V5W1K0HA"
        );
        assert!(
            run["required"]
                .as_array()
                .is_some_and(|r| r.iter().any(|f| f == "run_id")),
            "Run.run_id must be required"
        );
        // Key routes carry the rate-limit headers and the error responses.
        let runs = &doc["paths"]["/api/runs"]["get"]["responses"];
        assert!(runs["200"]["headers"]["RateLimit-Remaining"].is_object());
        assert!(runs["429"]["headers"]["Retry-After"].is_object());
        assert!(runs["401"].is_object() && runs["403"].is_object());
    }

    /// Fails when the committed document is not what the code generates.
    /// Run with UPDATE_OPENAPI=1 to write it.
    #[test]
    fn openapi_committed_document_matches_the_code() {
        let generated = document_json();
        let path = committed_path();
        if std::env::var("UPDATE_OPENAPI").is_ok_and(|v| v == "1") {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, &generated).unwrap();
            return;
        }
        let committed = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            committed == generated,
            "docs/api/openapi.json is out of date with the code. Run \
             `UPDATE_OPENAPI=1 cargo test -p gateway openapi` in services/ and commit the result."
        );
    }

    #[test]
    fn only_key_routes_are_open_to_keys() {
        assert!(is_key_route("GET", Some("api::list_runs")));
        assert!(is_key_route("POST", Some("plans::create_plan")));
        // Same handler, other method: not public.
        assert!(!is_key_route("DELETE", Some("api::list_runs")));
        // Dashboard routes, key management itself, and webhooks.
        for h in [
            "api::sync_repos",
            "users::invite_user",
            "api_keys::create",
            "api_keys::list",
            "tests::ci_upload",
        ] {
            assert!(
                !is_key_route("POST", Some(h)) && !is_key_route("GET", Some(h)),
                "{h}"
            );
        }
        assert!(!is_key_route("GET", None));
    }
}
