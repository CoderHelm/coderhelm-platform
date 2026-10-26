//! Cross-origin answers for browsers.
//!
//! The gateway owns CORS, not API Gateway: an HTTP API's CORS setting covers
//! the whole API with one origin list and one credentials flag, and while it
//! is on, API Gateway drops every CORS header the gateway sends. That made the
//! public name check unreadable from the docs install guide.
//!
//! Two kinds of answer:
//! - The dashboard's own origins (`CORS_ORIGINS`, comma separated; set only
//!   where the dashboard lives on another origin than the API) get their
//!   origin back with credentials, on every route, as before.
//! - A few public routes answer any origin with `*` and never with
//!   credentials: the name check the install guide calls as people type, the
//!   OpenAPI document, and a self-hosted install's setup routes for the guide.
//!
//! Every other origin gets no CORS headers, so a browser on it can't read the
//! answer. Preflights are answered here, without reaching a handler.

use axum::{
    extract::Request,
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::sync::OnceLock;

/// What the dashboard's origins may send (the same as the old API Gateway setting).
const CREDENTIALED_METHODS: &str = "GET, POST, PUT, DELETE, OPTIONS";
const CREDENTIALED_HEADERS: &str = "Content-Type, Authorization, Cookie";

/// The origins from a comma-separated list, trimmed, without empties.
pub fn parse_origins(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|o| o.trim().trim_end_matches('/').to_string())
        .filter(|o| !o.is_empty())
        .collect()
}

fn origins_from_env() -> &'static [String] {
    static ORIGINS: OnceLock<Vec<String>> = OnceLock::new();
    ORIGINS.get_or_init(|| parse_origins(&std::env::var("CORS_ORIGINS").unwrap_or_default()))
}

/// The methods any origin may use on a public path, or None if the path isn't public.
pub fn public_methods(path: &str) -> Option<&'static str> {
    if path == "/openapi.json" || path == "/api/setup/status" {
        return Some("GET");
    }
    if let Some(name) = path.strip_prefix("/hybrid/names/") {
        // GET /hybrid/names/:name only; /hybrid/names/records is for installs.
        if !name.is_empty() && !name.contains('/') && name != "records" {
            return Some("GET");
        }
        return None;
    }
    if path == "/api/setup/hybrid" || path.starts_with("/api/setup/hybrid/") {
        return Some("GET, POST");
    }
    None
}

/// The middleware, with the origins from `CORS_ORIGINS`.
pub async fn cors(req: Request, next: Next) -> Response {
    apply(origins_from_env(), req, next).await
}

/// The middleware, with the dashboard's origins given.
pub async fn apply(credentialed: &[String], req: Request, next: Next) -> Response {
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let path = req.uri().path().to_owned();
    let trusted = origin
        .as_deref()
        .filter(|o| credentialed.iter().any(|c| c == o))
        .map(str::to_owned);
    let public = public_methods(&path);

    let preflight = req.method() == Method::OPTIONS
        && origin.is_some()
        && req
            .headers()
            .contains_key(header::ACCESS_CONTROL_REQUEST_METHOD);
    if preflight {
        let mut res = StatusCode::NO_CONTENT.into_response();
        let h = res.headers_mut();
        if let Some(o) = &trusted {
            trusted_headers(h, o);
            h.insert(
                header::ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_static(CREDENTIALED_METHODS),
            );
            h.insert(
                header::ACCESS_CONTROL_ALLOW_HEADERS,
                HeaderValue::from_static(CREDENTIALED_HEADERS),
            );
        } else if let Some(methods) = public {
            h.insert(
                header::ACCESS_CONTROL_ALLOW_ORIGIN,
                HeaderValue::from_static("*"),
            );
            h.insert(
                header::ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_static(methods),
            );
            h.insert(
                header::ACCESS_CONTROL_ALLOW_HEADERS,
                HeaderValue::from_static("Content-Type"),
            );
        }
        if trusted.is_some() || public.is_some() {
            h.insert(
                header::ACCESS_CONTROL_MAX_AGE,
                HeaderValue::from_static("600"),
            );
        }
        return res;
    }

    let mut res = next.run(req).await;
    let h = res.headers_mut();
    if let Some(o) = &trusted {
        trusted_headers(h, o);
    } else {
        // Credentials only ever go with the dashboard's own origins.
        h.remove(header::ACCESS_CONTROL_ALLOW_CREDENTIALS);
        if public.is_some() {
            h.insert(
                header::ACCESS_CONTROL_ALLOW_ORIGIN,
                HeaderValue::from_static("*"),
            );
        }
    }
    res
}

fn trusted_headers(h: &mut HeaderMap, origin: &str) {
    if let Ok(v) = HeaderValue::from_str(origin) {
        h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, v);
        h.insert(
            header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
            HeaderValue::from_static("true"),
        );
        h.append(header::VARY, HeaderValue::from_static("Origin"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        routing::{get, post},
        Router,
    };
    use tower::ServiceExt;

    const DOCS: &str = "https://docs.coderhelm.com";
    const APP: &str = "https://app.coderhelm.com";

    /// The routes the install guide calls, answering the way their handlers do
    /// (the name check through the same reply wrapper as routes::hybrid).
    fn app() -> Router {
        let origins = vec![APP.to_string(), "https://coderhelm.com".to_string()];
        Router::new()
            .route(
                "/hybrid/names/:name",
                get(|| async {
                    crate::routes::hybrid::public_reply(Ok(axum::Json(serde_json::json!({
                        "name": "acme", "available": true, "code": "ok", "error": null
                    }))))
                }),
            )
            .route("/openapi.json", get(|| async { "{}" }))
            .route("/api/setup/status", get(|| async { "{}" }))
            .route("/api/setup/hybrid/name", post(|| async { "{}" }))
            .route("/api/teams", get(|| async { "[]" }))
            .layer(axum::middleware::from_fn(move |req, next| {
                let origins = origins.clone();
                async move { apply(&origins, req, next).await }
            }))
    }

    async fn send(method: &str, path: &str, origin: Option<&str>, preflight: bool) -> Response {
        let mut b = Request::builder().method(method).uri(path);
        if let Some(o) = origin {
            b = b.header(header::ORIGIN, o);
        }
        if preflight {
            b = b.header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET");
        }
        app().oneshot(b.body(Body::empty()).unwrap()).await.unwrap()
    }

    fn get_h(r: &Response, h: header::HeaderName) -> Option<&str> {
        r.headers().get(h).and_then(|v| v.to_str().ok())
    }

    #[tokio::test]
    async fn the_name_check_answers_the_docs_origin_without_credentials() {
        let r = send("GET", "/hybrid/names/acme", Some(DOCS), false).await;
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(get_h(&r, header::ACCESS_CONTROL_ALLOW_ORIGIN), Some("*"));
        assert_eq!(get_h(&r, header::ACCESS_CONTROL_ALLOW_CREDENTIALS), None);
        let body = axum::body::to_bytes(r.into_body(), 4096).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["available"], true);
    }

    #[tokio::test]
    async fn a_name_check_preflight_from_the_docs_origin_is_answered() {
        let r = send("OPTIONS", "/hybrid/names/acme", Some(DOCS), true).await;
        assert_eq!(r.status(), StatusCode::NO_CONTENT);
        assert_eq!(get_h(&r, header::ACCESS_CONTROL_ALLOW_ORIGIN), Some("*"));
        assert_eq!(get_h(&r, header::ACCESS_CONTROL_ALLOW_METHODS), Some("GET"));
        assert_eq!(get_h(&r, header::ACCESS_CONTROL_ALLOW_CREDENTIALS), None);
    }

    #[tokio::test]
    async fn the_dashboard_origin_keeps_credentials_everywhere() {
        for path in ["/api/teams", "/hybrid/names/acme"] {
            let r = send("GET", path, Some(APP), false).await;
            assert_eq!(
                get_h(&r, header::ACCESS_CONTROL_ALLOW_ORIGIN),
                Some(APP),
                "{path}"
            );
            assert_eq!(
                get_h(&r, header::ACCESS_CONTROL_ALLOW_CREDENTIALS),
                Some("true"),
                "{path}"
            );
            assert_eq!(get_h(&r, header::VARY), Some("Origin"), "{path}");
        }
        let r = send("OPTIONS", "/api/teams", Some(APP), true).await;
        assert_eq!(r.status(), StatusCode::NO_CONTENT);
        assert_eq!(get_h(&r, header::ACCESS_CONTROL_ALLOW_ORIGIN), Some(APP));
        assert_eq!(
            get_h(&r, header::ACCESS_CONTROL_ALLOW_CREDENTIALS),
            Some("true")
        );
        assert_eq!(
            get_h(&r, header::ACCESS_CONTROL_ALLOW_METHODS),
            Some(CREDENTIALED_METHODS)
        );
        assert_eq!(
            get_h(&r, header::ACCESS_CONTROL_ALLOW_HEADERS),
            Some(CREDENTIALED_HEADERS)
        );
    }

    #[tokio::test]
    async fn the_docs_origin_gets_nothing_on_private_routes() {
        let r = send("GET", "/api/teams", Some(DOCS), false).await;
        assert_eq!(get_h(&r, header::ACCESS_CONTROL_ALLOW_ORIGIN), None);
        assert_eq!(get_h(&r, header::ACCESS_CONTROL_ALLOW_CREDENTIALS), None);
        let r = send("OPTIONS", "/api/teams", Some(DOCS), true).await;
        assert_eq!(r.status(), StatusCode::NO_CONTENT);
        assert_eq!(get_h(&r, header::ACCESS_CONTROL_ALLOW_ORIGIN), None);
    }

    #[tokio::test]
    async fn an_installs_setup_routes_answer_the_docs_origin() {
        for (m, path) in [
            ("GET", "/api/setup/status"),
            ("POST", "/api/setup/hybrid/name"),
            ("GET", "/openapi.json"),
        ] {
            let r = send(m, path, Some(DOCS), false).await;
            assert_eq!(r.status(), StatusCode::OK, "{path}");
            assert_eq!(
                get_h(&r, header::ACCESS_CONTROL_ALLOW_ORIGIN),
                Some("*"),
                "{path}"
            );
            assert_eq!(
                get_h(&r, header::ACCESS_CONTROL_ALLOW_CREDENTIALS),
                None,
                "{path}"
            );
        }
    }

    #[test]
    fn public_paths_are_only_the_guides() {
        assert_eq!(public_methods("/hybrid/names/acme"), Some("GET"));
        assert_eq!(public_methods("/hybrid/names/records"), None);
        assert_eq!(public_methods("/hybrid/names/a/b"), None);
        assert_eq!(public_methods("/hybrid/installs"), None);
        assert_eq!(
            public_methods("/api/setup/hybrid/register"),
            Some("GET, POST")
        );
        assert_eq!(public_methods("/api/teams"), None);
        assert_eq!(
            parse_origins(" https://a.example/, ,https://b.example"),
            vec!["https://a.example", "https://b.example"]
        );
    }
}
