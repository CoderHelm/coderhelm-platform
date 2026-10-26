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

    if sibling_with_cookies(&req, origin.as_deref(), trusted.is_some(), hosts_names()) {
        return (
            StatusCode::FORBIDDEN,
            axum::Json(serde_json::json!({
                "code": "cross_origin_refused",
                "error": "This change came from another page on the same domain, not from CoderHelm. Open CoderHelm itself and try again.",
            })),
        )
            .into_response();
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

/// A change (any method but GET, HEAD and OPTIONS) sent with cookies from
/// another origin on the same site that isn't the dashboard's. SameSite
/// cookies don't stop these: a customer's `{name}.coderhelm.com` is the same
/// site as the dashboard and the API, so its pages could post a form or a
/// no-cors fetch that carries the session. `Sec-Fetch-Site` says so on
/// current browsers; on older ones, on the deployment that hands out the
/// names (`hosts_names`), an origin under the hybrid zone does. Same-origin
/// pages, API clients (no Origin) and cookieless calls (the install guide)
/// are never affected.
fn sibling_with_cookies(
    req: &Request,
    origin: Option<&str>,
    trusted: bool,
    hosts_names: bool,
) -> bool {
    if trusted
        || matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS)
        || !req.headers().contains_key(header::COOKIE)
    {
        return false;
    }
    match req
        .headers()
        .get("sec-fetch-site")
        .and_then(|v| v.to_str().ok())
    {
        Some(site) => site.eq_ignore_ascii_case("same-site"),
        None => hosts_names && origin.is_some_and(under_hybrid_zone),
    }
}

/// Whether this deployment writes `{name}.<zone>` records (the hosted side
/// of hybrid; an install on such a name serves its dashboard and API from
/// that one origin, so it never needs the fallback above).
fn hosts_names() -> bool {
    std::env::var("HYBRID_ZONE_ID").is_ok_and(|z| !z.trim().is_empty())
}

/// True for an origin whose host is the hybrid zone or under it.
fn under_hybrid_zone(origin: &str) -> bool {
    let host = origin
        .split("://")
        .nth(1)
        .unwrap_or("")
        .split(['/', ':'])
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    let zone = common::hybrid::names::zone();
    host == zone || host.ends_with(&format!(".{zone}"))
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
            .route("/api/teams", get(|| async { "[]" }).post(|| async { "{}" }))
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

    async fn post_with(origin: Option<&str>, fetch_site: Option<&str>, cookie: bool) -> StatusCode {
        let mut b = Request::builder()
            .method("POST")
            .uri("/api/teams")
            .header(header::CONTENT_TYPE, "text/plain");
        if let Some(o) = origin {
            b = b.header(header::ORIGIN, o);
        }
        if let Some(s) = fetch_site {
            b = b.header("sec-fetch-site", s);
        }
        if cookie {
            b = b.header(header::COOKIE, "__Host-coderhelm_session=abc");
        }
        app()
            .oneshot(b.body(Body::from("{}")).unwrap())
            .await
            .unwrap()
            .status()
    }

    /// A customer's `{name}.coderhelm.com` is the same site as the
    /// dashboard and the API, so a SameSite=Strict session cookie goes along
    /// with a form or a no-cors fetch from its pages. A change sent with
    /// cookies from any same-site origin but the dashboard's own is refused.
    #[tokio::test]
    async fn a_customer_subdomain_cant_send_changes_with_the_session() {
        const EVIL: &str = "https://example.coderhelm.com";
        assert_eq!(
            post_with(Some(EVIL), Some("same-site"), true).await,
            StatusCode::FORBIDDEN
        );
        // The dashboard, same-origin pages, API clients and the guide still work.
        assert_eq!(
            post_with(Some(APP), Some("same-site"), true).await,
            StatusCode::OK
        );
        assert_eq!(
            post_with(Some("https://api.coderhelm.com"), Some("same-origin"), true).await,
            StatusCode::OK
        );
        assert_eq!(post_with(None, None, true).await, StatusCode::OK);
        assert_eq!(
            post_with(Some(DOCS), Some("same-site"), false).await,
            StatusCode::OK
        );
        // Reads stay as they were (CORS already hides the answer).
        let r = app()
            .oneshot(
                Request::builder()
                    .uri("/api/teams")
                    .header(header::ORIGIN, EVIL)
                    .header("sec-fetch-site", "same-site")
                    .header(header::COOKIE, "__Host-coderhelm_session=abc")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(get_h(&r, header::ACCESS_CONTROL_ALLOW_ORIGIN), None);
    }

    /// Browsers without Sec-Fetch-Site: on the hosted side, an origin under
    /// the zone that isn't the dashboard's is refused; an install on its own
    /// name (same origin, no Sec-Fetch-Site) is never refused.
    #[test]
    fn without_sec_fetch_site_the_hosted_side_goes_by_the_origin() {
        let req = |o: &str| {
            Request::builder()
                .method("POST")
                .uri("/api/teams")
                .header(header::ORIGIN, o)
                .header(header::COOKIE, "a=b")
                .body(Body::empty())
                .unwrap()
        };
        let evil = "https://example.coderhelm.com";
        assert!(sibling_with_cookies(&req(evil), Some(evil), false, true));
        assert!(!sibling_with_cookies(&req(evil), Some(evil), true, true));
        assert!(!sibling_with_cookies(&req(evil), Some(evil), false, false));
        let other = "https://example.org";
        assert!(!sibling_with_cookies(&req(other), Some(other), false, true));
        assert!(under_hybrid_zone("https://a.coderhelm.com:443"));
        assert!(!under_hybrid_zone("https://coderhelm.com.example.org"));
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
