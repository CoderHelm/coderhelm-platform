use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
use axum::{
    async_trait,
    extract::{FromRequestParts, Request, State},
    http::{request::Parts, Extensions, HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
};
use std::collections::HashMap;
use std::convert::Infallible;
use std::net::IpAddr;
use std::sync::{Arc, LazyLock, Mutex};
use tracing::warn;

use crate::AppState;

struct RateLimitEntry {
    count: u64,
    window_key: u64,
}

/// In-memory rate limit store. Resets on Lambda cold start and counts per
/// instance: for limits that only need to slow a client down. Limits that
/// must hold across instances use [`allow_shared`].
static RATE_STORE: LazyLock<Mutex<HashMap<String, RateLimitEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The address a request came from, as the edge in front of the API saw it.
///
/// - Hosted (split layout): clients reach API Gateway directly, so its
///   `requestContext.http.sourceIp` is the client.
/// - Self-hosted (single origin): CloudFront is in front, so the source is
///   a CloudFront address. CloudFront appends the viewer's address to
///   `X-Forwarded-For`, and API Gateway appends the source after it; the
///   entry CloudFront appended is the client. Everything before it is what
///   the client sent and is never used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIp(pub String);

#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for ClientIp {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        Ok(client_ip(&parts.headers, &parts.extensions))
    }
}

fn source_ip(ext: &Extensions) -> Option<&str> {
    match ext.get::<lambda_http::request::RequestContext>()? {
        lambda_http::request::RequestContext::ApiGatewayV2(c) => c.http.source_ip.as_deref(),
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

fn client_ip(headers: &HeaderMap, ext: &Extensions) -> ClientIp {
    let xff = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok());
    ClientIp(pick_client_ip(
        source_ip(ext),
        xff,
        common::public_urls::self_hosted(),
    ))
}

/// [`ClientIp`]'s rule. Pure, for the tests.
pub fn pick_client_ip(source: Option<&str>, xff: Option<&str>, behind_cloudfront: bool) -> String {
    let source = source
        .map(str::trim)
        .filter(|s| s.parse::<IpAddr>().is_ok());
    let mut hops: Vec<&str> = xff
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .collect();
    // The hop API Gateway appended itself.
    if source.is_some() && hops.last().copied() == source {
        hops.pop();
    }
    let edge = hops.last().copied().filter(|h| h.parse::<IpAddr>().is_ok());
    let picked = if behind_cloudfront {
        edge.or(source)
    } else {
        source.or(edge)
    };
    picked.unwrap_or("unknown").to_string()
}

/// Rate limit middleware for the sign-in routes: 20 requests per address
/// per 5 minutes, counted in DynamoDB so the limit holds across instances.
pub async fn rate_limit_auth(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let ip = client_ip(req.headers(), req.extensions());
    allow_shared(&state, "auth", &ip.0, 20, 300).await?;
    Ok(next.run(req).await)
}

/// Rate limit for the public unsubscribe links: 60 requests per IP per
/// 5 minutes, counted apart from sign-in so mail apps posting one-click
/// unsubscribes never use up anyone's sign-in attempts.
pub async fn rate_limit_email(
    State(_state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let ip = client_ip(req.headers(), req.extensions());
    check_rate_limit(&ip.0, "email", 60, 300)?;
    Ok(next.run(req).await)
}

/// The install wizard's public progress view: 120 requests per IP per 5
/// minutes (a poll every few seconds), counted on its own.
pub fn allow_setup_poll(ip: &ClientIp) -> Result<(), StatusCode> {
    check_rate_limit(&ip.0, "setup", 120, 300)
}

/// A per-address limit for one public scope (hybrid name checks), counted
/// per instance.
pub fn allow(ip: &ClientIp, scope: &str, max: u64, window_secs: u64) -> Result<(), StatusCode> {
    check_rate_limit(&ip.0, scope, max, window_secs)
}

/// A limit that holds across instances: one counter per scope, key (an
/// address, or an install for signed calls) and window on the settings
/// table (atomic `ADD`, gone with its TTL). If the store can't be reached,
/// the instance's own count decides.
pub async fn allow_shared(
    state: &AppState,
    scope: &str,
    key: &str,
    max: u64,
    window_secs: u64,
) -> Result<(), StatusCode> {
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let window = now / window_secs;
    let r = state
        .dynamo
        .update_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", AttributeValue::S(shared_pk(scope, key)))
        .key("sk", AttributeValue::S(window.to_string()))
        .update_expression("ADD n :one SET #ttl = if_not_exists(#ttl, :ttl)")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":one", AttributeValue::N("1".into()))
        .expression_attribute_values(
            ":ttl",
            AttributeValue::N(((window + 2) * window_secs).to_string()),
        )
        .return_values(ReturnValue::UpdatedNew)
        .send()
        .await;
    match r {
        Ok(out) => {
            let count = out
                .attributes()
                .and_then(|a| a.get("n"))
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(1);
            if count > max {
                warn!(key, scope, count, "Rate limit exceeded");
                Err(StatusCode::TOO_MANY_REQUESTS)
            } else {
                Ok(())
            }
        }
        Err(e) => {
            warn!(
                error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e),
                scope,
                "rate limit: shared counter unavailable, counting on this instance"
            );
            check_rate_limit(key, scope, max, window_secs)
        }
    }
}

/// The settings-table partition of one shared counter.
pub fn shared_pk(scope: &str, ip: &str) -> String {
    format!("RATELIMIT#{scope}#{ip}")
}

/// In-memory sliding window rate limiter.
/// Zero latency, no external calls. Resets on cold start.
fn check_rate_limit(
    ip: &str,
    scope: &str,
    max_requests: u64,
    window_secs: u64,
) -> Result<(), StatusCode> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let window_key = now / window_secs;
    let key = format!("{scope}:{ip}");

    let mut store = RATE_STORE.lock().unwrap_or_else(|e| e.into_inner());

    // Evict expired entries periodically (every ~100 checks)
    if store.len() > 1000 {
        store.retain(|_, v| v.window_key >= window_key);
    }

    let entry = store.entry(key).or_insert(RateLimitEntry {
        count: 0,
        window_key,
    });

    // Reset counter if we've moved to a new window
    if entry.window_key != window_key {
        entry.count = 0;
        entry.window_key = window_key;
    }

    entry.count += 1;

    if entry.count > max_requests {
        warn!(ip, scope, count = entry.count, "Rate limit exceeded");
        Err(StatusCode::TOO_MANY_REQUESTS)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosted_keys_on_the_source_address_api_gateway_saw() {
        // Whatever the client put in X-Forwarded-For is ignored.
        assert_eq!(
            pick_client_ip(Some("203.0.113.9"), Some("1.2.3.4, 203.0.113.9"), false),
            "203.0.113.9"
        );
        assert_eq!(
            pick_client_ip(Some("203.0.113.9"), Some("1.2.3.4"), false),
            "203.0.113.9"
        );
        assert_eq!(
            pick_client_ip(Some("203.0.113.9"), None, false),
            "203.0.113.9"
        );
    }

    #[test]
    fn self_hosted_keys_on_the_address_cloudfront_appended() {
        // The client's own entries, CloudFront's viewer address, then the
        // CloudFront address API Gateway saw and appended.
        let xff = "1.2.3.4, 5.6.7.8, 198.51.100.7, 64.252.1.1";
        assert_eq!(
            pick_client_ip(Some("64.252.1.1"), Some(xff), true),
            "198.51.100.7"
        );
        // API Gateway didn't append it: CloudFront's entry is the last.
        assert_eq!(
            pick_client_ip(Some("64.252.1.1"), Some("1.2.3.4, 198.51.100.7"), true),
            "198.51.100.7"
        );
        // A client changing its own entries changes nothing.
        assert_eq!(
            pick_client_ip(
                Some("64.252.1.1"),
                Some("9.9.9.9, 198.51.100.7, 64.252.1.1"),
                true
            ),
            "198.51.100.7"
        );
    }

    /// The extractor reads the source address from the request context
    /// lambda_http puts on every request from API Gateway.
    #[tokio::test]
    async fn the_extractor_reads_api_gateways_request_context() {
        use lambda_http::aws_lambda_events::apigw::{
            ApiGatewayV2httpRequestContext, ApiGatewayV2httpRequestContextHttpDescription,
        };
        let ctx =
            lambda_http::request::RequestContext::ApiGatewayV2(ApiGatewayV2httpRequestContext {
                http: ApiGatewayV2httpRequestContextHttpDescription {
                    source_ip: Some("203.0.113.9".into()),
                    ..Default::default()
                },
                ..Default::default()
            });
        let req = axum::http::Request::builder()
            .header("x-forwarded-for", "1.2.3.4")
            .extension(ctx)
            .body(())
            .unwrap();
        let (mut parts, _) = req.into_parts();
        let ip = ClientIp::from_request_parts(&mut parts, &()).await.unwrap();
        assert_eq!(ip, ClientIp("203.0.113.9".into()));
    }

    #[test]
    fn what_is_not_an_address_is_never_a_key() {
        assert_eq!(pick_client_ip(None, None, true), "unknown");
        assert_eq!(pick_client_ip(None, Some("not-an-ip"), true), "unknown");
        assert_eq!(
            pick_client_ip(Some("nope"), Some("2001:db8::1"), false),
            "2001:db8::1"
        );
    }

    #[test]
    fn a_shared_counter_is_per_scope_and_address() {
        assert_ne!(
            shared_pk("auth", "1.2.3.4"),
            shared_pk("hybrid-register", "1.2.3.4")
        );
        assert_ne!(shared_pk("auth", "1.2.3.4"), shared_pk("auth", "1.2.3.5"));
        assert!(shared_pk("auth", "1.2.3.4").starts_with("RATELIMIT#"));
    }

    /// Without the store, the instance's own count still refuses.
    #[tokio::test]
    async fn a_shared_limit_still_holds_when_the_store_is_down() {
        let state = crate::tests::hosted_state();
        for _ in 0..3 {
            assert!(allow_shared(&state, "test-shared", "192.0.2.200", 3, 300)
                .await
                .is_ok());
        }
        assert_eq!(
            allow_shared(&state, "test-shared", "192.0.2.200", 3, 300).await,
            Err(StatusCode::TOO_MANY_REQUESTS)
        );
    }
}
