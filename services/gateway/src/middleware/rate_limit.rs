use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use tracing::warn;

use crate::AppState;

struct RateLimitEntry {
    count: u64,
    window_key: u64,
}

/// In-memory rate limit store. Resets on Lambda cold start, which is acceptable —
/// each instance independently enforces limits. No DynamoDB cost or latency.
static RATE_STORE: LazyLock<Mutex<HashMap<String, RateLimitEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Extract client IP from X-Forwarded-For header (set by API Gateway).
fn client_ip(req: &Request) -> String {
    req.headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Rate limit middleware for auth endpoints.
/// Allows 20 requests per IP per 5-minute window using in-memory counters.
pub async fn rate_limit_auth(
    State(_state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    check_rate_limit(&client_ip(&req), "auth", 20, 300)?;
    Ok(next.run(req).await)
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
