//! A request that runs out of time says so.
//!
//! The Lambda's own limit ends an invocation with nothing logged between its
//! START and END lines, so a request that hangs on a call can't be told
//! apart from any other. Each request here runs against its invocation's
//! own deadline (the Lambda context's, less [`ANSWER_TIME`] to write the
//! log line and the answer). When it is reached the request is dropped,
//! one line names the route (its pattern, never the path's values), where
//! the time went (every AWS call it made, from
//! [`common::call_clock`], and the ones still waiting), and the caller gets
//! a 503 it can retry, never a timeout from API Gateway.
//!
//! Outside Lambda (the streaming function's own server, the tests) there is
//! no invocation deadline, and requests run as they always did; the calls
//! are still timed.

use std::time::{Duration, Instant, SystemTime};

use axum::{
    extract::{MatchedPath, Request},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use common::call_clock::{self, Calls, Spent};
use serde_json::json;

/// Kept back from the invocation's deadline to log and answer.
pub const ANSWER_TIME: Duration = Duration::from_millis(1500);

/// What one timed-out request logs (no content: the route's pattern,
/// the method, durations and AWS operation names).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutOfTime {
    pub method: String,
    pub route: String,
    pub elapsed_ms: u64,
    pub budget_ms: u64,
    pub spent: Spent,
}

impl OutOfTime {
    fn log(&self) {
        tracing::error!(
            method = %self.method,
            route = %self.route,
            elapsed_ms = self.elapsed_ms,
            budget_ms = self.budget_ms,
            aws_calls = self.spent.calls,
            aws_calls_ms = self.spent.calls_ms,
            slowest_call = %self.spent.slowest,
            waiting_on = %self.spent.waiting.join(","),
            "Request ran out of time"
        );
    }
}

/// How long the request may run: until the invocation's deadline, less
/// [`ANSWER_TIME`]. `None` without one.
pub fn budget(deadline: Option<SystemTime>, now: SystemTime) -> Option<Duration> {
    let left = deadline?.duration_since(now).unwrap_or(Duration::ZERO);
    Some(left.saturating_sub(ANSWER_TIME))
}

/// The route's pattern (`/api/runs/:id`), or `unmatched`.
fn route_of(req: &Request) -> String {
    req.extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string())
}

/// Runs `fut` (the request) with its AWS calls timed, within `budget`.
/// `Err` when the budget ran out first, with what to log.
pub async fn run_within<F>(
    method: String,
    route: String,
    budget: Option<Duration>,
    fut: F,
) -> Result<Response, OutOfTime>
where
    F: std::future::Future<Output = Response>,
{
    let calls = Calls::default();
    let started = Instant::now();
    let work = call_clock::within(calls.clone(), fut);
    let Some(budget) = budget else {
        return Ok(work.await);
    };
    match tokio::time::timeout(budget, work).await {
        Ok(r) => Ok(r),
        Err(_) => Err(OutOfTime {
            method,
            route,
            elapsed_ms: started.elapsed().as_millis() as u64,
            budget_ms: budget.as_millis() as u64,
            spent: calls.spent(),
        }),
    }
}

/// The middleware (main.rs, `build_app`).
pub async fn guard(req: Request, next: Next) -> Response {
    let deadline = req
        .extensions()
        .get::<lambda_http::Context>()
        .map(|c| c.deadline());
    let method = req.method().to_string();
    let route = route_of(&req);
    let budget = budget(deadline, SystemTime::now());
    match run_within(method, route, budget, next.run(req)).await {
        Ok(r) => r,
        Err(t) => {
            t.log();
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "This took too long to answer. Try again in a moment."})),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_budget_is_the_invocations_own_less_the_time_to_answer() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        assert_eq!(budget(None, now), None);
        assert_eq!(
            budget(Some(now + Duration::from_secs(30)), now),
            Some(Duration::from_secs(30) - ANSWER_TIME)
        );
        // Already past it: no time at all, never a panic.
        assert_eq!(
            budget(Some(now - Duration::from_secs(1)), now),
            Some(Duration::ZERO)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_that_never_finishes_is_named_and_answered() {
        let r = run_within(
            "GET".into(),
            "/api/runs/:id".into(),
            Some(Duration::from_secs(28)),
            std::future::pending::<Response>(),
        )
        .await;
        let t = r.unwrap_err();
        assert_eq!(
            (t.method.as_str(), t.route.as_str()),
            ("GET", "/api/runs/:id")
        );
        assert!(t.elapsed_ms >= 28_000 && t.budget_ms == 28_000, "{t:?}");
    }

    /// A DynamoDB client, timed as main() builds them, pointed at a port
    /// that accepts and never answers.
    async fn silent_dynamo() -> (aws_sdk_dynamodb::Client, tokio::task::JoinHandle<()>) {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let hold = tokio::spawn(async move {
            let mut open = Vec::new();
            while let Ok((s, _)) = l.accept().await {
                open.push(s);
            }
        });
        let conf = aws_sdk_dynamodb::Config::builder()
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                "a", "b", None, None, "test",
            ))
            .endpoint_url(format!("http://127.0.0.1:{port}"))
            .interceptor(common::call_clock::CallClock)
            .build();
        (aws_sdk_dynamodb::Client::from_conf(conf), hold)
    }

    #[derive(Clone, Default)]
    struct Lines(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Lines {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// The incident: a request stuck on an AWS call that never answers.
    /// Through the router, with a Lambda deadline, it answers 503 before
    /// the deadline and logs the route's pattern and the call it waited on.
    #[tokio::test]
    async fn a_request_stuck_on_aws_logs_its_route_and_the_call_and_answers_503() {
        use axum::{body::Body, extract::Path, routing::get, Router};
        use tower::ServiceExt;
        let (dynamo, hold) = silent_dynamo().await;
        let handler = move |Path(_id): Path<String>| {
            let dynamo = dynamo.clone();
            async move {
                let _ = dynamo.get_item().table_name("settings").send().await;
                StatusCode::OK
            }
        };
        let app = Router::new()
            .nest("/api", Router::new().route("/runs/:id", get(handler)))
            .layer(axum::middleware::from_fn(guard));
        let lines = Lines::default();
        let w = lines.clone();
        let _log = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .json()
                .flatten_event(true)
                .with_writer(move || w.clone())
                .finish(),
        );
        let mut ctx = lambda_http::Context::default();
        let deadline = SystemTime::now() + ANSWER_TIME + Duration::from_millis(400);
        ctx.deadline = deadline
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let mut req = axum::http::Request::get("/api/runs/run-secret-42")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(ctx);
        let res = app.oneshot(req).await.unwrap();
        hold.abort();
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            SystemTime::now() < deadline + ANSWER_TIME,
            "answered before the deadline"
        );
        let out = String::from_utf8(lines.0.lock().unwrap().clone()).unwrap();
        let line = out
            .lines()
            .find(|l| l.contains("Request ran out of time"))
            .unwrap_or_else(|| panic!("no timeout line in {out}"));
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(v["route"], "/api/runs/:id", "{line}");
        assert_eq!(v["method"], "GET");
        assert!(
            v["waiting_on"]
                .as_str()
                .unwrap()
                .starts_with("DynamoDB.GetItem="),
            "{line}"
        );
        assert!(
            !line.contains("run-secret-42"),
            "the path's values stay out: {line}"
        );
    }

    #[test]
    fn the_gateway_runs_every_request_under_the_guard_with_timed_clients() {
        let main = include_str!("../main.rs");
        let app = &main[main.find("fn build_app(").unwrap()..];
        let app = &app[..app.find("\n}\n").unwrap()];
        let guard = app
            .find("middleware::deadline::guard")
            .expect("the guard is a layer");
        assert!(
            !app[guard..].contains(".layer("),
            "the guard is the outermost layer, so it covers the others"
        );
        let start = &main[main.find("async fn main()").unwrap()..];
        let start = &start[..start.find("let state = Arc::new").unwrap()];
        assert!(start.contains(".interceptor(common::call_clock::CallClock)"));
        assert!(
            !start.contains("::Client::new(&aws_config)"),
            "every AWS client of the gateway times its calls"
        );
    }

    #[tokio::test]
    async fn without_a_deadline_a_request_runs_as_before() {
        let r = run_within("GET".into(), "/x".into(), None, async {
            StatusCode::OK.into_response()
        })
        .await;
        assert_eq!(r.unwrap().status(), StatusCode::OK);
    }
}
