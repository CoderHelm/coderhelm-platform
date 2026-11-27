//! What the streaming function's own routes ask of DynamoDB, checked
//! against the list its role is granted from.
//!
//! The chat streams (`/api/brain/stream`, `/api/plans/chat/stream`) run in
//! their own Lambda (infra/lib/streaming-stack.ts), whose role grants table
//! by table. A check added to a shared path (the member-session check the
//! Require single sign-on change added) works in the main gateway, which can
//! read every table, and fails only in the streaming function, in production.
//!
//! So these tests drive both stream routes through the real router against
//! the AWS stand-in, once per kind of session (hosted and self-hosted;
//! password, Okta and Microsoft sessions; the rule on and off; a session
//! refused), record every DynamoDB call as IAM authorizes it, and require
//! each one to be listed in `infra/streaming-dynamodb.json`. The infra tests
//! (infra/test/streaming-access.test.ts) then require the synthesized
//! streaming role to allow every action that file lists. Code that starts
//! reading a new table from a stream route fails here until the file names
//! it, and the file can't name it without the role granting it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
    Router,
};
use serde_json::{json, Value};
use tower::ServiceExt;

use crate::auth::jwt::{self, SignInProof};
use crate::fake_aws::{self, Fake};
use crate::middleware::auth::SESSION_TTL_SECS;
use crate::AppState;

const TEAM: &str = "TEAM#stream";
const ISSUER: &str = "https://idp.example.com";
const BRAIN: &str = "/api/brain/stream";
const PLAN_CHAT: &str = "/api/plans/chat/stream";

/// The file the streaming role's DynamoDB grants are checked against.
fn granted_file() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../infra/streaming-dynamodb.json")
}

/// `{ resource: [actions] }` from the file (`users`, `users/index/gsi2`).
fn granted() -> BTreeMap<String, BTreeSet<String>> {
    let text = std::fs::read_to_string(granted_file()).expect("infra/streaming-dynamodb.json");
    let v: Value = serde_json::from_str(&text).expect("valid JSON");
    v["dynamodb"]
        .as_object()
        .expect("a \"dynamodb\" object")
        .iter()
        .map(|(k, a)| {
            let actions = a
                .as_array()
                .expect("a list of actions")
                .iter()
                .map(|x| x.as_str().unwrap().to_string())
                .collect();
            (k.clone(), actions)
        })
        .collect()
}

struct World {
    f: Fake,
    state: Arc<AppState>,
    app: Router,
}

async fn world() -> World {
    let f = fake_aws::start().await;
    let state = fake_aws::state_all_tables(&f.url);
    let app = crate::build_app(state.clone());
    World { f, state, app }
}

impl World {
    fn member(&self, extra: &[(&str, Value)]) {
        let mut item = vec![
            ("pk", json!(TEAM)),
            ("sk", json!("USER#1")),
            ("email", json!("ann@example.com")),
            ("role", json!("owner")),
            ("status", json!("active")),
            ("gsi2pk", json!("EMAIL#ann@example.com")),
            ("gsi2sk", json!(TEAM)),
        ];
        item.extend(extra.iter().cloned());
        self.f.store.put("users", &item);
    }

    /// A hosted team's rule: on with `provider`, or off.
    fn rule(&self, provider: Option<&str>) {
        let mut item = vec![
            ("pk", json!(TEAM)),
            ("sk", json!(super::sign_in::TEAM_RULE_SK)),
        ];
        if let Some(p) = provider {
            item.push(("require_sso", json!(p)));
            item.push(("require_sso_since", json!(1_000)));
        }
        self.f.store.put("settings", &item);
    }

    fn okta(&self) {
        self.f.store.put(
            "settings",
            &[
                ("pk", json!(TEAM)),
                ("sk", json!(super::oidc_signin::OKTA_SK)),
                ("issuer", json!(ISSUER)),
                ("client_id", json!("client")),
                ("client_secret_sealed", json!("sealed")),
                ("domains", json!(["example.com"])),
                ("enabled", json!(true)),
                ("tested_at", json!("2026-11-01T00:00:00Z")),
            ],
        );
    }

    /// A self-hosted install's org record.
    fn org(&self, rule_on: bool) {
        self.f.store.put(
            "settings",
            &[
                ("pk", json!("ORG")),
                ("sk", json!("META")),
                ("team_id", json!(TEAM)),
                ("domains", json!(["example.com"])),
                ("require_sso", json!(if rule_on { "okta" } else { "" })),
                ("require_sso_since", json!(1_000)),
                ("created_at", json!("2026-01-01T00:00:00Z")),
            ],
        );
    }

    /// A stream token for `route`, minted from a session signed in by `proof`.
    fn token(&self, route: &str, proof: &SignInProof) -> String {
        let session = jwt::create_session_token(
            "USER#1",
            TEAM,
            "ann@example.com",
            "owner",
            None,
            proof,
            &self.state.secrets.jwt_secret,
            SESSION_TTL_SECS,
        )
        .unwrap();
        let claims = jwt::validate_token(&session, &self.state.secrets.jwt_secret).unwrap();
        let audience = if route == BRAIN {
            jwt::BRAIN_AUDIENCE
        } else {
            jwt::PLAN_CHAT_AUDIENCE
        };
        jwt::create_scoped_token(
            &claims,
            proof,
            audience,
            &self.state.secrets.jwt_secret,
            300,
        )
        .unwrap()
    }

    /// Posts to a stream route and reads what it answers before the model
    /// would be called (a stream is read for a moment, then dropped).
    async fn post(&self, route: &str, proof: &SignInProof) -> (StatusCode, Value) {
        let body = if route == BRAIN {
            json!({ "question": "What does the billing service call?" })
        } else {
            json!({ "messages": [{ "role": "user", "content": "Plan the billing change." }] })
        };
        let req = Request::builder()
            .method("POST")
            .uri(route)
            .header(jwt::STREAM_TOKEN_HEADER, self.token(route, proof))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let res = self.app.clone().oneshot(req).await.unwrap();
        let status = res.status();
        let read = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            axum::body::to_bytes(res.into_body(), 1 << 20),
        )
        .await;
        let body = match read {
            Ok(Ok(b)) => serde_json::from_slice(&b).unwrap_or(Value::Null),
            _ => Value::Null,
        };
        (status, body)
    }
}

fn google() -> SignInProof {
    SignInProof::new(true, "google")
}

fn okta() -> SignInProof {
    SignInProof::new(false, "okta").with_okta_sub(Some("o".into()))
}

fn microsoft() -> SignInProof {
    SignInProof::new(false, "microsoft").with_entra(Some("t/o1".into()))
}

/// Every kind of session each stream route checks, and what each one asked
/// of DynamoDB.
async fn every_session(route: &str) -> BTreeSet<(String, String)> {
    let mut calls = BTreeSet::new();
    for self_hosted in [false, true] {
        let _mode = crate::routes::setup_checks::test_self_hosted::set(self_hosted);
        for (rule, proof) in [
            (None, google()),
            (Some("okta"), google()),
            (Some("okta"), okta()),
            (None, microsoft()),
            (Some("microsoft"), microsoft()),
        ] {
            let w = world().await;
            w.member(&[
                ("okta_issuer", json!(ISSUER)),
                ("okta_sub", json!("o")),
                ("ms_tid", json!("t")),
                ("ms_oid", json!("o1")),
            ]);
            w.okta();
            if self_hosted {
                w.org(rule.is_some());
            } else {
                w.rule(rule);
            }
            let (status, _) = w.post(route, &proof).await;
            assert_ne!(
                status,
                StatusCode::NOT_FOUND,
                "{route} is served by the streaming function"
            );
            calls.extend(w.f.store.dynamo_calls());
        }
    }
    calls
}

/// The table a recorded resource belongs to, as the file names it.
fn listed(granted: &BTreeMap<String, BTreeSet<String>>, action: &str, resource: &str) -> bool {
    granted.get(resource).is_some_and(|a| a.contains(action))
}

#[tokio::test]
async fn every_table_a_stream_route_reads_is_granted_to_the_streaming_function() {
    let granted = granted();
    let mut missing = Vec::new();
    let mut seen = BTreeSet::new();
    for route in [BRAIN, PLAN_CHAT] {
        for (action, resource) in every_session(route).await {
            if !listed(&granted, &action, &resource) {
                missing.push(format!("{route}: {action} on {resource}"));
            }
            seen.insert(resource);
        }
    }
    assert!(
        missing.is_empty(),
        "the streaming function's code calls DynamoDB where its role may not \
         (add each to infra/streaming-dynamodb.json and grant it in \
         infra/lib/streaming-stack.ts):\n{}",
        missing.join("\n")
    );
    // The member check and the team's rule are read on every stream: the
    // recording is real, not empty.
    assert!(seen.contains("users"), "{seen:?}");
    assert!(seen.contains("settings"), "{seen:?}");
}

/// The file is the role's DynamoDB contract; a typo in it would grant the
/// wrong thing quietly.
#[test]
fn the_granted_file_names_only_dynamodb_actions_on_known_tables() {
    let known = [
        "main",
        "teams",
        "users",
        "settings",
        "repos",
        "runs",
        "analytics",
        "events",
        "jira-tokens",
        "jira-events",
        "plans",
        "jira-config",
        "aws-insights",
        "infra",
        "banners",
        "mcp-configs",
        "waitlist",
        "traces",
        "tests",
    ];
    for (resource, actions) in granted() {
        let table = resource.split('/').next().unwrap();
        assert!(known.contains(&table), "unknown table {resource}");
        assert!(!actions.is_empty(), "{resource} lists no action");
        for a in actions {
            assert!(
                a.starts_with("dynamodb:") && !a.contains('*'),
                "{resource}: {a} must be one DynamoDB action"
            );
        }
    }
}

/// A member record that can't be read is the server's failure: Chat says to
/// try again, and never that the session ended.
#[tokio::test]
async fn a_member_record_that_cant_be_read_is_not_an_ended_session() {
    let w = world().await;
    w.member(&[]);
    w.rule(None);
    w.f.store.fail_reads("users", TEAM);
    let (status, body) = w.post(BRAIN, &google()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["code"], "sign_in_check_failed", "{body}");
    assert_eq!(
        body["error"],
        "Chat couldn't check your sign-in right now. Try again."
    );
    let (status, _) = w.post(PLAN_CHAT, &google()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    // A session that really ended still says so.
    let w = world().await;
    w.rule(None);
    let (status, body) = w.post(BRAIN, &google()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(body["code"], "session_ended", "{body}");
}

#[test]
fn only_a_refusal_says_the_session_ended() {
    use super::brain::scoped_refusal;
    for s in [
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::BAD_GATEWAY,
    ] {
        let (status, body) = scoped_refusal(s);
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.0["code"], "sign_in_check_failed");
        assert!(!body.0["error"].as_str().unwrap().contains("session ended"));
    }
    for s in [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
        let (status, body) = scoped_refusal(s);
        assert_eq!(status, s);
        assert_eq!(body.0["code"], "session_ended");
    }
}
