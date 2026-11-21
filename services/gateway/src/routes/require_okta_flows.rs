//! Require Okta sign-in through the real handlers and the real middleware,
//! against the AWS stand-in (`crate::fake_aws`): one test per way a session
//! is made or used. Each one makes a session or a sign-in that main lets
//! through and checks that the rule refuses it (and that it works again
//! with the rule off).

use std::sync::Arc;

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
    Router,
};
use serde_json::{json, Value};
use tower::ServiceExt;

use crate::auth::jwt::{self, SignInProof};
use crate::fake_aws::{self, Fake, User};
use crate::middleware::auth::SESSION_TTL_SECS;
use crate::AppState;

const GATED: &str = "TEAM#gated";
const OTHER: &str = "TEAM#other";
const ISSUER: &str = "https://idp.example.com";
const RULE_SINCE: u64 = 1_000;

struct World {
    f: Fake,
    state: Arc<AppState>,
    app: Router,
}

async fn world() -> World {
    let f = fake_aws::start().await;
    let state = fake_aws::state(&f.url);
    let app = crate::build_app(state.clone());
    World { f, state, app }
}

impl World {
    fn rule(&self, team: &str, on: bool) {
        self.f.store.put(
            "settings",
            &[
                ("pk", json!(team)),
                ("sk", json!(super::sign_in::TEAM_RULE_SK)),
                ("require_okta", json!(on)),
                ("require_okta_since", json!(RULE_SINCE)),
            ],
        );
    }

    fn okta(&self, team: &str, enabled: bool) {
        self.f.store.put(
            "settings",
            &[
                ("pk", json!(team)),
                ("sk", json!(super::oidc_signin::OKTA_SK)),
                ("issuer", json!(ISSUER)),
                ("client_id", json!("client")),
                ("client_secret_sealed", json!("sealed")),
                ("domains", json!(["example.com"])),
                ("enabled", json!(enabled)),
                ("tested_at", json!("2026-11-01T00:00:00Z")),
            ],
        );
    }

    fn member(&self, team: &str, user: &str, email: &str, role: &str, extra: &[(&str, Value)]) {
        let mut item = vec![
            ("pk", json!(team)),
            ("sk", json!(user)),
            ("email", json!(email)),
            ("role", json!(role)),
            ("status", json!("active")),
            ("gsi2pk", json!(format!("EMAIL#{email}"))),
            ("gsi2sk", json!(team)),
        ];
        item.extend(extra.iter().cloned());
        self.f.store.put("users", &item);
    }

    fn invite(&self, team: &str, email: &str) {
        self.f.store.put(
            "users",
            &[
                ("pk", json!(team)),
                ("sk", json!("USER#invite_1")),
                ("email", json!(email)),
                ("role", json!("member")),
                ("status", json!("invited")),
                ("invited_at", json!(chrono::Utc::now().to_rfc3339())),
                ("gsi2pk", json!(format!("EMAIL#{email}"))),
                ("gsi2sk", json!(team)),
            ],
        );
    }

    fn cookie(
        &self,
        team: &str,
        user: &str,
        email: &str,
        role: &str,
        proof: &SignInProof,
    ) -> String {
        let t = jwt::create_session_token(
            user,
            team,
            email,
            role,
            None,
            proof,
            &self.state.secrets.jwt_secret,
            SESSION_TTL_SECS,
        )
        .unwrap();
        format!("__Host-coderhelm_session={t}")
    }

    async fn call(&self, method: &str, path: &str, cookie: &str, body: Option<Value>) -> Answer {
        let mut req = Request::builder().method(method).uri(path);
        if !cookie.is_empty() {
            req = req.header(header::COOKIE, cookie);
        }
        let req = match body {
            Some(b) => req
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(b.to_string())),
            None => req.body(Body::empty()),
        }
        .unwrap();
        let res = self.app.clone().oneshot(req).await.unwrap();
        let status = res.status();
        let cookies: Vec<String> = res
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .map(|v| v.to_str().unwrap().to_string())
            .collect();
        let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .unwrap();
        Answer {
            status,
            body: serde_json::from_slice(&bytes).unwrap_or(Value::Null),
            cookies,
        }
    }

    /// The proof of the session an answer set, if it set one.
    fn session_proof(&self, a: &Answer) -> Option<(crate::models::Claims, SignInProof)> {
        let c = a.cookies.iter().find_map(|c| {
            c.strip_prefix("__Host-coderhelm_session=")
                .map(|r| r.split(';').next().unwrap_or("").to_string())
        })?;
        if c.is_empty() {
            return None;
        }
        let claims = jwt::validate_token(&c, &self.state.secrets.jwt_secret).ok()?;
        Some((claims, jwt::read_proof(&c, &self.state.secrets.jwt_secret)))
    }
}

struct Answer {
    status: StatusCode,
    body: Value,
    cookies: Vec<String>,
}

impl Answer {
    fn ends_session(&self) -> bool {
        self.cookies
            .iter()
            .any(|c| c.starts_with("__Host-coderhelm_session=;") && c.contains("Max-Age=0"))
    }
}

fn okta_proof(sub: &str) -> SignInProof {
    SignInProof::new(false, "okta").with_okta_sub(Some(sub.into()))
}

const STATUS: &str = "/api/users/2fa";

// ── Sessions in use (middleware) ────────────────────────────────────

#[tokio::test]
async fn every_session_from_another_way_in_ends_while_the_team_requires_okta() {
    let w = world().await;
    w.member(GATED, "USER#1", "ann@example.com", "member", &[]);
    for (via, mfa) in [
        ("password", false),
        ("password", true),
        ("google", true),
        ("github", false),
        ("microsoft", true),
    ] {
        let c = w.cookie(
            GATED,
            "USER#1",
            "ann@example.com",
            "member",
            &SignInProof::new(mfa, via),
        );
        w.rule(GATED, false);
        assert_eq!(
            w.call("GET", STATUS, &c, None).await.status,
            StatusCode::OK,
            "{via} off"
        );
        w.rule(GATED, true);
        let a = w.call("GET", STATUS, &c, None).await;
        assert_eq!(a.status, StatusCode::UNAUTHORIZED, "{via}");
        assert_eq!(a.body["code"], "okta_required", "{via}");
        assert!(a.body["sign_in_url"]
            .as_str()
            .unwrap()
            .contains("signin_error=okta_required"));
        assert!(a.ends_session(), "{via}: the cookie is cleared");
    }
    // A token from before sign-in proofs existed.
    let old = jwt::create_token(
        "USER#1",
        GATED,
        "ann@example.com",
        "member",
        None,
        &w.state.secrets.jwt_secret,
        300,
    )
    .unwrap();
    let a = w
        .call(
            "GET",
            STATUS,
            &format!("__Host-coderhelm_session={old}"),
            None,
        )
        .await;
    assert_eq!(a.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn an_okta_session_of_the_team_stands() {
    let w = world().await;
    w.rule(GATED, true);
    w.okta(GATED, true);
    w.member(
        GATED,
        "USER#1",
        "ann@example.com",
        "member",
        &[("okta_issuer", json!(ISSUER)), ("okta_sub", json!("sub-1"))],
    );
    let c = w.cookie(
        GATED,
        "USER#1",
        "ann@example.com",
        "member",
        &okta_proof("sub-1"),
    );
    assert_eq!(w.call("GET", STATUS, &c, None).await.status, StatusCode::OK);
}

#[tokio::test]
async fn a_break_glass_session_stands_only_for_the_owner_with_a_second_factor() {
    let w = world().await;
    w.rule(GATED, true);
    w.member(GATED, "USER#o", "owner@example.com", "owner", &[]);
    let bg = |mfa: bool| SignInProof::new(mfa, "password").with_break_glass(Some(GATED));
    let c = w.cookie(GATED, "USER#o", "owner@example.com", "owner", &bg(true));
    assert_eq!(w.call("GET", STATUS, &c, None).await.status, StatusCode::OK);
    // Without its second factor: refused.
    let c = w.cookie(GATED, "USER#o", "owner@example.com", "owner", &bg(false));
    assert_eq!(
        w.call("GET", STATUS, &c, None).await.status,
        StatusCode::UNAUTHORIZED
    );
    // Marked for another team: refused here.
    let c = w.cookie(
        GATED,
        "USER#o",
        "owner@example.com",
        "owner",
        &SignInProof::new(true, "password").with_break_glass(Some(OTHER)),
    );
    assert_eq!(
        w.call("GET", STATUS, &c, None).await.status,
        StatusCode::UNAUTHORIZED
    );
    // No longer the owner on their record: refused, whatever the token says.
    w.member(GATED, "USER#o", "owner@example.com", "admin", &[]);
    let c = w.cookie(GATED, "USER#o", "owner@example.com", "owner", &bg(true));
    assert_eq!(
        w.call("GET", STATUS, &c, None).await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn on_hosted_only_the_team_that_requires_okta_is_held() {
    let w = world().await;
    w.rule(GATED, true);
    w.member(GATED, "USER#1", "ann@example.com", "member", &[]);
    w.member(OTHER, "USER#1", "ann@example.com", "member", &[]);
    let c = w.cookie(
        OTHER,
        "USER#1",
        "ann@example.com",
        "member",
        &SignInProof::new(false, "google"),
    );
    assert_eq!(w.call("GET", STATUS, &c, None).await.status, StatusCode::OK);
    // Their record on the other team was never touched.
    assert!(w.f.store.get("users", OTHER, "USER#1").unwrap()["sessions_valid_after"].is_null());
}

#[tokio::test]
async fn a_connected_app_works_only_from_an_okta_sessions_grant() {
    let w = world().await;
    w.rule(GATED, true);
    w.okta(GATED, true);
    w.member(
        GATED,
        "USER#1",
        "ann@example.com",
        "member",
        &[("okta_issuer", json!(ISSUER)), ("okta_sub", json!("sub-1"))],
    );
    let claims = |via: &str| {
        crate::routes::oidc_signin::mark_okta(
            jwt::validate_token(
                w.cookie(
                    GATED,
                    "USER#1",
                    "ann@example.com",
                    "member",
                    &SignInProof::new(true, via),
                )
                .trim_start_matches("__Host-coderhelm_session="),
                &w.state.secrets.jwt_secret,
            )
            .unwrap(),
            Some(via),
            Some("sub-1".into()),
        )
    };
    // The check every MCP call makes (mcp_server, middleware app_request).
    let pw = super::sign_in::check_session(
        &w.state,
        &claims("password"),
        &SignInProof::new(true, "password").with_break_glass(Some(GATED)),
        None,
        true,
    )
    .await;
    assert_eq!(pw.unwrap_err().status(), StatusCode::UNAUTHORIZED);
    let okta =
        super::sign_in::check_session(&w.state, &claims("okta"), &okta_proof("sub-1"), None, true)
            .await;
    assert!(okta.is_ok());
}

#[tokio::test]
async fn turning_the_rule_off_lets_every_way_in_back() {
    let w = world().await;
    w.member(GATED, "USER#1", "ann@example.com", "member", &[]);
    let c = w.cookie(
        GATED,
        "USER#1",
        "ann@example.com",
        "member",
        &SignInProof::new(false, "google"),
    );
    w.rule(GATED, true);
    assert_eq!(
        w.call("GET", STATUS, &c, None).await.status,
        StatusCode::UNAUTHORIZED
    );
    w.rule(GATED, false);
    assert_eq!(w.call("GET", STATUS, &c, None).await.status, StatusCode::OK);
}

#[tokio::test]
async fn an_email_answer_link_takes_only_a_session_the_rule_allows() {
    let w = world().await;
    w.rule(GATED, true);
    w.okta(GATED, true);
    w.member(
        GATED,
        "USER#1",
        "ann@example.com",
        "member",
        &[("okta_issuer", json!(ISSUER)), ("okta_sub", json!("sub-1"))],
    );
    let headers = |c: String| {
        let mut h = axum::http::HeaderMap::new();
        h.insert(header::COOKIE, c.parse().unwrap());
        h
    };
    let pw = w.cookie(
        GATED,
        "USER#1",
        "ann@example.com",
        "member",
        &SignInProof::new(true, "password"),
    );
    assert!(crate::middleware::auth::signed_in_as(
        &headers(pw.clone()),
        &w.state.secrets.jwt_secret,
        GATED,
        "ann@example.com"
    ));
    assert!(
        !crate::middleware::auth::session_answers_as(
            &w.state,
            &headers(pw),
            GATED,
            "ann@example.com"
        )
        .await
    );
    let okta = w.cookie(
        GATED,
        "USER#1",
        "ann@example.com",
        "member",
        &okta_proof("sub-1"),
    );
    assert!(
        crate::middleware::auth::session_answers_as(
            &w.state,
            &headers(okta),
            GATED,
            "ann@example.com"
        )
        .await
    );
}

// ── Sessions made (sign-ins, switches, renewals) ────────────────────

fn password_user(w: &World, email: &str, mfa: bool) {
    w.f.store.add_user(User {
        sub: format!("sub-{email}"),
        email: email.into(),
        password: "pw-correct-horse".into(),
        mfa,
    });
}

fn login(email: &str, break_glass: bool) -> Value {
    json!({ "email": email, "password": "pw-correct-horse", "break_glass": break_glass })
}

#[tokio::test]
async fn a_password_sign_in_to_a_team_that_requires_okta_is_refused_and_makes_nothing() {
    let w = world().await;
    password_user(&w, "ann@example.com", false);
    w.member(
        GATED,
        "USER#sub-ann@example.com",
        "ann@example.com",
        "member",
        &[],
    );
    w.rule(GATED, false);
    let a = w
        .call(
            "POST",
            "/auth/login",
            "",
            Some(login("ann@example.com", false)),
        )
        .await;
    assert_eq!(a.status, StatusCode::OK);
    assert_eq!(w.session_proof(&a).unwrap().0.team_id, GATED);

    w.rule(GATED, true);
    let a = w
        .call(
            "POST",
            "/auth/login",
            "",
            Some(login("ann@example.com", false)),
        )
        .await;
    assert_eq!(a.status, StatusCode::FORBIDDEN);
    assert_eq!(a.body["code"], "okta_required");
    assert!(w.session_proof(&a).is_none());
    // No personal team was made instead.
    assert!(w
        .f
        .store
        .items("users", "TEAM#sub-ann@example.com")
        .is_empty());
    // Asking for break-glass doesn't help a member who isn't the owner.
    let a = w
        .call(
            "POST",
            "/auth/login",
            "",
            Some(login("ann@example.com", true)),
        )
        .await;
    assert_eq!(a.body["code"], "okta_required");
}

#[tokio::test]
async fn a_password_sign_in_lands_on_the_persons_other_team_and_leaves_the_invite() {
    let w = world().await;
    password_user(&w, "ann@example.com", false);
    w.rule(GATED, true);
    w.invite(GATED, "ann@example.com");
    w.member(
        OTHER,
        "USER#sub-ann@example.com",
        "ann@example.com",
        "member",
        &[],
    );
    let a = w
        .call(
            "POST",
            "/auth/login",
            "",
            Some(login("ann@example.com", false)),
        )
        .await;
    assert_eq!(a.status, StatusCode::OK);
    assert_eq!(w.session_proof(&a).unwrap().0.team_id, OTHER);
    // The invite to the team that requires Okta is accepted only with Okta.
    assert_eq!(
        w.f.store.get("users", GATED, "USER#invite_1").unwrap()["status"],
        "invited"
    );
}

#[tokio::test]
async fn break_glass_is_the_owners_with_a_second_factor_and_is_logged() {
    let w = world().await;
    password_user(&w, "owner@example.com", true);
    w.rule(GATED, true);
    w.member(
        GATED,
        "USER#sub-owner@example.com",
        "owner@example.com",
        "owner",
        &[],
    );
    // Without asking for break-glass: refused like everyone else.
    let a = w
        .call(
            "POST",
            "/auth/login",
            "",
            Some(login("owner@example.com", false)),
        )
        .await;
    assert_eq!(a.body["status"], "mfa_required");
    let a = w
        .call(
            "POST",
            "/auth/mfa/verify",
            "",
            Some(json!({
                "email": "owner@example.com",
                "session": a.body["session"],
                "code": "123456",
            })),
        )
        .await;
    assert_eq!(a.body["code"], "okta_required");
    assert!(w.session_proof(&a).is_none());
    // Asking: Cognito's own code first.
    let a = w
        .call(
            "POST",
            "/auth/login",
            "",
            Some(login("owner@example.com", true)),
        )
        .await;
    assert_eq!(a.body["status"], "mfa_required");
    let a = w
        .call(
            "POST",
            "/auth/mfa/verify",
            "",
            Some(json!({
                "email": "owner@example.com",
                "session": a.body["session"],
                "code": "123456",
                "break_glass": true,
            })),
        )
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    let (claims, proof) = w.session_proof(&a).unwrap();
    assert_eq!(claims.team_id, GATED);
    assert_eq!(proof.break_glass.as_deref(), Some(GATED));
    assert!(proof.mfa);
    // Logged before the session: the page's list and the audit log.
    let settings = w.f.store.items("settings", GATED);
    assert!(settings
        .iter()
        .any(|i| i["sk"].as_str().unwrap().starts_with("BREAK_GLASS#")
            && i["email"] == "owner@example.com"));
    assert!(settings
        .iter()
        .any(|i| i["action"] == "sign_in_break_glass"));
    // And the session works.
    let c = a
        .cookies
        .iter()
        .find(|c| c.starts_with("__Host-coderhelm_session="))
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    assert_eq!(w.call("GET", STATUS, &c, None).await.status, StatusCode::OK);
}

#[tokio::test]
async fn break_glass_without_any_second_factor_is_refused() {
    let w = world().await;
    password_user(&w, "owner@example.com", false);
    w.rule(GATED, true);
    w.member(
        GATED,
        "USER#sub-owner@example.com",
        "owner@example.com",
        "owner",
        &[],
    );
    let a = w
        .call(
            "POST",
            "/auth/login",
            "",
            Some(login("owner@example.com", true)),
        )
        .await;
    assert_eq!(a.status, StatusCode::FORBIDDEN);
    assert_eq!(a.body["code"], "break_glass_needs_2fa");
    assert!(w.session_proof(&a).is_none());
    assert!(!w
        .f
        .store
        .items("settings", GATED)
        .iter()
        .any(|i| i["sk"].as_str().unwrap().starts_with("BREAK_GLASS#")));
}

/// The authenticator-app step (after a password, Google, GitHub or
/// Microsoft): break-glass with the app's code is logged; any other sign-in
/// to a team that turned the rule on meanwhile is refused at the code.
#[tokio::test]
async fn the_second_step_obeys_the_rule_and_logs_break_glass() {
    let w = world().await;
    password_user(&w, "owner@example.com", false);
    w.member(
        GATED,
        "USER#sub-owner@example.com",
        "owner@example.com",
        "owner",
        &[],
    );
    // The owner's app factor, sealed as the gateway seals it.
    let secret = b"12345678901234567890";
    let (pk, sk) =
        super::sign_in::hosted_home_key("USER#sub-owner@example.com", "owner@example.com");
    let sealed = {
        use base64::Engine;
        use common::mcp_credentials::Cipher;
        let kms = common::mcp_credentials::Kms {
            client: &w.state.kms,
            key_id: &w.state.config.secrets_key_id,
        };
        let ctx = std::collections::HashMap::from([
            ("team".to_string(), pk.clone()),
            ("user".to_string(), sk.clone()),
            ("purpose".to_string(), "two_factor".to_string()),
        ]);
        base64::engine::general_purpose::STANDARD.encode(kms.encrypt(ctx, secret).await.unwrap())
    };
    w.f.store.put(
        "users",
        &[
            ("pk", json!(pk)),
            ("sk", json!(sk)),
            ("email", json!("owner@example.com")),
            ("totp_enabled", json!(true)),
            ("totp_sealed", json!(sealed)),
        ],
    );
    let code = |step: u64| super::sign_in::totp_at(secret, step);
    let now_step = chrono::Utc::now().timestamp() as u64 / 30;
    w.rule(GATED, true);
    let a = w
        .call(
            "POST",
            "/auth/login",
            "",
            Some(login("owner@example.com", true)),
        )
        .await;
    assert_eq!(a.body["status"], "second_step", "{}", a.body);
    let step_cookie = a
        .cookies
        .iter()
        .find(|c| c.starts_with("__Host-coderhelm_2fa="))
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let a = w
        .call(
            "POST",
            "/auth/2fa/verify",
            &step_cookie,
            Some(json!({ "code": code(now_step) })),
        )
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert_eq!(
        w.session_proof(&a).unwrap().1.break_glass.as_deref(),
        Some(GATED)
    );
    assert!(w
        .f
        .store
        .items("settings", GATED)
        .iter()
        .any(|i| i["sk"].as_str().unwrap().starts_with("BREAK_GLASS#")));

    // A Google sign-in that reached the code step before the rule: refused.
    let step = jwt::SecondStep {
        sub: "USER#sub-owner@example.com".into(),
        team_id: GATED.into(),
        email: "owner@example.com".into(),
        role: "owner".into(),
        github_login: None,
        via: "google".into(),
        okta_sub: None,
        break_glass: false,
        jti: String::new(),
        aud: String::new(),
        iat: 0,
        exp: 0,
    };
    let t = jwt::create_second_step(&step, &w.state.secrets.jwt_secret, 300).unwrap();
    let a = w
        .call(
            "POST",
            "/auth/2fa/verify",
            &format!("__Host-coderhelm_2fa={t}"),
            Some(json!({ "code": code(now_step + 1) })),
        )
        .await;
    assert_eq!(a.status, StatusCode::FORBIDDEN, "{}", a.body);
    assert_eq!(a.body["code"], "okta_required");
    assert!(w.session_proof(&a).is_none());
}

/// Microsoft's (and every OpenID sign-in's) shared ending.
#[tokio::test]
async fn a_microsoft_sign_in_is_refused_for_the_team_and_lands_elsewhere() {
    let w = world().await;
    w.member(GATED, "USER#ms", "ann@example.com", "member", &[]);
    let person = super::oidc_signin::Person {
        email: "ann@example.com".into(),
        name: "Ann".into(),
        new_user_id: "USER#ms".into(),
    };
    let finish = || {
        super::auth::finish_oidc(
            &w.state,
            "127.0.0.1",
            super::oauth_signin::Provider::Microsoft,
            &person,
            None,
            false,
        )
    };
    w.rule(GATED, false);
    let r = finish().await.unwrap();
    assert!(r.headers().get_all(header::SET_COOKIE).iter().any(|c| c
        .to_str()
        .unwrap()
        .starts_with("__Host-coderhelm_session=ey")));
    w.rule(GATED, true);
    let r = finish().await.unwrap();
    let to = r.headers()[header::LOCATION].to_str().unwrap().to_string();
    assert!(to.contains("signin_error=okta_required"), "{to}");
    assert!(to.contains("provider=microsoft"), "{to}");
    assert!(!r.headers().get_all(header::SET_COOKIE).iter().any(|c| c
        .to_str()
        .unwrap()
        .starts_with("__Host-coderhelm_session=ey")));
    // No personal team was made instead.
    let teams = w.f.store.items("main", "TEAM#");
    assert!(teams.is_empty());
    // On another team too: that one is landed on.
    w.member(OTHER, "USER#ms", "ann@example.com", "member", &[]);
    let r = finish().await.unwrap();
    assert!(r.headers().get_all(header::SET_COOKIE).iter().any(|c| c
        .to_str()
        .unwrap()
        .starts_with("__Host-coderhelm_session=ey")));
}

#[tokio::test]
async fn switching_into_a_team_that_requires_okta_is_refused() {
    let w = world().await;
    w.member(OTHER, "USER#1", "ann@example.com", "member", &[]);
    w.member(GATED, "USER#1", "ann@example.com", "admin", &[]);
    let c = w.cookie(
        OTHER,
        "USER#1",
        "ann@example.com",
        "member",
        &SignInProof::new(true, "password"),
    );
    let switch = json!({ "team_id": GATED });
    w.rule(GATED, false);
    let a = w
        .call("POST", "/api/teams/switch", &c, Some(switch.clone()))
        .await;
    assert_eq!(a.status, StatusCode::OK);
    assert_eq!(w.session_proof(&a).unwrap().0.team_id, GATED);
    w.rule(GATED, true);
    let a = w.call("POST", "/api/teams/switch", &c, Some(switch)).await;
    assert_eq!(a.status, StatusCode::FORBIDDEN);
    assert_eq!(a.body["code"], "okta_required");
    assert!(w.session_proof(&a).is_none());
}

#[tokio::test]
async fn the_team_list_accepts_no_invite_to_a_team_that_requires_okta() {
    let w = world().await;
    w.member(OTHER, "USER#1", "ann@example.com", "member", &[]);
    w.invite(GATED, "ann@example.com");
    w.rule(GATED, true);
    let c = w.cookie(
        OTHER,
        "USER#1",
        "ann@example.com",
        "member",
        &SignInProof::new(true, "password"),
    );
    assert_eq!(
        w.call("GET", "/api/teams", &c, None).await.status,
        StatusCode::OK
    );
    assert!(w.f.store.get("users", GATED, "USER#1").is_none());
    assert_eq!(
        w.f.store.get("users", GATED, "USER#invite_1").unwrap()["status"],
        "invited"
    );
}

// ── New members through Okta ────────────────────────────────────────

#[tokio::test]
async fn an_invite_accepted_through_okta_is_one_transaction_and_never_a_second_link() {
    let w = world().await;
    w.invite(GATED, "new@example.com");
    let invite = {
        use aws_sdk_dynamodb::types::AttributeValue as A;
        let mut i = std::collections::HashMap::new();
        for (k, v) in [
            ("pk", GATED),
            ("sk", "USER#invite_1"),
            ("email", "new@example.com"),
            ("role", "member"),
            ("status", "invited"),
        ] {
            i.insert(k.to_string(), A::S(v.to_string()));
        }
        i
    };
    let now = chrono::Utc::now().to_rfc3339();
    let rec =
        super::require_okta::invited_member(&invite, "USER#okta_s1", "New", ISSUER, "s1", &now);
    super::require_okta::accept_invite(&w.state, &rec, &invite)
        .await
        .unwrap();
    let m = w.f.store.get("users", GATED, "USER#okta_s1").unwrap();
    assert_eq!(
        (m["okta_sub"].as_str(), m["status"].as_str()),
        (Some("s1"), Some("active"))
    );
    assert!(w.f.store.get("users", GATED, "USER#invite_1").is_none());
    let (row_pk, row_sk) = super::oidc_signin::link_row(GATED, ISSUER, "s1");
    assert!(w.f.store.get("settings", &row_pk, &row_sk).is_some());

    // The same identity again (a second invite): its link row is taken, so
    // nothing is written.
    w.f.store.put(
        "users",
        &[
            ("pk", json!(GATED)),
            ("sk", json!("USER#invite_2")),
            ("email", json!("other@example.com")),
            ("status", json!("invited")),
        ],
    );
    let mut second = invite.clone();
    second.insert(
        "sk".into(),
        aws_sdk_dynamodb::types::AttributeValue::S("USER#invite_2".into()),
    );
    let rec2 =
        super::require_okta::invited_member(&second, "USER#okta_s1b", "", ISSUER, "s1", &now);
    assert!(super::require_okta::accept_invite(&w.state, &rec2, &second)
        .await
        .is_err());
    assert!(w.f.store.get("users", GATED, "USER#okta_s1b").is_none());
    assert!(w.f.store.get("users", GATED, "USER#invite_2").is_some());

    // An existing record with that id is never taken over.
    w.member(GATED, "USER#okta_s2", "x@example.com", "admin", &[]);
    let rec3 = super::require_okta::invited_member(&second, "USER#okta_s2", "", ISSUER, "s2", &now);
    assert!(super::require_okta::accept_invite(&w.state, &rec3, &second)
        .await
        .is_err());
    assert_eq!(
        w.f.store.get("users", GATED, "USER#okta_s2").unwrap()["role"],
        "admin"
    );
}

#[tokio::test]
async fn an_invite_made_while_the_rule_is_on_always_waits_for_okta() {
    let w = world().await;
    w.rule(GATED, true);
    // Someone with a password account already (main adds them straight to
    // the team, with no invite to accept).
    password_user(&w, "has-account@example.com", false);
    // While the rule is on, an admin acts from an Okta session.
    w.okta(GATED, true);
    w.member(
        GATED,
        "USER#adm",
        "adm@example.com",
        "admin",
        &[("okta_issuer", json!(ISSUER)), ("okta_sub", json!("adm"))],
    );
    let c = w.cookie(
        GATED,
        "USER#adm",
        "adm@example.com",
        "admin",
        &okta_proof("adm"),
    );
    let a = w
        .call(
            "POST",
            "/api/users/invite",
            &c,
            Some(json!({ "email": "has-account@example.com", "role": "member" })),
        )
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    let made: Vec<Value> =
        w.f.store
            .items("users", GATED)
            .into_iter()
            .filter(|i| i["email"] == "has-account@example.com")
            .collect();
    assert_eq!(made.len(), 1);
    assert_eq!(made[0]["status"], "invited");
}

// ── Settings, Sign-in rules ─────────────────────────────────────────

#[tokio::test]
async fn turning_the_rule_on_or_off_is_the_owners_and_on_needs_okta_set_up() {
    let w = world().await;
    w.member(GATED, "USER#adm", "adm@example.com", "admin", &[]);
    w.member(GATED, "USER#o", "o@example.com", "owner", &[]);
    let admin = w.cookie(
        GATED,
        "USER#adm",
        "adm@example.com",
        "admin",
        &SignInProof::new(true, "password"),
    );
    let owner = w.cookie(
        GATED,
        "USER#o",
        "o@example.com",
        "owner",
        &SignInProof::new(true, "password"),
    );
    let on = json!({ "require_okta": true });
    let a = w
        .call("PUT", "/api/sign-in", &admin, Some(on.clone()))
        .await;
    assert_eq!(
        (a.status, a.body["code"].as_str()),
        (StatusCode::FORBIDDEN, Some("owner_only"))
    );
    let a = w
        .call("PUT", "/api/sign-in", &owner, Some(on.clone()))
        .await;
    assert_eq!(
        (a.status, a.body["code"].as_str()),
        (StatusCode::CONFLICT, Some("okta_not_on"))
    );
    // Set up and on, but its domain was never proven.
    w.okta(GATED, true);
    let a = w
        .call("PUT", "/api/sign-in", &owner, Some(on.clone()))
        .await;
    assert_eq!(a.body["code"], "domains_not_verified");
    // Nothing was saved.
    assert!(w
        .f
        .store
        .get("settings", GATED, super::sign_in::TEAM_RULE_SK)
        .is_none());

    // Turning it off: the owner, logged.
    w.rule(GATED, true);
    w.okta(GATED, true);
    w.member(
        GATED,
        "USER#o",
        "o@example.com",
        "owner",
        &[("okta_issuer", json!(ISSUER)), ("okta_sub", json!("o"))],
    );
    let owner_okta = w.cookie(GATED, "USER#o", "o@example.com", "owner", &okta_proof("o"));
    let off = json!({ "require_okta": false });
    w.member(
        GATED,
        "USER#adm",
        "adm@example.com",
        "admin",
        &[("okta_issuer", json!(ISSUER)), ("okta_sub", json!("adm"))],
    );
    let admin_okta = w.cookie(
        GATED,
        "USER#adm",
        "adm@example.com",
        "admin",
        &okta_proof("adm"),
    );
    let a = w
        .call("PUT", "/api/sign-in", &admin_okta, Some(off.clone()))
        .await;
    assert_eq!(a.body["code"], "owner_only");
    let a = w.call("PUT", "/api/sign-in", &owner_okta, Some(off)).await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert_eq!(a.body["require_okta"], false);
    assert!(w
        .f
        .store
        .items("settings", GATED)
        .iter()
        .any(|i| i["action"] == "sign_in_rules"
            && i["detail"]
                .as_str()
                .unwrap()
                .contains("Okta sign-in no longer required")));
}

#[tokio::test]
async fn the_page_lists_unlinked_members_break_glass_and_what_turning_on_needs() {
    let w = world().await;
    w.rule(GATED, true);
    w.okta(GATED, true);
    w.member(
        GATED,
        "USER#o",
        "o@example.com",
        "owner",
        &[("okta_issuer", json!(ISSUER)), ("okta_sub", json!("o"))],
    );
    w.member(GATED, "USER#2", "b@example.com", "member", &[]);
    w.f.store.put(
        "settings",
        &[
            ("pk", json!(GATED)),
            ("sk", json!("BREAK_GLASS#2026-11-20T00:00:00.000Z#1")),
            ("at", json!("2026-11-20T00:00:00.000Z")),
            ("email", json!("o@example.com")),
        ],
    );
    let c = w.cookie(GATED, "USER#o", "o@example.com", "owner", &okta_proof("o"));
    let a = w.call("GET", "/api/sign-in", &c, None).await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert_eq!(a.body["require_okta"], true);
    assert_eq!(a.body["unlinked"][0]["email"], "b@example.com");
    assert_eq!(a.body["unlinked"].as_array().unwrap().len(), 1);
    assert_eq!(a.body["break_glass"][0]["email"], "o@example.com");
    assert_eq!(a.body["okta"]["session_is_okta"], true);
    assert_eq!(a.body["okta"]["is_owner"], true);
    assert_eq!(a.body["okta"]["domains_verified"], false);
    assert_eq!(a.body["can_require_okta"], false);
    let people = a.body["people"].as_array().unwrap();
    assert!(people
        .iter()
        .any(|p| p["email"] == "o@example.com" && p["okta_linked"] == true));
}

#[tokio::test]
async fn okta_cant_be_turned_off_or_removed_while_it_is_required() {
    let w = world().await;
    w.rule(GATED, true);
    w.okta(GATED, true);
    w.member(
        GATED,
        "USER#o",
        "o@example.com",
        "owner",
        &[("okta_issuer", json!(ISSUER)), ("okta_sub", json!("o"))],
    );
    let c = w.cookie(GATED, "USER#o", "o@example.com", "owner", &okta_proof("o"));
    let a = w
        .call(
            "PUT",
            "/api/sign-in/okta",
            &c,
            Some(json!({ "enabled": false })),
        )
        .await;
    assert_eq!(a.body["code"], "okta_required_on");
    let a = w.call("DELETE", "/api/sign-in/okta", &c, None).await;
    assert_eq!(a.body["code"], "okta_required_on");
    let a = w
        .call("DELETE", "/api/sign-in/okta/domains/example.com", &c, None)
        .await;
    assert_eq!(a.body["code"], "okta_required_on");
    // Still on.
    let live =
        w.f.store
            .get("settings", GATED, super::oidc_signin::OKTA_SK)
            .unwrap();
    assert_eq!(live["enabled"], true);
}
