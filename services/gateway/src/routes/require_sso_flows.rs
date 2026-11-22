//! Require single sign-on through the real handlers and the real middleware,
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
    /// The team's rule: on with Okta, or off.
    fn rule(&self, team: &str, on: bool) {
        self.rule_with(team, on.then_some("okta"));
    }

    /// The team's rule with a provider's slug, or off.
    fn rule_with(&self, team: &str, provider: Option<&str>) {
        let mut item = vec![
            ("pk", json!(team)),
            ("sk", json!(super::sign_in::TEAM_RULE_SK)),
        ];
        if let Some(p) = provider {
            item.push(("require_sso", json!(p)));
            item.push(("require_sso_since", json!(RULE_SINCE)));
        }
        self.f.store.put("settings", &item);
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
        let location = res
            .headers()
            .get(header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
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
            location,
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
    location: Option<String>,
}

impl Answer {
    fn ends_session(&self) -> bool {
        self.cookies
            .iter()
            .any(|c| c.starts_with("__Host-coderhelm_session=;") && c.contains("Max-Age=0"))
    }
}

/// A stored item (plain strings) as the SDK reads it.
fn as_item(
    v: &Value,
) -> std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue> {
    v.as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                aws_sdk_dynamodb::types::AttributeValue::S(v.as_str().unwrap().to_string()),
            )
        })
        .collect()
}

fn okta_id(sub: &str) -> String {
    super::oidc_signin::okta_user_id(ISSUER, sub)
}

fn okta_link(sub: &str) -> super::require_sso::Link {
    super::require_sso::Link::Okta {
        issuer: ISSUER.into(),
        sub: sub.into(),
    }
}

/// The Okta link an invite-made record carries.
fn link_of(
    rec: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> super::require_sso::Link {
    okta_link(rec["okta_sub"].as_s().unwrap())
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
        assert_eq!(a.body["code"], "sso_required", "{via}");
        assert!(a.body["sign_in_url"]
            .as_str()
            .unwrap()
            .contains("signin_error=sso_required"));
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

/// Item 1: there is no break-glass. The owner's own sessions from every
/// other way in end like anyone's, a second factor or not.
#[tokio::test]
async fn the_owners_sessions_from_any_other_way_in_end_like_anyones() {
    let w = world().await;
    w.rule(GATED, true);
    w.okta(GATED, true);
    w.member(GATED, "USER#o", "owner@example.com", "owner", &[]);
    let other_tenant = SignInProof::new(true, "microsoft")
        .with_entra(Some(format!("{}/o1", super::entra_sso::CONSUMER_TENANT)));
    for proof in [
        SignInProof::new(true, "password"),
        SignInProof::new(false, "password"),
        SignInProof::new(true, "google"),
        SignInProof::new(true, "github"),
        SignInProof::new(true, "microsoft"),
        other_tenant,
    ] {
        let c = w.cookie(GATED, "USER#o", "owner@example.com", "owner", &proof);
        let a = w.call("GET", STATUS, &c, None).await;
        assert_eq!(a.status, StatusCode::UNAUTHORIZED, "{proof:?}");
        // Another tenant's identity is no link of theirs: the member check
        // ends it before the rule is even asked.
        let code = a.body["code"].as_str().unwrap_or_default();
        assert!(
            code == "sso_required" || code == "session_ended",
            "{proof:?} {code}"
        );
        assert!(a.ends_session());
    }
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
        crate::routes::oidc_signin::mark_session(
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
            &SignInProof::new(true, via).with_okta_sub(Some("sub-1".into())),
        )
    };
    // The check every MCP call makes (mcp_server, middleware app_request).
    let pw = super::sign_in::check_session(
        &w.state,
        &claims("password"),
        &SignInProof::new(true, "password"),
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

fn login(email: &str) -> Value {
    json!({ "email": email, "password": "pw-correct-horse" })
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
        .call("POST", "/auth/login", "", Some(login("ann@example.com")))
        .await;
    assert_eq!(a.status, StatusCode::OK);
    assert_eq!(w.session_proof(&a).unwrap().0.team_id, GATED);

    w.rule(GATED, true);
    let a = w
        .call("POST", "/auth/login", "", Some(login("ann@example.com")))
        .await;
    assert_eq!(a.status, StatusCode::FORBIDDEN);
    assert_eq!(a.body["code"], "sso_required");
    assert!(w.session_proof(&a).is_none());
    // No personal team was made instead.
    assert!(w
        .f
        .store
        .items("users", "TEAM#sub-ann@example.com")
        .is_empty());
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
        .call("POST", "/auth/login", "", Some(login("ann@example.com")))
        .await;
    assert_eq!(a.status, StatusCode::OK);
    assert_eq!(w.session_proof(&a).unwrap().0.team_id, OTHER);
    // The invite to the team that requires Okta is accepted only with Okta.
    assert_eq!(
        w.f.store.get("users", GATED, "USER#invite_1").unwrap()["status"],
        "invited"
    );
}

/// Item 1: the owner's password and Cognito's code make no session while
/// the rule is on, and nothing is written as if they had.
#[tokio::test]
async fn the_owners_password_and_second_factor_sign_in_is_refused() {
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
    // An old dashboard still sending the retired fields gets nothing more.
    for body in [
        login("owner@example.com"),
        json!({ "email": "owner@example.com", "password": "pw-correct-horse", "break_glass": true, "break_glass_team": GATED }),
    ] {
        let a = w.call("POST", "/auth/login", "", Some(body)).await;
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
        assert_eq!(a.status, StatusCode::FORBIDDEN, "{}", a.body);
        assert_eq!(a.body["code"], "sso_required");
        assert!(w.session_proof(&a).is_none());
    }
    // No personal team instead, and nothing logged as a way in.
    assert!(w
        .f
        .store
        .items("users", "TEAM#sub-owner@example.com")
        .is_empty());
    assert!(!w
        .f
        .store
        .items("settings", GATED)
        .iter()
        .any(|i| i["sk"].as_str().unwrap().starts_with("BREAK_GLASS#")));
}

/// The authenticator-app step (after a password, Google, GitHub or
/// Microsoft): the owner's password and app code make no session while the
/// rule is on, and a sign-in that reached the code before the rule was
/// turned on is refused at the code.
#[tokio::test]
async fn the_second_step_obeys_the_rule_with_no_way_around_it() {
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
    // With the rule on, the owner's password is refused before any step.
    w.rule(GATED, true);
    let a = w
        .call("POST", "/auth/login", "", Some(login("owner@example.com")))
        .await;
    assert_eq!(a.body["code"], "sso_required", "{}", a.body);
    // A password sign-in that reached the code before the rule went on: the
    // owner's own app code makes no session.
    w.rule(GATED, false);
    let a = w
        .call("POST", "/auth/login", "", Some(login("owner@example.com")))
        .await;
    assert_eq!(a.body["status"], "second_step", "{}", a.body);
    w.rule(GATED, true);
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
    assert_eq!(a.status, StatusCode::FORBIDDEN, "{}", a.body);
    assert_eq!(a.body["code"], "sso_required");
    assert!(w.session_proof(&a).is_none());

    // A Google sign-in that reached the code step before the rule: refused.
    let step = jwt::SecondStep {
        sub: "USER#sub-owner@example.com".into(),
        team_id: GATED.into(),
        email: "owner@example.com".into(),
        role: "owner".into(),
        github_login: None,
        via: "google".into(),
        okta_sub: None,
        entra: None,
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
    assert_eq!(a.body["code"], "sso_required");
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
    assert!(to.contains("signin_error=sso_required"), "{to}");
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
    assert_eq!(a.body["code"], "sso_required");
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
    // The invite as the callback reads it.
    let invite = as_item(&w.f.store.get("users", GATED, "USER#invite_1").unwrap());
    let now = chrono::Utc::now().to_rfc3339();
    let rec =
        super::require_sso::invited_member(&invite, "USER#okta_s1", "New", &okta_link("s1"), &now);
    super::require_sso::accept_invite(&w.state, &rec, &invite, &link_of(&rec))
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
    let second = as_item(&w.f.store.get("users", GATED, "USER#invite_2").unwrap());
    let rec2 =
        super::require_sso::invited_member(&second, "USER#okta_s1b", "", &okta_link("s1"), &now);
    assert!(
        super::require_sso::accept_invite(&w.state, &rec2, &second, &link_of(&rec2))
            .await
            .is_err()
    );
    assert!(w.f.store.get("users", GATED, "USER#okta_s1b").is_none());
    assert!(w.f.store.get("users", GATED, "USER#invite_2").is_some());

    // An existing record with that id is never taken over.
    w.member(GATED, "USER#okta_s2", "x@example.com", "admin", &[]);
    let rec3 =
        super::require_sso::invited_member(&second, "USER#okta_s2", "", &okta_link("s2"), &now);
    assert!(
        super::require_sso::accept_invite(&w.state, &rec3, &second, &link_of(&rec3))
            .await
            .is_err()
    );
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
    let on = json!({ "require_sso": "okta" });
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
        (StatusCode::CONFLICT, Some("sso_not_on"))
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
    let off = json!({ "require_sso": "" });
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
    assert_eq!(a.body["require_sso"], Value::Null);
    assert!(w
        .f
        .store
        .items("settings", GATED)
        .iter()
        .any(|i| i["action"] == "sign_in_rules"
            && i["detail"]
                .as_str()
                .unwrap()
                .contains("Single sign-on no longer required")));
}

#[tokio::test]
async fn the_page_lists_unlinked_members_claims_and_what_turning_on_needs() {
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
            ("pk", json!(format!("SSO_CLAIM#{GATED}"))),
            ("sk", json!("USER#2")),
            ("email", json!("b@example.com")),
            ("role", json!("member")),
            ("provider", json!("okta")),
            ("token_hash", json!("h")),
            ("expires_at", json!(4_000_000_000u64)),
            ("used", json!(false)),
        ],
    );
    let c = w.cookie(GATED, "USER#o", "o@example.com", "owner", &okta_proof("o"));
    let a = w.call("GET", "/api/sign-in", &c, None).await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert_eq!(a.body["require_sso"], "okta");
    let okta = &a.body["sso_options"][0];
    assert_eq!(okta["provider"], "okta");
    assert_eq!(okta["unlinked"][0]["email"], "b@example.com");
    assert_eq!(okta["unlinked"].as_array().unwrap().len(), 1);
    assert_eq!(okta["session_from_provider"], true);
    assert_eq!(okta["is_owner"], true);
    assert_eq!(okta["domains_verified"], false);
    assert_eq!(okta["ready"], false);
    assert!(okta["recovery"].as_str().unwrap().contains("support"));
    assert_eq!(a.body["sso_options"][1]["provider"], "microsoft");
    assert_eq!(a.body["sso_options"][1]["set_up"], false);
    assert_eq!(a.body["claim_links"][0]["email"], "b@example.com");
    assert_eq!(a.body["claim_links"][0]["used"], false);
    assert!(a.body["claim_links"][0].get("token_hash").is_none());
    assert!(a.body.get("break_glass").is_none());
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
    assert_eq!(a.body["code"], "sso_required_on");
    let a = w.call("DELETE", "/api/sign-in/okta", &c, None).await;
    assert_eq!(a.body["code"], "sso_required_on");
    let a = w
        .call("DELETE", "/api/sign-in/okta/domains/example.com", &c, None)
        .await;
    assert_eq!(a.body["code"], "sso_required_on");
    // Still on.
    let live =
        w.f.store
            .get("settings", GATED, super::oidc_signin::OKTA_SK)
            .unwrap();
    assert_eq!(live["enabled"], true);
}

// ── Second review ───────────────────────────────────────────────────

impl World {
    /// A proof of `domain` by `team`, checked just now (no DNS read needed).
    fn proven(&self, team: &str, domain: &str) {
        let now = chrono::Utc::now().to_rfc3339();
        self.f.store.put(
            "settings",
            &[
                ("pk", json!(format!("OKTA_DOMAIN#{domain}"))),
                ("sk", json!("OKTA")),
                ("team_id", json!(team)),
                ("verified_at", json!(now)),
                ("checked_at", json!(now)),
            ],
        );
    }

    fn invite_to(&self, team: &str, sk: &str, email: &str, role: &str) {
        self.f.store.put(
            "users",
            &[
                ("pk", json!(team)),
                ("sk", json!(sk)),
                ("email", json!(email)),
                ("role", json!(role)),
                ("status", json!("invited")),
                ("invited_at", json!(chrono::Utc::now().to_rfc3339())),
                ("gsi2pk", json!(format!("EMAIL#{email}"))),
                ("gsi2sk", json!(team)),
            ],
        );
    }

    fn live_setup(&self, team: &str) -> super::oidc_signin::OktaSetup {
        super::oidc_signin::OktaSetup {
            team_id: team.into(),
            issuer: ISSUER.into(),
            client_id: "client".into(),
            secret_sealed: "sealed".into(),
            domains: vec!["example.com".into()],
            enabled: true,
            tested_at: Some("2026-11-01T00:00:00Z".into()),
            ..Default::default()
        }
    }

    /// An Okta sign-in whose id_token for this team's setup was checked.
    async fn okta_lands(
        &self,
        team: &str,
        sub: &str,
        email: &str,
    ) -> Result<axum::response::Response, String> {
        let claims = json!({ "iss": ISSUER, "sub": sub, "email": email, "email_verified": true, "name": "Someone" });
        super::oidc_signin::okta_land(
            &self.state,
            "127.0.0.1",
            &self.live_setup(team),
            &claims,
            None,
        )
        .await
        .map_err(|f| f.reason.code().to_string())
    }
}

fn sets_session(r: &axum::response::Response) -> bool {
    r.headers().get_all(header::SET_COOKIE).iter().any(|c| {
        c.to_str()
            .unwrap()
            .starts_with("__Host-coderhelm_session=ey")
    })
}

/// Finding 1: an invite is accepted only for exactly Okta's verified
/// address; `+tag` addresses are other mailboxes.
#[tokio::test]
async fn an_okta_sign_in_accepts_only_the_invite_for_exactly_its_address() {
    let w = world().await;
    w.rule(GATED, true);
    w.okta(GATED, true);
    w.proven(GATED, "example.com");
    w.invite_to(
        GATED,
        "USER#invite_c",
        "alice+contractor@example.com",
        "member",
    );
    // Another Okta user whose address folds to the same one: refused, and
    // the invite stays.
    assert_eq!(
        w.okta_lands(GATED, "s-alice", "alice@example.com")
            .await
            .unwrap_err(),
        "sso_ask_admin"
    );
    assert_eq!(
        w.f.store.get("users", GATED, "USER#invite_c").unwrap()["status"],
        "invited"
    );
    assert!(w.f.store.get("users", GATED, &okta_id("s-alice")).is_none());
    // The invited address itself: accepted, linked, signed in.
    let r = w
        .okta_lands(GATED, "s-c", "Alice+Contractor@example.com")
        .await
        .unwrap();
    assert!(sets_session(&r));
    let m = w.f.store.get("users", GATED, &okta_id("s-c")).unwrap();
    assert_eq!(m["email"], "alice+contractor@example.com");
    assert_eq!(m["okta_sub"], "s-c");
    assert!(w.f.store.get("users", GATED, "USER#invite_c").is_none());

    // The other way round: an invite for the plain address isn't taken by
    // a `+tag` Okta user.
    w.invite_to(GATED, "USER#invite_p", "bob@example.com", "member");
    assert_eq!(
        w.okta_lands(GATED, "s-bob-x", "bob+x@example.com")
            .await
            .unwrap_err(),
        "sso_ask_admin"
    );
    assert_eq!(
        w.f.store.get("users", GATED, "USER#invite_p").unwrap()["status"],
        "invited"
    );
}

/// Finding 1: the Okta landing still needs a domain this team proved, and
/// never accepts beside an existing member of the same person.
#[tokio::test]
async fn an_okta_invite_needs_the_proven_domain_and_no_existing_member() {
    let w = world().await;
    w.rule(GATED, true);
    w.okta(GATED, true);
    // Not proven: refused before any invite is looked at.
    w.invite_to(GATED, "USER#invite_1", "new@example.com", "member");
    assert_eq!(
        w.okta_lands(GATED, "s-new", "new@example.com")
            .await
            .unwrap_err(),
        "not_on_team"
    );
    // Proven by another team: refused too.
    w.proven(OTHER, "example.com");
    assert_eq!(
        w.okta_lands(GATED, "s-new", "new@example.com")
            .await
            .unwrap_err(),
        "not_on_team"
    );
    w.proven(GATED, "example.com");
    // A member who is the same person (a `+tag` invite beside them).
    w.member(GATED, "USER#9", "dana@example.com", "member", &[]);
    w.invite_to(GATED, "USER#invite_d", "dana+2@example.com", "admin");
    assert_eq!(
        w.okta_lands(GATED, "s-dana", "dana+2@example.com")
            .await
            .unwrap_err(),
        "sso_ask_admin"
    );
    assert!(w.f.store.get("users", GATED, "USER#invite_d").is_some());
    // The plain case works.
    assert!(sets_session(
        &w.okta_lands(GATED, "s-new", "new@example.com")
            .await
            .unwrap()
    ));
}

/// Finding 6: an invite changed (or re-sent) after it was read is not
/// accepted with its old role.
#[tokio::test]
async fn an_invite_changed_after_it_was_read_is_not_accepted() {
    let w = world().await;
    w.invite_to(GATED, "USER#invite_1", "new@example.com", "member");
    let invite = as_item(&w.f.store.get("users", GATED, "USER#invite_1").unwrap());
    // An admin changes the role meanwhile.
    w.invite_to(GATED, "USER#invite_1", "new@example.com", "viewer");
    let now = chrono::Utc::now().to_rfc3339();
    let rec = super::require_sso::invited_member(&invite, "USER#okta_n", "", &okta_link("n"), &now);
    assert!(
        super::require_sso::accept_invite(&w.state, &rec, &invite, &link_of(&rec))
            .await
            .is_err()
    );
    assert!(w.f.store.get("users", GATED, "USER#okta_n").is_none());
    assert_eq!(
        w.f.store.get("users", GATED, "USER#invite_1").unwrap()["role"],
        "viewer"
    );
}

/// Finding 2: a write that narrows Okta lands only while the rule is off,
/// checked in the same transaction; turning the rule on holds only while the
/// setup is unchanged.
#[tokio::test]
async fn narrowing_okta_and_turning_the_rule_on_never_both_land() {
    use aws_sdk_dynamodb::types::{Delete, TransactWriteItem};
    let w = world().await;
    w.okta(GATED, true);
    w.proven(GATED, "example.com");
    let delete = || {
        TransactWriteItem::builder()
            .delete(
                Delete::builder()
                    .table_name("settings")
                    .key(
                        "pk",
                        aws_sdk_dynamodb::types::AttributeValue::S(GATED.into()),
                    )
                    .key(
                        "sk",
                        aws_sdk_dynamodb::types::AttributeValue::S(
                            super::oidc_signin::OKTA_SK.into(),
                        ),
                    )
                    .build()
                    .unwrap(),
            )
            .build()
    };
    // The rule went on first: the narrowing write is refused, nothing changes.
    w.rule(GATED, true);
    let r = super::require_sso::while_rule_off(&w.state, GATED, None, vec![delete()]).await;
    assert_eq!(r.unwrap_err().1 .0["code"], "sso_required_on");
    assert!(w
        .f
        .store
        .get("settings", GATED, super::oidc_signin::OKTA_SK)
        .is_some());

    // Turning on against a setup that changed since it was checked fails.
    w.rule(GATED, false);
    let checked = super::oidc_signin::live_setup_of(&w.state, GATED)
        .await
        .unwrap()
        .unwrap();
    w.f.store.put(
        "settings",
        &[
            ("pk", json!(GATED)),
            ("sk", json!(super::oidc_signin::OKTA_SK)),
            ("issuer", json!(ISSUER)),
            ("enabled", json!(false)),
            ("updated_at", json!("2026-11-21T00:00:00Z")),
        ],
    );
    let parts = super::require_sso::setup_unchanged(
        &w.state,
        &super::require_sso::Live::Okta(checked.clone()),
    )
    .unwrap();
    assert!(w
        .state
        .dynamo
        .transact_write_items()
        .set_transact_items(Some(parts))
        .send()
        .await
        .is_err());
    // Unchanged: it holds.
    w.okta(GATED, true);
    let checked = super::oidc_signin::live_setup_of(&w.state, GATED)
        .await
        .unwrap()
        .unwrap();
    let parts = super::require_sso::setup_unchanged(
        &w.state,
        &super::require_sso::Live::Okta(checked.clone()),
    )
    .unwrap();
    assert!(w
        .state
        .dynamo
        .transact_write_items()
        .set_transact_items(Some(parts))
        .send()
        .await
        .is_ok());
    // A failing domain proof makes turning on fail too.
    w.f.store.put(
        "settings",
        &[
            ("pk", json!("OKTA_DOMAIN#example.com")),
            ("sk", json!("OKTA")),
            ("team_id", json!(GATED)),
            ("failing_at", json!("2026-11-21T00:00:00Z")),
        ],
    );
    let parts = super::require_sso::setup_unchanged(
        &w.state,
        &super::require_sso::Live::Okta(checked.clone()),
    )
    .unwrap();
    assert!(w
        .state
        .dynamo
        .transact_write_items()
        .set_transact_items(Some(parts))
        .send()
        .await
        .is_err());
}

/// Finding 2, through the handlers: with the rule on, turning Okta off,
/// removing it or releasing a domain is refused and changes nothing.
#[tokio::test]
async fn the_okta_setup_handlers_refuse_narrowing_while_the_rule_is_on() {
    let w = world().await;
    w.rule(GATED, true);
    w.okta(GATED, true);
    w.proven(GATED, "example.com");
    w.member(
        GATED,
        "USER#o",
        "o@example.com",
        "owner",
        &[("okta_issuer", json!(ISSUER)), ("okta_sub", json!("o"))],
    );
    let c = w.cookie(GATED, "USER#o", "o@example.com", "owner", &okta_proof("o"));
    for (m, path, body) in [
        (
            "PUT",
            "/api/sign-in/okta",
            Some(json!({ "enabled": false })),
        ),
        ("DELETE", "/api/sign-in/okta", None),
        ("DELETE", "/api/sign-in/okta/domains/example.com", None),
    ] {
        assert_eq!(
            w.call(m, path, &c, body).await.body["code"],
            "sso_required_on",
            "{m} {path}"
        );
    }
    assert!(w
        .f
        .store
        .get("settings", "OKTA_DOMAIN#example.com", "OKTA")
        .is_some());
    assert_eq!(
        w.f.store
            .get("settings", GATED, super::oidc_signin::OKTA_SK)
            .unwrap()["enabled"],
        true
    );
}

/// Finding 3 (self-hosted, run as one): while the organization requires
/// Okta and its Okta signs in the allowed domains, an admin can't drop one.
#[tokio::test]
async fn a_self_hosted_org_cant_drop_the_domains_its_okta_relies_on() {
    let _self_hosted = crate::routes::setup_checks::test_self_hosted::set(true);
    let w = world().await;
    let org = |on: bool, domains: &[&str]| {
        w.f.store.put(
            "settings",
            &[
                ("pk", json!("ORG")),
                ("sk", json!("META")),
                ("team_id", json!(GATED)),
                ("domains", json!(domains)),
                ("require_sso", json!(if on { "okta" } else { "" })),
                ("require_sso_since", json!(RULE_SINCE)),
                ("created_at", json!("2026-01-01T00:00:00Z")),
            ],
        )
    };
    org(true, &["example.com", "example.org"]);
    // The organization's Okta lists no domains: it signs in the org's.
    w.f.store.put(
        "settings",
        &[
            ("pk", json!(GATED)),
            ("sk", json!(super::oidc_signin::OKTA_SK)),
            ("issuer", json!(ISSUER)),
            ("client_id", json!("client")),
            ("domains", json!([])),
            ("enabled", json!(true)),
        ],
    );
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
            "PUT",
            "/api/sign-in",
            &c,
            Some(json!({ "domains": ["example.com"] })),
        )
        .await;
    assert_eq!(a.status, StatusCode::CONFLICT, "{}", a.body);
    assert_eq!(a.body["code"], "sso_required_on");
    assert_eq!(
        w.f.store.get("settings", "ORG", "META").unwrap()["domains"],
        json!(["example.com", "example.org"])
    );
    // Adding one is fine.
    let a = w
        .call(
            "PUT",
            "/api/sign-in",
            &c,
            Some(json!({ "domains": ["example.com", "example.org", "example.net"] })),
        )
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    // And a password sign-in to the organization is refused there too.
    password_user(&w, "adm@example.com", false);
    let a = w
        .call("POST", "/auth/login", "", Some(login("adm@example.com")))
        .await;
    assert_eq!(a.body["code"], "sso_required");
}

/// Finding 5: a non-member learns nothing about a team's rules from a switch.
#[tokio::test]
async fn a_switch_to_a_team_youre_not_on_is_a_bare_refusal() {
    let w = world().await;
    w.rule(GATED, true);
    w.member(OTHER, "USER#1", "ann@example.com", "member", &[]);
    let c = w.cookie(
        OTHER,
        "USER#1",
        "ann@example.com",
        "member",
        &SignInProof::new(true, "password"),
    );
    // Its rules can't even be read: still the same bare 403 (not asked).
    w.f.store.fail_reads("settings", GATED);
    let a = w
        .call(
            "POST",
            "/api/teams/switch",
            &c,
            Some(json!({ "team_id": GATED })),
        )
        .await;
    assert_eq!(a.status, StatusCode::FORBIDDEN);
    assert!(a.body.is_null(), "{}", a.body);
}

/// The suspicion about short-lived tokens: a stream token minted from a
/// non-Okta session ends with the session.
#[tokio::test]
async fn a_stream_token_from_a_non_okta_session_ends_with_the_rule() {
    let w = world().await;
    w.member(GATED, "USER#1", "ann@example.com", "member", &[]);
    let claims = jwt::validate_token(
        w.cookie(
            GATED,
            "USER#1",
            "ann@example.com",
            "member",
            &SignInProof::new(true, "google"),
        )
        .trim_start_matches("__Host-coderhelm_session="),
        &w.state.secrets.jwt_secret,
    )
    .unwrap();
    let minted = jwt::create_scoped_token(
        &claims,
        &SignInProof::new(true, "google"),
        jwt::BRAIN_AUDIENCE,
        &w.state.secrets.jwt_secret,
        300,
    )
    .unwrap();
    let back =
        jwt::validate_scoped_token(&minted, jwt::BRAIN_AUDIENCE, &w.state.secrets.jwt_secret)
            .unwrap();
    let proof = jwt::read_scoped_proof(&minted, jwt::BRAIN_AUDIENCE, &w.state.secrets.jwt_secret);
    assert_eq!(proof.via.as_deref(), Some("google"));
    w.rule(GATED, false);
    assert!(super::require_sso::check_scoped(&w.state, &back, &proof)
        .await
        .is_ok());
    w.rule(GATED, true);
    let r = super::require_sso::check_scoped(&w.state, &back, &proof)
        .await
        .unwrap_err();
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    // Through the brain stream itself.
    let a = {
        let req = Request::builder()
            .method("POST")
            .uri("/api/brain/stream")
            .header(jwt::STREAM_TOKEN_HEADER, &minted)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(json!({ "question": "hi" }).to_string()))
            .unwrap();
        w.app.clone().oneshot(req).await.unwrap()
    };
    assert_eq!(a.status(), StatusCode::UNAUTHORIZED);
}

/// The suspicion about unlinking: refused while the rule is on.
#[tokio::test]
async fn unlinking_okta_is_refused_while_the_rule_is_on() {
    let w = world().await;
    w.rule(GATED, true);
    w.okta(GATED, true);
    w.member(
        GATED,
        "USER#1",
        "ann@example.com",
        "member",
        &[("okta_issuer", json!(ISSUER)), ("okta_sub", json!("s1"))],
    );
    let c = w.cookie(
        GATED,
        "USER#1",
        "ann@example.com",
        "member",
        &okta_proof("s1"),
    );
    let a = w.call("DELETE", "/api/users/okta/link", &c, None).await;
    assert_eq!(a.body["code"], "sso_required_on");
    assert_eq!(
        w.f.store.get("users", GATED, "USER#1").unwrap()["okta_sub"],
        "s1"
    );
}

/// Setting up a second factor re-mints the session: a session from the
/// team's tenant stays one (its identity is kept), and nothing else is
/// added to it.
#[tokio::test]
async fn a_re_minted_session_keeps_its_tenant_identity_and_nothing_more() {
    let w = world().await;
    let tenant = SignInProof::new(false, "microsoft").with_entra(Some("t/o1".into()));
    let mut claims = jwt::validate_token(
        w.cookie(GATED, "USER#o", "o@example.com", "owner", &tenant)
            .trim_start_matches("__Host-coderhelm_session="),
        &w.state.secrets.jwt_secret,
    )
    .unwrap();
    claims = super::oidc_signin::mark_session(claims, &tenant);
    let cookie =
        super::sign_in::session_with_second_factor(&w.state, &claims, Some(&tenant)).unwrap();
    let token = cookie
        .trim_start_matches("__Host-coderhelm_session=")
        .split(';')
        .next()
        .unwrap();
    let proof = jwt::read_proof(token, &w.state.secrets.jwt_secret);
    assert_eq!(proof.entra.as_deref(), Some("t/o1"));
    assert_eq!(proof.via.as_deref(), Some("microsoft"));
    assert!(proof.mfa);
    let pw = SignInProof::new(false, "password");
    let cookie = super::sign_in::session_with_second_factor(&w.state, &claims, Some(&pw)).unwrap();
    let token = cookie
        .trim_start_matches("__Host-coderhelm_session=")
        .split(';')
        .next()
        .unwrap();
    assert_eq!(
        jwt::read_proof(token, &w.state.secrets.jwt_secret)
            .via
            .as_deref(),
        Some("password")
    );
}

/// Reads that fail refuse: the rule, the member's role, the sign-in check.
#[tokio::test]
async fn a_read_that_fails_refuses() {
    let w = world().await;
    w.member(GATED, "USER#1", "ann@example.com", "member", &[]);
    w.rule(GATED, false);
    let c = w.cookie(
        GATED,
        "USER#1",
        "ann@example.com",
        "member",
        &SignInProof::new(true, "google"),
    );
    assert_eq!(w.call("GET", STATUS, &c, None).await.status, StatusCode::OK);
    w.f.store.fail_reads("settings", GATED);
    let a = w.call("GET", STATUS, &c, None).await;
    assert_eq!(a.status, StatusCode::SERVICE_UNAVAILABLE);
    // A password sign-in whose team's rules can't be read gets no session.
    password_user(&w, "ann@example.com", false);
    w.member(
        GATED,
        "USER#sub-ann@example.com",
        "ann@example.com",
        "member",
        &[],
    );
    let a = w
        .call("POST", "/auth/login", "", Some(login("ann@example.com")))
        .await;
    assert_eq!(a.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(w.session_proof(&a).is_none());
    // A session whose member record can't be read.
    let w2 = world().await;
    w2.rule(GATED, true);
    let ok = w2.cookie(GATED, "USER#o", "o@example.com", "owner", &okta_proof("o"));
    w2.f.store.fail_reads("users", GATED);
    assert_eq!(
        w2.call("GET", STATUS, &ok, None).await.status,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

/// The stand-in refuses what it doesn't understand, so no test passes on a
/// fake laxer than DynamoDB.
#[tokio::test]
#[should_panic(expected = "doesn't support")]
async fn the_stand_in_fails_a_test_that_uses_what_it_doesnt_support() {
    let w = world().await;
    let _ = w
        .state
        .dynamo
        .put_item()
        .table_name("settings")
        .item("pk", aws_sdk_dynamodb::types::AttributeValue::S("x".into()))
        .item("sk", aws_sdk_dynamodb::types::AttributeValue::S("y".into()))
        .condition_expression("size(pk) > :n")
        .expression_attribute_values(":n", aws_sdk_dynamodb::types::AttributeValue::N("1".into()))
        .send()
        .await;
    let _ = w.state.dynamo.scan().table_name("settings").send().await;
}

#[tokio::test]
async fn the_stand_in_refuses_a_transaction_that_touches_one_item_twice() {
    use aws_sdk_dynamodb::types::{AttributeValue as A, Put, TransactWriteItem};
    let w = world().await;
    let put = || {
        TransactWriteItem::builder()
            .put(
                Put::builder()
                    .table_name("settings")
                    .item("pk", A::S("x".into()))
                    .item("sk", A::S("y".into()))
                    .build()
                    .unwrap(),
            )
            .build()
    };
    assert!(w
        .state
        .dynamo
        .transact_write_items()
        .set_transact_items(Some(vec![put(), put()]))
        .send()
        .await
        .is_err());
    assert!(w.f.store.get("settings", "x", "y").is_none());
}

// ── Third review ────────────────────────────────────────────────────

/// A self-hosted organization with its Okta on and an owner who linked it.
fn selfhost_org(w: &World, rule_on: bool) -> String {
    w.f.store.put(
        "settings",
        &[
            ("pk", json!("ORG")),
            ("sk", json!("META")),
            ("team_id", json!(GATED)),
            ("domains", json!(["example.com"])),
            ("require_sso", json!(if rule_on { "okta" } else { "" })),
            ("require_sso_since", json!(RULE_SINCE)),
            ("created_at", json!("2026-01-01T00:00:00Z")),
        ],
    );
    w.f.store.put(
        "settings",
        &[
            ("pk", json!(GATED)),
            ("sk", json!(super::oidc_signin::OKTA_SK)),
            ("issuer", json!(ISSUER)),
            ("client_id", json!("client")),
            ("client_secret_sealed", json!("sealed")),
            ("domains", json!(["example.com"])),
            ("enabled", json!(true)),
            ("tested_at", json!("2026-11-01T00:00:00Z")),
            ("updated_at", json!("2026-11-01T00:00:00Z")),
        ],
    );
    w.member(
        GATED,
        "USER#o",
        "o@example.com",
        "owner",
        &[("okta_issuer", json!(ISSUER)), ("okta_sub", json!("o"))],
    );
    w.cookie(GATED, "USER#o", "o@example.com", "owner", &okta_proof("o"))
}

fn audit_lines(w: &World, team: &str) -> Vec<String> {
    w.f.store
        .items("settings", team)
        .into_iter()
        .filter(|i| i["action"] == "sign_in_rules")
        .map(|i| i["detail"].as_str().unwrap_or("").to_string())
        .collect()
}

/// N4: turning the rule on is logged after its transaction lands; a
/// cancelled attempt is logged as cancelled, and nothing changed.
#[tokio::test]
async fn turning_the_rule_on_is_logged_by_its_outcome() {
    let _self_hosted = crate::routes::setup_checks::test_self_hosted::set(true);
    let w = world().await;
    let c = selfhost_org(&w, false);
    let on = json!({ "require_sso": "okta", "acknowledge_unlinked": true });
    w.f.store.cancel_transactions();
    let a = w.call("PUT", "/api/sign-in", &c, Some(on.clone())).await;
    assert_eq!(a.body["code"], "sso_changed", "{}", a.body);
    let lines = audit_lines(&w, GATED);
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0].starts_with("Cancelled, nothing changed"),
        "{lines:?}"
    );
    assert_eq!(
        w.f.store.get("settings", "ORG", "META").unwrap()["require_sso"],
        ""
    );

    let w = world().await;
    let c = selfhost_org(&w, false);
    let a = w.call("PUT", "/api/sign-in", &c, Some(on)).await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert_eq!(
        w.f.store.get("settings", "ORG", "META").unwrap()["require_sso"],
        "okta"
    );
    let lines = audit_lines(&w, GATED);
    assert_eq!(
        lines,
        vec!["Single sign-on with Okta required for everyone".to_string()]
    );
}

/// N3: while the rule is on, a new Okta app is saved only by the owner,
/// from their own Okta session, after their own Test sign-in of it.
#[tokio::test]
async fn a_new_okta_app_needs_the_owners_own_test_while_the_rule_is_on() {
    let _self_hosted = crate::routes::setup_checks::test_self_hosted::set(true);
    let w = world().await;
    let owner = selfhost_org(&w, true);
    let draft = |tested_by: &str, tested_sub: &str, tested_at: String| {
        w.f.store.put(
            "settings",
            &[
                ("pk", json!(GATED)),
                ("sk", json!(super::oidc_signin::OKTA_DRAFT_SK)),
                ("issuer", json!(ISSUER)),
                ("client_id", json!("new-client")),
                ("client_secret_sealed", json!("new-sealed")),
                ("domains", json!(["example.com"])),
                ("enabled", json!(false)),
                ("tested_at", json!(tested_at)),
                ("tested_by", json!(tested_by)),
                ("tested_sub", json!(tested_sub)),
                ("updated_at", json!("2026-11-20T00:00:00Z")),
            ],
        )
    };
    let now = || chrono::Utc::now().to_rfc3339();
    let save = json!({ "enabled": true, "issuer": ISSUER, "client_id": "new-client", "updated_at": "2026-11-20T00:00:00Z" });
    // Refused as a client change, or (a session from another way in) ended.
    let refused =
        |a: &Answer| a.body["code"] == "okta_client_change" || a.body["code"] == "sso_required";
    // Tested by someone else: refused, the live app unchanged.
    draft("USER#adm", "o", now());
    let a = w
        .call("POST", "/api/sign-in/okta/save", &owner, Some(save.clone()))
        .await;
    assert!(refused(&a), "{}", a.body);
    // The owner ran it, but signed in to Okta as another identity.
    draft("USER#o", "someone-else", now());
    let a = w
        .call("POST", "/api/sign-in/okta/save", &owner, Some(save.clone()))
        .await;
    assert!(refused(&a), "{}", a.body);
    // The owner's own identity, but the test is over an hour old.
    draft(
        "USER#o",
        "o",
        (chrono::Utc::now() - chrono::Duration::minutes(61)).to_rfc3339(),
    );
    let a = w
        .call("POST", "/api/sign-in/okta/save", &owner, Some(save.clone()))
        .await;
    assert!(refused(&a), "{}", a.body);
    // From a password session with a second factor: refused (it ends at
    // once, the rule being on).
    let bg = w.cookie(
        GATED,
        "USER#o",
        "o@example.com",
        "owner",
        &SignInProof::new(true, "password"),
    );
    draft("USER#o", "o", now());
    let a = w
        .call("POST", "/api/sign-in/okta/save", &bg, Some(save.clone()))
        .await;
    assert!(refused(&a), "{}", a.body);
    assert_eq!(
        w.f.store
            .get("settings", GATED, super::oidc_signin::OKTA_SK)
            .unwrap()["client_id"],
        "client"
    );
    // The owner, from Okta, after their own fresh test as their own identity.
    let a = w
        .call("POST", "/api/sign-in/okta/save", &owner, Some(save))
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert_eq!(
        w.f.store
            .get("settings", GATED, super::oidc_signin::OKTA_SK)
            .unwrap()["client_id"],
        "new-client"
    );
}

/// N1: a domain whose proof is failing accepts no new member through Okta,
/// while members who linked before keep signing in.
#[tokio::test]
async fn a_failing_domain_accepts_no_invite_but_linked_members_still_sign_in() {
    let w = world().await;
    w.rule(GATED, true);
    w.okta(GATED, true);
    w.proven(GATED, "example.com");
    w.f.store.put(
        "settings",
        &[
            ("pk", json!("OKTA_DOMAIN#example.com")),
            ("sk", json!("OKTA")),
            ("team_id", json!(GATED)),
            ("verified_at", json!(chrono::Utc::now().to_rfc3339())),
            ("checked_at", json!(chrono::Utc::now().to_rfc3339())),
            ("failing_at", json!(chrono::Utc::now().to_rfc3339())),
        ],
    );
    w.invite_to(GATED, "USER#invite_n", "new@example.com", "member");
    assert_eq!(
        w.okta_lands(GATED, "s-new", "new@example.com")
            .await
            .unwrap_err(),
        "sso_ask_admin"
    );
    assert_eq!(
        w.f.store.get("users", GATED, "USER#invite_n").unwrap()["status"],
        "invited"
    );
    w.member(
        GATED,
        "USER#linked",
        "ann@example.com",
        "member",
        &[("okta_issuer", json!(ISSUER)), ("okta_sub", json!("s-ann"))],
    );
    assert!(sets_session(
        &w.okta_lands(GATED, "s-ann", "ann@example.com")
            .await
            .unwrap()
    ));
}

/// N1: the page shows when a failing proof will be released.
#[tokio::test]
async fn the_page_shows_when_a_failing_proof_is_released() {
    let w = world().await;
    w.rule(GATED, true);
    w.okta(GATED, true);
    let since = "2026-11-20T00:00:00+00:00";
    w.f.store.put(
        "settings",
        &[
            ("pk", json!("OKTA_DOMAIN#example.com")),
            ("sk", json!("OKTA")),
            ("team_id", json!(GATED)),
            ("verified_at", json!(since)),
            ("failing_at", json!(since)),
        ],
    );
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
    let a = w.call("GET", "/api/sign-in/okta", &c, None).await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert_eq!(a.body["domains"][0]["failing_since"], since);
    assert_eq!(
        a.body["domains"][0]["release_after"],
        "2026-11-27T00:00:00+00:00"
    );
}

/// N2: releasing a draft's domains happens only while the rule is off,
/// checked in the same write.
#[tokio::test]
async fn a_discarded_drafts_domains_are_kept_while_the_rule_is_on() {
    let w = world().await;
    w.rule(GATED, true);
    w.okta(GATED, true);
    w.proven(GATED, "draft.example");
    w.f.store.put(
        "settings",
        &[
            ("pk", json!(GATED)),
            ("sk", json!(super::oidc_signin::OKTA_DRAFT_SK)),
            ("issuer", json!(ISSUER)),
            ("client_id", json!("client")),
            ("domains", json!(["example.com", "draft.example"])),
            ("enabled", json!(false)),
        ],
    );
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
    let a = w.call("DELETE", "/api/sign-in/okta/draft", &c, None).await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert!(w
        .f
        .store
        .get("settings", "OKTA_DOMAIN#draft.example", "OKTA")
        .is_some());
    // With the rule off it is released.
    w.rule(GATED, false);
    w.f.store.put(
        "settings",
        &[
            ("pk", json!(GATED)),
            ("sk", json!(super::oidc_signin::OKTA_DRAFT_SK)),
            ("issuer", json!(ISSUER)),
            ("client_id", json!("client")),
            ("domains", json!(["draft.example"])),
            ("enabled", json!(false)),
        ],
    );
    let a = w.call("DELETE", "/api/sign-in/okta/draft", &c, None).await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert!(w
        .f
        .store
        .get("settings", "OKTA_DOMAIN#draft.example", "OKTA")
        .is_none());
}

/// Info: a guarded write that fails for another reason than the rule says
/// so (here the domain belongs to another team).
#[tokio::test]
async fn a_guarded_write_refused_for_another_reason_says_so() {
    use aws_sdk_dynamodb::types::{AttributeValue as A, Delete, TransactWriteItem};
    let w = world().await;
    w.proven(OTHER, "example.com");
    let delete = TransactWriteItem::builder()
        .delete(
            Delete::builder()
                .table_name("settings")
                .key("pk", A::S("OKTA_DOMAIN#example.com".into()))
                .key("sk", A::S("OKTA".into()))
                .condition_expression("team_id = :t")
                .expression_attribute_values(":t", A::S(GATED.into()))
                .build()
                .unwrap(),
        )
        .build();
    let e = super::require_sso::while_rule_off(&w.state, GATED, None, vec![delete])
        .await
        .unwrap_err();
    assert_eq!(e.1 .0["code"], "changed");
    assert!(w
        .f
        .store
        .get("settings", "OKTA_DOMAIN#example.com", "OKTA")
        .is_some());
}

/// Info: the stand-in refuses an expression value no expression uses, as
/// DynamoDB does.
#[tokio::test]
#[should_panic(expected = "doesn't support")]
async fn the_stand_in_refuses_unused_expression_values() {
    let w = world().await;
    let _ = w
        .state
        .dynamo
        .get_item()
        .table_name("settings")
        .key("pk", aws_sdk_dynamodb::types::AttributeValue::S("x".into()))
        .key("sk", aws_sdk_dynamodb::types::AttributeValue::S("y".into()))
        .projection_expression("pk")
        .expression_attribute_names("#unused", "sk")
        .send()
        .await;
}

// ── Microsoft Entra: the team's own tenant (item 4) ─────────────────

const TID: &str = "11111111-2222-3333-4444-555555555555";
const OTHER_TID: &str = "99999999-2222-3333-4444-555555555555";

impl World {
    fn entra(&self, team: &str, enabled: bool) {
        self.f.store.put(
            "settings",
            &[
                ("pk", json!(team)),
                ("sk", json!(super::entra_sso::SETUP_SK)),
                ("tid", json!(TID)),
                ("domains", json!(["example.com"])),
                ("enabled", json!(enabled)),
                ("updated_at", json!("2026-11-01T00:00:00Z")),
            ],
        );
    }

    fn entra_setup(&self, team: &str) -> super::entra_sso::EntraSetup {
        super::entra_sso::EntraSetup {
            team_id: team.into(),
            tid: TID.into(),
            domains: vec!["example.com".into()],
            enabled: true,
            updated_at: Some("2026-11-01T00:00:00Z".into()),
            ..Default::default()
        }
    }

    /// A sign-in through the team's tenant whose id_token was checked.
    async fn entra_lands(
        &self,
        team: &str,
        tid: &str,
        oid: &str,
        email: &str,
        claim: Option<&str>,
    ) -> Result<axum::response::Response, String> {
        let claims = json!({
            "iss": format!("https://login.microsoftonline.com/{tid}/v2.0"),
            "tid": tid, "oid": oid, "sub": "pairwise", "email": email,
            "xms_edov": true, "name": "Someone",
        });
        super::entra_sso::entra_land(
            &self.state,
            "127.0.0.1",
            &self.entra_setup(team),
            &claims,
            claim,
        )
        .await
        .map_err(|f| f.reason.code().to_string())
    }
}

fn entra_proof(tid: &str, oid: &str) -> SignInProof {
    SignInProof::new(false, "microsoft").with_entra(Some(super::entra_sso::entra_id(tid, oid)))
}

fn session_of_response(w: &World, r: &axum::response::Response) -> Option<SignInProof> {
    r.headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .find_map(|c| {
            let c = c.to_str().ok()?;
            let t = c
                .strip_prefix("__Host-coderhelm_session=")?
                .split(';')
                .next()?;
            (!t.is_empty()).then(|| jwt::read_proof(t, &w.state.secrets.jwt_secret))
        })
}

/// Only a session from the team's own tenant, linked to the member, stands
/// while the team requires single sign-on with Microsoft: never a plain
/// Microsoft sign-in, another tenant, personal accounts, Okta, or a session
/// whose link or setup changed.
#[tokio::test]
async fn with_microsoft_required_only_the_teams_tenant_linked_to_the_member_stands() {
    let w = world().await;
    w.rule_with(GATED, Some("microsoft"));
    w.entra(GATED, true);
    w.okta(GATED, true);
    w.member(
        GATED,
        "USER#1",
        "ann@example.com",
        "member",
        &[
            ("ms_tid", json!(TID)),
            ("ms_oid", json!("o1")),
            ("okta_issuer", json!(ISSUER)),
            ("okta_sub", json!("s1")),
        ],
    );
    let ok = w.cookie(
        GATED,
        "USER#1",
        "ann@example.com",
        "member",
        &entra_proof(TID, "o1"),
    );
    assert_eq!(
        w.call("GET", STATUS, &ok, None).await.status,
        StatusCode::OK
    );
    for proof in [
        SignInProof::new(true, "microsoft"),
        entra_proof(OTHER_TID, "o1"),
        entra_proof(super::entra_sso::CONSUMER_TENANT, "o1"),
        entra_proof(TID, "someone-else"),
        okta_proof("s1"),
        SignInProof::new(true, "password"),
        SignInProof::new(true, "google"),
    ] {
        let c = w.cookie(GATED, "USER#1", "ann@example.com", "member", &proof);
        let a = w.call("GET", STATUS, &c, None).await;
        assert_eq!(a.status, StatusCode::UNAUTHORIZED, "{proof:?}");
        assert!(a.ends_session(), "{proof:?}");
    }
    // Turning the tenant off, or a new link, ends the session.
    w.entra(GATED, false);
    assert_eq!(
        w.call("GET", STATUS, &ok, None).await.status,
        StatusCode::UNAUTHORIZED
    );
    w.entra(GATED, true);
    w.member(
        GATED,
        "USER#1",
        "ann@example.com",
        "member",
        &[("ms_tid", json!(TID)), ("ms_oid", json!("o2"))],
    );
    assert_eq!(
        w.call("GET", STATUS, &ok, None).await.status,
        StatusCode::UNAUTHORIZED
    );
    // With the rule off, a tenant session still needs its link.
    w.rule_with(GATED, None);
    assert_eq!(
        w.call("GET", STATUS, &ok, None).await.status,
        StatusCode::UNAUTHORIZED
    );
}

/// A sign-in through the team's tenant lands on the member who linked
/// exactly that tenant and object id, never by email; the session is held
/// to the team and is never a platform admin.
#[tokio::test]
async fn a_tenant_sign_in_lands_only_on_the_linked_member() {
    let w = world().await;
    w.rule_with(GATED, Some("microsoft"));
    w.entra(GATED, true);
    w.member(
        GATED,
        "USER#1",
        "ann@example.com",
        "member",
        &[("ms_tid", json!(TID)), ("ms_oid", json!("o1"))],
    );
    w.member(GATED, "USER#2", "bo@example.com", "member", &[]);
    let r = w
        .entra_lands(GATED, TID, "o1", "someone-else@example.com", None)
        .await
        .unwrap();
    let proof = session_of_response(&w, &r).expect("a session");
    assert_eq!(proof.entra.as_deref(), Some(format!("{TID}/o1").as_str()));
    assert_eq!(proof.via.as_deref(), Some("microsoft"));
    assert!(super::oidc_signin::is_held(&proof, false));
    // Bo never linked: Microsoft's email matching his record links nothing.
    assert_eq!(
        w.entra_lands(GATED, TID, "o9", "bo@example.com", None)
            .await
            .unwrap_err(),
        "sso_ask_admin"
    );
    assert!(w.f.store.get("users", GATED, "USER#2").unwrap()["ms_oid"].is_null());
    // Another tenant's token never reaches the team.
    assert_eq!(
        w.entra_lands(GATED, OTHER_TID, "o1", "ann@example.com", None)
            .await
            .unwrap_err(),
        "not_on_team"
    );
    // With the rule off, an unlinked account is told to link first.
    w.rule_with(GATED, None);
    assert_eq!(
        w.entra_lands(GATED, TID, "o9", "bo@example.com", None)
            .await
            .unwrap_err(),
        "sso_not_linked"
    );
}

/// An invite is accepted through the tenant only for exactly Microsoft's
/// verified email at a domain the team proved, in one transaction.
#[tokio::test]
async fn a_tenant_sign_in_accepts_only_the_exact_invite_at_a_proven_domain() {
    let w = world().await;
    w.rule_with(GATED, Some("microsoft"));
    w.entra(GATED, true);
    w.invite(GATED, "new@example.com");
    // Not proven yet: refused.
    assert_eq!(
        w.entra_lands(GATED, TID, "n1", "new@example.com", None)
            .await
            .unwrap_err(),
        "sso_ask_admin"
    );
    w.proven(GATED, "example.com");
    assert_eq!(
        w.entra_lands(GATED, TID, "n1", "new+x@example.com", None)
            .await
            .unwrap_err(),
        "sso_ask_admin"
    );
    let r = w
        .entra_lands(GATED, TID, "n1", "new@example.com", None)
        .await
        .unwrap();
    assert!(sets_session(&r));
    let id = super::entra_sso::ms_user_id(TID, "n1");
    let m = w.f.store.get("users", GATED, &id).unwrap();
    assert_eq!(
        (m["ms_tid"].as_str(), m["ms_oid"].as_str()),
        (Some(TID), Some("n1"))
    );
    assert!(w.f.store.get("users", GATED, "USER#invite_1").is_none());
    assert!(w
        .f
        .store
        .get(
            "settings",
            &format!("ENTRA_LINK#{GATED}"),
            &format!("{TID}#n1")
        )
        .is_some());
}

/// Turning the rule on with Microsoft needs the tenant on and the owner's
/// own linked tenant session; switching providers needs it off first.
#[tokio::test]
async fn turning_microsoft_on_needs_the_owners_own_tenant_session() {
    let _self_hosted = crate::routes::setup_checks::test_self_hosted::set(true);
    let w = world().await;
    let okta_cookie = selfhost_org(&w, false);
    w.entra(GATED, true);
    w.member(
        GATED,
        "USER#o",
        "o@example.com",
        "owner",
        &[
            ("ms_tid", json!(TID)),
            ("ms_oid", json!("oo")),
            ("okta_issuer", json!(ISSUER)),
            ("okta_sub", json!("o")),
        ],
    );
    let on = json!({ "require_sso": "microsoft", "acknowledge_unlinked": true });
    let a = w
        .call("PUT", "/api/sign-in", &okta_cookie, Some(on.clone()))
        .await;
    assert_eq!(a.body["code"], "sign_in_with_provider_first", "{}", a.body);
    let ms = w.cookie(
        GATED,
        "USER#o",
        "o@example.com",
        "owner",
        &entra_proof(TID, "oo"),
    );
    let a = w.call("PUT", "/api/sign-in", &ms, Some(on.clone())).await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert_eq!(a.body["require_sso"], "microsoft");
    // The owner's own session stands; an Okta one ends.
    assert_eq!(
        w.call("GET", STATUS, &ms, None).await.status,
        StatusCode::OK
    );
    assert_eq!(
        w.call("GET", STATUS, &okta_cookie, None).await.status,
        StatusCode::UNAUTHORIZED
    );
    // Switching straight to Okta is refused.
    let a = w
        .call(
            "PUT",
            "/api/sign-in",
            &ms,
            Some(json!({ "require_sso": "okta" })),
        )
        .await;
    assert_eq!(a.body["code"], "sso_required_on");
}

/// While the rule names Microsoft, its setup and links are locked; Okta's
/// aren't (the lock follows the provider in use).
#[tokio::test]
async fn the_microsoft_setup_is_locked_while_the_rule_needs_it() {
    let _self_hosted = crate::routes::setup_checks::test_self_hosted::set(true);
    let w = world().await;
    selfhost_org(&w, false);
    w.rule_with("", None);
    w.f.store.put(
        "settings",
        &[
            ("pk", json!("ORG")),
            ("sk", json!("META")),
            ("team_id", json!(GATED)),
            ("require_sso", json!("microsoft")),
            ("require_sso_since", json!(RULE_SINCE)),
        ],
    );
    w.entra(GATED, true);
    w.member(
        GATED,
        "USER#o",
        "o@example.com",
        "owner",
        &[("ms_tid", json!(TID)), ("ms_oid", json!("oo"))],
    );
    let c = w.cookie(
        GATED,
        "USER#o",
        "o@example.com",
        "owner",
        &entra_proof(TID, "oo"),
    );
    for (m, path, body) in [
        (
            "PUT",
            "/api/sign-in/microsoft",
            Some(json!({ "tenant_id": OTHER_TID })),
        ),
        (
            "PUT",
            "/api/sign-in/microsoft/enabled",
            Some(json!({ "enabled": false })),
        ),
        ("DELETE", "/api/sign-in/microsoft", None),
        ("DELETE", "/api/users/microsoft/link", None),
    ] {
        let a = w.call(m, path, &c, body).await;
        assert_eq!(a.body["code"], "sso_required_on", "{m} {path}: {}", a.body);
    }
    let s =
        w.f.store
            .get("settings", GATED, super::entra_sso::SETUP_SK)
            .unwrap();
    assert_eq!(
        (s["tid"].as_str(), s["enabled"].as_bool()),
        (Some(TID), Some(true))
    );
    // Okta's setup isn't the one in use: turning it off goes through.
    let a = w
        .call(
            "PUT",
            "/api/sign-in/okta",
            &c,
            Some(json!({ "enabled": false })),
        )
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
}

/// The tenant turns on only after the owner's own Test sign-in within the
/// hour; a new tenant is saved off and untested, and drops the links.
#[tokio::test]
async fn the_tenant_turns_on_only_after_the_owners_own_fresh_test() {
    let _self_hosted = crate::routes::setup_checks::test_self_hosted::set(true);
    let w = world().await;
    let c = selfhost_org(&w, false);
    let a = w
        .call(
            "PUT",
            "/api/sign-in/microsoft",
            &c,
            Some(json!({ "tenant_id": TID })),
        )
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert_eq!(a.body["setup"]["enabled"], false);
    let on = Some(json!({ "enabled": true }));
    let a = w
        .call("PUT", "/api/sign-in/microsoft/enabled", &c, on.clone())
        .await;
    assert_eq!(a.body["code"], "test_first");
    let updated =
        w.f.store
            .get("settings", GATED, super::entra_sso::SETUP_SK)
            .unwrap()["updated_at"]
            .clone();
    let tested = |by: &str, at: String| {
        w.f.store.put(
            "settings",
            &[
                ("pk", json!(GATED)),
                ("sk", json!(super::entra_sso::SETUP_SK)),
                ("tid", json!(TID)),
                ("domains", json!([])),
                ("enabled", json!(false)),
                ("updated_at", updated.clone()),
                ("tested_by", json!(by)),
                ("tested_oid", json!("oo")),
                ("tested_at", json!(at)),
            ],
        )
    };
    tested("USER#someone-else", chrono::Utc::now().to_rfc3339());
    let a = w
        .call("PUT", "/api/sign-in/microsoft/enabled", &c, on.clone())
        .await;
    assert_eq!(a.body["code"], "test_first");
    tested(
        "USER#o",
        (chrono::Utc::now() - chrono::Duration::minutes(61)).to_rfc3339(),
    );
    let a = w
        .call("PUT", "/api/sign-in/microsoft/enabled", &c, on.clone())
        .await;
    assert_eq!(a.body["code"], "test_first");
    tested("USER#o", chrono::Utc::now().to_rfc3339());
    let a = w
        .call("PUT", "/api/sign-in/microsoft/enabled", &c, on.clone())
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert_eq!(a.body["setup"]["enabled"], true);
    // An admin can't change it at all.
    let admin = w.cookie(GATED, "USER#a", "a@example.com", "admin", &okta_proof("a"));
    w.member(
        GATED,
        "USER#a",
        "a@example.com",
        "admin",
        &[("okta_issuer", json!(ISSUER)), ("okta_sub", json!("a"))],
    );
    let a = w
        .call(
            "PUT",
            "/api/sign-in/microsoft",
            &admin,
            Some(json!({ "tenant_id": OTHER_TID })),
        )
        .await;
    assert_eq!(a.status, StatusCode::FORBIDDEN, "{}", a.body);
    // A new tenant drops every link.
    w.member(
        GATED,
        "USER#m",
        "m@example.com",
        "member",
        &[("ms_tid", json!(TID)), ("ms_oid", json!("om"))],
    );
    let a = w
        .call(
            "PUT",
            "/api/sign-in/microsoft",
            &c,
            Some(json!({ "tenant_id": OTHER_TID })),
        )
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert!(w.f.store.get("users", GATED, "USER#m").unwrap()["ms_oid"].is_null());
    // Personal accounts' tenant is never a team's.
    let a = w
        .call(
            "PUT",
            "/api/sign-in/microsoft",
            &c,
            Some(json!({ "tenant_id": super::entra_sso::CONSUMER_TENANT })),
        )
        .await;
    assert_eq!(a.status, StatusCode::BAD_REQUEST);
}

/// `/auth/microsoft?team_id=` signs in through the team's tenant only.
#[tokio::test]
async fn the_teams_microsoft_sign_in_goes_to_its_tenant_only() {
    let w = world().await;
    w.entra(GATED, true);
    let a = w
        .call(
            "GET",
            &format!("/auth/microsoft?team_id={}", urlencoding::encode(GATED)),
            "",
            None,
        )
        .await;
    assert_eq!(a.status, StatusCode::SEE_OTHER);
    let to = a.location.clone().unwrap_or_default();
    assert!(
        to.starts_with(&format!(
            "https://login.microsoftonline.com/{TID}/oauth2/v2.0/authorize"
        )),
        "{to}"
    );
    // A team without a tenant on gets the sign-in screen, not Microsoft.
    let a = w
        .call(
            "GET",
            &format!("/auth/microsoft?team_id={}", urlencoding::encode(OTHER)),
            "",
            None,
        )
        .await;
    assert_eq!(a.status, StatusCode::FOUND);
}

// ── Claim links (item 3) ────────────────────────────────────────────

/// The admin's link: (url, the flow value its start carries).
async fn issue_claim(w: &World, admin: &str, user: &str) -> Answer {
    w.call(
        "POST",
        "/api/sign-in/claims",
        admin,
        Some(json!({ "user_id": user })),
    )
    .await
}

fn token_of(url: &str) -> String {
    url.split("token=").nth(1).unwrap().to_string()
}

/// The flow value a claim link's start puts in the signed cookie.
fn carried(token: &str) -> String {
    let (_, member, secret) = super::sso_claim::parse_token(token).unwrap();
    super::sso_claim::flow_value(&member, &super::sso_claim::token_hash(&secret))
}

/// An admin gives one unlinked member a one-time link; the provider's
/// sign-in uses it once, for exactly that member, in one transaction, and
/// a plain unlinked sign-in still has to ask an admin.
#[tokio::test]
async fn a_claim_link_links_exactly_its_member_once() {
    let w = world().await;
    w.rule_with(GATED, Some("microsoft"));
    w.entra(GATED, true);
    w.proven(GATED, "example.com");
    w.member(
        GATED,
        "USER#adm",
        "adm@example.com",
        "admin",
        &[("ms_tid", json!(TID)), ("ms_oid", json!("oa"))],
    );
    w.member(GATED, "USER#own", "own@example.com", "owner", &[]);
    w.member(GATED, "USER#pat", "Pat@Example.com", "member", &[]);
    let admin = w.cookie(
        GATED,
        "USER#adm",
        "adm@example.com",
        "admin",
        &entra_proof(TID, "oa"),
    );
    // A plain unlinked sign-in: ask an admin, nothing linked by email.
    assert_eq!(
        w.entra_lands(GATED, TID, "op", "pat@example.com", None)
            .await
            .unwrap_err(),
        "sso_ask_admin"
    );
    // Never for the owner; never for someone already linked.
    assert_eq!(issue_claim(&w, &admin, "own").await.body["code"], "owner");
    assert_eq!(
        issue_claim(&w, &admin, "adm").await.body["code"],
        "already_linked"
    );
    let a = issue_claim(&w, &admin, "pat").await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    let url = a.body["url"].as_str().unwrap().to_string();
    let token = token_of(&url);
    let row =
        w.f.store
            .get("settings", &format!("SSO_CLAIM#{GATED}"), "USER#pat")
            .unwrap();
    // Only the secret's hash is stored, with the member's email and role.
    assert!(!row
        .to_string()
        .contains(&super::sso_claim::parse_token(&token).unwrap().2));
    assert_eq!(
        (
            row["email"].as_str(),
            row["role"].as_str(),
            row["used"].as_bool()
        ),
        (Some("Pat@Example.com"), Some("member"), Some(false))
    );
    assert!(audit_lines(&w, GATED)
        .iter()
        .any(|l| l.contains("claim link issued for Pat@Example.com")));
    // The public start goes to the team's tenant, carrying the claim.
    let a = w
        .call("GET", &format!("/auth/sso/claim?token={token}"), "", None)
        .await;
    assert_eq!(a.status, StatusCode::SEE_OTHER, "{:?}", a.location);
    assert!(a
        .location
        .as_deref()
        .unwrap()
        .starts_with(&format!("https://login.microsoftonline.com/{TID}/")));
    let flow = a
        .cookies
        .iter()
        .find(|c| c.starts_with("__Host-oauth_flow_ms="))
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let f = super::oidc_signin::read_flow(
        super::oauth_signin::Provider::Microsoft,
        &flow,
        &w.state.secrets.jwt_secret,
    )
    .unwrap();
    assert_eq!(f.claim, carried(&token));
    assert_eq!(f.team, GATED);
    // Another email (even the same person's +tag) can't use it.
    assert_eq!(
        w.entra_lands(GATED, TID, "op", "pat+x@example.com", Some(&f.claim))
            .await
            .unwrap_err(),
        "sso_ask_admin"
    );
    // Exactly the member's address (trimmed, lowercase): linked and in.
    let r = w
        .entra_lands(GATED, TID, "op", "pat@example.com", Some(&f.claim))
        .await
        .unwrap();
    assert!(sets_session(&r));
    let m = w.f.store.get("users", GATED, "USER#pat").unwrap();
    assert_eq!(
        (m["ms_tid"].as_str(), m["ms_oid"].as_str()),
        (Some(TID), Some("op"))
    );
    let row =
        w.f.store
            .get("settings", &format!("SSO_CLAIM#{GATED}"), "USER#pat")
            .unwrap();
    assert_eq!(row["used"], true);
    assert!(audit_lines(&w, GATED)
        .iter()
        .any(|l| l.contains("claim link used")));
    // Once only: used, and its link can't be started or revoked again.
    w.member(GATED, "USER#pat", "Pat@Example.com", "member", &[]);
    assert_eq!(
        w.entra_lands(GATED, TID, "op2", "pat@example.com", Some(&f.claim))
            .await
            .unwrap_err(),
        "sso_ask_admin"
    );
    let a = w
        .call("GET", &format!("/auth/sso/claim?token={token}"), "", None)
        .await;
    assert!(a
        .location
        .unwrap()
        .contains("signin_error=sso_claim_invalid&reason=used"));
    let a = w
        .call("DELETE", "/api/sign-in/claims/pat", &admin, None)
        .await;
    assert_eq!(a.body["code"], "no_unused_claim");
}

/// Every condition the transaction holds: unexpired, the member unchanged
/// (role, email, still unlinked), the identity linked to nobody, and on
/// hosted a proven, non-failing domain. A revoked link is gone.
#[tokio::test]
async fn a_claim_is_refused_when_anything_changed() {
    let w = world().await;
    w.rule_with(GATED, Some("microsoft"));
    w.entra(GATED, true);
    w.proven(GATED, "example.com");
    w.member(
        GATED,
        "USER#adm",
        "adm@example.com",
        "admin",
        &[("ms_tid", json!(TID)), ("ms_oid", json!("oa"))],
    );
    let admin = w.cookie(
        GATED,
        "USER#adm",
        "adm@example.com",
        "admin",
        &entra_proof(TID, "oa"),
    );
    let fresh = |w: &World| w.member(GATED, "USER#pat", "pat@example.com", "member", &[]);
    // Expired.
    fresh(&w);
    let c = new_claim(&w, &admin).await;
    let key = format!("SSO_CLAIM#{GATED}");
    let mut row = w.f.store.get("settings", &key, "USER#pat").unwrap();
    row["expires_at"] = json!(1);
    w.f.store.put(
        "settings",
        &row.as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect::<Vec<_>>(),
    );
    assert_eq!(pat_lands(&w, &c, "p1").await.unwrap_err(), "sso_ask_admin");
    // The member's role changed since.
    fresh(&w);
    let c = new_claim(&w, &admin).await;
    w.member(GATED, "USER#pat", "pat@example.com", "admin", &[]);
    assert_eq!(pat_lands(&w, &c, "p1").await.unwrap_err(), "sso_ask_admin");
    // The identity is already another member's.
    fresh(&w);
    let c = new_claim(&w, &admin).await;
    w.f.store.put(
        "settings",
        &[
            ("pk", json!(format!("ENTRA_LINK#{GATED}"))),
            ("sk", json!(format!("{TID}#taken"))),
            ("member", json!("USER#adm")),
        ],
    );
    assert_eq!(
        pat_lands(&w, &c, "taken").await.unwrap_err(),
        "sso_ask_admin"
    );
    assert!(w.f.store.get("users", GATED, "USER#pat").unwrap()["ms_oid"].is_null());
    // The domain's proof is failing: no claim is accepted at it.
    let mut d =
        w.f.store
            .get("settings", "OKTA_DOMAIN#example.com", "OKTA")
            .unwrap();
    d["failing_at"] = json!(chrono::Utc::now().to_rfc3339());
    w.f.store.put(
        "settings",
        &d.as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect::<Vec<_>>(),
    );
    assert_eq!(pat_lands(&w, &c, "p2").await.unwrap_err(), "sso_ask_admin");
    w.proven(GATED, "example.com");
    // Revoked: nothing to use.
    let a = w
        .call("DELETE", "/api/sign-in/claims/pat", &admin, None)
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert!(w.f.store.get("settings", &key, "USER#pat").is_none());
    assert_eq!(pat_lands(&w, &c, "p3").await.unwrap_err(), "sso_ask_admin");
    // Only admins issue them, and only while the rule is on.
    w.member(
        GATED,
        "USER#mem",
        "mem@example.com",
        "member",
        &[("ms_tid", json!(TID)), ("ms_oid", json!("om"))],
    );
    let member = w.cookie(
        GATED,
        "USER#mem",
        "mem@example.com",
        "member",
        &entra_proof(TID, "om"),
    );
    assert_eq!(
        issue_claim(&w, &member, "pat").await.status,
        StatusCode::FORBIDDEN
    );
    w.rule_with(GATED, None);
    let admin2 = w.cookie(
        GATED,
        "USER#adm",
        "adm@example.com",
        "admin",
        &entra_proof(TID, "oa"),
    );
    assert_eq!(
        issue_claim(&w, &admin2, "pat").await.body["code"],
        "sso_not_required"
    );
}

async fn new_claim(w: &World, admin: &str) -> String {
    let a = issue_claim(w, admin, "pat").await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    carried(&token_of(a.body["url"].as_str().unwrap()))
}

async fn pat_lands(w: &World, c: &str, oid: &str) -> Result<axum::response::Response, String> {
    w.entra_lands(GATED, TID, oid, "pat@example.com", Some(c))
        .await
}

/// An Okta claim works the same way through okta_land.
#[tokio::test]
async fn an_okta_claim_links_the_okta_identity() {
    let w = world().await;
    w.rule(GATED, true);
    w.okta(GATED, true);
    w.proven(GATED, "example.com");
    w.member(
        GATED,
        "USER#adm",
        "adm@example.com",
        "admin",
        &[("okta_issuer", json!(ISSUER)), ("okta_sub", json!("a"))],
    );
    w.member(GATED, "USER#pat", "pat@example.com", "member", &[]);
    let admin = w.cookie(
        GATED,
        "USER#adm",
        "adm@example.com",
        "admin",
        &okta_proof("a"),
    );
    let a = issue_claim(&w, &admin, "pat").await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    let c = carried(&token_of(a.body["url"].as_str().unwrap()));
    let claims =
        json!({ "iss": ISSUER, "sub": "p", "email": "pat@example.com", "email_verified": true });
    let r = super::oidc_signin::okta_land(
        &w.state,
        "127.0.0.1",
        &w.live_setup(GATED),
        &claims,
        Some(&c),
    )
    .await
    .unwrap();
    assert!(sets_session(&r));
    let m = w.f.store.get("users", GATED, "USER#pat").unwrap();
    assert_eq!(
        (m["okta_issuer"].as_str(), m["okta_sub"].as_str()),
        (Some(ISSUER), Some("p"))
    );
    // Unverified email: no claim.
    w.member(GATED, "USER#q", "q@example.com", "member", &[]);
    let a = issue_claim(&w, &admin, "q").await;
    let c = carried(&token_of(a.body["url"].as_str().unwrap()));
    let claims =
        json!({ "iss": ISSUER, "sub": "q", "email": "q@example.com", "email_verified": false });
    assert!(super::oidc_signin::okta_land(
        &w.state,
        "127.0.0.1",
        &w.live_setup(GATED),
        &claims,
        Some(&c)
    )
    .await
    .is_err());
    assert!(w.f.store.get("users", GATED, "USER#q").unwrap()["okta_sub"].is_null());
}

// ── Recovery (item 2) ───────────────────────────────────────────────

/// Hosted: CoderHelm support turns the rule off only with a verification
/// note, from its own session with a second factor; the team's log has it,
/// and no session is made for anyone.
#[tokio::test]
async fn support_recovery_turns_only_the_rule_off_and_makes_no_session() {
    let w = world().await;
    w.rule(GATED, true);
    w.member(GATED, "USER#own", "own@example.com", "owner", &[]);
    w.member(
        "TEAM#coderhelm",
        "USER#staff",
        "admin@coderhelm.com",
        "owner",
        &[],
    );
    let path = format!(
        "/api/admin/teams/{}/sso-recovery",
        urlencoding::encode(GATED)
    );
    let note = json!({ "verification": "Video call with the owner, ID checked against the signed contract." });
    let staff = |mfa: bool| {
        w.cookie(
            "TEAM#coderhelm",
            "USER#staff",
            "admin@coderhelm.com",
            "owner",
            &SignInProof::new(mfa, "google"),
        )
    };
    // Not a platform admin: refused.
    let owner = w.cookie(
        GATED,
        "USER#own",
        "own@example.com",
        "owner",
        &okta_proof("x"),
    );
    assert_eq!(
        w.call("POST", &path, &owner, Some(note.clone()))
            .await
            .status,
        StatusCode::UNAUTHORIZED
    );
    // No second factor, or no real note: refused.
    let a = w
        .call("POST", &path, &staff(false), Some(note.clone()))
        .await;
    assert_eq!(a.body["code"], "second_factor", "{}", a.body);
    let a = w
        .call(
            "POST",
            &path,
            &staff(true),
            Some(json!({ "verification": "ok" })),
        )
        .await;
    assert_eq!(a.body["code"], "verification_needed");
    assert_eq!(
        super::sign_in::team_rule(&w.state, GATED)
            .await
            .unwrap()
            .require_sso,
        Some(super::require_sso::SsoProvider::Okta)
    );
    // Done right: the rule is off, logged with the note, no cookie set.
    let a = w.call("POST", &path, &staff(true), Some(note)).await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert!(a
        .cookies
        .iter()
        .all(|c| !c.starts_with("__Host-coderhelm_session=ey")));
    assert_eq!(
        super::sign_in::team_rule(&w.state, GATED)
            .await
            .unwrap()
            .require_sso,
        None
    );
    let lines = audit_lines(&w, GATED);
    assert!(
        lines
            .iter()
            .any(|l| l.contains("turned off by CoderHelm support")
                && l.contains("Video call with the owner")),
        "{lines:?}"
    );
    // Not on again: nothing to recover.
    let a = w
        .call(
            "POST",
            &path,
            &staff(true),
            Some(json!({ "verification": "Video call with the owner, ID checked again." })),
        )
        .await;
    assert_eq!(a.body["code"], "sso_not_required");
}

/// Self-hosted: the recovery script's notice is taken by the first request
/// that reads the rule, exactly once.
#[tokio::test]
async fn the_recovery_scripts_notice_is_taken_once() {
    let _self_hosted = crate::routes::setup_checks::test_self_hosted::set(true);
    let w = world().await;
    let c = selfhost_org(&w, false);
    let mut org = w.f.store.get("settings", "ORG", "META").unwrap();
    org.as_object_mut().unwrap().remove("require_sso");
    org["sso_recovery_notice"] = json!("okta|Ops person|2026-11-21T12:00:00.000Z");
    w.f.store.put(
        "settings",
        &org.as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect::<Vec<_>>(),
    );
    // Any request that reads the rule (here a member's own page).
    assert_eq!(w.call("GET", STATUS, &c, None).await.status, StatusCode::OK);
    assert!(w.f.store.get("settings", "ORG", "META").unwrap()["sso_recovery_notice"].is_null());
    // Taken once: a second request finds nothing to send.
    assert_eq!(w.call("GET", STATUS, &c, None).await.status, StatusCode::OK);
    assert_eq!(
        super::sign_in::team_rule(&w.state, "")
            .await
            .unwrap()
            .require_sso,
        None
    );
}
