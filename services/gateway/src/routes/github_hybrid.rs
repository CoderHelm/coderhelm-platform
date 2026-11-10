//! The install side of GitHub sign-in through CoderHelm's GitHub App, for a
//! hybrid install with no GitHub OAuth client of its own
//! (common::hybrid::github_sign_in has the whole flow).
//!
//! The hosted side only carries the trip to GitHub and exchanges the code
//! the install hands it with the verifier only the install holds. This side
//! checks its own sealed state on the way back, reads who the person is from
//! GitHub with their token, and signs them in by its own rules
//! ([`super::auth::github_sign_in_with_token`]): team members and invites,
//! no sign-up, two-factor. Then the hosted side revokes the token at GitHub.

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use common::hybrid::{client, github_sign_in as gs};
use serde::Deserialize;
use serde_json::{json, Value};

use super::auth::{self, TokenFrom};
use super::oauth_signin::{self, Provider, Reason, SignInFailure};
use crate::AppState;

const P: Provider = Provider::Github;

/// GitHub sign-in through CoderHelm works here: a self-hosted install
/// registered with CoderHelm, with hybrid not turned off.
pub fn available() -> bool {
    super::setup_checks::self_hosted()
        && !common::hybrid::turned_off()
        && client::credentials().is_some()
}

/// The cookie key: this install's own session key, which the hosted side
/// never has.
fn cookie_key(state: &AppState) -> &str {
    &state.secrets.jwt_secret
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn app_url(state: &AppState) -> &str {
    common::public_urls::app_url(&state.config.stage)
}

/// The install's callback address for this flow.
pub fn callback_url(api_url: &str) -> String {
    format!("{}{}", api_url.trim_end_matches('/'), gs::CALLBACK_PATH)
}

fn hosted_refusal(e: &client::CallError) -> Reason {
    match e.code.as_str() {
        "sign_in_unavailable"
        | "redirect_not_allowed"
        | "not_verified"
        | "broker_revoked"
        | "unknown_install" => Reason::NotSetUp,
        _ => Reason::Server,
    }
}

/// Why the code exchange at CoderHelm failed: GitHub's own refusal (a
/// wrong or used code, a verifier GitHub doesn't accept) is the provider's;
/// anything else (CoderHelm unreachable, storage, another install's flow) is
/// a problem on CoderHelm's side or with setup, never "GitHub didn't confirm".
fn exchange_refusal(e: &client::CallError) -> Reason {
    match e.code.as_str() {
        "github_refused" | "invalid_grant" | "bad_verification_code" => Reason::Provider,
        _ => hosted_refusal(e),
    }
}

/// GET /auth/github on a hybrid install: a new state and PKCE pair, a signed
/// start at CoderHelm, a check of the GitHub address it answers, then GitHub.
pub async fn start(state: &AppState) -> Response {
    let app = app_url(state).to_string();
    let Some(creds) = client::credentials() else {
        return SignInFailure::new(
            P,
            Reason::NotSetUp,
            "start",
            "not registered with CoderHelm",
        )
        .into_redirect(&app);
    };
    let (install_state, verifier) = (gs::new_state(), gs::new_verifier());
    let challenge = gs::challenge(&verifier);
    let api = common::public_urls::api_url(&state.config.stage);
    let body = json!({
        "redirect_uri": callback_url(api),
        "state": install_state,
        "code_challenge": challenge,
    });
    let v = match client::call(&state.http, &creds, "POST", gs::START_PATH, Some(&body)).await {
        Ok(v) => v,
        Err(e) => {
            return SignInFailure::new(P, hosted_refusal(&e), "hybrid_start", e).into_redirect(&app)
        }
    };
    let flow = v["flow"].as_str().unwrap_or("");
    let url = v["authorize_url"].as_str().unwrap_or("");
    if !gs::is_flow(flow) || !gs::authorize_url_ok(url, flow, &challenge) {
        return SignInFailure::new(
            P,
            Reason::Server,
            "hybrid_start",
            "CoderHelm answered a sign-in address that isn't GitHub's for this flow",
        )
        .into_redirect(&app);
    }
    let pending = gs::Pending {
        state: install_state,
        flow: flow.to_string(),
        verifier,
    };
    let cookie = oauth_signin::state_cookie(P, &gs::seal(cookie_key(state), &pending, now()));
    let mut r = (StatusCode::FOUND, [(header::LOCATION, url.to_string())]).into_response();
    if let Ok(c) = HeaderValue::from_str(&cookie) {
        r.headers_mut().append(header::SET_COOKIE, c);
    }
    r
}

#[derive(Debug, Default, Deserialize)]
pub struct HybridReturn {
    pub code: Option<String>,
    pub state: Option<String>,
    pub flow: Option<String>,
    pub error: Option<String>,
}

/// The checks on the way back, before anything is sent anywhere: the sealed
/// cookie opens (this install made it, it's unchanged and not expired), the
/// state and the flow are this browser's, and GitHub said yes. Answers the
/// pending flow and the code.
pub fn check_return<'a>(
    key: &str,
    cookie_header: &str,
    q: &'a HybridReturn,
    now: i64,
) -> Result<(gs::Pending, &'a str), SignInFailure> {
    let Some(raw) = cookie_header
        .split(';')
        .map(str::trim)
        .find_map(|c| {
            c.strip_prefix(P.state_cookie())
                .and_then(|r| r.strip_prefix('='))
        })
        .filter(|v| !v.is_empty())
    else {
        return Err(SignInFailure::new(
            P,
            Reason::Expired,
            "state_cookie",
            "no state cookie: finished in another browser or app, or after it expired",
        ));
    };
    let Some(pending) = gs::open(key, raw, now) else {
        return Err(SignInFailure::new(
            P,
            Reason::Expired,
            "state_cookie",
            "the state cookie is expired or wasn't made by this install",
        ));
    };
    if !gs::return_matches(
        &pending,
        q.state.as_deref().unwrap_or(""),
        q.flow.as_deref().unwrap_or(""),
    ) {
        return Err(SignInFailure::new(
            P,
            Reason::Expired,
            "state_match",
            "the state or flow in the URL doesn't match this browser's",
        ));
    }
    if let Some(err) = q.error.as_deref().filter(|e| !e.is_empty()) {
        let reason = if err == "access_denied" {
            Reason::Cancelled
        } else {
            Reason::Provider
        };
        let shown: String = err.chars().take(64).collect();
        return Err(SignInFailure::new(P, reason, "provider_error", shown));
    }
    let Some(code) = q.code.as_deref().filter(|c| !c.is_empty()) else {
        return Err(SignInFailure::new(
            P,
            Reason::Provider,
            "missing_code",
            "callback without a code",
        ));
    };
    Ok((pending, code))
}

/// The token in the hosted side's answer (nothing else in it is used).
pub fn token_from(v: &Value) -> Option<&str> {
    v["access_token"]
        .as_str()
        .filter(|t| !t.is_empty() && t.len() <= 512 && !t.contains(char::is_whitespace))
}

/// GET /auth/github/hybrid-callback?code=&state=&flow=: the browser back from
/// GitHub (through CoderHelm). Only a hybrid install answers it.
pub async fn callback(
    State(state): State<Arc<AppState>>,
    ip: crate::middleware::rate_limit::ClientIp,
    headers: HeaderMap,
    Query(q): Query<HybridReturn>,
) -> Response {
    let app = app_url(&state).to_string();
    if !available() {
        return SignInFailure::new(P, Reason::NotSetUp, "callback", "not a hybrid install")
            .into_redirect(&app);
    }
    match callback_inner(&state, &ip.0, &headers, &q).await {
        Ok(r) => r,
        Err(f) => f.into_redirect(&app),
    }
}

async fn callback_inner(
    state: &Arc<AppState>,
    ip: &str,
    headers: &HeaderMap,
    q: &HybridReturn,
) -> Result<Response, SignInFailure> {
    let (pending, code) = check_return(cookie_key(state), auth::cookie_header(headers), q, now())?;
    let creds = client::credentials()
        .ok_or_else(|| SignInFailure::new(P, Reason::NotSetUp, "exchange", "not registered"))?;
    let body = json!({
        "flow": pending.flow,
        "code": code,
        "code_verifier": pending.verifier,
    });
    let v = client::call(&state.http, &creds, "POST", gs::EXCHANGE_PATH, Some(&body))
        .await
        .map_err(|e| SignInFailure::new(P, exchange_refusal(&e), "exchange", e))?;
    let token = token_from(&v).ok_or_else(|| {
        SignInFailure::new(
            P,
            Reason::Provider,
            "exchange",
            "no token in CoderHelm's answer",
        )
    })?;
    let out =
        auth::github_sign_in_with_token(state, ip, headers, token, TokenFrom::Coderhelm).await;
    // Done with it, whatever the outcome: GitHub takes it back.
    let gone = json!({ "access_token": token });
    let revoked = client::call(&state.http, &creds, "POST", gs::REVOKE_PATH, Some(&gone))
        .await
        .is_ok_and(|v| v["revoked"] == json!(true));
    if !revoked {
        tracing::warn!("hybrid GitHub sign-in: the token wasn't revoked; it expires at GitHub");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "install-session-key";

    fn pending() -> gs::Pending {
        gs::Pending {
            state: "S".repeat(43),
            flow: format!("hsi_{}", "f".repeat(40)),
            verifier: "v".repeat(64),
        }
    }

    fn cookie(value: &str) -> String {
        format!("other=1; {}={value}", P.state_cookie())
    }

    fn ret(code: Option<&str>, state: Option<&str>, flow: Option<&str>) -> HybridReturn {
        HybridReturn {
            code: code.map(Into::into),
            state: state.map(Into::into),
            flow: flow.map(Into::into),
            error: None,
        }
    }

    #[test]
    fn a_return_for_this_browser_flow_gives_the_code_and_the_verifier() {
        let p = pending();
        let c = cookie(&gs::seal(KEY, &p, 1_000));
        let q = ret(Some("c0de"), Some(&p.state), Some(&p.flow));
        let (got, code) = check_return(KEY, &c, &q, 1_010).expect("accepted");
        assert_eq!(code, "c0de");
        assert_eq!(got, p);
    }

    #[test]
    fn a_return_without_the_browser_cookie_is_refused() {
        // Login CSRF: someone else's code and flow pushed into this browser.
        let p = pending();
        let q = ret(Some("c0de"), Some(&p.state), Some(&p.flow));
        let f = check_return(KEY, "", &q, 1_000).unwrap_err();
        assert_eq!((f.reason, f.step), (Reason::Expired, "state_cookie"));
    }

    #[test]
    fn a_forged_expired_or_other_install_cookie_is_refused() {
        let p = pending();
        let q = ret(Some("c0de"), Some(&p.state), Some(&p.flow));
        // Sealed by another key: another install, or the hosted side.
        let other = cookie(&gs::seal("hosted-side-key", &p, 1_000));
        assert_eq!(
            check_return(KEY, &other, &q, 1_000).unwrap_err().step,
            "state_cookie"
        );
        // Expired.
        let old = cookie(&gs::seal(KEY, &p, 1_000));
        assert!(check_return(KEY, &old, &q, 1_000 + gs::COOKIE_TTL_SECS + 1).is_err());
        // Made by hand.
        let hand = cookie(&format!(
            "{}.{}.{}.9999999999.00",
            p.state, p.flow, p.verifier
        ));
        assert!(check_return(KEY, &hand, &q, 1_000).is_err());
    }

    #[test]
    fn a_return_for_another_flow_or_state_is_refused() {
        // Mix-up: the flow or state of another sign-in (or another install's).
        let p = pending();
        let c = cookie(&gs::seal(KEY, &p, 1_000));
        let other_flow = format!("hsi_{}", "g".repeat(40));
        for q in [
            ret(Some("c0de"), Some(&p.state), Some(&other_flow)),
            ret(Some("c0de"), Some(&"T".repeat(43)), Some(&p.flow)),
            ret(Some("c0de"), None, Some(&p.flow)),
            ret(Some("c0de"), Some(&p.state), None),
        ] {
            let f = check_return(KEY, &c, &q, 1_000).unwrap_err();
            assert_eq!((f.reason, f.step), (Reason::Expired, "state_match"));
        }
    }

    #[test]
    fn a_cancelled_or_codeless_return_never_reaches_the_exchange() {
        let p = pending();
        let c = cookie(&gs::seal(KEY, &p, 1_000));
        let mut q = ret(None, Some(&p.state), Some(&p.flow));
        q.error = Some("access_denied".into());
        assert_eq!(
            check_return(KEY, &c, &q, 1_000).unwrap_err().reason,
            Reason::Cancelled
        );
        q.error = None;
        assert_eq!(
            check_return(KEY, &c, &q, 1_000).unwrap_err().step,
            "missing_code"
        );
    }

    #[test]
    fn only_a_token_is_taken_from_the_hosted_answer() {
        assert_eq!(
            token_from(&json!({ "access_token": "ghu_abc" })),
            Some("ghu_abc")
        );
        // Anything the hosted side says about who signed in is not read at
        // all: identity comes from GitHub with the token.
        let v = json!({ "access_token": "ghu_abc", "login": "someone-else", "id": 1 });
        assert_eq!(token_from(&v), Some("ghu_abc"));
        assert_eq!(token_from(&json!({})), None);
        assert_eq!(token_from(&json!({ "access_token": "" })), None);
        assert_eq!(token_from(&json!({ "access_token": "a b" })), None);
    }

    #[test]
    fn the_callback_is_on_the_install_api_address() {
        assert_eq!(
            callback_url("https://acme.coderhelm.com/"),
            "https://acme.coderhelm.com/auth/github/hybrid-callback"
        );
        assert!(gs::redirect_allowed(
            &callback_url("https://acme.coderhelm.com"),
            &["https://acme.coderhelm.com".to_string()]
        ));
    }

    #[test]
    fn hosted_refusals_that_mean_setup_read_as_not_set_up() {
        let e = |c: &str| client::CallError {
            status: Some(400),
            code: c.into(),
            message: String::new(),
        };
        assert_eq!(hosted_refusal(&e("sign_in_unavailable")), Reason::NotSetUp);
        assert_eq!(hosted_refusal(&e("redirect_not_allowed")), Reason::NotSetUp);
        assert_eq!(hosted_refusal(&e("broker_unreachable")), Reason::Server);
    }

    #[test]
    fn only_githubs_own_refusal_reads_as_github_at_the_exchange() {
        let e = |c: &str| client::CallError {
            status: Some(400),
            code: c.into(),
            message: String::new(),
        };
        assert_eq!(exchange_refusal(&e("github_refused")), Reason::Provider);
        assert_eq!(exchange_refusal(&e("invalid_grant")), Reason::Provider);
        assert_eq!(exchange_refusal(&e("store_unavailable")), Reason::Server);
        assert_eq!(exchange_refusal(&e("wrong_install")), Reason::Server);
        assert_eq!(exchange_refusal(&e("not_verified")), Reason::NotSetUp);
    }
}
