//! The parts of Google and GitHub sign-in that decide what a person sees when
//! sign-in cannot finish, kept free of AWS and HTTP calls so each path has a
//! unit test.
//!
//! A failed callback never answers with a bare status code: the person lands
//! back on the dashboard's sign-in screen with `?signin_error=<code>&provider=<p>`
//! and the dashboard turns the code into one plain sentence and a Try again
//! button. The URL carries only that short code; the reason and the step are
//! logged at WARN.

use axum::{
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use tracing::warn;

/// How long the CSRF state cookie lives: the whole trip to the provider and back.
pub const STATE_MAX_AGE_SECS: u32 = 600;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    Google,
    Github,
}

impl Provider {
    /// The name in the dashboard URL and the logs.
    pub fn slug(self) -> &'static str {
        match self {
            Provider::Google => "google",
            Provider::Github => "github",
        }
    }

    /// The CSRF state cookie for this provider's trip.
    pub fn state_cookie(self) -> &'static str {
        match self {
            Provider::Google => "__Host-oauth_state_g",
            Provider::Github => "__Host-oauth_state_gh",
        }
    }
}

/// Why sign-in stopped. `code()` is all the dashboard ever sees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// Google refuses sign-in inside an app's built-in browser (a web view).
    InAppBrowser,
    /// The state cookie was missing or did not match: sign-in finished in a
    /// different browser or app than it started in, or after the cookie expired.
    Expired,
    /// The person said no on the provider's page.
    Cancelled,
    /// The provider sent an error, or would not confirm the code or the profile.
    Provider,
    /// Something on our side failed.
    Server,
}

impl Reason {
    pub fn code(self) -> &'static str {
        match self {
            Reason::InAppBrowser => "in_app_browser",
            Reason::Expired => "expired",
            Reason::Cancelled => "cancelled",
            Reason::Provider => "provider",
            Reason::Server => "server",
        }
    }
}

/// The query string a provider (or Cognito) sends back to the callback. Every
/// field is optional so a provider error reaches our handler instead of axum's
/// plain-text "Failed to deserialize query string".
#[derive(Debug, Default, Deserialize)]
pub struct CallbackQuery {
    pub code: Option<String>,
    pub state: Option<String>,
    pub error: Option<String>,
}

/// A sign-in that could not finish: which provider, why, at which step, and a
/// detail for the log only.
#[derive(Debug)]
pub struct SignInFailure {
    pub provider: Provider,
    pub reason: Reason,
    pub step: &'static str,
    pub detail: String,
}

impl SignInFailure {
    pub fn new(
        provider: Provider,
        reason: Reason,
        step: &'static str,
        detail: impl ToString,
    ) -> Self {
        Self {
            provider,
            reason,
            step,
            detail: detail.to_string(),
        }
    }

    /// Log the failure at WARN and send the person back to the sign-in screen.
    pub fn into_redirect(self, app_url: &str) -> Response {
        warn!(
            provider = self.provider.slug(),
            step = self.step,
            reason = self.reason.code(),
            detail = %self.detail,
            "Sign-in did not finish"
        );
        let mut resp = (
            StatusCode::FOUND,
            [(
                header::LOCATION,
                failure_url(app_url, self.provider, self.reason),
            )],
        )
            .into_response();
        if let Ok(v) = HeaderValue::from_str(&clear_state_cookie(self.provider)) {
            resp.headers_mut().append(header::SET_COOKIE, v);
        }
        resp
    }
}

/// Where a failed sign-in lands: the dashboard's sign-in screen with a short code.
pub fn failure_url(app_url: &str, provider: Provider, reason: Reason) -> String {
    format!(
        "{}/?signin_error={}&provider={}",
        app_url.trim_end_matches('/'),
        reason.code(),
        provider.slug()
    )
}

pub fn state_cookie(provider: Provider, value: &str) -> String {
    format!(
        "{}={value}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age={STATE_MAX_AGE_SECS}",
        provider.state_cookie()
    )
}

/// The state cookie is single use: cleared once the callback has run.
pub fn clear_state_cookie(provider: Provider) -> String {
    format!(
        "{}=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0",
        provider.state_cookie()
    )
}

/// The first steps of every callback: a provider error, a missing code, and the
/// CSRF state check. Returns the authorization code to exchange.
pub fn check_callback<'a>(
    provider: Provider,
    cookie_header: &str,
    q: &'a CallbackQuery,
) -> Result<&'a str, SignInFailure> {
    if let Some(err) = q.error.as_deref().filter(|e| !e.is_empty()) {
        let reason = if err == "access_denied" {
            Reason::Cancelled
        } else {
            Reason::Provider
        };
        // The provider's error name is a fixed token (RFC 6749 §4.1.2.1); keep
        // the log line short whatever arrives.
        let shown: String = err.chars().take(64).collect();
        return Err(SignInFailure::new(
            provider,
            reason,
            "provider_error",
            shown,
        ));
    }
    let Some(code) = q.code.as_deref().filter(|c| !c.is_empty()) else {
        return Err(SignInFailure::new(
            provider,
            Reason::Provider,
            "missing_code",
            "callback without a code",
        ));
    };
    let Some(expected) = cookie_value(cookie_header, provider.state_cookie()) else {
        return Err(SignInFailure::new(
            provider,
            Reason::Expired,
            "state_cookie",
            "no state cookie: finished in another browser or app, or after it expired",
        ));
    };
    match q.state.as_deref() {
        Some(s) if !s.is_empty() && s == expected => Ok(code),
        _ => Err(SignInFailure::new(
            provider,
            Reason::Expired,
            "state_match",
            "state in the URL does not match the cookie",
        )),
    }
}

fn cookie_value<'a>(cookie_header: &'a str, name: &str) -> Option<&'a str> {
    cookie_header
        .split(';')
        .map(str::trim)
        .find_map(|c| c.strip_prefix(name).and_then(|rest| rest.strip_prefix('=')))
        .filter(|v| !v.is_empty())
}

/// An app's built-in browser (a web view inside another app), where Google
/// shows "This browser or app may not be secure" instead of a sign-in page.
///
/// - Android System WebView marks itself with `; wv)`.
/// - On iPhone and iPad every real browser (Safari, Chrome, Firefox, Edge,
///   Brave, DuckDuckGo) and the in-app Safari view keep the `Safari/` token; a
///   bare WKWebView drops it.
/// - Some apps name themselves (Facebook, Instagram, LinkedIn, Line, WeChat,
///   TikTok, Snapchat) whatever the engine underneath.
pub fn is_in_app_browser(user_agent: &str) -> bool {
    const APP_TOKENS: [&str; 10] = [
        "FBAN/",
        "FBAV/",
        "FB_IAB",
        "Instagram",
        "LinkedInApp",
        " Line/",
        "MicroMessenger",
        "musical_ly",
        "BytedanceWebview",
        "Snapchat",
    ];
    if APP_TOKENS.iter().any(|t| user_agent.contains(t)) {
        return true;
    }
    if user_agent.contains("Android") && user_agent.contains("; wv)") {
        return true;
    }
    let apple_mobile =
        user_agent.contains("iPhone") || user_agent.contains("iPad") || user_agent.contains("iPod");
    apple_mobile && user_agent.contains("AppleWebKit") && !user_agent.contains("Safari/")
}

#[cfg(test)]
mod tests {
    use super::*;

    const APP: &str = "https://app.coderhelm.com";

    fn q(code: Option<&str>, state: Option<&str>, error: Option<&str>) -> CallbackQuery {
        CallbackQuery {
            code: code.map(Into::into),
            state: state.map(Into::into),
            error: error.map(Into::into),
        }
    }

    #[test]
    fn a_matching_state_returns_the_code() {
        let query = q(Some("abc"), Some("S1"), None);
        for p in [Provider::Google, Provider::Github] {
            let cookies = format!("other=1; {}=S1", p.state_cookie());
            assert_eq!(check_callback(p, &cookies, &query).unwrap(), "abc");
        }
    }

    #[test]
    fn no_state_cookie_is_expired_at_the_cookie_step() {
        let f =
            check_callback(Provider::Google, "", &q(Some("abc"), Some("S1"), None)).unwrap_err();
        assert_eq!((f.reason, f.step), (Reason::Expired, "state_cookie"));
        // The other provider's cookie does not count.
        let f = check_callback(
            Provider::Google,
            "__Host-oauth_state_gh=S1",
            &q(Some("abc"), Some("S1"), None),
        )
        .unwrap_err();
        assert_eq!((f.reason, f.step), (Reason::Expired, "state_cookie"));
        // An emptied cookie does not count either.
        let f = check_callback(
            Provider::Github,
            "__Host-oauth_state_gh=",
            &q(Some("abc"), Some(""), None),
        )
        .unwrap_err();
        assert_eq!((f.reason, f.step), (Reason::Expired, "state_cookie"));
    }

    #[test]
    fn a_different_or_missing_state_is_expired_at_the_match_step() {
        let cookies = "__Host-oauth_state_gh=S1";
        for state in [Some("S2"), Some(""), None] {
            let f = check_callback(Provider::Github, cookies, &q(Some("abc"), state, None))
                .unwrap_err();
            assert_eq!(
                (f.reason, f.step),
                (Reason::Expired, "state_match"),
                "state {state:?}"
            );
        }
    }

    #[test]
    fn a_cookie_whose_name_only_starts_the_same_does_not_count() {
        let f = check_callback(
            Provider::Google,
            "__Host-oauth_state_gh=S1",
            &q(Some("abc"), Some("S1"), None),
        )
        .unwrap_err();
        assert_eq!(f.step, "state_cookie");
    }

    /// The whole callback decision from the URL a browser lands on, as the
    /// handler sees it: axum's query extractor, then `check_callback`, then the
    /// redirect a failure turns into.
    fn landing(provider: Provider, url: &str, cookies: &str) -> Result<String, String> {
        let uri: axum::http::Uri = url.parse().unwrap();
        let axum::extract::Query(query) = axum::extract::Query::<CallbackQuery>::try_from_uri(&uri)
            .map_err(|e| format!("rejected: {e}"))?;
        match check_callback(provider, cookies, &query) {
            Ok(code) => Ok(format!("exchange {code}")),
            Err(f) => {
                let resp = f.into_redirect(APP);
                assert_eq!(resp.status(), StatusCode::FOUND);
                Err(resp.headers()[header::LOCATION]
                    .to_str()
                    .unwrap()
                    .to_string())
            }
        }
    }

    #[test]
    fn every_callback_failure_lands_on_the_sign_in_screen() {
        let g = "https://api.coderhelm.com/auth/google/callback";
        let gh = "https://api.coderhelm.com/auth/github/callback";
        let cases = [
            // Cancelled on Google: Cognito sends error= and no code (was a plain-text 400).
            (
                Provider::Google,
                format!("{g}?error=access_denied&state=S1"),
                "__Host-oauth_state_g=S1",
                "cancelled",
            ),
            (
                Provider::Github,
                format!("{gh}?error=access_denied&error_description=The+user+has+denied&state=S1"),
                "",
                "cancelled",
            ),
            (
                Provider::Google,
                format!("{g}?error=invalid_request&error_description=x"),
                "",
                "provider",
            ),
            (
                Provider::Github,
                format!("{gh}?state=S1"),
                "__Host-oauth_state_gh=S1",
                "provider",
            ),
            // Finished in another browser or app: no state cookie (was a blank 403).
            (
                Provider::Google,
                format!("{g}?code=c&state=S1"),
                "",
                "expired",
            ),
            (
                Provider::Github,
                format!("{gh}?code=c&state=S1"),
                "_ga=1",
                "expired",
            ),
            (
                Provider::Github,
                format!("{gh}?code=c&state=S2"),
                "__Host-oauth_state_gh=S1",
                "expired",
            ),
            (
                Provider::Google,
                format!("{g}?code=c"),
                "__Host-oauth_state_g=S1",
                "expired",
            ),
        ];
        for (p, url, cookies, code) in cases {
            let want = format!(
                "https://app.coderhelm.com/?signin_error={code}&provider={}",
                p.slug()
            );
            assert_eq!(landing(p, &url, cookies), Err(want), "{url}");
        }
        assert_eq!(
            landing(
                Provider::Google,
                &format!("{g}?code=c&state=S1"),
                "__Host-oauth_state_g=S1"
            ),
            Ok("exchange c".into())
        );
    }

    #[test]
    fn saying_no_on_the_provider_page_is_cancelled() {
        let f = check_callback(
            Provider::Google,
            "",
            &q(None, Some("S1"), Some("access_denied")),
        )
        .unwrap_err();
        assert_eq!((f.reason, f.step), (Reason::Cancelled, "provider_error"));
    }

    #[test]
    fn any_other_provider_error_is_a_provider_failure() {
        for err in ["invalid_request", "server_error", "temporarily_unavailable"] {
            let f = check_callback(
                Provider::Github,
                "__Host-oauth_state_gh=S1",
                &q(None, Some("S1"), Some(err)),
            )
            .unwrap_err();
            assert_eq!((f.reason, f.step), (Reason::Provider, "provider_error"));
            assert_eq!(f.detail, err);
        }
        let long = "x".repeat(500);
        let f = check_callback(Provider::Github, "", &q(None, None, Some(&long))).unwrap_err();
        assert_eq!(f.detail.len(), 64);
    }

    #[test]
    fn no_code_and_no_error_is_a_provider_failure() {
        for code in [None, Some("")] {
            let f = check_callback(
                Provider::Google,
                "__Host-oauth_state_g=S1",
                &q(code, Some("S1"), None),
            )
            .unwrap_err();
            assert_eq!((f.reason, f.step), (Reason::Provider, "missing_code"));
        }
    }

    #[test]
    fn the_failure_url_is_the_sign_in_screen_with_a_short_code() {
        assert_eq!(
            failure_url(APP, Provider::Google, Reason::Expired),
            "https://app.coderhelm.com/?signin_error=expired&provider=google"
        );
        assert_eq!(
            failure_url(
                "https://app.example.com/",
                Provider::Github,
                Reason::Cancelled
            ),
            "https://app.example.com/?signin_error=cancelled&provider=github"
        );
        let codes: Vec<_> = [
            Reason::InAppBrowser,
            Reason::Expired,
            Reason::Cancelled,
            Reason::Provider,
            Reason::Server,
        ]
        .iter()
        .map(|r| r.code())
        .collect();
        assert_eq!(
            codes,
            [
                "in_app_browser",
                "expired",
                "cancelled",
                "provider",
                "server"
            ]
        );
    }

    #[test]
    fn a_failure_redirects_to_the_app_and_clears_the_state_cookie() {
        let resp = SignInFailure::new(
            Provider::Github,
            Reason::Server,
            "session_token",
            "jwt failed",
        )
        .into_redirect(APP);
        assert_eq!(resp.status(), StatusCode::FOUND);
        assert_eq!(
            resp.headers().get(header::LOCATION).unwrap(),
            "https://app.coderhelm.com/?signin_error=server&provider=github"
        );
        let cookie = resp
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(cookie.starts_with("__Host-oauth_state_gh=;"), "{cookie}");
        assert!(cookie.contains("Max-Age=0"));
        // The internal detail never reaches the URL.
        assert!(!resp
            .headers()
            .get(header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("jwt"));
    }

    #[test]
    fn the_state_cookie_lives_for_the_trip_and_is_host_only() {
        let c = state_cookie(Provider::Google, "S1");
        assert_eq!(
            c,
            "__Host-oauth_state_g=S1; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=600"
        );
        // SameSite=Lax: the provider's redirect back is a top-level GET, which
        // carries Lax cookies; Strict would drop it.
        assert!(!c.contains("Domain="));
    }

    #[test]
    fn in_app_browsers_are_recognised() {
        let in_app = [
            // iOS WKWebView (an app's built-in browser): no Safari/ token.
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
            // Android System WebView.
            "Mozilla/5.0 (Linux; Android 14; Pixel 7 Build/UQ1A.240205.004; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/129.0.6668.81 Mobile Safari/537.36",
            // Apps that name themselves.
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 [FBAN/FBIOS;FBAV/450.0.0.38.108]",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Instagram 330.0.3.21.91",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 [LinkedInApp]/9.29.8962",
            "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Mobile Safari/537.36 MicroMessenger/8.0.49",
        ];
        for ua in in_app {
            assert!(is_in_app_browser(ua), "{ua}");
        }
    }

    #[test]
    fn real_browsers_are_not_in_app_browsers() {
        let browsers = [
            // iPhone Safari, Chrome, Firefox, Edge on iOS.
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/129.0.6668.69 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/131.0 Mobile/15E148 Safari/605.1.15",
            "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) EdgiOS/129.0 Version/17.0 Mobile/15E148 Safari/604.1",
            // Android Chrome and Samsung Internet.
            "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.6668.81 Mobile Safari/537.36",
            "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/26.0 Chrome/122.0.0.0 Mobile Safari/537.36",
            // Desktop.
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "",
        ];
        for ua in browsers {
            assert!(!is_in_app_browser(ua), "{ua}");
        }
    }
}
