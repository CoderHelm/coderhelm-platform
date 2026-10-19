//! Unsubscribe links in emails. Public: the signed token in the link is the
//! only credential, and it can do exactly one thing — turn the weekly summary
//! off for the one person and team it names.
//!
//! `GET` is the link people click; `POST` is the one-click unsubscribe mail
//! apps send for the `List-Unsubscribe-Post` header (RFC 8058). Both do the
//! same thing and answer with a short page.

use axum::{
    extract::{Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use common::unsubscribe::{self, Subscriber};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{error, info};

use crate::AppState;

const NOTIFICATIONS_URL: &str = "https://app.coderhelm.com/settings/notifications/";

#[derive(Deserialize)]
pub struct UnsubscribeQuery {
    #[serde(default)]
    t: String,
}

/// Where the preference is kept. A trait so the handler can be tested
/// without DynamoDB.
trait Prefs {
    async fn turn_off_weekly_summary(&self, who: &Subscriber) -> Result<(), String>;
}

struct DynamoPrefs<'a>(&'a AppState);

impl Prefs for DynamoPrefs<'_> {
    async fn turn_off_weekly_summary(&self, who: &Subscriber) -> Result<(), String> {
        let s = |v: &str| aws_sdk_dynamodb::types::AttributeValue::S(v.to_string());
        self.0
            .dynamo
            .update_item()
            .table_name(&self.0.config.settings_table_name)
            .key("pk", s(&who.team_id))
            .key("sk", s(&format!("NOTIFICATIONS#{}", who.user_id)))
            .update_expression("SET email_weekly_summary = :off, updated_at = :now")
            .expression_attribute_values(
                ":off",
                aws_sdk_dynamodb::types::AttributeValue::Bool(false),
            )
            .expression_attribute_values(":now", s(&chrono::Utc::now().to_rfc3339()))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("{e}"))
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Unsubscribed,
    /// The link came from a test email sent to an address not on the team.
    Test,
    /// Bad, edited or expired: the page says the same for all of them.
    Invalid,
    Failed,
}

async fn handle(prefs: &impl Prefs, secret: &str, token: &str, now: i64) -> Outcome {
    let token = token.trim();
    if token == unsubscribe::TEST_TOKEN {
        return Outcome::Test;
    }
    let who = match unsubscribe::verify(secret, token, unsubscribe::WEEKLY_SUMMARY, now) {
        Ok(w) => w,
        Err(e) => {
            info!(reason = ?e, "Unsubscribe link refused");
            return Outcome::Invalid;
        }
    };
    match prefs.turn_off_weekly_summary(&who).await {
        Ok(()) => {
            info!(team_id = %who.team_id, "Weekly summary turned off from an email link");
            Outcome::Unsubscribed
        }
        Err(e) => {
            error!(error = %e, "Could not turn off the weekly summary");
            Outcome::Failed
        }
    }
}

fn page(outcome: &Outcome) -> Response {
    let (status, title, body) = match outcome {
        Outcome::Unsubscribed => (
            StatusCode::OK,
            "You're unsubscribed",
            format!(
                "You won't get the weekly summary email from Coderhelm any more. \
                 Other emails you have on are not affected.</p>\
                 <p><a class=\"button\" href=\"{NOTIFICATIONS_URL}\">Turn it back on</a>"
            ),
        ),
        Outcome::Test => (
            StatusCode::OK,
            "This was a test email",
            format!(
                "This link came from a test of the weekly summary, so there was nothing to \
                 unsubscribe. In real emails it turns the summary off for the person it was \
                 sent to.</p><p><a class=\"button\" href=\"{NOTIFICATIONS_URL}\">Email settings</a>"
            ),
        ),
        Outcome::Invalid => (
            StatusCode::BAD_REQUEST,
            "This link doesn't work",
            format!(
                "It may be incomplete or too old. You can turn the weekly summary off in your \
                 email settings instead.</p>\
                 <p><a class=\"button\" href=\"{NOTIFICATIONS_URL}\">Email settings</a>"
            ),
        ),
        Outcome::Failed => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Something went wrong on our side",
            format!(
                "Nothing was changed. Try the link again in a minute, or turn the weekly \
                 summary off in your email settings.</p>\
                 <p><a class=\"button\" href=\"{NOTIFICATIONS_URL}\">Email settings</a>"
            ),
        ),
    };
    let html = format!(
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
         <meta name=\"referrer\" content=\"no-referrer\"><meta name=\"robots\" content=\"noindex\">\
         <title>{title} · Coderhelm</title><style>\
         body{{margin:0;background:#0a0a0a;color:#ccc;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif}}\
         main{{max-width:480px;margin:64px auto;padding:32px;background:#111;border:1px solid #222;border-radius:8px}}\
         @media (max-width:520px){{main{{margin:16px;padding:24px}}}}\
         h1{{color:#fff;font-size:20px;margin:24px 0 12px}}p{{font-size:15px;line-height:1.6}}\
         a.button{{display:inline-block;background:#fff;color:#000;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:600;margin-top:8px}}\
         </style></head><body><main>\
         <img src=\"https://coderhelm.com/favicon.svg\" width=\"32\" height=\"32\" alt=\"Coderhelm\">\
         <h1>{title}</h1><p>{body}</p></main></body></html>"
    );
    let mut resp = (status, html).into_response();
    let h = resp.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    // The token is in this page's address: never send it on in a Referer.
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    h.insert("x-robots-tag", HeaderValue::from_static("noindex"));
    resp
}

/// GET /email/unsubscribe?t= — the link in the email.
pub async fn unsubscribe_link(
    State(state): State<Arc<AppState>>,
    Query(q): Query<UnsubscribeQuery>,
) -> Response {
    let now = chrono::Utc::now().timestamp();
    page(&handle(&DynamoPrefs(&state), &state.secrets.jwt_secret, &q.t, now).await)
}

/// POST /email/unsubscribe?t= — one-click unsubscribe from a mail app
/// (body `List-Unsubscribe=One-Click`, which carries nothing we need).
pub async fn unsubscribe_one_click(
    State(state): State<Arc<AppState>>,
    Query(q): Query<UnsubscribeQuery>,
) -> Response {
    let now = chrono::Utc::now().timestamp();
    page(&handle(&DynamoPrefs(&state), &state.secrets.jwt_secret, &q.t, now).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    const SECRET: &str = "secret";
    const NOW: i64 = 1_800_000_000;

    #[derive(Default)]
    struct Fake {
        calls: Mutex<Vec<Subscriber>>,
        fail: bool,
    }

    impl Prefs for Fake {
        async fn turn_off_weekly_summary(&self, who: &Subscriber) -> Result<(), String> {
            self.calls.lock().unwrap().push(who.clone());
            if self.fail {
                Err("down".into())
            } else {
                Ok(())
            }
        }
    }

    fn token(exp: i64) -> String {
        unsubscribe::sign(SECRET, "team-1", "USER#a", unsubscribe::WEEKLY_SUMMARY, exp)
    }

    async fn body(resp: Response) -> String {
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn a_good_link_turns_off_only_that_persons_summary() {
        let fake = Fake::default();
        let out = handle(&fake, SECRET, &token(NOW + 60), NOW).await;
        assert_eq!(out, Outcome::Unsubscribed);
        assert_eq!(
            *fake.calls.lock().unwrap(),
            vec![Subscriber {
                team_id: "team-1".into(),
                user_id: "USER#a".into()
            }]
        );
        let resp = page(&out);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::REFERRER_POLICY], "no-referrer");
        let html = body(resp).await;
        assert!(html.contains("You're unsubscribed"));
        assert!(html.contains("Turn it back on"));
        assert!(html.contains(NOTIFICATIONS_URL));
    }

    #[tokio::test]
    async fn bad_edited_or_expired_links_change_nothing_and_say_the_same() {
        let fake = Fake::default();
        let good = token(NOW + 60);
        let (head, _) = good.split_once('.').unwrap();
        let cases = [
            String::new(),
            "garbage".to_string(),
            format!("{head}.AAAA"),
            token(NOW - 1),
            unsubscribe::sign(
                "other",
                "team-1",
                "USER#a",
                unsubscribe::WEEKLY_SUMMARY,
                NOW + 60,
            ),
        ];
        let mut pages = Vec::new();
        for t in &cases {
            let out = handle(&fake, SECRET, t, NOW).await;
            assert_eq!(out, Outcome::Invalid, "{t}");
            let resp = page(&out);
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            let html = body(resp).await;
            if !t.is_empty() {
                assert!(!html.contains(t.as_str()), "the page repeats the token");
            }
            pages.push(html);
        }
        assert!(fake.calls.lock().unwrap().is_empty());
        // No hint about which check failed.
        assert!(pages.windows(2).all(|w| w[0] == w[1]));
        assert!(!pages[0].contains("team-1"));
    }

    #[tokio::test]
    async fn a_test_link_changes_nothing() {
        let fake = Fake::default();
        let out = handle(&fake, SECRET, unsubscribe::TEST_TOKEN, NOW).await;
        assert_eq!(out, Outcome::Test);
        assert!(fake.calls.lock().unwrap().is_empty());
        assert!(body(page(&out)).await.contains("test"));
    }

    #[tokio::test]
    async fn a_store_failure_says_nothing_changed() {
        let fake = Fake {
            fail: true,
            ..Default::default()
        };
        let out = handle(&fake, SECRET, &token(NOW + 60), NOW).await;
        assert_eq!(out, Outcome::Failed);
        let resp = page(&out);
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body(resp).await.contains("Nothing was changed"));
    }
}
