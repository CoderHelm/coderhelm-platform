//! Unsubscribe links in emails. Public: the signed token in the link is the
//! only credential, and it can do exactly one thing — turn the weekly summary
//! off for the one person and team it names.
//!
//! `GET` is the link people click. It changes nothing: mail scanners open
//! links before people do, so it only asks "Unsubscribe from weekly
//! summaries for <team>?" with one button. `POST` does the unsubscribe: that
//! button, and the one-click unsubscribe mail apps send for the
//! `List-Unsubscribe-Post` header (RFC 8058).

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

/// The notification settings page.
fn notifications_url() -> String {
    format!(
        "{}/settings/notifications/",
        common::public_urls::app_url_from_env()
    )
}

/// The page footer: the product name, and the site when there is one.
fn footer() -> String {
    match common::public_urls::site_url() {
        Some(site) => format!(
            "Coderhelm · <a href=\"{site}\" style=\"color:inherit\">{}</a>",
            common::public_urls::host_of(site)
        ),
        None => "Coderhelm".to_string(),
    }
}

#[derive(Deserialize)]
pub struct UnsubscribeQuery {
    #[serde(default)]
    t: String,
}

/// Where the preference is kept. A trait so the handler can be tested
/// without DynamoDB.
trait Prefs {
    async fn turn_off_weekly_summary(&self, who: &Subscriber) -> Result<(), String>;
    /// The team's name for the question page; empty when unknown.
    async fn team_name(&self, team_id: &str) -> String;
}

struct DynamoPrefs<'a>(&'a AppState);

fn attr(v: &str) -> aws_sdk_dynamodb::types::AttributeValue {
    aws_sdk_dynamodb::types::AttributeValue::S(v.to_string())
}

impl Prefs for DynamoPrefs<'_> {
    async fn turn_off_weekly_summary(&self, who: &Subscriber) -> Result<(), String> {
        self.0
            .dynamo
            .update_item()
            .table_name(&self.0.config.settings_table_name)
            .key("pk", attr(&who.team_id))
            .key("sk", attr(&format!("NOTIFICATIONS#{}", who.user_id)))
            .update_expression("SET email_weekly_summary = :off, updated_at = :now")
            .expression_attribute_values(
                ":off",
                aws_sdk_dynamodb::types::AttributeValue::Bool(false),
            )
            .expression_attribute_values(":now", attr(&chrono::Utc::now().to_rfc3339()))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("{e}"))
    }

    async fn team_name(&self, team_id: &str) -> String {
        self.0
            .dynamo
            .get_item()
            .table_name(&self.0.config.table_name)
            .key("pk", attr(team_id))
            .key("sk", attr("META"))
            .projection_expression("github_org")
            .send()
            .await
            .ok()
            .and_then(|r| r.item)
            .and_then(|i| i.get("github_org").and_then(|v| v.as_s().ok()).cloned())
            .unwrap_or_default()
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// A good link, opened: ask before changing anything.
    Confirm {
        team: String,
    },
    Unsubscribed,
    /// The link came from a test email sent to an address not on the team.
    Test,
    /// Bad, edited or expired: the page says the same for all of them.
    Invalid,
    Failed,
}

fn verify(secret: &str, token: &str, now: i64) -> Result<Subscriber, Outcome> {
    let token = token.trim();
    if token == unsubscribe::TEST_TOKEN {
        return Err(Outcome::Test);
    }
    unsubscribe::verify(secret, token, unsubscribe::WEEKLY_SUMMARY, now).map_err(|e| {
        info!(reason = ?e, "Unsubscribe link refused");
        Outcome::Invalid
    })
}

/// GET: check the link and ask. Never changes the preference.
async fn ask(prefs: &impl Prefs, secret: &str, token: &str, now: i64) -> Outcome {
    match verify(secret, token, now) {
        Ok(who) => Outcome::Confirm {
            team: prefs.team_name(&who.team_id).await,
        },
        Err(out) => out,
    }
}

/// POST: check the link and turn the summary off.
async fn handle(prefs: &impl Prefs, secret: &str, token: &str, now: i64) -> Outcome {
    let who = match verify(secret, token, now) {
        Ok(w) => w,
        Err(out) => return out,
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

fn esc(v: &str) -> String {
    v.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// Same look as the weekly summary email: zinc grays, the blue accent, the
/// "///" mark as text, dark like the dashboard.
const STYLE: &str = ":root{color-scheme:dark;--page:#09090b;--card:#18181b;--border:#27272a;\
--t1:#f4f4f5;--t2:#a1a1aa;--t3:#8f8f99;--blue:#3b82f6;--button:#2563eb;--link:#60a5fa}\
*{box-sizing:border-box}\
body{margin:0;background:var(--page);color:var(--t2);\
font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;\
-webkit-font-smoothing:antialiased}\
.wrap{max-width:480px;margin:0 auto;padding:48px 16px}\
.mark{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:18px;font-weight:700;\
color:var(--t1);margin:0 4px 20px}\
.mark i{font-style:normal;letter-spacing:-0.08em}.mark b{color:var(--blue);letter-spacing:-0.08em}\
main{background:var(--card);border:1px solid var(--border);border-radius:16px;padding:40px}\
@media (max-width:520px){main{padding:28px 24px}.wrap{padding-top:32px}}\
h1{color:var(--t1);font-size:24px;line-height:32px;letter-spacing:-0.02em;margin:0 0 12px;text-wrap:balance}\
p{font-size:16px;line-height:24px;margin:0 0 24px}\
form{margin:0}\
.button{display:inline-block;background:var(--button);color:#fafafa;border:0;border-radius:8px;\
padding:0 28px;font:inherit;font-size:16px;font-weight:600;line-height:48px;text-decoration:none;\
cursor:pointer;white-space:nowrap}\
.button:hover{background:#1d4ed8}\
.button:focus-visible{outline:3px solid var(--blue);outline-offset:2px}\
.more{margin:20px 0 0;font-size:14px;line-height:20px;color:var(--t3)}\
.more a{color:var(--link);font-weight:600;text-decoration:none}\
footer{margin:24px 4px 0;font-size:13px;line-height:20px;color:var(--t3);text-align:center}";

fn page(outcome: &Outcome) -> Response {
    let notifications = notifications_url();
    let footer = footer();
    let settings = format!(
        r#"<a class="button" href="{}">Email settings</a>"#,
        notifications_url()
    );
    let (status, title, body) = match outcome {
        Outcome::Confirm { team } => {
            let team = if team.trim().is_empty() {
                "your team".to_string()
            } else {
                esc(team.trim())
            };
            (
                StatusCode::OK,
                format!("Unsubscribe from weekly summaries for {team}?"),
                format!(
                    "<p>You'll stop getting the Monday summary email for {team}. Other Coderhelm \
                     emails stay as they are.</p>\
                     <form method=\"post\"><button class=\"button\" type=\"submit\">Unsubscribe</button></form>\
                     <p class=\"more\">Changed your mind? Just close this page. \
                     <a href=\"{notifications}\">Email settings&nbsp;&rarr;</a></p>"
                ),
            )
        }
        Outcome::Unsubscribed => (
            StatusCode::OK,
            "You're unsubscribed".to_string(),
            format!(
                "<p>You won't get the weekly summary email from Coderhelm any more. Other \
                 emails you have on are not affected.</p>\
                 <a class=\"button\" href=\"{notifications}\">Turn it back on</a>"
            ),
        ),
        Outcome::Test => (
            StatusCode::OK,
            "This was a test email".to_string(),
            format!(
                "<p>This link came from a test of the weekly summary, so there is nothing to \
                 unsubscribe. In real emails it turns the summary off for the person it was \
                 sent to.</p>{settings}"
            ),
        ),
        Outcome::Invalid => (
            StatusCode::BAD_REQUEST,
            "This link doesn't work".to_string(),
            format!(
                "<p>It may be incomplete or too old. You can turn the weekly summary off in \
                 your email settings instead.</p>{settings}"
            ),
        ),
        Outcome::Failed => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Something went wrong on our side".to_string(),
            format!(
                "<p>Nothing was changed. Try again in a minute, or turn the weekly summary off \
                 in your email settings.</p>{settings}"
            ),
        ),
    };
    let html = format!(
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
         <meta name=\"color-scheme\" content=\"dark\">\
         <meta name=\"referrer\" content=\"no-referrer\"><meta name=\"robots\" content=\"noindex\">\
         <title>{title} · Coderhelm</title><style>{STYLE}</style></head><body>\
         <div class=\"wrap\"><div class=\"mark\" aria-label=\"Coderhelm\"><i>/</i><b>/</b><i>/</i>&nbsp;Coderhelm</div>\
         <main><h1>{title}</h1>{body}</main>\
         <footer>{footer}</footer>\
         </div></body></html>"
    );
    let mut resp = (status, html).into_response();
    let h = resp.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    // The token is in this page's address: never send it on in a Referer,
    // and never keep the page in a cache.
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    h.insert("x-robots-tag", HeaderValue::from_static("noindex"));
    resp
}

/// GET /email/unsubscribe?t= — the link in the email. Asks; changes nothing.
pub async fn unsubscribe_link(
    State(state): State<Arc<AppState>>,
    Query(q): Query<UnsubscribeQuery>,
) -> Response {
    let now = chrono::Utc::now().timestamp();
    page(&ask(&DynamoPrefs(&state), &state.secrets.jwt_secret, &q.t, now).await)
}

/// POST /email/unsubscribe?t= — the button on that page, or one-click
/// unsubscribe from a mail app (body `List-Unsubscribe=One-Click`, which
/// carries nothing we need).
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

        async fn team_name(&self, team_id: &str) -> String {
            if team_id == "team-1" {
                "Example <Team>".into()
            } else {
                String::new()
            }
        }
    }

    fn token(exp: i64) -> String {
        unsubscribe::sign(SECRET, "team-1", "USER#a", unsubscribe::WEEKLY_SUMMARY, exp)
    }

    /// The unsubscribe pages of a self-hosted install link to the install
    /// and name no CoderHelm site.
    #[tokio::test]
    async fn a_self_hosted_install_pages_link_to_itself() {
        common::public_urls::pretend_self_hosted();
        let outcomes = [
            Outcome::Confirm {
                team: "Acme".into(),
            },
            Outcome::Unsubscribed,
            Outcome::Test,
            Outcome::Invalid,
            Outcome::Failed,
        ];
        let mut pages = Vec::new();
        for o in &outcomes {
            pages.push(body(page(o)).await);
        }
        common::public_urls::override_for_this_thread(None);
        for p in &pages {
            assert_eq!(
                common::public_urls::hosted_mentions(p),
                Vec::<String>::new(),
                "{p}"
            );
        }
        assert!(pages[1].contains(common::public_urls::EXAMPLE_SELF_HOST));
    }

    async fn body(resp: Response) -> String {
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn opening_a_good_link_only_asks() {
        let fake = Fake::default();
        let t = token(NOW + 60);
        let out = ask(&fake, SECRET, &t, NOW).await;
        assert_eq!(
            out,
            Outcome::Confirm {
                team: "Example <Team>".into()
            }
        );
        // A mail scanner opening the link changes nothing.
        assert!(fake.calls.lock().unwrap().is_empty());
        let resp = page(&out);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::REFERRER_POLICY], "no-referrer");
        assert_eq!(resp.headers()[header::CACHE_CONTROL], "no-store");
        let html = body(resp).await;
        assert!(html.contains("Unsubscribe from weekly summaries for Example &lt;Team&gt;?"));
        // One button that posts back to this same address (token and all).
        assert_eq!(html.matches("<form").count(), 1);
        assert!(html.contains(r#"<form method="post"><button"#));
        assert!(!html.contains("action="));
        assert!(!html.contains(t.as_str()), "the page repeats the token");
    }

    #[tokio::test]
    async fn posting_a_good_link_turns_off_only_that_persons_summary() {
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
        let html = body(resp).await;
        assert!(html.contains("You're unsubscribed"));
        assert!(html.contains("Turn it back on"));
        assert!(html.contains(&notifications_url()));
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
            for out in [
                ask(&fake, SECRET, t, NOW).await,
                handle(&fake, SECRET, t, NOW).await,
            ] {
                assert_eq!(out, Outcome::Invalid, "{t}");
                let resp = page(&out);
                assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
                let html = body(resp).await;
                if !t.is_empty() {
                    assert!(!html.contains(t.as_str()), "the page repeats the token");
                }
                pages.push(html);
            }
        }
        assert!(fake.calls.lock().unwrap().is_empty());
        // No hint about which check failed.
        assert!(pages.windows(2).all(|w| w[0] == w[1]));
        assert!(!pages[0].contains("Example"));
        assert!(!pages[0].contains("<form"));
    }

    #[tokio::test]
    async fn a_test_link_changes_nothing() {
        let fake = Fake::default();
        for out in [
            ask(&fake, SECRET, unsubscribe::TEST_TOKEN, NOW).await,
            handle(&fake, SECRET, unsubscribe::TEST_TOKEN, NOW).await,
        ] {
            assert_eq!(out, Outcome::Test);
            assert!(body(page(&out)).await.contains("test"));
        }
        assert!(fake.calls.lock().unwrap().is_empty());
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

    /// `UNSUBSCRIBE_PREVIEW=/path/dir cargo test -p gateway unsubscribe_preview -- --ignored`
    /// writes each page as HTML, to look at in a browser.
    #[tokio::test]
    #[ignore]
    async fn unsubscribe_preview() {
        let dir = std::env::var("UNSUBSCRIBE_PREVIEW").expect("UNSUBSCRIBE_PREVIEW dir");
        for (name, out) in [
            (
                "confirm",
                Outcome::Confirm {
                    team: "Example Team".into(),
                },
            ),
            ("done", Outcome::Unsubscribed),
            ("invalid", Outcome::Invalid),
        ] {
            std::fs::write(
                format!("{dir}/unsubscribe-{name}.html"),
                body(page(&out)).await,
            )
            .unwrap();
        }
    }
}
