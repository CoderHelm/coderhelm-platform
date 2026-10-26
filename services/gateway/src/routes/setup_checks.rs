//! Setup checks for a self-hosted install: is everything it needs in place,
//! and when something isn't, what exactly to do about it.
//!
//! Three routes:
//!
//! - `GET /api/system/checks` (install admins): every server-side check,
//!   each with a status (`pass`, `warn`, `fail`, `skip`), what is wrong in
//!   one sentence, why it matters in one sentence, and the fix (a command
//!   to copy, a link, or a dashboard page). Settings, then Setup, shows it.
//! - `GET /api/system/preflight` (install admins): what would block an
//!   update right now, and what to do about each (Settings, then System).
//! - `GET /api/setup/status` (public, no session): the install guide's view
//!   during the first install. Only while the setup window is open (until
//!   the first person has an account), and only check ids, labels, statuses
//!   and codes: no account or app ids, keys, addresses, emails or URLs.
//!
//! The checks are pure functions over what the AWS and GitHub calls
//! answered (tested below with made-up answers); the handlers only gather
//! those answers. The CLI's `doctor` command (installer/lib/checks.mjs)
//! runs the same checks from the operator's machine with their AWS
//! credentials, plus the ones only that machine can run (stacks, CDK).
//! The contract is documented in docs/self-host/check-your-install.md.
//! (The GitHub App manifest landing, `/api/setup/github/callback`, lives in
//! routes::setup.)

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::{extract::State, http::StatusCode, Extension, Json};
use serde::Serialize;
use serde_json::{json, Value};
use tracing::warn;

use super::api::is_platform_admin;
use crate::models::Claims;
use crate::AppState;

/// The command that runs the CLI from any machine (Node.js 20+).
pub const CLI: &str = "npx -y github:CoderHelm/coderhelm-platform";

// ── The shape of a check ──────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Pass,
    Warn,
    Fail,
    /// Not set up, and not required (an optional integration).
    Skip,
}

/// How to fix a check: plain instructions, and any of a command to copy, a
/// link outside CoderHelm, or a dashboard page.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Fix {
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub href: Option<String>,
}

impl Fix {
    fn text(t: impl Into<String>) -> Self {
        Fix {
            text: t.into(),
            ..Default::default()
        }
    }
    fn command(mut self, c: impl Into<String>) -> Self {
        self.command = Some(c.into());
        self
    }
    fn url(mut self, u: impl Into<String>) -> Self {
        self.url = Some(u.into());
        self
    }
    fn href(mut self, h: impl Into<String>) -> Self {
        self.href = Some(h.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Check {
    pub key: &'static str,
    pub group: &'static str,
    pub label: &'static str,
    pub status: Status,
    /// What is true now, in one sentence (what is wrong, when it is).
    pub detail: String,
    /// Why it matters, in one sentence (empty when it passes).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub why: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix: Option<Fix>,
}

impl Check {
    fn pass(
        key: &'static str,
        group: &'static str,
        label: &'static str,
        detail: impl Into<String>,
    ) -> Self {
        Check {
            key,
            group,
            label,
            status: Status::Pass,
            detail: detail.into(),
            why: String::new(),
            fix: None,
        }
    }
    fn not_ok(
        status: Status,
        key: &'static str,
        group: &'static str,
        label: &'static str,
        detail: impl Into<String>,
        why: impl Into<String>,
        fix: Fix,
    ) -> Self {
        Check {
            key,
            group,
            label,
            status,
            detail: detail.into(),
            why: why.into(),
            fix: Some(fix),
        }
    }
}

/// Counts and the verdict: ready when nothing fails.
pub fn summary(checks: &[Check]) -> Value {
    let n = |s: Status| checks.iter().filter(|c| c.status == s).count();
    json!({
        "pass": n(Status::Pass),
        "warn": n(Status::Warn),
        "fail": n(Status::Fail),
        "skip": n(Status::Skip),
        "ready": n(Status::Fail) == 0,
    })
}

// ── GitHub ────────────────────────────────────────────────────────────────

/// What the app must be allowed to do (installer/lib/github-app.mjs
/// PERMISSIONS, which creates it).
pub const REQUIRED_PERMISSIONS: &[(&str, &str, &str)] = &[
    ("contents", "write", "Contents"),
    ("pull_requests", "write", "Pull requests"),
    ("issues", "write", "Issues"),
    ("checks", "read", "Checks"),
    ("actions", "read", "Actions"),
    ("statuses", "read", "Commit statuses"),
    ("workflows", "write", "Workflows"),
    ("metadata", "read", "Metadata"),
    ("email_addresses", "read", "Email addresses"),
];

/// The webhook events the gateway handles.
pub const REQUIRED_EVENTS: &[&str] = &[
    "issues",
    "issue_comment",
    "pull_request",
    "pull_request_review",
    "pull_request_review_comment",
    "pull_request_review_thread",
    "push",
    "check_run",
    "check_suite",
    "workflow_run",
    "repository",
];

const G: &str = "GitHub";

fn level(p: &str) -> u8 {
    match p {
        "admin" => 3,
        "write" => 2,
        "read" => 1,
        _ => 0,
    }
}

/// Where the app's settings are on github.com.
pub fn app_settings_url(app: &Value) -> String {
    let slug = app["slug"].as_str().unwrap_or("");
    let owner = app["owner"]["login"].as_str().unwrap_or("");
    if app["owner"]["type"].as_str() == Some("Organization") && !owner.is_empty() {
        format!("https://github.com/organizations/{owner}/settings/apps/{slug}")
    } else {
        format!("https://github.com/settings/apps/{slug}")
    }
}

/// The app's keys are stored at all.
pub fn github_keys(app_id: &str, private_key: &str) -> Check {
    if !app_id.trim().is_empty() && private_key.contains("PRIVATE KEY") {
        return Check::pass(
            "github_keys",
            G,
            "GitHub App keys stored",
            format!("App {app_id}."),
        );
    }
    Check::not_ok(
        Status::Fail,
        "github_keys",
        G,
        "GitHub App keys stored",
        "No GitHub App is stored for this install.",
        "Without its app, CoderHelm can't read code, open pull requests or receive events.",
        Fix::text("Create the app from a machine with a browser (one click on github.com).")
            .command(format!("{CLI} github-app")),
    )
}

/// GitHub's answer to `GET /app` signed with the stored id and key.
pub fn github_app(status: Option<u16>) -> Check {
    match status {
        Some(200) => Check::pass("github_app", G, "GitHub accepts the app's keys", "GitHub answered as the app."),
        Some(401) | Some(403) | Some(404) => Check::not_ok(
            Status::Fail,
            "github_app",
            G,
            "GitHub accepts the app's keys",
            "GitHub refused the stored app id and private key.",
            "Every call CoderHelm makes as the app will fail until they match.",
            Fix::text("Store the right app id and a new private key (App settings, Private keys, Generate), or create a new app.")
                .command(format!("{CLI} github-app --force")),
        ),
        Some(other) => Check::not_ok(
            Status::Warn,
            "github_app",
            G,
            "GitHub accepts the app's keys",
            format!("GitHub answered {other}; the keys could not be checked."),
            "The check runs again each time this page opens.",
            Fix::text("Check GitHub's status, then check again.").url("https://www.githubstatus.com"),
        ),
        None => Check::not_ok(
            Status::Warn,
            "github_app",
            G,
            "GitHub accepts the app's keys",
            "GitHub could not be reached from the install.",
            "CoderHelm needs to reach api.github.com to work.",
            Fix::text("Check GitHub's status and the install's outbound network, then check again.")
                .url("https://www.githubstatus.com"),
        ),
    }
}

/// The app's permissions and events against what CoderHelm needs.
pub fn github_permissions(app: &Value) -> Check {
    let have = &app["permissions"];
    let mut problems: Vec<String> = Vec::new();
    for (key, need, name) in REQUIRED_PERMISSIONS {
        let got = have[*key].as_str().unwrap_or("none");
        if level(got) < level(need) {
            problems.push(if got == "none" {
                format!("{name}: {need} (missing)")
            } else {
                format!("{name}: {need} (now {got})")
            });
        }
    }
    let events: Vec<&str> = app["events"]
        .as_array()
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let missing_events: Vec<&str> = REQUIRED_EVENTS
        .iter()
        .copied()
        .filter(|e| !events.contains(e))
        .collect();
    if problems.is_empty() && missing_events.is_empty() {
        return Check::pass(
            "github_permissions",
            G,
            "GitHub App permissions and events",
            "Every permission and event CoderHelm uses is granted.",
        );
    }
    let mut detail = String::new();
    if !problems.is_empty() {
        detail.push_str(&format!("Permissions to change: {}.", problems.join(", ")));
    }
    if !missing_events.is_empty() {
        if !detail.is_empty() {
            detail.push(' ');
        }
        detail.push_str(&format!(
            "Events to subscribe to: {}.",
            missing_events.join(", ")
        ));
    }
    Check::not_ok(
        Status::Fail,
        "github_permissions",
        G,
        "GitHub App permissions and events",
        detail,
        "Without them CoderHelm can't push some changes or misses the events that start work.",
        Fix::text("Open the app's Permissions and events page, set the values listed, save, then accept the new permissions on each installation.")
            .url(format!("{}/permissions", app_settings_url(app))),
    )
}

/// The app is installed on at least one account or organization.
pub fn github_installations(count: Option<usize>, app: &Value) -> Check {
    let install_url = app["html_url"]
        .as_str()
        .map(|u| format!("{u}/installations/new"))
        .unwrap_or_else(|| "https://github.com/settings/installations".into());
    match count {
        Some(0) => Check::not_ok(
            Status::Fail,
            "github_installations",
            G,
            "GitHub App installed",
            "The app isn't installed on any account or organization yet.",
            "CoderHelm only sees repositories the app is installed on.",
            Fix::text("Install the app and pick the repositories CoderHelm should work on.")
                .url(install_url),
        ),
        Some(n) => Check::pass(
            "github_installations",
            G,
            "GitHub App installed",
            format!("Installed on {n} account{}.", if n == 1 { "" } else { "s" }),
        ),
        None => Check::not_ok(
            Status::Warn,
            "github_installations",
            G,
            "GitHub App installed",
            "Could not list the app's installations.",
            "The check runs again each time this page opens.",
            Fix::text("Check again in a minute.").url(install_url),
        ),
    }
}

/// One webhook delivery, as GitHub lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub status_code: u16,
    pub event: String,
    pub delivered_at: String,
}

/// The webhook URL points here, and recent deliveries arrived.
pub fn github_webhook(
    config: &Value,
    expected_url: &str,
    deliveries: Option<&[Delivery]>,
    app: &Value,
) -> Check {
    let hook_url = format!("{}/advanced", app_settings_url(app));
    let got = config["url"].as_str().unwrap_or("").trim_end_matches('/');
    if got != expected_url {
        return Check::not_ok(
            Status::Fail,
            "github_webhook",
            G,
            "GitHub webhook reaches this install",
            format!(
                "The app's webhook URL is {}, not {expected_url}.",
                if got.is_empty() { "empty" } else { got }
            ),
            "GitHub sends issue, pull request and CI events there, so CoderHelm never hears about them.",
            Fix::text(format!("In the app's General settings, set Webhook URL to {expected_url} and save."))
                .url(app_settings_url(app)),
        );
    }
    let Some(list) = deliveries else {
        return Check::pass(
            "github_webhook",
            G,
            "GitHub webhook reaches this install",
            "The webhook URL is right.",
        );
    };
    let Some(last) = list.first() else {
        return Check::not_ok(
            Status::Warn,
            "github_webhook",
            G,
            "GitHub webhook reaches this install",
            "The webhook URL is right, but GitHub hasn't delivered an event yet.",
            "The first event arrives when the app is installed or an issue changes.",
            Fix::text("Install the app on a repository, or open an issue, then check again.")
                .url(hook_url),
        );
    };
    match last.status_code {
        200..=299 => Check::pass(
            "github_webhook",
            G,
            "GitHub webhook reaches this install",
            format!("The last event ({}) arrived at {}.", last.event, last.delivered_at),
        ),
        401 => Check::not_ok(
            Status::Fail,
            "github_webhook",
            G,
            "GitHub webhook reaches this install",
            "The install refused GitHub's last event: the webhook secret doesn't match.",
            "Every event is refused until the secret on GitHub and the stored one are the same.",
            Fix::text("Store the app again so both sides get the same secret, then redeliver the event from the app's Advanced page.")
                .command(format!("{CLI} github-app --force"))
                .url(hook_url),
        ),
        0 => Check::not_ok(
            Status::Fail,
            "github_webhook",
            G,
            "GitHub webhook reaches this install",
            "GitHub couldn't connect to the install for the last event.",
            "Events never arrive while the address doesn't answer.",
            Fix::text("Check that the install's address opens in a browser, then redeliver the event.").url(hook_url),
        ),
        code => Check::not_ok(
            Status::Fail,
            "github_webhook",
            G,
            "GitHub webhook reaches this install",
            format!("The install answered GitHub's last event ({}) with {code}.", last.event),
            "Events that fail are lost unless redelivered.",
            Fix::text("Open the delivery on the app's Advanced page to see the answer, fix it, then redeliver.").url(hook_url),
        ),
    }
}

// ── Email ─────────────────────────────────────────────────────────────────

const E: &str = "Email";

/// What SES says about the sender identity.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Identity {
    pub verified: bool,
    /// SUCCESS, PENDING, FAILED, TEMPORARY_FAILURE, NOT_STARTED.
    pub dkim_status: String,
    pub dkim_tokens: Vec<String>,
}

/// The domain SES verifies, from the sender address.
pub fn sender_domain(from: &str) -> &str {
    from.rsplit_once('@').map(|(_, d)| d).unwrap_or(from)
}

/// The sender identity is verified, DKIM included.
pub fn email_identity(from: &str, region: &str, id: Option<&Identity>) -> Check {
    let domain = sender_domain(from);
    let console =
        format!("https://console.aws.amazon.com/ses/home?region={region}#/identities/{domain}");
    let Some(id) = id else {
        return Check::not_ok(
            Status::Fail,
            "email_identity",
            E,
            "Email sender verified",
            format!("{domain} isn't an identity in Amazon SES in {region}."),
            "SES refuses to send from an address it hasn't verified.",
            Fix::text(format!("Create the identity for {domain} in SES, then add the three DKIM records it shows."))
                .command(format!("aws sesv2 create-email-identity --email-identity {domain} --region {region}"))
                .url(console),
        );
    };
    if id.verified && id.dkim_status.eq_ignore_ascii_case("success") {
        return Check::pass(
            "email_identity",
            E,
            "Email sender verified",
            format!("{domain} is verified, with DKIM."),
        );
    }
    let records = id
        .dkim_tokens
        .iter()
        .map(|t| format!("{t}._domainkey.{domain} CNAME {t}.dkim.amazonses.com"))
        .collect::<Vec<_>>()
        .join("\n");
    let detail = if id.verified {
        format!(
            "{domain} is verified, but DKIM is {}.",
            id.dkim_status.to_lowercase().replace('_', " ")
        )
    } else {
        format!(
            "{domain} isn't verified yet (DKIM {}).",
            id.dkim_status.to_lowercase().replace('_', " ")
        )
    };
    Check::not_ok(
        if id.verified {
            Status::Warn
        } else {
            Status::Fail
        },
        "email_identity",
        E,
        "Email sender verified",
        detail,
        "Mail without DKIM lands in spam, and SES won't send at all from an unverified domain.",
        Fix {
            text: if records.is_empty() {
                format!(
                    "Open {domain} in SES and add the DKIM records it shows at your DNS provider."
                )
            } else {
                format!("Add these DNS records for {domain} at your DNS provider; SES checks them within an hour:")
            },
            command: (!records.is_empty()).then_some(records),
            url: Some(console),
            href: None,
        },
    )
}

/// Out of the SES sandbox (production access).
pub fn email_sandbox(region: &str, production: Option<bool>) -> Check {
    match production {
        Some(true) => Check::pass("email_sandbox", E, "Email can reach anyone", "SES production access is on."),
        Some(false) => Check::not_ok(
            Status::Warn,
            "email_sandbox",
            E,
            "Email can reach anyone",
            "This AWS account is in the SES sandbox: email only reaches verified addresses, 200 a day.",
            "Invites and run results won't reach your teammates until production access is granted.",
            Fix::text("Request production access (Account dashboard, Request production access); AWS usually answers within a day.")
                .url(format!("https://console.aws.amazon.com/ses/home?region={region}#/account")),
        ),
        None => Check::not_ok(
            Status::Warn,
            "email_sandbox",
            E,
            "Email can reach anyone",
            "Could not read the SES account.",
            "The check runs again each time this page opens.",
            Fix::text("Send a test email from this page to check by hand.").href("/settings/setup#getting-started"),
        ),
    }
}

// ── Models ────────────────────────────────────────────────────────────────

const M: &str = "Claude";

/// What a one-token call answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelProbe {
    Ok,
    /// The team has no key yet (Anthropic API).
    NoKey,
    /// The API's status, and what it said.
    Refused(u16, String),
    Unreachable(String),
}

/// Bedrock's console page where model access is turned on.
pub fn bedrock_access_url(region: &str) -> String {
    format!("https://console.aws.amazon.com/bedrock/home?region={region}#/modelaccess")
}

pub fn model(bedrock: bool, region: &str, model_id: &str, probe: &ModelProbe) -> Check {
    let label = "Claude answers a test call";
    match probe {
        ModelProbe::Ok => Check::pass(
            "model",
            M,
            label,
            format!(
                "{model_id} answered through {}.",
                if bedrock {
                    "Amazon Bedrock"
                } else {
                    "the Anthropic API"
                }
            ),
        ),
        ModelProbe::NoKey => Check::not_ok(
            Status::Fail,
            "model",
            M,
            label,
            "Your team hasn't added an Anthropic API key.",
            "Runs, reviews and chat all call Claude with the team's key.",
            Fix::text("Add a key from console.anthropic.com in Settings, AI models.")
                .href("/settings/models")
                .url("https://console.anthropic.com/settings/keys"),
        ),
        ModelProbe::Refused(code, msg) if bedrock => {
            let access = msg.to_ascii_lowercase();
            let detail = if access.contains("access") || *code == 403 {
                format!("Bedrock refused {model_id} in {region}: model access isn't enabled.")
            } else {
                format!("Bedrock refused {model_id} in {region} ({code}).")
            };
            Check::not_ok(
                Status::Fail,
                "model",
                M,
                label,
                detail,
                "Every run calls this model; none can start until Bedrock allows it.",
                Fix::text(format!(
                    "Open Model access in the Bedrock console for {region} and enable the Anthropic models (Claude Opus 5.5 and Claude Sonnet 5.5), then check again."
                ))
                .url(bedrock_access_url(region)),
            )
        }
        ModelProbe::Refused(401, _) | ModelProbe::Refused(403, _) => Check::not_ok(
            Status::Fail,
            "model",
            M,
            label,
            "Anthropic refused your team's API key.",
            "Every run fails until the key works.",
            Fix::text("Create a new key and save it in Settings, AI models.")
                .href("/settings/models")
                .url("https://console.anthropic.com/settings/keys"),
        ),
        ModelProbe::Refused(code, _) => Check::not_ok(
            if common::model_retry::is_transient_status(*code) {
                Status::Warn
            } else {
                Status::Fail
            },
            "model",
            M,
            label,
            format!("The test call to {model_id} answered {code}."),
            "Runs make the same call and will fail the same way.",
            Fix::text(
                "Check the model chosen in Settings, AI models, and the provider's status page.",
            )
            .href("/settings/models")
            .url(if bedrock {
                "https://health.aws.amazon.com/health/status"
            } else {
                "https://status.anthropic.com"
            }),
        ),
        ModelProbe::Unreachable(_) => Check::not_ok(
            Status::Warn,
            "model",
            M,
            label,
            "The test call couldn't reach the model provider.",
            "Runs need to reach it too.",
            Fix::text("Check the provider's status, then check again.").url(if bedrock {
                "https://health.aws.amazon.com/health/status"
            } else {
                "https://status.anthropic.com"
            }),
        ),
    }
}

// ── The install itself ────────────────────────────────────────────────────

const I: &str = "Install";

pub fn dlq(count: Option<u64>) -> Check {
    match count {
        Some(0) => Check::pass(
            "dlq",
            I,
            "No failed events waiting",
            "The dead-letter queue is empty.",
        ),
        Some(n) => Check::not_ok(
            Status::Warn,
            "dlq",
            I,
            "No failed events waiting",
            format!(
                "{n} event{} failed and wait in the dead-letter queue.",
                if n == 1 { "" } else { "s" }
            ),
            "Each one is work CoderHelm didn't do, such as a ticket that never started.",
            Fix::text("Open the failed events page to see each one, then retry or clear it.")
                .href("/admin/queue"),
        ),
        None => Check::not_ok(
            Status::Warn,
            "dlq",
            I,
            "No failed events waiting",
            "Could not read the dead-letter queue.",
            "The check runs again each time this page opens.",
            Fix::text("Open the failed events page.").href("/admin/queue"),
        ),
    }
}

/// The updater answers, and whether auto-update is on.
pub fn updates(status: Option<&Value>) -> Check {
    match status {
        None => Check::not_ok(
            Status::Warn,
            "updates",
            I,
            "Updater reachable",
            "The updater didn't answer.",
            "You won't hear about new releases, and updates from this page can't start.",
            Fix::text("Redeploy the install; it recreates the updater.")
                .command(format!("{CLI} deploy")),
        ),
        Some(v) if v["settings"]["auto"].as_bool() == Some(true) => Check::pass(
            "updates",
            I,
            "Updater reachable",
            format!(
                "Auto-update is on: {} {:02}:00 {}.",
                v["settings"]["day"].as_str().unwrap_or("sun"),
                v["settings"]["hour"].as_u64().unwrap_or(3),
                v["settings"]["timezone"].as_str().unwrap_or("UTC")
            ),
        ),
        Some(_) => Check::pass(
            "updates",
            I,
            "Updater reachable",
            "Auto-update is off; admins are emailed about each new release.",
        ),
    }
}

/// The public address answers over HTTPS. `problem` is set when a
/// self-hosted install doesn't know its own address
/// ([`common::public_urls::problem`]): its links would be relative, so this
/// fails rather than linking anywhere else.
pub fn public_url(url: &str, status: Option<u16>, problem: Option<&str>) -> Check {
    if let Some(p) = problem {
        return Check::not_ok(
            Status::Fail,
            "url",
            I,
            "Web address answers",
            p.to_string(),
            "Links in emails, Slack, Teams and GitHub point at this address.",
            Fix::text("Deploy again from your machine: the installer sets the address.")
                .command(format!("{CLI} deploy")),
        );
    }
    match status {
        Some(200..=399) => Check::pass(
            "url",
            I,
            "Web address answers",
            format!("{url} answers over HTTPS."),
        ),
        Some(code) => Check::not_ok(
            Status::Fail,
            "url",
            I,
            "Web address answers",
            format!("{url} answered {code}."),
            "People and GitHub reach the install at this address.",
            Fix::text(
                "Run the doctor from your machine: it checks DNS, the certificate and CloudFront.",
            )
            .command(format!("{CLI} doctor")),
        ),
        None => Check::not_ok(
            Status::Fail,
            "url",
            I,
            "Web address answers",
            format!("{url} didn't answer (DNS, certificate or network)."),
            "People and GitHub reach the install at this address.",
            Fix::text(
                "Run the doctor from your machine: it shows the expected and current DNS records.",
            )
            .command(format!("{CLI} doctor")),
        ),
    }
}

// ── Integrations (optional) ───────────────────────────────────────────────

const X: &str = "Integrations";

/// A chat channel (Slack or Teams): saved, and posting on.
pub fn chat(key: &'static str, name: &'static str, saved: bool, enabled: bool) -> Check {
    let label = if key == "slack" {
        "Slack"
    } else {
        "Microsoft Teams"
    };
    match (saved, enabled) {
        (false, _) => Check::not_ok(
            Status::Skip,
            key,
            X,
            label,
            "Not connected (optional).",
            format!("Connect {name} to get alerts and pull request links there."),
            Fix::text(format!(
                "Paste a {name} incoming webhook URL in Settings, Alerts."
            ))
            .href("/settings/alerts"),
        ),
        (true, false) => Check::not_ok(
            Status::Warn,
            key,
            X,
            label,
            "A channel is saved but posting is off.",
            "Nothing is posted until it is on.",
            Fix::text("Turn posting on and send a test message in Settings, Alerts.")
                .href("/settings/alerts"),
        ),
        (true, true) => Check::pass(key, X, label, "A channel is saved and posting is on."),
    }
}

pub fn jira(connected: bool) -> Check {
    if connected {
        Check::pass("jira", X, "Jira", "Connected.")
    } else {
        Check::not_ok(
            Status::Skip,
            "jira",
            X,
            "Jira",
            "Not connected (optional).",
            "Connect Jira to start work from tickets.",
            Fix::text("Connect from Settings, Integrations, Jira (a webhook, or your own copy of the Forge app).")
                .href("/settings/integrations/jira"),
        )
    }
}

// ── Your own domain ───────────────────────────────────────────────────────
//
// The updater requests the certificate and moves the domain along
// (lambda/updater, `domain_status`); these checks look at what the rest of
// the world sees: the DNS records (asked over DNS-over-HTTPS, from the
// install), the certificate, and whether CoderHelm answers on the hostname
// over HTTPS. docs/self-host/domains.md lists every code and its fix.

const D: &str = "Domain";

/// The certificate authorities whose CAA names let Amazon issue certificates.
pub const AMAZON_CAA: &[&str] = &[
    "amazon.com",
    "amazontrust.com",
    "awstrust.com",
    "amazonaws.com",
];

/// One DNS record to add (name, type, value), as the updater lists it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct DnsRecord {
    pub purpose: String,
    pub name: String,
    #[serde(rename = "type")]
    pub rtype: String,
    pub value: String,
}

/// What public DNS answered (None: it couldn't be asked).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DnsAnswers {
    /// The CNAME at the certificate's validation name.
    pub validation_cname: Option<Vec<String>>,
    /// The CNAME at the hostname.
    pub host_cname: Option<Vec<String>>,
    /// The hostname's A records (after any CNAME).
    pub host_a: Option<Vec<String>>,
    /// The hostname is a zone apex (it has its own SOA).
    pub apex: bool,
    /// The closest CAA record set above the hostname (None: none found).
    pub caa: Option<Vec<String>>,
    /// The zone's name servers, as public DNS answers (Route 53 zones).
    pub zone_ns: Option<Vec<String>>,
}

/// Everything the domain checks decide from.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DomainFacts {
    /// None: the updater didn't answer.
    pub known: bool,
    pub hostname: Option<String>,
    /// none, validating, attaching, live, failed, removing.
    pub state: String,
    /// route53, external or config.
    pub mode: String,
    pub reason: String,
    /// CloudFront's own hostname, the value of the address record.
    pub target: Option<String>,
    /// The hostname the install answers on now.
    pub current: Option<String>,
    pub cert_status: Option<String>,
    pub cert_failure: Option<String>,
    pub validation: Option<DnsRecord>,
    /// The Route 53 zone's own name servers.
    pub name_servers: Vec<String>,
    pub zone: Option<String>,
    pub records: Vec<DnsRecord>,
    pub dns: DnsAnswers,
    /// GET https://<hostname>/ answered this.
    pub https: Option<u16>,
    /// GET https://<hostname>/api/setup/status?probe=1 answered as CoderHelm.
    pub api: Option<bool>,
}

fn norm(s: &str) -> String {
    s.trim().trim_end_matches('.').to_ascii_lowercase()
}

/// The facts from the updater's `domain_status` answer.
pub fn domain_facts_from(v: Option<&Value>) -> DomainFacts {
    let Some(v) = v.filter(|v| v.get("ok").and_then(Value::as_bool) != Some(false)) else {
        return DomainFacts::default();
    };
    let s = |k: &str| v[k].as_str().map(norm).filter(|x| !x.is_empty());
    let rec = |r: &Value| DnsRecord {
        purpose: r["purpose"].as_str().unwrap_or("").to_string(),
        name: norm(r["name"].as_str().unwrap_or("")),
        rtype: r["type"].as_str().unwrap_or("CNAME").to_string(),
        value: norm(r["value"].as_str().unwrap_or("")),
    };
    let cert = &v["certificate"];
    DomainFacts {
        known: true,
        hostname: s("hostname"),
        state: v["state"].as_str().unwrap_or("none").to_string(),
        mode: v["mode"].as_str().unwrap_or("").to_string(),
        reason: v["reason"].as_str().unwrap_or("").to_string(),
        target: s("target"),
        current: s("current"),
        cert_status: cert["status"].as_str().map(str::to_string),
        cert_failure: cert["failure_reason"].as_str().map(str::to_string),
        validation: cert["validation"]
            .is_object()
            .then(|| rec(&cert["validation"])),
        name_servers: v["name_servers"]
            .as_array()
            .map(|a| a.iter().filter_map(Value::as_str).map(norm).collect())
            .unwrap_or_default(),
        zone: s("zone"),
        records: v["records"]
            .as_array()
            .map(|a| a.iter().map(rec).collect())
            .unwrap_or_default(),
        ..Default::default()
    }
}

/// Whether a CAA record set lets Amazon issue a certificate. No `issue`
/// tag at all means any authority may.
pub fn caa_allows_amazon(records: &[String]) -> bool {
    let issuers: Vec<String> = records
        .iter()
        .filter_map(|r| {
            let mut parts = r.split_whitespace();
            let _flags = parts.next()?;
            let tag = parts.next()?.to_ascii_lowercase();
            let value: String = parts.collect::<Vec<_>>().join(" ");
            (tag == "issue").then(|| {
                value
                    .trim_matches('"')
                    .split(';')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_ascii_lowercase()
            })
        })
        .collect();
    issuers.is_empty() || issuers.iter().any(|i| AMAZON_CAA.contains(&i.as_str()))
}

fn same_set(a: &[String], b: &[String]) -> bool {
    let mut x: Vec<String> = a.iter().map(|s| norm(s)).collect();
    let mut y: Vec<String> = b.iter().map(|s| norm(s)).collect();
    x.sort();
    x.dedup();
    y.sort();
    y.dedup();
    !x.is_empty() && x == y
}

/// The decision: (status, code). The guide, the dashboard and the doctor
/// show one sentence and one fix for each code.
pub fn domain_code(f: &DomainFacts) -> (&'static str, &'static str) {
    if !f.known {
        return ("warn", "unknown");
    }
    let Some(host) = f.hostname.as_deref() else {
        return ("skip", "none");
    };
    let dns = &f.dns;
    let caa_blocks = dns.caa.as_deref().is_some_and(|c| !caa_allows_amazon(c));
    match f.state.as_str() {
        "removing" => return ("wait", "removing"),
        "attaching" => return ("wait", "attaching"),
        "failed" => {
            let reason = format!("{} {}", f.reason, f.cert_failure.as_deref().unwrap_or(""));
            if reason.contains("CAA") || caa_blocks {
                return ("fail", "caa_blocked");
            }
            let cert_failed = matches!(
                f.cert_status.as_deref(),
                Some("FAILED" | "VALIDATION_TIMED_OUT" | "REVOKED" | "EXPIRED")
            );
            return if cert_failed {
                ("fail", "certificate_failed")
            } else {
                ("fail", "attach_failed")
            };
        }
        "validating" => {
            if caa_blocks {
                return ("fail", "caa_blocked");
            }
            if f.mode == "route53" {
                if let Some(public) = dns.zone_ns.as_deref() {
                    if !f.name_servers.is_empty() && !same_set(public, &f.name_servers) {
                        return ("fail", "zone_not_delegated");
                    }
                }
                return ("wait", "validating");
            }
            if let (Some(found), Some(want)) =
                (dns.validation_cname.as_deref(), f.validation.as_ref())
            {
                if let Some(first) = found.first() {
                    if norm(first) != want.value {
                        return ("fail", "validation_wrong_value");
                    }
                }
            }
            return ("wait", "validating");
        }
        _ => {}
    }
    // Live (or set in the config): what visitors see.
    let cname = dns
        .host_cname
        .as_deref()
        .and_then(|c| c.first())
        .map(|c| norm(c));
    if let (Some(c), Some(t)) = (cname.as_deref(), f.target.as_deref()) {
        if c != t && f.mode != "route53" {
            return ("fail", "dns_wrong_value");
        }
    }
    let resolves = cname.is_some() || dns.host_a.as_deref().is_some_and(|a| !a.is_empty());
    let asked = dns.host_cname.is_some() || dns.host_a.is_some();
    match f.https {
        Some(200..=399) => {}
        _ if asked && !resolves => {
            return if dns.apex && f.mode != "route53" {
                ("fail", "cname_at_apex")
            } else {
                ("wait", "dns_missing")
            };
        }
        _ => return ("fail", "https_failed"),
    }
    let _ = host;
    if f.api == Some(true) {
        ("pass", "ok")
    } else {
        ("fail", "api_unreachable")
    }
}

/// The records as lines to paste into a DNS provider's form.
pub fn records_text(records: &[DnsRecord]) -> String {
    records
        .iter()
        .map(|r| format!("{}  {}  {}", r.name, r.rtype, r.value))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The domain, for Settings, then Setup and the doctor: one sentence, why,
/// and the fix (the records to add, in `command`).
pub fn domain(f: &DomainFacts) -> Check {
    let label = "Your own domain";
    let (_, code) = domain_code(f);
    let host = f.hostname.clone().unwrap_or_default();
    let target = f
        .target
        .clone()
        .unwrap_or_else(|| "your CloudFront address".into());
    let page = "/settings/system#domain";
    let status = |s: &str| match s {
        "pass" => Status::Pass,
        "skip" => Status::Skip,
        "fail" => Status::Fail,
        _ => Status::Warn,
    };
    let (st, _) = domain_code(f);
    let check = |detail: String, why: &str, fix: Fix| {
        Check::not_ok(status(st), "domain", D, label, detail, why, fix)
    };
    let address = DnsRecord {
        purpose: "address".into(),
        name: host.clone(),
        rtype: "CNAME".into(),
        value: target.clone(),
    };
    match code {
        "ok" => Check::pass(
            "domain",
            D,
            label,
            format!("https://{host} answers with its certificate, and so does the API."),
        ),
        "none" => check(
            format!(
                "CoderHelm uses its AWS address{} (optional: use your own domain).",
                f.current.as_deref().map(|c| format!(" {c}")).unwrap_or_default()
            ),
            "A domain of your own is easier to remember and share, and Google sign-in needs one.",
            Fix::text("Add it in Settings, then System. With Route 53 in this account it is set up for you; otherwise you add two DNS records.")
                .href(page),
        ),
        "validating" if f.mode == "route53" => check(
            format!("The certificate for {host} is waiting for its DNS check; the record is in your Route 53 zone."),
            "CloudFront needs the certificate before it can answer on your domain.",
            Fix::text("Nothing to do: AWS usually issues it within 30 minutes, then CoderHelm moves to the domain by itself.").href(page),
        ),
        "validating" => {
            let mut fix = Fix::text("Add these DNS records where your domain's DNS is hosted. AWS checks every few minutes; new records can take up to an hour to show up.")
                .href(page);
            fix.command = (!f.records.is_empty()).then(|| records_text(&f.records));
            check(
                format!("The certificate for {host} is waiting for its DNS record."),
                "CloudFront needs the certificate before it can answer on your domain.",
                fix,
            )
        }
        "zone_not_delegated" => {
            let mut fix = Fix::text(format!(
                "At the registrar where you bought {}, set the name servers to the ones below. The change can take up to a day.",
                f.zone.as_deref().unwrap_or(&host)
            ))
            .href(page);
            fix.command = Some(f.name_servers.join("\n"));
            check(
                format!("Public DNS doesn't use the Route 53 zone {} yet.", f.zone.as_deref().unwrap_or("")),
                "Nothing in the zone is seen until the registrar points at it, so the certificate can't be checked.",
                fix,
            )
        }
        "validation_wrong_value" => {
            let want = f.validation.clone().unwrap_or_default();
            let found = f.dns.validation_cname.as_ref().and_then(|v| v.first()).cloned().unwrap_or_default();
            let mut fix = Fix::text(format!("Change the record's value to {} and save.", want.value)).href(page);
            fix.command = Some(records_text(std::slice::from_ref(&want)));
            check(
                format!("{} points to {}, not to the value AWS gave.", want.name, norm(&found)),
                "AWS only issues the certificate when the value matches exactly.",
                fix,
            )
        }
        "caa_blocked" => check(
            format!("Your domain's CAA records don't allow Amazon to issue a certificate for {host}."),
            "Without a certificate CloudFront can't answer on your domain.",
            {
                let mut fix = Fix::text("Add this CAA record next to the ones you have, then press Try again in Settings, then System.").href(page);
                fix.command = Some(format!("{}  CAA  0 issue \"amazon.com\"", f.zone.as_deref().unwrap_or(&host)));
                fix
            },
        ),
        "certificate_failed" => check(
            format!(
                "AWS couldn't issue the certificate for {host}{}.",
                f.cert_failure.as_deref().map(|r| format!(" ({})", r.to_lowercase().replace('_', " "))).unwrap_or_default()
            ),
            "CloudFront needs the certificate before it can answer on your domain.",
            Fix::text("Check the records, then press Try again in Settings, then System for a new certificate.").href(page),
        ),
        "attach_failed" => check(
            format!(
                "Moving CoderHelm to {host} failed{}.",
                if f.reason.is_empty() { String::new() } else { format!(": {}", f.reason) }
            ),
            "CoderHelm still answers on its AWS address.",
            Fix::text(format!("If {host} is on another CloudFront distribution, remove it there first. Then press Try again in Settings, then System; the update history has the log.")).href(page),
        ),
        "attaching" => check(
            format!("The certificate for {host} is issued; CoderHelm is moving to the domain."),
            "It takes about 10 minutes, and CoderHelm keeps working on its AWS address meanwhile.",
            Fix::text("Nothing to do: this page checks again by itself.").href("/settings/system#history"),
        ),
        "removing" => check(
            "CoderHelm is moving back to its AWS address.".to_string(),
            "It takes about 10 minutes.",
            Fix::text("Nothing to do: this page checks again by itself.").href("/settings/system#history"),
        ),
        "dns_missing" => {
            let mut fix = Fix::text("Add this record where your domain's DNS is hosted. New records can take up to an hour to show up.").href(page);
            fix.command = Some(records_text(&[address]));
            check(
                format!("{host} doesn't point anywhere yet."),
                "People and GitHub can't reach CoderHelm on the domain until it does.",
                fix,
            )
        }
        "dns_wrong_value" => {
            let found = f.dns.host_cname.as_ref().and_then(|v| v.first()).map(|s| norm(s)).unwrap_or_default();
            let mut fix = Fix::text(format!("Change the record's value to {target} and save.")).href(page);
            fix.command = Some(records_text(&[address]));
            check(
                format!("{host} points to {found}, not to CoderHelm."),
                "Visitors reach the other address instead of CoderHelm.",
                fix,
            )
        }
        "cname_at_apex" => check(
            format!("{host} is the root of your domain, where most DNS providers don't allow a CNAME record."),
            "Without a record, nobody reaches CoderHelm on the domain.",
            Fix::text(format!("Use a subdomain such as coderhelm.{host} instead, or an ALIAS, ANAME or flattened CNAME record to {target} if your DNS provider has one.")).href(page),
        ),
        "https_failed" => check(
            format!("{host} answers, but not as CoderHelm over HTTPS."),
            "Browsers refuse the page, or reach another server.",
            Fix::text(format!("Point the record at {target} only, with no proxy in front (with Cloudflare, set the record to DNS only), then check again.")).href(page),
        ),
        "api_unreachable" => check(
            format!("The page loads on {host}, but CoderHelm's API doesn't answer there."),
            "Sign-in, GitHub events and the dashboard all go through the API.",
            Fix::text("Run the doctor from your machine: it checks CloudFront and the API.")
                .command(format!("{CLI} doctor")),
        ),
        _ => check(
            "The domain couldn't be checked (the updater didn't answer).".to_string(),
            "The check runs again each time this page opens.",
            Fix::text("Check again in a minute.").href(page),
        ),
    }
}

/// The domain for the install guide: codes only, never the hostname.
pub fn public_domain(f: &DomainFacts) -> PublicCheck {
    let (status, code) = domain_code(f);
    PublicCheck::new("domain", "Your domain", status, code).optional()
}

// ── Sign-in and updates (optional) ────────────────────────────────────────

/// "Sign in with Google": on in the config, and its keys in the secret.
pub fn google_sign_in(enabled: bool, has_keys: bool) -> Check {
    let label = "Sign in with Google";
    match (enabled, has_keys) {
        (true, true) => Check::pass("google_sign_in", X, label, "People can sign in with their Google account."),
        (true, false) => Check::not_ok(
            Status::Fail,
            "google_sign_in",
            X,
            label,
            "Google sign-in is on, but its client id and secret aren't in the install's secret.",
            "The Google button fails until they are there.",
            Fix::text("Add google_client_id and google_client_secret to the secret, then deploy again.")
                .url("https://console.cloud.google.com/apis/credentials"),
        ),
        (false, _) => Check::not_ok(
            Status::Skip,
            "google_sign_in",
            X,
            label,
            "Off: people sign in with email and password, with optional passkeys and two-factor codes (optional).",
            "Google sign-in saves your team a password; it needs your own domain.",
            Fix::text("Create an OAuth client in Google Cloud, add its keys to the secret, set integrations.googleSignIn, and deploy.")
                .url("https://console.cloud.google.com/apis/credentials"),
        ),
    }
}

pub fn public_google_sign_in(enabled: bool, has_keys: bool) -> PublicCheck {
    let c = |s, code| PublicCheck::new("google_sign_in", "Sign in with Google", s, code).optional();
    match (enabled, has_keys) {
        (true, true) => c("pass", "ok"),
        (true, false) => c("fail", "missing_keys"),
        (false, _) => c("skip", "off"),
    }
}

/// Auto-update, from the updater's status (None: it didn't answer).
pub fn public_updates(status: Option<&Value>) -> PublicCheck {
    let c = |s, code| PublicCheck::new("updates", "Automatic updates", s, code).optional();
    match status {
        None => c("warn", "unknown"),
        Some(v) if v["settings"]["auto"].as_bool() == Some(true) => c("pass", "ok"),
        Some(_) => c("skip", "auto_off"),
    }
}

// ── Update preflight ──────────────────────────────────────────────────────

/// One thing that would stop (or should pause) an update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Blocker {
    pub key: String,
    /// `block`: the update won't start; `warn`: it will, but read this first.
    pub severity: &'static str,
    pub detail: String,
    pub fix: Fix,
}

/// What would block an update now, from the checks and the updater's status.
pub fn preflight(checks: &[Check], status: Option<&Value>) -> Vec<Blocker> {
    let mut out: Vec<Blocker> = checks
        .iter()
        .filter(|c| c.status == Status::Fail && !matches!(c.group, "Integrations"))
        .map(|c| Blocker {
            key: c.key.to_string(),
            severity: "block",
            detail: format!(
                "{} The update's health check would fail, so it wouldn't start.",
                c.detail
            ),
            fix: c.fix.clone().unwrap_or_default(),
        })
        .collect();
    let Some(v) = status else {
        out.push(Blocker {
            key: "updater".into(),
            severity: "block",
            detail: "The updater didn't answer, so an update can't start from here.".into(),
            fix: Fix::text("Update from your machine instead.").command(format!("{CLI} update")),
        });
        return out;
    };
    if v["running"].as_bool() == Some(true) {
        out.push(Blocker {
            key: "running".into(),
            severity: "block",
            detail: "An update is already running.".into(),
            fix: Fix::text("Wait for it to finish (10 to 20 minutes); this page shows the result.")
                .href("/settings/system#history"),
        });
    }
    let latest = &v["latest"];
    if latest["breaking"].as_bool() == Some(true) {
        let ver = latest["version"].as_str().unwrap_or("the next release");
        out.push(Blocker {
            key: "migration".into(),
            severity: "warn",
            detail: format!("{ver} is a major release whose notes ask you to do something before or after updating."),
            fix: Fix::text("Read the release notes below and do what they say, then update.")
                .href("/settings/system#version")
                .url(latest["url"].as_str().unwrap_or("https://github.com/CoderHelm/coderhelm-platform/releases").to_string()),
        });
    }
    if let (Some(ver), Some(failed)) = (latest["version"].as_str(), v["failed_versions"].as_array())
    {
        if failed.iter().any(|f| f.as_str() == Some(ver)) {
            let reason = v["history"]
                .as_array()
                .and_then(|h| {
                    h.iter()
                        .find(|e| e["version"].as_str() == Some(ver) && e["status"] != "succeeded")
                })
                .and_then(|e| e["reason"].as_str())
                .unwrap_or("see the update history");
            out.push(Blocker {
                key: "failed_before".into(),
                severity: "warn",
                detail: format!("The last try to update to {ver} didn't succeed: {reason}."),
                fix: Fix::text("Fix what the history says, then try again. Auto-update skips this release until then.")
                    .href("/settings/system#history")
                    .command(format!("{CLI} doctor")),
            });
        }
    }
    out
}

// ── Gathering the answers ─────────────────────────────────────────────────

#[derive(Serialize)]
struct AppJwt {
    iat: u64,
    exp: u64,
    iss: String,
}

fn app_jwt(app_id: &str, pem: &str) -> Option<String> {
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    let key = EncodingKey::from_rsa_pem(pem.as_bytes()).ok()?;
    encode(
        &Header::new(Algorithm::RS256),
        &AppJwt {
            iat: now.saturating_sub(60),
            exp: now + 540,
            iss: app_id.to_string(),
        },
        &key,
    )
    .ok()
}

async fn github_get(state: &AppState, jwt: &str, path: &str) -> Option<(u16, Value)> {
    let r = state
        .http
        .get(format!("https://api.github.com{path}"))
        .bearer_auth(jwt)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Coderhelm-setup-check")
        .timeout(Duration::from_secs(8))
        .send()
        .await
        .ok()?;
    let code = r.status().as_u16();
    let body = r.json::<Value>().await.unwrap_or(Value::Null);
    Some((code, body))
}

/// Every GitHub check, from the stored keys.
async fn github_checks(state: &AppState, deep: bool) -> Vec<Check> {
    let s = &state.secrets;
    let keys = github_keys(&s.github_app_id, &s.github_private_key);
    if keys.status != Status::Pass {
        return vec![keys];
    }
    let Some(jwt) = app_jwt(&s.github_app_id, &s.github_private_key) else {
        return vec![
            keys,
            Check::not_ok(
                Status::Fail,
                "github_app",
                G,
                "GitHub accepts the app's keys",
                "The stored private key can't be read.",
                "CoderHelm signs every call to GitHub with it.",
                Fix::text("Generate a new private key on the app's page and store the app again.")
                    .command(format!("{CLI} github-app --force")),
            ),
        ];
    };
    let app = github_get(state, &jwt, "/app").await;
    let app_check = github_app(app.as_ref().map(|(c, _)| *c));
    let mut out = vec![keys, app_check.clone()];
    if app_check.status != Status::Pass || !deep {
        return out;
    }
    let app = app.map(|(_, b)| b).unwrap_or(Value::Null);
    out.push(github_permissions(&app));
    let (installs, config, deliveries) = tokio::join!(
        github_get(state, &jwt, "/app/installations?per_page=100"),
        github_get(state, &jwt, "/app/hook/config"),
        github_get(state, &jwt, "/app/hook/deliveries?per_page=10"),
    );
    out.push(github_installations(
        installs
            .filter(|(c, _)| *c == 200)
            .and_then(|(_, b)| b.as_array().map(Vec::len)),
        &app,
    ));
    let expected = format!(
        "{}/webhooks/github",
        common::public_urls::api_url_from_env().trim_end_matches('/')
    );
    let list: Option<Vec<Delivery>> = deliveries.filter(|(c, _)| *c == 200).and_then(|(_, b)| {
        b.as_array().map(|a| {
            a.iter()
                .map(|d| Delivery {
                    status_code: d["status_code"].as_u64().unwrap_or(0) as u16,
                    event: d["event"].as_str().unwrap_or("").to_string(),
                    delivered_at: d["delivered_at"].as_str().unwrap_or("").to_string(),
                })
                .collect()
        })
    });
    let config = config.map(|(_, b)| b).unwrap_or(Value::Null);
    out.push(github_webhook(&config, &expected, list.as_deref(), &app));
    out
}

async fn email_checks(state: &AppState, deep: bool) -> Vec<Check> {
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let from = &state.config.ses_from_address;
    let domain = sender_domain(from).to_string();
    let identity = match state
        .ses
        .get_email_identity()
        .email_identity(&domain)
        .send()
        .await
    {
        Ok(o) => Some(Identity {
            verified: o.verified_for_sending_status(),
            dkim_status: o
                .dkim_attributes()
                .and_then(|d| d.status())
                .map(|s| s.as_str().to_string())
                .unwrap_or_else(|| "NOT_STARTED".into()),
            dkim_tokens: o
                .dkim_attributes()
                .map(|d| d.tokens().to_vec())
                .unwrap_or_default(),
        }),
        Err(e) => {
            warn!(error = %aws_sdk_sesv2::error::DisplayErrorContext(&e), "SES identity check");
            None
        }
    };
    let mut out = vec![email_identity(from, &region, identity.as_ref())];
    if deep {
        let production = state
            .ses
            .get_account()
            .send()
            .await
            .ok()
            .map(|a| a.production_access_enabled());
        out.push(email_sandbox(&region, production));
    }
    out
}

/// The team's key and chosen model (the same item plan chat reads).
async fn team_model(state: &AppState, team_id: &str) -> (Option<String>, String) {
    let item = state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("SETTINGS#MODEL_PROVIDER".into()),
        )
        .send()
        .await
        .ok()
        .and_then(|r| r.item().cloned());
    let get = |k: &str| {
        item.as_ref()
            .and_then(|i| i.get(k))
            .and_then(|v| v.as_s().ok())
            .filter(|s| !s.is_empty())
            .cloned()
    };
    (
        get("api_key"),
        get("primary_model").unwrap_or_else(|| "claude-sonnet-5-5".to_string()),
    )
}

async fn model_check(state: &AppState, team_id: &str) -> Check {
    let bedrock = common::model_provider::is_bedrock();
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let (key, model_name) = team_model(state, team_id).await;
    let shown = if bedrock {
        common::model_provider::bedrock_model_id(&model_name)
    } else {
        model_name.clone()
    };
    let probe = if !bedrock && key.is_none() {
        ModelProbe::NoKey
    } else {
        let meter = crate::model_call::chat_meter(state, team_id);
        crate::model_call::probe(
            &state.http,
            key.as_deref().unwrap_or(""),
            &meter,
            &model_name,
        )
        .await
    };
    model(bedrock, &region, &shown, &probe)
}

async fn dlq_count(state: &AppState) -> Option<u64> {
    use aws_sdk_sqs::types::QueueAttributeName as A;
    if state.config.dlq_url.is_empty() {
        return None;
    }
    let r = state
        .sqs
        .get_queue_attributes()
        .queue_url(&state.config.dlq_url)
        .attribute_names(A::ApproximateNumberOfMessages)
        .send()
        .await
        .ok()?;
    r.attributes()?
        .get(&A::ApproximateNumberOfMessages)?
        .parse()
        .ok()
}

async fn url_status(state: &AppState) -> (String, Option<u16>) {
    let url = common::public_urls::app_url_from_env()
        .trim_end_matches('/')
        .to_string();
    if url.is_empty() {
        return (url, None);
    }
    let code = state
        .http
        .get(format!("{url}/"))
        .timeout(Duration::from_secs(8))
        .send()
        .await
        .ok()
        .map(|r| r.status().as_u16());
    (url, code)
}

async fn integration_checks(state: &AppState, team_id: &str) -> Vec<Check> {
    use common::notify::{store, ChannelKind};
    let jira_key = [("pk", team_id), ("sk", "JIRA#config")];
    let (notify, jira_secret, jira_config) = tokio::join!(
        super::alerts::team_notify_settings(state, team_id),
        super::api::load_jira_secret(state, team_id),
        get_item(state, &state.config.jira_config_table_name, &jira_key),
    );
    let forge = jira_config.as_ref().is_some_and(|i| {
        i.get("add_comment_url")
            .and_then(|v| v.as_s().ok())
            .is_some_and(|s| !s.is_empty())
    });
    vec![
        chat(
            "slack",
            "Slack",
            store::stored(&notify.item, ChannelKind::Slack).is_set(),
            notify.enabled,
        ),
        chat(
            "teams",
            "Teams",
            store::stored(&notify.item, ChannelKind::Teams).is_set(),
            notify.enabled,
        ),
        jira(forge || jira_secret.is_some()),
    ]
}

async fn get_item(
    state: &AppState,
    table: &str,
    key: &[(&str, &str)],
) -> Option<std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>> {
    let mut req = state.dynamo.get_item().table_name(table);
    for (k, v) in key {
        req = req.key(
            *k,
            aws_sdk_dynamodb::types::AttributeValue::S((*v).to_string()),
        );
    }
    req.send().await.ok().and_then(|o| o.item().cloned())
}

// ── Asking DNS and the domain ─────────────────────────────────────────────

/// One DNS question over DNS-over-HTTPS (Cloudflare, then Google): the
/// answers of `rtype` for `name`, and whether `name` has its own SOA.
/// None when neither resolver answered.
async fn doh(state: &AppState, name: &str, rtype: u16) -> Option<(Vec<String>, bool)> {
    let urls = [
        format!("https://cloudflare-dns.com/dns-query?name={name}&type={rtype}"),
        format!("https://dns.google/resolve?name={name}&type={rtype}"),
    ];
    for url in urls {
        let Ok(r) = state
            .http
            .get(&url)
            .header("Accept", "application/dns-json")
            .timeout(Duration::from_secs(4))
            .send()
            .await
        else {
            continue;
        };
        let Ok(v) = r.json::<Value>().await else {
            continue;
        };
        // NOERROR or NXDOMAIN: an answer either way.
        if !matches!(v["Status"].as_u64(), Some(0) | Some(3)) {
            continue;
        }
        let want = norm(name);
        let answers = v["Answer"].as_array().cloned().unwrap_or_default();
        let data: Vec<String> = answers
            .iter()
            .filter(|a| a["type"].as_u64() == Some(rtype as u64))
            .filter(|a| rtype == 1 || norm(a["name"].as_str().unwrap_or("")) == want)
            .filter_map(|a| a["data"].as_str().map(str::to_string))
            .collect();
        let soa = answers.iter().any(|a| {
            a["type"].as_u64() == Some(6) && norm(a["name"].as_str().unwrap_or("")) == want
        });
        return Some((data, soa));
    }
    None
}

/// The closest CAA record set at or above `host` (CAA is inherited).
async fn closest_caa(state: &AppState, host: &str) -> Option<Vec<String>> {
    let labels: Vec<&str> = host.split('.').collect();
    for i in 0..labels.len().saturating_sub(1) {
        let name = labels[i..].join(".");
        match doh(state, &name, 257).await {
            Some((caa, _)) if !caa.is_empty() => return Some(caa),
            Some(_) => continue,
            None => return None,
        }
    }
    None
}

async fn https_status(state: &AppState, url: &str) -> Option<u16> {
    state
        .http
        .get(url)
        .timeout(Duration::from_secs(8))
        .send()
        .await
        .ok()
        .map(|r| r.status().as_u16())
}

/// CoderHelm's own probe answer on the hostname (`/api/setup/status?probe=1`).
async fn api_answers(state: &AppState, host: &str) -> Option<bool> {
    let r = state
        .http
        .get(format!("https://{host}/api/setup/status?probe=1"))
        .timeout(Duration::from_secs(8))
        .send()
        .await
        .ok()?;
    let v = r.json::<Value>().await.ok()?;
    Some(v["service"] == "coderhelm")
}

/// The updater's view of the domain plus what DNS and HTTPS answer now.
pub async fn domain_facts_now(state: &AppState) -> (DomainFacts, Option<Value>) {
    let view = super::system::call(state, json!({ "action": "domain_status" }))
        .await
        .ok();
    let mut f = domain_facts_from(view.as_ref());
    let Some(host) = f.hostname.clone() else {
        return (f, view);
    };
    match f.state.as_str() {
        "validating" => {
            let v_name = f.validation.as_ref().map(|v| v.name.clone());
            let zone = f.zone.clone();
            let (vc, caa, ns) = tokio::join!(
                async {
                    match v_name {
                        Some(n) => doh(state, &n, 5).await.map(|(d, _)| d),
                        None => None,
                    }
                },
                closest_caa(state, &host),
                async {
                    match zone {
                        Some(z) => doh(state, &z, 2).await.map(|(d, _)| d),
                        None => None,
                    }
                },
            );
            f.dns.validation_cname = vc;
            f.dns.caa = caa;
            f.dns.zone_ns = ns;
        }
        "failed" => {
            f.dns.caa = closest_caa(state, &host).await;
        }
        "live" => {
            let root = format!("https://{host}/");
            let (cname, a, soa, https, api) = tokio::join!(
                doh(state, &host, 5),
                doh(state, &host, 1),
                doh(state, &host, 6),
                https_status(state, &root),
                api_answers(state, &host),
            );
            f.dns.host_cname = cname.map(|(d, _)| d);
            f.dns.host_a = a.map(|(d, _)| d);
            f.dns.apex = soa.is_some_and(|(_, apex)| apex);
            f.https = https;
            f.api = api;
        }
        _ => {}
    }
    (f, view)
}

fn google_enabled() -> bool {
    std::env::var("GOOGLE_SIGN_IN").is_ok_and(|v| v == "true")
}

fn google_keys(s: &crate::models::Secrets) -> bool {
    s.google_client_id
        .as_deref()
        .is_some_and(|v| !v.trim().is_empty())
        && s.google_client_secret
            .as_deref()
            .is_some_and(|v| !v.trim().is_empty())
}

/// Every check, in the order the page shows them.
pub async fn run_all(state: &AppState, team_id: &str) -> (Vec<Check>, Option<Value>) {
    let (gh, email, model, dlq_n, url, integrations, updater, domain_now) = tokio::join!(
        github_checks(state, true),
        email_checks(state, true),
        model_check(state, team_id),
        dlq_count(state),
        url_status(state),
        integration_checks(state, team_id),
        async {
            super::system::call(state, json!({ "action": "status" }))
                .await
                .ok()
                .filter(|b| b.get("ok").and_then(Value::as_bool) != Some(false))
        },
        async {
            if super::system::has_updater() {
                Some(domain_facts_now(state).await.0)
            } else {
                None
            }
        },
    );
    let mut checks = Vec::new();
    checks.push(public_url(
        &url.0,
        url.1,
        common::public_urls::problem().as_deref(),
    ));
    if let Some(f) = domain_now {
        checks.push(domain(&f));
    }
    checks.extend(gh);
    checks.extend(email);
    checks.push(model);
    checks.push(dlq(dlq_n));
    if super::system::has_updater() {
        checks.push(updates(updater.as_ref()));
    }
    checks.extend(integrations);
    checks.push(google_sign_in(
        google_enabled(),
        google_keys(&state.secrets),
    ));
    (checks, updater)
}

fn allowed(claims: &Claims) -> Result<(), (StatusCode, Json<Value>)> {
    claims.require_role(3).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Only team admins can run the setup checks." })),
        )
    })?;
    if !is_platform_admin(claims) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Only the install's admins can run the setup checks." })),
        ));
    }
    Ok(())
}

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

/// GET /api/system/checks: every server-side check, with fixes.
pub async fn checks(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    claims.require_role(3).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Only team admins can run the setup checks." })),
        )
    })?;
    allowed(&claims)?;
    let (checks, _) = run_all(&state, &claims.team_id).await;
    Ok(Json(json!({
        "checked_at": chrono::Utc::now().to_rfc3339(),
        "summary": summary(&checks),
        "checks": checks,
    })))
}

/// GET /api/system/preflight: what would block an update now.
pub async fn preflight_route(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    claims.require_role(3).map_err(|s| {
        (
            s,
            Json(json!({ "error": "Only team admins can run the setup checks." })),
        )
    })?;
    allowed(&claims)?;
    let (checks, status) = run_all(&state, &claims.team_id).await;
    let blockers = preflight(&checks, status.as_ref());
    let latest = status
        .as_ref()
        .map(|s| s["latest"].clone())
        .unwrap_or(Value::Null);
    Ok(Json(json!({
        "checked_at": chrono::Utc::now().to_rfc3339(),
        "ready": !blockers.iter().any(|b| b.severity == "block"),
        "current": super::system::stamp()["version"],
        "latest": latest["version"],
        "blockers": blockers,
        "checks": checks,
    })))
}

// ── The public setup status ───────────────────────────────────────────────
//
// GET /api/setup/status, for the install guide (docs.coderhelm.com) to poll
// from the person's browser during the first install. Shape (schema 1):
//
//   { schema, service, self_hosted, setup_open, version, checked_at, ready,
//     github_manifest_ready, checks: [{ id, label, status, code, required? }] }
//
// `status` is pass, fail, warn, wait (not done yet, nothing wrong) or skip.
// Only ids, labels, statuses and codes: never an account id, key, app id or
// slug, address, email, region or URL (a test holds it to that). The guide
// builds its own links from what it already knows (the region it launched
// in, the app slug GitHub's manifest flow hands back).

/// One public check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PublicCheck {
    pub id: &'static str,
    pub label: &'static str,
    pub status: &'static str,
    pub code: &'static str,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub optional: bool,
}

impl PublicCheck {
    fn new(
        id: &'static str,
        label: &'static str,
        status: &'static str,
        code: &'static str,
    ) -> Self {
        PublicCheck {
            id,
            label,
            status,
            code,
            optional: false,
        }
    }
    fn optional(mut self) -> Self {
        self.optional = true;
        self
    }
}

/// Serialized with `required: false` for optional checks (the guide's field).
fn public_json(c: &PublicCheck) -> Value {
    let mut v = json!({ "id": c.id, "label": c.label, "status": c.status, "code": c.code });
    if c.optional {
        v["required"] = json!(false);
    }
    v
}

/// Ready when every required check passes (warnings, waits on optional
/// checks and skips don't block).
pub fn public_ready(checks: &[PublicCheck]) -> bool {
    checks
        .iter()
        .all(|c| c.optional || c.status == "pass" || c.status == "skip")
}

/// What the stored secret says about the GitHub App, before calling GitHub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppKeys {
    Unreadable,
    /// No app yet; whether the Launch Stack's one-time setup key is there.
    Missing {
        setup_key: bool,
    },
    BadKey,
    Present,
}

pub fn app_keys(secret: Option<&Value>) -> AppKeys {
    let Some(s) = secret else {
        return AppKeys::Unreadable;
    };
    let id = s["github_app_id"].as_str().unwrap_or("");
    let pem = s["github_private_key"].as_str().unwrap_or("");
    if id.trim().is_empty() || pem.trim().is_empty() {
        let setup_key = s["setup_token"].as_str().is_some_and(|t| t.len() >= 32);
        return AppKeys::Missing { setup_key };
    }
    if !pem.contains("PRIVATE KEY") {
        return AppKeys::BadKey;
    }
    AppKeys::Present
}

/// The two GitHub checks: the app exists, and it is installed somewhere.
/// `app_status` is GitHub's answer to GET /app; `installs` the count.
pub fn public_github(
    keys: AppKeys,
    app_status: Option<u16>,
    installs: Option<usize>,
) -> [PublicCheck; 2] {
    let app = |s, c| PublicCheck::new("github_app", "GitHub App created", s, c);
    let inst = |s, c| {
        PublicCheck::new(
            "github_installed",
            "GitHub App installed on your repositories",
            s,
            c,
        )
    };
    match keys {
        AppKeys::Unreadable => [app("fail", "secret_unreadable"), inst("wait", "needs_app")],
        AppKeys::Missing { setup_key: true } => [app("fail", "missing"), inst("wait", "needs_app")],
        AppKeys::Missing { setup_key: false } => [
            app("fail", "missing_no_setup_key"),
            inst("wait", "needs_app"),
        ],
        AppKeys::BadKey => [app("pass", "ok"), inst("fail", "bad_key")],
        AppKeys::Present => match (app_status, installs) {
            (Some(200), Some(0)) => [app("pass", "ok"), inst("wait", "not_installed")],
            (Some(200), Some(_)) => [app("pass", "ok"), inst("pass", "ok")],
            (Some(401 | 403 | 404), _) => [app("pass", "ok"), inst("fail", "app_rejected")],
            _ => [app("pass", "ok"), inst("warn", "github_unreachable")],
        },
    }
}

/// Bedrock's refusal, as the guide's codes (one for each fix it shows).
pub fn bedrock_code(status: u16, message: &str) -> &'static str {
    let m = message.to_ascii_lowercase();
    if m.contains("use case") || m.contains("use-case") {
        "use_case_required"
    } else if m.contains("being processed") || m.contains("subscription") && m.contains("pending") {
        "subscription_pending"
    } else if m.contains("marketplace") || m.contains("payment") {
        "marketplace_failed"
    } else if m.contains("not authorized to perform") || m.contains("explicit deny") {
        "iam_denied"
    } else if m.contains("don't have access")
        || m.contains("do not have access")
        || m.contains("access to the model")
    {
        "access_denied"
    } else if m.contains("not available")
        || m.contains("not supported")
        || m.contains("model identifier is invalid")
        || status == 404
    {
        "not_available"
    } else if status == 403 {
        "access_denied"
    } else {
        "unknown"
    }
}

/// Claude: with Bedrock, a real one-token call; with Anthropic keys, a skip
/// (each team adds its key after signing in).
pub fn public_models(bedrock: bool, probe: Option<&ModelProbe>) -> PublicCheck {
    let c = |s, code| PublicCheck::new("models", "Claude access", s, code);
    if !bedrock {
        return c("skip", "anthropic_keys_per_team");
    }
    match probe {
        Some(ModelProbe::Ok) => c("pass", "ok"),
        Some(ModelProbe::Refused(status, msg)) => match bedrock_code(*status, msg) {
            "subscription_pending" => c("warn", "subscription_pending"),
            code => c("fail", code),
        },
        Some(ModelProbe::Unreachable(_)) | Some(ModelProbe::NoKey) | None => c("fail", "unknown"),
    }
}

/// Email: never blocks the install.
pub fn public_email(identity: Option<&Identity>, production: Option<bool>) -> PublicCheck {
    let c = |s, code| PublicCheck::new("email", "Email", s, code).optional();
    match (identity, production) {
        (None, _) => c("warn", "unknown"),
        (Some(i), _) if !i.verified => c("warn", "identity_unverified"),
        (Some(_), Some(false)) => c("warn", "sandbox"),
        (Some(_), Some(true)) => c("pass", "ok"),
        (Some(_), None) => c("warn", "unknown"),
    }
}

/// The whole answer, from its parts.
pub fn public_status(
    version: Value,
    open: bool,
    github: [PublicCheck; 2],
    models: PublicCheck,
    email: PublicCheck,
    optional: Vec<PublicCheck>,
    manifest_ready: bool,
) -> Value {
    let admin = if open {
        PublicCheck::new("admin", "First admin signed up", "wait", "not_signed_up")
    } else {
        PublicCheck::new("admin", "First admin signed up", "pass", "ok")
    };
    let mut checks = vec![PublicCheck::new("api", "CoderHelm answers", "pass", "ok")];
    checks.extend(github);
    checks.push(models);
    checks.push(email);
    checks.extend(optional);
    checks.push(admin);
    json!({
        "schema": 1,
        "service": "coderhelm",
        "self_hosted": true,
        "setup_open": open,
        "version": version,
        "checked_at": chrono::Utc::now().to_rfc3339(),
        "ready": public_ready(&checks),
        "github_manifest_ready": manifest_ready,
        "checks": checks.iter().map(public_json).collect::<Vec<_>>(),
    })
}

/// The answer once the setup window has closed: nothing to show the guide
/// but "done, sign in".
pub fn closed_status(version: Value) -> Value {
    json!({
        "schema": 1,
        "service": "coderhelm",
        "self_hosted": true,
        "setup_open": false,
        "version": version,
        "ready": true,
        "github_manifest_ready": false,
        "checks": [public_json(&PublicCheck::new("admin", "First admin signed up", "pass", "ok"))],
    })
}

/// Only on a self-hosted install (the hosted deployment answers 404).
pub(crate) fn self_hosted() -> bool {
    std::env::var("SETUP_ENDPOINTS").is_ok_and(|v| v == "1") || super::system::has_updater()
}

/// The setup window: open until the first person has an account. Once it
/// closes it stays closed for this process (it never reopens).
static CLOSED: Mutex<bool> = Mutex::new(false);
static CACHE: Mutex<Option<(Instant, Value)>> = Mutex::new(None);
/// A Bedrock pass is kept 10 minutes, a failure 20 seconds (one call each).
static MODEL_CACHE: Mutex<Option<(Instant, ModelProbe)>> = Mutex::new(None);
const CACHE_FOR: Duration = Duration::from_secs(10);
const FRESH_FOR: Duration = Duration::from_secs(3);

pub(crate) async fn setup_open(state: &AppState) -> bool {
    if *CLOSED.lock().unwrap_or_else(|e| e.into_inner()) {
        return false;
    }
    let anyone = state
        .dynamo
        .scan()
        .table_name(&state.config.users_table_name)
        .limit(1)
        .projection_expression("pk")
        .send()
        .await
        .map(|r| r.count() > 0)
        // Can't tell: treat the window as closed (the safe answer).
        .unwrap_or(true);
    if anyone {
        *CLOSED.lock().unwrap_or_else(|e| e.into_inner()) = true;
    }
    !anyone
}

/// The install's secret as stored now (the services' copy is read at start;
/// the manifest landing writes a new app while they run).
async fn stored_secret() -> Option<Value> {
    static CLIENT: tokio::sync::OnceCell<aws_sdk_secretsmanager::Client> =
        tokio::sync::OnceCell::const_new();
    let client = CLIENT
        .get_or_init(|| async {
            aws_sdk_secretsmanager::Client::new(&aws_config::load_from_env().await)
        })
        .await;
    let name =
        std::env::var("SECRETS_NAME").unwrap_or_else(|_| "coderhelm/prod/secrets".to_string());
    let r = client
        .get_secret_value()
        .secret_id(name)
        .send()
        .await
        .ok()?;
    serde_json::from_str(r.secret_string()?).ok()
}

/// GitHub through CoderHelm's app (hybrid): the app is CoderHelm's, and it's
/// installed once an installation is linked to this install.
pub fn public_github_hosted(links: usize) -> [PublicCheck; 2] {
    [
        PublicCheck::new("github_app", "GitHub App", "pass", "hosted_app"),
        if links > 0 {
            PublicCheck::new(
                "github_installed",
                "Installed on your repositories",
                "pass",
                "ok",
            )
        } else {
            PublicCheck::new(
                "github_installed",
                "Installed on your repositories",
                "fail",
                "not_linked",
            )
        },
    ]
}

async fn public_github_now(state: &AppState) -> ([PublicCheck; 2], bool) {
    if super::hybrid_install::github_hosted(state).await {
        let links = super::hybrid_install::linked_installations(state).await;
        return (public_github_hosted(links), false);
    }
    let secret = stored_secret().await;
    let keys = app_keys(secret.as_ref());
    let manifest_ready = matches!(keys, AppKeys::Missing { setup_key: true });
    if keys != AppKeys::Present {
        return (public_github(keys, None, None), manifest_ready);
    }
    let s = secret.unwrap_or(Value::Null);
    let Some(jwt) = app_jwt(
        s["github_app_id"].as_str().unwrap_or(""),
        s["github_private_key"].as_str().unwrap_or(""),
    ) else {
        return (public_github(AppKeys::BadKey, None, None), false);
    };
    let app = github_get(state, &jwt, "/app").await.map(|(c, _)| c);
    let installs = if app == Some(200) {
        github_get(state, &jwt, "/app/installations?per_page=100")
            .await
            .filter(|(c, _)| *c == 200)
            .and_then(|(_, b)| b.as_array().map(Vec::len))
    } else {
        None
    };
    (public_github(keys, app, installs), false)
}

async fn public_models_now(state: &AppState) -> PublicCheck {
    let bedrock = common::model_provider::is_bedrock();
    if !bedrock {
        return public_models(false, None);
    }
    if let Some((at, p)) = MODEL_CACHE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
    {
        let keep = if p == ModelProbe::Ok {
            Duration::from_secs(600)
        } else {
            Duration::from_secs(20)
        };
        if at.elapsed() < keep {
            return public_models(true, Some(&p));
        }
    }
    // Metered like any call, on the install's own setup meter (no team yet).
    let meter = crate::model_call::chat_meter(state, "SETUP");
    let p = crate::model_call::probe(&state.http, "", &meter, "claude-sonnet-5-5").await;
    *MODEL_CACHE.lock().unwrap_or_else(|e| e.into_inner()) = Some((Instant::now(), p.clone()));
    public_models(true, Some(&p))
}

async fn public_email_now(state: &AppState) -> PublicCheck {
    let domain = sender_domain(&state.config.ses_from_address).to_string();
    let (id, acct) = tokio::join!(
        state
            .ses
            .get_email_identity()
            .email_identity(&domain)
            .send(),
        state.ses.get_account().send(),
    );
    let identity = id.ok().map(|o| Identity {
        verified: o.verified_for_sending_status(),
        ..Default::default()
    });
    public_email(
        identity.as_ref(),
        acct.ok().map(|a| a.production_access_enabled()),
    )
}

/// The optional checks the guide lists after Ready: the domain, Google
/// sign-in and automatic updates (codes only).
async fn public_optional_now(state: &AppState) -> Vec<PublicCheck> {
    let mut out = Vec::new();
    if super::system::has_updater() {
        let (domain_now, updater) = tokio::join!(
            domain_facts_now(state),
            super::system::call(state, json!({ "action": "status" })),
        );
        out.push(public_domain(&domain_now.0));
        out.push(public_updates(
            updater
                .ok()
                .filter(|b| b.get("ok").and_then(Value::as_bool) != Some(false))
                .as_ref(),
        ));
    }
    let secret = stored_secret().await;
    let keys = secret.as_ref().is_some_and(|s| {
        s["google_client_id"]
            .as_str()
            .is_some_and(|v| !v.trim().is_empty())
            && s["google_client_secret"]
                .as_str()
                .is_some_and(|v| !v.trim().is_empty())
    });
    out.push(public_google_sign_in(google_enabled(), keys));
    out
}

#[derive(serde::Deserialize, Default)]
pub struct StatusQuery {
    fresh: Option<String>,
    /// `probe=1`: only "this is CoderHelm" (the domain check calls it on the
    /// hostname; no checks run, so it can't call itself again).
    probe: Option<String>,
}

/// The answer to `?probe=1`.
pub fn probe_answer() -> Value {
    json!({ "schema": 1, "service": "coderhelm", "probe": true })
}

/// GET /api/setup/status: public, polled by the install guide. Any origin
/// may read it (GET only, no credentials); rate limited per address.
pub async fn status(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(q): axum::extract::Query<StatusQuery>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let cors = [
        (axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        (axum::http::header::ACCESS_CONTROL_ALLOW_METHODS, "GET"),
    ];
    if !self_hosted() {
        return StatusCode::NOT_FOUND.into_response();
    }
    if q.probe.as_deref() == Some("1") {
        return (cors, Json(probe_answer())).into_response();
    }
    if let Err(code) = crate::middleware::rate_limit::allow_setup_poll(&headers) {
        return (code, cors).into_response();
    }
    let version = super::system::stamp()["version"].clone();
    if !setup_open(&state).await {
        return (cors, Json(closed_status(version))).into_response();
    }
    let keep = if q.fresh.as_deref() == Some("1") {
        FRESH_FOR
    } else {
        CACHE_FOR
    };
    if let Some((at, v)) = CACHE.lock().unwrap_or_else(|e| e.into_inner()).clone() {
        if at.elapsed() < keep {
            return (cors, Json(v)).into_response();
        }
    }
    let ((github, manifest_ready), models, email, optional) = tokio::join!(
        public_github_now(&state),
        public_models_now(&state),
        public_email_now(&state),
        public_optional_now(&state),
    );
    let mut v = public_status(
        version,
        true,
        github,
        models,
        email,
        optional,
        manifest_ready,
    );
    // Hybrid's own checks (registered, the CoderHelm address, the GitHub
    // link), before the admin check, when the install chose hybrid.
    let hybrid = super::hybrid_install::public_checks(&state).await;
    if let Some(list) = v["checks"].as_array_mut() {
        let at = list.len().saturating_sub(1);
        for c in hybrid.into_iter().rev() {
            list.insert(at, c);
        }
    }
    *CACHE.lock().unwrap_or_else(|e| e.into_inner()) = Some((Instant::now(), v.clone()));
    (cors, Json(v)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app(perms: Value, events: Value) -> Value {
        json!({
            "id": 1, "slug": "coderhelm-prod", "html_url": "https://github.com/apps/coderhelm-prod",
            "owner": {"login": "acme", "type": "Organization"},
            "permissions": perms, "events": events,
        })
    }

    fn full() -> Value {
        let mut p = serde_json::Map::new();
        for (k, v, _) in REQUIRED_PERMISSIONS {
            p.insert((*k).into(), json!(v));
        }
        app(Value::Object(p), json!(REQUIRED_EVENTS))
    }

    #[test]
    fn missing_keys_fail_with_the_cli_command() {
        let c = github_keys("", "");
        assert_eq!(c.status, Status::Fail);
        assert!(c.fix.unwrap().command.unwrap().ends_with("github-app"));
        assert_eq!(
            github_keys("12", "-----BEGIN RSA PRIVATE KEY-----").status,
            Status::Pass
        );
    }

    #[test]
    fn refused_keys_fail_and_outages_warn() {
        assert_eq!(github_app(Some(200)).status, Status::Pass);
        let c = github_app(Some(401));
        assert_eq!(c.status, Status::Fail);
        assert!(c
            .fix
            .unwrap()
            .command
            .unwrap()
            .contains("github-app --force"));
        assert_eq!(github_app(Some(502)).status, Status::Warn);
        assert_eq!(github_app(None).status, Status::Warn);
    }

    #[test]
    fn permissions_name_each_missing_or_weaker_one() {
        assert_eq!(github_permissions(&full()).status, Status::Pass);
        let mut a = full();
        a["permissions"]["workflows"] = json!("read");
        a["permissions"].as_object_mut().unwrap().remove("checks");
        a["events"] = json!(["issues", "push"]);
        let c = github_permissions(&a);
        assert_eq!(c.status, Status::Fail);
        assert!(
            c.detail.contains("Workflows: write (now read)"),
            "{}",
            c.detail
        );
        assert!(c.detail.contains("Checks: read (missing)"), "{}", c.detail);
        assert!(c.detail.contains("pull_request"), "{}", c.detail);
        assert_eq!(
            c.fix.unwrap().url.unwrap(),
            "https://github.com/organizations/acme/settings/apps/coderhelm-prod/permissions"
        );
        // Admin satisfies write.
        let mut b = full();
        b["permissions"]["contents"] = json!("admin");
        assert_eq!(github_permissions(&b).status, Status::Pass);
    }

    #[test]
    fn personal_apps_link_to_personal_settings() {
        let mut a = full();
        a["owner"]["type"] = json!("User");
        assert_eq!(
            app_settings_url(&a),
            "https://github.com/settings/apps/coderhelm-prod"
        );
    }

    #[test]
    fn installations() {
        let c = github_installations(Some(0), &full());
        assert_eq!(c.status, Status::Fail);
        assert_eq!(
            c.fix.unwrap().url.unwrap(),
            "https://github.com/apps/coderhelm-prod/installations/new"
        );
        assert_eq!(
            github_installations(Some(2), &full()).detail,
            "Installed on 2 accounts."
        );
        assert_eq!(github_installations(None, &full()).status, Status::Warn);
    }

    #[test]
    fn webhook_url_secret_and_deliveries() {
        let want = "https://helm.example.com/webhooks/github";
        let ok = json!({"url": want});
        let d = |code| Delivery {
            status_code: code,
            event: "issues".into(),
            delivered_at: "2026-10-25T10:00:00Z".into(),
        };

        let wrong = github_webhook(
            &json!({"url": "https://old.example.com/webhooks/github"}),
            want,
            None,
            &full(),
        );
        assert_eq!(wrong.status, Status::Fail);
        assert!(wrong.detail.contains("old.example.com"));
        assert!(wrong.fix.unwrap().text.contains(want));

        assert_eq!(
            github_webhook(&ok, want, Some(&[]), &full()).status,
            Status::Warn
        );
        assert_eq!(
            github_webhook(&ok, want, Some(&[d(200)]), &full()).status,
            Status::Pass
        );
        let secret = github_webhook(&ok, want, Some(&[d(401), d(200)]), &full());
        assert_eq!(secret.status, Status::Fail);
        assert!(secret.detail.contains("secret"));
        assert_eq!(
            github_webhook(&ok, want, Some(&[d(0)]), &full()).status,
            Status::Fail
        );
        assert!(github_webhook(&ok, want, Some(&[d(500)]), &full())
            .detail
            .contains("500"));
        // A trailing slash on GitHub's side is the same URL.
        assert_eq!(
            github_webhook(&json!({"url": format!("{want}/")}), want, None, &full()).status,
            Status::Pass
        );
    }

    #[test]
    fn email_identity_shows_the_dkim_records() {
        let missing = email_identity("helm@example.com", "eu-west-1", None);
        assert_eq!(missing.status, Status::Fail);
        assert!(missing
            .fix
            .as_ref()
            .unwrap()
            .command
            .as_ref()
            .unwrap()
            .contains("create-email-identity --email-identity example.com"));

        let pending = Identity {
            verified: false,
            dkim_status: "PENDING".into(),
            dkim_tokens: vec!["abc".into(), "def".into()],
        };
        let c = email_identity("helm@example.com", "eu-west-1", Some(&pending));
        assert_eq!(c.status, Status::Fail);
        let cmd = c.fix.unwrap().command.unwrap();
        assert!(cmd.contains("abc._domainkey.example.com CNAME abc.dkim.amazonses.com"));
        assert!(cmd.contains("def._domainkey.example.com"));

        let no_dkim = Identity {
            verified: true,
            dkim_status: "FAILED".into(),
            dkim_tokens: vec![],
        };
        assert_eq!(
            email_identity("a@example.com", "us-east-1", Some(&no_dkim)).status,
            Status::Warn
        );

        let good = Identity {
            verified: true,
            dkim_status: "SUCCESS".into(),
            dkim_tokens: vec![],
        };
        assert_eq!(
            email_identity("a@example.com", "us-east-1", Some(&good)).status,
            Status::Pass
        );
    }

    #[test]
    fn the_sandbox_warns_with_the_console_link() {
        let c = email_sandbox("eu-west-1", Some(false));
        assert_eq!(c.status, Status::Warn);
        assert!(c
            .fix
            .unwrap()
            .url
            .unwrap()
            .contains("region=eu-west-1#/account"));
        assert_eq!(email_sandbox("eu-west-1", Some(true)).status, Status::Pass);
        assert_eq!(email_sandbox("eu-west-1", None).status, Status::Warn);
    }

    #[test]
    fn model_outcomes() {
        assert_eq!(
            model(false, "us-east-1", "claude-sonnet-5-5", &ModelProbe::Ok).status,
            Status::Pass
        );
        let nokey = model(false, "us-east-1", "claude-sonnet-5-5", &ModelProbe::NoKey);
        assert_eq!(nokey.status, Status::Fail);
        assert_eq!(nokey.fix.unwrap().href.unwrap(), "/settings/models");
        assert!(model(
            false,
            "us-east-1",
            "m",
            &ModelProbe::Refused(401, String::new())
        )
        .detail
        .contains("refused"));
        let bedrock = model(
            true,
            "eu-west-1",
            "eu.anthropic.claude-sonnet-5-5",
            &ModelProbe::Refused(
                403,
                "AccessDeniedException: You don't have access to the model".into(),
            ),
        );
        assert_eq!(bedrock.status, Status::Fail);
        assert!(bedrock.detail.contains("model access isn't enabled"));
        let f = bedrock.fix.unwrap();
        assert!(f.url.unwrap().ends_with("region=eu-west-1#/modelaccess"));
        assert!(f.text.contains("Claude Sonnet 5.5"));
        assert_eq!(
            model(
                false,
                "us-east-1",
                "m",
                &ModelProbe::Refused(529, String::new())
            )
            .status,
            Status::Warn
        );
        assert_eq!(
            model(
                false,
                "us-east-1",
                "m",
                &ModelProbe::Unreachable("x".into())
            )
            .status,
            Status::Warn
        );
    }

    #[test]
    fn install_checks() {
        assert_eq!(dlq(Some(0)).status, Status::Pass);
        assert!(dlq(Some(3)).detail.starts_with("3 events"));
        assert_eq!(updates(None).status, Status::Warn);
        assert!(updates(Some(&json!({"settings": {"auto": true, "day": "sat", "hour": 22, "timezone": "Europe/Berlin"}}))).detail.contains("sat 22:00 Europe/Berlin"));
        assert_eq!(
            public_url("https://h", Some(200), None).status,
            Status::Pass
        );
        assert_eq!(
            public_url("https://h", Some(403), None).status,
            Status::Fail
        );
        let unknown = public_url("", None, Some("APP_URL and API_URL aren't set"));
        assert_eq!(unknown.status, Status::Fail);
        assert!(unknown.detail.contains("APP_URL"));
        assert!(public_url("https://h", None, None)
            .fix
            .unwrap()
            .command
            .unwrap()
            .ends_with("doctor"));
    }

    #[test]
    fn optional_integrations_skip_instead_of_failing() {
        assert_eq!(chat("slack", "Slack", false, false).status, Status::Skip);
        assert_eq!(chat("slack", "Slack", true, false).status, Status::Warn);
        assert_eq!(chat("teams", "Teams", true, true).status, Status::Pass);
        assert_eq!(jira(false).status, Status::Skip);
        let s = summary(&[jira(false), chat("slack", "Slack", true, true)]);
        assert_eq!(s["ready"], true);
        assert_eq!(s["skip"], 1);
    }

    #[test]
    fn preflight_blocks_on_failures_and_a_running_update() {
        let checks = vec![github_app(Some(401)), jira(false), dlq(Some(0))];
        let status = json!({
            "running": true,
            "latest": {"version": "v0.8.0", "breaking": true, "url": "https://github.com/CoderHelm/coderhelm-platform/releases/tag/v0.8.0"},
            "failed_versions": ["v0.8.0"],
            "history": [{"version": "v0.8.0", "status": "rolled_back", "reason": "health check failed after deploying v0.8.0"}],
        });
        let b = preflight(&checks, Some(&status));
        let keys: Vec<&str> = b.iter().map(|x| x.key.as_str()).collect();
        assert_eq!(
            keys,
            ["github_app", "running", "migration", "failed_before"]
        );
        assert_eq!(b[0].severity, "block");
        assert_eq!(b[2].severity, "warn");
        assert!(b[3].detail.contains("health check failed"));
        // Nothing wrong: nothing blocks.
        assert!(preflight(
            &[dlq(Some(0))],
            Some(&json!({"running": false, "latest": null}))
        )
        .is_empty());
        // No updater: one blocker, with the CLI instead.
        let none = preflight(&[], None);
        assert_eq!(
            none[0].fix.command.as_deref(),
            Some("npx -y github:CoderHelm/coderhelm-platform update")
        );
    }

    #[test]
    fn the_public_status_carries_no_details() {
        let secret = json!({"github_app_id": "123456", "github_private_key": "-----BEGIN RSA PRIVATE KEY-----", "setup_token": "x"});
        let keys = app_keys(Some(&secret));
        assert_eq!(keys, AppKeys::Present);
        let v = public_status(
            json!("v0.7.0"),
            true,
            public_github(keys, Some(200), Some(0)),
            public_models(true, Some(&ModelProbe::Refused(403, "AccessDeniedException: arn:aws:iam::111122223333:role/x is not authorized to perform bedrock:Converse on us.anthropic.claude".into()))),
            public_email(Some(&Identity { verified: true, ..Default::default() }), Some(false)),
            vec![
                public_domain(&DomainFacts { known: true, hostname: Some("helm.example.org".into()), state: "validating".into(), mode: "external".into(),
                    target: Some("d111test.cloudfront.net".into()), ..Default::default() }),
                public_google_sign_in(false, false),
                public_updates(Some(&json!({"settings": {"auto": false}}))),
            ],
            false,
        );
        let text = serde_json::to_string(&v).unwrap();
        for leak in [
            "123456",
            "111122223333",
            "arn:",
            "PRIVATE",
            "http",
            "@",
            "us-east",
            "anthropic.claude",
            "coderhelm-prod",
            "example.org",
            "cloudfront",
        ] {
            assert!(!text.contains(leak), "public status leaks {leak}: {text}");
        }
        assert_eq!(v["schema"], 1);
        let ids: Vec<&str> = v["checks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["id"].as_str().unwrap())
            .collect();
        assert_eq!(
            ids,
            [
                "api",
                "github_app",
                "github_installed",
                "models",
                "email",
                "domain",
                "google_sign_in",
                "updates",
                "admin"
            ]
        );
        assert_eq!(v["checks"][2]["code"], "not_installed");
        assert_eq!(v["checks"][3]["code"], "iam_denied");
        assert_eq!(v["checks"][4]["required"], false);
        assert_eq!(v["checks"][5]["code"], "validating");
        assert_eq!(v["checks"][5]["required"], false);
        assert_eq!(v["checks"][6]["code"], "off");
        assert_eq!(v["checks"][7]["code"], "auto_off");
        assert_eq!(v["checks"][8]["status"], "wait");
        assert_eq!(v["ready"], false);
        // Every check has only these fields.
        for c in v["checks"].as_array().unwrap() {
            for k in c.as_object().unwrap().keys() {
                assert!(
                    ["id", "label", "status", "code", "required"].contains(&k.as_str()),
                    "unexpected field {k}"
                );
            }
        }
    }

    #[test]
    fn the_public_status_github_codes() {
        let codes = |c: [PublicCheck; 2]| (c[0].code, c[0].status, c[1].code, c[1].status);
        assert_eq!(
            codes(public_github(AppKeys::Unreadable, None, None)),
            ("secret_unreadable", "fail", "needs_app", "wait")
        );
        let fresh =
            json!({"github_app_id": "", "github_private_key": "", "setup_token": "a".repeat(40)});
        assert_eq!(app_keys(Some(&fresh)), AppKeys::Missing { setup_key: true });
        assert_eq!(
            codes(public_github(app_keys(Some(&fresh)), None, None)).0,
            "missing"
        );
        assert_eq!(
            app_keys(Some(&json!({"github_app_id": ""}))),
            AppKeys::Missing { setup_key: false }
        );
        assert_eq!(
            codes(public_github(
                AppKeys::Missing { setup_key: false },
                None,
                None
            ))
            .0,
            "missing_no_setup_key"
        );
        assert_eq!(
            app_keys(Some(
                &json!({"github_app_id": "1", "github_private_key": "junk"})
            )),
            AppKeys::BadKey
        );
        assert_eq!(
            codes(public_github(AppKeys::BadKey, None, None)).2,
            "bad_key"
        );
        assert_eq!(
            codes(public_github(AppKeys::Present, Some(200), Some(3))),
            ("ok", "pass", "ok", "pass")
        );
        assert_eq!(
            codes(public_github(AppKeys::Present, Some(401), None)).2,
            "app_rejected"
        );
        assert_eq!(
            codes(public_github(AppKeys::Present, None, None)).2,
            "github_unreachable"
        );
    }

    #[test]
    fn the_public_status_model_and_email_codes() {
        assert_eq!(public_models(false, None).status, "skip");
        assert_eq!(public_models(true, Some(&ModelProbe::Ok)).code, "ok");
        let code = |m: &str| bedrock_code(400, m);
        assert_eq!(
            code("Model use case details have not been submitted for this account"),
            "use_case_required"
        );
        assert_eq!(
            code("Your request is being processed"),
            "subscription_pending"
        );
        assert_eq!(
            code("aws-marketplace:Subscribe failed: no valid payment method"),
            "marketplace_failed"
        );
        assert_eq!(
            code("User is not authorized to perform: bedrock:Converse with an explicit deny"),
            "iam_denied"
        );
        assert_eq!(
            code("You don't have access to the model with the specified model ID."),
            "access_denied"
        );
        assert_eq!(
            code("The provided model identifier is invalid."),
            "not_available"
        );
        assert_eq!(bedrock_code(403, "AccessDenied"), "access_denied");
        assert_eq!(code("boom"), "unknown");
        assert_eq!(
            public_models(
                true,
                Some(&ModelProbe::Refused(400, "being processed".into()))
            )
            .status,
            "warn"
        );
        assert_eq!(public_email(None, None).code, "unknown");
        assert_eq!(
            public_email(Some(&Identity::default()), Some(true)).code,
            "identity_unverified"
        );
        let ok = Identity {
            verified: true,
            ..Default::default()
        };
        assert_eq!(public_email(Some(&ok), Some(false)).code, "sandbox");
        assert_eq!(public_email(Some(&ok), Some(true)).status, "pass");
        // Optional and skipped checks don't block ready.
        let all_pass = public_status(
            json!(null),
            false,
            public_github(AppKeys::Present, Some(200), Some(1)),
            public_models(false, None),
            public_email(None, None),
            vec![public_domain(&DomainFacts {
                known: true,
                hostname: Some("h.example.org".into()),
                state: "validating".into(),
                ..Default::default()
            })],
            false,
        );
        assert_eq!(all_pass["ready"], true);
    }

    #[test]
    fn the_closed_window_says_only_done() {
        let v = closed_status(json!("v0.7.0"));
        assert_eq!(v["setup_open"], false);
        assert_eq!(v["ready"], true);
        assert_eq!(v["checks"].as_array().unwrap().len(), 1);
        assert_eq!(v["checks"][0]["id"], "admin");
    }

    // ── Your own domain ──

    fn ext(state: &str) -> DomainFacts {
        domain_facts_from(Some(&json!({
            "ok": true,
            "hostname": "helm.example.org",
            "state": state,
            "mode": "external",
            "target": "d111test.cloudfront.net",
            "current": "d111test.cloudfront.net",
            "certificate": {"status": "PENDING_VALIDATION", "validation": {"name": "_a1.helm.example.org.", "type": "CNAME", "value": "_b2.acm-validations.aws."}},
            "records": [
                {"purpose": "certificate", "name": "_a1.helm.example.org", "type": "CNAME", "value": "_b2.acm-validations.aws"},
                {"purpose": "address", "name": "helm.example.org", "type": "CNAME", "value": "d111test.cloudfront.net"}
            ]
        })))
    }

    #[test]
    fn domain_facts_read_the_updater() {
        let f = ext("validating");
        assert!(f.known);
        assert_eq!(
            f.validation.as_ref().unwrap().value,
            "_b2.acm-validations.aws"
        );
        assert_eq!(f.records.len(), 2);
        assert!(!domain_facts_from(None).known);
        assert!(!domain_facts_from(Some(&json!({"ok": false, "error": "x"}))).known);
        assert_eq!(domain_code(&domain_facts_from(None)), ("warn", "unknown"));
        let none = domain_facts_from(Some(
            &json!({"ok": true, "state": "none", "current": "d111test.cloudfront.net"}),
        ));
        assert_eq!(domain_code(&none), ("skip", "none"));
        let c = domain(&none);
        assert_eq!(c.status, Status::Skip);
        assert!(c.detail.contains("d111test.cloudfront.net"));
        assert_eq!(c.fix.unwrap().href.unwrap(), "/settings/system#domain");
    }

    #[test]
    fn caa_lets_amazon_issue_or_not() {
        let v = |x: &[&str]| x.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert!(caa_allows_amazon(&v(&[])));
        assert!(caa_allows_amazon(&v(&[
            "0 iodef \"mailto:sec@example.org\""
        ])));
        assert!(caa_allows_amazon(&v(&[
            "0 issue \"letsencrypt.org\"",
            "0 issue \"amazon.com\""
        ])));
        assert!(caa_allows_amazon(&v(&[
            "0 issue \"amazontrust.com; account=1\""
        ])));
        assert!(!caa_allows_amazon(&v(&["0 issue \"letsencrypt.org\""])));
        assert!(!caa_allows_amazon(&v(&["0 issue \";\""])));
    }

    #[test]
    fn waiting_for_the_certificate_shows_the_records_and_spots_a_wrong_value() {
        let mut f = ext("validating");
        assert_eq!(domain_code(&f), ("wait", "validating"));
        let c = domain(&f);
        assert_eq!(c.status, Status::Warn);
        let cmd = c.fix.unwrap().command.unwrap();
        assert!(
            cmd.contains("_a1.helm.example.org  CNAME  _b2.acm-validations.aws"),
            "{cmd}"
        );
        assert!(
            cmd.contains("helm.example.org  CNAME  d111test.cloudfront.net"),
            "{cmd}"
        );
        // Not there yet: still waiting (propagation).
        f.dns.validation_cname = Some(vec![]);
        assert_eq!(domain_code(&f).1, "validating");
        f.dns.validation_cname = Some(vec!["_b2.acm-validations.aws.".into()]);
        assert_eq!(domain_code(&f).1, "validating");
        f.dns.validation_cname = Some(vec!["_typo.acm-validations.aws.".into()]);
        assert_eq!(domain_code(&f), ("fail", "validation_wrong_value"));
        let c = domain(&f);
        assert!(
            c.detail.contains("_typo.acm-validations.aws"),
            "{}",
            c.detail
        );
        assert!(c.fix.unwrap().text.contains("_b2.acm-validations.aws"));
        // CAA that leaves Amazon out.
        f.dns.validation_cname = None;
        f.dns.caa = Some(vec!["0 issue \"letsencrypt.org\"".into()]);
        assert_eq!(domain_code(&f), ("fail", "caa_blocked"));
        assert!(domain(&f)
            .fix
            .unwrap()
            .command
            .unwrap()
            .contains("CAA  0 issue \"amazon.com\""));
    }

    #[test]
    fn a_route53_zone_public_dns_does_not_use_says_so() {
        let mut f = ext("validating");
        f.mode = "route53".into();
        f.zone = Some("example.org".into());
        f.name_servers = vec!["ns-1.awsdns-01.org".into(), "ns-2.awsdns-02.com".into()];
        f.dns.zone_ns = Some(vec![
            "ns-2.awsdns-02.com.".into(),
            "NS-1.awsdns-01.org.".into(),
        ]);
        assert_eq!(domain_code(&f), ("wait", "validating"));
        f.dns.zone_ns = Some(vec!["dns1.registrar.example".into()]);
        assert_eq!(domain_code(&f), ("fail", "zone_not_delegated"));
        assert!(domain(&f)
            .fix
            .unwrap()
            .command
            .unwrap()
            .contains("ns-1.awsdns-01.org"));
    }

    #[test]
    fn failures_name_their_cause() {
        let mut f = ext("failed");
        f.cert_status = Some("FAILED".into());
        f.cert_failure = Some("CAA_ERROR".into());
        assert_eq!(domain_code(&f).1, "caa_blocked");
        f.cert_failure = Some("DOMAIN_NOT_ALLOWED".into());
        assert_eq!(domain_code(&f).1, "certificate_failed");
        assert!(domain(&f).detail.contains("domain not allowed"));
        f.cert_status = Some("ISSUED".into());
        f.reason = "deploy failed: CNAMEAlreadyExists".into();
        assert_eq!(domain_code(&f), ("fail", "attach_failed"));
        assert!(domain(&f).detail.contains("CNAMEAlreadyExists"));
        assert_eq!(domain_code(&ext("attaching")), ("wait", "attaching"));
        assert_eq!(domain_code(&ext("removing")), ("wait", "removing"));
    }

    #[test]
    fn a_live_domain_resolves_serves_https_and_the_api() {
        let mut f = ext("live");
        f.dns.host_cname = Some(vec!["d111test.cloudfront.net.".into()]);
        f.dns.host_a = Some(vec!["192.0.2.1".into()]);
        f.https = Some(200);
        f.api = Some(true);
        assert_eq!(domain_code(&f), ("pass", "ok"));
        assert_eq!(domain(&f).status, Status::Pass);
        f.api = Some(false);
        assert_eq!(domain_code(&f).1, "api_unreachable");
        f.api = Some(true);
        f.dns.host_cname = Some(vec!["old-host.example.net.".into()]);
        assert_eq!(domain_code(&f), ("fail", "dns_wrong_value"));
        assert!(domain(&f).detail.contains("old-host.example.net"));
        // Nothing there yet: wait, unless it's the apex (no CNAME allowed).
        f.dns.host_cname = Some(vec![]);
        f.dns.host_a = Some(vec![]);
        f.https = None;
        assert_eq!(domain_code(&f), ("wait", "dns_missing"));
        assert!(domain(&f)
            .fix
            .unwrap()
            .command
            .unwrap()
            .contains("helm.example.org  CNAME  d111test.cloudfront.net"));
        f.dns.apex = true;
        assert_eq!(domain_code(&f), ("fail", "cname_at_apex"));
        // Resolves somewhere, but no HTTPS (a proxy or another server).
        f.dns.apex = false;
        f.dns.host_a = Some(vec!["198.51.100.7".into()]);
        assert_eq!(domain_code(&f), ("fail", "https_failed"));
        assert!(domain(&f).fix.unwrap().text.contains("DNS only"));
        // A domain from the config (Route 53 aliases, no CNAME) only needs HTTPS and the API.
        let mut cfg = ext("live");
        cfg.mode = "config".into();
        cfg.dns.host_a = Some(vec!["192.0.2.1".into()]);
        cfg.dns.host_cname = Some(vec![]);
        cfg.https = Some(200);
        cfg.api = Some(true);
        assert_eq!(domain_code(&cfg).1, "ok");
    }

    #[test]
    fn every_domain_code_has_a_sentence_and_a_fix() {
        let mut all = vec![];
        let mut f = ext("validating");
        all.push(domain(&f));
        f.dns.validation_cname = Some(vec!["_x.acm-validations.aws".into()]);
        all.push(domain(&f));
        f.dns.caa = Some(vec!["0 issue \"letsencrypt.org\"".into()]);
        all.push(domain(&f));
        for st in ["attaching", "removing", "failed"] {
            all.push(domain(&ext(st)));
        }
        for c in &all {
            assert!(!c.detail.is_empty());
            assert!(c.status == Status::Pass || c.fix.as_ref().is_some_and(|x| !x.text.is_empty()));
        }
        let text = serde_json::to_string(&all).unwrap();
        for g in ['\u{2192}', '\u{21D2}', '\u{279C}'] {
            assert!(!text.contains(g));
        }
        assert!(!text.contains("brain"));
    }

    #[test]
    fn google_sign_in_and_updates_for_the_guide() {
        assert_eq!(google_sign_in(false, false).status, Status::Skip);
        assert_eq!(google_sign_in(true, false).status, Status::Fail);
        assert_eq!(google_sign_in(true, true).status, Status::Pass);
        assert_eq!(public_google_sign_in(true, false).code, "missing_keys");
        assert!(public_google_sign_in(false, false).optional);
        assert_eq!(public_updates(None).code, "unknown");
        assert_eq!(
            public_updates(Some(&json!({"settings": {"auto": true}}))).code,
            "ok"
        );
        assert_eq!(
            public_updates(Some(&json!({"settings": {"auto": false}}))).code,
            "auto_off"
        );
        assert_eq!(probe_answer()["service"], "coderhelm");
    }

    #[test]
    fn no_arrow_glyphs_in_any_copy() {
        let all = [
            github_keys("", ""),
            github_permissions(&app(json!({}), json!([]))),
            email_identity("a@b.co", "us-east-1", None),
            model(
                true,
                "us-east-1",
                "m",
                &ModelProbe::Refused(403, "access".into()),
            ),
        ];
        let text = serde_json::to_string(&all).unwrap();
        for g in ['\u{2192}', '\u{21D2}', '\u{279C}', '\u{25B6}'] {
            assert!(!text.contains(g));
        }
    }
}
