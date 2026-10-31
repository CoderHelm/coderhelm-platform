//! Every email CoderHelm sends goes through here: which emails an install
//! sends at all, and how each one leaves (the install's own Amazon SES, or
//! CoderHelm's mail relay when a self-hosted admin chose it).
//!
//! - Essential emails (invites, approval requests, alerts an admin needs,
//!   test emails, update notices) always send.
//! - Optional emails ([`OPTIONAL`]: the weekly summary, the welcome email)
//!   send on the hosted deployment as before. On a self-hosted install each
//!   is off until an install admin turns it on (Settings, Emails), stored in
//!   [`SETTINGS_PK`]/[`SETTINGS_SK`]; the config's starting value
//!   (`OPTIONAL_EMAIL_DEFAULTS`) applies until then.
//! - A self-hosted install sends through CoderHelm only when an admin chose
//!   "Through CoderHelm" and the install is registered for hybrid. Nothing
//!   falls back to another sender: a relay that can't be reached is a failed
//!   send, shown in Deliveries.
//! - A self-hosted install never emails a CoderHelm address
//!   ([`relay::is_coderhelm_address`]), by either route.
//!
//! Sign-in codes are not here: Amazon Cognito makes, sends and checks them
//! inside the install (its own sender, or the install's SES), so a code never
//! reaches CoderHelm.

pub mod naming;
pub mod relay;
pub mod stats;
pub mod templates;

#[cfg(test)]
mod golden_tests;

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Where the install's email settings live on the settings table.
pub const SETTINGS_PK: &str = "SYSTEM";
pub const SETTINGS_SK: &str = "SETTINGS#EMAIL_OPTIONAL";

/// What kind of deployment this is (`DEPLOYMENT_KIND`). Anything but
/// `hosted` counts as self-hosted, so a missing value never turns optional
/// email on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deployment {
    Hosted,
    SelfHosted,
}

pub fn deployment_from(v: Option<&str>) -> Deployment {
    match v.map(str::trim) {
        Some("hosted") => Deployment::Hosted,
        _ => Deployment::SelfHosted,
    }
}

pub fn deployment() -> Deployment {
    deployment_from(std::env::var("DEPLOYMENT_KIND").ok().as_deref())
}

/// An email the product works without.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct OptionalEmail {
    pub id: &'static str,
    pub label: &'static str,
    pub description: &'static str,
}

pub const WEEKLY_SUMMARY: &str = "weekly_summary";
pub const WELCOME: &str = "welcome";

pub const OPTIONAL: &[OptionalEmail] = &[
    OptionalEmail {
        id: WEEKLY_SUMMARY,
        label: "Weekly summary",
        description: "A Monday email to each person with their team's week: merged pull requests, runs, reviews and token use.",
    },
    OptionalEmail {
        id: WELCOME,
        label: "Welcome email",
        description: "A short hello with a link to get started, sent once when someone signs up.",
    },
];

/// An email the install needs, and why.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct EssentialEmail {
    pub id: &'static str,
    pub label: &'static str,
    pub why: &'static str,
}

pub const ESSENTIAL: &[EssentialEmail] = &[
    EssentialEmail { id: "sign_in", label: "Sign-up codes and password resets", why: "People can't verify their address or get back into their account without them. Amazon Cognito sends these inside your install." },
    EssentialEmail { id: "team_invite", label: "Team invites", why: "The only way an invited person learns how to join." },
    EssentialEmail { id: "plugin_approval", label: "Plugin approval requests", why: "A named approver must approve a plugin draft before it publishes." },
    EssentialEmail { id: "knowledge_signin", label: "Knowledge source sign-in alerts", why: "Tells admins when a knowledge source stopped syncing and needs signing in again." },
    EssentialEmail { id: "token_warning", label: "Token limit warnings", why: "Tells the team before work stops at the monthly limit, and when it does. Each person can turn these off." },
    EssentialEmail { id: "support_update", label: "Support ticket updates", why: "Replies and status changes on tickets someone filed." },
    EssentialEmail { id: "webhook_disabled", label: "Webhook endpoint turned off", why: "Tells a team's admins when CoderHelm stopped sending to an endpoint that failed for days." },
    EssentialEmail { id: "two_factor_lockout", label: "Two-factor lockout alerts", why: "Tells admins and owners when someone's two-factor sign-in was paused after too many wrong codes." },
    EssentialEmail { id: "two_factor_required", label: "Two-factor sign-in required", why: "Tells people without a second factor that an admin now requires one, before their next sign-in is held." },
    EssentialEmail { id: "two_factor_reset", label: "Two-factor sign-in reset", why: "Tells a person an admin reset their second factor, so a reset they didn't ask for doesn't go unnoticed." },
    EssentialEmail { id: "update_notice", label: "Update notices", why: "Tells install admins about new releases, finished or failed updates and address changes." },
    EssentialEmail { id: "test_email", label: "Test email", why: "Sent only when an admin presses Send test email in Setup." },
];

/// How a self-hosted install sends.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    /// The install's own Amazon SES (the default).
    #[default]
    Ses,
    /// CoderHelm's mail relay (hybrid).
    Coderhelm,
}

impl Transport {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ses => "ses",
            Self::Coderhelm => "coderhelm",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "ses" => Some(Self::Ses),
            "coderhelm" => Some(Self::Coderhelm),
            _ => None,
        }
    }
}

/// The install's email settings, as stored.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settings {
    /// Optional email id: on or off. Missing: the config's starting value.
    #[serde(default)]
    pub optional: BTreeMap<String, bool>,
    #[serde(default)]
    pub transport: Transport,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub updated_by: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub updated_at: String,
}

/// The optional emails the config turns on from the start
/// (`OPTIONAL_EMAIL_DEFAULTS`, comma separated). Empty on a self-hosted
/// install unless its config says otherwise.
pub fn config_defaults() -> Vec<String> {
    defaults_from(std::env::var("OPTIONAL_EMAIL_DEFAULTS").ok().as_deref())
}

/// How the install sends until an admin decides (`EMAIL_TRANSPORT_DEFAULT`,
/// from the config's `email.transport`): its own SES unless set.
pub fn transport_default() -> Transport {
    std::env::var("EMAIL_TRANSPORT_DEFAULT")
        .ok()
        .and_then(|v| Transport::parse(v.trim()))
        .unwrap_or_default()
}

/// The transport in force: the stored choice, else the config's.
pub fn transport_of(settings: Option<&Settings>, default: Transport) -> Transport {
    settings.map(|s| s.transport).unwrap_or(default)
}

pub fn defaults_from(v: Option<&str>) -> Vec<String> {
    v.unwrap_or("")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| OPTIONAL.iter().any(|o| o.id == s))
        .collect()
}

/// Whether optional email `id` is on.
pub fn optional_on(
    dep: Deployment,
    settings: Option<&Settings>,
    defaults: &[String],
    id: &str,
) -> bool {
    match dep {
        Deployment::Hosted => true,
        Deployment::SelfHosted => settings
            .and_then(|s| s.optional.get(id).copied())
            .unwrap_or_else(|| defaults.iter().any(|d| d == id)),
    }
}

/// How one email leaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    Ses,
    Relay,
}

/// The route for a send, or why there is none. The hosted deployment always
/// uses its SES; a self-hosted install uses the relay only when an admin
/// chose it, and then only that.
pub fn route(
    dep: Deployment,
    transport: Transport,
    registered: bool,
) -> Result<Route, &'static str> {
    match (dep, transport) {
        (Deployment::Hosted, _) | (Deployment::SelfHosted, Transport::Ses) => Ok(Route::Ses),
        (Deployment::SelfHosted, Transport::Coderhelm) if crate::hybrid::turned_off() => {
            Err("relay_turned_off")
        }
        (Deployment::SelfHosted, Transport::Coderhelm) if registered => Ok(Route::Relay),
        (Deployment::SelfHosted, Transport::Coderhelm) => Err("relay_not_registered"),
    }
}

pub fn from_item(item: &std::collections::HashMap<String, AttributeValue>) -> Option<Settings> {
    let data = item.get("data")?.as_s().ok()?;
    serde_json::from_str(data).ok()
}

static CACHE: Mutex<Option<(Instant, Option<Settings>)>> = Mutex::new(None);
const CACHE_FOR: Duration = Duration::from_secs(30);

/// The stored settings (cached 30 seconds). `Err` when the store can't be
/// read: callers send no optional email and no relayed email then.
pub async fn load(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
) -> Result<Option<Settings>, String> {
    if let Some((at, s)) = CACHE.lock().ok().and_then(|g| g.clone()) {
        if at.elapsed() < CACHE_FOR {
            return Ok(s);
        }
    }
    let r = dynamo
        .get_item()
        .table_name(table)
        .key("pk", AttributeValue::S(SETTINGS_PK.into()))
        .key("sk", AttributeValue::S(SETTINGS_SK.into()))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("email settings: {e}"))?;
    let s = r.item().and_then(from_item);
    if let Ok(mut g) = CACHE.lock() {
        *g = Some((Instant::now(), s.clone()));
    }
    Ok(s)
}

/// Drops the cache (after an admin saves).
pub fn forget() {
    if let Ok(mut g) = CACHE.lock() {
        *g = None;
    }
}

pub async fn save(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    s: &Settings,
) -> Result<(), String> {
    dynamo
        .put_item()
        .table_name(table)
        .item("pk", AttributeValue::S(SETTINGS_PK.into()))
        .item("sk", AttributeValue::S(SETTINGS_SK.into()))
        .item(
            "data",
            AttributeValue::S(serde_json::to_string(s).unwrap_or_default()),
        )
        .send()
        .await
        .map_err(|e| format!("email settings: {e}"))?;
    forget();
    Ok(())
}

/// Whether optional email `id` should go out on this deployment now.
pub async fn optional_allowed(dynamo: &aws_sdk_dynamodb::Client, table: &str, id: &str) -> bool {
    let dep = deployment();
    if dep == Deployment::Hosted {
        return true;
    }
    match load(dynamo, table).await {
        Ok(s) => optional_on(dep, s.as_ref(), &config_defaults(), id),
        Err(e) => {
            tracing::warn!(error = %e, email = id, "Email settings unreadable: optional email not sent");
            false
        }
    }
}

/// The SES form of an email (how it has always been sent).
#[derive(Debug, Clone)]
pub enum SesBody {
    Simple {
        subject: String,
        text: String,
        html: Option<String>,
        headers: Vec<(String, String)>,
    },
    Template {
        name: String,
        data: String,
    },
}

/// One email to send.
#[derive(Debug, Clone)]
pub struct Mail {
    /// The Deliveries event (`invite`, `weekly_summary`, ...).
    pub kind: String,
    pub to: String,
    /// The relay form: a template id and its parameters.
    pub template: &'static str,
    pub params: Map<String, Value>,
    /// The SES form.
    pub ses: SesBody,
}

/// Where a mail is sent from, and what with.
pub struct Sender<'a> {
    pub ses: &'a aws_sdk_sesv2::Client,
    pub http: &'a reqwest::Client,
    pub dynamo: &'a aws_sdk_dynamodb::Client,
    pub settings_table: &'a str,
    pub from: &'a str,
}

/// A failed send, with the reason code Deliveries explains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendError {
    pub code: String,
    pub message: String,
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message, self.code)
    }
}

impl std::error::Error for SendError {}

fn err(code: &str, message: impl Into<String>) -> SendError {
    SendError {
        code: code.into(),
        message: message.into(),
    }
}

/// Sends one email by the install's route. Never falls back to the other.
pub async fn send(s: &Sender<'_>, mail: &Mail) -> Result<Route, SendError> {
    let dep = deployment();
    if dep == Deployment::SelfHosted && relay::is_coderhelm_address(&mail.to) {
        return Err(err(
            "coderhelm_address",
            "A self-hosted install never emails CoderHelm's own addresses.",
        ));
    }
    let transport = if dep == Deployment::Hosted {
        Transport::Ses
    } else {
        let stored = load(s.dynamo, s.settings_table)
            .await
            .map_err(|e| err("settings_unreadable", e))?;
        transport_of(stored.as_ref(), transport_default())
    };
    let creds = crate::hybrid::client::credentials();
    match route(dep, transport, creds.is_some()) {
        Ok(Route::Ses) => send_ses(s, mail).await.map(|_| Route::Ses),
        Ok(Route::Relay) => {
            let creds = creds.ok_or_else(|| err("relay_not_registered", "Not registered."))?;
            send_relay(s.http, &creds, mail).await.map(|_| Route::Relay)
        }
        Err(code) => Err(err(
            code,
            match code {
                "relay_turned_off" => "Email is set to go through CoderHelm, but this install's config turned hybrid off.",
                _ => "Email is set to go through CoderHelm, but this install isn't registered with CoderHelm. Register it in Settings, System, or switch email back to Amazon SES.",
            },
        )),
    }
}

async fn send_ses(s: &Sender<'_>, mail: &Mail) -> Result<(), SendError> {
    use aws_sdk_sesv2::types::{
        Body, Content, Destination, EmailContent, Message, MessageHeader, Template,
    };
    if s.from.is_empty() {
        return Err(err("email_not_configured", "No sender address configured."));
    }
    let content = match &mail.ses {
        SesBody::Simple {
            subject,
            text,
            html,
            headers,
        } => {
            let c = |v: &str| {
                Content::builder()
                    .data(v)
                    .charset("UTF-8")
                    .build()
                    .map_err(|e| err("email_failed", e.to_string()))
            };
            let mut body = Body::builder().text(c(text)?);
            if let Some(h) = html {
                body = body.html(c(h)?);
            }
            let mut m = Message::builder().subject(c(subject)?).body(body.build());
            for (name, value) in headers {
                m = m.headers(
                    MessageHeader::builder()
                        .name(name)
                        .value(value)
                        .build()
                        .map_err(|e| err("email_failed", e.to_string()))?,
                );
            }
            EmailContent::builder().simple(m.build()).build()
        }
        SesBody::Template { name, data } => EmailContent::builder()
            .template(
                Template::builder()
                    .template_name(name)
                    .template_data(data)
                    .build(),
            )
            .build(),
    };
    // The kind, for the counts Deliveries shows (stats::read_event).
    let kind = stats::kind_of(if mail.template.is_empty() {
        &mail.kind
    } else {
        mail.template
    });
    let tag = aws_sdk_sesv2::types::MessageTag::builder()
        .name(stats::KIND_TAG)
        .value(kind)
        .build()
        .map_err(|e| err("email_failed", e.to_string()))?;
    s.ses
        .send_email()
        .from_email_address(ses_from(s.from))
        .destination(Destination::builder().to_addresses(&mail.to).build())
        .content(content)
        .email_tags(tag)
        .send()
        .await
        .map_err(|e| {
            let text = aws_sdk_sesv2::error::DisplayErrorContext(&e).to_string();
            let r = text.to_ascii_lowercase();
            let code = if r.contains("not verified") || r.contains("sandbox") {
                "email_sandbox"
            } else if r.contains("accessdenied") || r.contains("not authorized") {
                "email_denied"
            } else {
                "email_failed"
            };
            err(code, text)
        })?;
    Ok(())
}

/// The install's own SES From line: its address with a name
/// ([`naming::ses_display_name`]) on a self-hosted install. A sender that
/// already carries a name, and the hosted deployment's, stay as they are.
pub fn ses_from(from: &str) -> String {
    if deployment() == Deployment::Hosted || from.contains('<') {
        return from.to_string();
    }
    let app = crate::public_urls::app_url_from_env();
    let name = naming::ses_display_name(
        std::env::var("EMAIL_FROM_NAME").ok().as_deref(),
        std::env::var("ORGANIZATION_NAME").ok().as_deref(),
        crate::public_urls::host_of(app),
        &naming::coderhelm_zone(),
    );
    naming::from_header(&name, from)
}

/// Sends through CoderHelm's relay. An address the relay doesn't know yet is
/// listed once (as a member, vouched for by an install admin) and the send
/// tried again; invites are listed by the invite handler before sending.
async fn send_relay(
    http: &reqwest::Client,
    creds: &crate::hybrid::client::Credentials,
    mail: &Mail,
) -> Result<(), SendError> {
    // Checked here too, so a mistake shows on the install with the reason.
    let t = templates::get(mail.template)
        .ok_or_else(|| err("unknown_template", "No such email template."))?;
    let req = |id: String| relay::SendRequest {
        request_id: id,
        to: mail.to.clone(),
        template: t.id.to_string(),
        params: mail.params.clone(),
        contact: install_admin(),
        org: naming::org_from_env(),
    };
    let call = |r: relay::SendRequest| async move {
        let body = serde_json::to_value(&r).unwrap_or(Value::Null);
        crate::hybrid::client::call(http, creds, "POST", relay::PATH_SEND, Some(&body)).await
    };
    match call(req(relay::new_request_id())).await {
        Ok(_) => Ok(()),
        Err(e) if e.code == relay::Refusal::NotListed.code() => {
            let by = install_admin().ok_or_else(|| {
                err(
                    "recipient_not_listed",
                    "The recipient isn't listed with CoderHelm and the install has no admin to vouch for them.",
                )
            })?;
            // The admin is listed too, so the first listing of an install
            // whose config chose the relay works without a dashboard visit.
            let mut add = vec![relay::NewRecipient {
                email: by.clone(),
                kind: relay::RecipientKind::Admin,
            }];
            if relay::normalize(&mail.to) != relay::normalize(&by) {
                add.push(relay::NewRecipient {
                    email: mail.to.clone(),
                    kind: relay::RecipientKind::Member,
                });
            }
            list(http, creds, &by, &add).await?;
            call(req(relay::new_request_id()))
                .await
                .map(|_| ())
                .map_err(|e| err(&e.code, e.message))
        }
        // A link to the install's own domain the relay hasn't proved yet:
        // ask it to (it reaches the install over HTTPS on that name), then
        // try once more.
        Err(e) if e.code == "link_not_own_host" => {
            let host =
                crate::public_urls::host_of(crate::public_urls::app_url_from_env()).to_string();
            if !relay::custom_domain_ok(&host) {
                return Err(err(&e.code, e.message));
            }
            let body = serde_json::json!({ "hostname": host });
            crate::hybrid::client::call(http, creds, "POST", relay::PATH_DOMAIN, Some(&body))
                .await
                .map_err(|e| err(&e.code, e.message))?;
            call(req(relay::new_request_id()))
                .await
                .map(|_| ())
                .map_err(|e| err(&e.code, e.message))
        }
        Err(e) => Err(err(&e.code, e.message)),
    }
}

/// The first install admin (`PLATFORM_ADMIN_EMAILS`), who stands behind
/// listings the install makes on its own.
pub fn install_admin() -> Option<String> {
    std::env::var("PLATFORM_ADMIN_EMAILS")
        .ok()?
        .split(',')
        .map(str::trim)
        .find(|e| relay::valid_address(e))
        .map(str::to_string)
}

/// Lists addresses with the relay. `by` is the install admin behind it.
pub async fn list(
    http: &reqwest::Client,
    creds: &crate::hybrid::client::Credentials,
    by: &str,
    add: &[relay::NewRecipient],
) -> Result<(), SendError> {
    for chunk in add.chunks(relay::MAX_PER_REQUEST) {
        let body = relay::RecipientsRequest {
            request_id: relay::new_request_id(),
            by: by.to_string(),
            add: chunk.to_vec(),
            remove: vec![],
        };
        let body = serde_json::to_value(&body).unwrap_or(Value::Null);
        crate::hybrid::client::call(http, creds, "POST", relay::PATH_RECIPIENTS, Some(&body))
            .await
            .map_err(|e| err(&e.code, e.message))?;
    }
    Ok(())
}

/// Lists one address as a pending invite, when the install sends through
/// CoderHelm (nothing to do otherwise). `by` is the admin sending it.
pub async fn list_invite(s: &Sender<'_>, by: &str, to: &str) -> Result<(), SendError> {
    if deployment() == Deployment::Hosted {
        return Ok(());
    }
    let stored = load(s.dynamo, s.settings_table)
        .await
        .map_err(|e| err("settings_unreadable", e))?;
    let transport = transport_of(stored.as_ref(), transport_default());
    let Some(creds) = crate::hybrid::client::credentials() else {
        return Ok(());
    };
    if transport != Transport::Coderhelm {
        return Ok(());
    }
    list(
        s.http,
        &creds,
        by,
        &[relay::NewRecipient {
            email: to.to_string(),
            kind: relay::RecipientKind::Invite,
        }],
    )
    .await
}

/// The Deliveries record for one send: domain only, never the address, and
/// never a parameter.
pub fn delivery(
    kind: &str,
    to: &str,
    result: &Result<Route, SendError>,
) -> crate::deliveries::Delivery {
    let r = result.as_ref().map(|_| ()).map_err(|e| e.code.clone());
    let mut d = crate::deliveries::email(kind, to, &r);
    if let Err(e) = result {
        // `email` guessed from the text; the code is known here.
        d.reason = e.code.clone();
    } else if let Ok(Route::Relay) = result {
        d.outcome = "Sent through CoderHelm".into();
    }
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_or_unknown_deployment_kind_is_self_hosted() {
        assert_eq!(deployment_from(Some("hosted")), Deployment::Hosted);
        assert_eq!(deployment_from(Some("self-hosted")), Deployment::SelfHosted);
        assert_eq!(deployment_from(Some("")), Deployment::SelfHosted);
        assert_eq!(deployment_from(None), Deployment::SelfHosted);
    }

    #[test]
    fn self_hosted_sends_no_optional_email_until_an_admin_opts_in() {
        let dep = Deployment::SelfHosted;
        for o in OPTIONAL {
            assert!(!optional_on(dep, None, &[], o.id), "{}", o.id);
            assert!(!optional_on(dep, Some(&Settings::default()), &[], o.id));
        }
        let mut s = Settings::default();
        s.optional.insert(WEEKLY_SUMMARY.into(), true);
        assert!(optional_on(dep, Some(&s), &[], WEEKLY_SUMMARY));
        assert!(!optional_on(dep, Some(&s), &[], WELCOME));
        // The config's starting value, until the admin decides.
        let d = defaults_from(Some("weekly_summary, nonsense"));
        assert_eq!(d, vec!["weekly_summary".to_string()]);
        assert!(optional_on(dep, None, &d, WEEKLY_SUMMARY));
        s.optional.insert(WEEKLY_SUMMARY.into(), false);
        assert!(!optional_on(dep, Some(&s), &d, WEEKLY_SUMMARY));
    }

    #[test]
    fn hosted_sends_optional_email_as_before() {
        for o in OPTIONAL {
            assert!(optional_on(Deployment::Hosted, None, &[], o.id));
            let mut s = Settings::default();
            s.optional.insert(o.id.into(), false);
            assert!(optional_on(Deployment::Hosted, Some(&s), &[], o.id));
        }
    }

    #[test]
    fn essential_and_optional_lists_do_not_overlap() {
        for e in ESSENTIAL {
            assert!(!OPTIONAL.iter().any(|o| o.id == e.id), "{}", e.id);
        }
    }

    #[test]
    fn the_relay_is_used_only_when_a_self_hosted_admin_chose_it() {
        use Deployment::*;
        assert_eq!(route(Hosted, Transport::Coderhelm, true), Ok(Route::Ses));
        assert_eq!(route(SelfHosted, Transport::Ses, true), Ok(Route::Ses));
        assert_eq!(route(SelfHosted, Transport::Ses, false), Ok(Route::Ses));
        assert_eq!(
            route(SelfHosted, Transport::Coderhelm, true),
            Ok(Route::Relay)
        );
        // Chosen but not registered: refused, never SES instead.
        assert_eq!(
            route(SelfHosted, Transport::Coderhelm, false),
            Err("relay_not_registered")
        );
        assert_eq!(Settings::default().transport, Transport::Ses);
        let stored: Settings = serde_json::from_str("{}").unwrap();
        assert_eq!(stored.transport, Transport::Ses);
        // The config's choice holds until an admin saves one.
        assert_eq!(
            transport_of(None, Transport::Coderhelm),
            Transport::Coderhelm
        );
        assert_eq!(transport_of(None, Transport::Ses), Transport::Ses);
        assert_eq!(
            transport_of(Some(&Settings::default()), Transport::Coderhelm),
            Transport::Ses
        );
    }

    #[test]
    fn settings_round_trip() {
        let mut s = Settings {
            transport: Transport::Coderhelm,
            ..Default::default()
        };
        s.optional.insert(WELCOME.into(), true);
        let back: Settings = serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
        assert_eq!(back, s);
        assert_eq!(Transport::parse("coderhelm"), Some(Transport::Coderhelm));
        assert_eq!(Transport::parse("smtp"), None);
    }

    #[test]
    fn deliveries_keep_the_domain_and_the_reason_only() {
        let mail = Mail {
            kind: "invite".into(),
            to: "dev@example.com".into(),
            template: "team_invite",
            params: Map::new(),
            ses: SesBody::Template {
                name: "x".into(),
                data: "{}".into(),
            },
        };
        let d = delivery(
            &mail.kind,
            &mail.to,
            &Err(err("recipient_not_listed", "no")),
        );
        assert_eq!(d.target, "example.com");
        assert_eq!(d.reason, "recipient_not_listed");
        let d = delivery(&mail.kind, &mail.to, &Ok(Route::Relay));
        assert_eq!(d.outcome, "Sent through CoderHelm");
        assert!(!serde_json::to_string(&d).unwrap().contains("dev@"));
    }
}

/// The adversarial pass: a self-hosted install (or someone holding its
/// registration secret) trying to use CoderHelm's mail relay to look like
/// CoderHelm, to reach outside people, or to carry a phishing link. Each
/// attempt is refused by the same code the hosted side runs.
#[cfg(test)]
mod adversarial {
    use super::relay::{self, Listing, RecipientKind, Refusal};
    use super::templates::{self, check};
    use crate::hybrid::sign;
    use serde_json::{json, Map, Value};

    const MASTER: &str = "0123456789abcdef0123456789abcdef-master";
    const A: &str = "ins_aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "ins_bbbbbbbbbbbbbbbbbbbbbbbb";
    const NOW: i64 = 1_800_000_000;

    fn hosts() -> Vec<String> {
        vec![
            "d1example.cloudfront.net".into(),
            "acme.coderhelm.com".into(),
        ]
    }

    fn obj(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn an_email_dressed_as_coderhelm_support_is_refused_or_reads_as_the_install() {
        // No subject, no HTML, no sender of its own: unknown parameters.
        for field in [
            "subject",
            "html",
            "from",
            "reply_to",
            "display_name",
            "headers",
        ] {
            let r = check(
                "test_email",
                &obj(json!({ field: "CoderHelm Support" })),
                &hosts(),
                true,
            );
            assert_eq!(
                r.unwrap_err(),
                templates::Refusal::UnknownParam(field.into()),
                "{field}"
            );
        }
        // Wording that reads as CoderHelm staff, in any parameter.
        let r = check(
            "support_update",
            &obj(json!({
                "ticket": "CoderHelm Security: verify your account",
                "note": "Hello",
                "link": "https://acme.coderhelm.com/support/"
            })),
            &hosts(),
            true,
        );
        assert_eq!(r.unwrap_err().code(), "param_reserved");
        // A claimed name like support.coderhelm.com, or an organization
        // named "CoderHelm Support", can't become the sender name.
        for (org, name) in [
            (None, "support.coderhelm.com"),
            (None, "coderhelm-security.coderhelm.com"),
            (Some("CoderHelm Support"), "c0derhe1m.coderhelm.com"),
            (Some("Billing"), "d1example.cloudfront.net"),
        ] {
            let label = relay::sender_label(org, None, Some(name), NOW);
            assert_eq!(relay::display_name(label.as_deref()), "CoderHelm", "{name}");
        }
        // The relay's own notices (pause) can't be asked for by an install.
        assert_eq!(
            check(
                "relay_paused",
                &obj(json!({"reason": "bounces"})),
                &hosts(),
                true
            )
            .unwrap_err(),
            templates::Refusal::NotForInstalls
        );
        // Every subject is fixed, and every email says whose install sent it.
        let (t, v) = check("test_email", &Map::new(), &hosts(), true).unwrap();
        let r = templates::render(t, &v, "acme.coderhelm.com", None, Some("acme"));
        assert_eq!(r.subject, "acme CoderHelm: test email, delivery works");
        assert!(r
            .text
            .contains("self-hosted CoderHelm install at acme.coderhelm.com"));
        assert!(r.text.contains("never asks for your password"));
    }

    #[test]
    fn an_outside_address_gets_nothing() {
        // Not listed: refused.
        assert_eq!(
            relay::may_send("ceo@victim.example", &Listing::default(), false),
            Err(Refusal::NotListed)
        );
        // Listing it needs a listed admin behind the request...
        let req = relay::RecipientsRequest {
            request_id: relay::new_request_id(),
            by: "attacker@acme.test".into(),
            add: vec![relay::NewRecipient {
                email: "ceo@victim.example".into(),
                kind: RecipientKind::Invite,
            }],
            remove: vec![],
        };
        assert_eq!(relay::may_list(&req, None, true, 0), Err(Refusal::NotAdmin));
        assert_eq!(
            relay::may_list(&req, Some(RecipientKind::Member), true, 0),
            Err(Refusal::NotAdmin)
        );
        // ...and even an admin lists only a few new addresses a day.
        let mut many = req.clone();
        many.by = "admin@acme.test".into();
        many.add = (0..=relay::NEW_RECIPIENTS_PER_DAY)
            .map(|i| relay::NewRecipient {
                email: format!("p{i}@victim.example"),
                kind: RecipientKind::Invite,
            })
            .collect();
        assert_eq!(
            relay::may_list(&many, Some(RecipientKind::Admin), true, 0),
            Err(Refusal::TooManyNew)
        );
        // CoderHelm's own mailboxes are never a recipient.
        let listed = Listing {
            recipient: Some(RecipientKind::Member),
            suppressed: false,
        };
        assert_eq!(
            relay::may_send("security@coderhelm.com", &listed, false),
            Err(Refusal::CoderhelmAddress)
        );
        // Header injection into the recipient.
        assert_eq!(
            relay::may_send("a@acme.test\r\nBcc: ceo@victim.example", &listed, false),
            Err(Refusal::BadAddress)
        );
    }

    #[test]
    fn a_phishing_link_never_reaches_the_email() {
        // As a link: only the install's own verified host.
        for link in [
            "https://coderhelm-login.example/reset",
            "https://acme.coderhelm.com.evil.example/",
            "https://acme.coderhelm.com@evil.example/",
            "https://app.coderhelm.com/settings",
            "http://acme.coderhelm.com/",
        ] {
            let r = check("welcome", &obj(json!({ "link": link })), &hosts(), true);
            assert_eq!(r.unwrap_err().code(), "link_not_own_host", "{link}");
        }
        // Hidden in text: removed before anything is written.
        let (t, v) = check(
            "knowledge_signin",
            &obj(json!({
                "source": "Docs",
                "problem": "Sign in again at https://evil.example/login or evil.com/login or www.evil.example",
                "link": "https://acme.coderhelm.com/settings/knowledge/"
            })),
            &hosts(),
            true,
        )
        .unwrap();
        let r = templates::render(t, &v, "acme.coderhelm.com", None, None);
        for bad in ["evil.example", "evil.com"] {
            assert!(
                !r.text.contains(bad) && !r.html.contains(bad),
                "{bad} survived"
            );
        }
        // The only anchors are the template's buttons, to the install.
        let hrefs: Vec<&str> = r
            .html
            .match_indices("href=\"")
            .map(|(i, _)| &r.html[i + 6..])
            .collect();
        assert!(
            hrefs
                .iter()
                .all(|h| h.starts_with("https://acme.coderhelm.com/")),
            "{hrefs:?}"
        );
    }

    #[test]
    fn one_install_cannot_sign_as_another_or_replay() {
        let secret_a = sign::install_secret(MASTER, A);
        let secret_b = sign::install_secret(MASTER, B);
        assert_ne!(secret_a, secret_b);
        let body = br#"{"request_id":"01JABCDEFGHJKMNPQRSTVWXYZ0","to":"x@acme.test","template":"test_email","params":{}}"#;
        let sig = sign::sign_request(&secret_a, NOW, "POST", relay::PATH_SEND, body);
        let ts = NOW.to_string();
        // A's own signature, checked as A: fine.
        assert!(sign::verify_request(
            &secret_a,
            Some(&ts),
            Some(&sig),
            "POST",
            relay::PATH_SEND,
            body,
            NOW
        )
        .is_ok());
        // Sent with B's id: the hosted side derives B's secret, and it fails.
        assert!(sign::verify_request(
            &secret_b,
            Some(&ts),
            Some(&sig),
            "POST",
            relay::PATH_SEND,
            body,
            NOW
        )
        .is_err());
        // Old: outside the five-minute window.
        assert!(sign::verify_request(
            &secret_a,
            Some(&ts),
            Some(&sig),
            "POST",
            relay::PATH_SEND,
            body,
            NOW + 3600
        )
        .is_err());
        // Changed body (another recipient) under the same signature.
        let other = br#"{"request_id":"01JABCDEFGHJKMNPQRSTVWXYZ0","to":"ceo@victim.example","template":"test_email","params":{}}"#;
        assert!(sign::verify_request(
            &secret_a,
            Some(&ts),
            Some(&sig),
            "POST",
            relay::PATH_SEND,
            other,
            NOW
        )
        .is_err());
        // The request id is signed (it's in the body) and remembered for
        // longer than the signature window, so a replay inside the window is
        // refused by the store (`EMAILREQ#<id>`, attribute_not_exists).
        const { assert!(relay::REPLAY_TTL_SECS > 2 * sign::TOLERANCE_SECS) };
        assert!(relay::valid_request_id("01JABCDEFGHJKMNPQRSTVWXYZ0"));
    }

    #[test]
    fn secret_parameters_never_reach_a_record() {
        let t = templates::get("plugin_approval").unwrap();
        let p = obj(json!({
            "plugin": "p", "title": "t", "excerpt": "e", "version": 3,
            "approve_link": "https://acme.coderhelm.com/approve?t=TOKEN",
            "review_link": "https://acme.coderhelm.com/plugins/draft/?id=1"
        }));
        assert!(!serde_json::to_string(&templates::without_secrets(t, &p))
            .unwrap()
            .contains("TOKEN"));
        let d = super::delivery(
            "plugin_approval",
            "approver@acme.test",
            &Ok(super::Route::Relay),
        );
        let all = serde_json::to_string(&d).unwrap();
        assert!(!all.contains("TOKEN") && !all.contains("approver@"));
    }
}
