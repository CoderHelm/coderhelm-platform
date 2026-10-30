//! Email on a self-hosted install: what it sends and how it went
//! (Deliveries), and whether it can send at all (Setup and the install
//! guide's `/api/setup/status`).
//!
//! The facts ([`facts_now`]) are read once and judged by pure functions,
//! so every answer has a precise code and a plain fix:
//!
//! - **which sender**: CoderHelm's mail relay or the install's own SES
//!   (Settings, Emails);
//! - **the sender's identity**: verified, DKIM, SPF (SES's custom MAIL
//!   FROM) and DMARC (the domain's `_dmarc` record, looked up through the
//!   install's own resolver);
//! - **the SES sandbox**, which silently limits who gets mail;
//! - **Amazon Cognito's sender** for sign-up codes and password resets:
//!   Cognito's own address is capped at about 50 a day;
//! - **the last successful send**, from SES's own events
//!   (common::mail::stats).
//!
//! The install guide's check used to answer `warn unknown` whenever any
//! read failed: the identity lookup asked for the sender's domain even when
//! the identity is one address, an install sending through CoderHelm (no
//! SES sender at all) was judged on SES, and an unreadable account looked
//! the same as everything else. Each case has its own code now.

use std::sync::Arc;

use common::mail::{self, stats, Transport};
use serde_json::{json, Value};

use super::setup_checks::{sender_domain, Check, Fix, Status};
use crate::AppState;

const E: &str = "Email";

/// What SES says about one identity.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IdentityFacts {
    /// The identity found: the sender's domain, or the address itself.
    pub name: String,
    pub verified: bool,
    /// SES's DKIM status (`SUCCESS`, `PENDING`, `FAILED`, `NOT_STARTED`, ...).
    pub dkim: String,
    pub dkim_tokens: Vec<String>,
    /// The custom MAIL FROM domain's status (SPF), when one is set.
    pub mail_from: Option<String>,
}

/// The sender identity, as read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityRead {
    Found(IdentityFacts),
    /// Neither the domain nor the address is an identity in this region.
    Missing,
    /// SES refused or failed the read (the reason, for the detail line).
    Unreadable(String),
}

/// Everything the email checks judge.
#[derive(Debug, Clone, PartialEq)]
pub struct Facts {
    pub transport: Transport,
    /// Registered with CoderHelm (the relay can be asked).
    pub registered: bool,
    /// CoderHelm's relay status, when sending through it.
    pub relay: Option<Result<Value, String>>,
    pub from: String,
    pub region: String,
    pub identity: Option<IdentityRead>,
    /// SES production access; None when the account couldn't be read.
    pub production: Option<bool>,
    /// The `_dmarc` policy: None when not looked up or no answer, Some(None)
    /// when the domain has no DMARC record.
    pub dmarc: Option<Option<String>>,
    pub cognito: Option<stats::CognitoSender>,
    pub last_send_at: Option<String>,
    pub last_delivery_at: Option<String>,
}

impl Default for Facts {
    fn default() -> Self {
        Facts {
            transport: Transport::Ses,
            registered: false,
            relay: None,
            from: String::new(),
            region: "us-east-1".into(),
            identity: None,
            production: None,
            dmarc: None,
            cognito: None,
            last_send_at: None,
            last_delivery_at: None,
        }
    }
}

async fn read_identity(state: &AppState, name: &str) -> Result<Option<IdentityFacts>, String> {
    match state
        .ses
        .get_email_identity()
        .email_identity(name)
        .send()
        .await
    {
        Ok(o) => Ok(Some(IdentityFacts {
            name: name.to_string(),
            verified: o.verified_for_sending_status(),
            dkim: o
                .dkim_attributes()
                .and_then(|d| d.status())
                .map(|s| s.as_str().to_string())
                .unwrap_or_else(|| "NOT_STARTED".into()),
            dkim_tokens: o
                .dkim_attributes()
                .map(|d| d.tokens().to_vec())
                .unwrap_or_default(),
            mail_from: o
                .mail_from_attributes()
                .filter(|m| !m.mail_from_domain().is_empty())
                .map(|m| m.mail_from_domain_status().as_str().to_string()),
        })),
        Err(e) => {
            let text = aws_sdk_sesv2::error::DisplayErrorContext(&e).to_string();
            if text.contains("NotFoundException") {
                Ok(None)
            } else if text.contains("AccessDenied") || text.contains("not authorized") {
                Err("this service isn't allowed to read SES identities".into())
            } else {
                Err(text.chars().take(160).collect())
            }
        }
    }
}

/// Reads the facts: the sender in use and what its side says.
pub async fn facts_now(state: &AppState) -> Facts {
    let stored = mail::load(&state.dynamo, &state.config.settings_table_name)
        .await
        .ok()
        .flatten();
    let transport = mail::transport_of(stored.as_ref(), mail::transport_default());
    let creds = common::hybrid::client::credentials();
    let mut f = Facts {
        transport,
        registered: creds.is_some(),
        from: state.config.ses_from_address.clone(),
        region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into()),
        cognito: stats::cognito_sender(),
        ..Default::default()
    };
    let (own_last, relay) = tokio::join!(
        stats::read_last(
            &state.dynamo,
            &state.config.settings_table_name,
            stats::OWN_PK
        ),
        async {
            match (&creds, transport) {
                (Some(c), Transport::Coderhelm) => Some(
                    common::hybrid::client::call(
                        &state.http,
                        c,
                        "GET",
                        mail::relay::PATH_STATUS,
                        None,
                    )
                    .await
                    .map_err(|e| e.message),
                ),
                _ => None,
            }
        }
    );
    if let Ok(s) = own_last {
        f.last_send_at = s.last_send_at;
        f.last_delivery_at = s.last_delivery_at;
    }
    if transport == Transport::Coderhelm {
        if let Some(Ok(v)) = &relay {
            let later = |a: &mut Option<String>, b: Option<&str>| {
                if b > a.as_deref() {
                    *a = b.map(str::to_string);
                }
            };
            later(&mut f.last_send_at, v["stats"]["last_send_at"].as_str());
            later(
                &mut f.last_delivery_at,
                v["stats"]["last_delivery_at"].as_str(),
            );
        }
        f.relay = relay;
        return f;
    }
    if f.from.trim().is_empty() {
        return f;
    }
    let domain = sender_domain(&f.from).to_string();
    let dmarc_name = format!("_dmarc.{domain}");
    let (by_domain, acct, dmarc) = tokio::join!(
        read_identity(state, &domain),
        state.ses.get_account().send(),
        common::dns_txt::lookup(&dmarc_name),
    );
    f.identity = Some(match by_domain {
        Ok(Some(i)) => IdentityRead::Found(i),
        // The identity may be the address itself (`email.identity`).
        Ok(None) => match read_identity(state, f.from.trim()).await {
            Ok(Some(i)) => IdentityRead::Found(i),
            Ok(None) => IdentityRead::Missing,
            Err(e) => IdentityRead::Unreadable(e),
        },
        Err(e) => IdentityRead::Unreadable(e),
    });
    f.production = acct.ok().map(|a| a.production_access_enabled());
    f.dmarc = dmarc.ok().map(|r| common::dns_txt::dmarc_policy(&r));
    f
}

// ── The install guide's code (public: ids, statuses and codes only) ──────

/// The guide's email check: status and code (pure; tested). Never
/// `unknown`: each way it can't be sure has its own code.
pub fn public_code(f: &Facts) -> (&'static str, &'static str) {
    match f.transport {
        Transport::Coderhelm => match &f.relay {
            _ if !f.registered => ("warn", "relay_not_registered"),
            None | Some(Err(_)) => ("warn", "relay_unreachable"),
            Some(Ok(v)) if v["paused"].is_object() => ("warn", "relay_paused"),
            Some(Ok(v)) if v["verified"].as_bool() != Some(true) => ("warn", "relay_unverified"),
            Some(Ok(_)) => ("pass", "relay_ok"),
        },
        Transport::Ses => {
            if f.from.trim().is_empty() {
                return ("warn", "not_configured");
            }
            match &f.identity {
                None | Some(IdentityRead::Unreadable(_)) => ("warn", "identity_unreadable"),
                Some(IdentityRead::Missing) => ("warn", "identity_missing"),
                Some(IdentityRead::Found(i)) if !i.verified => ("warn", "identity_unverified"),
                Some(IdentityRead::Found(i)) if !i.dkim.eq_ignore_ascii_case("success") => {
                    ("warn", "dkim_pending")
                }
                Some(IdentityRead::Found(_)) => match f.production {
                    Some(true) => ("pass", "ok"),
                    Some(false) => ("warn", "sandbox"),
                    None => ("warn", "account_unreadable"),
                },
            }
        }
    }
}

// ── Settings, Setup (admins: full detail and fixes) ──────────────────────

fn console(region: &str, path: &str) -> String {
    format!("https://console.aws.amazon.com/ses/home?region={region}#/{path}")
}

fn last_send_line(f: &Facts) -> String {
    match (&f.last_delivery_at, &f.last_send_at) {
        (Some(d), _) => format!("Last delivered: {d}."),
        (None, Some(s)) => format!("Last sent: {s} (no delivery reported yet)."),
        _ => "Nothing sent yet.".into(),
    }
}

/// Which sender, and the last successful send.
pub fn sender_check(f: &Facts) -> Check {
    let last = last_send_line(f);
    match f.transport {
        Transport::Coderhelm => match &f.relay {
            _ if !f.registered => Check::not_ok(
                Status::Fail,
                "email_sender",
                E,
                "Email sender",
                "Email is set to go through CoderHelm, but this install isn't registered with CoderHelm.",
                "Nothing is sent another way meanwhile; Deliveries lists each email that failed.",
                Fix::text("Register the install in Settings, System, Hybrid, or switch Settings, Emails back to Amazon SES.").href("/settings/emails/"),
            ),
            None | Some(Err(_)) => Check::not_ok(
                Status::Warn,
                "email_sender",
                E,
                "Email sender",
                format!("Through CoderHelm, but its mail service didn't answer: {}", f.relay.as_ref().and_then(|r| r.as_ref().err()).cloned().unwrap_or_default()),
                "Emails fail until it answers again; nothing is sent another way.",
                Fix::text("Usually temporary: press Check again. CoderHelm's status page shows any outage."),
            ),
            Some(Ok(v)) if v["paused"].is_object() => Check::not_ok(
                Status::Fail,
                "email_sender",
                E,
                "Email sender",
                format!("Through CoderHelm, paused after too many {}.", v["paused"]["reason"].as_str().unwrap_or("bounces or complaints")),
                "No email leaves the install until CoderHelm support resumes it.",
                Fix::text("Fix the addresses that bounced (Deliveries lists them), then ask CoderHelm support to resume, or switch Settings, Emails to Amazon SES.").href("/settings/deliveries/"),
            ),
            Some(Ok(v)) if v["verified"].as_bool() != Some(true) => Check::not_ok(
                Status::Warn,
                "email_sender",
                E,
                "Email sender",
                "Through CoderHelm, but CoderHelm hasn't verified this install's address yet.",
                "CoderHelm sends an install's email only after it proves its address.",
                Fix::text("Finish verifying in Settings, System, Hybrid.").href("/settings/system/#hybrid"),
            ),
            Some(Ok(v)) => Check::pass(
                "email_sender",
                E,
                "Email sender",
                format!(
                    "Through CoderHelm, from {}: {} of {} today. {last}",
                    v["from"].as_str().unwrap_or("CoderHelm's mail service"),
                    v["sent_today"].as_u64().unwrap_or(0),
                    v["quota"].as_u64().unwrap_or(0),
                ),
            ),
        },
        Transport::Ses if f.from.trim().is_empty() => Check::not_ok(
            Status::Fail,
            "email_sender",
            E,
            "Email sender",
            "Email goes through this install's Amazon SES, but it has no sender address.",
            "No email can be sent.",
            Fix::text("Set email.from in coderhelm.config.json and deploy, or choose Through CoderHelm in Settings, Emails.").href("/settings/emails/"),
        ),
        Transport::Ses => Check::pass(
            "email_sender",
            E,
            "Email sender",
            format!("This install's Amazon SES, from {}. {last}", f.from),
        ),
    }
}

/// The sender's identity: verified, DKIM, SPF and DMARC.
pub fn identity_check(f: &Facts) -> Option<Check> {
    let label = "Email sender verified";
    if f.transport == Transport::Coderhelm {
        let v = f.relay.as_ref()?.as_ref().ok()?;
        let s = &v["sender"];
        if !s.is_object() {
            return None;
        }
        let ok = s["verified"].as_bool() == Some(true)
            && s["dkim"]
                .as_str()
                .is_some_and(|d| d.eq_ignore_ascii_case("success"));
        return Some(if ok {
            Check::pass(
                "email_identity",
                E,
                label,
                format!(
                    "CoderHelm's sending domain {} is verified, with DKIM, SPF ({}) and DMARC ({}).",
                    s["domain"].as_str().unwrap_or(""),
                    s["mail_from"].as_str().unwrap_or("set").to_ascii_lowercase(),
                    s["dmarc"].as_str().unwrap_or("reject"),
                ),
            )
        } else {
            Check::not_ok(
                Status::Warn,
                "email_identity",
                E,
                label,
                "CoderHelm's sending domain isn't fully verified right now.",
                "Mail may land in spam until it is.",
                Fix::text("Nothing to do on your side; CoderHelm is told."),
            )
        });
    }
    if f.from.trim().is_empty() {
        return None;
    }
    let domain = sender_domain(&f.from).to_string();
    let region = &f.region;
    Some(match f.identity.as_ref()? {
        IdentityRead::Unreadable(e) => Check::not_ok(
            Status::Warn,
            "email_identity",
            E,
            label,
            format!("Couldn't read the SES identity for {domain}: {e}."),
            "Whether email can be sent isn't known until it can be read.",
            Fix::text("Send a test email from this page. If the read keeps failing, deploy again so the service gets its SES read permission back.").href("/settings/setup#getting-started"),
        ),
        IdentityRead::Missing => Check::not_ok(
            Status::Fail,
            "email_identity",
            E,
            label,
            format!("Neither {domain} nor {} is an identity in Amazon SES in {region}.", f.from),
            "SES refuses to send from an address it hasn't verified.",
            Fix::text(format!("Create the identity for {domain} in SES, then add the three DKIM records it shows."))
                .command(format!("aws sesv2 create-email-identity --email-identity {domain} --region {region}"))
                .url(console(region, "identities")),
        ),
        IdentityRead::Found(i) => {
            let records = i
                .dkim_tokens
                .iter()
                .map(|t| format!("{t}._domainkey.{} CNAME {t}.dkim.amazonses.com", i.name))
                .collect::<Vec<_>>()
                .join("\n");
            let dkim_ok = i.dkim.eq_ignore_ascii_case("success");
            let spf = match i.mail_from.as_deref() {
                Some(s) if s.eq_ignore_ascii_case("success") => "SPF passes (custom MAIL FROM)".to_string(),
                Some(s) => format!("SPF: the custom MAIL FROM is {}", s.to_ascii_lowercase()),
                None => "SPF: SES's own MAIL FROM (amazonses.com)".to_string(),
            };
            let dmarc = match &f.dmarc {
                Some(Some(p)) => format!("DMARC p={p}"),
                Some(None) => "no DMARC record".to_string(),
                None => "DMARC couldn't be looked up".to_string(),
            };
            if !i.verified || !dkim_ok {
                return Some(Check::not_ok(
                    if i.verified { Status::Warn } else { Status::Fail },
                    "email_identity",
                    E,
                    label,
                    format!(
                        "{} {} (DKIM {}); {spf}; {dmarc}.",
                        i.name,
                        if i.verified { "is verified" } else { "isn't verified yet" },
                        i.dkim.to_lowercase().replace('_', " ")
                    ),
                    "Mail without DKIM lands in spam, and SES won't send at all from an unverified identity.",
                    Fix {
                        text: if records.is_empty() {
                            format!("Open {} in SES and add the DKIM records it shows at your DNS provider (an address identity confirms by the link SES emailed it).", i.name)
                        } else {
                            format!("Add these DNS records for {} at your DNS provider; SES checks them within an hour:", i.name)
                        },
                        command: (!records.is_empty()).then_some(records),
                        url: Some(console(region, &format!("identities/{}", i.name))),
                        href: None,
                    },
                ));
            }
            match &f.dmarc {
                Some(None) => Check::not_ok(
                    Status::Warn,
                    "email_identity",
                    E,
                    label,
                    format!("{} is verified with DKIM; {spf}; but {domain} has no DMARC record.", i.name),
                    "Some providers send mail without DMARC to spam, or refuse it.",
                    Fix::text(format!("Add a TXT record at _dmarc.{domain}:")).command("v=DMARC1; p=none;"),
                ),
                _ => Check::pass(
                    "email_identity",
                    E,
                    label,
                    format!("{} is verified with DKIM; {spf}; {dmarc}.", i.name),
                ),
            }
        }
    })
}

/// The SES sandbox (own SES only).
pub fn sandbox_check(f: &Facts) -> Option<Check> {
    if f.transport == Transport::Coderhelm {
        return None;
    }
    let label = "Email can reach anyone";
    let region = &f.region;
    Some(match f.production {
        Some(true) => Check::pass("email_sandbox", E, label, "SES production access is on."),
        Some(false) => Check::not_ok(
            Status::Warn,
            "email_sandbox",
            E,
            label,
            "This AWS account is in the SES sandbox: email only reaches verified addresses, 200 a day. Mail to anyone else is dropped without an error here.",
            "Invites and run results won't reach your teammates until production access is granted.",
            Fix::text("Request production access (Account dashboard, Request production access); AWS usually answers within a day.")
                .url(console(region, "account")),
        ),
        None => Check::not_ok(
            Status::Warn,
            "email_sandbox",
            E,
            label,
            "Couldn't read the SES account, so whether it is in the sandbox isn't known.",
            "In the sandbox, email only reaches verified addresses.",
            Fix::text("Open the SES account dashboard to see the mode, or send a test email from this page to an address SES hasn't verified.")
                .url(console(region, "account")),
        ),
    })
}

/// Who sends Amazon Cognito's sign-up codes and password resets.
pub fn cognito_check(f: &Facts) -> Check {
    let label = "Sign-up and password emails";
    match f.cognito {
        Some(stats::CognitoSender::Ses) => Check::pass(
            "email_cognito",
            E,
            label,
            format!("Amazon Cognito sends sign-up codes and password resets through this install's SES, from {}.", f.from),
        ),
        Some(stats::CognitoSender::CognitoDefault) => Check::not_ok(
            Status::Warn,
            "email_cognito",
            E,
            label,
            format!("Amazon Cognito sends sign-up codes and password resets from its own address, limited to about {} a day.", stats::COGNITO_DEFAULT_DAILY),
            "Past the limit, people can't verify their address or reset a password until the next day.",
            Fix::text("Once the SES identity shows Verified, set email.cognitoUsesSes to true in coderhelm.config.json and deploy."),
        ),
        None => Check::not_ok(
            Status::Skip,
            "email_cognito",
            E,
            label,
            "This release's deploy didn't say who sends Cognito's email.",
            "Deploy again to see it here.",
            Fix::text("Update CoderHelm (Settings, System, Updates)."),
        ),
    }
}

/// Every email check for Settings, Setup, in order.
pub fn checks(f: &Facts) -> Vec<Check> {
    let mut out = vec![sender_check(f)];
    out.extend(identity_check(f));
    out.extend(sandbox_check(f));
    out.push(cognito_check(f));
    out
}

// ── Deliveries ───────────────────────────────────────────────────────────

/// One kind of email's counts in the overview.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
pub struct EmailKindRow {
    /// `sign_in`, `password_reset`, `notices`, `weekly_summary`, `alerts`,
    /// `other`, or `all` for the totals.
    #[schema(example = "notices")]
    pub kind: String,
    /// The kind in words.
    #[schema(example = "Run and review notices")]
    pub label: String,
    /// Accepted by SES for sending (per recipient).
    #[schema(example = 42)]
    pub sends: u64,
    /// Accepted by the receiving mail server.
    #[schema(example = 41)]
    pub deliveries: u64,
    /// Returned by the receiving server (the address doesn't exist, or a full mailbox).
    #[schema(example = 1)]
    pub bounces: u64,
    /// Marked as spam by the recipient.
    #[schema(example = 0)]
    pub complaints: u64,
    /// Refused by SES before sending.
    #[schema(example = 0)]
    pub rejections: u64,
}

impl From<&stats::KindCounts> for EmailKindRow {
    fn from(k: &stats::KindCounts) -> Self {
        EmailKindRow {
            kind: k.kind.clone(),
            label: k.label.clone(),
            sends: k.sends,
            deliveries: k.deliveries,
            bounces: k.bounces,
            complaints: k.complaints,
            rejections: k.rejections,
        }
    }
}

/// Email in the Deliveries overview (self-hosted installs).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
pub struct EmailOverview {
    /// The sender in use: `relay` (through CoderHelm) or `ses` (the install's own).
    #[schema(example = "relay")]
    pub sender: String,
    /// The first UTC day counted.
    #[schema(example = "2026-10-24")]
    pub from: String,
    /// The last UTC day counted (today).
    #[schema(example = "2026-10-30")]
    pub to: String,
    /// Every kind, in a fixed order, zeros included.
    pub kinds: Vec<EmailKindRow>,
    /// Every kind added up.
    pub totals: EmailKindRow,
    /// When an email was last accepted for sending (any period).
    #[schema(example = "2026-10-29T14:03:11Z")]
    pub last_send_at: Option<String>,
    /// When an email was last delivered (any period).
    #[schema(example = "2026-10-29T14:03:13Z")]
    pub last_delivery_at: Option<String>,
    /// When an email last bounced (any period).
    #[schema(example = "2026-10-21T09:12:40Z")]
    pub last_bounce_at: Option<String>,
    /// When a recipient last marked an email as spam (any period).
    #[schema(example = "2026-10-02T17:30:00Z")]
    pub last_complaint_at: Option<String>,
    /// Who sends Amazon Cognito's sign-up codes and password resets:
    /// `cognito_default` (Cognito's own address) or `ses`. Null when the
    /// deploy didn't say.
    #[schema(example = "cognito_default")]
    pub cognito_sender: Option<String>,
    /// Cognito's own sender's daily limit, when it is the sender.
    #[schema(example = 50)]
    pub cognito_daily_limit: Option<u64>,
}

/// The overview's email part from the counts (pure; tested).
pub fn overview_of(
    transport: Transport,
    from: &str,
    to: &str,
    s: &stats::Stats,
    cognito: Option<stats::CognitoSender>,
) -> EmailOverview {
    EmailOverview {
        sender: if transport == Transport::Coderhelm {
            "relay"
        } else {
            "ses"
        }
        .into(),
        from: from.into(),
        to: to.into(),
        kinds: s.kinds.iter().map(EmailKindRow::from).collect(),
        totals: EmailKindRow::from(&s.totals),
        last_send_at: s.last_send_at.clone(),
        last_delivery_at: s.last_delivery_at.clone(),
        last_bounce_at: s.last_bounce_at.clone(),
        last_complaint_at: s.last_complaint_at.clone(),
        cognito_sender: cognito.map(|c| match c {
            stats::CognitoSender::Ses => "ses".to_string(),
            stats::CognitoSender::CognitoDefault => "cognito_default".to_string(),
        }),
        cognito_daily_limit: (cognito == Some(stats::CognitoSender::CognitoDefault))
            .then_some(stats::COGNITO_DEFAULT_DAILY),
    }
}

/// The email part of the Deliveries overview: counts by kind over the last
/// `days` UTC days from the install's own SES and from CoderHelm's relay
/// (an install that switched senders sees both, added up).
pub async fn overview(state: &Arc<AppState>, days: i64) -> EmailOverview {
    let (from, to) = stats::day_range(days, chrono::Utc::now());
    let stored = mail::load(&state.dynamo, &state.config.settings_table_name)
        .await
        .ok()
        .flatten();
    let transport = mail::transport_of(stored.as_ref(), mail::transport_default());
    let own = stats::read(
        &state.dynamo,
        &state.config.settings_table_name,
        stats::OWN_PK,
        &from,
        &to,
    )
    .await
    .ok();
    let relay = match common::hybrid::client::credentials() {
        Some(c) => common::hybrid::client::call(
            &state.http,
            &c,
            "POST",
            mail::relay::PATH_STATS,
            Some(&json!({ "from": from, "to": to })),
        )
        .await
        .ok()
        .and_then(|v| stats::from_json(&v)),
        None => None,
    };
    let mut all = own.unwrap_or_else(|| stats::from_items(&[], None, "", ""));
    if let Some(r) = &relay {
        all.merge(r);
    }
    overview_of(transport, &from, &to, &all, stats::cognito_sender())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ses(identity: Option<IdentityRead>, production: Option<bool>) -> Facts {
        Facts {
            from: "helm@example.com".into(),
            region: "eu-west-1".into(),
            identity,
            production,
            dmarc: Some(Some("reject".into())),
            ..Default::default()
        }
    }

    fn found(verified: bool, dkim: &str) -> IdentityRead {
        IdentityRead::Found(IdentityFacts {
            name: "example.com".into(),
            verified,
            dkim: dkim.into(),
            dkim_tokens: vec!["tok1".into()],
            mail_from: Some("SUCCESS".into()),
        })
    }

    fn relay(v: Value) -> Facts {
        Facts {
            transport: Transport::Coderhelm,
            registered: true,
            relay: Some(Ok(v)),
            ..Default::default()
        }
    }

    #[test]
    fn every_public_code_is_precise() {
        let cases: Vec<(Facts, (&str, &str))> = vec![
            (
                ses(Some(found(true, "SUCCESS")), Some(true)),
                ("pass", "ok"),
            ),
            (
                ses(Some(found(true, "SUCCESS")), Some(false)),
                ("warn", "sandbox"),
            ),
            (
                ses(Some(found(true, "SUCCESS")), None),
                ("warn", "account_unreadable"),
            ),
            (
                ses(Some(found(true, "PENDING")), Some(true)),
                ("warn", "dkim_pending"),
            ),
            (
                ses(Some(found(false, "PENDING")), Some(true)),
                ("warn", "identity_unverified"),
            ),
            (
                ses(Some(IdentityRead::Missing), Some(true)),
                ("warn", "identity_missing"),
            ),
            (
                ses(Some(IdentityRead::Unreadable("x".into())), Some(true)),
                ("warn", "identity_unreadable"),
            ),
            (ses(None, Some(true)), ("warn", "identity_unreadable")),
            (Facts::default(), ("warn", "not_configured")),
            (relay(json!({ "verified": true })), ("pass", "relay_ok")),
            (
                relay(json!({ "verified": false })),
                ("warn", "relay_unverified"),
            ),
            (
                relay(json!({ "verified": true, "paused": { "reason": "bounces" } })),
                ("warn", "relay_paused"),
            ),
            (
                Facts {
                    relay: Some(Err("down".into())),
                    ..relay(json!({}))
                },
                ("warn", "relay_unreachable"),
            ),
            (
                Facts {
                    registered: false,
                    relay: None,
                    ..relay(json!({}))
                },
                ("warn", "relay_not_registered"),
            ),
        ];
        for (f, want) in cases {
            let got = public_code(&f);
            assert_eq!(got, want);
            assert_ne!(got.1, "unknown");
        }
    }

    #[test]
    fn setup_shows_the_sender_identity_sandbox_cognito_and_last_send() {
        let mut f = ses(Some(found(true, "SUCCESS")), Some(true));
        f.cognito = Some(stats::CognitoSender::CognitoDefault);
        f.last_delivery_at = Some("2026-10-29T10:00:00Z".into());
        let c = checks(&f);
        let keys: Vec<&str> = c.iter().map(|c| c.key).collect();
        assert_eq!(
            keys,
            [
                "email_sender",
                "email_identity",
                "email_sandbox",
                "email_cognito"
            ]
        );
        assert_eq!(c[0].status, Status::Pass);
        assert!(c[0].detail.contains("2026-10-29T10:00:00Z"));
        assert!(c[1].detail.contains("DMARC p=reject"));
        assert!(c[1].detail.contains("SPF passes"));
        assert_eq!(c[3].status, Status::Warn);
        assert!(c[3].detail.contains("50"));
        assert!(c[3].fix.as_ref().unwrap().text.contains("cognitoUsesSes"));
        // Every problem has a fix line.
        for f in [
            ses(Some(IdentityRead::Missing), Some(false)),
            ses(Some(found(false, "PENDING")), None),
            Facts::default(),
            relay(json!({ "verified": true, "paused": { "reason": "complaints" } })),
        ] {
            for c in checks(&f) {
                if c.status != Status::Pass {
                    assert!(
                        c.fix.as_ref().is_some_and(|x| !x.text.is_empty()),
                        "{}",
                        c.key
                    );
                }
            }
        }
        // No DMARC record is a warning with the record to add.
        let mut f = ses(Some(found(true, "SUCCESS")), Some(true));
        f.dmarc = Some(None);
        let id = identity_check(&f).unwrap();
        assert_eq!(id.status, Status::Warn);
        assert!(id.fix.unwrap().command.unwrap().contains("v=DMARC1"));
        // Sending through CoderHelm: no SES sandbox check, the relay's sender.
        let f = relay(
            json!({ "verified": true, "from": "notifications@notify.example.com", "sent_today": 3, "quota": 500,
            "sender": { "domain": "notify.example.com", "verified": true, "dkim": "SUCCESS", "mail_from": "SUCCESS", "dmarc": "reject" } }),
        );
        let c = checks(&f);
        assert!(c.iter().all(|c| c.key != "email_sandbox"));
        assert!(c[0].detail.contains("3 of 500"));
        assert_eq!(c[1].status, Status::Pass);
    }

    #[test]
    fn the_overview_counts_by_kind_and_names_cognitos_sender() {
        let mut s = stats::from_items(&[], None, "", "");
        s.kinds[0].sends = 3;
        s.totals.sends = 3;
        s.last_delivery_at = Some("2026-10-29T10:00:00Z".into());
        let o = overview_of(
            Transport::Coderhelm,
            "2026-10-24",
            "2026-10-30",
            &s,
            Some(stats::CognitoSender::CognitoDefault),
        );
        assert_eq!(o.sender, "relay");
        assert_eq!(o.kinds.len(), stats::KINDS.len());
        assert_eq!((o.kinds[0].kind.as_str(), o.kinds[0].sends), ("sign_in", 3));
        assert_eq!(o.totals.sends, 3);
        assert_eq!(o.cognito_daily_limit, Some(50));
        let o = overview_of(
            Transport::Ses,
            "a",
            "b",
            &s,
            Some(stats::CognitoSender::Ses),
        );
        assert_eq!(
            (
                o.sender.as_str(),
                o.cognito_sender.as_deref(),
                o.cognito_daily_limit
            ),
            ("ses", Some("ses"), None)
        );
    }
}
