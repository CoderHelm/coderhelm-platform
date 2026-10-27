//! The emails CoderHelm's mail relay may carry, and how it writes them.
//!
//! A self-hosted install that sends its email through CoderHelm names a
//! template and fills in its parameters; it never sends a subject, HTML or
//! free text of its own. Every subject is fixed here. Every parameter has a
//! type and a length limit, is plain text, and is escaped when written into
//! the email. Text parameters lose anything that looks like a link or an
//! address, and a link parameter must point at the install's own verified
//! address. Parameters marked `secret` (signed approval and unsubscribe
//! links) are never logged or stored.
//!
//! The same definitions check a request on the install (before it is sent)
//! and on the hosted side (before anything is written), so the two can't
//! drift.

use serde_json::{Map, Value};
use std::collections::BTreeMap;

/// What a parameter may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// One line of plain text, at most this many characters.
    Line(usize),
    /// Plain text that may span lines, at most this many characters.
    Text(usize),
    /// A whole number.
    Number,
    /// A release name such as `v1.4.0` or `1.4.0-rc.1`.
    Version,
    /// One of these words.
    OneOf(&'static [&'static str]),
    /// An `https://` link to the install's own verified address.
    Link,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Param {
    pub name: &'static str,
    pub kind: Kind,
    /// Kept out of every log, record and preview (signed links).
    pub secret: bool,
    pub required: bool,
}

const fn p(name: &'static str, kind: Kind) -> Param {
    Param {
        name,
        kind,
        secret: false,
        required: true,
    }
}

const fn secret(name: &'static str, kind: Kind) -> Param {
    Param {
        name,
        kind,
        secret: true,
        required: true,
    }
}

const fn optional(name: &'static str, kind: Kind) -> Param {
    Param {
        name,
        kind,
        secret: false,
        required: false,
    }
}

/// One piece of an email body.
#[derive(Debug, Clone, Copy)]
pub enum Part {
    /// A paragraph; `{name}` is replaced by that parameter, escaped.
    Para(&'static str),
    /// A parameter's text set apart as a quote.
    Quote(&'static str),
    /// A button: label and the link parameter it opens.
    Button(&'static str, &'static str),
}

#[derive(Debug, Clone, Copy)]
pub struct Template {
    pub id: &'static str,
    /// The whole subject. Never filled from a parameter, and for what an
    /// install may send, never naming CoderHelm: the mail comes from someone
    /// else's install, so its subject must not read as CoderHelm's own.
    pub subject: &'static str,
    pub params: &'static [Param],
    pub body: &'static [Part],
    /// Whether an install may ask for it (false: the relay's own notices).
    pub install_may_send: bool,
}

pub const UPDATE_WHAT: &[&str] = &[
    "available",
    "succeeded",
    "failed",
    "rolled_back",
    "blocked",
    "domain_live",
    "domain_failed",
    "domain_removed",
];

pub const TEMPLATES: &[Template] = &[
    Template {
        id: "test_email",
        subject: "Test email from your install",
        params: &[],
        body: &[Part::Para(
            "Email from your CoderHelm install works. Run results, invites and update notices will arrive like this one.",
        )],
        install_may_send: true,
    },
    Template {
        id: "team_invite",
        subject: "You're invited to your team's workspace",
        params: &[
            p("team", Kind::Line(80)),
            p("inviter", Kind::Line(80)),
            p("role", Kind::OneOf(&["viewer", "member", "billing", "admin", "owner"])),
            p("link", Kind::Link),
        ],
        body: &[
            Part::Para("{inviter} invited you to the {team} workspace as {role}."),
            Part::Button("Accept the invite", "link"),
        ],
        install_may_send: true,
    },
    Template {
        id: "welcome",
        subject: "Your account is ready",
        params: &[p("link", Kind::Link)],
        body: &[
            Part::Para("Your account is ready. Connect a repository and CoderHelm starts on your next ticket."),
            Part::Button("Open CoderHelm", "link"),
        ],
        install_may_send: true,
    },
    Template {
        id: "plugin_approval",
        subject: "A plugin draft is waiting for your approval",
        params: &[
            p("plugin", Kind::Line(80)),
            p("title", Kind::Line(200)),
            p("excerpt", Kind::Text(1200)),
            p("version", Kind::Number),
            secret("approve_link", Kind::Link),
            p("review_link", Kind::Link),
        ],
        body: &[
            Part::Para("A draft from the {plugin} plugin is waiting for your approval: {title}"),
            Part::Quote("excerpt"),
            Part::Para("Approve or reject this exact version (version {version}). The link works for three days and only for you."),
            Part::Button("Approve or reject", "approve_link"),
            Part::Button("Review it in CoderHelm", "review_link"),
        ],
        install_may_send: true,
    },
    Template {
        id: "knowledge_signin",
        subject: "A knowledge source needs signing in again",
        params: &[
            p("source", Kind::Line(80)),
            p("problem", Kind::Text(300)),
            p("link", Kind::Link),
        ],
        body: &[
            Part::Para("CoderHelm can't read the {source} knowledge source any more."),
            Part::Quote("problem"),
            Part::Button("Sign in again", "link"),
        ],
        install_may_send: true,
    },
    Template {
        id: "token_warning",
        subject: "Your team is near its monthly token limit",
        params: &[
            p("limit_name", Kind::Line(40)),
            p("month", Kind::Line(20)),
            p("used", Kind::Line(20)),
            p("limit", Kind::Line(20)),
            p("pct", Kind::Number),
            p("link", Kind::Link),
        ],
        body: &[
            Part::Para("Your team has used {pct}% of its monthly {limit_name} limit for {month}: {used} of {limit}."),
            Part::Button("See usage", "link"),
        ],
        install_may_send: true,
    },
    Template {
        id: "token_limit_reached",
        subject: "Your team reached its monthly token limit",
        params: &[
            p("limit_name", Kind::Line(40)),
            p("month", Kind::Line(20)),
            p("used", Kind::Line(20)),
            p("limit", Kind::Line(20)),
            p("link", Kind::Link),
        ],
        body: &[
            Part::Para("Your team reached its monthly {limit_name} limit for {month}: {used} of {limit}. New work waits until an admin raises the limit or the month ends."),
            Part::Button("See usage", "link"),
        ],
        install_may_send: true,
    },
    Template {
        id: "support_update",
        subject: "Update on a support ticket",
        params: &[
            p("ticket", Kind::Line(200)),
            p("note", Kind::Text(2000)),
            p("link", Kind::Link),
        ],
        body: &[
            Part::Para("There is news on the ticket \"{ticket}\"."),
            Part::Quote("note"),
            Part::Button("Open the ticket", "link"),
        ],
        install_may_send: true,
    },
    Template {
        id: "update_notice",
        subject: "Your install has an update notice",
        params: &[
            p("what", Kind::OneOf(UPDATE_WHAT)),
            optional("version", Kind::Version),
            p("link", Kind::Link),
        ],
        body: &[
            Part::Para("Update notice: {what} {version}."),
            Part::Button("Open Settings, System", "link"),
        ],
        install_may_send: true,
    },
    Template {
        id: "weekly_summary",
        subject: "Your weekly summary",
        params: &[
            p("team", Kind::Line(80)),
            p("period", Kind::Line(40)),
            p("merged", Kind::Number),
            p("runs", Kind::Number),
            p("reviews", Kind::Number),
            p("waiting", Kind::Number),
            optional("highlights", Kind::Text(1500)),
            p("link", Kind::Link),
            secret("unsubscribe", Kind::Link),
        ],
        body: &[
            Part::Para("{team}, {period}: {merged} pull requests merged, {runs} runs finished, {reviews} reviews written, {waiting} waiting on someone."),
            Part::Quote("highlights"),
            Part::Button("Open CoderHelm", "link"),
            Part::Button("Stop these emails", "unsubscribe"),
        ],
        install_may_send: true,
    },
    Template {
        id: "webhook_disabled",
        subject: "A webhook endpoint was turned off",
        // No endpoint URL or host: the address can carry a secret, and
        // the relay never needs it.
        params: &[p("days", Kind::Number), p("link", Kind::Link)],
        body: &[
            Part::Para("Every delivery to one of your team's webhook endpoints failed for {days} days, so CoderHelm stopped sending to it. Fix it, turn it back on in Settings, Webhooks, and replay what it missed."),
            Part::Button("Open Webhooks", "link"),
        ],
        install_may_send: true,
    },
    Template {
        id: "two_factor_lockout",
        subject: "Two-factor sign-in paused after wrong codes",
        // No name or address: the install's own SES email carries those.
        params: &[p("minutes", Kind::Number), p("link", Kind::Link)],
        body: &[
            Part::Para("Two-factor sign-in was paused for {minutes} minutes for someone in your organization after too many wrong codes. If they lost their phone, check it's really them, then reset their two-factor sign-in in Settings, Sign-in. If it wasn't them, someone may know their password: ask them to change it."),
            Part::Button("Open Settings", "link"),
        ],
        install_may_send: true,
    },
    Template {
        id: "two_factor_required",
        subject: "Two-factor sign-in is now required",
        // No team or admin name: the install's own SES email carries those.
        params: &[p("link", Kind::Link)],
        body: &[
            Part::Para("Two-factor sign-in is now required for you. The next time you sign in, you'll set up an authenticator app on your phone first (it takes about a minute); nothing else works until then."),
            Part::Button("Set it up now", "link"),
        ],
        install_may_send: true,
    },
    Template {
        id: "two_factor_reset",
        subject: "Your two-factor sign-in was reset",
        params: &[p("link", Kind::Link)],
        body: &[
            Part::Para("An admin reset your two-factor sign-in. Set it up again at your next sign-in. If you didn't ask for this, tell your admins and change your password now."),
            Part::Button("Open Security settings", "link"),
        ],
        install_may_send: true,
    },
    // The relay's own notice to an install's admins. Installs can't ask for it.
    Template {
        id: "relay_paused",
        subject: "CoderHelm paused email for your install",
        params: &[p("reason", Kind::OneOf(&["bounces", "complaints"]))],
        body: &[Part::Para(
            "CoderHelm stopped sending email for your self-hosted install because too many recent emails {reason}. Check the recipients in Settings, Emails, then ask CoderHelm to resume, or switch the install to its own Amazon SES.",
        )],
        install_may_send: false,
    },
];

pub fn get(id: &str) -> Option<&'static Template> {
    TEMPLATES.iter().find(|t| t.id == id)
}

/// Why a request was refused. `code()` is what the install's Deliveries
/// page explains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    UnknownTemplate,
    NotForInstalls,
    UnknownParam(String),
    MissingParam(String),
    BadParam(String),
    TooLong(String),
    LinkNotOwnHost(String),
    Reserved(String),
}

impl Refusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::UnknownTemplate => "unknown_template",
            Self::NotForInstalls => "template_not_allowed",
            Self::UnknownParam(_) => "unknown_param",
            Self::MissingParam(_) => "missing_param",
            Self::BadParam(_) => "bad_param",
            Self::TooLong(_) => "param_too_long",
            Self::LinkNotOwnHost(_) => "link_not_own_host",
            Self::Reserved(_) => "param_reserved",
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::UnknownTemplate => "No such email template.".into(),
            Self::NotForInstalls => "Installs can't send this template.".into(),
            Self::UnknownParam(n) => format!("The template has no parameter {n}."),
            Self::MissingParam(n) => format!("The parameter {n} is required."),
            Self::BadParam(n) => format!("The parameter {n} has the wrong type."),
            Self::TooLong(n) => format!("The parameter {n} is too long."),
            Self::LinkNotOwnHost(n) => {
                format!("The link {n} must point at this install's own verified address.")
            }
            Self::Reserved(n) => {
                format!("The parameter {n} uses wording reserved for CoderHelm itself.")
            }
        }
    }
}

/// Checked parameter values, ready to render.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Values(pub BTreeMap<String, String>);

impl Values {
    pub fn get(&self, k: &str) -> &str {
        self.0.get(k).map(String::as_str).unwrap_or("")
    }
}

/// The parameters with every secret one taken out (for logs and previews).
pub fn without_secrets(t: &Template, params: &Map<String, Value>) -> Map<String, Value> {
    params
        .iter()
        .filter(|(k, _)| t.params.iter().any(|p| p.name == k.as_str() && !p.secret))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Wording a parameter may not use: it would read as CoderHelm itself
/// speaking (support, security, billing), not as the install.
const RESERVED_PHRASES: &[&str] = &[
    "coderhelm support",
    "coderhelm security",
    "coderhelm team",
    "coderhelm staff",
    "coderhelm billing",
    "coderhelm trust",
    "coderhelm admin",
    "from coderhelm",
    "coderhelm account",
];

fn squash(s: &str) -> String {
    let mut out = String::new();
    let mut last_space = true;
    for c in s.chars() {
        let c = c.to_ascii_lowercase();
        if c.is_ascii_alphanumeric() {
            out.push(c);
            last_space = false;
        } else if !last_space {
            out.push(' ');
            last_space = true;
        }
    }
    out.trim().to_string()
}

pub fn uses_reserved_wording(s: &str) -> bool {
    let sq = squash(s);
    // "Coder Helm" and "Coder-Helm" read the same.
    let joined = sq.replace("coder helm", "coderhelm");
    RESERVED_PHRASES.iter().any(|p| joined.contains(p))
}

/// Top-level domains that turn a word into a link in mail apps. File
/// extensions that are also country domains (`.md`, `.rs`, `.py`, `.sh`) are
/// left out so PR titles keep their file names.
const TLDS: &[&str] = &[
    "com", "net", "org", "io", "co", "ai", "app", "dev", "xyz", "info", "biz", "me", "us", "uk",
    "ru", "cn", "link", "click", "top", "site", "online", "live", "support", "help", "security",
    "page", "cc", "tk", "ly", "gl", "gg", "de", "fr", "nl", "eu", "in", "jp", "br", "au", "ca",
    "tv", "ws", "su", "ml", "ga", "cf", "gq", "icu", "buzz", "shop", "store", "email", "mail",
    "login", "account", "cloud", "tech", "zip", "mov", "work", "today", "run", "website", "space",
    "fun", "pw", "ink", "bank", "pro", "es", "it", "ch", "se", "no", "fi", "pl", "kr", "tw", "hk",
    "sg", "nz", "za", "mx", "ar", "cl", "id", "vn", "th", "my", "ph", "tr", "ir", "ua", "ro",
];

const LINK_REMOVED: &str = "[link removed]";

fn looks_like_link(token: &str) -> bool {
    let t = token.trim_matches(|c: char| {
        !c.is_alphanumeric() && c != '/' && c != ':' && c != '@' && c != '.' && c != '-'
    });
    if t.is_empty() {
        return false;
    }
    let lower = t.to_lowercase();
    if lower.contains("://") || lower.starts_with("www.") {
        return true;
    }
    for scheme in [
        "javascript:",
        "data:",
        "mailto:",
        "tel:",
        "vbscript:",
        "file:",
        "sms:",
    ] {
        if lower.contains(scheme) {
            return true;
        }
    }
    // An address (`someone@host.tld`).
    if let Some((_, host)) = lower.split_once('@') {
        if host.contains('.') {
            return true;
        }
    }
    let host = lower
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .trim_end_matches('.');
    if !host.contains('.') {
        return false;
    }
    // A host written with letters from another alphabet.
    if !host.is_ascii() {
        return true;
    }
    // An IP address.
    if host.split('.').count() == 4 && host.split('.').all(|o| o.parse::<u8>().is_ok()) {
        return true;
    }
    let tld = host.rsplit('.').next().unwrap_or("");
    let labels_ok = host
        .split('.')
        .all(|l| !l.is_empty() && l.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'));
    labels_ok && TLDS.contains(&tld)
}

/// Plain text with every link-looking word taken out, and no control
/// characters. `one_line` also turns line breaks into spaces.
pub fn plain(s: &str, one_line: bool) -> String {
    let mut lines = Vec::new();
    for line in s.lines() {
        let words: Vec<String> = line
            .split_whitespace()
            .map(|w| {
                let w: String = w.chars().filter(|c| !c.is_control()).collect();
                // Bidirectional overrides and zero-width characters hide
                // what a word says.
                let w: String = w
                    .chars()
                    .filter(|c| {
                        !matches!(
                            *c,
                            '\u{200B}'..='\u{200F}' | '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}' | '\u{FEFF}'
                        )
                    })
                    .collect();
                if looks_like_link(&w) {
                    LINK_REMOVED.to_string()
                } else {
                    w
                }
            })
            .collect();
        lines.push(words.join(" "));
    }
    if one_line {
        lines.join(" ").trim().to_string()
    } else {
        lines.join("\n").trim().to_string()
    }
}

/// Whether `link` is an `https://` link to one of `own_hosts`, with no
/// user, password or port.
pub fn link_on_own_host(link: &str, own_hosts: &[String]) -> bool {
    let Ok(u) = url::Url::parse(link) else {
        return false;
    };
    if u.scheme() != "https" || !u.username().is_empty() || u.password().is_some() {
        return false;
    }
    if u.port().is_some() {
        return false;
    }
    let Some(host) = u.host_str() else {
        return false;
    };
    let host = host.to_ascii_lowercase();
    own_hosts
        .iter()
        .any(|h| !h.is_empty() && h.eq_ignore_ascii_case(&host))
}

fn as_text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Checks a request's parameters against its template. `own_hosts` are
/// the install's verified addresses (its CloudFront host and its name).
pub fn check(
    template_id: &str,
    params: &Map<String, Value>,
    own_hosts: &[String],
    from_install: bool,
) -> Result<(&'static Template, Values), Refusal> {
    let t = get(template_id).ok_or(Refusal::UnknownTemplate)?;
    if from_install && !t.install_may_send {
        return Err(Refusal::NotForInstalls);
    }
    if let Some(k) = params
        .keys()
        .find(|k| !t.params.iter().any(|p| p.name == k.as_str()))
    {
        return Err(Refusal::UnknownParam(k.clone()));
    }
    let mut out = BTreeMap::new();
    for spec in t.params {
        let Some(raw) = params.get(spec.name).filter(|v| !v.is_null()) else {
            if spec.required {
                return Err(Refusal::MissingParam(spec.name.into()));
            }
            continue;
        };
        let bad = || Refusal::BadParam(spec.name.into());
        let value = match spec.kind {
            Kind::Line(max) | Kind::Text(max) => {
                let s = raw.as_str().ok_or_else(bad)?;
                if s.chars().count() > max {
                    return Err(Refusal::TooLong(spec.name.into()));
                }
                let clean = plain(s, matches!(spec.kind, Kind::Line(_)));
                if uses_reserved_wording(&clean) {
                    return Err(Refusal::Reserved(spec.name.into()));
                }
                clean
            }
            Kind::Number => {
                let s = as_text(raw).ok_or_else(bad)?;
                if s.is_empty() || s.len() > 12 || !s.chars().all(|c| c.is_ascii_digit()) {
                    return Err(bad());
                }
                s
            }
            Kind::Version => {
                let s = raw.as_str().ok_or_else(bad)?;
                let ok = !s.is_empty()
                    && s.len() <= 40
                    && s.chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '+')
                    && s.chars().any(|c| c.is_ascii_digit())
                    && !looks_like_link(s);
                if !ok {
                    return Err(bad());
                }
                s.to_string()
            }
            Kind::OneOf(words) => {
                let s = raw.as_str().ok_or_else(bad)?;
                if !words.contains(&s) {
                    return Err(bad());
                }
                s.to_string()
            }
            Kind::Link => {
                let s = raw.as_str().ok_or_else(bad)?;
                if s.len() > 2048 {
                    return Err(Refusal::TooLong(spec.name.into()));
                }
                if !link_on_own_host(s, own_hosts) {
                    return Err(Refusal::LinkNotOwnHost(spec.name.into()));
                }
                s.to_string()
            }
        };
        out.insert(spec.name.to_string(), value);
    }
    Ok((t, Values(out)))
}

/// Cuts text to at most `max` characters (the install side, before it asks).
pub fn fit(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

/// A written email.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rendered {
    pub subject: String,
    pub text: String,
    pub html: String,
}

pub fn escape(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

fn fill(pattern: &str, v: &Values, html: bool) -> String {
    let mut out = String::new();
    let mut rest = pattern;
    while let Some(start) = rest.find('{') {
        out.push_str(&if html {
            escape(&rest[..start])
        } else {
            rest[..start].to_string()
        });
        let after = &rest[start + 1..];
        let Some(end) = after.find('}') else {
            out.push_str(&rest[start..]);
            return out;
        };
        let name = &after[..end];
        let value = v.get(name);
        out.push_str(&if html {
            escape(value)
        } else {
            value.to_string()
        });
        rest = &after[end + 1..];
    }
    out.push_str(&if html { escape(rest) } else { rest.to_string() });
    out
}

/// Writes the email. `install_host` is the install's verified address,
/// named in the footer so a reader knows whose email this is; `contact` is
/// the install admin people can ask (the relay checks it is one).
pub fn render(t: &Template, v: &Values, install_host: &str, contact: Option<&str>) -> Rendered {
    let mut text = Vec::new();
    let mut html = String::new();
    for part in t.body {
        match part {
            Part::Para(p) => {
                text.push(fill(p, v, false));
                html.push_str(&format!(
                    "<p style=\"color:#222;font-size:15px;line-height:1.5;margin:0 0 16px\">{}</p>",
                    fill(p, v, true)
                ));
            }
            Part::Quote(name) => {
                let q = v.get(name);
                if q.is_empty() {
                    continue;
                }
                text.push(
                    q.lines()
                        .map(|l| format!("> {l}"))
                        .collect::<Vec<_>>()
                        .join("\n"),
                );
                html.push_str(&format!(
                    "<blockquote style=\"border-left:3px solid #ccc;margin:0 0 16px;padding:4px 12px;color:#444;white-space:pre-wrap\">{}</blockquote>",
                    escape(q)
                ));
            }
            Part::Button(label, name) => {
                let link = v.get(name);
                if link.is_empty() {
                    continue;
                }
                text.push(format!("{label}: {link}"));
                html.push_str(&format!(
                    "<p style=\"margin:0 0 16px\"><a href=\"{}\" style=\"display:inline-block;background:#222;color:#fff;padding:10px 16px;border-radius:6px;text-decoration:none\">{}</a></p>",
                    escape(link),
                    escape(label)
                ));
            }
        }
    }
    let footer = footer(install_host, contact);
    text.push(String::new());
    text.push(footer.clone());
    html.push_str(&format!(
        "<p style=\"color:#777;font-size:12px;border-top:1px solid #ddd;padding-top:12px;margin-top:24px\">{}</p>",
        escape(&footer)
    ));
    Rendered {
        subject: t.subject.to_string(),
        text: text.join("\n\n"),
        html: format!(
            "<!DOCTYPE html><html><head><meta charset=\"utf-8\"></head><body style=\"font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#fff;padding:24px\">{html}</body></html>"
        ),
    }
}

/// Every relayed email ends with this: whose install sent it, and that
/// CoderHelm never asks for passwords or codes.
pub fn footer(install_host: &str, contact: Option<&str>) -> String {
    let ask = match contact {
        Some(c) if !c.is_empty() => format!(" Questions about it go to the install's admin, {c}."),
        _ => " Questions about it go to the install's admins.".to_string(),
    };
    format!(
        "Sent through CoderHelm's mail service for the self-hosted CoderHelm install at {install_host}. Links in this email go only to that install.{ask} CoderHelm never asks for your password or sign-in codes by email."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn hosts() -> Vec<String> {
        vec![
            "d111.cloudfront.net".to_string(),
            "acme.coderhelm.com".to_string(),
        ]
    }

    fn obj(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn every_template_has_a_fixed_subject_and_known_parameters() {
        for t in TEMPLATES {
            assert!(
                !t.subject.contains('{'),
                "{} subject takes a parameter",
                t.id
            );
            for part in t.body {
                let names: Vec<&str> = match part {
                    Part::Para(s) => s
                        .split('{')
                        .skip(1)
                        .filter_map(|x| x.split('}').next())
                        .collect(),
                    Part::Quote(n) | Part::Button(_, n) => vec![n],
                };
                for n in names {
                    assert!(
                        t.params.iter().any(|p| p.name == n),
                        "{} uses {n} without declaring it",
                        t.id
                    );
                }
            }
            for part in t.body {
                if let Part::Button(_, n) = part {
                    let spec = t.params.iter().find(|p| p.name == *n).unwrap();
                    assert_eq!(spec.kind, Kind::Link, "{}: a button opens a link", t.id);
                }
            }
        }
    }

    #[test]
    fn what_an_install_sends_has_a_subject_that_never_names_coderhelm() {
        for t in TEMPLATES.iter().filter(|t| t.install_may_send) {
            let plain = t.subject.to_ascii_lowercase().replace([' ', '-'], "");
            assert!(
                !plain.contains("coderhelm") && !uses_reserved_wording(t.subject),
                "{}: {}",
                t.id,
                t.subject
            );
        }
        // The relay's own notice is CoderHelm speaking, and says so.
        assert!(get("relay_paused").unwrap().subject.contains("CoderHelm"));
    }

    #[test]
    fn a_link_must_point_at_the_installs_own_verified_host() {
        let ok = check(
            "welcome",
            &obj(json!({"link": "https://acme.coderhelm.com/runs/1"})),
            &hosts(),
            true,
        );
        assert!(ok.is_ok());
        for bad in [
            "https://evil.example/runs/1",
            "http://acme.coderhelm.com/",
            "https://acme.coderhelm.com.evil.example/",
            "https://user@acme.coderhelm.com/",
            "https://acme.coderhelm.com:8443/",
            "javascript:alert(1)",
            "https://coderhelm.com/login",
            "https://app.coderhelm.com/settings",
        ] {
            let r = check("welcome", &obj(json!({ "link": bad })), &hosts(), true);
            assert_eq!(
                r.unwrap_err(),
                Refusal::LinkNotOwnHost("link".into()),
                "{bad}"
            );
        }
    }

    #[test]
    fn text_parameters_lose_links_and_addresses() {
        for (raw, want) in [
            (
                "Fix login at https://evil.example/x",
                "Fix login at [link removed]",
            ),
            ("see www.evil.example now", "see [link removed] now"),
            ("mail help@evil.example", "mail [link removed]"),
            ("go to evil.com/reset", "go to [link removed]"),
            ("ping 10.0.0.1", "ping [link removed]"),
            ("visit сoderhelm.com", "visit [link removed]"),
            ("click javascript:alert(1)", "click [link removed]"),
            (
                "Update README.md and main.rs",
                "Update README.md and main.rs",
            ),
        ] {
            assert_eq!(plain(raw, true), want, "{raw}");
        }
        assert_eq!(plain("a\u{202E}b\u{200B}c", true), "abc");
        assert_eq!(plain("one\ntwo", true), "one two");
        assert_eq!(plain("one\ntwo", false), "one\ntwo");
    }

    #[test]
    fn parameters_are_escaped_when_written() {
        let (t, v) = check(
            "support_update",
            &obj(json!({
                "ticket": "<b>Hi</b>",
                "note": "<script>x</script> & <a href=x>y</a>",
                "link": "https://acme.coderhelm.com/support/ticket/?id=1"
            })),
            &hosts(),
            true,
        )
        .unwrap();
        let r = render(t, &v, "acme.coderhelm.com", Some("admin@acme.test"));
        assert!(r.text.contains("admin@acme.test"));
        assert!(!r.html.contains("<script>"));
        assert!(!r.html.contains("<b>Hi"));
        assert!(r.html.contains("&lt;script&gt;"));
        assert!(r.html.contains("&lt;a href=x&gt;"));
        assert_eq!(r.subject, "Update on a support ticket");
        assert!(r.text.contains("acme.coderhelm.com"));
    }

    #[test]
    fn wording_that_reads_as_coderhelm_itself_is_refused() {
        for raw in [
            "CoderHelm Support",
            "coder-helm security team",
            "A message from CoderHelm",
            "CODERHELM   STAFF",
        ] {
            let r = check(
                "support_update",
                &obj(json!({
                    "ticket": raw,
                    "note": "hello",
                    "link": "https://acme.coderhelm.com/x"
                })),
                &hosts(),
                true,
            );
            assert_eq!(r.unwrap_err(), Refusal::Reserved("ticket".into()), "{raw}");
        }
        assert!(!uses_reserved_wording("Acme platform team"));
    }

    #[test]
    fn unknown_missing_wrong_and_long_parameters_are_refused() {
        assert_eq!(
            check("nope", &Map::new(), &hosts(), true).unwrap_err(),
            Refusal::UnknownTemplate
        );
        assert_eq!(
            check(
                "relay_paused",
                &obj(json!({"reason": "bounces"})),
                &hosts(),
                true
            )
            .unwrap_err(),
            Refusal::NotForInstalls
        );
        assert_eq!(
            check(
                "test_email",
                &obj(json!({"subject": "Reset your password"})),
                &hosts(),
                true
            )
            .unwrap_err(),
            Refusal::UnknownParam("subject".into())
        );
        assert_eq!(
            check(
                "test_email",
                &obj(json!({"html": "<p>hi</p>"})),
                &hosts(),
                true
            )
            .unwrap_err(),
            Refusal::UnknownParam("html".into())
        );
        assert_eq!(
            check("welcome", &Map::new(), &hosts(), true).unwrap_err(),
            Refusal::MissingParam("link".into())
        );
        let long = "x".repeat(81);
        let r = check(
            "team_invite",
            &obj(
                json!({"team": long, "inviter": "a", "role": "member", "link": "https://acme.coderhelm.com/"}),
            ),
            &hosts(),
            true,
        );
        assert_eq!(r.unwrap_err(), Refusal::TooLong("team".into()));
        let r = check(
            "team_invite",
            &obj(
                json!({"team": "t", "inviter": "a", "role": "superuser", "link": "https://acme.coderhelm.com/"}),
            ),
            &hosts(),
            true,
        );
        assert_eq!(r.unwrap_err(), Refusal::BadParam("role".into()));
        let r = check(
            "update_notice",
            &obj(
                json!({"what": "available", "version": "evil.com", "link": "https://acme.coderhelm.com/"}),
            ),
            &hosts(),
            true,
        );
        assert_eq!(r.unwrap_err(), Refusal::BadParam("version".into()));
    }

    #[test]
    fn secret_parameters_are_marked_and_dropped_for_logs() {
        let t = get("plugin_approval").unwrap();
        assert!(t
            .params
            .iter()
            .any(|p| p.name == "approve_link" && p.secret));
        let t2 = get("weekly_summary").unwrap();
        assert!(t2
            .params
            .iter()
            .any(|p| p.name == "unsubscribe" && p.secret));
        let params =
            obj(json!({"approve_link": "https://acme.coderhelm.com/a?t=SECRET", "plugin": "p"}));
        let shown = without_secrets(t, &params);
        assert!(!serde_json::to_string(&shown).unwrap().contains("SECRET"));
        assert!(shown.contains_key("plugin"));
    }

    #[test]
    fn every_relayed_email_rendered_for_a_self_hosted_install_names_no_hosted_address() {
        use crate::public_urls::{app_url_from_env, hosted_mentions, pretend_self_hosted};
        pretend_self_hosted();
        let app = app_url_from_env().to_string();
        let host = crate::public_urls::host_of(&app).to_string();
        for t in TEMPLATES {
            let mut params = Map::new();
            for p in t.params {
                let v = match p.kind {
                    Kind::Line(_) | Kind::Text(_) => Value::from("Acme platform"),
                    Kind::Number => Value::from(3),
                    Kind::Version => Value::from("v1.2.3"),
                    Kind::OneOf(words) => Value::from(words[0]),
                    Kind::Link => Value::from(format!("{app}/settings/")),
                };
                params.insert(p.name.to_string(), v);
            }
            let (t, v) = check(t.id, &params, std::slice::from_ref(&host), false)
                .unwrap_or_else(|e| panic!("{}: {e:?}", t.id));
            let r = render(t, &v, &host, Some("admin@acme.test"));
            for part in [&r.subject, &r.text, &r.html] {
                assert_eq!(hosted_mentions(part), Vec::<String>::new(), "{}", t.id);
            }
        }
    }

    #[test]
    fn fit_cuts_to_the_limit() {
        assert_eq!(fit("abc", 5), "abc");
        assert_eq!(fit("abcdef", 4).chars().count(), 4);
    }
}
