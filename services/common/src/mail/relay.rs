//! CoderHelm's mail relay for hybrid installs: the rules both sides follow.
//!
//! A self-hosted install that chose "Send email through CoderHelm" posts
//! `POST /hybrid/email` signed with its registration secret (the same
//! signature as every hybrid call, `hybrid::sign`). The body names a
//! request id (replay protection), one recipient, a template and its
//! parameters ([`super::templates`]). The hosted side sends only when:
//!
//! - the install is registered, verified and not revoked;
//! - the request id is new (kept for [`REPLAY_TTL_SECS`]);
//! - the recipient is on the install's list (members, admins and pending
//!   invites the install registered, [`Recipients`]) and not suppressed;
//! - the install is within its per-minute rate and daily quota, and email
//!   isn't paused for it (bounces or complaints, [`should_pause`]).
//!
//! The hosted side keeps a salted hash of each listed address, never the
//! address, except admins' addresses sealed with KMS so the relay can tell
//! them when it pauses. After sending it keeps delivery metadata only
//! (template id, a redacted address, status) for [`DELIVERY_TTL_SECS`].

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

/// Sends per install per minute.
pub const PER_MINUTE: u64 = 30;
/// Sends per install per UTC day.
pub const PER_DAY: u64 = 500;
/// New addresses an install may list per UTC day.
pub const NEW_RECIPIENTS_PER_DAY: u64 = 50;
/// The first listing (turning the relay on) may bring the install's
/// current people at once, up to this many.
pub const BOOTSTRAP_RECIPIENTS: usize = 200;
/// Most addresses one request may list.
pub const MAX_PER_REQUEST: usize = 200;
/// How long a request id is remembered.
pub const REPLAY_TTL_SECS: i64 = 2 * crate::hybrid::sign::TOLERANCE_SECS + 60;
/// How long a relayed email's delivery record is kept.
pub const DELIVERY_TTL_SECS: i64 = 3 * 86_400;
/// The window bounce and complaint rates are measured over (days).
pub const RATE_WINDOW_DAYS: i64 = 7;

pub const PATH_SEND: &str = "/hybrid/email";
pub const PATH_RECIPIENTS: &str = "/hybrid/email/recipients";
pub const PATH_STATUS: &str = "/hybrid/email/status";

/// The settings-table sort keys under `HYBRID#<install>`.
pub fn recipient_sk(hash: &str) -> String {
    format!("EMAILTO#{hash}")
}
pub fn suppressed_sk(hash: &str) -> String {
    format!("EMAILSUPPRESS#{hash}")
}
pub fn request_sk(id: &str) -> String {
    format!("EMAILREQ#{id}")
}
pub fn day_sk(day: &str) -> String {
    format!("EMAILDAY#{day}")
}
pub fn minute_sk(minute: &str) -> String {
    format!("EMAILMIN#{minute}")
}
pub const PAUSE_SK: &str = "EMAILPAUSE";

pub fn normalize(email: &str) -> String {
    email.trim().to_ascii_lowercase()
}

/// A salted hash of an address, per install: the same address on two
/// installs gives two unrelated hashes.
pub fn recipient_hash(install_id: &str, email: &str) -> String {
    let mut h = Sha256::new();
    h.update(b"coderhelm/email-relay/v1\n");
    h.update(install_id.as_bytes());
    h.update(b"\n");
    h.update(normalize(email).as_bytes());
    hex::encode(h.finalize())
}

/// `a***@example.com`: what a delivery record keeps.
pub fn redact(email: &str) -> String {
    let e = normalize(email);
    match e.split_once('@') {
        Some((local, domain)) => {
            let first: String = local.chars().take(1).collect();
            format!("{first}***@{domain}")
        }
        None => "***".to_string(),
    }
}

/// A plausible single address (no lists, no display names, no header tricks).
pub fn valid_address(email: &str) -> bool {
    let e = email.trim();
    if e.len() > 254 || e.is_empty() {
        return false;
    }
    if e.chars().any(|c| {
        c.is_whitespace()
            || c.is_control()
            || matches!(
                c,
                ',' | ';' | '<' | '>' | '"' | '(' | ')' | '[' | ']' | '\\'
            )
    }) {
        return false;
    }
    let Some((local, domain)) = e.split_once('@') else {
        return false;
    };
    !local.is_empty()
        && !domain.contains('@')
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && domain.is_ascii()
}

/// Whether an address is CoderHelm's own (`coderhelm.com` or below). A
/// self-hosted install never emails these, by any route.
pub fn is_coderhelm_address(email: &str) -> bool {
    let e = normalize(email);
    let Some((_, domain)) = e.rsplit_once('@') else {
        return false;
    };
    let domain = domain.trim_end_matches('.');
    domain == "coderhelm.com" || domain.ends_with(".coderhelm.com")
}

/// What a listed address is to the install.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RecipientKind {
    Admin,
    Member,
    Invite,
}

impl RecipientKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Member => "member",
            Self::Invite => "invite",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "admin" => Some(Self::Admin),
            "member" => Some(Self::Member),
            "invite" => Some(Self::Invite),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewRecipient {
    pub email: String,
    pub kind: RecipientKind,
}

/// `POST /hybrid/email/recipients`: list addresses. `by` is the install
/// admin who stands behind the request (an invite's sender, or the admin
/// who turned the relay on).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecipientsRequest {
    pub request_id: String,
    pub by: String,
    #[serde(default)]
    pub add: Vec<NewRecipient>,
    #[serde(default)]
    pub remove: Vec<String>,
}

/// `POST /hybrid/email`: one email.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendRequest {
    pub request_id: String,
    pub to: String,
    pub template: String,
    #[serde(default)]
    pub params: Map<String, Value>,
    /// The install admin named in the footer. The relay names them only
    /// when they are a listed admin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact: Option<String>,
}

/// A request id: 16 to 64 letters, digits, `-` or `_`.
pub fn valid_request_id(id: &str) -> bool {
    (16..=64).contains(&id.len())
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

pub fn new_request_id() -> String {
    ulid::Ulid::new().to_string()
}

/// Why the relay refused, as the install's Deliveries page explains it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    BadRequest,
    Replayed,
    NotVerified,
    BadAddress,
    CoderhelmAddress,
    NotListed,
    Suppressed,
    NotAdmin,
    TooManyNew,
    RateLimited,
    QuotaReached,
    Paused,
}

impl Refusal {
    pub fn code(self) -> &'static str {
        match self {
            Self::BadRequest => "bad_request",
            Self::Replayed => "request_replayed",
            Self::NotVerified => "install_not_verified",
            Self::BadAddress => "bad_address",
            Self::CoderhelmAddress => "coderhelm_address",
            Self::NotListed => "recipient_not_listed",
            Self::Suppressed => "recipient_suppressed",
            Self::NotAdmin => "inviter_not_admin",
            Self::TooManyNew => "too_many_new_recipients",
            Self::RateLimited => "email_rate_limited",
            Self::QuotaReached => "email_quota_reached",
            Self::Paused => "email_paused",
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::BadRequest => "The email request isn't in the expected shape.",
            Self::Replayed => "This request id was already used.",
            Self::NotVerified => "The install must finish verifying its address before CoderHelm sends its email.",
            Self::BadAddress => "That isn't a single email address.",
            Self::CoderhelmAddress => "CoderHelm's mail service doesn't send to CoderHelm's own addresses.",
            Self::NotListed => "The recipient isn't one of the install's people or pending invites.",
            Self::Suppressed => "This address bounced or marked CoderHelm email as spam, so nothing more is sent to it.",
            Self::NotAdmin => "Only an install admin can add people to the email list.",
            Self::TooManyNew => "The install added too many new addresses today.",
            Self::RateLimited => "Too many emails this minute.",
            Self::QuotaReached => "The install reached today's email quota.",
            Self::Paused => "Email through CoderHelm is paused for this install after bounces or complaints.",
        }
    }

    pub fn status(self) -> u16 {
        match self {
            Self::BadRequest | Self::BadAddress => 400,
            Self::Replayed => 409,
            Self::RateLimited | Self::QuotaReached | Self::TooManyNew => 429,
            _ => 403,
        }
    }
}

/// What the hosted side knows about the install's list, for one decision.
#[derive(Debug, Clone, Default)]
pub struct Listing {
    /// The hash's kind, when the address is listed.
    pub recipient: Option<RecipientKind>,
    pub suppressed: bool,
}

/// May this one email go out? Pure: the caller loaded what it needs.
pub fn may_send(to: &str, listing: &Listing, paused: bool) -> Result<(), Refusal> {
    if !valid_address(to) {
        return Err(Refusal::BadAddress);
    }
    if is_coderhelm_address(to) {
        return Err(Refusal::CoderhelmAddress);
    }
    if paused {
        return Err(Refusal::Paused);
    }
    if listing.recipient.is_none() {
        return Err(Refusal::NotListed);
    }
    if listing.suppressed {
        return Err(Refusal::Suppressed);
    }
    Ok(())
}

/// May this listing request change the list? `by_kind` is what `by` is on
/// the list now; `admins_listed` whether the list has any admin yet.
/// `new_today` is how many addresses were added today before this request.
pub fn may_list(
    req: &RecipientsRequest,
    by_kind: Option<RecipientKind>,
    admins_listed: bool,
    new_today: u64,
) -> Result<(), Refusal> {
    if req.add.len() + req.remove.len() > MAX_PER_REQUEST || !valid_address(&req.by) {
        return Err(Refusal::BadRequest);
    }
    for r in &req.add {
        if !valid_address(&r.email) {
            return Err(Refusal::BadAddress);
        }
        if is_coderhelm_address(&r.email) {
            return Err(Refusal::CoderhelmAddress);
        }
    }
    let by_is_admin = by_kind == Some(RecipientKind::Admin);
    // The first listing: the admin who turns the relay on lists themself.
    let bootstrap = !admins_listed
        && req
            .add
            .iter()
            .any(|r| r.kind == RecipientKind::Admin && normalize(&r.email) == normalize(&req.by));
    if !by_is_admin && !bootstrap {
        return Err(Refusal::NotAdmin);
    }
    let limit = if bootstrap {
        BOOTSTRAP_RECIPIENTS as u64
    } else {
        NEW_RECIPIENTS_PER_DAY
    };
    if new_today + req.add.len() as u64 > limit {
        return Err(Refusal::TooManyNew);
    }
    Ok(())
}

/// The day's send counters for one install.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    pub sent: u64,
    pub bounced: u64,
    pub complained: u64,
}

/// Whether bounces or complaints over the window call for a pause: more
/// than 5% hard bounces, or more than 0.5% complaints (and at least two),
/// once there are at least 20 sends to judge by. Amazon SES reviews an
/// account at 5% and 0.1%; pausing one install early keeps the shared
/// sender healthy for everyone.
pub fn should_pause(window: Counts) -> Option<&'static str> {
    if window.complained >= 2 && window.complained * 200 > window.sent {
        return Some("complaints");
    }
    if window.sent >= 20 && window.bounced * 20 > window.sent {
        return Some("bounces");
    }
    if window.complained >= 5 {
        return Some("complaints");
    }
    None
}

/// The From line: the install's verified address and "via CoderHelm",
/// never words the install chose. A claimed name that reads as CoderHelm
/// staff falls back to the CloudFront address.
pub fn display_name(name_host: Option<&str>, cloudfront: &str) -> String {
    let clean = |h: &str| -> String {
        h.chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '.' || *c == '-')
            .take(80)
            .collect::<String>()
            .to_ascii_lowercase()
    };
    let name_ok = |h: &str| {
        let first = h.split('.').next().unwrap_or("");
        // The same rule names follow (`names::looks_like_coderhelm`), and
        // stricter: any staff word anywhere, both readings of `1`.
        !first.is_empty()
            && !crate::hybrid::names::looks_like_coderhelm(first)
            && unconfuse(first).iter().all(|plain| {
                !STAFF_WORDS
                    .iter()
                    .any(|w| plain.contains(w) || plain.replace('-', "").contains(w))
                    && !super::templates::uses_reserved_wording(&plain.replace('-', " "))
            })
    };
    let host = match name_host.map(clean) {
        Some(h) if name_ok(&h) => h,
        _ => clean(cloudfront),
    };
    format!("{host} via CoderHelm")
}

/// A name with look-alike characters read as letters (`c0derhe1m`,
/// `5upp0rt`), so a staff word can't hide behind digits. `1`, `!` and `|`
/// read as either `l` or `i`, so both readings come back.
pub fn unconfuse(s: &str) -> Vec<String> {
    let read = |one: char| {
        s.chars()
            .map(|c| match c.to_ascii_lowercase() {
                '0' => 'o',
                '1' | '!' | '|' => one,
                '3' => 'e',
                '4' | '@' => 'a',
                '5' | '$' => 's',
                '7' => 't',
                '8' => 'b',
                '9' => 'g',
                c => c,
            })
            .collect::<String>()
            // "rn" reads as "m" and "vv" as "w" in many fonts.
            .replace("rn", "m")
            .replace("vv", "w")
    };
    vec![read('l'), read('i')]
}

/// Words a sender name may not carry: they would read as CoderHelm itself.
const STAFF_WORDS: &[&str] = &[
    "app",
    "api",
    "auth",
    "mail",
    "payment",
    "invoice",
    "status",
    "support",
    "security",
    "secure",
    "billing",
    "admin",
    "help",
    "official",
    "staff",
    "team",
    "verify",
    "account",
    "login",
    "signin",
    "password",
    "noreply",
    "no-reply",
    "coderhelm",
    "trust",
    "abuse",
    "postmaster",
    "notify",
    "notifications",
    "service",
];

/// `"name" <address>` with the name quoted safely.
pub fn from_header(display: &str, address: &str) -> String {
    let d: String = display
        .chars()
        .filter(|c| !matches!(c, '"' | '\\' | '\r' | '\n' | '<' | '>'))
        .collect();
    format!("\"{d}\" <{address}>")
}

/// A bounce or complaint, read from the SES event the topic delivers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailEvent {
    pub install_id: String,
    pub kind: EventKind,
    pub recipients: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// A permanent bounce: the address doesn't exist or refuses mail.
    Bounce,
    Complaint,
}

/// The KMS encryption context an admin's sealed address is bound to: one
/// install, this purpose only.
pub fn admin_context(install: &str) -> std::collections::HashMap<String, String> {
    std::collections::HashMap::from([
        ("install".to_string(), install.to_string()),
        ("purpose".to_string(), "email-relay-admin".to_string()),
    ])
}

/// The message tag that names the install on every relayed email.
pub const INSTALL_TAG: &str = "install";
pub const TEMPLATE_TAG: &str = "template";

/// Reads an SES event (raw SNS delivery to the queue). `None` for anything
/// else, for transient bounces, and for mail without an install tag.
pub fn parse_event(v: &Value) -> Option<MailEvent> {
    let kind = v
        .get("eventType")
        .or_else(|| v.get("notificationType"))?
        .as_str()?;
    let install_id = v
        .pointer("/mail/tags")
        .and_then(|t| t.get(INSTALL_TAG))
        .and_then(|l| l.as_array())
        .and_then(|l| l.first())
        .and_then(|x| x.as_str())
        .filter(|id| crate::hybrid::sign::is_install_id(id))?
        .to_string();
    let (kind, list) = match kind {
        "Bounce" => {
            if v.pointer("/bounce/bounceType").and_then(Value::as_str) != Some("Permanent") {
                return None;
            }
            (EventKind::Bounce, v.pointer("/bounce/bouncedRecipients")?)
        }
        "Complaint" => (
            EventKind::Complaint,
            v.pointer("/complaint/complainedRecipients")?,
        ),
        _ => return None,
    };
    let recipients = list
        .as_array()?
        .iter()
        .filter_map(|r| r.get("emailAddress").and_then(Value::as_str))
        .map(normalize)
        .collect::<Vec<_>>();
    if recipients.is_empty() {
        return None;
    }
    Some(MailEvent {
        install_id,
        kind,
        recipients,
    })
}

/// Whether a queued message is an SES event (and not worker work).
pub fn is_ses_event(v: &Value) -> bool {
    v.get("type").is_none()
        && v.get("mail").is_some()
        && (v.get("eventType").is_some() || v.get("notificationType").is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(by: &str, add: &[(&str, RecipientKind)]) -> RecipientsRequest {
        RecipientsRequest {
            request_id: new_request_id(),
            by: by.into(),
            add: add
                .iter()
                .map(|(e, k)| NewRecipient {
                    email: (*e).into(),
                    kind: *k,
                })
                .collect(),
            remove: vec![],
        }
    }

    #[test]
    fn hashes_are_per_install_and_ignore_case() {
        let a = recipient_hash("ins_a", "Dev@Example.com");
        assert_eq!(a, recipient_hash("ins_a", " dev@example.com "));
        assert_ne!(a, recipient_hash("ins_b", "dev@example.com"));
        assert!(!a.contains("example"));
    }

    #[test]
    fn only_listed_unsuppressed_outside_addresses_get_email() {
        let listed = Listing {
            recipient: Some(RecipientKind::Member),
            suppressed: false,
        };
        assert_eq!(may_send("dev@example.com", &listed, false), Ok(()));
        assert_eq!(
            may_send("stranger@example.org", &Listing::default(), false),
            Err(Refusal::NotListed)
        );
        assert_eq!(
            may_send("support@coderhelm.com", &listed, false),
            Err(Refusal::CoderhelmAddress)
        );
        assert_eq!(
            may_send("x@mail.coderhelm.com", &listed, false),
            Err(Refusal::CoderhelmAddress)
        );
        assert_eq!(
            may_send(
                "dev@example.com",
                &Listing {
                    suppressed: true,
                    ..listed.clone()
                },
                false
            ),
            Err(Refusal::Suppressed)
        );
        assert_eq!(
            may_send("dev@example.com", &listed, true),
            Err(Refusal::Paused)
        );
        for bad in [
            "a@b.com, c@d.com",
            "\"x\" <a@b.com>",
            "a@b.com\r\nBcc: c@d.com",
            "nope",
        ] {
            assert_eq!(
                may_send(bad, &listed, false),
                Err(Refusal::BadAddress),
                "{bad}"
            );
        }
    }

    #[test]
    fn only_a_listed_admin_adds_people_after_the_first_listing() {
        use RecipientKind::*;
        // The first listing: the admin lists themself and the install's people.
        let first = req(
            "admin@acme.test",
            &[("admin@acme.test", Admin), ("dev@acme.test", Member)],
        );
        assert_eq!(may_list(&first, None, false, 0), Ok(()));
        // Not a bootstrap once an admin is listed: `by` must be that admin.
        assert_eq!(may_list(&first, None, true, 0), Err(Refusal::NotAdmin));
        let invite = req("dev@acme.test", &[("new@acme.test", Invite)]);
        assert_eq!(
            may_list(&invite, Some(Member), true, 0),
            Err(Refusal::NotAdmin)
        );
        assert_eq!(may_list(&invite, None, true, 0), Err(Refusal::NotAdmin));
        let invite = req("admin@acme.test", &[("new@acme.test", Invite)]);
        assert_eq!(may_list(&invite, Some(Admin), true, 0), Ok(()));
        // A bootstrap can't name someone else as the admin behind it.
        let sneaky = req("admin@acme.test", &[("other@acme.test", Admin)]);
        assert_eq!(may_list(&sneaky, None, false, 0), Err(Refusal::NotAdmin));
        // Never CoderHelm's own addresses.
        let ch = req("admin@acme.test", &[("security@coderhelm.com", Invite)]);
        assert_eq!(
            may_list(&ch, Some(Admin), true, 0),
            Err(Refusal::CoderhelmAddress)
        );
    }

    #[test]
    fn new_addresses_per_day_are_limited() {
        use RecipientKind::*;
        let one = req("admin@acme.test", &[("n@acme.test", Invite)]);
        assert_eq!(
            may_list(&one, Some(Admin), true, NEW_RECIPIENTS_PER_DAY - 1),
            Ok(())
        );
        assert_eq!(
            may_list(&one, Some(Admin), true, NEW_RECIPIENTS_PER_DAY),
            Err(Refusal::TooManyNew)
        );
        let many: Vec<(String, RecipientKind)> = (0..BOOTSTRAP_RECIPIENTS)
            .map(|i| (format!("p{i}@acme.test"), Member))
            .collect();
        let mut boot = req("admin@acme.test", &[("admin@acme.test", Admin)]);
        boot.add.extend(many.iter().map(|(e, k)| NewRecipient {
            email: e.clone(),
            kind: *k,
        }));
        assert_eq!(may_list(&boot, None, false, 0), Err(Refusal::BadRequest));
        boot.add.truncate(BOOTSTRAP_RECIPIENTS);
        assert_eq!(may_list(&boot, None, false, 0), Ok(()));
    }

    #[test]
    fn pauses_on_bounce_or_complaint_rates() {
        assert_eq!(
            should_pause(Counts {
                sent: 100,
                bounced: 5,
                complained: 0
            }),
            None
        );
        assert_eq!(
            should_pause(Counts {
                sent: 100,
                bounced: 6,
                complained: 0
            }),
            Some("bounces")
        );
        assert_eq!(
            should_pause(Counts {
                sent: 10,
                bounced: 3,
                complained: 0
            }),
            None
        );
        assert_eq!(
            should_pause(Counts {
                sent: 100,
                bounced: 0,
                complained: 2
            }),
            Some("complaints")
        );
        assert_eq!(
            should_pause(Counts {
                sent: 1000,
                bounced: 0,
                complained: 2
            }),
            None
        );
        assert_eq!(
            should_pause(Counts {
                sent: 5000,
                bounced: 0,
                complained: 5
            }),
            Some("complaints")
        );
    }

    #[test]
    fn the_sender_name_is_the_verified_address_never_staff_wording() {
        assert_eq!(
            display_name(Some("acme.coderhelm.com"), "d1.cloudfront.net"),
            "acme.coderhelm.com via CoderHelm"
        );
        for bad in [
            "support.coderhelm.com",
            "coderhelm-security.coderhelm.com",
            "billing-team.coderhelm.com",
            "noreply.coderhelm.com",
            "acme-help.coderhelm.com",
            "c0derhe1m.coderhelm.com",
            "5upp0rt.coderhelm.com",
            "c0der-he1m-team.coderhelm.com",
            "app.coderhelm.com",
            "api-acme.coderhelm.com",
            "acme-login.coderhelm.com",
            "acme-b1lling.coderhelm.com",
            "coderhelrn.coderhelm.com",
        ] {
            assert_eq!(
                display_name(Some(bad), "d1.cloudfront.net"),
                "d1.cloudfront.net via CoderHelm",
                "{bad}"
            );
        }
        assert_eq!(
            display_name(None, "d1.cloudfront.net"),
            "d1.cloudfront.net via CoderHelm"
        );
        // Nothing outside letters, digits, dots and dashes survives.
        assert_eq!(display_name(Some("a\"<b>\r\nc"), "x"), "abc via CoderHelm");
        assert_eq!(
            from_header("a\"b", "notifications@notify.coderhelm.com"),
            "\"ab\" <notifications@notify.coderhelm.com>"
        );
    }

    #[test]
    fn request_ids_are_checked() {
        assert!(valid_request_id(&new_request_id()));
        assert!(!valid_request_id("short"));
        assert!(!valid_request_id(&"a".repeat(65)));
        assert!(!valid_request_id("01HXXXXXXXXXXXXXXXXXXXXXX/../"));
    }

    #[test]
    fn redacted_addresses_keep_only_the_first_letter_and_domain() {
        assert_eq!(redact("Dev@Example.com"), "d***@example.com");
        assert_eq!(redact("nope"), "***");
    }

    #[test]
    fn reads_permanent_bounces_and_complaints_with_the_install_tag() {
        let id = "ins_abcdefghijklmnopqrstuvwx";
        let bounce = json!({
            "eventType": "Bounce",
            "mail": {"tags": {"install": [id], "template": ["welcome"]}},
            "bounce": {"bounceType": "Permanent", "bouncedRecipients": [{"emailAddress": "Gone@Example.com"}]}
        });
        assert!(is_ses_event(&bounce));
        let e = parse_event(&bounce).unwrap();
        assert_eq!(e.install_id, id);
        assert_eq!(e.kind, EventKind::Bounce);
        assert_eq!(e.recipients, vec!["gone@example.com"]);
        let transient = json!({
            "eventType": "Bounce",
            "mail": {"tags": {"install": [id]}},
            "bounce": {"bounceType": "Transient", "bouncedRecipients": [{"emailAddress": "a@b.com"}]}
        });
        assert!(parse_event(&transient).is_none());
        let complaint = json!({
            "eventType": "Complaint",
            "mail": {"tags": {"install": [id]}},
            "complaint": {"complainedRecipients": [{"emailAddress": "a@b.com"}]}
        });
        assert_eq!(parse_event(&complaint).unwrap().kind, EventKind::Complaint);
        let untagged = json!({
            "eventType": "Complaint",
            "mail": {"tags": {}},
            "complaint": {"complainedRecipients": [{"emailAddress": "a@b.com"}]}
        });
        assert!(parse_event(&untagged).is_none());
        assert!(!is_ses_event(&json!({"type": "ticket", "mail": {}})));
    }
}
