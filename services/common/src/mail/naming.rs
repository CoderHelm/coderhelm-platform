//! Whose email this is: the sender name and the start of every subject.
//!
//! An install's email names the install, never its CloudFront address
//! (`d1234.cloudfront.net` reads as spam). In this order:
//!
//! 1. the organization the config names (`email.organization`): `Acme`;
//! 2. the install's own address: its verified custom domain
//!    (`helm.example.org`) or its CoderHelm name (`acme`, from
//!    `acme.coderhelm.com`);
//! 3. nothing, and the email comes from plain `CoderHelm`.
//!
//! The sender name is `<label> CoderHelm` and subjects start with
//! `<label> CoderHelm: `. Through CoderHelm's mail relay the label must not
//! read as CoderHelm itself ([`org_label`], [`host_label`]); through the
//! install's own Amazon SES an admin's own `email.fromName` wins.

use base64::Engine;

/// The most characters an organization label keeps.
pub const ORG_MAX: usize = 40;

/// The product name every label is followed by.
pub const PRODUCT: &str = "CoderHelm";

/// Words a label may not be (whole words, any case, look-alikes read as
/// letters): they would read as CoderHelm's own mail.
const STAFF_WORDS: &[&str] = &[
    "app",
    "api",
    "auth",
    "mail",
    "email",
    "payment",
    "payments",
    "invoice",
    "status",
    "support",
    "security",
    "secure",
    "billing",
    "admin",
    "administrator",
    "help",
    "helpdesk",
    "official",
    "staff",
    "team",
    "verify",
    "verification",
    "account",
    "accounts",
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
    "notification",
    "notifications",
    "service",
    "alert",
    "alerts",
];

/// A name with look-alike characters read as letters (`c0derhe1m`,
/// `5upp0rt`). `1`, `!` and `|` read as either `l` or `i`, so both readings
/// come back.
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

fn names_coderhelm(s: &str) -> bool {
    unconfuse(s).iter().any(|plain| {
        let joined: String = plain.chars().filter(|c| c.is_alphanumeric()).collect();
        joined.contains("coderhelm") || joined.contains("coderheim")
    })
}

fn is_staff_word(word: &str) -> bool {
    unconfuse(word).iter().any(|plain| {
        STAFF_WORDS.contains(&plain.as_str())
            || STAFF_WORDS.contains(&plain.replace('-', "").as_str())
    })
}

/// A host that is an AWS address, never a name people should see.
pub fn is_aws_host(host: &str) -> bool {
    let h = host.trim().trim_end_matches('.').to_ascii_lowercase();
    h.contains("cloudfront.net") || h.contains("amazonaws.com") || h.contains("awsapps.com")
}

/// The organization name as a label, when it is safe to put in front of
/// "CoderHelm" on mail CoderHelm's relay sends: one line of letters, digits,
/// spaces and `-` `.` `&` `'`, at most [`ORG_MAX`] characters, nothing that
/// looks like a link or an address, no AWS host, and no word that reads as
/// CoderHelm's own mail (`Support`, `Billing`, `C0derHelm`, ...).
pub fn org_label(raw: &str) -> Option<String> {
    let s = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    if s.is_empty() || s.chars().count() > ORG_MAX {
        return None;
    }
    if !s
        .chars()
        .all(|c| c.is_alphanumeric() || matches!(c, ' ' | '-' | '.' | '&' | '\''))
    {
        return None;
    }
    if !s.chars().any(char::is_alphanumeric) || is_aws_host(&s) {
        return None;
    }
    // A word that a mail app would turn into a link.
    if super::templates::plain(&s, true) != s {
        return None;
    }
    if names_coderhelm(&s) || super::templates::uses_reserved_wording(&s) {
        return None;
    }
    let words = s.split(|c: char| !c.is_alphanumeric() && c != '-');
    if words.filter(|w| !w.is_empty()).any(is_staff_word) {
        return None;
    }
    Some(s)
}

/// A label from the install's own address: the CoderHelm name (`acme` from
/// `acme.coderhelm.com`, the zone dropped) or a custom domain as written.
/// Never an AWS host, and nothing that reads as CoderHelm's own mail.
pub fn host_label(host: &str, coderhelm_zone: &str) -> Option<String> {
    let h = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if h.is_empty() || is_aws_host(&h) || h.len() > 80 {
        return None;
    }
    if !h
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
    {
        return None;
    }
    let zone = coderhelm_zone
        .trim()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let name = if !zone.is_empty() {
        h.strip_suffix(&format!(".{zone}")).map(str::to_string)
    } else {
        None
    };
    let first = h.split('.').next().unwrap_or("");
    let label = match name {
        // A CoderHelm name: one label, shown alone.
        Some(n) if !n.contains('.') => n,
        Some(_) => return None,
        None => h.clone(),
    };
    let ok = !first.is_empty()
        && !crate::hybrid::names::looks_like_coderhelm(first)
        && !names_coderhelm(first)
        && !first.split('-').any(is_staff_word)
        && unconfuse(first)
            .iter()
            .all(|plain| !super::templates::uses_reserved_wording(&plain.replace('-', " ")));
    ok.then_some(label)
}

/// The label in force: the organization, else the install's custom domain,
/// else its CoderHelm name. `None`: plain "CoderHelm".
pub fn label(
    org: Option<&str>,
    domain: Option<&str>,
    name_host: Option<&str>,
    coderhelm_zone: &str,
) -> Option<String> {
    org.and_then(org_label)
        .or_else(|| domain.and_then(|d| host_label(d, coderhelm_zone)))
        .or_else(|| name_host.and_then(|n| host_label(n, coderhelm_zone)))
}

/// `Acme CoderHelm`, or `CoderHelm` with no label.
pub fn display_name(label: Option<&str>) -> String {
    match label.map(str::trim).filter(|l| !l.is_empty()) {
        Some(l) => format!("{l} {PRODUCT}"),
        None => PRODUCT.to_string(),
    }
}

/// What every subject an install sends starts with: `Acme CoderHelm: `, or
/// `Your CoderHelm install: ` with no label (so it never reads as CoderHelm
/// itself writing).
pub fn subject_prefix(label: Option<&str>) -> String {
    match label.map(str::trim).filter(|l| !l.is_empty()) {
        Some(l) => format!("{l} {PRODUCT}: "),
        None => format!("Your {PRODUCT} install: "),
    }
}

/// A From header within RFC 5322: `"Acme CoderHelm" <address>`, with the
/// name quoted (quotes, backslashes, angle brackets and line breaks taken
/// out), or as RFC 2047 encoded words when it isn't plain ASCII.
pub fn from_header(display: &str, address: &str) -> String {
    let d: String = display
        .chars()
        .filter(|c| !matches!(c, '"' | '\\' | '<' | '>') && !c.is_control())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if d.is_empty() {
        return address.to_string();
    }
    if d.is_ascii() {
        return format!("\"{d}\" <{address}>");
    }
    format!("{} <{address}>", encoded_words(&d))
}

/// RFC 2047 `=?UTF-8?B?...?=` words, each at most 75 characters, split on
/// character boundaries.
fn encoded_words(s: &str) -> String {
    let b64 = base64::engine::general_purpose::STANDARD;
    let mut words = Vec::new();
    let mut chunk = String::new();
    for c in s.chars() {
        if chunk.len() + c.len_utf8() > 45 {
            words.push(format!("=?UTF-8?B?{}?=", b64.encode(chunk.as_bytes())));
            chunk.clear();
        }
        chunk.push(c);
    }
    if !chunk.is_empty() {
        words.push(format!("=?UTF-8?B?{}?=", b64.encode(chunk.as_bytes())));
    }
    words.join(" ")
}

/// The install's own SES sender name: `email.fromName` when an admin set
/// one (anything but the default `CoderHelm`), else `<label> CoderHelm`
/// from the organization and the install's own address.
pub fn ses_display_name(
    from_name: Option<&str>,
    org: Option<&str>,
    app_host: &str,
    coderhelm_zone: &str,
) -> String {
    if let Some(n) = from_name.map(str::trim).filter(|n| !n.is_empty()) {
        if n != PRODUCT && !is_aws_host(n) {
            return n.to_string();
        }
    }
    display_name(ses_label(org, app_host, coderhelm_zone).as_deref())
}

/// The label an install's own SES mail uses: its organization (its own
/// sender, so only an AWS host is refused), else its own address.
pub fn ses_label(org: Option<&str>, app_host: &str, coderhelm_zone: &str) -> Option<String> {
    let org = org
        .map(|o| o.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|o| !o.is_empty() && !is_aws_host(o))
        .map(|o| o.chars().take(ORG_MAX).collect::<String>());
    org.or_else(|| host_label(app_host, coderhelm_zone))
}

/// The organization this install names (`ORGANIZATION_NAME`), else the
/// config's `email.fromName` with a trailing "CoderHelm" dropped, when an
/// admin set one.
pub fn org_from_env() -> Option<String> {
    org_from(
        std::env::var("ORGANIZATION_NAME").ok().as_deref(),
        std::env::var("EMAIL_FROM_NAME").ok().as_deref(),
    )
}

pub fn org_from(org: Option<&str>, from_name: Option<&str>) -> Option<String> {
    let set = |v: Option<&str>| {
        v.map(|s| s.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|s| !s.is_empty())
    };
    set(org).or_else(|| {
        let n = set(from_name)?;
        let lower = n.to_ascii_lowercase();
        let base = if lower == "coderhelm" {
            return None;
        } else if lower.ends_with(" coderhelm") {
            n[..n.len() - " coderhelm".len()].trim().to_string()
        } else {
            n
        };
        (!base.is_empty()).then_some(base)
    })
}

/// The CoderHelm zone hybrid names live in (`coderhelm.com` unless the
/// deployment says otherwise).
pub fn coderhelm_zone() -> String {
    crate::hybrid::names::zone().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ZONE: &str = "coderhelm.com";

    #[test]
    fn the_organization_comes_first_then_the_installs_own_address() {
        assert_eq!(
            label(
                Some("Acme"),
                Some("helm.example.org"),
                Some("acme.coderhelm.com"),
                ZONE
            ),
            Some("Acme".into())
        );
        assert_eq!(
            label(
                None,
                Some("helm.example.org"),
                Some("acme.coderhelm.com"),
                ZONE
            ),
            Some("helm.example.org".into())
        );
        assert_eq!(
            label(None, None, Some("acme.coderhelm.com"), ZONE),
            Some("acme".into())
        );
        assert_eq!(label(None, None, None, ZONE), None);
        assert_eq!(display_name(Some("Acme")), "Acme CoderHelm");
        assert_eq!(display_name(None), "CoderHelm");
        assert_eq!(subject_prefix(Some("acme")), "acme CoderHelm: ");
        assert_eq!(subject_prefix(None), "Your CoderHelm install: ");
    }

    #[test]
    fn a_cloudfront_host_never_becomes_a_name() {
        for host in [
            "d1example.cloudfront.net",
            "D1EXAMPLE.CLOUDFRONT.NET.",
            "abc.execute-api.us-east-1.amazonaws.com",
        ] {
            assert_eq!(host_label(host, ZONE), None, "{host}");
            assert_eq!(org_label(host), None, "{host}");
            assert_eq!(label(Some(host), Some(host), Some(host), ZONE), None);
            assert_eq!(ses_label(Some(host), host, ZONE), None);
            let name = ses_display_name(Some("CoderHelm"), Some(host), host, ZONE);
            assert_eq!(name, "CoderHelm");
            assert_eq!(ses_display_name(Some(host), None, host, ZONE), "CoderHelm");
        }
    }

    #[test]
    fn an_organization_that_reads_as_coderhelm_staff_is_refused() {
        for bad in [
            "CoderHelm",
            "Coder Helm",
            "C0derHe1m Labs",
            "Acme Support",
            "Billing",
            "5upp0rt",
            "Security Team",
            "No-Reply",
            "acme.com",
            "Acme <x@y.com>",
            "Acme\r\nBcc: x@y.com",
            "Acme \u{202E}lol",
            "a\"b",
            &"x".repeat(ORG_MAX + 1),
            "",
            "  ",
            "--",
        ] {
            assert_eq!(org_label(bad), None, "{bad:?}");
        }
        for good in [
            "Acme",
            "Acme Corp",
            "Mailchimp",
            "AT&T",
            "O'Brien & Co.",
            "Société Générale",
            "Teamworks",
        ] {
            assert_eq!(org_label(good).as_deref(), Some(good), "{good}");
        }
        // Spacing is tidied.
        assert_eq!(org_label("  Acme   Corp ").as_deref(), Some("Acme Corp"));
    }

    #[test]
    fn a_claimed_name_that_reads_as_staff_falls_through() {
        for bad in [
            "support.coderhelm.com",
            "coderhelm-security.coderhelm.com",
            "billing-team.coderhelm.com",
            "noreply.coderhelm.com",
            "c0derhe1m.coderhelm.com",
            "5upp0rt.coderhelm.com",
            "app.coderhelm.com",
            "api-acme.coderhelm.com",
            "acme-login.coderhelm.com",
            "acme-b1lling.coderhelm.com",
            "coderhelrn.coderhelm.com",
            "a.b.coderhelm.com",
        ] {
            assert_eq!(host_label(bad, ZONE), None, "{bad}");
        }
        assert_eq!(
            host_label("acme-eng.coderhelm.com", ZONE).as_deref(),
            Some("acme-eng")
        );
        assert_eq!(
            host_label("Helm.Example.org.", ZONE).as_deref(),
            Some("helm.example.org")
        );
        assert_eq!(host_label("support.example.org", ZONE), None);
        assert_eq!(
            label(Some("Acme Support"), None, Some("acme.coderhelm.com"), ZONE).as_deref(),
            Some("acme")
        );
    }

    #[test]
    fn the_from_header_is_quoted_or_encoded() {
        let a = "notifications@notify.coderhelm.com";
        assert_eq!(
            from_header("Acme CoderHelm", a),
            "\"Acme CoderHelm\" <notifications@notify.coderhelm.com>"
        );
        assert_eq!(
            from_header("a\"b\\c<d>\r\ne f", a),
            format!("\"abcde f\" <{a}>")
        );
        assert_eq!(from_header("", a), a);
        let enc = from_header("Société Générale CoderHelm", a);
        assert!(enc.starts_with("=?UTF-8?B?"), "{enc}");
        assert!(enc.ends_with(&format!(" <{a}>")));
        assert!(!enc.contains('"'));
        // Each encoded word stays within 75 characters, and decodes back.
        let long = "Ünïcödé ".repeat(10) + "CoderHelm";
        let enc = from_header(&long, a);
        let words: Vec<&str> = enc.split(' ').filter(|w| w.starts_with("=?")).collect();
        assert!(words.len() > 1);
        let mut back = String::new();
        for w in &words {
            assert!(w.len() <= 75, "{w}");
            let b = w.trim_start_matches("=?UTF-8?B?").trim_end_matches("?=");
            let bytes = base64::engine::general_purpose::STANDARD.decode(b).unwrap();
            back.push_str(&String::from_utf8(bytes).unwrap());
        }
        assert_eq!(back, long.split_whitespace().collect::<Vec<_>>().join(" "));
    }

    #[test]
    fn the_installs_own_ses_sender_name() {
        // An admin's own name wins on their own SES.
        assert_eq!(
            ses_display_name(Some("Acme Robots"), Some("Acme"), "x", ZONE),
            "Acme Robots"
        );
        // The default name gives way to the organization, then the address.
        assert_eq!(
            ses_display_name(Some("CoderHelm"), Some("Acme"), "d1.cloudfront.net", ZONE),
            "Acme CoderHelm"
        );
        assert_eq!(
            ses_display_name(Some("CoderHelm"), None, "helm.example.org", ZONE),
            "helm.example.org CoderHelm"
        );
        assert_eq!(
            ses_display_name(None, None, "acme.coderhelm.com", ZONE),
            "acme CoderHelm"
        );
        assert_eq!(
            ses_display_name(None, None, "d1.cloudfront.net", ZONE),
            "CoderHelm"
        );
    }

    #[test]
    fn the_organization_comes_from_the_config() {
        assert_eq!(
            org_from(Some(" Acme "), Some("Other")).as_deref(),
            Some("Acme")
        );
        assert_eq!(
            org_from(None, Some("Acme CoderHelm")).as_deref(),
            Some("Acme")
        );
        assert_eq!(
            org_from(None, Some("Acme Robots")).as_deref(),
            Some("Acme Robots")
        );
        assert_eq!(org_from(None, Some("CoderHelm")), None);
        assert_eq!(org_from(Some(""), Some("")), None);
    }
}
