//! CoderHelm addresses: `{name}.coderhelm.com` for a self-hosted install.
//!
//! The hosted side owns the coderhelm.com zone; an install claims a name and
//! sends the two DNS records its own certificate and CloudFront need. This
//! module decides which names may be claimed and which records may be
//! written for them, so the hosted provisioning API (and its tests) can never
//! write anything else into the zone. The IAM policy in infra repeats the
//! record-type and name limits as a second fence.

/// The parent zone by default. Moving installs to a separate domain later
/// (the way some hosts use a dedicated domain for customer sites) is the
/// `HYBRID_ZONE_NAME` setting, not a code change.
pub const DEFAULT_ZONE: &str = "coderhelm.com";

/// The parent zone the names live under (`HYBRID_ZONE_NAME`, else coderhelm.com).
pub fn zone() -> String {
    std::env::var("HYBRID_ZONE_NAME")
        .ok()
        .map(|z| z.trim().trim_end_matches('.').to_ascii_lowercase())
        .filter(|z| z.contains('.') && !z.starts_with('.'))
        .unwrap_or_else(|| DEFAULT_ZONE.to_string())
}

/// Shortest and longest name.
pub const MIN_LEN: usize = 3;
pub const MAX_LEN: usize = 40;

/// The reserved names: CoderHelm's own hosts in the zone and a blocklist
/// (contract/reserved_names.json, shared with the infra tests).
pub const RESERVED_JSON: &str = include_str!("../../contract/reserved_names.json");

/// Every reserved name.
pub fn reserved() -> &'static [String] {
    static R: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    R.get_or_init(|| {
        let v: serde_json::Value =
            serde_json::from_str(RESERVED_JSON).expect("reserved_names.json is JSON");
        ["zone_records", "blocklist"]
            .iter()
            .flat_map(|k| v[*k].as_array().cloned().unwrap_or_default())
            .filter_map(|n| n.as_str().map(str::to_string))
            .collect()
    })
}

/// Why a name can't be used, as one plain sentence for the person typing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameProblem {
    Empty,
    TooShort,
    TooLong,
    BadCharacters,
    HyphenAtEdge,
    DoubleHyphen,
    Reserved,
    Taken,
}

impl NameProblem {
    /// A stable code for the API and the guide.
    pub fn code(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::TooShort => "too_short",
            Self::TooLong => "too_long",
            Self::BadCharacters => "bad_characters",
            Self::HyphenAtEdge => "hyphen_at_edge",
            Self::DoubleHyphen => "double_hyphen",
            Self::Reserved => "reserved",
            Self::Taken => "taken",
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::Empty => "Type a name, such as your company's.",
            Self::TooShort => "Use at least 3 characters.",
            Self::TooLong => "Use at most 40 characters.",
            Self::BadCharacters => "Use lowercase letters, digits and hyphens only.",
            Self::HyphenAtEdge => "Start and end with a letter or a digit.",
            Self::DoubleHyphen => "Two hyphens in a row aren't allowed.",
            Self::Reserved => "That name is reserved. Try another one.",
            Self::Taken => "That name is taken. Try another one.",
        }
    }
}

/// The name as it will be used (trimmed, lowercase), or why it can't be.
/// Uniqueness is the store's job ([`NameProblem::Taken`]).
pub fn validate_name(raw: &str) -> Result<String, NameProblem> {
    let name = raw.trim().to_ascii_lowercase();
    let name = name
        .strip_suffix(&format!(".{}", zone()))
        .unwrap_or(&name)
        .to_string();
    if name.is_empty() {
        return Err(NameProblem::Empty);
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err(NameProblem::BadCharacters);
    }
    if name.len() < MIN_LEN {
        return Err(NameProblem::TooShort);
    }
    if name.len() > MAX_LEN {
        return Err(NameProblem::TooLong);
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Err(NameProblem::HyphenAtEdge);
    }
    // Also rules out punycode ("xn--"), which could spell look-alike names.
    if name.contains("--") {
        return Err(NameProblem::DoubleHyphen);
    }
    if reserved().iter().any(|r| r == &name) || looks_like_coderhelm(&name) {
        return Err(NameProblem::Reserved);
    }
    Ok(name)
}

/// Words no part of a name may be: CoderHelm's own hosts and the pages
/// people sign in, pay or ask for help on. A name is a real coderhelm.com
/// address with a valid certificate, so "app-login" or "support-team" would
/// read as CoderHelm's own.
pub const LOOKALIKE_WORDS: &[&str] = &[
    "api",
    "app",
    "apps",
    "auth",
    "docs",
    "stream",
    "www",
    "fallback",
    "login",
    "logon",
    "signin",
    "signup",
    "sso",
    "oauth",
    "admin",
    "support",
    "helpdesk",
    "help",
    "billing",
    "invoice",
    "pay",
    "payment",
    "payments",
    "account",
    "accounts",
    "secure",
    "security",
    "verify",
    "verification",
    "password",
    "passwords",
    "reset",
    "official",
    "staff",
    "status",
    "mail",
    "email",
    "webmail",
    "dashboard",
    "console",
    "portal",
    "trust",
];

/// Whether a (well-formed) name could pass for CoderHelm's own address:
/// "coderhelm" anywhere, digits standing in for letters included ("c0derhe1m"),
/// or a [`LOOKALIKE_WORDS`] word as one of its hyphen-separated parts, with
/// trailing digits ignored ("app2", "api-v2", "auth1").
pub fn looks_like_coderhelm(name: &str) -> bool {
    let letters: String = name
        .chars()
        .filter(|c| *c != '-')
        .map(|c| match c {
            '0' => 'o',
            '1' => 'l',
            '3' => 'e',
            '4' => 'a',
            '5' => 's',
            '7' => 't',
            c => c,
        })
        .collect();
    if letters.contains("coderhelm") || letters.contains("coderheim") {
        return true;
    }
    name.split('-').any(|part| {
        let word = part.trim_end_matches(|c: char| c.is_ascii_digit());
        LOOKALIKE_WORDS.contains(&word)
    })
}

/// The full hostname for a valid name.
pub fn hostname(name: &str) -> String {
    format!("{name}.{}", zone())
}

/// One DNS record an install asks the hosted side to write.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NameRecord {
    /// `certificate` (ACM's DNS validation) or `address` (to CloudFront).
    pub purpose: String,
    pub name: String,
    #[serde(rename = "type")]
    pub rtype: String,
    pub value: String,
}

fn bare(s: &str) -> String {
    s.trim().trim_end_matches('.').to_ascii_lowercase()
}

fn is_hex(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// True for a CloudFront distribution's own hostname (`d1234abcd.cloudfront.net`).
pub fn is_cloudfront_domain(host: &str) -> bool {
    let h = bare(host);
    let Some(label) = h.strip_suffix(".cloudfront.net") else {
        return false;
    };
    label.len() >= 8
        && label.len() <= 20
        && label.starts_with('d')
        && label
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
}

/// Whether the hosted side may write this record for `name`, pointing only at
/// `verified_cloudfront` (the install's verified distribution). Returns the
/// record with normalized values, or the reason it is refused.
pub fn check_record(
    name: &str,
    rec: &NameRecord,
    verified_cloudfront: &str,
) -> Result<NameRecord, &'static str> {
    let host = hostname(name);
    if !rec.rtype.eq_ignore_ascii_case("CNAME") {
        return Err("only CNAME records can be written");
    }
    let rname = bare(&rec.name);
    let value = bare(&rec.value);
    match rec.purpose.as_str() {
        "address" => {
            if rname != host {
                return Err("the address record must be the name itself");
            }
            if !is_cloudfront_domain(&value) {
                return Err("the address record must point at a CloudFront distribution");
            }
            if value != bare(verified_cloudfront) {
                return Err("the address record must point at this install's own CloudFront");
            }
        }
        "certificate" => {
            // ACM: _<32 hex>.<hostname>  CNAME  _<32 hex>.<label>.acm-validations.aws
            let Some(token) = rname.strip_suffix(&format!(".{host}")) else {
                return Err("the certificate record must be under the name");
            };
            let ok_name = token
                .strip_prefix('_')
                .is_some_and(|t| t.len() == 32 && is_hex(t));
            if !ok_name {
                return Err("the certificate record's name isn't one ACM makes");
            }
            let Some(rest) = value.strip_suffix(".acm-validations.aws") else {
                return Err("the certificate record must point at acm-validations.aws");
            };
            let mut parts = rest.split('.');
            let first = parts.next().unwrap_or("");
            let ok_value = first
                .strip_prefix('_')
                .is_some_and(|t| t.len() == 32 && is_hex(t))
                && parts.clone().count() == 1
                && parts.all(|p| {
                    !p.is_empty()
                        && p.chars()
                            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
                });
            if !ok_value {
                return Err("the certificate record's value isn't one ACM makes");
            }
        }
        _ => return Err("purpose must be certificate or address"),
    }
    Ok(NameRecord {
        purpose: rec.purpose.clone(),
        name: rname,
        rtype: "CNAME".into(),
        value,
    })
}

/// One Route 53 change the provisioning API makes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsChange {
    /// Fails when the record already exists: nothing is ever overwritten
    /// that this API didn't write itself.
    Create(DnsRecord),
    /// Only for a record this API wrote before (it's in the stored list).
    Upsert(DnsRecord),
    Delete(DnsRecord),
}

/// A record in the zone: a CNAME, or an alias (A/AAAA) to the hosted
/// fallback site while a name is still being set up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsRecord {
    Cname { name: String, value: String },
    FallbackAlias { name: String, ipv6: bool },
}

impl DnsRecord {
    pub fn name(&self) -> &str {
        match self {
            Self::Cname { name, .. } | Self::FallbackAlias { name, .. } => name,
        }
    }
}

fn cname(r: &NameRecord) -> DnsRecord {
    DnsRecord::Cname {
        name: r.name.clone(),
        value: r.value.clone(),
    }
}

fn fallback(host: &str) -> [DnsRecord; 2] {
    [
        DnsRecord::FallbackAlias {
            name: host.to_string(),
            ipv6: false,
        },
        DnsRecord::FallbackAlias {
            name: host.to_string(),
            ipv6: true,
        },
    ]
}

/// Claiming a name: the name answers with the hosted "being set up" page
/// until the install's own address takes over. CREATE, so a name that has
/// any record already is refused.
pub fn plan_claim(name: &str) -> Vec<DnsChange> {
    fallback(&hostname(name))
        .into_iter()
        .map(DnsChange::Create)
        .collect()
}

/// Writing checked records for a name. `stored` is what this API wrote
/// before; `pending` says the fallback alias is still in place.
pub fn plan_records(
    name: &str,
    stored: &[NameRecord],
    pending: bool,
    new: &[NameRecord],
) -> Vec<DnsChange> {
    let mut out = vec![];
    for n in new {
        let old = stored.iter().find(|o| o.purpose == n.purpose);
        match (n.purpose.as_str(), old) {
            (_, Some(o)) if o.name == n.name && o.value == n.value => {}
            (_, Some(o)) if o.name == n.name => out.push(DnsChange::Upsert(cname(n))),
            (_, Some(o)) => {
                out.push(DnsChange::Delete(cname(o)));
                out.push(DnsChange::Create(cname(n)));
            }
            ("address", None) if pending => {
                // In one batch: the fallback alias goes and the CNAME comes.
                for a in fallback(&hostname(name)) {
                    out.push(DnsChange::Delete(a));
                }
                out.push(DnsChange::Create(cname(n)));
            }
            (_, None) => out.push(DnsChange::Create(cname(n))),
        }
    }
    out
}

/// Releasing a name: everything this API wrote for it goes.
pub fn plan_release(name: &str, stored: &[NameRecord], pending: bool) -> Vec<DnsChange> {
    let mut out: Vec<DnsChange> = stored.iter().map(|r| DnsChange::Delete(cname(r))).collect();
    if pending {
        out.extend(fallback(&hostname(name)).into_iter().map(DnsChange::Delete));
    }
    out
}

/// Every change touches only the name or `_<token>.<name>` (a second
/// fence after [`check_record`]).
pub fn changes_stay_inside(name: &str, changes: &[DnsChange]) -> bool {
    let host = hostname(name);
    changes.iter().all(|c| {
        let r = match c {
            DnsChange::Create(r) | DnsChange::Upsert(r) | DnsChange::Delete(r) => r,
        };
        let n = r.name();
        n == host
            || n.strip_suffix(&format!(".{host}"))
                .is_some_and(|t| t.starts_with('_') && !t.contains('.'))
    })
}

/// How many name changes (claim or release) an install may make per day.
pub const CHANGES_PER_DAY: u32 = 5;
/// How many availability checks one address may make per minute.
pub const CHECKS_PER_MINUTE: u32 = 30;

#[cfg(test)]
mod tests {
    use super::*;

    const CF: &str = "d1abcdef234567.cloudfront.net";

    fn rec(purpose: &str, name: &str, value: &str) -> NameRecord {
        NameRecord {
            purpose: purpose.into(),
            name: name.into(),
            rtype: "CNAME".into(),
            value: value.into(),
        }
    }

    #[test]
    fn names_are_normalized_and_checked() {
        assert_eq!(validate_name(" Example-Co ").unwrap(), "example-co");
        assert_eq!(validate_name("example.coderhelm.com").unwrap(), "example");
        assert_eq!(validate_name(""), Err(NameProblem::Empty));
        assert_eq!(validate_name("ab"), Err(NameProblem::TooShort));
        assert_eq!(validate_name(&"a".repeat(41)), Err(NameProblem::TooLong));
        assert_eq!(validate_name("ex_ample"), Err(NameProblem::BadCharacters));
        assert_eq!(validate_name("ex.ample"), Err(NameProblem::BadCharacters));
        assert_eq!(validate_name("-example"), Err(NameProblem::HyphenAtEdge));
        assert_eq!(validate_name("example-"), Err(NameProblem::HyphenAtEdge));
        assert_eq!(
            validate_name("xn--exmple-cua"),
            Err(NameProblem::DoubleHyphen)
        );
        assert_eq!(validate_name("ex--ample"), Err(NameProblem::DoubleHyphen));
        assert_eq!(validate_name("ünïcode"), Err(NameProblem::BadCharacters));
    }

    #[test]
    fn reserved_names_are_refused() {
        for n in [
            "www",
            "api",
            "app",
            "docs",
            "support",
            "admin",
            "status",
            "login",
            "coderhelm",
        ] {
            assert_eq!(validate_name(n), Err(NameProblem::Reserved), "{n}");
        }
        // Every reserved word is itself a well-formed name (or it could never match).
        for n in reserved() {
            assert!(n.len() >= 2 && !n.contains('.'), "{n}");
        }
        // CoderHelm's own hosts are all reserved.
        for n in [
            "api", "app", "auth", "docs", "stream", "www", "careers", "legal", "about", "blog",
        ] {
            assert!(reserved().iter().any(|r| r == n), "{n}");
        }
        assert_eq!(zone(), DEFAULT_ZONE);
    }

    /// A name is a real coderhelm.com address with a valid certificate, so
    /// it must never read as CoderHelm's own: no "coderhelm" (with digit
    /// look-alikes), and no word of a CoderHelm host or a sign-in, billing
    /// or support page as one of its parts.
    #[test]
    fn look_alike_names_are_refused() {
        for n in [
            "coderhelm-support",
            "my-coderhelm",
            "c0derhelm",
            "coderhe1m",
            "c0derhe1m-app",
            "coder-helm",
            "app2",
            "api-v2",
            "app-login",
            "login-app",
            "secure-signin",
            "acme-sso",
            "support-team",
            "billing-portal",
            "auth1",
            "www2",
            "docs-site",
            "verify-account",
            "password-reset",
        ] {
            assert_eq!(validate_name(n), Err(NameProblem::Reserved), "{n}");
        }
        for n in [
            "example",
            "example-co",
            "acme",
            "my-team",
            "apple",
            "application-lab",
            "mapbox",
            "rapid7",
            "team42",
        ] {
            assert!(validate_name(n).is_ok(), "{n}");
        }
    }

    #[test]
    fn every_problem_reads_plainly() {
        for p in [
            NameProblem::Empty,
            NameProblem::TooShort,
            NameProblem::TooLong,
            NameProblem::BadCharacters,
            NameProblem::HyphenAtEdge,
            NameProblem::DoubleHyphen,
            NameProblem::Reserved,
            NameProblem::Taken,
        ] {
            assert!(p.message().ends_with('.'));
            assert!(!p.code().is_empty());
        }
    }

    #[test]
    fn the_address_record_points_only_at_the_verified_distribution() {
        let ok = check_record(
            "example",
            &rec(
                "address",
                "example.coderhelm.com.",
                "D1ABCDEF234567.cloudfront.net.",
            ),
            CF,
        )
        .unwrap();
        assert_eq!(ok.name, "example.coderhelm.com");
        assert_eq!(ok.value, CF);
        assert!(check_record("example", &rec("address", "other.coderhelm.com", CF), CF).is_err());
        assert!(check_record(
            "example",
            &rec("address", "example.coderhelm.com", "evil.example.org"),
            CF
        )
        .is_err());
        assert!(check_record(
            "example",
            &rec(
                "address",
                "example.coderhelm.com",
                "d9999999999.cloudfront.net"
            ),
            CF
        )
        .is_err());
        assert!(check_record("example", &rec("address", "coderhelm.com", CF), CF).is_err());
    }

    #[test]
    fn the_certificate_record_must_look_like_acm() {
        let n = format!("_{}.example.coderhelm.com", "a".repeat(32));
        let v = format!("_{}.xlfgrmvvlj.acm-validations.aws.", "b".repeat(32));
        assert!(check_record("example", &rec("certificate", &n, &v), CF).is_ok());
        // Not under the name.
        let other = format!("_{}.other.coderhelm.com", "a".repeat(32));
        assert!(check_record("example", &rec("certificate", &other, &v), CF).is_err());
        // Not ACM's shape.
        assert!(check_record(
            "example",
            &rec("certificate", "_short.example.coderhelm.com", &v),
            CF
        )
        .is_err());
        assert!(check_record("example", &rec("certificate", &n, "evil.example.org"), CF).is_err());
        let deep = format!("_{}.a.b.acm-validations.aws", "b".repeat(32));
        assert!(check_record("example", &rec("certificate", &n, &deep), CF).is_err());
    }

    #[test]
    fn only_cname_records_and_known_purposes() {
        let mut r = rec("address", "example.coderhelm.com", CF);
        r.rtype = "TXT".into();
        assert_eq!(
            check_record("example", &r, CF),
            Err("only CNAME records can be written")
        );
        assert!(check_record("example", &rec("mx", "example.coderhelm.com", CF), CF).is_err());
    }

    fn nr(purpose: &str, name: &str, value: &str) -> NameRecord {
        rec(purpose, name, value)
    }

    #[test]
    fn a_claim_points_the_name_at_the_setup_page_without_overwriting() {
        let c = plan_claim("example");
        assert_eq!(c.len(), 2);
        assert!(c.iter().all(|x| matches!(x, DnsChange::Create(DnsRecord::FallbackAlias { name, .. }) if name == "example.coderhelm.com")));
        assert!(changes_stay_inside("example", &c));
    }

    #[test]
    fn records_are_created_once_then_only_our_own_are_changed() {
        let cert = nr(
            "certificate",
            &format!("_{}.example.coderhelm.com", "a".repeat(32)),
            "_b.x.acm-validations.aws",
        );
        let addr = nr("address", "example.coderhelm.com", CF);
        // First certificate record: CREATE (refused by Route 53 if anything is there).
        let c = plan_records("example", &[], true, std::slice::from_ref(&cert));
        assert_eq!(c, vec![DnsChange::Create(cname(&cert))]);
        // The address replaces the setup page in one batch.
        let c = plan_records(
            "example",
            std::slice::from_ref(&cert),
            true,
            &[cert.clone(), addr.clone()],
        );
        assert_eq!(c.len(), 3);
        assert!(matches!(
            &c[0],
            DnsChange::Delete(DnsRecord::FallbackAlias { ipv6: false, .. })
        ));
        assert!(matches!(
            &c[1],
            DnsChange::Delete(DnsRecord::FallbackAlias { ipv6: true, .. })
        ));
        assert_eq!(c[2], DnsChange::Create(cname(&addr)));
        // Nothing changed: nothing written.
        assert!(plan_records(
            "example",
            &[cert.clone(), addr.clone()],
            false,
            &[cert.clone(), addr.clone()]
        )
        .is_empty());
        // A new certificate token: the old one goes, the new one is created.
        let cert2 = nr(
            "certificate",
            &format!("_{}.example.coderhelm.com", "c".repeat(32)),
            "_d.x.acm-validations.aws",
        );
        let c = plan_records(
            "example",
            std::slice::from_ref(&cert),
            false,
            std::slice::from_ref(&cert2),
        );
        assert_eq!(
            c,
            vec![
                DnsChange::Delete(cname(&cert)),
                DnsChange::Create(cname(&cert2))
            ]
        );
        // Same name, new value: UPSERT is only ever for a record we wrote.
        let addr2 = nr(
            "address",
            "example.coderhelm.com",
            "d9999999999.cloudfront.net",
        );
        assert_eq!(
            plan_records(
                "example",
                std::slice::from_ref(&addr),
                false,
                std::slice::from_ref(&addr2)
            ),
            vec![DnsChange::Upsert(cname(&addr2))]
        );
        assert!(changes_stay_inside(
            "example",
            &plan_records("example", std::slice::from_ref(&cert), true, &[cert2, addr])
        ));
    }

    #[test]
    fn a_release_removes_only_what_was_written() {
        let addr = nr("address", "example.coderhelm.com", CF);
        let c = plan_release("example", std::slice::from_ref(&addr), false);
        assert_eq!(c, vec![DnsChange::Delete(cname(&addr))]);
        assert_eq!(plan_release("example", &[], true).len(), 2);
        assert!(plan_release("example", &[], false).is_empty());
    }

    #[test]
    fn changes_outside_the_name_are_caught() {
        let bad = vec![DnsChange::Create(DnsRecord::Cname {
            name: "api.coderhelm.com".into(),
            value: CF.into(),
        })];
        assert!(!changes_stay_inside("example", &bad));
        let deep = vec![DnsChange::Create(DnsRecord::Cname {
            name: "a.b.example.coderhelm.com".into(),
            value: CF.into(),
        })];
        assert!(!changes_stay_inside("example", &deep));
        let ok = vec![DnsChange::Delete(DnsRecord::Cname {
            name: "_tok.example.coderhelm.com".into(),
            value: "v".into(),
        })];
        assert!(changes_stay_inside("example", &ok));
    }

    #[test]
    fn cloudfront_domains_are_recognized() {
        assert!(is_cloudfront_domain("d1abcdef234567.cloudfront.net"));
        assert!(is_cloudfront_domain("d1abcdef234567.cloudfront.net."));
        assert!(!is_cloudfront_domain("cloudfront.net"));
        assert!(!is_cloudfront_domain("x.d1abcdef234567.cloudfront.net"));
        assert!(!is_cloudfront_domain(
            "d1abcdef234567.cloudfront.net.evil.org"
        ));
    }
}
