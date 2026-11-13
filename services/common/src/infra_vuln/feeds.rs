//! The public feeds infrastructure findings are matched against, as pure
//! parsers (the worker fetches and stores them:
//! `worker::passes::infra_feeds`).
//!
//! | Feed | What it gives |
//! |---|---|
//! | ALAS RSS (AL2023, AL2) | every advisory: id, severity, package, CVEs, date |
//! | ALAS advisory page | the fixed AL2023 release and the new package versions |
//! | Bottlerocket `updateinfo.xml.gz` | every advisory with its fixed package versions |
//! | AWS security bulletins RSS | the last 30 days of bulletins (titles and links only) |
//! | Bulletin page | its per-service status: who acts, and the fixed versions |
//! | Lambda runtimes page | each runtime's deprecation and block dates |
//! | SSM public parameters | AWS's latest ECS, EKS and Bottlerocket images |
//!
//! Every parser answers `Err` when the page no longer looks the way it did
//! (no items, no body): the caller keeps the last good copy and logs it,
//! so a format change never empties what teams are matched against.

use std::collections::BTreeSet;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const ALAS2023_RSS: &str = "https://alas.aws.amazon.com/AL2023/alas.rss";
pub const ALAS2_RSS: &str = "https://alas.aws.amazon.com/AL2/alas.rss";
pub const BOTTLEROCKET_UPDATEINFO: &str = "https://advisories.bottlerocket.aws/updateinfo.xml.gz";
pub const BULLETINS_RSS: &str = "https://aws.amazon.com/security/security-bulletins/rss/feed/";
pub const BULLETIN_PAGE: &str = "https://aws.amazon.com/security/security-bulletins/";
pub const LAMBDA_RUNTIMES: &str =
    "https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html";

/// The hosts the feeds are read from (the self-host docs list them).
pub const HOSTS: &[&str] = &[
    "alas.aws.amazon.com",
    "advisories.bottlerocket.aws",
    "aws.amazon.com",
    "docs.aws.amazon.com",
];

/// The feeds, by the name their status row carries.
pub const FEEDS: &[(&str, &str)] = &[
    ("alas2023", "Amazon Linux 2023 advisories"),
    ("alas2", "Amazon Linux 2 advisories"),
    ("bottlerocket", "Bottlerocket advisories"),
    ("bulletins", "AWS security bulletins"),
    ("lambda_runtimes", "Lambda runtime deprecations"),
    ("latest_images", "AWS latest AMI parameters"),
];

/// A feed whose read went wrong: why, in a line for the log and the page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn err<T>(m: impl Into<String>) -> Result<T, ParseError> {
    Err(ParseError(m.into()))
}

/// One feed's last read. A failed read keeps `ok_at` and `count` from the
/// last good one and only sets the error.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedStatus {
    pub feed: String,
    /// When it last parsed (RFC 3339), empty when never.
    #[serde(default)]
    pub ok_at: String,
    /// Items in the last good read.
    #[serde(default)]
    pub count: usize,
    /// The last error, cleared by the next good read.
    #[serde(default)]
    pub error: String,
    #[serde(default)]
    pub error_at: String,
}

impl FeedStatus {
    pub fn ok(mut self, count: usize, now: &str) -> FeedStatus {
        self.ok_at = now.to_string();
        self.count = count;
        self.error.clear();
        self.error_at.clear();
        self
    }

    pub fn failed(mut self, e: &str, now: &str) -> FeedStatus {
        self.error = crate::truncate_str(e, 300).to_string();
        self.error_at = now.to_string();
        self
    }

    /// Read in the last two days.
    pub fn fresh(&self, now: DateTime<Utc>) -> bool {
        DateTime::parse_from_rfc3339(&self.ok_at)
            .map(|t| (now - t.with_timezone(&Utc)).num_hours() < 48)
            .unwrap_or(false)
    }
}

// ── Text helpers ────────────────────────────────────────────────────────

fn re(p: &str) -> Regex {
    Regex::new(p).expect("a valid pattern")
}

/// HTML to plain text: tags dropped, entities decoded, spaces collapsed.
pub fn text_of(html: &str) -> String {
    let no_tags = re(r"(?s)<[^>]*>").replace_all(html, " ");
    let decoded = no_tags
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&rsquo;", "'")
        .replace("&lsquo;", "'")
        .replace("&ldquo;", "\"")
        .replace("&rdquo;", "\"");
    decoded.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn tag<'a>(xml: &'a str, name: &str) -> Option<&'a str> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let a = xml.find(&open)? + open.len();
    let b = xml[a..].find(&close)? + a;
    Some(&xml[a..b])
}

fn attr(xml: &str, name: &str) -> Option<String> {
    let r = Regex::new(&format!(r#"\b{name}="([^"]*)""#)).ok()?;
    r.captures(xml).map(|c| c[1].to_string())
}

fn cves_in(s: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for m in re(r"CVE-\d{4}-\d{4,}").find_iter(s) {
        let v = m.as_str().to_string();
        if !out.contains(&v) {
            out.push(v);
        }
    }
    out
}

/// An RFC 2822 date (RSS) as YYYY-MM-DD.
fn rss_day(s: &str) -> String {
    DateTime::parse_from_rfc2822(s.trim())
        .map(|d| d.with_timezone(&Utc).format("%Y-%m-%d").to_string())
        .or_else(|_| {
            // "Mon, 09 Nov 2026 23:27:00 GMT" parses above; tolerate a
            // missing weekday too.
            NaiveDateTime::parse_from_str(s.trim(), "%d %b %Y %H:%M:%S GMT")
                .map(|d| d.format("%Y-%m-%d").to_string())
        })
        .unwrap_or_default()
}

// ── Amazon Linux (ALAS) and Bottlerocket ────────────────────────────────

/// Which operating system an advisory is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Distro {
    #[default]
    Al2023,
    Al2,
    Bottlerocket,
}

impl Distro {
    pub fn feed(self) -> &'static str {
        match self {
            Distro::Al2023 => "alas2023",
            Distro::Al2 => "alas2",
            Distro::Bottlerocket => "bottlerocket",
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Distro::Al2023 => "Amazon Linux 2023",
            Distro::Al2 => "Amazon Linux 2",
            Distro::Bottlerocket => "Bottlerocket",
        }
    }
}

/// A package version an advisory ships.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pkg {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub release: String,
}

/// One Amazon Linux or Bottlerocket advisory.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Advisory {
    pub id: String,
    pub distro: Distro,
    /// critical, important, medium or low.
    pub severity: String,
    /// The affected package (`kernel`, `kernel6.12`, `openssl`).
    pub package: String,
    pub cves: Vec<String>,
    /// YYYY-MM-DD.
    pub released: String,
    pub link: String,
    /// AL2023: the first release with the fix (`2023.12.20261102`), from
    /// the advisory page. Empty until the page is read.
    #[serde(default)]
    pub releasever: String,
    /// The fixed package versions (one per name).
    #[serde(default)]
    pub fixed: Vec<Pkg>,
    /// The advisory page was read (AL2023 and AL2 only; Bottlerocket's feed
    /// carries everything).
    #[serde(default)]
    pub detailed: bool,
}

impl Advisory {
    /// A kernel advisory (the ones that decide an AMI).
    pub fn is_kernel(&self) -> bool {
        self.package.starts_with("kernel") && !self.package.starts_with("kernel-livepatch")
    }
    /// Worth reading the page for: kernel advisories and every important or
    /// critical one.
    pub fn wants_detail(&self) -> bool {
        self.distro != Distro::Bottlerocket
            && !self.detailed
            && (self.is_kernel() || matches!(self.severity.as_str(), "critical" | "important"))
    }
}

/// An ALAS RSS feed (`alas.rss`): every advisory in it.
pub fn parse_alas_rss(xml: &str, distro: Distro) -> Result<Vec<Advisory>, ParseError> {
    let title_re = re(r"^\s*(ALAS[0-9A-Z.\-]*-\d{4}-\d+)\s*\(([a-z]+)\):\s*(.+?)\s*$");
    let mut out = Vec::new();
    for item in xml.split("<item>").skip(1) {
        let item = item.split("</item>").next().unwrap_or("");
        let Some(title) = tag(item, "title") else {
            continue;
        };
        let title = text_of(title);
        let Some(c) = title_re.captures(&title) else {
            continue;
        };
        out.push(Advisory {
            id: c[1].to_string(),
            distro,
            severity: c[2].to_string(),
            package: c[3].to_string(),
            cves: cves_in(tag(item, "description").unwrap_or("")),
            released: rss_day(tag(item, "pubDate").unwrap_or("")),
            link: tag(item, "link").map(text_of).unwrap_or_default(),
            ..Default::default()
        });
    }
    if out.is_empty() {
        return err(format!(
            "{}: no advisories in the feed (its format may have changed)",
            distro.label()
        ));
    }
    Ok(out)
}

/// What an ALAS advisory page adds to its RSS row.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AlasPage {
    pub id: String,
    pub severity: String,
    pub released: String,
    pub cves: Vec<String>,
    pub releasever: String,
    pub fixed: Vec<Pkg>,
}

/// A package's full name (`kernel-6.1.189-236.400.amzn2023.x86_64`) as
/// name, version and release.
pub fn parse_nevra(s: &str) -> Option<Pkg> {
    let s = s.trim();
    let (rest, arch) = s.rsplit_once('.')?;
    if !matches!(arch, "x86_64" | "aarch64" | "noarch" | "i686" | "src") {
        return None;
    }
    let (rest, release) = rest.rsplit_once('-')?;
    let (name, version) = rest.rsplit_once('-')?;
    if name.is_empty() || !version.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    Some(Pkg {
        name: name.to_string(),
        version: version.to_string(),
        release: release.to_string(),
    })
}

/// An ALAS advisory page (`ALAS-2026-3365.html`).
pub fn parse_alas_page(html: &str) -> Result<AlasPage, ParseError> {
    let id = re(r"<h1[^>]*>\s*(ALAS[0-9A-Z.\-]*-\d{4}-\d+)\s*</h1>")
        .captures(html)
        .map(|c| c[1].to_string());
    let Some(id) = id else {
        return err("ALAS page: no advisory id (its format may have changed)");
    };
    let field = |label: &str| -> String {
        let r = Regex::new(&format!(r"(?s)<b>{label}:</b>(.*?)</(?:span|div)>")).expect("pattern");
        r.captures(html).map(|c| text_of(&c[1])).unwrap_or_default()
    };
    let severity = re(r"(?s)id='severity'[^>]*>(.*?)</div>")
        .captures(html)
        .map(|c| text_of(&c[1]).replace("Severity:", ""))
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let released = field("Advisory Released Date");
    let releasever = re(r"--releasever[ =](\d{4}\.\d+\.\d{8})")
        .captures(html)
        .map(|c| c[1].to_string())
        .unwrap_or_default();
    let mut fixed: Vec<Pkg> = Vec::new();
    if let Some(block) = html.split("id='new_packages'").nth(1) {
        let block = block.split("</div>").next().unwrap_or("");
        for line in text_of(&block.replace("<br />", "\n").replace("<br/>", "\n"))
            .split(' ')
            .filter(|w| w.contains('-'))
        {
            if let Some(p) = parse_nevra(line) {
                if !fixed.iter().any(|f| f.name == p.name) {
                    fixed.push(p);
                }
            }
        }
    }
    let refs = html
        .split("id='references'")
        .nth(1)
        .map(|r| r.split("</div>").next().unwrap_or(""))
        .unwrap_or("");
    Ok(AlasPage {
        id,
        severity: severity.split_whitespace().next().unwrap_or("").to_string(),
        released,
        cves: cves_in(refs),
        releasever,
        fixed,
    })
}

impl Advisory {
    /// Takes what the advisory's page says.
    pub fn apply_page(&mut self, p: &AlasPage) {
        if !p.releasever.is_empty() {
            self.releasever = p.releasever.clone();
        }
        if !p.fixed.is_empty() {
            self.fixed = p.fixed.clone();
        }
        for c in &p.cves {
            if !self.cves.contains(c) {
                self.cves.push(c.clone());
            }
        }
        self.detailed = true;
    }
}

/// Bottlerocket's `updateinfo.xml` (unzipped): every security advisory.
pub fn parse_bottlerocket(xml: &str) -> Result<Vec<Advisory>, ParseError> {
    let mut out = Vec::new();
    for u in xml.split("<update ").skip(1) {
        let head = u.split('>').next().unwrap_or("");
        if attr(head, "type")
            .as_deref()
            .is_some_and(|t| t != "security")
        {
            continue;
        }
        let body = u.split("</update>").next().unwrap_or("");
        let Some(id) = tag(body, "id").map(text_of) else {
            continue;
        };
        let title = tag(body, "title").map(text_of).unwrap_or_default();
        let issued = re(r#"<issued date="(\d{4}-\d{2}-\d{2})"#)
            .captures(body)
            .map(|c| c[1].to_string())
            .unwrap_or_default();
        let mut fixed: Vec<Pkg> = Vec::new();
        for p in body.split("<package ").skip(1) {
            let p = p.split("/>").next().unwrap_or("");
            let (Some(name), Some(version)) = (attr(p, "name"), attr(p, "version")) else {
                continue;
            };
            if !fixed.iter().any(|f| f.name == name) {
                fixed.push(Pkg {
                    name,
                    version,
                    release: attr(p, "release").unwrap_or_default(),
                });
            }
        }
        let package = title.split_whitespace().next().unwrap_or("").to_string();
        out.push(Advisory {
            id,
            distro: Distro::Bottlerocket,
            severity: tag(body, "severity")
                .map(|s| text_of(s).to_ascii_lowercase())
                .unwrap_or_default(),
            package,
            cves: cves_in(body),
            released: issued,
            link: "https://advisories.bottlerocket.aws/".into(),
            fixed,
            detailed: true,
            ..Default::default()
        });
    }
    if out.is_empty() {
        return err("Bottlerocket: no advisories in updateinfo.xml (its format may have changed)");
    }
    Ok(out)
}

// ── AWS security bulletins ──────────────────────────────────────────────

/// One bulletin in the RSS feed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BulletinRef {
    /// `2026-160-AWS`.
    pub id: String,
    pub title: String,
    /// The bulletin's page.
    pub url: String,
    /// YYYY-MM-DD.
    pub published: String,
}

/// The bulletin's page for an id (`2026-160-AWS`).
pub fn bulletin_url(id: &str) -> String {
    format!("{BULLETIN_PAGE}{}/", id.to_ascii_lowercase())
}

/// The bulletins RSS feed (the last 30 days).
pub fn parse_bulletin_rss(xml: &str) -> Result<Vec<BulletinRef>, ParseError> {
    let id_re = re(r"(?i)(\d{4}-\d{3,}-aws)");
    let mut out = Vec::new();
    for item in xml.split("<item>").skip(1) {
        let item = item.split("</item>").next().unwrap_or("");
        let link = tag(item, "link").map(text_of).unwrap_or_default();
        let Some(id) = id_re
            .captures(&link)
            .or_else(|| id_re.captures(item))
            .map(|c| c[1].to_ascii_uppercase())
        else {
            continue;
        };
        out.push(BulletinRef {
            url: bulletin_url(&id),
            id,
            title: tag(item, "title").map(text_of).unwrap_or_default(),
            published: rss_day(tag(item, "pubDate").unwrap_or("")),
        });
    }
    if out.is_empty() && xml.contains("<item>") {
        return err(
            "AWS bulletins: items without bulletin ids (the feed's format may have changed)",
        );
    }
    if !xml.contains("<channel>") {
        return err("AWS bulletins: not an RSS feed");
    }
    Ok(out)
}

/// Who has to act for one service, as the bulletin says.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Who {
    /// The customer changes what runs (a new AMI, a kernel update, a node
    /// group version).
    Customer,
    /// The customer applies something AWS made available, in AWS (pending
    /// maintenance, a service update).
    CustomerInAws,
    /// AWS patched it, or patches it on its own schedule: nothing to do.
    Aws,
    /// AWS says the service is not affected.
    NotAffected,
    /// The page doesn't say in words this reader knows.
    #[default]
    Unknown,
}

impl Who {
    /// The team has something to do.
    pub fn acts(self) -> bool {
        matches!(self, Who::Customer | Who::CustomerInAws)
    }
}

/// The fixed versions a bulletin names for a service.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixedIn {
    /// The first Amazon Linux 2023 release with the fix (`2023.12.20261102`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub al2023_release: String,
    /// EKS optimized AMI release (`v20261031`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub eks_ami: String,
    /// ECS optimized AMI release (`20261031`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ecs_ami: String,
    /// Bottlerocket OS version (`1.68.1`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub bottlerocket: String,
    /// Named self-service updates (`elasticache-20261106-intel`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub service_updates: Vec<String>,
    /// What starts after this moment (RFC 3339, UTC) runs patched.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub patched_after: String,
    /// Older tasks, pods or instances are replaced within this many days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retire_days: Option<u32>,
}

/// One service's row in a bulletin.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceStatus {
    /// What CoderHelm matches resources by (`ec2`, `ecs_fargate`, `rds`, ...).
    pub service: String,
    /// The bulletin's own heading for it.
    pub heading: String,
    pub who: Who,
    /// The bulletin's words for this service, shortened.
    pub reason: String,
    #[serde(default)]
    pub fixed: FixedIn,
}

/// One bulletin, read from its page.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bulletin {
    pub id: String,
    pub title: String,
    pub url: String,
    /// `important` or `informational`.
    pub severity: String,
    /// YYYY-MM-DD.
    pub published: String,
    pub updated: String,
    pub cves: Vec<String>,
    /// Amazon Linux advisories it names.
    pub advisories: Vec<String>,
    pub summary: String,
    /// The per-service status, in page order. Empty for a bulletin about
    /// one product (an SDK, a tool) with no per-service table.
    pub services: Vec<ServiceStatus>,
}

impl Bulletin {
    /// Every row for `service`.
    pub fn rows(&self, service: &str) -> impl Iterator<Item = &ServiceStatus> {
        let service = service.to_string();
        self.services.iter().filter(move |s| s.service == service)
    }
    /// What the bulletin says for `service`, when it names it.
    pub fn status(&self, service: &str) -> Option<&ServiceStatus> {
        self.rows(service).next()
    }
}

/// The services CoderHelm knows by name: its key, the words that name it in
/// a heading, and the words that name it inside a paragraph of a heading
/// that covers several (`Amazon EKS` covers the control plane, nodes, Auto
/// Mode and Fargate). In-AWS services are applied by the customer in AWS.
struct Known {
    key: &'static str,
    heading: &'static [&'static str],
    paragraph: &'static [&'static str],
    in_aws: bool,
}

const KNOWN: &[Known] = &[
    Known {
        key: "amazon_linux",
        heading: &["amazon linux"],
        paragraph: &[],
        in_aws: false,
    },
    Known {
        key: "ec2",
        heading: &["amazon ec2"],
        paragraph: &[],
        in_aws: false,
    },
    Known {
        key: "eks_control_plane",
        heading: &[],
        paragraph: &["control plane"],
        in_aws: false,
    },
    Known {
        key: "eks_nodes",
        heading: &[],
        paragraph: &["managed node groups", "self-managed nodes", "karpenter"],
        in_aws: false,
    },
    Known {
        key: "eks_auto_mode",
        heading: &[],
        paragraph: &["auto mode"],
        in_aws: false,
    },
    Known {
        key: "eks_fargate",
        heading: &[],
        paragraph: &["eks fargate"],
        in_aws: false,
    },
    Known {
        key: "ecs_ec2",
        heading: &[],
        paragraph: &["ecs on ec2"],
        in_aws: false,
    },
    Known {
        key: "ecs_fargate",
        heading: &[],
        paragraph: &["fargate:"],
        in_aws: false,
    },
    Known {
        key: "ecs_managed_instances",
        heading: &[],
        paragraph: &["ecs managed instances"],
        in_aws: false,
    },
    Known {
        key: "bottlerocket",
        heading: &["bottlerocket"],
        paragraph: &[],
        in_aws: false,
    },
    Known {
        key: "lambda",
        heading: &["aws lambda", "amazon lambda"],
        paragraph: &[],
        in_aws: false,
    },
    Known {
        key: "rds",
        heading: &["rds", "aurora"],
        paragraph: &[],
        in_aws: true,
    },
    Known {
        key: "elasticache",
        heading: &["elasticache"],
        paragraph: &[],
        in_aws: true,
    },
    Known {
        key: "memorydb",
        heading: &["memorydb"],
        paragraph: &[],
        in_aws: true,
    },
    Known {
        key: "opensearch",
        heading: &["opensearch"],
        paragraph: &[],
        in_aws: true,
    },
    Known {
        key: "neptune",
        heading: &["neptune"],
        paragraph: &[],
        in_aws: true,
    },
    Known {
        key: "dms",
        heading: &["database migration service", "aws dms"],
        paragraph: &[],
        in_aws: true,
    },
    Known {
        key: "mq",
        heading: &["amazon mq"],
        paragraph: &[],
        in_aws: true,
    },
    Known {
        key: "storage_gateway",
        heading: &["storage gateway"],
        paragraph: &[],
        in_aws: true,
    },
    Known {
        key: "redshift",
        heading: &["redshift"],
        paragraph: &[],
        in_aws: true,
    },
    Known {
        key: "elb",
        heading: &["load balancer"],
        paragraph: &[],
        in_aws: false,
    },
    Known {
        key: "cloudfront",
        heading: &["cloudfront"],
        paragraph: &[],
        in_aws: false,
    },
    Known {
        key: "api_gateway",
        heading: &["api gateway"],
        paragraph: &[],
        in_aws: false,
    },
    Known {
        key: "waf",
        heading: &["web application firewall"],
        paragraph: &[],
        in_aws: false,
    },
    Known {
        key: "global_accelerator",
        heading: &["global accelerator"],
        paragraph: &[],
        in_aws: false,
    },
    Known {
        key: "beanstalk",
        heading: &["elastic beanstalk"],
        paragraph: &[],
        in_aws: false,
    },
    Known {
        key: "lightsail",
        heading: &["lightsail"],
        paragraph: &[],
        in_aws: false,
    },
];

/// The services a heading names that split by paragraph.
fn splits(heading: &str) -> Option<(&'static str, &'static [&'static str])> {
    let h = heading.to_ascii_lowercase();
    if h.contains("eks") {
        Some((
            "eks_nodes",
            &[
                "eks_control_plane",
                "eks_nodes",
                "eks_auto_mode",
                "eks_fargate",
            ],
        ))
    } else if h.contains("ecs") {
        Some((
            "ecs_ec2",
            &["ecs_ec2", "ecs_fargate", "ecs_managed_instances"],
        ))
    } else {
        None
    }
}

fn known(key: &str) -> Option<&'static Known> {
    KNOWN.iter().find(|k| k.key == key)
}

fn slug(h: &str) -> String {
    let mut s: String = h
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    while s.contains("__") {
        s = s.replace("__", "_");
    }
    format!("other:{}", s.trim_matches('_'))
}

/// How a paragraph reads: the strongest statement wins (not affected, then
/// AWS-patched, then a customer action).
fn classify(p: &str) -> Option<Who> {
    let l = p.to_ascii_lowercase();
    let not_affected = ["not affected", "are not vulnerable", "is not vulnerable"];
    let aws = [
        "no customer action is required",
        "no immediate customer action is required",
        "no action is required",
        "no customer action",
        "have been mitigated",
        "has been mitigated",
        "has been updated to address",
        "have been updated to address",
        "run on the patched",
        "use the patched",
        "automatically replaces",
        "are retired",
        "are drained",
        "aws has completed updates",
    ];
    let customer = [
        "should update",
        "should apply",
        "customers should",
        "you should",
        "are responsible for updating",
        "is responsible for updating",
        "remediation will be for you",
        "update the kernel",
        "update the platform version",
        "should create a new",
        "has been released",
        "have been released",
        "were released",
        "apply it now",
    ];
    if not_affected.iter().any(|w| l.contains(w)) {
        return Some(Who::NotAffected);
    }
    if aws.iter().any(|w| l.contains(w)) {
        return Some(Who::Aws);
    }
    if customer.iter().any(|w| l.contains(w)) {
        return Some(Who::Customer);
    }
    None
}

fn utc_after(text: &str) -> String {
    // "(November 1, 2026, 5:00 PM UTC)"
    let r = re(r"([A-Z][a-z]+ \d{1,2}, \d{4}), (\d{1,2}:\d{2} [AP]M) UTC");
    r.captures(text)
        .and_then(|c| {
            NaiveDateTime::parse_from_str(&format!("{} {}", &c[1], &c[2]), "%B %d, %Y %I:%M %p")
                .ok()
        })
        .map(|d| d.and_utc().to_rfc3339())
        .unwrap_or_default()
}

fn fixed_in(text: &str) -> FixedIn {
    let first = |p: &str| {
        re(p)
            .captures(text)
            .map(|c| c[1].to_string())
            .unwrap_or_default()
    };
    let mut updates: Vec<String> = Vec::new();
    for m in re(r"\b((?:elasticache|memorydb)-\d{8}-[a-z0-9]+)\b").find_iter(text) {
        if !updates.contains(&m.as_str().to_string()) {
            updates.push(m.as_str().to_string());
        }
    }
    let retire = re(r"(?:within|after) (\d{1,3}) days")
        .captures(text)
        .and_then(|c| c[1].parse().ok());
    FixedIn {
        al2023_release: first(r"(2023\.\d+\.\d{8})"),
        eks_ami: first(r"\b(v\d{8})\b"),
        ecs_ami: first(r"ECS-optimized AMI \(release (\d{8})\)"),
        bottlerocket: first(r"(?i)bottlerocket v(\d+\.\d+\.\d+)"),
        service_updates: updates,
        patched_after: utc_after(text),
        retire_days: retire,
    }
}

/// The body of a bulletin page: the rich-text field of the page's data
/// (where the AWS site keeps it today), or the page itself.
fn bulletin_body(page: &str) -> Option<String> {
    if let Some(i) = page.find("\"itemLongLoc\":") {
        let rest = page[i + "\"itemLongLoc\":".len()..].trim_start();
        let mut de = serde_json::Deserializer::from_str(rest);
        if let Ok(s) = <String as serde::Deserialize>::deserialize(&mut de) {
            if s.contains("Bulletin ID") {
                return Some(s);
            }
        }
    }
    page.contains("Bulletin ID").then(|| page.to_string())
}

fn us_day(s: &str) -> String {
    re(r"(\d{1,2})/(\d{1,2})/(\d{4})")
        .captures(s)
        .and_then(|c| {
            NaiveDate::from_ymd_opt(c[3].parse().ok()?, c[1].parse().ok()?, c[2].parse().ok()?)
        })
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

/// A bulletin page: its header, CVEs, and per-service status.
pub fn parse_bulletin(page: &str, url: &str) -> Result<Bulletin, ParseError> {
    let Some(body) = bulletin_body(page) else {
        return err("AWS bulletin: no bulletin body on the page (its format may have changed)");
    };
    let header = |label: &str| -> String {
        let r = Regex::new(&format!(r"(?s)<b>{label}:</b>(.*?)(?:<br|</p>)")).expect("pattern");
        r.captures(&body)
            .map(|c| text_of(&c[1]))
            .unwrap_or_default()
    };
    let id = header("Bulletin ID").to_ascii_uppercase();
    if !re(r"^\d{4}-\d{3,}-AWS$").is_match(&id) {
        return err(format!("AWS bulletin: no bulletin id on {url}"));
    }
    let content = header("Content Type").to_ascii_lowercase();
    let severity = if content.starts_with("important") {
        "important"
    } else {
        "informational"
    };
    let title = re(r"<title>(.*?)</title>")
        .captures(page)
        .map(|c| text_of(&c[1]))
        .unwrap_or_default();
    let mut advisories: Vec<String> = Vec::new();
    for m in re(r"\bALAS[0-9A-Z.\-]*-\d{4}-\d+\b").find_iter(&text_of(&body)) {
        if !advisories.contains(&m.as_str().to_string()) {
            advisories.push(m.as_str().to_string());
        }
    }
    let summary = body
        .split("Summary:")
        .nth(1)
        .and_then(|s| re(r"(?s)<p>(.*?)</p>").captures(s).map(|c| text_of(&c[1])))
        .unwrap_or_default();

    // Sections: a paragraph that is only a bold heading, then its paragraphs.
    let status_part = body
        .split("Mitigation Status")
        .nth(1)
        .or_else(|| body.split("Affected").nth(1))
        .unwrap_or("");
    let block_re = re(r"(?s)<p>(.*?)</p>");
    let heading_re = re(r"(?s)^\s*<b>([^<]{2,200})</b>\s*(?:<br>\s*)?$");
    let mut sections: Vec<(String, Vec<String>)> = Vec::new();
    for c in block_re.captures_iter(status_part) {
        let inner = &c[1];
        if let Some(h) = heading_re.captures(inner) {
            sections.push((text_of(&h[1]), Vec::new()));
        } else if let Some(last) = sections.last_mut() {
            let t = text_of(inner);
            if !t.is_empty() {
                last.1.push(t);
            }
        }
    }
    let mut services: Vec<ServiceStatus> = Vec::new();
    for (heading, paras) in &sections {
        let hl = heading.to_ascii_lowercase();
        if hl.contains("workarounds") || hl.contains("interim") || hl.contains("references") {
            continue;
        }
        let all = paras.join(" ");
        // (key, paragraphs that speak of it)
        let mut subjects: Vec<(String, Vec<&String>)> = Vec::new();
        fn add<'p>(subjects: &mut Vec<(String, Vec<&'p String>)>, key: String, p: &'p String) {
            match subjects.iter_mut().find(|(k, _)| *k == key) {
                Some((_, v)) => v.push(p),
                None => subjects.push((key, vec![p])),
            }
        }
        if let Some((default, keys)) = splits(heading) {
            let mut current = default.to_string();
            for p in paras {
                let pl = p.to_ascii_lowercase();
                let named: Vec<&str> = keys
                    .iter()
                    .copied()
                    .filter(|k| {
                        known(k).is_some_and(|kn| kn.paragraph.iter().any(|w| pl.contains(w)))
                    })
                    .collect();
                // The control plane line may also name nodes; the first
                // named subject a paragraph opens with is the one it's about.
                let pick = named
                    .iter()
                    .min_by_key(|k| {
                        known(k)
                            .unwrap()
                            .paragraph
                            .iter()
                            .filter_map(|w| pl.find(w))
                            .min()
                            .unwrap_or(usize::MAX)
                    })
                    .copied();
                if let Some(k) = pick {
                    current = k.to_string();
                }
                add(&mut subjects, current.clone(), p);
            }
        } else {
            let mut keys: Vec<String> = KNOWN
                .iter()
                .filter(|k| k.heading.iter().any(|w| hl.contains(w)))
                .map(|k| k.key.to_string())
                .collect();
            // "Amazon Linux" also matches nothing else; "RDS/Aurora" is one key.
            keys.dedup();
            if keys.is_empty() {
                keys.push(slug(heading));
            }
            for k in keys {
                for p in paras {
                    add(&mut subjects, k.clone(), p);
                }
            }
        }
        for (key, ps) in subjects {
            let text = ps.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(" ");
            let mut who = Who::Unknown;
            for p in &ps {
                match classify(p) {
                    Some(Who::NotAffected) => who = Who::NotAffected,
                    Some(Who::Aws) if who != Who::NotAffected => who = Who::Aws,
                    Some(Who::Customer) if who == Who::Unknown => who = Who::Customer,
                    _ => {}
                }
            }
            if who == Who::Customer && known(&key).is_some_and(|k| k.in_aws) {
                who = Who::CustomerInAws;
            }
            let mut fixed = fixed_in(&text);
            // A section-wide AMI release (stated once for several nodes).
            if fixed.al2023_release.is_empty() {
                fixed.al2023_release = fixed_in(&all).al2023_release;
            }
            services.push(ServiceStatus {
                service: key,
                heading: heading.clone(),
                who,
                reason: crate::truncate_str(&text, 600).to_string(),
                fixed,
            });
        }
    }
    // The CVEs the bulletin is about: those its title and summary name,
    // above the per-service status. A CVE named only inside one service's
    // words belongs to that service's row (`actions::tie` reads it there)
    // and is not put on every finding the bulletin touches. A page with
    // no CVE above the status falls back to every CVE on it.
    let head = ["Mitigation Status", "Affected"]
        .iter()
        .find_map(|m| body.split_once(m).map(|(h, _)| h))
        .unwrap_or(&body);
    let mut cves = cves_in(&format!("{title} {}", text_of(head)));
    if cves.is_empty() {
        cves = cves_in(&text_of(&body));
    }
    Ok(Bulletin {
        id,
        title,
        url: url.to_string(),
        severity: severity.into(),
        published: us_day(&header("Publication Date")),
        updated: us_day(&header("Last Updated Date")),
        cves,
        advisories,
        summary: crate::truncate_str(&summary, 800).to_string(),
        services,
    })
}

// ── Lambda runtimes ─────────────────────────────────────────────────────

/// One Lambda runtime and its dates (YYYY-MM-DD, empty when not scheduled).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Runtime {
    pub name: String,
    /// `python3.12`.
    pub id: String,
    pub os: String,
    pub deprecation: String,
    pub block_create: String,
    pub block_update: String,
    /// Listed under deprecated runtimes.
    pub deprecated: bool,
}

impl Runtime {
    /// Deprecated at `today` (YYYY-MM-DD): AWS no longer patches it.
    pub fn deprecated_at(&self, today: &str) -> bool {
        self.deprecated || (!self.deprecation.is_empty() && self.deprecation.as_str() <= today)
    }
}

fn doc_day(s: &str) -> String {
    let t = s.trim();
    NaiveDate::parse_from_str(t, "%b %d, %Y")
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

/// The Lambda runtimes page: the supported and deprecated tables.
pub fn parse_lambda_runtimes(html: &str) -> Result<Vec<Runtime>, ParseError> {
    let table_re = re(r"(?s)<table[^>]*>(.*?)</table>");
    let row_re = re(r"(?s)<tr>(.*?)</tr>");
    let cell_re = re(r"(?s)<td[^>]*>(.*?)</td>");
    let mut out: Vec<Runtime> = Vec::new();
    let mut runtime_tables = 0;
    for t in table_re.captures_iter(html) {
        let t = &t[1];
        if !(t.contains("Identifier") && t.contains("Deprecation date")) {
            continue;
        }
        runtime_tables += 1;
        // The first such table lists supported runtimes, the next deprecated.
        let deprecated = runtime_tables > 1;
        for r in row_re.captures_iter(t) {
            let cells: Vec<String> = cell_re
                .captures_iter(&r[1])
                .map(|c| text_of(&c[1]))
                .collect();
            if cells.len() < 6 || cells[1].is_empty() {
                continue;
            }
            out.push(Runtime {
                name: cells[0].clone(),
                id: cells[1].clone(),
                os: cells[2].clone(),
                deprecation: doc_day(&cells[3]),
                block_create: doc_day(&cells[4]),
                block_update: doc_day(&cells[5]),
                deprecated,
            });
        }
    }
    if out.is_empty() {
        return err("Lambda runtimes: no runtime table on the page (its format may have changed)");
    }
    Ok(out)
}

// ── AWS's latest images (public SSM parameters) ─────────────────────────

/// The public parameters read for the latest images, in us-east-1 (release
/// versions are the same in every region; AMI IDs are read per account in
/// its own region).
pub const LATEST_PARAMETERS: &[&str] = &[
    "/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended",
    "/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended",
    "/aws/service/ecs/optimized-ami/amazon-linux-2/recommended",
    "/aws/service/eks/optimized-ami/1.32/amazon-linux-2023/x86_64/standard/recommended",
    "/aws/service/eks/optimized-ami/1.33/amazon-linux-2023/x86_64/standard/recommended",
    "/aws/service/eks/optimized-ami/1.34/amazon-linux-2023/x86_64/standard/recommended",
    "/aws/service/eks/optimized-ami/1.35/amazon-linux-2023/x86_64/standard/recommended",
    "/aws/service/bottlerocket/aws-k8s-1.34/x86_64/latest/image_version",
    "/aws/service/bottlerocket/aws-ecs-2/x86_64/latest/image_version",
];

/// The AL2023 parameters an AMI-ID fix points Terraform at.
pub const AL2023_PARAMETERS: &[(&str, &str)] = &[
    (
        "x86_64",
        "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64",
    ),
    (
        "arm64",
        "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64",
    ),
];

/// AWS's latest image for one parameter.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatestImage {
    pub parameter: String,
    /// `ecs`, `eks` or `bottlerocket`.
    pub kind: String,
    pub image_id: String,
    pub image_name: String,
    /// `20261031` (ECS), `1.34.0-20261031` (EKS), `1.68.1` (Bottlerocket).
    pub release: String,
}

/// One parameter's value as AWS returns it.
pub fn parse_latest(parameter: &str, value: &str) -> Option<LatestImage> {
    let kind = if parameter.contains("/ecs/") {
        "ecs"
    } else if parameter.contains("/eks/") {
        "eks"
    } else if parameter.contains("/bottlerocket/") {
        "bottlerocket"
    } else {
        return None;
    };
    if kind == "bottlerocket" {
        let v = value.trim().trim_start_matches('v');
        return (!v.is_empty()).then(|| LatestImage {
            parameter: parameter.into(),
            kind: kind.into(),
            release: v.split('-').next().unwrap_or(v).to_string(),
            ..Default::default()
        });
    }
    let j: Value = serde_json::from_str(value).ok()?;
    let name = j["image_name"].as_str().unwrap_or("").to_string();
    let release = match kind {
        "eks" => j["release_version"].as_str().unwrap_or("").to_string(),
        _ => re(r"(\d{8})")
            .captures(&name)
            .map(|c| c[1].to_string())
            .unwrap_or_default(),
    };
    Some(LatestImage {
        parameter: parameter.into(),
        kind: kind.into(),
        image_id: j["image_id"].as_str().unwrap_or("").to_string(),
        image_name: name,
        release,
    })
}

/// The newest EKS AMI release among the latest images (`v20261031`).
pub fn newest_eks_release(images: &[LatestImage]) -> Option<String> {
    images
        .iter()
        .filter(|i| i.kind == "eks")
        .filter_map(|i| i.release.rsplit('-').next().map(|d| format!("v{d}")))
        .max()
}

/// Ids in `ids` not already in `known` (what's new since the last read).
pub fn new_ids<'a>(ids: impl Iterator<Item = &'a str>, known: &BTreeSet<String>) -> Vec<String> {
    ids.filter(|i| !known.contains(*i))
        .map(String::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const AL2023_RSS: &str = include_str!("testdata/alas2023-rss.xml");
    const AL2_RSS: &str = include_str!("testdata/alas2-rss.xml");
    const ALAS_PAGE: &str = include_str!("testdata/alas2023-2026-3365.html");
    const BR: &str = include_str!("testdata/bottlerocket-updateinfo.xml");
    const RSS: &str = include_str!("testdata/bulletins-rss.xml");
    const BULLETIN: &str = include_str!("testdata/bulletin-2026-160-aws.html");
    const RUNTIMES: &str = include_str!("testdata/lambda-runtimes.html");

    #[test]
    fn alas_rss_reads_every_advisory() {
        let a = parse_alas_rss(AL2023_RSS, Distro::Al2023).unwrap();
        assert!(a.len() >= 20, "{}", a.len());
        let k = a
            .iter()
            .find(|x| x.id == "ALAS2023-2026-3365")
            .expect("the kernel advisory");
        assert_eq!(k.severity, "important");
        assert_eq!(k.package, "kernel");
        assert!(k.cves.contains(&"CVE-2026-98432".to_string()));
        assert!(k.released.starts_with("2026-1"), "{}", k.released);
        assert!(k.link.contains("2026-3365"), "{}", k.link);
        assert!(k.is_kernel() && k.wants_detail());
        let lp = a.iter().find(|x| x.package.starts_with("kernel-livepatch"));
        if let Some(lp) = lp {
            assert!(!lp.is_kernel());
        }
        let two = parse_alas_rss(AL2_RSS, Distro::Al2).unwrap();
        assert!(two.iter().any(|x| x.id == "ALAS2-2026-4065"));
        assert!(two.iter().all(|x| x.distro == Distro::Al2));
    }

    #[test]
    fn a_feed_that_changed_shape_is_an_error() {
        assert!(parse_alas_rss("<rss><channel></channel></rss>", Distro::Al2023).is_err());
        assert!(parse_alas_rss("<html>Service Unavailable</html>", Distro::Al2).is_err());
        assert!(parse_bottlerocket("<updates></updates>").is_err());
        assert!(parse_bulletin("<html>Access denied</html>", "u").is_err());
        assert!(parse_lambda_runtimes("<html><table><tr><td>x</td></tr></table></html>").is_err());
        assert!(parse_bulletin_rss("<html>nope</html>").is_err());
        assert!(parse_alas_page("<html></html>").is_err());
    }

    #[test]
    fn the_alas_page_gives_the_fixed_release_and_packages() {
        let p = parse_alas_page(ALAS_PAGE).unwrap();
        assert_eq!(p.id, "ALAS2023-2026-3365");
        assert_eq!(p.severity, "important");
        assert_eq!(p.released, "2026-10-30");
        assert_eq!(p.releasever, "2023.12.20261102");
        assert_eq!(p.cves, vec!["CVE-2026-98432"]);
        let k = p.fixed.iter().find(|x| x.name == "kernel").expect("kernel");
        assert_eq!(k.version, "6.1.189");
        assert_eq!(k.release, "236.400.amzn2023");
        let mut a = Advisory {
            id: p.id.clone(),
            package: "kernel".into(),
            ..Default::default()
        };
        a.apply_page(&p);
        assert!(a.detailed && !a.wants_detail());
        assert_eq!(a.releasever, "2023.12.20261102");
    }

    #[test]
    fn nevra_splits_from_the_right() {
        let p = parse_nevra("kernel-livepatch-6.1.189-236.400-1.0-0.amzn2023.x86_64").unwrap();
        assert_eq!(p.name, "kernel-livepatch-6.1.189-236.400");
        assert_eq!(p.version, "1.0");
        assert_eq!(
            parse_nevra("python3-perf-6.1.189-236.400.amzn2023.aarch64")
                .unwrap()
                .name,
            "python3-perf"
        );
        assert!(parse_nevra("not-a-package").is_none());
        assert!(parse_nevra("x86_64:").is_none());
    }

    #[test]
    fn bottlerocket_advisories_carry_their_fixed_kernels() {
        let a = parse_bottlerocket(BR).unwrap();
        let k = a
            .iter()
            .find(|x| x.id == "BRSA-quiyere1yzbn")
            .expect("the kernel advisory");
        assert_eq!(k.severity, "important");
        assert_eq!(k.released, "2026-10-26");
        assert_eq!(k.cves, vec!["CVE-2026-98432"]);
        assert_eq!(k.package, "kernel");
        let names: Vec<&str> = k.fixed.iter().map(|p| p.name.as_str()).collect();
        assert!(
            names.contains(&"kernel-6.12") && names.contains(&"kernel-6.18"),
            "{names:?}"
        );
        assert_eq!(
            k.fixed
                .iter()
                .find(|p| p.name == "kernel-6.12")
                .unwrap()
                .version,
            "6.12.113"
        );
        assert!(!k.wants_detail(), "the feed carries everything");
    }

    #[test]
    fn the_bulletin_feed_lists_ids_and_page_links() {
        let b = parse_bulletin_rss(RSS).unwrap();
        let k = b
            .iter()
            .find(|x| x.id == "2026-160-AWS")
            .expect("2026-160-AWS");
        assert_eq!(
            k.url,
            "https://aws.amazon.com/security/security-bulletins/2026-160-aws/"
        );
        assert!(k.title.contains("CVE-2026-98432"));
        assert_eq!(k.published, "2026-10-31");
        assert!(parse_bulletin_rss("<rss><channel></channel></rss>")
            .unwrap()
            .is_empty());
    }

    fn who(b: &Bulletin, s: &str) -> Who {
        b.status(s)
            .unwrap_or_else(|| {
                panic!(
                    "{s} not in {:?}",
                    b.services.iter().map(|x| &x.service).collect::<Vec<_>>()
                )
            })
            .who
    }

    #[test]
    fn the_bulletin_page_says_who_acts_per_service() {
        let b = parse_bulletin(BULLETIN, &bulletin_url("2026-160-AWS")).unwrap();
        assert_eq!(b.id, "2026-160-AWS");
        assert_eq!(b.severity, "important");
        assert_eq!(b.published, "2026-10-30");
        assert_eq!(b.updated, "2026-11-10");
        assert_eq!(b.cves, vec!["CVE-2026-98432"]);
        assert!(b.advisories.contains(&"ALAS2023-2026-3365".to_string()));
        assert!(b.advisories.contains(&"ALAS2-2026-4065".to_string()));
        assert!(b.summary.contains("IPv4 TCP"));
        // The customer changes what runs.
        for s in [
            "amazon_linux",
            "ec2",
            "eks_nodes",
            "ecs_ec2",
            "bottlerocket",
        ] {
            assert_eq!(who(&b, s), Who::Customer, "{s}");
        }
        // The customer applies it in AWS.
        for s in ["rds", "elasticache", "memorydb"] {
            assert_eq!(who(&b, s), Who::CustomerInAws, "{s}");
        }
        // AWS's to patch.
        for s in [
            "eks_control_plane",
            "eks_auto_mode",
            "eks_fargate",
            "ecs_fargate",
            "ecs_managed_instances",
            "elb",
            "cloudfront",
            "api_gateway",
        ] {
            assert_eq!(who(&b, s), Who::Aws, "{s}");
        }
        assert_eq!(who(&b, "lambda"), Who::NotAffected);
        assert_eq!(who(&b, "global_accelerator"), Who::NotAffected);
        // The fixed versions it names.
        assert_eq!(
            b.status("amazon_linux").unwrap().fixed.al2023_release,
            "2023.12.20261102"
        );
        assert_eq!(b.status("eks_nodes").unwrap().fixed.eks_ami, "v20261031");
        assert_eq!(b.status("ecs_ec2").unwrap().fixed.ecs_ami, "20261031");
        assert_eq!(
            b.status("bottlerocket").unwrap().fixed.bottlerocket,
            "1.68.1"
        );
        assert_eq!(
            b.status("elasticache").unwrap().fixed.service_updates,
            vec!["elasticache-20261106-arm", "elasticache-20261106-intel"]
        );
        let f = &b.status("ecs_fargate").unwrap().fixed;
        assert_eq!(f.patched_after, "2026-11-01T17:00:00+00:00");
        let ef = &b.status("eks_fargate").unwrap().fixed;
        assert_eq!(ef.retire_days, Some(14));
        assert!(b.status("lambda").unwrap().reason.contains("not affected"));
        // Unknown headings keep their own key and words.
        assert!(b.services.iter().any(|s| s.service.starts_with("other:")));
        // Interim steps are not a service.
        assert!(!b
            .services
            .iter()
            .any(|s| s.heading.to_ascii_lowercase().contains("workaround")));
    }

    #[test]
    fn a_cve_named_only_in_one_services_words_is_not_the_bulletins() {
        let b = parse_bulletin(BULLETIN, "u").unwrap();
        assert_eq!(b.cves, vec!["CVE-2026-98432"]);
        // The same page with another CVE named only under ElastiCache.
        let page = BULLETIN.replacen(
            "ElastiCache clusters named either",
            "ElastiCache clusters (it also addresses CVE-2026-11112) named either",
            1,
        );
        assert_ne!(
            page, BULLETIN,
            "the fixture still has the ElastiCache sentence"
        );
        let b = parse_bulletin(&page, "u").unwrap();
        assert_eq!(b.cves, vec!["CVE-2026-98432"], "not put on every service");
        assert!(b
            .status("elasticache")
            .unwrap()
            .reason
            .contains("CVE-2026-11112"));
    }

    #[test]
    fn a_bulletin_without_a_service_table_has_no_rows() {
        let body = r#"<p><b>Bulletin ID:</b> 2026-175-AWS<br><b>Scope:</b> AWS<br><b>Content Type:</b> Informational<br><b>Publication Date:</b> 11/09/2026 10:00 AM PST</p><h3>Summary</h3><p>An SDK issue (CVE-2026-115419).</p>"#;
        let page = format!(
            r#"<title>t</title><script>{{"itemLongLoc":{}}}</script>"#,
            serde_json::to_string(body).unwrap()
        );
        let b = parse_bulletin(&page, "u").unwrap();
        assert_eq!(b.id, "2026-175-AWS");
        assert_eq!(b.severity, "informational");
        assert!(b.services.is_empty());
        assert_eq!(b.cves, vec!["CVE-2026-115419"]);
    }

    #[test]
    fn lambda_runtimes_read_both_tables() {
        let r = parse_lambda_runtimes(RUNTIMES).unwrap();
        let p312 = r.iter().find(|x| x.id == "python3.12").expect("python3.12");
        assert!(!p312.deprecated);
        assert_eq!(p312.deprecation, "2028-10-31");
        assert!(!p312.deprecated_at("2026-11-10"));
        let old = r
            .iter()
            .find(|x| x.deprecated)
            .expect("a deprecated runtime");
        assert!(old.deprecated_at("2026-11-10"));
        let n26 = r.iter().find(|x| x.id == "nodejs26.x").expect("nodejs26.x");
        assert!(n26.deprecation.is_empty(), "not scheduled");
    }

    #[test]
    fn latest_images_read_their_release() {
        let ecs = parse_latest(
            "/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended",
            r#"{"ecs_agent_version":"1.99.0","ecs_runtime_version":"Docker version 25.0.8","image_id":"ami-0aaa1111bbbb2222c","image_name":"al2023-ami-ecs-hvm-2023.0.20261031-kernel-6.1-x86_64","image_version":"2023.0.20261031","os":"Amazon Linux 2023","schema_version":1,"source_image_name":"al2023-ami-minimal-2023.12.20261102.0-kernel-6.1-x86_64"}"#,
        )
        .unwrap();
        assert_eq!(ecs.release, "20261031");
        assert_eq!(ecs.image_id, "ami-0aaa1111bbbb2222c");
        let eks = parse_latest(
            "/aws/service/eks/optimized-ami/1.34/amazon-linux-2023/x86_64/standard/recommended",
            r#"{"image_id":"ami-0123","image_name":"amazon-eks-node-al2023-x86_64-standard-1.34-v20261031","release_version":"1.34.2-20261031"}"#,
        )
        .unwrap();
        assert_eq!(eks.release, "1.34.2-20261031");
        let br = parse_latest(
            "/aws/service/bottlerocket/aws-k8s-1.34/x86_64/latest/image_version",
            "1.68.1-abcdef12",
        )
        .unwrap();
        assert_eq!(br.release, "1.68.1");
        assert_eq!(
            newest_eks_release(&[ecs, eks, br]).as_deref(),
            Some("v20261031")
        );
        assert!(parse_latest("/aws/service/other", "x").is_none());
    }

    #[test]
    fn a_failed_read_keeps_the_last_good_one() {
        let s = FeedStatus {
            feed: "alas2023".into(),
            ..Default::default()
        }
        .ok(3135, "2026-11-10T06:00:00Z");
        let f = s
            .clone()
            .failed("no advisories in the feed", "2026-11-11T06:00:00Z");
        assert_eq!(f.ok_at, "2026-11-10T06:00:00Z");
        assert_eq!(f.count, 3135);
        assert_eq!(f.error, "no advisories in the feed");
        let back = f.ok(3140, "2026-11-12T06:00:00Z");
        assert!(back.error.is_empty() && back.error_at.is_empty());
        let now = DateTime::parse_from_rfc3339("2026-11-13T05:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(back.fresh(now));
        assert!(!s.fresh(now));
    }
}
