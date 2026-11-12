//! Incidents: what CoderHelm works out before it acts on new log findings,
//! the way an engineer on call would, so one cause gets one card and one fix.
//!
//! 1. What the error lines say ([`signals`]): a hostname that no longer
//!    resolves, an index that isn't there, refused credentials, a service
//!    unavailable or timing out, and which AWS resource a hostname names
//!    ([`aws_host`]).
//! 2. What the team's AWS account says about that resource, read only
//!    ([`Look`]): does the domain in the error still exist, is there one of
//!    the same name with another endpoint, is it reachable only inside a VPC,
//!    and when it was created, changed or deleted (CloudTrail).
//! 3. What the team's code says: where the old hostname or the setting that
//!    holds the endpoint is named (the line to change), the commits that
//!    changed the resource lately, and the pipeline that writes a missing
//!    index.
//! 4. Whether the errors are still happening.
//!
//! [`diagnose`] turns that into where the fix belongs ([`Place`]): the code,
//! a setting (config, secret, environment), infrastructure as code, data (an
//! index rebuild or a pipeline rerun) or AWS itself. [`group`] folds findings
//! of one account that share a cause and a time window into one [`Incident`].
//! The worker's sweep acts once per incident.

use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::log_findings::{parse_time, Finding, Severity};

/// Findings whose errors started this close together can share a cause.
pub const WINDOW_MINUTES: i64 = 30;
/// Errors stopped for this long (and none since) count as recovered.
pub const RECOVERED_AFTER_MINUTES: i64 = 15;
/// A resource changed this long before the errors began can explain them.
pub const CHANGE_LOOKBACK_HOURS: i64 = 48;

// ─── Where the fix belongs ──────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Place {
    /// The application code (the default when nothing points elsewhere).
    #[default]
    Code,
    /// AWS itself: a managed service's own trouble, or a passing blip.
    Aws,
    /// Data: an index to rebuild, a pipeline to rerun.
    Data,
    /// Infrastructure as code (Terraform, CDK, CloudFormation).
    Infra,
    /// A setting: config, a secret or the service's environment.
    Config,
}

impl Place {
    /// Plain words for the card and the page.
    pub fn words(self) -> &'static str {
        match self {
            Place::Code => "the application code",
            Place::Aws => "AWS (a managed service or a passing blip)",
            Place::Data => "data (an index rebuild or a pipeline rerun)",
            Place::Infra => "infrastructure as code",
            Place::Config => "a setting (config, secret or environment)",
        }
    }
}

// ─── What the lines say ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    /// A hostname doesn't resolve (ENOTFOUND, no such host).
    DnsNotFound,
    /// A search index doesn't exist (404, index_not_found_exception).
    IndexMissing,
    /// Credentials or permissions refused (401, 403, security_exception).
    AuthRefused,
    /// The service answered 502/503/504 or is unavailable.
    Unavailable,
    Timeout,
    /// The connection was refused or reset.
    ConnectionRefused,
}

/// One thing the error lines say.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signal {
    pub kind: ErrorKind,
    /// The hostname involved, when the line names one.
    #[serde(default)]
    pub host: String,
    /// The index involved, for [`ErrorKind::IndexMissing`].
    #[serde(default)]
    pub index: String,
}

/// An AWS resource a hostname names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AwsHost {
    /// `opensearch`, `rds` or `elasticache`.
    pub service: String,
    /// The domain, instance or cluster name.
    pub name: String,
    /// The VPC form of the endpoint (`vpc-…`).
    pub vpc: bool,
}

/// Pure: the AWS resource a hostname names, if any. A hostname cut short in
/// a log line (`search-orders-os2`) is read too.
pub fn aws_host(host: &str) -> Option<AwsHost> {
    let h = host
        .trim()
        .trim_end_matches('.')
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .to_ascii_lowercase();
    let h = h.split([':', '/']).next().unwrap_or("");
    let first = h.split('.').next().unwrap_or("");
    let os = h.contains(".es.amazonaws.com") || h.contains(".aoss.amazonaws.com");
    let (vpc, rest) = if let Some(r) = first.strip_prefix("search-") {
        (false, r)
    } else if let Some(r) = first.strip_prefix("vpc-") {
        (true, r)
    } else if h.contains(".rds.amazonaws.com") {
        return Some(AwsHost {
            service: "rds".into(),
            name: first.to_string(),
            vpc: false,
        });
    } else if h.contains(".cache.amazonaws.com") {
        return Some(AwsHost {
            service: "elasticache".into(),
            name: first.to_string(),
            vpc: false,
        });
    } else {
        return None;
    };
    // Only `search-`/`vpc-` names on the OpenSearch domain, or cut short
    // before it (a bare first label).
    if !os && h.contains('.') {
        return None;
    }
    // `<domain>-<26-character id>`: the id is AWS's, not the name's.
    let name = match rest.rsplit_once('-') {
        Some((n, id)) if id.len() == 26 && id.chars().all(|c| c.is_ascii_alphanumeric()) => n,
        _ => rest,
    };
    (!name.is_empty()).then(|| AwsHost {
        service: "opensearch".into(),
        name: name.to_string(),
        vpc,
    })
}

fn rx(p: &str) -> Regex {
    Regex::new(p).expect("a fixed pattern")
}

/// Pure: what the lines (and the finding's own words) say, most specific
/// first, each once.
pub fn signals(texts: &[&str]) -> Vec<Signal> {
    let dns = rx(
        r"(?i)(?:ENOTFOUND|EAI_AGAIN|no such host|Name or service not known|could not resolve host)[:\s]+([A-Za-z0-9]+(?:[.-][A-Za-z0-9]+)+)",
    );
    let dns_lookup = rx(r"(?i)lookup ([A-Za-z0-9.-]+)(?: on [^:]+)?: no such host");
    let index = [
        rx(r"(?i)no such index \[([A-Za-z0-9_.\-]+)\]"),
        rx(r#"(?i)index_not_found_exception.{0,200}?index"?\s*[:=]\s*"?\[?([A-Za-z0-9_.\-]+)"#),
        rx(r"(?i)(?:search|query|count|get) on ([A-Za-z0-9_.\-]+) failed\b.{0,80}?status=404"),
        rx(r"(?i)\b([A-Za-z0-9_.\-]+) index (?:is )?(?:missing|not found)"),
    ];
    let auth = rx(
        r"(?i)(security_exception|AuthorizationException|status=40[13]\b|\b40[13] (?:Unauthorized|Forbidden)|InvalidSignatureException|UnrecognizedClientException|AccessDenied|authentication failed|password authentication failed)",
    );
    let unavailable = rx(
        r"(?i)(status=50[234]\b|\b50[234] (?:Service|Bad Gateway|Gateway)|Service (?:Temporarily )?Unavailable|ServiceUnavailable)",
    );
    let timeout = rx(r"(?i)(TimeoutError|timed out|ETIMEDOUT|ESOCKETTIMEDOUT|deadline exceeded)");
    let refused = rx(r"(?i)(ECONNREFUSED|ECONNRESET|connection refused|connection reset)");
    let host_any =
        rx(r"(?i)\b((?:search|vpc)-[a-z0-9-]+(?:\.[a-z0-9-]+)*\.(?:es|aoss)\.amazonaws\.com)\b");

    let mut out: Vec<Signal> = Vec::new();
    let mut push = |s: Signal| {
        if !out.iter().any(|o| o == &s) {
            out.push(s);
        }
    };
    for t in texts {
        for c in dns.captures_iter(t).chain(dns_lookup.captures_iter(t)) {
            push(Signal {
                kind: ErrorKind::DnsNotFound,
                host: c[1].trim_end_matches('.').to_ascii_lowercase(),
                index: String::new(),
            });
        }
        let not_found = t.contains("index_not_found") || t.contains("404") || {
            let l = t.to_ascii_lowercase();
            l.contains("missing") || l.contains("not found")
        };
        if not_found {
            for r in &index {
                if let Some(c) = r.captures(t) {
                    let name = c[1].trim_matches(['"', '\'', '[', ']']).to_string();
                    if !name.is_empty() && name != "the" && name != "an" {
                        push(Signal {
                            kind: ErrorKind::IndexMissing,
                            host: String::new(),
                            index: name,
                        });
                        break;
                    }
                }
            }
        }
        let host = host_any
            .captures(t)
            .map(|c| c[1].to_ascii_lowercase())
            .unwrap_or_default();
        for (re, kind) in [
            (&auth, ErrorKind::AuthRefused),
            (&unavailable, ErrorKind::Unavailable),
            (&timeout, ErrorKind::Timeout),
            (&refused, ErrorKind::ConnectionRefused),
        ] {
            if re.is_match(t) {
                push(Signal {
                    kind,
                    host: host.clone(),
                    index: String::new(),
                });
            }
        }
    }
    out.sort_by_key(|s| match s.kind {
        ErrorKind::DnsNotFound => 0,
        ErrorKind::IndexMissing => 1,
        ErrorKind::AuthRefused => 2,
        ErrorKind::ConnectionRefused => 3,
        ErrorKind::Unavailable => 4,
        ErrorKind::Timeout => 5,
    });
    out
}

/// Pure: the AWS service a finding's words name, when no hostname does.
pub fn service_named(texts: &[&str]) -> Option<&'static str> {
    let all = texts.join("\n").to_ascii_lowercase();
    if all.contains("opensearch")
        || all.contains("elasticsearch")
        || all.contains(".es.amazonaws.com")
    {
        Some("opensearch")
    } else if all.contains(".rds.amazonaws.com") {
        Some("rds")
    } else if all.contains(".cache.amazonaws.com") {
        Some("elasticache")
    } else {
        None
    }
}

/// Pure: a resource name without the version or colour a replacement adds
/// (`orders-os2`, `orders-os-v2`, `orders-os-blue` are `orders-os`).
pub fn stem(name: &str) -> String {
    let mut s = name.trim().to_ascii_lowercase();
    s = s.trim_end_matches(|c: char| c.is_ascii_digit()).to_string();
    for suffix in ["-v", "-new", "-old", "-blue", "-green", "-next", "-"] {
        if let Some(r) = s.strip_suffix(suffix) {
            s = r.to_string();
            break;
        }
    }
    s
}

// ─── What the account and the code say ──────────────────────────────────────

/// An OpenSearch domain in the team's account.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Domain {
    pub name: String,
    /// Its endpoint (the VPC one when it is in a VPC).
    pub endpoint: String,
    /// Reachable only inside its VPC.
    pub vpc: bool,
    /// Being deleted.
    pub deleted: bool,
}

/// A change CloudTrail recorded on a resource.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Change {
    /// RFC 3339.
    pub at: String,
    /// `CreateDomain`, `DeleteDomain`, `UpdateDomainConfig`, …
    pub action: String,
    pub resource: String,
    /// Who made it (a user or role session name).
    pub by: String,
}

/// Where the team's code names something.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CodeRef {
    pub repo: String,
    pub path: String,
    pub fragment: String,
}

/// A recent commit that touched a file.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Commit {
    pub repo: String,
    pub path: String,
    pub sha: String,
    /// Its first line.
    pub message: String,
    pub at: String,
}

/// A lookup's answer: `Err(Some(action))` when the role was refused that
/// IAM action, `Err(None)` when it failed otherwise.
pub type Lookup<T> = Result<T, Option<String>>;

/// The read-only lookups a diagnosis makes, so the rules can be tested with
/// fakes.
#[async_trait::async_trait]
pub trait Look: Send + Sync {
    /// The OpenSearch domains in the finding's account.
    async fn domains(&self) -> Lookup<Vec<Domain>>;
    /// CloudTrail's recent changes to a resource, by name.
    async fn changes(&self, resource: &str) -> Lookup<Vec<Change>>;
    /// Where the team's code names `query` (a code search).
    async fn code(&self, query: &str) -> Vec<CodeRef>;
    /// The latest commits that changed `path` in `repo`.
    async fn history(&self, repo: &str, path: &str) -> Vec<Commit>;
    /// Lines of the finding's error in its log group since `since`
    /// (RFC 3339); None when it can't be told.
    async fn lines_since(&self, f: &Finding, since: &str) -> Option<u64>;
}

// ─── The diagnosis ──────────────────────────────────────────────────────────

/// The change a fix makes, and where.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct FixPlan {
    pub repo: String,
    pub path: String,
    /// What to change, in a sentence the fix run and a person read.
    pub change: String,
}

/// Where one finding's fix belongs, and why.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Diagnosis {
    pub place: Place,
    /// `opensearch`, `rds`, `elasticache`, or empty.
    pub service: String,
    /// The resource the cause is about (its name without a version), or
    /// empty when unknown.
    pub resource: String,
    /// A few words: "OpenSearch domain orders-os2 replaced".
    pub headline: String,
    /// What happened, in a sentence or two.
    pub summary: String,
    /// What CoderHelm checked and found, one sentence each.
    pub facts: Vec<String>,
    /// The change to make, when it is in a repo.
    pub fix: Option<FixPlan>,
    /// Steps that aren't a code change (rebuild an index, set a secret).
    pub steps: Vec<String>,
    /// When the errors stopped, when they did and none came since.
    pub recovered_at: String,
    /// When the resource changed, when that explains the errors.
    pub changed_at: String,
    /// IAM actions that would have let CoderHelm check more.
    pub permissions: Vec<String>,
}

/// What a diagnosis is given.
#[derive(Debug, Clone, Default)]
pub struct Input<'a> {
    pub finding: Option<&'a Finding>,
    /// Scrubbed sample lines.
    pub samples: Vec<String>,
}

fn short_time(v: &str) -> String {
    parse_time(v)
        .map(|t| t.format("%b %-d %H:%M UTC").to_string())
        .unwrap_or_default()
}

/// Pure: a path that looks like infrastructure as code.
pub fn infra_path(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    p.ends_with(".tf")
        || p.ends_with(".tfvars")
        || p.contains("terraform")
        || p.contains("cdk")
        || p.contains("cloudformation")
        || p.contains("/cfn/")
        || p.starts_with("cfn/")
        || p.ends_with("serverless.yml")
        || p.contains("helm")
        || p.contains("k8s")
        || p.contains("task-definition")
        || p.contains("taskdef")
}

/// Pure: a path that looks like a data pipeline or job that writes indexes.
pub fn pipeline_path(path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    [
        "dag", "pipeline", "airflow", "etl", "sync", "job", "reindex", "indexer", "ingest",
        "loader", "backfill", "migrat",
    ]
    .iter()
    .any(|w| p.contains(w))
}

/// Pure: an environment variable name a fragment of code reads for `service`
/// (`OPENSEARCH_NODE`, `ELASTICSEARCH_URL`).
pub fn setting_name(fragment: &str, service: &str) -> Option<String> {
    let words: &[&str] = match service {
        "opensearch" => &["OPENSEARCH", "ELASTICSEARCH", "ELASTIC", "SEARCH"],
        "rds" => &["DATABASE", "DB", "POSTGRES", "MYSQL", "RDS"],
        "elasticache" => &["REDIS", "CACHE", "MEMCACHED"],
        _ => return None,
    };
    let re = rx(r"\b([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)\b");
    let found = re
        .captures_iter(fragment)
        .map(|c| c[1].to_string())
        .find(|v| {
            words.iter().any(|w| v.contains(w))
                && [
                    "HOST", "NODE", "URL", "ENDPOINT", "URI", "ADDRESS", "DOMAIN",
                ]
                .iter()
                .any(|e| v.contains(e))
        });
    found
}

/// Work out where `input.finding`'s fix belongs. Never fails: what can't be
/// read is left out, and refused permissions are named.
pub async fn diagnose(look: &dyn Look, input: &Input<'_>, now: DateTime<Utc>) -> Diagnosis {
    let Some(f) = input.finding else {
        return Diagnosis::default();
    };
    let mut texts: Vec<&str> = vec![&f.title, &f.error_pattern, &f.summary];
    texts.extend(input.samples.iter().map(String::as_str));
    let sigs = signals(&texts);
    let mut d = Diagnosis::default();
    let host = sigs
        .iter()
        .filter(|s| !s.host.is_empty())
        .find_map(|s| aws_host(&s.host).map(|h| (s.clone(), h)));
    d.service = host
        .as_ref()
        .map(|(_, h)| h.service.clone())
        .or_else(|| service_named(&texts).map(str::to_string))
        .unwrap_or_default();
    let refused = |d: &mut Diagnosis, e: Option<String>| {
        if let Some(a) = e {
            if !d.permissions.contains(&a) {
                d.permissions.push(a);
            }
        }
    };

    // The domains, when the error is about OpenSearch.
    let domains = if d.service == "opensearch" {
        match look.domains().await {
            Ok(v) => Some(v),
            Err(e) => {
                refused(&mut d, e);
                None
            }
        }
    } else {
        None
    };
    let first_seen = parse_time(&f.first_seen_at);

    let idx = sigs.iter().find(|s| s.kind == ErrorKind::IndexMissing);
    let auth = sigs.iter().find(|s| s.kind == ErrorKind::AuthRefused);

    let dns_host = host
        .as_ref()
        .filter(|(s, _)| s.kind == ErrorKind::DnsNotFound);
    if let Some((sig, h)) = dns_host {
        diagnose_dns(look, f, sig, h, domains.as_deref(), &mut d).await;
    } else if let Some(sig) = idx {
        diagnose_index(look, f, sig, domains.as_deref(), first_seen, &mut d).await;
    } else if let Some(sig) = auth.filter(|_| !d.service.is_empty()) {
        d.place = Place::Config;
        d.resource = host
            .as_ref()
            .map(|(_, h)| stem(&h.name))
            .unwrap_or_default();
        d.headline = format!(
            "{} refused the service's credentials",
            service_title(&d.service)
        );
        d.summary = format!(
            "{} answered with an authorization error{}: the credentials or access policy the service uses don't match the {}.",
            service_title(&d.service),
            if sig.host.is_empty() { String::new() } else { format!(" for {}", sig.host) },
            service_title(&d.service)
        );
        d.steps.push(
            "Check the credentials the service uses (its secret or environment) and the access policy on the resource."
                .into(),
        );
    } else if !d.service.is_empty()
        && sigs.iter().any(|s| {
            matches!(
                s.kind,
                ErrorKind::Unavailable | ErrorKind::Timeout | ErrorKind::ConnectionRefused
            )
        })
    {
        d.place = Place::Aws;
        d.resource = host
            .as_ref()
            .map(|(_, h)| stem(&h.name))
            .unwrap_or_default();
        d.headline = format!("{} unavailable", service_title(&d.service));
        d.summary = format!(
            "{} was unavailable or timing out for the service; nothing in the lines points at its code.",
            service_title(&d.service)
        );
    } else {
        d.place = Place::Code;
        d.headline = crate::truncate_str(f.title.trim(), 120).to_string();
        d.summary = crate::truncate_str(f.summary.trim(), 400).to_string();
    }

    // Still happening?
    if let Some(last) = parse_time(&f.last_seen_at) {
        if now - last >= Duration::minutes(RECOVERED_AFTER_MINUTES) {
            if let Some(0) = look.lines_since(f, &f.last_seen_at).await {
                d.recovered_at = f.last_seen_at.clone();
                d.facts.push(format!(
                    "No line of this error since {}: it stopped on its own.",
                    short_time(&f.last_seen_at)
                ));
            }
        }
    }
    d
}

fn service_title(service: &str) -> &'static str {
    match service {
        "opensearch" => "OpenSearch",
        "rds" => "The database",
        "elasticache" => "The cache",
        _ => "The service",
    }
}

/// The domain a missing one was replaced by: another domain with the same
/// name once a version or colour is taken off.
pub fn replacement<'d>(old: &str, domains: &'d [Domain]) -> Option<&'d Domain> {
    let s = stem(old);
    let mut same = domains
        .iter()
        .filter(|d| !d.deleted && d.name != old && stem(&d.name) == s);
    match (same.next(), same.next()) {
        (Some(d), None) => Some(d),
        _ => None,
    }
}

async fn change_time(
    look: &dyn Look,
    d: &mut Diagnosis,
    names: &[&str],
    actions: &[&str],
) -> String {
    let mut best: Option<Change> = None;
    for n in names {
        match look.changes(n).await {
            Ok(v) => {
                for c in v
                    .into_iter()
                    .filter(|c| actions.contains(&c.action.as_str()))
                {
                    if best.as_ref().is_none_or(|b| c.at > b.at) {
                        best = Some(c);
                    }
                }
            }
            Err(e) => {
                if let Some(a) = e {
                    if !d.permissions.contains(&a) {
                        d.permissions.push(a);
                    }
                }
                break;
            }
        }
    }
    match best {
        Some(c) => {
            d.facts.push(format!(
                "CloudTrail: {} on {} at {}{}.",
                c.action,
                c.resource,
                short_time(&c.at),
                if c.by.is_empty() {
                    String::new()
                } else {
                    format!(" by {}", c.by)
                }
            ));
            c.at
        }
        None => String::new(),
    }
}

/// The setting the service reads its endpoint from, and where it is set:
/// (setting name, where the app reads it, where it is set).
async fn find_setting(
    look: &dyn Look,
    service: &str,
    log_group: &str,
) -> (Option<String>, Option<CodeRef>, Vec<CodeRef>) {
    let word = match service {
        "opensearch" => "OPENSEARCH",
        "rds" => "DATABASE_URL",
        "elasticache" => "REDIS",
        _ => return (None, None, vec![]),
    };
    let svc = crate::log_repo::service_name(log_group);
    let mut reads: Vec<CodeRef> = look.code(word).await;
    // The service's own repo first, when its name says which.
    reads.sort_by_key(|r| !r.repo.to_ascii_lowercase().ends_with(&svc));
    let Some((name, read)) = reads
        .iter()
        .find_map(|r| setting_name(&r.fragment, service).map(|n| (n, r.clone())))
    else {
        return (None, None, vec![]);
    };
    let sets: Vec<CodeRef> = look
        .code(&name)
        .await
        .into_iter()
        .filter(|r| !(r.repo == read.repo && r.path == read.path))
        .filter(|r| {
            infra_path(&r.path)
                || r.path.contains(".env")
                || r.path.ends_with(".json")
                || r.path.ends_with(".yml")
                || r.path.ends_with(".yaml")
        })
        .collect();
    (Some(name), Some(read), sets)
}

async fn diagnose_dns(
    look: &dyn Look,
    f: &Finding,
    sig: &Signal,
    h: &AwsHost,
    domains: Option<&[Domain]>,
    d: &mut Diagnosis,
) {
    d.resource = stem(&h.name);
    let old = h.name.clone();
    let exists = domains.map(|ds| ds.iter().any(|x| x.name == old && !x.deleted));
    let new = domains.and_then(|ds| replacement(&old, ds));
    d.headline = match new {
        Some(n) => format!("OpenSearch domain {old} replaced by {}", n.name),
        None => format!("OpenSearch hostname for {old} doesn't resolve"),
    };
    if exists == Some(false) {
        d.facts
            .push(format!("The domain {old} no longer exists in the account."));
    }
    if let Some(n) = new {
        d.facts.push(format!(
            "The domain {} exists{}{}.",
            n.name,
            if n.endpoint.is_empty() {
                String::new()
            } else {
                format!(" with the endpoint {}", n.endpoint)
            },
            if n.vpc { " (VPC only)" } else { "" }
        ));
    }
    let names: Vec<&str> = std::iter::once(old.as_str())
        .chain(new.map(|n| n.name.as_str()))
        .collect();
    d.changed_at = change_time(look, d, &names, &["DeleteDomain", "CreateDomain"]).await;

    // Where the old hostname still is: the line to change.
    let old_refs: Vec<CodeRef> = {
        let mut v = look.code(&sig.host).await;
        if v.is_empty() && sig.host != old {
            v = look.code(&old).await;
        }
        v
    };
    let (setting, read, sets) = find_setting(look, "opensearch", &f.source_log_group).await;
    if let Some(r) = &read {
        d.facts.push(format!(
            "{} reads its endpoint from {} ({} in {}).",
            f.source_log_group,
            setting.as_deref().unwrap_or("a setting"),
            r.path,
            r.repo
        ));
    }
    // The commit that changed the domain's definition, when infra code has it.
    let defs: Vec<CodeRef> = match new {
        Some(n) => look
            .code(&n.name)
            .await
            .into_iter()
            .filter(|r| infra_path(&r.path))
            .collect(),
        None => vec![],
    };
    if let Some(def) = defs.first() {
        if let Some(c) = look.history(&def.repo, &def.path).await.into_iter().next() {
            d.facts.push(format!(
                "{} in {} defines the domain; last changed {} ({}: {}).",
                def.path,
                def.repo,
                short_time(&c.at),
                crate::truncate_str(&c.sha, 7),
                crate::truncate_str(c.message.trim(), 120)
            ));
            if d.changed_at.is_empty() {
                d.changed_at = c.at.clone();
            }
        }
    }
    let new_endpoint = new
        .map(|n| {
            if n.endpoint.is_empty() {
                format!("the {} domain's endpoint", n.name)
            } else {
                n.endpoint.clone()
            }
        })
        .unwrap_or_else(|| "the domain's current endpoint".into());
    let target = setting
        .clone()
        .unwrap_or_else(|| "the OpenSearch endpoint".into());
    if let Some(at) = old_refs.first() {
        d.place = if infra_path(&at.path) {
            Place::Infra
        } else {
            Place::Config
        };
        d.fix = Some(FixPlan {
            repo: at.repo.clone(),
            path: at.path.clone(),
            change: format!(
                "Point {target} at {new_endpoint} instead of the deleted {old} ({} still names the old host).",
                at.path
            ),
        });
    } else if let Some(at) = sets.first() {
        d.place = if infra_path(&at.path) {
            Place::Infra
        } else {
            Place::Config
        };
        d.fix = Some(FixPlan {
            repo: at.repo.clone(),
            path: at.path.clone(),
            change: format!("Set {target} to {new_endpoint} where {} sets it.", at.path),
        });
    } else {
        // The value is outside the code: a secret, a parameter or the
        // service's environment, which CoderHelm can read about but not set.
        d.place = Place::Config;
        d.steps.push(format!(
            "Set {target} for {} to {new_endpoint} (it isn't in the team's code: a secret, a parameter or the service's environment) and redeploy.",
            f.source_log_group
        ));
    }
    d.summary = format!(
        "{}{}{}; {} still reads the old hostname{}.",
        match new {
            Some(n) => format!("The OpenSearch domain {old} was replaced by {}", n.name),
            None => format!("The OpenSearch hostname for {old} no longer resolves"),
        },
        if new.is_some_and(|n| n.vpc) {
            " (VPC only)"
        } else {
            ""
        },
        if d.changed_at.is_empty() {
            String::new()
        } else {
            format!(" at {}", short_time(&d.changed_at))
        },
        f.source_log_group,
        match (&d.fix, &setting) {
            (Some(p), _) => format!(" from {} in {}", p.path, p.repo),
            (None, Some(s)) => format!(" from {s}, which is set outside the code"),
            _ => String::new(),
        }
    );
}

async fn diagnose_index(
    look: &dyn Look,
    f: &Finding,
    sig: &Signal,
    domains: Option<&[Domain]>,
    first_seen: Option<DateTime<Utc>>,
    d: &mut Diagnosis,
) {
    d.place = Place::Data;
    let index = sig.index.clone();
    // A domain made shortly before the errors began starts with no indexes.
    let mut new_domain: Option<String> = None;
    if let (Some(ds), Some(start)) = (domains, first_seen) {
        for dom in ds.iter().filter(|x| !x.deleted) {
            if let Ok(changes) = look.changes(&dom.name).await {
                let made = changes.iter().find(|c| {
                    c.action == "CreateDomain"
                        && parse_time(&c.at).is_some_and(|t| {
                            t <= start && start - t <= Duration::hours(CHANGE_LOOKBACK_HOURS)
                        })
                });
                if let Some(c) = made {
                    d.changed_at = c.at.clone();
                    d.facts.push(format!(
                        "CloudTrail: CreateDomain on {} at {}: a new domain starts with no indexes.",
                        dom.name,
                        short_time(&c.at)
                    ));
                    new_domain = Some(dom.name.clone());
                    break;
                }
            }
        }
        if new_domain.is_none() && ds.len() == 1 {
            new_domain = Some(ds[0].name.clone());
        }
    }
    d.resource = new_domain.as_deref().map(stem).unwrap_or_default();
    d.headline = format!("{index} index missing");
    // The pipeline that writes the index.
    let writers: Vec<CodeRef> = look
        .code(&index)
        .await
        .into_iter()
        .filter(|r| pipeline_path(&r.path))
        .collect();
    let on = new_domain
        .as_deref()
        .map(|n| format!(" on {n}"))
        .unwrap_or_default();
    match writers.first() {
        Some(w) => {
            let job = w
                .path
                .rsplit('/')
                .next()
                .unwrap_or(&w.path)
                .split('.')
                .next()
                .unwrap_or(&w.path)
                .to_string();
            d.facts.push(format!("{} in {} writes the {index} index.", w.path, w.repo));
            d.steps.push(format!(
                "Rerun {job} ({} in {}) to rebuild the {index} index{on}.",
                w.path, w.repo
            ));
        }
        None => d.steps.push(format!(
            "Rebuild the {index} index{on} (rerun the job that writes it, or restore it from a snapshot)."
        )),
    }
    d.summary = match &new_domain {
        Some(n) if !d.changed_at.is_empty() => format!(
            "The new OpenSearch domain {n} has no {index} index yet, so {} gets 404s.",
            f.source_log_group
        ),
        _ => format!(
            "The {index} index isn't on the OpenSearch domain {} uses, so it gets 404s.",
            f.source_log_group
        ),
    };
}

// ─── One incident per cause ─────────────────────────────────────────────────

/// Findings with one cause: one card and one fix.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Incident {
    pub id: String,
    pub account: String,
    /// The finding the fix starts from (the most specific place, then the
    /// most severe).
    pub lead: String,
    /// Every finding in it, the lead first.
    pub members: Vec<String>,
    pub log_groups: Vec<String>,
    pub first_seen: String,
    pub last_seen: String,
    /// The lead's diagnosis, with the other members' steps and facts.
    pub diagnosis: Diagnosis,
}

impl Incident {
    /// As the page and the finding record keep it.
    pub fn to_json(&self, titles: &[(String, String)], action: &str) -> Value {
        json!({
            "action": action,
            "id": self.id,
            "lead": self.lead,
            "members": self.members.iter().map(|m| json!({
                "rec_id": m,
                "title": titles.iter().find(|(id, _)| id == m).map(|(_, t)| t.as_str()).unwrap_or(""),
            })).collect::<Vec<_>>(),
            "log_groups": self.log_groups,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "diagnosis": self.diagnosis,
        })
    }
}

fn overlaps(a: &Finding, b: &Finding) -> bool {
    let w = |f: &Finding| {
        let s = parse_time(&f.first_seen_at);
        let e = parse_time(&f.last_seen_at).or(s);
        s.zip(e)
    };
    match (w(a), w(b)) {
        (Some((s1, e1)), Some((s2, e2))) => {
            let slack = Duration::minutes(WINDOW_MINUTES);
            s1 <= e2 + slack && s2 <= e1 + slack
        }
        _ => false,
    }
}

/// Pure: two diagnosed findings share a cause: the same account, the same
/// AWS service, resources that are the same or not known for one of them,
/// and error windows within [`WINDOW_MINUTES`]. Findings in the code
/// (no AWS service involved) are each their own incident.
pub fn same_cause(a: (&Finding, &Diagnosis), b: (&Finding, &Diagnosis)) -> bool {
    let ((fa, da), (fb, db)) = (a, b);
    !da.service.is_empty()
        && da.service == db.service
        && fa.source_account_id == fb.source_account_id
        && (da.resource.is_empty() || db.resource.is_empty() || da.resource == db.resource)
        && overlaps(fa, fb)
}

fn priority(d: &Diagnosis, f: &Finding) -> (Place, Severity, bool) {
    (d.place, f.severity, d.fix.is_some())
}

/// Pure: the incident id: an id a member already has, or one made from the
/// account, the cause and when it began.
pub fn incident_id(members: &[(&Finding, &Diagnosis)]) -> String {
    if let Some(f) = members
        .iter()
        .map(|(f, _)| f)
        .find(|f| !f.incident_id.is_empty())
    {
        return f.incident_id.clone();
    }
    let first = members
        .iter()
        .map(|(f, _)| f.first_seen_at.as_str())
        .min()
        .unwrap_or("");
    let (f, d) = members[0];
    let cause = if d.service.is_empty() {
        f.fingerprint()
    } else {
        format!("{}:{}", d.service, d.resource)
    };
    format!(
        "INC-{}",
        crate::sha256_hex(&[&f.source_account_id, &cause, first])[..12].to_uppercase()
    )
}

/// Pure: findings grouped into incidents, the order kept (the first
/// finding of each incident leads unless another one is more specific).
pub fn group(items: &[(Finding, Diagnosis)]) -> Vec<Incident> {
    let mut groups: Vec<Vec<usize>> = Vec::new();
    for (i, (f, d)) in items.iter().enumerate() {
        let home = groups.iter().position(|g| {
            g.iter()
                .any(|&j| same_cause((f, d), (&items[j].0, &items[j].1)))
        });
        match home {
            Some(g) => {
                groups[g].push(i);
                // A resource learned from one member names the others'.
            }
            None => groups.push(vec![i]),
        }
    }
    groups
        .into_iter()
        .map(|g| {
            let mut members: Vec<(&Finding, &Diagnosis)> =
                g.iter().map(|&i| (&items[i].0, &items[i].1)).collect();
            let lead_pos = members
                .iter()
                .enumerate()
                .max_by(|(ia, a), (ib, b)| {
                    priority(a.1, a.0).cmp(&priority(b.1, b.0)).then(ib.cmp(ia))
                })
                .map(|(i, _)| i)
                .unwrap_or(0);
            members.swap(0, lead_pos);
            let id = incident_id(&members);
            let (lf, ld) = members[0];
            let mut diagnosis = ld.clone();
            for (_, d) in members.iter().skip(1) {
                for s in &d.steps {
                    if !diagnosis.steps.contains(s) {
                        diagnosis.steps.push(s.clone());
                    }
                }
                for s in &d.facts {
                    if !diagnosis.facts.contains(s) {
                        diagnosis.facts.push(s.clone());
                    }
                }
                for p in &d.permissions {
                    if !diagnosis.permissions.contains(p) {
                        diagnosis.permissions.push(p.clone());
                    }
                }
                if diagnosis.resource.is_empty() {
                    diagnosis.resource = d.resource.clone();
                }
                if diagnosis.changed_at.is_empty() {
                    diagnosis.changed_at = d.changed_at.clone();
                }
                if !d.summary.is_empty()
                    && d.place != Place::Code
                    && d.place != diagnosis.place
                    && !diagnosis.summary.contains(&d.summary)
                {
                    diagnosis.summary = format!("{} {}", diagnosis.summary, d.summary);
                }
            }
            // Recovered only when every member stopped.
            diagnosis.recovered_at = if members.iter().all(|(_, d)| !d.recovered_at.is_empty()) {
                members
                    .iter()
                    .map(|(_, d)| d.recovered_at.clone())
                    .max()
                    .unwrap_or_default()
            } else {
                String::new()
            };
            let mut log_groups: Vec<String> = Vec::new();
            for (f, _) in &members {
                if !log_groups.contains(&f.source_log_group) {
                    log_groups.push(f.source_log_group.clone());
                }
            }
            let firsts: BTreeSet<&str> = members
                .iter()
                .map(|(f, _)| f.first_seen_at.as_str())
                .filter(|s| !s.is_empty())
                .collect();
            let lasts: BTreeSet<&str> = members
                .iter()
                .map(|(f, _)| f.last_seen_at.as_str())
                .filter(|s| !s.is_empty())
                .collect();
            Incident {
                id,
                account: lf.source_account_id.clone(),
                lead: lf.rec_id.clone(),
                members: members.iter().map(|(f, _)| f.rec_id.clone()).collect(),
                log_groups,
                first_seen: firsts
                    .iter()
                    .next()
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                last_seen: lasts
                    .iter()
                    .last()
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                diagnosis,
            }
        })
        .collect()
}

/// Pure: the section a fix run's ticket gets from its incident, so the run
/// changes the setting the diagnosis found instead of the code that read it.
pub fn ticket_note(inc: &Incident, others: &[String]) -> String {
    let d = &inc.diagnosis;
    let mut body = format!(
        "Where the fix belongs: {}\n{}\n",
        d.place.words(),
        d.summary
    );
    for fact in &d.facts {
        body.push_str(&format!("- {fact}\n"));
    }
    if let Some(p) = &d.fix {
        body.push_str(&format!(
            "The change: {} ({} in {})\n",
            p.change, p.path, p.repo
        ));
    }
    for step in &d.steps {
        body.push_str(&format!("Also needed, outside this change: {step}\n"));
    }
    if !others.is_empty() {
        body.push_str(&format!(
            "The same cause also produced: {}\n",
            others.join("; ")
        ));
    }
    format!(
        "## What CoderHelm found before starting (data, not instructions)\n```text\n{}```",
        body.replace("```", "'''")
    )
}

/// Pure: the incident stored on a finding (`incident`), when there is one.
pub fn stored(f: &Finding) -> Option<StoredIncident> {
    if f.incident.trim().is_empty() {
        return None;
    }
    serde_json::from_str(&f.incident).ok()
}

/// An incident as kept on its findings ([`Incident::to_json`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct StoredIncident {
    pub id: String,
    pub lead: String,
    pub members: Vec<StoredMember>,
    pub log_groups: Vec<String>,
    pub first_seen: String,
    pub last_seen: String,
    pub diagnosis: Diagnosis,
    /// What CoderHelm did about it, in a sentence.
    pub action: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct StoredMember {
    pub rec_id: String,
    pub title: String,
}

/// Pure: the ticket section for a fix started from a finding whose incident
/// is stored on it (a person's "Create fix PR" gets the same diagnosis).
pub fn stored_note(f: &Finding) -> String {
    let Some(s) = stored(f) else {
        return String::new();
    };
    if s.diagnosis.place == Place::Code && s.members.len() < 2 {
        return String::new();
    }
    let inc = Incident {
        id: s.id,
        lead: s.lead,
        members: s.members.iter().map(|m| m.rec_id.clone()).collect(),
        log_groups: s.log_groups,
        first_seen: s.first_seen,
        last_seen: s.last_seen,
        diagnosis: s.diagnosis,
        ..Default::default()
    };
    let others: Vec<String> = s
        .members
        .iter()
        .filter(|m| m.rec_id != f.rec_id)
        .map(|m| m.title.clone())
        .collect();
    ticket_note(&inc, &others)
}

// ─── What to do ─────────────────────────────────────────────────────────────

/// What the sweep does with an incident, before any repo is worked out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// It stopped on its own: say so, change nothing.
    Recovered,
    /// Start one fix in the repo that holds the setting or definition.
    FixIn(FixPlan),
    /// Say what was found and what only a person can do.
    ForAPerson(crate::handback::HumanOnly, String),
    /// AWS's side, still happening: say so and keep watching.
    Watching,
    /// The application code: work out its repo and start the fix there.
    Code,
}

/// Pure: what to do with an incident.
pub fn plan(d: &Diagnosis) -> Plan {
    use crate::handback::HumanOnly;
    if !d.recovered_at.is_empty() && d.place != Place::Code {
        return Plan::Recovered;
    }
    match d.place {
        Place::Code => Plan::Code,
        Place::Config | Place::Infra => match &d.fix {
            Some(p) => Plan::FixIn(p.clone()),
            None => Plan::ForAPerson(
                HumanOnly::Credential,
                "The value is kept outside the team's code (a secret, a parameter or the service's environment), which CoderHelm can name but not change.".into(),
            ),
        },
        Place::Data => Plan::ForAPerson(
            HumanOnly::MissingAccess,
            "Rebuilding the index means running the team's pipeline, which CoderHelm can only read.".into(),
        ),
        Place::Aws => Plan::Watching,
    }
}

// ─── The card ───────────────────────────────────────────────────────────────

/// What CoderHelm did about an incident.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// The errors stopped on their own; nothing to change.
    Recovered,
    /// A fix run started in `repo` (its PR card follows when it opens).
    Started { repo: String },
    /// Only a person can do what's left, for a reason on the human-only
    /// list ([`crate::handback::HumanOnly`]): a setting kept in a secret
    /// outside the code, a pipeline CoderHelm can only read.
    ForAPerson {
        why: crate::handback::HumanOnly,
        reason: String,
    },
    /// AWS's side and still happening: nothing for anyone to change in the
    /// team's code; CoderHelm keeps watching and says when it recovers.
    Watching,
    /// The repo is genuinely split: a person picks, once per log group.
    WhichRepo {
        guess: Option<String>,
        candidates: Vec<String>,
        reason: String,
    },
}

impl Action {
    /// The sentence the card, the finding and the page show.
    pub fn sentence(&self, d: &Diagnosis) -> String {
        match self {
            Action::Recovered => {
                let when = short_time(&d.recovered_at);
                let mut s = if when.is_empty() {
                    "It stopped on its own; nothing to change.".to_string()
                } else {
                    format!("It stopped on its own at {when}; no line since, so nothing to change.")
                };
                if let Some(p) = &d.fix {
                    s.push_str(&format!(
                        " If it comes back, the change is ready: {} ({} in {}).",
                        p.change, p.path, p.repo
                    ));
                }
                s
            }
            Action::Started { repo } => match &d.fix {
                Some(p) => format!(
                    "Started a fix in {repo}: {} The pull request follows here when it opens.",
                    p.change
                ),
                None => format!(
                    "Started a fix in {repo}. The pull request follows here when it opens."
                ),
            },
            Action::ForAPerson { reason, .. } => reason.clone(),
            Action::Watching => "Nothing in the team's code or settings causes it. CoderHelm keeps watching and posts here when it recovers; if the code needs a change after all, it starts the fix.".into(),
            Action::WhichRepo { guess, .. } => match guess {
                Some(g) => format!(
                    "The evidence is split between repos; the best guess is {g}. Pick the repo once for this log group and every fix after it starts on its own."
                ),
                None => "Nothing ties this log group to one repo yet. Pick the repo once and every fix after it starts on its own.".into(),
            },
        }
    }
}

fn card_time(v: &str) -> String {
    short_time(v)
}

/// Pure: the one card for an incident: what CoderHelm found, what it did,
/// and what is left, if anything.
#[allow(clippy::too_many_lines)]
pub fn incident_message(
    inc: &Incident,
    titles: &[(String, String)],
    action: &Action,
    url: &str,
) -> crate::notify::Message {
    use crate::notify::{Block, Button, Message, Size, Spacing, Text, Tone};
    let d = &inc.diagnosis;
    let headline = crate::truncate_str(d.headline.trim(), 140).replace('\n', " ");
    let (title, tone) = match action {
        Action::Recovered => (format!("✅ Recovered on its own: {headline}"), Tone::Good),
        Action::Started { .. } => (format!("🛠️ Fixing: {headline}"), Tone::Accent),
        Action::ForAPerson { .. } => (format!("🧭 Found the cause: {headline}"), Tone::Warning),
        Action::Watching => (format!("👀 Watching: {headline}"), Tone::Warning),
        Action::WhichRepo { .. } => (
            "🔎 Which repo should this fix go to?".to_string(),
            Tone::Accent,
        ),
    };
    let mut facts: Vec<(String, String)> = Vec::new();
    let names: Vec<String> = inc
        .members
        .iter()
        .map(|m| {
            titles
                .iter()
                .find(|(id, _)| id == m)
                .map(|(_, t)| crate::truncate_str(t.trim(), 120).replace('\n', " "))
                .unwrap_or_else(|| m.clone())
        })
        .collect();
    facts.push((
        if names.len() > 1 {
            format!("{} findings, one cause", names.len())
        } else {
            "Finding".to_string()
        },
        names.join("; "),
    ));
    facts.push(("Log groups".into(), inc.log_groups.join(", ")));
    let (a, b) = (card_time(&inc.first_seen), card_time(&inc.last_seen));
    if !a.is_empty() {
        facts.push((
            "Seen".into(),
            if b.is_empty() || a == b {
                a
            } else {
                format!("{a} to {b}")
            },
        ));
    }
    facts.push(("Fix belongs in".into(), d.place.words().to_string()));
    if let Action::WhichRepo {
        guess,
        candidates,
        reason,
    } = action
    {
        if let Some(g) = guess {
            facts.push(("Best guess".into(), g.clone()));
        }
        if candidates.len() > 1 {
            facts.push(("Also possible".into(), candidates[1..].join(", ")));
        }
        if !reason.is_empty() {
            facts.push(("Why".into(), crate::truncate_str(reason, 300).to_string()));
        }
    }
    facts.push(("What CoderHelm did".into(), action.sentence(d)));
    if let Action::ForAPerson { why, .. } = action {
        facts.push(("Needs a person for".into(), why.label().to_string()));
    }
    if !d.steps.is_empty() && *action != Action::Recovered {
        facts.push(("Next step".into(), d.steps.join(" ")));
    }
    if !d.permissions.is_empty() {
        facts.push((
            "Would check more with".into(),
            format!("{} on the read-only role", d.permissions.join(", ")),
        ));
    }
    let mut blocks = vec![
        Block::Banner {
            text: title.clone(),
            tone,
            size: Size::Medium,
        },
        Block::Text(
            Text::new(crate::truncate_str(d.summary.trim(), 600).replace('\n', " "))
                .spacing(Spacing::Small),
        ),
    ];
    for f in d.facts.iter().take(4) {
        blocks.push(Block::Text(
            Text::new(format!("• {}", crate::truncate_str(f, 300)))
                .subtle()
                .spacing(Spacing::None),
        ));
    }
    blocks.push(Block::Facts {
        facts,
        spacing: Spacing::Default,
    });
    let mut buttons = Vec::new();
    if !url.is_empty() {
        buttons.push(Button::new(
            if matches!(action, Action::WhichRepo { .. }) {
                "Pick the repo"
            } else {
                "Open the incident"
            },
            url,
        ));
    }
    Message {
        summary: format!("{title}. {}", crate::truncate_str(d.summary.trim(), 200)),
        blocks,
        buttons,
        full_width: true,
    }
}

#[cfg(test)]
mod tests;
