//! Is this team affected, and what is the fix: a team's resources
//! ([`super::inventory`]) matched by version against the public feeds
//! ([`super::feeds`]). No model is called.
//!
//! - One finding per fix: one AMI, node group, database or cache is one
//!   row, however many advisories it clears.
//! - Three kinds of fix:
//!   - **pr**: the code that defines the resource changes (the AMI or its
//!     SSM parameter, a node group's `release_version`, a Lambda
//!     `runtime`). It goes through the security fix runs and their caps.
//!   - **runbook**: a step in AWS (apply pending maintenance or a service
//!     update), with the exact CLI, the console path and AWS's apply-by
//!     date. CoderHelm never runs it.
//!   - **aws**: AWS-managed, nothing to do, with the bulletin's reason.
//!     Never counted as open.
//! - Urgency uses the same thresholds as the dependency findings
//!   (`security_fixes::tier_of`), with network exposure standing in for
//!   code reachability.
//! - A finding closes only when the next read of the account shows the fix
//!   running ([`still_open`]), never when a pull request merges.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::feeds::{Advisory, Bulletin, Distro, LatestImage, Runtime, Who};
use super::inventory::{Exposure, Kind, Location, Os, Resource};
use crate::security_fixes::Tier;

/// What kind of fix a finding has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum FixKind {
    Pr,
    Runbook,
    Aws,
}

/// One advisory or bulletin behind a finding.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Source {
    pub id: String,
    /// `bulletin`, `alas`, `bottlerocket`, `runtime`, `maintenance`.
    pub kind: String,
    pub url: String,
    #[serde(default)]
    pub severity: String,
}

/// The fix: what changes, where, and how to apply it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fix {
    pub kind: Option<FixKind>,
    /// One line: "Bump the AMI to AL2023 2023.12.20261102".
    pub summary: String,
    /// Where the change goes (pr), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<Location>,
    /// The version it runs now, and the one that fixes it.
    pub from: String,
    pub to: String,
    /// The exact CLI for a runbook step (or the roll-out after a PR).
    #[serde(default)]
    pub cli: Vec<String>,
    /// Where it is in the AWS console.
    #[serde(default)]
    pub console: String,
    /// AWS's apply-by date (YYYY-MM-DD), empty when none.
    #[serde(default)]
    pub apply_by: String,
    /// Why nothing is needed (aws), in the bulletin's words.
    #[serde(default)]
    pub reason: String,
}

/// One infrastructure finding: one fix for one group of resources.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InfraFinding {
    /// Stable: the account, region, group and what fixes it.
    pub id: String,
    pub account: String,
    pub region: String,
    pub group: String,
    pub kind: Option<Kind>,
    pub name: String,
    /// The resources this fix covers, and how many things run there.
    pub resources: Vec<String>,
    pub count: u32,
    pub title: String,
    pub cves: Vec<String>,
    pub sources: Vec<Source>,
    /// critical, important, medium, low.
    pub severity: String,
    pub kev: bool,
    pub exposure: Exposure,
    /// P0 to P3; empty for AWS-managed rows.
    pub tier: Option<Tier>,
    pub fix: Fix,
    /// What Amazon Inspector adds, when the team runs it: its findings
    /// replace CoderHelm's own matching for the resource.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inspector: Option<InspectorFacts>,
}

/// Amazon Inspector's facts on a finding.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectorFacts {
    pub exploit_available: bool,
    /// Inspector's network reachability: `None` when it didn't say.
    pub reachable: Option<bool>,
    pub findings: usize,
}

impl InfraFinding {
    /// Counted as open (AWS-managed rows never are).
    pub fn is_open(&self) -> bool {
        self.fix.kind != Some(FixKind::Aws)
    }
}

/// What a team's resources are matched against.
#[derive(Debug, Clone)]
pub struct Feeds<'a> {
    pub advisories: &'a [Advisory],
    pub bulletins: &'a [Bulletin],
    pub runtimes: &'a [Runtime],
    pub latest: &'a [LatestImage],
    /// CVE id to the CISA list's due date.
    pub kev: &'a BTreeMap<String, String>,
    /// AWS's latest AL2023 AMI per architecture in the resource's region.
    pub latest_al2023: &'a BTreeMap<String, String>,
    /// YYYY-MM-DD.
    pub today: &'a str,
}

fn sev_rank(s: &str) -> u8 {
    match s {
        "critical" => 4,
        "important" | "high" => 3,
        "medium" | "moderate" => 2,
        "low" => 1,
        _ => 0,
    }
}

/// A score from the severity when no CVSS is known (ALAS and the bulletins
/// rate, they don't score).
pub fn score_of(severity: &str) -> f64 {
    match sev_rank(severity) {
        4 => 9.0,
        3 => 7.5,
        2 => 5.0,
        1 => 2.5,
        _ => 5.0,
    }
}

/// The tier, by the dependency findings' thresholds with network exposure
/// for reachability: exploited (CISA list) P0; 9+ and internet-facing P0;
/// 7+ and internet-facing P1; 4+ and internet-facing, or 7+, P2; else P3.
pub fn tier(severity: &str, kev: bool, public: bool) -> Tier {
    let s = score_of(severity);
    if kev || (s >= 9.0 && public) {
        Tier::P0
    } else if s >= 7.0 && public {
        Tier::P1
    } else if (s >= 4.0 && public) || s >= 7.0 {
        Tier::P2
    } else {
        Tier::P3
    }
}

/// Compares dotted versions (`1.68.1` and `1.67.0`, `2023.12.20261102`).
pub fn version_lt(a: &str, b: &str) -> bool {
    let parts = |s: &str| -> Vec<u64> {
        s.trim_start_matches('v')
            .split(['.', '-'])
            .map(|p| p.parse().unwrap_or(0))
            .collect()
    };
    parts(a) < parts(b)
}

fn short_id(parts: &[&str]) -> String {
    crate::content_hash(&parts.join("|"))
        .chars()
        .take(16)
        .collect()
}

fn bulletin_rows<'a>(
    feeds: &'a Feeds,
    service: &'a str,
) -> impl Iterator<Item = (&'a Bulletin, &'a super::feeds::ServiceStatus)> + 'a {
    feeds
        .bulletins
        .iter()
        .flat_map(move |b| b.rows(service).map(move |r| (b, r)))
}

struct Hits {
    cves: Vec<String>,
    sources: Vec<Source>,
    severity: String,
    kev: bool,
}

impl Hits {
    fn new() -> Hits {
        Hits {
            cves: Vec::new(),
            sources: Vec::new(),
            severity: String::new(),
            kev: false,
        }
    }
    fn add_cves(&mut self, cves: &[String], kev: &BTreeMap<String, String>) {
        for c in cves {
            if !self.cves.contains(c) {
                self.cves.push(c.clone());
            }
            self.kev |= kev.contains_key(c);
        }
    }
    fn severity(&mut self, s: &str) {
        if sev_rank(s) > sev_rank(&self.severity) {
            self.severity = s.to_string();
        }
    }
    fn bulletin(&mut self, b: &Bulletin, kev: &BTreeMap<String, String>) {
        if !self.sources.iter().any(|s| s.id == b.id) {
            self.sources.push(Source {
                id: b.id.clone(),
                kind: "bulletin".into(),
                url: b.url.clone(),
                severity: b.severity.clone(),
            });
        }
        self.add_cves(&b.cves, kev);
        if b.severity == "important" {
            self.severity("important");
        }
    }
    fn advisory(&mut self, a: &Advisory, kev: &BTreeMap<String, String>) {
        if !self.sources.iter().any(|s| s.id == a.id) {
            self.sources.push(Source {
                id: a.id.clone(),
                kind: if a.distro == Distro::Bottlerocket {
                    "bottlerocket".into()
                } else {
                    "alas".into()
                },
                url: a.link.clone(),
                severity: a.severity.clone(),
            });
        }
        self.add_cves(&a.cves, kev);
        self.severity(&a.severity);
    }
}

/// Sources shown per finding: bulletins first, then the newest advisories.
const MAX_SOURCES: usize = 20;

fn finding(r: &Resource, key: &str, title: String, hits: Hits, fix: Fix) -> InfraFinding {
    let open = fix.kind != Some(FixKind::Aws);
    let mut cves = hits.cves;
    cves.sort();
    let mut hits = Hits {
        cves: Vec::new(),
        ..hits
    };
    hits.sources.sort_by_key(|s| s.kind != "bulletin");
    hits.sources.truncate(MAX_SOURCES);
    InfraFinding {
        id: short_id(&[&r.account, &r.region, &r.group, key]),
        account: r.account.clone(),
        region: r.region.clone(),
        group: r.group.clone(),
        kind: Some(r.kind),
        name: r.name.clone(),
        resources: vec![r.id.clone()],
        count: r.count.max(1),
        title,
        cves,
        sources: hits.sources,
        tier: open.then(|| tier(&hits.severity, hits.kev, r.exposure.public)),
        severity: hits.severity,
        kev: hits.kev,
        exposure: r.exposure.clone(),
        fix,
        inspector: None,
    }
}

fn iac(r: &Resource) -> Option<Location> {
    r.defined_in
        .clone()
        .filter(|l| !l.repo.is_empty() && !l.file.is_empty())
}

/// An AMI-based group (Auto Scaling group, instance, ECS container hosts).
fn match_ami(r: &Resource, feeds: &Feeds, out: &mut Vec<InfraFinding>) {
    let Some(ami) = r.ami() else {
        return;
    };
    let mut hits = Hits::new();
    let mut to = String::new();
    match (ami.os, ami.flavor.as_str()) {
        (Os::Al2023, "al2023") if !ami.release.is_empty() => {
            for a in feeds
                .advisories
                .iter()
                .filter(|a| a.distro == Distro::Al2023 && a.is_kernel() && !a.releasever.is_empty())
            {
                if version_lt(&ami.release, &a.releasever) {
                    hits.advisory(a, feeds.kev);
                    if to.is_empty() || version_lt(&to, &a.releasever) {
                        to = a.releasever.clone();
                    }
                }
            }
            for (b, row) in bulletin_rows(feeds, "amazon_linux").chain(bulletin_rows(feeds, "ec2"))
            {
                let fixed = &row.fixed.al2023_release;
                if row.who.acts() && !fixed.is_empty() && version_lt(&ami.release, fixed) {
                    hits.bulletin(b, feeds.kev);
                    if to.is_empty() || version_lt(&to, fixed) {
                        to = fixed.clone();
                    }
                }
            }
        }
        (Os::Al2023 | Os::Al2, "ecs") => {
            for (b, row) in bulletin_rows(feeds, "ecs_ec2") {
                let fixed = &row.fixed.ecs_ami;
                if row.who.acts() && !fixed.is_empty() && ami.release.as_str() < fixed.as_str() {
                    hits.bulletin(b, feeds.kev);
                    if to.as_str() < fixed.as_str() {
                        to = fixed.clone();
                    }
                }
            }
        }
        (Os::Al2, "al2") => {
            // AL2 advisories carry no release; an AMI built before the
            // advisory's date doesn't have its kernel.
            for a in feeds.advisories.iter().filter(|a| {
                a.distro == Distro::Al2 && a.is_kernel() && a.package.contains(&ami.kernel_line)
            }) {
                let day = a.released.replace('-', "");
                if !ami.date.is_empty() && ami.date < day {
                    hits.advisory(a, feeds.kev);
                    if to < day {
                        to = format!("an AMI built after {}", a.released);
                    }
                }
            }
        }
        _ => {}
    }
    if hits.sources.is_empty() {
        return;
    }
    let location = iac(r);
    let what = match r.kind {
        Kind::AutoScalingGroup => "Auto Scaling group",
        _ => "instance",
    };
    let latest = feeds
        .latest_al2023
        .get("x86_64")
        .cloned()
        .unwrap_or_default();
    let refresh = format!(
        "aws autoscaling start-instance-refresh --auto-scaling-group-name {} --preferences MinHealthyPercentage=90 --region {}",
        r.name, r.region
    );
    let fix = Fix {
        kind: Some(FixKind::Pr),
        summary: if location.is_some() {
            format!(
                "Point the AMI at AWS's latest {} parameter (now {to})",
                if ami.flavor == "ecs" {
                    "ECS-optimized"
                } else {
                    "AL2023"
                }
            )
        } else {
            format!("Move the {what} to an AMI at {to} or later")
        },
        location,
        from: ami.release.clone(),
        to: to.clone(),
        cli: if r.kind == Kind::AutoScalingGroup {
            vec![refresh]
        } else {
            Vec::new()
        },
        console: if latest.is_empty() {
            String::new()
        } else {
            format!("AWS's latest AL2023 AMI in {} is {latest}", r.region)
        },
        apply_by: String::new(),
        reason: String::new(),
    };
    out.push(finding(
        r,
        &format!("ami:{to}"),
        format!(
            "{} {} runs AMI {} (fixed in {to})",
            r.kind.label(),
            r.name,
            ami.release
        ),
        hits,
        fix,
    ));
}

fn match_nodegroup(r: &Resource, feeds: &Feeds, out: &mut Vec<InfraFinding>) {
    let Some(ami) = r.ami() else {
        return;
    };
    let mut hits = Hits::new();
    let mut to = String::new();
    if ami.os == Os::Bottlerocket {
        for (b, row) in bulletin_rows(feeds, "bottlerocket") {
            let fixed = &row.fixed.bottlerocket;
            if row.who.acts() && !fixed.is_empty() && version_lt(&ami.release, fixed) {
                hits.bulletin(b, feeds.kev);
                if to.is_empty() || version_lt(&to, fixed) {
                    to = fixed.clone();
                }
            }
        }
    } else {
        for (b, row) in bulletin_rows(feeds, "eks_nodes") {
            let fixed = &row.fixed.eks_ami;
            if row.who.acts()
                && !fixed.is_empty()
                && !ami.release.is_empty()
                && ami.release.as_str() < fixed.as_str()
            {
                hits.bulletin(b, feeds.kev);
                if to.as_str() < fixed.as_str() {
                    to = fixed.clone();
                }
            }
        }
    }
    if hits.sources.is_empty() {
        return;
    }
    // The release_version for this Kubernetes version, when AWS's latest
    // parameter for it is known and at least the fixed release.
    let k8s = &r.versions.k8s_version;
    let release = feeds
        .latest
        .iter()
        .filter(|l| l.kind == "eks" && l.release.starts_with(&format!("{k8s}.")))
        .map(|l| l.release.clone())
        .find(|rel| {
            rel.rsplit('-')
                .next()
                .is_some_and(|d| format!("v{d}") >= to)
        })
        .unwrap_or_else(|| format!("{k8s}.x-{}", to.trim_start_matches('v')));
    let cluster = r.tags.get("coderhelm:cluster").cloned().unwrap_or_default();
    let fix = Fix {
        kind: Some(FixKind::Pr),
        summary: format!("Set the node group's release_version to {release} (AMI {to})"),
        location: iac(r),
        from: r.versions.node_release.clone(),
        to: release.clone(),
        cli: vec![format!(
            "aws eks update-nodegroup-version --cluster-name {cluster} --nodegroup-name {} --release-version {release} --region {}",
            r.name, r.region
        )],
        console: format!("EKS > Clusters > {cluster} > Compute > {} > Update now", r.name),
        apply_by: String::new(),
        reason: String::new(),
    };
    out.push(finding(
        r,
        &format!("nodegroup:{to}"),
        format!(
            "EKS node group {} runs {} (fixed in {to})",
            r.name, ami.release
        ),
        hits,
        fix,
    ));
}

fn aws_row(
    r: &Resource,
    key: &str,
    title: String,
    reason: String,
    b: Option<&Bulletin>,
    feeds: &Feeds,
    out: &mut Vec<InfraFinding>,
) {
    let mut hits = Hits::new();
    if let Some(b) = b {
        hits.bulletin(b, feeds.kev);
    }
    out.push(finding(
        r,
        key,
        title,
        hits,
        Fix {
            kind: Some(FixKind::Aws),
            summary: "AWS-managed, nothing to do".into(),
            reason,
            ..Default::default()
        },
    ));
}

fn match_fargate(r: &Resource, feeds: &Feeds, out: &mut Vec<InfraFinding>) {
    for (b, row) in bulletin_rows(feeds, "ecs_fargate") {
        if row.who != Who::Aws && row.who != Who::NotAffected {
            continue;
        }
        let started = &r.versions.oldest_task_started;
        let after = &row.fixed.patched_after;
        let older = !after.is_empty() && !started.is_empty() && started.as_str() < after.as_str();
        let reason = if older {
            format!(
                "AWS patched Fargate: tasks started after {} run patched. Some of this service's tasks started before then (the oldest {}); AWS retires them{}. Force a new deployment to replace them sooner.",
                after.chars().take(16).collect::<String>().replace('T', " "),
                started.chars().take(10).collect::<String>(),
                row.fixed
                    .retire_days
                    .map(|d| format!(" within {d} days"))
                    .unwrap_or_else(|| " per the service's task retirement settings".into())
            )
        } else {
            format!(
                "AWS patched Fargate; this service's tasks already run on the patched platform. {}",
                crate::truncate_str(&row.reason, 200)
            )
        };
        aws_row(
            r,
            &format!("aws:{}", b.id),
            format!("ECS service {} on Fargate: AWS-managed ({})", r.name, b.id),
            reason,
            Some(b),
            feeds,
            out,
        );
    }
}

fn match_lambda(r: &Resource, feeds: &Feeds, out: &mut Vec<InfraFinding>) {
    let rt = &r.versions.runtime;
    if r.versions.package_type == "Image" {
        // An image function runs the team's own base image: Dockerfile
        // findings cover it.
        return;
    }
    if let Some(runtime) = feeds.runtimes.iter().find(|x| &x.id == rt) {
        if runtime.deprecated_at(feeds.today) {
            let next = feeds
                .runtimes
                .iter()
                .filter(|x| {
                    !x.deprecated_at(feeds.today)
                        && x.id
                            .trim_end_matches(|c: char| c.is_ascii_digit() || c == '.' || c == 'x')
                            == rt.trim_end_matches(|c: char| {
                                c.is_ascii_digit() || c == '.' || c == 'x'
                            })
                })
                .max_by(|a, b| {
                    if version_lt(
                        &a.id.replace(|c: char| c.is_ascii_alphabetic(), ""),
                        &b.id.replace(|c: char| c.is_ascii_alphabetic(), ""),
                    ) {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                })
                .map(|x| x.id.clone())
                .unwrap_or_default();
            let mut hits = Hits::new();
            hits.sources.push(Source {
                id: format!("lambda-runtime-{rt}"),
                kind: "runtime".into(),
                url: super::feeds::LAMBDA_RUNTIMES.into(),
                severity: "important".into(),
            });
            hits.severity("medium");
            let fix = Fix {
                kind: Some(FixKind::Pr),
                summary: format!(
                    "Change the runtime from {rt} to {next} (AWS no longer patches {rt})"
                ),
                location: iac(r),
                from: rt.clone(),
                to: next,
                apply_by: runtime.block_update.clone(),
                ..Default::default()
            };
            out.push(finding(
                r,
                &format!("runtime:{rt}"),
                format!("Lambda {} runs a deprecated runtime ({rt})", r.name),
                hits,
                fix,
            ));
            return;
        }
    }
    for (b, row) in bulletin_rows(feeds, "lambda") {
        if matches!(row.who, Who::Aws | Who::NotAffected) {
            aws_row(
                r,
                &format!("aws:{}", b.id),
                format!("Lambda {}: AWS-managed ({})", r.name, b.id),
                crate::truncate_str(&row.reason, 300).to_string(),
                Some(b),
                feeds,
                out,
            );
        }
    }
}

fn runbook(
    r: &Resource,
    pending: &super::inventory::Pending,
    cli: String,
    console: String,
    service: &str,
    feeds: &Feeds,
    out: &mut Vec<InfraFinding>,
) {
    let mut hits = Hits::new();
    hits.sources.push(Source {
        id: format!("{}:{}", pending.kind, pending.name),
        kind: "maintenance".into(),
        url: String::new(),
        severity: String::new(),
    });
    // A bulletin that says the customer applies it in AWS names the CVEs.
    for (b, row) in bulletin_rows(feeds, service) {
        if row.who == Who::CustomerInAws {
            hits.bulletin(b, feeds.kev);
        }
    }
    if hits.severity.is_empty() {
        hits.severity(
            if pending.kind == "system-update"
                || pending
                    .description
                    .to_ascii_lowercase()
                    .contains("security")
            {
                "important"
            } else {
                "medium"
            },
        );
    }
    let fix = Fix {
        kind: Some(FixKind::Runbook),
        summary: format!(
            "Apply {} {} in AWS",
            pending.kind.replace('-', " "),
            if pending.name == pending.kind {
                String::new()
            } else {
                pending.name.clone()
            }
        )
        .replace("  ", " ")
        .trim()
        .to_string(),
        location: None,
        from: r.versions.engine_version.clone(),
        to: pending.name.clone(),
        cli: vec![cli],
        console,
        apply_by: pending.apply_by.clone(),
        reason: pending.description.clone(),
    };
    out.push(finding(
        r,
        &format!("{}:{}", pending.kind, pending.name),
        format!(
            "{} {}: {}",
            r.kind.label(),
            r.name,
            if pending.description.is_empty() {
                pending.name.clone()
            } else {
                pending.description.clone()
            }
        ),
        hits,
        fix,
    ));
}

fn match_managed(r: &Resource, feeds: &Feeds, out: &mut Vec<InfraFinding>) {
    for p in &r.pending {
        match r.kind {
            Kind::RdsCluster | Kind::RdsInstance
                if matches!(p.kind.as_str(), "system-update" | "db-upgrade") =>
            {
                let cli = format!(
                    "aws rds apply-pending-maintenance-action --resource-identifier {} --apply-action {} --opt-in-type next-maintenance --region {}",
                    r.id, p.kind, r.region
                );
                let console = format!(
                    "RDS > Databases > {} > Maintenance & backups > Pending maintenance > Apply",
                    r.name
                );
                runbook(r, p, cli, console, "rds", feeds, out);
            }
            Kind::ElasticacheGroup if p.kind == "service-update" => {
                let cli = format!(
                    "aws elasticache batch-apply-update-action --service-update-name {} --replication-group-ids {} --region {}",
                    p.name, r.name, r.region
                );
                let console = format!("ElastiCache > Service updates > {} > Apply now", p.name);
                runbook(r, p, cli, console, "elasticache", feeds, out);
            }
            _ if p.kind == "health" => {
                let cli = format!(
                    "aws health describe-events --filter eventTypeCodes={} --region us-east-1",
                    p.name
                );
                let console = format!(
                    "AWS Health Dashboard > Your account health > {}",
                    if p.auto_applied_after.is_empty() {
                        "Open and recent issues"
                    } else {
                        "Scheduled changes"
                    }
                );
                runbook(r, p, cli, console, "", feeds, out);
            }
            Kind::OpensearchDomain if p.kind == "service-software" => {
                let cli = format!("aws opensearch start-service-software-update --domain-name {} --schedule-at OFF_PEAK_WINDOW --region {}", r.name, r.region);
                let console = format!(
                    "OpenSearch Service > Domains > {} > Actions > Update",
                    r.name
                );
                runbook(r, p, cli, console, "opensearch", feeds, out);
            }
            _ => {}
        }
    }
}

fn inspector_rank(s: &str) -> &'static str {
    match s.to_ascii_uppercase().as_str() {
        "CRITICAL" => "critical",
        "HIGH" => "important",
        "MEDIUM" => "medium",
        _ => "low",
    }
}

/// Amazon Inspector's findings on a resource replace CoderHelm's own
/// matching for it: one finding for the resource's fix, with Inspector's
/// CVEs, exploit status and network reachability. CoderHelm's own fix (an
/// AMI bump, a node group release) is kept when it found one; otherwise the
/// fix is the packages Inspector names.
fn with_inspector(r: &Resource, own: Vec<InfraFinding>, feeds: &Feeds) -> Vec<InfraFinding> {
    let hits = &r.inspector;
    let mut hit_sources = Hits::new();
    let mut exploit = false;
    let mut reachable: Option<bool> = None;
    for h in hits {
        if !hit_sources.sources.iter().any(|s| s.id == h.id) {
            hit_sources.sources.push(Source {
                id: h.id.clone(),
                kind: "inspector".into(),
                url: String::new(),
                severity: h.severity.to_ascii_lowercase(),
            });
        }
        if h.vulnerability.starts_with("CVE-") {
            hit_sources.add_cves(std::slice::from_ref(&h.vulnerability), feeds.kev);
        }
        hit_sources.severity(inspector_rank(&h.severity));
        exploit |= h.exploit_available;
        reachable = match (reachable, h.reachable) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (None, x) => x,
            (x, None) => x,
            _ => Some(false),
        };
    }
    let facts = InspectorFacts {
        exploit_available: exploit,
        reachable,
        findings: hits.len(),
    };
    let public = reachable.unwrap_or(r.exposure.public);
    let mut exposure = r.exposure.clone();
    if let Some(reach) = reachable {
        exposure = Exposure {
            public: reach,
            why: if reach {
                "Inspector: reachable from the internet".into()
            } else {
                "Inspector: no path from the internet".into()
            },
        };
    }
    let mut base = own.into_iter().find(|f| f.fix.kind == Some(FixKind::Pr));
    let mut f = match base.take() {
        Some(mut f) => {
            for s in hit_sources.sources.drain(..) {
                f.sources.push(s);
            }
            for c in &hit_sources.cves {
                if !f.cves.contains(c) {
                    f.cves.push(c.clone());
                }
            }
            f.cves.sort();
            if sev_rank(&hit_sources.severity) > sev_rank(&f.severity) {
                f.severity = hit_sources.severity.clone();
            }
            f.kev |= hit_sources.kev;
            f
        }
        None => {
            let packages: Vec<String> = hits
                .iter()
                .flat_map(|h| h.packages.clone())
                .take(12)
                .collect();
            let fix = Fix {
                kind: Some(FixKind::Runbook),
                summary: if hits.iter().any(|h| h.fix_available) {
                    "Update the packages Amazon Inspector names, or move to an image with them fixed".into()
                } else {
                    "No fixed version yet: Amazon Inspector reports no fix available".into()
                },
                cli: if matches!(r.ami().map(|a| a.os), Some(Os::Al2023)) {
                    vec!["sudo dnf upgrade --security -y && sudo reboot".into()]
                } else {
                    Vec::new()
                },
                console: "Amazon Inspector > Findings > By instance".into(),
                reason: hits
                    .iter()
                    .map(|h| h.remediation.clone())
                    .find(|t| !t.is_empty())
                    .unwrap_or_default(),
                from: packages.join(", "),
                ..Default::default()
            };
            let mut f = finding(
                r,
                "inspector",
                format!(
                    "{} {}: {} Amazon Inspector findings",
                    r.kind.label(),
                    r.name,
                    hits.len()
                ),
                hit_sources,
                fix,
            );
            f.cves.sort();
            f
        }
    };
    f.exposure = exposure;
    f.inspector = Some(facts);
    // A known exploit counts like the CISA list.
    f.tier = Some(tier(&f.severity, f.kev || exploit, public));
    vec![f]
}

/// Every finding for a team's resources, one per fix, sorted by tier.
pub fn findings(resources: &[Resource], feeds: &Feeds) -> Vec<InfraFinding> {
    let mut out = Vec::new();
    for r in resources {
        if !r.inspector.is_empty()
            && matches!(
                r.kind,
                Kind::AutoScalingGroup
                    | Kind::Ec2Instance
                    | Kind::EksNodegroup
                    | Kind::ContainerImage
                    | Kind::LambdaFunction
            )
        {
            let mut own = Vec::new();
            match r.kind {
                Kind::AutoScalingGroup | Kind::Ec2Instance => match_ami(r, feeds, &mut own),
                Kind::EksNodegroup => match_nodegroup(r, feeds, &mut own),
                Kind::LambdaFunction => match_lambda(r, feeds, &mut own),
                _ => {}
            }
            out.extend(with_inspector(r, own, feeds));
            match_managed(r, feeds, &mut out);
            continue;
        }
        match r.kind {
            Kind::AutoScalingGroup | Kind::Ec2Instance => match_ami(r, feeds, &mut out),
            Kind::EksNodegroup => match_nodegroup(r, feeds, &mut out),
            Kind::EcsFargateService => match_fargate(r, feeds, &mut out),
            Kind::LambdaFunction => match_lambda(r, feeds, &mut out),
            Kind::RdsCluster
            | Kind::RdsInstance
            | Kind::ElasticacheGroup
            | Kind::OpensearchDomain => match_managed(r, feeds, &mut out),
            Kind::EksCluster | Kind::ContainerImage => {}
        }
    }
    out.sort_by(|a, b| {
        (a.tier.is_none(), a.tier, a.fix.kind, a.title.as_str()).cmp(&(
            b.tier.is_none(),
            b.tier,
            b.fix.kind,
            b.title.as_str(),
        ))
    });
    out
}

/// Whether a finding is still open after a new read of the account: its
/// group still runs the old version (or its pending step is still there).
/// A merged pull request alone never closes one.
pub fn still_open(old: &InfraFinding, fresh: &[InfraFinding]) -> bool {
    fresh.iter().any(|f| f.id == old.id)
}

/// Counts for the overview: open findings by tier, and AWS-managed rows.
pub fn counts(found: &[InfraFinding]) -> serde_json::Value {
    let mut by = BTreeMap::new();
    for t in Tier::ALL {
        by.insert(
            t.as_str(),
            found.iter().filter(|f| f.tier == Some(t)).count(),
        );
    }
    serde_json::json!({
        "open": found.iter().filter(|f| f.is_open()).count(),
        "prs": found.iter().filter(|f| f.fix.kind == Some(FixKind::Pr)).count(),
        "runbooks": found.iter().filter(|f| f.fix.kind == Some(FixKind::Runbook)).count(),
        "aws_managed": found.iter().filter(|f| f.fix.kind == Some(FixKind::Aws)).count(),
        "p0": by["P0"], "p1": by["P1"], "p2": by["P2"], "p3": by["P3"],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra_vuln::feeds;
    use crate::infra_vuln::inventory;
    use crate::infra_vuln::raw::RawAccount;

    const ACCOUNT: &str = include_str!("testdata/account-2026-160.json");
    const BULLETIN: &str = include_str!("testdata/bulletin-2026-160-aws.html");
    const ALAS_RSS: &str = include_str!("testdata/alas2023-rss.xml");
    const ALAS_PAGE: &str = include_str!("testdata/alas2023-2026-3365.html");
    const RUNTIMES: &str = include_str!("testdata/lambda-runtimes.html");
    const TF: &str = include_str!("testdata/infra-2026-160.tf");

    /// The team of the plan's worked example: an AL2023 Auto Scaling group,
    /// an EKS managed node group, an ECS Fargate service, a zip Lambda on
    /// python3.12 and an Aurora cluster with an OS update pending, matched
    /// against bulletin 2026-160-AWS and its ALAS advisory.
    fn example() -> Vec<InfraFinding> {
        let raw: RawAccount = serde_json::from_str(ACCOUNT).unwrap();
        let mut resources = raw.resources();
        let files = vec![(
            "acme/infra".to_string(),
            "envs/prod/main.tf".to_string(),
            TF.to_string(),
        )];
        for r in resources.iter_mut() {
            r.defined_in = inventory::locate(r, &["acme/infra".to_string()], &files);
        }
        resources.extend(inventory::lambdas_from_terraform(
            "acme/infra",
            "envs/prod/main.tf",
            TF,
            &raw.account,
            &raw.region,
            &raw.read_at,
        ));
        let b = feeds::parse_bulletin(BULLETIN, &feeds::bulletin_url("2026-160-AWS")).unwrap();
        let mut adv = feeds::parse_alas_rss(ALAS_RSS, Distro::Al2023).unwrap();
        let page = feeds::parse_alas_page(ALAS_PAGE).unwrap();
        for a in adv.iter_mut().filter(|a| a.id == page.id) {
            a.apply_page(&page);
        }
        let runtimes = feeds::parse_lambda_runtimes(RUNTIMES).unwrap();
        let kev = BTreeMap::new();
        let f = Feeds {
            advisories: &adv,
            bulletins: std::slice::from_ref(&b),
            runtimes: &runtimes,
            latest: &[],
            kev: &kev,
            latest_al2023: &raw.latest_al2023,
            today: "2026-11-10",
        };
        findings(&resources, &f)
    }

    #[test]
    fn the_2026_160_example_gives_two_prs_one_runbook_and_aws_rows_for_fargate_and_lambda() {
        let f = example();
        let prs: Vec<&InfraFinding> = f
            .iter()
            .filter(|x| x.fix.kind == Some(FixKind::Pr))
            .collect();
        let runbooks: Vec<&InfraFinding> = f
            .iter()
            .filter(|x| x.fix.kind == Some(FixKind::Runbook))
            .collect();
        let aws: Vec<&InfraFinding> = f
            .iter()
            .filter(|x| x.fix.kind == Some(FixKind::Aws))
            .collect();
        let titles: Vec<&str> = f.iter().map(|x| x.title.as_str()).collect();
        assert_eq!(prs.len(), 2, "one PR per AMI: {titles:#?}");
        assert_eq!(runbooks.len(), 1, "{titles:#?}");
        assert_eq!(aws.len(), 2, "{titles:#?}");
        assert_eq!(f.len(), 5, "no noise: {titles:#?}");

        // PR 1: the Auto Scaling group's AMI, in the Terraform that defines it.
        let asg = prs
            .iter()
            .find(|x| x.kind == Some(Kind::AutoScalingGroup))
            .expect("the AMI PR");
        assert_eq!(asg.fix.from, "2023.12.20261020");
        assert_eq!(asg.fix.to, "2023.12.20261102");
        assert_eq!(asg.count, 3);
        let at = asg.fix.location.as_ref().expect("located");
        assert_eq!(
            (at.repo.as_str(), at.address.as_str()),
            ("acme/infra", "aws_launch_template.web")
        );
        assert!(asg.cves.contains(&"CVE-2026-98432".to_string()));
        assert!(asg.sources.iter().any(|s| s.id == "2026-160-AWS"));
        assert!(asg.sources.iter().any(|s| s.id == "ALAS2023-2026-3365"));
        assert!(asg.fix.cli[0].starts_with("aws autoscaling start-instance-refresh"));
        // Behind an internet-facing load balancer: exposure raises the tier.
        assert_eq!(asg.tier, Some(Tier::P1));

        // PR 2: the node group's release_version.
        let ng = prs
            .iter()
            .find(|x| x.kind == Some(Kind::EksNodegroup))
            .expect("the node group PR");
        assert_eq!(ng.fix.from, "1.31.0-20261015");
        assert!(ng.fix.to.ends_with("20261031"), "{}", ng.fix.to);
        assert_eq!(
            ng.fix.location.as_ref().unwrap().address,
            "aws_eks_node_group.prod"
        );
        assert_eq!(ng.tier, Some(Tier::P2), "not internet-facing");

        // The Aurora runbook card: exact CLI, console path, AWS's date.
        let db = runbooks[0];
        assert_eq!(db.kind, Some(Kind::RdsCluster));
        assert!(db.fix.cli[0].starts_with("aws rds apply-pending-maintenance-action --resource-identifier arn:aws:rds:us-east-1:111122223333:cluster:orders --apply-action system-update"));
        assert!(db.fix.console.contains("Pending maintenance"));
        assert_eq!(db.fix.apply_by, "2026-12-01");
        assert!(
            db.sources.iter().any(|s| s.id == "2026-160-AWS"),
            "the bulletin says RDS customers apply it"
        );

        // AWS-managed rows, never counted as open.
        let fargate = aws
            .iter()
            .find(|x| x.kind == Some(Kind::EcsFargateService))
            .expect("Fargate");
        assert!(
            fargate.fix.reason.contains("started before then"),
            "{}",
            fargate.fix.reason
        );
        assert!(
            fargate.fix.reason.contains("Force a new deployment"),
            "{}",
            fargate.fix.reason
        );
        assert!(fargate.tier.is_none() && !fargate.is_open());
        let lambda = aws
            .iter()
            .find(|x| x.kind == Some(Kind::LambdaFunction))
            .expect("Lambda");
        assert!(
            lambda.fix.reason.contains("not affected"),
            "{}",
            lambda.fix.reason
        );
        let c = counts(&f);
        assert_eq!(c["open"], 3);
        assert_eq!(c["aws_managed"], 2);
    }

    #[test]
    fn a_finding_closes_only_when_the_next_read_shows_the_fix_running() {
        let before = example();
        let asg = before
            .iter()
            .find(|x| x.kind == Some(Kind::AutoScalingGroup))
            .unwrap()
            .clone();
        // The PR merged but nothing rolled out: the next read still finds it.
        assert!(still_open(&asg, &before));
        // The new AMI runs: gone from the next read, so closed.
        let mut raw: RawAccount = serde_json::from_str(ACCOUNT).unwrap();
        for i in raw.images.iter_mut() {
            i.name = i.name.replace("20261020", "20261102");
        }
        let after = findings(
            &raw.resources(),
            &Feeds {
                advisories: &[],
                bulletins: &[feeds::parse_bulletin(BULLETIN, "u").unwrap()],
                runtimes: &[],
                latest: &[],
                kev: &BTreeMap::new(),
                latest_al2023: &BTreeMap::new(),
                today: "2026-11-12",
            },
        );
        assert!(!still_open(&asg, &after));
    }

    #[test]
    fn inspector_findings_replace_own_matching_and_raise_the_tier() {
        let raw: RawAccount = serde_json::from_str(ACCOUNT).unwrap();
        let mut resources = raw.resources();
        let ng = resources
            .iter_mut()
            .find(|r| r.kind == Kind::EksNodegroup)
            .unwrap();
        ng.inspector = vec![inventory::InspectorHit {
            id: "finding-1".into(),
            vulnerability: "CVE-2026-98432".into(),
            severity: "HIGH".into(),
            exploit_available: true,
            fix_available: true,
            reachable: Some(false),
            ..Default::default()
        }];
        let b = feeds::parse_bulletin(BULLETIN, "u").unwrap();
        let kev = BTreeMap::new();
        let latest = BTreeMap::new();
        let f = findings(
            &resources,
            &Feeds {
                advisories: &[],
                bulletins: std::slice::from_ref(&b),
                runtimes: &[],
                latest: &[],
                kev: &kev,
                latest_al2023: &latest,
                today: "2026-11-10",
            },
        );
        let ng = f
            .iter()
            .find(|x| x.kind == Some(Kind::EksNodegroup))
            .unwrap();
        assert_eq!(
            ng.fix.kind,
            Some(FixKind::Pr),
            "CoderHelm's node group fix is kept"
        );
        assert!(ng.sources.iter().any(|s| s.kind == "inspector"));
        let facts = ng.inspector.as_ref().unwrap();
        assert!(facts.exploit_available && facts.reachable == Some(false));
        assert_eq!(
            ng.tier,
            Some(Tier::P0),
            "a known exploit counts like the CISA list"
        );
        assert!(ng.exposure.why.starts_with("Inspector"));
        assert_eq!(
            f.iter()
                .filter(|x| x.kind == Some(Kind::EksNodegroup))
                .count(),
            1,
            "one row per fix"
        );
    }

    #[test]
    fn a_health_event_on_a_resource_is_a_runbook_card() {
        let r = Resource {
            id: "arn:aws:rds:us-east-1:111122223333:cluster:orders".into(),
            kind: Kind::RdsCluster,
            name: "orders".into(),
            group: "orders".into(),
            pending: vec![inventory::Pending {
                kind: "health".into(),
                name: "AWS_RDS_SECURITY_NOTIFICATION".into(),
                description: "Apply the OS update".into(),
                apply_by: "2026-11-20".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let kev = BTreeMap::new();
        let latest = BTreeMap::new();
        let f = findings(
            &[r],
            &Feeds {
                advisories: &[],
                bulletins: &[],
                runtimes: &[],
                latest: &[],
                kev: &kev,
                latest_al2023: &latest,
                today: "2026-11-10",
            },
        );
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].fix.kind, Some(FixKind::Runbook));
        assert!(f[0].fix.cli[0].contains("eventTypeCodes=AWS_RDS_SECURITY_NOTIFICATION"));
        assert!(f[0].fix.console.starts_with("AWS Health Dashboard"));
    }

    #[test]
    fn tiers_follow_the_dependency_thresholds() {
        assert_eq!(tier("important", true, false), Tier::P0, "exploited");
        assert_eq!(tier("critical", false, true), Tier::P0);
        assert_eq!(tier("critical", false, false), Tier::P2);
        assert_eq!(tier("important", false, true), Tier::P1);
        assert_eq!(tier("medium", false, true), Tier::P2);
        assert_eq!(tier("low", false, false), Tier::P3);
        assert!(version_lt("1.67.0", "1.68.1"));
        assert!(version_lt("2023.12.20261020", "2023.12.20261102"));
        assert!(!version_lt("1.68.1", "1.68.1"));
    }

    #[test]
    fn a_deprecated_lambda_runtime_is_a_pr() {
        let runtimes = feeds::parse_lambda_runtimes(RUNTIMES).unwrap();
        let old = runtimes
            .iter()
            .find(|r| r.deprecated && r.id.starts_with("python"))
            .expect("a deprecated python");
        let r = Resource {
            id: "fn".into(),
            kind: Kind::LambdaFunction,
            name: "thumbs".into(),
            group: "fn".into(),
            versions: inventory::Versions {
                runtime: old.id.clone(),
                package_type: "Zip".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let kev = BTreeMap::new();
        let latest = BTreeMap::new();
        let f = findings(
            &[r],
            &Feeds {
                advisories: &[],
                bulletins: &[],
                runtimes: &runtimes,
                latest: &[],
                kev: &kev,
                latest_al2023: &latest,
                today: "2026-11-10",
            },
        );
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].fix.kind, Some(FixKind::Pr));
        assert!(f[0].fix.to.starts_with("python3."), "{}", f[0].fix.to);
        assert!(
            !runtimes
                .iter()
                .find(|r| r.id == f[0].fix.to)
                .unwrap()
                .deprecated
        );
    }
}
