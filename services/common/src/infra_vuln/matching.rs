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

use super::actions::{self, Meaning, Tie};
use super::feeds::{Advisory, Bulletin, Distro, LatestImage, Runtime, Who};
use super::inventory::{Exposure, Kind, Location, Os, Pending, Resource};
use super::runbook::{self, PrNotes, Runbook};
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
    /// Why this is a pull request, or why it is a runbook, in words.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub decision: String,
    /// What a pending action means on the ARN it is on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meaning: Option<Meaning>,
    /// The whole runbook, when a person applies the update in AWS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runbook: Option<Runbook>,
    /// What a pull request must say and check, for the tool that defines
    /// the resource.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<PrNotes>,
    /// What to do in the code once the update is applied in AWS.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub code_note: String,
    /// Its place among related work: the other actions on the same
    /// resource, and non-production before production.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order: Vec<String>,
    /// The findings that go first (their ids).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after: Vec<String>,
    /// What is said about advisories: why none is named, or that one is
    /// only related.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub advisory_note: String,
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
    /// `production`, `staging`, ...; empty when not known.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub environment: String,
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
    /// A bulletin with only the CVEs that tie to the action (none for a
    /// bulletin that is only related).
    fn bulletin_with(&mut self, b: &Bulletin, cves: &[String], kev: &BTreeMap<String, String>) {
        if !self.sources.iter().any(|s| s.id == b.id) {
            self.sources.push(Source {
                id: b.id.clone(),
                kind: "bulletin".into(),
                url: b.url.clone(),
                severity: b.severity.clone(),
            });
        }
        self.add_cves(cves, kev);
        if b.severity == "important" && !cves.is_empty() {
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
        environment: r
            .defined_in
            .as_ref()
            .map(|l| l.environment.clone())
            .filter(|e| !e.is_empty())
            .unwrap_or_else(|| super::locate::environment_of_resource(r)),
    }
}

/// The place to open a pull request against: found, with a file, and not
/// a guess.
fn iac(r: &Resource) -> Option<Location> {
    r.defined_in.clone().filter(Location::sure)
}

/// Why there is no pull request for a change that has a code equivalent.
fn not_in_code(r: &Resource) -> String {
    match &r.defined_in {
        None => "This change has a code equivalent, but CoderHelm did not find this resource in any connected repository, so it is a runbook. If it is defined in code somewhere else, change it there too, or the next apply may undo this.".to_string(),
        Some(l) => format!(
            "This change has a code equivalent, but CoderHelm is not sure where this resource is defined, so it opens no pull request. {} {}",
            super::locate::describe(l),
            l.evidence.join(". ")
        )
        .trim()
        .to_string(),
    }
}

/// A code fix for a resource: a pull request where the code was found for
/// sure, with the guidance's notes for its tool; a runbook otherwise.
fn code_fix(r: &Resource, feeds: &Feeds, mut fix: Fix) -> Fix {
    let service = runbook::service_of(r.kind, &r.versions.engine);
    let action = runbook::action_of(r.kind);
    match iac(r) {
        Some(l) => {
            let v = runbook::vars(r, None, &fix.from, &fix.to, feeds.today);
            fix.notes = Some(runbook::pr_notes(
                service,
                action,
                &l.tool,
                BTreeMap::new(),
                &v,
                feeds.today,
            ));
            fix.decision = format!(
                "A pull request: this change has a code equivalent and the code that defines the resource was found. {}",
                super::locate::describe(&l)
            );
            fix.kind = Some(FixKind::Pr);
            fix.location = Some(l);
        }
        None => {
            fix.kind = Some(FixKind::Runbook);
            fix.decision = not_in_code(r);
            fix.location = r.defined_in.clone();
        }
    }
    fix
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
        ..Default::default()
    };
    let fix = code_fix(r, feeds, fix);
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
        ..Default::default()
    };
    let fix = code_fix(r, feeds, fix);
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
            let fix = code_fix(r, feeds, fix);
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

/// What a stored pending row means: its own reading, or, for a row saved
/// before meanings were kept, what its kind says.
fn meaning(r: &Resource, p: &Pending) -> Meaning {
    if p.meaning != Meaning::Unknown {
        return p.meaning;
    }
    match (r.kind, p.kind.as_str()) {
        (_, "health") => Meaning::Health,
        (Kind::ElasticacheGroup, "service-update") => Meaning::ServiceUpdate,
        (Kind::OpensearchDomain, "service-software") => Meaning::ServiceSoftware,
        (Kind::RdsInstance, k) => actions::meaning_of(k, "instance", &r.versions.engine),
        (Kind::RdsCluster, k) => match p.arns.first() {
            Some(a) => actions::meaning_of(k, actions::scope_of(a), &r.versions.engine),
            // Which ARN it was on was not kept: only names that mean one
            // thing on both are read.
            None => match k {
                "db-upgrade" | "os-upgrade" => actions::meaning_of(k, "cluster", ""),
                _ => Meaning::Unknown,
            },
        },
        _ => Meaning::Unknown,
    }
}

/// The severity an update's own words give, when no advisory does.
fn own_severity(m: Meaning, description: &str) -> &'static str {
    let d = description.to_ascii_lowercase();
    for s in ["critical", "important", "medium", "low"] {
        if d.split(|c: char| !c.is_ascii_alphabetic()).any(|w| w == s) {
            return s;
        }
    }
    if m == Meaning::OsUpdate || d.contains("security") {
        "important"
    } else {
        "medium"
    }
}

/// Whether the code's value for a version already allows `target`: the
/// same version, a later one, or a prefix of it (`16` for `16.6`).
fn code_covers(code: &str, target: &str) -> bool {
    !code.is_empty()
        && !target.is_empty()
        && (code == target || target.starts_with(&format!("{code}.")) || !version_lt(code, target))
}

/// One pending update of a database, cache or search domain: a pull
/// request when the update has a code equivalent and the code was found
/// for sure, a runbook otherwise, and in both cases why.
fn update(r: &Resource, p: &Pending, feeds: &Feeds, out: &mut Vec<InfraFinding>) {
    let m = meaning(r, p);
    let service = runbook::service_of(r.kind, &r.versions.engine);
    let action = m.guidance_action();
    let from = r.versions.engine_version.clone();
    let mut p = p.clone();
    p.meaning = m;
    if p.arns.is_empty() {
        p.arns = vec![r.id.clone()];
    }

    // Advisories: only what ties to this action.
    let mut hits = Hits::new();
    hits.sources.push(Source {
        id: format!("{}:{}", p.kind, p.name),
        kind: "maintenance".into(),
        url: String::new(),
        severity: String::new(),
    });
    let bulletin_service = match r.kind {
        Kind::RdsCluster | Kind::RdsInstance => "rds",
        Kind::ElasticacheGroup => "elasticache",
        Kind::OpensearchDomain => "opensearch",
        _ => "",
    };
    let target = actions::Target {
        meaning: m,
        engine: &r.versions.engine,
        running: &from,
        target: &p.target,
        name: &p.name,
        today: feeds.today,
    };
    let mut advisory_note = String::new();
    for (b, row) in bulletin_rows(feeds, bulletin_service) {
        match actions::tie(b, row, &target) {
            Ok(Tie::Exact { cves, .. }) => hits.bulletin_with(b, &cves, feeds.kev),
            Ok(Tie::Related { why }) => {
                hits.bulletin_with(b, &[], feeds.kev);
                advisory_note = why;
            }
            Err(_) => {}
        }
    }
    let q = super::guidance::Query {
        service,
        action,
        tool: "",
        facts: BTreeMap::new(),
    };
    let first = |section: &str| {
        super::guidance::section(&q, section)
            .first()
            .map(|e| e.text.clone())
            .unwrap_or_default()
    };
    if hits.cves.is_empty() && advisory_note.is_empty() {
        advisory_note = first("cve");
    }
    if hits.severity.is_empty() {
        hits.severity(own_severity(m, &p.description));
    }

    // Pull request or runbook.
    let located = r.defined_in.clone();
    let sure = located.clone().filter(Location::sure);
    let where_it_is = |l: &Location| {
        format!(
            "`{}`{} in {}",
            l.file,
            if l.address.is_empty() {
                String::new()
            } else {
                format!(" (`{}`)", l.address)
            },
            l.repo
        )
    };
    let mut kind = FixKind::Runbook;
    let mut notes = None;
    let mut code_note = String::new();
    let decision = if m == Meaning::Unknown {
        format!(
            "A runbook: AWS documents the `{}` action on a cluster only for Aurora PostgreSQL (an engine patch) and on an instance as an operating system update. On this {} ({}) AWS does not say what it does, so CoderHelm does not either: read AWS's description of the action before applying it.",
            p.kind,
            r.kind.label(),
            r.versions.engine
        )
    } else if !m.in_code() {
        let mut d = first("in_code");
        if d.is_empty() {
            d = "This is applied in AWS; nothing in infrastructure code does it.".into();
        }
        if let Some(l) = &sure {
            d.push_str(&format!(
                " The resource is defined in {}; nothing there changes for this update.",
                where_it_is(l)
            ));
        }
        d
    } else if p.target.is_empty() {
        "This update has a code equivalent, but AWS's description of the action does not name the version it moves to, so CoderHelm cannot write the change. Read the valid targets with the first check below.".to_string()
    } else {
        match (&sure, sure.as_ref().and_then(|l| l.edit.as_ref())) {
            (Some(_), Some(e)) if e.value.is_empty() => {
                code_note = format!(
                    "The code that defines this resource sets no `{}` ({}), so no version there holds this back and there is nothing to change.",
                    e.attribute, e.file
                );
                "A runbook: the code that defines this resource does not pin a version, so the upgrade is not a code change. Apply it in AWS, or let AWS apply it in the maintenance window.".to_string()
            }
            (Some(_), Some(e)) if code_covers(&e.value, &p.target) => {
                code_note = format!(
                    "The code already allows {}: {} sets `{}` to `{}` (line {}). No code change is needed.",
                    p.target, e.file, e.attribute, e.value, e.line
                );
                format!(
                    "A runbook: the code that defines this resource already allows version {} ({} is `{}`), so there is nothing to change there. The upgrade has not run yet: it is waiting for the maintenance window or for the change to be applied.",
                    p.target, e.attribute, e.value
                )
            }
            (Some(l), Some(_)) => {
                kind = FixKind::Pr;
                let mut facts = BTreeMap::new();
                if let Some(major) = actions::is_major(&r.versions.engine, &from, &p.target) {
                    facts.insert("major".to_string(), major.to_string());
                }
                let v = runbook::vars(r, Some(&p), &from, &p.target, feeds.today);
                notes = Some(runbook::pr_notes(
                    service,
                    action,
                    &l.tool,
                    facts,
                    &v,
                    feeds.today,
                ));
                format!(
                    "A pull request: an engine version change has a code equivalent, and the code that defines this resource was found. {}",
                    super::locate::describe(l)
                )
            }
            _ => {
                code_note = "If this resource is defined in code, raise its pinned version there to the new version once this is applied. A version pinned behind the live one makes the next apply a downgrade attempt, which the service refuses.".into();
                not_in_code(r)
            }
        }
    };

    // Built for a pull request too: if the replacement guard gives up on
    // the code change, the finding falls back to it.
    let rb = Some(runbook::build(r, &p, feeds.today));
    let (cli, console) = match rb.as_ref().filter(|_| kind == FixKind::Runbook) {
        Some(rb) => (
            rb.steps
                .iter()
                .filter(|s| !s.cli.is_empty())
                .map(|s| s.cli.clone())
                .take(p.arns.len().max(1))
                .collect(),
            rb.console.first().cloned().unwrap_or_default(),
        ),
        None => (Vec::new(), String::new()),
    };
    let what = if m == Meaning::Unknown {
        format!("AWS action `{}`", p.kind)
    } else if p.target.is_empty() {
        m.label().to_string()
    } else {
        format!("{} to {}", m.label(), p.target)
    };
    let summary = match (kind, &sure) {
        (FixKind::Pr, Some(l)) => {
            let e = l.edit.as_ref();
            format!(
                "Raise `{}` to {} in {}",
                e.map(|e| e.attribute.as_str())
                    .unwrap_or("the engine version"),
                p.target,
                e.map(|e| e.file.as_str()).unwrap_or(l.file.as_str())
            )
        }
        _ => format!("Apply the {what} in AWS (`{}`)", p.kind),
    };
    let mut order = Vec::new();
    if !p.order_why.is_empty() {
        order.push(p.order_why.clone());
    }
    for x in &p.merged {
        order.push(format!("{}.", super::locate::sentence(x)));
    }
    let fix = Fix {
        kind: Some(kind),
        summary,
        location: if kind == FixKind::Pr { sure } else { located },
        from,
        to: p.target.clone(),
        cli,
        console,
        apply_by: p.apply_by.clone(),
        reason: p.description.clone(),
        decision,
        meaning: Some(m),
        runbook: rb,
        notes,
        code_note,
        order,
        after: Vec::new(),
        advisory_note,
    };
    out.push(finding(
        r,
        &format!("{}:{}", p.kind, p.name),
        format!("{} {}: {what} pending", r.kind.label(), r.name),
        hits,
        fix,
    ));
}

/// An AWS Health event on a resource: a card that says where to read it.
fn health(r: &Resource, p: &Pending, out: &mut Vec<InfraFinding>) {
    let mut hits = Hits::new();
    hits.sources.push(Source {
        id: format!("{}:{}", p.kind, p.name),
        kind: "maintenance".into(),
        url: String::new(),
        severity: String::new(),
    });
    hits.severity(own_severity(Meaning::Health, &p.description));
    let fix = Fix {
        kind: Some(FixKind::Runbook),
        summary: format!("Act on the AWS Health event {}", p.name),
        from: r.versions.engine_version.clone(),
        cli: vec![format!(
            "aws health describe-events --filter eventTypeCodes={} --region us-east-1",
            p.name
        )],
        console: format!(
            "AWS Health Dashboard > Your account health > {}",
            if p.auto_applied_after.is_empty() {
                "Open and recent issues"
            } else {
                "Scheduled changes"
            }
        ),
        apply_by: p.apply_by.clone(),
        reason: p.description.clone(),
        decision: "A runbook: an AWS Health event says what to do in its own words; read the event for the steps.".into(),
        meaning: Some(Meaning::Health),
        advisory_note: "AWS Health events carry an action type and dates, not a CVE list.".into(),
        ..Default::default()
    };
    out.push(finding(
        r,
        &format!("{}:{}", p.kind, p.name),
        format!(
            "{} {}: {}",
            r.kind.label(),
            r.name,
            if p.description.is_empty() {
                p.name.clone()
            } else {
                p.description.clone()
            }
        ),
        hits,
        fix,
    ));
}

fn match_managed(r: &Resource, feeds: &Feeds, out: &mut Vec<InfraFinding>) {
    for p in &r.pending {
        match meaning(r, p) {
            Meaning::Health => health(r, p, out),
            Meaning::OsUpdate
            | Meaning::EnginePatch
            | Meaning::EngineUpgrade
            | Meaning::ServiceUpdate
            | Meaning::ServiceSoftware => update(r, p, feeds, out),
            // An update action AWS does not document for this ARN (a
            // cluster-level `system-update` on an engine other than Aurora
            // PostgreSQL): shown, named as AWS names it, with no meaning
            // made up for it.
            Meaning::Unknown
                if matches!(r.kind, Kind::RdsCluster | Kind::RdsInstance)
                    && matches!(
                        p.kind.as_str(),
                        "system-update" | "os-upgrade" | "db-upgrade"
                    ) =>
            {
                update(r, p, feeds, out)
            }
            // Hardware maintenance, certificate rotation and the
            // serverless platform version are not security updates
            // CoderHelm describes: they are not made into findings.
            _ => {}
        }
    }
}

/// The same resource in more than one environment: the non-production
/// one goes first, and both rows say so.
fn order_environments(found: &mut [InfraFinding], siblings: &[(usize, String)]) {
    let rule = super::guidance::section(
        &super::guidance::Query {
            service: "any",
            action: "any",
            tool: "",
            facts: BTreeMap::new(),
        },
        "order",
    )
    .first()
    .map(|e| e.text.clone())
    .unwrap_or_default();
    let mut groups: BTreeMap<(String, Option<Meaning>, String), Vec<usize>> = BTreeMap::new();
    for (i, key) in siblings {
        let f = &found[*i];
        if f.fix.kind == Some(FixKind::Aws) {
            continue;
        }
        groups
            .entry((key.clone(), f.fix.meaning, f.fix.to.clone()))
            .or_default()
            .push(*i);
    }
    for members in groups.values() {
        let prod: Vec<usize> = members
            .iter()
            .copied()
            .filter(|i| found[*i].environment == "production")
            .collect();
        let first: Vec<usize> = members
            .iter()
            .copied()
            .filter(|i| !found[*i].environment.is_empty() && found[*i].environment != "production")
            .collect();
        if prod.is_empty() || first.is_empty() {
            continue;
        }
        let names = |ids: &[usize]| {
            ids.iter()
                .map(|i| format!("`{}` ({})", found[*i].name, found[*i].environment))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let (before, after_names) = (names(&first), names(&prod));
        let ids: Vec<String> = first.iter().map(|i| found[*i].id.clone()).collect();
        for i in &prod {
            found[*i].fix.after = ids.clone();
            found[*i].fix.order.push(format!(
                "This is production. The same update is open on {before}: do that first, check it, then this one. {rule}"
            ));
        }
        for i in &first {
            found[*i].fix.order.push(format!(
                "This is the non-production copy of {after_names}: do this one first. {rule}"
            ));
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
    let mut base = own
        .into_iter()
        .find(|f| f.fix.kind != Some(FixKind::Aws) && !f.fix.to.is_empty());
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
    let mut siblings: Vec<(usize, String)> = Vec::new();
    for r in resources {
        let start = out.len();
        match_one(r, feeds, &mut out);
        let key = super::locate::sibling_key(r);
        siblings.extend((start..out.len()).map(|i| (i, key.clone())));
    }
    order_environments(&mut out, &siblings);
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

fn match_one(r: &Resource, feeds: &Feeds, out: &mut Vec<InfraFinding>) {
    {
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
            match_managed(r, feeds, out);
            return;
        }
        match r.kind {
            Kind::AutoScalingGroup | Kind::Ec2Instance => match_ami(r, feeds, out),
            Kind::EksNodegroup => match_nodegroup(r, feeds, out),
            Kind::EcsFargateService => match_fargate(r, feeds, out),
            Kind::LambdaFunction => match_lambda(r, feeds, out),
            Kind::RdsCluster
            | Kind::RdsInstance
            | Kind::ElasticacheGroup
            | Kind::OpensearchDomain => match_managed(r, feeds, out),
            Kind::EksCluster | Kind::ContainerImage => {}
        }
    }
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
        // The action is pending on the instance ARN, where `system-update`
        // is an operating system update: the command names that ARN.
        assert!(db.fix.cli[0].starts_with("aws rds apply-pending-maintenance-action --resource-identifier arn:aws:rds:us-east-1:111122223333:db:orders-1 --apply-action system-update"), "{:?}", db.fix.cli);
        assert!(
            db.fix.console.contains("Patch at next window"),
            "{}",
            db.fix.console
        );
        assert_eq!(db.fix.apply_by, "2026-12-01");
        assert_eq!(db.fix.meaning, Some(Meaning::OsUpdate));
        assert!(db.title.contains("operating system update"), "{}", db.title);
        // AWS publishes no CVE list for an OS update: the bulletin that
        // tells RDS customers to patch the database OS is shown as
        // related, and its CVE is not put on the action.
        assert!(db.cves.is_empty(), "{:?}", db.cves);
        assert!(
            db.sources.iter().any(|s| s.id == "2026-160-AWS"),
            "the bulletin says RDS customers patch the database OS"
        );
        assert!(
            db.fix.advisory_note.contains("no CVE list"),
            "{}",
            db.fix.advisory_note
        );
        // The cluster is in the team's Terraform, and it is still a
        // runbook, because an OS update cannot be expressed in code.
        assert!(
            db.fix.decision.contains("cannot be expressed in code"),
            "{}",
            db.fix.decision
        );
        assert!(
            db.fix.decision.contains("aws_rds_cluster.orders"),
            "{}",
            db.fix.decision
        );
        let rb = db.fix.runbook.as_ref().expect("a full runbook");
        assert!(!rb.prechecks.is_empty() && !rb.backup.is_empty() && !rb.rollback.is_empty());

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
            Some(FixKind::Runbook),
            "CoderHelm's node group fix is kept; not found in code, it is a runbook"
        );
        assert!(ng.fix.to.ends_with("20261031"), "{}", ng.fix.to);
        assert!(ng.fix.cli[0].starts_with("aws eks update-nodegroup-version"));
        assert!(
            ng.fix.decision.contains("did not find this resource"),
            "{}",
            ng.fix.decision
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
            defined_in: Some(Location {
                repo: "acme/infra".into(),
                file: "lambda.tf".into(),
                address: "aws_lambda_function.thumbs".into(),
                how: "terraform_name".into(),
                tool: "terraform".into(),
                confidence: inventory::Confidence::High,
                ..Default::default()
            }),
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
        let notes = f[0].fix.notes.as_ref().expect("the guidance for Terraform");
        assert!(notes.attribute[0].contains("`runtime` on the `aws_lambda_function`"));
        assert!(notes.rollback[0].contains("block-update date"));
    }

    fn feeds_with<'a>(
        bulletins: &'a [Bulletin],
        kev: &'a BTreeMap<String, String>,
        latest: &'a BTreeMap<String, String>,
    ) -> Feeds<'a> {
        Feeds {
            advisories: &[],
            bulletins,
            runtimes: &[],
            latest: &[],
            kev,
            latest_al2023: latest,
            today: "2026-11-13",
        }
    }

    fn database(name: &str, engine: &str, version: &str, lines: &[(&str, &str, &str)]) -> Resource {
        let cluster = engine.starts_with("aurora");
        let arn = format!(
            "arn:aws:rds:us-east-1:111122223333:{}:{name}",
            if cluster { "cluster" } else { "db" }
        );
        let lines: Vec<actions::Line> = lines
            .iter()
            .map(|(action, description, by)| actions::Line {
                arn: arn.clone(),
                action: action.to_string(),
                description: description.to_string(),
                apply_by: by.to_string(),
                ..Default::default()
            })
            .collect();
        Resource {
            id: arn.clone(),
            kind: if cluster {
                Kind::RdsCluster
            } else {
                Kind::RdsInstance
            },
            name: name.into(),
            account: "111122223333".into(),
            region: "us-east-1".into(),
            group: arn,
            versions: inventory::Versions {
                engine: engine.into(),
                engine_version: version.into(),
                ..Default::default()
            },
            pending: actions::reconcile(engine, version, &lines),
            count: 1,
            ..Default::default()
        }
    }

    fn in_terraform(
        file: &str,
        address: &str,
        value: &str,
        confidence: inventory::Confidence,
    ) -> Location {
        Location {
            repo: "acme/infra".into(),
            file: file.into(),
            address: address.into(),
            line: 3,
            how: "terraform_name".into(),
            tool: "terraform".into(),
            confidence,
            edit: Some(inventory::Edit {
                file: file.into(),
                line: 5,
                attribute: "engine_version".into(),
                value: value.into(),
                via: "resource".into(),
                shared: String::new(),
            }),
            ..Default::default()
        }
    }

    fn one(r: Resource, bulletins: &[Bulletin]) -> Vec<InfraFinding> {
        let (kev, latest) = (BTreeMap::new(), BTreeMap::new());
        findings(&[r], &feeds_with(bulletins, &kev, &latest))
    }

    #[test]
    fn an_engine_upgrade_is_a_pull_request_when_the_code_is_found_for_sure() {
        let mut r = database(
            "billing-prod",
            "mysql",
            "8.0.36",
            &[("db-upgrade", "Upgrade to 8.0.40", "2026-12-10")],
        );
        r.defined_in = Some(in_terraform(
            "db/main.tf",
            "aws_db_instance.billing",
            "8.0.36",
            inventory::Confidence::High,
        ));
        let f = one(r.clone(), &[]);
        assert_eq!(f.len(), 1);
        let x = &f[0];
        assert_eq!(x.fix.kind, Some(FixKind::Pr));
        assert_eq!(
            (x.fix.from.as_str(), x.fix.to.as_str()),
            ("8.0.36", "8.0.40")
        );
        assert_eq!(
            x.fix.summary,
            "Raise `engine_version` to 8.0.40 in db/main.tf"
        );
        assert!(
            x.fix.decision.starts_with("A pull request"),
            "{}",
            x.fix.decision
        );
        // The runbook is kept behind the pull request, for a fallback; the
        // card's own commands stay empty while it is a pull request.
        assert!(x
            .fix
            .runbook
            .as_ref()
            .is_some_and(|rb| !rb.steps.is_empty()));
        assert!(x.fix.cli.is_empty());
        let n = x.fix.notes.as_ref().unwrap();
        assert!(n.takes_effect[0].contains("`apply_immediately` defaults to `false`"));
        assert!(n.interruption[0].contains("Engine upgrades require downtime"));
        assert!(n.rollback[0].contains("one-way"));
        assert!(n.never_change.contains(&"identifier".to_string()));
        assert!(n
            .traps
            .iter()
            .any(|t| t.contains("Never set a value below the live version")));
        assert!(
            x.cves.is_empty(),
            "the action names no CVE and no bulletin ties one to it"
        );
        assert_eq!(x.fix.apply_by, "2026-12-10");

        // The same update, the code found but not for sure: a runbook
        // that says what was found and why it is unsure.
        let mut unsure = r.clone();
        let mut l = in_terraform(
            "db/main.tf",
            "aws_db_instance.billing",
            "8.0.36",
            inventory::Confidence::Low,
        );
        l.evidence = vec!["2 other places fit about as well".into()];
        unsure.defined_in = Some(l);
        let x = &one(unsure, &[])[0];
        assert_eq!(x.fix.kind, Some(FixKind::Runbook));
        assert!(
            x.fix
                .decision
                .contains("is not sure where this resource is defined"),
            "{}",
            x.fix.decision
        );
        assert!(
            x.fix.decision.contains("2 other places fit"),
            "{}",
            x.fix.decision
        );
        assert!(x.fix.location.is_some(), "what was found is still shown");
        assert!(x.fix.runbook.as_ref().unwrap().steps[0].cli.contains(
            "modify-db-instance --db-instance-identifier billing-prod --engine-version 8.0.40"
        ));
        assert!(x.fix.code_note.contains("raise its pinned version"));

        // Not in any repository: a runbook.
        let mut nowhere = r.clone();
        nowhere.defined_in = None;
        let x = &one(nowhere, &[])[0];
        assert_eq!(x.fix.kind, Some(FixKind::Runbook));
        assert!(x
            .fix
            .decision
            .contains("did not find this resource in any connected repository"));

        // The code already allows the target (a prefix): nothing to change.
        let mut prefix = r.clone();
        prefix.defined_in = Some(in_terraform(
            "db/main.tf",
            "aws_db_instance.billing",
            "8.0",
            inventory::Confidence::High,
        ));
        let x = &one(prefix, &[])[0];
        assert_eq!(x.fix.kind, Some(FixKind::Runbook));
        assert!(
            x.fix.decision.contains("already allows version 8.0.40"),
            "{}",
            x.fix.decision
        );

        // The code pins no version at all: nothing to change there.
        let mut unpinned = r.clone();
        let mut l = in_terraform(
            "db/main.tf",
            "aws_db_instance.billing",
            "",
            inventory::Confidence::High,
        );
        if let Some(e) = l.edit.as_mut() {
            e.via = "add".into();
        }
        unpinned.defined_in = Some(l);
        let x = &one(unpinned, &[])[0];
        assert_eq!(x.fix.kind, Some(FixKind::Runbook));
        assert!(
            x.fix.decision.contains("does not pin a version"),
            "{}",
            x.fix.decision
        );

        // AWS's description names no version: no change can be written.
        let mut vague = database(
            "billing-prod",
            "mysql",
            "8.0.36",
            &[("db-upgrade", "A new engine version is available", "")],
        );
        vague.defined_in = r.defined_in.clone();
        let x = &one(vague, &[])[0];
        assert_eq!(x.fix.kind, Some(FixKind::Runbook));
        assert!(x.fix.decision.contains("does not name the version"));
    }

    #[test]
    fn what_cannot_be_expressed_in_code_stays_a_runbook_even_when_the_resource_is_in_code() {
        for (engine, version, action, description, meaning) in [
            (
                "postgres",
                "16.4",
                "system-update",
                "New Operating System update is available",
                Meaning::OsUpdate,
            ),
            (
                "aurora-postgresql",
                "16.4",
                "system-update",
                "Upgrade to Aurora PostgreSQL 16.4.3",
                Meaning::EnginePatch,
            ),
            (
                "aurora-postgresql",
                "16.4",
                "os-upgrade",
                "New Operating System patch is available",
                Meaning::OsUpdate,
            ),
        ] {
            let mut r = database("orders", engine, version, &[(action, description, "")]);
            r.defined_in = Some(in_terraform(
                "main.tf",
                "aws_rds_cluster.orders",
                version,
                inventory::Confidence::High,
            ));
            let f = one(r, &[]);
            assert_eq!(f.len(), 1, "{action} on {engine}");
            let x = &f[0];
            assert_eq!(x.fix.meaning, Some(meaning));
            assert_eq!(x.fix.kind, Some(FixKind::Runbook), "{action} on {engine}");
            assert!(
                x.fix.decision.contains("cannot be expressed in code"),
                "{}",
                x.fix.decision
            );
            assert!(
                x.fix
                    .decision
                    .contains("nothing there changes for this update"),
                "{}",
                x.fix.decision
            );
            assert!(x.fix.notes.is_none());
            assert!(x.cves.is_empty());
        }
        // Hardware maintenance is not made into a finding. An update
        // action AWS does not document for this ARN is shown under AWS's
        // own name, with no meaning made up for it.
        let r = database(
            "orders",
            "aurora-mysql",
            "8.0.mysql_aurora.3.08.2",
            &[
                ("hardware-maintenance", "", ""),
                ("system-update", "something", "2026-12-01"),
            ],
        );
        let f = one(r, &[]);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].fix.meaning, Some(Meaning::Unknown));
        assert!(
            f[0].title.contains("AWS action `system-update` pending"),
            "{}",
            f[0].title
        );
        assert!(
            f[0].fix.decision.contains("AWS does not say what it does"),
            "{}",
            f[0].fix.decision
        );
        assert!(
            f[0].fix.cli[0].contains("--apply-action system-update --opt-in-type next-maintenance"),
            "{:?}",
            f[0].fix.cli
        );
        assert!(f[0].cves.is_empty());
    }

    #[test]
    fn an_advisory_is_attached_only_where_it_applies() {
        let b = Bulletin {
            id: "2026-170-AWS".into(),
            severity: "important".into(),
            published: "2026-11-01".into(),
            url: "u".into(),
            cves: vec!["CVE-2026-11111".into()],
            services: vec![feeds::ServiceStatus {
                service: "rds".into(),
                who: Who::CustomerInAws,
                reason: "Customers should upgrade to Aurora PostgreSQL 16.4.3 or RDS for MySQL version 8.0.40.".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let bs = std::slice::from_ref(&b);
        // The OS update on any database: never the CVE.
        let os = one(
            database(
                "a",
                "postgres",
                "16.4",
                &[(
                    "system-update",
                    "New Operating System update is available",
                    "",
                )],
            ),
            bs,
        );
        assert!(os[0].cves.is_empty() && !os[0].sources.iter().any(|s| s.id == b.id));
        // The Aurora PostgreSQL patch to the fixed version: the CVE.
        let patch = one(
            database(
                "b",
                "aurora-postgresql",
                "16.4",
                &[("system-update", "Upgrade to Aurora PostgreSQL 16.4.3", "")],
            ),
            bs,
        );
        assert_eq!(patch[0].cves, vec!["CVE-2026-11111"]);
        assert_eq!(patch[0].severity, "important");
        // A MySQL upgrade that stops short of the fix: not the CVE.
        let short = one(
            database(
                "c",
                "mysql",
                "8.0.36",
                &[("db-upgrade", "Upgrade to 8.0.39", "")],
            ),
            bs,
        );
        assert!(short[0].cves.is_empty());
        // A MySQL upgrade that reaches it: the CVE.
        let reach = one(
            database(
                "d",
                "mysql",
                "8.0.36",
                &[("db-upgrade", "Upgrade to 8.0.40", "")],
            ),
            bs,
        );
        assert_eq!(reach[0].cves, vec!["CVE-2026-11111"]);
        // MariaDB is not named by the bulletin.
        let other = one(
            database(
                "e",
                "mariadb",
                "10.11.6",
                &[("db-upgrade", "Upgrade to 10.11.9", "")],
            ),
            bs,
        );
        assert!(other[0].cves.is_empty());
        assert!(
            other[0].fix.advisory_note.contains("no CVE list"),
            "{}",
            other[0].fix.advisory_note
        );
    }

    #[test]
    fn the_non_production_copy_goes_first_and_both_rows_say_so() {
        let up = [("db-upgrade", "Upgrade to 16.6", "2027-01-15")];
        let mut prod = database("orders-prod", "postgres", "16.4", &up);
        prod.defined_in = Some(in_terraform(
            "envs/prod/main.tf",
            "aws_db_instance.orders",
            "16.4",
            inventory::Confidence::High,
        ));
        let mut staging = database("orders-staging", "postgres", "16.4", &up);
        staging.defined_in = Some(in_terraform(
            "envs/staging/main.tf",
            "aws_db_instance.orders",
            "16.4",
            inventory::Confidence::High,
        ));
        let other = database("billing-prod", "postgres", "16.4", &up);
        let (kev, latest) = (BTreeMap::new(), BTreeMap::new());
        let f = findings(&[prod, staging, other], &feeds_with(&[], &kev, &latest));
        let by = |n: &str| f.iter().find(|x| x.name == n).unwrap();
        let (p, s, o) = (by("orders-prod"), by("orders-staging"), by("billing-prod"));
        assert_eq!(
            (p.environment.as_str(), s.environment.as_str()),
            ("production", "staging")
        );
        assert_eq!(p.fix.after, vec![s.id.clone()]);
        assert!(
            p.fix
                .order
                .iter()
                .any(|x| x.contains("`orders-staging` (staging): do that first")),
            "{:?}",
            p.fix.order
        );
        assert!(s.fix.after.is_empty());
        assert!(
            s.fix.order.iter().any(|x| x.contains("do this one first")),
            "{:?}",
            s.fix.order
        );
        assert!(s.fix.order.iter().any(|x| x.contains("Well-Architected")));
        assert!(
            o.fix.after.is_empty() && o.fix.order.is_empty(),
            "no copy of it elsewhere"
        );
    }
}
