//! What a pending action means, which advisories apply to it, and how
//! several pending actions on one resource become one coherent, ordered
//! set of rows.
//!
//! - **Meaning by ARN type.** AWS reuses action names: `system-update` on
//!   an Aurora cluster ARN patches the DB engine (Aurora PostgreSQL), and
//!   on an instance ARN updates the operating system ([`meaning_of`]).
//! - **In code or not.** An engine version change has a code equivalent;
//!   an operating system update, an Aurora PostgreSQL patch and a
//!   service-managed patch do not ([`Meaning::in_code`]).
//! - **Advisories.** A bulletin is attached only when it ties to this
//!   service, this kind of action, this engine and version (or this named
//!   update) and its date ([`tie`]). An action AWS publishes without a CVE
//!   list never gets one.
//! - **One row per meaning.** The same update listed on a cluster and on
//!   each of its instances is one row naming every ARN; two upgrade
//!   actions with different targets are one row for the higher target;
//!   the rows of one resource are numbered in the order to do them
//!   ([`reconcile`]).

use regex::Regex;
use serde::{Deserialize, Serialize};

use super::feeds::{Bulletin, ServiceStatus, Who};
use super::inventory::Pending;
use super::matching::version_lt;

/// What a pending action does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Meaning {
    /// The operating system under a database.
    OsUpdate,
    /// An Aurora PostgreSQL engine patch (the third digit).
    EnginePatch,
    /// A new engine version (`db-upgrade`).
    EngineUpgrade,
    HardwareMaintenance,
    CertificateRotation,
    /// The platform version of serverless instances.
    ServerlessPlatform,
    /// An ElastiCache service update.
    ServiceUpdate,
    /// OpenSearch service software.
    ServiceSoftware,
    /// An AWS Health event.
    Health,
    #[default]
    Unknown,
}

impl Meaning {
    /// The guidance store's name for it.
    pub fn guidance_action(self) -> &'static str {
        match self {
            Meaning::OsUpdate => "os_update",
            Meaning::EnginePatch => "engine_patch",
            Meaning::EngineUpgrade => "engine_upgrade",
            Meaning::ServiceUpdate => "service_update",
            Meaning::ServiceSoftware => "service_software",
            _ => "any",
        }
    }

    /// Whether changing the code that defines the resource does the same
    /// thing. Only an engine version change does.
    pub fn in_code(self) -> bool {
        self == Meaning::EngineUpgrade
    }

    /// AWS publishes this action with no CVE list of its own, and nothing
    /// names the fixed version: no CVE may be attached to it.
    pub fn never_has_cves(self) -> bool {
        matches!(
            self,
            Meaning::OsUpdate
                | Meaning::HardwareMaintenance
                | Meaning::CertificateRotation
                | Meaning::ServerlessPlatform
        )
    }

    /// A plain name for a title.
    pub fn label(self) -> &'static str {
        match self {
            Meaning::OsUpdate => "operating system update",
            Meaning::EnginePatch => "engine patch",
            Meaning::EngineUpgrade => "engine version upgrade",
            Meaning::HardwareMaintenance => "hardware maintenance",
            Meaning::CertificateRotation => "certificate rotation",
            Meaning::ServerlessPlatform => "serverless platform update",
            Meaning::ServiceUpdate => "service update",
            Meaning::ServiceSoftware => "service software update",
            Meaning::Health => "AWS Health event",
            Meaning::Unknown => "pending maintenance",
        }
    }
}

/// `cluster` or `instance`, from an RDS ARN (`...:cluster:name`, `...:db:name`).
pub fn scope_of(arn: &str) -> &'static str {
    if arn.contains(":cluster:") {
        "cluster"
    } else if arn.contains(":db:") {
        "instance"
    } else {
        ""
    }
}

/// What an RDS or Aurora pending maintenance action means on this ARN.
/// `engine` is the resource's (`aurora-postgresql`, `mysql`).
pub fn meaning_of(action: &str, scope: &str, engine: &str) -> Meaning {
    match (action, scope) {
        ("db-upgrade", _) => Meaning::EngineUpgrade,
        ("os-upgrade", _) => Meaning::OsUpdate,
        ("system-update", "instance") => Meaning::OsUpdate,
        // AWS documents the cluster-level action as "patch the DB engine
        // for Aurora PostgreSQL". For any other engine it is not
        // documented, so it is not given a meaning here.
        ("system-update", "cluster") if engine.contains("postgres") => Meaning::EnginePatch,
        ("hardware-maintenance", _) => Meaning::HardwareMaintenance,
        ("ca-certificate-rotation" | "server-certificate-rotation", _) => {
            Meaning::CertificateRotation
        }
        ("serverless-platform-version-update", _) => Meaning::ServerlessPlatform,
        _ => Meaning::Unknown,
    }
}

fn re(p: &str) -> Regex {
    Regex::new(p).expect("a valid pattern")
}

/// The version a description names (`Upgrade to Aurora PostgreSQL 3.3.2`,
/// `... to 8.0.mysql_aurora.3.08.2`): the last version in it, empty when
/// it names none.
pub fn target_in(description: &str) -> String {
    if let Some(m) = re(r"\d+\.\d+\.mysql_aurora\.\d+(?:\.\d+){1,2}")
        .find_iter(description)
        .last()
    {
        return m.as_str().to_string();
    }
    re(r"\b\d+\.\d+(?:\.\d+){0,2}\b")
        .find_iter(description)
        .last()
        .map(|m| m.as_str().to_string())
        .unwrap_or_default()
}

/// The part of an engine version that names its major version.
fn major_of(engine: &str, version: &str) -> String {
    if let Some(a) = version.split("mysql_aurora.").nth(1) {
        return a.split('.').next().unwrap_or("").to_string();
    }
    let parts: Vec<&str> = version.split('.').collect();
    let two = engine.contains("mysql") || engine.contains("mariadb");
    if two && parts.len() >= 2 {
        format!("{}.{}", parts[0], parts[1])
    } else {
        parts.first().copied().unwrap_or("").to_string()
    }
}

/// Whether moving `from` to `to` crosses a major version. `None` when
/// either is not known.
pub fn is_major(engine: &str, from: &str, to: &str) -> Option<bool> {
    if from.is_empty() || to.is_empty() {
        return None;
    }
    Some(major_of(engine, from) != major_of(engine, to))
}

/// The engine family a bulletin's words or an API engine name speak of.
pub fn engine_family(s: &str) -> &'static str {
    let l = s.to_ascii_lowercase();
    let aurora = l.contains("aurora");
    if l.contains("postgres") {
        if aurora {
            "aurora-postgresql"
        } else {
            "postgres"
        }
    } else if l.contains("mysql") {
        if aurora {
            "aurora-mysql"
        } else {
            "mysql"
        }
    } else if l.contains("mariadb") {
        "mariadb"
    } else if l.contains("valkey") {
        "valkey"
    } else if l.contains("redis") {
        "redis"
    } else if l.contains("opensearch") {
        "opensearch"
    } else {
        ""
    }
}

/// The `(engine family, version)` pairs a bulletin row names as fixed
/// (`Aurora PostgreSQL 16.4.3`, `RDS for MySQL version 8.0.40`).
pub fn engines_fixed(text: &str) -> Vec<(String, String)> {
    let r = re(
        r"(?i)\b(aurora postgresql|aurora mysql|rds for postgresql|rds for mysql|rds for mariadb|postgresql|mysql|mariadb|valkey|redis oss)(?:[- ]compatible(?: edition)?)?\s+(?:versions?\s+)?(\d+(?:\.\d+){1,3})",
    );
    let mut out: Vec<(String, String)> = Vec::new();
    for c in r.captures_iter(text) {
        let pair = (engine_family(&c[1]).to_string(), c[2].to_string());
        if !pair.0.is_empty() && !out.contains(&pair) {
            out.push(pair);
        }
    }
    out
}

fn names_os_patch(text: &str) -> bool {
    let l = text.to_ascii_lowercase();
    [
        "database os",
        "operating system",
        "os patch",
        "os update",
        "patch your database os",
    ]
    .iter()
    .any(|w| l.contains(w))
}

/// A bulletin older than this is not taken to speak of an operating
/// system update pending today (AWS schedules required patching "once
/// every few months").
pub const OS_TIE_DAYS: i64 = 120;

/// What a bulletin is being matched to.
#[derive(Debug, Clone, Default)]
pub struct Target<'a> {
    pub meaning: Meaning,
    /// The resource's engine (`aurora-postgresql`).
    pub engine: &'a str,
    /// The version it runs, and the version the action moves it to.
    pub running: &'a str,
    pub target: &'a str,
    /// The update's own name (an ElastiCache service update, an
    /// OpenSearch service software release).
    pub name: &'a str,
    /// YYYY-MM-DD.
    pub today: &'a str,
}

/// How a bulletin ties to an action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tie {
    /// The bulletin names this update, or this engine with a fixed version
    /// the action reaches: its CVEs are this action's.
    Exact { cves: Vec<String>, why: String },
    /// The bulletin tells customers of this service to make this kind of
    /// update, recently, but AWS does not say this pending action is that
    /// update: shown as related, with no CVE.
    Related { why: String },
}

/// Why a bulletin does not apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoTie {
    /// The bulletin's row is not something the customer applies in AWS.
    NotACustomerAction,
    /// Published after today.
    NotYetPublished,
    /// The row speaks of another kind of update.
    DifferentAction,
    /// Too old to speak of an update pending today.
    TooOld,
    /// It names no fixed version for this engine.
    NoVersionForEngine,
    /// The resource already runs the fixed version or a later one.
    AlreadyFixed,
    /// The action's target is not known, or is below the fixed version.
    TargetDoesNotReachFix,
    /// It does not name this update.
    DifferentUpdate,
    /// AWS publishes this action with no CVE list and no bulletin can
    /// stand in for one.
    NoAdvisoriesForThisAction,
}

fn days_between(from: &str, to: &str) -> Option<i64> {
    let d = |s: &str| chrono::NaiveDate::parse_from_str(s.get(..10)?, "%Y-%m-%d").ok();
    Some((d(to)? - d(from)?).num_days())
}

fn row_cves(b: &Bulletin, row: &ServiceStatus) -> Vec<String> {
    let mut named: Vec<String> = Vec::new();
    for m in re(r"CVE-\d{4}-\d{4,7}").find_iter(&row.reason) {
        let c = m.as_str().to_string();
        if !named.contains(&c) {
            named.push(c);
        }
    }
    if named.is_empty() {
        b.cves.clone()
    } else {
        named
    }
}

/// Whether a bulletin's row for the resource's service applies to this
/// action: service (the caller picked the row), who acts, the kind of
/// action, the engine and version range or the update's name, and the
/// date.
pub fn tie(b: &Bulletin, row: &ServiceStatus, t: &Target) -> Result<Tie, NoTie> {
    if !matches!(row.who, Who::CustomerInAws | Who::Customer) {
        return Err(NoTie::NotACustomerAction);
    }
    if !b.published.is_empty() && !t.today.is_empty() && b.published.as_str() > t.today {
        return Err(NoTie::NotYetPublished);
    }
    match t.meaning {
        Meaning::OsUpdate => {
            if !names_os_patch(&row.reason) {
                return Err(NoTie::DifferentAction);
            }
            let last = if b.updated.is_empty() {
                &b.published
            } else {
                &b.updated
            };
            if days_between(last, t.today).is_none_or(|d| d > OS_TIE_DAYS) {
                return Err(NoTie::TooOld);
            }
            Ok(Tie::Related {
                why: format!(
                    "AWS publishes no CVE list for this action. Bulletin {} ({}) tells customers of this service to patch the database operating system; AWS does not say this pending update is that patch.",
                    b.id, b.published
                ),
            })
        }
        Meaning::EnginePatch | Meaning::EngineUpgrade => {
            let family = engine_family(t.engine);
            let fixed: Vec<String> = engines_fixed(&row.reason)
                .into_iter()
                .filter(|(f, _)| f == family)
                .map(|(_, v)| v)
                .collect();
            if family.is_empty() || fixed.is_empty() {
                return Err(NoTie::NoVersionForEngine);
            }
            // The fixed version in the running version's line: the same
            // major, and for a patch the same major.minor.
            let line = |v: &str| -> String {
                if t.meaning == Meaning::EnginePatch {
                    v.split('.').take(2).collect::<Vec<_>>().join(".")
                } else {
                    major_of(t.engine, v)
                }
            };
            let Some(fix) = fixed.iter().find(|v| line(v) == line(t.running)) else {
                return Err(NoTie::NoVersionForEngine);
            };
            let patch = t.meaning == Meaning::EnginePatch;
            if !patch && !version_lt(t.running, fix) {
                return Err(NoTie::AlreadyFixed);
            }
            // A patch's target is the Aurora version; the fixed version
            // a bulletin names is one too, in the same numbering.
            if t.target.is_empty() || version_lt(t.target, fix) {
                return Err(NoTie::TargetDoesNotReachFix);
            }
            Ok(Tie::Exact {
                cves: row_cves(b, row),
                why: format!(
                    "Bulletin {} names {} {fix} as fixed; this action moves {} to {}.",
                    b.id, family, t.running, t.target
                ),
            })
        }
        Meaning::ServiceUpdate | Meaning::ServiceSoftware => {
            let named = !t.name.is_empty()
                && (row.fixed.service_updates.iter().any(|u| u == t.name)
                    || row.reason.contains(t.name));
            if !named {
                return Err(NoTie::DifferentUpdate);
            }
            Ok(Tie::Exact {
                cves: row_cves(b, row),
                why: format!("Bulletin {} names the update {}.", b.id, t.name),
            })
        }
        _ => Err(NoTie::NoAdvisoriesForThisAction),
    }
}

/// One pending maintenance action as AWS lists it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Line {
    pub arn: String,
    pub action: String,
    pub description: String,
    /// YYYY-MM-DD, empty when AWS gives none.
    pub apply_by: String,
    pub auto_applied_after: String,
    /// The instance is its cluster's writer (it goes last).
    pub writer: bool,
}

fn earliest(a: &str, b: &str) -> String {
    match (a.is_empty(), b.is_empty()) {
        (true, _) => b.to_string(),
        (_, true) => a.to_string(),
        _ => a.min(b).to_string(),
    }
}

/// The pending actions of one database (a cluster with its instances, or
/// one instance) as rows: one per meaning, each naming every ARN it is
/// applied to, numbered in the order to do them.
///
/// - The cluster-level `os-upgrade` stands for the same operating system
///   update listed on each instance (AWS recommends it); with only
///   instance-level actions the row lists the instances, readers first.
/// - Two upgrade actions with different targets are one row for the
///   higher target, and the row says what it stands for.
/// - Order: operating system updates before a MySQL major upgrade (AWS
///   says so); otherwise the earlier AWS date first; with nothing to tell
///   them apart, the engine upgrade, then the patch, then the operating
///   system, and the row says that this order is CoderHelm's.
pub fn reconcile(engine: &str, running: &str, lines: &[Line]) -> Vec<Pending> {
    let mut rows: Vec<Pending> = Vec::new();
    for l in lines {
        let scope = scope_of(&l.arn);
        let meaning = meaning_of(&l.action, scope, engine);
        let target = if matches!(meaning, Meaning::EngineUpgrade | Meaning::EnginePatch) {
            target_in(&l.description)
        } else {
            String::new()
        };
        match rows.iter_mut().find(|r| r.meaning == meaning) {
            None => rows.push(Pending {
                kind: l.action.clone(),
                name: if meaning == Meaning::EnginePatch {
                    "engine-patch".into()
                } else {
                    l.action.clone()
                },
                description: l.description.clone(),
                apply_by: l.apply_by.clone(),
                auto_applied_after: l.auto_applied_after.clone(),
                status: "pending".into(),
                meaning,
                scope: scope.into(),
                arns: vec![l.arn.clone()],
                target,
                ..Default::default()
            }),
            Some(r) => {
                r.apply_by = earliest(&r.apply_by, &l.apply_by);
                r.auto_applied_after = earliest(&r.auto_applied_after, &l.auto_applied_after);
                let cluster_wide = meaning == Meaning::OsUpdate && l.action == "os-upgrade";
                if cluster_wide && r.kind != "os-upgrade" {
                    // The cluster-level action replaces the per-instance ones.
                    r.merged.push(format!(
                        "also listed as `{}` on {} instance{}; the cluster-level `os-upgrade` covers every instance",
                        r.kind,
                        r.arns.len(),
                        if r.arns.len() == 1 { "" } else { "s" }
                    ));
                    r.kind = l.action.clone();
                    r.name = l.action.clone();
                    r.scope = scope.into();
                    r.description = l.description.clone();
                    r.arns = vec![l.arn.clone()];
                } else if meaning == Meaning::OsUpdate && r.kind == "os-upgrade" {
                    if !r.merged.iter().any(|m| m.starts_with("also listed")) {
                        r.merged.push(format!(
                            "also listed as `{}` on its instances; the cluster-level `os-upgrade` covers every instance",
                            l.action
                        ));
                    }
                } else if !target.is_empty() && target != r.target {
                    let (keep, drop) = if r.target.is_empty() || version_lt(&r.target, &target) {
                        (target.clone(), r.target.clone())
                    } else {
                        (r.target.clone(), target.clone())
                    };
                    if !drop.is_empty() {
                        r.merged.push(format!(
                            "AWS lists two {}s, to {drop} and to {keep}; this row is the one to {keep}, which passes {drop}",
                            meaning.label()
                        ));
                    }
                    if keep == target {
                        r.description = l.description.clone();
                    }
                    r.target = keep;
                    if !r.arns.contains(&l.arn) {
                        r.arns.push(l.arn.clone());
                    }
                } else if !r.arns.contains(&l.arn) {
                    // The same action on another instance: readers first.
                    if l.writer {
                        r.arns.push(l.arn.clone());
                    } else {
                        let at = r
                            .arns
                            .iter()
                            .position(|a| lines.iter().any(|x| &x.arn == a && x.writer))
                            .unwrap_or(r.arns.len());
                        r.arns.insert(at, l.arn.clone());
                    }
                }
            }
        }
    }
    // An upgrade row on a cluster's instances is the cluster's.
    order(engine, running, &mut rows);
    rows
}

fn order(engine: &str, running: &str, rows: &mut [Pending]) {
    let acted: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter(|(_, r)| {
            matches!(
                r.meaning,
                Meaning::OsUpdate | Meaning::EnginePatch | Meaning::EngineUpgrade
            )
        })
        .map(|(i, _)| i)
        .collect();
    if acted.len() < 2 {
        return;
    }
    let mysql_major = engine_family(engine) == "mysql"
        && rows.iter().any(|r| {
            r.meaning == Meaning::EngineUpgrade
                && is_major(engine, running, &r.target) == Some(true)
        });
    let rank = |m: Meaning| match m {
        Meaning::EngineUpgrade => 0,
        Meaning::EnginePatch => 1,
        _ => 2,
    };
    let mut sorted = acted.clone();
    let mut by_date = false;
    sorted.sort_by(|&a, &b| {
        let (ra, rb) = (&rows[a], &rows[b]);
        if mysql_major {
            let os = |r: &Pending| r.meaning != Meaning::OsUpdate;
            if os(ra) != os(rb) {
                return os(ra).cmp(&os(rb));
            }
        }
        let key = |r: &Pending| (r.apply_by.is_empty(), r.apply_by.clone());
        key(ra)
            .cmp(&key(rb))
            .then(rank(ra.meaning).cmp(&rank(rb.meaning)))
    });
    let dates: Vec<&str> = acted.iter().map(|&i| rows[i].apply_by.as_str()).collect();
    if dates.iter().any(|d| !d.is_empty()) && dates.windows(2).any(|w| w[0] != w[1]) {
        by_date = true;
    }
    let of = sorted.len() as u8;
    let has_upgrade = rows.iter().any(|r| r.meaning == Meaning::EngineUpgrade);
    for (n, &i) in sorted.iter().enumerate() {
        let others: Vec<String> = sorted
            .iter()
            .filter(|&&j| j != i)
            .map(|&j| rows[j].meaning.label().to_string())
            .collect();
        let mut why = format!(
            "{} of {of} pending on this database (with the {}).",
            n + 1,
            others.join(" and the ")
        );
        if mysql_major && rows[i].meaning == Meaning::OsUpdate {
            why.push_str(" AWS says to apply available operating system updates before a MySQL major version upgrade.");
        } else if by_date {
            why.push_str(" Ordered by the date AWS applies each one, earliest first.");
        } else {
            why.push_str(" AWS gives no order for these; this order is CoderHelm's.");
        }
        if rows[i].meaning == Meaning::EnginePatch && has_upgrade {
            why.push_str(" AWS does not say whether this patch is still offered after the engine version upgrade: read pending maintenance again once the upgrade has run, and apply the patch only if it is still listed.");
        }
        if rows[i].meaning == Meaning::EngineUpgrade && rows.len() > 1 && n > 0 {
            why.push_str(
                " Each of these restarts the database: plan them as separate interruptions.",
            );
        }
        rows[i].step = (n + 1) as u8;
        rows[i].steps = of;
        rows[i].order_why = why;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLUSTER: &str = "arn:aws:rds:us-east-1:111122223333:cluster:orders";
    const WRITER: &str = "arn:aws:rds:us-east-1:111122223333:db:orders-1";
    const READER: &str = "arn:aws:rds:us-east-1:111122223333:db:orders-2";

    fn line(arn: &str, action: &str, description: &str, by: &str) -> Line {
        Line {
            arn: arn.into(),
            action: action.into(),
            description: description.into(),
            apply_by: by.into(),
            writer: arn == WRITER,
            ..Default::default()
        }
    }

    #[test]
    fn the_same_action_name_means_different_things_on_a_cluster_and_an_instance() {
        let pg = "aurora-postgresql";
        assert_eq!(scope_of(CLUSTER), "cluster");
        assert_eq!(scope_of(WRITER), "instance");
        assert_eq!(
            meaning_of("system-update", "cluster", pg),
            Meaning::EnginePatch
        );
        assert_eq!(
            meaning_of("system-update", "instance", pg),
            Meaning::OsUpdate
        );
        assert_eq!(meaning_of("os-upgrade", "cluster", pg), Meaning::OsUpdate);
        assert_eq!(
            meaning_of("db-upgrade", "cluster", pg),
            Meaning::EngineUpgrade
        );
        assert_eq!(
            meaning_of("system-update", "instance", "mysql"),
            Meaning::OsUpdate
        );
        // Not documented for a cluster of another engine: no meaning is made up.
        assert_eq!(
            meaning_of("system-update", "cluster", "aurora-mysql"),
            Meaning::Unknown
        );
        assert_eq!(
            meaning_of("hardware-maintenance", "instance", pg),
            Meaning::HardwareMaintenance
        );
        // Only an engine version change has a code equivalent.
        assert!(Meaning::EngineUpgrade.in_code());
        for m in [
            Meaning::OsUpdate,
            Meaning::EnginePatch,
            Meaning::ServiceUpdate,
            Meaning::ServiceSoftware,
            Meaning::HardwareMaintenance,
        ] {
            assert!(!m.in_code(), "{m:?}");
        }
        assert!(Meaning::OsUpdate.never_has_cves());
        assert!(!Meaning::EnginePatch.never_has_cves());
    }

    #[test]
    fn a_description_names_its_target_version() {
        assert_eq!(target_in("Upgrade to Aurora PostgreSQL 3.3.2"), "3.3.2");
        assert_eq!(
            target_in("Automatic minor version upgrade to postgres 16.6"),
            "16.6"
        );
        assert_eq!(
            target_in("Upgrade to 8.0.mysql_aurora.3.08.2"),
            "8.0.mysql_aurora.3.08.2"
        );
        assert_eq!(target_in("New Operating System update is available"), "");
        assert_eq!(is_major("postgres", "16.4", "16.6"), Some(false));
        assert_eq!(is_major("postgres", "15.8", "16.6"), Some(true));
        assert_eq!(is_major("mysql", "8.0.36", "8.0.40"), Some(false));
        assert_eq!(is_major("mysql", "8.0.36", "8.4.7"), Some(true));
        assert_eq!(
            is_major(
                "aurora-mysql",
                "8.0.mysql_aurora.3.04.0",
                "8.0.mysql_aurora.3.08.2"
            ),
            Some(false)
        );
        assert_eq!(is_major("postgres", "16.4", ""), None);
    }

    #[test]
    fn one_os_update_listed_on_the_cluster_and_each_instance_is_one_row() {
        let rows = reconcile(
            "aurora-postgresql",
            "16.4",
            &[
                line(
                    WRITER,
                    "system-update",
                    "New Operating System patch is available",
                    "2026-12-01",
                ),
                line(
                    READER,
                    "system-update",
                    "New Operating System patch is available",
                    "2026-11-28",
                ),
                line(
                    CLUSTER,
                    "os-upgrade",
                    "New Operating System patch is available",
                    "",
                ),
            ],
        );
        assert_eq!(rows.len(), 1, "{rows:#?}");
        let r = &rows[0];
        assert_eq!(r.meaning, Meaning::OsUpdate);
        assert_eq!(
            (r.kind.as_str(), r.scope.as_str()),
            ("os-upgrade", "cluster")
        );
        assert_eq!(r.arns, vec![CLUSTER.to_string()]);
        assert_eq!(
            r.apply_by, "2026-11-28",
            "the earliest date of what it stands for"
        );
        assert!(
            r.merged[0].contains("covers every instance"),
            "{:?}",
            r.merged
        );
        assert_eq!(r.step, 0, "one row needs no order");
    }

    #[test]
    fn instance_level_updates_list_every_instance_readers_first() {
        let rows = reconcile(
            "aurora-postgresql",
            "16.4",
            &[
                line(
                    WRITER,
                    "system-update",
                    "New Operating System patch is available",
                    "",
                ),
                line(
                    READER,
                    "system-update",
                    "New Operating System patch is available",
                    "",
                ),
            ],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].arns, vec![READER.to_string(), WRITER.to_string()]);
        assert_eq!(rows[0].kind, "system-update");
    }

    #[test]
    fn a_patch_and_an_upgrade_on_one_cluster_are_two_ordered_rows() {
        let rows = reconcile(
            "aurora-postgresql",
            "16.4",
            &[
                line(
                    CLUSTER,
                    "system-update",
                    "Upgrade to Aurora PostgreSQL 16.4.3",
                    "",
                ),
                line(
                    CLUSTER,
                    "db-upgrade",
                    "Upgrade to Aurora PostgreSQL 16.6",
                    "",
                ),
                line(
                    WRITER,
                    "system-update",
                    "New Operating System patch is available",
                    "",
                ),
            ],
        );
        assert_eq!(rows.len(), 3, "{rows:#?}");
        let by = |m: Meaning| rows.iter().find(|r| r.meaning == m).unwrap();
        let (up, patch, os) = (
            by(Meaning::EngineUpgrade),
            by(Meaning::EnginePatch),
            by(Meaning::OsUpdate),
        );
        assert_eq!((up.step, up.steps), (1, 3));
        assert_eq!(patch.step, 2);
        assert_eq!(os.step, 3);
        assert_eq!(up.target, "16.6");
        assert_eq!(patch.target, "16.4.3");
        // The engine patch and the OS update share an action name: their
        // keys differ.
        assert_ne!(
            (patch.kind.as_str(), patch.name.as_str()),
            (os.kind.as_str(), os.name.as_str())
        );
        assert!(
            up.order_why.contains("this order is CoderHelm's"),
            "{}",
            up.order_why
        );
        assert!(
            patch
                .order_why
                .contains("AWS does not say whether this patch is still offered"),
            "{}",
            patch.order_why
        );
    }

    #[test]
    fn aws_dates_decide_the_order_and_mysql_majors_wait_for_the_os() {
        let rows = reconcile(
            "aurora-postgresql",
            "16.4",
            &[
                line(CLUSTER, "db-upgrade", "Upgrade to 16.6", "2027-01-15"),
                line(
                    CLUSTER,
                    "system-update",
                    "Upgrade to Aurora PostgreSQL 16.4.3",
                    "2026-12-01",
                ),
            ],
        );
        let patch = rows
            .iter()
            .find(|r| r.meaning == Meaning::EnginePatch)
            .unwrap();
        assert_eq!(patch.step, 1, "the earlier AWS date goes first");
        assert!(patch.order_why.contains("earliest first"));
        let db = "arn:aws:rds:us-east-1:111122223333:db:billing";
        let rows = reconcile(
            "mysql",
            "8.0.36",
            &[
                line(db, "db-upgrade", "Upgrade to 8.4.7", "2026-12-01"),
                line(
                    db,
                    "system-update",
                    "New Operating System update is available",
                    "",
                ),
            ],
        );
        let os = rows
            .iter()
            .find(|r| r.meaning == Meaning::OsUpdate)
            .unwrap();
        assert_eq!(os.step, 1);
        assert!(os
            .order_why
            .contains("before a MySQL major version upgrade"));
    }

    #[test]
    fn two_upgrades_with_different_targets_are_one_row_for_the_higher() {
        let rows = reconcile(
            "postgres",
            "16.4",
            &[
                line(WRITER, "db-upgrade", "Upgrade to 16.6", "2026-12-10"),
                line(WRITER, "db-upgrade", "Upgrade to 16.8", "2026-12-20"),
            ],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].target, "16.8");
        assert_eq!(rows[0].apply_by, "2026-12-10");
        assert!(
            rows[0].merged[0].contains("to 16.6 and to 16.8"),
            "{:?}",
            rows[0].merged
        );
    }

    fn bulletin(reason: &str, who: Who, published: &str) -> (Bulletin, ServiceStatus) {
        let row = ServiceStatus {
            service: "rds".into(),
            who,
            reason: reason.into(),
            ..Default::default()
        };
        (
            Bulletin {
                id: "2026-160-AWS".into(),
                published: published.into(),
                cves: vec!["CVE-2026-98432".into()],
                services: vec![row.clone()],
                ..Default::default()
            },
            row,
        )
    }

    const OS_ROW: &str = "Long term remediation will be for you to patch your database OS. This patch is now available for you to apply.";

    #[test]
    fn an_os_update_never_gets_a_cve_and_only_a_recent_bulletin_about_the_os_is_related() {
        let (b, row) = bulletin(OS_ROW, Who::CustomerInAws, "2026-10-30");
        let os = Target {
            meaning: Meaning::OsUpdate,
            engine: "aurora-postgresql",
            running: "16.4",
            today: "2026-11-10",
            ..Default::default()
        };
        match tie(&b, &row, &os).unwrap() {
            Tie::Related { why } => assert!(why.contains("no CVE list"), "{why}"),
            t => panic!("an OS update got {t:?}"),
        }
        // Too old to be about an update pending today.
        let late = Target {
            today: "2027-06-01",
            ..os.clone()
        };
        assert_eq!(tie(&b, &row, &late), Err(NoTie::TooOld));
        // Not yet published.
        let early = Target {
            today: "2026-10-01",
            ..os.clone()
        };
        assert_eq!(tie(&b, &row, &early), Err(NoTie::NotYetPublished));
        // AWS patched it itself: nothing for the customer.
        let (b2, row2) = bulletin(OS_ROW, Who::Aws, "2026-10-30");
        assert_eq!(tie(&b2, &row2, &os), Err(NoTie::NotACustomerAction));
        // A bulletin about an engine version says nothing of an OS update.
        let (b3, row3) = bulletin(
            "Upgrade to Aurora PostgreSQL 16.4.3 or later.",
            Who::CustomerInAws,
            "2026-10-30",
        );
        assert_eq!(tie(&b3, &row3, &os), Err(NoTie::DifferentAction));
        // Hardware maintenance: no advisory at all.
        let hw = Target {
            meaning: Meaning::HardwareMaintenance,
            ..os
        };
        assert_eq!(tie(&b, &row, &hw), Err(NoTie::NoAdvisoriesForThisAction));
    }

    #[test]
    fn an_engine_action_gets_a_bulletin_only_for_its_engine_and_version_range() {
        let text = "Customers should upgrade to Aurora PostgreSQL 16.4.3 or 15.8.5, or RDS for MySQL version 8.0.40.";
        let (b, row) = bulletin(text, Who::CustomerInAws, "2026-10-30");
        assert_eq!(
            engines_fixed(text),
            vec![
                ("aurora-postgresql".to_string(), "16.4.3".to_string()),
                ("mysql".to_string(), "8.0.40".to_string())
            ]
        );
        let patch = Target {
            meaning: Meaning::EnginePatch,
            engine: "aurora-postgresql",
            running: "16.4",
            target: "16.4.3",
            today: "2026-11-10",
            ..Default::default()
        };
        match tie(&b, &row, &patch).unwrap() {
            Tie::Exact { cves, .. } => assert_eq!(cves, vec!["CVE-2026-98432"]),
            t => panic!("{t:?}"),
        }
        // The same bulletin on a MySQL upgrade that reaches the fix.
        let mysql = Target {
            meaning: Meaning::EngineUpgrade,
            engine: "mysql",
            running: "8.0.36",
            target: "8.0.40",
            today: "2026-11-10",
            ..Default::default()
        };
        assert!(matches!(tie(&b, &row, &mysql), Ok(Tie::Exact { .. })));
        // Already past the fix, a target short of it, a target not known,
        // another engine, another minor line: none get the CVE.
        for (t, why) in [
            (
                Target {
                    running: "8.0.41",
                    target: "8.0.43",
                    ..mysql.clone()
                },
                NoTie::AlreadyFixed,
            ),
            (
                Target {
                    target: "8.0.39",
                    ..mysql.clone()
                },
                NoTie::TargetDoesNotReachFix,
            ),
            (
                Target {
                    target: "",
                    ..mysql.clone()
                },
                NoTie::TargetDoesNotReachFix,
            ),
            (
                Target {
                    engine: "mariadb",
                    ..mysql.clone()
                },
                NoTie::NoVersionForEngine,
            ),
            (
                Target {
                    running: "8.4.3",
                    target: "8.4.7",
                    ..mysql.clone()
                },
                NoTie::NoVersionForEngine,
            ),
            (
                Target {
                    running: "14.9",
                    target: "14.9.2",
                    ..patch.clone()
                },
                NoTie::NoVersionForEngine,
            ),
        ] {
            assert_eq!(tie(&b, &row, &t), Err(why), "{t:?}");
        }
        // A bulletin that only speaks of the OS says nothing of an engine patch.
        let (b2, row2) = bulletin(OS_ROW, Who::CustomerInAws, "2026-10-30");
        assert_eq!(tie(&b2, &row2, &patch), Err(NoTie::NoVersionForEngine));
    }

    #[test]
    fn a_service_update_gets_a_bulletin_only_by_its_own_name() {
        let (b, mut row) = bulletin(
            "Apply the self-service update named elasticache-20261106-intel.",
            Who::CustomerInAws,
            "2026-10-30",
        );
        row.fixed.service_updates = vec!["elasticache-20261106-intel".into()];
        let t = Target {
            meaning: Meaning::ServiceUpdate,
            name: "elasticache-20261106-intel",
            today: "2026-11-10",
            ..Default::default()
        };
        assert!(matches!(tie(&b, &row, &t), Ok(Tie::Exact { .. })));
        let other = Target {
            name: "elasticache-20260901-intel",
            ..t
        };
        assert_eq!(tie(&b, &row, &other), Err(NoTie::DifferentUpdate));
    }
}
