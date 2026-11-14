//! One count of advisories: fixed, remaining and introduced.
//!
//! A dependency pull request said "N high advisories left". The package
//! manager's production audit of the same head showed fewer, every one in a
//! package the pull request does not touch. The first number was counted
//! from CoderHelm's own findings, and nothing compared it with the audit.
//!
//! Here the base branch and the head are read the same way, and the result
//! is compared with the package manager's audit before a number is written:
//!
//! - [`reconcile`] evaluates every advisory CoderHelm holds for the
//!   repository against the copies the base branch's lockfile installs and
//!   against the copies the head's lockfile installs. Withdrawn advisories
//!   are dropped and ids that are aliases of one advisory are counted once.
//!   Fixed is in the base and not in the head, remaining is in both,
//!   introduced is in the head only.
//! - The package manager's audit of the head (production dependencies, and
//!   every dependency) is then laid beside it. Where the two agree nothing
//!   is said. Where they differ the difference has a [`Cause`] and a
//!   sentence: an advisory the audit lists that CoderHelm did not hold is
//!   taken in; one CoderHelm holds that the audit does not list is shown
//!   with where CoderHelm has it from; a finding recorded for a version the
//!   lockfile never installed is set aside with the override or the stale
//!   record that explains it; an advisory only the audit of every
//!   dependency lists is development only.
//! - Every remaining advisory names its package, whether this change
//!   touches it, whether it is in production, and why it stays.
//! - Without an audit result the numbers are CoderHelm's own and are said
//!   to be: never the audit's.
//!
//! Everything here is pure; the lockfile reads and the audit are the
//! worker's.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::dep_audit::{Listed, Listings};
use crate::security_fixes::{
    clusters, compare_versions, in_range, primary_id, same_line, Finding, FindingState, Scope,
    Status,
};

/// An advisory CoderHelm holds for one package of the repository.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Held {
    /// The id it is shown by.
    pub id: String,
    /// Every id it is known by, the shown one included.
    pub ids: Vec<String>,
    pub package: String,
    /// `critical`, `high`, `moderate`, `low`, or empty.
    pub severity: String,
    /// The vulnerable ranges, as the sources wrote them.
    pub ranges: Vec<String>,
    /// The patched versions the sources name.
    pub fixed: Vec<String>,
    /// The version the finding was recorded against.
    pub recorded: String,
    pub scope: Scope,
    pub withdrawn: bool,
    /// Where CoderHelm has it from (`dependabot`, `osv`, `github_advisory`).
    pub source: String,
    pub finding: String,
}

impl Held {
    pub fn of(f: &Finding, st: &FindingState) -> Held {
        let mut ids = f.advisory.ids.clone();
        if !f.advisory.primary.is_empty() && !ids.contains(&f.advisory.primary) {
            ids.push(f.advisory.primary.clone());
        }
        let known = |id: &String| st.withdrawn.iter().any(|w| w.eq_ignore_ascii_case(id));
        let mut fixed = st.fixed.clone();
        if let Some(v) = f.fixed_version.as_ref().filter(|v| !v.is_empty()) {
            if !fixed.contains(v) {
                fixed.push(v.clone());
            }
        }
        Held {
            id: f.advisory.primary.clone(),
            withdrawn: f.status == Status::Withdrawn || (!ids.is_empty() && ids.iter().all(known)),
            ids,
            package: f.package.clone(),
            severity: st
                .severity
                .clone()
                .unwrap_or_else(|| match f.advisory.cvss {
                    Some(c) if c >= 9.0 => "critical".into(),
                    Some(c) if c >= 7.0 => "high".into(),
                    Some(c) if c >= 4.0 => "moderate".into(),
                    Some(_) => "low".into(),
                    None => String::new(),
                })
                .to_ascii_lowercase(),
            ranges: st.ranges.clone(),
            fixed,
            recorded: f.version.clone(),
            scope: f.scope,
            source: f.advisory.source.clone(),
            finding: f.id.clone(),
        }
    }

    /// Whether one installed version is vulnerable to it: inside a range
    /// the sources wrote; or, with no range, below the patched version of
    /// its line (or below every patched version, when its line has none).
    /// With neither, only the version the finding was recorded against.
    pub fn affects(&self, version: &str) -> bool {
        if !self.ranges.is_empty() {
            return self.ranges.iter().any(|r| in_range(version, r));
        }
        if self.fixed.is_empty() {
            return compare_versions(version, &self.recorded) == Ordering::Equal;
        }
        match self.fixed.iter().find(|f| same_line(version, f)) {
            Some(f) => compare_versions(version, f) == Ordering::Less,
            None => self
                .fixed
                .iter()
                .all(|f| compare_versions(version, f) == Ordering::Less),
        }
    }
}

/// Why CoderHelm's read of the lockfile and the package manager's audit
/// differ on one advisory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cause {
    /// The audit lists it and CoderHelm did not hold it: taken in.
    NotHeld,
    /// CoderHelm holds it and the audit does not list it.
    NotInAudit,
    /// The repository's audit configuration ignores it.
    Ignored,
    /// The audit lists it for a version CoderHelm's ranges put outside it.
    RangeDiffers,
    /// The finding was recorded for a version the lockfile never
    /// installed, and the manifest overrides the package.
    Override,
    /// The same, with no override: the record is behind the lockfile.
    StaleRecord,
    /// Only the audit of every dependency lists it, not the production
    /// one.
    DevelopmentOnly,
}

/// One difference, with how it was settled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Difference {
    pub id: String,
    pub package: String,
    pub cause: Cause,
    /// The difference in one sentence, with what was done about it.
    pub text: String,
}

/// Where a remaining advisory's package is installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Where {
    Production,
    Development,
    #[default]
    NotKnown,
}

impl Where {
    fn words(self) -> &'static str {
        match self {
            Where::Production => "production",
            Where::Development => "development only",
            Where::NotKnown => "production or development not known",
        }
    }
}

/// One advisory with the package it is in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Entry {
    pub id: String,
    pub package: String,
    pub severity: String,
    /// The vulnerable versions installed (the base's for a fixed one, the
    /// head's otherwise); empty when only the audit names it.
    pub versions: Vec<String>,
    pub scope: Where,
    /// This change moves the package (a remaining or introduced one).
    pub touched: bool,
    /// Why it stays (a remaining one).
    pub why: String,
}

/// How the audit stands beside the count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AuditState {
    /// The production audit and the audit of every dependency, as people
    /// read them; empty for one that gave no listing.
    pub production: String,
    pub all: String,
    /// Why a listing is missing; empty when both were read.
    pub missing: String,
    /// A listing held more advisories than the sandbox's output carries.
    pub cut: bool,
}

/// The advisories of one head, counted once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Reconciled {
    pub sha: String,
    pub lockfile: String,
    pub fixed: Vec<Entry>,
    pub remaining: Vec<Entry>,
    pub introduced: Vec<Entry>,
    pub differences: Vec<Difference>,
    pub audit: AuditState,
    /// Advisories CoderHelm held that were withdrawn: dropped.
    pub withdrawn: usize,
    /// Ids that were aliases of an advisory already counted.
    pub aliases: usize,
}

/// What [`reconcile`] reads.
#[derive(Debug, Clone, Default)]
pub struct Input<'a> {
    pub sha: &'a str,
    pub lockfile: &'a str,
    pub held: &'a [Held],
    /// The copies the base branch's lockfile and the head's install.
    pub base: &'a [(String, String)],
    pub head: &'a [(String, String)],
    /// The package manager's audit of the head; `None` when none ran.
    pub audit: Option<&'a Listings>,
    /// Why no audit ran, when none did.
    pub no_audit: &'a str,
    /// Advisory ids the repository's audit configuration ignores.
    pub ignored: &'a [String],
    /// Packages the manifest overrides.
    pub overrides: &'a [String],
    /// Why a finding stays, by finding id (a deferral's reason).
    pub stays: &'a [(String, String)],
}

fn versions_of<'a>(copies: &'a [(String, String)], package: &str) -> Vec<&'a str> {
    let mut v: Vec<&str> = copies
        .iter()
        .filter(|(n, _)| n.eq_ignore_ascii_case(package))
        .map(|(_, v)| v.as_str())
        .collect();
    v.sort_by(|a, b| compare_versions(a, b));
    v.dedup();
    v
}

fn key(id: &str, package: &str) -> (String, String) {
    (id.to_ascii_uppercase(), package.to_ascii_lowercase())
}

/// Counts the advisories of a head once (see the module notes).
pub fn reconcile(i: &Input<'_>) -> Reconciled {
    let mut out = Reconciled {
        sha: i.sha.to_string(),
        lockfile: i.lockfile.to_string(),
        ..Default::default()
    };
    // One id per advisory: aliases collapse.
    let mut records: Vec<Vec<String>> = i.held.iter().map(|h| h.ids.clone()).collect();
    let listed: Vec<&Listed> = i
        .audit
        .into_iter()
        .flat_map(|a| a.production.iter().chain(a.all.iter()))
        .flat_map(|l| l.listed.iter())
        .collect();
    records.extend(listed.iter().map(|l| vec![l.id.clone()]));
    let canon: BTreeMap<String, String> = clusters(&records)
        .into_iter()
        .flat_map(|c| {
            let p = primary_id(&c);
            c.into_iter().map(move |id| (id, p.clone()))
        })
        .collect();
    let canonical = |id: &str| -> String {
        let u = id.trim().to_ascii_uppercase();
        canon.get(&u).cloned().unwrap_or(u)
    };
    // CoderHelm's own read, base and head the same way.
    type Seen = BTreeMap<(String, String), (Vec<String>, Vec<String>, usize)>;
    let mut own: Seen = BTreeMap::new();
    let mut counted: BTreeSet<(String, String)> = BTreeSet::new();
    for (n, h) in i.held.iter().enumerate() {
        if h.withdrawn {
            out.withdrawn += 1;
            continue;
        }
        let k = key(&canonical(&h.id), &h.package);
        if !counted.insert(k.clone()) {
            out.aliases += 1;
        }
        let hit = |copies: &[(String, String)]| -> Vec<String> {
            versions_of(copies, &h.package)
                .into_iter()
                .filter(|v| h.affects(v))
                .map(String::from)
                .collect()
        };
        let (b, hd) = (hit(i.base), hit(i.head));
        // A finding recorded for a version neither lockfile installs.
        let recorded_installed = versions_of(i.base, &h.package)
            .iter()
            .any(|v| compare_versions(v, &h.recorded) == Ordering::Equal);
        if b.is_empty() && hd.is_empty() && !recorded_installed && !h.recorded.is_empty() {
            let installed = versions_of(i.head, &h.package);
            let overridden = i
                .overrides
                .iter()
                .any(|o| o.eq_ignore_ascii_case(&h.package));
            let at = if installed.is_empty() {
                "the lockfile does not install it".to_string()
            } else {
                format!("the lockfile installs {}", installed.join(", "))
            };
            out.differences.push(Difference {
                id: h.id.clone(),
                package: h.package.clone(),
                cause: if overridden {
                    Cause::Override
                } else {
                    Cause::StaleRecord
                },
                text: if overridden {
                    format!(
                        "CoderHelm's finding was recorded for `{}` {}, and {at}: the manifest overrides the package, which the source of the finding does not read. Not counted.",
                        h.package, h.recorded
                    )
                } else {
                    format!(
                        "CoderHelm's finding was recorded for `{}` {}, and {at}: the record is behind the lockfile. Not counted.",
                        h.package, h.recorded
                    )
                },
            });
            continue;
        }
        let e = own.entry(k).or_insert((vec![], vec![], n));
        for v in b {
            if !e.0.contains(&v) {
                e.0.push(v);
            }
        }
        for v in hd {
            if !e.1.contains(&v) {
                e.1.push(v);
            }
        }
    }
    // The audit of the head.
    let (prod, all) = match i.audit {
        Some(a) => (a.production.as_ref(), a.all.as_ref()),
        None => (None, None),
    };
    out.audit = AuditState {
        production: prod.map(|l| l.shown.clone()).unwrap_or_default(),
        all: all.map(|l| l.shown.clone()).unwrap_or_default(),
        missing: match i.audit {
            Some(a) => a.why.clone(),
            None if i.no_audit.is_empty() => "no audit ran on this head".to_string(),
            None => i.no_audit.to_string(),
        },
        cut: prod.is_some_and(|l| l.cut()) || all.is_some_and(|l| l.cut()),
    };
    let set_of = |l: Option<&crate::dep_audit::Listing>| -> BTreeMap<(String, String), String> {
        l.into_iter()
            .flat_map(|l| l.listed.iter())
            .map(|a| (key(&canonical(&a.id), &a.package), a.severity.clone()))
            .collect()
    };
    let (in_prod, in_all) = (set_of(prod), set_of(all));
    let audited = |k: &(String, String)| in_prod.contains_key(k) || in_all.contains_key(k);
    let scope_of = |k: &(String, String), held: Option<&Held>| -> Where {
        if in_prod.contains_key(k) {
            Where::Production
        } else if in_all.contains_key(k) && prod.is_some() {
            Where::Development
        } else {
            match held.map(|h| h.scope) {
                Some(Scope::Runtime) => Where::Production,
                Some(Scope::Dev) => Where::Development,
                _ => Where::NotKnown,
            }
        }
    };
    let touched = |package: &str| versions_of(i.base, package) != versions_of(i.head, package);
    let why_stays = |h: &Held, moved: bool| -> String {
        if let Some((_, why)) = i.stays.iter().find(|(id, _)| *id == h.finding) {
            return why.clone();
        }
        if h.fixed.is_empty() {
            "no patched release exists".to_string()
        } else if moved {
            "this change moves the package and the version installed is still below the patched one"
                .to_string()
        } else {
            "this change does not touch the package".to_string()
        }
    };
    // Whether the audit can speak for an advisory: its listing of every
    // dependency was read whole.
    let audit_complete = all.is_some_and(|l| !l.cut());
    for (k, (b, hd, n)) in &own {
        let h = &i.held[*n];
        let entry = |versions: &[String], why: String| Entry {
            id: h.id.clone(),
            package: h.package.clone(),
            severity: h.severity.clone(),
            versions: versions.to_vec(),
            scope: scope_of(k, Some(h)),
            touched: touched(&h.package),
            why,
        };
        match (b.is_empty(), hd.is_empty()) {
            (true, true) => {}
            (false, true) => {
                if audited(k) {
                    // CoderHelm's ranges say fixed; the audit still lists it.
                    out.differences.push(Difference {
                        id: h.id.clone(),
                        package: h.package.clone(),
                        cause: Cause::RangeDiffers,
                        text: format!(
                            "`{}` lists `{}` for {} on the head, and by the ranges CoderHelm holds the versions installed there ({}) are patched. Counted as remaining until the two agree.",
                            if in_prod.contains_key(k) { &out.audit.production } else { &out.audit.all },
                            h.package,
                            h.id,
                            versions_of(i.head, &h.package).join(", ")
                        ),
                    });
                    let head_versions: Vec<String> = versions_of(i.head, &h.package)
                        .into_iter()
                        .map(String::from)
                        .collect();
                    out.remaining.push(entry(
                        &head_versions,
                        "the package manager's audit still lists it on the head".to_string(),
                    ));
                } else {
                    out.fixed.push(entry(b, String::new()));
                }
            }
            (_, false) => {
                if audit_complete && !audited(k) {
                    let ignored = h
                        .ids
                        .iter()
                        .any(|id| i.ignored.iter().any(|g| g.eq_ignore_ascii_case(id)));
                    out.differences.push(Difference {
                        id: h.id.clone(),
                        package: h.package.clone(),
                        cause: if ignored { Cause::Ignored } else { Cause::NotInAudit },
                        text: if ignored {
                            format!(
                                "`{}` does not list {} in `{}` because the repository's audit configuration ignores it. CoderHelm counts it.",
                                out.audit.all, h.id, h.package
                            )
                        } else {
                            format!(
                                "CoderHelm holds {} for `{}` {} (from {}), and `{}` does not list it. CoderHelm counts it; the registry's audit does not serve every advisory.",
                                h.id,
                                h.package,
                                hd.join(", "),
                                if h.source.is_empty() { "its advisory sources" } else { &h.source },
                                out.audit.all
                            )
                        },
                    });
                }
                let e = entry(hd, why_stays(h, touched(&h.package)));
                if b.is_empty() {
                    out.introduced.push(Entry {
                        why: String::new(),
                        ..e
                    });
                } else {
                    out.remaining.push(e);
                }
            }
        }
    }
    // What only the audit names.
    let mut audit_only: BTreeMap<(String, String), String> = in_all.clone();
    for (k, sev) in &in_prod {
        audit_only.entry(k.clone()).or_insert_with(|| sev.clone());
    }
    for (k, sev) in audit_only {
        if own.contains_key(&k) {
            continue;
        }
        let listed_as = listed
            .iter()
            .find(|l| key(&canonical(&l.id), &l.package) == k)
            .map(|l| (canonical(&l.id), l.package.clone()))
            .unwrap_or((k.0.clone(), k.1.clone()));
        let (id, package) = listed_as;
        let head_versions: Vec<String> = versions_of(i.head, &package)
            .into_iter()
            .map(String::from)
            .collect();
        let base_versions = versions_of(i.base, &package);
        // New on the head: every copy the head has is one the base lacks.
        let new_here = !head_versions.is_empty()
            && head_versions
                .iter()
                .all(|v| !base_versions.iter().any(|b| b == v));
        let scope = scope_of(&k, None);
        out.differences.push(Difference {
            id: id.clone(),
            package: package.clone(),
            cause: Cause::NotHeld,
            text: format!(
                "`{}` lists {id} in `{package}`, which CoderHelm did not hold. Taken in and counted as {}.",
                if in_prod.contains_key(&k) { &out.audit.production } else { &out.audit.all },
                if new_here { "introduced by this change" } else { "remaining" }
            ),
        });
        let e = Entry {
            id,
            touched: touched(&package),
            package,
            severity: sev,
            versions: head_versions,
            scope,
            why: if new_here {
                String::new()
            } else {
                "this change does not touch the package".to_string()
            },
        };
        if new_here {
            out.introduced.push(e);
        } else {
            out.remaining.push(e);
        }
    }
    // What the production audit leaves out is development only: said once.
    if prod.is_some() {
        for e in out.remaining.iter().chain(out.introduced.iter()) {
            if e.scope == Where::Development {
                out.differences.push(Difference {
                    id: e.id.clone(),
                    package: e.package.clone(),
                    cause: Cause::DevelopmentOnly,
                    text: format!(
                        "`{}` lists {} in `{}` and `{}` does not: it is installed for development only.",
                        out.audit.all, e.id, e.package, out.audit.production
                    ),
                });
            }
        }
    }
    let order = |a: &Entry, b: &Entry| {
        (rank(&b.severity), &a.package, &a.id).cmp(&(rank(&a.severity), &b.package, &b.id))
    };
    out.fixed.sort_by(order);
    out.remaining.sort_by(order);
    out.introduced.sort_by(order);
    out.differences
        .sort_by(|a, b| (a.cause, &a.package, &a.id).cmp(&(b.cause, &b.package, &b.id)));
    out
}

/// The package an override's key is for: its parent chain and its version
/// selector are dropped (`web-kit>glob-walk@2` and `**/glob-walk` are
/// `glob-walk`, `@scope/kit@1` is `@scope/kit`).
fn override_name(key: &str) -> &str {
    let last = key.rsplit('>').next().unwrap_or(key).trim();
    let last = last.trim_start_matches("**/");
    let last = match last.rfind("/@") {
        Some(i) => &last[i + 1..],
        None if last.starts_with('@') => last,
        None => last.rsplit('/').next().unwrap_or(last),
    };
    match last.strip_prefix('@') {
        Some(rest) => match rest.find('@') {
            Some(i) => &last[..i + 1],
            None => last,
        },
        None => last.split('@').next().unwrap_or(last),
    }
}

fn override_names(table: &serde_json::Value, out: &mut Vec<String>) {
    let Some(map) = table.as_object() else {
        return;
    };
    for (key, value) in map {
        let name = override_name(key);
        if !name.is_empty() && name != "." {
            out.push(name.to_string());
        }
        // npm nests a parent's children under it.
        override_names(value, out);
    }
}

/// The packages a JavaScript manifest overrides (`overrides`,
/// `pnpm.overrides`, `resolutions`), by name, sorted.
pub fn overrides_of(manifest: &str) -> Vec<String> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(manifest) else {
        return vec![];
    };
    let mut out: Vec<String> = vec![];
    for table in [&v["overrides"], &v["pnpm"]["overrides"], &v["resolutions"]] {
        override_names(table, &mut out);
    }
    out.sort();
    out.dedup();
    out
}

/// The advisory ids a repository's audit configuration ignores: pnpm's
/// `auditConfig.ignoreGhsas` and `ignoreCves` in the manifest, and
/// `audit.ignore` in `pnpm-workspace.yaml`.
pub fn ignored_of(manifest: &str, workspace_yaml: &str) -> Vec<String> {
    let mut out: Vec<String> = vec![];
    let mut add = |id: &str| {
        let id = id.trim().to_string();
        if !id.is_empty() && !out.contains(&id) {
            out.push(id);
        }
    };
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(manifest) {
        for key in ["ignoreGhsas", "ignoreCves"] {
            for id in v["pnpm"]["auditConfig"][key]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|x| x.as_str())
            {
                add(id);
            }
        }
    }
    if let Ok(y) = serde_yaml::from_str::<serde_yaml::Value>(workspace_yaml) {
        for key in ["audit", "auditConfig"] {
            for list in ["ignore", "ignoreGhsas", "ignoreCves"] {
                for id in y[key][list]
                    .as_sequence()
                    .into_iter()
                    .flatten()
                    .filter_map(|x| x.as_str())
                {
                    add(id);
                }
            }
        }
    }
    out
}

fn rank(severity: &str) -> u8 {
    match severity {
        "critical" => 4,
        "high" => 3,
        "moderate" | "medium" => 2,
        "low" => 1,
        _ => 0,
    }
}

fn plural(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

impl Reconciled {
    /// How many remaining advisories are in production, development only,
    /// or not known.
    pub fn remaining_by_scope(&self) -> (usize, usize, usize) {
        let n = |w: Where| self.remaining.iter().filter(|e| e.scope == w).count();
        (
            n(Where::Production),
            n(Where::Development),
            n(Where::NotKnown),
        )
    }

    /// The count in one sentence, with what it was read from.
    pub fn headline(&self) -> String {
        let sha = &self.sha[..self.sha.len().min(7)];
        let from = if self.audit.all.is_empty() && self.audit.production.is_empty() {
            format!(
                "from the advisories CoderHelm holds against `{}` only ({}; the package manager's audit is not part of this count)",
                self.lockfile, self.audit.missing
            )
        } else {
            let audits: Vec<String> = [&self.audit.production, &self.audit.all]
                .into_iter()
                .filter(|a| !a.is_empty())
                .map(|a| format!("`{a}`"))
                .collect();
            format!(
                "from the advisories CoderHelm holds against `{}`, compared with {}",
                self.lockfile,
                audits.join(" and ")
            )
        };
        let (p, d, u) = self.remaining_by_scope();
        let mut parts: Vec<String> = vec![];
        if p > 0 {
            parts.push(format!("{p} in production"));
        }
        if d > 0 {
            parts.push(format!("{d} development only"));
        }
        if u > 0 {
            parts.push(format!("{u} not known which"));
        }
        format!(
            "Advisories on commit `{sha}`, the base branch and the head read the same way ({from}): {} fixed, {} remaining{}, {} introduced.",
            self.fixed.len(),
            self.remaining.len(),
            if parts.is_empty() {
                String::new()
            } else {
                format!(" ({})", parts.join(", "))
            },
            self.introduced.len()
        )
    }

    /// The count as the pull request states it.
    pub fn section(&self) -> String {
        let mut s = format!("**{}**\n", self.headline());
        let line = |e: &Entry, with_why: bool| {
            format!(
                "- {}{} in `{}`{} ({}; {}){}\n",
                e.id,
                if e.severity.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", e.severity)
                },
                e.package,
                if e.versions.is_empty() {
                    String::new()
                } else {
                    format!(" {}", e.versions.join(", "))
                },
                e.scope.words(),
                if e.touched {
                    "this change moves the package"
                } else {
                    "this change does not touch the package"
                },
                if with_why && !e.why.is_empty() {
                    format!(": {}", e.why)
                } else {
                    String::new()
                }
            )
        };
        if !self.remaining.is_empty() {
            s.push_str("Remaining:\n");
            for e in self.remaining.iter().take(25) {
                s.push_str(&line(e, true));
            }
            if self.remaining.len() > 25 {
                s.push_str(&format!("- and {} more\n", self.remaining.len() - 25));
            }
        }
        if !self.introduced.is_empty() {
            s.push_str("Introduced by this change:\n");
            for e in self.introduced.iter().take(15) {
                s.push_str(&line(e, false));
            }
        }
        if !self.differences.is_empty() {
            s.push_str(&format!(
                "Where CoderHelm's read and the package manager's audit differ ({}), each with its cause:\n",
                plural(self.differences.len(), "advisory", "advisories")
            ));
            for d in self.differences.iter().take(15) {
                s.push_str(&format!("- {}\n", d.text));
            }
            if self.differences.len() > 15 {
                s.push_str(&format!("- and {} more\n", self.differences.len() - 15));
            }
        }
        if self.audit.cut {
            s.push_str("- The audit listed more advisories than the sandbox's output carries; the ones not carried are not part of the comparison.\n");
        }
        if !self.audit.missing.is_empty()
            && !(self.audit.all.is_empty() && self.audit.production.is_empty())
        {
            s.push_str(&format!("- Not read: {}.\n", self.audit.missing));
        }
        if self.withdrawn > 0 || self.aliases > 0 {
            s.push_str(&format!(
                "- Left out of the count: {} withdrawn, {} that {} another id of an advisory already counted.\n",
                plural(self.withdrawn, "advisory", "advisories"),
                self.aliases,
                if self.aliases == 1 { "is" } else { "are" }
            ));
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dep_audit::Listing;

    fn copies(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect()
    }

    fn held(id: &str, package: &str, recorded: &str, fixed: &str) -> Held {
        Held {
            id: id.into(),
            ids: vec![id.into()],
            package: package.into(),
            severity: "high".into(),
            fixed: if fixed.is_empty() {
                vec![]
            } else {
                vec![fixed.into()]
            },
            recorded: recorded.into(),
            source: "dependabot".into(),
            finding: format!("f-{id}"),
            ..Default::default()
        }
    }

    fn listing(shown: &str, items: &[(&str, &str, &str)]) -> Listing {
        Listing {
            shown: shown.into(),
            total: items.len(),
            listed: items
                .iter()
                .map(|(id, sev, pkg)| Listed {
                    id: id.to_string(),
                    severity: sev.to_string(),
                    package: pkg.to_string(),
                })
                .collect(),
        }
    }

    fn audit(prod: &[(&str, &str, &str)], all: &[(&str, &str, &str)]) -> Listings {
        Listings {
            production: Some(listing("pnpm audit --prod --json", prod)),
            all: Some(listing("pnpm audit --json", all)),
            why: String::new(),
        }
    }

    #[test]
    fn base_and_head_are_read_the_same_way() {
        let h = [
            held("GHSA-1111-1111-1111", "body-kit", "1.2.0", "1.2.9"),
            held("GHSA-2222-2222-2222", "left-pad-x", "3.0.0", "3.0.4"),
        ];
        let base = copies(&[("body-kit", "1.2.0"), ("left-pad-x", "3.0.0")]);
        let head = copies(&[("body-kit", "1.2.9"), ("left-pad-x", "3.0.0")]);
        let a = audit(
            &[("GHSA-2222-2222-2222", "high", "left-pad-x")],
            &[("GHSA-2222-2222-2222", "high", "left-pad-x")],
        );
        let r = reconcile(&Input {
            sha: "abcdef1234",
            lockfile: "pnpm-lock.yaml",
            held: &h,
            base: &base,
            head: &head,
            audit: Some(&a),
            ..Default::default()
        });
        assert_eq!(r.fixed.len(), 1);
        assert_eq!(r.fixed[0].package, "body-kit");
        assert_eq!(r.remaining.len(), 1);
        let left = &r.remaining[0];
        assert_eq!(left.package, "left-pad-x");
        assert_eq!(left.scope, Where::Production);
        assert!(!left.touched);
        assert_eq!(left.why, "this change does not touch the package");
        // The two agree: nothing to explain.
        assert!(r.differences.is_empty());
        assert_eq!(
            r.headline(),
            "Advisories on commit `abcdef1`, the base branch and the head read the same way (from the advisories CoderHelm holds against `pnpm-lock.yaml`, compared with `pnpm audit --prod --json` and `pnpm audit --json`): 1 fixed, 1 remaining (1 in production), 0 introduced."
        );
    }

    #[test]
    fn the_review_case_fewer_in_production_and_none_in_a_touched_package() {
        // CoderHelm's findings said three high advisories were left. One is
        // recorded for a version an override never installs, one sits in a
        // development tool, one is in a production package the change does
        // not touch.
        let h = [
            held("GHSA-AAAA-AAAA-AAAA", "glob-walk", "2.3.1", "2.3.9"),
            held("GHSA-BBBB-BBBB-BBBB", "lint-kit", "8.0.0", "9.0.0"),
            held("GHSA-CCCC-CCCC-CCCC", "xml-read", "1.1.0", ""),
        ];
        let base = copies(&[
            ("glob-walk", "2.4.0"),
            ("lint-kit", "8.0.0"),
            ("xml-read", "1.1.0"),
        ]);
        let head = base.clone();
        let a = audit(
            &[("GHSA-CCCC-CCCC-CCCC", "high", "xml-read")],
            &[
                ("GHSA-CCCC-CCCC-CCCC", "high", "xml-read"),
                ("GHSA-BBBB-BBBB-BBBB", "high", "lint-kit"),
            ],
        );
        let overrides = vec!["glob-walk".to_string()];
        let r = reconcile(&Input {
            sha: "abcdef1",
            lockfile: "pnpm-lock.yaml",
            held: &h,
            base: &base,
            head: &head,
            audit: Some(&a),
            overrides: &overrides,
            ..Default::default()
        });
        assert_eq!(r.remaining.len(), 2);
        assert_eq!(r.remaining_by_scope(), (1, 1, 0));
        assert!(r.remaining.iter().all(|e| !e.touched));
        let xml = r
            .remaining
            .iter()
            .find(|e| e.package == "xml-read")
            .unwrap();
        assert_eq!(xml.why, "no patched release exists");
        // Each disagreement has its cause.
        let causes: Vec<Cause> = r.differences.iter().map(|d| d.cause).collect();
        assert_eq!(causes, vec![Cause::Override, Cause::DevelopmentOnly]);
        assert!(r.differences[0].text.contains(
            "recorded for `glob-walk` 2.3.1, and the lockfile installs 2.4.0: the manifest overrides the package"
        ));
        assert!(r.differences[1]
            .text
            .contains("it is installed for development only"));
        let text = r.section();
        assert!(text.contains("2 remaining (1 in production, 1 development only)"));
        assert!(text.contains(
            "- GHSA-CCCC-CCCC-CCCC (high) in `xml-read` 1.1.0 (production; this change does not touch the package): no patched release exists"
        ));
    }

    #[test]
    fn each_disagreement_is_settled_or_shown_with_its_cause() {
        let h = [
            // Held, and the audit does not list it.
            held("OSV-2026-77", "odd-parser", "0.4.0", "0.5.0"),
            // Held, ignored by the repository's audit configuration.
            held("GHSA-DDDD-DDDD-DDDD", "old-tool", "1.0.0", "2.0.0"),
            // CoderHelm's range says patched; the audit still lists it.
            Held {
                ranges: vec![">= 4.0.0, < 4.2.0".into()],
                ..held("GHSA-EEEE-EEEE-EEEE", "net-kit", "4.1.0", "4.2.0")
            },
            // Recorded for a version the lockfile never had, no override.
            held("GHSA-FFFF-FFFF-FFFF", "ghost", "0.1.0", "0.1.5"),
        ];
        let base = copies(&[
            ("odd-parser", "0.4.0"),
            ("old-tool", "1.0.0"),
            ("net-kit", "4.1.0"),
            ("ghost", "0.2.0"),
            ("fresh", "1.0.0"),
            ("quiet", "7.0.0"),
        ]);
        let head = copies(&[
            ("odd-parser", "0.4.0"),
            ("old-tool", "1.0.0"),
            ("net-kit", "4.2.0"),
            ("ghost", "0.2.0"),
            ("fresh", "2.0.0"),
            ("quiet", "7.0.0"),
        ]);
        let a = audit(
            &[
                ("GHSA-EEEE-EEEE-EEEE", "high", "net-kit"),
                // In a version this change brought in: introduced.
                ("GHSA-9999-9999-9999", "critical", "fresh"),
                // There before, never held: taken in as remaining.
                ("GHSA-8888-8888-8888", "low", "quiet"),
            ],
            &[
                ("GHSA-EEEE-EEEE-EEEE", "high", "net-kit"),
                ("GHSA-9999-9999-9999", "critical", "fresh"),
                ("GHSA-8888-8888-8888", "low", "quiet"),
            ],
        );
        let ignored = vec!["GHSA-dddd-dddd-dddd".to_string()];
        let r = reconcile(&Input {
            sha: "abc",
            lockfile: "pnpm-lock.yaml",
            held: &h,
            base: &base,
            head: &head,
            audit: Some(&a),
            ignored: &ignored,
            ..Default::default()
        });
        let cause_of = |pkg: &str| -> Vec<Cause> {
            r.differences
                .iter()
                .filter(|d| d.package == pkg)
                .map(|d| d.cause)
                .collect()
        };
        assert_eq!(cause_of("odd-parser"), vec![Cause::NotInAudit]);
        assert_eq!(cause_of("old-tool"), vec![Cause::Ignored]);
        assert_eq!(cause_of("net-kit"), vec![Cause::RangeDiffers]);
        assert_eq!(cause_of("ghost"), vec![Cause::StaleRecord]);
        assert_eq!(cause_of("fresh"), vec![Cause::NotHeld]);
        assert_eq!(cause_of("quiet"), vec![Cause::NotHeld]);
        // Nothing is silently one number: what the audit names and
        // CoderHelm did not hold is counted, what the audit still lists is
        // not called fixed, and the stale record is not counted at all.
        assert!(r.fixed.is_empty());
        let names = |v: &[Entry]| -> Vec<String> { v.iter().map(|e| e.package.clone()).collect() };
        assert_eq!(names(&r.introduced), vec!["fresh"]);
        assert_eq!(
            names(&r.remaining),
            vec!["net-kit", "odd-parser", "old-tool", "quiet"]
        );
        assert!(r
            .section()
            .contains("Where CoderHelm's read and the package manager's audit differ (6 advisories), each with its cause:"));
    }

    #[test]
    fn withdrawn_advisories_are_dropped_and_aliases_count_once() {
        let h = [
            Held {
                withdrawn: true,
                ..held("GHSA-1111-2222-3333", "pkg-a", "1.0.0", "1.0.1")
            },
            Held {
                ids: vec!["CVE-2026-0001".into(), "GHSA-4444-5555-6666".into()],
                ..held("CVE-2026-0001", "pkg-b", "2.0.0", "2.0.3")
            },
            // The same advisory under its other id, from another source.
            held("GHSA-4444-5555-6666", "pkg-b", "2.0.0", "2.0.3"),
        ];
        let base = copies(&[("pkg-a", "1.0.0"), ("pkg-b", "2.0.0")]);
        let a = audit(
            &[("GHSA-4444-5555-6666", "high", "pkg-b")],
            &[("GHSA-4444-5555-6666", "high", "pkg-b")],
        );
        let r = reconcile(&Input {
            sha: "abc",
            lockfile: "package-lock.json",
            held: &h,
            base: &base,
            head: &base,
            audit: Some(&a),
            ..Default::default()
        });
        assert_eq!((r.withdrawn, r.aliases), (1, 1));
        assert_eq!(r.remaining.len(), 1);
        // The audit's GHSA id is the held CVE's alias: they agree.
        assert!(r.differences.is_empty());
        assert!(r.section().contains(
            "Left out of the count: 1 advisory withdrawn, 1 that is another id of an advisory already counted."
        ));
        // A finding whose advisory was withdrawn is read as withdrawn.
        let f = Finding {
            status: Status::Withdrawn,
            ..Default::default()
        };
        assert!(Held::of(&f, &FindingState::default()).withdrawn);
    }

    #[test]
    fn without_an_audit_the_numbers_are_said_to_be_coderhelms_own() {
        let h = [held("GHSA-1111-1111-1111", "body-kit", "1.2.0", "1.2.9")];
        let base = copies(&[("body-kit", "1.2.0")]);
        let r = reconcile(&Input {
            sha: "abcdef1",
            lockfile: "poetry.lock",
            held: &h,
            base: &base,
            head: &base,
            no_audit: "CoderHelm has no production-scoped audit for poetry",
            ..Default::default()
        });
        assert_eq!(r.remaining.len(), 1);
        assert_eq!(r.remaining[0].scope, Where::NotKnown);
        // Never a disagreement invented from an audit that did not run.
        assert!(r.differences.is_empty());
        assert!(r.headline().contains(
            "from the advisories CoderHelm holds against `poetry.lock` only (CoderHelm has no production-scoped audit for poetry; the package manager's audit is not part of this count)"
        ));
        // A listing cut short can't speak for what it does not carry.
        let mut a = audit(&[], &[]);
        a.all.as_mut().unwrap().total = 60;
        let r = reconcile(&Input {
            sha: "abc",
            lockfile: "pnpm-lock.yaml",
            held: &h,
            base: &base,
            head: &base,
            audit: Some(&a),
            ..Default::default()
        });
        assert!(r.differences.is_empty());
        assert!(r.audit.cut);
    }

    #[test]
    fn a_version_is_judged_by_the_range_or_the_patched_version_of_its_line() {
        let h = Held {
            fixed: vec!["1.2.5".into(), "2.0.3".into()],
            ..held("X", "p", "1.0.0", "")
        };
        assert!(h.affects("1.1.0"));
        assert!(!h.affects("1.2.5"));
        assert!(h.affects("2.0.1"));
        assert!(!h.affects("2.0.4"));
        assert!(!h.affects("3.0.0"));
        assert!(h.affects("0.9.0"));
        let ranged = Held {
            ranges: vec![">= 2.0.0, < 2.0.3".into()],
            ..h.clone()
        };
        assert!(!ranged.affects("1.1.0"));
        assert!(ranged.affects("2.0.2"));
        // Neither a range nor a patched version: only the recorded one.
        let bare = held("Y", "p", "1.0.0", "");
        assert!(bare.affects("1.0.0"));
        assert!(!bare.affects("1.0.1"));
    }

    #[test]
    fn overrides_and_ignores_are_read_from_the_repository() {
        let manifest = r#"{
            "overrides": {"glob-walk@2": "^2.3.9", "web-kit": {"upload-kit": "2.0.2"}},
            "pnpm": {
                "overrides": {"web-kit>path-match@3": "^3.1.4", "@scope/kit@1": "1.2.3", "@scope/plain": "2.0.0"},
                "auditConfig": {"ignoreGhsas": ["GHSA-aaaa-bbbb-cccc"], "ignoreCves": ["CVE-2026-1"]}
            },
            "resolutions": {"**/deep-kit": "1.0.1"}
        }"#;
        assert_eq!(
            overrides_of(manifest),
            vec![
                "@scope/kit",
                "@scope/plain",
                "deep-kit",
                "glob-walk",
                "path-match",
                "upload-kit",
                "web-kit"
            ]
        );
        assert!(overrides_of("not json").is_empty());
        assert_eq!(
            ignored_of(manifest, "audit:\n  ignore:\n    - GHSA-dddd-eeee-ffff\n"),
            vec!["GHSA-aaaa-bbbb-cccc", "CVE-2026-1", "GHSA-dddd-eeee-ffff"]
        );
    }
}
