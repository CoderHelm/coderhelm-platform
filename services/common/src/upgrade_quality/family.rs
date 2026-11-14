//! Packages released together move together.
//!
//! A dependency pull request raised one package of a framework to 10.4.22
//! and left its siblings from the same source repository at 10.4.15. Each
//! bump was correct on its own advisory; together they installed a mix of
//! releases the framework never shipped as one.
//!
//! A family is found from what the registry and the lockfile say, never
//! from a list of names kept here:
//!
//! - the registry record of each package names its source repository
//!   (`upgrade_notes::source_repo`), and the base branch's lockfile says
//!   which of them it installs at the same version. Same repository and the
//!   same version on the base branch is a lockstep family;
//! - the release a bump moves to declares peer ranges. A range that holds a
//!   sibling to one minor line makes that sibling a member;
//! - a shared scope or name stem is supporting evidence only: it orders
//!   which packages are worth a registry read ([`candidates`]) and is shown
//!   beside the evidence that decides.
//!
//! Packages of one repository at different versions on the base branch are
//! versioned on their own: named ([`Independent`]), never held together.
//!
//! No grouping data of another project is used. The public grouping tables
//! of dependency update tools are part of projects under licences this
//! codebase does not take data from; everything here is derived from the
//! registry's metadata and the repository's lockfile.
//!
//! Everything here is pure; the registry reads are the worker's
//! (`worker::passes::upgrade_quality`).

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::dep_fix::{Companion, DepPlan};
use crate::security_fixes::{compare_versions, is_major_bump, version_line};
use crate::upgrade_notes::range_admits;

/// Registry reads one bumped package's family search makes at most.
pub const MAX_CANDIDATES: usize = 12;
/// A version this many packages of a lockfile share says nothing by itself
/// (`1.0.0`): only packages related by name are then read.
const COMMON_VERSION: usize = 25;

/// Why packages are taken to be one family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Evidence {
    /// The registry records of the members name one source repository.
    SameSource { repo: String },
    /// The base branch's lockfile installs every member at this version.
    EqualOnBase { version: String },
    /// The release `from` moves to holds `on` to one minor line.
    PeerPin {
        from: String,
        on: String,
        range: String,
    },
    /// The members share a scope or a name stem. Supporting only.
    NamePattern { pattern: String },
}

impl Evidence {
    pub fn line(&self) -> String {
        match self {
            Evidence::SameSource { repo } => {
                format!("the registry names `{repo}` as the source repository of each")
            }
            Evidence::EqualOnBase { version } => {
                format!("the base branch's lockfile installs each at {version}")
            }
            Evidence::PeerPin { from, on, range } => {
                format!("`{from}` declares the peer range `{range}` for `{on}`")
            }
            Evidence::NamePattern { pattern } => format!("the names share `{pattern}`"),
        }
    }
}

/// What is known of one package of the lockfile.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Known {
    pub name: String,
    /// `owner/name` of its source repository, lowercase; `None` when the
    /// registry record was not read or names none.
    pub source: Option<String>,
}

/// A package of the bumped one's source repository the base branch has at
/// another version: versioned on its own, not held to the family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Independent {
    pub name: String,
    pub version: String,
}

/// Packages released in lockstep, in one lockfile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Family {
    /// The source repository (`owner/name`), or `peers of <package>`.
    pub key: String,
    pub lockfile: String,
    /// The version every member has on the base branch.
    pub base_version: String,
    /// Every member, the bumped one included, by name.
    pub members: Vec<String>,
    pub evidence: Vec<Evidence>,
    /// The version the family moves to; empty when it does not move as one.
    pub target: String,
    /// Why it does not move as one; empty when it does. A family with a
    /// reason here is allowed to differ on the head, and the reason is
    /// shown.
    pub held: String,
    /// Packages of the same repository at other versions on the base
    /// branch.
    pub independent: Vec<Independent>,
}

impl Family {
    pub fn evidence_line(&self) -> String {
        self.evidence
            .iter()
            .map(Evidence::line)
            .collect::<Vec<_>>()
            .join("; ")
    }
}

/// The scope of a scoped name (`@scope/x` is `@scope`), or the first word
/// of a plain one (`pkg-core` is `pkg`, `pkg.core` is `pkg`).
fn stem(name: &str) -> &str {
    if let Some(rest) = name.strip_prefix('@') {
        return match rest.find('/') {
            Some(i) => &name[..i + 1],
            None => name,
        };
    }
    // A Maven coordinate's group, a Go module's repository.
    if let Some((group, _)) = name.split_once(':') {
        return group;
    }
    let end = name.find(['-', '.', '_', '/']).unwrap_or(name.len());
    &name[..end]
}

/// What two names share, when they share something: `@scope`, or a stem of
/// three letters or more. A scope named after a plain package counts
/// (`runner` and `@runner/coverage`).
pub fn name_pattern(a: &str, b: &str) -> Option<String> {
    let (sa, sb) = (stem(a), stem(b));
    let bare = |s: &str| s.trim_start_matches('@').to_ascii_lowercase();
    if a.eq_ignore_ascii_case(b) || bare(sa).len() < 3 {
        return None;
    }
    if bare(sa) != bare(sb) {
        return None;
    }
    Some(if sa.starts_with('@') {
        format!("{sa}/")
    } else if sb.starts_with('@') {
        format!("{sb}/")
    } else {
        sa.to_string()
    })
}

/// The packages of the base branch's lockfile worth a registry read to
/// find `package`'s family, at most [`MAX_CANDIDATES`]: the ones it
/// installs at the same version (related names first), then the related
/// names at other versions (they are named as versioned on their own). A
/// version half the lockfile shares brings in related names only.
pub fn candidates(package: &str, from: &str, base: &[(String, String)]) -> Vec<String> {
    let mut names: Vec<(&str, bool)> = base
        .iter()
        .filter(|(n, _)| !n.eq_ignore_ascii_case(package))
        .map(|(n, v)| (n.as_str(), compare_versions(v, from) == Ordering::Equal))
        .collect();
    names.sort();
    names.dedup();
    let same: Vec<&str> = names
        .iter()
        .filter(|(_, eq)| *eq)
        .map(|(n, _)| *n)
        .collect();
    let common = same.len() > COMMON_VERSION;
    let related = |n: &&str| name_pattern(package, n).is_some();
    let mut out: Vec<&str> = same.iter().copied().filter(related).collect();
    if !common {
        out.extend(same.iter().copied().filter(|n| !related(n)));
    }
    out.extend(
        names
            .iter()
            .filter(|(n, eq)| !*eq && related(n) && !same.contains(n))
            .map(|(n, _)| *n),
    );
    out.dedup();
    out.into_iter()
        .take(MAX_CANDIDATES)
        .map(String::from)
        .collect()
}

/// A peer range that holds its package to one minor line: an exact
/// version, a tilde range, or `1.2.x`. A caret or an open range lets the
/// peer move on its own.
pub fn pins(range: &str) -> bool {
    let r = range.trim();
    if r.is_empty() || r.contains("||") || r.contains(' ') || r == "*" {
        return false;
    }
    let body = r.trim_start_matches(['=', 'v']);
    if let Some(t) = r.strip_prefix('~') {
        return !t.starts_with('>') && t.matches('.').count() >= 1;
    }
    if r.starts_with(['^', '>', '<']) {
        return false;
    }
    let parts: Vec<&str> = body.split('.').collect();
    match parts.as_slice() {
        [a, b, c] => {
            let num = |s: &str| !s.is_empty() && s.chars().all(|ch| ch.is_ascii_digit());
            num(a)
                && num(b)
                && (c.eq_ignore_ascii_case("x")
                    || *c == "*"
                    || c.chars().next().is_some_and(|ch| ch.is_ascii_digit()))
        }
        _ => false,
    }
}

/// The family of `package` (at `from` on the base branch, in `lockfile`):
/// the packages the base branch's lockfile has at the same version whose
/// registry record names the same source repository, and the packages the
/// release it moves to pins as peers (`peers`: peer name and range).
/// `known`: what the registry said of `package` and of each candidate.
/// `None` when it has no family; a package with only independently
/// versioned siblings gets a family of itself, so they are named.
pub fn detect(
    package: &str,
    from: &str,
    lockfile: &str,
    base: &[(String, String)],
    known: &[Known],
    peers: &[(String, String)],
) -> Option<Family> {
    let source = known
        .iter()
        .find(|k| k.name.eq_ignore_ascii_case(package))
        .and_then(|k| k.source.clone());
    let installed = |name: &str| -> Vec<&str> {
        base.iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
            .collect()
    };
    let mut members: BTreeSet<String> = BTreeSet::new();
    let mut evidence: Vec<Evidence> = vec![];
    let mut independent: Vec<Independent> = vec![];
    if let Some(repo) = &source {
        for k in known {
            if k.name.eq_ignore_ascii_case(package) || k.source.as_ref() != Some(repo) {
                continue;
            }
            let versions = installed(&k.name);
            if versions
                .iter()
                .any(|v| compare_versions(v, from) == Ordering::Equal)
            {
                members.insert(k.name.clone());
            } else if let Some(v) = versions.first() {
                independent.push(Independent {
                    name: k.name.clone(),
                    version: v.to_string(),
                });
            }
        }
        if !members.is_empty() {
            evidence.push(Evidence::SameSource { repo: repo.clone() });
            evidence.push(Evidence::EqualOnBase {
                version: from.to_string(),
            });
        }
    }
    for (peer, range) in peers {
        if !pins(range) || installed(peer).is_empty() || peer.eq_ignore_ascii_case(package) {
            continue;
        }
        members.insert(peer.clone());
        evidence.push(Evidence::PeerPin {
            from: package.to_string(),
            on: peer.clone(),
            range: range.clone(),
        });
    }
    if members.is_empty() && independent.is_empty() {
        return None;
    }
    let mut patterns: Vec<String> = members
        .iter()
        .filter_map(|m| name_pattern(package, m))
        .collect();
    patterns.sort();
    patterns.dedup();
    for pattern in patterns {
        evidence.push(Evidence::NamePattern { pattern });
    }
    let key = match (&source, evidence.first()) {
        (Some(repo), Some(Evidence::SameSource { .. })) | (Some(repo), None) => repo.clone(),
        _ => format!("peers of {package}"),
    };
    members.insert(package.to_string());
    independent.sort_by(|a, b| a.name.cmp(&b.name));
    Some(Family {
        key,
        lockfile: lockfile.to_string(),
        base_version: from.to_string(),
        members: members.into_iter().collect(),
        evidence,
        independent,
        ..Default::default()
    })
}

/// The version a family moves to: the lowest of `wanted` (the versions
/// that fix the advisory, lowest first) every member publishes.
/// `published`: per member, which of `wanted` its registry has; a member
/// missing from it was not read. `Err` says why there is none.
pub fn target(
    family: &Family,
    wanted: &[String],
    published: &BTreeMap<String, Vec<String>>,
) -> Result<String, String> {
    if family.members.len() < 2 {
        return Err("it has no member released with it".to_string());
    }
    let mut wanted: Vec<&String> = wanted.iter().collect();
    wanted.sort_by(|a, b| compare_versions(a, b));
    wanted.dedup();
    let unread: Vec<&String> = family
        .members
        .iter()
        .filter(|m| !published.contains_key(*m))
        .collect();
    if let Some(m) = unread.first() {
        return Err(format!(
            "the registry could not be read for `{m}`, so no version every member publishes is known"
        ));
    }
    for v in &wanted {
        if family
            .members
            .iter()
            .all(|m| published[m].iter().any(|p| p == *v))
        {
            return Ok((*v).clone());
        }
    }
    let lacking: Vec<String> = family
        .members
        .iter()
        .filter(|m| {
            wanted
                .first()
                .is_some_and(|v| !published[*m].iter().any(|p| p == *v))
        })
        .map(|m| format!("`{m}`"))
        .collect();
    Err(match wanted.first() {
        Some(v) => format!(
            "{} publish{} no {v}, the lowest version that fixes the advisory",
            lacking.join(", "),
            if lacking.len() == 1 { "es" } else { "" }
        ),
        None => "no fixed version is known for it".to_string(),
    })
}

/// Brings each family of `families` into the plan: the first bump of a
/// member carries the others as companions at the family's target, so one
/// probe, one commit and one deferral move the family or none of it. A
/// second bump of the same family folds into the first (its findings with
/// it). A target that would newly cross a major version is not taken: the
/// family is held with that reason, and the bump goes alone.
pub fn attach(plan: &mut DepPlan, families: &mut [Family]) {
    for fam in families.iter_mut() {
        let in_family = |b: &crate::dep_fix::PlanBump| {
            b.manifest == fam.lockfile
                && fam
                    .members
                    .iter()
                    .any(|m| m.eq_ignore_ascii_case(&b.package))
                && version_line(&b.from) == version_line(&fam.base_version)
        };
        let Some(first) = plan.bumps.iter().position(in_family) else {
            continue;
        };
        if fam.target.is_empty() || !fam.held.is_empty() {
            plan.bumps[first].family = fam.key.clone();
            plan.bumps[first].family_note = if fam.held.is_empty() {
                "its family has no version to move to together".to_string()
            } else {
                fam.held.clone()
            };
            continue;
        }
        let crosses = plan
            .bumps
            .iter()
            .filter(|b| in_family(b))
            .any(|b| !b.major && is_major_bump(&b.from, &fam.target));
        if crosses {
            fam.held = format!(
                "the lowest version every member publishes, {}, is a major version away, and the advisory is fixed inside the current one",
                fam.target
            );
            fam.target.clear();
            plan.bumps[first].family = fam.key.clone();
            plan.bumps[first].family_note = fam.held.clone();
            continue;
        }
        // Fold the family's other bumps into the first.
        let mut folded: Vec<crate::dep_fix::PlanBump> = vec![];
        let mut i = first + 1;
        while i < plan.bumps.len() {
            if in_family(&plan.bumps[i]) {
                folded.push(plan.bumps.remove(i));
            } else {
                i += 1;
            }
        }
        let primary = &mut plan.bumps[first];
        primary.family = fam.key.clone();
        if compare_versions(&fam.target, &primary.to) == Ordering::Greater {
            primary.to = fam.target.clone();
        }
        for other in folded {
            for id in other.finding_ids {
                if !primary.finding_ids.contains(&id) {
                    primary.finding_ids.push(id);
                }
            }
            // The family is verified as its riskiest member is.
            if other.major && !primary.major {
                primary.major = true;
                primary.major_reason = other.major_reason;
                primary.risk = other.risk;
            } else if !other.major
                && !primary.major
                && primary.risk() > other.risk.unwrap_or(crate::dep_fix::Risk::Safe)
            {
                primary.risk = other.risk;
            }
        }
        let line = primary.line.clone();
        for m in &fam.members {
            if m.eq_ignore_ascii_case(&primary.package) {
                continue;
            }
            primary.companions.push(Companion {
                package: m.clone(),
                from: fam.base_version.clone(),
                to: fam.target.clone(),
                line: line.clone(),
            });
        }
        primary.family_note = format!(
            "moves with {} to {} ({})",
            primary
                .companions
                .iter()
                .map(|c| format!("`{}`", c.package))
                .collect::<Vec<_>>()
                .join(", "),
            fam.target,
            fam.evidence_line()
        );
    }
}

/// A family the head installs at more than one version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Split {
    pub family: String,
    /// Each member and the version the head has it at.
    pub at: Vec<(String, String)>,
}

impl Split {
    pub fn line(&self) -> String {
        let at: Vec<String> = self
            .at
            .iter()
            .map(|(m, v)| format!("`{m}` at {v}"))
            .collect();
        format!(
            "the family `{}` is split on the head: {}",
            self.family,
            at.join(", ")
        )
    }
}

/// Where a member that was at `base_version` stands on the head: still
/// there, or the lowest version the head has above it.
fn member_at(name: &str, base_version: &str, head: &[(String, String)]) -> Option<String> {
    let versions: Vec<&str> = head
        .iter()
        .filter(|(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
        .collect();
    if versions
        .iter()
        .any(|v| compare_versions(v, base_version) == Ordering::Equal)
    {
        return Some(base_version.to_string());
    }
    versions
        .iter()
        .copied()
        .filter(|v| compare_versions(v, base_version) == Ordering::Greater)
        .min_by(|a, b| compare_versions(a, b))
        .or_else(|| {
            versions
                .iter()
                .copied()
                .min_by(|a, b| compare_versions(a, b))
        })
        .map(String::from)
}

/// The families the head installs at more than one version. A family held
/// for a recorded reason is allowed to differ and is not returned; a member
/// the head no longer installs is not part of the comparison.
pub fn splits(families: &[Family], lockfile: &str, head: &[(String, String)]) -> Vec<Split> {
    let mut out = vec![];
    for fam in families {
        if fam.lockfile != lockfile || !fam.held.is_empty() || fam.members.len() < 2 {
            continue;
        }
        let at: Vec<(String, String)> = fam
            .members
            .iter()
            .filter_map(|m| member_at(m, &fam.base_version, head).map(|v| (m.clone(), v)))
            .collect();
        let distinct: BTreeSet<&str> = at.iter().map(|(_, v)| v.as_str()).collect();
        if distinct.len() > 1 {
            out.push(Split {
                family: fam.key.clone(),
                at,
            });
        }
    }
    out
}

/// One peer range a bumped release declares.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PeerRange {
    pub from: String,
    pub on: String,
    pub range: String,
}

/// The peer ranges no copy on the head satisfies, each as a sentence. A
/// peer the head does not install, or a range that can't be evaluated, is
/// not judged.
pub fn peers_unmet(ecosystem: &str, peers: &[PeerRange], head: &[(String, String)]) -> Vec<String> {
    let mut out = vec![];
    for p in peers {
        let versions: Vec<&str> = head
            .iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case(&p.on))
            .map(|(_, v)| v.as_str())
            .collect();
        if versions.is_empty() {
            continue;
        }
        let verdicts: Vec<Option<bool>> = versions
            .iter()
            .map(|v| range_admits(ecosystem, &p.range, v))
            .collect();
        if verdicts.iter().all(|v| *v == Some(false)) {
            out.push(format!(
                "`{}` declares the peer range `{}` for `{}`, and the head installs it at {}",
                p.from,
                p.range,
                p.on,
                versions.join(", ")
            ));
        }
    }
    out
}

/// The families as the pull request states them.
pub fn section(families: &[Family], splits: &[Split], peers: &[String]) -> String {
    if families.is_empty() && peers.is_empty() {
        return String::new();
    }
    let mut s = String::from(
        "**Families released together** (from the registry's source repository of each package and the versions in the base branch's lockfile):\n",
    );
    for f in families {
        let members: Vec<String> = f.members.iter().map(|m| format!("`{m}`")).collect();
        if f.members.len() < 2 {
            // Only independently versioned siblings.
        } else if !f.target.is_empty() && f.held.is_empty() {
            let split = splits.iter().find(|x| x.family == f.key);
            s.push_str(&format!(
                "- {} move together from {} to {}: {}. {}\n",
                members.join(", "),
                f.base_version,
                f.target,
                f.evidence_line(),
                match split {
                    Some(x) => format!("Not coherent: {}.", x.line()),
                    None => "The head installs every member at one version.".to_string(),
                }
            ));
        } else {
            s.push_str(&format!(
                "- {} are one family ({}) and do not move together here: {}.\n",
                members.join(", "),
                f.evidence_line(),
                if f.held.is_empty() {
                    "no version to move to together was found"
                } else {
                    f.held.as_str()
                }
            ));
        }
        if !f.independent.is_empty() {
            let named: Vec<String> = f
                .independent
                .iter()
                .take(6)
                .map(|i| format!("`{}` {}", i.name, i.version))
                .collect();
            s.push_str(&format!(
                "- Same source repository `{}`, at other versions on the base branch, so versioned on their own and left where they are: {}{}.\n",
                f.key,
                named.join(", "),
                if f.independent.len() > 6 {
                    format!(" and {} more", f.independent.len() - 6)
                } else {
                    String::new()
                }
            ));
        }
    }
    for p in peers {
        s.push_str(&format!("- Peer range not met: {p}.\n"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dep_fix::PlanBump;

    fn copies(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect()
    }

    fn known(name: &str, repo: &str) -> Known {
        Known {
            name: name.into(),
            source: (!repo.is_empty()).then(|| repo.to_string()),
        }
    }

    fn base() -> Vec<(String, String)> {
        copies(&[
            ("@lattice/common", "10.4.15"),
            ("@lattice/core", "10.4.15"),
            ("@lattice/platform-http", "10.4.15"),
            ("@lattice/cli", "10.2.0"),
            ("left-trim", "10.4.15"),
            ("zeta", "1.0.0"),
        ])
    }

    fn lattice() -> Family {
        detect(
            "@lattice/common",
            "10.4.15",
            "pnpm-lock.yaml",
            &base(),
            &[
                known("@lattice/common", "lattice-js/lattice"),
                known("@lattice/core", "lattice-js/lattice"),
                known("@lattice/platform-http", "lattice-js/lattice"),
                known("@lattice/cli", "lattice-js/lattice"),
                known("left-trim", "someone/left-trim"),
            ],
            &[],
        )
        .unwrap()
    }

    #[test]
    fn a_family_is_the_same_repository_at_the_same_version_on_the_base() {
        let f = lattice();
        assert_eq!(f.key, "lattice-js/lattice");
        assert_eq!(
            f.members,
            vec!["@lattice/common", "@lattice/core", "@lattice/platform-http"]
        );
        // The unrelated package at the same version is not a member: the
        // registry names another repository for it.
        assert!(!f.members.iter().any(|m| m == "left-trim"));
        // The scope is supporting evidence, after the two that decide.
        assert_eq!(
            f.evidence,
            vec![
                Evidence::SameSource {
                    repo: "lattice-js/lattice".into()
                },
                Evidence::EqualOnBase {
                    version: "10.4.15".into()
                },
                Evidence::NamePattern {
                    pattern: "@lattice/".into()
                },
            ]
        );
    }

    #[test]
    fn a_sibling_at_another_version_is_versioned_on_its_own() {
        let f = lattice();
        assert_eq!(
            f.independent,
            vec![Independent {
                name: "@lattice/cli".into(),
                version: "10.2.0".into()
            }]
        );
        // A repository whose packages all differ on the base is no lockstep
        // family: one type-definitions repository publishing hundreds of
        // packages on their own versions.
        let types = detect(
            "@defs/alpha",
            "18.2.1",
            "package-lock.json",
            &copies(&[("@defs/alpha", "18.2.1"), ("@defs/beta", "4.17.3")]),
            &[
                known("@defs/alpha", "defs/definitions"),
                known("@defs/beta", "defs/definitions"),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(types.members, vec!["@defs/alpha"]);
        assert_eq!(types.independent.len(), 1);
        // Nothing is held together, so nothing can be split.
        let head = copies(&[("@defs/alpha", "18.2.5"), ("@defs/beta", "4.17.3")]);
        assert!(splits(&[types], "package-lock.json", &head).is_empty());
    }

    #[test]
    fn a_name_alone_never_makes_a_family() {
        // Same scope, same version, the registry names different
        // repositories: no family.
        assert!(detect(
            "@lattice/common",
            "10.4.15",
            "pnpm-lock.yaml",
            &base(),
            &[
                known("@lattice/common", "lattice-js/lattice"),
                known("@lattice/core", "elsewhere/core"),
            ],
            &[],
        )
        .is_none());
        // And one whose registry record was not read is not assumed in.
        assert!(detect(
            "@lattice/common",
            "10.4.15",
            "pnpm-lock.yaml",
            &base(),
            &[
                known("@lattice/common", "lattice-js/lattice"),
                known("@lattice/core", "")
            ],
            &[],
        )
        .is_none());
    }

    #[test]
    fn a_pinning_peer_range_makes_a_member() {
        assert!(pins("18.3.1"));
        assert!(pins("~2.1.0"));
        assert!(pins("2.1.x"));
        assert!(!pins("^18.0.0"));
        assert!(!pins(">=16"));
        assert!(!pins("^17 || ^18"));
        assert!(!pins("*"));
        let f = detect(
            "view-dom",
            "18.2.0",
            "yarn.lock",
            &copies(&[("view-dom", "18.2.0"), ("view", "18.2.0")]),
            &[known("view-dom", ""), known("view", "")],
            &[("view".to_string(), "18.3.1".to_string())],
        )
        .unwrap();
        assert_eq!(f.key, "peers of view-dom");
        assert_eq!(f.members, vec!["view", "view-dom"]);
        assert!(matches!(f.evidence[0], Evidence::PeerPin { .. }));
    }

    #[test]
    fn candidates_are_the_same_version_with_related_names_first() {
        let c = candidates("@lattice/common", "10.4.15", &base());
        assert_eq!(
            c,
            vec![
                "@lattice/core",
                "@lattice/platform-http",
                "left-trim",
                "@lattice/cli"
            ]
        );
        // A scope named after a plain package is related.
        assert_eq!(
            name_pattern("runner", "@runner/coverage"),
            Some("@runner/".into())
        );
        assert_eq!(name_pattern("view", "view-dom"), Some("view".into()));
        assert_eq!(name_pattern("ab", "ab-c"), None);
        // A version half the lockfile shares brings related names only.
        let mut many = copies(&[("pkg-core", "1.0.0"), ("pkg-extra", "1.0.0")]);
        for i in 0..40 {
            many.push((format!("other{i}"), "1.0.0".into()));
        }
        assert_eq!(candidates("pkg-core", "1.0.0", &many), vec!["pkg-extra"]);
    }

    fn published(pairs: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(m, v)| (m.to_string(), v.iter().map(|s| s.to_string()).collect()))
            .collect()
    }

    #[test]
    fn the_family_moves_to_the_lowest_fix_every_member_publishes() {
        let f = lattice();
        let wanted = vec!["10.4.23".to_string(), "10.4.22".to_string()];
        let all = published(&[
            ("@lattice/common", &["10.4.22", "10.4.23"]),
            ("@lattice/core", &["10.4.22", "10.4.23"]),
            ("@lattice/platform-http", &["10.4.22", "10.4.23"]),
        ]);
        assert_eq!(target(&f, &wanted, &all), Ok("10.4.22".into()));
        // One member skipped the lowest: the next every member has.
        let skipped = published(&[
            ("@lattice/common", &["10.4.22", "10.4.23"]),
            ("@lattice/core", &["10.4.23"]),
            ("@lattice/platform-http", &["10.4.22", "10.4.23"]),
        ]);
        assert_eq!(target(&f, &wanted, &skipped), Ok("10.4.23".into()));
        // None in common: said, with who lacks it.
        let none = published(&[
            ("@lattice/common", &["10.4.22"]),
            ("@lattice/core", &[]),
            ("@lattice/platform-http", &["10.4.22"]),
        ]);
        assert_eq!(
            target(&f, &wanted, &none).unwrap_err(),
            "`@lattice/core` publishes no 10.4.22, the lowest version that fixes the advisory"
        );
        // A member the registry could not be read for is never assumed.
        let unread = published(&[("@lattice/common", &["10.4.22"])]);
        assert!(target(&f, &wanted, &unread)
            .unwrap_err()
            .contains("could not be read"));
    }

    fn bump(package: &str, from: &str, to: &str, id: &str) -> PlanBump {
        PlanBump {
            package: package.into(),
            from: from.into(),
            to: to.into(),
            line: version_line(from),
            manifest: "pnpm-lock.yaml".into(),
            finding_ids: vec![id.into()],
            ..Default::default()
        }
    }

    #[test]
    fn one_bump_carries_its_family_and_a_second_folds_into_it() {
        let mut fam = lattice();
        fam.target = "10.4.22".into();
        let mut plan = DepPlan {
            bumps: vec![
                bump("zeta", "1.0.0", "1.0.4", "f-z"),
                bump("@lattice/common", "10.4.15", "10.4.22", "f-1"),
                bump("@lattice/core", "10.4.15", "10.4.20", "f-2"),
            ],
            deferred: vec![],
        };
        let mut fams = vec![fam];
        attach(&mut plan, &mut fams);
        assert_eq!(plan.bumps.len(), 2);
        let b = &plan.bumps[1];
        assert_eq!(b.package, "@lattice/common");
        assert_eq!(b.to, "10.4.22");
        assert_eq!(b.finding_ids, vec!["f-1", "f-2"]);
        let names: Vec<&str> = b.companions.iter().map(|c| c.package.as_str()).collect();
        assert_eq!(names, vec!["@lattice/core", "@lattice/platform-http"]);
        assert!(b.companions.iter().all(|c| c.to == "10.4.22"));
        assert!(b.family_note.starts_with("moves with `@lattice/core`"));
        // The apply script is told every member.
        let specs = crate::dep_fix::specs(&[b]);
        let applied: Vec<&str> = specs[0].bumps.iter().map(|a| a.name.as_str()).collect();
        assert_eq!(
            applied,
            vec!["@lattice/common", "@lattice/core", "@lattice/platform-http"]
        );
    }

    #[test]
    fn a_family_with_no_common_version_goes_alone_with_the_reason() {
        let mut fam = lattice();
        fam.held =
            "`@lattice/core` publishes no 10.4.22, the lowest version that fixes the advisory"
                .into();
        let mut plan = DepPlan {
            bumps: vec![bump("@lattice/common", "10.4.15", "10.4.22", "f-1")],
            deferred: vec![],
        };
        let mut fams = vec![fam];
        attach(&mut plan, &mut fams);
        assert!(plan.bumps[0].companions.is_empty());
        assert!(plan.bumps[0].family_note.contains("publishes no 10.4.22"));
        // The head then differs, for a recorded reason: not a split.
        let head = copies(&[
            ("@lattice/common", "10.4.22"),
            ("@lattice/core", "10.4.15"),
            ("@lattice/platform-http", "10.4.15"),
        ]);
        assert!(splits(&fams, "pnpm-lock.yaml", &head).is_empty());
        assert!(section(&fams, &[], &[]).contains("do not move together here"));
    }

    #[test]
    fn a_target_that_would_newly_cross_a_major_is_not_taken() {
        let mut fam = lattice();
        fam.target = "11.0.0".into();
        let mut plan = DepPlan {
            bumps: vec![bump("@lattice/common", "10.4.15", "10.4.22", "f-1")],
            deferred: vec![],
        };
        let mut fams = vec![fam];
        attach(&mut plan, &mut fams);
        assert_eq!(plan.bumps[0].to, "10.4.22");
        assert!(plan.bumps[0].companions.is_empty());
        assert!(fams[0].held.contains("a major version away"));
    }

    #[test]
    fn a_split_family_is_found_on_the_head() {
        let mut fam = lattice();
        fam.target = "10.4.22".into();
        let fams = vec![fam];
        // The review's case: one member raised, its siblings left behind.
        let split_head = copies(&[
            ("@lattice/common", "10.4.22"),
            ("@lattice/core", "10.4.15"),
            ("@lattice/platform-http", "10.4.15"),
            ("@lattice/cli", "10.2.0"),
        ]);
        let found = splits(&fams, "pnpm-lock.yaml", &split_head);
        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].line(),
            "the family `lattice-js/lattice` is split on the head: `@lattice/common` at 10.4.22, `@lattice/core` at 10.4.15, `@lattice/platform-http` at 10.4.15"
        );
        // Moved together: coherent. The independent sibling is not judged.
        let whole = copies(&[
            ("@lattice/common", "10.4.22"),
            ("@lattice/core", "10.4.22"),
            ("@lattice/platform-http", "10.4.22"),
            ("@lattice/cli", "10.2.0"),
        ]);
        assert!(splits(&fams, "pnpm-lock.yaml", &whole).is_empty());
        // Untouched: coherent. Another lockfile's copies are not this
        // family's.
        assert!(splits(&fams, "pnpm-lock.yaml", &base()).is_empty());
        assert!(splits(&fams, "apps/x/pnpm-lock.yaml", &split_head).is_empty());
        let text = section(&fams, &found, &[]);
        assert!(text.contains("Not coherent: the family `lattice-js/lattice` is split on the head"));
        assert!(text.contains("versioned on their own"));
    }

    #[test]
    fn an_unmet_peer_range_is_named_and_an_unreadable_one_is_not_judged() {
        let peers = vec![
            PeerRange {
                from: "view-dom".into(),
                on: "view".into(),
                range: "^18.3.0".into(),
            },
            PeerRange {
                from: "view-dom".into(),
                on: "absent".into(),
                range: "^1.0.0".into(),
            },
        ];
        let head = copies(&[("view-dom", "18.3.1"), ("view", "18.2.0")]);
        let unmet = peers_unmet("npm", &peers, &head);
        assert_eq!(
            unmet,
            vec!["`view-dom` declares the peer range `^18.3.0` for `view`, and the head installs it at 18.2.0"]
        );
        let ok = copies(&[("view-dom", "18.3.1"), ("view", "18.3.1")]);
        assert!(peers_unmet("npm", &peers, &ok).is_empty());
    }
}
