//! What a dependency pull request's head resolves, read from its lockfiles.
//!
//! A dependency run planned five packages as major version jumps. A review
//! round then kept each of them inside its current major with a scoped
//! override, and the code was right. The description still showed the plan:
//! its table listed the five as "(major)", its headline counted twenty major
//! versions, and its count of fixed advisories had never been compared with
//! what the final lockfile installs.
//!
//! The description of a dependency pull request is written from the head,
//! never from the plan:
//!
//! - [`resolve`] compares each lockfile on the base branch with the same
//!   lockfile on the head, one answer per package and version line of the
//!   plan: where the line's copies are now, whether that crosses a major
//!   version, whether the line is gone, or that it did not move.
//! - [`HeadFacts::fixed`] counts an advisory as fixed only when every copy
//!   left on its line is at or past the advisory's patched version (or the
//!   line is no longer installed).
//! - What can't be read is said as not known ([`Resolved::Unknown`], with
//!   why). The plan's numbers are then shown as a plan, never as a result.
//!
//! Everything here is pure; the worker reads the lockfiles
//! (`worker::passes::security_fixes::head_facts`).

use std::cmp::Ordering;

use crate::security_fixes::{
    compare_versions, in_bump, is_major_bump, version_line, Finding, FindingState, FixUnit,
};

/// One lockfile as the base branch and the head have it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LockAt {
    pub path: String,
    /// Every copy it installs on the base branch (name, version); `None`
    /// when it could not be read there.
    pub base: Option<Vec<(String, String)>>,
    /// The same on the head.
    pub head: Option<Vec<(String, String)>>,
    /// Why a side could not be read; empty when both were.
    pub why: String,
}

/// Where one package and version line of the plan stands on the head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolved {
    /// The line's lowest copy moved from `from` to `to`.
    Moved {
        from: String,
        to: String,
        major: bool,
    },
    /// The head installs the same version as the base branch.
    NotMoved { at: String },
    /// The head no longer installs the package at all.
    Removed { from: String },
    /// The lockfile could not be read on one side.
    Unknown { why: String },
}

/// One line of the plan with what the head resolves for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadLine {
    pub package: String,
    pub planned_from: String,
    pub planned_to: String,
    pub resolved: Resolved,
}

/// Whether the head fixes one advisory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fixed {
    Yes,
    No,
    Unknown,
}

/// What the head of a dependency pull request resolves: one [`HeadLine`]
/// per bump of the unit, in the unit's order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HeadFacts {
    /// The head commit the lockfiles were read on.
    pub sha: String,
    pub lines: Vec<HeadLine>,
}

fn lowest<'a>(versions: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    versions.min_by(|a, b| compare_versions(a, b))
}

fn versions_of<'a>(copies: &'a [(String, String)], package: &str) -> Vec<&'a str> {
    copies
        .iter()
        .filter(|(n, _)| n.eq_ignore_ascii_case(package))
        .map(|(_, v)| v.as_str())
        .collect()
}

fn resolve_line(package: &str, planned_from: &str, lock: &LockAt) -> Resolved {
    let (Some(base), Some(head)) = (&lock.base, &lock.head) else {
        return Resolved::Unknown {
            why: if lock.why.is_empty() {
                format!("`{}` could not be read", lock.path)
            } else {
                lock.why.clone()
            },
        };
    };
    let line = version_line(planned_from);
    let on_line = |v: &&str| version_line(v) == line;
    let base_all = versions_of(base, package);
    let head_all = versions_of(head, package);
    // The line's weakest copy on the base branch is where it starts.
    let from = lowest(base_all.iter().copied().filter(on_line))
        .unwrap_or(planned_from)
        .to_string();
    if head_all.is_empty() {
        return Resolved::Removed { from };
    }
    // Still on its line: the weakest copy left decides what is fixed.
    if let Some(to) = lowest(head_all.iter().copied().filter(on_line)) {
        return if compare_versions(to, &from) == Ordering::Equal {
            Resolved::NotMoved { at: from }
        } else {
            Resolved::Moved {
                major: false,
                to: to.to_string(),
                from,
            }
        };
    }
    // The line is gone and the package is still installed: its copies moved
    // to another line. The version the head has and the base did not is
    // where they went; with none new, they joined a line already installed.
    let new = lowest(
        head_all
            .iter()
            .copied()
            .filter(|v| !base_all.iter().any(|b| b == v)),
    );
    let above = lowest(
        head_all
            .iter()
            .copied()
            .filter(|v| compare_versions(v, &from) == Ordering::Greater),
    );
    let to = new
        .or(above)
        .or_else(|| lowest(head_all.iter().copied()))
        .unwrap_or_default()
        .to_string();
    Resolved::Moved {
        major: is_major_bump(&from, &to),
        from,
        to,
    }
}

/// What the head resolves for each bump of `unit`. `locks`: the
/// repository's lockfiles as read on the base branch and on `sha`.
pub fn resolve(unit: &FixUnit, sha: &str, locks: &[LockAt]) -> HeadFacts {
    let paths: Vec<String> = locks.iter().map(|l| l.path.clone()).collect();
    let lines = unit
        .bumps
        .iter()
        .map(|b| {
            let lock = match crate::dep_fix::lockfile_for(&b.manifest, &paths) {
                Some(p) => locks.iter().find(|l| l.path == p),
                None if locks.len() == 1 => locks.first(),
                None => None,
            };
            let resolved = match lock {
                Some(l) => resolve_line(&b.package, &b.from, l),
                None => Resolved::Unknown {
                    why: if b.manifest.is_empty() {
                        "the lockfile that installs it is not known".to_string()
                    } else {
                        format!(
                            "the lockfile that installs it could not be told from `{}`",
                            b.manifest
                        )
                    },
                },
            };
            HeadLine {
                package: b.package.clone(),
                planned_from: b.from.clone(),
                planned_to: b.to.clone(),
                resolved,
            }
        })
        .collect();
    HeadFacts {
        sha: sha.to_string(),
        lines,
    }
}

/// The facts when nothing could be read: every line unknown, with why.
pub fn unknown(unit: &FixUnit, why: &str) -> HeadFacts {
    HeadFacts {
        sha: String::new(),
        lines: unit
            .bumps
            .iter()
            .map(|b| HeadLine {
                package: b.package.clone(),
                planned_from: b.from.clone(),
                planned_to: b.to.clone(),
                resolved: Resolved::Unknown {
                    why: why.to_string(),
                },
            })
            .collect(),
    }
}

fn plural(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

impl HeadFacts {
    /// The table's Version cell for the unit's bump `i`.
    pub fn version_cell(&self, i: usize) -> String {
        match self.lines.get(i).map(|l| (&l.resolved, l)) {
            Some((Resolved::Moved { from, to, major }, _)) => {
                format!("{from} to {to}{}", if *major { " (major)" } else { "" })
            }
            Some((Resolved::NotMoved { at }, _)) => format!("{at}, not moved on the head"),
            Some((Resolved::Removed { from }, _)) => format!("{from}, no longer installed"),
            Some((Resolved::Unknown { .. }, l)) => format!(
                "{} to {} planned; not read from the head",
                l.planned_from, l.planned_to
            ),
            None => "not known".to_string(),
        }
    }

    /// `unit` with each bump as the head resolves it: the real versions and
    /// whether it crosses a major. A line that could not be read keeps the
    /// plan's (the table marks it as a plan).
    pub fn unit_at_head(&self, unit: &FixUnit) -> FixUnit {
        let mut out = unit.clone();
        for (b, l) in out.bumps.iter_mut().zip(&self.lines) {
            match &l.resolved {
                Resolved::Moved { from, to, major } => {
                    b.from = from.clone();
                    b.to = to.clone();
                    b.major = *major;
                }
                Resolved::NotMoved { at } => {
                    b.from = at.clone();
                    b.to = at.clone();
                    b.major = false;
                }
                Resolved::Removed { from } => {
                    b.from = from.clone();
                    b.major = false;
                }
                Resolved::Unknown { .. } => {}
            }
        }
        out
    }

    /// Lines the head moves across a major version.
    pub fn majors(&self) -> usize {
        self.lines
            .iter()
            .filter(|l| matches!(l.resolved, Resolved::Moved { major: true, .. }))
            .count()
    }

    /// Lines the head could not be read for, with the first reason.
    pub fn unknown_lines(&self) -> (usize, &str) {
        let mut why = "";
        let mut n = 0;
        for l in &self.lines {
            if let Resolved::Unknown { why: w } = &l.resolved {
                n += 1;
                if why.is_empty() {
                    why = w;
                }
            }
        }
        (n, why)
    }

    /// Whether the head fixes `f`: every copy left on its line is at or
    /// past the advisory's patched version, or the line is gone.
    pub fn fixed(&self, unit: &FixUnit, f: &Finding) -> Fixed {
        let Some(line) = unit
            .bumps
            .iter()
            .position(|b| in_bump(b, f))
            .and_then(|i| self.lines.get(i))
        else {
            return Fixed::Unknown;
        };
        let at = match &line.resolved {
            Resolved::Removed { .. } => return Fixed::Yes,
            Resolved::Unknown { .. } => return Fixed::Unknown,
            Resolved::NotMoved { at } => at,
            Resolved::Moved { to, .. } => to,
        };
        match f.fixed_version.as_deref().filter(|v| !v.is_empty()) {
            // No patched version is recorded for it: a move alone proves
            // nothing.
            None => Fixed::Unknown,
            Some(patched) if compare_versions(at, patched) != Ordering::Less => Fixed::Yes,
            Some(_) => Fixed::No,
        }
    }

    /// Each advisory of `members` once, with whether the head fixes it: an
    /// advisory in several places is fixed only when every place is.
    pub fn advisories(
        &self,
        unit: &FixUnit,
        members: &[(&Finding, &FindingState)],
    ) -> Vec<(String, Fixed)> {
        let mut out: Vec<(String, Fixed)> = vec![];
        for (f, _) in members {
            let id = f.advisory.primary.clone();
            let now = self.fixed(unit, f);
            match out.iter_mut().find(|(i, _)| *i == id) {
                Some((_, was)) => {
                    *was = match (*was, now) {
                        (Fixed::No, _) | (_, Fixed::No) => Fixed::No,
                        (Fixed::Unknown, _) | (_, Fixed::Unknown) => Fixed::Unknown,
                        _ => Fixed::Yes,
                    }
                }
                None => out.push((id, now)),
            }
        }
        out
    }

    /// The description's first sentences: how many advisories the head
    /// fixes, in how many package version lines, how many of those cross a
    /// major version, and what is not fixed or not known.
    pub fn headline(&self, unit: &FixUnit, members: &[(&Finding, &FindingState)]) -> String {
        let advisories = self.advisories(unit, members);
        let count = |want: Fixed| advisories.iter().filter(|(_, f)| *f == want).count();
        let (fixed, open, unknown) = (count(Fixed::Yes), count(Fixed::No), count(Fixed::Unknown));
        let (unread, why) = self.unknown_lines();
        let at = if self.sha.is_empty() {
            "the head".to_string()
        } else {
            format!("the head `{}`", &self.sha[..self.sha.len().min(7)])
        };
        if unread == self.lines.len() && !self.lines.is_empty() {
            return format!(
                "What {at} installs is not known: {why}. No count of fixed advisories is claimed \
                 here, and the versions in the table are the run's plan, marked as such."
            );
        }
        let moved = self
            .lines
            .iter()
            .filter(|l| {
                matches!(
                    l.resolved,
                    Resolved::Moved { .. } | Resolved::Removed { .. }
                )
            })
            .count();
        let majors = self.majors();
        let mut s = format!(
            "Fixes {} of {} in {} ({} a major version), read from the lockfile on {at}.",
            fixed,
            plural(advisories.len(), "advisory", "advisories"),
            plural(moved, "package version line", "package version lines"),
            if majors == 1 {
                "1 crosses".to_string()
            } else {
                format!("{majors} cross")
            },
        );
        if open > 0 {
            let ids: Vec<&str> = advisories
                .iter()
                .filter(|(_, f)| *f == Fixed::No)
                .map(|(i, _)| i.as_str())
                .take(5)
                .collect();
            s.push_str(&format!(
                " Not fixed on the head: {}{} (the version installed is below the patched one).",
                ids.join(", "),
                if open > ids.len() {
                    format!(" and {} more", open - ids.len())
                } else {
                    String::new()
                }
            ));
        }
        if unknown > 0 {
            s.push_str(&format!(
                " Not known for {}: {}.",
                plural(unknown, "advisory", "advisories"),
                if unread > 0 {
                    why
                } else {
                    "no patched version is recorded for it"
                }
            ));
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security_fixes::{Advisory, Bump};

    fn copies(v: &[(&str, &str)]) -> Option<Vec<(String, String)>> {
        Some(
            v.iter()
                .map(|(n, v)| (n.to_string(), v.to_string()))
                .collect(),
        )
    }

    fn bump(package: &str, from: &str, to: &str) -> Bump {
        Bump {
            package: package.into(),
            from: from.into(),
            to: to.into(),
            major: is_major_bump(from, to),
            manifest: "pnpm-lock.yaml".into(),
        }
    }

    fn finding(id: &str, package: &str, version: &str, patched: Option<&str>) -> Finding {
        Finding {
            id: format!("f-{id}"),
            package: package.into(),
            version: version.into(),
            fixed_version: patched.map(String::from),
            manifest: "pnpm-lock.yaml".into(),
            advisory: Advisory {
                primary: id.into(),
                ids: vec![id.into()],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// The plan said a major version for each; the head keeps each on its
    /// line through a scoped override.
    fn planned_major_stays_on_its_line() -> (FixUnit, HeadFacts, Vec<Finding>) {
        let unit = FixUnit {
            bundle: true,
            bumps: vec![
                bump("glob-walk", "2.3.1", "4.0.4"),
                bump("path-match", "3.1.2", "11.0.1"),
                bump("left-trim", "1.0.2", "1.0.5"),
            ],
            ..Default::default()
        };
        let lock = LockAt {
            path: "pnpm-lock.yaml".into(),
            base: copies(&[
                ("glob-walk", "2.3.1"),
                ("glob-walk", "4.0.1"),
                ("path-match", "3.1.2"),
                ("left-trim", "1.0.2"),
            ]),
            head: copies(&[
                ("glob-walk", "2.3.2"),
                ("glob-walk", "4.0.1"),
                ("path-match", "3.1.5"),
                ("left-trim", "1.0.5"),
            ]),
            why: String::new(),
        };
        let facts = resolve(&unit, "abc1234def", &[lock]);
        let findings = vec![
            finding("GHSA-aaaa", "glob-walk", "2.3.1", Some("2.3.2")),
            finding("GHSA-bbbb", "path-match", "3.1.2", Some("3.1.5")),
            finding("GHSA-cccc", "left-trim", "1.0.2", Some("1.0.5")),
        ];
        (unit, facts, findings)
    }

    #[test]
    fn a_line_the_plan_called_major_is_shown_as_the_head_resolves_it() {
        let (unit, facts, findings) = planned_major_stays_on_its_line();
        assert_eq!(facts.version_cell(0), "2.3.1 to 2.3.2");
        assert_eq!(facts.version_cell(1), "3.1.2 to 3.1.5");
        assert_eq!(facts.majors(), 0);
        let at_head = facts.unit_at_head(&unit);
        assert!(at_head.bumps.iter().all(|b| !b.major), "{at_head:?}");
        assert_eq!(at_head.bumps[0].to, "2.3.2");
        let st = FindingState::default();
        let members: Vec<(&Finding, &FindingState)> = findings.iter().map(|f| (f, &st)).collect();
        let h = facts.headline(&unit, &members);
        assert_eq!(
            h,
            "Fixes 3 of 3 advisories in 3 package version lines (0 cross a major version), read \
             from the lockfile on the head `abc1234`."
        );
        assert!(!h.contains("4.0.4") && !h.contains("11.0.1"));
    }

    #[test]
    fn a_line_that_really_crosses_a_major_is_counted_as_one() {
        let unit = FixUnit {
            bumps: vec![bump("glob-walk", "2.3.1", "2.3.2")],
            ..Default::default()
        };
        let lock = LockAt {
            path: "pnpm-lock.yaml".into(),
            base: copies(&[("glob-walk", "2.3.1")]),
            head: copies(&[("glob-walk", "4.0.4")]),
            why: String::new(),
        };
        let facts = resolve(&unit, "", &[lock]);
        assert_eq!(facts.version_cell(0), "2.3.1 to 4.0.4 (major)");
        assert_eq!(facts.majors(), 1);
        assert!(facts.unit_at_head(&unit).bumps[0].major);
    }

    #[test]
    fn an_advisory_is_fixed_only_at_or_past_its_patched_version() {
        let unit = FixUnit {
            bumps: vec![
                bump("glob-walk", "2.3.1", "2.3.4"),
                bump("path-match", "3.1.2", "3.1.5"),
                bump("old-dep", "0.4.0", "0.4.2"),
                bump("no-patch", "1.0.0", "1.0.1"),
            ],
            ..Default::default()
        };
        let lock = LockAt {
            path: "pnpm-lock.yaml".into(),
            base: copies(&[
                ("glob-walk", "2.3.1"),
                ("path-match", "3.1.2"),
                ("old-dep", "0.4.0"),
                ("no-patch", "1.0.0"),
            ]),
            // glob-walk moved, but short of the patch; path-match did not
            // move; old-dep is gone; no-patch moved with no patched version
            // on record.
            head: copies(&[
                ("glob-walk", "2.3.2"),
                ("path-match", "3.1.2"),
                ("no-patch", "1.0.1"),
            ]),
            why: String::new(),
        };
        let facts = resolve(&unit, "f00", &[lock]);
        let fs = [
            finding("GHSA-1", "glob-walk", "2.3.1", Some("2.3.4")),
            finding("GHSA-2", "path-match", "3.1.2", Some("3.1.5")),
            finding("GHSA-3", "old-dep", "0.4.0", Some("0.4.2")),
            finding("GHSA-4", "no-patch", "1.0.0", None),
        ];
        assert_eq!(facts.fixed(&unit, &fs[0]), Fixed::No);
        assert_eq!(facts.fixed(&unit, &fs[1]), Fixed::No);
        assert_eq!(facts.fixed(&unit, &fs[2]), Fixed::Yes);
        assert_eq!(facts.fixed(&unit, &fs[3]), Fixed::Unknown);
        assert_eq!(facts.version_cell(1), "3.1.2, not moved on the head");
        assert_eq!(facts.version_cell(2), "0.4.0, no longer installed");
        let st = FindingState::default();
        let members: Vec<(&Finding, &FindingState)> = fs.iter().map(|f| (f, &st)).collect();
        let h = facts.headline(&unit, &members);
        assert!(
            h.starts_with("Fixes 1 of 4 advisories in 3 package version lines"),
            "{h}"
        );
        assert!(h.contains("Not fixed on the head: GHSA-1, GHSA-2"), "{h}");
        assert!(h.contains("Not known for 1 advisory"), "{h}");
    }

    #[test]
    fn what_cannot_be_read_is_said_as_not_known_never_as_the_plan() {
        let unit = FixUnit {
            bumps: vec![bump("glob-walk", "2.3.1", "4.0.4")],
            ..Default::default()
        };
        let lock = LockAt {
            path: "pnpm-lock.yaml".into(),
            base: copies(&[("glob-walk", "2.3.1")]),
            head: None,
            why: "`pnpm-lock.yaml` could not be read on the head (too large)".into(),
        };
        let facts = resolve(&unit, "abc", &[lock]);
        assert_eq!(
            facts.version_cell(0),
            "2.3.1 to 4.0.4 planned; not read from the head"
        );
        let f = finding("GHSA-1", "glob-walk", "2.3.1", Some("2.3.2"));
        let st = FindingState::default();
        let h = facts.headline(&unit, &[(&f, &st)]);
        assert!(
            h.starts_with("What the head `abc` installs is not known"),
            "{h}"
        );
        assert!(!h.contains("Fixes"), "{h}");
        assert_eq!(
            facts.majors(),
            0,
            "an unread line is never counted as a major"
        );
        // A bump whose lockfile can't be told is unknown too.
        let mut other = unit.clone();
        other.bumps[0].manifest = "sbom".into();
        let two = [
            LockAt {
                path: "a/yarn.lock".into(),
                ..Default::default()
            },
            LockAt {
                path: "b/yarn.lock".into(),
                ..Default::default()
            },
        ];
        assert!(matches!(
            resolve(&other, "abc", &two).lines[0].resolved,
            Resolved::Unknown { .. }
        ));
    }
}
