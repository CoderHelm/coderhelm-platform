//! A dependency pull request's facts, from what the pipeline records today.
//!
//! The description reads [`DependencyFacts`] and nothing else about a
//! dependency update. This is the one place they are filled: the lines and
//! the advisories from what the lockfile on the head resolves
//! ([`crate::dep_head`]), and the evidence for a major version jump from the
//! upgrade notes read before the run ([`crate::upgrade_notes`]). The fields
//! other dependency modules fill when they are there (package families, a
//! frozen install from a verification set of its own, behaviour probes)
//! stay empty here, and an empty field is said as not known, never guessed.

use crate::dep_head::{Fixed, HeadFacts, Resolved};
use crate::pr_facts::{Advisories, DepLine, DependencyFacts, MajorEvidence, Unverified};
use crate::security_fixes::{Finding, FindingState, FixUnit};
use crate::upgrade_notes::WhatToCheck;

/// The facts of `unit` on the head `head` was read on.
pub fn facts(
    unit: &FixUnit,
    members: &[(&Finding, &FindingState)],
    head: &HeadFacts,
    check: &WhatToCheck,
) -> DependencyFacts {
    let mut out = DependencyFacts {
        ecosystem: unit.ecosystem.clone(),
        ..Default::default()
    };
    for (i, b) in unit.bumps.iter().enumerate() {
        let Some(line) = head.lines.get(i) else {
            continue;
        };
        let mine = || {
            members
                .iter()
                .filter(|(f, _)| crate::security_fixes::in_bump(b, f))
        };
        let mut remaining: Vec<String> = mine()
            .filter(|(f, _)| head.fixed(unit, f) == Fixed::No)
            .map(|(f, _)| f.advisory.primary.clone())
            .collect();
        remaining.sort();
        remaining.dedup();
        let mut fixed: Vec<String> = mine()
            .filter(|(f, _)| head.fixed(unit, f) == Fixed::Yes)
            .map(|(f, _)| f.advisory.primary.clone())
            .filter(|id| !id.is_empty())
            .collect();
        fixed.sort();
        fixed.dedup();
        let (from, to, moved, major) = match &line.resolved {
            Resolved::Moved { from, to, major } => (from.clone(), to.clone(), true, *major),
            Resolved::NotMoved { at } => (at.clone(), at.clone(), false, false),
            Resolved::Removed { from } => {
                (from.clone(), "no longer installed".to_string(), true, false)
            }
            Resolved::Unknown { why } => {
                out.unread.push(Unverified {
                    what: format!("`{}` {}", b.package, b.from),
                    why: why.clone(),
                });
                (b.from.clone(), String::new(), false, false)
            }
        };
        if moved && major {
            let mut evidence = Vec::new();
            if let Some(n) = crate::upgrade_notes::notes_for_bump(check, b).filter(|n| n.read()) {
                evidence.push(format!(
                    "its release notes were read ({}): {} listed, {} found in this code",
                    n.source,
                    count(n.breaking.len(), "breaking change", "breaking changes"),
                    count(
                        n.call_sites.len(),
                        "use of what changed",
                        "uses of what changed"
                    ),
                ));
                if check.ci.state == "passing" {
                    evidence.push("CI passes on the head with it".to_string());
                }
            }
            out.majors.push(MajorEvidence {
                package: b.package.clone(),
                from: from.clone(),
                to: to.clone(),
                evidence,
            });
        }
        out.lines.push(DepLine {
            package: b.package.clone(),
            from,
            to,
            moved,
            major,
            remaining,
            fixed,
        });
    }
    let mut advisories = Advisories::default();
    for (id, fixed) in head.advisories(unit, members) {
        match fixed {
            Fixed::Yes => advisories.fixed.push(id),
            Fixed::No => {
                let worst = members
                    .iter()
                    .filter(|(f, _)| f.advisory.primary == id)
                    .map(|(f, st)| crate::security_fixes::score(f, st))
                    .fold(0.0_f64, f64::max);
                *advisories
                    .remaining_by_severity
                    .entry(crate::security_fixes::severity_word(worst).to_string())
                    .or_default() += 1;
                advisories.remaining.push(id);
            }
            Fixed::Unknown => out.unread.push(Unverified {
                what: id,
                why: "whether the head fixes it could not be read from the lockfile".to_string(),
            }),
        }
    }
    out.advisories = advisories;
    out
}

fn count(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}
