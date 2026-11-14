//! A dependency update decided and proven the way a senior engineer would.
//!
//! An outside review of one dependency security pull request (about fifty
//! packages, four across a major version) found the code sound and the
//! account of it wanting in five places:
//!
//! 1. the count of advisories left was CoderHelm's own and disagreed with
//!    the package manager's production audit of the same head;
//! 2. one package of a framework was raised and its siblings from the same
//!    source repository stayed behind;
//! 3. no type check was recorded, because the repository's CI has none and
//!    CoderHelm's checks mirrored CI;
//! 4. it said "contract unchanged" although a validation library's major
//!    changed the 400 error bodies of two endpoints;
//! 5. each override across a major lacked the reason it is safe.
//!
//! The mechanisms, one module each, all general across package managers
//! through a table where they differ:
//!
//! - [`family`]: packages released together move together, found from the
//!   registry's metadata and the lockfile, and verified on the head.
//! - [`verify`]: the set a dependency change is verified with, found in
//!   the project and not in its CI, budgeted to the sandbox's time.
//! - [`advisories`]: fixed, remaining and introduced, the base branch and
//!   the head read the same way and laid beside the package manager's
//!   audit; every disagreement with its cause.
//! - [`safety`]: why each major is safe, from its dependency paths, where
//!   they run, whether the repository uses it, and how it is fixed.
//! - [`behaviour`]: a before-and-after probe through the call sites of a
//!   major; "unchanged" stands only on a probe.
//!
//! [`UpgradeFacts`] is what they record for one head commit. The pull
//! request's description and the reviewer cite it; a head that has moved
//! since is said to have, and nothing recorded on an earlier commit is
//! shown as true of the new one.

pub mod advisories;
pub mod behaviour;
pub mod family;
pub mod safety;
pub mod verify;

use serde::{Deserialize, Serialize};

/// What the dependency run recorded for one head commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct UpgradeFacts {
    /// The commit every fact below was recorded on.
    pub sha: String,
    pub families: Vec<family::Family>,
    /// The families the head installs at more than one version.
    pub splits: Vec<family::Split>,
    /// The peer ranges the head leaves unmet, each as a sentence.
    pub peers_unmet: Vec<String>,
    /// The minimum set's results on the head, and on the base branch where
    /// the head failed something.
    pub verified: Option<verify::Verified>,
    pub verified_base: Option<verify::Verified>,
    pub advisories: Option<advisories::Reconciled>,
    pub majors: Vec<safety::MajorSafety>,
    pub behaviour: Vec<behaviour::Behaviour>,
    /// What did not run, each with why: never shown as a result.
    pub not_run: Vec<String>,
}

fn short(sha: &str) -> &str {
    &sha[..sha.len().min(7)]
}

impl UpgradeFacts {
    /// From a ticket record's `dep_facts` (JSON); `None` when it has none.
    pub fn from_json(json: &str) -> Option<Self> {
        if json.trim().is_empty() {
            return None;
        }
        serde_json::from_str(json).ok()
    }

    /// The facts were recorded on `head` (a full or short sha).
    pub fn of_head(&self, head: &str) -> bool {
        !self.sha.is_empty()
            && !head.is_empty()
            && (self.sha.starts_with(head) || head.starts_with(&self.sha))
    }

    /// Whether a probe shows the behaviour of every probed major
    /// unchanged, and no major on a production path went unprobed. Only
    /// then may a text call the contract or the behaviour unchanged.
    pub fn unchanged_shown(&self) -> bool {
        let probed_ok = !self.behaviour.is_empty()
            && self
                .behaviour
                .iter()
                .all(behaviour::Behaviour::unchanged_shown);
        let all_probed = self.majors.iter().all(|m| {
            !matches!(m.probe, safety::ProbeNeed::Needed { .. })
                || self
                    .behaviour
                    .iter()
                    .any(|b| b.package == m.package && b.unchanged_shown())
        });
        probed_ok && all_probed
    }

    /// The facts as the pull request states them, under "What to check".
    /// `head`: the commit the description is being written for; when the
    /// facts are of another commit that is said first, and they are shown
    /// as that commit's.
    pub fn section(&self, head: &str) -> String {
        let mut s = format!(
            "### Upgrade evidence\n\nRecorded by CoderHelm on commit `{}`.",
            short(&self.sha)
        );
        if !head.is_empty() && !self.of_head(head) {
            s.push_str(&format!(
                " The head is now `{}`: everything in this section is of the earlier commit and has not been run again on the new one.",
                short(head)
            ));
        }
        s.push_str("\n\n");
        let mut blocks: Vec<String> = vec![];
        if let Some(a) = &self.advisories {
            blocks.push(a.section());
        }
        let fam = family::section(&self.families, &self.splits, &self.peers_unmet);
        if !fam.is_empty() {
            blocks.push(fam);
        }
        if let Some(v) = &self.verified {
            blocks.push(verify::section(v, self.verified_base.as_ref()));
        }
        if !self.majors.is_empty() {
            let mut m = String::from(
                "**Why each major version bump is safe, or what is not known about it:**\n",
            );
            for major in &self.majors {
                m.push_str(&major.section());
            }
            blocks.push(m);
        }
        if !self.behaviour.is_empty() {
            let mut b = String::from(
                "**Behaviour changes** (the same requests sent to the application built from the base branch and from this commit):\n",
            );
            for one in &self.behaviour {
                b.push_str(&one.section());
            }
            blocks.push(b);
        }
        if !self.not_run.is_empty() {
            let mut n = String::from("**Not run, and not claimed:**\n");
            for line in &self.not_run {
                n.push_str(&format!("- {line}\n"));
            }
            blocks.push(n);
        }
        s.push_str(&blocks.join("\n"));
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_facts_are_of_one_commit_and_say_so_when_the_head_has_moved() {
        let f = UpgradeFacts {
            sha: "abc1234def5678".into(),
            not_run: vec!["the unit tests: the sandbox build had no time left".into()],
            ..Default::default()
        };
        assert!(f.of_head("abc1234def5678"));
        assert!(f.of_head("abc1234"));
        assert!(!f.of_head("fff0000"));
        assert!(!f.of_head(""));
        let same = f.section("abc1234def5678");
        assert!(same
            .starts_with("### Upgrade evidence\n\nRecorded by CoderHelm on commit `abc1234`.\n"));
        assert!(same.contains("**Not run, and not claimed:**\n- the unit tests"));
        let moved = f.section("fff0000aaaa");
        assert!(moved.contains(
            "The head is now `fff0000`: everything in this section is of the earlier commit and has not been run again on the new one."
        ));
        // It survives the ticket record.
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(UpgradeFacts::from_json(&json), Some(f));
        assert_eq!(UpgradeFacts::from_json(""), None);
        assert_eq!(UpgradeFacts::from_json("not json"), None);
    }

    #[test]
    fn unchanged_needs_every_production_major_probed_and_the_same() {
        let same = behaviour::Compared {
            outcome: behaviour::Outcome::Same,
            reached: true,
            ..Default::default()
        };
        let shown = behaviour::Behaviour {
            package: "shape-check".into(),
            compared: vec![same.clone()],
            ..Default::default()
        };
        let needs = |package: &str| safety::MajorSafety {
            package: package.into(),
            probe: safety::ProbeNeed::Needed { why: "x".into() },
            ..Default::default()
        };
        // Nothing probed: never.
        assert!(!UpgradeFacts::default().unchanged_shown());
        // One probed and the same: yes.
        let f = UpgradeFacts {
            behaviour: vec![shown.clone()],
            majors: vec![needs("shape-check")],
            ..Default::default()
        };
        assert!(f.unchanged_shown());
        // A second production major with no probe: no.
        let f = UpgradeFacts {
            behaviour: vec![shown.clone()],
            majors: vec![needs("shape-check"), needs("upload-kit")],
            ..Default::default()
        };
        assert!(!f.unchanged_shown());
        // One changed answer: no.
        let changed = behaviour::Behaviour {
            package: "upload-kit".into(),
            compared: vec![behaviour::Compared {
                outcome: behaviour::Outcome::Changed,
                reached: true,
                ..Default::default()
            }],
            ..Default::default()
        };
        let f = UpgradeFacts {
            behaviour: vec![shown, changed],
            ..Default::default()
        };
        assert!(!f.unchanged_shown());
    }
}
