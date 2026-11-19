//! The one line at the top of a run page and its pull request: what the
//! run is waiting on, and whether the pull request is ready for review.
//!
//! A finished run's page once showed "completed" with a merge standing
//! code (`needs_person_approval`) and a paragraph about auto-merge
//! settings: nobody could tell at a glance that the pull request only
//! waited for a person's approving review, or that its checks had passed.
//! [`line`] builds the sentence from what CoderHelm records about the pull
//! request (`merge_standing`, `draft_hold`, the preview), so the run page
//! and the description say the same thing.

use crate::draft_hold::{DraftHold, Kind};
use crate::merge_standing::Standing;
use crate::preview::{Stage, State as Preview};

/// What [`line`] reads.
#[derive(Debug, Clone, Default)]
pub struct Inputs<'a> {
    pub pr: u64,
    pub standing: Option<&'a Standing>,
    pub hold: Option<&'a DraftHold>,
    pub preview: Option<&'a Preview>,
    /// The repository's checks passed on the head (`ci_last` = passed).
    pub checks_passed: bool,
}

fn pr(n: u64) -> String {
    if n > 0 {
        format!("#{n}")
    } else {
        "the pull request".into()
    }
}

fn clause(s: &str) -> String {
    let s = s.trim().trim_end_matches('.');
    let mut c = s.chars();
    match c.next() {
        Some(f) if f.is_uppercase() && !c.clone().next().is_some_and(|n| n.is_uppercase()) => {
            f.to_lowercase().collect::<String>() + c.as_str()
        }
        _ => s.to_string(),
    }
}

/// What the pull request waits on, as "Waiting on: …." (or what happened
/// to it), from the draft hold first (it holds the pull request before
/// anything about merging), then the merge standing.
fn waiting(i: &Inputs) -> Option<String> {
    let n = pr(i.pr);
    if let Some(h) = i.hold {
        let what = h
            .reasons
            .first()
            .map(|r| clause(&r.what))
            .unwrap_or_default();
        return Some(match h.kind {
            Kind::Waiting => format!(
                "Waiting on: {what} on {n} (CoderHelm checks again by itself and marks it ready)."
            ),
            Kind::Held => format!("Waiting on: a person, {n} stays a draft because {what}."),
            Kind::Failed => {
                format!("Waiting on: a person to mark {n} ready (CoderHelm couldn't: {what}).")
            }
        });
    }
    let s = i.standing?;
    Some(match s.code.as_str() {
        "needs_person_approval" => format!(
            "Waiting on: an approving review from a person on {n} (this repo requires one before CoderHelm merges)."
        ),
        "auto_merge_off" => format!(
            "Waiting on: a person on your team to merge {n} (auto-merge is off for this repo)."
        ),
        "repo_requires_person_review" => format!(
            "Waiting on: the branch rules on {n} (they require an approving review from a person, or another rule GitHub enforces)."
        ),
        "reviewing" => format!("Waiting on: CoderHelm's review of the head of {n}."),
        "fixing_review" => format!("Waiting on: CoderHelm's fix round for its review of {n}."),
        "review_changes_requested" => format!(
            "Waiting on: a person, CoderHelm's review asks for changes on {n} it could not make (listed on the pull request)."
        ),
        "checks_running" => format!("Waiting on: the checks still running on {n}."),
        "checks_failing" => format!("Waiting on: CoderHelm's fix for the failing checks on {n}."),
        "conflict" => format!("Waiting on: CoderHelm resolving the conflict on {n}."),
        "draft" => format!("Waiting on: {n} leaving draft (its description says why at the top)."),
        "merging" => format!("Nothing: {n} is approved and CoderHelm merges it."),
        "merged" => format!("Nothing: {n} is merged; CoderHelm watches the deploy."),
        "closed" => format!("Nothing: {n} was closed without merging."),
        _ => format!("Waiting on: {}", clause(&s.text)) + ".",
    })
}

/// Whether the pull request is ready for review, and on what: its checks,
/// and its preview when the change has one. None while it is a draft, done
/// or not yet checked.
fn readiness(i: &Inputs) -> Option<String> {
    if i.hold.is_some()
        || i.standing.is_some_and(|s| {
            matches!(
                s.code.as_str(),
                "merged" | "closed" | "draft" | "checks_failing" | "checks_running" | "conflict"
            )
        })
    {
        return None;
    }
    let mut on: Vec<&str> = Vec::new();
    if i.checks_passed {
        on.push("checks passed");
    }
    match i.preview {
        Some(p) if p.stage == Stage::Checked => on.push("preview checked"),
        Some(p) if p.stage == Stage::Failed => {
            return Some("Not ready for review: the preview checks failed.".into());
        }
        Some(p) if p.stage == Stage::Approval => {
            return Some("Not ready for review yet: preview waiting for an approval.".into());
        }
        Some(p) if p.pending() => {
            return Some(format!(
                "Not ready for review yet: {}.",
                p.phrase().unwrap_or("preview deploying")
            ));
        }
        Some(p) if p.stage == Stage::Blocked => {
            return Some(format!(
                "Ready for review without a preview: {}.",
                clause(if p.why.is_empty() {
                    "its preview couldn't be started"
                } else {
                    &p.why
                })
            ));
        }
        _ => {}
    }
    if !i.checks_passed {
        return None;
    }
    Some(format!("Ready for review: {}.", on.join(", ")))
}

/// The line: "Waiting on: …. Ready for review: …." Empty when nothing is
/// known about the pull request.
pub fn line(i: &Inputs) -> String {
    [waiting(i), readiness(i)]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_standing::{decide, Facts};

    fn approval_standing() -> Standing {
        decide(
            "org/web",
            &Facts {
                head: "abc1234def".into(),
                reviewer_on: true,
                own_verdict: Some(("APPROVE".into(), "abc1234def".into())),
                auto_merge: true,
                require_human_approval: true,
                ..Default::default()
            },
        )
    }

    #[test]
    fn needs_person_approval_reads_as_one_plain_line() {
        let s = approval_standing();
        assert_eq!(s.code, "needs_person_approval");
        let p = Preview {
            stage: Stage::Checked,
            ..Default::default()
        };
        let l = line(&Inputs {
            pr: 42,
            standing: Some(&s),
            hold: None,
            preview: Some(&p),
            checks_passed: true,
        });
        assert_eq!(
            l,
            "Waiting on: an approving review from a person on #42 (this repo requires one before CoderHelm merges). Ready for review: checks passed, preview checked."
        );
    }

    #[test]
    fn a_preview_still_deploying_is_not_called_ready() {
        let s = approval_standing();
        let p = Preview {
            stage: Stage::Deploying,
            ..Default::default()
        };
        let l = line(&Inputs {
            pr: 42,
            standing: Some(&s),
            preview: Some(&p),
            checks_passed: true,
            ..Default::default()
        });
        assert!(
            l.ends_with("Not ready for review yet: preview deploying."),
            "{l}"
        );
        assert!(!l.contains("Ready for review:"));
    }

    #[test]
    fn a_preview_held_for_an_environment_approval_is_not_called_ready() {
        let s = approval_standing();
        let p = Preview {
            stage: Stage::Approval,
            ..Default::default()
        };
        let l = line(&Inputs {
            pr: 42,
            standing: Some(&s),
            preview: Some(&p),
            checks_passed: true,
            ..Default::default()
        });
        assert!(
            l.ends_with("Not ready for review yet: preview waiting for an approval."),
            "{l}"
        );
        assert!(!l.contains("Ready for review:"));
    }

    #[test]
    fn a_draft_hold_comes_first() {
        let h = DraftHold::new(
            Kind::Waiting,
            vec![crate::draft_hold::Reason::new(
                "The preview checks are running",
                "",
            )],
            7,
            "",
        )
        .unwrap();
        let l = line(&Inputs {
            pr: 7,
            hold: Some(&h),
            checks_passed: true,
            ..Default::default()
        });
        assert_eq!(
            l,
            "Waiting on: the preview checks are running on #7 (CoderHelm checks again by itself and marks it ready)."
        );
    }

    #[test]
    fn nothing_known_says_nothing() {
        assert_eq!(line(&Inputs::default()), "");
    }
}
