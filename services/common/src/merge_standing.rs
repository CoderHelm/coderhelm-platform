//! Where a finished run's pull request stands on its way to merge, in the
//! one reason that is true.
//!
//! A run page once said "Needs a person" and "merging is your team's call"
//! for every finished run with an open pull request, whatever held the
//! merge: auto-merge being off, the repository asking for a person's
//! approval, CoderHelm's own review still asking for changes, or nothing at
//! all. A person is needed only for what only a person can give, and then
//! the page says exactly which thing that is and where it is set.
//!
//! [`decide`] reads the facts in the order they hold the merge and returns
//! one [`Standing`]: its code (stable, for the dashboard and analytics), the
//! sentence shown, whether a person has to act, and the setting it comes
//! from when it comes from a setting. The worker writes it on the run
//! record ([`FIELD`]) whenever it looks at the pull request.

use serde::{Deserialize, Serialize};

/// The run record's field holding the standing, as JSON.
pub const FIELD: &str = "merge_standing";

/// What the worker knows about the pull request and the repository's rules.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Facts {
    pub merged: bool,
    pub closed: bool,
    pub draft: bool,
    /// The pull request's head commit.
    pub head: String,
    /// CoderHelm's reviewer is on for the repository.
    pub reviewer_on: bool,
    /// CoderHelm's latest review verdict and the commit it is on.
    pub own_verdict: Option<(String, String)>,
    /// A fix round or a review of the head is queued or running.
    pub working: bool,
    /// Checks failing on the head (names), and whether any still runs.
    pub failing: Vec<String>,
    pub checks_running: bool,
    /// The pull request conflicts with its base.
    pub conflict: bool,
    /// The repository's auto-merge setting, and whether it asks for a
    /// person's approval before CoderHelm merges.
    pub auto_merge: bool,
    pub require_human_approval: bool,
    /// A person approved the head.
    pub human_approved: bool,
    /// GitHub reports the merge blocked by the branch's rules although the
    /// checks are green (`mergeable_state` = `blocked`).
    pub blocked_by_rules: bool,
}

/// One standing, as shown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Standing {
    /// Stable code: `merged`, `closed`, `reviewing`, `fixing_review`,
    /// `review_changes_requested`, `checks_running`, `checks_failing`,
    /// `conflict`, `draft`, `auto_merge_off`, `needs_person_approval`,
    /// `repo_requires_person_review`, `merging`.
    pub code: String,
    /// What is true now, one sentence.
    pub text: String,
    /// What happens next, one sentence.
    pub next: String,
    /// A person has to act for the merge to happen.
    pub needs_person: bool,
    /// Where the rule is set, when it is a setting (a dashboard path, or a
    /// GitHub URL); empty otherwise.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub link: String,
}

impl Standing {
    fn new(code: &str, text: String, next: &str, needs_person: bool, link: String) -> Standing {
        Standing {
            code: code.to_string(),
            text,
            next: next.to_string(),
            needs_person,
            link,
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    pub fn from_json(s: &str) -> Option<Standing> {
        serde_json::from_str(s).ok()
    }
}

fn short(sha: &str) -> &str {
    sha.get(..7).unwrap_or(sha)
}

/// The dashboard page where a repository's reviewer and auto-merge are set.
pub fn settings_path(_repo: &str) -> String {
    "/reviewer/config".to_string()
}

/// GitHub's page for a repository's branch rules.
pub fn branch_rules_url(repo: &str) -> String {
    format!("https://github.com/{repo}/settings/branches")
}

/// The standing of `repo`'s pull request, from the facts, first holder
/// first. CoderHelm's own work (a review of the head, a fix round, checks)
/// never asks for a person; only the team's own rules do.
pub fn decide(repo: &str, f: &Facts) -> Standing {
    let none = String::new;
    if f.merged {
        return Standing::new(
            "merged",
            "The pull request is merged.".into(),
            "CoderHelm watches the deploy of the merge commit.",
            false,
            none(),
        );
    }
    if f.closed {
        return Standing::new(
            "closed",
            "The pull request was closed without merging.".into(),
            "Nothing further runs on it.",
            false,
            none(),
        );
    }
    if f.conflict {
        return Standing::new(
            "conflict",
            "The pull request conflicts with its base branch.".into(),
            "CoderHelm merges the base branch in and resolves the conflict.",
            false,
            none(),
        );
    }
    if !f.failing.is_empty() && !f.checks_running {
        return Standing::new(
            "checks_failing",
            format!(
                "Checks fail on `{}`: {}.",
                short(&f.head),
                f.failing
                    .iter()
                    .take(4)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            "CoderHelm fixes them, or says on the pull request why they are not this change's.",
            false,
            none(),
        );
    }
    if f.reviewer_on {
        match &f.own_verdict {
            Some((verdict, sha)) if sha == &f.head && verdict == "APPROVE" => {}
            Some((verdict, sha)) if sha == &f.head && verdict == "REQUEST_CHANGES" => {
                return if f.working {
                    Standing::new(
                        "fixing_review",
                        format!(
                            "CoderHelm's review asked for changes on `{}` and a fix round is \
                             taking them.",
                            short(sha)
                        ),
                        "The fix is pushed and the new head is reviewed.",
                        false,
                        none(),
                    )
                } else {
                    Standing::new(
                        "review_changes_requested",
                        format!(
                            "CoderHelm's review asks for changes on `{}`; what it could not fix \
                             is listed as deferred on the pull request.",
                            short(sha)
                        ),
                        "CoderHelm does not merge over its own review; the deferred findings \
                         say what each needs.",
                        false,
                        none(),
                    )
                };
            }
            // No verdict, or one on an older commit: never shown as the
            // verdict on this head.
            other => {
                let was = other
                    .as_ref()
                    .map(|(_, sha)| format!(" (its last review was of `{}`)", short(sha)))
                    .unwrap_or_default();
                return Standing::new(
                    "reviewing",
                    format!("CoderHelm is reviewing the head `{}`{was}.", short(&f.head)),
                    "Its verdict on this commit decides what follows: a fix round, or the merge.",
                    false,
                    none(),
                );
            }
        }
    }
    if f.checks_running {
        return Standing::new(
            "checks_running",
            format!("Checks are still running on `{}`.", short(&f.head)),
            "CoderHelm reads their result when they finish.",
            false,
            none(),
        );
    }
    if f.draft {
        return Standing::new(
            "draft",
            "The pull request is still a draft; its description says why at the top.".into(),
            "CoderHelm marks it ready once what holds it is settled.",
            false,
            none(),
        );
    }
    if !f.auto_merge {
        return Standing::new(
            "auto_merge_off",
            "Auto-merge is off for this repo, so CoderHelm leaves the merge to your team.".into(),
            "Merge it, or turn auto-merge on and CoderHelm merges approved pull requests itself.",
            true,
            settings_path(repo),
        );
    }
    if f.require_human_approval && !f.human_approved {
        return Standing::new(
            "needs_person_approval",
            "This repo's auto-merge setting requires an approving review from a person before \
             CoderHelm merges."
                .into(),
            "Once a person approves this commit, CoderHelm merges it and follows the deploy.",
            true,
            settings_path(repo),
        );
    }
    if f.blocked_by_rules {
        return Standing::new(
            "repo_requires_person_review",
            "Your repo's branch rules block the merge: they require an approving review from a \
             person (or another rule GitHub enforces on this branch)."
                .into(),
            "Once the branch's rules are met, CoderHelm merges it and follows the deploy.",
            true,
            branch_rules_url(repo),
        );
    }
    Standing::new(
        "merging",
        "Approved, checks green, and the repo's rules allow it: CoderHelm merges it.".into(),
        "After the merge CoderHelm watches the deploy of the merge commit.",
        false,
        none(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEAD: &str = "9f8e7d6c5b4a39281706f5e4d3c2b1a098765432";
    const OLD: &str = "1111111aaaaaaa2222222bbbbbbb3333333ccccc";

    /// Everything in place for CoderHelm to merge on its own.
    fn clear() -> Facts {
        Facts {
            head: HEAD.into(),
            reviewer_on: true,
            own_verdict: Some(("APPROVE".into(), HEAD.into())),
            auto_merge: true,
            require_human_approval: false,
            ..Default::default()
        }
    }

    #[test]
    fn approved_green_and_allowed_merges_with_nobody_asked() {
        let s = decide("acme/shop", &clear());
        assert_eq!(s.code, "merging");
        assert!(!s.needs_person);
        assert!(s.next.contains("watches the deploy"));
    }

    #[test]
    fn a_verdict_on_an_older_commit_is_never_the_standing_verdict() {
        let mut f = clear();
        f.own_verdict = Some(("REQUEST_CHANGES".into(), OLD.into()));
        let s = decide("acme/shop", &f);
        assert_eq!(s.code, "reviewing");
        assert!(!s.needs_person, "a review of the head is CoderHelm's work");
        assert!(
            s.text.contains("`9f8e7d6`") && s.text.contains("its last review was of `1111111`")
        );
        assert!(!s.text.to_lowercase().contains("changes requested"));
        f.own_verdict = None;
        assert_eq!(decide("acme/shop", &f).code, "reviewing");
    }

    #[test]
    fn changes_requested_on_the_head_is_coderhelms_to_fix() {
        let mut f = clear();
        f.own_verdict = Some(("REQUEST_CHANGES".into(), HEAD.into()));
        f.working = true;
        let s = decide("acme/shop", &f);
        assert_eq!(s.code, "fixing_review");
        assert!(!s.needs_person);
        f.working = false;
        let s = decide("acme/shop", &f);
        assert_eq!(s.code, "review_changes_requested");
        assert!(!s.needs_person);
        assert!(s.text.contains("deferred"));
    }

    #[test]
    fn a_person_is_asked_only_for_the_teams_own_rule_and_the_rule_is_named() {
        let mut off = clear();
        off.auto_merge = false;
        let s = decide("acme/shop", &off);
        assert_eq!(s.code, "auto_merge_off");
        assert!(s.needs_person);
        assert!(s.text.starts_with("Auto-merge is off for this repo"));
        assert_eq!(s.link, "/reviewer/config");

        let mut key = clear();
        key.require_human_approval = true;
        let s = decide("acme/shop", &key);
        assert_eq!(s.code, "needs_person_approval");
        assert!(s.needs_person);
        assert!(s
            .text
            .contains("requires an approving review from a person"));
        key.human_approved = true;
        assert_eq!(decide("acme/shop", &key).code, "merging");

        let mut rules = clear();
        rules.blocked_by_rules = true;
        let s = decide("acme/shop", &rules);
        assert_eq!(s.code, "repo_requires_person_review");
        assert!(s
            .text
            .starts_with("Your repo's branch rules block the merge"));
        assert_eq!(s.link, "https://github.com/acme/shop/settings/branches");
    }

    #[test]
    fn coderhelms_own_work_comes_before_any_ask_of_a_person() {
        // Auto-merge off AND red checks: the checks are said, not the setting.
        let mut f = clear();
        f.auto_merge = false;
        f.failing = vec!["unit".into()];
        let s = decide("acme/shop", &f);
        assert_eq!(s.code, "checks_failing");
        assert!(!s.needs_person);
        f.failing.clear();
        f.conflict = true;
        assert_eq!(decide("acme/shop", &f).code, "conflict");
        f.conflict = false;
        f.checks_running = true;
        assert_eq!(decide("acme/shop", &f).code, "checks_running");
    }

    #[test]
    fn no_standing_says_a_person_is_needed_without_naming_why() {
        let mut all = vec![clear()];
        for i in 0..7 {
            let mut f = clear();
            match i {
                0 => f.auto_merge = false,
                1 => f.require_human_approval = true,
                2 => f.blocked_by_rules = true,
                3 => f.draft = true,
                4 => f.merged = true,
                5 => f.closed = true,
                _ => f.reviewer_on = false,
            }
            all.push(f);
        }
        for f in all {
            let s = decide("acme/shop", &f);
            assert!(!s.text.is_empty() && !s.next.is_empty(), "{s:?}");
            if s.needs_person {
                assert!(
                    !s.link.is_empty(),
                    "a person's turn names its setting: {s:?}"
                );
            }
            assert_eq!(Standing::from_json(&s.to_json()), Some(s));
        }
    }
}
