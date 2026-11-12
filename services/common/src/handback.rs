//! The only reasons a run hands its work to a person.
//!
//! A run that stalled, met a red check it couldn't fix, ran out of a budget,
//! or had a check error after its work used to end `needs_input` with "a
//! person should look". None of those need a person: CoderHelm changes its
//! approach, defers what resists with the reason, and ships what is done
//! (the worker's `passes::supervisor`). A person is needed only for what
//! only a person has, and this module is that list.
//!
//! [`HandBack`] is the permission to wait on a person: its fields are
//! private, so it exists only for a reason on [`HUMAN_ONLY`], and
//! `run_wait::NeedsInput::needs_input` (the one way to write the status)
//! takes one. The reason is kept on the run's wait (`handback`), so the run
//! page, analytics and the stuck-run sweep can all tell why.

use std::fmt;

/// The only reasons a run waits on a person.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HumanOnly {
    /// A secret, token or sign-in only a person can provide.
    Credential,
    /// Access CoderHelm lacks: a repository not turned on, a permission the
    /// app wasn't granted, a system no connection reaches.
    MissingAccess,
    /// A product or business choice the ticket leaves open.
    ProductDecision,
    /// The ticket reads two ways and the readings lead to different code.
    AmbiguousTicket,
    /// Another run or person already works on the same pull request or
    /// ticket.
    HeldByAnother,
    /// The team's usage limit stops runs.
    UsageLimit,
    /// The team's agent policy waits for a person's approval.
    Approval,
    /// A person stopped, reopened, redirected the run, or put its pull
    /// request back to draft.
    PersonAction,
}

/// Every reason on the list.
pub const HUMAN_ONLY: &[HumanOnly] = &[
    HumanOnly::Credential,
    HumanOnly::MissingAccess,
    HumanOnly::ProductDecision,
    HumanOnly::AmbiguousTicket,
    HumanOnly::HeldByAnother,
    HumanOnly::UsageLimit,
    HumanOnly::Approval,
    HumanOnly::PersonAction,
];

impl HumanOnly {
    /// The code kept on the run's wait.
    pub fn code(self) -> &'static str {
        match self {
            HumanOnly::Credential => "credential",
            HumanOnly::MissingAccess => "missing_access",
            HumanOnly::ProductDecision => "product_decision",
            HumanOnly::AmbiguousTicket => "ambiguous_ticket",
            HumanOnly::HeldByAnother => "held_by_another",
            HumanOnly::UsageLimit => "usage_limit",
            HumanOnly::Approval => "approval",
            HumanOnly::PersonAction => "person_action",
        }
    }

    pub fn from_code(code: &str) -> Option<HumanOnly> {
        HUMAN_ONLY.iter().copied().find(|h| h.code() == code.trim())
    }

    /// What a person reads.
    pub fn label(self) -> &'static str {
        match self {
            HumanOnly::Credential => "a credential only a person can provide",
            HumanOnly::MissingAccess => "access CoderHelm doesn't have",
            HumanOnly::ProductDecision => "a product decision",
            HumanOnly::AmbiguousTicket => "the ticket reads two ways",
            HumanOnly::HeldByAnother => "someone else is working on it",
            HumanOnly::UsageLimit => "the team's usage limit",
            HumanOnly::Approval => "an approval",
            HumanOnly::PersonAction => "a person's action on the run",
        }
    }
}

impl fmt::Display for HumanOnly {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

/// Permission to wait on a person: a reason on the list and the one thing
/// the person does. Made only by [`HandBack::new`] and [`HandBack::of`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandBack {
    why: HumanOnly,
    need: String,
}

impl HandBack {
    /// `None` without a need: a hand-back says exactly what is needed.
    pub fn new(why: HumanOnly, need: &str) -> Option<HandBack> {
        let n = need.split_whitespace().collect::<Vec<_>>().join(" ");
        (!n.is_empty()).then(|| HandBack {
            why,
            need: crate::truncate_str(&n, 600).to_string(),
        })
    }

    /// For a wait whose need is written in the wait itself (its action).
    pub fn of(why: HumanOnly) -> HandBack {
        HandBack {
            why,
            need: why.label().to_string(),
        }
    }

    pub fn why(&self) -> HumanOnly {
        self.why
    }

    pub fn need(&self) -> &str {
        &self.need
    }

    /// From what the stop judge found only a person has.
    pub fn of_person_only(p: crate::stop_judge::PersonOnly, question: &str) -> Option<HandBack> {
        use crate::stop_judge::PersonOnly as P;
        let why = match p {
            P::ProductChoice => HumanOnly::ProductDecision,
            P::Access => {
                if human_only_in(question) == Some(HumanOnly::Credential) {
                    HumanOnly::Credential
                } else {
                    HumanOnly::MissingAccess
                }
            }
            P::DivergentReadings => HumanOnly::AmbiguousTicket,
        };
        HandBack::new(why, question)
    }

    /// From the stop judge's decision: its question (always about what only
    /// a person has), or a hold whose blocker names something on the list.
    /// `None` for a proceed, and for a hold on anything else (a stall, a red
    /// check, a budget, a tool error): those are CoderHelm's to finish.
    pub fn of_judged(j: &crate::stop_judge::Judged) -> Option<HandBack> {
        use crate::stop_judge::Verdict;
        match j.verdict() {
            Verdict::Ask(q) => HandBack::of_person_only(q.person_only, &q.question),
            Verdict::Hold(h) => {
                human_only_in(&h.blocker).and_then(|why| HandBack::new(why, &h.blocker))
            }
            Verdict::Proceed { .. } => None,
        }
    }
}

/// Reads a reason on the list from words a run, a check or the judge wrote.
/// `None` when they name none, which is the case for every stall, red
/// check, tool error, time limit or budget.
pub fn human_only_in(text: &str) -> Option<HumanOnly> {
    let t = text.to_lowercase();
    let any = |marks: &[&str]| marks.iter().any(|m| t.contains(m));
    if any(&[
        "api key",
        "secret value",
        "a secret only",
        "credentials for",
        "credential for",
        "a token for",
        "password for",
        "sign in to",
        "log in to",
    ]) {
        return Some(HumanOnly::Credential);
    }
    if any(&[
        "turn the repository on",
        "not turned on",
        "the app wasn't granted",
        "the app lacks",
        "app isn't installed",
        "no access to",
        "access coderhelm doesn't have",
        "access coderhelm lacks",
        "grant access",
        "resource not accessible by integration",
        "outside the code",
        "no connection reaches",
    ]) {
        return Some(HumanOnly::MissingAccess);
    }
    if any(&[
        "product decision",
        "product choice",
        "business decision",
        "which behavior you want",
        "which behaviour you want",
    ]) {
        return Some(HumanOnly::ProductDecision);
    }
    if any(&[
        "ticket is ambiguous",
        "reads two ways",
        "divergent readings",
    ]) {
        return Some(HumanOnly::AmbiguousTicket);
    }
    if any(&["approval", "approve the"]) && any(&["policy", "agent permission", "paused"]) {
        return Some(HumanOnly::Approval);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hand_back_needs_a_listed_reason_and_what_is_needed() {
        assert!(HandBack::new(HumanOnly::Credential, "  ").is_none());
        let h = HandBack::new(HumanOnly::Credential, "Add the  PAYMENTS_KEY secret").unwrap();
        assert_eq!(h.need(), "Add the PAYMENTS_KEY secret");
        for r in HUMAN_ONLY {
            assert_eq!(HumanOnly::from_code(r.code()), Some(*r));
        }
    }

    #[test]
    fn stalls_red_checks_budgets_and_errors_are_not_on_the_list() {
        for words in [
            "Stopped: steps 2 and 3 made no progress. The branch stayed at a8c8814",
            "CI fixes stalled on the same failure",
            "A person looks at the failing check: CoderHelm's fixes keep meeting the same failure.",
            "The step reached its time limit and the run couldn't continue in a fresh step",
            "A check after the work errored: the link check.",
            "CoderHelm's fix budget for this run is spent",
            "Validation Failed: body is too long (maximum is 65536 characters)",
            "Handed to a person: CoderHelm took 2 fix rounds on its own review's findings",
            "7 checks are failing on this head",
            "The coder used this step's turns with work left",
            "Stopped before opening a pull request: 2 of 9 planned tasks are not done",
        ] {
            assert_eq!(human_only_in(words), None, "{words}");
        }
        assert_eq!(
            human_only_in("Turn the repository on for runs in CoderHelm, then press Retry"),
            Some(HumanOnly::MissingAccess)
        );
        assert_eq!(
            human_only_in("It needs the API key for the payments sandbox"),
            Some(HumanOnly::Credential)
        );
    }
}
