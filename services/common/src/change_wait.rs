//! A run waiting on another change (`awaiting_change`).
//!
//! A ticket can be about a file, a resource or a table another ticket's
//! work creates and hasn't merged yet. The planner sees that the target
//! doesn't exist yet; before this, the plan check read it as "a change
//! outside the code that leaves nothing for a person and waits for
//! nothing", planned again, and ended the run failed. It does wait for
//! something: the other change.
//!
//! [`find`] reads the planner's cause, the ticket and its links for the
//! change that creates the target (a ticket key, with its open pull request
//! when a run of the team has one). With one, the run waits
//! ([`AWAITING_CHANGE`](crate::run_status::AWAITING_CHANGE)): the
//! [`Dependency`] is kept on the run (`waiting_change`), the ticket hears
//! [`note`] once, and [`settle`] decides what the run does when the
//! dependency moves: merged, the run plans again against the new base and
//! goes on; closed without merging, it stops as "needs you" with that
//! reason; its run failed or stuck, the page says so and the self-heal
//! sweep resumes that run.

use serde::{Deserialize, Serialize};

/// Run attribute holding the [`Dependency`] JSON.
pub const FIELD: &str = "waiting_change";

/// The change a waiting run waits for.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dependency {
    /// Its ticket key, when known.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ticket: String,
    /// `owner/repo` of its pull request, when known.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub repo: String,
    /// Its pull request, when known (0: none yet).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub pr: u64,
    /// The run building it, when one of the team's runs is.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub run_id: String,
    /// When the wait started (RFC 3339).
    #[serde(default)]
    pub since: String,
    /// Whether the ticket was told (one note per wait).
    #[serde(default)]
    pub noted: bool,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

impl Dependency {
    /// What the ticket and the run page call it: `TK-1 (acme/site#12)`,
    /// `TK-1`, or `acme/site#12`.
    pub fn label(&self) -> String {
        let pr =
            (self.pr > 0 && !self.repo.is_empty()).then(|| format!("{}#{}", self.repo, self.pr));
        match (self.ticket.is_empty(), pr) {
            (false, Some(p)) => format!("{} ({p})", self.ticket),
            (false, None) => self.ticket.clone(),
            (true, Some(p)) => p,
            (true, None) => "the change this ticket builds on".to_string(),
        }
    }

    /// The same change (a wait already noted for it is not noted again).
    pub fn same(&self, other: &Dependency) -> bool {
        (!self.ticket.is_empty() && self.ticket.eq_ignore_ascii_case(&other.ticket))
            || (self.pr > 0 && self.pr == other.pr && self.repo.eq_ignore_ascii_case(&other.repo))
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".into())
    }

    pub fn from_json(s: &str) -> Option<Dependency> {
        serde_json::from_str(s).ok()
    }
}

/// The one note on the ticket.
pub fn note(dep: &Dependency) -> String {
    format!(
        "Waiting for {} to merge; this run continues by itself after that.",
        dep.label()
    )
}

/// Whether the ticket hears about this wait: once per dependency, never
/// again for a wait already noted (a redelivery, a later step, the sweep).
pub fn should_note(earlier: Option<&Dependency>, dep: &Dependency) -> bool {
    !earlier.is_some_and(|e| e.noted && e.same(dep))
}

/// Words a planner writes when what the ticket changes isn't there yet.
const NOT_THERE_YET: &[&str] = &[
    "doesn't exist yet",
    "don't exist yet",
    "does not exist yet",
    "do not exist yet",
    "isn't there yet",
    "aren't there yet",
    "not created yet",
    "not yet created",
    "isn't on main",
    "aren't on main",
    "not on main yet",
    "hasn't merged",
    "haven't merged",
    "isn't merged",
    "aren't merged",
    "not merged yet",
    "until it merges",
    "once it merges",
];

/// Whether the cause says the ticket's target doesn't exist yet.
pub fn target_missing(cause: &str) -> bool {
    let t = cause.to_lowercase().replace('’', "'");
    NOT_THERE_YET.iter().any(|m| t.contains(m))
}

/// Ticket keys (`ABC-123`) in `text`, in order, once each.
pub fn ticket_keys(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let b: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < b.len() {
        let start = i;
        let boundary = start == 0 || !b[start - 1].is_ascii_alphanumeric();
        while i < b.len() && b[i].is_ascii_uppercase() {
            i += 1;
        }
        let letters = i - start;
        if boundary && letters >= 2 && i < b.len() && b[i] == '-' {
            let d0 = i + 1;
            let mut j = d0;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            let ends = j == b.len() || !b[j].is_ascii_alphanumeric();
            if j > d0 && ends {
                let key: String = b[start..j].iter().collect();
                if !out.contains(&key) {
                    out.push(key);
                }
                i = j;
                continue;
            }
        }
        i = if i == start { i + 1 } else { i };
    }
    out
}

/// One of the team's runs for another ticket, as the record has it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OtherWork {
    pub ticket: String,
    pub run_id: String,
    pub repo: String,
    pub pr: u64,
    pub status: String,
}

/// The change a run waits for: a ticket the planner's cause names (else
/// one the ticket's description or links name) whose work is the target,
/// with its run and open pull request when the team has them. `None` when
/// the cause doesn't say the target is missing, or names no other change.
pub fn find(
    cause: &str,
    ticket_text: &str,
    linked: &[String],
    own_ticket: &str,
    work: &[OtherWork],
    now: &str,
) -> Option<Dependency> {
    if !target_missing(cause) {
        return None;
    }
    let own = own_ticket.trim();
    let mut keys: Vec<String> = ticket_keys(cause);
    for k in linked.iter().cloned().chain(ticket_keys(ticket_text)) {
        if !keys.contains(&k) {
            keys.push(k);
        }
    }
    keys.retain(|k| !k.eq_ignore_ascii_case(own));
    // The first key with work (a run, best one with its pull request);
    // else the first key the cause itself names.
    let named_in_cause = ticket_keys(cause);
    let with_work = keys.iter().find_map(|k| {
        let mut runs: Vec<&OtherWork> = work
            .iter()
            .filter(|w| w.ticket.eq_ignore_ascii_case(k))
            .collect();
        runs.sort_by_key(|w| std::cmp::Reverse(w.pr > 0));
        runs.first().map(|w| (k.clone(), (*w).clone()))
    });
    let dep = match with_work {
        Some((k, w)) => Dependency {
            ticket: k,
            repo: w.repo,
            pr: w.pr,
            run_id: w.run_id,
            since: now.to_string(),
            noted: false,
        },
        None => {
            let k = named_in_cause
                .into_iter()
                .find(|k| !k.eq_ignore_ascii_case(own))?;
            Dependency {
                ticket: k,
                since: now.to_string(),
                ..Default::default()
            }
        }
    };
    Some(dep)
}

/// Where the dependency stands now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Settled {
    /// Merged: the waiting run plans again against the new base and goes on.
    Merged,
    /// Closed without merging: the run stops as "needs you".
    ClosedUnmerged,
    /// Its run failed or is stuck: the run page says so (the self-heal
    /// sweep resumes that run); still waiting.
    DependencyStuck(String),
    /// Still open.
    Waiting,
}

/// The dependency's pull request, when it has one: `(state, merged)` as
/// GitHub gives them; its run's status, when it has one.
pub fn settle(pr: Option<(&str, bool)>, dep_run_status: Option<&str>) -> Settled {
    match pr {
        Some((_, true)) => return Settled::Merged,
        Some(("closed", false)) => return Settled::ClosedUnmerged,
        _ => {}
    }
    match dep_run_status {
        Some(s @ ("failed" | "needs_input" | "cancelled" | "closed")) => Settled::DependencyStuck(
            format!("the run building it stopped ({s}); CoderHelm resumes it when it can"),
        ),
        Some("merged") => Settled::Merged,
        _ => Settled::Waiting,
    }
}

/// The reason a run stops when its dependency closed without merging.
pub fn closed_reason(dep: &Dependency) -> String {
    format!(
        "{} was closed without merging, so what this ticket changes still doesn't exist.",
        dep.label()
    )
}

/// The line the run page shows while the dependency's run is stuck.
pub fn stuck_line(dep: &Dependency, why: &str) -> String {
    format!("Still waiting for {}: {why}.", dep.label())
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAUSE: &str = "The ticket flips dry_run in envs/prod/job.tf, but that file, the job \
        (TK-10) and the table it writes don't exist yet in any environment.";

    #[test]
    fn a_missing_target_names_the_change_that_makes_it() {
        assert!(target_missing(CAUSE));
        assert!(!target_missing(
            "The fix belongs in the vendor's dashboard."
        ));
        assert_eq!(
            ticket_keys("see TK-10, AB-2 and TK-10; not x-1 or A-1"),
            vec!["TK-10", "AB-2"]
        );
        let work = vec![OtherWork {
            ticket: "TK-10".into(),
            run_id: "R10".into(),
            repo: "acme/infra".into(),
            pr: 12,
            status: "awaiting_ci".into(),
        }];
        let d = find(CAUSE, "A follow-up of TK-10", &[], "TK-11", &work, "t").unwrap();
        assert_eq!(d.label(), "TK-10 (acme/infra#12)");
        assert_eq!(
            note(&d),
            "Waiting for TK-10 (acme/infra#12) to merge; this run continues by itself after that."
        );
        // No run for it: the ticket alone.
        let d = find(CAUSE, "", &[], "TK-11", &[], "t").unwrap();
        assert_eq!(d.label(), "TK-10");
        // Its own key is never its dependency; nothing missing, no wait.
        assert!(find("TK-11 doesn't exist yet", "", &[], "TK-11", &[], "t").is_none());
        assert!(find("needs a vendor setting", "TK-10", &[], "TK-11", &work, "t").is_none());
    }

    #[test]
    fn the_dependency_settles_the_wait() {
        assert_eq!(settle(Some(("closed", true)), None), Settled::Merged);
        assert_eq!(
            settle(Some(("closed", false)), None),
            Settled::ClosedUnmerged
        );
        assert_eq!(
            settle(Some(("open", false)), Some("running")),
            Settled::Waiting
        );
        assert!(matches!(
            settle(Some(("open", false)), Some("failed")),
            Settled::DependencyStuck(_)
        ));
        assert_eq!(settle(None, None), Settled::Waiting);
        let d = Dependency {
            ticket: "TK-10".into(),
            ..Default::default()
        };
        assert!(d.same(&Dependency {
            ticket: "tk-10".into(),
            pr: 3,
            ..Default::default()
        }));
        assert_eq!(Dependency::from_json(&d.to_json()), Some(d));
    }
}
