//! Never ask a person to connect what is already connected.
//!
//! A run on a Jira ticket finished its audit, wrote eight follow-up ticket
//! drafts and stopped: "this run has no Jira access to file them. Can you
//! connect Jira?" The team had Jira connected; the run had been started from
//! that Jira ticket and commented on it. The planner's list of capabilities
//! never named the ticket tracker, so it read "Connected apps: not set up"
//! as "no Jira", and the stop judge let the question through.
//!
//! Here is what the run already reaches ([`Reach`], from the team's
//! connections), and the one test every hand-off passes: an ask or a hold
//! whose words ask a person to connect, integrate or give access to a system
//! on that list is refused ([`asks_to_connect`]). The stop judge re-asks
//! with [`rule`], and an answer that still asks becomes a proceed
//! ([`proceed`]); the plan's person steps that ask it become CoderHelm's.
//!
//! A precise gap on a connected system is still a person's (a project
//! permission the app lacks, named by Jira's answer): that wording names a
//! permission, not a connection, and passes.

use serde::{Deserialize, Serialize};

/// A system the run reaches through the team's own connection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reach {
    /// What people call it (`Jira`).
    pub system: String,
    /// The words that name it in a question, lowercase (`jira`, `atlassian`).
    pub names: Vec<String>,
    /// What CoderHelm does through it, one sentence.
    pub does: String,
}

/// Jira, connected through the Coderhelm app on `site` (empty when the
/// site isn't known). `files` says whether the app can file tickets.
pub fn jira(site: &str, files: bool) -> Reach {
    let on = if site.trim().is_empty() {
        "the team's Jira site".to_string()
    } else {
        site.trim().trim_end_matches('/').to_string()
    };
    let does = if files {
        format!(
            "Jira is connected through the Coderhelm app on {on}: CoderHelm reads tickets, \
             comments on them and files new tickets (file_followup_tickets), linked to the \
             ticket they came from."
        )
    } else {
        format!(
            "Jira is connected through the Coderhelm app on {on}: CoderHelm reads tickets and \
             comments on them."
        )
    };
    Reach {
        system: "Jira".into(),
        names: vec!["jira".into(), "atlassian".into()],
        does,
    }
}

/// Words that ask for the connection itself, not for one permission on it.
const CONNECT_WORDS: &[&str] = &[
    "connect",
    "integrat",
    "link jira",
    "link the jira",
    "set up jira",
    "install the",
    "no access",
    "without access",
    "lacks access",
    "has no access",
    "don't have access",
    "doesn't have access",
    "can't reach",
    "cannot reach",
    "no way to reach",
];

/// How far (in characters) around a system's name a connect word counts as
/// being about that system.
const NEAR: usize = 40;

/// The system on `reaches` that `words` ask a person to connect or give
/// access to, if any: a connect or access word stands next to the system's
/// name ("connect Jira", "no Jira access", "Jira isn't connected"), within
/// [`NEAR`] characters either side. A connect word further away is about
/// something else in the same text and doesn't count.
pub fn asks_to_connect<'a>(words: &str, reaches: &'a [Reach]) -> Option<&'a Reach> {
    let w = words.to_lowercase();
    reaches.iter().find(|r| {
        r.names.iter().filter(|n| !n.is_empty()).any(|n| {
            w.match_indices(n.as_str()).any(|(at, _)| {
                let from = floor(&w, at.saturating_sub(NEAR));
                let to = ceil(&w, (at + n.len() + NEAR).min(w.len()));
                let around = &w[from..to];
                CONNECT_WORDS.iter().any(|c| around.contains(c))
                    || around.contains(&format!("{n} access"))
                    || around.contains(&format!("access to {n}"))
            })
        })
    })
}

fn floor(s: &str, mut i: usize) -> usize {
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

fn ceil(s: &str, mut i: usize) -> usize {
    while i < s.len() && !s.is_char_boundary(i) {
        i += 1;
    }
    i
}

/// What the stop judge's re-ask says about an answer that asked for it.
pub fn rule(r: &Reach) -> String {
    format!(
        "{} is already connected, so a person is never asked to connect it or give access to \
         it: {} Do the step through that connection and proceed; ask only for a permission the \
         connection reported missing, by its name",
        r.system, r.does
    )
}

/// The assumption a proceed carries when an answer still asked for it.
pub fn proceed(r: &Reach) -> String {
    format!(
        "{} is connected to CoderHelm, so the run asks no one to connect it and does the step \
         through that connection. {}",
        r.system, r.does
    )
}

/// The section a model is given: what the run already reaches.
pub fn prompt_section(reaches: &[Reach]) -> String {
    if reaches.is_empty() {
        return String::new();
    }
    let mut s = String::from(
        "## Systems CoderHelm already reaches\nThese are connected. Never ask a person to \
         connect them or to give access to them; a step on one is CoderHelm's.\n",
    );
    for r in reaches {
        s.push_str(&format!("- {}: {}\n", r.system, r.does));
    }
    s.push('\n');
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asking_to_connect_a_connected_tracker_is_caught() {
        let r = [jira("https://example.atlassian.net", true)];
        for q in [
            "this run has no Jira access to file them in ABC. Can you connect Jira so CoderHelm files them?",
            "CoderHelm has no Jira connection (Connected apps: not set up)",
            "Connect Jira in Settings, Integrations",
            "Jira isn't connected, so I can't file the tickets",
            "Give CoderHelm access to Jira",
        ] {
            assert_eq!(
                asks_to_connect(q, &r).map(|r| r.system.as_str()),
                Some("Jira"),
                "{q}"
            );
        }
    }

    #[test]
    fn a_named_permission_and_other_systems_pass() {
        let r = [jira("", true)];
        for q in [
            "The Coderhelm app on the Jira site lacks the Create Issues permission in ABC; a Jira admin grants it in the project's permissions",
            "Connect the production database in Data sources",
            "Which of the two layouts should the page use?",
            "The Jira ticket names two accounts for the payment provider, and the code can only connect one of them: which?",
        ] {
            assert!(asks_to_connect(q, &r).is_none(), "{q}");
        }
        assert!(asks_to_connect("connect Jira", &[]).is_none());
    }

    #[test]
    fn the_section_and_rule_name_what_the_tracker_does() {
        let r = jira("https://example.atlassian.net/", true);
        assert!(
            r.does.contains("https://example.atlassian.net:"),
            "{}",
            r.does
        );
        assert!(r.does.contains("files new tickets"));
        assert!(rule(&r).starts_with("Jira is already connected"));
        assert!(prompt_section(&[r]).contains("Never ask a person to connect them"));
        assert!(prompt_section(&[]).is_empty());
        assert!(!jira("", false).does.contains("files new tickets"));
    }
}
