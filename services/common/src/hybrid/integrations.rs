//! Which integrations a hybrid install can use through CoderHelm's hosted
//! apps, and what that means for each one.
//!
//! This is the one list the API returns, the dashboard renders and the docs
//! repeat. It is honest per integration: some relay through the hosted side,
//! some never needed a hosted app (the install talks to the provider itself),
//! and some can't be offered (with the reason).

use serde::Serialize;

/// How an integration works for a hybrid install.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// CoderHelm's app; events come through the relay and tokens through the broker.
    Hosted,
    /// Works from the install directly with no app from anyone.
    Direct,
    /// On for every registered install, with nothing to choose: it works
    /// through CoderHelm's app until the install has its own.
    Included,
    /// Not offered through CoderHelm's apps.
    Unavailable,
}

/// One integration's entry.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Entry {
    pub id: &'static str,
    pub name: &'static str,
    pub mode: Mode,
    /// What CoderHelm's servers handle for it, in one sentence.
    pub through_coderhelm: &'static str,
    /// What the install does itself.
    pub stays_with_you: &'static str,
    /// For `Unavailable`: why, and what to do instead.
    #[serde(skip_serializing_if = "str::is_empty")]
    pub reason: &'static str,
    /// The pieces one entry covers, by the ids an install's choices, the
    /// relay and the broker use (Atlassian: the Jira app's events and the
    /// Confluence sign-in). Empty: the entry is one piece, its own id.
    #[serde(skip_serializing_if = "<[&str]>::is_empty")]
    pub parts: &'static [&'static str],
}

impl Entry {
    /// The ids this entry stands for: its parts, or itself.
    pub fn ids(&self) -> &[&'static str] {
        if self.parts.is_empty() {
            std::slice::from_ref(&self.id)
        } else {
            self.parts
        }
    }
}

/// Every integration, in the order the pages list them.
pub const CATALOG: &[Entry] = &[
    Entry {
        id: "github",
        name: "GitHub",
        mode: Mode::Hosted,
        through_coderhelm: "GitHub sends the app's events to CoderHelm, which checks GitHub's signature and forwards each one to your install, re-signed. Short-lived installation tokens are minted for your install only, for the repositories you chose.",
        stays_with_you: "Your code, branches, runs and reviews. CoderHelm stores no code; an event's payload is held only while its forward is retried (at most 24 hours), and delivery records (ids, types, status) are kept for 14 days.",
        reason: "",
        parts: &[],
    },
    Entry {
        id: "atlassian",
        name: "Atlassian (Jira and Confluence)",
        mode: Mode::Hosted,
        through_coderhelm: "CoderHelm's Jira app sends issue events to CoderHelm, which forwards them to your install unchanged and re-signed; the app's own secret is checked by your install, so CoderHelm can't forge an event. For Confluence you sign in through CoderHelm's Atlassian app: CoderHelm keeps the sign-in (encrypted) and hands your install access tokens that expire within the hour.",
        stays_with_you: "Tickets, plans and runs, the Confluence pages your install reads and the index it builds. Your install calls Jira through the app's own web triggers directly, and agents reach Atlassian's own MCP server from your install with its own sign-in.",
        reason: "",
        parts: &["jira", "confluence"],
    },
    Entry {
        id: "notion",
        name: "Notion",
        mode: Mode::Hosted,
        through_coderhelm: "You sign in through CoderHelm's Notion integration. CoderHelm keeps the sign-in (encrypted) and hands your install an access token when it syncs.",
        stays_with_you: "The pages your install reads and the index it builds. Notion's tokens don't expire by themselves: disconnecting revokes the grant at CoderHelm, and Notion's page lets you remove access completely.",
        reason: "",
        parts: &[],
    },
    Entry {
        id: "google_drive",
        name: "Google Drive",
        mode: Mode::Hosted,
        through_coderhelm: "You sign in through CoderHelm's Google app (read-only Drive access). CoderHelm keeps the sign-in (encrypted) and hands your install access tokens that expire within the hour.",
        stays_with_you: "The files your install reads and the index it builds.",
        reason: "",
        parts: &[],
    },
    Entry {
        id: "figma",
        name: "Figma",
        mode: Mode::Hosted,
        through_coderhelm: "You sign in through CoderHelm's Figma app (read-only: your profile and file contents). CoderHelm keeps the sign-in (encrypted) and hands your install access tokens that expire within ten minutes.",
        stays_with_you: "The designs your install reads. Agents call Figma's API from your install with those tokens.",
        reason: "",
        parts: &[],
    },
    Entry {
        id: "slack_notifications",
        name: "Slack and Teams notifications",
        mode: Mode::Direct,
        through_coderhelm: "Nothing: notifications go from your install to the incoming webhook you paste.",
        stays_with_you: "Everything. No app from CoderHelm or anyone else is needed.",
        reason: "",
        parts: &[],
    },
    Entry {
        id: "mcp_apps",
        name: "Linear, Sentry, Sanity and other sign-in apps",
        mode: Mode::Direct,
        through_coderhelm: "Nothing: your install registers itself with each provider's sign-in server and keeps its own tokens.",
        stays_with_you: "Everything.",
        reason: "",
        parts: &[],
    },
    Entry {
        id: "slack_commands",
        name: "Slack and Teams commands",
        mode: Mode::Unavailable,
        through_coderhelm: "",
        stays_with_you: "",
        reason: "CoderHelm has no shared Slack or Teams app: each team connects its own app's signing secret in Plugins settings, so there is nothing to relay. That keeps working the same way on a hybrid install.",
        parts: &[],
    },
    Entry {
        id: "github_sign_in",
        name: "Sign in with GitHub",
        mode: Mode::Included,
        through_coderhelm: "Without a GitHub OAuth client of your own, GitHub's sign-in page is CoderHelm's GitHub App: CoderHelm passes GitHub's one-time code to your install, exchanges it when your install asks (with a secret only your install holds), and revokes the token at GitHub seconds later. It stores neither and doesn't ask GitHub who signed in.",
        stays_with_you: "Who signs in: your install asks GitHub itself whose account it is, then applies its own rules (team members and invites, two-factor). CoderHelm can't start a session on your install or choose who signs in.",
        reason: "",
        parts: &[],
    },
];

/// One entry by its id or by the id of a piece it covers (`jira` and
/// `confluence` are both Atlassian's).
pub fn entry(id: &str) -> Option<&'static Entry> {
    CATALOG.iter().find(|e| e.id == id || e.parts.contains(&id))
}

/// The ids an install may opt in to (the hosted pieces). These are what
/// installs saved, and what the relay and the broker check, so an entry that
/// covers several pieces keeps every choice made before it covered them.
pub fn hosted_ids() -> impl Iterator<Item = &'static str> {
    CATALOG
        .iter()
        .filter(|e| e.mode == Mode::Hosted)
        .flat_map(|e| e.ids().iter().copied())
}

/// The piece ids an id from a page names: an entry's own id (`atlassian`)
/// stands for its pieces, anything else for itself.
pub fn expand(id: &str) -> Vec<String> {
    match CATALOG.iter().find(|e| e.id == id && !e.parts.is_empty()) {
        Some(e) => e.parts.iter().map(|p| p.to_string()).collect(),
        None => vec![id.to_string()],
    }
}

/// The tools the install-wide Other apps choice covers (Settings, System,
/// Hybrid, and the install guide): every hosted one but GitHub, which has its
/// own choice. An entry added here is on for every install that chose
/// CoderHelm's apps, with nothing more to choose.
pub fn app_choice_ids() -> impl Iterator<Item = &'static str> {
    hosted_ids().filter(|id| *id != "github")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_entry_says_what_happens_or_why_not() {
        let mut seen = std::collections::HashSet::new();
        for e in CATALOG {
            assert!(seen.insert(e.id), "duplicate {}", e.id);
            match e.mode {
                Mode::Unavailable => {
                    assert!(e.reason.ends_with('.') && e.through_coderhelm.is_empty())
                }
                _ => assert!(
                    e.through_coderhelm.ends_with('.')
                        && e.stays_with_you.ends_with('.')
                        && e.reason.is_empty()
                ),
            }
        }
    }

    #[test]
    fn the_hosted_ones_are_the_ones_the_broker_and_relay_serve() {
        let ids: Vec<_> = hosted_ids().collect();
        assert_eq!(
            ids,
            vec![
                "github",
                "jira",
                "confluence",
                "notion",
                "google_drive",
                "figma"
            ]
        );
        assert_eq!(
            app_choice_ids().collect::<Vec<_>>(),
            vec!["jira", "confluence", "notion", "google_drive", "figma"]
        );
        assert!(entry("github").is_some() && entry("nope").is_none());
        // One Atlassian entry; the pieces installs chose before keep their ids.
        assert_eq!(entry("jira").unwrap().id, "atlassian");
        assert_eq!(entry("confluence").unwrap().id, "atlassian");
        assert_eq!(
            CATALOG
                .iter()
                .filter(|e| e.name.contains("Atlassian") || e.name.contains("Jira"))
                .count(),
            1
        );
        assert_eq!(expand("atlassian"), vec!["jira", "confluence"]);
        assert_eq!(expand("notion"), vec!["notion"]);
        let a = serde_json::to_value(entry("atlassian").unwrap()).unwrap();
        assert_eq!(a["parts"], serde_json::json!(["jira", "confluence"]));
        let j = serde_json::to_value(entry("slack_commands").unwrap()).unwrap();
        assert_eq!(j["mode"], "unavailable");
        assert!(serde_json::to_value(entry("github").unwrap())
            .unwrap()
            .get("reason")
            .is_none());
    }
}
