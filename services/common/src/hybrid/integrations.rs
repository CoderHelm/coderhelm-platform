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
    },
    Entry {
        id: "jira",
        name: "Jira (Forge app)",
        mode: Mode::Hosted,
        through_coderhelm: "CoderHelm's Jira app sends issue events to CoderHelm, which forwards them to your install unchanged and re-signed. The app's own secret is checked by your install, so CoderHelm can't forge an event.",
        stays_with_you: "Tickets, plans and runs. Your install calls Jira through the app's own web triggers directly.",
        reason: "",
    },
    Entry {
        id: "notion",
        name: "Notion",
        mode: Mode::Hosted,
        through_coderhelm: "You sign in through CoderHelm's Notion integration. CoderHelm keeps the sign-in (encrypted) and hands your install an access token when it syncs.",
        stays_with_you: "The pages your install reads and the index it builds. Notion's tokens don't expire by themselves: disconnecting revokes the grant at CoderHelm, and Notion's page lets you remove access completely.",
        reason: "",
    },
    Entry {
        id: "confluence",
        name: "Confluence (Atlassian)",
        mode: Mode::Hosted,
        through_coderhelm: "You sign in through CoderHelm's Atlassian app. CoderHelm keeps the sign-in (encrypted) and hands your install access tokens that expire within the hour.",
        stays_with_you: "The pages your install reads and the index it builds.",
        reason: "",
    },
    Entry {
        id: "google_drive",
        name: "Google Drive",
        mode: Mode::Hosted,
        through_coderhelm: "You sign in through CoderHelm's Google app (read-only Drive access). CoderHelm keeps the sign-in (encrypted) and hands your install access tokens that expire within the hour.",
        stays_with_you: "The files your install reads and the index it builds.",
        reason: "",
    },
    Entry {
        id: "slack_notifications",
        name: "Slack and Teams notifications",
        mode: Mode::Direct,
        through_coderhelm: "Nothing: notifications go from your install to the incoming webhook you paste.",
        stays_with_you: "Everything. No app from CoderHelm or anyone else is needed.",
        reason: "",
    },
    Entry {
        id: "mcp_apps",
        name: "Figma, Linear, Sentry, Sanity and other sign-in apps",
        mode: Mode::Direct,
        through_coderhelm: "Nothing: your install registers itself with each provider's sign-in server and keeps its own tokens.",
        stays_with_you: "Everything.",
        reason: "",
    },
    Entry {
        id: "slack_commands",
        name: "Slack and Teams commands",
        mode: Mode::Unavailable,
        through_coderhelm: "",
        stays_with_you: "",
        reason: "CoderHelm has no shared Slack or Teams app: each team connects its own app's signing secret in Plugins settings, so there is nothing to relay. That keeps working the same way on a hybrid install.",
    },
    Entry {
        id: "github_sign_in",
        name: "Sign in with GitHub",
        mode: Mode::Unavailable,
        through_coderhelm: "",
        stays_with_you: "",
        reason: "Signing people in to your install with GitHub needs your own GitHub App: a hosted app's sign-in would hand CoderHelm your people's identities. Use email sign-in, or Google with your own domain.",
    },
];

/// One entry by id.
pub fn entry(id: &str) -> Option<&'static Entry> {
    CATALOG.iter().find(|e| e.id == id)
}

/// The ids an install may opt in to (the hosted ones).
pub fn hosted_ids() -> impl Iterator<Item = &'static str> {
    CATALOG
        .iter()
        .filter(|e| e.mode == Mode::Hosted)
        .map(|e| e.id)
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
            vec!["github", "jira", "notion", "confluence", "google_drive"]
        );
        assert!(entry("github").is_some() && entry("nope").is_none());
        let j = serde_json::to_value(entry("slack_commands").unwrap()).unwrap();
        assert_eq!(j["mode"], "unavailable");
        assert!(serde_json::to_value(entry("github").unwrap())
            .unwrap()
            .get("reason")
            .is_none());
    }
}
