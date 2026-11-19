//! One answer to "is Jira connected", and to "which of its abilities can be
//! used right now", from the team's `JIRA#config` record.
//!
//! The integrations page, chat and the runs all read the same record, and
//! used to ask it different questions: the page counted the Jira app as
//! connected once it had registered its comment trigger, while chat counted
//! Jira as connected only when the ticket lookup trigger was there too. A
//! site whose app gained the lookup after it registered (Forge rolls a new
//! minor version out to every site without telling the app) showed as
//! connected on the page and ran tickets, while chat told people Jira
//! wasn't connected at all.
//!
//! So here:
//!
//! - [`Connection::connected`] is the only test of "connected", for every
//!   reader: the app registered with its secret, or a plain Jira webhook.
//! - [`Connection::ticket_reads`] says whether tickets can be read on
//!   demand, and when not, exactly which ability is missing and why
//!   ([`Gap`], [`Gap::words`]): never "not connected" for a connected team.
//! - [`missing_registrations`] names the triggers the current app always
//!   registers that the record lacks, so the caller asks the app to register
//!   again by itself ([`REGISTER_AGAIN`]) instead of telling a person.
//! - [`registered`] lists the triggers a record holds, so a self-hosted
//!   install linked through CoderHelm knows when the hosted record gained one
//!   and copies it again.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

type Item = HashMap<String, AttributeValue>;

/// The Jira app's triggers: the `JIRA#config` field that holds each, and the
/// trigger's name (as the hybrid proxy path names it).
pub const TRIGGERS: &[(&str, &str)] = &[
    ("list_projects_url", "list_projects"),
    ("create_ticket_url", "create_ticket"),
    ("add_comment_url", "add_comment"),
    ("publish_release_url", "publish_release"),
    ("get_issues_url", "get_issues"),
    ("confluence_trigger_url", "confluence"),
];

/// The triggers the current app registers on every site
/// (`coderhelm-jira/src/register.js`). The release and Confluence triggers
/// are left empty by the app on purpose (Confluence goes through the
/// Atlassian connection), so their absence is never a gap.
pub const ALWAYS_REGISTERED: &[&str] = &[
    "list_projects_url",
    "create_ticket_url",
    "add_comment_url",
    "get_issues_url",
];

/// The body that asks the app, through its project-list trigger, to send
/// all of its trigger links to CoderHelm again (`coderhelm-jira`'s
/// `listProjectsHandler`). The app checks `forge_secret` against its own.
pub const REGISTER_AGAIN: &str = "register_again";

/// Words for a team with no Jira connection at all.
pub const NOT_CONNECTED: &str = "Jira isn't connected to Coderhelm";

fn text(item: &Item, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .map(|v| v.trim().to_string())
        .unwrap_or_default()
}

/// A trigger link the code may call: https only.
fn link(item: &Item, k: &str) -> Option<String> {
    Some(text(item, k)).filter(|u| u.starts_with("https://"))
}

/// The team's Jira connection, as every reader sees it.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Connection {
    /// The Coderhelm app registered on a site: its comment trigger and the
    /// shared secret are kept.
    pub app: bool,
    /// A plain Jira webhook is set up (the `JIRA_SECRET` record).
    pub webhook: bool,
    /// `https://<site>.atlassian.net`, when known.
    pub site: String,
    secret: String,
    issues_url: Option<String>,
    list_projects_url: Option<String>,
}

// By hand, so the secret never reaches a log.
impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("app", &self.app)
            .field("webhook", &self.webhook)
            .field("site", &self.site)
            .finish_non_exhaustive()
    }
}

/// Why tickets can't be read on demand from a connected team's Jira.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gap {
    /// No Jira connection at all.
    NotConnected,
    /// The app is connected but hasn't registered its ticket lookup with
    /// Coderhelm yet (it gained it after it registered).
    LookupNotRegistered,
    /// Only a plain Jira webhook is set up: it sends events, it can't be
    /// asked for tickets.
    WebhookOnly,
}

impl Gap {
    /// The plain sentence for the answer.
    pub fn words(self) -> &'static str {
        match self {
            Gap::NotConnected => NOT_CONNECTED,
            Gap::LookupNotRegistered => "Jira is connected, but the Coderhelm app on the Jira site hasn't registered its ticket lookup with Coderhelm yet, so tickets can't be looked up on demand right now (Coderhelm has asked the app to register it again, which usually takes a minute)",
            Gap::WebhookOnly => "Jira is connected through a Jira webhook, which sends Coderhelm ticket events but can't be asked for tickets (the Coderhelm app in Jira adds ticket lookups)",
        }
    }
}

/// What a ticket lookup can use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TicketReads {
    /// The app's lookup trigger, its secret and the site.
    Ready {
        issues_url: String,
        secret: String,
        site: String,
    },
    /// Not available, and why.
    Off(Gap),
}

impl Connection {
    /// From the team's `JIRA#config` (if any) and whether a plain Jira
    /// webhook is set up.
    pub fn from_config(item: Option<&Item>, webhook: bool) -> Connection {
        let Some(item) = item else {
            return Connection {
                webhook,
                ..Connection::default()
            };
        };
        let secret = text(item, "forge_secret");
        Connection {
            // The same test the integrations page always made, plus the
            // secret every call to the app needs.
            app: !text(item, "add_comment_url").is_empty() && !secret.is_empty(),
            webhook,
            site: Some(text(item, "site_url"))
                .filter(|u| u.starts_with("https://"))
                .unwrap_or_default(),
            secret,
            issues_url: link(item, "get_issues_url"),
            list_projects_url: link(item, "list_projects_url"),
        }
    }

    /// Whether Jira is connected: the app or a plain webhook.
    pub fn connected(&self) -> bool {
        self.app || self.webhook
    }

    /// Whether tickets can be read on demand, and if not, why.
    pub fn ticket_reads(&self) -> TicketReads {
        match (&self.issues_url, self.app) {
            (Some(u), true) => TicketReads::Ready {
                issues_url: u.clone(),
                secret: self.secret.clone(),
                site: self.site.clone(),
            },
            (_, true) => TicketReads::Off(Gap::LookupNotRegistered),
            (_, false) if self.webhook => TicketReads::Off(Gap::WebhookOnly),
            _ => TicketReads::Off(Gap::NotConnected),
        }
    }

    /// Where to ask the app to register again, and the body to send, when it
    /// is connected but lacks a trigger it always registers. `None` when
    /// nothing is missing or the app can't be reached.
    pub fn register_again(&self, item: Option<&Item>) -> Option<(String, serde_json::Value)> {
        if !self.app
            || item
                .map(missing_registrations)
                .unwrap_or_default()
                .is_empty()
        {
            return None;
        }
        let url = self.list_projects_url.clone()?;
        Some((
            url,
            serde_json::json!({ REGISTER_AGAIN: true, "forge_secret": self.secret }),
        ))
    }
}

/// The triggers the current app always registers that the record lacks
/// (empty for a record without the app).
pub fn missing_registrations(item: &Item) -> Vec<&'static str> {
    if text(item, "add_comment_url").is_empty() || text(item, "forge_secret").is_empty() {
        return Vec::new();
    }
    ALWAYS_REGISTERED
        .iter()
        .copied()
        .filter(|k| text(item, k).is_empty())
        .collect()
}

/// The trigger names a record holds a link for, in [`TRIGGERS`] order.
pub fn registered(item: &Item) -> Vec<&'static str> {
    TRIGGERS
        .iter()
        .filter(|(field, _)| !text(item, field).is_empty())
        .map(|(_, name)| *name)
        .collect()
}

/// Whether a self-hosted install's copy of a linked site's settings is out
/// of date: the hosted record names triggers (`hosted`, from
/// [`registered`]) that differ from the ones last copied. `None` from a
/// hosted side that doesn't send the list (then only a relink copies).
pub fn copy_is_stale(hosted: Option<&[String]>, copied: &[String]) -> bool {
    hosted.is_some_and(|h| {
        let mut a: Vec<&str> = h.iter().map(String::as_str).collect();
        let mut b: Vec<&str> = copied.iter().map(String::as_str).collect();
        a.sort_unstable();
        b.sort_unstable();
        a != b
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), AttributeValue::S(v.to_string())))
            .collect()
    }

    /// The app registered before it had the ticket lookup.
    fn before_lookup() -> Item {
        item(&[
            (
                "list_projects_url",
                "https://x.hello.atlassian-dev.net/x1/lp",
            ),
            (
                "create_ticket_url",
                "https://x.hello.atlassian-dev.net/x1/ct",
            ),
            ("add_comment_url", "https://x.hello.atlassian-dev.net/x1/ac"),
            ("publish_release_url", ""),
            ("get_issues_url", ""),
            ("confluence_trigger_url", ""),
            ("forge_secret", "secret-1"),
            ("site_url", "https://example.atlassian.net"),
        ])
    }

    #[test]
    fn an_app_without_the_lookup_is_connected_and_names_the_missing_ability() {
        let i = before_lookup();
        let c = Connection::from_config(Some(&i), false);
        assert!(c.connected());
        assert_eq!(c.ticket_reads(), TicketReads::Off(Gap::LookupNotRegistered));
        assert!(!Gap::LookupNotRegistered.words().contains("isn't connected"));
        assert_eq!(missing_registrations(&i), vec!["get_issues_url"]);
        let (url, body) = c.register_again(Some(&i)).unwrap();
        assert!(url.ends_with("/lp"));
        assert_eq!(body[REGISTER_AGAIN], true);
        assert_eq!(body["forge_secret"], "secret-1");
        // The secret never reaches a log.
        assert!(!format!("{c:?}").contains("secret-1"));
    }

    #[test]
    fn a_full_registration_is_ready_and_asks_nothing() {
        let mut i = before_lookup();
        i.insert(
            "get_issues_url".into(),
            AttributeValue::S("https://x.hello.atlassian-dev.net/x1/gi".into()),
        );
        let c = Connection::from_config(Some(&i), false);
        assert!(
            matches!(c.ticket_reads(), TicketReads::Ready { ref issues_url, .. } if issues_url.ends_with("/gi"))
        );
        assert!(missing_registrations(&i).is_empty());
        assert!(c.register_again(Some(&i)).is_none());
        assert_eq!(
            registered(&i),
            vec![
                "list_projects",
                "create_ticket",
                "add_comment",
                "get_issues"
            ]
        );
    }

    #[test]
    fn only_no_connection_at_all_is_not_connected() {
        let none = Connection::from_config(None, false);
        assert!(!none.connected());
        assert_eq!(none.ticket_reads(), TicketReads::Off(Gap::NotConnected));
        let hook = Connection::from_config(None, true);
        assert!(hook.connected());
        assert_eq!(hook.ticket_reads(), TicketReads::Off(Gap::WebhookOnly));
        assert!(!Gap::WebhookOnly.words().contains("isn't connected"));
        // A record without the app's secret isn't the app.
        let mut i = before_lookup();
        i.remove("forge_secret");
        assert!(!Connection::from_config(Some(&i), false).connected());
        assert!(missing_registrations(&i).is_empty());
    }

    #[test]
    fn a_copy_is_stale_when_the_hosted_triggers_differ() {
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let copied = s(&["list_projects", "create_ticket", "add_comment"]);
        let hosted = s(&[
            "add_comment",
            "create_ticket",
            "get_issues",
            "list_projects",
        ]);
        assert!(copy_is_stale(Some(&hosted), &copied));
        assert!(!copy_is_stale(Some(&hosted), &hosted));
        // Order doesn't matter.
        let shuffled = s(&[
            "get_issues",
            "list_projects",
            "add_comment",
            "create_ticket",
        ]);
        assert!(!copy_is_stale(Some(&hosted), &shuffled));
        // An older hosted side that sends no list: nothing to compare.
        assert!(!copy_is_stale(None, &copied));
    }
}
