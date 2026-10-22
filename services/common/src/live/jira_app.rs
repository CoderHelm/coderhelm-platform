//! Confluence through the Coderhelm app a team installed in its Jira.
//!
//! The Jira app already has Confluence access on its site (its manifest asks
//! for page read and write scopes), so a team with the app on a site that has
//! Confluence doesn't need a separate API token. When the app registers its
//! web triggers it sends a Confluence trigger only if Confluence answered on
//! that site; that trigger's address, the app's shared secret and the site
//! are kept in the team's `JIRA#config`.
//!
//! [`with_jira_app`] folds that into the team's connections: a Confluence
//! connection that goes through the app appears on its own, reading only.
//! Writes stay off until an admin turns them on and lists the pages, the same
//! as with a token; the settings live in the usual `CONNECTOR#confluence`
//! record (with `via` set), so turning it off or listing pages sticks. A team
//! that connected Confluence with its own API token keeps that connection.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use super::confluence::check_site;
use super::connectors::{self, ConnKind, Connector, VIA_JIRA_APP};

type Item = HashMap<String, AttributeValue>;

/// The settings-table record the Jira app's trigger addresses live in.
pub const JIRA_CONFIG_SK: &str = "JIRA#config";
/// The `JIRA#config` field holding the app's Confluence trigger address.
pub const CONFLUENCE_TRIGGER_FIELD: &str = "confluence_trigger_url";

/// The team's Jira app, as far as Confluence needs it.
#[derive(Clone, PartialEq, Eq)]
pub struct JiraApp {
    /// `https://<site>.atlassian.net`, checked.
    pub site: String,
    /// The app's Confluence web trigger.
    pub confluence_url: String,
    /// The shared secret the app checks on every call.
    pub secret: String,
}

// Written by hand so the secret never reaches a log.
impl std::fmt::Debug for JiraApp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JiraApp")
            .field("site", &self.site)
            .finish_non_exhaustive()
    }
}

fn s(item: &Item, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .map(|v| v.trim().to_string())
        .unwrap_or_default()
}

impl JiraApp {
    /// From a `JIRA#config` record: only when the app registered a Confluence
    /// trigger, has its secret, and is on an Atlassian Cloud site.
    pub fn from_item(item: &Item) -> Option<JiraApp> {
        let confluence_url = s(item, CONFLUENCE_TRIGGER_FIELD);
        let secret = s(item, "forge_secret");
        if !confluence_url.starts_with("https://") || secret.is_empty() {
            return None;
        }
        let site = check_site(&s(item, "site_url")).ok()?;
        Some(JiraApp {
            site,
            confluence_url,
            secret,
        })
    }

    /// The connection a team gets from the app before anyone changes it:
    /// on, reading only.
    pub fn default_confluence(&self) -> Connector {
        Connector {
            enabled: true,
            status: "ready".into(),
            status_message: "Connected through the Jira app.".into(),
            site: self.site.clone(),
            via: VIA_JIRA_APP.into(),
            ..Connector::new(ConnKind::Confluence)
        }
    }
}

/// The team's Jira app, when it can reach Confluence. `None` without one, or
/// when the table isn't set (local runs).
pub async fn load(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
) -> Option<JiraApp> {
    if table.is_empty() {
        return None;
    }
    let out = dynamo
        .get_item()
        .table_name(table)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S(JIRA_CONFIG_SK.to_string()))
        .send()
        .await
        .map_err(|e| tracing::warn!(error = %e, "Could not read the Jira app's settings"))
        .ok()?;
    out.item().and_then(JiraApp::from_item)
}

/// The team's connections with Confluence through the Jira app folded in:
///
/// - a Confluence connection with its own token stays as it is;
/// - one saved to go through the app takes the app's site, or, when the app
///   can't reach Confluence any more, is shown as failing (and isn't used);
/// - with none saved and the app able to reach Confluence, the app's default
///   (on, reading only) is added.
pub fn with_jira_app(mut saved: Vec<Connector>, app: Option<&JiraApp>) -> Vec<Connector> {
    match saved.iter_mut().find(|c| c.kind() == ConnKind::Confluence) {
        Some(c) if c.via_jira_app() => match app {
            Some(a) => c.site = a.site.clone(),
            None => {
                c.status = "error".into();
                c.status_message = "The Coderhelm app in Jira can't reach Confluence. Update the app in Jira, then open Coderhelm Settings there and save.".into();
            }
        },
        Some(_) => {}
        None => {
            if let Some(a) = app {
                saved.push(a.default_confluence());
            }
        }
    }
    saved
}

/// The team's connections ([`connectors::list`]) with Confluence through the
/// Jira app folded in, and the app (to reach it).
pub async fn list_connectors(
    dynamo: &aws_sdk_dynamodb::Client,
    settings_table: &str,
    jira_config_table: &str,
    team_id: &str,
) -> Result<(Vec<Connector>, Option<JiraApp>), String> {
    let (saved, app) = tokio::join!(
        connectors::list(dynamo, settings_table, team_id),
        load(dynamo, jira_config_table, team_id)
    );
    Ok((with_jira_app(saved?, app.as_ref()), app))
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

    fn app() -> JiraApp {
        JiraApp {
            site: "https://yourcompany.atlassian.net".into(),
            confluence_url: "https://example.invalid/x1/trigger".into(),
            secret: "s3cret".into(),
        }
    }

    #[test]
    fn the_app_counts_only_with_a_confluence_trigger_a_secret_and_a_cloud_site() {
        let full = item(&[
            ("site_url", "https://YourCompany.atlassian.net"),
            ("forge_secret", "s3cret"),
            (
                CONFLUENCE_TRIGGER_FIELD,
                "https://example.invalid/x1/trigger",
            ),
        ]);
        let a = JiraApp::from_item(&full).unwrap();
        assert_eq!(a.site, "https://yourcompany.atlassian.net");
        assert_eq!(a.secret, "s3cret");
        for missing in ["site_url", "forge_secret", CONFLUENCE_TRIGGER_FIELD] {
            let mut i = full.clone();
            i.remove(missing);
            assert!(JiraApp::from_item(&i).is_none(), "without {missing}");
        }
        let mut http = full.clone();
        http.insert(
            CONFLUENCE_TRIGGER_FIELD.into(),
            AttributeValue::S("http://example.invalid/x".into()),
        );
        assert!(JiraApp::from_item(&http).is_none(), "https only");
        let mut other = full;
        other.insert(
            "site_url".into(),
            AttributeValue::S("https://example.com".into()),
        );
        assert!(JiraApp::from_item(&other).is_none(), "Atlassian Cloud only");
    }

    #[test]
    fn the_secret_never_prints() {
        assert!(!format!("{:?}", app()).contains("s3cret"));
    }

    #[test]
    fn with_the_app_and_nothing_saved_confluence_reads_through_it() {
        let out = with_jira_app(Vec::new(), Some(&app()));
        assert_eq!(out.len(), 1);
        let c = &out[0];
        assert_eq!(c.kind(), ConnKind::Confluence);
        assert!(c.via_jira_app());
        assert!(c.usable_for("owner/repo"));
        assert_eq!(c.site, "https://yourcompany.atlassian.net");
        assert!(!c.write_enabled && c.write_pages.is_empty(), "reads only");
        assert!(!c.may_write_page("42"));
        assert!(with_jira_app(Vec::new(), None).is_empty());
    }

    #[test]
    fn saved_app_settings_are_kept_and_a_token_connection_wins() {
        let saved = Connector {
            enabled: false,
            status: "ready".into(),
            via: VIA_JIRA_APP.into(),
            write_enabled: true,
            write_pages: vec!["42".into()],
            ..Connector::new(ConnKind::Confluence)
        };
        let out = with_jira_app(vec![saved.clone()], Some(&app()));
        assert_eq!(out.len(), 1);
        assert!(!out[0].enabled, "turned off stays off");
        assert!(out[0].may_write_page("42"));
        assert_eq!(out[0].site, "https://yourcompany.atlassian.net");

        let gone = with_jira_app(vec![saved], None);
        assert_eq!(gone[0].status, "error");
        assert!(!gone[0].usable_for("owner/repo"));

        let token = Connector {
            enabled: true,
            status: "ready".into(),
            site: "https://other.atlassian.net".into(),
            key_sealed: "sealed".into(),
            ..Connector::new(ConnKind::Confluence)
        };
        let out = with_jira_app(vec![token.clone()], Some(&app()));
        assert_eq!(out, vec![token]);
    }
}
