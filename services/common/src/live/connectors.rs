//! Monitoring, docs and pipeline connections a team turns on for its agents:
//! New Relic, Confluence, OpenSearch, a REST API, CloudWatch metrics and
//! Airflow (MWAA).
//!
//! One record per kind per team (`CONNECTOR#<kind>` in the settings table),
//! except REST APIs: a team connects as many as it uses, each under its own
//! name (`CONNECTOR#rest#<name>`).
//! Each is off until an admin turns it on, reads only, and every call it makes
//! is written to the same activity log as a data-source lookup. The one write
//! (appending to a Confluence page) needs its own admin switch and a list of
//! page ids it may touch; nothing else can write.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde_json::{json, Value};

use super::aws::AwsTarget;

const PREFIX: &str = "CONNECTOR#";
/// A Confluence connection reached through the team's installed Coderhelm
/// Jira app instead of an API token.
pub const VIA_JIRA_APP: &str = "jira_app";
/// Pages a Confluence write list may hold.
pub const MAX_WRITE_PAGES: usize = 20;
/// Lookups one agent pass may make across every connection here.
pub const MAX_CALLS_PER_RUN: usize = 16;
/// Confluence appends one agent pass may make.
pub const MAX_WRITES_PER_RUN: usize = 2;

type Item = HashMap<String, AttributeValue>;

/// What a connection reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnKind {
    NewRelic,
    Confluence,
    OpenSearch,
    Rest,
    Metrics,
    Airflow,
}

impl ConnKind {
    pub const ALL: [ConnKind; 6] = [
        ConnKind::NewRelic,
        ConnKind::Confluence,
        ConnKind::OpenSearch,
        ConnKind::Rest,
        ConnKind::Metrics,
        ConnKind::Airflow,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ConnKind::NewRelic => "newrelic",
            ConnKind::Confluence => "confluence",
            ConnKind::OpenSearch => "opensearch",
            ConnKind::Rest => "rest",
            ConnKind::Metrics => "metrics",
            ConnKind::Airflow => "airflow",
        }
    }

    pub fn parse(s: &str) -> Option<ConnKind> {
        ConnKind::ALL.into_iter().find(|k| k.as_str() == s)
    }

    /// The name people see.
    pub fn label(self) -> &'static str {
        match self {
            ConnKind::NewRelic => "New Relic",
            ConnKind::Confluence => "Confluence",
            ConnKind::OpenSearch => "OpenSearch",
            ConnKind::Rest => "REST API",
            ConnKind::Metrics => "CloudWatch metrics",
            ConnKind::Airflow => "Airflow (MWAA)",
        }
    }

    /// Reached through the team's AWS role rather than a key.
    pub fn uses_aws(self) -> bool {
        matches!(self, ConnKind::Metrics | ConnKind::Airflow)
    }

    /// The id its lookups carry in the activity log.
    pub fn activity_id(self) -> String {
        format!("connector-{}", self.as_str())
    }
}

/// Characters of a connection's guide for agents.
pub const MAX_INSTRUCTIONS: usize = 4000;

/// Whether a team may connect several of `kind`, each under its own name.
pub fn nameable(kind: ConnKind) -> bool {
    matches!(kind, ConnKind::Rest | ConnKind::OpenSearch)
}

/// Checks a connection's name: 1–40 lowercase letters, digits, `-` and `_`,
/// starting with a letter (it is how agents pick it and part of its key).
pub fn check_name(name: &str) -> Result<(), String> {
    let ok = !name.is_empty()
        && name.len() <= 40
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_');
    if ok {
        Ok(())
    } else {
        Err(format!(
            "{name} isn't a name an API can have: use lowercase letters, digits, - and _, starting with a letter."
        ))
    }
}

/// One connection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Connector {
    pub kind: Option<ConnKind>,
    /// REST: the API's name, unique in the team (agents pick it by name).
    /// Empty for every other kind, and for a REST API saved before names.
    pub name: String,
    /// Agents may use it. Off until an admin turns it on.
    pub enabled: bool,
    /// `ready` once a test passed; `error` when the last test failed.
    pub status: String,
    pub status_message: String,
    /// New Relic: a user key. Confluence: an API token. OpenSearch: the
    /// password. Sealed for the team.
    pub key_sealed: String,
    pub key_hint: String,
    /// New Relic: `us` or `eu`. Metrics and Airflow: the AWS region.
    pub region: String,
    /// New Relic accounts agents may query. Empty: every account the key sees.
    pub account_ids: Vec<String>,
    /// Confluence: `https://<site>.atlassian.net`. OpenSearch: the cluster's
    /// `https://` address. REST: the API's base address.
    pub site: String,
    /// Confluence: the account the token belongs to. OpenSearch: the user.
    pub email: String,
    /// Confluence spaces agents may read (keys). Empty: every space the
    /// account sees.
    pub spaces: Vec<String>,
    /// Confluence: appending to a page is allowed (a second admin switch).
    pub write_enabled: bool,
    /// Confluence page ids an append may touch. Nothing else is writable.
    pub write_pages: Vec<String>,
    /// Confluence: how Coderhelm reaches it. Empty: the API token above.
    /// [`VIA_JIRA_APP`]: through the Coderhelm app installed in the team's
    /// Jira (no token; the site is the app's site).
    pub via: String,
    /// Metrics and Airflow: the team's AWS role (the log reader).
    pub aws: Option<AwsTarget>,
    /// Airflow: MWAA environment names agents may read.
    pub environments: Vec<String>,
    /// OpenSearch: index names or patterns agents may read. Empty: every
    /// index the user sees.
    pub indexes: Vec<String>,
    /// OpenSearch and REST: field names hidden in results, on top of the
    /// defaults.
    pub hidden_fields: Vec<String>,
    /// REST: the header the key is sent in (e.g. `X-Api-Key`).
    pub header_name: String,
    /// REST: path prefixes agents may GET. Nothing outside them is read.
    pub paths: Vec<String>,
    /// `owner/name` repos whose work may use it. Empty: every repo.
    pub repos: Vec<String>,
    /// What agents should know when using it (what the system is, what its
    /// fields mean, where else its data lives). Shown with the connection.
    pub instructions: String,
    pub updated_at: String,
    pub updated_by: String,
}

fn s(item: &Item, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}
fn b(item: &Item, k: &str) -> bool {
    item.get(k)
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false)
}
fn strings(item: &Item, k: &str) -> Vec<String> {
    item.get(k)
        .and_then(|v| v.as_l().ok())
        .map(|l| l.iter().filter_map(|x| x.as_s().ok().cloned()).collect())
        .unwrap_or_default()
}
fn av_s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}
fn av_list(v: &[String]) -> AttributeValue {
    AttributeValue::L(v.iter().map(|x| av_s(x)).collect())
}

impl Connector {
    pub fn new(kind: ConnKind) -> Self {
        Connector {
            kind: Some(kind),
            ..Default::default()
        }
    }

    pub fn kind(&self) -> ConnKind {
        self.kind.unwrap_or(ConnKind::NewRelic)
    }

    /// The connection's id in routes and on the page: `rest:<name>` for a
    /// named REST API, else the kind.
    pub fn id(&self) -> String {
        match self.name.as_str() {
            "" => self.kind().as_str().to_string(),
            n => format!("{}:{n}", self.kind().as_str()),
        }
    }

    /// The name people see: `REST API (name)` for a named API.
    pub fn label(&self) -> String {
        match self.name.as_str() {
            "" => self.kind().label().to_string(),
            n => format!("{} ({n})", self.kind().label()),
        }
    }

    pub fn to_item(&self, team_id: &str) -> Item {
        let mut item: Item = HashMap::from([
            ("pk".to_string(), av_s(team_id)),
            ("sk".to_string(), av_s(&sort_key(self.kind(), &self.name))),
            ("record_type".to_string(), av_s("connector")),
            ("enabled".to_string(), AttributeValue::Bool(self.enabled)),
            ("status".to_string(), av_s(&self.status)),
            ("status_message".to_string(), av_s(&self.status_message)),
            ("key_sealed".to_string(), av_s(&self.key_sealed)),
            ("key_hint".to_string(), av_s(&self.key_hint)),
            ("region".to_string(), av_s(&self.region)),
            ("account_ids".to_string(), av_list(&self.account_ids)),
            ("site".to_string(), av_s(&self.site)),
            ("email".to_string(), av_s(&self.email)),
            ("spaces".to_string(), av_list(&self.spaces)),
            (
                "write_enabled".to_string(),
                AttributeValue::Bool(self.write_enabled),
            ),
            ("write_pages".to_string(), av_list(&self.write_pages)),
            ("via".to_string(), av_s(&self.via)),
            ("environments".to_string(), av_list(&self.environments)),
            ("indexes".to_string(), av_list(&self.indexes)),
            ("hidden_fields".to_string(), av_list(&self.hidden_fields)),
            ("header_name".to_string(), av_s(&self.header_name)),
            ("paths".to_string(), av_list(&self.paths)),
            ("repos".to_string(), av_list(&self.repos)),
            ("instructions".to_string(), av_s(&self.instructions)),
            ("updated_at".to_string(), av_s(&self.updated_at)),
            ("updated_by".to_string(), av_s(&self.updated_by)),
        ]);
        if let Some(a) = &self.aws {
            item.insert(
                "aws".to_string(),
                av_s(&serde_json::to_string(a).unwrap_or_default()),
            );
        }
        item
    }

    pub fn from_item(item: &Item) -> Option<Connector> {
        let sk = s(item, "sk");
        let rest = sk.strip_prefix(PREFIX)?;
        let (kind, name) = rest.split_once('#').unwrap_or((rest, ""));
        let kind = ConnKind::parse(kind)?;
        Some(Connector {
            kind: Some(kind),
            name: name.to_string(),
            enabled: b(item, "enabled"),
            status: s(item, "status"),
            status_message: s(item, "status_message"),
            key_sealed: s(item, "key_sealed"),
            key_hint: s(item, "key_hint"),
            region: s(item, "region"),
            account_ids: strings(item, "account_ids"),
            site: s(item, "site"),
            email: s(item, "email"),
            spaces: strings(item, "spaces"),
            write_enabled: b(item, "write_enabled"),
            write_pages: strings(item, "write_pages"),
            via: s(item, "via"),
            aws: serde_json::from_str(&s(item, "aws")).ok(),
            environments: strings(item, "environments"),
            indexes: strings(item, "indexes"),
            hidden_fields: strings(item, "hidden_fields"),
            header_name: s(item, "header_name"),
            paths: strings(item, "paths"),
            repos: strings(item, "repos"),
            instructions: s(item, "instructions"),
            updated_at: s(item, "updated_at"),
            updated_by: s(item, "updated_by"),
        })
    }

    /// What a browser may see. The sealed key and the AWS private ID never
    /// are.
    pub fn public_json(&self) -> Value {
        let aws = self.aws.as_ref().map(|a| {
            json!({
                "account": a.account(),
                "region": a.region,
                "role": a.role_arn.rsplit('/').next().unwrap_or(""),
            })
        });
        json!({
            "kind": self.kind().as_str(),
            "id": self.id(),
            "name": self.name,
            "label": self.label(),
            "connected": true,
            "enabled": self.enabled,
            "status": self.status,
            "status_message": self.status_message,
            "key_hint": self.key_hint,
            "region": self.region,
            "account_ids": self.account_ids,
            "site": self.site,
            "email": self.email,
            "spaces": self.spaces,
            "write_enabled": self.write_enabled,
            "write_pages": self.write_pages,
            "via": self.via,
            "environments": self.environments,
            "indexes": self.indexes,
            "hidden_fields": self.hidden_fields,
            "header_name": self.header_name,
            "paths": self.paths,
            "repos": self.repos,
            "instructions": self.instructions,
            "aws": aws,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
        })
    }

    /// Reached through the team's Coderhelm Jira app, not a key of its own.
    pub fn via_jira_app(&self) -> bool {
        self.via == VIA_JIRA_APP
    }

    /// Whether work on `repo` (`owner/name`) may use it.
    pub fn usable_for(&self, repo: &str) -> bool {
        self.enabled
            && self.status == "ready"
            && (self.repos.is_empty() || self.repos.iter().any(|r| r.eq_ignore_ascii_case(repo)))
    }

    /// Whether an append to `page_id` is allowed: the write switch is on and
    /// the page is on the list.
    pub fn may_write_page(&self, page_id: &str) -> bool {
        self.kind() == ConnKind::Confluence
            && self.write_enabled
            && !page_id.is_empty()
            && self.write_pages.iter().any(|p| p == page_id)
    }
}

/// Checks a list of Confluence page ids: digits only, at most
/// [`MAX_WRITE_PAGES`].
pub fn check_page_ids(ids: &[String]) -> Result<(), String> {
    if ids.len() > MAX_WRITE_PAGES {
        return Err(format!("At most {MAX_WRITE_PAGES} pages can be writable."));
    }
    for id in ids {
        if id.is_empty() || id.len() > 20 || !id.chars().all(|c| c.is_ascii_digit()) {
            return Err(format!(
                "{id} isn't a page id. It's the number in the page's address, after /pages/."
            ));
        }
    }
    Ok(())
}

/// The settings-table sort key of a connection.
fn sort_key(kind: ConnKind, name: &str) -> String {
    match name {
        "" => format!("{PREFIX}{}", kind.as_str()),
        n => format!("{PREFIX}{}#{n}", kind.as_str()),
    }
}

pub async fn list(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
) -> Result<Vec<Connector>, String> {
    let out = dynamo
        .query()
        .table_name(table)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", av_s(team_id))
        .expression_attribute_values(":p", av_s(PREFIX))
        .send()
        .await
        .map_err(|e| format!("list connectors: {e}"))?;
    Ok(out
        .items()
        .iter()
        .filter_map(Connector::from_item)
        .collect())
}

pub async fn get(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    kind: ConnKind,
    name: &str,
) -> Result<Option<Connector>, String> {
    let out = dynamo
        .get_item()
        .table_name(table)
        .key("pk", av_s(team_id))
        .key("sk", av_s(&sort_key(kind, name)))
        .consistent_read(true)
        .send()
        .await
        .map_err(|e| format!("get connector: {e}"))?;
    Ok(out.item().and_then(Connector::from_item))
}

pub async fn put(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    c: &Connector,
) -> Result<(), String> {
    dynamo
        .put_item()
        .table_name(table)
        .set_item(Some(c.to_item(team_id)))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("save connector: {e}"))
}

pub async fn delete(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    kind: ConnKind,
    name: &str,
) -> Result<(), String> {
    dynamo
        .delete_item()
        .table_name(table)
        .key("pk", av_s(team_id))
        .key("sk", av_s(&sort_key(kind, name)))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("delete connector: {e}"))
}

/// Shortens a result for an agent or a page, on a character boundary.
pub fn clip(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    format!(
        "{}\n… (cut at {max} characters; ask for less)",
        crate::truncate_str(text, max)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn confluence() -> Connector {
        Connector {
            enabled: true,
            status: "ready".into(),
            site: "https://example.atlassian.net".into(),
            email: "bot@example.com".into(),
            key_sealed: "sealed".into(),
            write_enabled: true,
            write_pages: vec!["123".into()],
            ..Connector::new(ConnKind::Confluence)
        }
    }

    #[test]
    fn a_record_survives_the_round_trip() {
        let mut c = confluence();
        c.instructions = "Pages under ENG are the runbooks.".into();
        c.via = VIA_JIRA_APP.into();
        c.aws = Some(AwsTarget {
            role_arn: "arn:aws:iam::123456789012:role/CoderHelmLogReader".into(),
            external_id: "x".into(),
            region: "us-east-1".into(),
            ..Default::default()
        });
        let back = Connector::from_item(&c.to_item("TEAM#1")).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn what_a_browser_sees_has_no_key_or_private_id() {
        let mut c = confluence();
        c.aws = Some(AwsTarget {
            role_arn: "arn:aws:iam::123456789012:role/CoderHelmLogReader".into(),
            external_id: "private".into(),
            region: "us-east-1".into(),
            ..Default::default()
        });
        let text = c.public_json().to_string();
        assert!(!text.contains("sealed"), "{text}");
        assert!(!text.contains("private"), "{text}");
        assert!(text.contains("CoderHelmLogReader"));
    }

    #[test]
    fn writes_need_the_switch_and_the_list() {
        let c = confluence();
        assert!(c.may_write_page("123"));
        assert!(!c.may_write_page("124"), "not on the list");
        assert!(!c.may_write_page(""));
        let off = Connector {
            write_enabled: false,
            ..confluence()
        };
        assert!(!off.may_write_page("123"), "switch off");
        let other = Connector {
            kind: Some(ConnKind::NewRelic),
            ..confluence()
        };
        assert!(!other.may_write_page("123"), "only Confluence writes");
    }

    #[test]
    fn page_ids_are_numbers() {
        assert!(check_page_ids(&["12345".into()]).is_ok());
        assert!(check_page_ids(&["abc".into()]).is_err());
        assert!(check_page_ids(&["".into()]).is_err());
        let many: Vec<String> = (0..=MAX_WRITE_PAGES).map(|i| i.to_string()).collect();
        assert!(check_page_ids(&many).is_err());
    }

    #[test]
    fn usable_only_when_on_ready_and_in_scope() {
        let mut c = confluence();
        assert!(c.usable_for("o/r"));
        c.repos = vec!["o/other".into()];
        assert!(!c.usable_for("o/r"));
        c.repos.clear();
        c.status = "error".into();
        assert!(!c.usable_for("o/r"));
    }

    #[test]
    fn rest_apis_are_kept_apart_by_name() {
        let named = Connector {
            name: "billing".into(),
            site: "https://api.example.com".into(),
            ..Connector::new(ConnKind::Rest)
        };
        let item = named.to_item("TEAM#1");
        assert_eq!(item["sk"].as_s().unwrap(), "CONNECTOR#rest#billing");
        let back = Connector::from_item(&item).unwrap();
        assert_eq!(back, named);
        assert_eq!(back.id(), "rest:billing");
        assert_eq!(back.label(), "REST API (billing)");
        // A REST API saved before names still reads back, with no name.
        let old = Connector::from_item(&Connector::new(ConnKind::Rest).to_item("TEAM#1")).unwrap();
        assert_eq!((old.name.as_str(), old.id().as_str()), ("", "rest"));
        assert!(check_name("billing-v2").is_ok());
        for bad in ["", "Billing", "2fa", "a b", "a#b", &"x".repeat(41)] {
            assert!(check_name(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn kinds_parse_back() {
        for k in ConnKind::ALL {
            assert_eq!(ConnKind::parse(k.as_str()), Some(k));
        }
        assert_eq!(ConnKind::parse("bash"), None);
    }
}
