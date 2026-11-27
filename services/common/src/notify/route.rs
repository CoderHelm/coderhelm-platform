//! Which team channel a message goes to, by what kind of message it is.
//!
//! Every chat message CoderHelm sends to a team-wide channel is one of three
//! kinds ([`Kind`]):
//!
//! - **Alert**: a production alarm or incident, its follow-ups, a failed
//!   deploy, a critical security finding. Something is wrong now.
//! - **Action**: a person has to do something CoderHelm can't (resolve a
//!   conflict that needs a call, reconnect a sign-in, approve a plugin).
//! - **Update**: news. A fix PR opened or merged, new log findings.
//!
//! A team has two team-wide destinations: the alert channel
//! ([`ALERTS_SK`], set up under Alerts) and the updates channel
//! ([`UPDATES_SK`]). Each saves the kinds it takes (`kinds`). Saved before
//! kinds existed, the alert channel takes alerts only and the updates
//! channel takes action and updates: reading the old items this way is the
//! migration, so an alert channel saved last month stops getting anything
//! but alerts without a write.
//!
//! Every sender of a team-wide message picks its channels here, with its
//! kind ([`team_channels`]). Nothing else opens the alert channel's webhooks
//! for a message that is not an alert (`tests::only_the_router_opens_the_alert_channel`).
//! Destinations of their own (an alert route's channels, the reviewer's
//! per-repository channels, the agent approval channel) are not team-wide
//! and are not routed here.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;

use super::{store, ChannelKind, Channels};
use crate::mcp_credentials::Cipher;

type Item = HashMap<String, AttributeValue>;

/// Settings-table sort key of the team's alert channel.
pub const ALERTS_SK: &str = crate::alert_notify::TEAM_SETTINGS_SK;
/// Settings-table sort key of the team's updates channel.
pub const UPDATES_SK: &str = "UPDATES_NOTIFY";
/// The attribute on a destination item listing the kinds it takes.
pub const KINDS_ATTR: &str = "kinds";

/// What a message is, for where it may go.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    Alert,
    Action,
    Update,
}

impl Kind {
    pub const ALL: [Kind; 3] = [Kind::Alert, Kind::Action, Kind::Update];

    pub fn key(self) -> &'static str {
        match self {
            Kind::Alert => "alert",
            Kind::Action => "action",
            Kind::Update => "update",
        }
    }

    pub fn parse(s: &str) -> Option<Kind> {
        match s.trim() {
            "alert" => Some(Kind::Alert),
            "action" => Some(Kind::Action),
            "update" => Some(Kind::Update),
            _ => None,
        }
    }
}

/// Pure: the kind of every message label a sender passes to
/// [`super::deliver`]. `None` for a label not listed, which a test keeps
/// from happening.
pub fn kind_of(what: &str) -> Option<Kind> {
    Some(match what {
        // A production alarm, its recovery and what CoderHelm did about it.
        "alert" | "alert_pr" | "alert_no_change" | "log_incident" => Kind::Alert,
        // A person has to act.
        "merge_gate_wait" | "webhook_disabled" | "knowledge sign-in" | "plugin review"
        | "aws_permissions" | "agent approval" | "review_reminder" | "release_notes" => {
            Kind::Action
        }
        // News.
        "log_fix_pr" | "log_fix_pr_closed" | "findings" | "agent notice" => Kind::Update,
        // Security cards are an alert or an update by tier and stage
        // (`security_fixes::card_kind`); the label alone says neither.
        _ => return None,
    })
}

/// The kinds a destination takes when it never saved its own.
pub fn default_kinds(sk: &str) -> Vec<Kind> {
    if sk == UPDATES_SK {
        vec![Kind::Action, Kind::Update]
    } else {
        vec![Kind::Alert]
    }
}

/// Pure: the kinds `item` (stored under `sk`) takes: its saved `kinds`, or
/// the destination's default when none were saved.
pub fn kinds(item: &Item, sk: &str) -> Vec<Kind> {
    let saved: Option<Vec<String>> = match item.get(KINDS_ATTR) {
        Some(AttributeValue::L(l)) => {
            Some(l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
        }
        Some(AttributeValue::Ss(s)) => Some(s.clone()),
        _ => None,
    };
    let Some(saved) = saved else {
        return default_kinds(sk);
    };
    let mut out: Vec<Kind> = saved.iter().filter_map(|s| Kind::parse(s)).collect();
    out.sort();
    out.dedup();
    out
}

/// Pure: the attribute that saves `kinds` on a destination item.
pub fn kinds_attr(kinds: &[Kind]) -> AttributeValue {
    let mut k = kinds.to_vec();
    k.sort();
    k.dedup();
    AttributeValue::L(
        k.iter()
            .map(|k| AttributeValue::S(k.key().to_string()))
            .collect(),
    )
}

/// Pure: `kinds` from a request, checked: known keys, at least one.
pub fn parse_kinds(keys: &[String]) -> Result<Vec<Kind>, String> {
    let mut out = Vec::new();
    for k in keys {
        out.push(Kind::parse(k).ok_or_else(|| format!("unknown message kind {k:?}"))?);
    }
    out.sort();
    out.dedup();
    if out.is_empty() {
        return Err("a channel takes at least one kind of message".into());
    }
    Ok(out)
}

fn enabled(item: &Item) -> bool {
    item.get("enabled")
        .and_then(|v| v.as_bool().ok())
        .copied()
        .unwrap_or(false)
}

/// Pure: whether the destination saved under `sk` posts a `kind` message.
pub fn takes(item: &Item, sk: &str, kind: Kind) -> bool {
    enabled(item) && kinds(item, sk).contains(&kind)
}

/// Pure: the order destinations are tried for a kind. An alert goes to the
/// alert channel; action and updates go to the updates channel first, and
/// to the alert channel only when it was set to take them.
pub fn order(kind: Kind) -> [&'static str; 2] {
    match kind {
        Kind::Alert => [ALERTS_SK, UPDATES_SK],
        Kind::Action | Kind::Update => [UPDATES_SK, ALERTS_SK],
    }
}

/// Pure: per chat app, the first destination in [`order`] that takes `kind`
/// and has that app set. `opened` pairs each destination's sort key with its
/// item and its opened channels.
pub fn pick(kind: Kind, opened: &[(&str, &Item, Channels)]) -> Channels {
    let mut out = Channels::default();
    for sk in order(kind) {
        let Some((_, item, channels)) = opened.iter().find(|(k, _, _)| *k == sk) else {
            continue;
        };
        if !takes(item, sk, kind) {
            continue;
        }
        for app in ChannelKind::ALL {
            if out.get(app).is_none() {
                out.set(app, channels.get(app).map(str::to_string));
            }
        }
    }
    out.usable()
}

/// Pure: the chat apps a `kind` message posts to, from the stored items
/// (for a settings page; never opens a webhook).
pub fn apps(kind: Kind, items: &[(&str, Item)]) -> Vec<ChannelKind> {
    let mut out = Vec::new();
    for sk in order(kind) {
        let Some((_, item)) = items.iter().find(|(k, _)| *k == sk) else {
            continue;
        };
        if !takes(item, sk, kind) {
            continue;
        }
        for app in ChannelKind::ALL {
            if store::stored(item, app).is_set() && !out.contains(&app) {
                out.push(app);
            }
        }
    }
    out.sort_by_key(|a| ChannelKind::ALL.iter().position(|b| b == a));
    out
}

/// The team's destination items, as stored (sort key, item).
pub async fn load(dynamo: &DynamoClient, table: &str, team_id: &str) -> Vec<(&'static str, Item)> {
    let get = |sk: &'static str| {
        dynamo
            .get_item()
            .table_name(table)
            .key("pk", AttributeValue::S(team_id.to_string()))
            .key("sk", AttributeValue::S(sk.to_string()))
            .send()
    };
    let (alerts, updates) = tokio::join!(get(ALERTS_SK), get(UPDATES_SK));
    [(ALERTS_SK, alerts), (UPDATES_SK, updates)]
        .into_iter()
        .map(|(sk, r)| {
            (
                sk,
                r.ok().and_then(|o| o.item().cloned()).unwrap_or_default(),
            )
        })
        .collect()
}

/// The team's channels, opened once, for picking per message. No `Debug`:
/// the items hold webhooks.
#[derive(Clone, Default)]
pub struct TeamChannels {
    opened: Vec<(&'static str, Item, Channels)>,
}

impl TeamChannels {
    /// Opens every destination that takes any kind.
    pub async fn open(
        cipher: &dyn Cipher,
        team_id: &str,
        items: Vec<(&'static str, Item)>,
    ) -> Self {
        let mut opened = Vec::new();
        for (sk, item) in items {
            let channels = if enabled(&item) && !kinds(&item, sk).is_empty() {
                store::open_all(cipher, team_id, &item).await
            } else {
                Channels::default()
            };
            opened.push((sk, item, channels));
        }
        TeamChannels { opened }
    }

    /// No kind of message goes anywhere.
    pub fn is_empty(&self) -> bool {
        Kind::ALL.iter().all(|k| self.get(*k).is_empty())
    }

    /// Where a `kind` message goes. Empty = nowhere.
    pub fn get(&self, kind: Kind) -> Channels {
        let view: Vec<(&str, &Item, Channels)> = self
            .opened
            .iter()
            .map(|(sk, item, c)| (*sk, item, c.clone()))
            .collect();
        pick(kind, &view)
    }
}

/// The team's channels, read and opened: [`TeamChannels::get`] per message.
pub async fn team(
    dynamo: &DynamoClient,
    cipher: &dyn Cipher,
    table: &str,
    team_id: &str,
) -> TeamChannels {
    TeamChannels::open(cipher, team_id, load(dynamo, table, team_id).await).await
}

/// Where one `kind` message goes, read and opened. Empty = nowhere.
pub async fn team_channels(
    dynamo: &DynamoClient,
    cipher: &dyn Cipher,
    table: &str,
    team_id: &str,
    kind: Kind,
) -> Channels {
    team(dynamo, cipher, table, team_id).await.get(kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALERTS_TEAMS: &str = "https://acme.webhook.office.com/webhookb2/alerts";
    const UPDATES_TEAMS: &str = "https://acme.webhook.office.com/webhookb2/updates";
    const SLACK: &str = "https://hooks.slack.com/services/T000/B000/XXXX";

    fn item(on: bool, kinds: Option<&[&str]>) -> Item {
        let mut i = Item::new();
        i.insert("enabled".into(), AttributeValue::Bool(on));
        if let Some(k) = kinds {
            i.insert(
                KINDS_ATTR.into(),
                AttributeValue::L(k.iter().map(|s| AttributeValue::S(s.to_string())).collect()),
            );
        }
        i
    }

    fn teams(url: &str) -> Channels {
        Channels {
            teams: Some(url.into()),
            slack: None,
        }
    }

    #[test]
    fn an_alert_channel_saved_before_kinds_takes_alerts_only() {
        let alerts = item(true, None);
        assert_eq!(kinds(&alerts, ALERTS_SK), vec![Kind::Alert]);
        let opened = [(ALERTS_SK, &alerts, teams(ALERTS_TEAMS))];
        assert_eq!(
            pick(Kind::Alert, &opened).teams.as_deref(),
            Some(ALERTS_TEAMS)
        );
        assert!(pick(Kind::Action, &opened).is_empty());
        assert!(pick(Kind::Update, &opened).is_empty());
    }

    #[test]
    fn action_and_updates_go_to_the_updates_channel() {
        let alerts = item(true, None);
        let updates = item(true, None);
        let opened = [
            (ALERTS_SK, &alerts, teams(ALERTS_TEAMS)),
            (UPDATES_SK, &updates, teams(UPDATES_TEAMS)),
        ];
        assert_eq!(
            pick(Kind::Alert, &opened).teams.as_deref(),
            Some(ALERTS_TEAMS)
        );
        assert_eq!(
            pick(Kind::Action, &opened).teams.as_deref(),
            Some(UPDATES_TEAMS)
        );
        assert_eq!(
            pick(Kind::Update, &opened).teams.as_deref(),
            Some(UPDATES_TEAMS)
        );
    }

    #[test]
    fn an_alert_channel_set_to_take_more_gets_them() {
        let alerts = item(true, Some(&["alert", "action"]));
        let opened = [(ALERTS_SK, &alerts, teams(ALERTS_TEAMS))];
        assert_eq!(
            pick(Kind::Action, &opened).teams.as_deref(),
            Some(ALERTS_TEAMS)
        );
        assert!(pick(Kind::Update, &opened).is_empty());
        // The updates channel still wins for action when both take it.
        let updates = item(true, None);
        let both = [
            (ALERTS_SK, &alerts, teams(ALERTS_TEAMS)),
            (UPDATES_SK, &updates, teams(UPDATES_TEAMS)),
        ];
        assert_eq!(
            pick(Kind::Action, &both).teams.as_deref(),
            Some(UPDATES_TEAMS)
        );
    }

    #[test]
    fn a_channel_that_is_off_or_takes_nothing_gets_nothing() {
        let off = item(false, Some(&["alert", "action", "update"]));
        let none = item(true, Some(&[]));
        for k in Kind::ALL {
            assert!(pick(k, &[(ALERTS_SK, &off, teams(ALERTS_TEAMS))]).is_empty());
            assert!(pick(k, &[(UPDATES_SK, &none, teams(UPDATES_TEAMS))]).is_empty());
        }
    }

    #[test]
    fn each_app_comes_from_the_first_destination_that_has_it() {
        let alerts = item(true, Some(&["alert", "update"]));
        let updates = item(true, None);
        let opened = [
            (
                ALERTS_SK,
                &alerts,
                Channels {
                    teams: Some(ALERTS_TEAMS.into()),
                    slack: Some(SLACK.into()),
                },
            ),
            (UPDATES_SK, &updates, teams(UPDATES_TEAMS)),
        ];
        let c = pick(Kind::Update, &opened);
        assert_eq!(c.teams.as_deref(), Some(UPDATES_TEAMS));
        assert_eq!(c.slack.as_deref(), Some(SLACK));
    }

    #[test]
    fn kinds_parse_and_save_round_trip() {
        let k = parse_kinds(&["update".into(), "alert".into(), "alert".into()]).unwrap();
        assert_eq!(k, vec![Kind::Alert, Kind::Update]);
        assert!(parse_kinds(&[]).is_err());
        assert!(parse_kinds(&["everything".into()]).is_err());
        let mut i = item(true, None);
        i.insert(KINDS_ATTR.into(), kinds_attr(&k));
        assert_eq!(kinds(&i, ALERTS_SK), k);
        // A string set reads the same.
        i.insert(
            KINDS_ATTR.into(),
            AttributeValue::Ss(vec!["action".into(), "bogus".into()]),
        );
        assert_eq!(kinds(&i, ALERTS_SK), vec![Kind::Action]);
    }

    #[test]
    fn every_label_has_a_kind() {
        for (what, kind) in [
            ("alert", Kind::Alert),
            ("alert_pr", Kind::Alert),
            ("alert_no_change", Kind::Alert),
            ("log_incident", Kind::Alert),
            ("merge_gate_wait", Kind::Action),
            ("webhook_disabled", Kind::Action),
            ("knowledge sign-in", Kind::Action),
            ("plugin review", Kind::Action),
            ("aws_permissions", Kind::Action),
            ("log_fix_pr", Kind::Update),
            ("log_fix_pr_closed", Kind::Update),
            ("findings", Kind::Update),
        ] {
            assert_eq!(kind_of(what), Some(kind), "{what}");
        }
        assert_eq!(kind_of("security"), None);
    }

    /// Every file that names the alert channel's settings item. Each reads
    /// it for alerts, for its settings page, or through this router; a new
    /// sender reaches it only by picking a kind here.
    #[test]
    fn only_the_router_opens_the_alert_channel() {
        let services = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let allowed = [
            "common/src/alert_notify.rs",
            "common/src/notify/route.rs",
            // The settings API and the alert card's own route.
            "gateway/src/routes/alerts.rs",
            // Asserts its approval channel is not the alert channel.
            "worker/src/agent/policy.rs",
        ];
        let mut found = Vec::new();
        let mut stack = vec![services.clone()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for e in entries.flatten() {
                let p = e.path();
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if p.is_dir() {
                    if name != "target" && !name.starts_with('.') {
                        stack.push(p);
                    }
                    continue;
                }
                if !name.ends_with(".rs") {
                    continue;
                }
                let rel = p
                    .strip_prefix(&services)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                if rel.contains("/incidents/") {
                    continue;
                }
                let src = std::fs::read_to_string(&p).unwrap_or_default();
                if (src.contains("TEAM_SETTINGS_SK") || src.contains("\"ALERT_NOTIFY\""))
                    && !allowed.contains(&rel.as_str())
                {
                    found.push(rel);
                }
            }
        }
        found.sort();
        assert!(
            found.is_empty(),
            "these read the alert channel directly; pick a kind through notify::route instead: {found:?}"
        );
    }
}
