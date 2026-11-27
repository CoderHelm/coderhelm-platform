//! nt. A team's alert channel, set up for production alarms, also got
//! routine auto-merge status cards: a merge conflict that needed a person's
//! call, and a wait on an approving review the branch's rules ask for.
//! Every sender that wanted "the team's channel" opened the alert channel's
//! settings item (`ALERT_NOTIFY`) itself: the merge gate, security and
//! infrastructure cards, plugin reviews, turned-off webhook endpoints,
//! knowledge sign-ins, and log findings whose own setting was never saved.
//! The merge gate posted once per commit and wording, so each push could
//! post again, and it posted a wait on a person's review, which is a pull
//! request's normal state.
//!
//! Guarantee: every team-wide message has a kind (alert, action or update),
//! and its channels come from `common::notify::route` by that kind. An alert
//! channel saved before kinds existed takes alerts only; action and updates
//! go to the team's updates channel, or nowhere when there is none. The
//! merge gate never posts a wait on people's reviews to a channel (the PR's
//! status comment says it once), posts a conflict that needs a person only
//! where action messages go, and posts it once per PR and reason.

use super::fixture::fn_src;
use crate::passes::await_merge::{told_sk, TeamNotice};
use aws_sdk_dynamodb::types::AttributeValue;
use common::notify::route::{self, Kind, TeamChannels};
use common::notify::Channels;
use std::collections::HashMap;

const ALERTS: &str = "https://acme.webhook.office.com/webhookb2/alerts";
const UPDATES: &str = "https://acme.webhook.office.com/webhookb2/updates";

fn src() -> &'static str {
    include_str!("../await_merge.rs")
}

fn on(kinds: Option<&[&str]>) -> HashMap<String, AttributeValue> {
    let mut i = HashMap::from([("enabled".to_string(), AttributeValue::Bool(true))]);
    if let Some(k) = kinds {
        i.insert(
            route::KINDS_ATTR.into(),
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
fn the_alert_channel_as_it_was_saved_takes_alerts_only() {
    // The item as the team saved it: on, a Teams webhook, no kinds.
    let alerts = on(None);
    let only = [(route::ALERTS_SK, &alerts, teams(ALERTS))];
    assert_eq!(
        route::pick(Kind::Alert, &only).teams.as_deref(),
        Some(ALERTS)
    );
    assert!(
        route::pick(Kind::Action, &only).is_empty(),
        "a merge conflict card never reaches the alert channel"
    );
    assert!(route::pick(Kind::Update, &only).is_empty());
    // With an updates channel, action and updates go there.
    let updates = on(None);
    let both = [
        (route::ALERTS_SK, &alerts, teams(ALERTS)),
        (route::UPDATES_SK, &updates, teams(UPDATES)),
    ];
    assert_eq!(
        route::pick(Kind::Action, &both).teams.as_deref(),
        Some(UPDATES)
    );
    assert_eq!(
        route::pick(Kind::Update, &both).teams.as_deref(),
        Some(UPDATES)
    );
    assert_eq!(
        route::pick(Kind::Alert, &both).teams.as_deref(),
        Some(ALERTS)
    );
    // A team that wants requests for a person in its alert channel says so.
    let more = on(Some(&["alert", "action"]));
    let chose = [(route::ALERTS_SK, &more, teams(ALERTS))];
    assert_eq!(
        route::pick(Kind::Action, &chose).teams.as_deref(),
        Some(ALERTS)
    );
    assert!(route::pick(Kind::Update, &chose).is_empty());
    // Nothing set up: nothing posts.
    assert!(TeamChannels::default().is_empty());
}

#[test]
fn the_merge_gate_never_posts_a_review_wait_and_posts_a_conflict_once() {
    // Waiting on people: the status comment, never a channel.
    let eval = fn_src(src(), "async fn evaluate(");
    let people = &eval[eval.find("Action::WaitForPeople =>").unwrap()..];
    let people = &people[..people.find("Ok(Next::Recheck)").unwrap()];
    assert!(people.contains("publish_status("));
    assert!(!people.contains("tell_team("), "{people}");
    // A conflict a person must resolve: action, never the alert kind.
    let tell = fn_src(src(), "async fn tell_team(");
    assert!(tell.contains(".get(common::notify::route::Kind::Action)"));
    assert!(!tell.contains("Kind::Alert"));
    assert!(src().contains("tell_team(state, msg, pr, TeamNotice::Conflict, &line.1)"));
    // Once per PR and reason: not per commit or per wording.
    let a = told_sk("Acme", "web-api", 12, TeamNotice::Conflict);
    assert_eq!(a, told_sk("acme", "Web-API", 12, TeamNotice::Conflict));
    assert_eq!(a, "MERGEGATETOLD#acme/web-api#12#conflict");
    assert!(tell.contains("told_sk(&msg.repo_owner, &msg.repo_name, msg.pr_number, notice)"));
    assert!(!tell.contains("head_sha"));
    assert!(!tell.contains("content_hash"));
}

#[test]
fn every_team_channel_sender_picks_a_kind() {
    // The senders that opened the alert channel themselves now pick by kind.
    for (file, kind) in [
        (include_str!("../plugins.rs"), "Kind::Action"),
        (include_str!("../webhooks.rs"), "Kind::Action"),
        (include_str!("../knowledge_sync.rs"), "Kind::Action"),
        (include_str!("../log_autofix.rs"), "Kind::Update"),
        (include_str!("../infra_findings.rs"), "Kind::Update"),
    ] {
        let code = &file[..file.find("#[cfg(test)]").unwrap_or(file.len())];
        assert!(code.contains(kind), "{kind}");
        assert!(!code.contains("TEAM_SETTINGS_SK"));
    }
    // Security cards: a critical finding's urgent card is an alert; a pull
    // request opened, merged or withdrawn is an update.
    let sec = include_str!("../security_fixes.rs");
    assert!(sec.contains("channels.get(sec::card_kind(f, stage))"));
    assert!(!sec[..sec.find("#[cfg(test)]").unwrap()].contains("TEAM_SETTINGS_SK"));
}
