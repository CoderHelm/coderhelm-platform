//! jm. A team moved from hosted CoderHelm to a self-hosted install and kept
//! its team id; its Jira site stayed linked to that install through
//! CoderHelm. The site's Jira app registers its links with CoderHelm under
//! that id, and CoderHelm looked the team up first: it holds no team record
//! for a team that moved, so every registration was refused ("team not
//! found", 404). The app turned that into a 502 for the install's request
//! to register again, and the install's log said only "refused, 502". The
//! hosted Jira record, which CoderHelm's proxy and the install's copy read,
//! never gained the ticket lookup link, so chat on the install could not
//! read tickets, while the site's events reached the install, because the
//! intake routes them by the link without needing the team record.
//!
//! Guarantee: a registration for an id with no team record but a standing
//! link (route and link, the intake's own test) is the linked team's
//! (`common::jira_connection::Registrant`): checked by the secret on its
//! hosted record, validated as any registration, saved on that record, and
//! answered 200; the install then copies again because the registered
//! triggers changed (`registered`, `copy_is_stale`). An id with neither is
//! still not found. A refusal from the app is logged with the app's answer
//! (`refusal_words`), so it explains itself.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use common::jira_connection::{
    copy_is_stale, missing_registrations, refusal_words, registered, Registrant, REFUSAL_MAX,
};

type Item = HashMap<String, AttributeValue>;

fn item(pairs: &[(&str, &str)]) -> Item {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), AttributeValue::S(v.to_string())))
        .collect()
}

const FORGE: &str = "https://0a1b2c3d.hello.atlassian-dev.net/x1";

/// The moved team's hosted record before the fix: no ticket lookup.
fn hosted_before() -> Item {
    item(&[
        ("list_projects_url", &format!("{FORGE}/lp")),
        ("create_ticket_url", &format!("{FORGE}/ct")),
        ("add_comment_url", &format!("{FORGE}/ac")),
        ("get_issues_url", ""),
        ("forge_secret", "site-secret"),
        ("site_url", "https://example.atlassian.net"),
    ])
}

#[test]
fn a_moved_linked_team_registers_and_an_unknown_id_does_not() {
    // No team record on hosted, the link stands: the linked team's record
    // takes the registration, and the app hears 200.
    let r = Registrant::of(false, true).expect("a linked team registers");
    assert_eq!(r, Registrant::Linked);
    assert_eq!(r.status(), 200);
    // A hosted team is unchanged.
    assert_eq!(Registrant::of(true, false), Some(Registrant::Hosted));
    assert_eq!(Registrant::of(true, true), Some(Registrant::Hosted));
    // Neither a team record nor a link: not found, as before.
    assert_eq!(Registrant::of(false, false), None);
}

#[test]
fn the_saved_lookup_reaches_the_install_without_anyone() {
    let before = hosted_before();
    assert_eq!(missing_registrations(&before), vec!["get_issues_url"]);
    let copied: Vec<String> = registered(&before).iter().map(|s| s.to_string()).collect();
    let mut after = before.clone();
    after.insert(
        "get_issues_url".into(),
        AttributeValue::S(format!("{FORGE}/gi")),
    );
    assert!(missing_registrations(&after).is_empty());
    let hosted: Vec<String> = registered(&after).iter().map(|s| s.to_string()).collect();
    assert!(hosted.contains(&"get_issues".to_string()));
    // The triggers CoderHelm reports on `/hybrid/status` changed: the
    // install copies again by itself.
    assert!(copy_is_stale(Some(&hosted), &copied));
}

#[test]
fn a_refusal_from_the_app_names_its_reason() {
    let answer = br#"{"registered":false,"error":"Gateway returned 404"}"#;
    assert_eq!(refusal_words(answer), "Gateway returned 404");
    let long = format!(r#"{{"error":"{}"}}"#, "x".repeat(2000));
    assert!(refusal_words(long.as_bytes()).len() <= REFUSAL_MAX + 3);
}
