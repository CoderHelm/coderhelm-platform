//! jc. A new user on a self-hosted install asked chat what was waiting on
//! him. Chat answered "Jira isn't connected to Coderhelm", while Settings,
//! Integrations showed Atlassian connected and runs from Jira tickets
//! worked. The install linked its Jira site through CoderHelm before the
//! Jira app had its ticket lookup, and copied the hosted links once. The app
//! gained the lookup in a minor version, which Forge rolls out with no
//! lifecycle event, so the app never registered it, and the install never
//! copied again. Chat read "no lookup link" as "not connected".
//!
//! Guarantee: one answer to "is Jira connected" for every reader
//! (`common::jira_connection::Connection::connected`); a missing ability is
//! named as that ability, never as "not connected" (`Gap::words`); a record
//! that lacks a link the app always registers asks the app to register again
//! (`missing_registrations`, `Connection::register_again`); and an install's
//! copy that differs from what the site registered on CoderHelm is copied
//! again (`registered`, `copy_is_stale`).

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use common::jira_connection::{
    copy_is_stale, missing_registrations, registered, Connection, Gap, TicketReads,
    NOT_CONNECTED, REGISTER_AGAIN,
};

type Item = HashMap<String, AttributeValue>;

fn item(pairs: &[(&str, &str)]) -> Item {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), AttributeValue::S(v.to_string())))
        .collect()
}

const PROXY: &str = "https://api.example.com/hybrid/jira/trigger/ins_a";

/// The install's record as it was: the proxy links copied at link time, the
/// install's token, the site, a version seen today, no lookup.
fn install_record() -> Item {
    item(&[
        ("list_projects_url", &format!("{PROXY}/list_projects/t")),
        ("create_ticket_url", &format!("{PROXY}/create_ticket/t")),
        ("add_comment_url", &format!("{PROXY}/add_comment/t")),
        ("publish_release_url", ""),
        ("get_issues_url", ""),
        ("confluence_trigger_url", ""),
        ("forge_secret", "token-for-install"),
        ("site_url", "https://example.atlassian.net"),
        ("app_version", "7.3.0"),
    ])
}

/// The hosted team's record once the site's app registered again.
fn hosted_after_register() -> Item {
    let f = "https://0a1b2c3d.hello.atlassian-dev.net/x1";
    item(&[
        ("list_projects_url", &format!("{f}/lp")),
        ("create_ticket_url", &format!("{f}/ct")),
        ("add_comment_url", &format!("{f}/ac")),
        ("publish_release_url", ""),
        ("get_issues_url", &format!("{f}/gi")),
        ("confluence_trigger_url", ""),
        ("forge_secret", "site-secret"),
        ("site_url", "https://example.atlassian.net"),
    ])
}

#[test]
fn the_app_without_its_lookup_is_connected_and_the_gap_is_named() {
    let rec = install_record();
    let c = Connection::from_config(Some(&rec), false);
    assert!(c.connected(), "the integrations page and chat agree");
    let TicketReads::Off(gap) = c.ticket_reads() else {
        panic!("no lookup link: not ready");
    };
    assert_eq!(gap, Gap::LookupNotRegistered);
    let words = gap.words();
    assert!(!words.contains(NOT_CONNECTED), "{words}");
    assert!(!words.contains("isn't connected"), "{words}");
    assert!(words.starts_with("Jira is connected, but"), "{words}");
    assert!(words.contains("ticket lookup"), "{words}");
}

#[test]
fn the_system_asks_the_app_to_register_and_the_install_copies_again() {
    let rec = install_record();
    // The lookup the app always registers is missing: ask, through the
    // project-list link, with the secret the app checks.
    assert_eq!(missing_registrations(&rec), vec!["get_issues_url"]);
    let c = Connection::from_config(Some(&rec), false);
    let (url, body) = c.register_again(Some(&rec)).expect("asks");
    assert!(url.contains("/list_projects/"), "{url}");
    assert_eq!(body[REGISTER_AGAIN], true);
    // The site registered on CoderHelm: the install's copy is stale, so it
    // is copied again, and then has the lookup.
    let hosted = registered(&hosted_after_register());
    let copied: Vec<String> = registered(&rec).iter().map(|s| s.to_string()).collect();
    let hosted: Vec<String> = hosted.iter().map(|s| s.to_string()).collect();
    assert!(hosted.contains(&"get_issues".to_string()));
    assert!(copy_is_stale(Some(&hosted), &copied));
    assert!(!copy_is_stale(Some(&hosted), &hosted));
    // Once copied, nothing more is asked.
    let mut healed = rec.clone();
    healed.insert(
        "get_issues_url".into(),
        AttributeValue::S(format!("{PROXY}/get_issues/t")),
    );
    assert!(missing_registrations(&healed).is_empty());
    assert!(matches!(
        Connection::from_config(Some(&healed), false).ticket_reads(),
        TicketReads::Ready { .. }
    ));
}

#[test]
fn only_a_team_with_no_jira_at_all_hears_not_connected() {
    let none = Connection::from_config(None, false);
    assert_eq!(none.ticket_reads(), TicketReads::Off(Gap::NotConnected));
    assert_eq!(Gap::NotConnected.words(), NOT_CONNECTED);
    // A plain webhook is connected too, and says what it can't do.
    let hook = Connection::from_config(None, true);
    assert!(hook.connected());
    assert!(!Gap::WebhookOnly.words().contains("isn't connected"));
}
