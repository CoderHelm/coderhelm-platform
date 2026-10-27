//! Which team owns a GitHub installation that several teams carry.
//!
//! Older sign-ins created a team per person, and each one linked the org's
//! installation, so one installation can sit on several team records. Only
//! one of them may receive its events. The rule (see
//! [`super::github_webhook::pick_installation_owner`]) picks the first team
//! to link it, or for records from before link times the team with the most
//! members. Member counts change, so that answer could move from one team
//! to another between two webhooks; it also cost a query per team on every
//! event. The owner is now kept as a record of its own (settings table,
//! `INSTALLATION#<id>` / `OWNER`), written once when the choice is first
//! made, and every later resolution reads it.
//!
//! The other teams' links are stale. Linking already refuses an
//! installation another team owns (auth::github_callback), so no new stale
//! link is made; the old ones are shown to platform admins (the Teams page)
//! and to the stale team's own admins (installation status), and either can
//! release them. Releasing removes only the stale team's link, never the
//! owner's, and only while that team still carries this installation.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use tracing::warn;

use crate::AppState;

const SK: &str = "OWNER";

fn pk(installation_id: u64) -> String {
    format!("INSTALLATION#{installation_id}")
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

/// The owner to route to: the recorded one while it still carries the
/// installation, else `picked` (the rule's choice from the same records).
pub fn choose(
    team_ids: &[String],
    recorded: Option<&str>,
    picked: Option<String>,
) -> Option<String> {
    match recorded {
        Some(r) if team_ids.iter().any(|t| t == r) => Some(r.to_string()),
        _ => picked,
    }
}

/// The teams that carry the installation but don't own it.
pub fn stale_teams(team_ids: &[String], owner: Option<&str>) -> Vec<String> {
    let mut out: Vec<String> = team_ids
        .iter()
        .filter(|t| Some(t.as_str()) != owner)
        .cloned()
        .collect();
    out.sort();
    out.dedup();
    out
}

/// The recorded owner, if any. A failed read is none (the rule decides).
pub async fn recorded(state: &AppState, installation_id: u64) -> Option<String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.settings_table_name)
        .key("pk", s(&pk(installation_id)))
        .key("sk", s(SK))
        .consistent_read(true)
        .send()
        .await
        .ok()?
        .item()
        .and_then(|i| i.get("team_id"))
        .and_then(|v| v.as_s().ok())
        .cloned()
}

/// Records `team_id` as the owner unless one is recorded that still carries
/// the installation; returns the owner that stands.
pub async fn record(
    state: &AppState,
    installation_id: u64,
    team_id: &str,
    team_ids: &[String],
    rule: &str,
) -> String {
    let current = recorded(state, installation_id).await;
    if let Some(c) = current
        .as_deref()
        .filter(|c| team_ids.iter().any(|t| t == c))
    {
        return c.to_string();
    }
    let mut put = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", s(&pk(installation_id)))
        .item("sk", s(SK))
        .item("team_id", s(team_id))
        .item("rule", s(rule))
        .item("decided_at", s(&chrono::Utc::now().to_rfc3339()));
    // Replace only what was read: two webhooks deciding at once agree on one.
    put = match &current {
        Some(c) => put
            .condition_expression("team_id = :old")
            .expression_attribute_values(":old", s(c)),
        None => put.condition_expression("attribute_not_exists(pk)"),
    };
    match put.send().await {
        Ok(_) => team_id.to_string(),
        Err(e) => {
            if !common::run_fail::is_condition_failed(&e) {
                warn!(installation_id, error = %aws_sdk_dynamodb::error::DisplayErrorContext(&e), "Could not record the installation's owner");
                return team_id.to_string();
            }
            recorded(state, installation_id)
                .await
                .unwrap_or_else(|| team_id.to_string())
        }
    }
}

/// Removes `team_id`'s link to `installation_id` (both tables), only while
/// the team still carries that installation and isn't its owner. Returns
/// whether a link was removed.
pub async fn release_stale(
    state: &AppState,
    team_id: &str,
    installation_id: u64,
) -> Result<bool, String> {
    let owner = super::github_webhook::resolve_team_by_installation(state, installation_id).await;
    if owner.as_deref() == Some(team_id) || owner.is_none() {
        return Ok(false);
    }
    let now = chrono::Utc::now().to_rfc3339();
    let res = state
        .dynamo
        .update_item()
        .table_name(&state.config.teams_table_name)
        .key("team_id", s(team_id))
        .key("sk", s("META"))
        .update_expression(
            "REMOVE github_installation_id, github_org, installation_linked_at \
             SET updated_at = :t, installation_released_at = :t, installation_released = :iid",
        )
        .condition_expression("github_installation_id = :iid")
        .expression_attribute_values(":iid", AttributeValue::N(installation_id.to_string()))
        .expression_attribute_values(":t", s(&now))
        .send()
        .await;
    match res {
        Ok(_) => {}
        Err(e) if common::run_fail::is_condition_failed(&e) => return Ok(false),
        Err(e) => return Err(common::run_fail::error_text(&e)),
    }
    // The worker's copy, only while it names the same installation.
    if let Err(e) = state
        .dynamo
        .update_item()
        .table_name(&state.config.table_name)
        .key("pk", s(team_id))
        .key("sk", s("META"))
        .update_expression("REMOVE github_install_id, github_org SET updated_at = :t")
        .condition_expression("github_install_id = :iid")
        .expression_attribute_values(":iid", AttributeValue::N(installation_id.to_string()))
        .expression_attribute_values(":t", s(&now))
        .send()
        .await
    {
        if !common::run_fail::is_condition_failed(&e) {
            warn!(team_id, installation_id, error = %common::run_fail::error_text(&e), "Released the stale link; the worker's copy stayed");
        }
    }
    Ok(true)
}

/// Installations several teams carry, from the teams' records: id to team
/// ids. For the admin page.
pub fn shared(links: &[(String, u64)]) -> HashMap<u64, Vec<String>> {
    let mut by: HashMap<u64, Vec<String>> = HashMap::new();
    for (team, iid) in links {
        if *iid > 0 {
            by.entry(*iid).or_default().push(team.clone());
        }
    }
    by.retain(|_, teams| {
        teams.sort();
        teams.dedup();
        teams.len() > 1
    });
    by
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// The recorded owner stands while it carries the installation; the
    /// rule's pick (member counts can change) never moves it.
    #[test]
    fn the_recorded_owner_stands_while_it_holds_the_installation() {
        let teams = ids(&["TEAM#a", "TEAM#b", "TEAM#c"]);
        assert_eq!(
            choose(&teams, Some("TEAM#b"), Some("TEAM#c".into())).as_deref(),
            Some("TEAM#b")
        );
        // Released or gone: the rule decides again.
        assert_eq!(
            choose(&teams, Some("TEAM#z"), Some("TEAM#c".into())).as_deref(),
            Some("TEAM#c")
        );
        assert_eq!(
            choose(&teams, None, Some("TEAM#a".into())).as_deref(),
            Some("TEAM#a")
        );
    }

    #[test]
    fn every_team_but_the_owner_holds_a_stale_link() {
        let teams = ids(&["TEAM#c", "TEAM#a", "TEAM#b", "TEAM#a"]);
        assert_eq!(
            stale_teams(&teams, Some("TEAM#a")),
            ids(&["TEAM#b", "TEAM#c"])
        );
        assert_eq!(
            stale_teams(&ids(&["TEAM#a"]), Some("TEAM#a")),
            Vec::<String>::new()
        );
    }

    #[test]
    fn shared_installations_are_found_from_the_team_records() {
        let links = vec![
            ("TEAM#a".to_string(), 7),
            ("TEAM#b".to_string(), 7),
            ("TEAM#c".to_string(), 9),
            ("TEAM#d".to_string(), 0),
            ("TEAM#e".to_string(), 0),
        ];
        let s = shared(&links);
        assert_eq!(s.len(), 1);
        assert_eq!(s[&7], ids(&["TEAM#a", "TEAM#b"]));
    }
}
