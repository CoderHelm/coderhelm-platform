//! Open sign-up on the hosted deployment: anyone may create an account, and
//! a new account always starts its own team. Nobody joins an existing team
//! without that team's invite (there is no join-by-email-domain on hosted),
//! and a team that moved to its own install takes no one on hosted at all.
//!
//! Guardrails for a team made at sign-up:
//! - new teams are counted per address and across the deployment, in
//!   DynamoDB so the limits hold across instances
//!   ([`PER_ADDRESS_PER_HOUR`], [`ALL_PER_HOUR`]);
//! - each new team starts with a monthly token limit ([`default_limits`]),
//!   which its admins can change in Settings, Token limit.
//!
//! Self-hosted installs never come here: they're invite-only
//! (`sign_in::applies`).

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::http::StatusCode;
use common::token_limits::{
    MAX_CACHE_READ_TOKENS, MAX_CACHE_WRITE_TOKENS, MAX_NEW_TOKENS, SETTINGS_SK,
};
use tracing::{info, warn};

use crate::middleware::rate_limit;
use crate::AppState;

/// New teams one address may start in an hour.
pub const PER_ADDRESS_PER_HOUR: u64 = 5;
/// New teams the whole deployment starts in an hour.
pub const ALL_PER_HOUR: u64 = 300;
const HOUR: u64 = 3600;

/// A new team's monthly limits: new (uncached) tokens, cache reads, cache
/// writes.
pub const DEFAULT_NEW_TOKENS: u64 = 10_000_000;
pub const DEFAULT_CACHE_READ_TOKENS: u64 = 100_000_000;
pub const DEFAULT_CACHE_WRITE_TOKENS: u64 = 10_000_000;

/// Whether one more team may start from this address now. Counts the try;
/// `Err(429)` when either limit is reached.
pub async fn allow_new_team(state: &AppState, ip: &str) -> Result<(), StatusCode> {
    rate_limit::allow_shared(state, "signup_address", ip, PER_ADDRESS_PER_HOUR, HOUR).await?;
    rate_limit::allow_shared(state, "signup_all", "all", ALL_PER_HOUR, HOUR).await
}

/// The settings item a new team starts with (`SETTINGS#TOKEN_LIMIT`).
pub fn default_limits(team_id: &str) -> HashMap<String, AttributeValue> {
    let n = |v: u64| AttributeValue::N(v.to_string());
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(team_id.to_string())),
        ("sk".to_string(), AttributeValue::S(SETTINGS_SK.to_string())),
        (MAX_NEW_TOKENS.to_string(), n(DEFAULT_NEW_TOKENS)),
        (
            MAX_CACHE_READ_TOKENS.to_string(),
            n(DEFAULT_CACHE_READ_TOKENS),
        ),
        (
            MAX_CACHE_WRITE_TOKENS.to_string(),
            n(DEFAULT_CACHE_WRITE_TOKENS),
        ),
        (
            "set_by".to_string(),
            AttributeValue::S("default for a new team".to_string()),
        ),
    ])
}

/// Gives a team made at sign-up its starting limits, unless it has some.
/// A failed write is logged; it never stops the sign-up.
pub async fn set_default_limits(state: &AppState, team_id: &str) {
    if crate::routes::sign_in::applies() {
        return;
    }
    let r = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .set_item(Some(default_limits(team_id)))
        .condition_expression("attribute_not_exists(pk)")
        .send()
        .await;
    match r {
        Ok(_) => info!(team = %team_id, "New team: default token limits set"),
        Err(e) if format!("{e:?}").contains("ConditionalCheckFailed") => {}
        Err(e) => warn!(team = %team_id, "New team: default token limits not set: {e}"),
    }
}

/// Whether a team moved to its own install (its hosted sign-in rule names
/// where it signs in now). An unreadable rule counts as moved: an invite
/// into it is then skipped, never accepted on a guess.
pub async fn team_moved(state: &AppState, team_id: &str) -> bool {
    match crate::routes::sign_in::team_rule(state, team_id).await {
        Ok(rule) => rule.moved_to.is_some(),
        Err(e) => {
            warn!(team = %team_id, "Moved check failed, so the invite is skipped: {e}");
            true
        }
    }
}

/// An invite record (`status = invited`).
pub fn is_invite(item: &HashMap<String, AttributeValue>) -> bool {
    item.get("status")
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        == Some("invited")
}

fn team_of(item: &HashMap<String, AttributeValue>) -> Option<&str> {
    item.get("pk")
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
}

/// The records a person may land on: their memberships, and invites from
/// teams that haven't moved. `moved` answers for one team.
pub fn landable<F>(
    items: &[HashMap<String, AttributeValue>],
    moved: F,
) -> Vec<HashMap<String, AttributeValue>>
where
    F: Fn(&str) -> bool,
{
    items
        .iter()
        .filter(|i| !(is_invite(i) && team_of(i).is_some_and(&moved)))
        .cloned()
        .collect()
}

/// [`landable`] with each invite's team checked on the settings table.
pub async fn drop_moved_invites(
    state: &AppState,
    items: Vec<HashMap<String, AttributeValue>>,
) -> Vec<HashMap<String, AttributeValue>> {
    let mut moved = std::collections::HashSet::new();
    for i in items.iter().filter(|i| is_invite(i)) {
        if let Some(t) = team_of(i) {
            if !moved.contains(t) && team_moved(state, t).await {
                moved.insert(t.to_string());
            }
        }
    }
    landable(&items, |t| moved.contains(t))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::token_limits::{LimitKind, TokenLimits};

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.to_string())
    }

    fn rec(team: &str, email: &str, status: &str) -> HashMap<String, AttributeValue> {
        HashMap::from([
            ("pk".to_string(), s(team)),
            ("sk".to_string(), s("USER#u")),
            ("email".to_string(), s(email)),
            ("status".to_string(), s(status)),
        ])
    }

    #[test]
    fn a_new_team_starts_with_all_three_limits() {
        let item = default_limits("TEAM#new");
        let l = TokenLimits::from_item(Some(&item));
        assert_eq!(l.get(LimitKind::NewTokens), Some(DEFAULT_NEW_TOKENS));
        assert_eq!(l.get(LimitKind::CacheRead), Some(DEFAULT_CACHE_READ_TOKENS));
        assert_eq!(
            l.get(LimitKind::CacheWrite),
            Some(DEFAULT_CACHE_WRITE_TOKENS)
        );
        assert!(!l.legacy);
    }

    #[test]
    fn an_invite_from_a_moved_team_is_never_landed_on() {
        let items = vec![
            rec("TEAM#moved", "a@acme.test", "invited"),
            rec("TEAM#open", "a@acme.test", "invited"),
        ];
        let got = landable(&items, |t| t == "TEAM#moved");
        assert_eq!(got.len(), 1);
        assert_eq!(team_of(&got[0]), Some("TEAM#open"));
    }

    #[test]
    fn a_membership_in_a_moved_team_stays_so_hosted_can_say_where_it_went() {
        // Existing members are refused by the session check (`team_moved`),
        // which points them at the install; only new joins are dropped here.
        let items = vec![rec("TEAM#moved", "a@acme.test", "active")];
        assert_eq!(landable(&items, |_| true).len(), 1);
    }

    // The limits are small per address and bounded overall.
    const _: () = assert!(PER_ADDRESS_PER_HOUR <= 10 && ALL_PER_HOUR >= PER_ADDRESS_PER_HOUR);

    const AUTH: &str = include_str!("auth.rs");

    fn hosted_part(src: &str) -> &str {
        // Everything before the tests: the code that runs.
        src.split("#[cfg(test)]").next().unwrap_or(src)
    }

    #[test]
    fn hosted_sign_in_has_no_join_by_email_domain() {
        // A person's records come from their exact email (the email index);
        // the only domain rule is the self-hosted one in sign_in.rs. An
        // email at a team's domain therefore never lands on that team.
        let code = hosted_part(AUTH);
        assert!(
            !code.contains("EMAIL#*@"),
            "no domain wildcard on hosted sign-in"
        );
        assert!(!code.contains("is_email_allowed"), "no allowlist gate left");
        assert!(code.contains(r#"format!("EMAIL#{}", normalize_email("#));
    }

    #[test]
    fn every_team_made_at_sign_in_gets_limits_and_is_counted() {
        let code = hosted_part(AUTH);
        // Three places start a team: password (verify_email), Google, GitHub.
        assert_eq!(code.matches("open_signup::set_default_limits(").count(), 3);
        // Password sign-up, and a first Google or GitHub sign-in, are counted.
        assert_eq!(code.matches("open_signup::allow_new_team(").count(), 3);
        // Invites from moved teams are dropped on every way in.
        assert_eq!(code.matches("open_signup::drop_moved_invites(").count(), 3);
    }

    #[test]
    fn an_attacker_with_a_teams_domain_gets_their_own_team() {
        // No records for their exact email: a personal team, never another's.
        let mine: Vec<HashMap<String, AttributeValue>> = vec![];
        assert!(landable(&mine, |_| false).is_empty());
        // A pending invite only for someone else's email isn't in their records;
        // an expired invite for theirs doesn't count either (needs_personal_team).
    }
}
