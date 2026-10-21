//! Every queued message that names a GitHub installation must name the one
//! stored for its team. The worker mints installation tokens from the id in
//! the message, so a message that pairs one team with another team's
//! installation would act on that other team's repositories. Checked once,
//! at dispatch, for every message type.

use serde_json::Value;

/// What the check decided for one message.
#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Nothing to check, or every installation named is the team's own.
    Allow,
    /// Refuse the message; the reason is for the log.
    Refuse(&'static str),
}

/// Every non-zero `installation_id` and every `team_id` in the message,
/// nested ones included (a ticket comment carries the job it would start).
pub fn claims(message: &Value) -> (Vec<u64>, Vec<String>) {
    fn walk(v: &Value, ids: &mut Vec<u64>, teams: &mut Vec<String>) {
        match v {
            Value::Object(map) => {
                for (k, v) in map {
                    match (k.as_str(), v) {
                        ("installation_id", Value::Number(n)) => {
                            if let Some(id) = n.as_u64().filter(|id| *id != 0) {
                                ids.push(id);
                            }
                        }
                        ("team_id", Value::String(t)) => teams.push(t.clone()),
                        _ => walk(v, ids, teams),
                    }
                }
            }
            Value::Array(items) => {
                for v in items {
                    walk(v, ids, teams);
                }
            }
            _ => {}
        }
    }
    let mut ids = Vec::new();
    let mut teams = Vec::new();
    walk(message, &mut ids, &mut teams);
    (ids, teams)
}

/// The team a message is for, when it names exactly one.
pub fn single_team(teams: &[String]) -> Result<Option<&str>, &'static str> {
    match teams.first() {
        None => Ok(None),
        Some(first) if teams.iter().all(|t| t == first) => Ok(Some(first.as_str())),
        Some(_) => Err("message names more than one team"),
    }
}

/// Decide from the installations a message names and the one stored for its
/// team. A message that names no installation is allowed (global sweeps and
/// jobs that look the installation up themselves).
pub fn decide(claimed: &[u64], team: Option<&str>, stored: Option<u64>) -> Verdict {
    if claimed.is_empty() {
        return Verdict::Allow;
    }
    if team.is_none() {
        return Verdict::Refuse("names an installation but no team");
    }
    match stored {
        None => Verdict::Refuse("team has no GitHub installation stored"),
        Some(own) if claimed.iter().all(|id| *id == own) => Verdict::Allow,
        Some(_) => Verdict::Refuse("installation is not the one stored for the team"),
    }
}

/// The GitHub installation stored on the team's META record. A failed read is
/// an error (the message is retried), not an absent installation.
pub async fn stored_installation(
    dynamo: &aws_sdk_dynamodb::Client,
    teams_table: &str,
    team_id: &str,
) -> Result<Option<u64>, aws_sdk_dynamodb::Error> {
    use aws_sdk_dynamodb::types::AttributeValue;
    let out = dynamo
        .get_item()
        .table_name(teams_table)
        .key("team_id", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S("META".to_string()))
        .projection_expression("github_installation_id")
        .send()
        .await?;
    Ok(out
        .item()
        .and_then(|i| i.get("github_installation_id"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn claims_include_nested_jobs() {
        let msg = json!({
            "type": "jira_comment",
            "team_id": "TEAM#a",
            "installation_id": 7,
            "action": {"type": "ticket", "team_id": "TEAM#a", "installation_id": 9},
            "comments": [{"installation_id": 0}],
        });
        let (mut ids, teams) = claims(&msg);
        ids.sort();
        assert_eq!(ids, vec![7, 9], "zero means unset and is skipped");
        assert_eq!(single_team(&teams), Ok(Some("TEAM#a")));
    }

    #[test]
    fn two_teams_in_one_message_are_refused() {
        let msg = json!({"team_id": "TEAM#a", "action": {"team_id": "TEAM#b"}});
        let (_, teams) = claims(&msg);
        assert!(single_team(&teams).is_err());
        assert_eq!(single_team(&[]), Ok(None));
    }

    #[test]
    fn only_the_teams_own_installation_is_allowed() {
        assert_eq!(decide(&[7], Some("TEAM#a"), Some(7)), Verdict::Allow);
        assert_eq!(decide(&[7, 7], Some("TEAM#a"), Some(7)), Verdict::Allow);
        assert!(matches!(
            decide(&[8], Some("TEAM#a"), Some(7)),
            Verdict::Refuse(_)
        ));
        assert!(matches!(
            decide(&[7, 8], Some("TEAM#a"), Some(7)),
            Verdict::Refuse(_)
        ));
        assert!(matches!(
            decide(&[7], Some("TEAM#a"), None),
            Verdict::Refuse(_)
        ));
        assert!(matches!(decide(&[7], None, Some(7)), Verdict::Refuse(_)));
    }

    #[test]
    fn messages_without_an_installation_pass() {
        assert_eq!(decide(&[], None, None), Verdict::Allow);
        assert_eq!(decide(&[], Some("TEAM#a"), None), Verdict::Allow);
        let (ids, teams) = claims(&json!({"type": "weekly_summary"}));
        assert!(ids.is_empty() && teams.is_empty());
    }
}
