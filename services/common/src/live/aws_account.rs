//! A connected AWS account and what Coderhelm can do with it.
//!
//! One record per account (`AWS_CONN#<account>` in the AWS insights table),
//! made when a team connects its CoderHelmLogReader role. Besides logs, the
//! same role can read CloudWatch metrics and Airflow (MWAA) once the team adds
//! the read policy. Those are **abilities** of the account, found by a
//! read-only check of the role (one ListMetrics, one ListEnvironments):
//!
//! - an ability the role can use is on unless an admin switched it off;
//! - Airflow environments are the ones ListEnvironments returns, minus any an
//!   admin deselected (nobody types names);
//! - an ability the role can't use is off, and the page offers the one-click
//!   stack that adds it.
//!
//! Before abilities, metrics and Airflow were separate `CONNECTOR#metrics` and
//! `CONNECTOR#airflow` records. [`AwsAccount::fold_legacy`] carries what an
//! admin chose there onto the account, after which the old records are
//! removed.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::Serialize;
use serde_json::{json, Value};

use super::aws::AwsTarget;
use super::connectors::{ConnKind, Connector};
use super::identity::RoleCheck;

type Item = HashMap<String, AttributeValue>;

pub const PREFIX: &str = "AWS_CONN#";
/// A check older than this is run again when the Integrations page loads.
pub const RECHECK_AFTER_SECS: i64 = 15 * 60;

/// What the role can do, as last checked, and what an admin switched off.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Abilities {
    /// When the role was last checked (RFC 3339), empty when never.
    pub checked_at: String,
    /// Why the last check couldn't take on the role, empty when it could.
    pub check_error: String,
    /// `None` when not known yet.
    pub can_logs: Option<bool>,
    pub can_metrics: Option<bool>,
    pub can_airflow: Option<bool>,
    /// An admin switched it off.
    pub metrics_off: bool,
    pub airflow_off: bool,
    /// MWAA environments the role can see, from the last check.
    pub airflow_found: Vec<String>,
    /// Found environments an admin deselected.
    pub airflow_skip: Vec<String>,
    /// When old metrics or Airflow connections were folded in.
    pub migrated_at: String,
}

/// One connected AWS account.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AwsAccount {
    pub account: String,
    pub role_arn: String,
    pub external_id: String,
    pub region: String,
    /// `active` or `error` (from connecting or the log reader's test).
    pub status: String,
    pub abilities: Abilities,
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
fn ob(item: &Item, k: &str) -> Option<bool> {
    item.get(k).and_then(|v| v.as_bool().ok()).copied()
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

impl AwsAccount {
    pub fn from_item(item: &Item) -> Option<AwsAccount> {
        let account = s(item, "account_id");
        let role_arn = s(item, "role_arn");
        if account.is_empty() || role_arn.is_empty() {
            return None;
        }
        let region = s(item, "region");
        Some(AwsAccount {
            account,
            role_arn,
            external_id: s(item, "external_id"),
            region: if region.is_empty() {
                "us-east-1".into()
            } else {
                region
            },
            status: s(item, "status"),
            abilities: Abilities {
                checked_at: s(item, "abilities_checked_at"),
                check_error: s(item, "abilities_error"),
                can_logs: ob(item, "can_logs"),
                can_metrics: ob(item, "can_metrics"),
                can_airflow: ob(item, "can_airflow"),
                metrics_off: b(item, "metrics_off"),
                airflow_off: b(item, "airflow_off"),
                airflow_found: strings(item, "airflow_found"),
                airflow_skip: strings(item, "airflow_skip"),
                migrated_at: s(item, "abilities_migrated_at"),
            },
        })
    }

    /// The fields [`save_abilities`] writes, as `name → value`. `None`
    /// removes the attribute.
    pub fn ability_fields(&self) -> Vec<(&'static str, Option<AttributeValue>)> {
        let a = &self.abilities;
        let opt = |v: Option<bool>| v.map(AttributeValue::Bool);
        let text = |v: &str| (!v.is_empty()).then(|| av_s(v));
        vec![
            ("abilities_checked_at", text(&a.checked_at)),
            ("abilities_error", text(&a.check_error)),
            ("can_logs", opt(a.can_logs)),
            ("can_metrics", opt(a.can_metrics)),
            ("can_airflow", opt(a.can_airflow)),
            ("metrics_off", Some(AttributeValue::Bool(a.metrics_off))),
            ("airflow_off", Some(AttributeValue::Bool(a.airflow_off))),
            ("airflow_found", Some(av_list(&a.airflow_found))),
            ("airflow_skip", Some(av_list(&a.airflow_skip))),
            ("abilities_migrated_at", text(&a.migrated_at)),
        ]
    }

    /// The role, with the team's private ID, in the account's region.
    pub fn target(&self) -> AwsTarget {
        AwsTarget {
            role_arn: self.role_arn.clone(),
            external_id: self.external_id.clone(),
            region: self.region.clone(),
            ..Default::default()
        }
    }

    /// Agents may read CloudWatch metrics here.
    pub fn metrics_on(&self) -> bool {
        self.abilities.can_metrics == Some(true) && !self.abilities.metrics_off
    }

    /// The Airflow environments agents may read here (none when off).
    pub fn airflow_environments(&self) -> Vec<String> {
        let a = &self.abilities;
        if a.can_airflow != Some(true) || a.airflow_off {
            return Vec::new();
        }
        a.airflow_found
            .iter()
            .filter(|e| !a.airflow_skip.contains(e))
            .cloned()
            .collect()
    }

    pub fn airflow_on(&self) -> bool {
        !self.airflow_environments().is_empty()
    }

    /// The account was never checked, or the last check is older than
    /// [`RECHECK_AFTER_SECS`] at `now`.
    pub fn check_is_stale(&self, now: chrono::DateTime<chrono::Utc>) -> bool {
        match chrono::DateTime::parse_from_rfc3339(&self.abilities.checked_at) {
            Ok(t) => (now - t.with_timezone(&chrono::Utc)).num_seconds() >= RECHECK_AFTER_SECS,
            Err(_) => true,
        }
    }

    /// Takes the result of a role check. What an admin switched off stays
    /// off; deselected environments that no longer exist are forgotten.
    pub fn apply_check(&mut self, c: &RoleCheck, now: &str) {
        let a = &mut self.abilities;
        a.checked_at = now.to_string();
        if !c.works {
            a.check_error = if c.error.is_empty() {
                "Coderhelm couldn't take on the role.".into()
            } else {
                c.error.clone()
            };
            // Keep the last known abilities: a passing outage shouldn't
            // switch anything off. Agents get the same error when they try.
            return;
        }
        a.check_error.clear();
        a.can_logs = c.can_read_logs.or(a.can_logs);
        // `None` means AWS answered with something other than "not
        // allowed": keep what was known.
        a.can_metrics = c.can_read_metrics.or(a.can_metrics);
        match c.can_read_airflow {
            Some(true) => {
                a.can_airflow = Some(true);
                let mut found = c.airflow_environments.clone();
                found.sort();
                found.dedup();
                a.airflow_skip.retain(|e| found.contains(e));
                a.airflow_found = found;
            }
            Some(false) => {
                a.can_airflow = Some(false);
                a.airflow_found.clear();
            }
            None => {}
        }
    }

    /// Carries old `CONNECTOR#metrics` / `CONNECTOR#airflow` records for this
    /// account onto it. Returns whether anything was folded in.
    ///
    /// - A connection an admin had saved, tested and then turned off stays
    ///   off. One that was on, or never passed its test, follows what the
    ///   role can do (on by default).
    /// - Airflow environments named there are kept until the next check
    ///   lists what really exists; an environment the role can see but the
    ///   old connection didn't name is not deselected, since nobody chose to
    ///   leave it out.
    pub fn fold_legacy(&mut self, legacy: &[Connector], now: &str) -> bool {
        let mut folded = false;
        for c in legacy {
            let mine = c.aws.as_ref().is_some_and(|t| t.account() == self.account);
            if !mine {
                continue;
            }
            let chose_off = !c.enabled && c.status == "ready";
            match c.kind() {
                ConnKind::Metrics => {
                    self.abilities.metrics_off = chose_off;
                    folded = true;
                }
                ConnKind::Airflow => {
                    self.abilities.airflow_off = chose_off;
                    for e in &c.environments {
                        if !self.abilities.airflow_found.contains(e) {
                            self.abilities.airflow_found.push(e.clone());
                        }
                    }
                    if !c.environments.is_empty() && self.abilities.can_airflow.is_none() {
                        // It passed its test with these names before.
                        self.abilities.can_airflow = Some(c.status == "ready");
                    }
                    folded = true;
                }
                _ => {}
            }
            if c.kind() == ConnKind::Metrics
                && c.status == "ready"
                && self.abilities.can_metrics.is_none()
            {
                self.abilities.can_metrics = Some(true);
            }
        }
        if folded {
            self.abilities.migrated_at = now.to_string();
        }
        folded
    }

    /// What a browser may see. The private ID never is.
    pub fn public_json(&self) -> Value {
        let a = &self.abilities;
        json!({
            "account": self.account,
            "region": self.region,
            "role": self.role_arn.rsplit('/').next().unwrap_or(""),
            "status": self.status,
            "checked_at": a.checked_at,
            "check_error": a.check_error,
            "logs": { "available": a.can_logs, "on": a.can_logs != Some(false) },
            "metrics": { "available": a.can_metrics, "on": self.metrics_on(), "switched_off": a.metrics_off },
            "airflow": {
                "available": a.can_airflow,
                "on": self.airflow_on(),
                "switched_off": a.airflow_off,
                "found": a.airflow_found,
                "skipped": a.airflow_skip,
                "environments": self.airflow_environments(),
            },
        })
    }

    /// One line for an agent's prompt and the skill check.
    pub fn describe(&self) -> String {
        let mut can = vec!["logs"];
        if self.metrics_on() {
            can.push("CloudWatch metrics");
        }
        let envs = self.airflow_environments();
        let airflow = if envs.is_empty() {
            String::new()
        } else {
            format!(" (Airflow environments: {})", envs.join(", "))
        };
        if !envs.is_empty() {
            can.push("Airflow");
        }
        format!(
            "AWS account {} ({}): {}{}",
            self.account,
            self.region,
            can.join(", "),
            airflow
        )
    }
}

/// Every connected account of a team.
pub async fn list(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
) -> Result<Vec<AwsAccount>, String> {
    let out = dynamo
        .query()
        .table_name(table)
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", av_s(team_id))
        .expression_attribute_values(":p", av_s(PREFIX))
        .send()
        .await
        .map_err(|e| format!("list aws accounts: {e}"))?;
    Ok(out
        .items()
        .iter()
        .filter_map(AwsAccount::from_item)
        .collect())
}

/// Writes only the ability fields, and only while the account is still
/// connected (a disconnect in between wins).
pub async fn save_abilities(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &str,
    team_id: &str,
    a: &AwsAccount,
) -> Result<(), String> {
    let mut set = Vec::new();
    let mut remove = Vec::new();
    let mut req = dynamo
        .update_item()
        .table_name(table)
        .key("pk", av_s(team_id))
        .key("sk", av_s(&format!("{PREFIX}{}", a.account)))
        .condition_expression("attribute_exists(pk)");
    for (i, (name, value)) in a.ability_fields().into_iter().enumerate() {
        let n = format!("#a{i}");
        req = req.expression_attribute_names(&n, name);
        match value {
            Some(v) => {
                let v_name = format!(":a{i}");
                set.push(format!("{n} = {v_name}"));
                req = req.expression_attribute_values(v_name, v);
            }
            None => remove.push(n),
        }
    }
    let mut expr = format!("SET {}", set.join(", "));
    if !remove.is_empty() {
        expr.push_str(&format!(" REMOVE {}", remove.join(", ")));
    }
    req.update_expression(expr)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("save aws abilities: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account() -> AwsAccount {
        AwsAccount {
            account: "123456789012".into(),
            role_arn: "arn:aws:iam::123456789012:role/CoderHelmLogReader".into(),
            external_id: "private".into(),
            region: "us-east-1".into(),
            status: "active".into(),
            ..Default::default()
        }
    }

    fn check(metrics: Option<bool>, airflow: Option<bool>, envs: &[&str]) -> RoleCheck {
        RoleCheck {
            works: true,
            can_read_logs: Some(true),
            can_read_metrics: metrics,
            can_read_airflow: airflow,
            airflow_environments: envs.iter().map(|e| e.to_string()).collect(),
            ..Default::default()
        }
    }

    fn item(a: &AwsAccount) -> Item {
        let mut i: Item = HashMap::from([
            ("account_id".to_string(), av_s(&a.account)),
            ("role_arn".to_string(), av_s(&a.role_arn)),
            ("external_id".to_string(), av_s(&a.external_id)),
            ("region".to_string(), av_s(&a.region)),
            ("status".to_string(), av_s(&a.status)),
        ]);
        for (k, v) in a.ability_fields() {
            if let Some(v) = v {
                i.insert(k.to_string(), v);
            }
        }
        i
    }

    #[test]
    fn a_probe_that_finds_the_permissions_turns_each_ability_on() {
        let mut a = account();
        assert!(!a.metrics_on() && !a.airflow_on(), "off until checked");
        a.apply_check(
            &check(Some(true), Some(true), &["prod-airflow", "dev-airflow"]),
            "2026-10-19T10:00:00Z",
        );
        assert!(a.metrics_on());
        assert!(a.airflow_on());
        assert_eq!(
            a.airflow_environments(),
            vec!["dev-airflow", "prod-airflow"]
        );
        assert_eq!(a.abilities.checked_at, "2026-10-19T10:00:00Z");
    }

    #[test]
    fn a_probe_without_the_permissions_leaves_them_off() {
        let mut a = account();
        a.apply_check(&check(Some(false), Some(false), &[]), "t");
        assert!(!a.metrics_on());
        assert!(!a.airflow_on());
        assert_eq!(a.abilities.can_metrics, Some(false));
        let j = a.public_json();
        assert_eq!(j["metrics"]["available"], json!(false));
        assert_eq!(j["airflow"]["on"], json!(false));
    }

    #[test]
    fn an_admin_switch_survives_the_next_probe() {
        let mut a = account();
        a.abilities.metrics_off = true;
        a.abilities.airflow_skip = vec!["dev-airflow".into(), "gone".into()];
        a.apply_check(
            &check(Some(true), Some(true), &["prod-airflow", "dev-airflow"]),
            "t",
        );
        assert!(!a.metrics_on(), "switched off stays off");
        assert_eq!(a.airflow_environments(), vec!["prod-airflow"]);
        assert_eq!(
            a.abilities.airflow_skip,
            vec!["dev-airflow"],
            "a deselected environment that no longer exists is forgotten"
        );
        a.abilities.airflow_off = true;
        assert!(a.airflow_environments().is_empty());
    }

    #[test]
    fn environments_are_discovered_not_typed() {
        let mut a = account();
        a.apply_check(&check(Some(true), Some(true), &["prod-data-airflow"]), "t1");
        assert_eq!(a.airflow_environments(), vec!["prod-data-airflow"]);
        // A new environment appears; the next check finds it.
        a.apply_check(
            &check(
                Some(true),
                Some(true),
                &["prod-data-airflow", "staging-airflow"],
            ),
            "t2",
        );
        assert_eq!(
            a.airflow_environments(),
            vec!["prod-data-airflow", "staging-airflow"]
        );
        // An environment is deleted; it stops being offered.
        a.apply_check(&check(Some(true), Some(true), &["staging-airflow"]), "t3");
        assert_eq!(a.airflow_environments(), vec!["staging-airflow"]);
    }

    #[test]
    fn a_role_that_cannot_be_taken_on_keeps_what_was_known() {
        let mut a = account();
        a.apply_check(&check(Some(true), Some(true), &["prod"]), "t1");
        let broken = RoleCheck {
            works: false,
            error: "AccessDenied".into(),
            ..Default::default()
        };
        a.apply_check(&broken, "t2");
        assert_eq!(a.abilities.check_error, "AccessDenied");
        assert!(a.metrics_on());
        assert_eq!(a.airflow_environments(), vec!["prod"]);
        // An unclear answer for one ability (None) keeps the old value.
        a.apply_check(&check(None, None, &[]), "t3");
        assert!(a.abilities.check_error.is_empty());
        assert!(a.metrics_on());
        assert_eq!(a.airflow_environments(), vec!["prod"]);
    }

    #[test]
    fn staleness_follows_the_last_check() {
        let mut a = account();
        let now = chrono::DateTime::parse_from_rfc3339("2026-10-19T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert!(a.check_is_stale(now), "never checked");
        a.abilities.checked_at = "2026-10-19T09:50:00Z".into();
        assert!(!a.check_is_stale(now));
        a.abilities.checked_at = "2026-10-19T09:40:00Z".into();
        assert!(a.check_is_stale(now));
    }

    #[test]
    fn the_record_survives_the_round_trip() {
        let mut a = account();
        a.apply_check(&check(Some(true), Some(true), &["prod"]), "t");
        a.abilities.airflow_skip = vec!["prod".into()];
        a.abilities.metrics_off = true;
        a.abilities.migrated_at = "m".into();
        assert_eq!(AwsAccount::from_item(&item(&a)).unwrap(), a);
        // An account connected before abilities existed reads as unknown.
        let old = AwsAccount::from_item(&item(&account())).unwrap();
        assert_eq!(old.abilities.can_metrics, None);
        assert!(!old.metrics_on());
    }

    #[test]
    fn what_a_browser_sees_has_no_private_id() {
        let mut a = account();
        a.apply_check(&check(Some(true), Some(true), &["prod"]), "t");
        let text = a.public_json().to_string();
        assert!(!text.contains("private"), "{text}");
        assert!(text.contains("CoderHelmLogReader"));
        assert!(a.describe().contains("CloudWatch metrics"));
        assert!(a.describe().contains("Airflow environments: prod"));
    }

    fn legacy(
        kind: ConnKind,
        account: &str,
        enabled: bool,
        status: &str,
        envs: &[&str],
    ) -> Connector {
        Connector {
            enabled,
            status: status.into(),
            aws: Some(AwsTarget {
                role_arn: format!("arn:aws:iam::{account}:role/CoderHelmLogReader"),
                external_id: "x".into(),
                region: "us-east-1".into(),
                ..Default::default()
            }),
            environments: envs.iter().map(|e| e.to_string()).collect(),
            ..Connector::new(kind)
        }
    }

    #[test]
    fn old_metrics_and_airflow_connections_fold_into_the_account() {
        let mut a = account();
        let old = vec![
            legacy(ConnKind::Metrics, "123456789012", true, "ready", &[]),
            legacy(
                ConnKind::Airflow,
                "123456789012",
                true,
                "ready",
                &["prod-airflow"],
            ),
            legacy(ConnKind::Airflow, "999999999999", true, "ready", &["other"]),
        ];
        assert!(a.fold_legacy(&old, "now"));
        assert_eq!(a.abilities.migrated_at, "now");
        assert!(a.metrics_on(), "it was on and working");
        assert_eq!(a.airflow_environments(), vec!["prod-airflow"]);
        assert!(
            !a.abilities.airflow_found.contains(&"other".to_string()),
            "another account's"
        );
        // The next check lists what really exists.
        a.apply_check(
            &check(Some(true), Some(true), &["prod-airflow", "dev"]),
            "t",
        );
        assert_eq!(a.airflow_environments(), vec!["dev", "prod-airflow"]);
    }

    #[test]
    fn a_connection_an_admin_turned_off_stays_off_after_folding() {
        let mut a = account();
        let old = vec![
            legacy(ConnKind::Metrics, "123456789012", false, "ready", &[]),
            // Off because its test failed (the role lacked access): not a choice.
            legacy(ConnKind::Airflow, "123456789012", false, "error", &["prod"]),
        ];
        assert!(a.fold_legacy(&old, "now"));
        assert!(a.abilities.metrics_off);
        assert!(!a.abilities.airflow_off);
        a.apply_check(&check(Some(true), Some(true), &["prod"]), "t");
        assert!(!a.metrics_on());
        assert_eq!(a.airflow_environments(), vec!["prod"]);
    }

    #[test]
    fn nothing_to_fold_changes_nothing() {
        let mut a = account();
        let before = a.clone();
        let other = vec![legacy(
            ConnKind::Metrics,
            "999999999999",
            true,
            "ready",
            &[],
        )];
        assert!(!a.fold_legacy(&other, "now"));
        assert!(!a.fold_legacy(&[], "now"));
        assert_eq!(a, before);
    }
}
