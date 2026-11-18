//! Daily: checks every connected AWS account's CoderHelmLogReader role
//! against the permissions this release uses
//! (`common::live::permissions`), saves the result for the AWS page, and
//! tells each team once per new set of permissions when an account needs
//! the update (where the team's notifications post: Slack, Teams).
//!
//! The same sweep moves data sources off the script-made
//! `CoderhelmReadOnly` role onto the account's role stack
//! (`common::role_move`): a source switches only when the stack's role
//! passed the source's own connection check end to end.
//!
//! The same sweep sends a re-check of every run waiting for the role's
//! reads (`role_wait`), so a run whose fast re-checks are over still goes
//! on the day the role stack is updated.
//!
//! "Once" is a team record (`AWS_PERMS_NOTICE`) holding the permissions id
//! it was told about, written with a condition before the message is sent,
//! so two sweeps never post twice.

use std::collections::BTreeMap;

use aws_sdk_dynamodb::types::AttributeValue;
use common::live::aws_account::{self, AwsAccount};
use common::live::permissions::{self, PermissionCheck};
use tracing::{info, warn};

use crate::WorkerState;

pub const NOTICE_SK: &str = "AWS_PERMS_NOTICE";

#[derive(Debug, Default)]
pub struct Report {
    pub accounts: usize,
    pub need_update: usize,
    pub teams_told: usize,
    /// Data sources moved onto their account's role stack.
    pub sources_moved: usize,
}

/// Every connected account, by team.
async fn accounts_by_team(
    state: &WorkerState,
) -> Result<BTreeMap<String, Vec<AwsAccount>>, String> {
    let mut out: BTreeMap<String, Vec<AwsAccount>> = BTreeMap::new();
    let mut start = None;
    loop {
        let page = state
            .dynamo
            .scan()
            .table_name(&state.config.aws_insights_table_name)
            .filter_expression("begins_with(sk, :p)")
            .expression_attribute_values(":p", AttributeValue::S(aws_account::PREFIX.into()))
            .set_exclusive_start_key(start)
            .send()
            .await
            .map_err(|e| format!("scan aws connections: {e}"))?;
        for item in page.items() {
            let team = item
                .get("pk")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default();
            if let (false, Some(a)) = (team.is_empty(), AwsAccount::from_item(item)) {
                out.entry(team).or_default().push(a);
            }
        }
        start = page.last_evaluated_key().cloned();
        if start.is_none() {
            break;
        }
    }
    Ok(out)
}

/// Pure: whether a team that was last told about `told` should be told now.
pub fn should_tell(checks: &[PermissionCheck], told: Option<&str>) -> bool {
    let id = common::log_reader::permissions_id();
    told != Some(id.as_str()) && checks.iter().any(needs_people)
}

/// An account a person has to update: the install's own gets its
/// permissions with the next Coderhelm update, so it is never asked.
fn needs_people(c: &PermissionCheck) -> bool {
    c.status() == "update_needed" && !c.install_account
}

/// Claims this permissions id for the team: true only for the one sweep that
/// writes it first.
async fn claim_notice(state: &WorkerState, team_id: &str) -> bool {
    let id = common::log_reader::permissions_id();
    state
        .dynamo
        .put_item()
        .table_name(&state.config.aws_insights_table_name)
        .item("pk", AttributeValue::S(team_id.into()))
        .item("sk", AttributeValue::S(NOTICE_SK.into()))
        .item("permissions_id", AttributeValue::S(id.clone()))
        .item(
            "told_at",
            AttributeValue::S(chrono::Utc::now().to_rfc3339()),
        )
        .condition_expression("attribute_not_exists(pk) OR permissions_id <> :id")
        .expression_attribute_values(":id", AttributeValue::S(id))
        .send()
        .await
        .is_ok()
}

async fn told(state: &WorkerState, team_id: &str) -> Option<String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.aws_insights_table_name)
        .key("pk", AttributeValue::S(team_id.into()))
        .key("sk", AttributeValue::S(NOTICE_SK.into()))
        .send()
        .await
        .ok()?
        .item()?
        .get("permissions_id")?
        .as_s()
        .ok()
        .cloned()
}

/// Moves each of the team's data sources that still reads through a
/// script-made role onto its account's role stack, when the stack's role
/// reads it end to end; the rest stay as they are and are tried again on
/// the next sweep. Returns how many moved.
async fn move_sources(state: &WorkerState, team_id: &str, accounts: &[AwsAccount]) -> usize {
    use common::live::store;
    use common::role_move;
    let table = &state.config.settings_table_name;
    let Ok(sources) = store::list_sources(&state.dynamo, table, team_id).await else {
        return 0;
    };
    let mut moved = 0;
    for src in &sources {
        let Some(t) = src.aws.as_ref() else { continue };
        let Some(account) = accounts.iter().find(|a| a.account == t.account()) else {
            continue;
        };
        let Some(candidate) = role_move::candidate(t, account) else {
            continue;
        };
        match role_move::try_move(&state.sts, src, &candidate).await {
            Ok(next) => {
                let mut s = src.clone();
                s.aws = Some(next);
                match store::put_source(&state.dynamo, table, team_id, &s).await {
                    Ok(()) => {
                        moved += 1;
                        info!(team_id, source = %src.id, account = %account.account, "Data source moved onto the account's role stack");
                    }
                    Err(e) => {
                        warn!(team_id, source = %src.id, error = %e, "Could not save the moved data source")
                    }
                }
            }
            Err(why) => {
                info!(team_id, source = %src.id, account = %account.account, why = %why, "Data source stays on the script-made role until the role stack reads it")
            }
        }
    }
    moved
}

pub async fn run(state: &WorkerState) -> Result<Report, String> {
    let teams = accounts_by_team(state).await?;
    let mut report = Report::default();
    let now = chrono::Utc::now().to_rfc3339();
    for (team_id, accounts) in &teams {
        let mut checks = Vec::new();
        for a in accounts {
            let c = permissions::check(&state.sts, &a.target(), "permissions-check", &now).await;
            if let Err(e) = aws_account::save_permissions(
                &state.dynamo,
                &state.config.aws_insights_table_name,
                team_id,
                &c,
            )
            .await
            {
                warn!(team_id, account = %a.account, error = %e, "Could not save the permissions check");
            }
            report.accounts += 1;
            if c.status() == "update_needed" {
                report.need_update += 1;
            }
            checks.push(c);
        }
        report.sources_moved += move_sources(state, team_id, accounts).await;
        // Runs waiting for the role's reads (`awaiting_setup`) check the
        // role again now: the daily check after their fast re-checks.
        super::role_wait::wake(state, team_id, None).await;
        let last = told(state, team_id).await;
        if !should_tell(&checks, last.as_deref()) || !claim_notice(state, team_id).await {
            continue;
        }
        let cipher = common::mcp_credentials::Kms {
            client: &state.kms,
            key_id: "",
        };
        let channels = common::log_findings::channels(
            &state.dynamo,
            &cipher,
            &state.config.settings_table_name,
            team_id,
        )
        .await;
        if channels.is_empty() {
            continue;
        }
        let needing: Vec<(String, usize)> = checks
            .iter()
            .filter(|c| needs_people(c))
            .map(|c| (c.account.clone(), c.missing.len()))
            .collect();
        let url = format!(
            "{}/settings/integrations/aws",
            super::skills::app_url(&state.config.stage)
        );
        let msg = common::notify::messages::aws_permissions_update(&needing, &url);
        common::notify::deliver_recorded(
            &common::notify::Http(&state.http),
            &channels,
            &msg,
            "aws_permissions",
            &state.dynamo,
            &state.config.settings_table_name,
            team_id,
        )
        .await;
        report.teams_told += 1;
    }
    info!(
        accounts = report.accounts,
        need_update = report.need_update,
        teams_told = report.teams_told,
        sources_moved = report.sources_moved,
        "AWS permissions sweep"
    );
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::live::aws::AwsTarget;

    fn check(missing: &[&str]) -> PermissionCheck {
        let t = AwsTarget {
            role_arn: "arn:aws:iam::111122223333:role/CoderHelmLogReader".into(),
            external_id: "x".into(),
            region: "us-east-1".into(),
            ..Default::default()
        };
        let mut c = PermissionCheck::new(&t, "t");
        c.works = true;
        c.missing = missing.iter().map(|s| s.to_string()).collect();
        c
    }

    #[test]
    fn a_team_is_told_once_per_set_of_permissions() {
        let id = common::log_reader::permissions_id();
        let need = vec![check(&[]), check(&["ecs:ListTasks"])];
        assert!(should_tell(&need, None));
        assert!(should_tell(&need, Some("00000000")));
        assert!(!should_tell(&need, Some(&id)));
        assert!(!should_tell(&[check(&[])], None));
        // The install's own account is never asked to update.
        let mut own = need[0].clone();
        own.install_account = true;
        assert!(!should_tell(&[own], None));
    }
}
