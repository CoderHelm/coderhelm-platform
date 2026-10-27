//! Daily: checks every connected AWS account's CoderHelmLogReader role
//! against the permissions this release uses
//! (`common::live::permissions`), saves the result for the AWS page, and
//! tells each team once per new set of permissions when an account needs
//! the update (where the team's notifications post: Slack, Teams).
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
    told != Some(id.as_str()) && checks.iter().any(|c| c.status() == "update_needed")
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
            .filter(|c| c.status() == "update_needed")
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
    }
}
