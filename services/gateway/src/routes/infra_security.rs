//! Infrastructure vulnerabilities on the Security and Insights pages
//! (`common::infra_vuln` has the model, `worker::passes::infra_feeds` reads
//! the feeds and `worker::passes::infra_inventory` the team's accounts).
//!
//! - `GET /api/security/infrastructure/sources` — the public feeds: when
//!   each was last read, how many items, and its last error. Any team
//!   member.
//! - `GET /api/security/infrastructure/resources` — what runs in the team's
//!   connected accounts, one row per fix group, each account's last read
//!   and the permissions it was refused, and the accounts where the option
//!   is on but the role lacks the reads. Any team member.

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::extract::State;
use axum::{http::StatusCode, Extension, Json};
use common::infra_vuln::inventory::{Resource, RESOURCE_PREFIX};
use common::infra_vuln::store;
use common::live::aws_account;
use serde_json::{json, Value};
use tracing::warn;

use crate::models::Claims;
use crate::AppState;

type Item = HashMap<String, AttributeValue>;

/// Rows read per team.
const MAX_ROWS: usize = 2_000;
/// The summary row of an account's last read (the worker writes it).
const READ_PREFIX: &str = "RES_READ#";

/// GET /api/security/infrastructure/sources
pub async fn sources(
    State(state): State<Arc<AppState>>,
    Extension(_claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let st = store::statuses(&state.dynamo, &state.config.settings_table_name).await;
    Ok(Json(store::sources_view(&st, chrono::Utc::now())))
}

async fn rows(state: &AppState, team_id: &str, prefix: &str) -> Result<Vec<Item>, StatusCode> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let r = state
            .dynamo
            .query()
            .table_name(&state.config.aws_insights_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", AttributeValue::S(team_id.into()))
            .expression_attribute_values(":p", AttributeValue::S(prefix.into()))
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|e| {
                warn!(error = %e, "infrastructure: rows could not be read");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        out.extend(r.items().iter().cloned());
        match r.last_evaluated_key() {
            Some(k) if out.len() < MAX_ROWS => start = Some(k.clone()),
            _ => break,
        }
    }
    Ok(out)
}

fn json_of(i: &Item) -> &str {
    i.get("json")
        .and_then(|v| v.as_s().ok())
        .map(String::as_str)
        .unwrap_or("")
}

/// What the page shows, from the stored rows (pure, for tests).
pub fn resources_view(
    resources: Vec<Resource>,
    reads: Vec<Value>,
    accounts: &[aws_account::AwsAccount],
) -> Value {
    let mut resources = resources;
    resources.sort_by(|a, b| {
        (a.account.as_str(), a.kind, a.name.as_str()).cmp(&(
            b.account.as_str(),
            b.kind,
            b.name.as_str(),
        ))
    });
    let accounts: Vec<Value> = accounts
        .iter()
        .map(|a| {
            json!({
                "account": a.account,
                "region": a.region,
                "on": a.infra_on(),
                "available": a.infra_available(),
                "missing": a.infra_missing(),
                "read": reads.iter().find(|r| r["account"] == a.account.as_str()),
            })
        })
        .collect();
    json!({
        "accounts": accounts,
        "resources": resources,
        "count": resources.len(),
    })
}

/// GET /api/security/infrastructure/resources
pub async fn resources(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let res: Vec<Resource> = rows(&state, &claims.team_id, RESOURCE_PREFIX)
        .await?
        .iter()
        .filter_map(|i| serde_json::from_str(json_of(i)).ok())
        .collect();
    let reads: Vec<Value> = rows(&state, &claims.team_id, READ_PREFIX)
        .await?
        .iter()
        .filter_map(|i| serde_json::from_str(json_of(i)).ok())
        .collect();
    let accounts = aws_account::list(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        &claims.team_id,
    )
    .await
    .unwrap_or_default();
    Ok(Json(resources_view(res, reads, &accounts)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::infra_vuln::inventory::Kind;

    #[test]
    fn the_view_lists_resources_and_names_what_an_account_lacks() {
        let r = Resource {
            id: "web-asg".into(),
            kind: Kind::AutoScalingGroup,
            name: "web-asg".into(),
            account: "111122223333".into(),
            ..Default::default()
        };
        let mut a = aws_account::AwsAccount {
            account: "111122223333".into(),
            role_arn: "arn:aws:iam::111122223333:role/CoderHelmLogReader".into(),
            region: "us-east-1".into(),
            ..Default::default()
        };
        a.abilities.infra_on = true;
        let read = json!({ "account": "111122223333", "errors": [{ "call": "DescribeInstances", "permission": "ec2:DescribeInstances" }] });
        let v = resources_view(vec![r], vec![read], &[a]);
        assert_eq!(v["count"], 1);
        assert_eq!(v["resources"][0]["kind"], "auto_scaling_group");
        assert_eq!(v["accounts"][0]["on"], true);
        assert_eq!(
            v["accounts"][0]["read"]["errors"][0]["permission"],
            "ec2:DescribeInstances"
        );
        assert!(!v.to_string().contains("external_id"));
    }
}
