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
//! - `GET /api/security/infrastructure` — the findings, one per fix, the
//!   counts and each account's state. Any team member.
//! - `GET /api/security/infrastructure/insights` — Analytics' counts.
//! - `POST /api/security/infrastructure/check` — read every switched-on
//!   account again now. Admin.
//! - `POST /api/security/infrastructure/findings/{id}/fix` — open the fix
//!   PR now, within the day's limits. Admin.

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::extract::{Path, State};
use axum::{http::StatusCode, Extension, Json};
use common::infra_vuln::inventory::{Resource, RESOURCE_PREFIX};
use common::infra_vuln::records::{self, Record};
use common::infra_vuln::store;
use common::live::aws_account;
use common::security_fixes as sec;
use serde_json::{json, Value};
use tracing::warn;

use crate::models::{Claims, SecurityEventMessage, WorkerMessage};
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
    rows_in(
        state,
        &state.config.aws_insights_table_name,
        team_id,
        prefix,
    )
    .await
}

async fn rows_in(
    state: &AppState,
    table: &str,
    team_id: &str,
    prefix: &str,
) -> Result<Vec<Item>, StatusCode> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let r = state
            .dynamo
            .query()
            .table_name(table)
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
    resources_view_with(resources, reads, accounts, &[])
}

/// [`resources_view`], each resource with its open findings' ids.
pub fn resources_view_with(
    resources: Vec<Resource>,
    reads: Vec<Value>,
    accounts: &[aws_account::AwsAccount],
    found: &[Record],
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
    let rows: Vec<Value> = resources
        .iter()
        .map(|r| {
            let mut v = serde_json::to_value(r).unwrap_or(Value::Null);
            let ids: Vec<&str> = found
                .iter()
                .filter(|f| {
                    f.status.is_open()
                        && f.finding.account == r.account
                        && f.finding.resources.contains(&r.id)
                })
                .map(|f| f.finding.id.as_str())
                .collect();
            v["findings"] = json!(ids);
            v
        })
        .collect();
    json!({
        "accounts": accounts,
        "count": rows.len(),
        "resources": rows,
    })
}

fn account_states(accounts: &[aws_account::AwsAccount], reads: &[Value]) -> Vec<Value> {
    accounts
        .iter()
        .map(|a| {
            let missing = a.infra_missing();
            json!({
                "account": a.account,
                "region": a.region,
                "on": a.infra_on(),
                "available": a.infra_available(),
                "missing": missing,
                "update_url": (!missing.is_empty()).then(|| super::connectors::update_stack_url(&a.region, &a.chosen())).flatten(),
                "read": reads.iter().find(|r| r["account"] == a.account.as_str()),
            })
        })
        .collect()
}

/// The Infrastructure tab (pure, for tests).
pub fn findings_view(
    found: &[Record],
    accounts: Vec<Value>,
    cfg: &sec::SecurityConfig,
    reads: &[Value],
    now: chrono::DateTime<chrono::Utc>,
) -> Value {
    let mut shown: Vec<&Record> = found
        .iter()
        .filter(|r| {
            r.status != records::Status::Fixed
                || r.fixed_at
                    .as_deref()
                    .is_some_and(|t| t > (now - chrono::Duration::days(30)).to_rfc3339().as_str())
        })
        .collect();
    shown.sort_by(|a, b| {
        let k = |r: &Record| {
            (
                r.status == records::Status::Fixed,
                r.finding.fix.kind == Some(common::infra_vuln::matching::FixKind::Aws),
                r.tier().is_none(),
                r.tier(),
                r.finding.fix.kind,
            )
        };
        k(a).cmp(&k(b))
    });
    let read_at = reads.iter().filter_map(|r| r["read_at"].as_str()).max();
    json!({
        "enabled": cfg.enabled,
        "dry_run": cfg.dry_run,
        "counts": records::counts(found, now),
        "accounts": accounts,
        "findings": shown.iter().map(|r| r.view()).collect::<Vec<_>>(),
        "read_at": read_at,
    })
}

async fn load_records(state: &AppState, team_id: &str) -> Result<Vec<Record>, StatusCode> {
    Ok(rows_in(
        state,
        &state.config.settings_table_name,
        team_id,
        records::FINDING_PREFIX,
    )
    .await?
    .iter()
    .filter_map(|i| serde_json::from_str(json_of(i)).ok())
    .collect())
}

async fn reads(state: &AppState, team_id: &str) -> Result<Vec<Value>, StatusCode> {
    Ok(rows(state, team_id, READ_PREFIX)
        .await?
        .iter()
        .filter_map(|i| serde_json::from_str(json_of(i)).ok())
        .collect())
}

/// GET /api/security/infrastructure
pub async fn findings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let found = load_records(&state, &claims.team_id).await?;
    let r = reads(&state, &claims.team_id).await?;
    let accounts = aws_account::list(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        &claims.team_id,
    )
    .await
    .unwrap_or_default();
    let cfg = super::security::load_config(&state, &claims.team_id)
        .await?
        .unwrap_or_default();
    Ok(Json(findings_view(
        &found,
        account_states(&accounts, &r),
        &cfg,
        &r,
        chrono::Utc::now(),
    )))
}

/// GET /api/security/infrastructure/insights
pub async fn insights(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<Value>, StatusCode> {
    let found = load_records(&state, &claims.team_id).await?;
    Ok(Json(records::insights(&found, chrono::Utc::now())))
}

async fn queue(state: &AppState, claims: &Claims, kind: &str, id: &str) -> Result<(), StatusCode> {
    let msg = WorkerMessage::SecurityEvent(SecurityEventMessage {
        team_id: claims.team_id.clone(),
        installation_id: 0,
        kind: kind.into(),
        repo: id.into(),
        ..Default::default()
    });
    super::github_webhook::send_to_queue(state, &state.config.ticket_queue_url, &msg)
        .await
        .map(|_| ())
}

/// POST /api/security/infrastructure/check
pub async fn check(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    claims.require_role(3)?;
    queue(&state, &claims, "infrastructure_check", "").await?;
    tracing::info!(team = %claims.team_id, by = %claims.email, "Infrastructure check queued");
    Ok((StatusCode::ACCEPTED, Json(json!({ "status": "queued" }))))
}

/// POST /api/security/infrastructure/findings/{id}/fix
pub async fn fix(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(3)?;
    if id.len() > 64 || !id.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let found = load_records(&state, &claims.team_id).await?;
    let Some(r) = found.iter().find(|r| r.finding.id == id) else {
        return Err(StatusCode::NOT_FOUND);
    };
    let skip = match (
        r.finding.fix.kind,
        r.status,
        r.finding.fix.location.is_some(),
    ) {
        (Some(common::infra_vuln::matching::FixKind::Pr), records::Status::Open, true) => None,
        (Some(common::infra_vuln::matching::FixKind::Pr), records::Status::Open, false) => {
            Some("No file in your repositories defines this resource yet.")
        }
        (Some(common::infra_vuln::matching::FixKind::Pr), _, _) => {
            Some("A fix is already open or done.")
        }
        _ => Some("This fix happens in AWS or is AWS-managed: there is no PR to open."),
    };
    if let Some(reason) = skip {
        return Ok(Json(json!({ "status": "skipped", "reason": reason })));
    }
    queue(&state, &claims, "infrastructure_fix", &id).await?;
    tracing::info!(team = %claims.team_id, finding = %id, by = %claims.email, "Infrastructure fix requested");
    Ok(Json(json!({ "status": "queued", "reason": null })))
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

    #[test]
    fn the_tab_lists_open_fixes_first_and_aws_rows_last() {
        use common::infra_vuln::matching::{Fix, FixKind, InfraFinding};
        let f = |id: &str, kind: FixKind, tier: Option<sec::Tier>| InfraFinding {
            id: id.into(),
            account: "111122223333".into(),
            resources: vec![format!("r-{id}")],
            tier,
            fix: Fix {
                kind: Some(kind),
                ..Default::default()
            },
            ..Default::default()
        };
        let now = chrono::Utc::now();
        let recs = records::reconcile(
            &[],
            vec![
                f("aws", FixKind::Aws, None),
                f("pr", FixKind::Pr, Some(sec::Tier::P2)),
                f("run", FixKind::Runbook, Some(sec::Tier::P1)),
            ],
            now,
        );
        let v = findings_view(&recs, vec![], &sec::SecurityConfig::default(), &[], now);
        let ids: Vec<&str> = v["findings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["run", "pr", "aws"]);
        assert_eq!(v["counts"]["open"], 2);
        assert_eq!(v["findings"][2]["status"], "aws_managed");
        assert_eq!(v["findings"][0]["fix"]["kind"], "runbook");
        let r = Resource {
            id: "r-pr".into(),
            account: "111122223333".into(),
            ..Default::default()
        };
        let rv = resources_view_with(vec![r], vec![], &[], &recs);
        assert_eq!(rv["resources"][0]["findings"][0], "pr");
    }
}
