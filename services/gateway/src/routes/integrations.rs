//! The Integrations page: every outside system Coderhelm connects to, on one
//! page, each with its status, what Coderhelm does with it, when it was last
//! checked and the one thing to do next.
//!
//! Members see it read-only; admins act on it through each integration's own
//! routes (GitHub, Jira, AWS, New Relic and Confluence, MCP servers, Teams).
//!
//! A connected AWS account is one integration with abilities (logs, metrics,
//! Airflow). What the account's role can do is found by a read-only check,
//! run when the page loads (if the last one is older than a few minutes) and
//! on "Check again". Loading the page also folds old separate metrics and
//! Airflow connections into their AWS account (see
//! `common::live::aws_account`).

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use common::live::aws_account::{self, AwsAccount};
use common::live::connectors::{self, ConnKind, Connector};
use common::live::identity;
use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;
type Item = HashMap<String, AttributeValue>;

fn fail(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message })))
}

fn internal(context: &str, e: String) -> (StatusCode, Json<Value>) {
    error!(error = %e, "{context}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong on our side. Try again.",
    )
}

fn attr_s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn s(item: &Item, k: &str) -> String {
    item.get(k)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

fn ob(item: &Item, k: &str) -> Option<bool> {
    item.get(k).and_then(|v| v.as_bool().ok()).copied()
}

/// Where each integration sits on the page.
pub fn section_of(id: &str) -> &'static str {
    // A named REST API (`rest:<name>`) sits with the others.
    let id = id.split_once(':').map(|(k, _)| k).unwrap_or(id);
    match id {
        "github" | "jira" => "code",
        "aws" | "opensearch" | "rest" => "cloud",
        "newrelic" | "sentry" => "monitoring",
        "confluence" | "notion" | "figma" | "sanity" => "docs",
        "teams" => "chat",
        _ => "other",
    }
}

/// A card's state, worked out from what is saved. `status` is one of
/// `connected`, `needs_credentials`, `not_connected`, `error`; `action` is the
/// card's one primary action: `connect`, `add_credentials`, `check_again` or
/// `manage`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardState {
    pub status: &'static str,
    pub reason: String,
    pub action: &'static str,
}

fn cs(status: &'static str, reason: impl Into<String>, action: &'static str) -> CardState {
    CardState {
        status,
        reason: reason.into(),
        action,
    }
}

/// An MCP server from the catalog. Switched on without credentials is the
/// state that matters most: agents don't get it, and nothing else says so.
pub fn plugin_state(
    enabled: Option<bool>,
    has_credentials: bool,
    last_test_ok: Option<bool>,
    last_test_message: &str,
    tested_after_credentials: bool,
) -> CardState {
    match enabled {
        None | Some(false) => cs("not_connected", "", "connect"),
        Some(true) if !has_credentials => cs(
            "needs_credentials",
            "Switched on, but no credentials are saved, so agents don't get it.",
            "add_credentials",
        ),
        Some(true) => match last_test_ok.filter(|_| tested_after_credentials) {
            Some(false) => cs(
                "error",
                if last_test_message.is_empty() {
                    "The last test failed.".to_string()
                } else {
                    format!("The last test failed: {last_test_message}")
                },
                "check_again",
            ),
            _ => cs("connected", "", "manage"),
        },
    }
}

/// New Relic or Confluence.
pub fn connector_state(c: Option<&Connector>) -> CardState {
    match c {
        None => cs("not_connected", "", "connect"),
        Some(c) if c.key_sealed.is_empty() => {
            cs("needs_credentials", "No key is saved.", "add_credentials")
        }
        Some(c) if c.status != "ready" => cs(
            "error",
            if c.status_message.is_empty() {
                "The last test failed.".to_string()
            } else {
                c.status_message.clone()
            },
            "check_again",
        ),
        Some(c) if !c.enabled => cs(
            "connected",
            "Connected but paused: agents don't use it.",
            "manage",
        ),
        Some(_) => cs("connected", "", "manage"),
    }
}

/// A connected AWS account.
pub fn aws_state(a: &AwsAccount) -> CardState {
    if !a.abilities.check_error.is_empty() {
        return cs(
            "error",
            format!(
                "Coderhelm couldn't take on the role: {}",
                a.abilities.check_error
            ),
            "check_again",
        );
    }
    if a.status == "error" {
        return cs("error", "The last test of the role failed.", "check_again");
    }
    cs("connected", "", "manage")
}

/// What the account can't do yet that the add-on stack would give it.
pub fn aws_missing(a: &AwsAccount) -> Vec<&'static str> {
    let mut out = Vec::new();
    if a.abilities.can_metrics == Some(false) {
        out.push("metrics");
    }
    if a.abilities.can_airflow == Some(false) {
        out.push("airflow");
    }
    out
}

fn card(
    id: &str,
    name: &str,
    does: &str,
    st: CardState,
    last_check: &str,
    manage: &str,
    detail: Value,
) -> Value {
    json!({
        "id": id,
        "section": section_of(id),
        "name": name,
        "does": does,
        "status": st.status,
        "reason": st.reason,
        "action": st.action,
        "last_check": last_check,
        "manage_href": manage,
        "detail": detail,
    })
}

/// Checks the account's role (read-only) and saves what it can do.
pub(crate) async fn check_and_save(
    state: &AppState,
    team_id: &str,
    a: &mut AwsAccount,
    actor: &str,
) {
    let c = identity::check(&state.sts, &a.target(), Vec::new(), actor, true).await;
    a.apply_check(&c, &chrono::Utc::now().to_rfc3339());
    if let Err(e) = aws_account::save_abilities(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        team_id,
        a,
    )
    .await
    {
        warn!(error = %e, account = %a.account, "Could not save what the AWS role can do");
    }
}

/// Folds old separate metrics and Airflow connections into their AWS
/// accounts and removes them. Returns how many were folded.
async fn migrate_legacy(state: &AppState, team_id: &str, accounts: &mut [AwsAccount]) -> usize {
    let table = &state.config.settings_table_name;
    let legacy: Vec<Connector> = match connectors::list(&state.dynamo, table, team_id).await {
        Ok(l) => l.into_iter().filter(|c| c.kind().uses_aws()).collect(),
        Err(e) => {
            warn!(error = %e, "Could not read old connections to fold in");
            return 0;
        }
    };
    if legacy.is_empty() {
        return 0;
    }
    let now = chrono::Utc::now().to_rfc3339();
    let mut folded_kinds: Vec<(ConnKind, String)> = Vec::new();
    for a in accounts.iter_mut() {
        if a.fold_legacy(&legacy, &now) {
            if let Err(e) = aws_account::save_abilities(
                &state.dynamo,
                &state.config.aws_insights_table_name,
                team_id,
                a,
            )
            .await
            {
                warn!(error = %e, "Could not save folded abilities; keeping the old connections");
                continue;
            }
            for c in &legacy {
                if c.aws.as_ref().is_some_and(|t| t.account() == a.account) {
                    folded_kinds.push((c.kind(), a.account.clone()));
                }
            }
        }
    }
    let mut removed = 0;
    for (kind, account) in &folded_kinds {
        match connectors::delete(&state.dynamo, table, team_id, *kind, "").await {
            Ok(()) => {
                removed += 1;
                info!(
                    team_id,
                    kind = kind.as_str(),
                    account,
                    "Folded an old connection into its AWS account"
                );
            }
            Err(e) => warn!(error = %e, "Could not remove a folded connection"),
        }
    }
    removed
}

async fn get_item(state: &AppState, table: &str, key: &[(&str, &str)]) -> Option<Item> {
    let mut req = state.dynamo.get_item().table_name(table);
    for (k, v) in key {
        req = req.key(*k, attr_s(v));
    }
    req.send().await.ok().and_then(|o| o.item().cloned())
}

/// GET /api/integrations — every integration with its status. Checks each
/// AWS account's role when the last check is old, and folds old metrics and
/// Airflow connections into their accounts.
pub async fn list(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    if let Err(code) = claims.require_role(1) {
        return Err(fail(code, "Ask a member of the team to open this page."));
    }
    let team = claims.team_id.clone();
    let can_edit = crate::permissions::role_level(&claims.role) >= 3;

    // AWS accounts: fold old connections in, then check stale ones.
    let mut accounts =
        aws_account::list(&state.dynamo, &state.config.aws_insights_table_name, &team)
            .await
            .map_err(|e| internal("list aws accounts", e))?;
    let migrated = migrate_legacy(&state, &team, &mut accounts).await;
    let now = chrono::Utc::now();
    let actor = claims.email.clone();
    let checks = accounts
        .iter_mut()
        .filter(|a| a.check_is_stale(now))
        .map(|a| check_and_save(&state, &team, a, &actor));
    futures::future::join_all(checks).await;

    // Everything else, read in parallel.
    let settings = &state.config.settings_table_name;
    let team_key = [("team_id", team.as_str()), ("sk", "META")];
    let jira_key = [("pk", team.as_str()), ("sk", "JIRA#config")];
    let (saved, github, jira_secret, jira_config, teams, plugins) = tokio::join!(
        connectors::list(&state.dynamo, settings, &team),
        get_item(&state, &state.config.teams_table_name, &team_key),
        super::api::load_jira_secret(&state, &team),
        get_item(&state, &state.config.jira_config_table_name, &jira_key),
        super::alerts::team_notify_settings(&state, &team),
        state
            .dynamo
            .query()
            .table_name(super::plugins::mcp_table(&state))
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", attr_s(&team))
            .expression_attribute_values(":p", attr_s("PLUGIN#"))
            .send(),
    );
    let saved = saved.map_err(|e| internal("list connectors", e))?;
    let plugin_items: Vec<Item> = plugins
        .map(|o| o.items().to_vec())
        .map_err(|e| internal("list plugins", e.to_string()))?;

    let mut items = Vec::new();

    // GitHub.
    let org = github
        .as_ref()
        .map(|i| s(i, "github_org"))
        .unwrap_or_default();
    let installed = github
        .as_ref()
        .and_then(|i| i.get("github_installation_id"))
        .is_some();
    items.push(card(
        "github",
        "GitHub",
        "Reads your code, opens pull requests, reviews them and follows CI.",
        if installed && !org.is_empty() {
            cs("connected", format!("Installed on {org}."), "manage")
        } else {
            cs("not_connected", "", "connect")
        },
        "",
        "/settings/integrations/github",
        json!({ "org": org }),
    ));

    // Jira.
    let forge = jira_config
        .as_ref()
        .is_some_and(|i| !s(i, "add_comment_url").is_empty());
    let jira_on = forge || jira_secret.is_some();
    items.push(card(
        "jira",
        "Jira",
        "Starts work from Jira tickets and keeps their status in step with the pull request.",
        if jira_on {
            cs("connected", "", "manage")
        } else {
            cs("not_connected", "", "connect")
        },
        "",
        "/settings/integrations/jira",
        json!({ "app": forge }),
    ));

    // AWS accounts: one card each (or one to connect the first).
    let stack_url = |a: &AwsAccount| {
        (!aws_missing(a).is_empty()).then(|| super::connectors::ops_stack_url(&a.region))
    };
    if accounts.is_empty() {
        items.push(card(
            "aws",
            "AWS",
            "Reads CloudWatch logs, CloudWatch metrics and Airflow (MWAA) through a read-only role in your account.",
            cs("not_connected", "", "connect"),
            "",
            "/settings/integrations/aws",
            json!({ "accounts": [] }),
        ));
    } else {
        for a in &accounts {
            let mut detail = a.public_json();
            detail["missing"] = json!(aws_missing(a));
            detail["update_url"] = json!(stack_url(a));
            detail["stack_name"] = json!(super::connectors::OPS_STACK_NAME);
            items.push(card(
                &format!("aws-{}", a.account),
                &format!("AWS account {}", a.account),
                "Reads CloudWatch logs, CloudWatch metrics and Airflow (MWAA) through a read-only role in your account.",
                aws_state(a),
                &a.abilities.checked_at,
                "/settings/integrations/aws",
                detail,
            ));
        }
    }

    // New Relic, Confluence and OpenSearch.
    for (kind, does) in [
        (
            ConnKind::NewRelic,
            "NRQL queries, entity search, deployment markers and change events, and response time, error rate and throughput before and after a deploy.",
        ),
        (
            ConnKind::Confluence,
            "Searches and reads Confluence pages. Optionally adds text to the end of pages you list.",
        ),
        (
            ConnKind::OpenSearch,
            "Searches your OpenSearch or Elasticsearch indexes read-only, so agents can see the documents your app actually reads. Limit it to the indexes you list.",
        ),
        (
            ConnKind::Rest,
            "Reads a REST API you use (a booking or payments system, a vendor's API) with GET requests under the paths you allow, so agents can compare its records with yours.",
        ),
    ] {
        // REST: a card per connected API, then one to add another.
        let mine: Vec<&Connector> = saved.iter().filter(|c| c.kind() == kind).collect();
        let mut shown: Vec<Option<&Connector>> = mine.iter().map(|c| Some(*c)).collect();
        if mine.is_empty() || kind == ConnKind::Rest {
            shown.push(None);
        }
        for c in shown {
            let detail = c
                .map(|c| c.public_json())
                .unwrap_or_else(|| json!({ "kind": kind.as_str(), "connected": false }));
            let (id, name) = match c {
                Some(c) => (c.id(), c.label()),
                None if kind == ConnKind::Rest && !mine.is_empty() => {
                    (kind.as_str().to_string(), "Another REST API".to_string())
                }
                None => (kind.as_str().to_string(), kind.label().to_string()),
            };
            items.push(card(
                &id,
                &name,
                does,
                connector_state(c),
                c.map(|c| c.updated_at.as_str()).unwrap_or(""),
                "",
                detail,
            ));
        }
    }

    // Teams.
    let teams_on = teams.enabled && !teams.url.is_empty();
    items.push(card(
        "teams",
        "Microsoft Teams",
        "Posts alert cards, pull request links and recoveries to a Teams channel.",
        match (teams.url.is_empty(), teams.enabled) {
            (true, _) => cs("not_connected", "", "connect"),
            (false, false) => cs(
                "connected",
                "A channel is saved but posting is off.",
                "manage",
            ),
            (false, true) => cs("connected", "", "manage"),
        },
        "",
        "",
        json!({ "enabled": teams_on, "has_webhook": !teams.url.is_empty() }),
    ));

    // MCP servers.
    for p in super::plugins::catalog_json() {
        let id = p["id"].as_str().unwrap_or("").to_string();
        let rec = plugin_items
            .iter()
            .find(|i| s(i, "sk").strip_prefix("PLUGIN#") == Some(id.as_str()));
        let enabled = rec.and_then(|r| ob(r, "enabled"));
        let has_creds = rec.and_then(|r| ob(r, "has_credentials")).unwrap_or(false);
        let tested_at = rec.map(|r| s(r, "last_test_at")).unwrap_or_default();
        let creds_at = rec
            .map(|r| s(r, "credentials_updated_at"))
            .unwrap_or_default();
        let st = plugin_state(
            enabled,
            has_creds,
            rec.and_then(|r| ob(r, "last_test_ok")),
            &rec.map(|r| s(r, "last_test_message")).unwrap_or_default(),
            !tested_at.is_empty() && tested_at >= creds_at,
        );
        let custom_prompt = rec.map(|r| s(r, "custom_prompt")).unwrap_or_default();
        let detail = json!({
            "plugin": p,
            "enabled": enabled.unwrap_or(false),
            "has_credentials": has_creds,
            "custom_prompt": if can_edit { custom_prompt } else { String::new() },
            "last_test_ok": rec.and_then(|r| ob(r, "last_test_ok")),
            "last_test_message": rec.map(|r| s(r, "last_test_message")).unwrap_or_default(),
        });
        items.push(card(
            &id,
            p["name"].as_str().unwrap_or(&id),
            p["description"].as_str().unwrap_or(""),
            st,
            &tested_at,
            "",
            detail,
        ));
    }

    Ok(Json(json!({
        "can_edit": can_edit,
        "items": items,
        "migrated": migrated,
    })))
}

fn account_of(raw: &str) -> Result<&str, (StatusCode, Json<Value>)> {
    if raw.len() == 12 && raw.chars().all(|c| c.is_ascii_digit()) {
        Ok(raw)
    } else {
        Err(fail(StatusCode::NOT_FOUND, "No such AWS account."))
    }
}

async fn load_account(
    state: &AppState,
    team_id: &str,
    account: &str,
) -> Result<AwsAccount, (StatusCode, Json<Value>)> {
    aws_account::list(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        team_id,
    )
    .await
    .map_err(|e| internal("list aws accounts", e))?
    .into_iter()
    .find(|a| a.account == account)
    .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That AWS account isn't connected."))
}

fn account_reply(a: &AwsAccount) -> Value {
    let mut detail = a.public_json();
    detail["missing"] = json!(aws_missing(a));
    detail["update_url"] =
        json!((!aws_missing(a).is_empty()).then(|| super::connectors::ops_stack_url(&a.region)));
    detail["stack_name"] = json!(super::connectors::OPS_STACK_NAME);
    let st = aws_state(a);
    json!({ "account": detail, "status": st.status, "reason": st.reason, "last_check": a.abilities.checked_at })
}

/// POST /api/aws-connections/:id/abilities/check — checks now what the role
/// can do (read-only) and saves it.
pub async fn check_aws(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    if let Err(code) = claims.require_role(3) {
        return Err(fail(code, "Only admins can check integrations."));
    }
    let account = account_of(&id)?;
    let mut a = load_account(&state, &claims.team_id, account).await?;
    check_and_save(&state, &claims.team_id, &mut a, &claims.email).await;
    Ok(Json(account_reply(&a)))
}

/// What an admin may change about an account's abilities.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct AbilityChange {
    pub metrics: Option<bool>,
    pub airflow: Option<bool>,
    /// Environments agents may read; the rest of what was found is skipped.
    pub airflow_environments: Option<Vec<String>>,
}

impl AbilityChange {
    pub fn parse(body: &Value) -> Result<AbilityChange, String> {
        let flag = |k: &str| -> Result<Option<bool>, String> {
            match body.get(k) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::Bool(b)) => Ok(Some(*b)),
                Some(_) => Err(format!("{k} must be true or false.")),
            }
        };
        let envs = match body.get("airflow_environments") {
            None | Some(Value::Null) => None,
            Some(Value::Array(l)) if l.len() <= 50 => Some(
                l.iter()
                    .map(|v| {
                        v.as_str()
                            .map(str::to_string)
                            .ok_or("Environment names are text.")
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Some(_) => return Err("airflow_environments must be a list.".into()),
        };
        Ok(AbilityChange {
            metrics: flag("metrics")?,
            airflow: flag("airflow")?,
            airflow_environments: envs,
        })
    }

    /// Applies the change. Environments must be ones the check found.
    pub fn apply(&self, a: &mut AwsAccount) -> Result<(), String> {
        if let Some(m) = self.metrics {
            if m && a.abilities.can_metrics != Some(true) {
                return Err("The role can't read CloudWatch metrics yet. Add the permissions with the stack link, then check again.".into());
            }
            a.abilities.metrics_off = !m;
        }
        if let Some(on) = self.airflow {
            if on && a.abilities.can_airflow != Some(true) {
                return Err("The role can't read Airflow yet. Add the permissions with the stack link, then check again.".into());
            }
            a.abilities.airflow_off = !on;
        }
        if let Some(keep) = &self.airflow_environments {
            if let Some(bad) = keep.iter().find(|e| !a.abilities.airflow_found.contains(e)) {
                return Err(format!(
                    "{bad} isn't an Airflow environment the role can see."
                ));
            }
            a.abilities.airflow_skip = a
                .abilities
                .airflow_found
                .iter()
                .filter(|e| !keep.contains(e))
                .cloned()
                .collect();
        }
        Ok(())
    }
}

/// PUT /api/aws-connections/:id/abilities — switches metrics or Airflow on or
/// off for the account, and picks which found Airflow environments agents
/// may read.
pub async fn set_aws_abilities(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    if let Err(code) = claims.require_role(3) {
        return Err(fail(code, "Only admins can change integrations."));
    }
    let account = account_of(&id)?;
    let change = AbilityChange::parse(&body).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let mut a = load_account(&state, &claims.team_id, account).await?;
    change
        .apply(&mut a)
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    aws_account::save_abilities(
        &state.dynamo,
        &state.config.aws_insights_table_name,
        &claims.team_id,
        &a,
    )
    .await
    .map_err(|e| internal("save aws abilities", e))?;
    info!(team_id = %claims.team_id, account, metrics = a.metrics_on(), airflow = ?a.airflow_environments(), "AWS abilities changed");
    Ok(Json(account_reply(&a)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::live::identity::RoleCheck;

    fn account() -> AwsAccount {
        let mut a = AwsAccount {
            account: "123456789012".into(),
            role_arn: "arn:aws:iam::123456789012:role/CoderHelmLogReader".into(),
            external_id: "x".into(),
            region: "us-east-1".into(),
            status: "active".into(),
            ..Default::default()
        };
        a.apply_check(
            &RoleCheck {
                works: true,
                can_read_logs: Some(true),
                can_read_metrics: Some(true),
                can_read_airflow: Some(true),
                airflow_environments: vec!["prod".into(), "dev".into()],
                ..Default::default()
            },
            "2026-10-19T10:00:00Z",
        );
        a
    }

    #[test]
    fn sections_follow_the_page() {
        assert_eq!(section_of("github"), "code");
        assert_eq!(section_of("jira"), "code");
        assert_eq!(section_of("aws"), "cloud");
        assert_eq!(section_of("newrelic"), "monitoring");
        assert_eq!(section_of("sentry"), "monitoring");
        for d in ["confluence", "notion", "figma", "sanity"] {
            assert_eq!(section_of(d), "docs");
        }
        assert_eq!(section_of("teams"), "chat");
        assert_eq!(section_of("linear"), "other");
    }

    #[test]
    fn a_plugin_switched_on_without_credentials_needs_them() {
        let st = plugin_state(Some(true), false, None, "", false);
        assert_eq!(st.status, "needs_credentials");
        assert_eq!(st.action, "add_credentials");
        assert_eq!(
            plugin_state(None, false, None, "", false).status,
            "not_connected"
        );
        assert_eq!(
            plugin_state(Some(false), true, None, "", false).action,
            "connect"
        );
        assert_eq!(
            plugin_state(Some(true), true, None, "", false).status,
            "connected"
        );
        let bad = plugin_state(Some(true), true, Some(false), "401 Unauthorized", true);
        assert_eq!(bad.status, "error");
        assert!(bad.reason.contains("401"));
        assert_eq!(bad.action, "check_again");
        // New credentials since the failed test: not an error any more.
        assert_eq!(
            plugin_state(Some(true), true, Some(false), "401", false).status,
            "connected"
        );
    }

    #[test]
    fn connector_states() {
        assert_eq!(connector_state(None).status, "not_connected");
        let mut c = Connector::new(ConnKind::NewRelic);
        assert_eq!(connector_state(Some(&c)).status, "needs_credentials");
        c.key_sealed = "sealed".into();
        c.status = "error".into();
        c.status_message = "Bad key.".into();
        let st = connector_state(Some(&c));
        assert_eq!(
            (st.status, st.reason.as_str(), st.action),
            ("error", "Bad key.", "check_again")
        );
        c.status = "ready".into();
        c.enabled = true;
        assert_eq!(connector_state(Some(&c)), cs("connected", "", "manage"));
    }

    #[test]
    fn an_aws_account_that_cannot_be_reached_says_why() {
        let mut a = account();
        assert_eq!(aws_state(&a).status, "connected");
        a.abilities.check_error = "AccessDenied".into();
        let st = aws_state(&a);
        assert_eq!(st.status, "error");
        assert!(st.reason.contains("AccessDenied"));
    }

    #[test]
    fn missing_abilities_offer_the_stack() {
        let mut a = account();
        assert!(aws_missing(&a).is_empty());
        a.abilities.can_metrics = Some(false);
        a.abilities.can_airflow = Some(false);
        assert_eq!(aws_missing(&a), vec!["metrics", "airflow"]);
        let r = account_reply(&a);
        assert!(r["account"]["update_url"]
            .as_str()
            .unwrap()
            .contains("CoderHelmLogReaderOps"));
        assert!(account_reply(&account())["account"]["update_url"].is_null());
    }

    #[test]
    fn admins_switch_abilities_and_pick_environments() {
        let mut a = account();
        AbilityChange::parse(&json!({ "metrics": false, "airflow_environments": ["prod"] }))
            .unwrap()
            .apply(&mut a)
            .unwrap();
        assert!(!a.metrics_on());
        assert_eq!(a.airflow_environments(), vec!["prod"]);
        assert_eq!(a.abilities.airflow_skip, vec!["dev"]);
        AbilityChange::parse(&json!({ "metrics": true, "airflow": false }))
            .unwrap()
            .apply(&mut a)
            .unwrap();
        assert!(a.metrics_on());
        assert!(!a.airflow_on());
    }

    #[test]
    fn abilities_the_role_lacks_cannot_be_switched_on() {
        let mut a = account();
        a.abilities.can_metrics = Some(false);
        let e = AbilityChange::parse(&json!({ "metrics": true }))
            .unwrap()
            .apply(&mut a)
            .unwrap_err();
        assert!(e.contains("can't read CloudWatch metrics"));
        let e = AbilityChange::parse(&json!({ "airflow_environments": ["typed-name"] }))
            .unwrap()
            .apply(&mut account())
            .unwrap_err();
        assert!(e.contains("typed-name"), "only discovered environments");
        assert!(AbilityChange::parse(&json!({ "metrics": "yes" })).is_err());
        assert!(AbilityChange::parse(&json!({ "airflow_environments": "prod" })).is_err());
    }
}
