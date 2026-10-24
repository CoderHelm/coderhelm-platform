//! Daily reconciliation of our recorded model usage against Anthropic's own
//! Usage & Cost Admin API (`/v1/organizations/usage_report/messages` and
//! `/v1/organizations/cost_report`).
//!
//! Off unless the Secrets Manager secret named by `USAGE_RECONCILE_SECRET_NAME`
//! (default `coderhelm/<stage>/anthropic-admin-keys`) exists. Teams bring their
//! own Anthropic keys, so the secret maps each team to an Admin API key of the
//! organization its key belongs to:
//!
//! ```json
//! {"<team_id>": {"admin_key": "sk-ant-admin01-...", "api_key_ids": ["apikey_..."]}}
//! ```
//!
//! `api_key_ids` (optional) limits Anthropic's report to the key CoderHelm
//! uses, so other traffic in that organization doesn't read as drift. Per day
//! and model, a difference over [`DRIFT_LIMIT`] in tokens or cost is logged as
//! `usage_drift` (a warning); the comparison is kept on the analytics item
//! `RECONCILE#<day>`. Key values are never logged.

use std::collections::{BTreeMap, HashMap};

use aws_sdk_dynamodb::types::AttributeValue;
use serde::Deserialize;
use serde_json::Value;
use tracing::{info, warn};

use crate::models::UsageReconcileMessage;
use crate::WorkerState;

/// Relative difference that counts as drift.
pub const DRIFT_LIMIT: f64 = 0.02;

const API: &str = "https://api.anthropic.com/v1/organizations";

#[derive(Deserialize)]
struct TeamKey {
    admin_key: String,
    #[serde(default)]
    api_key_ids: Vec<String>,
}

/// One model's day: tokens (all four parts) and USD.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct DayTotals {
    pub tokens: u64,
    pub cost_usd: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Drift {
    pub model: String,
    pub ours: DayTotals,
    pub theirs: DayTotals,
    pub token_drift: f64,
    pub cost_drift: f64,
}

impl Drift {
    pub fn flagged(&self) -> bool {
        self.token_drift > DRIFT_LIMIT || self.cost_drift > DRIFT_LIMIT
    }
}

fn rel(ours: f64, theirs: f64) -> f64 {
    let base = theirs.abs().max(ours.abs());
    if base == 0.0 {
        0.0
    } else {
        (ours - theirs).abs() / base
    }
}

/// Compare per model; models on either side only show up with the other at 0.
pub fn compare(
    ours: &BTreeMap<String, DayTotals>,
    theirs: &BTreeMap<String, DayTotals>,
) -> Vec<Drift> {
    let mut models: Vec<&String> = ours.keys().chain(theirs.keys()).collect();
    models.sort();
    models.dedup();
    models
        .into_iter()
        .map(|m| {
            let o = ours.get(m).copied().unwrap_or_default();
            let t = theirs.get(m).copied().unwrap_or_default();
            Drift {
                model: m.clone(),
                ours: o,
                theirs: t,
                token_drift: rel(o.tokens as f64, t.tokens as f64),
                cost_drift: rel(o.cost_usd, t.cost_usd),
            }
        })
        .collect()
}

/// Our per-model day from the `DAY#<day>` analytics item.
pub fn ours_from_item(
    item: Option<&HashMap<String, AttributeValue>>,
) -> BTreeMap<String, DayTotals> {
    let mut out: BTreeMap<String, DayTotals> = BTreeMap::new();
    let Some(i) = item else {
        return out;
    };
    for (k, v) in i {
        let Some(rest) = k.strip_prefix("model#") else {
            continue;
        };
        let Some((model, field)) = rest.rsplit_once('#') else {
            continue;
        };
        let n: f64 = v.as_n().ok().and_then(|n| n.parse().ok()).unwrap_or(0.0);
        let e = out.entry(model.to_string()).or_default();
        match field {
            "tokens_in" | "cache_read" | "cache_write" | "tokens_out" => e.tokens += n as u64,
            "cost_usd" => e.cost_usd += n,
            _ => {}
        }
    }
    out
}

fn model_key(m: &str) -> String {
    common::usage::normalize_model(m)
}

/// Add one `usage_report/messages` page to per-model tokens.
pub fn add_usage_page(page: &Value, into: &mut BTreeMap<String, DayTotals>) {
    let n = |v: &Value, k: &str| v.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
    for bucket in page["data"].as_array().into_iter().flatten() {
        for r in bucket["results"].as_array().into_iter().flatten() {
            let model = r["model"].as_str().unwrap_or("unknown");
            let cc = &r["cache_creation"];
            let tokens = n(r, "uncached_input_tokens")
                + n(r, "cache_read_input_tokens")
                + n(cc, "ephemeral_5m_input_tokens")
                + n(cc, "ephemeral_1h_input_tokens")
                + n(r, "output_tokens");
            into.entry(model_key(model)).or_default().tokens += tokens;
        }
    }
}

/// Add one `cost_report` page (grouped by description) to per-model USD.
/// Amounts are decimal strings in cents.
pub fn add_cost_page(page: &Value, into: &mut BTreeMap<String, DayTotals>) {
    for bucket in page["data"].as_array().into_iter().flatten() {
        for r in bucket["results"].as_array().into_iter().flatten() {
            let Some(model) = r["model"].as_str() else {
                continue; // web search, code execution: not token usage
            };
            let cents: f64 = match &r["amount"] {
                Value::String(s) => s.parse().unwrap_or(0.0),
                Value::Number(n) => n.as_f64().unwrap_or(0.0),
                _ => 0.0,
            };
            into.entry(model_key(model)).or_default().cost_usd += cents / 100.0;
        }
    }
}

async fn fetch_all(
    http: &reqwest::Client,
    admin_key: &str,
    url: &str,
    query: &[(String, String)],
    mut on_page: impl FnMut(&Value),
) -> Result<(), String> {
    let mut page: Option<String> = None;
    for _ in 0..50 {
        let mut q = query.to_vec();
        if let Some(p) = &page {
            q.push(("page".into(), p.clone()));
        }
        let resp = http
            .get(url)
            .header("x-api-key", admin_key)
            .header("anthropic-version", "2023-06-01")
            .query(&q)
            .send()
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        let status = resp.status();
        let body: Value = resp.json().await.map_err(|e| format!("bad body: {e}"))?;
        if !status.is_success() {
            return Err(format!(
                "{status}: {}",
                body["error"]["message"].as_str().unwrap_or("error")
            ));
        }
        on_page(&body);
        if body["has_more"].as_bool() != Some(true) {
            return Ok(());
        }
        page = body["next_page"].as_str().map(str::to_string);
        if page.is_none() {
            return Ok(());
        }
    }
    Ok(())
}

/// The day to reconcile: the message's, or yesterday (UTC).
pub fn day_of(
    msg: &UsageReconcileMessage,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<chrono::NaiveDate> {
    if msg.day.trim().is_empty() {
        Some((now - chrono::Duration::days(1)).date_naive())
    } else {
        chrono::NaiveDate::parse_from_str(msg.day.trim(), "%Y-%m-%d").ok()
    }
}

pub async fn run(
    state: &WorkerState,
    msg: UsageReconcileMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let secret_name = std::env::var("USAGE_RECONCILE_SECRET_NAME")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| format!("coderhelm/{}/anthropic-admin-keys", state.config.stage));
    let aws = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let sm = aws_sdk_secretsmanager::Client::new(&aws);
    let raw = match sm.get_secret_value().secret_id(&secret_name).send().await {
        Ok(out) => out.secret_string().unwrap_or_default().to_string(),
        Err(e) => {
            info!(secret = %secret_name, error = %aws_sdk_secretsmanager::error::DisplayErrorContext(&e),
                "Usage reconciliation is off: no Anthropic admin-key secret");
            let off = common::usage::UsageCheck::Off {
                secret: secret_name.clone(),
            };
            store_check(state, &off).await;
            return Ok(());
        }
    };
    let teams: HashMap<String, TeamKey> =
        serde_json::from_str(&raw).map_err(|_| "admin-key secret is not the expected JSON map")?;
    let Some(day) = day_of(&msg, chrono::Utc::now()) else {
        return Err(format!("bad day {:?}", msg.day).into());
    };
    let start = format!("{day}T00:00:00Z");
    let end = format!("{}T00:00:00Z", day + chrono::Duration::days(1));

    let (mut checked_teams, mut flagged_total, mut failed_teams) = (0u64, 0u64, 0u64);
    for (team_id, key) in &teams {
        if key.admin_key.trim().is_empty() {
            continue;
        }
        let ours_item = state
            .dynamo
            .get_item()
            .table_name(&state.config.analytics_table_name)
            .key("team_id", AttributeValue::S(team_id.clone()))
            .key("period", AttributeValue::S(format!("DAY#{day}")))
            .send()
            .await?;
        let ours = ours_from_item(ours_item.item());

        let mut base: Vec<(String, String)> = vec![
            ("starting_at".into(), start.clone()),
            ("ending_at".into(), end.clone()),
            ("bucket_width".into(), "1d".into()),
        ];
        for id in &key.api_key_ids {
            base.push(("api_key_ids[]".into(), id.clone()));
        }
        let mut theirs: BTreeMap<String, DayTotals> = BTreeMap::new();
        let mut usage_q = base.clone();
        usage_q.push(("group_by[]".into(), "model".into()));
        if let Err(e) = fetch_all(
            &state.http,
            &key.admin_key,
            &format!("{API}/usage_report/messages"),
            &usage_q,
            |p| add_usage_page(p, &mut theirs),
        )
        .await
        {
            warn!(team_id, error = %e, "Anthropic usage report failed");
            failed_teams += 1;
            continue;
        }
        // The cost report has no api_key filter: compare cost only when the
        // organization is CoderHelm-only (no key filter given).
        let mut cost_q: Vec<(String, String)> = base
            .iter()
            .filter(|(k, _)| k != "api_key_ids[]" && k != "bucket_width")
            .cloned()
            .collect();
        cost_q.push(("group_by[]".into(), "description".into()));
        let compare_cost = key.api_key_ids.is_empty();
        if compare_cost {
            if let Err(e) = fetch_all(
                &state.http,
                &key.admin_key,
                &format!("{API}/cost_report"),
                &cost_q,
                |p| add_cost_page(p, &mut theirs),
            )
            .await
            {
                warn!(team_id, error = %e, "Anthropic cost report failed");
            }
        }

        let mut drifts = compare(&ours, &theirs);
        if !compare_cost {
            for d in &mut drifts {
                d.cost_drift = 0.0;
            }
        }
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert("team_id".into(), AttributeValue::S(team_id.clone()));
        item.insert(
            "period".into(),
            AttributeValue::S(format!("RECONCILE#{day}")),
        );
        item.insert(
            "checked_at".into(),
            AttributeValue::S(chrono::Utc::now().to_rfc3339()),
        );
        let mut flagged = 0;
        for d in &drifts {
            if d.flagged() {
                flagged += 1;
                warn!(
                    event = "usage_drift", team_id, day = %day, model = %d.model,
                    ours_tokens = d.ours.tokens, anthropic_tokens = d.theirs.tokens,
                    ours_cost_usd = d.ours.cost_usd, anthropic_cost_usd = d.theirs.cost_usd,
                    token_drift = d.token_drift, cost_drift = d.cost_drift,
                    "Recorded usage differs from Anthropic's report by more than 2%"
                );
            } else {
                info!(team_id, day = %day, model = %d.model, token_drift = d.token_drift,
                    cost_drift = d.cost_drift, "Usage matches Anthropic's report");
            }
            let m = &d.model;
            item.insert(
                format!("model#{m}#ours_tokens"),
                AttributeValue::N(d.ours.tokens.to_string()),
            );
            item.insert(
                format!("model#{m}#anthropic_tokens"),
                AttributeValue::N(d.theirs.tokens.to_string()),
            );
            item.insert(
                format!("model#{m}#ours_cost_usd"),
                AttributeValue::N(format!("{:.6}", d.ours.cost_usd)),
            );
            item.insert(
                format!("model#{m}#anthropic_cost_usd"),
                AttributeValue::N(format!("{:.6}", d.theirs.cost_usd)),
            );
        }
        item.insert(
            "flagged_models".into(),
            AttributeValue::N(flagged.to_string()),
        );
        checked_teams += 1;
        flagged_total += flagged;
        if let Err(e) = state
            .dynamo
            .put_item()
            .table_name(&state.config.analytics_table_name)
            .set_item(Some(item))
            .send()
            .await
        {
            warn!(team_id, error = %e, "Couldn't store the reconciliation result");
        }
    }
    let ran = common::usage::UsageCheck::Ran {
        day: day.to_string(),
        teams: checked_teams,
        flagged: flagged_total,
        failed: failed_teams,
    };
    store_check(state, &ran).await;
    Ok(())
}

/// Keep this run's outcome where the admin page reads it.
async fn store_check(state: &WorkerState, check: &common::usage::UsageCheck) {
    let item = common::usage::usage_check_item(check, &chrono::Utc::now().to_rfc3339());
    if let Err(e) = state
        .dynamo
        .put_item()
        .table_name(&state.config.analytics_table_name)
        .set_item(Some(item))
        .send()
        .await
    {
        warn!(error = %e, "Couldn't store the usage check status");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn anthropic_reports_parse_into_per_model_days() {
        let usage = json!({"data": [{"starting_at": "2026-10-17T00:00:00Z", "results": [
            {"model": "claude-opus-5-5", "uncached_input_tokens": 10, "cache_read_input_tokens": 1000,
             "cache_creation": {"ephemeral_5m_input_tokens": 200, "ephemeral_1h_input_tokens": 50},
             "output_tokens": 40},
            {"model": "claude-sonnet-5-5", "uncached_input_tokens": 1, "output_tokens": 1}
        ]}], "has_more": false});
        let cost = json!({"data": [{"results": [
            {"model": "claude-opus-5-5", "amount": "123.5", "currency": "USD", "token_type": "uncached_input_tokens"},
            {"model": "claude-opus-5-5", "amount": "26.5", "currency": "USD", "token_type": "output_tokens"},
            {"description": "Code Execution Usage", "amount": "999"}
        ]}]});
        let mut t = BTreeMap::new();
        add_usage_page(&usage, &mut t);
        add_cost_page(&cost, &mut t);
        assert_eq!(t["claude-opus-5-5"].tokens, 1300);
        assert!((t["claude-opus-5-5"].cost_usd - 1.5).abs() < 1e-9);
        assert_eq!(t["claude-sonnet-5-5"].tokens, 2);
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn our_day_item_reads_back_per_model() {
        let (_, names, values) = common::usage::add_expression(
            "run",
            "claude-opus-5-5",
            &common::usage::CallUsage {
                uncached_input: 10,
                cache_read: 1000,
                cache_write_5m: 250,
                cache_write_1h: 0,
                output: 40,
            },
        );
        let item: HashMap<String, AttributeValue> = names
            .iter()
            .map(|(ph, a)| (a.clone(), values[&ph.replace("#a", ":v")].clone()))
            .collect();
        let ours = ours_from_item(Some(&item));
        assert_eq!(ours["claude-opus-5-5"].tokens, 1300);
        assert!(ours["claude-opus-5-5"].cost_usd > 0.0);
    }

    #[test]
    fn drift_over_two_percent_is_flagged() {
        let mut ours = BTreeMap::new();
        let mut theirs = BTreeMap::new();
        ours.insert(
            "a".to_string(),
            DayTotals {
                tokens: 1000,
                cost_usd: 1.0,
            },
        );
        theirs.insert(
            "a".to_string(),
            DayTotals {
                tokens: 1015,
                cost_usd: 1.01,
            },
        );
        ours.insert(
            "b".to_string(),
            DayTotals {
                tokens: 1000,
                cost_usd: 1.0,
            },
        );
        theirs.insert(
            "b".to_string(),
            DayTotals {
                tokens: 1100,
                cost_usd: 1.0,
            },
        );
        theirs.insert(
            "c".to_string(),
            DayTotals {
                tokens: 5,
                cost_usd: 0.0,
            },
        );
        let d = compare(&ours, &theirs);
        assert_eq!(d.len(), 3);
        assert!(!d[0].flagged(), "1.5% is within the limit");
        assert!(d[1].flagged(), "9% is drift");
        assert!(d[2].flagged(), "a model we never recorded is drift");
    }

    #[test]
    fn the_day_defaults_to_yesterday() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-10-18T03:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let d = day_of(&UsageReconcileMessage::default(), now).unwrap();
        assert_eq!(d.to_string(), "2026-10-17");
        let m = UsageReconcileMessage {
            day: "2026-10-01".into(),
        };
        assert_eq!(day_of(&m, now).unwrap().to_string(), "2026-10-01");
        assert!(day_of(&UsageReconcileMessage { day: "x".into() }, now).is_none());
    }
}
