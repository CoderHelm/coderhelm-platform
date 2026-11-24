//! Duplicate follow-ups close themselves (`common::followup_dedupe`).
//!
//! The follow-ups of one source ticket (its family: filed from it, and from
//! those) are read with their latest runs, decided together, and:
//!
//! - each later duplicate's run is closed by CoderHelm with the same steps
//!   as a person's close (`run_close::close_as_coderhelm`), saying
//!   "Duplicate of KEY (topic). Closing; the work continues there.";
//! - two follow-ups taking opposite approaches are both noted on Jira and
//!   neither is closed.
//!
//! It runs on its own when a person starts a follow-up from CoderHelm
//! (`run_start`), and by hand for a source: `POST /api/followups/dedupe`
//! with `{"source": "ABC-1"}` (add `"apply": false` to see the plan only).

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use axum::{extract::State, http::StatusCode, Extension, Json};
use common::followup_dedupe::{self as fd, Candidate, Plan};
use common::followup_tickets as ft;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::models::Claims;
use crate::AppState;

type Item = HashMap<String, AttributeValue>;

fn s(item: &Item, key: &str) -> String {
    item.get(key)
        .and_then(|v| v.as_s().ok())
        .cloned()
        .unwrap_or_default()
}

/// The most follow-ups one family reads.
const MAX_FAMILY: usize = 40;

/// The follow-up keys CoderHelm filed from `source` (covering records
/// aside).
async fn filed_from(state: &AppState, team_id: &str, source: &str) -> Vec<String> {
    let prefix = format!("FOLLOWUP#{}#", source.trim().to_ascii_uppercase());
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let page = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :s)")
            .expression_attribute_values(":p", AttributeValue::S(team_id.to_string()))
            .expression_attribute_values(":s", AttributeValue::S(prefix.clone()))
            .set_exclusive_start_key(start.take())
            .send()
            .await;
        let Ok(page) = page else { break };
        for i in page.items() {
            let key = s(i, "key");
            if !key.is_empty() && s(i, "covers") != "1" && !out.contains(&key) {
                out.push(key);
            }
        }
        match page.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    out
}

/// The family of `root`: every follow-up filed from it, and from those.
pub fn family_of(root: &str, filed: &HashMap<String, Vec<String>>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut queue = vec![root.trim().to_ascii_uppercase()];
    while let Some(k) = queue.pop() {
        for f in filed.get(&k).into_iter().flatten() {
            let f = f.to_ascii_uppercase();
            if f != root && !out.contains(&f) && out.len() < MAX_FAMILY {
                out.push(f.clone());
                queue.push(f);
            }
        }
    }
    out.sort();
    out
}

async fn family(state: &AppState, team_id: &str, root: &str) -> Vec<String> {
    let mut filed: HashMap<String, Vec<String>> = HashMap::new();
    let mut queue = vec![root.trim().to_ascii_uppercase()];
    while let Some(k) = queue.pop() {
        if filed.contains_key(&k) || filed.len() > MAX_FAMILY {
            continue;
        }
        let kids = filed_from(state, team_id, &k).await;
        queue.extend(kids.iter().cloned());
        filed.insert(k, kids);
    }
    family_of(root, &filed)
}

/// The ticket `key` is a follow-up of: CoderHelm's record of it, else the
/// sentence its latest run's ticket text ends with (a follow-up filed
/// before the records existed).
async fn parent_of(state: &AppState, team_id: &str, key: &str) -> Option<String> {
    if let Some(src) = super::jira_webhook::followup_record_source(state, team_id, key).await {
        if !src.is_empty() {
            return Some(src);
        }
    }
    let c = candidate(state, team_id, key).await?;
    ft::source_of_body(&c.body).filter(|s| ft::project_of(s).is_some())
}

/// The source at the top of `key`'s family; `None` when `key` is no
/// follow-up.
pub(crate) async fn root_of(state: &AppState, team_id: &str, key: &str) -> Option<String> {
    let mut at = key.trim().to_ascii_uppercase();
    let mut found = None;
    for _ in 0..5 {
        match parent_of(state, team_id, &at).await {
            Some(src) if !src.eq_ignore_ascii_case(&at) => {
                at = src.to_ascii_uppercase();
                found = Some(at.clone());
            }
            _ => break,
        }
    }
    found
}

/// One follow-up as the decision reads it: its latest run's title, text,
/// status and pull request, else what CoderHelm's record keeps of it.
async fn candidate(state: &AppState, team_id: &str, key: &str) -> Option<Candidate> {
    let runs = common::log_findings::runs_for_ticket(
        &state.dynamo,
        &state.config.runs_table_name,
        team_id,
        key,
    )
    .await
    .ok()
    .unwrap_or_default();
    if let Some(latest) = runs.iter().max_by(|a, b| a.run_id.cmp(&b.run_id)) {
        let item = super::run_close::load_run(state, team_id, &latest.run_id)
            .await
            .ok()
            .flatten()?;
        return Some(Candidate {
            key: key.to_string(),
            title: s(&item, "title"),
            body: s(&item, "ticket_body"),
            run_id: latest.run_id.clone(),
            status: s(&item, "status"),
            has_pr: !super::run_close::run_prs(&item).is_empty(),
        });
    }
    let record = super::jira_webhook::followup_record(state, team_id, key).await?;
    let (title, body, ..) = super::run_start::followup_ticket(&record)?;
    Some(Candidate {
        key: key.to_string(),
        title,
        body,
        ..Default::default()
    })
}

/// What one sweep decided and did.
#[derive(Debug, Default)]
pub struct Report {
    pub plan: Plan,
    pub closed: Vec<String>,
    pub not_closed: Vec<(String, String)>,
    pub flagged: Vec<String>,
}

impl Report {
    pub fn json(&self) -> Value {
        json!({
            "close": self.plan.close.iter().map(|c| json!({
                "duplicate": c.dup, "run_id": c.run_id, "keep": c.keep, "topic": c.topic,
            })).collect::<Vec<_>>(),
            "flag": self.plan.flag.iter().map(|f| json!({
                "a": f.a, "b": f.b, "topic": f.topic,
            })).collect::<Vec<_>>(),
            "closed": self.closed,
            "not_closed": self.not_closed.iter().map(|(k, why)| json!({"key": k, "why": why})).collect::<Vec<_>>(),
            "flagged": self.flagged,
        })
    }
}

/// Reads `root`'s family, decides, and (with `apply`) closes the
/// duplicates and flags the opposed pairs.
pub async fn sweep(state: &AppState, team_id: &str, root: &str, apply: bool) -> Report {
    let keys = family(state, team_id, root).await;
    let mut cands = Vec::new();
    for k in &keys {
        if let Some(c) = candidate(state, team_id, k).await {
            cands.push(c);
        }
    }
    let judge = super::log_findings::GatewayJudge::for_team(state, team_id).await;
    let plan = fd::decide(&cands, Some(&judge)).await;
    let mut report = Report {
        plan,
        ..Default::default()
    };
    info!(
        team_id,
        root,
        tickets = cands.len(),
        close = report.plan.close.len(),
        flag = report.plan.flag.len(),
        apply,
        "Follow-up duplicates read"
    );
    if !apply {
        return report;
    }
    for c in report.plan.close.clone() {
        let said = fd::close_comment(&c.keep, &c.topic);
        let r = super::run_close::close_as_coderhelm(state, team_id, &c.run_id, &said).await;
        if r.ok() {
            report.closed.push(c.dup.clone());
        } else {
            report.not_closed.push((c.dup.clone(), r.message.clone()));
        }
    }
    let url = jira_comment_url(state, team_id).await;
    for f in report.plan.flag.clone() {
        let Some(url) = url.as_deref() else { break };
        for (on, other) in [(&f.a, &f.b), (&f.b, &f.a)] {
            let say = common::conversation::Say::note(
                &format!("followup_opposed_{}", other.to_ascii_lowercase()),
                fd::flag_comment(other, &f.topic),
            );
            match super::conversation::jira_at(
                state,
                team_id,
                url,
                on,
                say,
                "note",
                "Two follow-ups disagree",
            )
            .await
            {
                Ok(_) => report.flagged.push(on.clone()),
                Err(e) => {
                    warn!(team_id, key = %on, error = %e, "Opposed follow-up note not posted")
                }
            }
        }
    }
    report
}

async fn jira_comment_url(state: &AppState, team_id: &str) -> Option<String> {
    state
        .dynamo
        .get_item()
        .table_name(&state.config.jira_config_table_name)
        .key("pk", AttributeValue::S(team_id.to_string()))
        .key("sk", AttributeValue::S("JIRA#config".into()))
        .send()
        .await
        .ok()
        .and_then(|o| o.item)
        .map(|i| s(&i, "add_comment_url"))
        .filter(|u| !u.is_empty())
}

#[derive(Deserialize)]
pub struct DedupeBody {
    source: String,
    #[serde(default = "yes")]
    apply: bool,
}

fn yes() -> bool {
    true
}

/// POST /api/followups/dedupe — close a source's duplicate follow-ups now
/// (`apply: false` answers the plan only).
pub async fn dedupe(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<DedupeBody>,
) -> Result<Json<Value>, StatusCode> {
    claims.require_role(1)?; // member+, as closing a run is
    if ft::project_of(&body.source).is_none() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let report = sweep(&state, &claims.team_id, &body.source, body.apply).await;
    Ok(Json(report.json()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_family_is_every_generation_once() {
        let filed = HashMap::from([
            (
                "ABC-1".to_string(),
                vec!["ABC-2".to_string(), "ABC-3".to_string()],
            ),
            (
                "ABC-2".to_string(),
                vec!["ABC-4".to_string(), "ABC-1".to_string()],
            ),
            ("ABC-3".to_string(), vec!["ABC-4".to_string()]),
        ]);
        assert_eq!(family_of("ABC-1", &filed), vec!["ABC-2", "ABC-3", "ABC-4"]);
        assert!(family_of("ABC-9", &filed).is_empty());
    }
}
