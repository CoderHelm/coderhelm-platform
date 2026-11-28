//! Duplicate follow-ups close themselves (`common::followup_dedupe`).
//!
//! The follow-ups of one source ticket (its family: filed from it, and from
//! those) are read with their latest runs, decided together, and:
//!
//! - each topic's keeper is decided once and recorded (`FOLLOWUP_KEEP#`),
//!   and every later sweep, here or in the worker, keeps the recorded one
//!   (`followup_dedupe::keeper`); a closed member is never kept;
//! - each later duplicate's run is closed by CoderHelm with the same steps
//!   as a person's close (`run_close::close_as_coderhelm`), saying
//!   "Duplicate of KEY (topic). Closing; the work continues there.", once
//!   the keeper, read again, still carries the work and no person
//!   committed to the duplicate's pull request or asked for changes on it;
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
use common::followup_dedupe::{self as fd, Candidate, Kept, Plan};
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
        let has_pr = !super::run_close::run_prs(&item).is_empty();
        let outcome = s(&item, "pr_outcome");
        return Some(Candidate {
            key: key.to_string(),
            title: s(&item, "title"),
            body: s(&item, "ticket_body"),
            run_id: latest.run_id.clone(),
            status: s(&item, "status"),
            has_pr,
            pr_open: has_pr && !matches!(outcome.as_str(), "merged" | "closed"),
            pr_merged: outcome == "merged",
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

/// The keeper decisions recorded for `root`'s family; `None` when they
/// can't be read (nothing is decided then).
async fn kept_records(state: &AppState, team_id: &str, root: &str) -> Option<Vec<Kept>> {
    let mut out = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let page = state
            .dynamo
            .query()
            .table_name(&state.config.settings_table_name)
            .key_condition_expression("pk = :p AND begins_with(sk, :s)")
            .expression_attribute_values(":p", AttributeValue::S(team_id.to_string()))
            .expression_attribute_values(":s", AttributeValue::S(fd::keep_record_prefix(root)))
            .consistent_read(true)
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .ok()?;
        for i in page.items() {
            let members = i
                .get("members")
                .and_then(|v| v.as_l().ok())
                .map(|l| l.iter().filter_map(|v| v.as_s().ok().cloned()).collect())
                .unwrap_or_default();
            out.push(Kept {
                keep: s(i, "keep"),
                members,
                topic: s(i, "topic"),
                at: s(i, "decided_at"),
            });
        }
        match page.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => break,
        }
    }
    Some(out)
}

/// Records `k` as decided, once (a record already there stands). False
/// when it could not be written: nothing closes in its favour then.
async fn record_keeper(state: &AppState, team_id: &str, root: &str, k: &Kept) -> bool {
    let res = state
        .dynamo
        .put_item()
        .table_name(&state.config.settings_table_name)
        .item("pk", AttributeValue::S(team_id.to_string()))
        .item("sk", AttributeValue::S(fd::keep_record_sk(root, &k.keep)))
        .item("source", AttributeValue::S(root.to_string()))
        .item("keep", AttributeValue::S(k.keep.clone()))
        .item(
            "members",
            AttributeValue::L(
                k.members
                    .iter()
                    .map(|m| AttributeValue::S(m.clone()))
                    .collect(),
            ),
        )
        .item("topic", AttributeValue::S(k.topic.clone()))
        .item(
            "decided_at",
            AttributeValue::S(chrono::Utc::now().to_rfc3339()),
        )
        .condition_expression("attribute_not_exists(sk)")
        .send()
        .await;
    match res {
        Ok(_) => true,
        Err(e)
            if e.as_service_error()
                .is_some_and(|se| se.is_conditional_check_failed_exception()) =>
        {
            true
        }
        Err(e) => {
            warn!(team_id, root, keep = %k.keep, error = %common::run_fail::error_text(&e), "Follow-up keeper not recorded");
            false
        }
    }
}

/// Whether the duplicate's run (`run_id`) may lose its pull requests: no
/// open one is held by a person ([`fd::person_holds`]). `Err` says why it
/// stays, also when one can't be read.
async fn no_person_holds(state: &AppState, team_id: &str, run_id: &str) -> Result<(), String> {
    let item = super::run_close::load_run(state, team_id, run_id)
        .await?
        .ok_or("the run is gone")?;
    if matches!(s(&item, "pr_outcome").as_str(), "merged" | "closed") {
        return Ok(());
    }
    let prs = super::run_close::run_prs(&item);
    if prs.is_empty() {
        return Ok(());
    }
    let installation = item
        .get("installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .filter(|n| *n > 0);
    let installation = match installation {
        Some(i) => i,
        None => super::api::get_team_installation_id(state, team_id)
            .await
            .map_err(|_| "no GitHub installation".to_string())?,
    };
    let token = crate::auth::github_app::get_installation_token(state, installation)
        .await
        .map_err(|e| format!("no GitHub token: {e}"))?;
    for pr in prs {
        let base = format!(
            "https://api.github.com/repos/{}/{}/pulls/{}",
            pr.owner, pr.repo, pr.number
        );
        let get = |url: String| {
            let token = token.clone();
            async move {
                match super::merge_control::gh_get(state, &token, &url).await {
                    Ok((200, v)) => Ok(v),
                    Ok((code, _)) => Err(format!("GitHub answered {code}")),
                    Err(e) => Err(e),
                }
            }
        };
        let p = get(base.clone()).await?;
        if p["state"].as_str() != Some("open") {
            continue;
        }
        let commits = get(format!("{base}/commits?per_page=100")).await?;
        let reviews = get(format!("{base}/reviews?per_page=100")).await?;
        let list = |v: &Value| v.as_array().cloned().unwrap_or_default();
        if fd::person_holds(&list(&commits), &list(&reviews)) {
            return Err(format!(
                "a person committed to #{} or asked for changes on it",
                pr.number
            ));
        }
    }
    Ok(())
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
    let Some(kept) = kept_records(state, team_id, root).await else {
        warn!(
            team_id,
            root, "Follow-up keeper records not read: nothing decided"
        );
        return Report::default();
    };
    let plan = fd::decide(&cands, Some(&judge), &kept).await;
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
    let mut unrecorded: Vec<String> = Vec::new();
    for k in report.plan.record.clone() {
        if !record_keeper(state, team_id, root, &k).await {
            unrecorded.push(k.keep.clone());
        }
    }
    for c in report.plan.close.clone() {
        if unrecorded.contains(&c.keep) {
            report.not_closed.push((
                c.dup.clone(),
                "The keeper decision was not recorded.".into(),
            ));
            continue;
        }
        // The keeper, read again: it must still be in the running and, for
        // a duplicate with an open pull request, still carry the work.
        let dup_open = cands.iter().any(|x| x.key == c.dup && x.pr_open);
        let keeper_ok = candidate(state, team_id, &c.keep)
            .await
            .is_some_and(|k| !k.out() && (!dup_open || k.holds_work()));
        if !keeper_ok {
            report.not_closed.push((
                c.dup.clone(),
                format!("{} no longer carries the work.", c.keep),
            ));
            continue;
        }
        if let Err(why) = no_person_holds(state, team_id, &c.run_id).await {
            report.not_closed.push((c.dup.clone(), why));
            continue;
        }
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
                fd::flagged_comment(&f, other),
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
