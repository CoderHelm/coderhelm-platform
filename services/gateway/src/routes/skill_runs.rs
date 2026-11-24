//! Scheduled skill runs: a team's synced skill on a schedule, and Run now.
//!
//! Everyone on the team sees the schedules and their runs; only admins
//! create, change, pause, delete or run one. Each schedule is one
//! EventBridge schedule in the team's time zone that sends the worker a
//! `skill_run` message; Run now sends the same message with the run's id,
//! so the dashboard can open the run page at once. The rules live in
//! `common::skill_runs`.

use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue as A;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use common::plugins::kv::{Expect, KvError};
use common::plugins::schedule::{EventBridge, Scheduler};
use common::skill_runs::{self as sr, cron, RunEntry, Schedule, SkillRunMessage};
use common::skills::{Kind, SkillStore};
use serde_json::{json, Value};
use tracing::{error, info};

use crate::models::{Claims, WorkerMessage};
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

fn fail(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message })))
}

fn internal(context: &str, e: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    error!(error = %e, "{context}");
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong on our side. Try again.",
    )
}

fn admin(claims: &Claims) -> Result<(), (StatusCode, Json<Value>)> {
    if matches!(claims.role.as_str(), "admin" | "owner") {
        Ok(())
    } else {
        Err(fail(
            StatusCode::FORBIDDEN,
            "Only admins can manage scheduled skill runs.",
        ))
    }
}

fn kv(state: &AppState) -> common::plugins::kv::Dynamo {
    common::plugins::kv::Dynamo {
        client: state.dynamo.clone(),
        table: state.config.settings_table_name.clone(),
    }
}

fn scheduler(state: &AppState) -> EventBridge {
    EventBridge {
        client: state.scheduler.clone(),
        group: state.config.plugin_schedule_group.clone(),
        role_arn: state.config.plugin_scheduler_role_arn.clone(),
        queue_arn: state.config.ticket_queue_arn.clone(),
    }
}

/// A schedule as the dashboard shows it: its record and when it runs next.
pub fn view(s: &Schedule, now: chrono::DateTime<chrono::Utc>) -> Value {
    let next = match (cron::parse(&s.cron), cron::time_zone(&s.timezone)) {
        (Ok(c), Ok(tz)) if !s.paused => c
            .next_after(tz, now)
            .map(|t| json!({ "at": t.to_rfc3339(), "label": cron::local_label(t, tz) })),
        _ => None,
    };
    let mut v = serde_json::to_value(s).unwrap_or_default();
    v["next_run"] = next.unwrap_or(Value::Null);
    v
}

/// Puts the schedule's EventBridge schedule in line with it: there while it
/// runs, gone while paused.
async fn sync_schedule(sched: &dyn Scheduler, team: &str, s: &Schedule) -> Result<(), String> {
    let name = sr::eventbridge_name(team, &s.id);
    if s.paused {
        return sched.delete(&name).await.map(|_| ());
    }
    let expr = cron::parse(&s.cron)?.eventbridge();
    sched
        .cron_in(&name, &expr, &s.timezone, &sr::fire_message(team, &s.id))
        .await
}

/// Takes what the dashboard sent, keeping what only CoderHelm sets.
fn from_body(body: &Value, base: Schedule) -> Result<Schedule, String> {
    let mut s: Schedule = serde_json::from_value(body.clone())
        .map_err(|e| format!("The schedule isn't readable: {e}"))?;
    s.id = base.id;
    s.created_by = base.created_by;
    s.created_at = base.created_at;
    s.running = base.running;
    s.last_run = base.last_run;
    s.updated_at = chrono::Utc::now().to_rfc3339();
    sr::validate(&mut s)?;
    Ok(s)
}

/// GET /api/skill-runs — the team's schedules.
pub async fn list(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let all = sr::list(&kv(&state), &claims.team_id)
        .await
        .map_err(|e| internal("list skill schedules", e))?;
    let now = chrono::Utc::now();
    Ok(Json(json!({
        "schedules": all.iter().map(|s| view(s, now)).collect::<Vec<_>>(),
        "can_edit": matches!(claims.role.as_str(), "admin" | "owner"),
    })))
}

/// GET /api/skill-runs/options — what a schedule can pick: the team's
/// synced skills, its connected MCP servers and AWS accounts.
pub async fn options(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let st = SkillStore {
        dynamo: &state.dynamo,
        table: &state.config.settings_table_name,
        team: &claims.team_id,
    };
    let skills: Vec<Value> = st
        .items()
        .await
        .map_err(|e| internal("list skills", e))?
        .into_iter()
        .filter(|i| i.kind == Kind::Skill)
        .map(|i| {
            json!({
                "repo": i.repo, "path": i.path, "name": i.name,
                "description": i.description, "enabled": i.enabled, "files": i.files,
            })
        })
        .collect();
    let mut servers: Vec<Value> = Vec::new();
    let rt = super::mcp_servers::runtime(&state, &claims.team_id);
    if let Ok(remote) = rt.store.list_servers(&rt.team).await {
        servers.extend(
            remote
                .iter()
                .map(|s| json!({ "id": s.id, "name": s.name, "kind": "remote" })),
        );
    }
    if let Ok(out) = state
        .dynamo
        .query()
        .table_name(super::plugins::mcp_table(&state))
        .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
        .expression_attribute_values(":pk", A::S(claims.team_id.clone()))
        .expression_attribute_values(":p", A::S("PLUGIN#".into()))
        .send()
        .await
    {
        for it in out.items() {
            if let Some(id) = it
                .get("sk")
                .and_then(|v| v.as_s().ok())
                .and_then(|s| s.strip_prefix("PLUGIN#"))
            {
                servers.push(json!({ "id": id, "name": id, "kind": "catalog" }));
            }
        }
    }
    let mut accounts: Vec<String> = Vec::new();
    if !state.config.aws_insights_table_name.is_empty() {
        if let Ok(out) = state
            .dynamo
            .query()
            .table_name(&state.config.aws_insights_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(":pk", A::S(claims.team_id.clone()))
            .expression_attribute_values(":p", A::S("AWS_CONN#".into()))
            .send()
            .await
        {
            accounts = out
                .items()
                .iter()
                .filter_map(|it| {
                    it.get("sk")?
                        .as_s()
                        .ok()?
                        .strip_prefix("AWS_CONN#")
                        .map(str::to_string)
                })
                .collect();
        }
    }
    Ok(Json(json!({
        "skills": skills,
        "servers": servers,
        "aws_accounts": accounts,
    })))
}

/// POST /api/skill-runs — a new schedule.
pub async fn create(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let kv = kv(&state);
    let have = sr::list(&kv, &claims.team_id)
        .await
        .map_err(|e| internal("list skill schedules", e))?;
    if have.len() >= sr::MAX_SCHEDULES {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            &format!("A team keeps at most {} schedules.", sr::MAX_SCHEDULES),
        ));
    }
    let now = chrono::Utc::now().to_rfc3339();
    let s = from_body(
        &body,
        Schedule {
            id: ulid::Ulid::new().to_string(),
            created_by: claims.email.clone(),
            created_at: now,
            ..Default::default()
        },
    )
    .map_err(|e| fail(StatusCode::BAD_REQUEST, &e))?;
    sync_schedule(&scheduler(&state), &claims.team_id, &s)
        .await
        .map_err(|e| internal("create the skill schedule", e))?;
    sr::put(&kv, &claims.team_id, &s, Expect::Absent)
        .await
        .map_err(|e| internal("save skill schedule", e))?;
    info!(team = %claims.team_id, schedule = %s.id, skill = %s.skill_name, "Skill schedule created");
    Ok(Json(json!({ "schedule": view(&s, chrono::Utc::now()) })))
}

/// PUT /api/skill-runs/:id — a changed schedule (pausing is a change).
pub async fn update(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Reply {
    admin(&claims)?;
    let kv = kv(&state);
    let Some((old, rev)) = sr::get(&kv, &claims.team_id, &id)
        .await
        .map_err(|e| internal("get skill schedule", e))?
    else {
        return Err(fail(StatusCode::NOT_FOUND, "That schedule doesn't exist."));
    };
    let s = from_body(&body, old).map_err(|e| fail(StatusCode::BAD_REQUEST, &e))?;
    sync_schedule(&scheduler(&state), &claims.team_id, &s)
        .await
        .map_err(|e| internal("update the skill schedule", e))?;
    match sr::put(&kv, &claims.team_id, &s, Expect::Rev(rev)).await {
        Ok(_) => {}
        Err(KvError::Conflict) => {
            return Err(fail(
                StatusCode::CONFLICT,
                "The schedule changed meanwhile; open it again.",
            ))
        }
        Err(e) => return Err(internal("save skill schedule", e)),
    }
    Ok(Json(json!({ "schedule": view(&s, chrono::Utc::now()) })))
}

/// DELETE /api/skill-runs/:id — the schedule and its EventBridge schedule.
/// Its runs stay with the team's runs.
pub async fn remove(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    admin(&claims)?;
    scheduler(&state)
        .delete(&sr::eventbridge_name(&claims.team_id, &id))
        .await
        .map_err(|e| internal("delete the skill schedule", e))?;
    sr::delete(&kv(&state), &claims.team_id, &id)
        .await
        .map_err(|e| internal("delete skill schedule", e))?;
    Ok(Json(json!({ "ok": true })))
}

/// POST /api/skill-runs/:id/run — Run now: a run listed at once (queued),
/// then started by the worker. Runs a paused schedule too: it is how a
/// schedule is tested.
pub async fn run_now(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    admin(&claims)?;
    let kv = kv(&state);
    let Some((s, _)) = sr::get(&kv, &claims.team_id, &id)
        .await
        .map_err(|e| internal("get skill schedule", e))?
    else {
        return Err(fail(StatusCode::NOT_FOUND, "That schedule doesn't exist."));
    };
    if let Some(r) = &s.running {
        let fresh = chrono::DateTime::parse_from_rfc3339(&r.at)
            .map(|t| {
                (chrono::Utc::now() - t.with_timezone(&chrono::Utc)).num_seconds()
                    < sr::STALE_RUN_SECS
            })
            .unwrap_or(false);
        if fresh {
            return Err(fail(
                StatusCode::CONFLICT,
                "A run of this schedule is still working; it runs one at a time.",
            ));
        }
    }
    let run_id = ulid::Ulid::new().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let entry = RunEntry {
        run_id: run_id.clone(),
        schedule_id: s.id.clone(),
        trigger: "manual".into(),
        requested_by: claims.email.clone(),
        status: "queued".into(),
        started_at: now.clone(),
        ..Default::default()
    };
    sr::put_run(&kv, &claims.team_id, &entry)
        .await
        .map_err(|e| internal("save skill run", e))?;
    if !state.config.runs_table_name.is_empty() {
        let _ = state
            .dynamo
            .put_item()
            .table_name(&state.config.runs_table_name)
            .item("team_id", A::S(claims.team_id.clone()))
            .item("run_id", A::S(run_id.clone()))
            .item("status", A::S("queued".into()))
            .item("status_run_id", A::S(format!("queued#{run_id}")))
            .item("ticket_source", A::S("skill_run".into()))
            .item("ticket_id", A::S(format!("skill:{}", s.id)))
            .item("title", A::S(format!("Skill run: {}", s.skill_name)))
            .item("current_pass", A::S("skill_run".into()))
            .item("skill_schedule_id", A::S(s.id.clone()))
            .item("sender", A::S(claims.email.clone()))
            .item("created_at", A::S(now.clone()))
            .item("updated_at", A::S(now))
            .condition_expression("attribute_not_exists(run_id)")
            .send()
            .await;
        if !state.config.analytics_table_name.is_empty() {
            let counts = common::team_counts::DynamoCounts {
                dynamo: state.dynamo.clone(),
                table: state.config.analytics_table_name.clone(),
            };
            if let Err(e) = common::team_counts::count_run_started(
                &counts,
                &claims.team_id,
                &run_id,
                &entry.started_at,
            )
            .await
            {
                tracing::warn!(error = %e, "Couldn't count the skill run");
            }
        }
    }
    let msg = WorkerMessage::SkillRun(SkillRunMessage {
        team_id: claims.team_id.clone(),
        schedule_id: s.id.clone(),
        run_id: run_id.clone(),
        trigger: "manual".into(),
        requested_by: claims.email.clone(),
    });
    crate::routes::github_webhook::send_to_queue(&state, &state.config.ticket_queue_url, &msg)
        .await
        .map_err(|e| internal("queue the skill run", e.as_u16()))?;
    Ok(Json(json!({ "run_id": run_id })))
}

/// GET /api/skill-runs/:id/runs — the schedule's runs, newest first.
pub async fn runs(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    let runs = sr::runs(&kv(&state), &claims.team_id, &id)
        .await
        .map_err(|e| internal("list skill runs", e))?;
    Ok(Json(json!({ "runs": runs })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::plugins::schedule::{MemoryScheduler, Planned};

    fn sample() -> Schedule {
        let mut s = Schedule {
            id: "01S".into(),
            skill_repo: "acme/skills".into(),
            skill_path: "skills/visit-count/SKILL.md".into(),
            skill_name: "visit-count".into(),
            cron: "23 7 * * MON".into(),
            timezone: "America/New_York".into(),
            ..Default::default()
        };
        sr::validate(&mut s).unwrap();
        s
    }

    #[tokio::test]
    async fn a_schedule_is_an_eventbridge_schedule_while_it_runs_and_none_while_paused() {
        let sched = MemoryScheduler::default();
        let mut s = sample();
        sync_schedule(&sched, "TEAM#a", &s).await.unwrap();
        let name = sr::eventbridge_name("TEAM#a", "01S");
        match sched.items.lock().unwrap().get(&name).cloned().unwrap() {
            Planned::Cron(expr, body) => {
                assert_eq!(expr, "cron(23 7 ? * MON *)");
                assert_eq!(body["type"], "skill_run");
                assert_eq!(body["schedule_id"], "01S");
            }
            other => panic!("{other:?}"),
        }
        s.paused = true;
        sync_schedule(&sched, "TEAM#a", &s).await.unwrap();
        assert!(!sched.has(&name));
    }

    #[test]
    fn what_only_coderhelm_sets_is_kept_from_the_stored_schedule() {
        let base = Schedule {
            id: "01S".into(),
            created_by: "a@example.com".into(),
            created_at: "2026-11-01T00:00:00Z".into(),
            ..sample()
        };
        let mut body = serde_json::to_value(sample()).unwrap();
        body["id"] = json!("other");
        body["created_by"] = json!("mallory@example.com");
        let s = from_body(&body, base).unwrap();
        assert_eq!(s.id, "01S");
        assert_eq!(s.created_by, "a@example.com");
        body["cron"] = json!("* *");
        assert!(from_body(&body, sample()).is_err());
    }

    #[test]
    fn the_list_says_when_each_schedule_runs_next() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-11-24T15:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let v = view(&sample(), now);
        assert_eq!(v["next_run"]["label"], "Mon 30 Nov 07:23 EST");
        let mut p = sample();
        p.paused = true;
        assert!(view(&p, now)["next_run"].is_null());
    }
}
