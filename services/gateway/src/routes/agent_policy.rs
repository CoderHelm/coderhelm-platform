//! Agent permissions: the team's rules, presets, autonomy limit, agent
//! switches, "Pause all agents", sandbox allowlists, the "test a call" box,
//! approval cards and sign-in prompts. The engine is `common::agent_policy`.
//!
//! Who may do what:
//!
//! - Anyone on the team sees the policy, tests a call, and sees approval
//!   cards and the decision log.
//! - `agents.approve` (members and up by default) answers a card once.
//! - `agents.permissions` (admins and up by default) edits everything, and is
//!   needed to answer a card "always", since that writes a rule.
//!
//! Every change is checked against the version the editor read: a save on a
//! policy someone else changed meanwhile is refused (409).
//!
//! Signed links (`/agent-approvals/link`) are public: the token is the
//! credential. Opening one never answers anything. A Slack or Teams button
//! opens the approval page, where the person answers signed in. An email
//! link shows the card and Approve and Deny buttons; the answer is recorded
//! as the person the email went to, only if they may approve when they click.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Extension, Form, Json,
};
use common::agent_policy::approvals::{self, Approver, Choice, DecideError, Status, Via};
use common::agent_policy::audit::{self, ActorKind, Event};
use common::agent_policy::links::{self, Channel};
use common::agent_policy::model::*;
use common::agent_policy::store::{DynamoStore, PolicyStore, StoreError};
use common::agent_policy::{engine, pause, presets, registry, sandbox, signin};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::error;

use crate::models::Claims;
use crate::AppState;

type Reply = Result<Json<Value>, (StatusCode, Json<Value>)>;

pub const CAP_EDIT: &str = "agents.permissions";
pub const CAP_APPROVE: &str = "agents.approve";

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

fn editors_only(code: StatusCode) -> (StatusCode, Json<Value>) {
    fail(
        code,
        "Only people who can edit agent permissions can change this.",
    )
}

fn approvers_only(code: StatusCode) -> (StatusCode, Json<Value>) {
    fail(
        code,
        "You can't answer approvals. Ask someone who can approve agent actions.",
    )
}

/// A team API key (or a support view) is not a person: it never changes
/// agent permissions and never answers a card or a sign-in prompt.
fn person_only(claims: &Claims) -> Result<(), (StatusCode, Json<Value>)> {
    if claims.api_key.is_some() || claims.support_view.is_some() {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only a signed-in person can change agent permissions or answer approvals.",
        ));
    }
    Ok(())
}

pub fn store(state: &AppState) -> Arc<dyn PolicyStore> {
    Arc::new(DynamoStore::new(
        state.dynamo.clone(),
        state.config.settings_table_name.clone(),
    ))
}

fn now() -> String {
    common::agent_policy::now()
}

// ─── Reading ───────────────────────────────────────────────────────────────

fn agents_view(p: &TeamPolicy) -> Value {
    Value::Array(
        Agent::ALL
            .iter()
            .map(|a| {
                let repos_off: Vec<&String> = p
                    .switches
                    .repo_off
                    .iter()
                    .filter(|(_, s)| s.contains(a))
                    .map(|(r, _)| r)
                    .collect();
                json!({
                    "key": a.key(),
                    "label": a.label(),
                    "summary": a.summary(),
                    "on": !p.switches.team_off.contains(a),
                    "repos_off": repos_off,
                })
            })
            .collect(),
    )
}

/// The whole page's data.
pub fn policy_view(p: &TeamPolicy, claims: &Claims, pending: usize, now: &str) -> Value {
    let holder = claims.holder();
    json!({
        "policy": p,
        "agents": agents_view(p),
        "preset": presets::current(p).map(|x| x.key()),
        "paused": p.paused(now),
        "pending": pending,
        "kinds": CallKind::ALL.iter().map(|k| k.key()).collect::<Vec<_>>(),
        "default_egress": DEFAULT_EGRESS,
        "can": {
            "edit": holder.holds(CAP_EDIT),
            "approve": holder.holds(CAP_APPROVE),
        },
    })
}

/// GET /api/agents/policy
pub async fn get_policy(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    let st = store(&state);
    let p = st
        .load(&claims.team_id)
        .await
        .map_err(|e| internal("Couldn't read agent permissions", e))?;
    let pending = st
        .list_approvals(&claims.team_id, true)
        .await
        .map(|v| v.iter().filter(|a| !a.expired_at(&now())).count())
        .unwrap_or(0);
    Ok(Json(policy_view(&p, &claims, pending, &now())))
}

// ─── Changing ──────────────────────────────────────────────────────────────

/// Load, check the version the editor read, change, bump, save, log.
async fn mutate(
    state: &AppState,
    claims: &Claims,
    expect: Option<u64>,
    what: &str,
    change: impl FnOnce(&mut TeamPolicy) -> Result<(), String>,
) -> Reply {
    person_only(claims)?;
    let st = store(state);
    let mut p = st
        .load(&claims.team_id)
        .await
        .map_err(|e| internal("Couldn't read agent permissions", e))?;
    if let Some(v) = expect {
        if v != p.version {
            return Err(fail(
                StatusCode::CONFLICT,
                &StoreError::Conflict.to_string(),
            ));
        }
    }
    let read = p.version;
    change(&mut p).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    p.version = read + 1;
    p.updated_by = claims.email.clone();
    p.updated_at = now();
    match st.save(&claims.team_id, &p, read).await {
        Ok(()) => {}
        Err(StoreError::Conflict) => {
            return Err(fail(
                StatusCode::CONFLICT,
                &StoreError::Conflict.to_string(),
            ))
        }
        Err(e) => return Err(internal("Couldn't save agent permissions", e)),
    }
    st.audit(
        &claims.team_id,
        Event::new(
            &claims.email,
            ActorKind::Person,
            audit::action::POLICY_SAVED,
            "ok",
        )
        .detail(what),
    )
    .await;
    let pending = st
        .list_approvals(&claims.team_id, true)
        .await
        .map(|v| v.len())
        .unwrap_or(0);
    Ok(Json(policy_view(&p, claims, pending, &now())))
}

#[derive(Deserialize)]
pub struct SettingsBody {
    version: u64,
    #[serde(default)]
    autonomy: Option<String>,
    #[serde(default)]
    ask_timeout_secs: Option<u64>,
    #[serde(default)]
    limit_egress: Option<bool>,
    #[serde(default)]
    egress: Option<Vec<String>>,
}

/// PUT /api/agents/policy: the autonomy limit, the ask timeout and the
/// sandbox network.
pub async fn save_settings(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<SettingsBody>,
) -> Reply {
    claims.require_role(3).map_err(editors_only)?;
    mutate(
        &state,
        &claims,
        Some(body.version),
        "Saved agent settings",
        |p| {
            if let Some(a) = &body.autonomy {
                let a = Autonomy::parse(a)
                    .ok_or("Pick an autonomy limit: read_only, standard or full.")?;
                // Lowering the limit is always allowed: the cap holds the rules.
                p.autonomy = a;
            }
            if let Some(t) = body.ask_timeout_secs {
                if !(MIN_ASK_TIMEOUT_SECS..=MAX_ASK_TIMEOUT_SECS).contains(&t) {
                    return Err(
                        "The approval timeout must be between one minute and seven days.".into(),
                    );
                }
                p.ask_timeout_secs = t;
            }
            if let Some(l) = body.limit_egress {
                p.sandbox.limit_egress = l;
            }
            if let Some(list) = &body.egress {
                let mut clean: Vec<String> = Vec::new();
                for d in list {
                    let d = d.trim().to_ascii_lowercase();
                    if d.is_empty() {
                        continue;
                    }
                    if !sandbox::valid_domain(&d) {
                        return Err(format!(
                            "\"{d}\" isn't a domain. Write it like registry.npmjs.org."
                        ));
                    }
                    if !clean.contains(&d) {
                        clean.push(d);
                    }
                }
                if clean.len() > 200 {
                    return Err("Keep the network allowlist under 200 domains.".into());
                }
                p.sandbox.egress = clean;
            }
            Ok(())
        },
    )
    .await
}

#[derive(Deserialize)]
pub struct RuleBody {
    version: u64,
    rule: RuleInput,
}

#[derive(Deserialize)]
pub struct RuleInput {
    scope: String,
    #[serde(default)]
    repo: Option<String>,
    #[serde(default)]
    agent: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    server: Option<String>,
    #[serde(default)]
    tool: Option<String>,
    #[serde(default)]
    risk: Option<String>,
    #[serde(default)]
    command: Option<String>,
    decision: String,
    #[serde(default)]
    locked: bool,
    #[serde(default)]
    note: String,
}

fn opt(s: &Option<String>) -> Option<String> {
    s.as_ref()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

pub fn rule_from(input: &RuleInput, by: &str, now: &str) -> Result<Rule, String> {
    let scope =
        Scope::parse(&input.scope).ok_or("Pick where the rule applies: team, repo or agent.")?;
    let agent = match opt(&input.agent) {
        Some(a) => Some(Agent::parse(&a).ok_or("That agent doesn't exist.")?),
        None => None,
    };
    let kind = match opt(&input.kind) {
        Some(k) => Some(CallKind::parse(&k).ok_or("That kind of call doesn't exist.")?),
        None => None,
    };
    let risk = match opt(&input.risk) {
        Some(r) => Some(Risk::parse(&r).ok_or("Pick a risk: read, write or consequential.")?),
        None => None,
    };
    let decision = Decision::parse(&input.decision).ok_or("Pick allow, ask or deny.")?;
    Ok(Rule {
        id: format!("r_{}", ulid::Ulid::new().to_string().to_ascii_lowercase()),
        scope,
        repo: opt(&input.repo).map(|r| r.to_ascii_lowercase()),
        agent,
        kind,
        server: opt(&input.server),
        tool: opt(&input.tool),
        risk,
        command: opt(&input.command),
        decision,
        locked: input.locked,
        note: input.note.trim().to_string(),
        source: RuleSource::Manual,
        created_by: by.to_string(),
        created_at: now.to_string(),
    })
}

/// POST /api/agents/policy/rules
pub async fn add_rule(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<RuleBody>,
) -> Reply {
    claims.require_role(3).map_err(editors_only)?;
    let rule = rule_from(&body.rule, &claims.email, &now())
        .map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let label = rule.label();
    mutate(
        &state,
        &claims,
        Some(body.version),
        &format!("Added: {label}"),
        |p| {
            engine::validate_rule(&rule, p.autonomy)?;
            if p.rules.len() >= 300 {
                return Err("A team can keep up to 300 rules.".into());
            }
            p.rules.push(rule);
            Ok(())
        },
    )
    .await
}

#[derive(Deserialize)]
pub struct VersionQuery {
    version: u64,
}

/// DELETE /api/agents/policy/rules/:id?version=
pub async fn remove_rule(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Query(q): Query<VersionQuery>,
) -> Reply {
    claims.require_role(3).map_err(editors_only)?;
    mutate(
        &state,
        &claims,
        Some(q.version),
        &format!("Removed rule {id}"),
        |p| {
            let before = p.rules.len();
            p.rules.retain(|r| r.id != id);
            if p.rules.len() == before {
                return Err("That rule is already gone.".into());
            }
            Ok(())
        },
    )
    .await
}

#[derive(Deserialize)]
pub struct PresetBody {
    version: u64,
    preset: String,
}

/// POST /api/agents/policy/preset
pub async fn apply_preset(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<PresetBody>,
) -> Reply {
    claims.require_role(3).map_err(editors_only)?;
    let preset = presets::Preset::parse(&body.preset).ok_or_else(|| {
        fail(
            StatusCode::BAD_REQUEST,
            "Pick a preset: cautious, standard or autonomous.",
        )
    })?;
    let by = claims.email.clone();
    mutate(
        &state,
        &claims,
        Some(body.version),
        &format!("Applied the {} preset", preset.key()),
        |p| {
            presets::apply(p, preset, &by, &now());
            Ok(())
        },
    )
    .await
}

#[derive(Deserialize)]
pub struct SwitchBody {
    version: u64,
    agent: String,
    #[serde(default)]
    repo: Option<String>,
    on: bool,
}

/// PUT /api/agents/policy/switches: an agent on or off, for the team or one
/// repository.
pub async fn set_switch(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<SwitchBody>,
) -> Reply {
    claims.require_role(3).map_err(editors_only)?;
    let agent = Agent::parse(&body.agent)
        .ok_or_else(|| fail(StatusCode::BAD_REQUEST, "That agent doesn't exist."))?;
    let repo = opt(&body.repo).map(|r| r.to_ascii_lowercase());
    let what = format!(
        "Turned the {} {}{}",
        agent.label().to_lowercase(),
        if body.on { "on" } else { "off" },
        repo.as_deref()
            .map(|r| format!(" in {r}"))
            .unwrap_or_default()
    );
    mutate(&state, &claims, Some(body.version), &what, |p| {
        match &repo {
            None => {
                if body.on {
                    p.switches.team_off.remove(&agent);
                } else {
                    p.switches.team_off.insert(agent);
                }
            }
            Some(r) => {
                if !r.contains('/') {
                    return Err("Name the repository as owner/name.".into());
                }
                let set = p.switches.repo_off.entry(r.clone()).or_default();
                if body.on {
                    set.remove(&agent);
                } else {
                    set.insert(agent);
                }
                if set.is_empty() {
                    p.switches.repo_off.remove(r);
                }
            }
        }
        Ok(())
    })
    .await
}

#[derive(Deserialize)]
pub struct CommandsBody {
    version: u64,
    repo: String,
    commands: Vec<String>,
}

/// PUT /api/agents/policy/commands: a repository's sandbox allowlist.
pub async fn set_commands(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<CommandsBody>,
) -> Reply {
    claims.require_role(3).map_err(editors_only)?;
    let repo = body.repo.trim().to_ascii_lowercase();
    if !repo.contains('/') {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Name the repository as owner/name.",
        ));
    }
    let by = claims.email.clone();
    mutate(
        &state,
        &claims,
        Some(body.version),
        &format!("Saved the sandbox commands for {repo}"),
        |p| {
            let mut list: Vec<AllowedCommand> = Vec::new();
            for c in &body.commands {
                let c = c.trim();
                if c.is_empty() {
                    continue;
                }
                sandbox::valid_command(c)?;
                if list.iter().any(|x| x.command == c) {
                    continue;
                }
                let kept = p
                    .sandbox
                    .commands
                    .get(&repo)
                    .and_then(|l| l.iter().find(|x| x.command == c).cloned());
                list.push(kept.unwrap_or(AllowedCommand {
                    command: c.to_string(),
                    added_by: by.clone(),
                    origin: "manual".into(),
                }));
            }
            if list.len() > 50 {
                return Err("Keep a repository's allowlist under 50 commands.".into());
            }
            if list.is_empty() {
                p.sandbox.commands.remove(&repo);
            } else {
                p.sandbox.commands.insert(repo.clone(), list);
            }
            Ok(())
        },
    )
    .await
}

#[derive(Deserialize)]
pub struct SuggestionBody {
    version: u64,
    repo: String,
    command: String,
    accept: bool,
}

/// POST /api/agents/policy/suggestions: accept a suggested command onto the
/// allowlist, or dismiss it.
pub async fn decide_suggestion(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<SuggestionBody>,
) -> Reply {
    claims.require_role(3).map_err(editors_only)?;
    let repo = body.repo.trim().to_ascii_lowercase();
    let by = claims.email.clone();
    let what = format!(
        "{} the suggested command \"{}\" for {repo}",
        if body.accept { "Accepted" } else { "Dismissed" },
        body.command
    );
    mutate(&state, &claims, Some(body.version), &what, |p| {
        let list = p.sandbox.suggestions.entry(repo.clone()).or_default();
        let Some(at) = list.iter().position(|s| s.command == body.command) else {
            return Err("That suggestion is already gone.".into());
        };
        let s = list.remove(at);
        if list.is_empty() {
            p.sandbox.suggestions.remove(&repo);
        }
        if body.accept {
            sandbox::valid_command(&s.command)?;
            let cmds = p.sandbox.commands.entry(repo.clone()).or_default();
            if !cmds.iter().any(|c| c.command == s.command) {
                cmds.push(AllowedCommand {
                    command: s.command,
                    added_by: by.clone(),
                    origin: s.origin,
                });
            }
        }
        Ok(())
    })
    .await
}

#[derive(Deserialize)]
pub struct PauseBody {
    #[serde(default)]
    reason: String,
    /// RFC 3339; empty or missing: until lifted.
    #[serde(default)]
    until: Option<String>,
}

/// POST /api/agents/pause: "Pause all agents".
pub async fn pause_all(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<PauseBody>,
) -> Reply {
    claims.require_role(3).map_err(editors_only)?;
    person_only(&claims)?;
    if body.reason.len() > 300 {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Keep the reason under 300 characters.",
        ));
    }
    let until = match opt(&body.until) {
        Some(u) => {
            let t = chrono::DateTime::parse_from_rfc3339(&u).map_err(|_| {
                fail(
                    StatusCode::BAD_REQUEST,
                    "The until-time isn't a valid date and time.",
                )
            })?;
            if t <= chrono::Utc::now() {
                return Err(fail(
                    StatusCode::BAD_REQUEST,
                    "Pick an until-time in the future.",
                ));
            }
            Some(
                t.with_timezone(&chrono::Utc)
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            )
        }
        None => None,
    };
    let st = store(&state);
    pause::pause(&*st, &claims.team_id, &claims.email, &body.reason, until)
        .await
        .map_err(|e| match e {
            StoreError::Conflict => fail(StatusCode::CONFLICT, &e.to_string()),
            e => internal("Couldn't pause agents", e),
        })?;
    get_policy(State(state), Extension(claims)).await
}

/// Sends kept work back to the tickets queue.
struct Sqs<'a>(&'a AppState);

#[async_trait::async_trait]
impl pause::Resender for Sqs<'_> {
    async fn send(&self, message: &Value) -> Result<(), String> {
        self.0
            .sqs
            .send_message()
            .queue_url(&self.0.config.ticket_queue_url)
            .message_body(message.to_string())
            .send()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

/// DELETE /api/agents/pause: lift the pause; kept work starts again.
pub async fn resume_all(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
) -> Reply {
    claims.require_role(3).map_err(editors_only)?;
    person_only(&claims)?;
    let st = store(&state);
    let resumed = pause::lift(&*st, &claims.team_id, &claims.email, &Sqs(&state))
        .await
        .map_err(|e| internal("Couldn't resume agents", e))?;
    let Json(mut v) = get_policy(State(state), Extension(claims)).await?;
    v["resumed"] = json!(resumed);
    Ok(Json(v))
}

// ─── Test a call ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct TestBody {
    agent: String,
    #[serde(default)]
    tool: String,
    #[serde(default)]
    repo: Option<String>,
    /// A sandbox command, or a domain when `kind` is egress.
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    kind: Option<String>,
}

/// The call the box describes.
pub fn test_call_for(body: &TestBody) -> Result<(Call, Option<Risk>), String> {
    let agent = Agent::parse(&body.agent).ok_or("Pick an agent.")?;
    let kind = opt(&body.kind).and_then(|k| CallKind::parse(&k));
    let repo = opt(&body.repo);
    let call = match (kind, opt(&body.command)) {
        (Some(CallKind::Egress), Some(host)) => Call::egress(agent, &host),
        (Some(CallKind::Command), Some(cmd)) | (None, Some(cmd)) if body.tool.trim().is_empty() => {
            Call::command(agent, &cmd)
        }
        _ => {
            let tool = body.tool.trim();
            if tool.is_empty() {
                return Err("Name a tool (write_file, linear__create_issue) or a command.".into());
            }
            let (kind, server) = registry::kind_of(tool);
            Call {
                agent,
                kind,
                tool: tool.to_string(),
                server,
                repo: None,
                command: None,
            }
        }
    }
    .in_repo(repo.as_deref());
    let risk = match call.kind {
        CallKind::Command | CallKind::Egress => None,
        _ => registry::default_registry().risk(&call.tool),
    };
    Ok((call, risk))
}

/// POST /api/agents/policy/test: which rule would decide this call.
pub async fn test_call(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<TestBody>,
) -> Reply {
    let (call, risk) = test_call_for(&body).map_err(|m| fail(StatusCode::BAD_REQUEST, &m))?;
    let p = store(&state)
        .load(&claims.team_id)
        .await
        .map_err(|e| internal("Couldn't read agent permissions", e))?;
    let e = engine::explain(&p, &call, risk, &now());
    Ok(Json(
        json!({ "call": call, "risk": risk, "verdict": e.verdict, "considered": e.considered }),
    ))
}

// ─── Approvals ─────────────────────────────────────────────────────────────

/// GET /api/agents/approvals?status=pending|all&run_id=&thread_id=
pub async fn list_approvals(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<HashMap<String, String>>,
) -> Reply {
    let st = store(&state);
    // Cards past their time are expired on the way.
    let _ = approvals::expire_due(&*st, &claims.team_id, &now()).await;
    let pending_only = q.get("status").map(String::as_str) != Some("all");
    let mut list = st
        .list_approvals(&claims.team_id, pending_only)
        .await
        .map_err(|e| internal("Couldn't list approvals", e))?;
    if let Some(r) = q.get("run_id").filter(|s| !s.is_empty()) {
        list.retain(|a| &a.run_id == r);
    }
    if let Some(t) = q.get("thread_id").filter(|s| !s.is_empty()) {
        list.retain(|a| &a.thread_id == t);
    }
    list.truncate(200);
    let holder = claims.holder();
    Ok(Json(json!({
        "approvals": list,
        "can": { "approve": holder.holds(CAP_APPROVE), "always": holder.holds(CAP_EDIT) },
    })))
}

/// GET /api/agents/approvals/:id
pub async fn get_approval(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Reply {
    let a = store(&state)
        .get_approval(&claims.team_id, &id)
        .await
        .map_err(|e| internal("Couldn't read the approval", e))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "That approval doesn't exist."))?;
    let holder = claims.holder();
    Ok(Json(json!({
        "approval": a,
        "can": { "approve": holder.holds(CAP_APPROVE), "always": holder.holds(CAP_EDIT) },
    })))
}

#[derive(Deserialize)]
pub struct DecideBody {
    approve: bool,
    #[serde(default)]
    choice: Option<String>,
    #[serde(default)]
    via: Option<String>,
    #[serde(default)]
    note: String,
}

pub fn decide_error(e: DecideError) -> (StatusCode, Json<Value>) {
    let code = match e {
        DecideError::NotFound => StatusCode::NOT_FOUND,
        DecideError::NotAllowed(_) => StatusCode::FORBIDDEN,
        DecideError::AlreadyDecided(_) | DecideError::Expired => StatusCode::CONFLICT,
        DecideError::Invalid(_) => StatusCode::BAD_REQUEST,
        DecideError::Store(ref m) => return internal("Couldn't record the answer", m),
    };
    fail(code, &e.to_string())
}

/// POST /api/agents/approvals/:id/decide { approve, choice, via, note }
pub async fn decide(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(body): Json<DecideBody>,
) -> Reply {
    claims.require_role(1).map_err(approvers_only)?;
    person_only(&claims)?;
    let holder = claims.holder();
    let choice = match opt(&body.choice) {
        Some(c) => Choice::parse(&c).ok_or_else(|| {
            fail(
                StatusCode::BAD_REQUEST,
                "Pick once, always_repo or always_tool.",
            )
        })?,
        None => Choice::Once,
    };
    // Signed-in answers come from the dashboard; Slack, Teams and email
    // buttons land on these pages too, so they're recorded as such.
    let via = opt(&body.via)
        .and_then(|v| Via::parse(&v))
        .unwrap_or(Via::Settings);
    let approver = Approver {
        email: claims.email.clone(),
        can_approve: holder.holds(CAP_APPROVE),
        can_edit_rules: holder.holds(CAP_EDIT),
        api_key: claims.api_key.is_some(),
    };
    let a = approvals::decide(
        &*store(&state),
        &claims.team_id,
        &id,
        &approver,
        body.approve,
        choice,
        via,
        &body.note,
        &now(),
    )
    .await
    .map_err(decide_error)?;
    Ok(Json(json!({ "approval": a })))
}

/// GET /api/agents/activity?limit=
pub async fn activity(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Query(q): Query<HashMap<String, String>>,
) -> Reply {
    let limit = q
        .get("limit")
        .and_then(|l| l.parse::<usize>().ok())
        .unwrap_or(50)
        .clamp(1, 200);
    let events = store(&state)
        .recent_audit(&claims.team_id, limit)
        .await
        .map_err(|e| internal("Couldn't read the agent log", e))?;
    Ok(Json(json!({ "events": events })))
}

// ─── Sign-in prompts ───────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct DeclineBody {
    thread_id: String,
    server_id: String,
}

/// POST /api/agents/sign-in/decline: "Not now" on a sign-in card. That
/// connection isn't asked about again in this thread.
pub async fn decline_sign_in(
    State(state): State<Arc<AppState>>,
    Extension(claims): Extension<Claims>,
    Json(body): Json<DeclineBody>,
) -> Reply {
    claims.require_role(1).map_err(approvers_only)?;
    person_only(&claims)?;
    let thread = body.thread_id.trim();
    let server = body.server_id.trim();
    if thread.is_empty() || server.is_empty() || thread.len() > 120 || server.len() > 120 {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "Name the conversation and the connection.",
        ));
    }
    signin::decline(
        &*store(&state),
        &claims.team_id,
        thread,
        server,
        &claims.email,
    )
    .await;
    Ok(Json(json!({ "declined": true })))
}

// ─── Signed links (public) ─────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct LinkQuery {
    #[serde(default)]
    t: String,
    #[serde(default, rename = "do")]
    action: String,
}

#[derive(Deserialize)]
pub struct LinkForm {
    #[serde(default)]
    t: String,
    #[serde(default, rename = "do")]
    action: String,
    #[serde(default)]
    note: String,
}

fn dashboard_origin(stage: &str) -> &'static str {
    if stage == "prod" {
        "https://app.coderhelm.com"
    } else {
        "http://localhost:3000"
    }
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn page(status: StatusCode, title: &str, body: &str) -> Response {
    let html = format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <meta name=\"robots\" content=\"noindex\"><title>{t}</title>\
         <style>body{{font-family:system-ui,sans-serif;max-width:560px;margin:48px auto;padding:0 16px;color:#1a1a1a;line-height:1.5}}\
         h1{{font-size:1.25rem}}pre{{background:#f5f5f4;padding:12px;border-radius:8px;overflow:auto;font-size:.85rem}}\
         button{{font:inherit;padding:8px 16px;border-radius:8px;border:1px solid #ccc;background:#fff;cursor:pointer;margin-right:8px}}\
         button.primary{{background:#1a1a1a;color:#fff;border-color:#1a1a1a}}textarea{{width:100%;font:inherit}}</style>\
         </head><body>{b}</body></html>",
        t = esc(title),
        b = body
    );
    let mut r = (status, html).into_response();
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    r
}

fn redirect(to: &str) -> Response {
    let mut r = StatusCode::SEE_OTHER.into_response();
    if let Ok(v) = HeaderValue::from_str(to) {
        r.headers_mut().insert(header::LOCATION, v);
    }
    r
}

/// What the link page shows for a token, before any answer.
pub enum LinkView {
    /// Slack or Teams: open the approval page (signed in).
    Open(String),
    /// Email: show the card and ask.
    Ask,
    Invalid(&'static str),
}

pub fn link_view(
    secret: &str,
    stage: &str,
    token: &str,
    action: &str,
    now: i64,
) -> (LinkView, Option<links::LinkClaims>) {
    match links::verify(secret, token.trim(), now) {
        Ok(c) => match c.channel {
            Channel::Slack | Channel::Teams => (
                LinkView::Open(format!(
                    "{}/settings/agents/?approval={}&do={}",
                    dashboard_origin(stage),
                    urlencoding::encode(&c.approval_id),
                    if action == "deny" { "deny" } else { "approve" }
                )),
                Some(c),
            ),
            Channel::Email => (LinkView::Ask, Some(c)),
        },
        Err(links::LinkError::Expired) => {
            (LinkView::Invalid("This approval link has expired."), None)
        }
        Err(_) => (LinkView::Invalid("This approval link isn't valid."), None),
    }
}

fn card_html(a: &approvals::Approval) -> String {
    let args = serde_json::to_string_pretty(&a.args).unwrap_or_default();
    format!(
        "<p><strong>{}</strong></p><p>{}</p><p>Agent: {} &middot; Risk: {}{}</p><pre>{}</pre>",
        esc(&a.headline()),
        esc(if a.why.is_empty() { &a.reason } else { &a.why }),
        esc(a.agent.label()),
        esc(a.risk.map_or("unknown", |r| r.as_str())),
        a.repo
            .as_deref()
            .map(|r| format!(" &middot; {}", esc(r)))
            .unwrap_or_default(),
        esc(common::truncate_str(&args, 3000)),
    )
}

/// GET /agent-approvals/link?t=&do=: never answers anything.
pub async fn link(State(state): State<Arc<AppState>>, Query(q): Query<LinkQuery>) -> Response {
    let (view, claims) = link_view(
        &state.secrets.jwt_secret,
        &state.config.stage,
        &q.t,
        &q.action,
        chrono::Utc::now().timestamp(),
    );
    match view {
        LinkView::Open(to) => redirect(&to),
        LinkView::Invalid(m) => page(
            StatusCode::BAD_REQUEST,
            "Approval link",
            &format!("<h1>{}</h1>", esc(m)),
        ),
        LinkView::Ask => {
            let c = claims.expect("an email link has claims");
            let a = match store(&state).get_approval(&c.team_id, &c.approval_id).await {
                Ok(Some(a)) => a,
                _ => {
                    return page(
                        StatusCode::NOT_FOUND,
                        "Approval",
                        "<h1>That approval doesn't exist.</h1>",
                    )
                }
            };
            if a.status != Status::Pending || a.expired_at(&now()) {
                return page(
                    StatusCode::OK,
                    "Approval",
                    &format!(
                        "<h1>This was already answered ({}).</h1>{}",
                        esc(if a.status == Status::Pending {
                            "expired"
                        } else {
                            a.status.as_str()
                        }),
                        card_html(&a)
                    ),
                );
            }
            let t = esc(&q.t);
            page(
                StatusCode::OK,
                "Approve an agent action",
                &format!(
                    "<h1>An agent is waiting for your approval</h1>{}\
                     <form method=\"post\"><input type=\"hidden\" name=\"t\" value=\"{t}\">\
                     <p><label>Note (optional)<br><textarea name=\"note\" rows=\"2\" maxlength=\"500\"></textarea></label></p>\
                     <button class=\"primary\" name=\"do\" value=\"approve\">Approve once</button>\
                     <button name=\"do\" value=\"deny\">Deny</button></form>\
                     <p><small>To allow this every time, open the approval page in CoderHelm.</small></p>",
                    card_html(&a)
                ),
            )
        }
    }
}

/// The member an email link was sent to, and what they hold now.
async fn email_approver(state: &AppState, team_id: &str, email: &str) -> Option<Approver> {
    let mut start = None;
    loop {
        let out = state
            .dynamo
            .query()
            .table_name(&state.config.users_table_name)
            .key_condition_expression("pk = :pk AND begins_with(sk, :p)")
            .expression_attribute_values(
                ":pk",
                aws_sdk_dynamodb::types::AttributeValue::S(team_id.into()),
            )
            .expression_attribute_values(
                ":p",
                aws_sdk_dynamodb::types::AttributeValue::S("USER#".into()),
            )
            .set_exclusive_start_key(start)
            .send()
            .await
            .ok()?;
        for item in out.items() {
            let get = |k: &str| {
                item.get(k)
                    .and_then(|v| v.as_s().ok())
                    .cloned()
                    .unwrap_or_default()
            };
            if get("email").eq_ignore_ascii_case(email) && get("status") != "invited" {
                let holder = super::roles::holder_for_role(state, team_id, &get("role")).await;
                return Some(Approver {
                    email: email.to_string(),
                    can_approve: holder.holds(CAP_APPROVE),
                    // An email answer is once only; "always" is on the page.
                    can_edit_rules: false,
                    api_key: false,
                });
            }
        }
        match out.last_evaluated_key() {
            Some(k) if !k.is_empty() => start = Some(k.clone()),
            _ => return None,
        }
    }
}

/// POST /agent-approvals/link: the Approve or Deny button on an email
/// link's page.
pub async fn link_answer(State(state): State<Arc<AppState>>, Form(f): Form<LinkForm>) -> Response {
    let now_s = chrono::Utc::now().timestamp();
    let c = match links::verify(&state.secrets.jwt_secret, f.t.trim(), now_s) {
        Ok(c) if c.channel == Channel::Email && !c.recipient.is_empty() => c,
        Ok(_) => {
            return page(
                StatusCode::BAD_REQUEST,
                "Approval link",
                "<h1>Open this approval in CoderHelm to answer it.</h1>",
            )
        }
        Err(e) => {
            return page(
                StatusCode::BAD_REQUEST,
                "Approval link",
                &format!("<h1>{}</h1>", esc(&e.to_string())),
            )
        }
    };
    let approve = match f.action.as_str() {
        "approve" => true,
        "deny" => false,
        _ => {
            return page(
                StatusCode::BAD_REQUEST,
                "Approval link",
                "<h1>Pick Approve or Deny.</h1>",
            )
        }
    };
    let Some(approver) = email_approver(&state, &c.team_id, &c.recipient).await else {
        return page(
            StatusCode::FORBIDDEN,
            "Approval link",
            "<h1>You're no longer on this team.</h1>",
        );
    };
    match approvals::decide(
        &*store(&state),
        &c.team_id,
        &c.approval_id,
        &approver,
        approve,
        Choice::Once,
        Via::Email,
        &f.note,
        &now(),
    )
    .await
    {
        Ok(_) => page(
            StatusCode::OK,
            "Approval recorded",
            &format!(
                "<h1>{}</h1><p>The agent carries on with your answer. You can close this page.</p>",
                if approve { "Approved once." } else { "Denied." }
            ),
        ),
        Err(e) => page(
            StatusCode::CONFLICT,
            "Approval",
            &format!("<h1>{}</h1>", esc(&e.to_string())),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules_from_the_form_are_parsed_and_checked() {
        let input = RuleInput {
            scope: "repo".into(),
            repo: Some(" Acme/Web ".into()),
            agent: None,
            kind: Some("mcp".into()),
            server: Some("linear".into()),
            tool: Some("create_*".into()),
            risk: None,
            command: None,
            decision: "deny".into(),
            locked: false,
            note: "Linear writes go through triage".into(),
        };
        let r = rule_from(&input, "a@x.dev", "2026-10-24T00:00:00Z").unwrap();
        assert_eq!(r.repo.as_deref(), Some("acme/web"));
        assert_eq!(r.kind, Some(CallKind::Mcp));
        assert!(engine::validate_rule(&r, Autonomy::Standard).is_ok());
        let bad = RuleInput {
            decision: "maybe".into(),
            ..input
        };
        assert!(rule_from(&bad, "a", "").is_err());
    }

    #[test]
    fn the_test_box_reads_tools_commands_and_domains() {
        let (c, r) = test_call_for(&TestBody {
            agent: "coder".into(),
            tool: "linear__create_issue".into(),
            repo: Some("acme/web".into()),
            command: None,
            kind: None,
        })
        .unwrap();
        assert_eq!(c.kind, CallKind::Mcp);
        assert_eq!(c.server.as_deref(), Some("linear"));
        assert_eq!(r, Some(Risk::Write));
        let (c, _) = test_call_for(&TestBody {
            agent: "coder".into(),
            tool: String::new(),
            repo: Some("acme/web".into()),
            command: Some("npm run test".into()),
            kind: None,
        })
        .unwrap();
        assert_eq!(c.kind, CallKind::Command);
        let (c, _) = test_call_for(&TestBody {
            agent: "coder".into(),
            tool: String::new(),
            repo: None,
            command: Some("pypi.org".into()),
            kind: Some("egress".into()),
        })
        .unwrap();
        assert_eq!(c.kind, CallKind::Egress);
        assert!(test_call_for(&TestBody {
            agent: "nobody".into(),
            tool: "x".into(),
            repo: None,
            command: None,
            kind: None
        })
        .is_err());
    }

    fn signed(channel: Channel, recipient: &str, exp: i64) -> String {
        links::sign(
            "s3cret",
            &links::LinkClaims {
                team_id: "TEAM#a".into(),
                approval_id: "ap_1".into(),
                recipient: recipient.into(),
                channel,
                expires_at: exp,
            },
        )
    }

    #[test]
    fn slack_and_teams_links_open_the_approval_page_and_never_answer() {
        for ch in [Channel::Slack, Channel::Teams] {
            let (v, _) = link_view("s3cret", "prod", &signed(ch, "", 2_000), "deny", 1_000);
            match v {
                LinkView::Open(to) => {
                    assert_eq!(
                        to,
                        "https://app.coderhelm.com/settings/agents/?approval=ap_1&do=deny"
                    )
                }
                _ => panic!("expected a redirect"),
            }
        }
        // Email links ask first.
        let (v, c) = link_view(
            "s3cret",
            "prod",
            &signed(Channel::Email, "ann@x.dev", 2_000),
            "approve",
            1_000,
        );
        assert!(matches!(v, LinkView::Ask));
        assert_eq!(c.unwrap().recipient, "ann@x.dev");
        // Forged, wrong key or expired links show a plain message.
        assert!(matches!(
            link_view(
                "other",
                "prod",
                &signed(Channel::Slack, "", 2_000),
                "",
                1_000
            )
            .0,
            LinkView::Invalid(_)
        ));
        assert!(matches!(
            link_view(
                "s3cret",
                "prod",
                &signed(Channel::Slack, "", 2_000),
                "",
                2_000
            )
            .0,
            LinkView::Invalid("This approval link has expired.")
        ));
    }

    #[test]
    fn api_keys_and_support_views_are_not_people() {
        let mut c: Claims = serde_json::from_value(json!({
            "sub": "USER#1", "team_id": "TEAM#a", "email": "a@x.dev", "role": "owner", "exp": 0, "iat": 0
        }))
        .unwrap();
        assert!(person_only(&c).is_ok());
        c.api_key = Some(crate::models::ApiKeyRef {
            id: "k1".into(),
            name: "ci".into(),
            scopes: vec!["agents.approve".into(), "agents.permissions".into()],
            rate_limit_per_minute: 60,
            expires_at: None,
            created_by: "a@x.dev".into(),
        });
        assert_eq!(person_only(&c).unwrap_err().0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn link_pages_escape_what_they_show() {
        assert_eq!(
            esc("<script>\"x\"&'"),
            "&lt;script&gt;&quot;x&quot;&amp;&#39;"
        );
    }
}
