//! The plan chat's tools that run in the worker: the team's skills, agents
//! and always-apply standards for planning, and its read-only data sources
//! and connections.
//!
//! The chat itself streams from the gateway, which calls this once per
//! message to learn the tools and what to tell the model (`setup`), and
//! once per tool call (`call`). Each call is checked, bounded and logged
//! exactly as a planning run's: the same `TeamSkills`, `LiveSources` and
//! `OpsTools`, read-only, with the chat as the work they're logged under.

use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::agent::llm::{ToolDefinition, ToolExecutor};
use crate::clients::github::GitHubClient;
use crate::WorkerState;

use super::live_sources::{LiveSources, Role};
use super::ops_tools::OpsTools;
use super::skills::{Reader, TeamSkills, Work};

/// One request from the gateway.
#[derive(Debug, Clone, Deserialize)]
pub struct Request {
    /// "setup" or "call".
    pub op: String,
    pub team_id: String,
    /// The chat, so its reads, asks and lookups are logged together.
    pub chat_id: String,
    /// The repository the chat is about (`owner/name`), or empty.
    #[serde(default)]
    pub repo: String,
    /// Its branch, for an agent's reads.
    #[serde(default)]
    pub branch: String,
    /// For `setup`: keep the always-apply standards on the access log (the
    /// chat's first message only).
    #[serde(default)]
    pub record_applied: bool,
    /// For `call`: the tool and its input.
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub input: Value,
    /// Reads and asks this chat message already made.
    #[serde(default)]
    pub reads_used: usize,
    #[serde(default)]
    pub asks_used: usize,
}

/// Where the chat's records say they came from.
pub fn origin(chat_id: &str) -> String {
    format!("plan chat {}", common::truncate_str(chat_id.trim(), 40))
}

/// Tools that write, which a plan chat never gets.
fn writes(name: &str) -> bool {
    common::skills::AGENT_WRITE_TOOLS.contains(&name)
}

struct Loaded {
    skills: Option<TeamSkills>,
    live: Option<LiveSources>,
    ops: Option<OpsTools>,
}

async fn load(state: &WorkerState, req: &Request) -> Loaded {
    let origin = origin(&req.chat_id);
    let skills = TeamSkills::load(
        state,
        &req.team_id,
        Reader::Planner,
        origin.clone(),
        Work::new(req.repo.clone(), &["planner"], Vec::new()),
    )
    .await
    .map(|s| {
        s.with_task("Planning in the plan chat")
            .with_used(req.reads_used, req.asks_used)
    });
    let live = LiveSources::load(state, Role::Planner, &req.team_id, &req.repo, 0)
        .await
        .map(|l| l.with_run("", origin.clone()));
    let ops = OpsTools::load(state, &req.team_id, &req.repo, "planner", 0)
        .await
        .map(|o| o.with_run("", origin.clone()));
    Loaded { skills, live, ops }
}

fn defs(l: &Loaded) -> Vec<ToolDefinition> {
    let mut tools: Vec<ToolDefinition> = Vec::new();
    if let Some(s) = &l.skills {
        tools.extend(s.tools());
    }
    if let Some(v) = &l.live {
        tools.extend(v.tools());
    }
    if let Some(o) = &l.ops {
        tools.extend(o.tools());
    }
    tools.retain(|t| !writes(&t.name));
    tools
}

/// The data source tools, for an asked agent's reads.
struct Sources<'a> {
    live: Option<&'a LiveSources>,
    ops: Option<&'a OpsTools>,
    state: &'a WorkerState,
}

#[async_trait::async_trait]
impl ToolExecutor for Sources<'_> {
    async fn execute(
        &self,
        name: &str,
        input: &Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        if writes(name) {
            return Ok(json!("Not available here."));
        }
        if let Some(o) = self.ops {
            if let Some(out) = o.handle(self.state, name, input).await {
                return Ok(json!(out));
            }
        }
        if let Some(l) = self.live {
            if let Some(out) = l.handle(self.state, name, input).await {
                return Ok(json!(out));
            }
        }
        Ok(json!(format!("Unknown tool: {name}")))
    }
}

/// What a connection's tool reads, for the chat's "Looked up" chip.
fn connection_label(name: &str) -> Option<&'static str> {
    [
        ("newrelic_", "New Relic"),
        ("confluence_", "Confluence"),
        ("metrics_", "CloudWatch metrics"),
        ("airflow_", "Airflow"),
        ("aws_identity", "AWS account"),
    ]
    .iter()
    .find(|(p, _)| name.starts_with(p))
    .map(|(_, l)| *l)
}

/// What the chat shows for a tool call: a skill read, an agent asked, a
/// data source looked up, with a link to it.
pub fn used(kind: &str, label: &str, href: &str) -> Value {
    json!({ "kind": kind, "label": label, "href": href })
}

fn item_href(kind: &str, repo: &str, path: &str) -> String {
    format!(
        "/skills/{kind}/?repo={}&path={}",
        urlencoding::encode(repo),
        urlencoding::encode(path)
    )
}

/// Handles one request. Never fails: a problem is an answer the model reads.
pub async fn handle(state: &WorkerState, req: Request) -> Value {
    let l = load(state, &req).await;
    match req.op.as_str() {
        "setup" => {
            let tools = defs(&l);
            let mut prompt = String::new();
            if let Some(s) = &l.skills {
                prompt.push_str(&s.prompt_section());
                if req.record_applied {
                    s.record_applied(state).await;
                }
            }
            if let Some(v) = &l.live {
                prompt.push_str(&v.prompt_section());
            }
            if let Some(o) = &l.ops {
                prompt.push_str(&o.prompt_section());
            }
            info!(team_id = %req.team_id, tools = tools.len(), "Plan chat tools ready");
            json!({
                "tools": tools.iter().map(|t| json!({
                    "name": t.name, "description": t.description, "input_schema": t.input_schema
                })).collect::<Vec<_>>(),
                "prompt": prompt,
            })
        }
        "call" => call(state, &req, &l).await,
        other => json!({ "text": format!("Unknown request: {other}") }),
    }
}

async fn call(state: &WorkerState, req: &Request, l: &Loaded) -> Value {
    let name = req.name.as_str();
    let input = &req.input;
    if writes(name) || !defs(l).iter().any(|t| t.name == name) {
        return json!({ "text": format!("{name} isn't one of the plan chat's tools.") });
    }
    if let Some(s) = &l.skills {
        if matches!(name, "read_skill" | "read_skill_file" | "ask_team_agent") {
            let github = github_for(state, &req.team_id).await;
            let (owner, repo) = req.repo.split_once('/').unwrap_or(("", ""));
            let branch = if req.branch.is_empty() {
                "main"
            } else {
                req.branch.as_str()
            };
            let reads = github.as_ref().filter(|_| !owner.is_empty()).map(|g| {
                super::agent_tools::RepoReads {
                    github: g,
                    owner,
                    repo,
                    git_ref: branch,
                    http: &state.http,
                }
            });
            let sources = Sources {
                live: l.live.as_ref(),
                ops: l.ops.as_ref(),
                state,
            };
            s.offer(&defs(l));
            let host = s.host(&sources, reads);
            let text = s
                .handle(state, name, input, &host)
                .await
                .unwrap_or_default();
            let asked = input["name"].as_str().unwrap_or("");
            let item = s.item(asked);
            let used = item.map(|m| {
                let kind = m.kind.as_str();
                let href = item_href(kind, &m.repo, &m.path);
                if name == "ask_team_agent" {
                    super::plan_chat::used("agent", &m.name, &href)
                } else {
                    super::plan_chat::used("skill", &m.name, &href)
                }
            });
            return json!({ "text": text, "used": used });
        }
    }
    let sources = Sources {
        live: l.live.as_ref(),
        ops: l.ops.as_ref(),
        state,
    };
    let text = match sources.execute(name, input).await {
        Ok(Value::String(t)) => t,
        Ok(v) => v.to_string(),
        Err(e) => format!("({name} failed: {e})"),
    };
    let label = connection_label(name)
        .map(str::to_string)
        .or_else(|| input["source"].as_str().map(str::to_string))
        .unwrap_or_else(|| "a data source".into());
    json!({ "text": text, "used": used("source", &label, "/data-sources/?tab=activity") })
}

async fn github_for(state: &WorkerState, team_id: &str) -> Option<GitHubClient> {
    let id = state
        .dynamo
        .get_item()
        .table_name(&state.config.teams_table_name)
        .key(
            "team_id",
            aws_sdk_dynamodb::types::AttributeValue::S(team_id.to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("META".into()),
        )
        .send()
        .await
        .ok()?
        .item()?
        .get("github_installation_id")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .filter(|&n| n > 0)?;
    GitHubClient::new(
        &state.secrets.github_app_id,
        &state.secrets.github_private_key,
        id,
        &state.http,
    )
    .inspect_err(|e| warn!(error = %e, "Plan chat: no GitHub client for an agent's reads"))
    .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_reads_with_defaults() {
        let r: Request = serde_json::from_value(json!({
            "op": "setup", "team_id": "t", "chat_id": "c1"
        }))
        .unwrap();
        assert_eq!((r.reads_used, r.asks_used, r.record_applied), (0, 0, false));
        assert_eq!(origin(" c1 "), "plan chat c1");
    }

    #[test]
    fn writes_are_never_offered_and_sources_are_named() {
        assert!(writes("confluence_append") && writes("write_file"));
        assert!(!writes("confluence_page"));
        assert_eq!(connection_label("newrelic_nrql"), Some("New Relic"));
        assert_eq!(connection_label("logs_search"), None);
        assert_eq!(
            item_href("agent", "acme/playbook", "agents/x.md"),
            "/skills/agent/?repo=acme%2Fplaybook&path=agents%2Fx.md"
        );
    }
}
